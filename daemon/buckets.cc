/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "buckets.h"
#include "bucket_manager.h"
#include "connection.h"
#include "cookie.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "memcached.h"
#include "resource_allocation_domain.h"
#include "settings.h"
#include "stats.h"
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <platform/json_log_conversions.h>
#include <serverless/config.h>
#include <statistics/labelled_collector.h>
#include <utilities/throttle_utilities.h>

Bucket::Bucket(std::size_t idx) : index(idx) {};

void Bucket::reset() {
    std::lock_guard<std::mutex> guard(mutex);
    engine.reset();
    state = Bucket::State::None;
    name.clear();
    setEngine(nullptr);
    clusterConfiguration.reset();
    max_document_size = default_max_item_size;
    supportedFeatures = {};
    read_units_used = 0;
    write_units_used = 0;
    throttle_gauge.reset();
    throttle_reserved = std::numeric_limits<std::size_t>::max();
    throttle_hard_limit = std::numeric_limits<std::size_t>::max();
    num_throttled = 0;
    throttle_wait_time = 0;
    num_commands = 0;
    num_commands_with_metered_units = 0;
    num_metered_dcp_messages = 0;
    num_rejected = 0;
    data_ingress_status = cb::mcbp::Status::Success;
    file_chunk_read_bytes = 0;

    for (auto& c : responseCounters) {
        c.reset();
    }
    subjson_operation_times.reset();
    timings.reset();
    for (auto& s : stats) {
        s.reset();
    }
    type = BucketType::Unknown;
    throttledConnections.resize(Settings::instance().getNumWorkerThreads());
    management_operation_in_progress = false;
    pause_cancellation_source = folly::CancellationSource::invalid();
}

bool Bucket::supports(cb::engine::Feature feature) {
    return supportedFeatures.contains(feature);
}

nlohmann::json Bucket::to_json() const {
    const bool serverless = cb::serverless::isEnabled();
    std::lock_guard<std::mutex> guard(mutex);
    auto self = shared_from_this();

    if (state != State::None) {
        try {
            nlohmann::json json;
            json["index"] = index;
            json["connections"] =
                    self.use_count() - 2; // exclude this and BucketManager
            json["connections_closing"] = curr_conn_closing.load();
            json["state"] = state.load();
            json["references"] = references.load();
            json["name"] = name;
            json["type"] = type;
            json["data_ingress_status"] = data_ingress_status.load();
            json["num_rejected"] = num_rejected.load();
            json["num_throttled"] = num_throttled.load();
            json["throttle_reserved"] =
                    cb::throttle::limit_to_json(throttle_reserved.load());
            json["throttle_hard_limit"] =
                    cb::throttle::limit_to_json(throttle_hard_limit.load());
            json["throttle_wait_time"] = throttle_wait_time.load();
            json["num_commands"] = num_commands.load();
            if (serverless) {
                json["ru"] = read_units_used.load();
                json["wu"] = write_units_used.load();
                json["num_commands_with_metered_units"] =
                        num_commands_with_metered_units.load();
                json["num_metered_dcp_messages"] =
                        num_metered_dcp_messages.load();
            }
            return json;
        } catch (const std::exception& e) {
            LOG_ERROR_CTX("Failed to generate bucket details",
                          {"error", e.what()});
        }
    }
    return {};
}

void Bucket::addStats(const BucketStatCollector& collector) const {
    thread_stats thread_stats;
    thread_stats.aggregate(stats);

    using namespace cb::stats;
    collector.addStat(Key::cmd_get, thread_stats.cmd_get);
    collector.addStat(Key::cmd_set, thread_stats.cmd_set);
    collector.addStat(Key::cmd_flush, thread_stats.cmd_flush);

    collector.addStat(Key::cmd_subdoc_lookup, thread_stats.cmd_subdoc_lookup);
    collector.addStat(Key::cmd_subdoc_mutation,
                      thread_stats.cmd_subdoc_mutation);
    collector.addStat(Key::subdoc_offload_count,
                      thread_stats.subdoc_offload_count);

    collector.addStat(Key::bytes_subdoc_lookup_total,
                      thread_stats.bytes_subdoc_lookup_total);
    collector.addStat(Key::bytes_subdoc_lookup_extracted,
                      thread_stats.bytes_subdoc_lookup_extracted);
    collector.addStat(Key::bytes_subdoc_mutation_total,
                      thread_stats.bytes_subdoc_mutation_total);
    collector.addStat(Key::bytes_subdoc_mutation_inserted,
                      thread_stats.bytes_subdoc_mutation_inserted);

    collector.addStat(Key::stat_timings_mem_usage,
                      statTimings.getMemFootPrint());

    // bucket specific totals
    auto& current_bucket_timings = timings;
    uint64_t mutations = current_bucket_timings.get_aggregated_mutation_stats();
    uint64_t lookups = current_bucket_timings.get_aggregated_retrieval_stats();
    collector.addStat(Key::cmd_mutation, mutations);
    collector.addStat(Key::cmd_lookup, lookups);

    collector.addStat(Key::get_hits, thread_stats.get_hits);
    collector.addStat(Key::get_misses, thread_stats.get_misses);
    collector.addStat(Key::delete_misses, thread_stats.delete_misses);
    collector.addStat(Key::delete_hits, thread_stats.delete_hits);
    collector.addStat(Key::incr_misses, thread_stats.incr_misses);
    collector.addStat(Key::incr_hits, thread_stats.incr_hits);
    collector.addStat(Key::decr_misses, thread_stats.decr_misses);
    collector.addStat(Key::decr_hits, thread_stats.decr_hits);
    collector.addStat(Key::cas_misses, thread_stats.cas_misses);
    collector.addStat(Key::cas_hits, thread_stats.cas_hits);
    collector.addStat(Key::cas_badval, thread_stats.cas_badval);
    collector.addStat(Key::bytes_read, thread_stats.bytes_read);
    collector.addStat(Key::bytes_written, thread_stats.bytes_written);
    collector.addStat(Key::conn_yields, thread_stats.conn_yields);
    collector.addStat(Key::conn_timeslice_yields,
                      thread_stats.conn_timeslice_yields);
    collector.addStat(Key::subdoc_update_races,
                      thread_stats.subdoc_update_races);

    collector.addStat(Key::cmd_lock, thread_stats.cmd_lock);
    collector.addStat(Key::lock_errors, thread_stats.lock_errors);

    auto& respCounters = responseCounters;
    // Ignore success responses by starting from begin + 1
    uint64_t total_resp_errors = std::accumulate(
            std::begin(respCounters) + 1, std::end(respCounters), uint64_t(0));
    collector.addStat(Key::total_resp_errors, total_resp_errors);

    std::size_t connections = 0;
    {
        std::lock_guard<std::mutex> guard(mutex);
        auto self = shared_from_this();
        connections = self.use_count() - 2; // exclude this and BucketManager
    }
    collector.addStat(Key::curr_bucket_connections, connections);
    collector.addStat(Key::curr_bucket_connections_closing,
                      curr_conn_closing);

    collector.addStat(Key::items_in_transit, items_in_transit);

    collector.addStat(Key::reject_count_total, num_rejected);
    collector.addStat(Key::throttle_count_total, num_throttled);
    collector.addStat(Key::file_chunk_read, file_chunk_read_bytes.load());

    using namespace std::chrono;
    collector.addStat(
            Key::throttle_seconds_total,
            duration_cast<duration<double>>(microseconds(throttle_wait_time))
                    .count());

    collector.addStat(Key::throttle_reserved, throttle_reserved.load());
    collector.addStat(Key::throttle_hard_limit, throttle_hard_limit.load());
}

void Bucket::addMeteringMetrics(const BucketStatCollector& collector) const {
    using namespace cb::stats;
    if (hasEngine()) {
        // engine-related metering (e.g., disk usage)
        auto err = getEngine().get_prometheus_stats(
                collector, cb::prometheus::MetricGroup::Metering);
        if (err != cb::engine_errc::success) {
            LOG_WARNING_CTX(
                    "Bucket::addMeteringMetrics(): Failed to get metering "
                    "stats",
                    {"bucket", name},
                    {"error", err});
        }
    }

    collector.addStat(Key::op_count_total, num_commands);

    // the spec declares that some components may need to perform some
    // actions "on behalf of" other components - e.g., credit back
    // CUs, or throttle.
    // For now, simply report such metrics as "for kv", until we have
    // more information recorded internally to support this.
    auto forKV = collector.withLabel("for", "kv");

    // metering
    forKV.addStat(Key::meter_ru_total, read_units_used);
    forKV.addStat(Key::meter_wu_total, write_units_used);
    // kv does not currently account for actual compute (i.e., CPU) units
    // but other components do. Expose it for consistency and ease of use
    forKV.addStat(Key::meter_cu_total, 0);

    // credits
    forKV.addStat(Key::credit_ru_total, 0);
    forKV.addStat(Key::credit_wu_total, 0);
    forKV.addStat(Key::credit_cu_total, 0);

    // throttling
    forKV.addStat(Key::reject_count_total, num_rejected);
    forKV.addStat(Key::throttle_count_total, num_throttled);

    using namespace std::chrono;
    forKV.addStat(
            Key::throttle_seconds_total,
            duration_cast<duration<double>>(microseconds(throttle_wait_time))
                    .count());
}

cb::engine_errc Bucket::setThrottleLimits(std::size_t reserved,
                                          std::size_t hard) {
    if (reserved > hard) {
        return cb::engine_errc::invalid_arguments;
    }

    throttle_reserved.store(reserved);
    throttle_hard_limit.store(hard);
    return cb::engine_errc::success;
}

DcpIface* Bucket::getDcpIface() const {
    return bucketDcp;
}

bool Bucket::hasEngine() const {
    return bool(engine);
}

EngineIface& Bucket::getEngine() const {
    return *engine;
}

void Bucket::destroyEngine(bool force) {
    engine.get_deleter().force = force;
    engine.reset();
}

void Bucket::setEngine(unique_engine_ptr engine_) {
    engine = std::move(engine_);
    bucketDcp = dynamic_cast<DcpIface*>(engine.get());
}

void Bucket::consumedUnits(std::size_t units, ResourceAllocationDomain domain) {
    switch (domain) {
    case ResourceAllocationDomain::Bucket:
        throttle_gauge.increment(units);
        return;
    case ResourceAllocationDomain::Global:
        BucketManager::instance().consumedResources(units);
        return;
    case ResourceAllocationDomain::None:
        // The calling connection was allowed to bypass throttling
        // and using the "none" pool. Ignore it.
        return;
    }
    throw std::invalid_argument(
            "Bucket::consumedUnits() Unexpected ResourceAllocationDomain");
}

void Bucket::recordDcpMeteringReadBytes(const Connection& conn,
                                        std::size_t nread,
                                        ResourceAllocationDomain domain) {
    // The node supervisor runs for free
    if (conn.isNodeSupervisor()) {
        return;
    }

    auto& settings = Settings::instance();
    const auto ru = settings.toReadUnits(nread);
    consumedUnits(ru, domain);
    if (conn.isSubjectToMetering()) {
        read_units_used += ru;
        ++num_metered_dcp_messages;
    }
}

void Bucket::documentExpired(size_t nbytes) {
    if (nbytes) {
        auto& settings = Settings::instance();
        const auto wu = settings.toWriteUnits(nbytes);
        throttle_gauge.increment(wu);
        write_units_used += wu;
    } else {
        throttle_gauge.increment(1);
        ++write_units_used;
    }
}

void Bucket::commandExecuted(const Cookie& cookie) {
    ++num_commands;
    auto& connection = cookie.getConnection();

    if (connection.isNodeSupervisor()) {
        // The node supervisor should not be metered or subject to throttling
        // unless it tries to use an effective user
        if (cookie.getEffectiveUser()) {
            // but it may still run with euid which may be metered
            if (cookie.testPrivilege(cb::rbac::Privilege::Unmetered)
                        .success()) {
                return;
            }
        } else {
            return;
        }
    }

    auto& settings = Settings::instance();
    const auto [read, write] = cookie.getDocumentRWBytes();
    if (read || write) {
        auto ru = settings.toReadUnits(read);
        auto wu = settings.toWriteUnits(write);
        if (cookie.isDurable()) {
            wu *= 2;
        }

        const auto throttleUnits =
                std::size_t(ru * cookie.getReadThottlingFactor()) +
                (wu * cookie.getWriteThottlingFactor());

        consumedUnits(throttleUnits, cookie.getResourceAllocationDomain());
        throttle_wait_time += cookie.getTotalThrottleTime();
        const auto [nr, nw] = cookie.getDocumentMeteringRWUnits();
        read_units_used += nr;
        write_units_used += nw;
        if (nr || nw) {
            ++num_commands_with_metered_units;
        }
    }
}

void Bucket::rejectCommand(const Cookie&) {
    ++num_rejected;
}

void Bucket::tick() {
    if (throttle_hard_limit == std::numeric_limits<std::size_t>::max()) {
        throttle_gauge.reset();
    } else {
        throttle_gauge.tick(throttle_hard_limit);
    }
    // iterate over connections
    FrontEndThread::forEach([this](auto& thr) {
        std::deque<Connection*> connections;
        throttledConnections[thr.index].swap(connections);

        for (auto& c : connections) {
            if (c->reEvaluateThrottledCookies()) {
                throttledConnections[thr.index].push_back(c);
            }
        }
    });
}

void Bucket::deleteThrottledCommands() {
    FrontEndThread::forEach([this](auto& thr) {
        // Now nuke the throttledConnections belonging to this Bucket
        std::deque<Connection*> connections;
        throttledConnections[thr.index].swap(connections);

        for (auto& c : connections) {
            c->resetThrottledCookies();
            // Not re-adding this to throttledConnections as we've just
            // un-throttled everything
        }
    });
}

std::pair<bool, ResourceAllocationDomain> Bucket::shouldThrottle(
        const Connection& connection, std::size_t units) {
    if (throttle_reserved == std::numeric_limits<std::size_t>::max() ||
        connection.isUnthrottled()) {
        return {false, ResourceAllocationDomain::None};
    }

    // Check our reserved units
    if (throttle_gauge.isBelow(throttle_reserved, units)) {
        return {false, ResourceAllocationDomain::Bucket};
    }

    // Check the hard limit (if set)
    if (throttle_hard_limit != std::numeric_limits<std::size_t>::max()) {
        if (throttle_gauge.isBelow(throttle_hard_limit, units)) {
            return {false, ResourceAllocationDomain::Bucket};
        }
        return {true, ResourceAllocationDomain::None};
    }

    if (BucketManager::instance().isUnassignedResourcesAvailable(units)) {
        return {false, ResourceAllocationDomain::Global};
    }

    return {true, ResourceAllocationDomain::None};
}

std::pair<bool, ResourceAllocationDomain> Bucket::shouldThrottleDcp(
        const Connection& connection) {
    auto ret = shouldThrottle(connection, 1);
    if (ret.first) {
        num_throttled++;
        Expects(ret.second == ResourceAllocationDomain::None);
    }
    return ret;
}

bool Bucket::shouldThrottle(Cookie& cookie,
                            bool addConnectionToThrottleList,
                            size_t pendingBytes) {
    const auto& header = cookie.getHeader();
    if (header.isResponse() ||
        cb::mcbp::is_server_magic(cb::mcbp::Magic(header.getMagic())) ||
        !is_subject_for_throttling(header.getRequest().getClientOpcode())) {
        // Never throttle response messages or server ops
        return false;
    }

    auto [throttle, domain] =
            shouldThrottle(cookie.getConnection(), pendingBytes);
    if (throttle) {
        Expects(domain == ResourceAllocationDomain::None);
        if (cookie.getConnection().isNonBlockingThrottlingMode()) {
            num_rejected++;
        } else {
            num_throttled++;
            if (addConnectionToThrottleList) {
                auto* c = &cookie.getConnection();
                throttledConnections[c->getThread().index].push_back(c);
            }
        }
        return true;
    }
    cookie.setResourceAllocationDomain(domain);
    return false;
}

namespace BucketValidator {
std::string validateBucketName(std::string_view name) {
    if (name.empty()) {
        return "Name can't be empty";
    }

    if (name.length() > MaxBucketNameLength) {
        return "Name too long (exceeds " + std::to_string(MaxBucketNameLength) +
               ")";
    }

    // Verify that the bucket name only consists of legal characters
    for (const uint8_t ii : name) {
        if (!(isupper(ii) || islower(ii) || isdigit(ii))) {
            switch (ii) {
            case '_':
            case '-':
            case '.':
            case '%':
                break;
            default:
                return "Name contains invalid characters";
            }
        }
    }

    return {};
}
}

std::string format_as(Bucket::State state) {
    switch (state) {
    case Bucket::State::None:
        return "none";
    case Bucket::State::Creating:
        return "creating";
    case Bucket::State::Initializing:
        return "initializing";
    case Bucket::State::Ready:
        return "ready";
    case Bucket::State::Pausing:
        return "pausing";
    case Bucket::State::Paused:
        return "paused";
    case Bucket::State::Destroying:
        return "destroying";
    }
    throw std::invalid_argument("Invalid bucket state: " +
                                std::to_string(int(state)));
}

void to_json(nlohmann::json& json, const Bucket& bucket) {
    json = bucket.to_json();
}
