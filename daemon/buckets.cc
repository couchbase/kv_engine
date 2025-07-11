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
#include "bucket_destroyer.h"
#include "connection.h"
#include "cookie.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "memcached.h"
#include "resource_allocation_domain.h"
#include "stats.h"
#include <daemon/settings.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/config_parser.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <platform/base64.h>
#include <platform/json_log_conversions.h>
#include <platform/scope_timer.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <statistics/labelled_collector.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/throttle_utilities.h>

Bucket::Bucket() = default;

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

    if (state != State::None) {
        try {
            nlohmann::json json;
            json["state"] = state.load();
            json["clients"] = clients.load();
            json["name"] = name;
            json["type"] = type;
            json["data_ingress_status"] = data_ingress_status.load();
            json["num_rejected"] = num_rejected.load();
            if (serverless) {
                json["ru"] = read_units_used.load();
                json["wu"] = write_units_used.load();
                json["num_throttled"] = num_throttled.load();
                json["throttle_reserved"] =
                        cb::throttle::limit_to_json(throttle_reserved.load());
                json["throttle_hard_limit"] =
                        cb::throttle::limit_to_json(throttle_hard_limit.load());
                json["throttle_wait_time"] = throttle_wait_time.load();
                json["num_commands_with_metered_units"] =
                        num_commands_with_metered_units.load();
                json["num_metered_dcp_messages"] =
                        num_metered_dcp_messages.load();
                json["num_commands"] = num_commands.load();
            }
            return json;
        } catch (const std::exception& e) {
            LOG_ERROR_CTX("Failed to generate bucket details",
                          {"error", e.what()});
        }
    }
    return {};
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

void Bucket::setThrottleLimits(std::size_t reserved, std::size_t hard) {
    throttle_reserved.store(reserved);
    throttle_hard_limit.store(hard);
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
    // Metering stats are only applicable for serverless - avoid the cost
    // of tracking them if not.
    if (!cb::serverless::isEnabled()) {
        return;
    }

    // The node supervisor runs for free
    if (conn.isNodeSupervisor()) {
        return;
    }

    auto& inst = cb::serverless::Config::instance();
    const auto ru = inst.to_ru(nread);
    consumedUnits(ru, domain);
    if (conn.isSubjectToMetering()) {
        read_units_used += ru;
        ++num_metered_dcp_messages;
    }
}

void Bucket::documentExpired(size_t nbytes) {
    /// Metrics related to metering only applicable to serverless - avoid
    /// the cost of maintaining them if not serverless.
    if (!cb::serverless::isEnabled()) {
        return;
    }
    if (nbytes) {
        auto& inst = cb::serverless::Config::instance();
        const auto wu = inst.to_wu(nbytes);
        throttle_gauge.increment(wu);
        write_units_used += wu;
    } else {
        throttle_gauge.increment(1);
        ++write_units_used;
    }
}

void Bucket::commandExecuted(const Cookie& cookie) {
    // These statistics have a non-negligable cost to maintain - only do
    // so if needed for serverless profile.
    if (!cb::serverless::isEnabled()) {
        return;
    }
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

    auto& inst = cb::serverless::Config::instance();
    const auto [read, write] = cookie.getDocumentRWBytes();
    if (read || write) {
        auto ru = inst.to_ru(read);
        auto wu = inst.to_wu(write);
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

BucketManager& BucketManager::instance() {
    static BucketManager instance;
    return instance;
}

/**
 * All the buckets in couchbase is stored in this array.
 */
std::mutex buckets_lock;
std::array<Bucket, cb::limits::TotalBuckets + 1> all_buckets;

std::pair<cb::engine_errc, Bucket::State> BucketManager::setClusterConfig(
        std::string_view name,
        std::shared_ptr<ClusterConfiguration::Configuration> configuration) {
    // Make sure we don't race with anyone else touching the bucket array
    // (create/delete bucket or set cluster config).
    std::lock_guard<std::mutex> guard(buckets_lock);

    auto first_free = all_buckets.size();

    std::size_t ii = 0;
    for (auto& bucket : all_buckets) {
        std::lock_guard<std::mutex> bucketguard(bucket.mutex);
        if (bucket.name == name) {
            if (bucket.state == Bucket::State::Ready) {
                bucket.clusterConfiguration.setConfiguration(
                        std::move(configuration));
                return {cb::engine_errc::success, Bucket::State::Ready};
            }
            // We can't set the cluster configuration at this time as
            // the bucket is currently being initialized/paused/deleted,
            // but tell the client to try again :)
            return {cb::engine_errc::temporary_failure, bucket.state};
        }

        if (bucket.state == Bucket::State::None &&
            first_free == all_buckets.size()) {
            first_free = ii;
        }
        ++ii;
    }

    if (first_free == all_buckets.size()) {
        return {cb::engine_errc::too_big, Bucket::State::None};
    }

    std::lock_guard<std::mutex> bucketguard(all_buckets[first_free].mutex);
    auto& bucket = all_buckets[first_free];
    bucket.type = BucketType::ClusterConfigOnly;
    bucket.clusterConfiguration.setConfiguration(std::move(configuration));
    bucket.name = name;
    bucket.supportedFeatures.emplace(cb::engine::Feature::Collections);
    bucketStateChangeListener(bucket, Bucket::State::Ready);
    bucket.state = Bucket::State::Ready;
    LOG_INFO_CTX("Created bucket",
                 {"bucket", name},
                 {"type", bucket.type});
    return {cb::engine_errc::success, Bucket::State::Ready};
}

void BucketManager::iterateBuckets(const std::function<bool(Bucket&)>& fn) {
    for (auto& b : all_buckets) {
        std::lock_guard<std::mutex> guard(b.mutex);
        if (!fn(b)) {
            return;
        }
    }
}

std::pair<cb::engine_errc, Bucket*> BucketManager::allocateBucket(
        std::string_view name) {
    // Acquire the global mutex to lock out anyone else from adding/removing
    // buckets in the bucket array
    std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);
    // We need to find a new slot for the bucket, and verify that
    // the bucket don't exist.
    Bucket* free_bucket = nullptr;
    Bucket* existing_bucket = nullptr;
    bool einprogress = false;
    iterateBuckets(
            [&free_bucket, &existing_bucket, name, &einprogress](auto& bucket) {
                if (!free_bucket && bucket.state == Bucket::State::None) {
                    free_bucket = &bucket;
                } else if (name == bucket.name) {
                    if (bucket.management_operation_in_progress) {
                        einprogress = true;
                    } else if (bucket.type == BucketType::ClusterConfigOnly) {
                        free_bucket = &bucket;
                        bucket.management_operation_in_progress = true;
                    } else {
                        existing_bucket = &bucket;
                    }

                    return false;
                }
                return true;
            });

    if (einprogress) {
        return {cb::engine_errc::temporary_failure, nullptr};
    }

    if (existing_bucket) {
        return {cb::engine_errc::key_already_exists, nullptr};
    }

    if (!free_bucket) {
        return {cb::engine_errc::too_big, nullptr};
    }

    std::lock_guard<std::mutex> guard(free_bucket->mutex);
    // Set the bucket state to creating and copy the name over.
    // We can't set the state to Creating for a ClusterConfigOnly bucket as
    // that would cause all clients to disconnect.
    free_bucket->management_operation_in_progress = true;
    if (free_bucket->type == BucketType::Unknown) {
        free_bucket->name = name;
        bucketStateChangeListener(*free_bucket, Bucket::State::Creating);
        free_bucket->state = Bucket::State::Creating;
    }
    return {cb::engine_errc::success, free_bucket};
}

void BucketManager::createEngineInstance(Bucket& bucket,
                                         BucketType type,
                                         std::string_view name,
                                         std::string_view config,
                                         uint32_t cid) {
    auto start = std::chrono::steady_clock::now();
    bucket.setEngine(new_engine_instance(type, get_server_api));
    auto stop = std::chrono::steady_clock::now();
    if ((stop - start) > std::chrono::seconds{1}) {
        LOG_WARNING_CTX("Creation of bucket instance",
                        {"conn_id", cid},
                        {"bucket", name},
                        {"duration", stop - start});
    }

    // Set the state initializing so that people monitoring the
    // bucket states can pick it up. We can't change the state
    // for cluster-config-only buckets as that would cause clients
    // to disconnect
    if (bucket.type == BucketType::Unknown) {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucketStateChangeListener(bucket, Bucket::State::Initializing);
        bucket.state = Bucket::State::Initializing;
    }

    cb::logger::Json details = {
            {"conn_id", cid}, {"bucket", name}, {"type", type}};

    // Parse the configuration string and strip out various sensitive data to
    // avoid that being logged
    nlohmann::json encryption;
    std::string chronicleAuthToken;
    nlohmann::json collectionManifest;
    const auto stripped = cb::config::filter(
            config,
            [&encryption, &chronicleAuthToken, &collectionManifest](
                    auto k, auto v) -> bool {
                if (k == "encryption") {
                    encryption = nlohmann::json::parse(v);
                    return false;
                }
                if (k == "chronicle_auth_token") {
                    chronicleAuthToken = v;
                    return false;
                }
                if (k == "collection_manifest") {
                    if (v.empty()) {
                        return false;
                    }
                    auto decoded = cb::base64url::decode(v);
                    collectionManifest = nlohmann::json::parse(decoded);
                    return false;
                }
                return true;
            });

    if (!encryption.empty()) {
        auto no_keys = encryption;
        if (no_keys.contains("keys")) {
            for (auto& elem : no_keys["keys"]) {
                elem.erase("key");
            }
        }
        details["encryption"] = std::move(no_keys);
    }
    details["chronicle_auth_token"] =
            chronicleAuthToken.empty() ? "not-set" : "set";
    details["configuration"] = stripped;
    details["collection_manifest"] =
            collectionManifest.empty() ? "not-set" : "present";
    LOG_INFO_CTX("Initialize bucket", std::move(details));

    start = std::chrono::steady_clock::now();
    auto result = bucket.getEngine().initialize(
            stripped, encryption, chronicleAuthToken, collectionManifest);
    if (result != cb::engine_errc::success) {
        throw cb::engine_error(result, "initializeEngineInstance failed");
    }
    stop = std::chrono::steady_clock::now();
    if ((stop - start) > std::chrono::seconds{1}) {
        LOG_WARNING_CTX("Initialization of bucket completed",
                        {"conn_id", cid},
                        {"bucket", name},
                        {"duration", stop - start});
    }

    // We don't pass the storage threads down in the config like we do for
    // readers and writers because that evolved over time to be duplicated
    // in both configs. Instead, we just inform the engine of the number
    // of threads.
    auto& engine = bucket.getEngine();
    engine.set_num_storage_threads(ThreadPoolConfig::StorageThreadCount(
            Settings::instance().getNumStorageThreads()));

    bucket.max_document_size = engine.getMaxItemSize();
    bucket.supportedFeatures = engine.getFeatures();

    // MB-53498: Set the bucket type to the correct type
    bucketTypeChangeListener(bucket, type);
    bucket.type.store(type, std::memory_order_seq_cst);
}

cb::engine_errc BucketManager::create(CookieIface& cookie,
                                      std::string_view name,
                                      std::string_view config,
                                      BucketType type) {
    return create(cookie.getConnectionId(), name, config, type);
}

cb::engine_errc BucketManager::create(uint32_t cid,
                                      std::string_view name,
                                      std::string_view config,
                                      BucketType type) {
    LOG_INFO_CTX("Create bucket",
                 {"conn_id", cid},
                 {"bucket", name},
                 {"type", type});
    auto [err, free_bucket] = allocateBucket(name);
    if (err != cb::engine_errc::success) {
        LOG_ERROR_CTX("Create bucket failed",
                      {"conn_id", cid},
                      {"bucket", name},
                      {"error", err});
        return err;
    }

    if (!free_bucket) {
        throw std::logic_error(
                "BucketManager::create: allocateBucket returned success but no "
                "bucket");
    }
    auto& bucket = *free_bucket;

    if (cb::serverless::isEnabled()) {
        auto& instance = cb::serverless::Config::instance();
        bucket.setThrottleLimits(instance.defaultThrottleReservedUnits,
                                 instance.defaultThrottleHardLimit);
    }

    cb::engine_errc result = cb::engine_errc::success;
    try {
        createEngineInstance(bucket, type, name, config, cid);

        // The bucket is fully initialized and ready to use!
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.management_operation_in_progress = false;
            bucketStateChangeListener(bucket, Bucket::State::Ready);
            bucket.state = Bucket::State::Ready;
        }
        LOG_INFO_CTX("Bucket created successfully",
                     {"conn_id", cid},
                     {"bucket", name});
    } catch (const cb::engine_error& exception) {
        result = cb::engine_errc(exception.code().value());
        LOG_ERROR_CTX("Failed to create bucket",
                      {"conn_id", cid},
                      {"bucket", name},
                      {"error", exception.what()});
    } catch (const std::bad_alloc&) {
        LOG_ERROR_CTX("Failed to create bucket",
                      {"conn_id", cid},
                      {"bucket", name},
                      {"error", "No memory"});
        result = cb::engine_errc::no_memory;
    } catch (const std::exception& e) {
        LOG_ERROR_CTX("Failed to create bucket",
                      {"conn_id", cid},
                      {"bucket", name},
                      {"error", e.what()});
        result = cb::engine_errc::failed;
    }

    if (result != cb::engine_errc::success) {
        // An error occurred, we need to roll back
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucketStateChangeListener(bucket, Bucket::State::Destroying);
            bucket.state = Bucket::State::Destroying;
        }
        waitForEveryoneToDisconnect(
                bucket, "bucket creation rollback", std::to_string(cid));

        bucket.reset();
        if (result != cb::engine_errc::encryption_key_not_available) {
            result = cb::engine_errc::not_stored;
        }
    }
    return result;
}

cb::engine_errc BucketManager::doBlockingDestroy(
        CookieIface& cookie,
        std::string_view name,
        bool force,
        std::optional<BucketType> type) {
    return destroy(std::to_string(cookie.getConnectionId()), name, force, type);
}

cb::engine_errc BucketManager::destroy(std::string_view cid,
                                       std::string_view name,
                                       bool force,
                                       std::optional<BucketType> type) {
    auto [res, destroyer] = startDestroy(cid, name, force, type);
    Expects(destroyer || res != cb::engine_errc::would_block);
    while (res == cb::engine_errc::would_block) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10ms);
        res = destroyer->drive();
    }

    return res;
}

std::pair<cb::engine_errc, std::optional<BucketDestroyer>>
BucketManager::startDestroy(std::string_view cid,
                            std::string_view name,
                            bool force,
                            std::optional<BucketType> type) {
    cb::engine_errc ret = cb::engine_errc::no_such_key;
    Bucket* bucket_ptr = nullptr;

    {
        std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);
        iterateBuckets([&ret, &bucket_ptr, &name, this, &type](Bucket& b) {
            if (name != b.name) {
                return true;
            }

            if (b.management_operation_in_progress) {
                // Someone else is currently operating on the bucket
                ret = cb::engine_errc::temporary_failure;
                return false;
            }

            if (b.state == Bucket::State::Ready ||
                b.state == Bucket::State::Paused) {
                if (type && b.type != type.value()) {
                    ret = cb::engine_errc::key_already_exists;
                } else {
                    ret = cb::engine_errc::success;
                    bucket_ptr = &b;
                    b.management_operation_in_progress = true;
                    bucketStateChangeListener(b, Bucket::State::Destroying);
                    b.state = Bucket::State::Destroying;
                }
            } else {
                ret = cb::engine_errc::key_already_exists;
            }

            return true;
        });
    }

    if (ret != cb::engine_errc::success) {
        LOG_WARNING_CTX("Delete bucket",
                        {"conn_id", cid},
                        {"bucket", name},
                        {"status", ret});
        return {ret, {}};
    }
    // The destroyer _could_ be stepped here until it first returns
    // would_block, but startDestroy is called on a frontend thread and the
    // destroyer may use e.g., iterate_all_connections which should not be
    // called from a frontend thread context
    return {cb::engine_errc::would_block,
            BucketDestroyer(*bucket_ptr, std::string(cid), force)};
}

void BucketManager::waitForEveryoneToDisconnect(
        Bucket& bucket,
        std::string_view operation,
        std::string_view id,
        folly::CancellationToken cancellationToken) {
    // Wait until all users disconnected...
    {
        std::unique_lock<std::mutex> guard(bucket.mutex);

        if (bucket.clients > 0) {
            LOG_INFO_CTX("Operation waiting for clients to disconnect",
                         {"conn_id", id},
                         {"operation", operation},
                         {"bucket", bucket.name},
                         {"clients", bucket.clients});

            // Signal clients bound to the bucket before waiting
            guard.unlock();
            bucket.deleteThrottledCommands();
            iterate_all_connections([&bucket](Connection& connection) {
                if (&connection.getBucket() == &bucket) {
                    connection.signalIfIdle();
                }
            });
            guard.lock();
        }

        using std::chrono::minutes;
        using std::chrono::seconds;
        using std::chrono::steady_clock;

        // We need to disconnect all the clients.
        // Log pending connections that are connected every 2 minutes.
        auto nextLog = steady_clock::now() + minutes(2);
        while (bucket.clients > 0) {

            if (cancellationToken.isCancellationRequested()) {
                // Give up on disconnecting connections.
                return;
            }

            bucket.cond.wait_for(guard, seconds(1), [&bucket] {
                return bucket.clients == 0;
            });

            if (bucket.clients == 0) {
                break;
            }

            if (steady_clock::now() < nextLog) {
                guard.unlock();
                bucket.deleteThrottledCommands();
                iterate_all_connections([&bucket](Connection& connection) {
                    if (&connection.getBucket() == &bucket) {
                        connection.signalIfIdle();
                    }
                });
                if (bucket.type != BucketType::ClusterConfigOnly) {
                    bucket.getEngine().cancel_all_operations_in_ewb_state();
                }
                guard.lock();
                continue;
            }

            nextLog = steady_clock::now() + minutes(1);

            // drop the lock and notify the worker threads
            guard.unlock();

            nlohmann::json currConns;
            bucket.deleteThrottledCommands();
            iterate_all_connections([&bucket, &currConns](Connection& conn) {
                if (&conn.getBucket() == &bucket) {
                    conn.signalIfIdle();
                    currConns[std::to_string(conn.getId())] = conn.to_json();
                }
            });

            LOG_INFO_CTX("Operation still waiting for clients to disconnect",
                         {"conn_id", id},
                         {"operation", operation},
                         {"bucket", bucket.name},
                         {"clients", bucket.clients},
                         {"connections", currConns.dump()});

            guard.lock();
        }
    }

    auto num = bucket.items_in_transit.load();
    int counter = 0;
    while (num != 0) {
        if (++counter % 100 == 0) {
            LOG_INFO_CTX("Operation waiting for items stuck in transfer",
                         {"conn_id", id},
                         {"operation", operation},
                         {"bucket", bucket.name},
                         {"items", num});
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        num = bucket.items_in_transit.load();
    }
}

void BucketManager::tick() {
    auto limit = cb::serverless::Config::instance().nodeCapacity.load();

    forEach([&limit](auto& b) {
        if (b.type != BucketType::NoBucket) {
            auto [reserved, hard] = b.getThrottleLimits();
            if (reserved != std::numeric_limits<std::size_t>::max()) {
                if (reserved < limit) {
                    limit -= reserved;
                } else {
                    limit = 0;
                }
            }
            b.tick();
        }
        return true;
    });

    unassigned_resources_limit = limit;
    // Reset the unassigned gauge instead of tick as we don't want
    // to continue to pay on the overuse for the next sec ;)
    unassigned_resources_gauge.reset();

    FrontEndThread::forEach([](auto& thr) {
        // Iterate over all of the DCP connections bound to this thread
        // which is subject for throttling so that they may send more
        // data (in the case they stopped producing data due to throttling
        thr.iterateThrottleableDcpConnections([](Connection& c) {
            c.resumeThrottledDcpStream();
            c.triggerCallback();
        });
    });
}

void BucketManager::forEach(const std::function<bool(Bucket&)>& fn) {
    for (Bucket& bucket : all_buckets) {
        bool do_break = false;
        if (bucket.state == Bucket::State::Ready) {
            // MB-44827: We don't want to hold the bucket mutex for the
            //           entire time of the callback as it would block
            //           other threads to associate / leave the bucket,
            //           and we don't know how slow the callback is going
            //           to be. To make sure that the bucket won't get
            //           killed while we run the callback we'll bump
            //           the client reference and release it once
            //           we're done with the callback
            bool ready = true;
            {
                std::lock_guard<std::mutex> guard(bucket.mutex);
                if (bucket.state == Bucket::State::Ready) {
                    bucket.clients++;
                } else {
                    ready = false;
                }
            }
            if (ready) {
                if (!fn(bucket)) {
                    do_break = true;
                }
                // disconnect from the bucket (remove the client reference
                // we added earlier
                disconnect_bucket(bucket, nullptr);
                if (do_break) {
                    break;
                }
            }
        }
    }
}

Bucket* BucketManager::tryAssociateBucket(EngineIface* engine) {
    Bucket* associated = nullptr;
    forEach([&associated, engine](auto& bucket) {
        // Find the correct bucket
        if (engine != &bucket.getEngine()) {
            return true;
        }
        std::lock_guard<std::mutex> guard(bucket.mutex);
        if (bucket.state != Bucket::State::Ready) {
            // If it is not in the correct state, we cannot associate it
            return false;
        }

        // We "associate with a bucket" by incrementing the num of
        // clients to the bucket.
        bucket.clients++;
        associated = &bucket;
        return false;
    });

    return associated;
}

void BucketManager::disassociateBucket(Bucket* bucket) {
    // Remove the client reference we added earlier
    disconnect_bucket(*bucket, nullptr);
}

Bucket& BucketManager::at(size_t idx) {
    return all_buckets[idx];
}

void BucketManager::destroyBucketsInParallel() {
    // Iterate over all "ready" buckets and initiate their shutdown
    std::vector<std::pair<cb::engine_errc, BucketDestroyer>> destroyers;
    BucketManager::forEach([this, &destroyers](auto& bucket) -> bool {
        if (bucket.type == BucketType::NoBucket) {
            return true;
        }
        auto [res, instance] = startDestroy("<none>", bucket.name, false, {});
        if (res == cb::engine_errc::would_block && instance.has_value()) {
            LOG_INFO_CTX("Destroying bucket", {"name", bucket.name});
            destroyers.emplace_back(cb::engine_errc::would_block,
                                    std::move(instance.value()));
        }

        return true;
    });

    bool done = destroyers.empty();
    while (!done) {
        done = true;
        for (auto& [res, instance] : destroyers) {
            if (res == cb::engine_errc::would_block) {
                res = instance.drive();
                if (res == cb::engine_errc::would_block) {
                    done = false;
                }
            }
        }
        if (!done) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    }
}

void BucketManager::destroyAll() {
    LOG_INFO_RAW("Stop all buckets");

    bool active;
    do {
        destroyBucketsInParallel();
        active = false;
        // Start at one (not zero) because zero is reserved for "no bucket".
        // The "no bucket" has a state of Bucket::State::Ready but no name.
        for (size_t ii = 1; ii < all_buckets.size(); ++ii) {
            if (all_buckets[ii].state != Bucket::State::None) {
                LOG_INFO_CTX("Found bucket in unexpected state",
                             {"name", all_buckets[ii].name},
                             {"state", all_buckets[ii].state.load()});
                active = true;
            }
        }
        if (active) {
            LOG_INFO_RAW(
                    "Sleep 1 seconds to allow buckets time to enter a state "
                    "where they may be shut down");
            std::this_thread::sleep_for(std::chrono::seconds{1});
        }
    } while (active);
}

cb::engine_errc BucketManager::pause(CookieIface& cookie,
                                     std::string_view name) {
    return pause(std::to_string(cookie.getConnectionId()), name);
}

cb::engine_errc BucketManager::pause(std::string_view cid, std::string_view name) {
    // Find the specified bucket and check it can be paused.
    Bucket* bucket{nullptr};
    folly::CancellationToken cancellationToken;
    {
        // Make sure we don't race with anyone else touching the bucket array
        // (create/delete/pause/resume bucket or set cluster config).
        std::lock_guard all_bucket_lock(buckets_lock);

        // locate the bucket
        size_t idx = 0;
        for (auto& b : all_buckets) {
            std::lock_guard<std::mutex> bucketguard(b.mutex);
            if (b.name == name) {
                break;
            }
            ++idx;
        }

        if (idx == all_buckets.size()) {
            return cb::engine_errc::no_such_key;
        }

        bucket = &all_buckets.at(idx);
        // Perform final checks on bucket, and if all good modify the bucket
        // state to Pausing. After this we can release the buckets_lock.
        {
            std::lock_guard bucketguard(bucket->mutex);
            if (bucket->state == Bucket::State::Pausing ||
                bucket->state == Bucket::State::Paused) {
                // Cannot pause bucket if already pausing / paused.
                return cb::engine_errc::bucket_paused;
            }
            if (bucket->state != Bucket::State::Ready) {
                // perhaps we want a new error code for this?
                return cb::engine_errc::key_already_exists;
            }

            if (bucket->type == BucketType::ClusterConfigOnly) {
                return cb::engine_errc::not_supported;
            }

            // Change to 'Pausing' State to block any more requests, and
            // so observers can tell pausing has started.
            bucket->management_operation_in_progress = true;
            bucketStateChangeListener(*bucket, Bucket::State::Pausing);
            bucket->state = Bucket::State::Pausing;
            Expects(!bucket->pause_cancellation_source.canBeCancelled());
            bucket->pause_cancellation_source = folly::CancellationSource{};
            cancellationToken = bucket->pause_cancellation_source.getToken();
        }
    }

    LOG_INFO_CTX("Pausing bucket",
                 {"conn_id", cid},
                 {"bucket", name},
                 {"state", "notifying engine to quiesce state"});

    bucketPausingListener(name, "before_cancellation_callback");

    // Setup a cancellationCallback which, if pause is cancelled will restore
    // bucket to the state before pause() was started.
    //
    // Note: This can either be executed on the cancelling thread (typical
    // execution path), or it can be executed inline here if the
    // cancellationToken is cancelled before the cancellationCallback is
    // constructed, so we need to handle both cases:
    // - For inline execution: we must acquire Bucket::mutex around
    //   construction.
    // - For execution via cancelling thread: we must acquire Bucket::mutex
    //   before requesting cancellation - see BucketManager::resume().
    folly::CancellationCallback cancellationCallback = [&] {
        std::lock_guard guard(bucket->mutex);
        return folly::CancellationCallback{
                cancellationToken, [this, cid, bucket, name] {
                    LOG_INFO_CTX("Cancelling pause of bucket",
                                 {"conn_id", cid},
                                 {"bucket", name});
                    Expects(bucket->state == Bucket::State::Pausing ||
                            bucket->state == Bucket::State::Paused);
                    bucket->management_operation_in_progress = false;
                    bucketStateChangeListener(*bucket, bucket->state);
                    bucket->pause_cancellation_source =
                            folly::CancellationSource::invalid();
                    bucket->state = Bucket::State::Ready;
                }};
    }();

    bucketPausingListener(name, "before_disconnect");

    waitForEveryoneToDisconnect(*bucket, "Pause", cid, cancellationToken);

    {
        // Check if cancellation was requested since waiting for clients to
        // disconnect - if so cancel.
        // Note we don't /need/ to take the Bucket::mutex here - given we could
        // get cancellation occurring just after we unlock the mutex - but
        // we do acquire Bucket::mutex before checking later on (just before
        // we return success below) and hence for locking consistency we
        // acquire Bucket::mutex here also.
        std::lock_guard guard(bucket->mutex);
        if (cancellationToken.isCancellationRequested()) {
            // Cancel (fail) the pause() request. Registered callback(s) above
            // (and potentially others registered at lower levels) will "undo"
            // any necessary partial pause.
            return cb::engine_errc::cancelled;
        }
    }

    bucketPausingListener(name, "before_engine_pause");

    auto status = bucket->getEngine().pause(cancellationToken);

    if (status == cb::engine_errc::success) {
        std::lock_guard bucketguard(bucket->mutex);
        // Check if cancellation was requested (under the Bucket::mutex to
        // avoid racing with another thread attempting to cancel). If so
        // then return a failure status - note cleanup back to state ready etc
        // is done by cancellationCallback above.
        if (cancellationToken.isCancellationRequested()) {
            return cb::engine_errc::cancelled;
        }
        LOG_INFO_CTX("Paused bucket",
                     {"conn_id", cid},
                     {"bucket", name},
                     {"status", status});
        bucket->management_operation_in_progress = false;
        bucketStateChangeListener(*bucket, Bucket::State::Paused);
        bucket->pause_cancellation_source =
                folly::CancellationSource::invalid();
        bucket->state = Bucket::State::Paused;
    } else {
        std::lock_guard bucketguard(bucket->mutex);
        if (cancellationToken.isCancellationRequested()) {
            return cb::engine_errc::cancelled;
        }
        LOG_WARNING_CTX("Pausing bucket failed",
                        {"conn_id", cid},
                        {"bucket", name},
                        {"status", status});
        bucketStateChangeListener(*bucket, Bucket::State::Ready);
        bucket->pause_cancellation_source =
                folly::CancellationSource::invalid();
        bucket->state = Bucket::State::Ready;
    }

    return status;
}

cb::engine_errc BucketManager::resume(CookieIface& cookie,
                                      std::string_view name) {
    return resume(std::to_string(cookie.getConnectionId()), name);
}

cb::engine_errc BucketManager::resume(std::string_view cid,
                                      std::string_view name) {
    // Make sure we don't race with anyone else touching the bucket array
    // (create/delete/pause/resume bucket or set cluster config)
    std::lock_guard all_bucket_lock(buckets_lock);

    // locate the bucket
    size_t idx = 0;
    for (auto& bucket : all_buckets) {
        std::lock_guard<std::mutex> bucketguard(bucket.mutex);
        if (bucket.name == name) {
            // Can only resume Pausing buckets (in which case we cancel the
            // in-progress pause) or Paused buckets.
            if (bucket.state != Bucket::State::Pausing &&
                bucket.state != Bucket::State::Paused) {
                // @todo: Use a different error code?
                return cb::engine_errc::key_already_exists;
            }
            break;
        }
        ++idx;
    }

    if (idx == all_buckets.size()) {
        return cb::engine_errc::no_such_key;
    }

    auto& bucket = all_buckets.at(idx);

    // Check if a pause() operation is still in-flight. If so then no need to
    // perform a full resume operation - just cancel the in-flight pause.
    // This must be done under Bucket::mutex to avoid a race between checking
    // for in-flight pause and that pause completing - setting / clearing
    // Bucket::pause_cancellation_source is done under Bucket::mutex.
    {
        std::lock_guard bucketguard(bucket.mutex);
        if (bucket.pause_cancellation_source.canBeCancelled()) {
            bool alreadyRequested =
                    bucket.pause_cancellation_source.requestCancellation();
            LOG_INFO_CTX(
                    "Requesting cancellation of in-progress pause() request",
                    {"conn_id", cid},
                    {"bucket", name},
                    {"previously_requested", alreadyRequested});
            return cb::engine_errc::success;
        }
    }

    LOG_INFO_CTX("Resuming bucket", {"conn_id", cid}, {"bucket", name});
    auto status = bucket.getEngine().resume();
    if (status == cb::engine_errc::success) {
        LOG_INFO_CTX(
                "Bucket is back online", {"conn_id", cid}, {"bucket", name});
        std::lock_guard bucketguard(bucket.mutex);
        bucketStateChangeListener(bucket, Bucket::State::Ready);
        bucket.state = Bucket::State::Ready;
    } else {
        LOG_WARNING_CTX("Failed to resume bucket",
                        {"conn_id", cid},
                        {"bucket", name},
                        {"status", status});
    }

    return status;
}

BucketManager::BucketManager() {
    auto& settings = Settings::instance();

    size_t numthread = settings.getNumWorkerThreads() + 1;
    for (auto& b : all_buckets) {
        b.reset();
        b.stats.resize(numthread);
    }

    // To make the life easier for us in the code, index 0 in the array is
    // "no bucket"
    auto& nobucket = all_buckets.at(0);
    try {
        nobucket.setEngine(
                new_engine_instance(BucketType::NoBucket, get_server_api));
    } catch (const std::exception& exception) {
        FATAL_ERROR_CTX(
                EXIT_FAILURE,
                "Failed to create the internal bucket \"No bucket\": {}",
                {"error", exception.what()});
    }
    nobucket.max_document_size = nobucket.getEngine().getMaxItemSize();
    nobucket.supportedFeatures = nobucket.getEngine().getFeatures();
    nobucket.type = BucketType::NoBucket;
    bucketStateChangeListener(nobucket, Bucket::State::Ready);
    nobucket.state = Bucket::State::Ready;
}
