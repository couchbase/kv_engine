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
#include "connection.h"
#include "cookie.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "memcached.h"
#include "stats.h"
#include <daemon/server_core_api.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <platform/scope_timer.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <statistics/labelled_collector.h>
#include <utilities/engine_errc_2_mcbp.h>

Bucket::Bucket() = default;

void Bucket::reset() {
    std::lock_guard<std::mutex> guard(mutex);
    engine.reset();
    state = Bucket::State::None;
    memset(name, 0, sizeof(name));
    setEngine(nullptr);
    clusterConfiguration.reset();
    max_document_size = default_max_item_size;
    supportedFeatures = {};
    read_units_used = 0;
    write_units_used = 0;
    throttle_gauge.reset();
    if (isServerlessDeployment()) {
        throttle_limit.store(
                cb::serverless::Config::instance().defaultThrottleLimit.load(
                        std::memory_order_acquire),
                std::memory_order_release);
    } else {
        throttle_limit = std::numeric_limits<std::size_t>::max();
    }
    num_throttled = 0;
    throttle_wait_time = 0;
    num_commands = 0;
    num_commands_with_metered_units = 0;
    num_metered_dcp_messages = 0;
    num_rejected = 0;
    bucket_quota_exceeded = false;

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
}

bool Bucket::supports(cb::engine::Feature feature) {
    return supportedFeatures.find(feature) != supportedFeatures.end();
}

nlohmann::json Bucket::to_json() const {
    const bool serverless = isServerlessDeployment();
    std::lock_guard<std::mutex> guard(mutex);

    if (state != State::None) {
        try {
            nlohmann::json json;
            json["state"] = to_string(state.load());
            json["clients"] = clients.load();
            json["name"] = name;
            json["type"] = to_string(type);
            json["num_commands"] = num_commands.load();
            if (serverless) {
                json["ru"] = read_units_used.load();
                json["wu"] = write_units_used.load();
                json["num_throttled"] = num_throttled.load();
                json["throttle_limit"] = throttle_limit.load();
                json["throttle_wait_time"] = throttle_wait_time.load();
                json["num_commands_with_metered_units"] =
                        num_commands_with_metered_units.load();
                json["num_metered_dcp_messages"] =
                        num_metered_dcp_messages.load();
                json["num_rejected"] = num_rejected.load();
            }
            return json;
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to generate bucket details: {}", e.what());
        }
    }
    return {};
}

void Bucket::addMeteringMetrics(const BucketStatCollector& collector) const {
    using namespace cb::stats;
    // engine-related metering (e.g., disk usage)
    getEngine().get_prometheus_stats(
            collector, cb::prometheus::MetricGroup::Metering);

    // metering
    collector.addStat(Key::meter_ru_total, read_units_used);
    collector.addStat(Key::meter_wu_total, write_units_used);
    // kv does not currently account for actual compute (i.e., CPU) units
    // but other components do. Expose it for consistency and ease of use
    collector.addStat(Key::meter_cu_total, 0);

    collector.addStat(Key::op_count_total, num_commands);

    // the spec declares that some components may need to perform some
    // actions "on behalf of" other components - e.g., credit back
    // CUs, or throttle.
    // For now, simply report such metrics as "for kv", until we have
    // more information recorded internally to support this.
    auto forKV = collector.withLabel("for", "kv");

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
            duration_cast<seconds>(milliseconds(throttle_wait_time)).count());
}

void Bucket::setThrottleLimit(std::size_t limit) {
    if (limit == throttle_limit) {
        return;
    }
    throttle_limit.store(limit);
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

void Bucket::recordMeteringReadBytes(const Connection& conn,
                                     std::size_t nread) {
    // The node supervisor runs for free
    if (conn.isNodeSupervisor()) {
        return;
    }

    auto& inst = cb::serverless::Config::instance();
    const auto ru = inst.to_ru(nread);
    throttle_gauge.increment(ru);

    if (conn.isSubjectToMetering()) {
        read_units_used += ru;
        ++num_metered_dcp_messages;
    }
}

void Bucket::documentExpired(size_t nbytes) {
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
    ++num_commands;
    if (cookie.getConnection().isNodeSupervisor()) {
        return;
    }

    auto& inst = cb::serverless::Config::instance();
    const auto [read, write] = cookie.getDocumentRWBytes();
    if (read || write) {
        auto ru = inst.to_ru(read);
        auto wu = inst.to_wu(write);
        if (cookie.isDurable()) {
            wu *= 2;
        }
        throttle_gauge.increment(ru + wu);
        throttle_wait_time += cookie.getTotalThrottleTime();
        if (cookie.getConnection().isSubjectToMetering()) {
            read_units_used += ru;
            write_units_used += wu;
            ++num_commands_with_metered_units;
        }
    }
}

void Bucket::rejectCommand(const Cookie&) {
    ++num_rejected;
}

void Bucket::tick() {
    throttle_gauge.tick(throttle_limit);
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

bool Bucket::shouldThrottleDcp(const Connection& connection) {
    if (throttle_limit == std::numeric_limits<std::size_t>::max() ||
        connection.isUnthrottled()) {
        return false;
    }

    if (!throttle_gauge.isBelow(throttle_limit)) {
        num_throttled++;
        return true;
    }
    return false;
}

bool Bucket::shouldThrottle(const Cookie& cookie,
                            bool addConnectionToThrottleList) {
    if (throttle_limit == std::numeric_limits<std::size_t>::max() ||
        cookie.getConnection().isUnthrottled()) {
        // No limit specified, so we don't need to do any further inspection
        return false;
    }

    const auto& header = cookie.getHeader();
    if (header.isResponse() ||
        cb::mcbp::is_server_magic(cb::mcbp::Magic(header.getMagic()))) {
        // Never throttle response messages or server ops
        return false;
    }

    if (is_subject_for_throttling(header.getRequest().getClientOpcode()) &&
        !throttle_gauge.isBelow(throttle_limit)) {
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

    return false;
}

namespace BucketValidator {
std::string validateBucketName(std::string_view name) {
    if (name.empty()) {
        return "Name can't be empty";
    }

    if (name.length() > MAX_BUCKET_NAME_LENGTH) {
        return "Name too long (exceeds " +
               std::to_string(MAX_BUCKET_NAME_LENGTH) + ")";
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

std::string to_string(Bucket::State state) {
    switch (state) {
    case Bucket::State::None:
        return "none";
    case Bucket::State::Creating:
        return "creating";
    case Bucket::State::Initializing:
        return "initializing";
    case Bucket::State::Ready:
        return "ready";
    case Bucket::State::Stopping:
        return "stopping";
    case Bucket::State::Destroying:
        return "destroying";
    }
    throw std::invalid_argument("Invalid bucket state: " +
                                std::to_string(int(state)));
}

bool mayAccessBucket(Cookie& cookie, const std::string& bucket) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer(cookie, Code::CreateRbacContext);
    return cb::rbac::mayAccessBucket(cookie.getConnection().getUser(), bucket);
}

BucketManager& BucketManager::instance() {
    static BucketManager instance;
    return instance;
}

/**
 * All of the buckets in couchbase is stored in this array.
 */
std::mutex buckets_lock;
std::array<Bucket, cb::limits::TotalBuckets + 1> all_buckets;

cb::engine_errc BucketManager::setClusterConfig(
        const std::string& name,
        std::unique_ptr<ClusterConfiguration::Configuration> configuration) {
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
                return cb::engine_errc::success;
            }
            // We can't set the cluster configuration at this time as
            // the bucket is currently being initialized/paused/deleted,
            // but tell the client to try again :)
            return cb::engine_errc::temporary_failure;
        }

        if (bucket.state == Bucket::State::None &&
            first_free == all_buckets.size()) {
            first_free = ii;
        }
        ++ii;
    }

    if (first_free == all_buckets.size()) {
        return cb::engine_errc::too_big;
    }

    std::lock_guard<std::mutex> bucketguard(all_buckets[first_free].mutex);
    auto& bucket = all_buckets[first_free];
    bucket.type = BucketType::ClusterConfigOnly;
    bucket.clusterConfiguration.setConfiguration(std::move(configuration));
    strcpy(bucket.name, name.c_str());
    bucket.supportedFeatures.emplace(cb::engine::Feature::Collections);
    bucketStateChangeListener(bucket, Bucket::State::Ready);
    bucket.state = Bucket::State::Ready;
    LOG_INFO("Created cluster config bucket [{}]", name);
    return cb::engine_errc::success;
}

void BucketManager::iterateBuckets(std::function<bool(Bucket&)> fn) {
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
        std::copy(name.begin(), name.end(), free_bucket->name);
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
        LOG_WARNING("{}: Creation of bucket instance for bucket [{}] took {}",
                    cid,
                    name,
                    cb::time2text(stop - start));
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

    LOG_INFO(R"({}: Initialize {} bucket [{}] using configuration: "{}")",
             cid,
             to_string(type),
             name,
             config);
    start = std::chrono::steady_clock::now();
    auto result = bucket.getEngine().initialize(config);
    if (result != cb::engine_errc::success) {
        throw cb::engine_error(result, "initializeEngineInstance failed");
    }
    stop = std::chrono::steady_clock::now();
    if ((stop - start) > std::chrono::seconds{1}) {
        LOG_WARNING("{}: Initialization of bucket [{}] took {}",
                    cid,
                    name,
                    cb::time2text(stop - start));
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

cb::engine_errc BucketManager::create(Cookie& cookie,
                                      const std::string name,
                                      const std::string config,
                                      BucketType type) {
    return create(cookie.getConnectionId(), name, config, type);
}

cb::engine_errc BucketManager::create(uint32_t cid,
                                      const std::string name,
                                      const std::string config,
                                      BucketType type) {
    LOG_INFO("{}: Create {} bucket [{}]", cid, to_string(type), name);
    auto [err, free_bucket] = allocateBucket(name);
    if (err != cb::engine_errc::success) {
        LOG_ERROR("{}: Create bucket [{}] failed - {}",
                  cid,
                  name,
                  to_string(err));
        return err;
    }

    if (!free_bucket) {
        throw std::logic_error(
                "BucketManager::create: allocateBucket returned success but no "
                "bucket");
    }
    auto& bucket = *free_bucket;

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
        LOG_INFO("{}: Bucket [{}] created successfully", cid, name);
    } catch (const cb::engine_error& exception) {
        result = cb::engine_errc(exception.code().value());
        LOG_ERROR("{}: Failed to create bucket [{}]: {}",
                  cid,
                  name,
                  exception.what());
    } catch (const std::bad_alloc&) {
        LOG_ERROR("{}: Failed to create bucket [{}]: No memory", cid, name);
        result = cb::engine_errc::no_memory;
    } catch (const std::exception& e) {
        LOG_ERROR("{}: Failed to create bucket [{}]: {}", cid, name, e.what());
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
        result = cb::engine_errc::not_stored;
    }
    return result;
}

cb::engine_errc BucketManager::destroy(Cookie& cookie,
                                       const std::string name,
                                       bool force,
                                       std::optional<BucketType> type) {
    return destroy(std::to_string(cookie.getConnectionId()), name, force, type);
}

cb::engine_errc BucketManager::destroy(std::string_view cid,
                                       const std::string name,
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

            if (b.state == Bucket::State::Ready) {
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
        LOG_WARNING("{}: Delete bucket [{}]: {}", cid, name, to_string(ret));
        return ret;
    }

    auto& bucket = *bucket_ptr;
    if (bucket.type != BucketType::ClusterConfigOnly) {
        LOG_INFO("{}: Delete bucket [{}]. Notifying engine", cid, name);
        bucket.getEngine().initiate_shutdown();
        bucket.getEngine().cancel_all_operations_in_ewb_state();
    }

    LOG_INFO("{}: Delete bucket [{}]. Engine ready for shutdown", cid, name);

    // Wait until all users disconnected...
    waitForEveryoneToDisconnect(bucket, "Delete", cid);

    LOG_INFO("{}: Delete bucket [{}]. Shut down the bucket", cid, name);
    bucket.destroyEngine(force);

    LOG_INFO(
            "{}: Delete bucket [{}]. Clean up allocated resources ", cid, name);
    bucket.reset();

    LOG_INFO("{}: Delete bucket [{}] complete", cid, name);
    return cb::engine_errc::success;
}

void BucketManager::waitForEveryoneToDisconnect(Bucket& bucket,
                                                std::string_view operation,
                                                std::string_view id) {
    // Wait until all users disconnected...
    {
        std::unique_lock<std::mutex> guard(bucket.mutex);
        if (bucket.clients > 0) {
            LOG_INFO("{}: {} bucket [{}]. Wait for {} clients to disconnect",
                     id,
                     operation,
                     bucket.name,
                     bucket.clients);

            // Signal clients bound to the bucket before waiting
            guard.unlock();
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
            bucket.cond.wait_for(guard, seconds(1), [&bucket] {
                return bucket.clients == 0;
            });

            if (bucket.clients == 0) {
                break;
            }

            if (steady_clock::now() < nextLog) {
                guard.unlock();
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
            iterate_all_connections([&bucket, &currConns](Connection& conn) {
                if (&conn.getBucket() == &bucket) {
                    conn.signalIfIdle();
                    currConns[std::to_string(conn.getId())] = conn.toJSON();
                }
            });

            LOG_INFO(
                    R"({}: {} bucket [{}]. Still waiting: {} clients connected: {})",
                    id,
                    operation,
                    bucket.name,
                    bucket.clients,
                    currConns.dump());

            guard.lock();
        }
    }

    auto num = bucket.items_in_transit.load();
    int counter = 0;
    while (num != 0) {
        if (++counter % 100 == 0) {
            LOG_INFO(
                    R"({}: {} bucket [{}]. Still waiting: {} items still stuck in transfer.)",
                    id,
                    operation,
                    bucket.name,
                    num);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        num = bucket.items_in_transit.load();
    }
}

void BucketManager::tick() {
    forEach([](auto& b) {
        if (b.type != BucketType::NoBucket) {
            b.tick();
        }
        return true;
    });

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

void BucketManager::forEach(std::function<bool(Bucket&)> fn) {
    std::lock_guard<std::mutex> all_bucket_lock(buckets_lock);
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

Bucket& BucketManager::at(size_t idx) {
    return all_buckets[idx];
}

void BucketManager::destroyAll() {
    LOG_INFO_RAW("Stop all buckets");

    // Start at one (not zero) because zero is reserved for "no bucket".
    // The "no bucket" has a state of Bucket::State::Ready but no name.
    for (size_t ii = 1; ii < all_buckets.size(); ++ii) {
        if (all_buckets[ii].state == Bucket::State::Ready) {
            const std::string name{all_buckets[ii].name};
            LOG_INFO("Waiting for delete of {} to complete", name);
            destroy("<none>", name, false, {});
            LOG_INFO("Bucket {} deleted", name);
        }
    }
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
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to create the internal bucket \"No bucket\": {}",
                    exception.what());
    }
    nobucket.max_document_size = nobucket.getEngine().getMaxItemSize();
    nobucket.supportedFeatures = nobucket.getEngine().getFeatures();
    nobucket.type = BucketType::NoBucket;
    bucketStateChangeListener(nobucket, Bucket::State::Ready);
    nobucket.state = Bucket::State::Ready;
}
