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
#include "log_macros.h"
#include "memcached.h"
#include "stats.h"
#include <daemon/server_core_api.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <platform/scope_timer.h>
#include <platform/timeutils.h>
#include <utilities/engine_errc_2_mcbp.h>

static std::atomic<size_t> read_compute_unit_size;
static std::atomic<size_t> write_compute_unit_size;

Bucket::Bucket() = default;

void Bucket::reset() {
    std::lock_guard<std::mutex> guard(mutex);
    engine.reset();
    state = Bucket::State::None;
    name[0] = '\0';
    setEngine(nullptr);
    clusterConfiguration.reset();
    max_document_size = default_max_item_size;
    supportedFeatures = {};
    read_compute_units_used = 0;
    write_compute_units_used = 0;
    for (auto& c : responseCounters) {
        c.reset();
    }
    subjson_operation_times.reset();
    timings.reset();
    for (auto& s : stats) {
        s.reset();
    }
    type = BucketType::Unknown;
}

bool Bucket::supports(cb::engine::Feature feature) {
    return supportedFeatures.find(feature) != supportedFeatures.end();
}

nlohmann::json Bucket::to_json() const {
    std::lock_guard<std::mutex> guard(mutex);
    nlohmann::json json;
    if (state != State::None) {
        try {
            json["state"] = to_string(state.load());
            json["clients"] = clients.load();
            json["name"] = name;
            json["type"] = to_string(type);
            json["rcu"] = read_compute_units_used.load();
            json["wcu"] = write_compute_units_used.load();
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to generate bucket details: {}", e.what());
        }
    }
    return json;
}

DcpIface* Bucket::getDcpIface() const {
    return bucketDcp;
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

void Bucket::commandExecuted(const Cookie& cookie) {
    const auto [read, write] = cookie.getDocumentRWBytes();
    const auto rcu =
            (read + read_compute_unit_size - 1) / read_compute_unit_size;
    const auto wcu =
            (write + write_compute_unit_size - 1) / write_compute_unit_size;
    read_compute_units_used += rcu;
    write_compute_units_used += wcu;
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
    ScopeTimer<SpanStopwatch> timer(
            std::forward_as_tuple(cookie, Code::CreateRbacContext));
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

cb::engine_errc BucketManager::create(Cookie& cookie,
                                      const std::string name,
                                      const std::string config,
                                      BucketType type) {
    // If there is an error I should set the cookie error context
    cb::engine_errc result;
    auto cid = cookie.getConnectionId();

    LOG_INFO("{}: Create {} bucket [{}]", cid, to_string(type), name);

    size_t ii;
    size_t first_free = all_buckets.size();
    bool found = false;

    std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);
    for (ii = 0; ii < all_buckets.size() && !found; ++ii) {
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        if (first_free == all_buckets.size() &&
            all_buckets[ii].state == Bucket::State::None) {
            first_free = ii;
        }
        if (name == all_buckets[ii].name) {
            found = true;
        }
    }

    if (found) {
        result = cb::engine_errc::key_already_exists;
        LOG_ERROR("{}: Create bucket [{}] failed - Already exists", cid, name);
    } else if (first_free == all_buckets.size()) {
        result = cb::engine_errc::too_big;
        LOG_ERROR(
                "{}: Create bucket [{}] failed - Too many buckets", cid, name);
    } else {
        result = cb::engine_errc::success;
        ii = first_free;
        /*
         * split the creation of the bucket in two... so
         * we can release the global lock..
         */
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        all_buckets[ii].state = Bucket::State::Creating;
        all_buckets[ii].type = type;
        strcpy(all_buckets[ii].name, name.c_str());
    }
    all_bucket_lock.unlock();

    if (result != cb::engine_errc::success) {
        return result;
    }

    auto& bucket = all_buckets[ii];

    // People aren't allowed to use the engine in this state,
    // so we can do stuff without locking..
    try {
        const auto start = std::chrono::steady_clock::now();
        bucket.setEngine(new_engine_instance(type, get_server_api));
        const auto stop = std::chrono::steady_clock::now();
        if ((stop - start) > std::chrono::seconds{1}) {
            LOG_WARNING(
                    "{}: Creation of bucket instance for bucket [{}] took {}",
                    cid,
                    name,
                    cb::time2text(stop - start));
        }
    } catch (const cb::engine_error& exception) {
        bucket.reset();
        LOG_ERROR("{}: Failed to create bucket [{}]: {}",
                  cid,
                  name,
                  exception.what());
        result = cb::engine_errc(exception.code().value());
        return result;
    }

    auto& engine = bucket.getEngine();
    {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucket.state = Bucket::State::Initializing;
    }

    try {
        LOG_INFO(R"({}: Initialize {} bucket [{}] using configuration: "{}")",
                 cid,
                 to_string(type),
                 name,
                 config);
        const auto start = std::chrono::steady_clock::now();
        result = engine.initialize(config);
        const auto stop = std::chrono::steady_clock::now();
        if ((stop - start) > std::chrono::seconds{1}) {
            LOG_WARNING("{}: Initialization of bucket [{}] took {}",
                        cid,
                        name,
                        cb::time2text(stop - start));
        }
    } catch (const std::runtime_error& e) {
        LOG_ERROR("{}: Failed to create bucket [{}]: {}", cid, name, e.what());
        result = cb::engine_errc::failed;
    } catch (const std::bad_alloc& e) {
        LOG_ERROR("{}: Failed to create bucket [{}]: {}", cid, name, e.what());
        result = cb::engine_errc::no_memory;
    }

    if (result == cb::engine_errc::success) {
        // We don't pass the storage threads down in the config like we do for
        // readers and writers because that evolved over time to be duplicated
        // in both configs. Instead, we just inform the engine of the number
        // of threads.
        auto* serverCoreApi =
                dynamic_cast<ServerCoreApi*>(get_server_api()->core);
        if (!serverCoreApi) {
            throw std::runtime_error("Server core API is unexpected type");
        }
        engine.set_num_storage_threads(ThreadPoolConfig::StorageThreadCount(
                Settings::instance().getNumStorageThreads()));

        bucket.max_document_size = engine.getMaxItemSize();
        bucket.supportedFeatures = engine.getFeatures();

        // MB-47231: Reported use after free which was most likely caused
        // by setting the state of the bucket to ready _before_
        // initializing bucket.supportedFeatures so if another thread
        // tried to select the bucket and was scheduled in between of these
        // calls it would fetch the old value for supportedFeature and we're
        // experiencing undefined behavior (one thread will be writing into
        // the std::set while another one reads from it. The other thread
        // could also be traversing freed memory.
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = Bucket::State::Ready;
        }
        LOG_INFO("{}: Bucket [{}] created successfully", cid, name);
    } else {
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = Bucket::State::Destroying;
        }

        bucket.reset();

        result = cb::engine_errc::not_stored;
    }
    return result;
}
cb::engine_errc BucketManager::destroy(Cookie* cookie,
                                       const std::string name,
                                       bool force) {
    cb::engine_errc ret = cb::engine_errc::no_such_key;
    std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);

    Connection* connection = nullptr;
    if (cookie != nullptr) {
        connection = &cookie->getConnection();
    }

    /*
     * The destroy function will have access to a connection if the
     * McbpDestroyBucketTask originated from delete_bucket_executor().
     * However if we are in the process of shuting down and the
     * McbpDestroyBucketTask originated from main() then connection
     * will be set to nullptr.
     */
    const std::string connection_id{
            (connection == nullptr) ? "<none>"
                                    : std::to_string(connection->getId())};

    size_t idx = 0;
    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        if (name == all_buckets[ii].name) {
            idx = ii;
            if (all_buckets[ii].state == Bucket::State::Ready) {
                ret = cb::engine_errc::success;
                all_buckets[ii].state = Bucket::State::Destroying;
            } else {
                ret = cb::engine_errc::key_already_exists;
            }
        }
        if (ret != cb::engine_errc::no_such_key) {
            break;
        }
    }
    all_bucket_lock.unlock();

    if (ret != cb::engine_errc::success) {
        LOG_INFO("{}: Delete bucket [{}]: {}",
                 connection_id,
                 name,
                 to_string(ret));
        return ret;
    }

    LOG_INFO("{}: Delete bucket [{}]. Notifying engine", connection_id, name);

    all_buckets[idx].getEngine().initiate_shutdown();
    all_buckets[idx].getEngine().cancel_all_operations_in_ewb_state();

    LOG_INFO("{}: Delete bucket [{}]. Engine ready for shutdown",
             connection_id,
             name);

    /* If this thread is connected to the requested bucket... release it */
    if (connection != nullptr && idx == size_t(connection->getBucketIndex())) {
        disassociate_bucket(*connection, cookie);
    }

    // Wait until all users disconnected...
    auto& bucket = all_buckets[idx];
    {
        std::unique_lock<std::mutex> guard(bucket.mutex);
        if (bucket.clients > 0) {
            LOG_INFO(
                    "{}: Delete bucket [{}]. Wait for {} clients to disconnect",
                    connection_id,
                    name,
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

        // We need to disconnect all of the clients before we can delete the
        // bucket. We log pending connections that are blocking bucket deletion.
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
                bucket.getEngine().cancel_all_operations_in_ewb_state();
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
                    R"({}: Delete bucket [{}]. Still waiting: {} clients connected: {})",
                    connection_id,
                    name,
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
                    R"({}: Delete bucket [{}]. Still waiting: {} items still stuck in transfer.)",
                    connection_id,
                    name,
                    num);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        num = bucket.items_in_transit.load();
    }

    LOG_INFO("{}: Delete bucket [{}]. Shut down the bucket",
             connection_id,
             name);
    bucket.destroyEngine(force);

    LOG_INFO("{}: Delete bucket [{}]. Clean up allocated resources ",
             connection_id,
             name);
    bucket.reset();

    LOG_INFO("{}: Delete bucket [{}] complete", connection_id, name);
    return cb::engine_errc::success;
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
            BucketManager::instance().destroy(nullptr, name, false);
            LOG_INFO("Bucket {} deleted", name);
        }
    }
}

BucketManager::BucketManager() {
    auto& settings = Settings::instance();
    read_compute_unit_size = settings.getReadComputeUnitSize();
    write_compute_unit_size = settings.getWriteComputeUnitSize();

    settings.addChangeListener("read_compute_unit_size",
                               [](const std::string&, Settings& s) -> void {
                                   read_compute_unit_size =
                                           s.getReadComputeUnitSize();
                               });
    settings.addChangeListener("write_compute_unit_size",
                               [](const std::string&, Settings& s) -> void {
                                   write_compute_unit_size =
                                           s.getWriteComputeUnitSize();
                               });

    size_t numthread = settings.getNumWorkerThreads() + 1;
    for (auto& b : all_buckets) {
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
    nobucket.state = Bucket::State::Ready;
}
