/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "bucket_destroyer.h"

#include "connection.h"

#include "memcached.h"
#include <logger/logger.h>
#include <platform/json_log_conversions.h>

using namespace std::chrono_literals;

BucketDestroyer::BucketDestroyer(Bucket& bucket,
                                 std::string connectionId,
                                 bool force)
    : connectionId(std::move(connectionId)),
      bucket(bucket),
      name(bucket.name),
      force(force) {
    using std::chrono::steady_clock;
    // record the next time we should log about deleting this bucket
    // if connections are still lingering
    nextLogConnections = steady_clock::now() + 2min;
}

cb::engine_errc BucketDestroyer::drive() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Starting:
            ret = start();
            break;
        case State::WaitingForConnectionsToClose:
            ret = waitForConnections();
            break;
        case State::WaitingForItemsInTransit:
            ret = waitForItemsInTransit();
            break;
        case State::Cleanup:
            ret = cleanup();
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}

cb::engine_errc BucketDestroyer::start() {
    if (bucket.type != BucketType::ClusterConfigOnly) {
        LOG_INFO_CTX("Delete bucket. Notifying engine",
                     {"conn_id", connectionId},
                     {"bucket", name});
        bucket.getEngine().initiate_shutdown();
        bucket.getEngine().cancel_all_operations_in_ewb_state();
    }

    LOG_INFO_CTX("Delete bucket. Engine ready for shutdown",
                 {"conn_id", connectionId},
                 {"bucket", name});

    std::unique_lock<std::mutex> guard(bucket.mutex);
    if (bucket.clients > 0) {
        LOG_INFO_CTX("Delete bucket. Wait for clients to disconnect",
                     {"conn_id", connectionId},
                     {"bucket", name},
                     {"clients", bucket.clients});
        guard.unlock();
        bucket.deleteThrottledCommands();
        iterate_all_connections([&bucket = bucket](Connection& connection) {
            if (&connection.getBucket() == &bucket) {
                connection.signalIfIdle();
            }
        });
    }

    state = State::WaitingForConnectionsToClose;
    return cb::engine_errc::success;
}
cb::engine_errc BucketDestroyer::waitForConnections() {
    // Wait until all users disconnected...
    std::unique_lock<std::mutex> guard(bucket.mutex);

    using std::chrono::steady_clock;

    // We need to disconnect all of the clients before we can delete the
    // bucket. We log pending connections that are blocking bucket deletion.
    if (bucket.clients > 0) {
        if (steady_clock::now() < nextLogConnections) {
            guard.unlock();
            bucket.deleteThrottledCommands();
            iterate_all_connections([&bucket = bucket](Connection& conn) {
                if (&conn.getBucket() == &bucket) {
                    conn.signalIfIdle();
                }
            });
            if (bucket.type != BucketType::ClusterConfigOnly) {
                bucket.getEngine().cancel_all_operations_in_ewb_state();
            }
            return cb::engine_errc::would_block;
        }

        nextLogConnections = steady_clock::now() + 1min;

        // drop the lock and notify the worker threads
        guard.unlock();

        nlohmann::json currConns;
        bucket.deleteThrottledCommands();
        iterate_all_connections([&bucket = bucket,
                                 &currConns](Connection& conn) {
            if (&conn.getBucket() == &bucket) {
                conn.signalIfIdle();
                currConns[std::to_string(conn.getId())] = conn;
            }
        });

        LOG_INFO_CTX("Delete bucket. Still waiting: clients connected",
                     {"conn_id", connectionId},
                     {"bucket", name},
                     {"clients", bucket.clients},
                     {"description", currConns});

        // we need to wait more
        return cb::engine_errc::would_block;
    }

    state = State::WaitingForItemsInTransit;
    return cb::engine_errc::success;
}

cb::engine_errc BucketDestroyer::waitForItemsInTransit() {
    auto num = bucket.items_in_transit.load();
    if (num != 0) {
        if (++itemsInTransitCheckCounter % 100 == 0) {
            LOG_INFO_CTX(
                    "Delete bucket. Still waiting: items still stuck in "
                    "transfer",
                    {"conn_id", connectionId},
                    {"bucket", name},
                    {"items_in_transit", num});
        }
        return cb::engine_errc::would_block;
    }

    state = State::Cleanup;
    return cb::engine_errc::success;
}

cb::engine_errc BucketDestroyer::cleanup() {
    LOG_INFO_CTX("Delete bucket. Shut down the bucket",
                 {"conn_id", connectionId},
                 {"bucket", name});
    bucket.destroyEngine(force);

    LOG_INFO_CTX("Delete bucket. Clean up allocated resources",
                 {"conn_id", connectionId},
                 {"bucket", name});
    bucket.reset();

    LOG_INFO_CTX("Delete bucket complete",
                 {"conn_id", connectionId},
                 {"bucket", name});
    state = State::Done;
    return cb::engine_errc::success;
}