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
    LOG_INFO("{}: Delete bucket [{}]. Notifying engine", connectionId, name);

    bucket.getEngine().initiate_shutdown();
    bucket.getEngine().cancel_all_operations_in_ewb_state();

    LOG_INFO("{}: Delete bucket [{}]. Engine ready for shutdown",
             connectionId,
             name);

    std::unique_lock<std::mutex> guard(bucket.mutex);
    if (bucket.clients > 0) {
        LOG_INFO("{}: Delete bucket [{}]. Wait for {} clients to disconnect",
                 connectionId,
                 name,
                 bucket.clients);
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
            iterate_all_connections([&bucket = bucket](Connection& conn) {
                if (&conn.getBucket() == &bucket) {
                    conn.signalIfIdle();
                }
            });
            bucket.getEngine().cancel_all_operations_in_ewb_state();
            return cb::engine_errc::would_block;
        }

        nextLogConnections = steady_clock::now() + 1min;

        // drop the lock and notify the worker threads
        guard.unlock();

        nlohmann::json currConns;
        iterate_all_connections(
                [&bucket = bucket, &currConns](Connection& conn) {
                    if (&conn.getBucket() == &bucket) {
                        conn.signalIfIdle();
                        currConns[std::to_string(conn.getId())] = conn.toJSON();
                    }
                });

        LOG_INFO(
                "{}: Delete bucket [{}].Still waiting: {} clients connected: "
                "{}",
                connectionId,
                name,
                bucket.clients,
                currConns.dump());

        guard.lock();
    }
    state = State::WaitingForItemsInTransit;
    return cb::engine_errc::success;
}

cb::engine_errc BucketDestroyer::waitForItemsInTransit() {
    auto num = bucket.items_in_transit.load();
    if (num != 0) {
        if (++itemsInTransitCheckCounter % 100 == 0) {
            LOG_INFO(
                    "{}: Delete bucket [{}]. Still waiting: {} items still "
                    "stuck in transfer.",
                    connectionId,
                    name,
                    num);
        }
        return cb::engine_errc::would_block;
    }

    state = State::Cleanup;
    return cb::engine_errc::success;
}

cb::engine_errc BucketDestroyer::cleanup() {
    LOG_INFO(
            "{}: Delete bucket [{}]. Shut down the bucket", connectionId, name);
    bucket.destroyEngine(force);

    LOG_INFO("{}: Delete bucket [{}]. Clean up allocated resources ",
             connectionId,
             name);
    bucket.reset();

    LOG_INFO("{}: Delete bucket [{}] complete", connectionId, name);
    state = State::Done;
    return cb::engine_errc::success;
}