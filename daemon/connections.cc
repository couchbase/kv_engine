/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connections.h"
#include "buckets.h"
#include "connection.h"
#include "front_end_thread.h"

#include <folly/Synchronized.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <algorithm>
#include <deque>

/// List of all of the connections created in the system (so that we can
/// iterate over them)
folly::Synchronized<std::deque<Connection*>>& getConnections() {
    static folly::Synchronized<std::deque<Connection*>> connections;
    return connections;
}

int signal_idle_clients(FrontEndThread& me, bool dumpConnection) {
    int connected = 0;
    const auto index = me.index;

    iterate_thread_connections(
            &me, [index, &connected, dumpConnection](Connection& connection) {
                ++connected;
                if (!connection.signalIfIdle() && dumpConnection) {
                    auto details = connection.toJSON().dump();
                    LOG_INFO("Worker thread {}: {}", index, details);
                }
            });
    return connected;
}

void iterate_thread_connections(FrontEndThread* thread,
                                std::function<void(Connection&)> callback) {
    // Deny modifications to the connection map while we're iterating
    // over it
    auto locked = getConnections().rlock();
    for (auto* c : *locked) {
        if (&c->getThread() == thread) {
            callback(*c);
        }
    }
}

Connection* conn_new(SOCKET sfd,
                     FrontEndThread& thread,
                     std::shared_ptr<ListeningPort> descr,
                     uniqueSslPtr ssl) {
    std::unique_ptr<Connection> ret;
    Connection* c = nullptr;

    try {
        ret = std::make_unique<Connection>(
                sfd, thread, std::move(descr), std::move(ssl));
        getConnections().wlock()->push_back(ret.get());
        c = ret.release();
        stats.total_conns++;
    } catch (const std::bad_alloc&) {
        LOG_WARNING_RAW("Failed to allocate memory for connection");
    } catch (const std::exception& error) {
        LOG_WARNING("Failed to create connection: {}", error.what());
    } catch (...) {
        LOG_WARNING_RAW("Failed to create connection");
    }

    // We failed to create the connection (or failed to insert it into
    // the list of all connections
    if (!c) {
        return nullptr;
    }

    associate_initial_bucket(*c);
    const auto& bucket = c->getBucket();
    if (bucket.type != BucketType::NoBucket) {
        LOG_INFO("{}: Accepted new client connected to bucket:[{}]",
                 c->getId(),
                 bucket.name);
    }

    return c;
}

/**
 * Release a connection; removing it from the connection list management
 * and freeing the Connection object.
 */
void conn_destroy(Connection* c) {
    getConnections().withWLock([c](auto& conns) {
        auto iter = std::find(conns.begin(), conns.end(), c);
        // The connection should be one I know about
        cb_assert(iter != conns.end());
        conns.erase(iter);
    });
    // Finally free it
    delete c;
}
