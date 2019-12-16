/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
folly::Synchronized<std::deque<Connection*>> connections;

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
    auto locked = connections.rlock();
    for (auto* c : *locked) {
        if (&c->getThread() == thread) {
            callback(*c);
        }
    }
}

Connection* conn_new(SOCKET sfd,
                     const ListeningPort& interface,
                     struct event_base* base,
                     FrontEndThread& thread) {
    std::unique_ptr<Connection> ret;
    Connection* c = nullptr;

    try {
        ret = std::make_unique<Connection>(sfd, base, interface, thread);
        connections.wlock()->push_back(ret.get());
        c = ret.release();
        stats.total_conns++;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("Failed to allocate memory for connection");
    } catch (const std::exception& error) {
        LOG_WARNING("Failed to create connection: {}", error.what());
    } catch (...) {
        LOG_WARNING("Failed to create connection");
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
    connections.withWLock([c](auto& conns) {
        auto iter = std::find(conns.begin(), conns.end(), c);
        // The connection should be one I know about
        cb_assert(iter != conns.end());
        conns.erase(iter);
    });
    // Finally free it
    delete c;
}
