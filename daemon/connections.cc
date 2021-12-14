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
#include "runtime.h"
#include "settings.h"
#include "stats.h"

#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <algorithm>
#include <chrono>
#include <list>
#include <memory>
#include <thread>

/*
 * Free list management for connections.
 */
struct connections {
    std::mutex mutex;
    std::list<Connection*> conns;
} connections;


/** Types ********************************************************************/

/** Result of a buffer loan attempt */
enum class BufferLoan {
    Existing,
    Loaned,
    Allocated,
};

/** Function prototypes ******************************************************/

static BufferLoan loan_single_buffer(Connection& c,
                                     std::unique_ptr<cb::Pipe>& thread_buf,
                                     std::unique_ptr<cb::Pipe>& conn_buf);
static void maybe_return_single_buffer(Connection& c,
                                       std::unique_ptr<cb::Pipe>& thread_buf,
                                       std::unique_ptr<cb::Pipe>& conn_buf);
static void conn_destructor(Connection* c);
static Connection* allocate_connection(SOCKET sfd,
                                       event_base* base,
                                       std::shared_ptr<ListeningPort> interface,
                                       FrontEndThread& thread);

static void release_connection(Connection* c);

/** External functions *******************************************************/

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
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto* c : connections.conns) {
        if (&c->getThread() == thread) {
            callback(*c);
        }
    }
}

void destroy_connections() {
    std::lock_guard<std::mutex> lock(connections.mutex);
    /* traverse the list of connections. */
    for (auto* c : connections.conns) {
        conn_destructor(c);
    }
    connections.conns.clear();
}

void close_all_connections() {
    /* traverse the list of connections. */
    {
        std::lock_guard<std::mutex> lock(connections.mutex);
        for (auto* c : connections.conns) {
            if (!c->isSocketClosed()) {
                safe_close(c->getSocketDescriptor());
                c->setSocketDescriptor(INVALID_SOCKET);
            }

            if (c->getRefcount() > 1) {
                perform_callbacks(ON_DISCONNECT, NULL, c);
            }
        }
    }

    /*
     * do a second loop, this time wait for all of them to
     * be closed.
     */
    bool done;
    do {
        done = true;
        {
            std::lock_guard<std::mutex> lock(connections.mutex);
            for (auto* c : connections.conns) {
                if (c->getRefcount() > 1) {
                    done = false;
                    break;
                }
            }
        }

        if (!done) {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    } while (!done);
}

void run_event_loop(Connection* c, short which) {
    const auto start = std::chrono::steady_clock::now();
    c->runEventLoop(which);
    const auto stop = std::chrono::steady_clock::now();

    using namespace std::chrono;
    const auto ns = duration_cast<nanoseconds>(stop - start);
    c->addCpuTime(ns);
    scheduler_info[c->getThread().index].add(duration_cast<microseconds>(ns));

    if (c->shouldDelete()) {
        release_connection(c);
    }
}

Connection* conn_new(SOCKET sfd,
                     std::shared_ptr<ListeningPort> interface,
                     struct event_base* base,
                     FrontEndThread& thread) {
    auto* c = allocate_connection(sfd, base, std::move(interface), thread);
    if (c == nullptr) {
        return nullptr;
    }

    stats.total_conns++;

    c->incrementRefcount();

    associate_initial_bucket(*c);

    const auto& bucket = c->getBucket();
    if (bucket.type != Bucket::Type::NoBucket) {
        LOG_INFO("{}: Accepted new client connected to bucket:[{}]",
                 c->getId(),
                 bucket.name);
    }

    if (Settings::instance().getVerbose() > 1) {
        LOG_DEBUG("<{} new client connection", sfd);
    }

    return c;
}

void conn_loan_buffers(Connection* c) {
    if (c == nullptr) {
        return;
    }

    auto* ts = get_thread_stats(c);
    switch (loan_single_buffer(*c, c->getThread().read, c->read)) {
    case BufferLoan::Existing:
        ts->rbufs_existing++;
        break;
    case BufferLoan::Loaned:
        ts->rbufs_loaned++;
        break;
    case BufferLoan::Allocated:
        ts->rbufs_allocated++;
        break;
    }

    switch (loan_single_buffer(*c, c->getThread().write, c->write)) {
    case BufferLoan::Existing:
        ts->wbufs_existing++;
        break;
    case BufferLoan::Loaned:
        ts->wbufs_loaned++;
        break;
    case BufferLoan::Allocated:
        ts->wbufs_allocated++;
        break;
    }
}

void conn_return_buffers(Connection* c) {
    if (c == nullptr) {
        return;
    }

    if (c->isDCP()) {
        // DCP work differently - let them keep their buffers once allocated.
        return;
    }

    maybe_return_single_buffer(*c, c->getThread().read, c->read);
    maybe_return_single_buffer(*c, c->getThread().write, c->write);
}

/** Internal functions *******************************************************/

/**
 * Destructor for all connection objects. Release all allocated resources.
 */
static void conn_destructor(Connection* c) {
    delete c;
    stats.conn_structs--;
}

/** Allocate a connection, creating memory and adding it to the conections
 *  list. Returns a pointer to the newly-allocated connection if successful,
 *  else NULL.
 */
static Connection* allocate_connection(SOCKET sfd,
                                       event_base* base,
                                       std::shared_ptr<ListeningPort> interface,
                                       FrontEndThread& thread) {
    Connection* ret = nullptr;

    try {
        ret = new Connection(sfd, base, std::move(interface), thread);
        std::lock_guard<std::mutex> lock(connections.mutex);
        connections.conns.push_back(ret);
        stats.conn_structs++;
        return ret;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("Failed to allocate memory for connection");
    } catch (const std::exception& error) {
        LOG_WARNING("Failed to create connection: {}", error.what());
    } catch (...) {
        LOG_WARNING("Failed to create connection");
    }

    delete ret;
    return NULL;
}

/** Release a connection; removing it from the connection list management
 *  and freeing the Connection object.
 */
static void release_connection(Connection* c) {
    {
        std::lock_guard<std::mutex> lock(connections.mutex);
        auto iter = std::find(connections.conns.begin(), connections.conns.end(), c);
        // I should assert
        cb_assert(iter != connections.conns.end());
        connections.conns.erase(iter);
    }

    // Finally free it
    conn_destructor(c);
}

/**
 * If the connection doesn't already have a populated conn_buff, ensure that
 * it does by either loaning out the threads, or allocating a new one if
 * necessary.
 */
static BufferLoan loan_single_buffer(Connection& c,
                                     std::unique_ptr<cb::Pipe>& thread_buf,
                                     std::unique_ptr<cb::Pipe>& conn_buf) {
    /* Already have a (partial) buffer - nothing to do. */
    if (conn_buf) {
        return BufferLoan::Existing;
    }

    // If the thread has a buffer, let's loan that to the connection
    if (thread_buf) {
        thread_buf.swap(conn_buf);
        return BufferLoan::Loaned;
    }

    // Need to allocate a new buffer
    try {
        conn_buf = std::make_unique<cb::Pipe>(DATA_BUFFER_SIZE);
    } catch (const std::bad_alloc&) {
        // Unable to alloc a buffer for the thread. Not much we can do here
        // other than terminate the current connection.
        LOG_WARNING(
                "{}: Failed to allocate new network buffer.. closing "
                "connection {}",
                c.getId(),
                c.getDescription());
        c.setTerminationReason("Failed to allocate network buffer");
        c.setState(StateMachine::State::closing);
        return BufferLoan::Existing;
    }

    return BufferLoan::Allocated;
}

static void maybe_return_single_buffer(Connection& c,
                                       std::unique_ptr<cb::Pipe>& thread_buf,
                                       std::unique_ptr<cb::Pipe>& conn_buf) {
    if (conn_buf && conn_buf->empty()) {
        // Buffer clean, dispose of it
        if (thread_buf) {
            // Already got a thread buffer.. release this one
            conn_buf.reset();
        } else {
            conn_buf.swap(thread_buf);
        }
    }
}
