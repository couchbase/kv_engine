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
#include "utilities/protocol2text.h"

#include <cJSON.h>
#include <nlohmann/json.hpp>
#include <platform/cb_malloc.h>
#include <algorithm>
#include <list>
#include <memory>

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
                                       const ListeningPort& interface);

static void release_connection(Connection* c);

/** External functions *******************************************************/
int signal_idle_clients(FrontEndThread* me, int bucket_idx, bool logging) {
    // We've got a situation right now where we're seeing that
    // some of the connections is "stuck". Let's dump all
    // information until we solve the bug.
    logging = true;

    int connected = 0;
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto* c : connections.conns) {
        if (c->getThread() == me) {
            ++connected;
            if (bucket_idx == -1 || c->getBucketIndex() == bucket_idx) {
                c->signalIfIdle(logging, me->index);
            }
        }
    }

    return connected;
}

void iterate_thread_connections(FrontEndThread* thread,
                                std::function<void(Connection&)> callback) {
    // Deny modifications to the connection map while we're iterating
    // over it
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto* c : connections.conns) {
        if (c->getThread() == thread) {
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
            usleep(500);
        }
    } while (!done);
}

void run_event_loop(Connection* c, short which) {
    const auto start = ProcessClock::now();
    c->runEventLoop(which);
    const auto stop = ProcessClock::now();

    using namespace std::chrono;
    const auto ns = duration_cast<nanoseconds>(stop - start);
    c->addCpuTime(ns);

    auto* thread = c->getThread();
    if (thread != nullptr) {
        scheduler_info[thread->index].add(ns);
    }

    if (c->shouldDelete()) {
        release_connection(c);
    }
}

Connection* conn_new(const SOCKET sfd,
                     in_port_t parent_port,
                     struct event_base* base,
                     FrontEndThread* thread) {
    Connection* c;
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        auto* interface = get_listening_port_instance(parent_port);
        if (interface == nullptr) {
            LOG_WARNING("{}: failed to locate server port {}. Disconnecting",
                        (unsigned int)sfd,
                        parent_port);
            return nullptr;
        }

        c = allocate_connection(sfd, base, *interface);
    }

    if (c == nullptr) {
        return nullptr;
    }

    stats.total_conns++;

    c->incrementRefcount();

    associate_initial_bucket(*c);

    const auto& bucket = c->getBucket();
    if (bucket.type != BucketType::NoBucket) {
        LOG_INFO("{}: Accepted new client connected to bucket:[{}]",
                 c->getId(),
                 bucket.name);
    }

    c->setThread(thread);

    if (settings.getVerbose() > 1) {
        LOG_DEBUG("<{} new client connection", sfd);
    }

    return c;
}

ListeningPort *get_listening_port_instance(const in_port_t port) {
    for (auto &instance : stats.listening_ports) {
        if (instance.port == port) {
            return &instance;
        }
    }

    return nullptr;
}

#ifndef WIN32
/**
 * NOTE: This is <b>not</b> intended to be called during normal situation,
 * but in the case where we've been exhausting all connections to memcached
 * we need a way to be able to dump the connection states in order to search
 * for a bug.
 */
void dump_connection_stat_signal_handler(evutil_socket_t, short, void *) {
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto *c : connections.conns) {
        try {
            auto info = c->toJSON().dump();
            LOG_INFO("Connection: {}", info);
        } catch (const std::bad_alloc&) {
            LOG_WARNING("Failed to allocate memory to dump info for {}",
                        c->getId());
        }
    }
}
#endif

void conn_loan_buffers(Connection* c) {
    if (c == nullptr) {
        return;
    }

    auto* ts = get_thread_stats(c);
    switch (loan_single_buffer(*c, c->getThread()->read, c->read)) {
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

    switch (loan_single_buffer(*c, c->getThread()->write, c->write)) {
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

    auto thread = c->getThread();

    if (thread == nullptr) {
        // Connection already cleaned up - nothing to do.
        return;
    }

    if (c->isDCP()) {
        // DCP work differently - let them keep their buffers once allocated.
        return;
    }

    maybe_return_single_buffer(*c, thread->read, c->read);
    maybe_return_single_buffer(*c, thread->write, c->write);
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
                                       const ListeningPort& interface) {
    Connection* ret = nullptr;

    try {
        ret = new Connection(sfd, base, interface);
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
        c.setState(McbpStateMachine::State::closing);
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
