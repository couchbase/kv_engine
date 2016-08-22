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
#include "runtime.h"
#include "utilities/protocol2text.h"
#include "settings.h"
#include "stats.h"

#include <cJSON.h>
#include <list>
#include <algorithm>

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

static BufferLoan conn_loan_single_buffer(McbpConnection *c, struct net_buf *thread_buf,
                                             struct net_buf *conn_buf);
static void conn_return_single_buffer(Connection *c, struct net_buf *thread_buf,
                                      struct net_buf *conn_buf);
static void conn_destructor(Connection *c);
static Connection *allocate_connection(SOCKET sfd,
                                       event_base *base,
                                       const struct listening_port &interface);

static ListenConnection* allocate_listen_connection(SOCKET sfd,
                                                    event_base* base,
                                                    in_port_t port,
                                                    sa_family_t family,
                                                    const struct interface& interf);

static Connection *allocate_pipe_connection(int fd, event_base *base);
static void release_connection(Connection *c);

/** External functions *******************************************************/
int signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx, bool logging)
{
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

void assert_no_associations(int bucket_idx)
{
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto* c : connections.conns) {
        cb_assert(c->getBucketIndex() != bucket_idx);
    }
}

void destroy_connections(void)
{
    std::lock_guard<std::mutex> lock(connections.mutex);
    /* traverse the list of connections. */
    for (auto* c : connections.conns) {
        conn_destructor(c);
    }
    connections.conns.clear();
}

void close_all_connections(void)
{
    /* traverse the list of connections. */
    {
        std::lock_guard<std::mutex> lock(connections.mutex);
        for (auto* c : connections.conns) {
            if (!c->isSocketClosed()) {
                safe_close(c->getSocketDescriptor());
                c->setSocketDescriptor(INVALID_SOCKET);
            }

            if (c->getRefcount() > 1) {
                // @todo fix this for Greenstack
                auto* mcbp = dynamic_cast<McbpConnection*>(c);
                if (mcbp == nullptr) {
                    abort();
                } else {
                    perform_callbacks(ON_DISCONNECT, NULL, mcbp);
                }
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
    c->runEventLoop(which);
    if (c->shouldDelete()) {
        release_connection(c);
    }
}

ListenConnection* conn_new_server(const SOCKET sfd,
                                  in_port_t parent_port,
                                  sa_family_t family,
                                  const struct interface& interf,
                                  struct event_base* base) {
    auto* c = allocate_listen_connection(sfd, base, parent_port, family, interf);
    if (c == nullptr) {
        return nullptr;
    }
    c->incrementRefcount();

    MEMCACHED_CONN_ALLOCATE(c->getId());
    LOG_DEBUG(c, "<%d server listening on %s", sfd, c->getSockname().c_str());

    stats.total_conns++;
    return c;
}

Connection* conn_new(const SOCKET sfd, in_port_t parent_port,
                     struct event_base* base,
                     LIBEVENT_THREAD* thread) {

    Connection *c = nullptr;

    for (auto& interface : stats.listening_ports) {
        if (parent_port == interface.port) {
            c = allocate_connection(sfd, base, interface);
            if (c == nullptr) {
                return nullptr;
            }

            LOG_INFO(NULL, "%u: Using protocol: %s", c->getId(),
                     to_string(c->getProtocol()));
            break;
        }
    }

    if (c == nullptr) {
        LOG_WARNING(NULL, "%u: failed to locate server port %u. Disconnecting",
                    (unsigned int)sfd, parent_port);
        return nullptr;
    }

    stats.total_conns++;

    c->incrementRefcount();

    associate_initial_bucket(c);

    c->setThread(thread);
    MEMCACHED_CONN_ALLOCATE(c->getId());

    if (settings.getVerbose() > 1) {
        LOG_DEBUG(c, "<%d new client connection", sfd);
    }

    return c;
}

/*
    Pipe input
*/
Connection* conn_pipe_new(const int fd,
                          struct event_base *base,
                          LIBEVENT_THREAD* thread) {
    Connection *c = allocate_pipe_connection(fd, base);

    stats.total_conns++;
    c->incrementRefcount();
    c->setThread(thread);
    associate_initial_bucket(c);
    MEMCACHED_CONN_ALLOCATE(c->getId());

    return c;

}

void conn_cleanup_engine_allocations(McbpConnection * c) {
    ENGINE_HANDLE* handle = reinterpret_cast<ENGINE_HANDLE*>(c->getBucketEngine());
    if (c->getItem() != nullptr) {
        c->getBucketEngine()->release(handle, c, c->getItem());
        c->setItem(nullptr);
    }

    c->releaseReservedItems();
}

static void conn_cleanup(Connection *c) {
    if (c == nullptr) {
        throw std::invalid_argument("conn_cleanup: 'c' must be non-NULL");
    }
    c->setAdmin(false);

    auto* mcbpc = dynamic_cast<McbpConnection*>(c);
    if (mcbpc != nullptr) {
        mcbpc->releaseTempAlloc();

        mcbpc->read.curr = mcbpc->read.buf;
        mcbpc->read.bytes = 0;
        mcbpc->write.curr = mcbpc->write.buf;
        mcbpc->write.bytes = 0;

        /* Return any buffers back to the thread; before we disassociate the
         * connection from the thread. Note we clear TAP / UDP status first, so
         * conn_return_buffers() will actually free the buffers.
         */
        mcbpc->setTapIterator(nullptr);
        mcbpc->setDCP(false);
    }
    conn_return_buffers(c);
    if (mcbpc != nullptr) {
        mcbpc->clearDynamicBuffer();
    }
    c->setEngineStorage(nullptr);

    c->setThread(nullptr);
    cb_assert(c->getNext() == nullptr);
    c->setSocketDescriptor(INVALID_SOCKET);
    if (mcbpc != nullptr) {
        mcbpc->setStart(0);
        mcbpc->disableSSL();
    }
}

void conn_close(McbpConnection *c) {
    if (c == nullptr) {
        throw std::invalid_argument("conn_close: 'c' must be non-NULL");
    }
    if (!c->isSocketClosed()) {
        throw std::logic_error("conn_cleanup: socketDescriptor must be closed");
    }
    if (c->getState() != conn_immediate_close) {
        throw std::logic_error("conn_cleanup: Connection:state (which is " +
                               std::string(c->getStateName()) +
                               ") must be conn_immediate_close");
    }

    auto thread = c->getThread();
    if (thread == nullptr) {
        throw std::logic_error("conn_close: unable to obtain non-NULL thread from connection");
    }
    /* remove from pending-io list */
    if (settings.getVerbose() > 1 && list_contains(thread->pending_io, c)) {
        LOG_WARNING(c,
                    "Current connection was in the pending-io list.. Nuking it");
    }
    thread->pending_io = list_remove(thread->pending_io, c);

    conn_cleanup(c);

    if (c->getThread() != nullptr) {
        throw std::logic_error("conn_close: failed to disassociate connection from thread");
    }
    c->setState(conn_destroyed);
}

struct listening_port *get_listening_port_instance(const in_port_t port) {
    for (auto &instance : stats.listening_ports) {
        if (instance.port == port) {
            return &instance;
        }
    }

    return nullptr;
}

void connection_stats(ADD_STAT add_stats, const void* cookie, const int64_t fd) {
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto *c : connections.conns) {
        if (c->getSocketDescriptor() == fd || fd == -1) {
            cJSON* stats = c->toJSON();
            // no key, JSON value contains all properties of the connection.
            char *stats_str = cJSON_PrintUnformatted(stats);
            add_stats(nullptr, 0, stats_str, (uint32_t)strlen(stats_str),
                      cookie);
            cJSON_Free(stats_str);
            cJSON_Delete(stats);
        }
    }
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
            cJSON* json = c->toJSON();
            char* info = cJSON_PrintUnformatted(json);
            LOG_NOTICE(c, "Connection: %s", info);
            cJSON_Free(info);
            cJSON_Delete(json);
        } catch (const std::bad_alloc&) {
            LOG_NOTICE(c, "Failed to allocate memory to dump info for %u",
                       c->getId());
        }
    }
}
#endif


void conn_loan_buffers(Connection *connection) {
    auto *c = dynamic_cast<McbpConnection*>(connection);
    if (c == nullptr) {
        return;
    }

    auto res = conn_loan_single_buffer(c, &c->getThread()->read, &c->read);
    auto *ts = get_thread_stats(c);
    if (res == BufferLoan::Allocated) {
        ts->rbufs_allocated++;
    } else if (res == BufferLoan::Loaned) {
        ts->rbufs_loaned++;
    } else if (res == BufferLoan::Existing) {
        ts->rbufs_existing++;
    }

    res = conn_loan_single_buffer(c, &c->getThread()->write, &c->write);
    if (res == BufferLoan::Allocated) {
        ts->wbufs_allocated++;
    } else if (res == BufferLoan::Loaned) {
        ts->wbufs_loaned++;
    }
}

void conn_return_buffers(Connection *connection) {
    auto *c = dynamic_cast<McbpConnection*>(connection);
    if (c == nullptr) {
        return;
    }

    auto thread = c->getThread();

    if (thread == nullptr) {
        // Connection already cleaned up - nothing to do.
        cb_assert(c->read.buf == NULL);
        cb_assert(c->write.buf == NULL);
        return;
    }

    if (c->isTAP() || c->isDCP()) {
        /* TAP & DCP work differently - let them keep their buffers once
         * allocated.
         */
        return;
    }

    conn_return_single_buffer(c, &thread->read, &c->read);
    conn_return_single_buffer(c, &thread->write, &c->write);
}

/** Internal functions *******************************************************/

/**
 * Destructor for all connection objects. Release all allocated resources.
 */
static void conn_destructor(Connection *c) {
    delete c;
    stats.conn_structs--;
}

/** Allocate a connection, creating memory and adding it to the conections
 *  list. Returns a pointer to the newly-allocated connection if successful,
 *  else NULL.
 */
static Connection *allocate_connection(SOCKET sfd,
                                       event_base *base,
                                       const struct listening_port &interface) {
    Connection *ret = nullptr;

    try {
        switch (interface.protocol) {
        case Protocol::Memcached:
            ret = new McbpConnection(sfd, base, interface);
            break;
        case Protocol::Greenstack:
            ret = new GreenstackConnection(sfd, base, interface);
        }

        std::lock_guard<std::mutex> lock(connections.mutex);
        connections.conns.push_back(ret);
        stats.conn_structs++;
        return ret;
    } catch (std::bad_alloc) {
        LOG_WARNING(NULL, "Failed to allocate memory for connection");
    } catch (std::exception& error) {
        LOG_WARNING(NULL, "Failed to create connection: %s", error.what());
    } catch (...) {
        LOG_WARNING(NULL, "Failed to create connection");
    }

    delete ret;
    return NULL;
}

static ListenConnection* allocate_listen_connection(SOCKET sfd,
                                                    event_base* base,
                                                    in_port_t port,
                                                    sa_family_t family,
                                                    const struct interface& interf) {
    ListenConnection *ret = nullptr;

    try {
        ret = new ListenConnection(sfd, base, port, family, interf);
        std::lock_guard<std::mutex> lock(connections.mutex);
        connections.conns.push_back(ret);
        stats.conn_structs++;
        return ret;
    } catch (std::bad_alloc) {
        LOG_WARNING(NULL, "Failed to allocate memory for listen connection");
    } catch (std::exception& error) {
        LOG_WARNING(NULL, "Failed to create connection: %s", error.what());
    } catch (...) {
        LOG_WARNING(NULL, "Failed to create connection");
    }

    delete ret;
    return NULL;
}


/**
 * Allocate a PipeConnection and add it to the conections list.
 *
 * Returns a pointer to the newly-allocated Connection else NULL if failed.
 */
static Connection *allocate_pipe_connection(int fd, event_base *base) {
    Connection *ret = nullptr;

    try {
        ret = new PipeConnection(static_cast<SOCKET>(fd), base);

        std::lock_guard<std::mutex> lock(connections.mutex);
        connections.conns.push_back(ret);
        stats.conn_structs++;
        return ret;
    } catch (std::bad_alloc) {
        LOG_WARNING(nullptr, "Failed to allocate memory for PipeConnection");
    } catch (std::exception& error) {
        LOG_WARNING(nullptr, "Failed to create connection: %s", error.what());
    } catch (...) {
        LOG_WARNING(nullptr, "Failed to create connection");
    }

    delete ret;
    return nullptr;
}

/** Release a connection; removing it from the connection list management
 *  and freeing the Connection object.
 */
static void release_connection(Connection *c) {
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
static BufferLoan conn_loan_single_buffer(McbpConnection *c, struct net_buf *thread_buf,
                                    struct net_buf *conn_buf)
{
    /* Already have a (partial) buffer - nothing to do. */
    if (conn_buf->buf != NULL) {
        return BufferLoan::Existing;
    }

    if (thread_buf->buf != NULL) {
        /* Loan thread's buffer to connection. */
        *conn_buf = *thread_buf;

        thread_buf->buf = NULL;
        thread_buf->size = 0;
        return BufferLoan::Loaned;
    } else {
        /* Need to allocate a new buffer. */
        conn_buf->buf = reinterpret_cast<char*>(malloc(DATA_BUFFER_SIZE));
        if (conn_buf->buf == NULL) {
            /* Unable to alloc a buffer for the thread. Not much we can do here
             * other than terminate the current connection.
             */
            if (settings.getVerbose()) {
                LOG_WARNING(c,
                            "%u: Failed to allocate new read buffer.. closing"
                                " connection",
                            c->getId());
            }
            c->setState(conn_closing);
            return BufferLoan::Existing;
        }
        conn_buf->size = DATA_BUFFER_SIZE;
        conn_buf->curr = conn_buf->buf;
        conn_buf->bytes = 0;
        return BufferLoan::Allocated;
    }
}

/**
 * Return an empty read buffer back to the owning worker thread.
 */
static void conn_return_single_buffer(Connection *c, struct net_buf *thread_buf,
                                      struct net_buf *conn_buf) {
    if (conn_buf->buf == NULL) {
        /* No buffer - nothing to do. */
        return;
    }

    if ((conn_buf->curr == conn_buf->buf) && (conn_buf->bytes == 0)) {
        /* Buffer clean, dispose of it. */
        if (thread_buf->buf == NULL) {
            /* Give back to thread. */
            *thread_buf = *conn_buf;
        } else {
            free(conn_buf->buf);
        }
        conn_buf->buf = conn_buf->curr = NULL;
        conn_buf->size = 0;
    } else {
        /* Partial data exists; leave the buffer with the connection. */
    }
}

ENGINE_ERROR_CODE apply_connection_trace_mask(const std::string& connid,
                                              const std::string& mask) {
    uint32_t id;
    try {
        id = static_cast<uint32_t>(std::stoi(connid));
    } catch (...) {
        return ENGINE_EINVAL;
    }

    bool enable = mask != "0";
    bool found = false;

    {
        // Lock the connection array to avoid race conditions with
        // connections being added / removed / destroyed
        std::unique_lock<std::mutex> lock(connections.mutex);
        for (auto* c : connections.conns) {
            if (c->getId() == id) {
                c->setTraceEnabled(enable);
                found = true;
            }
        }
    }

    if (found) {
        const char *message = enable ? "Enabled" : "Disabled";
        LOG_NOTICE(nullptr, "%s trace for %u", message, id);
        return ENGINE_SUCCESS;
    }

    return ENGINE_KEY_ENOENT;
}
