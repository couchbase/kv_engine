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

static void conn_loan_buffers(Connection *c);
static void conn_return_buffers(Connection *c);
static BufferLoan conn_loan_single_buffer(Connection *c, struct net_buf *thread_buf,
                                             struct net_buf *conn_buf);
static void conn_return_single_buffer(Connection *c, struct net_buf *thread_buf,
                                      struct net_buf *conn_buf);
static void conn_destructor(Connection *c);
static Connection *allocate_connection(SOCKET sfd, const struct listening_port *interface = nullptr);
static void release_connection(Connection *c);

/** External functions *******************************************************/
int signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx, bool logging)
{
    int connected = 0;
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto* c : connections.conns) {
        if (c->getThread() == me) {
            ++connected;
            if (bucket_idx == -1 || c->getBucketIndex() == bucket_idx) {
                auto state = c->getState();
                if (state == conn_read || state == conn_waiting ||
                    state == conn_new_cmd) {
                    /*
                     * set write access to ensure it's handled (error logged in
                     * updateEvent().
                     */
                    c->updateEvent(EV_READ | EV_WRITE | EV_PERSIST);
                } else if (logging) {
                    auto* js = c->toJSON();
                    char* details = cJSON_PrintUnformatted(js);

                    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                                    "Worker thread %u: %s",
                                                    me->index, details);
                    cJSON_Free(details);
                    cJSON_Delete(js);
                }
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

void run_event_loop(Connection * c) {

    if (!is_listen_thread()) {
        conn_loan_buffers(c);
    }

    try {
        c->runStateMachinery();
    } catch (std::invalid_argument& e) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: exception occurred in runloop "
                                        "- closing connection: %s",
                                        c->getId(), e.what());
        c->setState(conn_closing);
    }

    if (!is_listen_thread()) {
        conn_return_buffers(c);
    }

    if (c->getState() == conn_destroyed) {
        /* Actually free the memory from this connection. Unsafe to dereference
         * c after this point.
         */
        release_connection(c);
        c = NULL;
    }
}

Connection* conn_new_server(const SOCKET sfd,
                            in_port_t parent_port,
                            struct event_base* base) {
    Connection* c = allocate_connection(sfd);
    if (c == nullptr) {
        return nullptr;
    }

    c->resolveConnectionName(true);
    c->setAuthContext(auth_create(NULL, NULL, NULL));
    c->setParentPort(parent_port);
    c->setState(conn_listening);
    c->setWriteAndGo(conn_listening);

    // Listen connections should not be associated with a bucket
    c->setBucketEngine(nullptr);
    c->setBucketIndex(-1);

    if (!c->initializeEvent(base)) {
        cb_assert(c->getThread() == nullptr);
        release_connection(c);
        return nullptr;
    }

    stats.total_conns++;

    c->incrementRefcount();


    MEMCACHED_CONN_ALLOCATE(c->getId());

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "<%d server listening", sfd);
    }

    return c;
}


Connection* conn_new(const SOCKET sfd, in_port_t parent_port,
                     struct event_base* base,
                     LIBEVENT_THREAD* thread) {

    Connection *c = nullptr;

    for (auto& interface : stats.listening_ports) {
        if (parent_port == interface.port) {
            c = allocate_connection(sfd, &interface);
            if (c == nullptr) {
                return nullptr;
            }

            settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                                            "%u: Using protocol: %s",
                                            c->getId(),
                                            to_string(c->getProtocol()));
        }
    }

    if (c == nullptr) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "%u: failed to locate server port "
                                            "%u. Disconnecting",
                                        (unsigned int)sfd, parent_port);
        return nullptr;
    }

    if (!c->initializeEvent(base)) {
        cb_assert(c->getThread() == nullptr);
        release_connection(c);
        return nullptr;
    }

    stats.total_conns++;

    c->incrementRefcount();

    associate_initial_bucket(c);

    c->setThread(thread);
    MEMCACHED_CONN_ALLOCATE(c->getId());

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "<%d new client connection", sfd);
    }

    return c;
}

void conn_cleanup_engine_allocations(Connection * c) {
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

    c->releaseTempAlloc();

    c->read.curr = c->read.buf;
    c->read.bytes = 0;
    c->write.curr = c->write.buf;
    c->write.bytes = 0;

    /* Return any buffers back to the thread; before we disassociate the
     * connection from the thread. Note we clear TAP / UDP status first, so
     * conn_return_buffers() will actually free the buffers.
     */
    c->setTapIterator(nullptr);
    c->setDCP(false);
    conn_return_buffers(c);
    c->clearDynamicBuffer();
    c->setEngineStorage(nullptr);

    c->setThread(nullptr);
    cb_assert(c->getNext() == nullptr);
    c->setSocketDescriptor(INVALID_SOCKET);
    c->setStart(0);
    c->disableSSL();
}

void conn_close(Connection *c) {
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
        std::logic_error("conn_close: unable to obtain non-NULL thread from connection");
    }
    /* remove from pending-io list */
    if (settings.verbose > 1 && list_contains(thread->pending_io, c)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Current connection was in the pending-io list.. Nuking it\n");
    }
    thread->pending_io = list_remove(thread->pending_io, c);

    conn_cleanup(c);

    if (c->getThread() != nullptr) {
        std::logic_error("conn_close: failed to disassociate connection from thread");
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

void connection_stats(ADD_STAT add_stats, Connection *cookie, const int64_t fd) {
    std::lock_guard<std::mutex> lock(connections.mutex);
    for (auto *c : connections.conns) {
        if (c->getSocketDescriptor() == fd || fd == -1) {
            cJSON* stats = c->toJSON();
            /* blank key - JSON value contains all properties of the connection. */
            char key[] = " ";
            char *stats_str = cJSON_PrintUnformatted(stats);
            add_stats(key, (uint16_t)strlen(key),
                      stats_str, (uint32_t)strlen(stats_str), cookie);
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
            settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                            "Connection: %s", info);
            cJSON_Free(info);
            cJSON_Delete(json);
        } catch (const std::bad_alloc&) {
            settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                            "Failed to allocate memory to dump info for %u",
                                            c->getId());
        }
    }
}
#endif

/** Internal functions *******************************************************/

/**
 * If the connection doesn't already have read/write buffers, ensure that it
 * does.
 *
 * In the common case, only one read/write buffer is created per worker thread,
 * and this buffer is loaned to the connection the worker is currently
 * handling. As long as the connection doesn't have a partial read/write (i.e.
 * the buffer is totally consumed) when it goes idle, the buffer is simply
 * returned back to the worker thread.
 *
 * If there is a partial read/write, then the buffer is left loaned to that
 * connection and the worker thread will allocate a new one.
 */
static void conn_loan_buffers(Connection *c) {

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

/**
 * Return any empty buffers back to the owning worker thread.
 *
 * Converse of conn_loan_buffer(); if any of the read/write buffers are empty
 * (have no partial data) then return the buffer back to the worker thread.
 * If there is partial data, then keep the buffer with the connection.
 */
static void conn_return_buffers(Connection *c) {
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
                                       const struct listening_port* interface) {
    Connection *ret = nullptr;

    try {
        if (interface == nullptr) {
            ret = new Connection;
        } else {
            ret = new Connection(sfd, *interface);
        }
        ret->setSocketDescriptor(sfd);
        stats.conn_structs++;

        std::lock_guard<std::mutex> lock(connections.mutex);
        connections.conns.push_back(ret);
        return ret;
    } catch (std::bad_alloc) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connection");
        delete ret;
        return NULL;
    }
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
static BufferLoan conn_loan_single_buffer(Connection *c, struct net_buf *thread_buf,
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
            if (settings.verbose) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                    "%u: Failed to allocate new read buffer.. closing connection",
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
