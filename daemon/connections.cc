/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include <cJSON.h>

/*
 * Free list management for connections.
 */
struct connections {
    Connection sentinal; /* Sentinal Connection object used as the base of the linked-list
                      of connections. */
    std::mutex mutex;
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
static Connection *allocate_connection(void);
static void release_connection(Connection *c);

/** External functions *******************************************************/
void signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx)
{
    std::lock_guard<std::mutex> lock(connections.mutex);
    Connection *c = connections.sentinal.getAllNext();
    while (c != &connections.sentinal) {
        if (c->thread == me && c->bucket.idx == bucket_idx) {
            if (c->getState() == conn_read || c->getState() == conn_waiting) {
                /* set write access to ensure it's handled */
                if (!c->updateEvent(EV_READ | EV_WRITE | EV_PERSIST)) {
                    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                    "Couldn't update event");
                }
            }
        }
        c = c->getAllNext();
    }
}

void assert_no_associations(int bucket_idx)
{
    std::lock_guard<std::mutex> lock(connections.mutex);
    Connection *c = connections.sentinal.getAllNext();
    while (c != &connections.sentinal) {
        cb_assert(c->bucket.idx != bucket_idx);
        c = c->getAllNext();
    }
}

void destroy_connections(void)
{
    /* traverse the list of connections. */
    Connection *c = connections.sentinal.getAllNext();
    while (c != &connections.sentinal) {
        Connection *next = c->getAllNext();
        conn_destructor(c);
        c = next;
    }
    connections.sentinal.setAllNext(&connections.sentinal);
    connections.sentinal.setAllPrev(&connections.sentinal);
}

void close_all_connections(void)
{
    /* traverse the list of connections. */
    Connection *c = connections.sentinal.getAllNext();
    while (c != &connections.sentinal) {
        Connection *next = c->getAllNext();

        if (!c->isSocketClosed()) {
            safe_close(c->getSocketDescriptor());
            c->setSocketDescriptor(INVALID_SOCKET);
        }

        if (c->refcount > 1) {
            perform_callbacks(ON_DISCONNECT, NULL, c);
        }
        c = next;
    }

    /*
     * do a second loop, this time wait for all of them to
     * be closed.
     */
    c = connections.sentinal.getAllNext();
    while (c != &connections.sentinal) {
        Connection *next = c->getAllNext();
        while (c->refcount > 1) {
            usleep(500);
        }
        c = next;
    }
}

void run_event_loop(Connection * c) {

    if (!is_listen_thread()) {
        conn_loan_buffers(c);
    }

    c->runStateMachinery();

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

Connection *conn_new(const SOCKET sfd, in_port_t parent_port,
               STATE_FUNC init_state,
               struct event_base *base) {
    Connection *c = allocate_connection();
    if (c == NULL) {
        return NULL;
    }

    if (init_state == conn_listening) {
        c->auth_context = auth_create(NULL, NULL, NULL);
    } else {
        c->resolveConnectionName();
        c->auth_context = auth_create(NULL, c->getPeername().c_str(),
                                      c->getSockname().c_str());

        for (int ii = 0; ii < settings.num_interfaces; ++ii) {
            if (parent_port == settings.interfaces[ii].port) {
                c->setProtocol(settings.interfaces[ii].protocol);
                c->setTcpNoDelay(settings.interfaces[ii].tcp_nodelay);
                if (settings.interfaces[ii].ssl.cert != NULL) {
                    if (!c->enableSSL(settings.interfaces[ii].ssl.cert,
                                      settings.interfaces[ii].ssl.key)) {
                        release_connection(c);
                        return NULL;
                    }
                }
            }
        }
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d server listening", sfd);
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d new client connection", sfd);
        }
    }

    c->setSocketDescriptor(sfd);
    c->setParentPort(parent_port);
    c->setState(init_state);
    c->write_and_go = init_state;

    if (!c->initializeEvent(base)) {
        cb_assert(c->thread == NULL);
        release_connection(c);
        return NULL;
    }

    stats.total_conns++;

    c->refcount = 1;

    if (init_state == conn_listening) {
        c->bucket.engine = NULL;
        c->bucket.idx = -1;
    } else {
        associate_initial_bucket(c);
    }

    MEMCACHED_CONN_ALLOCATE(c->getId());

    if (init_state != conn_listening) {
        perform_callbacks(ON_CONNECT, NULL, c);
    }

    return c;
}

void conn_cleanup_engine_allocations(Connection * c) {
    ENGINE_HANDLE* handle = reinterpret_cast<ENGINE_HANDLE*>(c->bucket.engine);
    if (c->item) {
        c->bucket.engine->release(handle, c, c->item);
        c->item = NULL;
    }

    for (auto *it : c->reservedItems) {
        c->bucket.engine->release(handle, c, it);
    }
    c->reservedItems.clear();
}

static void conn_cleanup(Connection *c) {
    cb_assert(c != NULL);
    c->setAdmin(false);

    for (auto *ptr : c->temp_alloc) {
        free(ptr);
    }
    c->temp_alloc.resize(0);

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }

    if (c->sasl_conn) {
        cbsasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

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
    free(c->dynamic_buffer.buffer);

    c->engine_storage = NULL;

    c->thread = NULL;
    cb_assert(c->next == NULL);
    c->setSocketDescriptor(INVALID_SOCKET);
    c->start = 0;
    c->disableSSL();
}

void conn_close(Connection *c) {
    cb_assert(c != NULL);
    cb_assert(c->isSocketClosed());
    cb_assert(c->getState() == conn_immediate_close);

    cb_assert(c->thread);
    /* remove from pending-io list */
    if (settings.verbose > 1 && list_contains(c->thread->pending_io, c)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Current connection was in the pending-io list.. Nuking it\n");
    }
    c->thread->pending_io = list_remove(c->thread->pending_io, c);

    conn_cleanup(c);

    cb_assert(c->thread == NULL);
    c->setState(conn_destroyed);
}

bool grow_dynamic_buffer(Connection *c, size_t needed) {
    size_t nsize = c->dynamic_buffer.size;
    size_t available = nsize - c->dynamic_buffer.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->dynamic_buffer.buffer == NULL) {
        nsize = 1024;
        available = c->dynamic_buffer.size = c->dynamic_buffer.offset = 0;
    }

    while (needed > available) {
        cb_assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->dynamic_buffer.offset;
    }

    if (nsize != c->dynamic_buffer.size) {
        char *ptr = reinterpret_cast<char*>(realloc(c->dynamic_buffer.buffer, nsize));
        if (ptr) {
            c->dynamic_buffer.buffer = ptr;
            c->dynamic_buffer.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

struct listening_port *get_listening_port_instance(const in_port_t port) {
    struct listening_port *port_ins = NULL;
    int ii;
    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        if (stats.listening_ports[ii].port == port) {
            port_ins = &stats.listening_ports[ii];
        }
    }
    return port_ins;

}

void connection_stats(ADD_STAT add_stats, Connection *cookie, const int64_t fd) {
    std::lock_guard<std::mutex> lock(connections.mutex);
    const Connection *iter;
    for (iter = connections.sentinal.getAllNext();
         iter != &connections.sentinal;
         iter = iter->getAllNext()) {
        if (iter->getSocketDescriptor() == fd || fd == -1) {
            cJSON* stats = iter->toJSON();
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

    auto res = conn_loan_single_buffer(c, &c->thread->read, &c->read);
    auto *ts = get_thread_stats(c);
    if (res == BufferLoan::Allocated) {
        ts->rbufs_allocated++;
    } else if (res == BufferLoan::Loaned) {
        ts->rbufs_loaned++;
    } else if (res == BufferLoan::Existing) {
        ts->rbufs_existing++;
    }

    res = conn_loan_single_buffer(c, &c->thread->write, &c->write);
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
    if (c->thread == NULL) {
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

    conn_return_single_buffer(c, &c->thread->read, &c->read);
    conn_return_single_buffer(c, &c->thread->write, &c->write);
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
static Connection *allocate_connection(void) {
    Connection *ret;

    try {
        ret = new Connection;
    } catch (std::bad_alloc) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connection");
        return NULL;
    }
    stats.conn_structs++;

    {
        std::lock_guard<std::mutex> lock(connections.mutex);
        // First update the new nodes' links ...
        ret->setAllNext(connections.sentinal.getAllNext());
        ret->setAllPrev(&connections.sentinal);
        // ... then the existing nodes' links.
        connections.sentinal.getAllNext()->setAllPrev(ret);
        connections.sentinal.setAllNext(ret);
    }

    return ret;
}

/** Release a connection; removing it from the connection list management
 *  and freeing the Connection object.
 */
static void release_connection(Connection *c) {
    {
        std::lock_guard<std::mutex> lock(connections.mutex);
        c->getAllNext()->setAllPrev(c->getAllPrev());
        c->getAllPrev()->setAllNext(c->getAllNext());
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
