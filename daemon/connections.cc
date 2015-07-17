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
    cb_mutex_t mutex;
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

static cJSON* get_connection_stats(const Connection *c);


/** External functions *******************************************************/
void signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx)
{
    cb_mutex_enter(&connections.mutex);
    Connection *c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        if (c->thread == me && c->bucket.idx == bucket_idx) {
            if (c->state == conn_read || c->state == conn_waiting) {
                /* set write access to ensure it's handled */
                if (!c->updateEvent(EV_READ | EV_WRITE | EV_PERSIST)) {
                    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                    "Couldn't update event");
                }
            }
        }
        c = c->all_next;
    }

    cb_mutex_exit(&connections.mutex);
}

void assert_no_associations(int bucket_idx)
{
    cb_mutex_enter(&connections.mutex);
    Connection *c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        cb_assert(c->bucket.idx != bucket_idx);
        c = c->all_next;
    }

    cb_mutex_exit(&connections.mutex);
}

void initialize_connections(void)
{
    cb_mutex_initialize(&connections.mutex);
    connections.sentinal.all_next = &connections.sentinal;
    connections.sentinal.all_prev = &connections.sentinal;
}

void destroy_connections(void)
{
    /* traverse the list of connections. */
    Connection *c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        Connection *next = c->all_next;
        conn_destructor(c);
        c = next;
    }
    connections.sentinal.all_next = &connections.sentinal;
    connections.sentinal.all_prev = &connections.sentinal;
}

void close_all_connections(void)
{
    /* traverse the list of connections. */
    Connection *c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        Connection *next = c->all_next;

        if (c->sfd != INVALID_SOCKET) {
            safe_close(c->sfd);
            c->sfd = INVALID_SOCKET;
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
    c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        Connection *next = c->all_next;
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

    do {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                    "%d - Running task: (%s)\n", c->sfd, state_text(c->state));
        }
    } while (c->state(c));

    if (!is_listen_thread()) {
        conn_return_buffers(c);
    }

    if (c->state == conn_destroyed) {
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
                c->protocol = settings.interfaces[ii].protocol;
                c->nodelay = settings.interfaces[ii].tcp_nodelay;
                if (settings.interfaces[ii].ssl.cert != NULL) {
                    if (!c->ssl.enable(settings.interfaces[ii].ssl.cert,
                                       settings.interfaces[ii].ssl.key)) {
                        release_connection(c);
                        return NULL;
                    }

                    if (settings.verbose > 1) {
                        c->ssl.dumpCipherList(sfd);
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

    c->sfd = sfd;
    c->parent_port = parent_port;
    c->state = init_state;
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

    MEMCACHED_CONN_ALLOCATE(c->sfd);

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
    c->tap_iterator = NULL;
    c->dcp = 0;
    conn_return_buffers(c);
    free(c->dynamic_buffer.buffer);

    c->engine_storage = NULL;

    c->thread = NULL;
    cb_assert(c->next == NULL);
    c->sfd = INVALID_SOCKET;
    c->start = 0;
    c->ssl.disable();
}

void conn_close(Connection *c) {
    cb_assert(c != NULL);
    cb_assert(c->sfd == INVALID_SOCKET);
    cb_assert(c->state == conn_immediate_close);

    cb_assert(c->thread);
    /* remove from pending-io list */
    if (settings.verbose > 1 && list_contains(c->thread->pending_io, c)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Current connection was in the pending-io list.. Nuking it\n");
    }
    c->thread->pending_io = list_remove(c->thread->pending_io, c);

    conn_cleanup(c);

    cb_assert(c->thread == NULL);
    c->state = conn_destroyed;
}

void conn_shrink(Connection *c) {
    cb_assert(c != NULL);

    if (c->read.size > READ_BUFFER_HIGHWAT && c->read.bytes < DATA_BUFFER_SIZE) {
        if (c->read.curr != c->read.buf) {
            /* Pack the buffer */
            memmove(c->read.buf, c->read.curr, (size_t)c->read.bytes);
        }

        char* newbuf = reinterpret_cast<char*>(realloc(c->read.buf, DATA_BUFFER_SIZE));

        if (newbuf) {
            c->read.buf = newbuf;
            c->read.size = DATA_BUFFER_SIZE;
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to shrink read buffer down to %" PRIu64
                                            " bytes.", c->sfd, DATA_BUFFER_SIZE);
        }
        c->read.curr = c->read.buf;
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        auto *newbuf = reinterpret_cast<struct msghdr*>(realloc(c->msglist,
                                                                MSG_LIST_INITIAL * sizeof(c->msglist[0])));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to shrink msglist down to %" PRIu64
                                            " bytes.", c->sfd,
                                            MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        }
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        auto *newbuf = reinterpret_cast<struct iovec*>
            (realloc(c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0])));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to shrink iov down to %" PRIu64
                                            " bytes.", c->sfd,
                                            IOV_LIST_INITIAL * sizeof(c->iov[0]));
        }
    }

    // The dynamic_buffer is only occasionally used - free the whole thing
    // if it's still allocated.
    if (c->dynamic_buffer.buffer != nullptr) {
        free(c->dynamic_buffer.buffer);
        c->dynamic_buffer.buffer = nullptr;
        c->dynamic_buffer.size = 0;
    }
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
    const Connection *iter = NULL;
    cb_mutex_enter(&connections.mutex);
    for (iter = connections.sentinal.all_next;
         iter != &connections.sentinal;
         iter = iter->all_next) {
        if (iter->sfd == fd || fd == -1) {
            cJSON* stats = get_connection_stats(iter);
            /* blank key - JSON value contains all properties of the connection. */
            char key[] = " ";
            char *stats_str = cJSON_PrintUnformatted(stats);
            add_stats(key, (uint16_t)strlen(key),
                      stats_str, (uint32_t)strlen(stats_str), cookie);
            cJSON_Free(stats_str);
            cJSON_Delete(stats);
        }
    }
    cb_mutex_exit(&connections.mutex);
}

bool connection_set_nodelay(Connection *c, bool enable)
{
    int flags = 0;
    if (enable) {
        flags = 1;
    }

#if defined(WIN32)
    char* flags_ptr = reinterpret_cast<char*>(&flags);
#else
    void* flags_ptr = reinterpret_cast<void*>(&flags);
#endif
    int error = setsockopt(c->sfd, IPPROTO_TCP, TCP_NODELAY, flags_ptr,
                           sizeof(flags));

    if (error != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "setsockopt(TCP_NODELAY): %s",
                                        strerror(errno));
        c->nodelay = false;
        return false;
    } else {
        c->nodelay = enable;
    }

    return true;
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

    if (c->tap_iterator != NULL || c->dcp) {
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

    cb_mutex_enter(&connections.mutex);
    // First update the new nodes' links ...
    ret->all_next = connections.sentinal.all_next;
    ret->all_prev = &connections.sentinal;
    // ... then the existing nodes' links.
    connections.sentinal.all_next->all_prev = ret;
    connections.sentinal.all_next = ret;
    cb_mutex_exit(&connections.mutex);

    return ret;
}

/** Release a connection; removing it from the connection list management
 *  and freeing the Connection object.
 */
static void release_connection(Connection *c) {
    cb_mutex_enter(&connections.mutex);
    c->all_next->all_prev = c->all_prev;
    c->all_prev->all_next = c->all_next;
    cb_mutex_exit(&connections.mutex);

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
                    "%d: Failed to allocate new read buffer.. closing connection\n",
                    c->sfd);
            }
            conn_set_state(c, conn_closing);
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

static const char *substate_text(enum bin_substates state) {
    switch (state) {
    case bin_no_state: return "bin_no_state";
    case bin_reading_packet: return "bin_reading_packet";
    default:
        return "illegal";
    }
}

/* cJSON uses double for all numbers, so only has 53 bits of precision.
 * Therefore encode 64bit integers as string.
 */
static cJSON* json_create_uintptr(uintptr_t value) {
    char buffer[32];
    if (snprintf(buffer, sizeof(buffer),
                 "0x%" PRIxPTR, value) >= int(sizeof(buffer))) {
        return cJSON_CreateString("<too long>");
    } else {
        return cJSON_CreateString(buffer);
    }
}

static void json_add_uintptr_to_object(cJSON *obj, const char *name,
                                       uintptr_t value) {
    cJSON_AddItemToObject(obj, name, json_create_uintptr(value));
}

static void json_add_bool_to_object(cJSON *obj, const char *name, bool value) {
    if (value) {
        cJSON_AddTrueToObject(obj, name);
    } else {
        cJSON_AddFalseToObject(obj, name);
    }
}

/* Returns a JSON object with stat for the given connection.
 * Caller is responsible for freeing the result with cJSON_Delete().
 */
static cJSON* get_connection_stats(const Connection *c) {
    cJSON *obj = cJSON_CreateObject();
    json_add_uintptr_to_object(obj, "connection", (uintptr_t)c);
    if (c->sfd == INVALID_SOCKET) {
        cJSON_AddStringToObject(obj, "socket", "disconnected");
    } else {
        cJSON_AddNumberToObject(obj, "socket", (double)c->sfd);
        switch (c->protocol) {
        case Protocol::Memcached:
            cJSON_AddStringToObject(obj, "protocol", "memcached");
            break;
         case Protocol::Greenstack:
            cJSON_AddStringToObject(obj, "protocol", "greenstack");
            break;
        default:
            cJSON_AddStringToObject(obj, "protocol", "unknown");
        }
        cJSON_AddStringToObject(obj, "peername", c->getPeername().c_str());
        cJSON_AddStringToObject(obj, "sockname", c->getSockname().c_str());
        cJSON_AddNumberToObject(obj, "max_reqs_per_event",
                                c->max_reqs_per_event);
        cJSON_AddNumberToObject(obj, "nevents", c->nevents);
        json_add_bool_to_object(obj, "admin", c->isAdmin());
        if (c->sasl_conn != NULL) {
            json_add_uintptr_to_object(obj, "sasl_conn",
                                       (uintptr_t)c->sasl_conn);
        }
        {
            cJSON *state = cJSON_CreateArray();
            cJSON_AddItemToArray(state,
                                 cJSON_CreateString(state_text(c->state)));
            cJSON_AddItemToArray(state,
                                 cJSON_CreateString(substate_text(c->substate)));
            cJSON_AddItemToObject(obj, "state", state);
        }
        json_add_bool_to_object(obj, "registered_in_libevent",
                                c->isRegisteredInLibevent());
        json_add_uintptr_to_object(obj, "ev_flags", (uintptr_t)c->getEventFlags());
        json_add_uintptr_to_object(obj, "which", (uintptr_t)c->getCurrentEvent());
        {
            cJSON *read = cJSON_CreateObject();
            json_add_uintptr_to_object(read, "buf", (uintptr_t)c->read.buf);
            json_add_uintptr_to_object(read, "curr", (uintptr_t)c->read.curr);
            cJSON_AddNumberToObject(read, "size", c->read.size);
            cJSON_AddNumberToObject(read, "bytes", c->read.bytes);

            cJSON_AddItemToObject(obj, "read", read);
        }
        {
            cJSON *write = cJSON_CreateObject();
            json_add_uintptr_to_object(write, "buf", (uintptr_t)c->write.buf);
            json_add_uintptr_to_object(write, "curr", (uintptr_t)c->write.curr);
            cJSON_AddNumberToObject(write, "size", c->write.size);
            cJSON_AddNumberToObject(write, "bytes", c->write.bytes);

            cJSON_AddItemToObject(obj, "write", write);
        }
        json_add_uintptr_to_object(obj, "write_and_go",
                                   (uintptr_t)c->write_and_go);
        json_add_uintptr_to_object(obj, "write_and_free",
                                   (uintptr_t)c->write_and_free);
        json_add_uintptr_to_object(obj, "ritem", (uintptr_t)c->ritem);
        cJSON_AddNumberToObject(obj, "rlbytes", c->rlbytes);
        json_add_uintptr_to_object(obj, "item", (uintptr_t)c->item);
        {
            cJSON *iov = cJSON_CreateObject();
            json_add_uintptr_to_object(iov, "ptr", (uintptr_t)c->iov);
            cJSON_AddNumberToObject(iov, "size", c->iovsize);
            cJSON_AddNumberToObject(iov, "used", c->iovused);

            cJSON_AddItemToObject(obj, "iov", iov);
        }
        {
            cJSON *msg = cJSON_CreateObject();
            json_add_uintptr_to_object(msg, "list", (uintptr_t)c->msglist);
            cJSON_AddNumberToObject(msg, "size", c->msgsize);
            cJSON_AddNumberToObject(msg, "used", c->msgused);
            cJSON_AddNumberToObject(msg, "curr", c->msgcurr);
            cJSON_AddNumberToObject(msg, "bytes", c->msgbytes);

            cJSON_AddItemToObject(obj, "msglist", msg);
        }
        {
            cJSON *ilist = cJSON_CreateObject();
            cJSON_AddNumberToObject(ilist, "size", c->reservedItems.size());
            cJSON_AddItemToObject(obj, "itemlist", ilist);
        }
        {
            cJSON *talloc = cJSON_CreateObject();
            cJSON_AddNumberToObject(talloc, "size", c->temp_alloc.size());
            cJSON_AddItemToObject(obj, "temp_alloc_list", talloc);
        }
        json_add_bool_to_object(obj, "noreply", c->noreply);
        json_add_bool_to_object(obj, "nodelay", c->nodelay);
        cJSON_AddNumberToObject(obj, "refcount", c->refcount);
        {
            cJSON *features = cJSON_CreateObject();
            json_add_bool_to_object(features, "datatype",
                                    c->supports_datatype);
            json_add_bool_to_object(features, "mutation_extras",
                                    c->supports_mutation_extras);

            cJSON_AddItemToObject(obj, "features", features);
        }
        {
            cJSON* dy_buf = cJSON_CreateObject();
            json_add_uintptr_to_object(dy_buf, "buffer",
                                       (uintptr_t)c->dynamic_buffer.buffer);
            cJSON_AddNumberToObject(dy_buf, "size", (double)c->dynamic_buffer.size);
            cJSON_AddNumberToObject(dy_buf, "offset", (double)c->dynamic_buffer.offset);

            cJSON_AddItemToObject(obj, "dynamic_buffer", dy_buf);
        }
        json_add_uintptr_to_object(obj, "engine_storage",
                                   (uintptr_t)c->engine_storage);
        /* @todo we should decode the binary header */
        json_add_uintptr_to_object(obj, "cas", c->cas);
        {
            cJSON *cmd = cJSON_CreateArray();
            cJSON_AddItemToArray(cmd, json_create_uintptr(c->cmd));
            const char* cmd_name = memcached_opcode_2_text(c->cmd);
            if (cmd_name == NULL) {
                cmd_name = "";
            }
            cJSON_AddItemToArray(cmd, cJSON_CreateString(cmd_name));

            cJSON_AddItemToObject(obj, "cmd", cmd);
        }
        json_add_uintptr_to_object(obj, "opaque", c->opaque);
        cJSON_AddNumberToObject(obj, "keylen", c->keylen);
        cJSON_AddNumberToObject(obj, "list_state", c->list_state);
        json_add_uintptr_to_object(obj, "next", (uintptr_t)c->next);
        json_add_uintptr_to_object(obj, "thread", (uintptr_t)c->thread);
        cJSON_AddNumberToObject(obj, "aiostat", c->aiostat);
        json_add_bool_to_object(obj, "ewouldblock", c->ewouldblock);
        json_add_uintptr_to_object(obj, "tap_iterator",
                                   (uintptr_t)c->tap_iterator);
        cJSON_AddNumberToObject(obj, "parent_port", c->parent_port);
        json_add_bool_to_object(obj, "dcp", c->dcp);

        {
            cJSON* ssl = cJSON_CreateObject();
            json_add_bool_to_object(ssl, "enabled", c->ssl.isEnabled());
            if (c->ssl.isEnabled()) {
                json_add_bool_to_object(ssl, "connected", c->ssl.isConnected());
            }

            cJSON_AddItemToObject(obj, "ssl", ssl);
        }
    }
    return obj;
}
