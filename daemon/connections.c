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

#include <cJSON.h>

/*
 * Free list management for connections.
 */
struct connections {
    conn sentinal; /* Sentinal conn object used as the base of the linked-list
                      of connections. */
    cb_mutex_t mutex;
} connections;

/** Types ********************************************************************/

/** Result of a buffer loan attempt */
enum loan_res {
    loan_existing,
    loan_loaned,
    loan_allocated,
};

/** Function prototypes ******************************************************/

static void conn_loan_buffers(conn *c);
static void conn_return_buffers(conn *c);
static bool conn_reset_buffersize(conn *c);
static enum loan_res conn_loan_single_buffer(conn *c, struct net_buf *thread_buf,
                                             struct net_buf *conn_buf);
static void conn_return_single_buffer(conn *c, struct net_buf *thread_buf,
                                      struct net_buf *conn_buf);
static int conn_constructor(conn *c);
static void conn_destructor(conn *c);
static conn *allocate_connection(void);
static void release_connection(conn *c);

static cJSON* get_connection_stats(const conn *c);


/** External functions *******************************************************/

void initialize_connections(void)
{
    cb_mutex_initialize(&connections.mutex);
    connections.sentinal.all_next = &connections.sentinal;
    connections.sentinal.all_prev = &connections.sentinal;
}

void destroy_connections(void)
{
    /* traverse the list of connections. */
    conn *c = connections.sentinal.all_next;
    while (c != &connections.sentinal) {
        conn *next = c->all_next;
        conn_destructor(c);
        c = next;
    }
    connections.sentinal.all_next = &connections.sentinal;
    connections.sentinal.all_prev = &connections.sentinal;
}

void run_event_loop(conn* c) {

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

conn *conn_new(const SOCKET sfd, in_port_t parent_port,
               STATE_FUNC init_state, int event_flags,
               unsigned int read_buffer_size, struct event_base *base,
               struct timeval *timeout) {
    conn *c = allocate_connection();
    if (c == NULL) {
        return NULL;
    }

    c->admin = false;
    cb_assert(c->thread == NULL);

    memset(&c->ssl, 0, sizeof(c->ssl));
    if (init_state != conn_listening) {
        int ii;
        for (ii = 0; ii < settings.num_interfaces; ++ii) {
            if (parent_port == settings.interfaces[ii].port) {
                if (settings.interfaces[ii].ssl.cert != NULL) {
                    const char *cert = settings.interfaces[ii].ssl.cert;
                    const char *pkey = settings.interfaces[ii].ssl.key;

                    c->ssl.ctx = SSL_CTX_new(SSLv23_server_method());

                    /* @todo don't read files, but use in-memory-copies */
                    if (!SSL_CTX_use_certificate_chain_file(c->ssl.ctx, cert) ||
                        !SSL_CTX_use_PrivateKey_file(c->ssl.ctx, pkey, SSL_FILETYPE_PEM)) {
                        release_connection(c);
                        return NULL;
                    }

                    c->ssl.enabled = true;
                    c->ssl.error = false;
                    c->ssl.client = NULL;

                    c->ssl.in.buffer = malloc(settings.bio_drain_buffer_sz);
                    c->ssl.out.buffer = malloc(settings.bio_drain_buffer_sz);

                    if (c->ssl.in.buffer == NULL || c->ssl.out.buffer == NULL) {
                        release_connection(c);
                        return NULL;
                    }

                    c->ssl.in.buffsz = settings.bio_drain_buffer_sz;
                    c->ssl.out.buffsz = settings.bio_drain_buffer_sz;
                    BIO_new_bio_pair(&c->ssl.application,
                                     settings.bio_drain_buffer_sz,
                                     &c->ssl.network,
                                     settings.bio_drain_buffer_sz);

                    c->ssl.client = SSL_new(c->ssl.ctx);
                    SSL_set_bio(c->ssl.client,
                                c->ssl.application,
                                c->ssl.application);
                }
            }
        }
    }

    c->request_addr_size = 0;

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
    c->max_reqs_per_event = settings.default_reqs_per_event;
    c->parent_port = parent_port;
    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->read.bytes = c->write.bytes = 0;
    c->write.curr = c->write.buf = NULL;
    c->read.curr = c->read.buf = NULL;
    c->read.size = c->write.size = 0;
    c->ritem = 0;
    c->icurr = c->ilist = NULL;
    c->temp_alloc_curr = c->temp_alloc_list;
    c->ileft = 0;
    c->temp_alloc_left = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->next = NULL;
    c->list_state = 0;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;
    c->supports_datatype = false;
    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (!register_event(c, timeout)) {
        cb_assert(c->thread == NULL);
        release_connection(c);
        return NULL;
    }

    STATS_LOCK();
    stats.total_conns++;
    STATS_UNLOCK();

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;
    c->refcount = 1;

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    perform_callbacks(ON_CONNECT, NULL, c);

    return c;
}

static void conn_cleanup(conn *c) {
    cb_assert(c != NULL);
    c->admin = false;
    if (c->item) {
        settings.engine.v1->release(settings.engine.v0, c, c->item);
        c->item = 0;
    }

    if (c->ileft != 0) {
        for (; c->ileft > 0; c->ileft--,c->icurr++) {
            settings.engine.v1->release(settings.engine.v0, c, *(c->icurr));
        }
    }

    if (c->temp_alloc_left != 0) {
        for (; c->temp_alloc_left > 0; c->temp_alloc_left--, c->temp_alloc_curr++) {
            free(*(c->temp_alloc_curr));
        }
    }

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

    c->engine_storage = NULL;

    c->thread = NULL;
    cb_assert(c->next == NULL);
    c->sfd = INVALID_SOCKET;
    c->start = 0;
    if (c->ssl.enabled) {
        BIO_free_all(c->ssl.network);
        SSL_free(c->ssl.client);
        c->ssl.enabled = false;
        c->ssl.error = false;
        free(c->ssl.in.buffer);
        free(c->ssl.out.buffer);
        memset(&c->ssl, 0, sizeof(c->ssl));
    }
}

void conn_close(conn *c) {
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

void conn_shrink(conn *c) {
    cb_assert(c != NULL);

    if (c->read.size > READ_BUFFER_HIGHWAT && c->read.bytes < DATA_BUFFER_SIZE) {
        void *newbuf;

        if (c->read.curr != c->read.buf) {
            /* Pack the buffer */
            memmove(c->read.buf, c->read.curr, (size_t)c->read.bytes);
        }

        newbuf = realloc(c->read.buf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->read.buf = newbuf;
            c->read.size = DATA_BUFFER_SIZE;
        }
        c->read.curr = c->read.buf;
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        void *newbuf = realloc(c->msglist,
                               MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        void *newbuf = realloc(c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
    }
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

void connection_stats(ADD_STAT add_stats, conn *cookie, const int64_t fd) {
    const conn *iter = NULL;
    cb_mutex_enter(&connections.mutex);
    for (iter = connections.sentinal.all_next;
         iter != &connections.sentinal;
         iter = iter->all_next) {
        if (iter->sfd == fd || fd == -1) {
            cJSON* stats = get_connection_stats(iter);
            char key[32];
            int res = snprintf(key, sizeof(key), "conn-%"PRId64,
                               (int64_t)iter->sfd);
            if (res < sizeof(key)) {
                char *stats_str = cJSON_PrintUnformatted(stats);
                add_stats(key, strlen(key), stats_str, strlen(stats_str), cookie);
                cJSON_Free(stats_str);
            }
            cJSON_Delete(stats);
        }
    }
    cb_mutex_exit(&connections.mutex);
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
static void conn_loan_buffers(conn *c) {
    enum loan_res res;
    res = conn_loan_single_buffer(c, &c->thread->read, &c->read);
    if (res == loan_allocated) {
        STATS_NOKEY(c, rbufs_allocated);
    } else if (res == loan_loaned) {
        STATS_NOKEY(c, rbufs_loaned);
    } else if (res == loan_existing) {
        STATS_NOKEY(c, rbufs_existing);
    }

    res = conn_loan_single_buffer(c, &c->thread->write, &c->write);
    if (res == loan_allocated) {
        STATS_NOKEY(c, wbufs_allocated);
    } else if (res == loan_loaned) {
        STATS_NOKEY(c, wbufs_loaned);
    }
}

/**
 * Return any empty buffers back to the owning worker thread.
 *
 * Converse of conn_loan_buffer(); if any of the read/write buffers are empty
 * (have no partial data) then return the buffer back to the worker thread.
 * If there is partial data, then keep the buffer with the connection.
 */
static void conn_return_buffers(conn *c) {
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
 * Reset all of the dynamic buffers used by a connection back to their
 * default sizes. The strategy for resizing the buffers is to allocate a
 * new one of the correct size and free the old one if the allocation succeeds
 * instead of using realloc to change the buffer size (because realloc may
 * not shrink the buffers, and will also copy the memory). If the allocation
 * fails the buffer will be unchanged.
 *
 * @param c the connection to resize the buffers for
 * @return true if all allocations succeeded, false if one or more of the
 *         allocations failed.
 */
static bool conn_reset_buffersize(conn *c) {
    bool ret = true;

    /* itemlist only needed for TAP / DCP connections, so we just free when the
     * connection is reset.
     */
    free(c->ilist);
    c->ilist = NULL;
    c->isize = 0;

    if (c->temp_alloc_size != TEMP_ALLOC_LIST_INITIAL) {
        void *ptr = malloc(sizeof(char *) * TEMP_ALLOC_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->temp_alloc_list);
            c->temp_alloc_list = ptr;
            c->temp_alloc_size = TEMP_ALLOC_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->iovsize != IOV_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct iovec) * IOV_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->iov);
            c->iov = ptr;
            c->iovsize = IOV_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->msgsize != MSG_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct msghdr) * MSG_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->msglist);
            c->msglist = ptr;
            c->msgsize = MSG_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    return ret;
}

/**
 * Constructor for all memory allocations of connection objects. Initialize
 * all members and allocate the transfer buffers.
 *
 * @param buffer The memory allocated by the object cache
 * @return 0 on success, 1 if we failed to allocate memory
 */
static int conn_constructor(conn *c) {
    memset(c, 0, sizeof(*c));
    MEMCACHED_CONN_CREATE(c);

    c->state = conn_immediate_close;
    c->sfd = INVALID_SOCKET;
    if (!conn_reset_buffersize(c)) {
        free(c->read.buf);
        free(c->write.buf);
        free(c->ilist);
        free(c->temp_alloc_list);
        free(c->iov);
        free(c->msglist);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING,
                                        NULL,
                                        "Failed to allocate buffers for connection\n");
        return 1;
    }

    STATS_LOCK();
    stats.conn_structs++;
    STATS_UNLOCK();

    return 0;
}

/**
 * Destructor for all connection objects. Release all allocated resources.
 */
static void conn_destructor(conn *c) {
    free(c->read.buf);
    free(c->write.buf);
    free(c->ilist);
    free(c->temp_alloc_list);
    free(c->iov);
    free(c->msglist);
    free(c);

    STATS_LOCK();
    stats.conn_structs--;
    STATS_UNLOCK();
}

/** Allocate a connection, creating memory and adding it to the conections
 *  list. Returns a pointer to the newly-allocated connection if successful,
 *  else NULL.
 */
static conn *allocate_connection(void) {
    conn *ret = malloc(sizeof(conn));
    if (ret == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connection");
        return NULL;
    }

    if (conn_constructor(ret) != 0) {
        free(ret);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connection");
        return NULL;
    }

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
 *  and freeing the conn object.
 */
static void release_connection(conn *c) {
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
static enum loan_res conn_loan_single_buffer(conn *c, struct net_buf *thread_buf,
                                    struct net_buf *conn_buf)
{
    /* Already have a (partial) buffer - nothing to do. */
    if (conn_buf->buf != NULL) {
        return loan_existing;
    }

    if (thread_buf->buf != NULL) {
        /* Loan thread's buffer to connection. */
        *conn_buf = *thread_buf;

        thread_buf->buf = NULL;
        thread_buf->size = 0;
        return loan_loaned;
    } else {
        /* Need to allocate a new buffer. */
        conn_buf->buf = malloc(DATA_BUFFER_SIZE);
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
            return loan_existing;
        }
        conn_buf->size = DATA_BUFFER_SIZE;
        conn_buf->curr = conn_buf->buf;
        conn_buf->bytes = 0;
        return loan_allocated;
    }
}

/**
 * Return an empty read buffer back to the owning worker thread.
 */
static void conn_return_single_buffer(conn *c, struct net_buf *thread_buf,
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
    case bin_reading_set_header: return "bin_reading_set_header";
    case bin_reading_cas_header: return "bin_reading_cas_header";
    case bin_read_set_value: return "bin_read_set_value";
    case bin_reading_sasl_auth: return "bin_reading_sasl_auth";
    case bin_reading_sasl_auth_data: return "bin_reading_sasl_auth_data";
    case bin_reading_packet: return "bin_reading_packet";
    default:
        return "illegal";
    }
}

/* cJSON uses double for all numbers, so only has 53 bits of precision.
 * Therefore encode 64bit integers as string.
 */
static void json_add_uintptr_to_object(cJSON *obj, const char *name,
                                       uintptr_t value) {
    char buffer[32];
    if (snprintf(buffer, sizeof(buffer), "0x%"PRIxPTR, value) >= sizeof(buffer)) {
        cJSON_AddStringToObject(obj, name, "<too long>");
    } else {
        cJSON_AddStringToObject(obj, name, buffer);
    }
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
static cJSON* get_connection_stats(const conn *c) {
    cJSON *obj = cJSON_CreateObject();
    json_add_uintptr_to_object(obj, "conn", (uintptr_t)c);
    if (c->sfd == INVALID_SOCKET) {
        cJSON_AddStringToObject(obj, "socket", "disconnected");
    } else {
        cJSON_AddNumberToObject(obj, "socket", c->sfd);
        cJSON_AddNumberToObject(obj, "nevents", c->nevents);
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
                                c->registered_in_libevent);
        cJSON_AddNumberToObject(obj, "ev_flags", c->ev_flags);
        cJSON_AddNumberToObject(obj, "which", c->which);
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
        cJSON_AddNumberToObject(obj, "store_op", c->store_op);
        cJSON_AddNumberToObject(obj, "sbytes", c->sbytes);
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
            json_add_uintptr_to_object(ilist, "list", (uintptr_t)c->ilist);
            cJSON_AddNumberToObject(ilist, "size", c->isize);
            json_add_uintptr_to_object(ilist, "curr", (uintptr_t)c->icurr);
            cJSON_AddNumberToObject(ilist, "left", c->ileft);

            cJSON_AddItemToObject(obj, "itemlist", ilist);
        }
        {
            cJSON *talloc = cJSON_CreateObject();
            json_add_uintptr_to_object(talloc, "list",
                                       (uintptr_t)c->temp_alloc_list);
            cJSON_AddNumberToObject(talloc, "size", c->temp_alloc_size);
            json_add_uintptr_to_object(talloc, "curr",
                                       (uintptr_t)c->temp_alloc_curr);
            cJSON_AddNumberToObject(talloc, "left", c->temp_alloc_left);

            cJSON_AddItemToObject(obj, "temp_alloc_list", talloc);
        }
        json_add_bool_to_object(obj, "noreply", c->noreply);
        cJSON_AddNumberToObject(obj, "refcount", c->refcount);
        {
            cJSON* dy_buf = cJSON_CreateObject();
            json_add_uintptr_to_object(dy_buf, "buffer",
                                       (uintptr_t)c->dynamic_buffer.buffer);
            cJSON_AddNumberToObject(dy_buf, "size", c->dynamic_buffer.size);
            cJSON_AddNumberToObject(dy_buf, "offset", c->dynamic_buffer.offset);

            cJSON_AddItemToObject(obj, "dynamic_buffer", dy_buf);
        }
        json_add_uintptr_to_object(obj, "engine_storage",
                                   (uintptr_t)c->engine_storage);
        /* @todo we should decode the binary header */
        json_add_uintptr_to_object(obj, "cas", c->cas);
        cJSON_AddNumberToObject(obj, "cmd", c->cmd);
        json_add_uintptr_to_object(obj, "opaque", c->opaque);
        cJSON_AddNumberToObject(obj, "keylen", c->keylen);
        cJSON_AddNumberToObject(obj, "list_state", c->list_state);
        json_add_uintptr_to_object(obj, "next", (uintptr_t)c->next);
        json_add_uintptr_to_object(obj, "thread", (uintptr_t)c->thread);
        cJSON_AddNumberToObject(obj, "aiostat", c->aiostat);
        json_add_bool_to_object(obj, "ewouldblock", c->ewouldblock);
        json_add_uintptr_to_object(obj, "tap_iterator",
                                   (uintptr_t)c->tap_iterator);
    }
    return obj;
}
