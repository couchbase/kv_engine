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
}

/** Result of a buffer loan attempt */
enum loan_res {
    loan_existing,
    loan_loaned,
    loan_allocated,
};

static enum loan_res conn_loan_single_buffer(conn *c, struct net_buf *thread_buf,
                                             struct net_buf *conn_buf);
static void conn_return_single_buffer(conn *c, struct net_buf *thread_buf,
                                      struct net_buf *conn_buf);


/*****************************************************************************
 * External functions
 *****************************************************************************/

void conn_loan_buffers(conn *c) {
    enum loan_res res;
    res = conn_loan_single_buffer(c, &c->thread->read, &c->read);
    if (res == loan_allocated) {
        STATS_NOKEY(c, rbufs_allocated);
    } else if (res == loan_loaned) {
        STATS_NOKEY(c, rbufs_loaned);
    }

    res = conn_loan_single_buffer(c, &c->thread->write, &c->write);
    if (res == loan_allocated) {
        STATS_NOKEY(c, wbufs_allocated);
    } else if (res == loan_loaned) {
        STATS_NOKEY(c, wbufs_loaned);
    }
}

void conn_return_buffers(conn *c) {
    if (c->thread == NULL) {
        // Connection already cleaned up - nothing to do.
        return;
    }

    if (c->tap_iterator != NULL || c->upr) {
        /* TAP & UPR work differently - let them keep their buffers once
         * allocated.
         */
        return;
    }

    conn_return_single_buffer(c, &c->thread->read, &c->read);
    conn_return_single_buffer(c, &c->thread->write, &c->write);
}

/*****************************************************************************
 * Internal functions
 *****************************************************************************/

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
