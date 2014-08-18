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

/*
 * Connection management and event loop handling.
 */

#ifndef CONNECTIONS_H
#define CONNECTIONS_H

#include "config.h"

#include "memcached.h"

/* Initialise connection management */
void initialize_connections(void);

/* Destroy all connections and reset connection management */
void destroy_connections(void);

/* Run the connection event loop; until an event handler returns false. */
void run_event_loop(conn* c);

/* Creates a new connection. Returns a pointer to the allocated connection if
 * successful, else NULL.
 */
conn *conn_new(const SOCKET sfd, in_port_t parent_port,
               STATE_FUNC init_state, int event_flags,
               unsigned int read_buffer_size, struct event_base *base,
               struct timeval *timeout);

/*
 * Closes a connection. Afterwards the connection is invalid (can no longer
 * be used), but it's memory is still allocated. See conn_destructor() to
 * actually free it's resources.
 */
void conn_close(conn *c);

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
void conn_shrink(conn *c);

/**
 * Return the TCP or domain socket listening_port structure that
 * has a given port number
 */
struct listening_port *get_listening_port_instance(const in_port_t port);

#endif /* CONNECTIONS_H */
