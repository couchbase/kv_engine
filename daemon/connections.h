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

#ifdef __cplusplus
extern "C" {
#endif

/* Destroy all connections and reset connection management */
void destroy_connections(void);

/* Run through all the connections and close them */
void close_all_connections(void);

/* Run the connection event loop; until an event handler returns false. */
void run_event_loop(Connection * c);

/* Creates a new connection. Returns a pointer to the allocated connection if
 * successful, else NULL.
 */
Connection *conn_new(const SOCKET sfd, in_port_t parent_port,
               STATE_FUNC init_state, struct event_base *base);

/*
 * Closes a connection. Afterwards the connection is invalid (can no longer
 * be used), but it's memory is still allocated. See conn_destructor() to
 * actually free it's resources.
 */
void conn_close(Connection *c);

/**
 * Return the TCP or domain socket listening_port structure that
 * has a given port number
 */
struct listening_port *get_listening_port_instance(const in_port_t port);

/* Dump stats for the connection with the given fd number, or all connections
 * if fd is -1.
 * Note: We hold the connections mutex for the duration of this function.
 */
void connection_stats(ADD_STAT add_stats, Connection *c, const int64_t fd);

/*
 * Use engine::release to drop any data we may have allocated with engine::allocate
 */
void conn_cleanup_engine_allocations(Connection * c);

/**
 * Signal (set writable) all idle clients bound to either a specific
 * bucket specified by its index, or any bucket (specified as -1).
 * Due to the threading model we're only going to look at the clients
 * connected to the thread represented by me.
 *
 * @param me the thread to inspect
 * @param bucket_idx the bucket we'd like to signal (set to -1 to signal
 *                   all buckets)
 * @return the number of client connections bound to this thread.
 */
int signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx);

/**
 * Assert that none of the connections is assciated with
 * the given bucket (debug function).
 */
void assert_no_associations(int bucket_idx);

#ifndef WIN32
/**
 * Signal handler for SIGUSR1 to dump the connection states
 * for all of the connections.
 *
 * Please note that you <b>should</b> use <code>mcstat connections</code> to
 * get these stats on your node unless you've exhausted the connection limit
 * on the node.
 */
void dump_connection_stat_signal_handler(evutil_socket_t, short, void *);
#endif

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* CONNECTIONS_H */
