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
#include "config.h"

#include "memcached.h"

#include <functional>

/* Destroy all connections and reset connection management */
void destroy_connections();

/* Run through all the connections and close them */
void close_all_connections();

/* Run the connection event loop; until an event handler returns false. */
void run_event_loop(Connection* c, short which);

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
void conn_loan_buffers(Connection* c);

/**
 * Return any empty buffers back to the owning worker thread.
 *
 * Converse of conn_loan_buffer(); if any of the read/write buffers are empty
 * (have no partial data) then return the buffer back to the worker thread.
 * If there is partial data, then keep the buffer with the connection.
 */
void conn_return_buffers(Connection* c);

/**
 * Cerate a new client connection
 *
 * @param sfd the socket descriptor
 * @param parent_port the port number the client connected to
 * @param base the event base to bind the client to
 * @param thread the libevent thread object to bind the client to
 * @return a connection object on success, nullptr otherwise
 */
Connection* conn_new(const SOCKET sfd,
                     in_port_t parent_port,
                     struct event_base* base,
                     LIBEVENT_THREAD* thread);

/**
 * Return the TCP or domain socket listening_port structure that
 * has a given port number
 */
ListeningPort *get_listening_port_instance(const in_port_t port);

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
int signal_idle_clients(LIBEVENT_THREAD *me, int bucket_idx, bool logging);

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

/**
 * Iterate over all of the connections and call the callback function
 * for each of the connections.
 *
 * @param thread the thread to look at (must be locked)
 * @param callback the callback function to be called for each of the
 *                 connections
 */
void iterate_thread_connections(LIBEVENT_THREAD* thread,
                                std::function<void(Connection&)> callback);
