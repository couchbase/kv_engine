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
#pragma once

/*
 * Connection management and event loop handling.
 */
#include "memcached.h"

#include <functional>

struct FrontEndThread;
class ListeningPort;
class Bucket;

/**
 * Cerate a new client connection
 *
 * @param sfd the socket descriptor
 * @param interface the interface description for the connection
 * @param thread the libevent thread object to bind the client to
 * @return a connection object on success, nullptr otherwise
 */
Connection* conn_new(SOCKET sfd,
                     const ListeningPort& interface,
                     FrontEndThread& thread);

/**
 * Destroy the connection
 *
 * @param c the connection object to destroy
 */
void conn_destroy(Connection* c);

/**
 * Signal all of the idle clients in the system.
 *
 * Due to the locking model we need to have the FrontEndThread locked
 * when calling the method.
 *
 * @param me The connections to inspect must be bound to this thread
 * @return The number of clients connections bound to this thread
 */
int signal_idle_clients(FrontEndThread& me, bool dumpConnection);

/**
 * Iterate over all of the connections and call the callback function
 * for each of the connections.
 *
 * @param thread the thread to look at (must be locked)
 * @param callback the callback function to be called for each of the
 *                 connections
 */
void iterate_thread_connections(FrontEndThread* thread,
                                std::function<void(Connection&)> callback);
