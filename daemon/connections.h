/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

/*
 * Connection management and event loop handling.
 */
#include "connection.h"
#include "memcached.h"

#include <functional>

struct FrontEndThread;
class ListeningPort;
class Bucket;

/**
 * Cerate a new client connection
 *
 * @param sfd the socket descriptor
 * @param thread The front end thread the connection should be bound to
 * @param descr Descriotion of the port accepting the connection
 * @param ssl The SSL structure to use if the connection is connecting
 *            over a port marked as SSL
 * @return a connection object on success, nullptr otherwise
 */
Connection* conn_new(SOCKET sfd,
                     FrontEndThread& thread,
                     std::shared_ptr<ListeningPort> descr,
                     uniqueSslPtr ssl);

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
