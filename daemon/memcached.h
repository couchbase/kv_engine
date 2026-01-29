/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_error.h>
#include <platform/socket.h>
#include <functional>
#include <vector>

#include <statistics/prometheus.h>

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

class Bucket;
class Cookie;
class Connection;
struct HighResolutionThreadStats;

/*
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
void worker_threads_init();

void threads_shutdown();

/// Safely closes a server socket.
void close_server_socket(SOCKET sfd);
/// Safely closes a client socket.
void close_client_socket(SOCKET sfd);

const char* get_server_version();
bool is_memcached_shutting_down();
void stop_memcached_main_base();

/**
 * Connection-related functions
 */

struct ServerApi;
ServerApi* get_server_api();

void shutdown_server();

/// Iterate over all of the connections and call the provided callback in
/// the context of the front end thread it is bound to. This means
/// that the function cannot be called in one of the front end context
void iterate_all_connections(std::function<void(Connection&)> callback);
