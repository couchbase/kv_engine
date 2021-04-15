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

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

/* Maximum length of config which can be validated */
#define CONFIG_VALIDATE_MAX_LENGTH (64 * 1024)

/* Maximum IOCTL get/set key and payload (body) length */
#define IOCTL_KEY_LENGTH 128
#define IOCTL_VAL_LENGTH 128

class Bucket;
class Cookie;
class Connection;
struct thread_stats;

void initialize_buckets();
void cleanup_buckets();

void associate_initial_bucket(Connection& connection);

/*
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
void worker_threads_init();

void threads_shutdown();

/**
 * Create a socketpair and make it non-blocking
 *
 * @param sockets Where to store the sockets
 * @return true if success, false otherwise (and the error reason logged)
 */
bool create_nonblocking_socketpair(std::array<SOCKET, 2>& sockets);

void threadlocal_stats_reset(std::vector<thread_stats>& thread_stats);

void notifyIoComplete(Cookie& cookie, cb::engine_errc status);
void scheduleDcpStep(Cookie& cookie);

void safe_close(SOCKET sfd);
const char* get_server_version();
bool is_memcached_shutting_down();

/**
 * Connection-related functions
 */

/**
 * Increments topkeys count for the key specified within the command context
 * provided by the cookie.
 */
void update_topkeys(const Cookie& cookie);

struct ServerApi;
ServerApi* get_server_api();

void shutdown_server();
/// associate to a bucket when running from a cookie context (to allow
/// trace span
bool associate_bucket(Cookie& cookie, const char* name);
bool associate_bucket(Connection& connection,
                      const char* name,
                      Cookie* cookie = nullptr);
void disassociate_bucket(Connection& connection, Cookie* cookie = nullptr);
void disconnect_bucket(Bucket& bucket, Cookie* cookie);

/**
 * The executor pool used to pick up the result for requests spawn by the
 * client io threads and dispatched over to a background thread (in order
 * to allow for out of order replies).
 */
namespace cb {
class ExecutorPool;
}
extern std::unique_ptr<cb::ExecutorPool> executorPool;

void iterate_all_connections(std::function<void(Connection&)> callback);

void start_stdin_listener(std::function<void()> function);
