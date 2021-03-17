/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <event.h>
#include <memcached/engine_error.h>
#include <memcached/types.h>
#include <platform/socket.h>
#include <subdoc/operations.h>
#include <gsl/gsl>

#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

/* Maximum length of config which can be validated */
#define CONFIG_VALIDATE_MAX_LENGTH (64 * 1024)

/* Maximum IOCTL get/set key and payload (body) length */
#define IOCTL_KEY_LENGTH 128
#define IOCTL_VAL_LENGTH 128

#define MAX_VERBOSITY_LEVEL 2

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
void threads_cleanup();

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
int add_conn_to_pending_io_list(Connection& c,
                                Cookie& cookie,
                                cb::engine_errc status);
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
void disassociate_bucket(Connection& connection);

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
