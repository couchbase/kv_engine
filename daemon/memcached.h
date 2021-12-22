/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <event.h>
#include <memcached/engine_error.h>
#include <memcached/server_callback_iface.h>
#include <memcached/types.h>
#include <platform/socket.h>
#include <subdoc/operations.h>

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

#define DATA_BUFFER_SIZE 2048
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of list of temprary auto allocates  */
#define TEMP_ALLOC_LIST_INITIAL 20

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 10

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 5

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define IOV_LIST_HIGHWAT 50
#define MSG_LIST_HIGHWAT 20

/* Maximum length of config which can be validated */
#define CONFIG_VALIDATE_MAX_LENGTH (64 * 1024)

/* Maximum IOCTL get/set key and payload (body) length */
#define IOCTL_KEY_LENGTH 128
#define IOCTL_VAL_LENGTH 128

#define MAX_VERBOSITY_LEVEL 2

extern struct stats stats;

// Forward decl
namespace cb {
class Pipe;
}
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

void thread_init(size_t nthreads,
                 struct event_base* main_base,
                 void (*dispatcher_callback)(evutil_socket_t, short, void*));
void threads_shutdown();
void threads_cleanup();

class ListeningPort;
void dispatch_conn_new(SOCKET sfd, std::shared_ptr<ListeningPort> interface);

/* Lock wrappers for cache functions that are called from main loop. */
int is_listen_thread(void);

void threadlocal_stats_reset(std::vector<thread_stats>& thread_stats);

void notify_io_complete(gsl::not_null<const void*> cookie,
                        ENGINE_ERROR_CODE status);
void safe_close(SOCKET sfd);
int add_conn_to_pending_io_list(Connection* c,
                                Cookie* cookie,
                                ENGINE_ERROR_CODE status);
void event_handler(evutil_socket_t fd, short which, void *arg);
void listen_event_handler(evutil_socket_t, short, void *);

void mcbp_collect_timings(Cookie& cookie);

void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *c);

const char* get_server_version(void);

/**
 * Connection-related functions
 */

/**
 * Increments topkeys count for the key specified within the command context
 * provided by the cookie.
 */
void update_topkeys(const Cookie& cookie);

SERVER_HANDLE_V1* get_server_api();

void shutdown_server();
bool associate_bucket(Connection& connection, const char* name);
void disassociate_bucket(Connection& connection);

void disable_listen();
bool is_listen_disabled();
uint64_t get_listen_disabled_num();

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
