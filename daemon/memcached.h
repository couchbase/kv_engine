/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <functional>
#include <mutex>
#include <unordered_set>
#include <vector>

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */
#include <cbsasl/cbsasl.h>
#include <event.h>
#include <platform/pipe.h>
#include <platform/platform.h>
#include <subdoc/operations.h>

#include <memcached/openssl.h>

#include <memcached/protocol_binary.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/extension.h>
#include <JSON_checker.h>

#include "dynamic_buffer.h"
#include "executorpool.h"
#include "log_macros.h"
#include "settings.h"
#include "timing_histogram.h"

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

enum class ThreadType {
    GENERAL = 11,
    DISPATCHER = 15
};

class Connection;
struct ConnectionQueueItem;

/**
 * The dispatcher accepts new clients and needs to dispatch them
 * to the worker threads. In order to do so we use the ConnectionQueue
 * where the dispatcher allocates the items and push on to the queue,
 * and the actual worker thread pop's the items off and start
 * serving them.
 */
class ConnectionQueue {
public:
    ~ConnectionQueue();

    void push(std::unique_ptr<ConnectionQueueItem> item);

    std::unique_ptr<ConnectionQueueItem> pop();

private:
    std::mutex mutex;
    std::queue<std::unique_ptr<ConnectionQueueItem> > connections;
};

struct LIBEVENT_THREAD {
    /**
     * Destructor.
     *
     * Close the notification pipe (if open)
     */
    ~LIBEVENT_THREAD();

    /// unique ID of this thread
    cb_thread_t thread_id = {};

    /// libevent handle this thread uses
    struct event_base* base = nullptr;

    /// listen event for notify pipe
    struct event notify_event = {};

    /**
     * notification pipe.
     *
     * The various worker threads are listening on index 0,
     * and in order to notify the thread other threads will
     * write data to index 1.
     */
    SOCKET notify[2] = {INVALID_SOCKET, INVALID_SOCKET};

    /// queue of new connections to handle
    ConnectionQueue new_conn_queue;

    /// Mutex to lock protect access to the pending_io
    std::mutex mutex;

    /// List of connection with pending async io ops (not owning)
    std::unordered_set<Connection*> pending_io;

    /// index of this thread in the threads array
    size_t index = 0;

    /// Type of IO this thread processes
    ThreadType type = ThreadType::GENERAL;

    /// Shared read buffer for all connections serviced by this thread.
    std::unique_ptr<cb::Pipe> read;

    /// Shared write buffer for all connections serviced by this thread.
    std::unique_ptr<cb::Pipe> write;

    /**
     * Shared sub-document operation for all connections serviced by this
     * thread
     */
    Subdoc::Operation subdoc_op;

    /**
     * When we're deleting buckets we need to disconnect idle
     * clients. This variable is incremented for every delete bucket
     * thread running and decremented when it's done. When this
     * variable is set we'll try to look through all connections and
     * update them with a write event if they're in an "idle"
     * state. That should cause them to be rescheduled and cause the
     * client to disconnect.
     */
    int deleting_buckets = 0;

    /**
     * Shared validator used by all connections serviced by this thread
     * when they need to validate a JSON document
     */
    JSON_checker::Validator validator;
};

#define LOCK_THREAD(t) t->mutex.lock();

#define UNLOCK_THREAD(t) t->mutex.unlock();

extern void notify_thread(LIBEVENT_THREAD& thread);
extern void notify_dispatcher();

#include "connection.h"

extern std::vector<TimingHistogram> scheduler_info;

/*
 * Functions
 */
#include "stats.h"
#include "trace.h"
#include "buckets.h"
#include <memcached/util.h>
#include <include/memcached/types.h>

/*
 * Functions to add / update the connection to libevent
 */
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

void dispatch_conn_new(SOCKET sfd, int parent_port);

/* Lock wrappers for cache functions that are called from main loop. */
int is_listen_thread(void);

void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_reset(std::vector<thread_stats>& thread_stats);

void notify_io_complete(gsl::not_null<const void*> cookie,
                        ENGINE_ERROR_CODE status);
void safe_close(SOCKET sfd);
int add_conn_to_pending_io_list(Connection* c);
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

void notify_thread_bucket_deletion(LIBEVENT_THREAD& me);

void threads_notify_bucket_deletion();
void threads_complete_bucket_deletion();
void threads_initiate_bucket_deletion();

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
extern std::unique_ptr<ExecutorPool> executorPool;

void iterate_all_connections(std::function<void(Connection&)> callback);

void start_stdin_listener(std::function<void()> function);

#endif
