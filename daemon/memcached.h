/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <mutex>
#include <vector>

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */
#include <event.h>
#include <platform/platform.h>
#include <subdoc/operations.h>
#include <cbsasl/cbsasl.h>

#include <memcached/openssl.h>

#include <memcached/protocol_binary.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/extension.h>
#include <JSON_checker.h>

#include "dynamic_buffer.h"
#include "executorpool.h"
#include "log_macros.h"
#include "net_buf.h"
#include "settings.h"

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
    TAP = 13,
    DISPATCHER = 15
};

class Connection;
class ConnectionQueue;

struct LIBEVENT_THREAD {
    cb_thread_t thread_id;      /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    SOCKET notify[2];           /* notification pipes */
    ConnectionQueue *new_conn_queue; /* queue of new connections to handle */
    cb_mutex_t mutex;      /* Mutex to lock protect access to the pending_io */
    bool is_locked;
    Connection *pending_io;    /* List of connection with pending async io ops */
    int index;                  /* index of this thread in the threads array */
    ThreadType type;      /* Type of IO this thread processes */

    rel_time_t last_checked;

    struct net_buf read; /** Shared read buffer for all connections serviced by this thread. */
    struct net_buf write; /** Shared write buffer for all connections serviced by this thread. */

    subdoc_OPERATION* subdoc_op; /** Shared sub-document operation for all
                                     connections serviced by this thread. */

    /**
     * When we're deleting buckets we need to disconnect idle
     * clients. This variable is incremented for every delete bucket
     * thread running and decremented when it's done. When this
     * variable is set we'll try to look through all connections and
     * update them with a write event if they're in an "idle"
     * state. That should cause them to be rescheduled and cause the
     * client to disconnect.
     */
    int deleting_buckets;

    JSON_checker::Validator *validator;
};

#define LOCK_THREAD(t) \
    cb_mutex_enter(&t->mutex); \
    cb_assert(!t->is_locked); \
    t->is_locked = true;

#define UNLOCK_THREAD(t) \
    cb_assert(t->is_locked); \
    t->is_locked = false; \
    cb_mutex_exit(&t->mutex);

extern void notify_thread(LIBEVENT_THREAD *thread);
extern void notify_dispatcher(void);
extern bool create_notification_pipe(LIBEVENT_THREAD *me);

#include "connection.h"
#include "connection_greenstack.h"
#include "connection_listen.h"
#include "connection_mcbp.h"

/* list of listening connections */
extern Connection *listen_conn;

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
void associate_initial_bucket(Connection *c);

/*
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */

void thread_init(int nthreads, struct event_base *main_base,
                 void (*dispatcher_callback)(evutil_socket_t, short, void *));
void threads_shutdown(void);
void threads_cleanup(void);

void dispatch_conn_new(SOCKET sfd, int parent_port);

/* Lock wrappers for cache functions that are called from main loop. */
int is_listen_thread(void);

void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_reset(struct thread_stats *thread_stats);

void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status);
void safe_close(SOCKET sfd);


/* Number of times this connection is in the given pending list */
bool list_contains(Connection *h, Connection *n);
Connection *list_remove(Connection *h, Connection *n);

bool load_extension(const char *soname, const char *config);

int add_conn_to_pending_io_list(Connection *c);

/* connection state machine */
bool conn_listening(ListenConnection *c);

void event_handler(evutil_socket_t fd, short which, void *arg);
void listen_event_handler(evutil_socket_t, short, void *);

void mcbp_collect_timings(const McbpConnection* c);

void log_socket_error(EXTENSION_LOG_LEVEL severity,
                      const void* client_cookie,
                      const char* prefix);
#ifdef WIN32
void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                          const void* client_cookie,
                          const char* prefix, DWORD err);
#else
void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                          const void* client_cookie,
                          const char* prefix, int err);
#endif
void log_system_error(EXTENSION_LOG_LEVEL severity,
                      const void* cookie,
                      const char* prefix);


void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *c);

const char* get_server_version(void);

/**
 * Connection-related functions
 */

/* Increments topkeys count for a key when called by a valid operation. */
void update_topkeys(const DocKey& key, McbpConnection *c);


void notify_thread_bucket_deletion(LIBEVENT_THREAD *me);

void threads_notify_bucket_deletion(void);
void threads_complete_bucket_deletion(void);
void threads_initiate_bucket_deletion(void);

// This should probably go in a network-helper file..
#ifdef WIN32
#define GetLastNetworkError() WSAGetLastError()

inline int is_blocking(DWORD dw) {
    return (dw == WSAEWOULDBLOCK);
}
inline int is_interrupted(DWORD dw) {
    return (dw == WSAEINTR);
}
inline int is_emfile(DWORD dw) {
    return (dw == WSAEMFILE);
}
inline int is_closed_conn(DWORD dw) {
    return (dw == WSAENOTCONN || dw == WSAECONNRESET);
}
inline int is_addrinuse(DWORD dw) {
    return (dw == WSAEADDRINUSE);
}
inline void set_ewouldblock(void) {
    WSASetLastError(WSAEWOULDBLOCK);
}
inline void set_econnreset(void) {
    WSASetLastError(WSAECONNRESET);
}
#else
#define GetLastNetworkError() errno
#define GetLastError() errno

inline int is_blocking(int dw) {
    return (dw == EAGAIN || dw == EWOULDBLOCK);
}
inline int is_interrupted(int dw) {
    return (dw == EINTR || dw == EAGAIN);
}
inline int is_emfile(int dw) {
    return (dw == EMFILE);
}
inline int is_closed_conn(int dw) {
    return  (dw == ENOTCONN || dw != ECONNRESET);
}
inline int is_addrinuse(int dw) {
    return (dw == EADDRINUSE);
}
inline void set_ewouldblock(void) {
    errno = EWOULDBLOCK;
}
inline void set_econnreset(void) {
    errno = ECONNRESET;
}
#endif

void shutdown_server(void);
bool associate_bucket(Connection *c, const char *name);
void disassociate_bucket(Connection *c);

bool is_listen_disabled(void);
uint64_t get_listen_disabled_num(void);

ENGINE_ERROR_CODE refresh_cbsasl(Connection *c);
ENGINE_ERROR_CODE refresh_ssl_certs(Connection *c);

/**
 * The executor pool used to pick up the result for requests spawn by the
 * client io threads and dispatched over to a background thread (in order
 * to allow for out of order replies).
 */
extern std::unique_ptr<ExecutorPool> executorPool;

#endif
