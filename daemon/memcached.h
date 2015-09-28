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
#include <memcached/extension.h>
#include "net_buf.h"
#include <JSON_checker.h>

#include "rbac.h"
#include "settings.h"

#include "dynamic_buffer.h"


/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

#define DATA_BUFFER_SIZE 2048
#define UDP_MAX_PAYLOAD_SIZE 1400
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

#define LOCK_THREAD(t)                          \
    cb_mutex_enter(&t->mutex);                  \
    cb_assert(t->is_locked == false);              \
    t->is_locked = true;

#define UNLOCK_THREAD(t)                         \
    cb_assert(t->is_locked == true);                \
    t->is_locked = false;                        \
    cb_mutex_exit(&t->mutex);

extern void notify_thread(LIBEVENT_THREAD *thread);
extern void notify_dispatcher(void);
extern bool create_notification_pipe(LIBEVENT_THREAD *me);

#include "connection.h"

typedef union {
    item_info info;
    char bytes[sizeof(item_info) + ((IOV_MAX - 1) * sizeof(struct iovec))];
} item_info_holder;

/* list of listening connections */
extern Connection *listen_conn;

/*
 * Functions
 */
#include "stats.h"
#include "trace.h"
#include <memcached/util.h>

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

void dispatch_conn_new(SOCKET sfd, int parent_port,
                       STATE_FUNC init_state);

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

/* Aggregate the maximum number of connections */
void calculate_maxconns(void);

bool load_extension(const char *soname, const char *config);

int add_conn_to_pending_io_list(Connection *c);

extern "C" void drop_privileges(void);

/* connection state machine */
bool conn_listening(Connection *c);
bool conn_new_cmd(Connection *c);
bool conn_waiting(Connection *c);
bool conn_read(Connection *c);
bool conn_parse_cmd(Connection *c);
bool conn_write(Connection *c);
bool conn_nread(Connection *c);
bool conn_pending_close(Connection *c);
bool conn_immediate_close(Connection *c);
bool conn_closing(Connection *c);
bool conn_destroyed(Connection *c);
bool conn_mwrite(Connection *c);
bool conn_ship_log(Connection *c);
bool conn_setup_tap_stream(Connection *c);
bool conn_refresh_cbsasl(Connection *c);
bool conn_refresh_ssl_certs(Connection *c);
bool conn_flush(Connection *c);
bool conn_audit_configuring(Connection *c);
bool conn_create_bucket(Connection *c);
bool conn_delete_bucket(Connection *c);

void event_handler(evutil_socket_t fd, short which, void *arg);
void collect_timings(const Connection *c);

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
/* TODO: move to connections.c */

/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
int add_iov(Connection *c, const void *buf, size_t len);

int add_bin_header(Connection *c, uint16_t err, uint8_t ext_len, uint16_t key_len,
                   uint32_t body_len, uint8_t datatype);

/* set up a connection to write a DynamicBuffer then free it once sent. */
void write_and_free(Connection *c, DynamicBuffer* buf);

void write_bin_packet(Connection *c, protocol_binary_response_status err);

/* Increments topkeys count for a key when called by a valid operation. */
void update_topkeys(const char *key, size_t nkey, Connection *c);

/**
 * Convert an error code generated from the storage engine to the corresponding
 * error code used by the protocol layer.
 * @param e the error code as used in the engine
 * @return the error code as used by the protocol layer
 */
protocol_binary_response_status engine_error_2_protocol_error(ENGINE_ERROR_CODE e);

void bucket_item_set_cas(Connection *c, item *it, uint64_t cas);

void bucket_reset_stats(Connection *c);
ENGINE_ERROR_CODE bucket_get_engine_vb_map(Connection *c,
                                           engine_get_vb_map_cb callback);

ENGINE_ERROR_CODE bucket_unknown_command(Connection *c,
                                         protocol_binary_request_header *request,
                                         ADD_RESPONSE response);

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



#endif
