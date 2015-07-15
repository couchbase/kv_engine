/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <mutex>

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

#include "rbac.h"
#include "settings.h"

#include "stats_counter.h"

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

enum bin_substates {
    bin_no_state,
    bin_reading_packet
};

#define MAX_VERBOSITY_LEVEL 2

struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct engine_event_handler *next;
};

extern struct stats stats;

enum class ThreadType {
    GENERAL = 11,
    TAP = 13,
    DISPATCHER = 15
};

/**
 * The structure representing a network buffer
 */
struct net_buf {
    char     *buf;  /** start of allocated buffer */
    char     *curr; /** but if we parsed some already, this is where we stopped */
    uint32_t size;  /** total allocated size of buf */
    uint32_t bytes; /** how much data, starting from curr, do we have unparsed */
};

struct dynamic_buffer {
     char *buffer;   /** Start of the allocated buffer */
     size_t size;    /** Total allocated size */
     size_t offset;  /** How much of the buffer has been used so far. */
};

typedef struct {
    cb_thread_t thread_id;      /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    SOCKET notify[2];           /* notification pipes */
    struct conn_queue *new_conn_queue; /* queue of new connections to handle */
    cb_mutex_t mutex;      /* Mutex to lock protect access to the pending_io */
    bool is_locked;
    struct Connection *pending_io;    /* List of connection with pending async io ops */
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
} LIBEVENT_THREAD;

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

typedef struct Connection conn;
typedef bool (*STATE_FUNC)(Connection *);

// Command context destructor function pointer.
typedef void (*cmd_context_dtor_t)(void*);

/**
 * The structure representing a connection into memcached.
 */
struct Connection {
    Connection * all_next; /** Intrusive list to track all connections */
    Connection * all_prev;

    SOCKET sfd;
    Protocol protocol; /* The protocol used by the connection */
    char *peername; /* Name of the peer if known */
    char *sockname; /* Name of the local socket if known */
    int max_reqs_per_event; /** The maximum requests we can process in a worker
                                thread timeslice */
    int nevents; /** number of events this connection can process in a single
                     worker thread timeslice */
    bool admin;
    cbsasl_conn_t *sasl_conn;
    STATE_FUNC   state;
    enum bin_substates substate;
    bool   registered_in_libevent;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */
    struct net_buf read; /** Read buffer */
    struct net_buf write; /* Write buffer */

    /** which state to go into after finishing current write */
    STATE_FUNC   write_and_go;
    void   *write_and_free; /** free this memory after finishing writing. Note:
                                used only by write_and_free(); shouldn't be
                                directly used by any commands.*/

    char   *ritem;  /** when we read in an item's value, it goes here */
    uint32_t rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data.
     */
    void   *item;

    /* data for the swallow state */
    uint32_t sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    void   **ilist;   /* list of items to write out */
    int    isize;
    void   **icurr;
    int    ileft;

    char   **temp_alloc_list;
    int    temp_alloc_size;
    char   **temp_alloc_curr;
    int    temp_alloc_left;

    struct sockaddr_storage request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    int    hdrsize;   /* number of headers' worth of space is allocated */

    bool   noreply;   /* True if the reply should not be sent. */
    bool nodelay; /* Is tcp nodelay enabled? */

    /* current stats command */

    uint8_t refcount; /* number of references to the object */
    bool   supports_datatype;
    /**
     * If the client enabled the mutation seqno feature each mutation
     * command will return the vbucket UUID and sequence number for the
     * mutation.
     */
    bool supports_mutation_extras;

    struct dynamic_buffer dynamic_buffer;

    // Pointer to engine-specific data which the engine has requested the server
    // to persist for the life of the connection.
    // See SERVER_COOKIE_API::{get,store}_engine_specific()
    void *engine_storage;

    hrtime_t start;

    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;
    uint64_t cas; /* the cas to return */
    uint8_t cmd; /* current command being processed */
    uint32_t opaque;
    int keylen;

    int list_state; /* bitmask of list state data for this connection */
    Connection *next;     /* Used for generating a list of Connection structures */
    LIBEVENT_THREAD *thread; /* Pointer to the thread object serving this connection */

    ENGINE_ERROR_CODE aiostat;
    bool ewouldblock;
    TAP_ITERATOR tap_iterator;
    in_port_t parent_port; /* Listening port that creates this connection instance */

    int dcp;

    /** command-specific context - for use by command executors to maintain
     *  additional state while executing a command. For example
     *  a command may want to maintain some temporary state between retries
     *  due to engine returning EWOULDBLOCK.
     *  Between each command this is reset to NULL. To allow for any cleanup
     *  of resources before setting it to NULL a cmd_context destructor can be
     *  assigned. If the dtor is non-NULL (and cmd_context is also non-NULL)
     *  then cmd_context_dtor(cmd_context) is called before resetting them
     *  both back to NULL.
     */
    void* cmd_context;
    cmd_context_dtor_t cmd_context_dtor;

    /* Clean up this at one point.. */
    struct {
        struct {
            // TODO: Replace buffer/buffsz with std::vector.
            char *buffer;
            int buffsz;
            int total;
            int current;
        } in, out;

        bool enabled;
        bool error;
        SSL_CTX *ctx;
        SSL *client;

        bool connected;
        BIO *application;
        BIO *network;
    } ssl;

    AuthContext *auth_context;
    struct {
        int idx; /* The internal index for the connected bucket */
        ENGINE_HANDLE_V1 *engine;
    } bucket;
};

typedef union {
    item_info info;
    char bytes[sizeof(item_info) + ((IOV_MAX - 1) * sizeof(struct iovec))];
} item_info_holder;

/* list of listening connections */
extern Connection *listen_conn;

/* States for the connection list_state */
#define LIST_STATE_PROCESSING 1
#define LIST_STATE_REQ_PENDING_IO 2

/*
 * Functions
 */
#include "stats.h"
#include "trace.h"
#include <memcached/util.h>

/*
 * Functions to add / update the connection to libevent
 */
bool register_event(Connection *c, struct timeval *timeout);
bool unregister_event(Connection *c);
bool update_event(Connection *c, const int new_flags);
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
                       STATE_FUNC init_state, int event_flags,
                       int read_buffer_size);

/* Lock wrappers for cache functions that are called from main loop. */
int is_listen_thread(void);

void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_reset(struct thread_stats *thread_stats);

void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status);
void conn_set_state(Connection *c, STATE_FUNC state);
const char *state_text(STATE_FUNC state);
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

/* set up a connection to write a dynamic_buffer then free it once sent. */
void write_and_free(Connection *c, struct dynamic_buffer* buf);

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

#endif
