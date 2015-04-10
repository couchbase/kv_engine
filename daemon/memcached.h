/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_H
#define MEMCACHED_H

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */
#include <event.h>
#include <platform/platform.h>
#include <cbsasl/cbsasl.h>

#include <memcached/openssl.h>

#include <memcached/protocol_binary.h>
#include <memcached/engine.h>
#include <memcached/extension.h>

#include "rbac.h"
#include "settings.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

/** Size of an incr buf. */
#define INCR_MAX_STORAGE_LEN 24

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

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define DONT_PREALLOC_SLABS
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)


#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

/* Maximum length of config which can be validated */
#define CONFIG_VALIDATE_MAX_LENGTH (64 * 1024)

/* Maximum IOCTL get/set key and payload (body) length */
#define IOCTL_KEY_LENGTH 128
#define IOCTL_VAL_LENGTH 128

/** Append a simple stat with a stat name, value format and value */
#define APPEND_STAT(name, fmt, val) \
    append_stat(name, add_stats, c, fmt, val);

/** Append an indexed stat with a stat name (with format), value format
    and value */
#define APPEND_NUM_FMT_STAT(name_fmt, num, name, fmt, val)          \
    klen = snprintf(key_str, STAT_KEY_LEN, name_fmt, num, name);    \
    vlen = snprintf(val_str, STAT_VAL_LEN, fmt, val);               \
    add_stats(key_str, klen, val_str, vlen, c);

/** Common APPEND_NUM_FMT_STAT format. */
#define APPEND_NUM_STAT(num, name, fmt, val) \
    APPEND_NUM_FMT_STAT("%d:%s", num, name, fmt, val)

enum bin_substates {
    bin_no_state,
    bin_reading_packet
};

/** Stats stored per slab (and per thread). */
struct slab_stats {
    uint64_t  cmd_set;
    uint64_t  get_hits;
    uint64_t  delete_hits;
    uint64_t  cas_hits;
    uint64_t  cas_badval;
};

/**
 * Stats stored per-thread.
 */
struct thread_stats {
    cb_mutex_t   mutex;
    uint64_t          cmd_get;
    uint64_t          get_misses;
    uint64_t          delete_misses;
    uint64_t          incr_misses;
    uint64_t          decr_misses;
    uint64_t          incr_hits;
    uint64_t          decr_hits;
    uint64_t          cas_misses;
    uint64_t          bytes_read;
    uint64_t          bytes_written;
    uint64_t          cmd_flush;
    uint64_t          conn_yields; /* # of yields for connections (-R option)*/
    uint64_t          auth_cmds;
    uint64_t          auth_errors;
    /* # of read buffers allocated. */
    uint64_t          rbufs_allocated;
    /* # of read buffers which could be loaned (and hence didn't need to be allocated). */
    uint64_t          rbufs_loaned;
    /* # of read buffers which already existed (with partial data) on the connection
       (and hence didn't need to be allocated). */
    uint64_t          rbufs_existing;
    /* # of write buffers allocated. */
    uint64_t          wbufs_allocated;
    /* # of write buffers which could be loaned (and hence didn't need to be allocated). */
    uint64_t          wbufs_loaned;
    /* Highest value iovsize has got to */
    uint64_t          iovused_high_watermark;
    /* High value conn->msgused has got to */
    uint64_t          msgused_high_watermark;
    struct slab_stats slab_stats[MAX_NUMBER_OF_SLAB_CLASSES];
};

/**
 * Listening port.
 */
struct listening_port {
    int port;
    int curr_conns;
    int maxconns;
};

/**
 * Global stats.
 */
struct stats {
    cb_mutex_t mutex;
    unsigned int  daemon_conns; /* conns used by the server */
    unsigned int  curr_conns;
    unsigned int  total_conns;
    unsigned int  conn_structs;
    time_t        started;          /* when the process was started */
    uint64_t      rejected_conns; /* number of times I reject a client */
    struct listening_port *listening_ports;
};

#define MAX_VERBOSITY_LEVEL 2


struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct engine_event_handler *next;
};

extern struct stats stats;

enum thread_type {
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

typedef struct {
    cb_thread_t thread_id;      /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    SOCKET notify[2];           /* notification pipes */
    struct conn_queue *new_conn_queue; /* queue of new connections to handle */
    cb_mutex_t mutex;      /* Mutex to lock protect access to the pending_io */
    bool is_locked;
    struct conn *pending_io;    /* List of connection with pending async io ops */
    int index;                  /* index of this thread in the threads array */
    enum thread_type type;      /* Type of IO this thread processes */

    rel_time_t last_checked;

    struct net_buf read; /** Shared read buffer for all connections serviced by this thread. */
    struct net_buf write; /** Shared write buffer for all connections serviced by this thread. */

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

typedef struct conn conn;
typedef bool (*STATE_FUNC)(conn *);


/**
 * The structure representing a connection into memcached.
 */
struct conn {
    conn* all_next; /** Intrusive list to track all connections */
    conn* all_prev;

    SOCKET sfd;
    protocol_t protocol; /* The protocol used by the connection */
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
    void   *write_and_free; /** free this memory after finishing writing */

    char   *ritem;  /** when we read in an item's value, it goes here */
    uint32_t rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */

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

    struct {
        char *buffer;
        size_t size;
        size_t offset;
    } dynamic_buffer;

    void *engine_storage;
    hrtime_t start;

    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;
    uint64_t cas; /* the cas to return */
    uint8_t cmd; /* current command being processed */
    int opaque;
    int keylen;

    int list_state; /* bitmask of list state data for this connection */
    conn   *next;     /* Used for generating a list of conn structures */
    LIBEVENT_THREAD *thread; /* Pointer to the thread object serving this connection */

    ENGINE_ERROR_CODE aiostat;
    bool ewouldblock;
    TAP_ITERATOR tap_iterator;
    in_port_t parent_port; /* Listening port that creates this connection instance */

    int dcp;

    /* Clean up this at one point.. */
    struct {
        struct {
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

    auth_context_t *auth_context;
};

/* list of listening connections */
extern conn *listen_conn;

/* States for the connection list_state */
#define LIST_STATE_PROCESSING 1
#define LIST_STATE_REQ_PENDING_IO 2

/*
 * Functions
 */
#include "stats.h"
#include "trace.h"
#include "hash.h"
#include <memcached/util.h>

/*
 * Functions to add / update the connection to libevent
 */
bool register_event(conn *c, struct timeval *timeout);
bool unregister_event(conn *c);
bool update_event(conn *c, const int new_flags);

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
void accept_new_conns(const bool do_accept);
conn *conn_from_freelist(void);
bool  conn_add_to_freelist(conn *c);
int   is_listen_thread(void);

void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_clear(struct thread_stats *stats);
void threadlocal_stats_reset(struct thread_stats *thread_stats);
void threadlocal_stats_aggregate(struct thread_stats *thread_stats, struct thread_stats *stats);
void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out);

/* Stat processing functions */
void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...);

void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status);
void conn_set_state(conn *c, STATE_FUNC state);
const char *state_text(STATE_FUNC state);
void safe_close(SOCKET sfd);


/* Number of times this connection is in the given pending list */
int number_of_pending(conn *c, conn *pending);
bool has_cycle(conn *c);
bool list_contains(conn *h, conn *n);
conn *list_remove(conn *h, conn *n);
void enlist_conn(conn *c, conn **list);

/* Aggregate the maximum number of connections */
void calculate_maxconns(void);

bool load_extension(const char *soname, const char *config);

int add_conn_to_pending_io_list(conn *c);

extern void drop_privileges(void);

/* connection state machine */
bool conn_listening(conn *c);
bool conn_new_cmd(conn *c);
bool conn_waiting(conn *c);
bool conn_read(conn *c);
bool conn_parse_cmd(conn *c);
bool conn_write(conn *c);
bool conn_nread(conn *c);
bool conn_pending_close(conn *c);
bool conn_immediate_close(conn *c);
bool conn_closing(conn *c);
bool conn_destroyed(conn *c);
bool conn_mwrite(conn *c);
bool conn_ship_log(conn *c);
bool conn_setup_tap_stream(conn *c);
bool conn_refresh_cbsasl(conn *c);
bool conn_refresh_ssl_certs(conn *c);
bool conn_flush(conn *c);
bool conn_audit_configuring(conn *c);

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

#ifdef __cplusplus
}
#endif

#endif
