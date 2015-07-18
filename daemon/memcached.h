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

class Connection;

typedef struct {
    cb_thread_t thread_id;      /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    SOCKET notify[2];           /* notification pipes */
    struct conn_queue *new_conn_queue; /* queue of new connections to handle */
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

typedef bool (*STATE_FUNC)(Connection *);

/**
 * The SslContext class is a holder class for all of the ssl-related
 * information used by the connection object.
 */
class SslContext {
public:
    SslContext()
         : enabled(false),
           connected(false),
           error(false),
           application(nullptr),
           network(nullptr),
           ctx(nullptr),
           client(nullptr)
    {
        in.total = 0;
        in.current = 0;
        out.total = 0;
        out.current = 0;
    }

    ~SslContext();

    /**
     * Is ssl enabled for this connection or not?
     */
    bool isEnabled() const {
        return enabled;
    }

    /**
     * Is the client fully connected over ssl or not?
     */
    bool isConnected() const {
        return connected;
    }

    /**
     * Set the status of the connected flag
     */
    void setConnected() {
        connected = true;
    }

    /**
     * Is there an error on the SSL stream?
     */
    bool hasError() const {
        return error;
    }

    /**
     * Enable SSL for this connection.
     *
     * @param cert the certificate file to use
     * @param pkey the private key file to use
     * @return true if success, false if we failed to enable SSL
     */
    bool enable(const std::string &cert, const std::string &pkey);

    /**
     * Disable SSL for this connection
     */
    void disable();

    /**
     * Try to fill the SSL stream with as much data from the network
     * as possible.
     *
     * @param sfd the socket to read data from
     */
    void drainBioRecvPipe(SOCKET sfd);

    /**
     * Try to drain the SSL stream with as much data as possible and
     * send it over the network.
     *
     * @param sfd the socket to write data to
     */
    void drainBioSendPipe(SOCKET sfd);

    bool moreInputAvailable() const {
        return (in.current < in.total);
    }

    bool morePendingOutput() const {
        return (out.total > 0);
    }

    /**
     * Dump the list of available ciphers to the log
     * @param sfd the socket bound to the connection. Its only used in the
     *            log messages.
     */
    void dumpCipherList(SOCKET sfd);

    int accept() {
        return SSL_accept(client);
    }

    int getError(int errormask) {
        return SSL_get_error(client, errormask);
    }

    int read(void *buf, int num) {
        return SSL_read(client, buf, num);
    }

    int write(const void *buf, int num) {
        return SSL_write(client, buf, num);
    }

    int peek(void *buf, int num) {
        return SSL_peek(client, buf, num);
    }

protected:
    bool enabled;
    bool connected;
    bool error;
    BIO *application;
    BIO *network;
    SSL_CTX *ctx;
    SSL *client;
    struct {
        // The data located in the buffer
        std::vector<char> buffer;
        // The number of bytes currently stored in the buffer
        size_t total;
        // The current offset of the buffer
        size_t current;
    } in, out;
};

/**
 *  A command may need to store command specific context during the duration
 *  of a command (you might for instance want to keep state between multiple
 *  calls that returns EWOULDBLOCK).
 *
 *  The implementation of such commands should subclass this class and
 *  allocate an instance and store in the commands cmd_context member (which
 *  will be deleted and set to nullptr between each command being processed)..
 */
class CommandContext {
public:
    virtual ~CommandContext() {};
};

/**
 * The structure representing a connection into memcached.
 */
class Connection {
public:
    Connection();
    ~Connection();
    Connection(const Connection&) = delete;

    Connection * all_next; /** Intrusive list to track all connections */
    Connection * all_prev;

    SOCKET sfd;
    int max_reqs_per_event; /** The maximum requests we can process in a worker
                                thread timeslice */
    int nevents; /** number of events this connection can process in a single
                     worker thread timeslice */

    cbsasl_conn_t *sasl_conn;
private:
    STATE_FUNC   state;
public:
    enum bin_substates substate;

private:
    /* The protocol used by the connection */
    Protocol protocol;

    /** Is the connection set up with admin privileges */
    bool admin;

    // Members related to libevent

    /** Is the connection currently registered in libevent? */
    bool registered_in_libevent;
    /** The libevent object */
    struct event event;
    /** The current flags we've registered in libevent */
    short ev_flags;
    /** which events were just triggered */
    short currentEvent;

public:
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

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    // List of items we've reserved during the command (should call item_release)
    std::vector<void *> reservedItems;
    std::vector<char*> temp_alloc;

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

    /**
     *  command-specific context - for use by command executors to maintain
     *  additional state while executing a command. For example
     *  a command may want to maintain some temporary state between retries
     *  due to engine returning EWOULDBLOCK.
     *
     *  Between each command this is deleted and reset to nullptr.
     */
    CommandContext* cmd_context;

    SslContext ssl;

    AuthContext *auth_context;
    struct {
        int idx; /* The internal index for the connected bucket */
        ENGINE_HANDLE_V1 *engine;
    } bucket;

    /**
     * Set the connection state in the state machine. Any special
     * processing that needs to happen on certain state transitions can
     * happen here.
     */
    void setState(STATE_FUNC next_state);

    /**
     * Get the current state
     */
    STATE_FUNC getState() const {
        return state;
    }

    /**
     * Run the state machinery
     */
    void runStateMachinery();

    /**
     * Resolve the name of the local socket and the peer for the connected
     * socket
     */
    void resolveConnectionName();

    const std::string &getPeername() const {
        return peername;
    }

    const std::string &getSockname() const {
        return sockname;
    }

    /**
     * Update the settings in libevent for this connection
     *
     * @param mask the new event mask to get notified about
     */
    bool updateEvent(const short new_flags);

    /**
     * Unregister the event structure from libevent
     * @return true if success, false otherwise
     */
    bool unregisterEvent();
    /**
     * Register the event structure in libevent
     * @return true if success, false otherwise
     */
    bool registerEvent();

    bool isRegisteredInLibevent() const {
        return registered_in_libevent;
    }

    short getEventFlags() const {
        return ev_flags;
    }

    /**
     * Initialize the event structure and add it to libevent
     *
     * @param base the event base to bind to
     * @return true upon success, false otherwise
     */
    bool initializeEvent(struct event_base *base);

    /**
     * Terminate the eventloop for the current event base. This method doesn't
     * really fit as a member for the class, but I don't want clients to access
     * the libevent details from outside the class (so I didn't want to make
     * a "getEventBase()" method.
     */
    void eventBaseLoopbreak() {
        event_base_loopbreak(event.ev_base);
    }

    short getCurrentEvent() const {
        return currentEvent;
    }

    void setCurrentEvent(short ev) {
        currentEvent = ev;
    }

    /** Is the current event a readevent? */
    bool isReadevent() const {
        return currentEvent & EV_READ;
    }

    /** Is the current event a writeevent? */
    bool isWriteevent() const {
        return currentEvent & EV_READ;
    }

    /** Is the connection authorized with admin privileges? */
    bool isAdmin() const {
        return admin;
    }

    void setAdmin(bool admin) {
        Connection::admin = admin;
    }


    const Protocol &getProtocol() const {
        return protocol;
    }

    void setProtocol(const Protocol &protocol) {
        Connection::protocol = protocol;
    }

    /**
     * Create a cJSON representation of the members of the connection
     * Caller is responsible for freeing the result with cJSON_Delete().
     */
    cJSON *toJSON() const;

private:
    std::string peername; /* Name of the peer if known */
    std::string sockname; /* Name of the local socket if known */

    void resetBufferSize();
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

// This should probably go in a network-helper file..
#ifdef WIN32
#define GetLastNetworkError() WSAGetLastError()

inline int is_blocking(DWORD dw) {
    return (dw == WSAEWOULDBLOCK);
}
inline int is_emfile(DWORD dw) {
    return (dw == WSAEMFILE);
}
inline int is_closed_conn(DWORD dw) {
    return (dw == WSAENOTCONN || WSAECONNRESET);
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
