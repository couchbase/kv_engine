/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "config.h"
#include "memcached.h"
#include "memcached/extension_loggers.h"
#include "alloc_hooks.h"
#include "utilities/engine_loader.h"

#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <ctype.h>
#include <stdarg.h>
#include <stddef.h>

typedef union {
    item_info info;
    char bytes[sizeof(item_info) + ((IOV_MAX - 1) * sizeof(struct iovec))];
} item_info_holder;

static const char* get_server_version(void);

static void item_set_cas(const void *cookie, item *it, uint64_t cas) {
    settings.engine.v1->item_set_cas(settings.engine.v0, cookie, it, cas);
}

#define MAX_SASL_MECH_LEN 32

/* The item must always be called "it" */
#define SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_stats[info.info.clsid].slab_op++;

#define THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->thread_op++;

#define THREAD_GUTS2(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_op++; \
    thread_stats->thread_op++;

#define SLAB_THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    THREAD_GUTS(conn, thread_stats, slab_op, thread_op)

#define STATS_INCR1(GUTS, conn, slab_op, thread_op, key, nkey) { \
    struct thread_stats *thread_stats = get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    GUTS(conn, thread_stats, slab_op, thread_op); \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_INCR(conn, op, key, nkey) \
    STATS_INCR1(THREAD_GUTS, conn, op, op, key, nkey)

#define SLAB_INCR(conn, op, key, nkey) \
    STATS_INCR1(SLAB_GUTS, conn, op, op, key, nkey)

#define STATS_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op, key, nkey)

#define SLAB_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op, key, nkey)

#define STATS_HIT(conn, op, key, nkey) \
    SLAB_TWO(conn, op##_hits, cmd_##op, key, nkey)

#define STATS_MISS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_misses, cmd_##op, key, nkey)

#define STATS_NOKEY(conn, op) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op++; \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_NOKEY2(conn, op1, op2) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op1++; \
    thread_stats->op2++; \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_ADD(conn, op, amt) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op += amt; \
    cb_mutex_exit(&thread_stats->mutex); \
}

volatile sig_atomic_t memcached_shutdown;

/* Lock for global stats */
static cb_mutex_t stats_lock;

void STATS_LOCK() {
    cb_mutex_enter(&stats_lock);
}

void STATS_UNLOCK() {
    cb_mutex_exit(&stats_lock);
}

#ifdef WIN32
static int is_blocking(DWORD dw) {
    return (dw == WSAEWOULDBLOCK);
}
static int is_emfile(DWORD dw) {
    return (dw == WSAEMFILE);
}
static int is_closed_conn(DWORD dw) {
    return (dw == WSAENOTCONN || WSAECONNRESET);
}
static int is_addrinuse(DWORD dw) {
    return (dw == WSAEADDRINUSE);
}
#else
static int is_blocking(int dw) {
    return (dw == EAGAIN || dw == EWOULDBLOCK);
}

static int is_emfile(int dw) {
    return (dw == EMFILE);
}

static int is_closed_conn(int dw) {
    return  (dw == ENOTCONN || dw != ECONNRESET);
}

static int is_addrinuse(int dw) {
    return (dw == EADDRINUSE);
}
#endif


/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;

/*
 * forward declarations
 */
static SOCKET new_socket(struct addrinfo *ai);
static int try_read_command(conn *c);
static struct thread_stats* get_independent_stats(conn *c);
static struct thread_stats* get_thread_stats(conn *c);
static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data);


enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c, bool aggregate);
static void process_stat_settings(ADD_STAT add_stats, void *c);


/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static void complete_nread(conn *c);
static void write_and_free(conn *c, char *buf, int bytes);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);


/* time handling */
static void set_current_time(void);  /* update the global variable holding
                              global 32-bit seconds-since-start time
                              (to avoid 64 bit time_t) */

/** exported globals **/
struct stats stats;
struct settings settings;
static time_t process_started;     /* when the process was started */

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;
static struct thread_stats *default_independent_stats;

static struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);

#define REALTIME_MAXDELTA 60*60*24*30

/* Perform all callbacks of a given type for the given connection. */
static void perform_callbacks(ENGINE_EVENT_TYPE type,
                              const void *data,
                              const void *c) {
    struct engine_event_handler *h;
    for (h = engine_event_handlers[type]; h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}

/*
 * given time value that's either unix time or delta from current unix time,
 * return unix time. Use the fact that delta can't exceed one month
 * (and real time value can't be that low).
 */
static rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + current_time);
    }
}

/**
 * Convert the relative time to an absolute time (relative to EPOC ;) )
 */
static time_t abstime(const rel_time_t exptime)
{
    return process_started + exptime;
}

/**
 * Return the TCP or domain socket listening_port structure that
 * has a given port number
 */
static struct listening_port *get_listening_port_instance(const int port) {
    struct listening_port *port_ins = NULL;
    int i;
    for (i = 0; i < settings.num_ports; ++i) {
        if (stats.listening_ports[i].port == port) {
            port_ins = &stats.listening_ports[i];
        }
    }
    return port_ins;
}

static void stats_init(void) {
    stats.daemon_conns = 0;
    stats.rejected_conns = 0;
    stats.curr_conns = stats.total_conns = 0;
    stats.listening_ports = calloc(settings.num_ports, sizeof(struct listening_port));

    stats_prefix_init();
}

static void stats_reset(const void *cookie) {
    struct conn *conn = (struct conn*)cookie;
    STATS_LOCK();
    stats.rejected_conns = 0;
    stats.total_conns = 0;
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset(get_independent_stats(conn));
    settings.engine.v1->reset_stats(settings.engine.v0, cookie);
}

static int get_number_of_worker_threads(void) {
    int ret;
    char *override = getenv("MEMCACHED_NUM_CPUS");
    if (override == NULL) {
#ifdef WIN32
        SYSTEM_INFO sysinfo;
        GetSystemInfo(&sysinfo);
        ret = (int)sysinfo.dwNumberOfProcessors;
#else
        ret = (int)sysconf(_SC_NPROCESSORS_ONLN);
#endif
        if (ret > 4) {
            ret *= 0.75;
            if (ret < 4) {
                ret = 4;
            }
        }
    } else {
        ret = atoi(override);
        if (ret == 0) {
            ret = 4;
        }
    }

    return ret;
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.port = 11211;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1000;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = get_number_of_worker_threads();
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.allow_detailed = true;
    settings.reqs_per_event = DEFAULT_REQS_PER_EVENT;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.require_sasl = false;
    settings.extensions.logger = get_stderr_logger();
    settings.num_ports = 1;
    settings.tcp_nodelay = getenv("MEMCACHED_DISABLE_TCP_NODELAY") == NULL;
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c)
{
    struct msghdr *msg;

    assert(c != NULL);

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg)
            return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    return 0;
}

struct {
    cb_mutex_t mutex;
    bool disabled;
    ssize_t count;
    uint64_t num_disable;
} listen_state;

static bool is_listen_disabled(void) {
    bool ret;
    cb_mutex_enter(&listen_state.mutex);
    ret = listen_state.disabled;
    cb_mutex_exit(&listen_state.mutex);
    return ret;
}

static uint64_t get_listen_disabled_num(void) {
    uint64_t ret;
    cb_mutex_enter(&listen_state.mutex);
    ret = listen_state.num_disable;
    cb_mutex_exit(&listen_state.mutex);
    return ret;
}

static void disable_listen(void) {
    conn *next;
    cb_mutex_enter(&listen_state.mutex);
    listen_state.disabled = true;
    listen_state.count = 10;
    ++listen_state.num_disable;
    cb_mutex_exit(&listen_state.mutex);

    for (next = listen_conn; next; next = next->next) {
        update_event(next, 0);
        if (listen(next->sfd, 1) != 0) {
            log_socket_error(EXTENSION_LOG_WARNING, NULL,
                             "listen() failed: %s");
        }
    }
}

void safe_close(SOCKET sfd) {
    if (sfd != INVALID_SOCKET) {
        int rval;
        while ((rval = closesocket(sfd)) == SOCKET_ERROR &&
               (errno == EINTR || errno == EAGAIN)) {
            /* go ahead and retry */
        }

        if (rval == SOCKET_ERROR) {
            char msg[80];
            snprintf(msg, sizeof(msg), "Failed to close socket %d (%%s)!!", (int)sfd);
            log_socket_error(EXTENSION_LOG_WARNING, NULL,
                             msg);
        } else {
            STATS_LOCK();
            stats.curr_conns--;
            STATS_UNLOCK();

            if (is_listen_disabled()) {
                notify_dispatcher();
            }
        }
    }
}

/**
 * Reset all of the dynamic buffers used by a connection back to their
 * default sizes. The strategy for resizing the buffers is to allocate a
 * new one of the correct size and free the old one if the allocation succeeds
 * instead of using realloc to change the buffer size (because realloc may
 * not shrink the buffers, and will also copy the memory). If the allocation
 * fails the buffer will be unchanged.
 *
 * @param c the connection to resize the buffers for
 * @return true if all allocations succeeded, false if one or more of the
 *         allocations failed.
 */
static bool conn_reset_buffersize(conn *c) {
    bool ret = true;

    if (c->rsize != DATA_BUFFER_SIZE) {
        void *ptr = malloc(DATA_BUFFER_SIZE);
        if (ptr != NULL) {
            free(c->rbuf);
            c->rbuf = ptr;
            c->rsize = DATA_BUFFER_SIZE;
        } else {
            ret = false;
        }
    }

    if (c->wsize != DATA_BUFFER_SIZE) {
        void *ptr = malloc(DATA_BUFFER_SIZE);
        if (ptr != NULL) {
            free(c->wbuf);
            c->wbuf = ptr;
            c->wsize = DATA_BUFFER_SIZE;
        } else {
            ret = false;
        }
    }

    if (c->isize != ITEM_LIST_INITIAL) {
        void *ptr = malloc(sizeof(item *) * ITEM_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->ilist);
            c->ilist = ptr;
            c->isize = ITEM_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->suffixsize != SUFFIX_LIST_INITIAL) {
        void *ptr = malloc(sizeof(char *) * SUFFIX_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->suffixlist);
            c->suffixlist = ptr;
            c->suffixsize = SUFFIX_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->iovsize != IOV_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct iovec) * IOV_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->iov);
            c->iov = ptr;
            c->iovsize = IOV_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->msgsize != MSG_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct msghdr) * MSG_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->msglist);
            c->msglist = ptr;
            c->msgsize = MSG_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    return ret;
}

/**
 * Constructor for all memory allocations of connection objects. Initialize
 * all members and allocate the transfer buffers.
 *
 * @param buffer The memory allocated by the object cache
 * @return 0 on success, 1 if we failed to allocate memory
 */
static int conn_constructor(conn *c) {
    memset(c, 0, sizeof(*c));
    MEMCACHED_CONN_CREATE(c);

    c->state = conn_immediate_close;
    c->sfd = INVALID_SOCKET;
    if (!conn_reset_buffersize(c)) {
        free(c->rbuf);
        free(c->wbuf);
        free(c->ilist);
        free(c->suffixlist);
        free(c->iov);
        free(c->msglist);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING,
                                        NULL,
                                        "Failed to allocate buffers for connection\n");
        return 1;
    }

    STATS_LOCK();
    stats.conn_structs++;
    STATS_UNLOCK();

    return 0;
}

/**
 * Destructor for all connection objects. Release all allocated resources.
 *
 * @param buffer The memory allocated by the objec cache
 */
static void conn_destructor(conn *c) {
    free(c->rbuf);
    free(c->wbuf);
    free(c->ilist);
    free(c->suffixlist);
    free(c->iov);
    free(c->msglist);

    STATS_LOCK();
    stats.conn_structs--;
    STATS_UNLOCK();
}

/*
 * Free list management for connections.
 */
struct connections {
    conn* free;
    conn** all;
    cb_mutex_t mutex;
    int next;
} connections;

static void initialize_connections(void)
{
    int preallocate;

    cb_mutex_initialize(&connections.mutex);
    connections.all = calloc(settings.maxconns, sizeof(conn*));
    if (connections.all == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connections");
        exit(EX_OSERR);
    }

    preallocate = settings.maxconns / 2;
    if (preallocate < 1000) {
        preallocate = settings.maxconns;
    }

    for (connections.next = 0; connections.next < preallocate; ++connections.next) {
        connections.all[connections.next] = malloc(sizeof(conn));
        if (conn_constructor(connections.all[connections.next]) != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to allocate memory for connections");
            exit(EX_OSERR);
        }
        connections.all[connections.next]->next = connections.free;
        connections.free = connections.all[connections.next];
    }
}

static void destroy_connections(void)
{
    int ii;
    for (ii = 0; ii < settings.maxconns; ++ii) {
        if (connections.all[ii]) {
            conn *c = connections.all[ii];
            conn_destructor(c);
            free(c);
        }
    }

    free(connections.all);
}

static conn *allocate_connection(void) {
    conn *ret;

    cb_mutex_enter(&connections.mutex);
    ret = connections.free;
    if (ret != NULL) {
        connections.free = connections.free->next;
        ret->next = NULL;
    }
    cb_mutex_exit(&connections.mutex);

    if (ret == NULL) {
        ret = malloc(sizeof(conn));
        if (ret == NULL) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to allocate memory for connection");
            return NULL;
        }

        if (conn_constructor(ret) != 0) {
            conn_destructor(ret);
            free(ret);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to allocate memory for connection");
            return NULL;
        }

        cb_mutex_enter(&connections.mutex);
        if (connections.next == settings.maxconns) {
            free(ret);
            ret = NULL;
        } else {
            connections.all[connections.next++] = ret;
        }
        cb_mutex_exit(&connections.mutex);
    }

    return ret;
}

static void release_connection(conn *c) {
    c->sfd = INVALID_SOCKET;
    cb_mutex_enter(&connections.mutex);
    c->next = connections.free;
    connections.free = c;
    cb_mutex_exit(&connections.mutex);
}

static const char *substate_text(enum bin_substates state) {
    switch (state) {
    case bin_no_state: return "bin_no_state";
    case bin_reading_set_header: return "bin_reading_set_header";
    case bin_reading_cas_header: return "bin_reading_cas_header";
    case bin_read_set_value: return "bin_read_set_value";
    case bin_reading_get_key: return "bin_reading_get_key";
    case bin_reading_stat: return "bin_reading_stat";
    case bin_reading_del_header: return "bin_reading_del_header";
    case bin_reading_incr_header: return "bin_reading_incr_header";
    case bin_read_flush_exptime: return "bin_reading_flush_exptime";
    case bin_reading_sasl_auth: return "bin_reading_sasl_auth";
    case bin_reading_sasl_auth_data: return "bin_reading_sasl_auth_data";
    case bin_reading_packet: return "bin_reading_packet";
    default:
        return "illegal";
    }
}

static const char *protocol_text(enum protocol protocol) {
    switch (protocol) {
    case binary_prot: return "binary";
    case negotiating_prot: return "negotiating";
    default:
        return "illegal";
    }
}

static void add_connection_stats(ADD_STAT add_stats, conn *d, conn *c) {
    append_stat("conn", add_stats, d, "%p", c);
    if (c->sfd == INVALID_SOCKET) {
        append_stat("socket", add_stats, d, "disconnected");
    } else {
        append_stat("socket", add_stats, d, "%lu", (long)c->sfd);
        append_stat("protocol", add_stats, d, "%s", protocol_text(c->protocol));
        append_stat("transport", add_stats, d, "TCP");
        append_stat("nevents", add_stats, d, "%u", c->nevents);
        if (c->sasl_conn != NULL) {
            append_stat("sasl_conn", add_stats, d, "%p", c->sasl_conn);
        }
        append_stat("state", add_stats, d, "%s", state_text(c->state));
        if (c->protocol == binary_prot) {
            append_stat("substate", add_stats, d, "%s",
                        substate_text(c->substate));
        }
        append_stat("registered_in_libevent", add_stats, d, "%d",
                    (int)c->registered_in_libevent);
        append_stat("ev_flags", add_stats, d, "%x", c->ev_flags);
        append_stat("which", add_stats, d, "%x", c->which);
        append_stat("rbuf", add_stats, d, "%p", c->rbuf);
        append_stat("rcurr", add_stats, d, "%p", c->rcurr);
        append_stat("rsize", add_stats, d, "%u", c->rsize);
        append_stat("rbytes", add_stats, d, "%u", c->rbytes);
        append_stat("wbuf", add_stats, d, "%p", c->wbuf);
        append_stat("wcurr", add_stats, d, "%p", c->wcurr);
        append_stat("wsize", add_stats, d, "%u", c->wsize);
        append_stat("wbytes", add_stats, d, "%u", c->wbytes);
        append_stat("write_and_go", add_stats, d, "%p", c->write_and_go);
        append_stat("write_and_free", add_stats, d, "%p", c->write_and_free);
        append_stat("ritem", add_stats, d, "%p", c->ritem);
        append_stat("rlbytes", add_stats, d, "%u", c->rlbytes);
        append_stat("item", add_stats, d, "%p", c->item);
        append_stat("store_op", add_stats, d, "%u", c->store_op);
        append_stat("sbytes", add_stats, d, "%u", c->sbytes);
        append_stat("iov", add_stats, d, "%p", c->iov);
        append_stat("iovsize", add_stats, d, "%u", c->iovsize);
        append_stat("iovused", add_stats, d, "%u", c->iovused);
        append_stat("msglist", add_stats, d, "%p", c->msglist);
        append_stat("msgsize", add_stats, d, "%u", c->msgsize);
        append_stat("msgused", add_stats, d, "%u", c->msgused);
        append_stat("msgcurr", add_stats, d, "%u", c->msgcurr);
        append_stat("msgbytes", add_stats, d, "%u", c->msgbytes);
        append_stat("ilist", add_stats, d, "%p", c->ilist);
        append_stat("isize", add_stats, d, "%u", c->isize);
        append_stat("icurr", add_stats, d, "%p", c->icurr);
        append_stat("ileft", add_stats, d, "%u", c->ileft);
        append_stat("suffixlist", add_stats, d, "%p", c->suffixlist);
        append_stat("suffixsize", add_stats, d, "%u", c->suffixsize);
        append_stat("suffixcurr", add_stats, d, "%p", c->suffixcurr);
        append_stat("suffixleft", add_stats, d, "%u", c->suffixleft);

        append_stat("noreply", add_stats, d, "%d", c->noreply);
        append_stat("refcount", add_stats, d, "%u", (int)c->refcount);
        append_stat("dynamic_buffer.buffer", add_stats, d, "%p",
                    c->dynamic_buffer.buffer);
        append_stat("dynamic_buffer.size", add_stats, d, "%zu",
                    c->dynamic_buffer.size);
        append_stat("dynamic_buffer.offset", add_stats, d, "%zu",
                    c->dynamic_buffer.offset);
        append_stat("engine_storage", add_stats, d, "%p", c->engine_storage);
        if (c->protocol == binary_prot) {
            /* @todo we should decode the binary header */
            append_stat("cas", add_stats, d, "%"PRIu64, c->cas);
            append_stat("cmd", add_stats, d, "%u", c->cmd);
            append_stat("opaque", add_stats, d, "%u", c->opaque);
            append_stat("keylen", add_stats, d, "%u", c->keylen);
        }
        append_stat("list_state", add_stats, d, "%u", c->list_state);
        append_stat("next", add_stats, d, "%p", c->next);
        append_stat("thread", add_stats, d, "%p", c->thread);
        append_stat("aiostat", add_stats, d, "%u", c->aiostat);
        append_stat("ewouldblock", add_stats, d, "%u", c->ewouldblock);
        append_stat("tap_iterator", add_stats, d, "%p", c->tap_iterator);
    }
}

/**
 * Do a full stats of all of the connections.
 * Do _NOT_ try to follow _ANY_ of the pointers in the conn structure
 * because we read all of the values _DIRTY_. We preallocated the array
 * of all of the connection pointers during startup, so we _KNOW_ that
 * we can iterate through all of them. All of the conn structs will
 * only appear in the connections.all array when we've allocated them,
 * and we don't release them so it's safe to look at them.
 */
static void connection_stats(ADD_STAT add_stats, conn *c) {
    int ii;
    for (ii = 0; ii < settings.maxconns && connections.all[ii]; ++ii) {
        add_connection_stats(add_stats, c, connections.all[ii]);
    }
}

conn *conn_new(const SOCKET sfd, const int parent_port,
               STATE_FUNC init_state, const int event_flags,
               const int read_buffer_size, struct event_base *base,
               struct timeval *timeout) {
    conn *c = allocate_connection();
    if (c == NULL) {
        return NULL;
    }

    assert(c->thread == NULL);

    if (c->rsize < read_buffer_size) {
        void *mem = malloc(read_buffer_size);
        if (mem) {
            c->rsize = read_buffer_size;
            free(c->rbuf);
            c->rbuf = mem;
        } else {
            assert(c->thread == NULL);
            release_connection(c);
            return NULL;
        }
    }

    c->protocol = settings.binding_protocol;
    c->request_addr_size = 0;

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d server listening (%s)\n", sfd,
                                            protocol_text(c->protocol));
        } else if (c->protocol == negotiating_prot) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d new auto-negotiating client connection\n",
                                            sfd);
        } else if (c->protocol == binary_prot) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d new binary client connection.\n", sfd);
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "<%d new unknown (%d) client connection\n",
                                            sfd, c->protocol);
            assert(false);
        }
    }

    c->sfd = sfd;
    c->parent_port = parent_port;
    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->next = NULL;
    c->list_state = 0;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (!register_event(c, timeout)) {
        assert(c->thread == NULL);
        release_connection(c);
        return NULL;
    }

    STATS_LOCK();
    stats.total_conns++;
    STATS_UNLOCK();

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;
    c->refcount = 1;

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    perform_callbacks(ON_CONNECT, NULL, c);

    return c;
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    if (c->item) {
        settings.engine.v1->release(settings.engine.v0, c, c->item);
        c->item = 0;
    }

    if (c->ileft != 0) {
        for (; c->ileft > 0; c->ileft--,c->icurr++) {
            settings.engine.v1->release(settings.engine.v0, c, *(c->icurr));
        }
    }

    if (c->suffixleft != 0) {
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }
    }

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }

    if (c->sasl_conn) {
        cbsasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

    c->engine_storage = NULL;
    c->tap_iterator = NULL;
    c->thread = NULL;
    assert(c->next == NULL);
    c->sfd = INVALID_SOCKET;
}

void conn_close(conn *c) {
    assert(c != NULL);
    assert(c->sfd == INVALID_SOCKET);
    assert(c->state == conn_immediate_close);

    assert(c->thread);
    /* remove from pending-io list */
    if (settings.verbose > 1 && list_contains(c->thread->pending_io, c)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Current connection was in the pending-io list.. Nuking it\n");
    }
    c->thread->pending_io = list_remove(c->thread->pending_io, c);

    conn_cleanup(c);

    /*
     * The contract with the object cache is that we should return the
     * object in a constructed state. Reset the buffers to the default
     * size
     */
    conn_reset_buffersize(c);
    assert(c->thread == NULL);
    release_connection(c);
}

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
    assert(c != NULL);

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

        newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->rbuf = newbuf;
            c->rsize = DATA_BUFFER_SIZE;
        }
        /* TODO check other branch... */
        c->rcurr = c->rbuf;
    }

    if (c->isize > ITEM_LIST_HIGHWAT) {
        item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
        if (newbuf) {
            c->ilist = newbuf;
            c->isize = ITEM_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
    /* TODO check return value */
    }
}

/**
 * Convert a state name to a human readable form.
 */
const char *state_text(STATE_FUNC state) {
    if (state == conn_listening) {
        return "conn_listening";
    } else if (state == conn_new_cmd) {
        return "conn_new_cmd";
    } else if (state == conn_waiting) {
        return "conn_waiting";
    } else if (state == conn_read) {
        return "conn_read";
    } else if (state == conn_parse_cmd) {
        return "conn_parse_cmd";
    } else if (state == conn_write) {
        return "conn_write";
    } else if (state == conn_nread) {
        return "conn_nread";
    } else if (state == conn_swallow) {
        return "conn_swallow";
    } else if (state == conn_closing) {
        return "conn_closing";
    } else if (state == conn_mwrite) {
        return "conn_mwrite";
    } else if (state == conn_ship_log) {
        return "conn_ship_log";
    } else if (state == conn_setup_tap_stream) {
        return "conn_setup_tap_stream";
    } else if (state == conn_pending_close) {
        return "conn_pending_close";
    } else if (state == conn_immediate_close) {
        return "conn_immediate_close";
    } else if (state == conn_refresh_cbsasl) {
        return "conn_refresh_cbsasl";
    } else {
        return "Unknown";
    }
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
void conn_set_state(conn *c, STATE_FUNC state) {
    assert(c != NULL);

    if (state != c->state) {
        /*
         * The connections in the "tap thread" behaves differently than
         * normal connections because they operate in a full duplex mode.
         * New messages may appear from both sides, so we can't block on
         * read from the nework / engine
         */
        if (c->tap_iterator != NULL || c->upr) {
            if (state == conn_waiting) {
                c->which = EV_WRITE;
                state = conn_ship_log;
            }
        }

        if (settings.verbose > 2 || c->state == conn_closing
            || c->state == conn_setup_tap_stream) {
            settings.extensions.logger->log(EXTENSION_LOG_DETAIL, c,
                                            "%d: going from %s to %s\n",
                                            c->sfd, state_text(c->state),
                                            state_text(state));
        }

        if (state == conn_write || state == conn_mwrite) {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }

        c->state = state;
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
    assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov)
            return -1;
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    assert(c != NULL);

    if (len == 0) {
        return 0;
    }

    do {
        m = &c->msglist[c->msgused - 1];

        /*
         * Limit the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = (1 == c->msgused);

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(conn *c) {
    char *ret = c->rcurr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    assert(ret >= c->rbuf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(conn *c) {
    return c->rcurr - (c->binary_header.request.keylen);
}

/**
 * Insert a key into a buffer, but replace all non-printable characters
 * with a '.'.
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param key the key to add to the buffer
 * @param nkey the number of bytes in the key
 * @return number of bytes in dest if success, -1 otherwise
 */
static ssize_t key_to_printable_buffer(char *dest, size_t destsz,
                                       int client, bool from_client,
                                       const char *prefix,
                                       const char *key,
                                       size_t nkey)
{
    char *ptr;
    ssize_t ii;
    ssize_t nw = snprintf(dest, destsz, "%c%d %s ", from_client ? '>' : '<',
                          client, prefix);
    if (nw == -1) {
        return -1;
    }

    ptr = dest + nw;
    destsz -= nw;
    if (nkey > destsz) {
        nkey = destsz;
    }

    for (ii = 0; ii < nkey; ++ii, ++key, ++ptr) {
        if (isgraph(*key)) {
            *ptr = *key;
        } else {
            *ptr = '.';
        }
    }

    *ptr = '\0';
    return ptr - dest;
}

/**
 * Convert a byte array to a text string
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param data the data to add to the buffer
 * @param size the number of bytes in data to print
 * @return number of bytes in dest if success, -1 otherwise
 */
static ssize_t bytes_to_output_string(char *dest, size_t destsz,
                                      int client, bool from_client,
                                      const char *prefix,
                                      const char *data,
                                      size_t size)
{
    ssize_t nw = snprintf(dest, destsz, "%c%d %s", from_client ? '>' : '<',
                          client, prefix);
    ssize_t offset = nw;
    ssize_t ii;

    if (nw == -1) {
        return -1;
    }

    for (ii = 0; ii < size; ++ii) {
        if (ii % 4 == 0) {
            if ((nw = snprintf(dest + offset, destsz - offset, "\n%c%d  ",
                               from_client ? '>' : '<', client)) == -1) {
                return  -1;
            }
            offset += nw;
        }
        if ((nw = snprintf(dest + offset, destsz - offset,
                           " 0x%02x", (unsigned char)data[ii])) == -1) {
            return -1;
        }
        offset += nw;
    }

    if ((nw = snprintf(dest + offset, destsz - offset, "\n")) == -1) {
        return -1;
    }

    return offset + nw;
}

static int add_bin_header(conn *c,
                          uint16_t err,
                          uint8_t hdr_len,
                          uint16_t key_len,
                          uint32_t body_len) {
    protocol_binary_response_header* header;

    assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        return -1;
    }

    header = (protocol_binary_response_header *)c->wbuf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = htonll(c->cas);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer, sizeof(buffer), c->sfd, false,
                                   "Writing bin response:",
                                   (const char*)header->bytes,
                                   sizeof(header->bytes)) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "%s", buffer);
        }
    }

    return add_iov(c, c->wbuf, sizeof(header->response));
}

/**
 * Convert an error code generated from the storage engine to the corresponding
 * error code used by the protocol layer.
 * @param e the error code as used in the engine
 * @return the error code as used by the protocol layer
 */
static protocol_binary_response_status engine_error_2_protocol_error(ENGINE_ERROR_CODE e) {
    protocol_binary_response_status ret;

    switch (e) {
    case ENGINE_SUCCESS:
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    case ENGINE_KEY_ENOENT:
        return PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    case ENGINE_KEY_EEXISTS:
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    case ENGINE_ENOMEM:
        return PROTOCOL_BINARY_RESPONSE_ENOMEM;
    case ENGINE_TMPFAIL:
        return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
    case ENGINE_NOT_STORED:
        return PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    case ENGINE_EINVAL:
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    case ENGINE_ENOTSUP:
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    case ENGINE_E2BIG:
        return PROTOCOL_BINARY_RESPONSE_E2BIG;
    case ENGINE_NOT_MY_VBUCKET:
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    case ENGINE_ERANGE:
        return PROTOCOL_BINARY_RESPONSE_ERANGE;
    case ENGINE_ROLLBACK:
        return PROTOCOL_BINARY_RESPONSE_ROLLBACK;
    default:
        ret = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    return ret;
}

static void write_bin_packet(conn *c, protocol_binary_response_status err, int swallow) {
    ssize_t len = 0;
    const char *errtext = memcached_protocol_errcode_2_text(err);
    if (err != PROTOCOL_BINARY_RESPONSE_SUCCESS && errtext != NULL) {
        len = strlen(errtext);
    }

    if (errtext && settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        ">%d Writing an error: %s\n", c->sfd,
                                        errtext);
    }

    if (add_bin_header(c, err, 0, 0, len) == -1) {
        conn_set_state(c, conn_closing);
        return;
    }

    if (errtext) {
        add_iov(c, errtext, len);
    }
    conn_set_state(c, conn_mwrite);
    if (swallow > 0) {
        c->sbytes = swallow;
        c->write_and_go = conn_swallow;
    } else {
        c->write_and_go = conn_new_cmd;
    }
}

/* Form and send a response to a command over the binary protocol */
static void write_bin_response(conn *c, const void *d, int hlen, int keylen, int dlen) {
    if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET ||
        c->cmd == PROTOCOL_BINARY_CMD_GETK) {
        if (add_bin_header(c, 0, hlen, keylen, dlen) == -1) {
            conn_set_state(c, conn_closing);
            return;
        }
        add_iov(c, d, dlen);
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
    } else {
        conn_set_state(c, conn_new_cmd);
    }
}

static void complete_incr_bin(conn *c) {
    protocol_binary_response_incr* rsp = (protocol_binary_response_incr*)c->wbuf;
    protocol_binary_request_incr* req = binary_get_request(c);
    ENGINE_ERROR_CODE ret;
    uint64_t delta;
    uint64_t initial;
    rel_time_t expiration;
    char *key;
    size_t nkey;
    bool incr;

    assert(c != NULL);
    assert(c->wsize >= sizeof(*rsp));

    if (req->message.header.request.cas != 0) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
        return;
    }

    /* fix byteorder in the request */
    delta = ntohll(req->message.body.delta);
    initial = ntohll(req->message.body.initial);
    expiration = ntohl(req->message.body.expiration);
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    incr = (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT ||
            c->cmd == PROTOCOL_BINARY_CMD_INCREMENTQ);

    if (settings.verbose > 1) {
        char buffer[1024];
        ssize_t nw;
        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                     incr ? "INCR" : "DECR", key, nkey);
        if (nw != -1) {
            if (snprintf(buffer + nw, sizeof(buffer) - nw,
                         " %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n",
                         delta, initial, (uint64_t)expiration) != -1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s",
                                                buffer);
            }
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    if (ret == ENGINE_SUCCESS) {
        ret = settings.engine.v1->arithmetic(settings.engine.v0,
                                             c, key, nkey, incr,
                                             req->message.body.expiration != 0xffffffff,
                                             delta, initial, expiration,
                                             &c->cas,
                                             &rsp->message.body.value,
                                             c->binary_header.request.vbucket);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        rsp->message.body.value = htonll(rsp->message.body.value);
        write_bin_response(c, &rsp->message.body, 0, 0,
                           sizeof (rsp->message.body.value));
        if (incr) {
            STATS_INCR(c, incr_hits, key, nkey);
        } else {
            STATS_INCR(c, decr_hits, key, nkey);
        }
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            STATS_INCR(c, incr_misses, key, nkey);
        } else {
            STATS_INCR(c, decr_misses, key, nkey);
        }
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0);
        break;
    case ENGINE_EINVAL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, 0);
        break;
    case ENGINE_NOT_STORED:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        abort();
    }
}

static void complete_update_bin(conn *c) {
    protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    ENGINE_ERROR_CODE ret;
    item *it;
    item_info_holder info;

    assert(c != NULL);
    it = c->item;
    memset(&info, 0, sizeof(info));
    info.info.nvalue = 1;
    if (!settings.engine.v1->get_item_info(settings.engine.v0, c, it,
                                           (void*)&info)) {
        settings.engine.v1->release(settings.engine.v0, c, it);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: Failed to get item info\n",
                                        c->sfd);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
        return;
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    if (ret == ENGINE_SUCCESS) {
        ret = settings.engine.v1->store(settings.engine.v0, c,
                                        it, &c->cas, c->store_op,
                                        c->binary_header.request.vbucket);
    }

#ifdef ENABLE_DTRACE
    switch (c->cmd) {
    case OPERATION_ADD:
        MEMCACHED_COMMAND_ADD(c->sfd, info.info.key, info.info.nkey,
                              (ret == ENGINE_SUCCESS) ? info.info.nbytes : -1, c->cas);
        break;
    case OPERATION_REPLACE:
        MEMCACHED_COMMAND_REPLACE(c->sfd, info.info.key, info.info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.info.nbytes : -1, c->cas);
        break;
    case OPERATION_APPEND:
        MEMCACHED_COMMAND_APPEND(c->sfd, info.info.key, info.info.nkey,
                                 (ret == ENGINE_SUCCESS) ? info.info.nbytes : -1, c->cas);
        break;
    case OPERATION_PREPEND:
        MEMCACHED_COMMAND_PREPEND(c->sfd, info.info.key, info.info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.info.nbytes : -1, c->cas);
        break;
    case OPERATION_SET:
        MEMCACHED_COMMAND_SET(c->sfd, info.info.key, info.info.nkey,
                              (ret == ENGINE_SUCCESS) ? info.info.nbytes : -1, c->cas);
        break;
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    default:
        if (c->store_op == OPERATION_ADD) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        } else if(c->store_op == OPERATION_REPLACE) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        }
        write_bin_packet(c, eno, 0);
    }

    if (c->store_op == OPERATION_CAS) {
        switch (ret) {
        case ENGINE_SUCCESS:
            SLAB_INCR(c, cas_hits, info.info.key, info.info.nkey);
            break;
        case ENGINE_KEY_EEXISTS:
            SLAB_INCR(c, cas_badval, info.info.key, info.info.nkey);
            break;
        case ENGINE_KEY_ENOENT:
            STATS_NOKEY(c, cas_misses);
            break;
        default:
            ;
        }
    } else {
        SLAB_INCR(c, cmd_set, info.info.key, info.info.nkey);
    }

    if (!c->ewouldblock) {
        /* release the c->item reference */
        settings.engine.v1->release(settings.engine.v0, c, c->item);
        c->item = 0;
    }
}

static void process_bin_get(conn *c) {
    item *it;
    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint16_t keylen;
    uint32_t bodylen;
    item_info_holder info;
    int ii;
    ENGINE_ERROR_CODE ret;
    memset(&info, 0, sizeof(info));

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "GET", key, nkey) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s\n",
                                            buffer);
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    if (ret == ENGINE_SUCCESS) {
        ret = settings.engine.v1->get(settings.engine.v0, c, &it, key, nkey,
                                      c->binary_header.request.vbucket);
    }

    info.info.nvalue = IOV_MAX;
    switch (ret) {
    case ENGINE_SUCCESS:
        if (!settings.engine.v1->get_item_info(settings.engine.v0, c, it,
                                               (void*)&info)) {
            settings.engine.v1->release(settings.engine.v0, c, it);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to get item info\n",
                                            c->sfd);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
            break;
        }

        keylen = 0;
        bodylen = sizeof(rsp->message.body) + info.info.nbytes;

        STATS_HIT(c, get, key, nkey);

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            bodylen += nkey;
            keylen = nkey;
        }

        if (add_bin_header(c, 0, sizeof(rsp->message.body),
                           keylen, bodylen) == -1) {
            conn_set_state(c, conn_closing);
            return;
        }
        rsp->message.header.response.cas = htonll(info.info.cas);

        /* add the flags */
        rsp->message.body.flags = info.info.flags;
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            add_iov(c, info.info.key, nkey);
        }

        for (ii = 0; ii < info.info.nvalue; ++ii) {
            add_iov(c, info.info.value[ii].iov_base,
                    info.info.value[ii].iov_len);
        }
        conn_set_state(c, conn_mwrite);
        /* Remember this item so we can garbage collect it later */
        c->item = it;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, get, key, nkey);

        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);

        if (c->noreply) {
            conn_set_state(c, conn_new_cmd);
        } else {
            if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                if (add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                                   0, nkey, nkey) == -1) {
                    conn_set_state(c, conn_closing);
                    return;
                }
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                conn_set_state(c, conn_mwrite);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            }
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
    }

    if (settings.detail_enabled && ret != ENGINE_EWOULDBLOCK) {
        stats_prefix_record_get(key, nkey, ret == ENGINE_SUCCESS);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header;

    memset(&header, 0, sizeof(header));
    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = PROTOCOL_BINARY_CMD_STAT;
    header.response.keylen = (uint16_t)htons(klen);
    header.response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header.response.bodylen = htonl(bodylen);
    header.response.opaque = c->opaque;

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->dynamic_buffer.offset += sizeof(header.response) + bodylen;
}

static bool grow_dynamic_buffer(conn *c, size_t needed) {
    size_t nsize = c->dynamic_buffer.size;
    size_t available = nsize - c->dynamic_buffer.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->dynamic_buffer.buffer == NULL) {
        nsize = 1024;
        available = c->dynamic_buffer.size = c->dynamic_buffer.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->dynamic_buffer.offset;
    }

    if (nsize != c->dynamic_buffer.size) {
        char *ptr = realloc(c->dynamic_buffer.buffer, nsize);
        if (ptr) {
            c->dynamic_buffer.buffer = ptr;
            c->dynamic_buffer.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

static void append_stats(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie)
{
    size_t needed;
    conn *c = (conn*)cookie;
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    needed = vlen + klen + sizeof(protocol_binary_response_header);
    if (!grow_dynamic_buffer(c, needed)) {
        return ;
    }
    append_bin_stats(key, klen, val, vlen, c);
    assert(c->dynamic_buffer.offset <= c->dynamic_buffer.size);
}

static void process_bin_stat(conn *c) {
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    ENGINE_ERROR_CODE ret;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "STATS", subcommand, nkey) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s\n",
                                            buffer);
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        if (nkey == 0) {
            /* request all statistics */
            ret = settings.engine.v1->get_stats(settings.engine.v0, c, NULL, 0, append_stats);
            if (ret == ENGINE_SUCCESS) {
                server_stats(&append_stats, c, false);
            }
        } else if (strncmp(subcommand, "reset", 5) == 0) {
            stats_reset(c);
            settings.engine.v1->reset_stats(settings.engine.v0, c);
        } else if (strncmp(subcommand, "settings", 8) == 0) {
            process_stat_settings(&append_stats, c);
        } else if (strncmp(subcommand, "cachedump", 9) == 0) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
            return;
        } else if (strncmp(subcommand, "detail", 6) == 0) {
            char *subcmd_pos = subcommand + 6;
            if (settings.allow_detailed) {
                if (strncmp(subcmd_pos, " dump", 5) == 0) {
                    int len;
                    char *dump_buf = stats_prefix_dump(&len);
                    if (dump_buf == NULL || len <= 0) {
                        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
                        return ;
                    } else {
                        append_stats("detailed", strlen("detailed"), dump_buf, len, c);
                        free(dump_buf);
                    }
                } else if (strncmp(subcmd_pos, " on", 3) == 0) {
                    settings.detail_enabled = 1;
                } else if (strncmp(subcmd_pos, " off", 4) == 0) {
                    settings.detail_enabled = 0;
                } else {
                    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
                    return;
                }
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
                return;
            }
        } else if (strncmp(subcommand, "aggregate", 9) == 0) {
            server_stats(&append_stats, c, true);
        } else if (strncmp(subcommand, "connections", 11) == 0) {
            connection_stats(&append_stats, c);
        } else {
            ret = settings.engine.v1->get_stats(settings.engine.v0, c,
                                                subcommand, nkey,
                                                append_stats);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        append_stats(NULL, 0, NULL, 0, c);
        write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
        c->dynamic_buffer.buffer = NULL;
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
}

static void bin_read_chunk(conn *c,
                           enum bin_substates next_substate,
                           uint32_t chunk) {
    ptrdiff_t offset;
    assert(c);
    c->substate = next_substate;
    c->rlbytes = chunk;

    /* Ok... do we have room for everything in our buffer? */
    offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
    if (c->rlbytes > c->rsize - offset) {
        size_t nsize = c->rsize;
        size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->rsize) {
            char *newm;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                        "%d: Need to grow buffer from %lu to %lu\n",
                        c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
            }
            newm = realloc(c->rbuf, nsize);
            if (newm == NULL) {
                if (settings.verbose) {
                    settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                            "%d: Failed to grow buffer.. closing connection\n",
                            c->sfd);
                }
                conn_set_state(c, conn_closing);
                return;
            }

            c->rbuf= newm;
            /* rcurr should point to the same offset in the packet */
            c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
            c->rsize = nsize;
        }
        if (c->rbuf != c->rcurr) {
            memmove(c->rbuf, c->rcurr, c->rbytes);
            c->rcurr = c->rbuf;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%d: Repack input buffer\n",
                                                c->sfd);
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
    conn_set_state(c, conn_nread);
}

static void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
    bin_read_chunk(c, next_substate, c->keylen + extra);
}


/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(conn *c) {
    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    if (settings.verbose) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                "%d: Protocol error (opcode %02x), close connection\n",
                c->sfd, c->binary_header.request.opcode);
    }
    c->write_and_go = conn_closing;
}

static void get_auth_data(const void *cookie, auth_data_t *data) {
    conn *c = (conn*)cookie;
    if (c->sasl_conn) {
        cbsasl_getprop(c->sasl_conn, CBSASL_USERNAME, (void*)&data->username);
        cbsasl_getprop(c->sasl_conn, CBSASL_CONFIG, (void*)&data->config);
    }
}

static void bin_list_sasl_mechs(conn *c) {
    const char *result_string = NULL;
    unsigned int string_length = 0;

    if (cbsasl_list_mechs(&result_string, &string_length) != SASL_OK) {
        /* Perhaps there's a better error for this... */
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                     "%d: Failed to list SASL mechanisms.\n",
                     c->sfd);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }
    write_bin_response(c, (char*)result_string, 0, 0, string_length);
}

struct sasl_tmp {
    int ksize;
    int vsize;
    char data[1]; /* data + ksize == value */
};

static void process_bin_sasl_auth(conn *c) {
    int nkey;
    int vlen;
    char *key;
    size_t buffer_size;
    struct sasl_tmp *data;

    assert(c->binary_header.request.extlen == 0);
    nkey = c->binary_header.request.keylen;
    vlen = c->binary_header.request.bodylen - nkey;

    if (nkey > MAX_SASL_MECH_LEN) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    key = binary_get_key(c);
    assert(key);

    buffer_size = sizeof(struct sasl_tmp) + nkey + vlen + 2;
    data = calloc(sizeof(struct sasl_tmp) + buffer_size, 1);
    if (!data) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    data->ksize = nkey;
    data->vsize = vlen;
    memcpy(data->data, key, nkey);

    c->item = data;
    c->ritem = data->data + nkey;
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_reading_sasl_auth_data;
}

static void process_bin_complete_sasl_auth(conn *c) {
    auth_data_t data;
    const char *out = NULL;
    unsigned int outlen = 0;
    int nkey;
    int vlen;
    struct sasl_tmp *stmp;
    char mech[1024];
    const char *challenge;
    int result=-1;

    assert(c->item);

    nkey = c->binary_header.request.keylen;
    if (nkey > 1023) {
        /* too big.. */
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }
    vlen = c->binary_header.request.bodylen - nkey;

    stmp = c->item;
    memcpy(mech, stmp->data, nkey);
    mech[nkey] = 0x00;

    if (settings.verbose) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                "%d: mech: ``%s'' with %d bytes of data\n", c->sfd, mech, vlen);
    }

    challenge = vlen == 0 ? NULL : (stmp->data + nkey);
    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        result = cbsasl_server_start(&c->sasl_conn, mech,
                                     challenge, vlen,
                                     (unsigned char **)&out, &outlen);
        break;
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        result = cbsasl_server_step(c->sasl_conn, challenge,
                                    vlen, &out, &outlen);
        break;
    default:
        assert(false); /* CMD should be one of the above */
        /* This code is pretty much impossible, but makes the compiler
           happier */
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                    "%d: Unhandled command %d with challenge %s\n",
                    c->sfd, c->cmd, challenge);
        }
        break;
    }

    free(c->item);
    c->item = NULL;
    c->ritem = NULL;

    if (settings.verbose) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                        "%d: sasl result code:  %d\n",
                                        c->sfd, result);
    }

    switch(result) {
    case SASL_OK:
        write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
        get_auth_data(c, &data);
        perform_callbacks(ON_AUTH, (const void*)&data, c);
        STATS_NOKEY(c, auth_cmds);
        break;
    case SASL_CONTINUE:
        if (add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0,
                           outlen) == -1) {
            conn_set_state(c, conn_closing);
            return;
        }
        add_iov(c, out, outlen);
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    case SASL_BADPARAM:
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                            "%d: Bad sasl params:  %d\n",
                                            c->sfd, result);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
        STATS_NOKEY2(c, auth_cmds, auth_errors);
        break;
    default:
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                            "%d: Unknown sasl response:  %d\n",
                                            c->sfd, result);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        STATS_NOKEY2(c, auth_cmds, auth_errors);
    }
}

static bool authenticated(conn *c) {
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
        rv = true;
        break;
    default:
        if (c->sasl_conn) {
            const void *uname = NULL;
            cbsasl_getprop(c->sasl_conn, CBSASL_USERNAME, &uname);
            rv = uname != NULL;
        }
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                "%d: authenticated() in cmd 0x%02x is %s\n",
                c->sfd, c->cmd, rv ? "true" : "false");
    }

    return rv;
}

static bool binary_response_handler(const void *key, uint16_t keylen,
                                    const void *ext, uint8_t extlen,
                                    const void *body, uint32_t bodylen,
                                    uint8_t datatype, uint16_t status,
                                    uint64_t cas, const void *cookie)
{
    protocol_binary_response_header header;
    char *buf;
    conn *c = (conn*)cookie;
    /* Look at append_bin_stats */
    size_t needed = keylen + extlen + bodylen + sizeof(protocol_binary_response_header);
    if (!grow_dynamic_buffer(c, needed)) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                    "<%d ERROR: Failed to allocate memory for response\n",
                    c->sfd);
        }
        return false;
    }

    buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    memset(&header, 0, sizeof(header));
    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = c->binary_header.request.opcode;
    header.response.keylen = (uint16_t)htons(keylen);
    header.response.extlen = extlen;
    header.response.datatype = datatype;
    header.response.status = (uint16_t)htons(status);
    header.response.bodylen = htonl(bodylen + keylen + extlen);
    header.response.opaque = c->opaque;
    header.response.cas = htonll(cas);

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (extlen > 0) {
        memcpy(buf, ext, extlen);
        buf += extlen;
    }

    if (keylen > 0) {
        memcpy(buf, key, keylen);
        buf += keylen;
    }

    if (bodylen > 0) {
        memcpy(buf, body, bodylen);
    }

    c->dynamic_buffer.offset += needed;

    return true;
}

/**
 * Tap stats (these are only used by the tap thread, so they don't need
 * to be in the threadlocal struct right now...
 */
struct tap_cmd_stats {
    uint64_t connect;
    uint64_t mutation;
    uint64_t checkpoint_start;
    uint64_t checkpoint_end;
    uint64_t delete;
    uint64_t flush;
    uint64_t opaque;
    uint64_t vbucket_set;
};

struct tap_stats {
    cb_mutex_t mutex;
    struct tap_cmd_stats sent;
    struct tap_cmd_stats received;
} tap_stats;

static void ship_tap_log(conn *c) {
    bool more_data = true;
    bool send_data = false;
    bool disconnect = false;
    item *it;
    uint32_t bodylen;
    int ii = 0;

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to create output headers. Shutting down tap connection\n", c->sfd);
        }
        conn_set_state(c, conn_closing);
        return ;
    }
    /* @todo add check for buffer overflow of c->wbuf) */
    c->wcurr = c->wbuf;
    c->icurr = c->ilist;
    do {
        /* @todo fixme! */
        void *engine;
        uint16_t nengine;
        uint8_t ttl;
        uint16_t tap_flags;
        uint32_t seqno;
        uint16_t vbucket;
        tap_event_t event;

        union {
            protocol_binary_request_tap_mutation mutation;
            protocol_binary_request_tap_delete delete;
            protocol_binary_request_tap_flush flush;
            protocol_binary_request_tap_opaque opaque;
            protocol_binary_request_noop noop;
        } msg;
        item_info_holder info;
        memset(&info, 0, sizeof(info));

        if (ii++ == 10) {
            break;
        }

        event = c->tap_iterator(settings.engine.v0, c, &it,
                                            &engine, &nengine, &ttl,
                                            &tap_flags, &seqno, &vbucket);
        memset(&msg, 0, sizeof(msg));
        msg.opaque.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
        msg.opaque.message.header.request.opaque = htonl(seqno);
        msg.opaque.message.body.tap.enginespecific_length = htons(nengine);
        msg.opaque.message.body.tap.ttl = ttl;
        msg.opaque.message.body.tap.flags = htons(tap_flags);
        msg.opaque.message.header.request.extlen = 8;
        msg.opaque.message.header.request.vbucket = htons(vbucket);
        info.info.nvalue = IOV_MAX;

        switch (event) {
        case TAP_NOOP :
            send_data = true;
            msg.noop.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
            msg.noop.message.header.request.extlen = 0;
            msg.noop.message.header.request.bodylen = htonl(0);
            memcpy(c->wcurr, msg.noop.bytes, sizeof(msg.noop.bytes));
            add_iov(c, c->wcurr, sizeof(msg.noop.bytes));
            c->wcurr += sizeof(msg.noop.bytes);
            c->wbytes += sizeof(msg.noop.bytes);
            break;
        case TAP_PAUSE :
            more_data = false;
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
        case TAP_MUTATION:
            if (!settings.engine.v1->get_item_info(settings.engine.v0, c, it,
                                                   (void*)&info)) {
                settings.engine.v1->release(settings.engine.v0, c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%d: Failed to get item info\n", c->sfd);
                break;
            }
            send_data = true;
            c->ilist[c->ileft++] = it;

            if (event == TAP_CHECKPOINT_START) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START;
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.checkpoint_start++;
                cb_mutex_exit(&tap_stats.mutex);
            } else if (event == TAP_CHECKPOINT_END) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END;
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.checkpoint_end++;
                cb_mutex_exit(&tap_stats.mutex);
            } else if (event == TAP_MUTATION) {
                msg.mutation.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_MUTATION;
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.mutation++;
                cb_mutex_exit(&tap_stats.mutex);
            }

            msg.mutation.message.header.request.cas = htonll(info.info.cas);
            msg.mutation.message.header.request.keylen = htons(info.info.nkey);
            msg.mutation.message.header.request.extlen = 16;

            bodylen = 16 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                bodylen += info.info.nbytes;
            }
            msg.mutation.message.header.request.bodylen = htonl(bodylen);

            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                msg.mutation.message.body.item.flags = htonl(info.info.flags);
            } else {
                msg.mutation.message.body.item.flags = info.info.flags;
            }
            msg.mutation.message.body.item.expiration = htonl(info.info.exptime);
            msg.mutation.message.body.tap.enginespecific_length = htons(nengine);
            msg.mutation.message.body.tap.ttl = ttl;
            msg.mutation.message.body.tap.flags = htons(tap_flags);
            memcpy(c->wcurr, msg.mutation.bytes, sizeof(msg.mutation.bytes));

            add_iov(c, c->wcurr, sizeof(msg.mutation.bytes));
            c->wcurr += sizeof(msg.mutation.bytes);
            c->wbytes += sizeof(msg.mutation.bytes);

            if (nengine > 0) {
                memcpy(c->wcurr, engine, nengine);
                add_iov(c, c->wcurr, nengine);
                c->wcurr += nengine;
                c->wbytes += nengine;
            }

            add_iov(c, info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                int xx;
                for (xx = 0; xx < info.info.nvalue; ++xx) {
                    add_iov(c, info.info.value[xx].iov_base,
                            info.info.value[xx].iov_len);
                }
            }

            break;
        case TAP_DELETION:
            /* This is a delete */
            if (!settings.engine.v1->get_item_info(settings.engine.v0, c, it,
                                                   (void*)&info)) {
                settings.engine.v1->release(settings.engine.v0, c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%d: Failed to get item info\n", c->sfd);
                break;
            }
            send_data = true;
            c->ilist[c->ileft++] = it;
            msg.delete.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_DELETE;
            msg.delete.message.header.request.cas = htonll(info.info.cas);
            msg.delete.message.header.request.keylen = htons(info.info.nkey);

            bodylen = 8 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                bodylen += info.info.nbytes;
            }
            msg.delete.message.header.request.bodylen = htonl(bodylen);

            memcpy(c->wcurr, msg.delete.bytes, sizeof(msg.delete.bytes));
            add_iov(c, c->wcurr, sizeof(msg.delete.bytes));
            c->wcurr += sizeof(msg.delete.bytes);
            c->wbytes += sizeof(msg.delete.bytes);

            if (nengine > 0) {
                memcpy(c->wcurr, engine, nengine);
                add_iov(c, c->wcurr, nengine);
                c->wcurr += nengine;
                c->wbytes += nengine;
            }

            add_iov(c, info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                int xx;
                for (xx = 0; xx < info.info.nvalue; ++xx) {
                    add_iov(c, info.info.value[xx].iov_base,
                            info.info.value[xx].iov_len);
                }
            }

            cb_mutex_enter(&tap_stats.mutex);
            tap_stats.sent.delete++;
            cb_mutex_exit(&tap_stats.mutex);
            break;

        case TAP_DISCONNECT:
            disconnect = true;
            more_data = false;
            break;
        case TAP_VBUCKET_SET:
        case TAP_FLUSH:
        case TAP_OPAQUE:
            send_data = true;

            if (event == TAP_OPAQUE) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_OPAQUE;
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.opaque++;
                cb_mutex_exit(&tap_stats.mutex);

            } else if (event == TAP_FLUSH) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_FLUSH;
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.flush++;
                cb_mutex_exit(&tap_stats.mutex);
            } else if (event == TAP_VBUCKET_SET) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET;
                msg.flush.message.body.tap.flags = htons(tap_flags);
                cb_mutex_enter(&tap_stats.mutex);
                tap_stats.sent.vbucket_set++;
                cb_mutex_exit(&tap_stats.mutex);
            }

            msg.flush.message.header.request.bodylen = htonl(8 + nengine);
            memcpy(c->wcurr, msg.flush.bytes, sizeof(msg.flush.bytes));
            add_iov(c, c->wcurr, sizeof(msg.flush.bytes));
            c->wcurr += sizeof(msg.flush.bytes);
            c->wbytes += sizeof(msg.flush.bytes);
            if (nengine > 0) {
                memcpy(c->wcurr, engine, nengine);
                add_iov(c, c->wcurr, nengine);
                c->wcurr += nengine;
                c->wbytes += nengine;
            }
            break;
        default:
            abort();
        }
    } while (more_data);

    c->ewouldblock = false;
    if (send_data) {
        conn_set_state(c, conn_mwrite);
        if (disconnect) {
            c->write_and_go = conn_closing;
        } else {
            c->write_and_go = conn_ship_log;
        }
    } else {
        if (disconnect) {
            conn_set_state(c, conn_closing);
        } else {
            /* No more items to ship to the slave at this time.. suspend.. */
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%d: No more items in tap log.. waiting\n",
                                                c->sfd);
            }
            c->ewouldblock = true;
        }
    }
}

static ENGINE_ERROR_CODE default_unknown_command(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                                                 ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response)
{
    return settings.engine.v1->unknown_command(handle, cookie, request, response);
}

struct request_lookup {
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor;
    BINARY_COMMAND_CALLBACK callback;
};

static struct request_lookup request_handlers[0x100];

typedef void (*RESPONSE_HANDLER)(conn*);
/**
 * A map between the response packets op-code and the function to handle
 * the response message.
 */
static RESPONSE_HANDLER response_handlers[0x100];

static void setup_binary_lookup_cmd(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                                    uint8_t cmd,
                                    BINARY_COMMAND_CALLBACK new_handler) {
    request_handlers[cmd].descriptor = descriptor;
    request_handlers[cmd].callback = new_handler;
}

static void process_bin_unknown_packet(conn *c) {
    void *packet = c->rcurr - (c->binary_header.request.bodylen +
                               sizeof(c->binary_header));
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        struct request_lookup *rq = request_handlers + c->binary_header.request.opcode;
        ret = rq->callback(rq->descriptor, settings.engine.v0, c, packet,
                           binary_response_handler);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (c->dynamic_buffer.buffer != NULL) {
            write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
            c->dynamic_buffer.buffer = NULL;
        } else {
            conn_set_state(c, conn_new_cmd);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        conn_set_state(c, conn_closing);
        break;
    default:
        /* Release the dynamic buffer.. it may be partial.. */
        free(c->dynamic_buffer.buffer);
        c->dynamic_buffer.buffer = NULL;
        write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
    }
}

static void *cbsasl_refresh_main(void *c)
{
    int rv = cbsasl_server_refresh();
    if (rv == SASL_OK) {
        notify_io_complete(c, ENGINE_SUCCESS);
    } else {
        notify_io_complete(c, ENGINE_EINVAL);
    }

    return NULL;
}

static ENGINE_ERROR_CODE refresh_cbsasl(conn *c)
{
    cb_thread_t tid;
    int err;

    err = cb_create_thread(&tid, cbsasl_refresh_main, c, 1);
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Failed to create cbsasl db "
                                        "update thread: %s",
                                        strerror(err));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
}

static void process_bin_tap_connect(conn *c) {
    TAP_ITERATOR iterator;
    char *packet = (c->rcurr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));
    protocol_binary_request_tap_connect *req = (void*)packet;
    const char *key = packet + sizeof(req->bytes);
    const char *data = key + c->binary_header.request.keylen;
    uint32_t flags = 0;
    size_t ndata = c->binary_header.request.bodylen -
        c->binary_header.request.extlen -
        c->binary_header.request.keylen;

    if (c->binary_header.request.extlen == 4) {
        flags = ntohl(req->message.body.flags);

        if (flags & TAP_CONNECT_FLAG_BACKFILL) {
            /* the userdata has to be at least 8 bytes! */
            if (ndata < 8) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%d: ERROR: Invalid tap connect message\n",
                                                c->sfd);
                conn_set_state(c, conn_closing);
                return ;
            }
        }
    } else {
        data -= 4;
        key -= 4;
    }

    if (settings.verbose && c->binary_header.request.keylen > 0) {
        char buffer[1024];
        int len = c->binary_header.request.keylen;
        if (len >= sizeof(buffer)) {
            len = sizeof(buffer) - 1;
        }
        memcpy(buffer, key, len);
        buffer[len] = '\0';
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "%d: Trying to connect with named tap connection: <%s>\n",
                                        c->sfd, buffer);
    }

    iterator = settings.engine.v1->get_tap_iterator(
        settings.engine.v0, c, key, c->binary_header.request.keylen,
        flags, data, ndata);

    if (iterator == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: FATAL: The engine does not support tap\n",
                                        c->sfd);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        c->write_and_go = conn_closing;
    } else {
        c->tap_iterator = iterator;
        c->which = EV_WRITE;
        conn_set_state(c, conn_ship_log);
    }
}

static void process_bin_tap_packet(tap_event_t event, conn *c) {
    char *packet;
    protocol_binary_request_tap_no_extras *tap;
    uint16_t nengine;
    uint16_t tap_flags;
    uint32_t seqno;
    uint8_t ttl;
    char *engine_specific;
    char *key;
    uint16_t nkey;
    char *data;
    uint32_t flags;
    uint32_t exptime;
    uint32_t ndata;
    ENGINE_ERROR_CODE ret;

    assert(c != NULL);
    packet = (c->rcurr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));
    tap = (void*)packet;
    nengine = ntohs(tap->message.body.tap.enginespecific_length);
    tap_flags = ntohs(tap->message.body.tap.flags);
    seqno = ntohl(tap->message.header.request.opaque);
    ttl = tap->message.body.tap.ttl;
    engine_specific = packet + sizeof(tap->bytes);
    key = engine_specific + nengine;
    nkey = c->binary_header.request.keylen;
    data = key + nkey;
    flags = 0;
    exptime = 0;
    ndata = c->binary_header.request.bodylen - nengine - nkey - 8;
    ret = c->aiostat;

    if (ttl == 0) {
        ret = ENGINE_EINVAL;
    } else {
        if (event == TAP_MUTATION || event == TAP_CHECKPOINT_START ||
            event == TAP_CHECKPOINT_END) {
            protocol_binary_request_tap_mutation *mutation = (void*)tap;

            /* engine_specific data in protocol_binary_request_tap_mutation is */
            /* at a different offset than protocol_binary_request_tap_no_extras */
            engine_specific = packet + sizeof(mutation->bytes);

            flags = mutation->message.body.item.flags;
            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                flags = ntohl(flags);
            }

            exptime = ntohl(mutation->message.body.item.expiration);
            key += 8;
            data += 8;
            ndata -= 8;
        }

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->tap_notify(settings.engine.v0, c,
                                                 engine_specific, nengine,
                                                 ttl - 1, tap_flags,
                                                 event, seqno,
                                                 key, nkey,
                                                 flags, exptime,
                                                 ntohll(tap->message.header.request.cas),
                                                 data, ndata,
                                                 c->binary_header.request.vbucket);
        }
    }

    switch (ret) {
    case ENGINE_DISCONNECT:
        conn_set_state(c, conn_closing);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        if ((tap_flags & TAP_FLAG_ACK) || (ret != ENGINE_SUCCESS)) {
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        } else {
            conn_set_state(c, conn_new_cmd);
        }
    }
}

static void process_bin_tap_ack(conn *c) {
    char *packet;
    protocol_binary_response_no_extras *rsp;
    uint32_t seqno;
    uint16_t status;
    char *key;
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    assert(c != NULL);
    packet = (c->rcurr - (c->binary_header.request.bodylen + sizeof(c->binary_header)));
    rsp = (void*)packet;
    seqno = ntohl(rsp->message.header.response.opaque);
    status = ntohs(rsp->message.header.response.status);
    key = packet + sizeof(rsp->bytes);

    if (settings.engine.v1->tap_notify != NULL) {
        ret = settings.engine.v1->tap_notify(settings.engine.v0, c, NULL, 0, 0, status,
                                             TAP_ACK, seqno, key,
                                             c->binary_header.request.keylen, 0, 0,
                                             0, NULL, 0, 0);
    }

    if (ret == ENGINE_DISCONNECT) {
        conn_set_state(c, conn_closing);
    } else {
        conn_set_state(c, conn_ship_log);
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(conn *c) {
    assert(c != NULL);
    conn_set_state(c, conn_new_cmd);
}

/*******************************************************************************
 **                             UPR MESSAGE PRODUCERS                         **
 ******************************************************************************/
static ENGINE_ERROR_CODE upr_message_stream_start(const void *cookie,
                                                  uint32_t opaque,
                                                  uint16_t vbucket)
{
    protocol_binary_request_upr_stream_start packet;
    conn *c = (void*)cookie;
    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_START;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_stream_end(const void *cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags)
{
    protocol_binary_request_upr_stream_end packet;
    conn *c = (void*)cookie;

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_END;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.bodylen = htonl(4);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_snapshot_start(const void *cookie,
                                                    uint32_t opaque,
                                                    uint16_t vbucket)
{
    protocol_binary_request_upr_snapshot_start packet;
    conn *c = (void*)cookie;

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_START;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_snapshot_end(const void *cookie,
                                                  uint32_t opaque,
                                                  uint16_t vbucket)
{
    protocol_binary_request_upr_snapshot_end packet;
    conn *c = (void*)cookie;

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_END;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_mutation(const void* cookie,
                                              uint32_t opaque,
                                              item *it,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              uint32_t lock_time)
{
    conn *c = (void*)cookie;
    item_info_holder info;
    protocol_binary_request_upr_mutation packet;
    int xx;

    memset(&info, 0, sizeof(info));
    info.info.nvalue = IOV_MAX;

    if (!settings.engine.v1->get_item_info(settings.engine.v0, c, it,
                                           (void*)&info)) {
        settings.engine.v1->release(settings.engine.v0, c, it);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: Failed to get item info\n", c->sfd);
        return ENGINE_FAILED;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.info.cas);
    packet.message.header.request.keylen = htons(info.info.nkey);
    packet.message.header.request.extlen = 28;
    packet.message.header.request.bodylen = ntohl(28 + info.info.nkey + info.info.nbytes);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.lock_time = htonl(lock_time);
    packet.message.body.flags = htonl(info.info.flags);
    packet.message.body.expiration = htonl(info.info.exptime);

    c->ilist[c->ileft++] = it;

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);
    add_iov(c, info.info.key, info.info.nkey);
    for (xx = 0; xx < info.info.nvalue; ++xx) {
        add_iov(c, info.info.value[xx].iov_base, info.info.value[xx].iov_len);
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_deletion(const void* cookie,
                                              uint32_t opaque,
                                              const void *key,
                                              uint16_t nkey,
                                              uint64_t cas,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno)
{
    conn *c = (void*)cookie;
    protocol_binary_request_upr_deletion packet;
    if (c->wbytes + sizeof(packet.bytes) + nkey >= c->wsize) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_DELETION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 16;
    packet.message.header.request.bodylen = ntohl(16 + nkey);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);

    add_iov(c, c->wcurr, sizeof(packet.bytes) + nkey);
    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);
    memcpy(c->wcurr, key, nkey);
    c->wcurr += nkey;
    c->wbytes += nkey;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_expiration(const void* cookie,
                                                uint32_t opaque,
                                                const void *key,
                                                uint16_t nkey,
                                                uint64_t cas,
                                                uint16_t vbucket,
                                                uint64_t by_seqno,
                                                uint64_t rev_seqno)
{
    conn *c = (void*)cookie;
    protocol_binary_request_upr_deletion packet;

    if (c->wbytes + sizeof(packet.bytes) + nkey >= c->wsize) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_EXPIRATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 16;
    packet.message.header.request.bodylen = ntohl(16 + nkey);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);

    add_iov(c, c->wcurr, sizeof(packet.bytes) + nkey);
    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);
    memcpy(c->wcurr, key, nkey);
    c->wcurr += nkey;
    c->wbytes += nkey;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_flush(const void* cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket)
{
    protocol_binary_request_upr_flush packet;
    conn *c = (void*)cookie;

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_FLUSH;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE upr_message_set_vbucket_state(const void* cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket,
                                                       vbucket_state_t state)
{
    protocol_binary_request_upr_set_vbucket_state packet;
    conn *c = (void*)cookie;

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE;
    packet.message.header.request.extlen = 1;
    packet.message.header.request.bodylen = htonl(1);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    switch (state) {
    case vbucket_state_active:
        packet.message.body.state = 0x01;
        break;
    case vbucket_state_pending:
        packet.message.body.state = 0x02;
        break;
    case vbucket_state_replica:
        packet.message.body.state = 0x03;
        break;
    case vbucket_state_dead:
        packet.message.body.state = 0x04;
        break;
    default:
        return ENGINE_EINVAL;
    }

    memcpy(c->wcurr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->wcurr, sizeof(packet.bytes));
    c->wcurr += sizeof(packet.bytes);
    c->wbytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static void ship_upr_log(conn *c) {
    static struct upr_message_producers producers = {
        upr_message_stream_start,
        upr_message_stream_end,
        upr_message_snapshot_start,
        upr_message_snapshot_end,
        upr_message_mutation,
        upr_message_deletion,
        upr_message_expiration,
        upr_message_flush,
        upr_message_set_vbucket_state
    };
    ENGINE_ERROR_CODE ret;

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d: Failed to create output headers. Shutting down UPR connection\n", c->sfd);
        }
        conn_set_state(c, conn_closing);
        return ;
    }

    c->wcurr = c->wbuf;
    c->icurr = c->ilist;



    ret = settings.engine.v1->upr.step(settings.engine.v0, c, &producers);
    if (ret == ENGINE_SUCCESS) {
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_ship_log;
    } else {
        conn_set_state(c, conn_closing);
    }
}

/******************************************************************************
 *                        TAP packet executors                                *
 ******************************************************************************/
static void tap_connect_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.connect++;
    cb_mutex_exit(&tap_stats.mutex);
    conn_set_state(c, conn_setup_tap_stream);
}

static void tap_mutation_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.mutation++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_MUTATION, c);
}

static void tap_delete_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.delete++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_DELETION, c);
}

static void tap_flush_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.flush++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_FLUSH, c);
}

static void tap_opaque_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.opaque++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_OPAQUE, c);
}

static void tap_vbucket_set_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.vbucket_set++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_VBUCKET_SET, c);
}

static void tap_checkpoint_start_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.checkpoint_start++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_CHECKPOINT_START, c);
}

static void tap_checkpoint_end_executor(conn *c, void *packet)
{
    cb_mutex_enter(&tap_stats.mutex);
    tap_stats.received.checkpoint_end++;
    cb_mutex_exit(&tap_stats.mutex);
    process_bin_tap_packet(TAP_CHECKPOINT_END, c);
}

/*******************************************************************************
 *                        UPR packet validators                                *
 ******************************************************************************/
static int upr_stream_req_validator(void *packet)
{
    protocol_binary_request_upr_stream_req *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4*sizeof(uint64_t) + 2*sizeof(uint32_t) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.body.flags != 0) { /* none specified yet */
        /* INCORRECT FORMAT */
        return -1;
    }

    return 0;
}

static int upr_get_failover_log_validator(void *packet)
{
    protocol_binary_request_upr_get_failover_log *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_stream_start_validator(void *packet)
{
    protocol_binary_request_upr_stream_start *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_stream_end_validator(void *packet)
{
    protocol_binary_request_upr_stream_start *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_snapshot_start_validator(void *packet)
{
    protocol_binary_request_upr_snapshot_start *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_snapshot_end_validator(void *packet)
{
    protocol_binary_request_upr_snapshot_end *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_mutation_validator(void *packet)
{
    protocol_binary_request_upr_mutation *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + 3 * sizeof(uint32_t)) ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.bodylen == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_deletion_validator(void *packet)
{
    protocol_binary_request_upr_deletion *req = packet;
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t bodylen = ntohl(req->message.header.request.bodylen) - klen;
    bodylen -= req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + 3 * sizeof(uint32_t)) ||
        req->message.header.request.keylen == 0 ||
        bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_expiration_validator(void *packet)
{
    protocol_binary_request_upr_deletion *req = packet;
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t bodylen = ntohl(req->message.header.request.bodylen) - klen;
    bodylen -= req->message.header.request.extlen;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + 3 * sizeof(uint32_t)) ||
        req->message.header.request.keylen == 0 ||
        bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_flush_validator(void *packet)
{
    protocol_binary_request_upr_snapshot_end *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int upr_set_vbucket_state_validator(void *packet)
{
    protocol_binary_request_upr_set_vbucket_state *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    if (req->message.body.state < 1 || req->message.body.state > 4) {
        return -1;
    }

    return 0;
}

static int isasl_refresh_validator(void *packet)
{
    protocol_binary_request_no_extras *req = packet;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

/*******************************************************************************
 *                         UPR packet executors                                *
 ******************************************************************************/
static void upr_stream_req_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_stream_req *req = (void*)packet;

    if (settings.engine.v1->upr.stream_req == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);
        uint64_t vbucket_uuid = ntohll(req->message.body.vbucket_uuid);
        uint64_t high_seqno = ntohll(req->message.body.high_seqno);
        uint64_t rollback_seqno;
        char *gid;
        size_t ngid = c->binary_header.request.bodylen - c->binary_header.request.extlen;

        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        gid = (char*)packet + sizeof(req->bytes);

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.stream_req(settings.engine.v0, c,
                                                     gid, ngid, flags,
                                                     c->binary_header.request.opaque,
                                                     c->binary_header.request.vbucket,
                                                     start_seqno, end_seqno,
                                                     vbucket_uuid, high_seqno,
                                                     &rollback_seqno);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->upr = 1;
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            break;

        case ENGINE_ROLLBACK:
            rollback_seqno = htonll(rollback_seqno);
            if (binary_response_handler(NULL, 0, NULL, 0, &rollback_seqno,
                                        sizeof(rollback_seqno), 0,
                                        PROTOCOL_BINARY_RESPONSE_ROLLBACK, 0,
                                        c)) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

/** Callback from the engine adding the response */
static ENGINE_ERROR_CODE add_failover_log(vbucket_failover_t*entries,
                                          size_t nentries,
                                          const void *cookie) {
    ENGINE_ERROR_CODE ret;
    size_t ii;
    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    if (binary_response_handler(NULL, 0, NULL, 0, entries,
                                nentries * sizeof(vbucket_failover_t), 0,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                (void*)cookie)) {
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }

    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    return ret;
}

static void upr_get_failover_log_executor(conn *c, void *packet) {
    protocol_binary_request_upr_get_failover_log *req = (void*)packet;

    if (settings.engine.v1->upr.get_failover_log == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.get_failover_log(settings.engine.v0, c,
                                                           req->message.header.request.opaque,
                                                           ntohs(req->message.header.request.vbucket),
                                                           add_failover_log);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_stream_start_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_stream_start *req = (void*)packet;

    if (settings.engine.v1->upr.stream_start == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.stream_start(settings.engine.v0, c,
                                                       req->message.header.request.opaque,
                                                       ntohs(req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_stream_end_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_stream_end *req = (void*)packet;

    if (settings.engine.v1->upr.stream_end == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.stream_end(settings.engine.v0, c,
                                                     req->message.header.request.opaque,
                                                     ntohs(req->message.header.request.vbucket),
                                                     ntohl(req->message.body.flags));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_snapshot_start_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_snapshot_start *req = (void*)packet;

    if (settings.engine.v1->upr.snapshot_start == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.snapshot_start(settings.engine.v0, c,
                                                         req->message.header.request.opaque,
                                                         ntohs(req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_snapshot_end_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_snapshot_end *req = (void*)packet;

    if (settings.engine.v1->upr.snapshot_end == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.snapshot_end(settings.engine.v0, c,
                                                       req->message.header.request.opaque,
                                                       ntohs(req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_mutation_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_mutation *req = (void*)packet;

    if (settings.engine.v1->upr.mutation == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            char *key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            void *value = key + nkey;
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey
                - req->message.header.request.extlen;
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t flags = ntohl(req->message.body.flags);
            uint8_t datatype = req->message.header.request.datatype;
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint32_t expiration = ntohl(req->message.body.expiration);
            uint32_t lock_time = ntohl(req->message.body.lock_time);
            ret = settings.engine.v1->upr.mutation(settings.engine.v0, c,
                                                   req->message.header.request.opaque,
                                                   key, nkey, value, nvalue, cas, vbucket,
                                                   flags, datatype, by_seqno, rev_seqno,
                                                   expiration, lock_time);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_deletion_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_deletion *req = (void*)packet;

    if (settings.engine.v1->upr.deletion == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            char *key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            ret = settings.engine.v1->upr.deletion(settings.engine.v0, c,
                                                   req->message.header.request.opaque,
                                                   key, nkey, cas, vbucket,
                                                   by_seqno, rev_seqno);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_expiration_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_expiration *req = (void*)packet;

    if (settings.engine.v1->upr.expiration == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            char *key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            ret = settings.engine.v1->upr.expiration(settings.engine.v0, c,
                                                     req->message.header.request.opaque,
                                                     key, nkey, cas, vbucket,
                                                     by_seqno, rev_seqno);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_flush_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_flush *req = (void*)packet;

    if (settings.engine.v1->upr.flush == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->upr.flush(settings.engine.v0, c,
                                                req->message.header.request.opaque,
                                                ntohs(req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            }
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void upr_set_vbucket_state_executor(conn *c, void *packet)
{
    protocol_binary_request_upr_set_vbucket_state *req = (void*)packet;

    if (settings.engine.v1->upr.set_vbucket_state== NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            vbucket_state_t state = (vbucket_state_t)req->message.body.state;
            ret = settings.engine.v1->upr.set_vbucket_state(settings.engine.v0, c,
                                                            c->binary_header.request.opaque,
                                                            c->binary_header.request.vbucket,
                                                            state);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0);
            break;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
        }
    }
}

static void isasl_refresh_executor(conn *c, void *packet)
{
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_cbsasl(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        conn_set_state(c, conn_refresh_cbsasl);
        break;
    case ENGINE_DISCONNECT:
        conn_set_state(c, conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
    }
}

static void verbosity_executor(conn *c, void *packet)
{
    protocol_binary_request_verbosity *req = packet;
    uint32_t level = (uint32_t)ntohl(req->message.body.level);
    if (level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    settings.verbose = (int)level;
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
    write_bin_response(c, NULL, 0, 0, 0);
}

typedef int (*bin_package_validate)(void *packet);
typedef void (*bin_package_execute)(conn *c, void *packet);

bin_package_validate validators[0xff];
bin_package_execute executors[0xff];

static void setup_bin_packet_handlers(void) {
    validators[PROTOCOL_BINARY_CMD_UPR_DELETION] = upr_deletion_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_EXPIRATION] = upr_expiration_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_FLUSH] = upr_flush_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_GET_FAILOVER_LOG] = upr_get_failover_log_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_MUTATION] = upr_mutation_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE] = upr_set_vbucket_state_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_STREAM_END] = upr_stream_end_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_STREAM_REQ] = upr_stream_req_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_END] = upr_snapshot_end_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_START] = upr_snapshot_start_validator;
    validators[PROTOCOL_BINARY_CMD_UPR_STREAM_START] = upr_stream_start_validator;
    validators[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_validator;

    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = tap_checkpoint_end_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = tap_checkpoint_start_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CONNECT] = tap_connect_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_DELETE] = tap_delete_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_FLUSH] = tap_flush_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_MUTATION] = tap_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = tap_opaque_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = tap_vbucket_set_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_DELETION] = upr_deletion_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_EXPIRATION] = upr_expiration_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_FLUSH] = upr_flush_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_GET_FAILOVER_LOG] = upr_get_failover_log_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_MUTATION] = upr_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE] = upr_set_vbucket_state_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_STREAM_END] = upr_stream_end_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_STREAM_REQ] = upr_stream_req_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_END] = upr_snapshot_end_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_STREAM_SNAPSHOT_START] = upr_snapshot_start_executor;
    executors[PROTOCOL_BINARY_CMD_UPR_STREAM_START] = upr_stream_start_executor;
    executors[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_executor;
}

static void process_bin_packet(conn *c) {

    char *packet = (c->rcurr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));

    uint8_t opcode = c->binary_header.request.opcode;
    bin_package_validate validator = validators[opcode];
    bin_package_execute executor = executors[opcode];

    if (validator != NULL && validator(packet) != 0) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    } else if (executor != NULL) {
        executor(c, packet);
    } else {
        process_bin_unknown_packet(c);
    }
}

static void dispatch_bin_command(conn *c) {
    int protocol_error = 0;

    int extlen = c->binary_header.request.extlen;
    uint16_t keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (settings.require_sasl && !authenticated(c)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        c->write_and_go = conn_closing;
        return;
    }

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
    c->noreply = true;

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SETQ:
        c->cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case PROTOCOL_BINARY_CMD_ADDQ:
        c->cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case PROTOCOL_BINARY_CMD_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
        break;
    case PROTOCOL_BINARY_CMD_QUITQ:
        c->cmd = PROTOCOL_BINARY_CMD_QUIT;
        break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
        break;
    case PROTOCOL_BINARY_CMD_APPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;
    case PROTOCOL_BINARY_CMD_GETQ:
        c->cmd = PROTOCOL_BINARY_CMD_GET;
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GETK;
        break;
    default:
        c->noreply = false;
    }

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, get_server_version(),
                                   0, 0, strlen(get_server_version()));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
                bin_read_key(c, bin_reading_set_header, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETK:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                bin_read_key(c, bin_reading_get_key, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                bin_read_key(c, bin_reading_del_header, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_incr_header, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                bin_read_key(c, bin_reading_set_header, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_STAT:
            if (extlen == 0) {
                bin_read_key(c, bin_reading_stat, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_QUIT:
            if (keylen == 0 && extlen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
                c->write_and_go = conn_closing;
                if (c->noreply) {
                    conn_set_state(c, conn_closing);
                }
            } else {
                protocol_error = 1;
            }
            break;
       case PROTOCOL_BINARY_CMD_TAP_CONNECT:
            if (settings.engine.v1->get_tap_iterator == NULL) {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, bodylen);
            } else {
                bin_read_chunk(c, bin_reading_packet,
                               c->binary_header.request.bodylen);
            }
            break;
       case PROTOCOL_BINARY_CMD_TAP_MUTATION:
       case PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START:
       case PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END:
       case PROTOCOL_BINARY_CMD_TAP_DELETE:
       case PROTOCOL_BINARY_CMD_TAP_FLUSH:
       case PROTOCOL_BINARY_CMD_TAP_OPAQUE:
       case PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET:
            if (settings.engine.v1->tap_notify == NULL) {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, bodylen);
            } else {
                bin_read_chunk(c, bin_reading_packet, c->binary_header.request.bodylen);
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                bin_list_sasl_mechs(c);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_AUTH:
        case PROTOCOL_BINARY_CMD_SASL_STEP:
            if (extlen == 0 && keylen != 0) {
                bin_read_key(c, bin_reading_sasl_auth, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_VERBOSITY:
            if (extlen == 4 && keylen == 0 && bodylen == 4) {
                bin_read_chunk(c, bin_reading_packet,
                               c->binary_header.request.bodylen);
            } else {
                protocol_error = 1;
            }
            break;

         case PROTOCOL_BINARY_CMD_ISASL_REFRESH:
            if (extlen != 0 || keylen != 0 || bodylen != 0) {
                protocol_error = 1;
            } else {
                bin_read_chunk(c, bin_reading_packet, 0);
            }
            break;
        default:
            if (settings.engine.v1->unknown_command == NULL) {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
                                bodylen);
            } else {
                bin_read_chunk(c, bin_reading_packet, c->binary_header.request.bodylen);
            }
    }

    if (protocol_error)
        handle_binary_protocol_error(c);
}

static void process_bin_update(conn *c) {
    char *key;
    uint16_t nkey;
    uint32_t vlen;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);
    ENGINE_ERROR_CODE ret;
    item_info_holder info;
    rel_time_t expiration;

    assert(c != NULL);
    memset(&info, 0, sizeof(info));
    info.info.nvalue = 1;
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    req->message.body.flags = req->message.body.flags;
    expiration = ntohl(req->message.body.expiration);

    vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);

    if (settings.verbose > 1) {
        size_t nw;
        char buffer[1024];
        const char *prefix;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            prefix = "ADD";
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            prefix = "SET";
        } else {
            prefix = "REPLACE";
        }

        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                     prefix, key, nkey);

        if (nw != -1) {
            if (snprintf(buffer + nw, sizeof(buffer) - nw,
                         " Value len is %d\n", vlen)) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s",
                                                buffer);
            }
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        ret = settings.engine.v1->allocate(settings.engine.v0, c,
                                           &it, key, nkey,
                                           vlen,
                                           req->message.body.flags,
                                           expiration);
        if (ret == ENGINE_SUCCESS && !settings.engine.v1->get_item_info(settings.engine.v0,
                                                                        c, it,
                                                                        (void*)&info)) {
            settings.engine.v1->release(settings.engine.v0, c, it);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
            return;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        item_set_cas(c, it, c->binary_header.request.cas);

        switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->store_op = OPERATION_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            if (c->binary_header.request.cas != 0) {
                c->store_op = OPERATION_CAS;
            } else {
                c->store_op = OPERATION_SET;
            }
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (c->binary_header.request.cas != 0) {
                c->store_op = OPERATION_CAS;
            } else {
                c->store_op = OPERATION_REPLACE;
            }
            break;
        default:
            assert(0);
        }

        c->item = it;
        c->ritem = info.info.value[0].iov_base;
        c->rlbytes = vlen;
        conn_set_state(c, conn_nread);
        c->substate = bin_read_set_value;
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (ret == ENGINE_E2BIG) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }

        /*
         * Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET (but only if cas matches).
         * Anywhere else too?
         */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            /* @todo fix this for the ASYNC interface! */
            uint64_t cas = 0;
            settings.engine.v1->remove(settings.engine.v0, c, key, nkey,
                                       &cas, c->binary_header.request.vbucket);
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_append_prepend(conn *c) {
    ENGINE_ERROR_CODE ret;
    char *key;
    int nkey;
    int vlen;
    item *it;
    item_info_holder info;
    memset(&info, 0, sizeof(info));
    info.info.nvalue = 1;

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    vlen = c->binary_header.request.bodylen - nkey;

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "Value len is %d\n", vlen);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        ret = settings.engine.v1->allocate(settings.engine.v0, c,
                                           &it, key, nkey,
                                           vlen, 0, 0);
        if (ret == ENGINE_SUCCESS && !settings.engine.v1->get_item_info(settings.engine.v0,
                                                                        c, it,
                                                                        (void*)&info)) {
            settings.engine.v1->release(settings.engine.v0, c, it);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
            return;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        item_set_cas(c, it, c->binary_header.request.cas);

        switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_APPEND:
            c->store_op = OPERATION_APPEND;
            break;
        case PROTOCOL_BINARY_CMD_PREPEND:
            c->store_op = OPERATION_PREPEND;
            break;
        default:
            assert(0);
        }

        c->item = it;
        c->ritem = info.info.value[0].iov_base;
        c->rlbytes = vlen;
        conn_set_state(c, conn_nread);
        c->substate = bin_read_set_value;
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (ret == ENGINE_E2BIG) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_flush(conn *c) {
    ENGINE_ERROR_CODE ret;
    time_t exptime = 0;
    protocol_binary_request_flush* req = binary_get_request(c);

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "%d: flush %ld", c->sfd,
                                        (long)exptime);
    }

    ret = settings.engine.v1->flush(settings.engine.v0, c, exptime);

    if (ret == ENGINE_SUCCESS) {
        write_bin_response(c, NULL, 0, 0, 0);
    } else if (ret == ENGINE_ENOTSUP) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
    STATS_NOKEY(c, cmd_flush);
}

static void process_bin_delete(conn *c) {
    ENGINE_ERROR_CODE ret;
    protocol_binary_request_delete* req = binary_get_request(c);
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint64_t cas = ntohll(req->message.header.request.cas);
    item_info_holder info;
    memset(&info, 0, sizeof(info));

    info.info.nvalue = 1;

    assert(c != NULL);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "DELETE", key, nkey) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s\n",
                                            buffer);
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        if (settings.detail_enabled) {
            stats_prefix_record_delete(key, nkey);
        }
        ret = settings.engine.v1->remove(settings.engine.v0, c, key, nkey,
                                         &cas, c->binary_header.request.vbucket);
    }

    /* For some reason the SLAB_INCR tries to access this... */
    switch (ret) {
    case ENGINE_SUCCESS:
        c->cas = cas;
        write_bin_response(c, NULL, 0, 0, 0);
        SLAB_INCR(c, delete_hits, key, nkey);
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        STATS_INCR(c, delete_misses, key, nkey);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
}

static void complete_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);

    switch(c->substate) {
    case bin_reading_set_header:
        if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
                c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
            process_bin_append_prepend(c);
        } else {
            process_bin_update(c);
        }
        break;
    case bin_read_set_value:
        complete_update_bin(c);
        break;
    case bin_reading_get_key:
        process_bin_get(c);
        break;
    case bin_reading_stat:
        process_bin_stat(c);
        break;
    case bin_reading_del_header:
        process_bin_delete(c);
        break;
    case bin_reading_incr_header:
        complete_incr_bin(c);
        break;
    case bin_read_flush_exptime:
        process_bin_flush(c);
        break;
    case bin_reading_sasl_auth:
        process_bin_sasl_auth(c);
        break;
    case bin_reading_sasl_auth_data:
        process_bin_complete_sasl_auth(c);
        break;
    case bin_reading_packet:
        if (c->binary_header.request.magic == PROTOCOL_BINARY_RES) {
            RESPONSE_HANDLER handler;
            handler = response_handlers[c->binary_header.request.opcode];
            if (handler) {
                handler(c);
            } else {
                settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                       "%d: ERROR: Unsupported response packet received: %u\n",
                        c->sfd, (unsigned int)c->binary_header.request.opcode);
                conn_set_state(c, conn_closing);
            }
        } else {
            process_bin_packet(c);
        }
        break;
    default:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                "Not handling substate %d\n", c->substate);
        abort();
    }
}

static void reset_cmd_handler(conn *c) {
    c->sbytes = 0;
    c->cmd = -1;
    c->substate = bin_no_state;
    if(c->item != NULL) {
        settings.engine.v1->release(settings.engine.v0, c, c->item);
        c->item = NULL;
    }
    conn_shrink(c);
    if (c->rbytes > 0) {
        conn_set_state(c, conn_parse_cmd);
    } else {
        conn_set_state(c, conn_waiting);
    }
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = bytes;
        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
    } else {
        conn_set_state(c, conn_closing);
    }
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name);
    assert(add_stats);
    assert(c);
    assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

static void aggregate_callback(void *in, void *out) {
    threadlocal_stats_aggregate(in, out);
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stats, conn *c, bool aggregate) {
#ifndef WIN32
    struct rusage usage;
    pid_t pid = getpid();
#endif
    struct slab_stats slab_stats;
    char stat_key[1024];
    int i;
    struct tap_stats ts;
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_clear(&thread_stats);

    if (aggregate && settings.engine.v1->aggregate_stats != NULL) {
        settings.engine.v1->aggregate_stats(settings.engine.v0,
                                            (const void *)c,
                                            aggregate_callback,
                                            &thread_stats);
    } else {
        threadlocal_stats_aggregate(get_independent_stats(c),
                                    &thread_stats);
    }

    slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef WIN32
    getrusage(RUSAGE_SELF, &usage);
#endif

    STATS_LOCK();

#ifndef WIN32
    APPEND_STAT("pid", "%lu", (long)pid);
#endif
    APPEND_STAT("uptime", "%u", now);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", get_server_version());
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif

    APPEND_STAT("daemon_connections", "%u", stats.daemon_conns);
    APPEND_STAT("curr_connections", "%u", stats.curr_conns);
    for (i = 0; i < settings.num_ports; ++i) {
        sprintf(stat_key, "%s", "max_conns_on_port_");
        sprintf(stat_key + strlen(stat_key), "%d", stats.listening_ports[i].port);
        APPEND_STAT(stat_key, "%d", stats.listening_ports[i].maxconns);
        sprintf(stat_key, "%s", "curr_conns_on_port_");
        sprintf(stat_key + strlen(stat_key), "%d", stats.listening_ports[i].port);
        APPEND_STAT(stat_key, "%d", stats.listening_ports[i].curr_conns);
    }
    APPEND_STAT("total_connections", "%u", stats.total_conns);
    APPEND_STAT("connection_structures", "%u", stats.conn_structs);
    APPEND_STAT("cmd_get", "%"PRIu64, thread_stats.cmd_get);
    APPEND_STAT("cmd_set", "%"PRIu64, slab_stats.cmd_set);
    APPEND_STAT("cmd_flush", "%"PRIu64, thread_stats.cmd_flush);
    APPEND_STAT("auth_cmds", "%"PRIu64, thread_stats.auth_cmds);
    APPEND_STAT("auth_errors", "%"PRIu64, thread_stats.auth_errors);
    APPEND_STAT("get_hits", "%"PRIu64, slab_stats.get_hits);
    APPEND_STAT("get_misses", "%"PRIu64, thread_stats.get_misses);
    APPEND_STAT("delete_misses", "%"PRIu64, thread_stats.delete_misses);
    APPEND_STAT("delete_hits", "%"PRIu64, slab_stats.delete_hits);
    APPEND_STAT("incr_misses", "%"PRIu64, thread_stats.incr_misses);
    APPEND_STAT("incr_hits", "%"PRIu64, thread_stats.incr_hits);
    APPEND_STAT("decr_misses", "%"PRIu64, thread_stats.decr_misses);
    APPEND_STAT("decr_hits", "%"PRIu64, thread_stats.decr_hits);
    APPEND_STAT("cas_misses", "%"PRIu64, thread_stats.cas_misses);
    APPEND_STAT("cas_hits", "%"PRIu64, slab_stats.cas_hits);
    APPEND_STAT("cas_badval", "%"PRIu64, slab_stats.cas_badval);
    APPEND_STAT("bytes_read", "%"PRIu64, thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%"PRIu64, thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%"PRIu64, settings.maxbytes);
    APPEND_STAT("accepting_conns", "%u",  is_listen_disabled() ? 0 : 1);
    APPEND_STAT("listen_disabled_num", "%"PRIu64, get_listen_disabled_num());
    APPEND_STAT("rejected_conns", "%" PRIu64, (uint64_t)stats.rejected_conns);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%" PRIu64, (uint64_t)thread_stats.conn_yields);
    STATS_UNLOCK();

    APPEND_STAT("tcp_nodelay", "%s", settings.tcp_nodelay ? "enable" : "disable");

    /*
     * Add tap stats (only if non-zero)
     */
    cb_mutex_enter(&tap_stats.mutex);
    ts = tap_stats;
    cb_mutex_exit(&tap_stats.mutex);

    if (ts.sent.connect) {
        APPEND_STAT("tap_connect_sent", "%"PRIu64, ts.sent.connect);
    }
    if (ts.sent.mutation) {
        APPEND_STAT("tap_mutation_sent", "%"PRIu64, ts.sent.mutation);
    }
    if (ts.sent.checkpoint_start) {
        APPEND_STAT("tap_checkpoint_start_sent", "%"PRIu64, ts.sent.checkpoint_start);
    }
    if (ts.sent.checkpoint_end) {
        APPEND_STAT("tap_checkpoint_end_sent", "%"PRIu64, ts.sent.checkpoint_end);
    }
    if (ts.sent.delete) {
        APPEND_STAT("tap_delete_sent", "%"PRIu64, ts.sent.delete);
    }
    if (ts.sent.flush) {
        APPEND_STAT("tap_flush_sent", "%"PRIu64, ts.sent.flush);
    }
    if (ts.sent.opaque) {
        APPEND_STAT("tap_opaque_sent", "%"PRIu64, ts.sent.opaque);
    }
    if (ts.sent.vbucket_set) {
        APPEND_STAT("tap_vbucket_set_sent", "%"PRIu64,
                    ts.sent.vbucket_set);
    }
    if (ts.received.connect) {
        APPEND_STAT("tap_connect_received", "%"PRIu64, ts.received.connect);
    }
    if (ts.received.mutation) {
        APPEND_STAT("tap_mutation_received", "%"PRIu64, ts.received.mutation);
    }
    if (ts.received.checkpoint_start) {
        APPEND_STAT("tap_checkpoint_start_received", "%"PRIu64, ts.received.checkpoint_start);
    }
    if (ts.received.checkpoint_end) {
        APPEND_STAT("tap_checkpoint_end_received", "%"PRIu64, ts.received.checkpoint_end);
    }
    if (ts.received.delete) {
        APPEND_STAT("tap_delete_received", "%"PRIu64, ts.received.delete);
    }
    if (ts.received.flush) {
        APPEND_STAT("tap_flush_received", "%"PRIu64, ts.received.flush);
    }
    if (ts.received.opaque) {
        APPEND_STAT("tap_opaque_received", "%"PRIu64, ts.received.opaque);
    }
    if (ts.received.vbucket_set) {
        APPEND_STAT("tap_vbucket_set_received", "%"PRIu64,
                    ts.received.vbucket_set);
    }
}

static void process_stat_settings(ADD_STAT add_stats, void *c) {
    assert(add_stats);
    APPEND_STAT("maxbytes", "%u", (unsigned int)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("allow_detailed", "%s",
                settings.allow_detailed ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("reqs_per_tap_event", "%d", settings.reqs_per_tap_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                protocol_text(settings.binding_protocol));
    APPEND_STAT("auth_enabled_sasl", "%s", "yes");

    APPEND_STAT("auth_sasl_engine", "%s", "cbsasl");
    APPEND_STAT("auth_required_sasl", "%s", settings.require_sasl ? "yes" : "no");
    APPEND_STAT("item_size_max", "%d", settings.item_size_max);
    {
        EXTENSION_DAEMON_DESCRIPTOR *ptr;
        for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
            APPEND_STAT("extension", "%s", ptr->get_name());
        }
    }

    APPEND_STAT("logger", "%s", settings.extensions.logger->get_name());
    {
        EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *ptr;
        for (ptr = settings.extensions.binary; ptr != NULL; ptr = ptr->next) {
            APPEND_STAT("binary_extension", "%s", ptr->get_name());
        }
    }

    APPEND_STAT("tcp_nodelay", "%s", settings.tcp_nodelay ? "enable" : "disable");
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);

    if (c->protocol == negotiating_prot)  {
        if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
            c->protocol = binary_prot;
        }

        if (settings.verbose > 1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                    "%d: Client using the %s protocol\n", c->sfd,
                    protocol_text(c->protocol));
        }
    }

    if (c->protocol == binary_prot) {
        /* Do we have the complete packet header? */
        if (c->rbytes < sizeof(c->binary_header)) {
            /* need more data! */
            return 0;
        } else {
#ifdef NEED_ALIGN
            if (((long)(c->rcurr)) % 8 != 0) {
                /* must realign input buffer */
                memmove(c->rbuf, c->rcurr, c->rbytes);
                c->rcurr = c->rbuf;
                if (settings.verbose > 1) {
                    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                             "%d: Realign input buffer\n", c->sfd);
                }
            }
#endif
            protocol_binary_request_header* req;
            req = (protocol_binary_request_header*)c->rcurr;

            if (settings.verbose > 1) {
                /* Dump the packet before we convert it to host order */
                char buffer[1024];
                ssize_t nw;
                nw = bytes_to_output_string(buffer, sizeof(buffer), c->sfd,
                                            true, "Read binary protocol data:",
                                            (const char*)req->bytes,
                                            sizeof(req->bytes));
                if (nw != -1) {
                    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                    "%s", buffer);
                }
            }

            c->binary_header = *req;
            c->binary_header.request.keylen = ntohs(req->request.keylen);
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.vbucket = ntohs(req->request.vbucket);
            c->binary_header.request.cas = ntohll(req->request.cas);

            if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ &&
                !(c->binary_header.request.magic == PROTOCOL_BINARY_RES &&
                  response_handlers[c->binary_header.request.opcode])) {
                if (settings.verbose) {
                    if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
                        settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                              "%d: Invalid magic:  %x\n", c->sfd,
                              c->binary_header.request.magic);
                    } else {
                        settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                              "%d: ERROR: Unsupported response packet received: %u\n",
                              c->sfd, (unsigned int)c->binary_header.request.opcode);

                    }
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            c->msgcurr = 0;
            c->msgused = 0;
            c->iovused = 0;
            if (add_msghdr(c) != 0) {
                conn_set_state(c, conn_closing);
                return -1;
            }

            c->cmd = c->binary_header.request.opcode;
            c->keylen = c->binary_header.request.keylen;
            c->opaque = c->binary_header.request.opaque;
            /* clear the returned cas value */
            c->cas = 0;

            dispatch_bin_command(c);

            c->rbytes -= sizeof(c->binary_header);
            c->rcurr += sizeof(c->binary_header);
        }
    } else {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Disconnecting client. Unknown protocol");
        conn_set_state(c, conn_closing);
    }

    return 1;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    int num_allocs = 0;
    assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }

    while (1) {
        int avail;
#ifdef WIN32
        DWORD error;
#else
        int error;
#endif

        if (c->rbytes >= c->rsize) {
            char *new_rbuf;

            if (num_allocs == 4) {
                return gotdata;
            }
            ++num_allocs;
            new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                if (settings.verbose > 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                                    "Couldn't realloc input buffer\n");
                }
                c->rbytes = 0; /* ignore what we read */
                conn_set_state(c, conn_closing);
                return READ_MEMORY_ERROR;
            }
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        avail = c->rsize - c->rbytes;
        res = recv(c->sfd, c->rbuf + c->rbytes, avail, 0);
        if (res > 0) {
            STATS_ADD(c, bytes_read, res);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            return READ_ERROR;
        }
        if (res == -1) {
#ifdef WIN32
            error = WSAGetLastError();
#else
            error = errno;
#endif

            if (is_blocking(error)) {
                break;
            }
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%d Closing connection due to read error: %s",
                                            c->sfd,
                                            strerror(errno));
            return READ_ERROR;
        }
    }
    return gotdata;
}

bool register_event(conn *c, struct timeval *timeout) {
    assert(!c->registered_in_libevent);
    assert(c->sfd != INVALID_SOCKET);

    if (event_add(&c->event, timeout) == -1) {
        log_system_error(EXTENSION_LOG_WARNING,
                         NULL,
                         "Failed to add connection to libevent: %s");
        return false;
    }

    c->registered_in_libevent = true;

    return true;
}

bool unregister_event(conn *c) {
    assert(c->registered_in_libevent);
    assert(c->sfd != INVALID_SOCKET);

    if (event_del(&c->event) == -1) {
        return false;
    }

    c->registered_in_libevent = false;

    return true;
}

bool update_event(conn *c, const int new_flags) {
    struct event_base *base;

    assert(c != NULL);
    base = c->event.ev_base;
    if (c->ev_flags == new_flags) {
        return true;
    }

    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                    "Updated event for %d to read=%s, write=%s\n",
                                    c->sfd, (new_flags & EV_READ ? "yes" : "no"),
                                    (new_flags & EV_WRITE ? "yes" : "no"));

    if (!unregister_event(c)) {
        return false;
    }

    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = new_flags;

    return register_event(c, NULL);
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
    assert(c != NULL);

    while (c->msgcurr < c->msgused &&
           c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }

    if (c->msgcurr < c->msgused) {
#ifdef WIN32
        DWORD error;
#else
        int error;
#endif
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];

        res = sendmsg(c->sfd, m, 0);
#ifdef WIN32
        error = WSAGetLastError();
#else
        error = errno;
#endif
        if (res > 0) {
            STATS_ADD(c, bytes_written, res);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (void*)((unsigned char*)m->msg_iov->iov_base + res);
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }

        if (res == -1 && is_blocking(error)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                            "Couldn't update event\n");
                }
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0) {
            if (res == -1) {
                log_socket_error(EXTENSION_LOG_WARNING, c,
                                 "Failed to write, and not due to blocking: %s");
            } else {
                int ii;
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%d - sendmsg returned 0\n",
                                                c->sfd);
                for (ii = 0; ii < m->msg_iovlen; ++ii) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                    "\t%d - %zu\n",
                                                    c->sfd, m->msg_iov[ii].iov_len);
                }

            }
        }

        conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

bool conn_listening(conn *c)
{
    int sfd;
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);
    int curr_conns;
    int port_conns;
    struct listening_port *port_instance;

    if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1) {
#ifdef WIN32
        DWORD error = WSAGetLastError();
#else
        int error = errno;
#endif

        if (is_emfile(error)) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open connections\n");
            disable_listen();
        } else if (!is_blocking(error)) {
            log_socket_error(EXTENSION_LOG_WARNING, c,
                             "Failed to accept new client: %s");
        }

        return false;
    }

    STATS_LOCK();
    curr_conns = ++stats.curr_conns;
    port_instance = get_listening_port_instance(c->parent_port);
    assert(port_instance);
    port_conns = ++port_instance->curr_conns;
    STATS_UNLOCK();

    if (curr_conns >= settings.maxconns || port_conns >= port_instance->maxconns) {
        STATS_LOCK();
        ++stats.rejected_conns;
        --port_instance->curr_conns;
        STATS_UNLOCK();

        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Too many open connections\n");

        safe_close(sfd);
        return false;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        STATS_LOCK();
        --port_instance->curr_conns;
        STATS_UNLOCK();
        safe_close(sfd);
        return false;
    }

    dispatch_conn_new(sfd, c->parent_port, conn_new_cmd, EV_READ | EV_PERSIST,
                      DATA_BUFFER_SIZE);

    return false;
}

/**
 * Ship tap log to the other end. This state differs with all other states
 * in the way that it support full duplex dialog. We're listening to both read
 * and write events from libevent most of the time. If a read event occurs we
 * switch to the conn_read state to read and execute the input message (that would
 * be an ack message from the other side). If a write event occurs we continue to
 * send tap log to the other end.
 * @param c the tap connection to drive
 * @return true if we should continue to process work for this connection, false
 *              if we should start processing events for other connections.
 */
bool conn_ship_log(conn *c) {
    bool cont = false;
    short mask = EV_READ | EV_PERSIST | EV_WRITE;

    if (c->sfd == INVALID_SOCKET) {
        return false;
    }

    if (c->which & EV_READ || c->rbytes > 0) {
        if (c->rbytes > 0) {
            if (try_read_command(c) == 0) {
                conn_set_state(c, conn_read);
            }
        } else {
            conn_set_state(c, conn_read);
        }

        /* we're going to process something.. let's proceed */
        cont = true;

        /* We have a finite number of messages in the input queue */
        /* so let's process all of them instead of backing off after */
        /* reading a subset of them. */
        /* Why? Because we've got every time we're calling ship_tap_log */
        /* we try to send a chunk of items.. This means that if we end */
        /* up in a situation where we're receiving a burst of nack messages */
        /* we'll only process a subset of messages in our input queue, */
        /* and it will slowly grow.. */
        c->nevents = settings.reqs_per_tap_event;
    } else if (c->which & EV_WRITE) {
        --c->nevents;
        if (c->nevents >= 0) {
            c->ewouldblock = false;
            if (c->upr) {
                ship_upr_log(c);
            } else {
                ship_tap_log(c);
            }
            if (c->ewouldblock) {
                mask = EV_READ | EV_PERSIST;
            } else {
                cont = true;
            }
        }
    }

    if (!update_event(c, mask)) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO,
                                            c, "Couldn't update event\n");
        }
        conn_set_state(c, conn_closing);
    }

    return cont;
}

bool conn_waiting(conn *c) {
    if (!update_event(c, EV_READ | EV_PERSIST)) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                            "Couldn't update event\n");
        }
        conn_set_state(c, conn_closing);
        return true;
    }
    conn_set_state(c, conn_read);
    return false;
}

bool conn_read(conn *c) {
    int res = try_read_network(c);
    switch (res) {
    case READ_NO_DATA_RECEIVED:
        conn_set_state(c, conn_waiting);
        break;
    case READ_DATA_RECEIVED:
        conn_set_state(c, conn_parse_cmd);
        break;
    case READ_ERROR:
        conn_set_state(c, conn_closing);
        break;
    case READ_MEMORY_ERROR: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(conn *c) {
    if (try_read_command(c) == 0) {
        /* wee need more data! */
        conn_set_state(c, conn_waiting);
    }

    return !c->ewouldblock;
}

bool conn_new_cmd(conn *c) {
    /* Only process nreqs at a time to avoid starving other connections */
    --c->nevents;
    if (c->nevents >= 0) {
        reset_cmd_handler(c);
    } else {
        STATS_NOKEY(c, conn_yields);
        if (c->rbytes > 0) {
            /* We have already read in data into the input buffer,
               so libevent will most likely not signal read events
               on the socket (unless more data is available. As a
               hack we should just put in a request to write data,
               because that should be possible ;-)
            */
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_INFO,
                                                    c, "Couldn't update event\n");
                }
                conn_set_state(c, conn_closing);
                return true;
            }
        }
        return false;
    }

    return true;
}

bool conn_swallow(conn *c) {
    ssize_t res;
#ifdef WIN32
    DWORD error;
#else
    int error;
#endif
    /* we are reading sbytes and throwing them away */
    if (c->sbytes == 0) {
        conn_set_state(c, conn_new_cmd);
        return true;
    }

    /* first check if we have leftovers in the conn_read buffer */
    if (c->rbytes > 0) {
        uint32_t tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
        c->sbytes -= tocopy;
        c->rcurr += tocopy;
        c->rbytes -= tocopy;
        return true;
    }

    /*  now try reading from the socket */
    res = recv(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize, 0);
#ifdef WIN32
    error = WSAGetLastError();
#else
    error = errno;
#endif
    if (res > 0) {
        STATS_ADD(c, bytes_read, res);
        c->sbytes -= res;
        return true;
    }
    if (res == 0) { /* end of stream */
        conn_set_state(c, conn_closing);
        return true;
    }
    if (res == -1 && is_blocking(error)) {
        if (!update_event(c, EV_READ | EV_PERSIST)) {
            if (settings.verbose > 0) {
                settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                                "Couldn't update event\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
        return false;
    }

    /* otherwise we have a real error, on which we close the connection */
    if (!is_closed_conn(error)) {
        char msg[80];
        snprintf(msg, sizeof(msg),
                 "%d Failed to read, and not due to blocking (%%s)",
                 (int)c->sfd);

        log_socket_error(EXTENSION_LOG_INFO, c, msg);
    }

    conn_set_state(c, conn_closing);

    return true;
}

bool conn_nread(conn *c) {
    ssize_t res;
#ifdef WIN32
    DWORD error;
#else
    int error;
#endif

    if (c->rlbytes == 0) {
        bool block = c->ewouldblock = false;
        complete_nread(c);
        if (c->ewouldblock) {
            unregister_event(c);
            block = true;
        }
        return !block;
    }
    /* first check if we have leftovers in the conn_read buffer */
    if (c->rbytes > 0) {
        uint32_t tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
        if (c->ritem != c->rcurr) {
            memmove(c->ritem, c->rcurr, tocopy);
        }
        c->ritem += tocopy;
        c->rlbytes -= tocopy;
        c->rcurr += tocopy;
        c->rbytes -= tocopy;
        if (c->rlbytes == 0) {
            return true;
        }
    }

    /*  now try reading from the socket */
    res = recv(c->sfd, c->ritem, c->rlbytes, 0);
#ifdef WIN32
    error = WSAGetLastError();
#else
    error = errno;
#endif
    if (res > 0) {
        STATS_ADD(c, bytes_read, res);
        if (c->rcurr == c->ritem) {
            c->rcurr += res;
        }
        c->ritem += res;
        c->rlbytes -= res;
        return true;
    }
    if (res == 0) { /* end of stream */
        conn_set_state(c, conn_closing);
        return true;
    }

    if (res == -1 && is_blocking(error)) {
        if (!update_event(c, EV_READ | EV_PERSIST)) {
            if (settings.verbose > 0) {
                settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                                "Couldn't update event\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
        return false;
    }

    /* otherwise we have a real error, on which we close the connection */
    if (!is_closed_conn(error)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d Failed to read, and not due to blocking:\n"
                                        "errno: %d %s \n"
                                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                                        c->sfd, errno, strerror(errno),
                                        (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                                        (int)c->rlbytes, (int)c->rsize);
    }
    conn_set_state(c, conn_closing);
    return true;
}

bool conn_write(conn *c) {
    /*
     * We want to write out a simple response. If we haven't already,
     * assemble it into a msgbuf list (this will be a single-entry
     * list for TCP).
     */
    if (c->iovused == 0) {
        if (add_iov(c, c->wcurr, c->wbytes) != 0) {
            if (settings.verbose > 0) {
                settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                                "Couldn't build response\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
    }

    return conn_mwrite(c);
}

bool conn_mwrite(conn *c) {
    switch (transmit(c)) {
    case TRANSMIT_COMPLETE:
        if (c->state == conn_mwrite) {
            while (c->ileft > 0) {
                item *it = *(c->icurr);
                settings.engine.v1->release(settings.engine.v0, c, it);
                c->icurr++;
                c->ileft--;
            }
            while (c->suffixleft > 0) {
                char *suffix = *(c->suffixcurr);
                cache_free(c->thread->suffix_cache, suffix);
                c->suffixcurr++;
                c->suffixleft--;
            }
            /* XXX:  I don't know why this wasn't the general case */
            if(c->protocol == binary_prot) {
                conn_set_state(c, c->write_and_go);
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        } else if (c->state == conn_write) {
            if (c->write_and_free) {
                free(c->write_and_free);
                c->write_and_free = 0;
            }
            conn_set_state(c, c->write_and_go);
        } else {
            if (settings.verbose > 0) {
                settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                                "Unexpected state %d\n", c->state);
            }
            conn_set_state(c, conn_closing);
        }
        break;

    case TRANSMIT_INCOMPLETE:
    case TRANSMIT_HARD_ERROR:
        break;                   /* Continue in state machine. */

    case TRANSMIT_SOFT_ERROR:
        return false;
    }

    return true;
}

bool conn_pending_close(conn *c) {
    assert(c->sfd == INVALID_SOCKET);
    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                    "Awaiting clients to release the cookie (pending close for %p)",
                                    (void*)c);
    /*
     * tell the tap connection that we're disconnecting it now,
     * but give it a grace period
     */
    perform_callbacks(ON_DISCONNECT, NULL, c);

    if (c->refcount > 1) {
        return false;
    }

    conn_set_state(c, conn_immediate_close);
    return true;
}

bool conn_immediate_close(conn *c) {
    struct listening_port *port_instance;
    assert(c->sfd == INVALID_SOCKET);
    settings.extensions.logger->log(EXTENSION_LOG_DETAIL, c,
                                    "Releasing connection %p",
                                    c);

    STATS_LOCK();
    port_instance = get_listening_port_instance(c->parent_port);
    assert(port_instance);
    --port_instance->curr_conns;
    STATS_UNLOCK();

    perform_callbacks(ON_DISCONNECT, NULL, c);
    conn_close(c);

    return false;
}

bool conn_closing(conn *c) {
    /* We don't want any network notifications anymore.. */
    unregister_event(c);
    safe_close(c->sfd);
    c->sfd = INVALID_SOCKET;

    if (c->refcount > 1 || c->ewouldblock) {
        conn_set_state(c, conn_pending_close);
    } else {
        conn_set_state(c, conn_immediate_close);
    }
    return true;
}

bool conn_setup_tap_stream(conn *c) {
    process_bin_tap_connect(c);
    return true;
}

bool conn_refresh_cbsasl(conn *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    assert(ret != ENGINE_EWOULDBLOCK);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        conn_set_state(c, conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret), 0);
    }

    return true;
}

void event_handler(const int fd, const short which, void *arg) {
    conn *c = arg;
    LIBEVENT_THREAD *thr;

    assert(c != NULL);

    if (memcached_shutdown) {
        event_base_loopbreak(c->event.ev_base);
        return ;
    }

    thr = c->thread;
    if (!is_listen_thread()) {
        assert(thr);
        LOCK_THREAD(thr);
        /*
         * Remove the list from the list of pending io's (in case the
         * object was scheduled to run in the dispatcher before the
         * callback for the worker thread is executed.
         */
        c->thread->pending_io = list_remove(c->thread->pending_io, c);
    }

    c->which = which;

    /* sanity */
    assert(fd == c->sfd);
    perform_callbacks(ON_SWITCH_CONN, c, c);


    c->nevents = settings.reqs_per_event;
    if (c->state == conn_ship_log) {
        c->nevents = settings.reqs_per_tap_event;
    }

    do {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "%d - Running task: (%s)\n",
                                            c->sfd, state_text(c->state));
        }
    } while (c->state(c));

    if (thr) {
        UNLOCK_THREAD(thr);
    }
}

static void dispatch_event_handler(int fd, short which, void *arg) {
    char buffer[80];
    ssize_t nr = recv(fd, buffer, sizeof(buffer), 0);

    if (nr != -1 && is_listen_disabled()) {
        bool enable = false;
        cb_mutex_enter(&listen_state.mutex);
        listen_state.count -= nr;
        if (listen_state.count <= 0) {
            enable = true;
            listen_state.disabled = false;
        }
        cb_mutex_exit(&listen_state.mutex);
        if (enable) {
            conn *next;
            for (next = listen_conn; next; next = next->next) {
                update_event(next, EV_READ | EV_PERSIST);
                if (listen(next->sfd, settings.backlog) != 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "listen() failed",
                                                    strerror(errno));
                }
            }
        }
    }
}

static SOCKET new_socket(struct addrinfo *ai) {
    SOCKET sfd;

    sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        return INVALID_SOCKET;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        safe_close(sfd);
        return INVALID_SOCKET;
    }

    return sfd;
}

#if 0
/* TROND SHOULD THIS BE THE DEFAULT FOR OTHER SOCKETS AS WELL?? */
/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&old_size, &intsize) != 0) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "getsockopt(SO_SNDBUF): %s",
                                            strerror(errno));
        }

        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                 "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
    }
}
#endif

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
static int server_socket(const char *interface,
                         int port,
                         FILE *portnumber_file) {
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints;
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = AF_UNSPEC;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (port == -1) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error= getaddrinfo(interface, port_buf, &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        log_errcode_error(EXTENSION_LOG_WARNING, NULL,
                          "getaddrinfo(): %s", error);
#else
        if (error != EAI_SYSTEM) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                     "getaddrinfo(): %s", gai_strerror(error));
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                     "getaddrinfo(): %s", strerror(error));
        }
#endif
        return 1;
    }

    for (next= ai; next; next= next->ai_next) {
        struct listening_port *port_instance;
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(IPV6_V6ONLY): %s",
                                                strerror(errno));
                safe_close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_KEEPALIVE): %s",
                                            strerror(errno));
        }

        error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_LINGER): %s",
                                            strerror(errno));
        }

        if (settings.tcp_nodelay) {
            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(TCP_NODELAY): %s",
                                                strerror(errno));
            }
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == SOCKET_ERROR) {
#ifdef WIN32
            DWORD error = WSAGetLastError();
#else
            int error = errno;
#endif
            if (!is_addrinuse(error)) {
                log_errcode_error(EXTENSION_LOG_WARNING, NULL,
                                  "bind(): %s", error);
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            safe_close(sfd);
            continue;
        } else {
            success++;
            if (listen(sfd, settings.backlog) == SOCKET_ERROR) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "listen(): %s",
                                                strerror(errno));
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&
                (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n", "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n", "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (!(listen_conn_add = conn_new(sfd, port, conn_listening,
                                         EV_READ | EV_PERSIST, 1,
                                         main_base, NULL))) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "failed to create listening connection\n");
            exit(EXIT_FAILURE);
        }
        listen_conn_add->next = listen_conn;
        listen_conn = listen_conn_add;
        STATS_LOCK();
        ++stats.curr_conns;
        ++stats.daemon_conns;
        port_instance = get_listening_port_instance(port);
        assert(port_instance);
        ++port_instance->curr_conns;
        STATS_UNLOCK();
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int server_sockets(int port, FILE *portnumber_file) {
    if (settings.inter == NULL) {
        stats.listening_ports[0].port = port == -1 ? 0 : port;
        stats.listening_ports[0].maxconns = settings.maxconns;
        return server_socket(settings.inter, port, portnumber_file);
    } else {
        /* tokenize them and bind to each one of them.. */
        int total_max_conns = 0;
        int num_zero_max_conns = 0;
        int pidx = 0;
        char *p;
        int ret = 0;
        char *list = strdup(settings.inter);

        if (list == NULL) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to allocate memory for parsing server interface string\n");
            return 1;
        }

        for (p = strtok(list, ";,"); p != NULL; p = strtok(NULL, ";,")) {

            int the_port = port;
            int max_conns_on_port = 0;
            char *s = strchr(p, ':');
            if (s != NULL) {
                char *m;

                *s = '\0';
                ++s;
                m = strchr(s, ':');
                if (m != NULL) {
                    *m = '\0';
                    ++m;
                }

                if (!safe_strtol(s, &the_port)) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "Invalid port number: \"%s\"", s);
                    return 1;
                }
                if (m && !safe_strtol(m, &max_conns_on_port)) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "Invalid max connection limit: \"%s\"", m);
                    return 1;
                }

                total_max_conns += max_conns_on_port;
                if (total_max_conns > settings.maxconns) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "Aggregated port max connections %d exceed "
                                                    " the process limit %d",
                                                    total_max_conns, settings.maxconns);
                    return 1;
                }
            }

            stats.listening_ports[pidx].port = the_port == -1 ? 0 : the_port;
            stats.listening_ports[pidx].maxconns = max_conns_on_port;
            ++pidx;
            if (max_conns_on_port == 0) {
                ++num_zero_max_conns;
            }

            if (strcmp(p, "*") == 0) {
                p = NULL;
            }
            ret |= server_socket(p, the_port, portnumber_file);
        }
        /* For ports whose max connection limit is missing from cmd */
        if (num_zero_max_conns > 0) {
            int max_conns = (settings.maxconns - total_max_conns) / num_zero_max_conns;
            int i;
            for (i = 0; i < settings.num_ports; ++i) {
                if (stats.listening_ports[i].maxconns == 0) {
                    stats.listening_ports[i].maxconns = max_conns;
                }
            }
        }

        free(list);
        return ret;
    }
}

static struct event clockevent;

/* time-sensitive callers can call it by hand with this, outside the normal ever-1-second timer */
static void set_current_time(void) {
#if 0 /* TROND FIXME */
    struct timeval timer;

    gettimeofday(&timer, NULL);
    current_time = (rel_time_t) (timer.tv_sec - process_started);
#endif
    current_time = (rel_time_t) time(NULL) - process_started;
}

static void clock_handler(const int fd, const short which, void *arg) {
    struct timeval t;
    static bool initialized = false;

    t.tv_sec = 1;
    t.tv_usec = 0;

    if (memcached_shutdown) {
        event_base_loopbreak(main_base);
        return ;
    }

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);

    set_current_time();
}

static void usage(void) {
    printf("memcached %s\n", get_server_version());
    printf("-p <num>      TCP port number to listen on (default: 11211)\n");
    printf("-l <addr>     interface to listen on (default: INADDR_ANY, all addresses)\n");
    printf("              <addr> may be specified as host:port:max_connections.\n");
    printf("              If you don't specify a port number, the value you specified\n");
    printf("              with -p or -U is used. You may specify multiple addresses\n");
    printf("              separated by comma or by using -l multiple times\n");
    printf("-d            run as a daemon\n");
#ifndef WIN32
    printf("-r            maximize core file limit\n");
    printf("-u <username> assume identity of <username> (only when run as root)\n");
#endif
    printf("-m <num>      max memory to use for items in megabytes (default: 64 MB)\n");
    printf("-M            return error on memory exhausted (rather than removing items)\n");
    printf("-c <num>      max simultaneous connections (default: 1000)\n");
    printf("-k            lock down all paged memory.  Note that there is a\n");
    printf("              limit on how much memory you may lock.  Trying to\n");
    printf("              allocate more than that would fail, so be sure you\n");
    printf("              set the limit correctly for the user you started\n");
    printf("              the daemon with (not for -u <username> user;\n");
    printf("              under sh this is done with 'ulimit -S -l NUM_KB').\n");
    printf("-v            verbose (print errors/warnings while in event loop)\n");
    printf("-vv           very verbose (also print client commands/reponses)\n");
    printf("-vvv          extremely verbose (also print internal state transitions)\n");
    printf("-h            print this help and exit\n");
    printf("-i            print memcached and libevent license\n");
#ifndef WIN32
    printf("-P <file>     save PID in <file>, only used with -d option\n");
#endif
    printf("-f <factor>   chunk size growth factor (default: 1.25)\n");
    printf("-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
    printf("-L            Try to use large memory pages (if available). Increasing\n");
    printf("              the memory page size could reduce the number of TLB misses\n");
    printf("              and improve the performance. In order to get large pages\n");
    printf("              from the OS, memcached will allocate the total item-cache\n");
    printf("              in one large chunk.\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n");
    printf("              This is used for per-prefix stats reporting. The default is\n");
    printf("              \":\" (colon). If this option is specified, stats collection\n");
    printf("              is turned on automatically; if not, then it may be turned on\n");
    printf("              by sending the \"stats detail on\" command to the server.\n");
    printf("-t <num>      number of threads to use (default: number of cpus * 0.75)\n");
    printf("-R            Maximum number of requests per event, limits the number of\n");
    printf("              requests process for a given connection to prevent \n");
    printf("              starvation (default: 20)\n");
    printf("-C            Disable use of CAS\n");
    printf("-b            Set the backlog queue limit (default: 1024)\n");
    printf("-B            Binding protocol - one of binary or auto (default)\n");
    printf("-I            Override the size of each slab page. Adjusts max item size\n");
    printf("              (default: 1mb, min: 1k, max: 128m)\n");
    printf("-q            Disable detailed stats commands\n");
    printf("-S            Require SASL authentication (CBSASL)\n");
    printf("-X module,cfg Load the module and initialize it with the config\n");
    printf("-E engine     Load engine as the storage engine\n");
    printf("-e config     Pass config as configuration options to the storage engine\n");
    printf("\nEnvironment variables:\n");
    printf("MEMCACHED_PORT_FILENAME   File to write port information to\n");
    printf("MEMCACHED_REQS_TAP_EVENT  Similar to -R but for tap_ship_log\n");
}

static void usage_license(void) {
    printf("memcached %s\n\n", get_server_version());
    printf("Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n");
    printf("All rights reserved.\n");
    printf("\n");
    printf("Redistribution and use in source and binary forms, with or without\n");
    printf("modification, are permitted provided that the following conditions are\n");
    printf("met:\n");
    printf("\n");
    printf("    * Redistributions of source code must retain the above copyright\n");
    printf("notice, this list of conditions and the following disclaimer.\n");
    printf("\n");
    printf("    * Redistributions in binary form must reproduce the above\n");
    printf("copyright notice, this list of conditions and the following disclaimer\n");
    printf("in the documentation and/or other materials provided with the\n");
    printf("distribution.\n");
    printf("\n");
    printf("    * Neither the name of the Danga Interactive nor the names of its\n");
    printf("contributors may be used to endorse or promote products derived from\n");
    printf("this software without specific prior written permission.\n");
    printf("\n");
    printf("THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n");
    printf("\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n");
    printf("LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n");
    printf("A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n");
    printf("OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n");
    printf("SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n");
    printf("LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n");
    printf("DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n");
    printf("THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n");
    printf("(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n");
    printf("OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n");
    printf("\n");
    printf("\n");
    printf("This product includes software developed by Niels Provos.\n");
    printf("\n");
    printf("[ libevent ]\n");
    printf("\n");
    printf("Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n");
    printf("All rights reserved.\n");
    printf("\n");
    printf("Redistribution and use in source and binary forms, with or without\n");
    printf("modification, are permitted provided that the following conditions\n");
    printf("are met:\n");
    printf("1. Redistributions of source code must retain the above copyright\n");
    printf("   notice, this list of conditions and the following disclaimer.\n");
    printf("2. Redistributions in binary form must reproduce the above copyright\n");
    printf("   notice, this list of conditions and the following disclaimer in the\n");
    printf("   documentation and/or other materials provided with the distribution.\n");
    printf("3. All advertising materials mentioning features or use of this software\n");
    printf("   must display the following acknowledgement:\n");
    printf("      This product includes software developed by Niels Provos.\n");
    printf("4. The name of the author may not be used to endorse or promote products\n");
    printf("   derived from this software without specific prior written permission.\n");
    printf("\n");
    printf("THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n");
    printf("IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n");
    printf("OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n");
    printf("IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n");
    printf("INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n");
    printf("NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n");
    printf("DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n");
    printf("THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n");
    printf("(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n");
    printf("THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n");
}

#ifndef WIN32
static void save_pid(const char *pid_file) {
    FILE *fp;

    if (access(pid_file, F_OK) == 0) {
        if ((fp = fopen(pid_file, "r")) != NULL) {
            char buffer[1024];
            if (fgets(buffer, sizeof(buffer), fp) != NULL) {
                unsigned int pid;
                if (safe_strtoul(buffer, &pid) && kill((pid_t)pid, 0) == 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "WARNING: The pid file contained the following (running) pid: %u\n", pid);
                }
            }
            fclose(fp);
        }
    }

    if ((fp = fopen(pid_file, "w")) == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                 "Could not open the pid file %s for writing: %s\n",
                 pid_file, strerror(errno));
        return;
    }

    fprintf(fp,"%ld\n", (long)getpid());
    if (fclose(fp) == -1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Could not close the pid file %s: %s\n",
                pid_file, strerror(errno));
    }
}

static void remove_pidfile(const char *pid_file) {
    if (pid_file != NULL) {
        if (unlink(pid_file) != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Could not remove the pid file %s: %s\n",
                    pid_file, strerror(errno));
        }
    }
}
#endif

#ifndef WIN32

#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif /* !HAVE_SIGIGNORE */

static void sigterm_handler(int sig) {
    assert(sig == SIGTERM || sig == SIGINT);
    memcached_shutdown = 1;
}
#endif

static int install_sigterm_handler(void) {
#ifndef WIN32
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGTERM, &sa, 0) == -1 ||
        sigaction(SIGINT, &sa, 0) == -1) {
        return -1;
    }
#endif

    return 0;
}

/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                  "Failed to set large pages: %s\nWill use default page size\n",
                  strerror(errno));
        } else {
            ret = 0;
        }
    } else {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
          "Failed to get supported pagesizes: %s\nWill use default page size\n",
          strerror(errno));
    }

    return ret;
#else
    return 0;
#endif
}

static const char* get_server_version(void) {
    return MEMCACHED_VERSION;
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    conn *c = (conn*)cookie;
    c->engine_storage = engine_data;
}

static void *get_engine_specific(const void *cookie) {
    conn *c = (conn*)cookie;
    return c->engine_storage;
}

static int get_socket_fd(const void *cookie) {
    conn *c = (conn *)cookie;
    return c->sfd;
}

static ENGINE_ERROR_CODE reserve_cookie(const void *cookie) {
    conn *c = (conn *)cookie;
    ++c->refcount;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE release_cookie(const void *cookie) {
    conn *c = (conn *)cookie;
    int notify;
    LIBEVENT_THREAD *thr;

    assert(c);
    thr = c->thread;
    assert(thr);
    LOCK_THREAD(thr);
    --c->refcount;

    /* Releasing the refererence to the object may cause it to change
     * state. (NOTE: the release call shall never be called from the
     * worker threads), so should put the connection in the pool of
     * pending IO and have the system retry the operation for the
     * connection
     */
    notify = add_conn_to_pending_io_list(c);
    UNLOCK_THREAD(thr);

    /* kick the thread in the butt */
    if (notify) {
        notify_thread(thr);
    }

    return ENGINE_SUCCESS;
}

static int num_independent_stats(void) {
    return settings.num_threads + 1;
}

static void *new_independent_stats(void) {
    int nrecords = num_independent_stats();
    struct thread_stats *ts = calloc(nrecords, sizeof(struct thread_stats));
    int ii;
    for (ii = 0; ii < nrecords; ii++) {
        cb_mutex_initialize(&ts[ii].mutex);
    }
    return ts;
}

static void release_independent_stats(void *stats) {
    int nrecords = num_independent_stats();
    struct thread_stats *ts = stats;
    int ii;
    for (ii = 0; ii < nrecords; ii++) {
        cb_mutex_destroy(&ts[ii].mutex);
    }
    free(ts);
}

static struct thread_stats* get_independent_stats(conn *c) {
    struct thread_stats *independent_stats;
    if (settings.engine.v1->get_stats_struct != NULL) {
        independent_stats = settings.engine.v1->get_stats_struct(settings.engine.v0, (const void *)c);
        if (independent_stats == NULL) {
            independent_stats = default_independent_stats;
        }
    } else {
        independent_stats = default_independent_stats;
    }
    return independent_stats;
}

static struct thread_stats *get_thread_stats(conn *c) {
    struct thread_stats *independent_stats;
    assert(c->thread->index < num_independent_stats());
    independent_stats = get_independent_stats(c);
    return &independent_stats[c->thread->index];
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data) {
    struct engine_event_handler *h =
        calloc(sizeof(struct engine_event_handler), 1);

    assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = engine_event_handlers[type];
    engine_event_handlers[type] = h;
}

static rel_time_t get_current_time(void)
{
    return current_time;
}

static void count_eviction(const void *cookie, const void *key, const int nkey) {
    (void)cookie;
    (void)key;
    (void)nkey;
}

/**
 * To make it easy for engine implementors that doesn't want to care about
 * writing their own incr/decr code, they can just set the arithmetic function
 * to NULL and use this implementation. It is not efficient, due to the fact
 * that it does multiple calls through the interface (get and then cas store).
 * If you don't care, feel free to use it..
 */
static ENGINE_ERROR_CODE internal_arithmetic(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const int nkey,
                                             const bool increment,
                                             const bool create,
                                             const uint64_t delta,
                                             const uint64_t initial,
                                             const rel_time_t exptime,
                                             uint64_t *cas,
                                             uint64_t *result,
                                             uint16_t vbucket)
{
    ENGINE_HANDLE_V1 *e = (ENGINE_HANDLE_V1*)handle;
    item *it = NULL;
    ENGINE_ERROR_CODE ret;

    ret = e->get(handle, cookie, &it, key, nkey, vbucket);

    if (ret == ENGINE_SUCCESS) {
        size_t nb;
        item *nit;
        char value[80];
        uint64_t val;
        item_info_holder info;
        item_info_holder i2;
        memset(&info, 0, sizeof(info));
        memset(&i2, 0, sizeof(i2));

        info.info.nvalue = 1;

        if (!e->get_item_info(handle, cookie, it, (void*)&info)) {
            e->release(handle, cookie, it);
            return ENGINE_FAILED;
        }

        if (info.info.value[0].iov_len > (sizeof(value) - 1)) {
            e->release(handle, cookie, it);
            return ENGINE_EINVAL;
        }

        memcpy(value, info.info.value[0].iov_base, info.info.value[0].iov_len);
        value[info.info.value[0].iov_len] = '\0';

        if (!safe_strtoull(value, &val)) {
            e->release(handle, cookie, it);
            return ENGINE_EINVAL;
        }

        if (increment) {
            val += delta;
        } else {
            if (delta > val) {
                val = 0;
            } else {
                val -= delta;
            }
        }

        nb = snprintf(value, sizeof(value), "%"PRIu64, val);
        *result = val;
        nit = NULL;
        if (e->allocate(handle, cookie, &nit, key,
                        nkey, nb, info.info.flags, info.info.exptime) != ENGINE_SUCCESS) {
            e->release(handle, cookie, it);
            return ENGINE_ENOMEM;
        }

        i2.info.nvalue = 1;
        if (!e->get_item_info(handle, cookie, nit, (void*)&i2)) {
            e->release(handle, cookie, it);
            e->release(handle, cookie, nit);
            return ENGINE_FAILED;
        }

        memcpy(i2.info.value[0].iov_base, value, nb);
        e->item_set_cas(handle, cookie, nit, info.info.cas);
        ret = e->store(handle, cookie, nit, cas, OPERATION_CAS, vbucket);
        e->release(handle, cookie, it);
        e->release(handle, cookie, nit);
    } else if (ret == ENGINE_KEY_ENOENT && create) {
        char value[80];
        size_t nb = snprintf(value, sizeof(value), "%"PRIu64"\r\n", initial);
        item_info_holder info;
        memset(&info, 0, sizeof(info));
        info.info.nvalue = 1;

        *result = initial;
        if (e->allocate(handle, cookie, &it, key, nkey, nb, 0, exptime) != ENGINE_SUCCESS) {
            e->release(handle, cookie, it);
            return ENGINE_ENOMEM;
        }

        if (!e->get_item_info(handle, cookie, it, (void*)&info)) {
            e->release(handle, cookie, it);
            return ENGINE_FAILED;
        }

        memcpy(info.info.value[0].iov_base, value, nb);
        ret = e->store(handle, cookie, it, cas, OPERATION_CAS, vbucket);
        e->release(handle, cookie, it);
    }

    /* We had a race condition.. just call ourself recursively to retry */
    if (ret == ENGINE_KEY_EEXISTS) {
        return internal_arithmetic(handle, cookie, key, nkey, increment, create, delta,
                                   initial, exptime, cas, result, vbucket);
    }

    return ret;
}

/**
 * Register an extension if it's not already registered
 *
 * @param type the type of the extension to register
 * @param extension the extension to register
 * @return true if success, false otherwise
 */
static bool register_extension(extension_type_t type, void *extension)
{
    if (extension == NULL) {
        return false;
    }

    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *ptr;
            for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
                if (ptr == extension) {
                    return false;
                }
            }
            ((EXTENSION_DAEMON_DESCRIPTOR *)(extension))->next = settings.extensions.daemons;
            settings.extensions.daemons = extension;
        }
        return true;
    case EXTENSION_LOGGER:
        settings.extensions.logger = extension;
        return true;

    case EXTENSION_BINARY_PROTOCOL:
        if (settings.extensions.binary != NULL) {
            EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *last;
            for (last = settings.extensions.binary; last->next != NULL;
                 last = last->next) {
                if (last == extension) {
                    return false;
                }
            }
            if (last == extension) {
                return false;
            }
            last->next = extension;
            last->next->next = NULL;
        } else {
            settings.extensions.binary = extension;
            settings.extensions.binary->next = NULL;
        }

        ((EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*)extension)->setup(setup_binary_lookup_cmd);
        return true;

    default:
        return false;
    }
}

/**
 * Unregister an extension
 *
 * @param type the type of the extension to remove
 * @param extension the extension to remove
 */
static void unregister_extension(extension_type_t type, void *extension)
{
    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *prev = NULL;
            EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (settings.extensions.daemons == ptr) {
                settings.extensions.daemons = ptr->next;
            }
        }
        break;
    case EXTENSION_LOGGER:
        if (settings.extensions.logger == extension) {
            if (get_stderr_logger() == extension) {
                settings.extensions.logger = get_null_logger();
            } else {
                settings.extensions.logger = get_stderr_logger();
            }
        }
        break;
    case EXTENSION_BINARY_PROTOCOL:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "You can't unregister a binary command handler!");
        abort();
        break;

    default:
        ;
    }

}

/**
 * Get the named extension
 */
static void* get_extension(extension_type_t type)
{
    switch (type) {
    case EXTENSION_DAEMON:
        return settings.extensions.daemons;

    case EXTENSION_LOGGER:
        return settings.extensions.logger;

    case EXTENSION_BINARY_PROTOCOL:
        return settings.extensions.binary;

    default:
        return NULL;
    }
}

static void shutdown_server(void) {
    memcached_shutdown = 1;
}

static EXTENSION_LOGGER_DESCRIPTOR* get_logger(void)
{
    return settings.extensions.logger;
}

static EXTENSION_LOG_LEVEL get_log_level(void)
{
    EXTENSION_LOG_LEVEL ret;
    switch (settings.verbose) {
    case 0: ret = EXTENSION_LOG_WARNING; break;
    case 1: ret = EXTENSION_LOG_INFO; break;
    case 2: ret = EXTENSION_LOG_DEBUG; break;
    default:
        ret = EXTENSION_LOG_DETAIL;
    }
    return ret;
}

static void set_log_level(EXTENSION_LOG_LEVEL severity)
{
    switch (severity) {
    case EXTENSION_LOG_WARNING: settings.verbose = 0; break;
    case EXTENSION_LOG_INFO: settings.verbose = 1; break;
    case EXTENSION_LOG_DEBUG: settings.verbose = 2; break;
    default:
        settings.verbose = 3;
    }
}

static void get_config_append_stats(const char *key, const uint16_t klen,
                                    const char *val, const uint32_t vlen,
                                    const void *cookie)
{
    char *pos;
    size_t nbytes;

    if (klen == 0  || vlen == 0) {
        return ;
    }

    pos = (char*)cookie;
    nbytes = strlen(pos);

    if ((nbytes + klen + vlen + 3) > 1024) {
        /* Not enough size in the buffer.. */
        return;
    }

    memcpy(pos + nbytes, key, klen);
    nbytes += klen;
    pos[nbytes] = '=';
    ++nbytes;
    memcpy(pos + nbytes, val, vlen);
    nbytes += vlen;
    memcpy(pos + nbytes, ";", 2);
}

static bool get_config(struct config_item items[]) {
    char config[1024];
    int rval;

    config[0] = '\0';
    process_stat_settings(get_config_append_stats, config);
    rval = parse_config(config, items, NULL);
    return rval >= 0;
}

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
static SERVER_HANDLE_V1 *get_server_api(void)
{
    static int init;
    static SERVER_CORE_API core_api;
    static SERVER_COOKIE_API server_cookie_api;
    static SERVER_STAT_API server_stat_api;
    static SERVER_LOG_API server_log_api;
    static SERVER_EXTENSION_API extension_api;
    static SERVER_CALLBACK_API callback_api;
    static ALLOCATOR_HOOKS_API hooks_api;
    static SERVER_HANDLE_V1 rv;

    if (!init) {
        init = 1;
        core_api.server_version = get_server_version;
        core_api.hash = hash;
        core_api.realtime = realtime;
        core_api.abstime = abstime;
        core_api.get_current_time = get_current_time;
        core_api.parse_config = parse_config;
        core_api.shutdown = shutdown_server;
        core_api.get_config = get_config;

        server_cookie_api.get_auth_data = get_auth_data;
        server_cookie_api.store_engine_specific = store_engine_specific;
        server_cookie_api.get_engine_specific = get_engine_specific;
        server_cookie_api.get_socket_fd = get_socket_fd;
        server_cookie_api.notify_io_complete = notify_io_complete;
        server_cookie_api.reserve = reserve_cookie;
        server_cookie_api.release = release_cookie;

        server_stat_api.new_stats = new_independent_stats;
        server_stat_api.release_stats = release_independent_stats;
        server_stat_api.evicting = count_eviction;

        server_log_api.get_logger = get_logger;
        server_log_api.get_level = get_log_level;
        server_log_api.set_level = set_log_level;

        extension_api.register_extension = register_extension;
        extension_api.unregister_extension = unregister_extension;
        extension_api.get_extension = get_extension;

        callback_api.register_callback = register_callback;
        callback_api.perform_callbacks = perform_callbacks;

        hooks_api.add_new_hook = mc_add_new_hook;
        hooks_api.remove_new_hook = mc_remove_new_hook;
        hooks_api.add_delete_hook = mc_add_delete_hook;
        hooks_api.remove_delete_hook = mc_remove_delete_hook;
        hooks_api.get_extra_stats_size = mc_get_extra_stats_size;
        hooks_api.get_allocator_stats = mc_get_allocator_stats;
        hooks_api.get_allocation_size = mc_get_allocation_size;
        hooks_api.get_detailed_stats = mc_get_detailed_stats;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    if (rv.engine == NULL) {
        rv.engine = settings.engine.v0;
    }

    return &rv;
}

static void initialize_binary_lookup_map(void) {
    int ii;
    for (ii = 0; ii < 0x100; ++ii) {
        request_handlers[ii].descriptor = NULL;
        request_handlers[ii].callback = default_unknown_command;
    }

    response_handlers[PROTOCOL_BINARY_CMD_NOOP] = process_bin_noop_response;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_MUTATION] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_DELETE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_FLUSH] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = process_bin_tap_ack;

}

/**
 * Load a shared object and initialize all the extensions in there.
 *
 * @param soname the name of the shared object (may not be NULL)
 * @param config optional configuration parameters
 * @return true if success, false otherwise
 */
static bool load_extension(const char *soname, const char *config) {
    cb_dlhandle_t handle;
    void *symbol;
    EXTENSION_ERROR_CODE error;
    union my_hack {
        MEMCACHED_EXTENSIONS_INITIALIZE initialize;
        void* voidptr;
    } funky;
    char *error_msg;

    if (soname == NULL) {
        return false;
    }

    handle = cb_dlopen(soname, &error_msg);
    if (handle == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to open library \"%s\": %s\n",
                                        soname, error_msg);
        free(error_msg);
        return false;
    }

    symbol = cb_dlsym(handle, "memcached_extensions_initialize", &error_msg);
    if (symbol == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Could not find symbol \"memcached_extensions_initialize\" in %s: %s\n",
                                        soname, error_msg);
        free(error_msg);
        return false;
    }
    funky.voidptr = symbol;

    error = (*funky.initialize)(config, get_server_api);
    if (error != EXTENSION_SUCCESS) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initalize extensions from %s. Error code: %d\n",
                soname, error);
        cb_dlclose(handle);
        return false;
    }

    if (settings.verbose > 0) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                "Loaded extensions from: %s\n", soname);
    }

    return true;
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
    /* One of our biggest problems is old and bogus libevents */
    const char *ever = event_get_version();
    if (ever != NULL) {
        if (strncmp(ever, "1.", 2) == 0) {
            /* Require at least 1.3 (that's still a couple of years old) */
            if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "You are using libevent %s.\nPlease upgrade to"
                        " a more recent version (1.3 or newer)\n",
                        event_get_version());
                return false;
            }
        }
    }

    return true;
}

/**
 * Log a socket error message.
 *
 * @param severity the severity to put in the log
 * @param cookie cookie representing the client
 * @param prefix What to put as a prefix (MUST INCLUDE
 *               the %s for where the string should go)
 */
void log_socket_error(EXTENSION_LOG_LEVEL severity,
                      const void* cookie,
                      const char* prefix)
{
#ifdef WIN32
    log_errcode_error(severity, cookie, prefix,
                      WSAGetLastError());
#else
    log_errcode_error(severity, cookie, prefix, errno);
#endif
}

/**
 * Log a system error message.
 *
 * @param severity the severity to put in the log
 * @param cookie cookie representing the client
 * @param prefix What to put as a prefix (MUST INCLUDE
 *               the %s for where the string should go)
 */
void log_system_error(EXTENSION_LOG_LEVEL severity,
                      const void* cookie,
                      const char* prefix)
{
#ifdef WIN32
    log_errcode_error(severity, cookie, prefix,
                      GetLastError());
#else
    log_errcode_error(severity, cookie, prefix, errno);
#endif
}

#ifdef WIN32
void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                       const void* cookie,
                       const char* prefix, DWORD err) {
    LPVOID error_msg;

    if (FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                      FORMAT_MESSAGE_FROM_SYSTEM |
                      FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, err, 0,
                      (LPTSTR)&error_msg, 0, NULL) != 0) {
        settings.extensions.logger->log(severity, cookie,
                                        prefix, error_msg);
        LocalFree(error_msg);
    } else {
        settings.extensions.logger->log(severity, cookie,
                                        prefix, "unknown error");
    }
}
#else
void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                       const void* cookie,
                       const char* prefix, int err) {
    settings.extensions.logger->log(severity,
                                    cookie,
                                    prefix,
                                    strerror(err));
}
#endif

#ifdef WIN32
static void parent_monitor_thread(void *arg) {
    HANDLE parent = arg;
    WaitForSingleObject(parent, INFINITE);
    ExitProcess(EXIT_FAILURE);
}

static void setup_parent_monitor(void) {
    char *env = getenv("MEMCACHED_PARENT_MONITOR");
    if (env != NULL) {
        HANDLE handle = OpenProcess(SYNCHRONIZE, FALSE, atoi(env));
        if (handle == INVALID_HANDLE_VALUE) {
            log_system_error(EXTENSION_LOG_WARNING, NULL,
                "Failed to open parent process: %s");
            exit(EXIT_FAILURE);
        }
        cb_create_thread(NULL, parent_monitor_thread, handle, 1);
    }
}
#else
static void setup_parent_monitor(void) {
    /* EMPTY */
}
#endif


int main (int argc, char **argv) {
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    char *username = NULL;
#ifndef WIN32
    char *pid_file = NULL;
    int maxcore = 0;
    struct passwd *pw;
    struct rlimit rlim;
#endif
    char unit = '\0';
    int size_max = 0;
    int num_ports = 0;
    bool protocol_specified = false;
    const char *engine = "default_engine.so";
    const char *engine_config = NULL;
    char old_options[1024];
    char *old_opts = old_options;
    int nfiles = 0;
    ENGINE_HANDLE *engine_handle = NULL;

    old_options[0] = '\0';
    /* make the time we started always be 2 seconds before we really
       did, so time(0) - time.started is never zero.  if so, things
       like 'settings.oldest_live' which act as booleans as well as
       values are now false in boolean context... */
    process_started = time(0) - 2;
    set_current_time();

    /* Initialize global variables */
    cb_mutex_initialize(&listen_state.mutex);
    cb_mutex_initialize(&connections.mutex);
    cb_mutex_initialize(&tap_stats.mutex);
    cb_mutex_initialize(&stats_lock);

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    init_alloc_hooks();

    /* init settings */
    settings_init();

    initialize_binary_lookup_map();

    setup_bin_packet_handlers();

    if (memcached_initialize_stderr_logger(get_server_api) != EXTENSION_SUCCESS) {
        fprintf(stderr, "Failed to initialize log system\n");
        return EX_OSERR;
    }

    if (!sanitycheck()) {
        return EX_OSERR;
    }

    /* process arguments */
    while ((c = getopt(argc, argv,
          "p:"  /* TCP port number to listen on */
          "m:"  /* max memory to use for items in megabytes */
          "M"   /* return error on memory exhausted */
          "c:"  /* max simultaneous connections */
          "k"   /* lock down all paged memory */
          "hi"  /* help, licence info */
#ifndef WIN32
          "r"   /* maximize core file limit */
          "u:"  /* user identity to run as */
          "P:"  /* save PID in file */
#endif
          "v"   /* verbose */
          "d"   /* daemon mode */
          "l:"  /* interface to listen on */
          "f:"  /* factor? */
          "n:"  /* minimum space allocated for key+value+flags */
          "t:"  /* threads */
          "D:"  /* prefix delimiter? */
          "L"   /* Large memory pages */
          "R:"  /* max requests per event */
          "C"   /* Disable use of CAS */
          "b:"  /* backlog queue limit */
          "B:"  /* Binding protocol */
          "I:"  /* Max item size */
          "S"   /* Sasl ON */
          "E:"  /* Engine to load */
          "e:"  /* Engine options */
          "q"   /* Disallow detailed stats */
          "X:"  /* Load extension */
        )) != -1) {
        switch (c) {

        case 'p':
            settings.port = atoi(optarg);
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
             old_opts += sprintf(old_opts, "cache_size=%lu;",
                                 (unsigned long)settings.maxbytes);
           break;
        case 'M':
            settings.evict_to_free = 0;
            old_opts += sprintf(old_opts, "eviction=false;");
            break;
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            settings.verbose++;
            perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
            break;
        case 'l':
            {
                char *ilist;
                char *p;

                if (settings.inter != NULL) {
                    size_t len = strlen(settings.inter) + strlen(optarg) + 2;
                    char *p = malloc(len);
                    if (p == NULL) {
                        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                        "Failed to allocate memory\n");
                        return 1;
                    }
                    snprintf(p, len, "%s,%s", settings.inter, optarg);
                    free(settings.inter);
                    settings.inter = p;
                } else {
                    settings.inter= strdup(optarg);
                }

                ilist = strdup(settings.inter);
                for (p = strtok(ilist, ";,"); p != NULL;
                     p = strtok(NULL, ";,"))
                {
                    ++num_ports;
                }
                free(ilist);
            }
            break;
        case 'd':
            do_daemonize = true;
            break;
#ifndef WIN32
        case 'r':
            maxcore = 1;
            break;
#endif
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event <= 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                      "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
#ifndef WIN32
        case 'P':
            pid_file = optarg;
            break;
#endif
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Factor must be greater than 1\n");
                return 1;
            }
             old_opts += sprintf(old_opts, "factor=%f;",
                                 settings.factor);
           break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Chunk size must be greater than 0\n");
                return 1;
            }
            old_opts += sprintf(old_opts, "chunk_size=%u;",
                                settings.chunk_size);
            break;
        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Number of threads must be greater than 0\n");
                return 1;
            }
            /* There're other problems when you get above 64 threads.
             * In the future we should portably detect # of cores for the
             * default.
             */
            if (settings.num_threads > 64) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "WARNING: Setting a high number of worker"
                        "threads is not recommended.\n"
                        " Set this value to the number of cores in"
                        " your machine or less.\n");
            }
            break;
        case 'D':
            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;
        case 'L' :
            if (enable_large_pages() == 0) {
                old_opts += sprintf(old_opts, "preallocate=true;");
            }
            break;
        case 'C' :
            settings.use_cas = false;
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto or binary\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'I':
            unit = optarg[strlen(optarg)-1];
            if (unit == 'k' || unit == 'm' ||
                unit == 'K' || unit == 'M') {
                optarg[strlen(optarg)-1] = '\0';
                size_max = atoi(optarg);
                if (unit == 'k' || unit == 'K')
                    size_max *= 1024;
                if (unit == 'm' || unit == 'M')
                    size_max *= 1024 * 1024;
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(optarg);
            }
            if (settings.item_size_max < 1024) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Item max size cannot be less than 1024 bytes.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024 * 128) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Cannot set item size limit higher than 128 mb.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "WARNING: Setting item max size above 1MB is not"
                    " recommended!\n"
                    " Raising this limit increases the minimum memory requirements\n"
                    " and will decrease your memory efficiency.\n"
                );
            }
            old_opts += sprintf(old_opts, "item_size_max=%"PRIu64";",
                                (uint64_t)settings.item_size_max);
            break;
        case 'E':
            engine = optarg;
            break;
        case 'e':
            engine_config = optarg;
            break;
        case 'q':
            settings.allow_detailed = false;
            break;
        case 'S': /* set Sasl authentication to true. Default is false */
            settings.require_sasl = true;
            break;
        case 'X' :
            {
                char *ptr = strchr(optarg, ',');
                if (ptr != NULL) {
                    *ptr = '\0';
                    ++ptr;
                }
                if (!load_extension(optarg, ptr)) {
                    exit(EXIT_FAILURE);
                }
                if (ptr != NULL) {
                    *(ptr - 1) = ',';
                }
            }
            break;
        default:
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    if (getenv("MEMCACHED_REQS_TAP_EVENT") != NULL) {
        settings.reqs_per_tap_event = atoi(getenv("MEMCACHED_REQS_TAP_EVENT"));
    }

    if (settings.reqs_per_tap_event <= 0) {
        settings.reqs_per_tap_event = DEFAULT_REQS_PER_TAP_EVENT;
    }


    if (install_sigterm_handler() != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to install SIGTERM handler\n");
        exit(EXIT_FAILURE);
    }

    if (num_ports > 0) {
        settings.num_ports = num_ports;
    }

    if (settings.require_sasl) {
        if (!protocol_specified) {
            settings.binding_protocol = binary_prot;
        } else {
            if (settings.binding_protocol == negotiating_prot) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: You cannot use auto-negotiating protocol while requiring SASL.\n");
                exit(EX_USAGE);
            }
        }
    }

    if (engine_config != NULL && strlen(old_options) > 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "ERROR: You can't mix -e with the old options\n");
        return EX_USAGE;
    } else if (engine_config == NULL && strlen(old_options) > 0) {
        engine_config = old_options;
    }

#ifndef WIN32
    /* @todo refactor this out to helper functions */
    if (maxcore != 0) {
        struct rlimit rlim_new;
        /*
         * First try raising to infinity; if that fails, try bringing
         * the soft limit to the hard.
         */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0) {
                /* failed. try raising just to the old max */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        /*
         * getrlimit again to see what we ended up with. Only fail if
         * the soft limit ends up 0, because then no core files will be
         * created at all.
         */

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }

    /*
     * If needed, increase rlimits to allow as many connections
     * as needed.
     */

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        int maxfiles = settings.maxconns + (3 * (settings.num_threads + 2));
        int syslimit = rlim.rlim_cur;
        if (rlim.rlim_cur < maxfiles) {
            rlim.rlim_cur = maxfiles;
        }
        if (rlim.rlim_max < rlim.rlim_cur) {
            rlim.rlim_max = rlim.rlim_cur;
        }
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            const char *fmt;
            int req;
            fmt = "WARNING: maxconns cannot be set to (%d) connections due to "
                "system\nresouce restrictions. Increase the number of file "
                "descriptors allowed\nto the memcached user process or start "
                "memcached as root (remember\nto use the -u parameter).\n"
                "The maximum number of connections is set to %d.\n";
            req = settings.maxconns;
            settings.maxconns = syslimit - (3 * (settings.num_threads + 2));
            if (settings.maxconns < 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                         "failed to set rlimit for open files. Try starting as"
                         " root or requesting smaller maxconns value.\n");
                exit(EX_OSERR);
            }
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            fmt, req, settings.maxconns);
        }
    }
#endif

    /* Sanity check for the connection structures */
    if (settings.port != 0) {
        nfiles += 2;
    }

    if (settings.maxconns <= nfiles) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Configuratioin error. \n"
                "You specified %d connections, but the system will use at "
                "least %d\nconnection structures to start.\n",
                settings.maxconns, nfiles);
        exit(EX_USAGE);
    }

    /* allocate the connection array */
    initialize_connections();

#ifndef WIN32
    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) {
        if (username == 0 || *username == '\0') {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "can't run as root without the -u switch\n");
            exit(EX_USAGE);
        }
        if ((pw = getpwnam(username)) == 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "can't find the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to assume identity of user %s: %s\n", username,
                    strerror(errno));
            exit(EX_OSERR);
        }
    }
#endif

    cbsasl_server_init();

    /* lock paged memory if needed */
    if (lock_memory) {
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "warning: -k invalid, mlockall() failed: %s\n",
                    strerror(errno));
        }
#else
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }

    /* initialize main thread libevent instance */
    main_base = event_base_new();

    /* Load the storage engine */
    if (!load_engine(engine,get_server_api,settings.extensions.logger,&engine_handle)) {
        /* Error already reported */
        exit(EXIT_FAILURE);
    }

    if (!init_engine(engine_handle,engine_config,settings.extensions.logger)) {
        return false;
    }

    if (settings.verbose > 0) {
        log_engine_details(engine_handle,settings.extensions.logger);
    }
    settings.engine.v1 = (ENGINE_HANDLE_V1 *) engine_handle;

    if (settings.engine.v1->arithmetic == NULL) {
        settings.engine.v1->arithmetic = internal_arithmetic;
    }

    /* initialize other stuff */
    stats_init();

    default_independent_stats = new_independent_stats();

#ifndef WIN32
    /* daemonize if requested */
    /* if we want to ensure our ability to dump core, don't chdir to / */
    if (do_daemonize) {
        if (sigignore(SIGHUP) == -1) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to ignore SIGHUP: ", strerror(errno));
        }
        if (daemonize(maxcore, settings.verbose) == -1) {
             settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }

    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
#endif

    /* start up worker threads if MT mode */
    thread_init(settings.num_threads, main_base, dispatch_event_handler);

    /* initialise clock event */
    clock_handler(0, 0, 0);

    /* create the listening socket, bind it, and init */
    {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        char temp_portnumber_filename[PATH_MAX];
        FILE *portnumber_file = NULL;

        if (portnumber_filename != NULL) {
            snprintf(temp_portnumber_filename,
                     sizeof(temp_portnumber_filename),
                     "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        if (settings.port && server_sockets(settings.port, portnumber_file)) {
            char msg[80];
            snprintf(msg, sizeof(msg), "Failed to listen on TCP port %d: %%s",
                    settings.port);
            log_socket_error(EXTENSION_LOG_WARNING, NULL, msg);
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

#ifndef WIN32
    if (pid_file != NULL) {
        save_pid(pid_file);
    }
#endif

    /* Drop privileges no longer needed */
    drop_privileges();

    /* Optional parent monitor */
    setup_parent_monitor();

    if (!memcached_shutdown) {
        /* enter the event loop */
        event_base_loop(main_base, 0);
    }

    if (settings.verbose) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                                        "Initiating shutdown\n");
    }
    threads_shutdown();

    settings.engine.v1->destroy(settings.engine.v0, false);

    /* remove the PID file if we're a daemon */
#ifndef WIN32
    if (do_daemonize)
        remove_pidfile(pid_file);
#endif

    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);
    /* Free the memory used by listening_port structure */
    if (stats.listening_ports) {
        free(stats.listening_ports);
    }

    event_base_free(main_base);
    release_independent_stats(default_independent_stats);
    destroy_connections();

    if (get_alloc_hooks_type() == none) {
        unload_engine();
    }

    return EXIT_SUCCESS;
}
