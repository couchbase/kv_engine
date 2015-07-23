/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config_parse.h"
#include "debug_helpers.h"
#include "memcached.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "alloc_hooks.h"
#include "utilities/engine_loader.h"
#include "timings.h"
#include "cmdline.h"
#include "connections.h"
#include "mcbp_topkeys.h"
#include "mcbp_validators.h"
#include "ioctl.h"
#include "mc_time.h"
#include "utilities/protocol2text.h"
#include "breakpad.h"
#include "runtime.h"
#include "mcaudit.h"
#include "subdocument.h"
#include "enginemap.h"
#include "buckets.h"
#include "hash.h"
#include "topkeys.h"

#include <platform/strerror.h>

#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <ctype.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <stdarg.h>
#include <stddef.h>
#include <snappy-c.h>
#include <cJSON.h>
#include <JSON_checker.h>
#include <engines/default_engine.h>
#include <vector>
// MB-14649: log crashing on windows..
#include <math.h>

#if HAVE_LIBNUMA
#include <numa.h>
#endif

/* Forward declaration */
bool binary_response_handler(const void *key, uint16_t keylen,
                             const void *ext, uint8_t extlen,
                             const void *body, uint32_t bodylen,
                             uint8_t datatype, uint16_t status,
                             uint64_t cas, const void *cookie);

/**
 * All of the buckets in couchbase is stored in this array.
 */
static cb_mutex_t buckets_lock;
std::vector<Bucket> all_buckets;

bool *topkey_commands;

static void cookie_set_admin(const void *cookie);
static bool cookie_is_admin(const void *cookie);

static ENGINE_HANDLE* v1_handle_2_handle(ENGINE_HANDLE_V1* v1) {
    return reinterpret_cast<ENGINE_HANDLE*>(v1);
}

/* Wrap the engine interface ! */

void bucket_item_set_cas(Connection *c, item *it, uint64_t cas) {
    c->bucket.engine->item_set_cas(v1_handle_2_handle(c->bucket.engine),
                                   c, it, cas);
}

void bucket_reset_stats(Connection *c) {
    c->bucket.engine->reset_stats(v1_handle_2_handle(c->bucket.engine), c);
}

ENGINE_ERROR_CODE bucket_get_engine_vb_map(Connection *c,
                                           engine_get_vb_map_cb callback) {
    return c->bucket.engine->get_engine_vb_map(v1_handle_2_handle(c->bucket.engine),
                                               c, callback);
}

static bool bucket_get_item_info(Connection *c, const item* item, item_info *item_info) {
    return c->bucket.engine->get_item_info(v1_handle_2_handle(c->bucket.engine),
                                           c, item, item_info);
}

static bool bucket_set_item_info(Connection *c, item* item, const item_info *item_info) {
    return c->bucket.engine->set_item_info(v1_handle_2_handle(c->bucket.engine),
                                           c, item, item_info);
}

static ENGINE_ERROR_CODE bucket_store(Connection *c,
                               item* item,
                               uint64_t *cas,
                               ENGINE_STORE_OPERATION operation,
                               uint16_t vbucket) {
    return c->bucket.engine->store(v1_handle_2_handle(c->bucket.engine), c, item,
                                   cas, operation, vbucket);
}

static ENGINE_ERROR_CODE bucket_get(Connection *c,
                             item** item,
                             const void* key,
                             const int nkey,
                             uint16_t vbucket) {
    return c->bucket.engine->get(v1_handle_2_handle(c->bucket.engine), c, item,
                                 key, nkey, vbucket);
}

ENGINE_ERROR_CODE bucket_unknown_command(Connection *c,
                                         protocol_binary_request_header *request,
                                         ADD_RESPONSE response) {
    return c->bucket.engine->unknown_command(v1_handle_2_handle(c->bucket.engine), c,
                                             request, response);
}

static void shutdown_server(void);

std::atomic<bool> memcached_shutdown;

/* Lock for global stats */
static cb_mutex_t stats_lock;

/**
 * Structure to save ns_server's session cas token.
 */
static struct session_cas {
    uint64_t value;
    uint64_t ctr;
    cb_mutex_t mutex;
} session_cas;

void STATS_LOCK() {
    cb_mutex_enter(&stats_lock);
}

void STATS_UNLOCK() {
    cb_mutex_exit(&stats_lock);
}

/*
 * forward declarations
 */
static SOCKET new_socket(struct addrinfo *ai);
static int try_read_command(Connection *c);
static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data);
static SERVER_HANDLE_V1 *get_server_api(void);

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, Connection *c);
static void process_stat_settings(ADD_STAT add_stats, void *c);
static void process_bucket_details(Connection *c);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void complete_nread(Connection *c);
static int ensure_iov_space(Connection *c);
static int add_msghdr(Connection *c);

/** exported globals **/
struct stats stats;
struct settings settings;

/** file scope variables **/
Connection *listen_conn = NULL;
static struct event_base *main_base;

static struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

/*
 * MB-12470 requests an easy way to see when (some of) the statistics
 * counters were reset. This functions grabs the current time and tries
 * to format it to the current timezone by using ctime_r/s (which adds
 * a newline at the end for some obscure reason which we'll need to
 * strip off).
 *
 * This function expects that the stats lock is held by the caller to get
 * a "sane" result (otherwise one thread may see a garbled version), but
 * no crash will occur since the buffer is big enough and always zero
 * terminated.
 */
static char reset_stats_time[80];
static void set_stats_reset_time(void)
{
    time_t now = time(NULL);
#ifdef WIN32
    ctime_s(reset_stats_time, sizeof(reset_stats_time), &now);
#else
    ctime_r(&now, reset_stats_time);
#endif
    char *ptr = strchr(reset_stats_time, '\n');
    if (ptr) {
        *ptr = '\0';
    }
}

static void disassociate_bucket(Connection *c) {
    Bucket &b = all_buckets.at(c->bucket.idx);
    cb_mutex_enter(&b.mutex);
    b.clients--;

    c->bucket.idx = 0;
    c->bucket.engine = NULL;

    if (b.clients == 0 && b.state == BucketState::Destroying) {
        cb_cond_signal(&b.cond);
    }

    cb_mutex_exit(&b.mutex);
}

static bool associate_bucket(Connection *c, const char *name) {
    bool found = false;

    /* leave the current bucket */
    disassociate_bucket(c);

    /* Try to associate with the named bucket */
    /* @todo add auth checks!!! */
    for (int ii = 1; ii < settings.max_buckets && !found; ++ii) {
        Bucket &b = all_buckets.at(ii);
        cb_mutex_enter(&b.mutex);
        if (b.state == BucketState::Ready && strcmp(b.name, name) == 0) {
            b.clients++;
            c->bucket.idx = ii;
            c->bucket.engine = b.engine;
            found = true;
        }
        cb_mutex_exit(&b.mutex);
    }

    if (!found) {
        /* Bucket not found, connect to the "no-bucket" */
        Bucket &b = all_buckets.at(0);
        cb_mutex_enter(&b.mutex);
        b.clients++;
        cb_mutex_exit(&b.mutex);
        c->bucket.idx = 0;
        c->bucket.engine = b.engine;
    }

    return found;
}

void associate_initial_bucket(Connection *c) {
    Bucket &b = all_buckets.at(0);
    cb_mutex_enter(&b.mutex);
    b.clients++;
    cb_mutex_exit(&b.mutex);

    c->bucket.idx = 0;
    c->bucket.engine = b.engine;

    associate_bucket(c, "default");
}

/* Perform all callbacks of a given type for the given connection. */
void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *cookie)
{
    const Connection * connection = reinterpret_cast<const Connection *>(cookie); /* May not be true, but... */
    struct engine_event_handler *h = NULL;

    switch (type) {
        /*
         * The following events operates on a connection which is passed in
         * as the cookie.
         */
    case ON_AUTH:
    case ON_CONNECT:
    case ON_DISCONNECT:
        cb_assert(connection);
        cb_assert(connection->bucket.idx != -1);
        h = all_buckets[connection->bucket.idx].engine_event_handlers[type];
        break;
    case ON_LOG_LEVEL:
        assert(cookie == NULL);
        h = engine_event_handlers[type];
        break;
    default:
        cb_assert(false /* Unknown event */);
    }

    while (h) {
        h->cb(cookie, type, data, h->cb_data);
        h = h->next;
    }
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data)
{
    int idx;
    engine_event_handler* h =
            reinterpret_cast<engine_event_handler*>(calloc(sizeof(*h), 1));

    cb_assert(h);
    h->cb = cb;
    h->cb_data = cb_data;

    switch (type) {
    /*
     * The following events operates on a connection which is passed in
     * as the cookie.
     */
    case ON_AUTH:
    case ON_CONNECT:
    case ON_DISCONNECT:
        cb_assert(eh);
        for (idx = 0; idx < settings.max_buckets; ++idx) {
            if ((void *)eh == (void *)all_buckets[idx].engine) {
                break;
            }
        }
        cb_assert(idx < settings.max_buckets);
        h->next = all_buckets[idx].engine_event_handlers[type];
        all_buckets[idx].engine_event_handlers[type] = h;
        break;
    case ON_LOG_LEVEL:
        cb_assert(eh == NULL);
        h->next = engine_event_handlers[type];
        engine_event_handlers[type] = h;
        break;
    default:
        cb_assert(false /* Unknown event */);
    }
}

static void free_callbacks() {
    // free per-bucket callbacks.
    for (int idx = 0; idx < settings.max_buckets; ++idx) {
        for (int type = 0; type < MAX_ENGINE_EVENT_TYPE; type++) {
            engine_event_handler* h = all_buckets[idx].engine_event_handlers[type];
            while (h != NULL) {
                engine_event_handler* tmp = h;
                h = h->next;
                free(tmp);
            }
        }
    }

    // free global callbacks
    for (int type = 0; type < MAX_ENGINE_EVENT_TYPE; type++) {
        engine_event_handler* h = engine_event_handlers[type];
        while (h != NULL) {
            engine_event_handler* tmp = h;
            h = h->next;
            free(tmp);
        }
    }
}

static void stats_init(void) {
    set_stats_reset_time();
    stats.conn_structs.reset();
    stats.total_conns.reset();
    stats.daemon_conns.reset();
    stats.rejected_conns.reset();
    stats.curr_conns.store(0, std::memory_order_relaxed);

    stats.listening_ports= reinterpret_cast<listening_port*>
        (calloc(settings.num_interfaces, sizeof(struct listening_port)));
}

struct thread_stats *get_thread_stats(Connection *c) {
    struct thread_stats *independent_stats;
    cb_assert(c->thread->index < (settings.num_threads + 1));
    independent_stats = all_buckets[c->bucket.idx].stats;
    return &independent_stats[c->thread->index];
}

static void stats_reset(const void *cookie) {
    auto *conn = (Connection *)cookie;
    STATS_LOCK();
    set_stats_reset_time();
    STATS_UNLOCK();
    stats.total_conns.reset();
    stats.rejected_conns.reset();
    threadlocal_stats_reset(all_buckets[conn->bucket.idx].stats);
    bucket_reset_stats(conn);
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
            ret = (int)(ret * 0.75f);
        }
        if (ret < 4) {
            ret = 4;
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
    static struct interface default_interface;
    default_interface.port = 11211;
    default_interface.maxconn = 1000;
    default_interface.backlog = 1024;

    memset(&settings, 0, sizeof(settings));
    settings.num_interfaces = 1;
    settings.interfaces = &default_interface;
    settings.bio_drain_buffer_sz = 8192;

    settings.verbose = 0;
    settings.num_threads = get_number_of_worker_threads();
    settings.require_sasl = false;
    settings.extensions.logger = get_stderr_logger();
    settings.config = NULL;
    settings.admin = NULL;
    settings.disable_admin = false;
    settings.datatype = false;
    settings.reqs_per_event_high_priority = 50;
    settings.reqs_per_event_med_priority = 5;
    settings.reqs_per_event_low_priority = 1;
    settings.default_reqs_per_event = 20;
    /*
     * The max object size is 20MB. Let's allow packets up to 30MB to
     * be handled "properly" by returing E2BIG, but packets bigger
     * than that will cause the server to disconnect the client
     */
    settings.max_packet_size = 30 * 1024 * 1024;

    settings.breakpad.enabled = false;
    settings.breakpad.minidump_dir = NULL;
    settings.breakpad.content = CONTENT_DEFAULT;
    settings.require_init = false;
    settings.max_buckets = COUCHBASE_MAX_NUM_BUCKETS;
    settings.admin = strdup("_admin");

    char *tmp = getenv("MEMCACHED_TOP_KEYS");
    settings.topkeys_size = 20;
    if (tmp != NULL) {
        int count;
        if (safe_strtol(tmp, &count)) {
            settings.topkeys_size = count;
        }
    }
}

static void settings_init_relocable_files(void)
{
    const char *root = DESTINATION_ROOT;
    const char sep = DIRECTORY_SEPARATOR_CHARACTER;

    if (settings.root) {
        root = settings.root;
    }

    if (settings.rbac_file == NULL) {
        char fname[PATH_MAX];
        sprintf(fname, "%s%cetc%csecurity%crbac.json", root,
                sep, sep, sep);
        FILE *fp = fopen(fname, "r");
        if (fp != NULL) {
            settings.rbac_file = strdup(fname);
            fclose(fp);
        }
    }
}


/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(Connection *c)
{
    struct msghdr *msg;

    cb_assert(c != NULL);

    if (c->msgsize == c->msgused) {
        cb_assert(c->msgsize > 0);
        msg = reinterpret_cast<struct msghdr*>(
                realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr)));
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

    c->msgbytes = 0;
    c->msgused++;
    STATS_MAX(c, msgused_high_watermark, c->msgused);

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
    Connection *next;
    cb_mutex_enter(&listen_state.mutex);
    listen_state.disabled = true;
    listen_state.count = 10;
    ++listen_state.num_disable;
    cb_mutex_exit(&listen_state.mutex);

    for (next = listen_conn; next; next = next->next) {
        next->updateEvent(0);
        if (listen(next->getSocketDescriptor(), 1) != 0) {
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
            stats.curr_conns.fetch_sub(1, std::memory_order_relaxed);
            if (is_listen_disabled()) {
                notify_dispatcher();
            }
        }
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
    } else if (state == conn_refresh_ssl_certs) {
        return "conn_refresh_ssl_cert";
    } else if (state == conn_flush) {
        return "conn_flush";
    } else if (state == conn_audit_configuring) {
        return "conn_audit_configuring";
    } else if (state == conn_create_bucket) {
        return "conn_create_bucket";
    } else if (state == conn_delete_bucket) {
        return "conn_delete_bucket";
    } else {
        return "Unknown";
    }
}

static bucket_id_t get_bucket_id(const void *cookie) {
    /* @todo fix this. Currently we're using the index as the id,
     * but this should be changed to be a uniqe ID that won't be
     * reused.
     */
    return ((Connection *)(cookie))->bucket.idx;
}

void collect_timings(const Connection *c) {
    hrtime_t now = gethrtime();
    const hrtime_t elapsed_ns = now - c->start;
    // aggregated timing for all buckets
    all_buckets[0].timings.collect(c->cmd, elapsed_ns);

    // timing for current bucket
    bucket_id_t bucketid = get_bucket_id(c);
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(c->cmd, elapsed_ns);
    }

    // Log operations taking longer than 0.5s
    const hrtime_t elapsed_ms = elapsed_ns / (1000 * 1000);
    if (elapsed_ms > 500) {
        const char *opcode = memcached_opcode_2_text(c->cmd);
        char opcodetext[10];
        if (opcode == NULL) {
            snprintf(opcodetext, sizeof(opcodetext), "0x%0X", c->cmd);
            opcode = opcodetext;
        }
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "%u: Slow %s operation on connection: %lu ms",
                                        c->getId(), opcode,
                                        (unsigned long)elapsed_ms);
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(Connection *c) {
    cb_assert(c != NULL);

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


int add_iov(Connection *c, const void *buf, size_t len) {
    struct msghdr *m;
    size_t leftover;
    bool limit_to_mtu;

    cb_assert(c != NULL);

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
            if (add_msghdr(c) != 0) {
                return -1;
            }
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

        c->msgbytes += (int)len;
        c->iovused++;
        STATS_MAX(c, iovused_high_watermark, c->iovused);
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(Connection *c) {
    char *ret = c->read.curr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    cb_assert(ret >= c->read.buf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(Connection *c) {
    return c->read.curr - (c->binary_header.request.keylen);
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
                                      uint32_t client, bool from_client,
                                      const char *prefix,
                                      const char *data,
                                      size_t size)
{
    ssize_t nw = snprintf(dest, destsz, "%c%u %s", from_client ? '>' : '<',
                          client, prefix);
    ssize_t offset = nw;

    if (nw == -1) {
        return -1;
    }

    for (size_t ii = 0; ii < size; ++ii) {
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

int add_bin_header(Connection *c, uint16_t err, uint8_t ext_len, uint16_t key_len,
                   uint32_t body_len, uint8_t datatype) {
    protocol_binary_response_header* header;

    cb_assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        return -1;
    }

    header = (protocol_binary_response_header *)c->write.buf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = ext_len;
    header->response.datatype = datatype;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = htonll(c->cas);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer, sizeof(buffer), c->getId(), false,
                                   "Writing bin response:",
                                   (const char*)header->bytes,
                                   sizeof(header->bytes)) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "%s", buffer);
        }
    }

    return add_iov(c, c->write.buf, sizeof(header->response));
}

protocol_binary_response_status engine_error_2_protocol_error(ENGINE_ERROR_CODE e) {
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
    case ENGINE_NO_BUCKET:
        return PROTOCOL_BINARY_RESPONSE_NO_BUCKET;
    case ENGINE_EBUSY:
        return PROTOCOL_BINARY_RESPONSE_EBUSY;
    default:
        ret = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    return ret;
}

static ENGINE_ERROR_CODE get_vb_map_cb(const void *cookie,
                                       const void *map,
                                       size_t mapsize)
{
    char *buf;
    Connection *c = (Connection *)cookie;
    protocol_binary_response_header header;
    size_t needed = mapsize+ sizeof(protocol_binary_response_header);
    if (!grow_dynamic_buffer(c, needed)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                    "<%d ERROR: Failed to allocate memory for response",
                    c->getId());
        return ENGINE_ENOMEM;
    }

    buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    memset(&header, 0, sizeof(header));

    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = c->binary_header.request.opcode;
    header.response.status = (uint16_t)htons(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    header.response.bodylen = htonl((uint32_t)mapsize);
    header.response.opaque = c->opaque;

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);
    memcpy(buf, map, mapsize);
    c->dynamic_buffer.offset += needed;

    return ENGINE_SUCCESS;
}

void write_bin_packet(Connection *c, protocol_binary_response_status err) {
    if (err == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
        ENGINE_ERROR_CODE ret;

        ret = bucket_get_engine_vb_map(c, get_vb_map_cb);
        if (ret == ENGINE_SUCCESS) {
            write_and_free(c, &c->dynamic_buffer);
        } else {
            c->setState(conn_closing);
        }
    } else {
        ssize_t len = 0;
        const char *errtext = NULL;

        switch (err) {
        case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        case PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED:
        case PROTOCOL_BINARY_RESPONSE_AUTH_STALE:
        case PROTOCOL_BINARY_RESPONSE_NO_BUCKET:
            break;
        default:
            errtext = memcached_protocol_errcode_2_text(err);
            if (errtext != NULL) {
                len = (ssize_t)strlen(errtext);
            }
        }

        if (errtext && settings.verbose > 1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            ">%u Writing an error: %s",
                                            c->getId(), errtext);
        }

        add_bin_header(c, err, 0, 0, len, PROTOCOL_BINARY_RAW_BYTES);
        if (errtext) {
            add_iov(c, errtext, len);
        }
        c->setState(conn_mwrite);
        c->write_and_go = conn_new_cmd;
    }
}

/* Form and send a response to a command over the binary protocol.
 * NOTE: Data from `d` is *not* immediately copied out (it's address is just
 *       added to an iovec), and thus must be live until transmit() is later
 *       called - (aka don't use stack for `d`).
 */
static void write_bin_response(Connection *c, const void *d, int extlen, int keylen,
                               int dlen) {
    if (!c->isNoReply() || c->cmd == PROTOCOL_BINARY_CMD_GET ||
        c->cmd == PROTOCOL_BINARY_CMD_GETK) {
        if (add_bin_header(c, 0, extlen, keylen, dlen, PROTOCOL_BINARY_RAW_BYTES) == -1) {
            c->setState(conn_closing);
            return;
        }
        add_iov(c, d, dlen);
        c->setState(conn_mwrite);
        c->write_and_go = conn_new_cmd;
    } else {
        if (c->start != 0) {
            collect_timings(c);
            c->start = 0;
        }
        c->setState(conn_new_cmd);
    }
}

/**
 * Triggers topkeys_update (i.e., increments topkeys stats) if called by a
 * valid operation.
 */
void update_topkeys(const char *key, size_t nkey, Connection *c) {

    if (topkey_commands[c->binary_header.request.opcode]) {
        cb_assert(all_buckets[c->bucket.idx].topkeys != nullptr);
        all_buckets[c->bucket.idx].topkeys->updateKey(key, nkey,
                                                      mc_time_get_current_time());
    }
}

static void process_bin_get(Connection *c) {
    item *it;
    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->write.buf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint16_t keylen;
    uint32_t bodylen;
    int ii;
    ENGINE_ERROR_CODE ret;
    uint8_t datatype;
    bool need_inflate = false;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    "GET", key, nkey) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s",
                                            buffer);
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    if (ret == ENGINE_SUCCESS) {
        ret = bucket_get(c, &it, key, (int)nkey,
                         c->binary_header.request.vbucket);
    }

    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = IOV_MAX;

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HIT(c, get, key, nkey);

        if (!bucket_get_item_info(c, it, &info.info)) {
            c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Failed to get item info",
                                            c->getId());
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            break;
        }

        datatype = info.info.datatype;
        if (!c->isSupportsDatatype()) {
            if ((datatype & PROTOCOL_BINARY_DATATYPE_COMPRESSED) == PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
                need_inflate = true;
            } else {
                datatype = PROTOCOL_BINARY_RAW_BYTES;
            }
        }

        keylen = 0;
        bodylen = sizeof(rsp->message.body) + info.info.nbytes;

        if ((c->cmd == PROTOCOL_BINARY_CMD_GETK) ||
            (c->cmd == PROTOCOL_BINARY_CMD_GETKQ)) {
            bodylen += (uint32_t)nkey;
            keylen = (uint16_t)nkey;
        }

        if (need_inflate) {
            if (info.info.nvalue != 1) {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            } else if (binary_response_handler(key, keylen,
                                               &info.info.flags, 4,
                                               info.info.value[0].iov_base,
                                               (uint32_t)info.info.value[0].iov_len,
                                               datatype,
                                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                               info.info.cas, c)) {
                write_and_free(c, &c->dynamic_buffer);
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            }
        } else {
            if (add_bin_header(c, 0, sizeof(rsp->message.body),
                               keylen, bodylen, datatype) == -1) {
                c->setState(conn_closing);
                return;
            }
            rsp->message.header.response.cas = htonll(info.info.cas);

            /* add the flags */
            rsp->message.body.flags = info.info.flags;
            add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

            if ((c->cmd == PROTOCOL_BINARY_CMD_GETK) ||
                (c->cmd == PROTOCOL_BINARY_CMD_GETKQ)) {
                add_iov(c, info.info.key, nkey);
            }

            for (ii = 0; ii < info.info.nvalue; ++ii) {
                add_iov(c, info.info.value[ii].iov_base,
                        info.info.value[ii].iov_len);
            }
            c->setState(conn_mwrite);
            /* Remember this item so we can garbage collect it later */
            c->item = it;
        }
        update_topkeys(key, nkey, c);
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, get, key, nkey);

        MEMCACHED_COMMAND_GET(c->getId(), key, nkey, -1, 0);

        if (c->isNoReply()) {
            c->setState(conn_new_cmd);
        } else {
            if ((c->cmd == PROTOCOL_BINARY_CMD_GETK) ||
                (c->cmd == PROTOCOL_BINARY_CMD_GETKQ)) {
                char *ofs = c->write.buf + sizeof(protocol_binary_response_header);
                if (add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                                   0, (uint16_t)nkey,
                                   (uint32_t)nkey, PROTOCOL_BINARY_RAW_BYTES) == -1) {
                    c->setState(conn_closing);
                    return;
                }
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                c->setState(conn_mwrite);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
            }
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             Connection *c) {
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
        cb_assert(key != NULL);
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->dynamic_buffer.offset += sizeof(header.response) + bodylen;
}


static void append_stats(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie)
{
    size_t needed;
    Connection *c = (Connection *)cookie;
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    needed = vlen + klen + sizeof(protocol_binary_response_header);
    if (!grow_dynamic_buffer(c, needed)) {
        return ;
    }
    append_bin_stats(key, klen, val, vlen, c);
    cb_assert(c->dynamic_buffer.offset <= c->dynamic_buffer.size);
}

static void bin_read_chunk(Connection *c,
                           bin_substates next_substate,
                           uint32_t chunk) {
    ptrdiff_t offset;
    cb_assert(c);
    c->setSubstate(next_substate);
    c->rlbytes = chunk;

    /* Ok... do we have room for everything in our buffer? */
    offset = c->read.curr + sizeof(protocol_binary_request_header) - c->read.buf;
    if (c->rlbytes > c->read.size - offset) {
        size_t nsize = c->read.size;
        size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->read.size) {
            char *newm;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                        "%u: Need to grow buffer from %lu to %lu",
                        c->getId(), (unsigned long)c->read.size, (unsigned long)nsize);
            }
            newm = reinterpret_cast<char*>(realloc(c->read.buf, nsize));
            if (newm == NULL) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                            "%u: Failed to grow buffer.. closing connection",
                            c->getId());
                c->setState(conn_closing);
                return;
            }

            c->read.buf= newm;
            /* rcurr should point to the same offset in the packet */
            c->read.curr = c->read.buf + offset - sizeof(protocol_binary_request_header);
            c->read.size = (int)nsize;
        }
        if (c->read.buf != c->read.curr) {
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%u: Repack input buffer\n",
                                                c->getId());
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->read.curr + sizeof(protocol_binary_request_header);
    c->setState(conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(Connection *c) {
    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                         "%u: Protocol error (opcode %02x), close connection",
                                    c->getId(), c->binary_header.request.opcode);
    c->write_and_go = conn_closing;
}

static void get_auth_data(const void *cookie, auth_data_t *data) {
    Connection *c = (Connection *)cookie;
    if (c->sasl_conn) {
        cbsasl_getprop(c->sasl_conn, CBSASL_USERNAME,
                       reinterpret_cast<const void**>(&data->username));
        cbsasl_getprop(c->sasl_conn, CBSASL_CONFIG,
                       reinterpret_cast<const void**>(&data->config));
    } else {
        data->username = NULL;
        data->config = NULL;
    }
}

static bool authenticated(Connection *c) {
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_HELLO:
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
                "%u: authenticated() in cmd 0x%02x is %s\n",
                c->getId(), c->cmd, rv ? "true" : "false");
    }

    return rv;
}

bool binary_response_handler(const void *key, uint16_t keylen,
                             const void *ext, uint8_t extlen,
                             const void *body, uint32_t bodylen,
                             uint8_t datatype, uint16_t status,
                             uint64_t cas, const void *cookie)
{
    protocol_binary_response_header header;
    char *buf;
    Connection *c = (Connection *)cookie;
    /* Look at append_bin_stats */
    size_t needed;
    bool need_inflate = false;
    size_t inflated_length;

    if (!c->isSupportsDatatype()) {
        if ((datatype & PROTOCOL_BINARY_DATATYPE_COMPRESSED) ==
                                PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
            need_inflate = true;
        }
        /* We may silently drop the knowledge about a JSON item */
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    needed = keylen + extlen + sizeof(protocol_binary_response_header);
    if (need_inflate) {
        if (snappy_uncompressed_length(reinterpret_cast<const char*>(body),
                                       bodylen, &inflated_length) != SNAPPY_OK) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                    "<%u ERROR: Failed to inflate body, "
                    "Key: %s may have an incorrect datatype, "
                    "Datatype indicates that document is %s" ,
                    c->getId(), (const char *)key,
                    (datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED) ?
                    "RAW_COMPRESSED" : "JSON_COMPRESSED");
            return false;
        }
        needed += inflated_length;
    } else {
        needed += bodylen;
    }

    if (!grow_dynamic_buffer(c, needed)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                          "<%u ERROR: Failed to allocate memory for response",
                          c->getId());
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
    if (need_inflate) {
        header.response.bodylen = htonl((uint32_t)(inflated_length + keylen + extlen));
    } else {
        header.response.bodylen = htonl(bodylen + keylen + extlen);
    }
    header.response.opaque = c->opaque;
    header.response.cas = htonll(cas);

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (extlen > 0) {
        memcpy(buf, ext, extlen);
        buf += extlen;
    }

    if (keylen > 0) {
        cb_assert(key != NULL);
        memcpy(buf, key, keylen);
        buf += keylen;
    }

    if (bodylen > 0) {
        if (need_inflate) {
        if (snappy_uncompress(reinterpret_cast<const char*>(body), bodylen,
                              buf, &inflated_length) != SNAPPY_OK) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                        "<%u Failed to inflate item", c->getId());
                return false;
            }
        } else {
            memcpy(buf, body, bodylen);
        }
    }

    c->dynamic_buffer.offset += needed;
    return true;
}

/**
 * Tap stats (these are only used by the tap thread, so they don't need
 * to be in the threadlocal struct right now...
 */
struct tap_cmd_stats {
    StatsCounter<uint64_t> connect;
    StatsCounter<uint64_t> mutation;
    StatsCounter<uint64_t> checkpoint_start;
    StatsCounter<uint64_t> checkpoint_end;
    StatsCounter<uint64_t> del;
    StatsCounter<uint64_t> flush;
    StatsCounter<uint64_t> opaque;
    StatsCounter<uint64_t> vbucket_set;
};

struct tap_stats {
    struct tap_cmd_stats sent;
    struct tap_cmd_stats received;
} tap_stats;

static void ship_tap_log(Connection *c) {
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
                                            "%u: Failed to create output headers. Shutting down tap connection",
                                            c->getId());
        }
        c->setState(conn_closing);
        return ;
    }
    /* @todo add check for buffer overflow of c->write.buf) */
    c->write.bytes = 0;
    c->write.curr = c->write.buf;

    auto tap_iterator = c->getTapIterator();
    do {
        /* @todo fixme! */
        void *engine;
        uint16_t nengine;
        uint8_t ttl;
        uint16_t tap_flags;
        uint32_t seqno;
        uint16_t vbucket;
        tap_event_t event;
        bool inflate = false;
        size_t inflated_length = 0;

        union {
            protocol_binary_request_tap_mutation mutation;
            protocol_binary_request_tap_delete del;
            protocol_binary_request_tap_flush flush;
            protocol_binary_request_tap_opaque opaque;
            protocol_binary_request_noop noop;
        } msg;
        item_info_holder info;

        if (ii++ == 10) {
            break;
        }

        event = tap_iterator(v1_handle_2_handle(c->bucket.engine), c, &it,
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
            memcpy(c->write.curr, msg.noop.bytes, sizeof(msg.noop.bytes));
            add_iov(c, c->write.curr, sizeof(msg.noop.bytes));
            c->write.curr += sizeof(msg.noop.bytes);
            c->write.bytes += sizeof(msg.noop.bytes);
            break;
        case TAP_PAUSE :
            more_data = false;
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
        case TAP_MUTATION:
            if (!bucket_get_item_info(c, it, &info.info)) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to get item info",
                                                c->getId());
                break;
            }
            try {
                c->reservedItems.push_back(it);
            } catch (std::bad_alloc) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to grow item array",
                                                c->getId());
                break;
            }
            send_data = true;

            if (event == TAP_CHECKPOINT_START) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START;
                tap_stats.sent.checkpoint_start++;
            } else if (event == TAP_CHECKPOINT_END) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END;
                tap_stats.sent.checkpoint_end++;
            } else if (event == TAP_MUTATION) {
                msg.mutation.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_MUTATION;
                tap_stats.sent.mutation++;
            }

            msg.mutation.message.header.request.cas = htonll(info.info.cas);
            msg.mutation.message.header.request.keylen = htons(info.info.nkey);
            msg.mutation.message.header.request.extlen = 16;
            if (c->isSupportsDatatype()) {
                msg.mutation.message.header.request.datatype = info.info.datatype;
            } else {
                switch (info.info.datatype) {
                case 0:
                    break;
                case PROTOCOL_BINARY_DATATYPE_JSON:
                    break;
                case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
                case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                    inflate = true;
                    break;
                default:
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                    "%u: shipping data with"
                                                    " an invalid datatype "
                                                    "(stripping info)",
                                                    c->getId());
                }
                msg.mutation.message.header.request.datatype = 0;
            }

            bodylen = 16 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    if (snappy_uncompressed_length
                            (reinterpret_cast<const char*>(info.info.value[0].iov_base),
                             info.info.nbytes, &inflated_length) == SNAPPY_OK) {
                        bodylen += (uint32_t)inflated_length;
                    } else {
                        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                        "<%u Failed to determine inflated size. Sending as compressed",
                                                        c->getId());
                        inflate = false;
                        bodylen += info.info.nbytes;
                    }
                } else {
                    bodylen += info.info.nbytes;
                }
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
            memcpy(c->write.curr, msg.mutation.bytes, sizeof(msg.mutation.bytes));

            add_iov(c, c->write.curr, sizeof(msg.mutation.bytes));
            c->write.curr += sizeof(msg.mutation.bytes);
            c->write.bytes += sizeof(msg.mutation.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                add_iov(c, c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            add_iov(c, info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    char *buf = reinterpret_cast<char*>(malloc(inflated_length));
                    if (buf == NULL) {
                        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                        "%u: FATAL: failed to allocate buffer of size %" PRIu64
                                                        " to inflate object into. "
                                                        "Shutting down connection",
                                                        c->getId(), inflated_length);
                        c->setState(conn_closing);
                        return;
                    }
                    const char *body = reinterpret_cast<const char*>(info.info.value[0].iov_base);
                    size_t bodylen = info.info.value[0].iov_len;
                    if (snappy_uncompress(body, bodylen,
                                          buf, &inflated_length) == SNAPPY_OK) {
                        try {
                            c->temp_alloc.push_back(buf);
                        } catch (std::bad_alloc) {
                            free(buf);
                            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                            "%u: FATAL: failed to allocate space to keep temporary buffer",
                                                            c->getId());
                            c->setState(conn_closing);
                            return;
                        }
                        add_iov(c, buf, inflated_length);
                    } else {
                        free(buf);
                        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                        "%u: FATAL: failed to inflate object. shutting down connection",
                                                        c->getId());
                        c->setState(conn_closing);
                        return;
                    }
                } else {
                    int xx;
                    for (xx = 0; xx < info.info.nvalue; ++xx) {
                        add_iov(c, info.info.value[xx].iov_base,
                                info.info.value[xx].iov_len);
                    }
                }
            }

            break;
        case TAP_DELETION:
            /* This is a delete */
            if (!bucket_get_item_info(c, it, &info.info)) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to get item info",
                                                c->getId());
                break;
            }
            try {
                c->reservedItems.push_back(it);
            } catch (std::bad_alloc) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to grow item array",
                                                c->getId());
                break;
            }
            send_data = true;
            msg.del.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_DELETE;
            msg.del.message.header.request.cas = htonll(info.info.cas);
            msg.del.message.header.request.keylen = htons(info.info.nkey);

            bodylen = 8 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                bodylen += info.info.nbytes;
            }
            msg.del.message.header.request.bodylen = htonl(bodylen);

            memcpy(c->write.curr, msg.del.bytes, sizeof(msg.del.bytes));
            add_iov(c, c->write.curr, sizeof(msg.del.bytes));
            c->write.curr += sizeof(msg.del.bytes);
            c->write.bytes += sizeof(msg.del.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                add_iov(c, c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            add_iov(c, info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                int xx;
                for (xx = 0; xx < info.info.nvalue; ++xx) {
                    add_iov(c, info.info.value[xx].iov_base,
                            info.info.value[xx].iov_len);
                }
            }

            tap_stats.sent.del++;
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
                tap_stats.sent.opaque++;
            } else if (event == TAP_FLUSH) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_FLUSH;
                tap_stats.sent.flush++;
            } else if (event == TAP_VBUCKET_SET) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET;
                msg.flush.message.body.tap.flags = htons(tap_flags);
                tap_stats.sent.vbucket_set++;
            }

            msg.flush.message.header.request.bodylen = htonl(8 + nengine);
            memcpy(c->write.curr, msg.flush.bytes, sizeof(msg.flush.bytes));
            add_iov(c, c->write.curr, sizeof(msg.flush.bytes));
            c->write.curr += sizeof(msg.flush.bytes);
            c->write.bytes += sizeof(msg.flush.bytes);
            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                add_iov(c, c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }
            break;
        default:
            abort();
        }
    } while (more_data);

    c->ewouldblock = false;
    if (send_data) {
        c->setState(conn_mwrite);
        if (disconnect) {
            c->write_and_go = conn_closing;
        } else {
            c->write_and_go = conn_ship_log;
        }
    } else {
        if (disconnect) {
            c->setState(conn_closing);
        } else {
            /* No more items to ship to the slave at this time.. suspend.. */
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%u: No more items in tap log.. waiting",
                                                c->getId());
            }
            c->ewouldblock = true;
        }
    }
}

static ENGINE_ERROR_CODE default_unknown_command(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*,
                                                 ENGINE_HANDLE*,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response)
{
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (!c->isSupportsDatatype() && request->request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie)) {
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_DISCONNECT;
        }
    } else {
        return bucket_unknown_command(c, request, response);
    }
}

struct request_lookup {
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor;
    BINARY_COMMAND_CALLBACK callback;
};

static struct request_lookup request_handlers[0x100];

typedef void (*RESPONSE_HANDLER)(Connection *);
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

static void process_bin_unknown_packet(Connection *c) {
    char* packet = c->read.curr -
                   (c->binary_header.request.bodylen + sizeof(c->binary_header));

    auto* req = reinterpret_cast<protocol_binary_request_header*>(packet);
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        struct request_lookup *rq = request_handlers + c->binary_header.request.opcode;
        ret = rq->callback(rq->descriptor,
                           v1_handle_2_handle(c->bucket.engine), c, req,
                           binary_response_handler);
    }



    switch (ret) {
    case ENGINE_SUCCESS:
        {
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, &c->dynamic_buffer);
            } else {
                c->setState(conn_new_cmd);
            }
            const char *key = packet +
                              sizeof(c->binary_header.request) +
                              c->binary_header.request.extlen;
            update_topkeys(key, c->binary_header.request.keylen, c);
            break;
        }
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        /* Release the dynamic buffer.. it may be partial.. */
        free(c->dynamic_buffer.buffer);
        c->dynamic_buffer.buffer = NULL;
        c->dynamic_buffer.size = 0;
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void cbsasl_refresh_main(void *c)
{
    int rv = cbsasl_server_refresh();
    if (rv == CBSASL_OK) {
        notify_io_complete(c, ENGINE_SUCCESS);
    } else {
        notify_io_complete(c, ENGINE_EINVAL);
    }
}

static ENGINE_ERROR_CODE refresh_cbsasl(Connection *c)
{
    cb_thread_t tid;
    int err;

    err = cb_create_named_thread(&tid, cbsasl_refresh_main, c, 1,
                                 "mc:refresh sasl");
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Failed to create cbsasl db "
                                        "update thread: %s",
                                        strerror(err));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
}

#if 0
static void ssl_certs_refresh_main(void *c)
{
    /* Update the internal certificates */

    notify_io_complete(c, ENGINE_SUCCESS);
}
#endif
static ENGINE_ERROR_CODE refresh_ssl_certs(Connection *c)
{
    (void)c;
#if 0
    cb_thread_t tid;
    int err;

    err = cb_create_thread(&tid, ssl_certs_refresh_main, c, 1);
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Failed to create ssl_certificate "
                                        "update thread: %s",
                                        strerror(err));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
#endif
    return ENGINE_SUCCESS;
}

static void process_bin_tap_connect(Connection *c) {
    TAP_ITERATOR iterator;
    char *packet = (c->read.curr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));
    auto* req = reinterpret_cast<protocol_binary_request_tap_connect*>(packet);
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
                                                "%u: ERROR: Invalid tap connect message",
                                                c->getId());
                c->setState(conn_closing);
                return ;
            }
        }
    } else {
        data -= 4;
        key -= 4;
    }

    if (settings.verbose && c->binary_header.request.keylen > 0) {
        char buffer[1024];
        size_t len = c->binary_header.request.keylen;
        if (len >= sizeof(buffer)) {
            len = sizeof(buffer) - 1;
        }
        memcpy(buffer, key, len);
        buffer[len] = '\0';
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "%u: Trying to connect with named tap connection: <%s>",
                                        c->getId(), buffer);
    }

    iterator = c->bucket.engine->get_tap_iterator(v1_handle_2_handle(c->bucket.engine),
                                                  c, key,
                                                  c->binary_header.request.keylen,
                                                  flags, data, ndata);

    if (iterator == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: FATAL: The engine does not support tap",
                                        c->getId());
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        c->write_and_go = conn_closing;
    } else {
        c->setMaxReqsPerEvent(settings.reqs_per_event_high_priority);
        c->setTapIterator(iterator);
        c->setCurrentEvent(EV_WRITE);
        c->setState(conn_ship_log);
    }
}

static void process_bin_tap_packet(tap_event_t event, Connection *c) {
    char *packet;
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

    cb_assert(c != NULL);
    packet = (c->read.curr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));
    auto* tap = reinterpret_cast<protocol_binary_request_tap_no_extras*>(packet);
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
            auto* mutation =
                    reinterpret_cast<protocol_binary_request_tap_mutation*>(tap);

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
            uint8_t datatype = c->binary_header.request.datatype;
            if (event == TAP_MUTATION && !c->isSupportsDatatype()) {
                if (checkUTF8JSON(reinterpret_cast<unsigned char*>(data),
                                  ndata)) {
                    datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                }
            }

            ret = c->bucket.engine->tap_notify(v1_handle_2_handle(c->bucket.engine), c,
                                               engine_specific, nengine,
                                               ttl - 1, tap_flags,
                                               event, seqno,
                                               key, nkey,
                                               flags, exptime,
                                               ntohll(tap->message.header.request.cas),
                                               datatype,
                                               data, ndata,
                                               c->binary_header.request.vbucket);
        }
    }

    switch (ret) {
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        if ((tap_flags & TAP_FLAG_ACK) || (ret != ENGINE_SUCCESS)) {
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        } else {
            c->setState(conn_new_cmd);
        }
    }
}

static void process_bin_tap_ack(Connection *c) {
    char *packet;
    uint32_t seqno;
    uint16_t status;
    char *key;
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    cb_assert(c != NULL);
    packet = (c->read.curr - (c->binary_header.request.bodylen + sizeof(c->binary_header)));
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(packet);
    seqno = ntohl(rsp->message.header.response.opaque);
    status = ntohs(rsp->message.header.response.status);
    key = packet + sizeof(rsp->bytes);

    if (c->bucket.engine->tap_notify != NULL) {
        ret = c->bucket.engine->tap_notify(v1_handle_2_handle(c->bucket.engine), c, NULL, 0,
                                           0, status,
                                           TAP_ACK, seqno, key,
                                           c->binary_header.request.keylen, 0, 0,
                                           0, c->binary_header.request.datatype, NULL,
                                           0, 0);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(Connection *c) {
    cb_assert(c != NULL);
    c->setState(conn_new_cmd);
}

/*******************************************************************************
 **                             DCP MESSAGE PRODUCERS                         **
 ******************************************************************************/
static ENGINE_ERROR_CODE dcp_message_get_failover_log(const void *cookie,
                                                      uint32_t opaque,
                                                      uint16_t vbucket)
{
    protocol_binary_request_dcp_get_failover_log packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_req(const void *cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags,
                                                uint64_t start_seqno,
                                                uint64_t end_seqno,
                                                uint64_t vbucket_uuid,
                                                uint64_t snap_start_seqno,
                                                uint64_t snap_end_seqno)
{
    protocol_binary_request_dcp_stream_req packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    packet.message.header.request.extlen = 48;
    packet.message.header.request.bodylen = htonl(48);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    packet.message.body.flags = ntohl(flags);
    packet.message.body.start_seqno = ntohll(start_seqno);
    packet.message.body.end_seqno = ntohll(end_seqno);
    packet.message.body.vbucket_uuid = ntohll(vbucket_uuid);
    packet.message.body.snap_start_seqno = ntohll(snap_start_seqno);
    packet.message.body.snap_end_seqno = ntohll(snap_end_seqno);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_add_stream_response(const void *cookie,
                                                         uint32_t opaque,
                                                         uint32_t dialogopaque,
                                                         uint8_t status)
{
    protocol_binary_response_dcp_add_stream packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic =  (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_ADD_STREAM;
    packet.message.header.response.extlen = 4;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = htonl(4);
    packet.message.header.response.opaque = opaque;
    packet.message.body.opaque = ntohl(dialogopaque);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker_response(const void *cookie,
                                                     uint32_t opaque,
                                                     uint8_t status)
{
    protocol_binary_response_dcp_snapshot_marker packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic =  (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state_response(const void *cookie,
                                                                uint32_t opaque,
                                                                uint8_t status)
{
    protocol_binary_response_dcp_set_vbucket_state packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic =  (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_end(const void *cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags)
{
    protocol_binary_request_dcp_stream_end packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_END;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.bodylen = htonl(4);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker(const void *cookie,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint32_t flags)
{
    protocol_binary_request_dcp_snapshot_marker packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.extlen = 20;
    packet.message.header.request.bodylen = htonl(20);
    packet.message.body.start_seqno = htonll(start_seqno);
    packet.message.body.end_seqno = htonll(end_seqno);
    packet.message.body.flags = htonl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_mutation(const void* cookie,
                                              uint32_t opaque,
                                              item *it,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              uint32_t lock_time,
                                              const void *meta,
                                              uint16_t nmeta,
                                              uint8_t nru)
{
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));
    item_info_holder info;
    protocol_binary_request_dcp_mutation packet;
    int xx;

    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    info.info.nvalue = IOV_MAX;

    if (!bucket_get_item_info(c, it, &info.info)) {
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    try {
        c->reservedItems.push_back(it);
    } catch (std::bad_alloc) {
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: Failed to grow item array",
                                        c->getId());
        return ENGINE_FAILED;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.info.cas);
    packet.message.header.request.keylen = htons(info.info.nkey);
    packet.message.header.request.extlen = 31;
    packet.message.header.request.bodylen = ntohl(31 + info.info.nkey + info.info.nbytes + nmeta);
    packet.message.header.request.datatype = info.info.datatype;
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.lock_time = htonl(lock_time);
    packet.message.body.flags = info.info.flags;
    packet.message.body.expiration = htonl(info.info.exptime);
    packet.message.body.nmeta = htons(nmeta);
    packet.message.body.nru = nru;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    add_iov(c, info.info.key, info.info.nkey);
    for (xx = 0; xx < info.info.nvalue; ++xx) {
        add_iov(c, info.info.value[xx].iov_base, info.info.value[xx].iov_len);
    }

    memcpy(c->write.curr, meta, nmeta);
    add_iov(c, c->write.curr, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_deletion(const void* cookie,
                                              uint32_t opaque,
                                              const void *key,
                                              uint16_t nkey,
                                              uint64_t cas,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              const void *meta,
                                              uint16_t nmeta)
{
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));
    protocol_binary_request_dcp_deletion packet;
    if (c->write.bytes + sizeof(packet.bytes) + nkey + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + nkey + nmeta);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    add_iov(c, c->write.curr, sizeof(packet.bytes) + nkey + nmeta);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;
    memcpy(c->write.curr, meta, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_expiration(const void* cookie,
                                                uint32_t opaque,
                                                const void *key,
                                                uint16_t nkey,
                                                uint64_t cas,
                                                uint16_t vbucket,
                                                uint64_t by_seqno,
                                                uint64_t rev_seqno,
                                                const void *meta,
                                                uint16_t nmeta)
{
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));
    protocol_binary_request_dcp_deletion packet;

    if (c->write.bytes + sizeof(packet.bytes) + nkey + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + nkey + nmeta);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    add_iov(c, c->write.curr, sizeof(packet.bytes) + nkey + nmeta);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;
    memcpy(c->write.curr, meta, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_flush(const void* cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket)
{
    protocol_binary_request_dcp_flush packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_FLUSH;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state(const void* cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket,
                                                       vbucket_state_t state)
{
    protocol_binary_request_dcp_set_vbucket_state packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
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

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_noop(const void* cookie,
                                          uint32_t opaque)
{
    protocol_binary_request_dcp_noop packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_NOOP;
    packet.message.header.request.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_buffer_acknowledgement(const void* cookie,
                                                            uint32_t opaque,
                                                            uint16_t vbucket,
                                                            uint32_t buffer_bytes)
{
    protocol_binary_request_dcp_buffer_acknowledgement packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.bodylen = ntohl(4);
    packet.message.body.buffer_bytes = ntohl(buffer_bytes);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    add_iov(c, c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_control(const void* cookie,
                                             uint32_t opaque,
                                             const void *key,
                                             uint16_t nkey,
                                             const void *value,
                                             uint32_t nvalue)
{
    protocol_binary_request_dcp_control packet;
    Connection *c = const_cast<Connection *>(reinterpret_cast<const Connection *>(cookie));

    if (c->write.bytes + sizeof(packet.bytes) + nkey + nvalue >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic =  (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.keylen = ntohs(nkey);
    packet.message.header.request.bodylen = ntohl(nvalue + nkey);

    add_iov(c, c->write.curr, sizeof(packet.bytes) + nkey + nvalue);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;

    memcpy(c->write.curr, value, nvalue);
    c->write.curr += nvalue;
    c->write.bytes += nvalue;

    return ENGINE_SUCCESS;
}

static void ship_dcp_log(Connection *c) {
    static struct dcp_message_producers producers = {
        dcp_message_get_failover_log,
        dcp_message_stream_req,
        dcp_message_add_stream_response,
        dcp_message_marker_response,
        dcp_message_set_vbucket_state_response,
        dcp_message_stream_end,
        dcp_message_marker,
        dcp_message_mutation,
        dcp_message_deletion,
        dcp_message_expiration,
        dcp_message_flush,
        dcp_message_set_vbucket_state,
        dcp_message_noop,
        dcp_message_buffer_acknowledgement,
        dcp_message_control
    };
    ENGINE_ERROR_CODE ret;

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Failed to create output headers. Shutting down DCP connection",
                                            c->getId());
        }
        c->setState(conn_closing);
        return ;
    }

    c->write.bytes = 0;
    c->write.curr = c->write.buf;
    c->ewouldblock = false;
    ret = c->bucket.engine->dcp.step(v1_handle_2_handle(c->bucket.engine), c, &producers);
    if (ret == ENGINE_SUCCESS) {
        /* the engine don't have more data to send at this moment */
        c->ewouldblock = true;
    } else if (ret == ENGINE_WANT_MORE) {
        /* The engine got more data it wants to send */
        ret = ENGINE_SUCCESS;
    }

    if (ret == ENGINE_SUCCESS) {
        c->setState(conn_mwrite);
        c->write_and_go = conn_ship_log;
    } else {
        c->setState(conn_closing);
    }
}

/******************************************************************************
 *                        TAP packet executors                                *
 ******************************************************************************/
static void tap_connect_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->get_tap_iterator == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.connect++;
        c->setState(conn_setup_tap_stream);
    }
}

static void tap_mutation_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.mutation++;
        process_bin_tap_packet(TAP_MUTATION, c);
    }
}

static void tap_delete_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.del++;
        process_bin_tap_packet(TAP_DELETION, c);
    }
}

static void tap_flush_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.flush++;
        process_bin_tap_packet(TAP_FLUSH, c);
    }
}

static void tap_opaque_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.opaque++;
        process_bin_tap_packet(TAP_OPAQUE, c);
    }
}

static void tap_vbucket_set_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.vbucket_set++;
        process_bin_tap_packet(TAP_VBUCKET_SET, c);
    }
}

static void tap_checkpoint_start_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_start++;
        process_bin_tap_packet(TAP_CHECKPOINT_START, c);
    }
}

static void tap_checkpoint_end_executor(Connection *c, void *packet)
{
    (void)packet;
    if (c->bucket.engine->tap_notify == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_end++;
        process_bin_tap_packet(TAP_CHECKPOINT_END, c);
    }
}

/*******************************************************************************
 *                         DCP packet executors                                *
 ******************************************************************************/
static void dcp_open_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_open*>(packet);

    if (c->bucket.engine->dcp.open == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;
        c->setSupportsDatatype(true);

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.open(v1_handle_2_handle(c->bucket.engine), c,
                                             req->message.header.request.opaque,
                                             ntohl(req->message.body.seqno),
                                             ntohl(req->message.body.flags),
                                             (void*)(req->bytes + sizeof(req->bytes)),
                                             ntohs(req->message.header.request.keylen));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            audit_dcp_open(c);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_add_stream_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_add_stream*>(packet);

    if (c->bucket.engine->dcp.add_stream == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.add_stream(v1_handle_2_handle(c->bucket.engine), c,
                                                   req->message.header.request.opaque,
                                                   ntohs(req->message.header.request.vbucket),
                                                   ntohl(req->message.body.flags));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setDCP(true);
            c->setState(conn_ship_log);
            break;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_close_stream_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_close_stream*>(packet);

    if (c->bucket.engine->dcp.close_stream == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t opaque = ntohl(req->message.header.request.opaque);
            ret = c->bucket.engine->dcp.close_stream(v1_handle_2_handle(c->bucket.engine), c,
                                                       opaque, vbucket);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

/** Callback from the engine adding the response */
static ENGINE_ERROR_CODE add_failover_log(vbucket_failover_t*entries,
                                          size_t nentries,
                                          const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    size_t ii;
    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    if (binary_response_handler(NULL, 0, NULL, 0, entries,
                                (uint32_t)(nentries * sizeof(vbucket_failover_t)), 0,
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

static void dcp_get_failover_log_executor(Connection *c, void *packet) {
    auto *req = reinterpret_cast<protocol_binary_request_dcp_get_failover_log*>(packet);

    if (c->bucket.engine->dcp.get_failover_log == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.get_failover_log(v1_handle_2_handle(c->bucket.engine), c,
                                                         req->message.header.request.opaque,
                                                         ntohs(req->message.header.request.vbucket),
                                                         add_failover_log);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, &c->dynamic_buffer);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_stream_req_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_stream_req*>(packet);

    if (c->bucket.engine->dcp.stream_req == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);
        uint64_t vbucket_uuid = ntohll(req->message.body.vbucket_uuid);
        uint64_t snap_start_seqno = ntohll(req->message.body.snap_start_seqno);
        uint64_t snap_end_seqno = ntohll(req->message.body.snap_end_seqno);
        uint64_t rollback_seqno;

        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        cb_assert(ret != ENGINE_ROLLBACK);

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.stream_req(v1_handle_2_handle(c->bucket.engine), c,
                                                   flags,
                                                   c->binary_header.request.opaque,
                                                   c->binary_header.request.vbucket,
                                                   start_seqno, end_seqno,
                                                   vbucket_uuid,
                                                   snap_start_seqno,
                                                   snap_end_seqno,
                                                   &rollback_seqno,
                                                   add_failover_log);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setDCP(true);
            c->setMaxReqsPerEvent(settings.reqs_per_event_med_priority);
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, &c->dynamic_buffer);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
            break;

        case ENGINE_ROLLBACK:
            rollback_seqno = htonll(rollback_seqno);
            if (binary_response_handler(NULL, 0, NULL, 0, &rollback_seqno,
                                        sizeof(rollback_seqno), 0,
                                        PROTOCOL_BINARY_RESPONSE_ROLLBACK, 0,
                                        c)) {
                write_and_free(c, &c->dynamic_buffer);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_stream_end_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_dcp_stream_end*>(packet);

    if (c->bucket.engine->dcp.stream_end == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.stream_end(v1_handle_2_handle(c->bucket.engine), c,
                                                     req->message.header.request.opaque,
                                                     ntohs(req->message.header.request.vbucket),
                                                     ntohl(req->message.body.flags));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_snapshot_marker_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_snapshot_marker*>(packet);

    if (c->bucket.engine->dcp.snapshot_marker == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        uint32_t opaque = req->message.header.request.opaque;
        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);

        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.snapshot_marker(v1_handle_2_handle(c->bucket.engine), c,
                                                          opaque, vbucket,
                                                          start_seqno,
                                                          end_seqno, flags);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_mutation_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

    if (c->bucket.engine->dcp.mutation == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            char *key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            void *value = key + nkey;
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t flags = req->message.body.flags;
            uint8_t datatype = req->message.header.request.datatype;
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint32_t expiration = ntohl(req->message.body.expiration);
            uint32_t lock_time = ntohl(req->message.body.lock_time);
            uint16_t nmeta = ntohs(req->message.body.nmeta);
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey
                - req->message.header.request.extlen - nmeta;

            ret = c->bucket.engine->dcp.mutation(v1_handle_2_handle(c->bucket.engine), c,
                                                 req->message.header.request.opaque,
                                                 key, nkey, value, nvalue, cas, vbucket,
                                                 flags, datatype, by_seqno, rev_seqno,
                                                 expiration, lock_time,
                                                 (char*)value + nvalue, nmeta,
                                                 req->message.body.nru);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_deletion_executor(Connection *c, void *packet)
{
    auto *req = reinterpret_cast<protocol_binary_request_dcp_deletion*>(packet);

    if (c->bucket.engine->dcp.deletion == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
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
            uint16_t nmeta = ntohs(req->message.body.nmeta);

            ret = c->bucket.engine->dcp.deletion(v1_handle_2_handle(c->bucket.engine), c,
                                                 req->message.header.request.opaque,
                                                 key, nkey, cas, vbucket,
                                                 by_seqno, rev_seqno, key + nkey, nmeta);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_expiration_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_dcp_expiration*>(packet);

    if (c->bucket.engine->dcp.expiration == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
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
            uint16_t nmeta = ntohs(req->message.body.nmeta);

            ret = c->bucket.engine->dcp.expiration(v1_handle_2_handle(c->bucket.engine), c,
                                                   req->message.header.request.opaque,
                                                   key, nkey, cas, vbucket,
                                                   by_seqno, rev_seqno, key + nkey, nmeta);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_flush_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_dcp_flush*>(packet);

    if (c->bucket.engine->dcp.flush == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.flush(v1_handle_2_handle(c->bucket.engine), c,
                                              req->message.header.request.opaque,
                                              ntohs(req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_set_vbucket_state_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(packet);

    if (c->bucket.engine->dcp.set_vbucket_state== NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            vbucket_state_t state = (vbucket_state_t)req->message.body.state;
            ret = c->bucket.engine->dcp.set_vbucket_state(v1_handle_2_handle(c->bucket.engine), c,
                                                          c->binary_header.request.opaque,
                                                          c->binary_header.request.vbucket,
                                                          state);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            c->setState(conn_closing);
            break;
        }
    }
}

static void dcp_noop_executor(Connection *c, void *packet)
{
    (void)packet;

    if (c->bucket.engine->dcp.noop == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->dcp.noop(v1_handle_2_handle(c->bucket.engine), c,
                                             c->binary_header.request.opaque);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_buffer_acknowledgement_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_dcp_buffer_acknowledgement*>(packet);

    if (c->bucket.engine->dcp.buffer_acknowledgement == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            uint32_t bbytes;
            memcpy(&bbytes, &req->message.body.buffer_bytes, 4);
            ret = c->bucket.engine->dcp.buffer_acknowledgement(v1_handle_2_handle(c->bucket.engine), c,
                                                               c->binary_header.request.opaque,
                                                               c->binary_header.request.vbucket,
                                                               ntohl(bbytes));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void dcp_control_executor(Connection *c, void *packet)
{
    if (c->bucket.engine->dcp.control == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->aiostat;
        c->aiostat = ENGINE_SUCCESS;
        c->ewouldblock = false;

        if (ret == ENGINE_SUCCESS) {
            auto* req = reinterpret_cast<protocol_binary_request_dcp_control*>(packet);
            const uint8_t *key = req->bytes + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            const uint8_t *value = key + nkey;
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey;
            ret = c->bucket.engine->dcp.control(v1_handle_2_handle(c->bucket.engine), c,
                                                  c->binary_header.request.opaque,
                                                  key, nkey, value, nvalue);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            break;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
        }
    }
}

static void isasl_refresh_executor(Connection *c, void *packet)
{
    ENGINE_ERROR_CODE ret = c->aiostat;
    (void)packet;

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
        c->setState(conn_refresh_cbsasl);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void ssl_certs_refresh_executor(Connection *c, void *packet)
{
    ENGINE_ERROR_CODE ret = c->aiostat;
    (void)packet;

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_ssl_certs(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        c->setState(conn_refresh_ssl_certs);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void verbosity_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_verbosity*>(packet);
    uint32_t level = (uint32_t)ntohl(req->message.body.level);
    if (level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    settings.verbose = (int)level;
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
    write_bin_response(c, NULL, 0, 0, 0);
}

static void process_hello_packet_executor(Connection *c, void *packet) {
    auto* req = reinterpret_cast<protocol_binary_request_hello*>(packet);
    char log_buffer[512];
    int offset = snprintf(log_buffer, sizeof(log_buffer), "HELO ");
    char *key = (char*)packet + sizeof(*req);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t total = (ntohl(req->message.header.request.bodylen) - klen) / 2;
    uint32_t ii;
    char *curr = key + klen;
    uint16_t out[MEMCACHED_TOTAL_HELLO_FEATURES];
    int jj = 0;
    bool tcpdelay_handled = false;
    memset((char*)out, 0, sizeof(out));

    /*
     * Disable all features the hello packet may enable, so that
     * the client can toggle features on/off during a connection
     */
    c->setSupportsDatatype(false);
    c->setSupportsMutationExtras(false);

    if (klen) {
        if (klen > 256) {
            klen = 256;
        }
        log_buffer[offset++] = '[';
        memcpy(log_buffer + offset, key, klen);
        offset += klen;
        log_buffer[offset++] = ']';
        log_buffer[offset++] = ' ';
        log_buffer[offset] = '\0';
    }

    for (ii = 0; ii < total; ++ii) {
        bool added = false;
        uint16_t in;
        /* to avoid alignment */
        memcpy(&in, curr, 2);
        curr += 2;
        in = ntohs(in);

        switch (in) {
        case PROTOCOL_BINARY_FEATURE_TLS:
            /* Not implemented */
            break;
        case PROTOCOL_BINARY_FEATURE_DATATYPE:
            if (settings.datatype && !c->isSupportsDatatype()) {
                c->setSupportsDatatype(true);
                added = true;
            }
            break;

        case PROTOCOL_BINARY_FEATURE_TCPNODELAY:
        case PROTOCOL_BINARY_FEATURE_TCPDELAY:
            if (!tcpdelay_handled) {
                c->setTcpNoDelay(in == PROTOCOL_BINARY_FEATURE_TCPNODELAY);
                tcpdelay_handled = true;
                added = true;
            }
            break;

        case PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO:
            if (!c->isSupportsMutationExtras()) {
                c->setSupportsMutationExtras(true);
                added = true;
            }
            break;
        }

        if (added) {
            out[jj++] = htons(in);
            offset += snprintf(log_buffer + offset,
                               sizeof(log_buffer) - offset,
                               "%s, ",
                               protocol_feature_2_text(in));
        }
    }

    if (jj == 0) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        binary_response_handler(NULL, 0, NULL, 0, out, 2 * jj,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                0, c);
        write_and_free(c, &c->dynamic_buffer);
    }


    /* Trim off the whitespace and potentially trailing commas */
    --offset;
    while (offset > 0 && (isspace(log_buffer[offset]) || log_buffer[offset] == ',')) {
        log_buffer[offset] = '\0';
        --offset;
    }
    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                    "%u: %s", c->getId(), log_buffer);
}

static void version_executor(Connection *c, void *packet)
{
    (void)packet;
    write_bin_response(c, get_server_version(), 0, 0,
                       (uint32_t)strlen(get_server_version()));
}

static void quit_executor(Connection *c, void *packet)
{
    (void)packet;
    write_bin_response(c, NULL, 0, 0, 0);
    c->write_and_go = conn_closing;
}

static void quitq_executor(Connection *c, void *packet)
{
    (void)packet;
    c->setState(conn_closing);
}

static void sasl_list_mech_executor(Connection *c, void *packet)
{
    const char *result_string = NULL;
    unsigned int string_length = 0;
    (void)packet;

    if (cbsasl_list_mechs(&result_string, &string_length) != CBSASL_OK) {
        /* Perhaps there's a better error for this... */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: Failed to list SASL mechanisms.\n",
                                        c->getId());
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        return;
    }
    write_bin_response(c, (char*)result_string, 0, 0, string_length);
}

static void sasl_auth_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    char mech[1024];
    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    if (nkey > 1023) {
        /* too big.. */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                "%u: sasl error. key: %d > 1023", c->getId(), nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        return;
    }

    memcpy(mech, req->bytes + sizeof(req->bytes), nkey);
    mech[nkey] = '\0';

    if (settings.verbose) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "%u: SASL auth with mech: '%s' with %d "
                                        "bytes of data\n", c->getId(), mech, vlen);
    }

    char *challenge =
            reinterpret_cast<char*>(req->bytes + sizeof(req->bytes) + nkey);
    if (vlen == 0) {
        challenge = NULL;
    }

    const char *out = NULL;
    unsigned int outlen = 0;
    int result;

    if (c->cmd == PROTOCOL_BINARY_CMD_SASL_AUTH) {
        result = cbsasl_server_start(&c->sasl_conn, mech, challenge, vlen,
                                     (unsigned char **)&out, &outlen);
    } else {
        result = cbsasl_server_step(c->sasl_conn, challenge, vlen,
                                    &out, &outlen);
    }

    switch(result) {
    case CBSASL_OK:
        {
            auth_data_t data;
            get_auth_data(c, &data);

            if (settings.verbose > 0) {
                settings.extensions.logger->log
                    (EXTENSION_LOG_INFO, c, "%u: Client %s authenticated as %s",
                     c->getId(), c->getPeername().c_str(), data.username);
            }

            write_bin_response(c, NULL, 0, 0, 0);

            /*
             * We've successfully changed our user identity.
             * Update the authentication context
             */
            c->setAuthContext(auth_create(data.username,
                                          c->getPeername().c_str(),
                                          c->getSockname().c_str()));

            if (settings.disable_admin) {
                /* "everyone is admins" */
                cookie_set_admin(c);
            } else if (settings.admin != NULL && data.username != NULL) {
                if (strcmp(settings.admin, data.username) == 0) {
                    cookie_set_admin(c);
                }
            }
            perform_callbacks(ON_AUTH, (const void*)&data, c);
            get_thread_stats(c)->auth_cmds++;

            /* associate the connection with the appropriate bucket */
            /* @TODO Trond do we really want to do this? */
            associate_bucket(c, data.username);
        }
        break;
    case CBSASL_CONTINUE:
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                            "%u: SASL continue",
                                            c->getId(), result);
        }

        if (add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0,
                           outlen, PROTOCOL_BINARY_RAW_BYTES) == -1) {
            c->setState(conn_closing);
            return;
        }
        add_iov(c, out, outlen);
        c->setState(conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    case CBSASL_BADPARAM:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: Bad sasl params: %d",
                                        c->getId(), result);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        {
             auto *ts = get_thread_stats(c);
             ts->auth_cmds++;
             ts->auth_errors++;
        }
        break;
    default:
        if (!is_server_initialized()) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: SASL AUTH failure during initialization",
                                            c->getId());
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);
            c->write_and_go = conn_closing;
            return ;
        }

        if (result == CBSASL_NOUSER || result == CBSASL_PWERR) {
            audit_auth_failure(c, result == CBSASL_NOUSER ?
                               "Unknown user" : "Incorrect password");
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Invalid username/password combination",
                                            c->getId());
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Unknown sasl response: %d",
                                            c->getId(), result);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);

        auto *ts = get_thread_stats(c);
        ts->auth_cmds++;
        ts->auth_errors++;
    }
}

static void noop_executor(Connection *c, void *packet)
{
    (void)packet;
    write_bin_response(c, NULL, 0, 0, 0);
}

static void flush_executor(Connection *c, void *packet)
{
    ENGINE_ERROR_CODE ret;
    time_t exptime = 0;
    auto* req = reinterpret_cast<protocol_binary_request_flush*>(packet);

    if (c->cmd == PROTOCOL_BINARY_CMD_FLUSHQ) {
        c->setNoReply(true);
    }

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                        "%u: flush %ld", c->getId(),
                                        (long)exptime);
    }

    ret = c->bucket.engine->flush(v1_handle_2_handle(c->bucket.engine), c, exptime);
    switch (ret) {
    case ENGINE_SUCCESS:
        get_thread_stats(c)->cmd_flush++;
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        c->setState(conn_flush);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void add_set_replace_executor(Connection *c, void *packet,
                                     ENGINE_STORE_OPERATION store_op)
{
    auto* req = reinterpret_cast<protocol_binary_request_add*>(packet);
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    uint8_t extlen = req->message.header.request.extlen;
    char *key = (char*)packet + sizeof(req->bytes);
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t vlen = ntohl(req->message.header.request.bodylen) - nkey - extlen;
    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = 1;

    if (req->message.header.request.cas != 0) {
        store_op = OPERATION_CAS;
    }

    if (c->item == NULL) {
        item *it;

        if (ret == ENGINE_SUCCESS) {
            rel_time_t expiration = ntohl(req->message.body.expiration);
            ret = c->bucket.engine->allocate(v1_handle_2_handle(c->bucket.engine), c,
                                             &it, key, nkey, vlen,
                                             req->message.body.flags,
                                             expiration,
                                             req->message.header.request.datatype);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            update_topkeys(key, nkey, c);
            break;
        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            return ;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return ;
        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
            return;
        }

        bucket_item_set_cas(c, it, ntohll(req->message.header.request.cas));
        if (!bucket_get_item_info(c, it, &info.info)) {
            c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }

        c->item = it;
        cb_assert(info.info.value[0].iov_len == vlen);
        memcpy(info.info.value[0].iov_base, key + nkey, vlen);

        if (!c->isSupportsDatatype()) {
            if (checkUTF8JSON(reinterpret_cast<unsigned char*>(info.info.value[0].iov_base),
                              info.info.value[0].iov_len)) {
                info.info.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                if (!bucket_set_item_info(c, it, &info.info)) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                            "%u: Failed to set item info",
                            c->getId());
                }
            }
        }
    }

    if (ret == ENGINE_SUCCESS) {
        ret = bucket_store(c, c->item, &c->cas, store_op,
                           ntohs(req->message.header.request.vbucket));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        if (c->isSupportsMutationExtras()) {
            info.info.nvalue = 1;
            if (!bucket_get_item_info(c, c->item, &info.info)) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, c->item);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to get item info",
                                                c->getId());
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
                return;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                    (c->write.buf + sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(info.info.vbucket_uuid);
            extras->seqno = htonll(info.info.seqno);
            write_bin_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            write_bin_response(c, NULL, 0, 0 ,0);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_NOT_STORED:
        if (store_op == OPERATION_ADD) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        } else if (store_op == OPERATION_REPLACE) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED);
        }
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    if (store_op == OPERATION_CAS) {
        switch (ret) {
        case ENGINE_SUCCESS:
            SLAB_INCR(c, cas_hits, key, nkey);
            break;
        case ENGINE_KEY_EEXISTS:
            SLAB_INCR(c, cas_badval, key, nkey);
            break;
        case ENGINE_KEY_ENOENT:
            get_thread_stats(c)->cas_misses++;
            break;
        default:
            ;
        }
    } else {
        SLAB_INCR(c, cmd_set, key, nkey);
    }

    if (!c->ewouldblock) {
        /* release the c->item reference */
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, c->item);
        c->item = 0;
    }
}


static void add_executor(Connection *c, void *packet)
{
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void addq_executor(Connection *c, void *packet)
{
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void set_executor(Connection *c, void *packet)
{
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void setq_executor(Connection *c, void *packet)
{
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void replace_executor(Connection *c, void *packet)
{
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void replaceq_executor(Connection *c, void *packet)
{
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void append_prepend_executor(Connection *c,
                                    void *packet,
                                    ENGINE_STORE_OPERATION store_op)
{
    auto* req = reinterpret_cast<protocol_binary_request_append*>(packet);
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    char *key = (char*)packet + sizeof(req->bytes);
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t vlen = ntohl(req->message.header.request.bodylen) - nkey;
    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = 1;

    if (c->item == NULL) {
        item *it;

        if (ret == ENGINE_SUCCESS) {
            ret = c->bucket.engine->allocate(v1_handle_2_handle(c->bucket.engine), c,
                                               &it, key, nkey,
                                               vlen, 0, 0,
                                               req->message.header.request.datatype);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            break;
        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            return ;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return ;
        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
            return;
        }

        bucket_item_set_cas(c, it, ntohll(req->message.header.request.cas));
        if (!bucket_get_item_info(c, it, &info.info)) {
            c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }

        c->item = it;
        cb_assert(info.info.value[0].iov_len == vlen);
        memcpy(info.info.value[0].iov_base, key + nkey, vlen);

        if (!c->isSupportsDatatype()) {
            if (checkUTF8JSON(reinterpret_cast<unsigned char*>(info.info.value[0].iov_base),
                              info.info.value[0].iov_len)) {
                info.info.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                if (!bucket_set_item_info(c, it, &info.info)) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                            "%u: Failed to set item info",
                            c->getId());
                }
            }
        }
        update_topkeys(key, nkey, c);

    }

    if (ret == ENGINE_SUCCESS) {
        ret = bucket_store(c, c->item, &c->cas, store_op,
                           ntohs(req->message.header.request.vbucket));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        if (c->isSupportsMutationExtras()) {
            info.info.nvalue = 1;
            if (!bucket_get_item_info(c, c->item, &info.info)) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, c->item);
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Failed to get item info",
                                                c->getId());
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
                return;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                    (c->write.buf + sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(info.info.vbucket_uuid);
            extras->seqno = htonll(info.info.seqno);
            write_bin_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            write_bin_response(c, NULL, 0, 0 ,0);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    SLAB_INCR(c, cmd_set, key, nkey);

    if (!c->ewouldblock) {
        /* release the c->item reference */
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, c->item);
        c->item = 0;
    }
}

static void append_executor(Connection *c, void *packet)
{
    c->setNoReply(false);
    append_prepend_executor(c, packet, OPERATION_APPEND);
}

static void appendq_executor(Connection *c, void *packet)
{
    c->setNoReply(true);
    append_prepend_executor(c, packet, OPERATION_APPEND);
}

static void prepend_executor(Connection *c, void *packet)
{
    c->setNoReply(false);
    append_prepend_executor(c, packet, OPERATION_PREPEND);
}

static void prependq_executor(Connection *c, void *packet)
{
    c->setNoReply(true);
    append_prepend_executor(c, packet, OPERATION_PREPEND);
}


static void get_executor(Connection *c, void *packet)
{
    (void)packet;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_GETQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GET:
        c->setNoReply(false);
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GETK:
        c->setNoReply(false);
        break;
    default:
        abort();
    }

    process_bin_get(c);
}

static void process_bin_delete(Connection *c);
static void delete_executor(Connection *c, void *packet)
{
    (void)packet;

    if (c->cmd == PROTOCOL_BINARY_CMD_DELETEQ) {
        c->setNoReply(true);
    }

    process_bin_delete(c);
}

static void stat_executor(Connection *c, void *packet)
{
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    ENGINE_ERROR_CODE ret;

    (void)packet;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
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
            ret = c->bucket.engine->get_stats(v1_handle_2_handle(c->bucket.engine), c,
                                              NULL, 0, append_stats);
            if (ret == ENGINE_SUCCESS) {
                server_stats(&append_stats, c);
            }
        } else if (strncmp(subcommand, "reset", 5) == 0) {
            stats_reset(c);
            bucket_reset_stats(c);
        } else if (strncmp(subcommand, "settings", 8) == 0) {
            process_stat_settings(&append_stats, c);
        } else if (nkey == 5 && strncmp(subcommand, "audit", 5) == 0) {
            if (c->isAdmin()) {
                process_auditd_stats(&append_stats, c);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
                return;
            }
        } else if (strncmp(subcommand, "cachedump", 9) == 0) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
            return;
        } else if (nkey == 14 && strncmp(subcommand, "bucket details", 14) == 0) {
            process_bucket_details(c);
        } else if (strncmp(subcommand, "aggregate", 9) == 0) {
            server_stats(&append_stats, c);
        } else if (strncmp(subcommand, "connections", 11) == 0) {
            int64_t fd = -1; /* default to all connections */
            /* Check for specific connection number - allow up to 32 chars for FD */
            if (nkey > 11 && nkey < (11 + 32)) {
                int64_t key;
                char buffer[32];
                const size_t fd_length = nkey - 11;
                memcpy(buffer, subcommand + 11, fd_length);
                buffer[fd_length] = '\0';
                if (safe_strtoll(buffer, &key)) {
                    fd = key;
                }
            }
            connection_stats(&append_stats, c, fd);
        } else if (strncmp(subcommand, "topkeys", nkey) == 0) {
            ret = all_buckets[c->bucket.idx].topkeys->stats(c,
                                mc_time_get_current_time(), append_stats);
        } else if (strncmp(subcommand, "topkeys_json", nkey) == 0) {
            cJSON *topkeys_doc = cJSON_CreateObject();

            ret = all_buckets[c->bucket.idx].topkeys->json_stats(topkeys_doc,
                                     mc_time_get_current_time());

            if (ret == ENGINE_SUCCESS) {
                char key[] = "topkeys_json";
                char *topkeys_str = cJSON_PrintUnformatted(topkeys_doc);
                append_stats(key, (uint16_t)strlen(key),
                             topkeys_str, (uint32_t)strlen(topkeys_str), c);
                cJSON_Free(topkeys_str);
            }
            cJSON_Delete(topkeys_doc);

        } else {
            ret = c->bucket.engine->get_stats(v1_handle_2_handle(c->bucket.engine), c,
                                                subcommand, (int)nkey,
                                                append_stats);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        append_stats(NULL, 0, NULL, 0, c);
        write_and_free(c, &c->dynamic_buffer);
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    }
}

static void arithmetic_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_incr*>(binary_get_request(c));
    ENGINE_ERROR_CODE ret;
    uint64_t delta;
    uint64_t initial;
    rel_time_t expiration;
    char *key;
    size_t nkey;
    bool incr;
    uint64_t result;

    (void)packet;


    cb_assert(c != NULL);


    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_INCREMENT:
        c->setNoReply(false);
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_DECREMENT:
        c->setNoReply(false);
        break;
    default:
        abort();
    }

    if (req->message.header.request.cas != 0) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
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
        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
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

    item* item = NULL;
    if (ret == ENGINE_SUCCESS) {
        ret = c->bucket.engine->arithmetic(v1_handle_2_handle(c->bucket.engine),
                                             c, key, (int)nkey, incr,
                                             req->message.body.expiration != 0xffffffff,
                                             delta, initial, expiration,
                                             &item,
                                             c->binary_header.request.datatype,
                                             &result,
                                             c->binary_header.request.vbucket);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
    {
        /* Lookup the item's info for necessary metadata. */
        item_info info;
        info.nvalue = 1;
        if (!bucket_get_item_info(c, item, &info)) {
            c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, item);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Failed to get item info",
                                            c->getId());
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }
        c->cas = info.cas;

        char* body_buf =
                (c->write.buf + sizeof(protocol_binary_response_incr));
        if (c->isSupportsMutationExtras()) {
            /* Response includes vbucket UUID and sequence number (in addition
             * to value) */
            struct mutation_extras_plus_body {
                mutation_descr_t extras;
                uint64_t value;
            };
            auto* body = reinterpret_cast<mutation_extras_plus_body*>(body_buf);
            body->extras.vbucket_uuid = htonll(info.vbucket_uuid);
            body->extras.seqno = htonll(info.seqno);
            body->value = htonll(result);
            write_bin_response(c, body, sizeof(body->extras), 0, sizeof(*body));
        } else {
            /* Just send value back. */
            uint64_t* const value_ptr = (uint64_t*)body_buf;
            *value_ptr = htonll(result);
            write_bin_response(c, value_ptr, 0, 0, sizeof(*value_ptr));
        }

        /* No further need for item; release it. */
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, item);

        if (incr) {
            STATS_INCR(c, incr_hits, key, nkey);
        } else {
            STATS_INCR(c, decr_hits, key, nkey);
        }
        update_topkeys(key, nkey, c);
        break;
    }
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        if ((c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) ||
            (c->cmd == PROTOCOL_BINARY_CMD_INCREMENTQ)) {
            STATS_INCR(c, incr_misses, key, nkey);
        } else {
            STATS_INCR(c, decr_misses, key, nkey);
        }
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        break;
    case ENGINE_EINVAL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL);
        break;
    case ENGINE_NOT_STORED:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        abort();
    }
}

static void get_cmd_timer_executor(Connection *c, void *packet)
{
    std::string str;
    auto* req = reinterpret_cast<protocol_binary_request_get_cmd_timer*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = c->bucket.idx;
    std::string bucket(key, keylen);

    if (bucket == "/all/") {
        index = 0;
        keylen = 0;
    }

    if (keylen == 0) {
        if (index == 0 && !cookie_is_admin(c)) {
            // We're not connected to a bucket, and we didn't
            // authenticate to a bucket.. Don't leak the
            // global stats...
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
            return;
        }
        str = all_buckets[index].timings.generate(req->message.body.opcode);
        binary_response_handler(NULL, 0, NULL, 0, str.data(),
                                uint32_t(str.length()),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                0, c);
        write_and_free(c, &c->dynamic_buffer);
    } else if (cookie_is_admin(c)) {
        bool found = false;
        for (int ii = 1; ii < settings.max_buckets && !found; ++ii) {
            // Need the lock to get the bucket state and name
            cb_mutex_enter(&all_buckets[ii].mutex);
            if ((all_buckets[ii].state == BucketState::Ready) &&
                (bucket == all_buckets[ii].name)) {
                str = all_buckets[ii].timings.generate(req->message.body.opcode);
                found = true;
            }
            cb_mutex_exit(&all_buckets[ii].mutex);
        }
        if (found) {
            binary_response_handler(NULL, 0, NULL, 0, str.data(),
                                    uint32_t(str.length()),
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                    0, c);
            write_and_free(c, &c->dynamic_buffer);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } else {
        // non-privileged connections can't specify bucket
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}

static void set_ctrl_token_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(packet);
    uint64_t old_cas = ntohll(req->message.header.request.cas);
    uint16_t ret = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    cb_mutex_enter(&(session_cas.mutex));
    if (session_cas.ctr > 0) {
        ret = PROTOCOL_BINARY_RESPONSE_EBUSY;
    } else {
        if (old_cas == session_cas.value || old_cas == 0) {
            session_cas.value = ntohll(req->message.body.new_cas);
        } else {
            ret = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        }
    }

    binary_response_handler(NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            ret, session_cas.value, c);
    cb_mutex_exit(&(session_cas.mutex));

    write_and_free(c, &c->dynamic_buffer);
}

static void get_ctrl_token_executor(Connection *c, void *packet)
{
    (void)packet;
    cb_mutex_enter(&(session_cas.mutex));
    binary_response_handler(NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS,
                            session_cas.value, c);
    cb_mutex_exit(&(session_cas.mutex));
    write_and_free(c, &c->dynamic_buffer);
}

static void init_complete_executor(Connection *c, void *packet)
{
    auto* init = reinterpret_cast<protocol_binary_request_init_complete*>(packet);
    uint64_t cas = ntohll(init->message.header.request.cas);;
    bool stale;

    cb_mutex_enter(&(session_cas.mutex));
    stale = (session_cas.value != cas);
    cb_mutex_exit(&(session_cas.mutex));

    if (stale) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    } else {
        set_server_initialized(true);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}

static void ioctl_get_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t value;

    ENGINE_ERROR_CODE status = ioctl_get_property(key, keylen, &value);

    if (status == ENGINE_SUCCESS) {
        char res_buffer[16];
        size_t length = snprintf(res_buffer, sizeof(res_buffer), "%ld", value);
        if ((length > sizeof(res_buffer) - 1) ||
            binary_response_handler(NULL, 0, NULL, 0, res_buffer,
                                    uint32_t(length),
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, c)) {
            write_and_free(c, &c->dynamic_buffer);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } else {
        write_bin_packet(c, engine_error_2_protocol_error(status));
    }
}

static void ioctl_set_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen);
    const char* value = key + keylen;

    ENGINE_ERROR_CODE status = ioctl_set_property(c, key, keylen, value,
                                                  vallen);

    write_bin_packet(c, engine_error_2_protocol_error(status));
}

static void config_validate_executor(Connection *c, void *packet) {
    const char* val_ptr = NULL;
    cJSON *errors = NULL;
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);

    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;

    /* Key not yet used, must be zero length. */
    if (keylen != 0) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    /* must have non-zero length config */
    if (vallen == 0 || vallen > CONFIG_VALIDATE_MAX_LENGTH) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    val_ptr = (const char*)(req->bytes + sizeof(req->bytes)) + keylen;

    /* null-terminate value, and convert to integer */
    try {
        std::string val_buffer(val_ptr, vallen);
        errors = cJSON_CreateArray();

        if (validate_proposed_config_changes(val_buffer.c_str(), errors)) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            /* problem(s). Send the errors back to the client. */
            char* error_string = cJSON_PrintUnformatted(errors);
            if (binary_response_handler(NULL, 0, NULL, 0, error_string,
                                        (uint32_t)strlen(error_string), 0,
                                        PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                        c)) {
                write_and_free(c, &c->dynamic_buffer);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
            cJSON_Free(error_string);
        }

        cJSON_Delete(errors);
    } catch (const std::bad_alloc&) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u: Failed to allocate buffer of size %" PRIu64
                                        " to validate config. Shutting down connection",
                                        c->getId(), vallen + 1);
        c->setState(conn_closing);
        return;
    }

}

static void config_reload_executor(Connection *c, void *packet) {
    (void)packet;
    reload_config_file();
    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static void audit_config_reload_executor(Connection *c, void *packet) {
    (void)packet;
    if (settings.audit_file) {
        if (configure_auditdaemon(settings.audit_file, c) == AUDIT_EWOULDBLOCK) {
            c->ewouldblock = true;
            c->setState(conn_audit_configuring);
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "configuration of audit "
                                            "daemon failed with config "
                                            "file: %s",
                                            settings.audit_file);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
        }
    } else {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}

static void assume_role_executor(Connection *c, void *packet)
{
    auto* req = reinterpret_cast<protocol_binary_request_assume_role*>(packet);
    size_t rlen = ntohs(req->message.header.request.keylen);
    AuthResult err;

    if (rlen > 0) {
        try {
            auto* role_ptr = reinterpret_cast<const char*>(req + 1);
            std::string role(role_ptr, rlen);
            err = c->assumeRole(role.c_str());

        } catch (const std::bad_alloc&) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            return;
        }
    } else {
        err = c->dropRole();
    }

    switch (err) {
    case AuthResult::FAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        break;
    case AuthResult::OK:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case AuthResult::STALE:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_STALE);
        break;
    default:
        abort();
    }
}

static void audit_put_executor(Connection *c, void *packet) {

    auto *req = reinterpret_cast<const protocol_binary_request_audit_put*>(packet);
    const void *payload = req->bytes + sizeof(req->message.header) +
                          req->message.header.request.extlen;

    const size_t payload_length = ntohl(req->message.header.request.bodylen) -
                                  req->message.header.request.extlen;

    if (put_audit_event(ntohl(req->message.body.id), payload, payload_length)
        == AUDIT_SUCCESS) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    }
}

static void create_bucket_main(void *c);

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static void create_bucket_executor(Connection *c, void *packet)
{
    ENGINE_ERROR_CODE ret = c->aiostat;
    (void)packet;

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        cb_thread_t tid;
        int err;

        err = cb_create_thread(&tid, create_bucket_main, c, 1);
        if (err == 0) {
            ret = ENGINE_EWOULDBLOCK;
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Failed to create thread to "
                                            "create bucket:: %s",
                                            strerror(err));
            ret = ENGINE_FAILED;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        c->setState(conn_create_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}


static void list_bucket_executor(Connection *c, void *)
{
    try {
        std::string blob;
        for (int ii = 0; ii < settings.max_buckets; ++ii) {
            cb_mutex_enter(&all_buckets[ii].mutex);
            if (all_buckets[ii].state == BucketState::Ready) {
                blob += all_buckets[ii].name + std::string(" ");
            }
            cb_mutex_exit(&all_buckets[ii].mutex);
        }

        if (blob.size() > 0) {
            /* remove trailing " " */
            blob.pop_back();
        }

        if (binary_response_handler(NULL, 0, NULL, 0, blob.data(),
                                    uint32_t(blob.size()), 0,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, c)) {
            write_and_free(c, &c->dynamic_buffer);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } catch (const std::bad_alloc&) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
    }
}

static void delete_bucket_main(void *c);

static void delete_bucket_executor(Connection *c, void *packet)
{
    ENGINE_ERROR_CODE ret = c->aiostat;
    (void)packet;

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    if (ret == ENGINE_SUCCESS) {
        cb_thread_t tid;
        int err;

        err = cb_create_thread(&tid, delete_bucket_main, c, 1);
        if (err == 0) {
            ret = ENGINE_EWOULDBLOCK;
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Failed to create thread to "
                                            "delete bucket:: %s",
                                            strerror(err));
            ret = ENGINE_FAILED;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        c->setState(conn_delete_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }
}

static void select_bucket_executor(Connection *c, void *packet) {
    /* The validator ensured that we're not doing a buffer overflow */
    char bucketname[1024];
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    memcpy(bucketname, req->bytes + (sizeof(*req)), klen);
    bucketname[klen] = '\0';

    if (associate_bucket(c, bucketname)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    }
}

typedef int (*bin_package_validate)(void *packet);
typedef void (*bin_package_execute)(Connection *c, void *packet);

mcbp_package_validate *validators;
bin_package_execute executors[0xff];

static void setup_bin_packet_handlers(void) {
    validators = get_mcbp_validators();
    topkey_commands = get_mcbp_topkeys();

    executors[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = dcp_snapshot_marker_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = tap_checkpoint_end_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = tap_checkpoint_start_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CONNECT] = tap_connect_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_DELETE] = tap_delete_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_FLUSH] = tap_flush_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_MUTATION] = tap_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = tap_opaque_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = tap_vbucket_set_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_FLUSH] = dcp_flush_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = dcp_get_failover_log_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = dcp_set_vbucket_state_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = dcp_buffer_acknowledgement_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_executor;
    executors[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH] = ssl_certs_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_executor;
    executors[PROTOCOL_BINARY_CMD_HELLO] = process_hello_packet_executor;
    executors[PROTOCOL_BINARY_CMD_VERSION] = version_executor;
    executors[PROTOCOL_BINARY_CMD_QUIT] = quit_executor;
    executors[PROTOCOL_BINARY_CMD_QUITQ] = quitq_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_LIST_MECHS] = sasl_list_mech_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_AUTH] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_STEP] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_NOOP] = noop_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSH] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSHQ] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_SETQ] = setq_executor;
    executors[PROTOCOL_BINARY_CMD_SET] = set_executor;
    executors[PROTOCOL_BINARY_CMD_ADDQ] = addq_executor;
    executors[PROTOCOL_BINARY_CMD_ADD] = add_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACEQ] = replaceq_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACE] = replace_executor;
    executors[PROTOCOL_BINARY_CMD_APPENDQ] = appendq_executor;
    executors[PROTOCOL_BINARY_CMD_APPEND] = append_executor;
    executors[PROTOCOL_BINARY_CMD_PREPENDQ] = prependq_executor;
    executors[PROTOCOL_BINARY_CMD_PREPEND] = prepend_executor;
    executors[PROTOCOL_BINARY_CMD_GET] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETK] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETKQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_DELETEQ] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_STAT] = stat_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CMD_TIMER] = get_cmd_timer_executor;
    executors[PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN] = set_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN] = get_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_INIT_COMPLETE] = init_complete_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_GET] = ioctl_get_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_SET] = ioctl_set_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_VALIDATE] = config_validate_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_RELOAD] = config_reload_executor;
    executors[PROTOCOL_BINARY_CMD_ASSUME_ROLE] = assume_role_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_PUT] = audit_put_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD] = audit_config_reload_executor;

    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = subdoc_dict_upsert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = subdoc_array_push_last_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = subdoc_array_push_first_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = subdoc_array_insert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = subdoc_array_add_unique_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_executor;

    executors[PROTOCOL_BINARY_CMD_CREATE_BUCKET] = create_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_LIST_BUCKETS] = list_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE_BUCKET] = delete_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_SELECT_BUCKET] = select_bucket_executor;

}

static int invalid_datatype(Connection *c) {
    switch (c->binary_header.request.datatype) {
    case PROTOCOL_BINARY_RAW_BYTES:
        return 0;

    case PROTOCOL_BINARY_DATATYPE_JSON:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
        if (c->isSupportsDatatype()) {
            return 0;
        }
        /* FALLTHROUGH */
    default:
        return 1;
    }
}

static void process_bin_packet(Connection *c) {

    char *packet = (c->read.curr - (c->binary_header.request.bodylen +
                                sizeof(c->binary_header)));

    uint8_t opcode = c->binary_header.request.opcode;

    bin_package_validate validator = validators[opcode];
    bin_package_execute executor = executors[opcode];

    switch (c->checkAccess(opcode)) {
    case AuthResult::FAIL:
        /* @TODO Should go to audit */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u (%s => %s): no access to command %s",
                                        c->getId(), c->getPeername().c_str(),
                                        c->getSockname().c_str(),
                                        memcached_opcode_2_text(opcode));
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
        break;
    case AuthResult::OK:
        if (validator != NULL && validator(packet) != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Invalid format for specified for %s",
                                            c->getId(),
                                            memcached_opcode_2_text(opcode));
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        } else if (executor != NULL) {
            executor(c, packet);
        } else {
            process_bin_unknown_packet(c);
        }
        break;
    case AuthResult::STALE:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_STALE);
        break;
    default:
        abort();
    }
}

static CB_INLINE bool is_initialized(Connection *c, uint8_t opcode)
{
    if (c->isAdmin() || is_server_initialized()) {
        return true;
    }

    switch (opcode) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        return true;
    default:
        return false;
    }
}

static void dispatch_bin_command(Connection *c) {
    uint16_t keylen = c->binary_header.request.keylen;

    /* @trond this should be in the Connection-connect part.. */
    /*        and in the select bucket */
    if (c->bucket.engine == NULL) {
        c->bucket.engine = all_buckets[c->bucket.idx].engine;
    }

    if (!is_initialized(c, c->binary_header.request.opcode)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);
        c->write_and_go = conn_closing;
        return;
    }

    if (settings.require_sasl && !authenticated(c)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        c->write_and_go = conn_closing;
        return;
    }

    if (invalid_datatype(c)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->write_and_go = conn_closing;
        return;
    }

    if (c->start == 0) {
        c->start = gethrtime();
    }

    MEMCACHED_PROCESS_COMMAND_START(c->getId(), c->read.curr, c->read.bytes);

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    c->setNoReply(false);

    /*
     * Protect ourself from someone trying to kill us by sending insanely
     * large packets.
     */
    if (c->binary_header.request.bodylen > settings.max_packet_size) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->write_and_go = conn_closing;
    } else {
        bin_read_chunk(c, bin_substates::bin_reading_packet,
                       c->binary_header.request.bodylen);
    }
}

static void process_bin_delete(Connection *c) {
    ENGINE_ERROR_CODE ret;
    auto* req = reinterpret_cast<protocol_binary_request_delete*>
            (binary_get_request(c));
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint64_t cas = ntohll(req->message.header.request.cas);

    cb_assert(c != NULL);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    "DELETE", key, nkey) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c, "%s\n",
                                            buffer);
        }
    }

    ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    mutation_descr_t mut_info;
    if (ret == ENGINE_SUCCESS) {
        ret = c->bucket.engine->remove(v1_handle_2_handle(c->bucket.engine), c, key, nkey,
                                       &cas, c->binary_header.request.vbucket,
                                       &mut_info);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->cas = cas;
        if (c->isSupportsMutationExtras()) {
            /* Response includes vbucket UUID and sequence number */
            mutation_descr_t* const extras = (mutation_descr_t*)
                    (c->write.buf + sizeof(protocol_binary_response_delete));

            extras->vbucket_uuid = htonll(mut_info.vbucket_uuid);
            extras->seqno = htonll(mut_info.seqno);
            write_bin_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            write_bin_response(c, NULL, 0, 0, 0);
        }
        SLAB_INCR(c, delete_hits, key, nkey);
        update_topkeys(key, nkey, c);
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        STATS_INCR(c, delete_misses, key, nkey);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
        break;
    case ENGINE_TMPFAIL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        break;
    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    }
}

static void complete_nread(Connection *c) {
    cb_assert(c != NULL);
    cb_assert(c->cmd >= 0);

    switch(c->getSubstate()) {
    case bin_substates::bin_reading_packet:
        if (c->binary_header.request.magic == PROTOCOL_BINARY_RES) {
            RESPONSE_HANDLER handler;
            handler = response_handlers[c->binary_header.request.opcode];
            if (handler) {
                handler(c);
            } else {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                       "%u: Unsupported response packet received: %u",
                        c->getId(), (unsigned int)c->binary_header.request.opcode);
                c->setState(conn_closing);
            }
        } else {
            process_bin_packet(c);
        }
        break;
    default:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                "Not handling substate %d\n", c->getSubstate());
        abort();
    }
}

static void reset_cmd_handler(Connection *c) {
    c->cmd = -1;
    c->setSubstate(bin_substates::bin_no_state);
    if(c->item != NULL) {
        c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, c->item);
        c->item = NULL;
    }

    c->resetCommandContext();

    if (c->read.bytes == 0) {
        /* Make the whole read buffer available. */
        c->read.curr = c->read.buf;
    }

    c->shrinkBuffers();
    if (c->read.bytes > 0) {
        c->setState(conn_parse_cmd);
    } else {
        c->setState(conn_waiting);
    }
}

void write_and_free(Connection *c, struct dynamic_buffer* buf) {
    if (buf->buffer) {
        c->write_and_free = buf->buffer;
        c->write.curr = buf->buffer;
        c->write.bytes = (uint32_t)buf->offset;
        c->setState(conn_write);
        c->write_and_go = conn_new_cmd;

        buf->buffer = NULL;
        buf->size = 0;
        buf->offset = 0;
    } else {
        c->setState(conn_closing);
    }
}

template <typename T>
void add_stat(const void *cookie, ADD_STAT add_stat_callback,
              const std::string &name, const T &val) {
    std::string value = std::to_string(val);
    add_stat_callback(name.c_str(), uint16_t(name.length()),
                      value.c_str(), uint32_t(value.length()), cookie);
}

void add_stat(const void *cookie, ADD_STAT add_stat_callback,
              const std::string &name, const std::string &value) {
    add_stat_callback(name.c_str(), uint16_t(name.length()),
                      value.c_str(), uint32_t(value.length()), cookie);
}

void add_stat(const void *cookie, ADD_STAT add_stat_callback,
              const std::string &name, const char *value) {
    add_stat(cookie, add_stat_callback, name, std::string(value));
}

void add_stat(const void *cookie, ADD_STAT add_stat_callback,
              const std::string &name, const bool value) {
    if (value) {
        add_stat(cookie, add_stat_callback, name, "true");
    } else {
        add_stat(cookie, add_stat_callback, name, "false");
    }
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stat_callback, Connection *c) {
#ifdef WIN32
    long pid = GetCurrentProcessId();
#else
    long pid = (long)getpid();
#endif
    rel_time_t now = mc_time_get_current_time();

    struct thread_stats thread_stats;
    thread_stats.aggregate(all_buckets[c->bucket.idx].stats, settings.num_threads);

    STATS_LOCK();

    add_stat(c, add_stat_callback, "pid", pid);
    add_stat(c, add_stat_callback, "uptime", now);
    add_stat(c, add_stat_callback, "stat_reset", (const char*)reset_stats_time);
    add_stat(c, add_stat_callback, "time", mc_time_convert_to_abs_time(now));
    add_stat(c, add_stat_callback, "version", get_server_version());
    add_stat(c, add_stat_callback, "memcached_version", MEMCACHED_VERSION);
    add_stat(c, add_stat_callback, "libevent", event_get_version());
    add_stat(c, add_stat_callback, "pointer_size", (8 * sizeof(void *)));

    add_stat(c, add_stat_callback, "daemon_connections", stats.daemon_conns);
    add_stat(c, add_stat_callback, "curr_connections",
                stats.curr_conns.load(std::memory_order_relaxed));
    for (int ii = 0; ii < settings.num_interfaces; ++ii) {
        std::string key = "max_conns_on_port_" +
                std::to_string(stats.listening_ports[ii].port);
        add_stat(c, add_stat_callback, key,
                 stats.listening_ports[ii].maxconns);
        key = "curr_conns_on_port_" +
                std::to_string(stats.listening_ports[ii].port);
        add_stat(c, add_stat_callback, key,
                 stats.listening_ports[ii].curr_conns);
    }
    add_stat(c, add_stat_callback, "total_connections", stats.total_conns);
    add_stat(c, add_stat_callback, "connection_structures",
                stats.conn_structs);
    add_stat(c, add_stat_callback, "cmd_get", thread_stats.cmd_get);
    add_stat(c, add_stat_callback, "cmd_set", thread_stats.cmd_set);
    add_stat(c, add_stat_callback, "cmd_flush", thread_stats.cmd_flush);
    // index 0 contains the aggregated timings for all buckets
    auto &timings = all_buckets[0].timings;
    uint64_t total_mutations = timings.get_aggregated_cmd_stats(CmdStat::TOTAL_MUTATION);
    uint64_t total_retrivals = timings.get_aggregated_cmd_stats(CmdStat::TOTAL_RETRIVAL);
    uint64_t total_ops = total_retrivals + total_mutations;
    add_stat(c, add_stat_callback, "cmd_total_sets", total_mutations);
    add_stat(c, add_stat_callback, "cmd_total_gets", total_retrivals);
    add_stat(c, add_stat_callback, "cmd_total_ops", total_ops);
    add_stat(c, add_stat_callback, "auth_cmds", thread_stats.auth_cmds);
    add_stat(c, add_stat_callback, "auth_errors", thread_stats.auth_errors);
    add_stat(c, add_stat_callback, "get_hits", thread_stats.get_hits);
    add_stat(c, add_stat_callback, "get_misses", thread_stats.get_misses);
    add_stat(c, add_stat_callback, "delete_misses", thread_stats.delete_misses);
    add_stat(c, add_stat_callback, "delete_hits", thread_stats.delete_hits);
    add_stat(c, add_stat_callback, "incr_misses", thread_stats.incr_misses);
    add_stat(c, add_stat_callback, "incr_hits", thread_stats.incr_hits);
    add_stat(c, add_stat_callback, "decr_misses", thread_stats.decr_misses);
    add_stat(c, add_stat_callback, "decr_hits", thread_stats.decr_hits);
    add_stat(c, add_stat_callback, "cas_misses", thread_stats.cas_misses);
    add_stat(c, add_stat_callback, "cas_hits", thread_stats.cas_hits);
    add_stat(c, add_stat_callback, "cas_badval", thread_stats.cas_badval);
    add_stat(c, add_stat_callback, "bytes_read", thread_stats.bytes_read);
    add_stat(c, add_stat_callback, "bytes_written", thread_stats.bytes_written);
    add_stat(c, add_stat_callback, "accepting_conns", is_listen_disabled() ? 0 : 1);
    add_stat(c, add_stat_callback, "listen_disabled_num", get_listen_disabled_num());
    add_stat(c, add_stat_callback, "rejected_conns", stats.rejected_conns);
    add_stat(c, add_stat_callback, "threads", settings.num_threads);
    add_stat(c, add_stat_callback, "conn_yields", thread_stats.conn_yields);
    add_stat(c, add_stat_callback, "rbufs_allocated", thread_stats.rbufs_allocated);
    add_stat(c, add_stat_callback, "rbufs_loaned", thread_stats.rbufs_loaned);
    add_stat(c, add_stat_callback, "rbufs_existing", thread_stats.rbufs_existing);
    add_stat(c, add_stat_callback, "wbufs_allocated", thread_stats.wbufs_allocated);
    add_stat(c, add_stat_callback, "wbufs_loaned", thread_stats.wbufs_loaned);
    add_stat(c, add_stat_callback, "iovused_high_watermark", thread_stats.iovused_high_watermark);
    add_stat(c, add_stat_callback, "msgused_high_watermark", thread_stats.msgused_high_watermark);
    STATS_UNLOCK();

    /*
     * Add tap stats (only if non-zero)
     */
    if (tap_stats.sent.connect) {
        add_stat(c, add_stat_callback, "tap_connect_sent", tap_stats.sent.connect);
    }
    if (tap_stats.sent.mutation) {
        add_stat(c, add_stat_callback, "tap_mutation_sent", tap_stats.sent.mutation);
    }
    if (tap_stats.sent.checkpoint_start) {
        add_stat(c, add_stat_callback, "tap_checkpoint_start_sent", tap_stats.sent.checkpoint_start);
    }
    if (tap_stats.sent.checkpoint_end) {
        add_stat(c, add_stat_callback, "tap_checkpoint_end_sent", tap_stats.sent.checkpoint_end);
    }
    if (tap_stats.sent.del) {
        add_stat(c, add_stat_callback, "tap_delete_sent", tap_stats.sent.del);
    }
    if (tap_stats.sent.flush) {
        add_stat(c, add_stat_callback, "tap_flush_sent", tap_stats.sent.flush);
    }
    if (tap_stats.sent.opaque) {
        add_stat(c, add_stat_callback, "tap_opaque_sent", tap_stats.sent.opaque);
    }
    if (tap_stats.sent.vbucket_set) {
        add_stat(c, add_stat_callback, "tap_vbucket_set_sent",
                 tap_stats.sent.vbucket_set);
    }
    if (tap_stats.received.connect) {
        add_stat(c, add_stat_callback, "tap_connect_received", tap_stats.received.connect);
    }
    if (tap_stats.received.mutation) {
        add_stat(c, add_stat_callback, "tap_mutation_received", tap_stats.received.mutation);
    }
    if (tap_stats.received.checkpoint_start) {
        add_stat(c, add_stat_callback, "tap_checkpoint_start_received", tap_stats.received.checkpoint_start);
    }
    if (tap_stats.received.checkpoint_end) {
        add_stat(c, add_stat_callback, "tap_checkpoint_end_received", tap_stats.received.checkpoint_end);
    }
    if (tap_stats.received.del) {
        add_stat(c, add_stat_callback, "tap_delete_received", tap_stats.received.del);
    }
    if (tap_stats.received.flush) {
        add_stat(c, add_stat_callback, "tap_flush_received", tap_stats.received.flush);
    }
    if (tap_stats.received.opaque) {
        add_stat(c, add_stat_callback, "tap_opaque_received", tap_stats.received.opaque);
    }
    if (tap_stats.received.vbucket_set) {
        add_stat(c, add_stat_callback, "tap_vbucket_set_received",
                 tap_stats.received.vbucket_set);
    }
}

static void process_stat_settings(ADD_STAT add_stat_callback, void *c) {
    int ii;
    cb_assert(add_stat_callback);
    add_stat(c, add_stat_callback, "maxconns", settings.maxconns);

    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        char interface[1024];
        int offset;
        if (settings.interfaces[ii].host == NULL) {
            offset = sprintf(interface, "interface-*:%u", settings.interfaces[ii].port);
        } else {
            offset = snprintf(interface, sizeof(interface), "interface-%s:%u",
                              settings.interfaces[ii].host,
                              settings.interfaces[ii].port);
        }

        snprintf(interface + offset, sizeof(interface) - offset, "-maxconn");
        add_stat(c, add_stat_callback, interface, settings.interfaces[ii].maxconn);
        snprintf(interface + offset, sizeof(interface) - offset, "-backlog");
        add_stat(c, add_stat_callback, interface, settings.interfaces[ii].backlog);
        snprintf(interface + offset, sizeof(interface) - offset, "-ipv4");
        add_stat(c, add_stat_callback, interface, settings.interfaces[ii].ipv4);
        snprintf(interface + offset, sizeof(interface) - offset, "-ipv6");
        add_stat(c, add_stat_callback, interface, settings.interfaces[ii].ipv6);

        snprintf(interface + offset, sizeof(interface) - offset,
                 "-tcp_nodelay");
        add_stat(c, add_stat_callback, interface, settings.interfaces[ii].tcp_nodelay);

        if (settings.interfaces[ii].ssl.key) {
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl-pkey");
            add_stat(c, add_stat_callback, interface, settings.interfaces[ii].ssl.key);
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl-cert");
            add_stat(c, add_stat_callback, interface, settings.interfaces[ii].ssl.cert);
        } else {
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl");
            add_stat(c, add_stat_callback, interface, "false");
        }
    }

    add_stat(c, add_stat_callback, "verbosity", settings.verbose.load());
    add_stat(c, add_stat_callback, "num_threads", settings.num_threads);
    add_stat(c, add_stat_callback, "reqs_per_event_high_priority",
                settings.reqs_per_event_high_priority);
    add_stat(c, add_stat_callback, "reqs_per_event_med_priority",
                settings.reqs_per_event_med_priority);
    add_stat(c, add_stat_callback, "reqs_per_event_low_priority",
                settings.reqs_per_event_low_priority);
    add_stat(c, add_stat_callback, "reqs_per_event_def_priority",
                settings.default_reqs_per_event);
    add_stat(c, add_stat_callback, "auth_enabled_sasl", "yes");
    add_stat(c, add_stat_callback, "auth_sasl_engine", "cbsasl");
    add_stat(c, add_stat_callback, "auth_required_sasl", settings.require_sasl);
    {
        EXTENSION_DAEMON_DESCRIPTOR *ptr;
        for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
            add_stat(c, add_stat_callback, "extension", ptr->get_name());
        }
    }

    add_stat(c, add_stat_callback, "logger", settings.extensions.logger->get_name());
    {
        EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *ptr;
        for (ptr = settings.extensions.binary; ptr != NULL; ptr = ptr->next) {
            add_stat(c, add_stat_callback, "binary_extension", ptr->get_name());
        }
    }

    if (settings.config) {
        add_stat(c, add_stat_callback, "config", settings.config);
    }

    if (settings.rbac_file) {
        add_stat(c, add_stat_callback, "rbac", settings.rbac_file);
    }

    if (settings.audit_file) {
        add_stat(c, add_stat_callback, "audit", settings.audit_file);
    }
}

static cJSON *get_bucket_details(int idx)
{
    Bucket &bucket = all_buckets.at(idx);
    Bucket copy;

    /* make a copy so I don't have to do everything with the locks */
    cb_mutex_enter(&bucket.mutex);
    copy = bucket;
    cb_mutex_exit(&bucket.mutex);

    if (copy.state == BucketState::None) {
        return NULL;
    }

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "index", idx);
    switch (copy.state) {
    case BucketState::Creating:
        cJSON_AddStringToObject(root, "state", "creating");
        break;
    case BucketState::Initializing:
        cJSON_AddStringToObject(root, "state", "initializing");
        break;
    case BucketState::Ready:
        cJSON_AddStringToObject(root, "state", "ready");
        break;
    case BucketState::Stopping:
        cJSON_AddStringToObject(root, "state", "stopping");
        break;
    case BucketState::Destroying:
        cJSON_AddStringToObject(root, "state", "destroying");
        break;
    default:
        cb_assert(false);
    }

    cJSON_AddNumberToObject(root, "clients", copy.clients);
    cJSON_AddStringToObject(root, "name", copy.name);

    switch (copy.type) {
    case BucketType::NoBucket:
        cJSON_AddStringToObject(root, "type", "no bucket");
        break;
    case BucketType::Memcached:
        cJSON_AddStringToObject(root, "type", "memcached");
        break;
    case BucketType::Couchstore:
        cJSON_AddStringToObject(root, "type", "couchstore");
        break;
    case BucketType::EWouldBlock:
        cJSON_AddStringToObject(root, "type", "ewouldblock");
        break;
    default:
        cb_assert(false);
    }

    return root;
}

/**
 * This is a very slow thing that you shouldn't use in production ;-)
 *
 * @param c the connection to return the details for
 */
static void process_bucket_details(Connection *c)
{
    cJSON *obj = cJSON_CreateObject();

    cJSON *array = cJSON_CreateArray();
    for (int ii = 0; ii < settings.max_buckets; ++ii) {
        cJSON *o = get_bucket_details(ii);
        if (o != NULL) {
            cJSON_AddItemToArray(array, o);
        }
    }
    cJSON_AddItemToObject(obj, "buckets", array);

    char *stats_str = cJSON_PrintUnformatted(obj);
    append_stats("bucket details", 14, stats_str, uint32_t(strlen(stats_str)),
                 c);
    cJSON_Free(stats_str);
    cJSON_Delete(obj);
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(Connection *c) {
    cb_assert(c != NULL);
    cb_assert(c->read.curr <= (c->read.buf + c->read.size));
    cb_assert(c->read.bytes > 0);

    /* Do we have the complete packet header? */
    if (c->read.bytes < sizeof(c->binary_header)) {
        /* need more data! */
        return 0;
    } else {
#ifdef NEED_ALIGN
        if (((long)(c->read.curr)) % 8 != 0) {
            /* must realign input buffer */
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%d: Realign input buffer\n", c->sfd);
            }
        }
#endif
        protocol_binary_request_header* req;
        req = (protocol_binary_request_header*)c->read.curr;

        if (settings.verbose > 1) {
            /* Dump the packet before we convert it to host order */
            char buffer[1024];
            ssize_t nw;
            nw = bytes_to_output_string(buffer, sizeof(buffer), c->getId(),
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
            if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Invalid magic: %x, closing connection",
                                                c->getId(),
                                                c->binary_header.request.magic);
            } else {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                                "%u: Unsupported response packet received: %u, closing connection",
                                                c->getId(), (unsigned int)c->binary_header.request.opcode);

            }
            c->setState(conn_closing);
            return -1;
        }

        c->msgcurr = 0;
        c->msgused = 0;
        c->iovused = 0;
        if (add_msghdr(c) != 0) {
            c->setState(conn_closing);
            return -1;
        }

        c->cmd = c->binary_header.request.opcode;
        c->keylen = c->binary_header.request.keylen;
        c->opaque = c->binary_header.request.opaque;
        /* clear the returned cas value */
        c->cas = 0;

        dispatch_bin_command(c);

        c->read.bytes -= sizeof(c->binary_header);
        c->read.curr += sizeof(c->binary_header);
    }

    return 1;
}

bool conn_listening(Connection *c)
{
    SOCKET sfd;
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);
    int curr_conns;
    int port_conns;
    struct listening_port *port_instance;

    if ((sfd = accept(c->getSocketDescriptor(), (struct sockaddr *)&addr, &addrlen)) == -1) {
        auto error = GetLastNetworkError();
        if (is_emfile(error)) {
#if defined(WIN32)
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open files.");
#else
            struct rlimit limit = {0};
            getrlimit(RLIMIT_NOFILE, &limit);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open files. Current limit: %d\n",
                                            limit.rlim_cur);
#endif
            disable_listen();
        } else if (!is_blocking(error)) {
            log_socket_error(EXTENSION_LOG_WARNING, c,
                             "Failed to accept new client: %s");
        }

        return false;
    }

    curr_conns = stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    STATS_LOCK();
    port_instance = get_listening_port_instance(c->getParentPort());
    cb_assert(port_instance);
    port_conns = ++port_instance->curr_conns;
    STATS_UNLOCK();

    if (curr_conns >= settings.maxconns || port_conns >= port_instance->maxconns) {
        STATS_LOCK();
        --port_instance->curr_conns;
        STATS_UNLOCK();
        stats.rejected_conns++;
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
            "Too many open connections. Current/Limit for port %d: %d/%d; "
            "total: %d/%d", port_instance->port,
            port_conns, port_instance->maxconns,
            curr_conns, settings.maxconns);

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

    dispatch_conn_new(sfd, c->getParentPort(), conn_new_cmd);

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
bool conn_ship_log(Connection *c) {
    bool cont = false;
    short mask = EV_READ | EV_PERSIST | EV_WRITE;

    if (c->isSocketClosed()) {
        return false;
    }

    if (c->isReadEvent() || c->read.bytes > 0) {
        if (c->read.bytes > 0) {
            if (try_read_command(c) == 0) {
                c->setState(conn_read);
            }
        } else {
            c->setState(conn_read);
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
        c->setNumEvents(c->getMaxReqsPerEvent());
    } else if (c->isWriteEvent()) {
        if (c->decrementNumEvents() >= 0) {
            c->ewouldblock = false;
            if (c->isDCP()) {
                ship_dcp_log(c);
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

    if (!c->updateEvent(mask)) {
        c->setState(conn_closing);
    }

    return cont;
}

static bool is_bucket_dying(Connection *c)
{
    bool disconnect = false;
    Bucket &b = all_buckets.at(c->bucket.idx);
    cb_mutex_enter(&b.mutex);

    if (b.state != BucketState::Ready) {
        disconnect = true;
    }
    cb_mutex_exit(&b.mutex);

    if (disconnect) {
        c->setState(conn_closing);
        return true;
    }

    return false;
}

bool conn_waiting(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    if (!c->updateEvent(EV_READ | EV_PERSIST)) {
        c->setState(conn_closing);
        return true;
    }
    c->setState(conn_read);
    return false;
}

bool conn_read(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    switch (c->tryReadNetwork()) {
    case Connection::TryReadResult::NoDataReceived:
        c->setState(conn_waiting);
        break;
    case Connection::TryReadResult::DataReceived:
        c->setState(conn_parse_cmd);
        break;
    case Connection::TryReadResult::SocketError:
        c->setState(conn_closing);
        break;
    case Connection::TryReadResult::MemoryError: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(Connection *c) {
    if (try_read_command(c) == 0) {
        /* wee need more data! */
        c->setState(conn_waiting);
    }

    return !c->ewouldblock;
}

bool conn_new_cmd(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    c->start = 0;

    /*
     * In order to ensure that all clients will be served each
     * connection will only process a certain number of operations
     * before they will back off.
     */
    if (c->decrementNumEvents() >= 0) {
        reset_cmd_handler(c);
    } else {
        get_thread_stats(c)->conn_yields++;

        /*
         * If we've got data in the input buffer we might get "stuck"
         * if we're waiting for a read event. Why? because we might
         * already have all of the data for the next command in the
         * userspace buffer so the client is idle waiting for the
         * response to arrive. Lets set up a _write_ notification,
         * since that'll most likely be true really soon.
         *
         * DCP and TAP connections is different from normal
         * connections in the way that they may not even get data from
         * the other end so that they'll _have_ to wait for a write event.
         */
        if (c->havePendingInputData() || c->isDCP() || c->isTAP()) {
            if (!c->updateEvent(EV_WRITE | EV_PERSIST)) {
                c->setState(conn_closing);
                return true;
            }
        }
        return false;
    }

    return true;
}

bool conn_nread(Connection *c) {
    ssize_t res;

    if (c->rlbytes == 0) {
        bool block = c->ewouldblock = false;
        complete_nread(c);
        if (c->ewouldblock) {
            c->unregisterEvent();
            block = true;
        }
        return !block;
    }
    /* first check if we have leftovers in the conn_read buffer */
    if (c->read.bytes > 0) {
        uint32_t tocopy = c->read.bytes > c->rlbytes ? c->rlbytes : c->read.bytes;
        if (c->ritem != c->read.curr) {
            memmove(c->ritem, c->read.curr, tocopy);
        }
        c->ritem += tocopy;
        c->rlbytes -= tocopy;
        c->read.curr += tocopy;
        c->read.bytes -= tocopy;
        if (c->rlbytes == 0) {
            return true;
        }
    }

    /*  now try reading from the socket */
    res = c->recv(c->ritem, c->rlbytes);
    auto error = GetLastNetworkError();
    if (res > 0) {
        get_thread_stats(c)->bytes_read += res;
        if (c->read.curr == c->ritem) {
            c->read.curr += res;
        }
        c->ritem += res;
        c->rlbytes -= res;
        return true;
    }
    if (res == 0) { /* end of stream */
        c->setState(conn_closing);
        return true;
    }

    if (res == -1 && is_blocking(error)) {
        if (!c->updateEvent(EV_READ | EV_PERSIST)) {
            c->setState(conn_closing);
            return true;
        }
        return false;
    }

    /* otherwise we have a real error, on which we close the connection */
    if (!is_closed_conn(error)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u Failed to read, and not due to blocking:\n"
                                        "errno: %d %s \n"
                                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                                        c->getId(), errno, strerror(errno),
                                        (long)c->read.curr, (long)c->ritem, (long)c->read.buf,
                                        (int)c->rlbytes, (int)c->read.size);
    }
    c->setState(conn_closing);
    return true;
}

bool conn_write(Connection *c) {
    /*
     * We want to write out a simple response. If we haven't already,
     * assemble it into a msgbuf list (this will be a single-entry
     * list for TCP).
     */
    if (c->iovused == 0) {
        if (add_iov(c, c->write.curr, c->write.bytes) != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Couldn't build response, closing connection");
            c->setState(conn_closing);
            return true;
        }
    }

    return conn_mwrite(c);
}

bool conn_mwrite(Connection *c) {
    switch (c->transmit()) {
    case Connection::TransmitResult::Complete:
        if (c->getState() == conn_mwrite) {
            for (auto *it : c->reservedItems) {
                c->bucket.engine->release(v1_handle_2_handle(c->bucket.engine), c, it);
            }
            c->reservedItems.clear();
            for (auto *temp_alloc : c->temp_alloc) {
                free(temp_alloc);
            }
            c->temp_alloc.resize(0);

            /* XXX:  I don't know why this wasn't the general case */
            c->setState(c->write_and_go);
        } else if (c->getState() == conn_write) {
            if (c->write_and_free) {
                free(c->write_and_free);
                c->write_and_free = 0;
            }
            c->setState(c->write_and_go);
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Unexpected state %d, closing",
                                            c->getId(), c->getState());
            c->setState(conn_closing);
        }
        break;

    case Connection::TransmitResult::Incomplete:
    case Connection::TransmitResult::HardError:
        break;                   /* Continue in state machine. */

    case Connection::TransmitResult::SoftError:
        return false;
    }

    return true;
}

bool conn_pending_close(Connection *c) {
    cb_assert(c->isSocketClosed());
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

    c->setState(conn_immediate_close);
    return true;
}

bool conn_immediate_close(Connection *c) {
    struct listening_port *port_instance;
    cb_assert(c->isSocketClosed());
    settings.extensions.logger->log(EXTENSION_LOG_DETAIL, c,
                                    "Releasing connection %p",
                                    c);

    STATS_LOCK();
    port_instance = get_listening_port_instance(c->getParentPort());
    cb_assert(port_instance);
    --port_instance->curr_conns;
    STATS_UNLOCK();

    perform_callbacks(ON_DISCONNECT, NULL, c);
    disassociate_bucket(c);
    conn_close(c);

    return false;
}

bool conn_closing(Connection *c) {
    /* We don't want any network notifications anymore.. */
    c->unregisterEvent();
    safe_close(c->getSocketDescriptor());
    c->setSocketDescriptor(INVALID_SOCKET);

    /* engine::release any allocated state */
    conn_cleanup_engine_allocations(c);

    if (c->refcount > 1 || c->ewouldblock) {
        c->setState(conn_pending_close);
    } else {
        c->setState(conn_immediate_close);
    }
    return true;
}

/** sentinal state used to represent a 'destroyed' connection which will
 *  actually be freed at the end of the event loop. Always returns false.
 */
bool conn_destroyed(Connection * c) {
    (void)c;
    return false;
}

bool conn_setup_tap_stream(Connection *c) {
    process_bin_tap_connect(c);
    return true;
}

bool conn_refresh_cbsasl(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    cb_assert(ret != ENGINE_EWOULDBLOCK);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    return true;
}

bool conn_refresh_ssl_certs(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    cb_assert(ret != ENGINE_EWOULDBLOCK);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    return true;
}

/**
 * The conn_flush state in the state machinery means that we're currently
 * running a slow (and blocking) flush. The connection is "suspended" in
 * this state and when the connection is signalled this function is called
 * which sends the response back to the client.
 *
 * @param c the connection to send the result back to (currently stored in
 *          c->aiostat).
 * @return true to ensure that we continue to process events for this
 *              connection.
 */
bool conn_flush(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    return true;
}

bool conn_audit_configuring(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;
    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    default:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "configuration of audit "
                                        "daemon failed with config "
                                        "file: %s",
                                        settings.audit_file);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    }
    return true;
}

bool conn_create_bucket(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    cb_assert(ret != ENGINE_EWOULDBLOCK);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    return true;
}

bool conn_delete_bucket(Connection *c) {
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;

    cb_assert(ret != ENGINE_EWOULDBLOCK);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
    }

    return true;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    Connection *c = reinterpret_cast<Connection *>(arg);
    LIBEVENT_THREAD *thr;

    cb_assert(c != NULL);

    if (memcached_shutdown) {
        c->eventBaseLoopbreak();
        return ;
    }

    thr = c->thread;
    if (!is_listen_thread()) {
        cb_assert(thr);
        LOCK_THREAD(thr);
        /*
         * Remove the list from the list of pending io's (in case the
         * object was scheduled to run in the dispatcher before the
         * callback for the worker thread is executed.
         */
        c->thread->pending_io = list_remove(c->thread->pending_io, c);
    }

    c->setCurrentEvent(which);

    /* sanity */
    cb_assert(fd == c->getSocketDescriptor());

    c->setNumEvents(c->getMaxReqsPerEvent());

    run_event_loop(c);

    if (thr) {
        UNLOCK_THREAD(thr);
    }
}

static void dispatch_event_handler(evutil_socket_t fd, short which, void *arg) {
    char buffer[80];

    (void)which;
    (void)arg;
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
            Connection *next;
            for (next = listen_conn; next; next = next->next) {
                int backlog = 1024;
                int ii;
                next->updateEvent(EV_READ | EV_PERSIST);
                auto parent_port = next->getParentPort();
                for (ii = 0; ii < settings.num_interfaces; ++ii) {
                    if (parent_port == settings.interfaces[ii].port) {
                        backlog = settings.interfaces[ii].backlog;
                        break;
                    }
                }

                if (listen(next->getSocketDescriptor(), backlog) != 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "listen() failed",
                                                    strerror(errno));
                }
            }
        }
    }
}

/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const SOCKET sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int old_size;
#if defined(WIN32)
    char* old_ptr = reinterpret_cast<char*>(&old_size);
#else
    void* old_ptr = reinterpret_cast<void*>(&old_size);
#endif

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, old_ptr, &intsize) != 0) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "getsockopt(SO_SNDBUF): %s",
                                            strerror(errno));
        }

        return;
    }

    /* Binary-search for the real maximum. */
    int min = old_size;
    int max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        int avg = ((unsigned int)(min + max)) / 2;
#if defined(WIN32)
        char* avg_ptr = reinterpret_cast<char*>(&avg);
#else
        void* avg_ptr = reinterpret_cast<void*>(&avg);
#endif
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, avg_ptr, intsize) == 0) {
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

    maximize_sndbuf(sfd);

    return sfd;
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
static int server_socket(struct interface *interf, FILE *portnumber_file) {
    SOCKET sfd;
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints;
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    const char *host = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (interf->ipv4 && interf->ipv6) {
        hints.ai_family = AF_UNSPEC;
    } else if (interf->ipv4) {
        hints.ai_family = AF_INET;
    } else if (interf->ipv6) {
        hints.ai_family = AF_INET6;
    }

    snprintf(port_buf, sizeof(port_buf), "%u", (unsigned int)interf->port);

    if (interf->host) {
        if (strlen(interf->host) > 0 && strcmp(interf->host, "*") != 0) {
            host = interf->host;
        }
    }
    error = getaddrinfo(host, port_buf, &hints, &ai);
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
        const struct linger ling = {0, 0};
        const int flags = 1;

#if defined(WIN32)
        const char* ling_ptr = reinterpret_cast<const char*>(&ling);
        const char* flags_ptr = reinterpret_cast<const char*>(&flags);
#else
        const void* ling_ptr = reinterpret_cast<const char*>(&ling);
        const void* flags_ptr = reinterpret_cast<const void*>(&flags);
#endif

        struct listening_port *port_instance;
        Connection *listen_conn_add;
        if ((sfd = new_socket(next)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, flags_ptr,
                               sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(IPV6_V6ONLY): %s",
                                                strerror(errno));
                safe_close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, flags_ptr, sizeof(flags));
        error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, flags_ptr,
                           sizeof(flags));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_KEEPALIVE): %s",
                                            strerror(errno));
        }

        error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, ling_ptr, sizeof(ling));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_LINGER): %s",
                                            strerror(errno));
        }

        if (interf->tcp_nodelay) {
            error = setsockopt(sfd, IPPROTO_TCP,
                               TCP_NODELAY, flags_ptr, sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(TCP_NODELAY): %s",
                                                strerror(errno));
            }
        }

        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) == SOCKET_ERROR) {
            auto error = GetLastNetworkError();
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
            if (listen(sfd, interf->backlog) == SOCKET_ERROR) {
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

        if (!(listen_conn_add = conn_new(sfd, interf->port, conn_listening,
                                         main_base))) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "failed to create listening connection\n");
            exit(EXIT_FAILURE);
        }
        listen_conn_add->next = listen_conn;
        listen_conn = listen_conn_add;

        stats.daemon_conns++;
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
        STATS_LOCK();
        port_instance = get_listening_port_instance(interf->port);
        cb_assert(port_instance);
        ++port_instance->curr_conns;
        STATS_UNLOCK();
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int server_sockets(FILE *portnumber_file) {
    int ret = 0;
    int ii = 0;

    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        stats.listening_ports[ii].port = settings.interfaces[ii].port;
        stats.listening_ports[ii].maxconns = settings.interfaces[ii].maxconn;
        ret |= server_socket(settings.interfaces + ii, portnumber_file);
    }

    return ret;
}

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

static void sigterm_handler(evutil_socket_t, short, void *) {
    shutdown_server();
}
#endif

#ifndef WIN32
// SIGTERM and SIGINT event structures.
static struct event *term_event;
static struct event *int_event;
#endif

static int install_sigterm_handler(void) {
#ifndef WIN32
    term_event = evsignal_new(main_base, SIGTERM, sigterm_handler, NULL);
    if (term_event == NULL || event_add(term_event, NULL) < 0) {
        return -1;
    }

    int_event = evsignal_new(main_base, SIGINT, sigterm_handler, NULL);
    if (int_event == NULL || event_add(int_event, NULL) < 0) {
        return -1;
    }
#endif

    return 0;
}

static void shutdown_sigterm_handler() {
#ifndef WIN32
    event_free(int_event);
    event_free(term_event);
#endif
}

const char* get_server_version(void) {
    if (strlen(PRODUCT_VERSION) == 0) {
        return "unknown";
    } else {
        return PRODUCT_VERSION;
    }
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    Connection *c = (Connection *)cookie;
    c->engine_storage = engine_data;
}

static void *get_engine_specific(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->engine_storage;
}

static bool is_datatype_supported(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->isSupportsDatatype();
}

static bool is_mutation_extras_supported(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->isSupportsMutationExtras();
}

static uint8_t get_opcode_if_ewouldblock_set(const void *cookie) {
    Connection *c = (Connection *)cookie;
    uint8_t opcode = PROTOCOL_BINARY_CMD_INVALID;
    if (c->ewouldblock) {
        opcode = c->binary_header.request.opcode;
    }
    return opcode;
}

static bool validate_session_cas(const uint64_t cas) {
    bool ret = true;
    cb_mutex_enter(&(session_cas.mutex));
    if (cas != 0) {
        if (session_cas.value != cas) {
            ret = false;
        } else {
            session_cas.ctr++;
        }
    } else {
        session_cas.ctr++;
    }
    cb_mutex_exit(&(session_cas.mutex));
    return ret;
}

static void decrement_session_ctr(void) {
    cb_mutex_enter(&(session_cas.mutex));
    cb_assert(session_cas.ctr != 0);
    session_cas.ctr--;
    cb_mutex_exit(&(session_cas.mutex));
}

static SOCKET get_socket_fd(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->getSocketDescriptor();
}

static ENGINE_ERROR_CODE reserve_cookie(const void *cookie) {
    Connection *c = (Connection *)cookie;
    ++c->refcount;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE release_cookie(const void *cookie) {
    Connection *c = (Connection *)cookie;
    int notify;
    LIBEVENT_THREAD *thr;

    cb_assert(c);
    thr = c->thread;
    cb_assert(thr);
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

static void cookie_set_admin(const void *cookie) {
    cb_assert(cookie);
    auto conn = const_cast<void*>(cookie);
    reinterpret_cast<Connection *>(conn)->setAdmin(true);
}

static bool cookie_is_admin(const void *cookie) {
    if (settings.disable_admin) {
        return true;
    }
    cb_assert(cookie);
    return reinterpret_cast<const Connection *>(cookie)->isAdmin();
}

static void cookie_set_priority(const void* cookie, CONN_PRIORITY priority) {
    Connection * c = (Connection *)cookie;
    switch (priority) {
    case CONN_PRIORITY_HIGH:
        c->setMaxReqsPerEvent(settings.reqs_per_event_high_priority);
        break;
    case CONN_PRIORITY_MED:
        c->setMaxReqsPerEvent(settings.reqs_per_event_med_priority);
        break;
    case CONN_PRIORITY_LOW:
        c->setMaxReqsPerEvent(settings.reqs_per_event_low_priority);
        break;
    default:
        abort();
    }
}

static void count_eviction(const void *cookie, const void *key, const int nkey) {
    (void)cookie;
    (void)key;
    (void)nkey;
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
            auto* ext_daemon =
                    reinterpret_cast<EXTENSION_DAEMON_DESCRIPTOR*>(extension);

            EXTENSION_DAEMON_DESCRIPTOR *ptr;
            for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
                if (ptr == ext_daemon) {
                    return false;
                }
            }
            ext_daemon->next = settings.extensions.daemons;
            settings.extensions.daemons = ext_daemon;
        }
        return true;

    case EXTENSION_LOGGER:
        settings.extensions.logger =
                reinterpret_cast<EXTENSION_LOGGER_DESCRIPTOR*>(extension);
        return true;

    case EXTENSION_BINARY_PROTOCOL:
        {
            auto* ext_binprot =
                    reinterpret_cast<EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*>(extension);

            if (settings.extensions.binary != NULL) {
                EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *last;
                for (last = settings.extensions.binary; last->next != NULL;
                     last = last->next) {
                    if (last == ext_binprot) {
                        return false;
                    }
                }
                if (last == ext_binprot) {
                    return false;
                }
                last->next = ext_binprot;
                last->next->next = NULL;
            } else {
                settings.extensions.binary = ext_binprot;
                settings.extensions.binary->next = NULL;
            }

            ext_binprot->setup(setup_binary_lookup_cmd);
            return true;
        }

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

            if (ptr != NULL && settings.extensions.daemons == ptr) {
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
    event_base_loopbreak(main_base);
}

static EXTENSION_LOGGER_DESCRIPTOR* get_logger(void)
{
    return settings.extensions.logger;
}

static EXTENSION_LOG_LEVEL get_log_level(void)
{
    EXTENSION_LOG_LEVEL ret;
    switch (settings.verbose.load()) {
    case 0: ret = EXTENSION_LOG_NOTICE; break;
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
    case EXTENSION_LOG_WARNING:
    case EXTENSION_LOG_NOTICE:
        settings.verbose = 0;
        break;
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
        core_api.realtime = mc_time_convert_to_real_time;
        core_api.abstime = mc_time_convert_to_abs_time;
        core_api.get_current_time = mc_time_get_current_time;
        core_api.parse_config = parse_config;
        core_api.shutdown = shutdown_server;
        core_api.get_config = get_config;

        server_cookie_api.get_auth_data = get_auth_data;
        server_cookie_api.store_engine_specific = store_engine_specific;
        server_cookie_api.get_engine_specific = get_engine_specific;
        server_cookie_api.is_datatype_supported = is_datatype_supported;
        server_cookie_api.is_mutation_extras_supported = is_mutation_extras_supported;
        server_cookie_api.get_opcode_if_ewouldblock_set = get_opcode_if_ewouldblock_set;
        server_cookie_api.validate_session_cas = validate_session_cas;
        server_cookie_api.decrement_session_ctr = decrement_session_ctr;
        server_cookie_api.get_socket_fd = get_socket_fd;
        server_cookie_api.notify_io_complete = notify_io_complete;
        server_cookie_api.reserve = reserve_cookie;
        server_cookie_api.release = release_cookie;
        server_cookie_api.set_admin = cookie_set_admin;
        server_cookie_api.is_admin = cookie_is_admin;
        server_cookie_api.set_priority = cookie_set_priority;
        server_cookie_api.get_bucket_id = get_bucket_id;

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
        hooks_api.release_free_memory = mc_release_free_memory;
        hooks_api.enable_thread_cache = mc_enable_thread_cache;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    // @trondn fixme!!!
    if (rv.engine == NULL) {
        /* rv.engine = settings.engine.v0; */
    }

    return &rv;
}

static void process_bin_dcp_response(Connection *c) {
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    c->setSupportsDatatype(true);

    if (c->bucket.engine->dcp.response_handler != NULL) {
        auto* header = reinterpret_cast<protocol_binary_response_header*>
            (c->read.curr - (c->binary_header.request.bodylen +
                             sizeof(c->binary_header)));
        ret = c->bucket.engine->dcp.response_handler
                (v1_handle_2_handle(c->bucket.engine), c, header);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}

/* BUCKET FUNCTIONS */

/**
 * @todo this should be run as its own thread!!! look at cbsasl refresh..
 */
static ENGINE_ERROR_CODE do_create_bucket(const std::string& bucket_name,
                                          char *config,
                                          BucketType engine) {
    int ii;
    int first_free = -1;
    bool found = false;
    ENGINE_ERROR_CODE ret;

    /*
     * the number of buckets cannot change without a restart, but we don't want
     * to lock the entire bucket array during checking for the existence
     * of the bucket and while we're locating the next entry.
     */
    cb_mutex_enter(&buckets_lock);

    for (ii = 0; ii < settings.max_buckets && !found; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (first_free == -1 && all_buckets[ii].state == BucketState::None) {
            first_free = ii;
        }
        if (bucket_name == all_buckets[ii].name) {
            found = true;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }

    if (found) {
        ret = ENGINE_KEY_EEXISTS;
    } else if (first_free == -1) {
        ret = ENGINE_E2BIG;
    } else {
        ret = ENGINE_SUCCESS;
        ii = first_free;
        /*
         * split the creation of the bucket in two... so
         * we can release the global lock..
         */
        cb_mutex_enter(&all_buckets[ii].mutex);
        all_buckets[ii].state = BucketState::Creating;
        all_buckets[ii].type = engine;
        strcpy(all_buckets[ii].name, bucket_name.c_str());
        try {
            all_buckets[ii].topkeys = new TopKeys(settings.topkeys_size);
        } catch (const std::bad_alloc &) {
            ret = ENGINE_ENOMEM;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }
    cb_mutex_exit(&buckets_lock);

    if (ret == ENGINE_SUCCESS) {
        /* People aren't allowed to use the engine in this state,
         * so we can do stuff without locking..
         */
        if (new_engine_instance(engine, get_server_api, (ENGINE_HANDLE**)&all_buckets[ii].engine)) {
            cb_mutex_enter(&all_buckets[ii].mutex);
            all_buckets[ii].state = BucketState::Initializing;
            cb_mutex_exit(&all_buckets[ii].mutex);

            ret = all_buckets[ii].engine->initialize
                    (v1_handle_2_handle(all_buckets[ii].engine), config);
            if (ret == ENGINE_SUCCESS) {
                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::Ready;
                cb_mutex_exit(&all_buckets[ii].mutex);
            } else {
                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::Destroying;
                cb_mutex_exit(&all_buckets[ii].mutex);
                all_buckets[ii].engine->destroy
                    (v1_handle_2_handle(all_buckets[ii].engine), false);

                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::None;
                all_buckets[ii].name[0] = '\0';
                cb_mutex_exit(&all_buckets[ii].mutex);

                ret = ENGINE_NOT_STORED;
            }
        } else {
            cb_mutex_enter(&all_buckets[ii].mutex);
            all_buckets[ii].state = BucketState::None;
            all_buckets[ii].name[0] = '\0';
            cb_mutex_exit(&all_buckets[ii].mutex);
            /* @todo should I change the error code? */
        }
    }

    return ret;
}

void create_bucket_main(void *arg)
{
    Connection *c = reinterpret_cast<Connection *>(arg);
    ENGINE_ERROR_CODE ret;
    char *packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));
    auto* req = reinterpret_cast<protocol_binary_request_create_bucket*>(packet);
    /* decode packet */
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    blen -= klen;

    try {
        std::string key((char*)(req + 1), klen);
        std::string value((char*)(req + 1) + klen, blen);

        char *config = NULL;

        // Check if (optional) config was included after the value.
        auto marker = value.find('\0');
        if (marker != std::string::npos) {
            config = &value[marker + 1];
        }

        BucketType engine = module_to_bucket_type(value.c_str());
        if (engine == BucketType::Unknown) {
            /* We should have other error codes as well :-) */
            ret = ENGINE_NOT_STORED;
        } else {
            ret = do_create_bucket(key, config, engine);
        }

    } catch (const std::bad_alloc&) {
        ret = ENGINE_ENOMEM;
    }

    notify_io_complete(c, ret);
}

void notify_thread_bucket_deletion(LIBEVENT_THREAD *me) {
    for (int ii = 0; ii < settings.max_buckets; ++ii) {
        bool destroy = false;
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (all_buckets[ii].state == BucketState::Destroying) {
            destroy = true;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
        if (destroy) {
            signal_idle_clients(me, ii);
        }
    }
}

/**
 * @todo this should be run as its own thread!!! look at cbsasl refresh..
 */
static ENGINE_ERROR_CODE do_delete_bucket(Connection *c,
                                          const std::string& bucket_name,
                                          bool force)
{
    ENGINE_ERROR_CODE ret = ENGINE_KEY_ENOENT;
    /*
     * the number of buckets cannot change without a restart, but we don't want
     * to lock the entire bucket array during checking for the existence
     * of the bucket and while we're locating the next entry.
     */
    int idx = 0;
    int ii;
    for (ii = 0; ii < settings.max_buckets; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (bucket_name == all_buckets[ii].name) {
            idx = ii;
            if (all_buckets[ii].state == BucketState::Ready) {
                ret = ENGINE_SUCCESS;
                all_buckets[ii].state = BucketState::Destroying;
            } else {
                ret = ENGINE_KEY_EEXISTS;
            }
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
        if (ret != ENGINE_KEY_ENOENT) {
            break;
        }
    }

    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    /* If this thread is connected to the requested bucket... release it */
    if (ii == c->bucket.idx) {
        disassociate_bucket(c);
    }

    /* Let all of the worker threads start invalidating connections */
    threads_initiate_bucket_deletion();


    /* Wait until all users disconnected... */
    cb_mutex_enter(&all_buckets[idx].mutex);
    while (all_buckets[idx].clients > 0) {
        /* drop the lock and notify the worker threads */
        cb_mutex_exit(&all_buckets[idx].mutex);
        threads_notify_bucket_deletion();
        cb_mutex_enter(&all_buckets[idx].mutex);

        cb_cond_timedwait(&all_buckets[idx].cond,
                          &all_buckets[idx].mutex,
                          1000);
    }
    cb_mutex_exit(&all_buckets[idx].mutex);

    /* Tell the worker threads to stop trying to invalidating connections */
    threads_complete_bucket_deletion();

    /* assert that all associations are gone. */
    assert_no_associations(idx);

    all_buckets[idx].engine->destroy
            (v1_handle_2_handle(all_buckets[idx].engine), force);

    /* Clean up the stats... */
    delete []all_buckets[idx].stats;
    int numthread = settings.num_threads + 1;
    all_buckets[idx].stats = new thread_stats[numthread];

    memset(&all_buckets[idx].engine_event_handlers, 0,
           sizeof(all_buckets[idx].engine_event_handlers));

    cb_mutex_enter(&all_buckets[idx].mutex);
    all_buckets[idx].state = BucketState::None;
    all_buckets[idx].engine = NULL;
    all_buckets[idx].name[0] = '\0';
    delete all_buckets[idx].topkeys;
    all_buckets[idx].topkeys = nullptr;
    cb_mutex_exit(&all_buckets[idx].mutex);
    // don't need lock because all timing data uses atomics
    all_buckets[idx].timings.reset();

    return ret;
}

static void delete_bucket_main(void *arg)
{
    Connection *c = reinterpret_cast<Connection *>(arg);
    ENGINE_ERROR_CODE ret;
    char *packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));

    auto* req = reinterpret_cast<protocol_binary_request_delete_bucket*>(packet);
    /* decode packet */
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    blen -= klen;


    try {
        std::string key((char*)(req + 1), klen);
        std::string config((char*)(req + 1) + klen, blen);

        bool force = false;
        struct config_item items[2];
        memset(&items, 0, sizeof(items));
        items[0].key = "force";
        items[0].datatype = DT_BOOL;
        items[0].value.dt_bool = &force;
        items[1].key = NULL;

        if (parse_config(config.c_str(), items, stderr) == 0) {
            ret = do_delete_bucket(c, key, force);
        } else {
            ret = ENGINE_EINVAL;
        }
    } catch (const std::bad_alloc&) {
        ret = ENGINE_ENOMEM;
    }

    notify_io_complete(c, ret);
}

static void initialize_buckets(void) {
    cb_mutex_initialize(&buckets_lock);
    all_buckets.resize(settings.max_buckets);

    int numthread = settings.num_threads + 1;
    for (auto &b : all_buckets) {
        b.stats = new thread_stats[numthread];
    }

    // To make the life easier for us in the code, index 0
    // in the array is "no bucket"
    ENGINE_HANDLE *handle;
    cb_assert(new_engine_instance(BucketType::NoBucket,
                                  get_server_api,
                                  &handle));

    cb_assert(handle != nullptr);
    auto &nobucket = all_buckets.at(0);
    nobucket.type = BucketType::NoBucket;
    nobucket.state = BucketState::Ready;
    nobucket.engine = (ENGINE_HANDLE_V1*)handle;
}

static void cleanup_buckets(void) {
    for (auto &bucket : all_buckets) {
        bool waiting;

        do {
            waiting = false;
            cb_mutex_enter(&bucket.mutex);
            switch (bucket.state) {
            case BucketState::Stopping:
            case BucketState::Destroying:
            case BucketState::Creating:
            case BucketState::Initializing:
                waiting = true;
                break;
            default:
                /* Empty */
                ;
            }
            cb_mutex_exit(&bucket.mutex);
            if (waiting) {
                usleep(250);
            }
        } while (waiting);

        if (bucket.state == BucketState::Ready) {
            bucket.engine->destroy(v1_handle_2_handle(bucket.engine), false);
        }

        delete []bucket.stats;
    }
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

    response_handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_FLUSH] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_RESERVED4] = process_bin_dcp_response;
}

/**
 * Load a shared object and initialize all the extensions in there.
 *
 * @param soname the name of the shared object (may not be NULL)
 * @param config optional configuration parameters
 * @return true if success, false otherwise
 */
bool load_extension(const char *soname, const char *config) {
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
    log_errcode_error(severity, cookie, prefix, GetLastNetworkError());
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
    log_errcode_error(severity, cookie, prefix, GetLastError());
}

void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                       const void* cookie,
                       const char* prefix,
                       cb_os_error_t err)
{
    std::string errmsg = cb_strerror(err);
    settings.extensions.logger->log(severity, cookie, prefix, errmsg.c_str());
}

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

static void set_max_filehandles(void) {
    /* EMPTY */
}

#else
static void parent_monitor_thread(void *arg) {
    pid_t pid = atoi(reinterpret_cast<char*>(arg));
    while (true) {
        sleep(1);
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            _exit(1);
        }
    }
}

static void setup_parent_monitor(void) {
    char *env = getenv("MEMCACHED_PARENT_MONITOR");
    if (env != NULL) {
        cb_thread_t t;
        if (cb_create_named_thread(&t, parent_monitor_thread, env, 1,
                                   "mc:parent mon") != 0) {
            log_system_error(EXTENSION_LOG_WARNING, NULL,
                "Failed to open parent process: %s");
            exit(EXIT_FAILURE);
        }
    }
}

static void set_max_filehandles(void) {
    struct rlimit rlim;

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        const rlim_t maxfiles = settings.maxconns + (3 * (settings.num_threads + 2));
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
}

#endif

static cb_mutex_t *openssl_lock_cs;

static unsigned long get_thread_id(void) {
    return (unsigned long)cb_thread_self();
}

static void openssl_locking_callback(int mode, int type, const char *file,
                                     int line)
{
    (void)line;
    (void)file;

    if (mode & CRYPTO_LOCK) {
        cb_mutex_enter(&(openssl_lock_cs[type]));
    } else {
        cb_mutex_exit(&(openssl_lock_cs[type]));
    }
}

static void initialize_openssl(void) {
    int ii;

    CRYPTO_malloc_init();
    SSL_library_init();
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();

    openssl_lock_cs = reinterpret_cast<cb_mutex_t*>
        (calloc(CRYPTO_num_locks(), sizeof(cb_mutex_t)));
    for (ii = 0; ii < CRYPTO_num_locks(); ii++) {
        cb_mutex_initialize(&(openssl_lock_cs[ii]));
    }

    CRYPTO_set_id_callback(get_thread_id);
    CRYPTO_set_locking_callback(openssl_locking_callback);
}

static void shutdown_openssl() {
    // Global OpenSSL cleanup:
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);
    ENGINE_cleanup();
    CONF_modules_unload(1);
    ERR_free_strings();
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();

    // per-thread cleanup:
    ERR_remove_state(0);

    // Newer versions of openssl (1.0.2a) have a the function
    // SSL_COMP_free_compression_methods() to perform this;
    // however we arn't that new...
    sk_SSL_COMP_free(SSL_COMP_get_compression_methods());

    free(openssl_lock_cs);
}

void calculate_maxconns(void) {
    int ii;
    settings.maxconns = 0;
    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        settings.maxconns += settings.interfaces[ii].maxconn;
    }
}

static void load_extensions(void) {
    for (int ii = 0; ii < settings.num_pending_extensions; ii++) {
        if (!load_extension(settings.pending_extensions[ii].soname,
                            settings.pending_extensions[ii].config)) {
            exit(EXIT_FAILURE);
        }
    }
}

int main (int argc, char **argv) {
    // MB-14649 log() crash on windows on some CPU's
#ifdef _WIN64
    _set_FMA3_enable (0);
#endif

#ifdef HAVE_LIBNUMA
    enum class NumaPolicy {
        NOT_AVAILABLE,
        DISABLED,
        INTERLEAVE
    } numa_policy = NumaPolicy::NOT_AVAILABLE;
    const char* mem_policy_env = NULL;

    if (numa_available() == 0) {
        // Set the default NUMA memory policy to interleaved.
        mem_policy_env = getenv("MEMCACHED_NUMA_MEM_POLICY");
        if (mem_policy_env != NULL && strcmp("disable", mem_policy_env) == 0) {
            numa_policy = NumaPolicy::DISABLED;
        } else {
            numa_set_interleave_mask(numa_all_nodes_ptr);
            numa_policy = NumaPolicy::INTERLEAVE;
        }
    }
#endif

    initialize_openssl();

    /* Initialize global variables */
    cb_mutex_initialize(&listen_state.mutex);
    cb_mutex_initialize(&stats_lock);
    cb_mutex_initialize(&session_cas.mutex);

    session_cas.value = 0xdeadbeef;
    session_cas.ctr = 0;

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

    {
        // MB-13642 Allow the user to specify the SSL cipher list
        //    If someone wants to use SSL we should try to be "secure
        //    by default", and only allow for using strong ciphers.
        //    Users that may want to use a less secure cipher list
        //    should be allowed to do so by setting an environment
        //    variable (since there is no place in the UI to do
        //    so currently). Whenever ns_server allows for specifying
        //    the SSL cipher list in the UI, it will be stored
        //    in memcached.json and override these settings.
        const char *env = getenv("COUCHBASE_SSL_CIPHER_LIST");
        if (env == NULL) {
            set_ssl_cipher_list("HIGH");
        } else {
            set_ssl_cipher_list(env);
        }
    }

    /* Parse command line arguments */
    parse_arguments(argc, argv);

    settings_init_relocable_files();

    set_server_initialized(!settings.require_init);

    /* Initialize breakpad crash catcher with our just-parsed settings. */
    initialize_breakpad(&settings.breakpad);

    /* load extensions specified in the settings */
    load_extensions();

    /* Logging available now extensions have been loaded. */
    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Couchbase version %s starting.",
                                    get_server_version());

#ifdef HAVE_LIBNUMA
    // Log the NUMA policy selected.
    switch (numa_policy) {
    case NumaPolicy::NOT_AVAILABLE:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: Not available - not setting mem policy.");
        break;

    case NumaPolicy::DISABLED:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: NOT setting memory allocation policy - "
                                        "disabled via MEMCACHED_NUMA_MEM_POLICY='%s'.",
                                        mem_policy_env);
        break;

    case NumaPolicy::INTERLEAVE:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: Set memory allocation policy to 'interleave'.");
        break;
    }
#endif


    /* Start the audit daemon */
    AUDIT_EXTENSION_DATA audit_extension_data;
    audit_extension_data.version = 1;
    audit_extension_data.min_file_rotation_time = 900;  // 15 minutes = 60*15
    audit_extension_data.max_file_rotation_time = 604800;  // 1 week = 60*60*24*7
    audit_extension_data.log_extension = settings.extensions.logger;
    audit_extension_data.notify_io_complete = notify_io_complete;
    if (start_auditdaemon(&audit_extension_data) != AUDIT_SUCCESS) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "FATAL: Failed to start "
                                        "audit daemon");
        abort();
    } else if (settings.audit_file) {
        /* configure the audit daemon */
        if (configure_auditdaemon(settings.audit_file, NULL) != AUDIT_SUCCESS) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "FATAL: Failed to initialize audit "
                                        "daemon with configuation file: %s",
                                        settings.audit_file);
            /* we failed configuring the audit.. run without it */
            free((void*)settings.audit_file);
            settings.audit_file = NULL;
        }
    }

    /* Initialize RBAC data */
    if (load_rbac_from_file(settings.rbac_file) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "FATAL: Failed to load RBAC configuration: %s",
                                        (settings.rbac_file) ?
                                        settings.rbac_file :
                                        "no file specified");
        abort();
    }

    /* inform interested parties of initial verbosity level */
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);

    set_max_filehandles();

    /* Aggregate the maximum number of connections */
    calculate_maxconns();

    {
        char *errmsg;
        if (!initialize_engine_map(&errmsg, settings.extensions.logger)) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "%s", errmsg);
            exit(EXIT_FAILURE);
        }
    }

    /* Initialize bucket engine */
    initialize_buckets();

    cbsasl_server_init();

    /* initialize main thread libevent instance */
    main_base = event_base_new();

    /* Initialize SIGINT/SIGTERM handler (requires libevent). */
    if (install_sigterm_handler() != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to install SIGTERM handler\n");
        exit(EXIT_FAILURE);
    }

    /* initialize other stuff */
    stats_init();

#ifndef WIN32
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

    /* Initialise memcached time keeping */
    mc_time_init(main_base);

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

        if (server_sockets(portnumber_file)) {
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

    /* Drop privileges no longer needed */
    drop_privileges();

    /* Optional parent monitor */
    setup_parent_monitor();

    cb_set_thread_name("mc:listener");

    if (!memcached_shutdown) {
        /* enter the event loop */
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "Initialization complete. Accepting clients.");
        event_base_loop(main_base, 0);
    }

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Initiating graceful shutdown.");

    /* Close down the audit daemon cleanly */
    shutdown_auditdaemon(settings.audit_file);

    threads_shutdown();

    close_all_connections();
    cleanup_buckets();

    threads_cleanup();

    /* Free the memory used by listening_port structure */
    if (stats.listening_ports) {
        free(stats.listening_ports);
    }

    shutdown_sigterm_handler();
    event_base_free(main_base);
    cbsasl_server_term();
    destroy_connections();

    shutdown_engine_map();
    destroy_breakpad();

    free_callbacks();
    free_settings(&settings);

    shutdown_openssl();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Shutdown complete.");

    return EXIT_SUCCESS;
}
