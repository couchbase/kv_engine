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
#include "mcbp.h"
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
#include "session_cas.h"
#include "settings.h"
#include "subdocument.h"
#include "enginemap.h"
#include "buckets.h"
#include "topkeys.h"
#include "stats.h"
#include "mcbp_executors.h"
#include "memcached_openssl.h"
#include "privileges.h"
#include "greenstack.h"
#include "mcbpdestroybuckettask.h"

#include <platform/backtrace.h>
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
#include <algorithm>
#include <cJSON_utils.h>

// MB-14649: log crashing on windows..
#include <math.h>

#if HAVE_LIBNUMA
#include <numa.h>
#endif

static EXTENSION_LOG_LEVEL get_log_level(void);

/**
 * All of the buckets in couchbase is stored in this array.
 */
static cb_mutex_t buckets_lock;
std::vector<Bucket> all_buckets;

static ENGINE_HANDLE* v1_handle_2_handle(ENGINE_HANDLE_V1* v1) {
    return reinterpret_cast<ENGINE_HANDLE*>(v1);
}

const char* getBucketName(const Connection* c) {
    return all_buckets[c->getBucketIndex()].name;
}

std::atomic<bool> memcached_shutdown;
std::atomic<bool> service_online;
// Should we enable to common ports (all of the ports which arn't tagged as
// management ports)
static std::atomic<bool> enable_common_ports;

std::unique_ptr<ExecutorPool> executorPool;

/* Mutex for global stats */
std::mutex stats_mutex;

/*
 * forward declarations
 */
static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data);
static SERVER_HANDLE_V1 *get_server_api(void);
static void create_listen_sockets(bool management);

/* stats */
static void stats_init(void);

/* defaults */
static void settings_init(void);

/** exported globals **/
struct stats stats;
struct settings settings;

/** file scope variables **/
Connection *listen_conn = NULL;
static struct event_base *main_base;

static engine_event_handler_array_t engine_event_handlers;

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
char reset_stats_time[80];
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

void disassociate_bucket(Connection *c) {
    Bucket &b = all_buckets.at(c->getBucketIndex());
    cb_mutex_enter(&b.mutex);
    b.clients--;

    c->setBucketIndex(0);
    c->setBucketEngine(nullptr);

    if (b.clients == 0 && b.state == BucketState::Destroying) {
        cb_cond_signal(&b.cond);
    }

    cb_mutex_exit(&b.mutex);
}

bool associate_bucket(Connection *c, const char *name) {
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
            c->setBucketIndex(ii);
            c->setBucketEngine(b.engine);
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
        c->setBucketIndex(0);
        c->setBucketEngine(b.engine);
    }

    return found;
}

void associate_initial_bucket(Connection *c) {
    Bucket &b = all_buckets.at(0);
    cb_mutex_enter(&b.mutex);
    b.clients++;
    cb_mutex_exit(&b.mutex);

    c->setBucketIndex(0);
    c->setBucketEngine(b.engine);

    associate_bucket(c, "default");
}

static void populate_log_level(void*) {
    // Lock the entire buckets array so that buckets can't be modified while
    // we notify them (blocking bucket creation/deletion)
    auto val = get_log_level();

    cb_mutex_enter(&buckets_lock);
    for (auto& bucket : all_buckets) {
        cb_mutex_enter(&bucket.mutex);
        if (bucket.state == BucketState::Ready &&
            bucket.engine->set_log_level != nullptr) {
            bucket.engine->set_log_level(reinterpret_cast<ENGINE_HANDLE*>(bucket.engine),
                                         val);
        }
        cb_mutex_exit(&bucket.mutex);
    }
    cb_mutex_exit(&buckets_lock);
}

/* Perform all callbacks of a given type for the given connection. */
void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *cookie)
{
    cb_thread_t tid;

    switch (type) {
        /*
         * The following events operates on a connection which is passed in
         * as the cookie.
         */
    case ON_DISCONNECT: {
        const Connection * connection = reinterpret_cast<const Connection *>(cookie);
        if (connection == nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie is NULL");
        }
        const auto bucket_idx = connection->getBucketIndex();
        if (bucket_idx == -1) {
            throw std::logic_error("perform_callbacks: connection (which is " +
                        std::to_string(connection->getId()) + ") cannot be "
                        "disconnected as it is not associated with a bucket");
        }

        for (auto& handler : all_buckets[bucket_idx].engine_event_handlers[type]) {
            handler.cb(cookie, ON_DISCONNECT, data, handler.cb_data);
        }
        break;
    }
    case ON_LOG_LEVEL:
        if (cookie != nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie "
                "(which is " +
                std::to_string(reinterpret_cast<uintptr_t>(cookie)) +
                ") should be NULL for ON_LOG_LEVEL");
        }
        for (auto& handler : engine_event_handlers[type]) {
            handler.cb(cookie, ON_LOG_LEVEL, data, handler.cb_data);
        }

        if (service_online) {
            if (cb_create_thread(&tid, populate_log_level, nullptr, 1) == -1) {
                LOG_WARNING(NULL,
                            "Failed to create thread to notify engines about "
                                "changing log level");
            }
        }
        break;

    case ON_DELETE_BUCKET: {
        /** cookie is the bucket entry */
        auto* bucket = reinterpret_cast<const Bucket*>(cookie);
        for (auto& handler : bucket->engine_event_handlers[type]) {
            handler.cb(cookie, ON_DELETE_BUCKET, data, handler.cb_data);
        }
        break;
    }

    case ON_INIT_COMPLETE:
        if ((data != nullptr) || (cookie != nullptr)) {
            throw std::invalid_argument("perform_callbacks: data and cookie"
                                            " should be nullptr");
        }
        enable_common_ports.store(true, std::memory_order_release);
        notify_dispatcher();
        break;

    default:
        throw std::invalid_argument("perform_callbacks: type "
                "(which is " + std::to_string(type) +
                "is not a valid ENGINE_EVENT_TYPE");
    }
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data)
{
    int idx;
    switch (type) {
    /*
     * The following events operates on a connection which is passed in
     * as the cookie.
     */
    case ON_DISCONNECT:
    case ON_DELETE_BUCKET:
        if (eh == nullptr) {
            throw std::invalid_argument("register_callback: 'eh' must be non-NULL");
        }
        for (idx = 0; idx < settings.max_buckets; ++idx) {
            if ((void *)eh == (void *)all_buckets[idx].engine) {
                break;
            }
        }
        if (idx == settings.max_buckets) {
            throw std::invalid_argument("register_callback: eh (which is" +
                    std::to_string(reinterpret_cast<uintptr_t>(eh)) +
                    ") is not a engine associated with a bucket");
        }
        all_buckets[idx].engine_event_handlers[type].push_back({cb, cb_data});
        break;

    case ON_LOG_LEVEL:
        if (eh != nullptr) {
            throw std::invalid_argument("register_callback: 'eh' must be NULL");
        }
        engine_event_handlers[type].push_back({cb, cb_data});
        break;

    default:
        throw std::invalid_argument("register_callback: type (which is " +
                                    std::to_string(type) +
                                    ") is not a valid ENGINE_EVENT_TYPE");
    }
}

static void free_callbacks() {
    // free per-bucket callbacks.
    for (int idx = 0; idx < settings.max_buckets; ++idx) {
        for (auto& type_vec : all_buckets[idx].engine_event_handlers) {
            type_vec.clear();
        }
    }

    // free global callbacks
    for (auto& type_vec : engine_event_handlers) {
        type_vec.clear();
    }
}

static void stats_init(void) {
    set_stats_reset_time();
    stats.conn_structs.reset();
    stats.total_conns.reset();
    stats.daemon_conns.reset();
    stats.rejected_conns.reset();
    stats.curr_conns.store(0, std::memory_order_relaxed);
}

struct thread_stats *get_thread_stats(Connection *c) {
    struct thread_stats *independent_stats;
    cb_assert(c->getThread()->index < (settings.num_threads + 1));
    independent_stats = all_buckets[c->getBucketIndex()].stats;
    return &independent_stats[c->getThread()->index];
}

void stats_reset(const void *cookie) {
    auto *conn = (Connection *)cookie;
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        set_stats_reset_time();
    }
    stats.total_conns.reset();
    stats.rejected_conns.reset();
    threadlocal_stats_reset(all_buckets[conn->getBucketIndex()].stats);
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
    settings.connection_idle_time = 0; // Connection idle time disabled
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
    // (we need entry 0 in the list to represent "no bucket")
    settings.max_buckets = COUCHBASE_MAX_NUM_BUCKETS + 1;
    settings.admin = strdup("_admin");
    settings.dedupe_nmvb_maps.store(false);

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

    if (settings.root) {
        root = settings.root;
    }

    if (settings.rbac_file == NULL) {
        std::string fname(root);
        fname.append("/etc/security/rbac.json");
#ifdef WIN32
        // Make sure that the path is in windows format
        std::replace(fname.begin(), fname.end(), '/', '\\');
#endif

        FILE *fp = fopen(fname.c_str(), "r");
        if (fp != NULL) {
            settings.rbac_file = strdup(fname.c_str());
            fclose(fp);
        }
    }
}

struct {
    std::mutex mutex;
    bool disabled;
    ssize_t count;
    uint64_t num_disable;
} listen_state;

bool is_listen_disabled(void) {
    std::lock_guard<std::mutex> guard(listen_state.mutex);
    return listen_state.disabled;
}

uint64_t get_listen_disabled_num(void) {
    std::lock_guard<std::mutex> guard(listen_state.mutex);
    return listen_state.num_disable;
}

static void disable_listen(void) {
    Connection *next;
    {
        std::lock_guard<std::mutex> guard(listen_state.mutex);
        listen_state.disabled = true;
        listen_state.count = 10;
        ++listen_state.num_disable;
    }

    for (next = listen_conn; next; next = next->getNext()) {
        auto* connection = dynamic_cast<ListenConnection*>(next);
        if (connection == nullptr) {
            LOG_WARNING(next, "Internal error. Tried to disable listen on"
                " an illegal connection object");
            continue;
        }
        connection->disable();
    }
}

void safe_close(SOCKET sfd) {
    if (sfd != INVALID_SOCKET) {
        int rval;

        do {
            rval = evutil_closesocket(sfd);
        } while (rval == SOCKET_ERROR && is_interrupted(GetLastNetworkError()));

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

bucket_id_t get_bucket_id(const void *cookie) {
    /* @todo fix this. Currently we're using the index as the id,
     * but this should be changed to be a uniqe ID that won't be
     * reused.
     */
    return bucket_id_t(((Connection*)(cookie))->getBucketIndex());
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

ENGINE_ERROR_CODE refresh_cbsasl(Connection *c)
{
    cb_thread_t tid;
    int err;

    err = cb_create_named_thread(&tid, cbsasl_refresh_main, c, 1,
                                 "mc:refresh_sasl");
    if (err != 0) {
        LOG_WARNING(c, "Failed to create cbsasl db update thread: %s",
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

ENGINE_ERROR_CODE refresh_ssl_certs(Connection *c)
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

static cJSON *get_bucket_details_UNLOCKED(const Bucket& bucket, int idx) {
    if (bucket.state == BucketState::None) {
        return nullptr;
    }

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "index", idx);
    switch (bucket.state) {
    case BucketState::None:
        cJSON_AddStringToObject(root, "state", "none");
        break;
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
    }

    cJSON_AddNumberToObject(root, "clients", bucket.clients);
    cJSON_AddStringToObject(root, "name", bucket.name);

    switch (bucket.type) {
    case BucketType::Unknown:
        cJSON_AddStringToObject(root, "type", "<<unknown>>");
        break;
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
    }

    return root;
}

cJSON *get_bucket_details(int idx)
{
    cJSON* ret;
    Bucket &bucket = all_buckets.at(idx);
    cb_mutex_enter(&bucket.mutex);
    ret = get_bucket_details_UNLOCKED(bucket, idx);
    cb_mutex_exit(&bucket.mutex);

    return ret;
}

bool conn_listening(ListenConnection *c)
{
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);
    SOCKET sfd = accept(c->getSocketDescriptor(), (struct sockaddr*)&addr,
                        &addrlen);

    if (sfd == INVALID_SOCKET) {
        auto error = GetLastNetworkError();
        if (is_emfile(error)) {
#if defined(WIN32)
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open files.");
#else
            struct rlimit limit = {0};
            getrlimit(RLIMIT_NOFILE, &limit);
            LOG_WARNING(c, "Too many open files. Current limit: %d",
                        limit.rlim_cur);
#endif
            disable_listen();
        } else if (!is_blocking(error)) {
            log_socket_error(EXTENSION_LOG_WARNING, c,
                             "Failed to accept new client: %s");
        }

        return false;
    }

    int port_conns;
    struct listening_port *port_instance;
    int curr_conns = stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        port_instance = get_listening_port_instance(c->getParentPort());
        cb_assert(port_instance);
        port_conns = ++port_instance->curr_conns;
    }

    if (curr_conns >= settings.maxconns || port_conns >= port_instance->maxconns) {
        {
            std::lock_guard<std::mutex> guard(stats_mutex);
            --port_instance->curr_conns;
        }
        stats.rejected_conns++;
        LOG_WARNING(c,
                    "Too many open connections. Current/Limit for port "
                        "%d: %d/%d; total: %d/%d", port_instance->port,
                    port_conns, port_instance->maxconns,
                    curr_conns, settings.maxconns);

        safe_close(sfd);
        return false;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        {
            std::lock_guard<std::mutex> guard(stats_mutex);
            --port_instance->curr_conns;
        }
        LOG_WARNING(c, "Failed to make socket non-blocking. closing it");
        safe_close(sfd);
        return false;
    }

    dispatch_conn_new(sfd, c->getParentPort());

    return false;
}

/**
 * Check if the associated bucket is dying or not. There is two reasons
 * for why a bucket could be dying: It is currently being deleted, or
 * someone initiated a shutdown process.
 */
bool is_bucket_dying(Connection *c)
{
    bool disconnect = memcached_shutdown;
    Bucket &b = all_buckets.at(c->getBucketIndex());
    cb_mutex_enter(&b.mutex);

    if (b.state != BucketState::Ready) {
        disconnect = true;
    }
    cb_mutex_exit(&b.mutex);

    if (disconnect) {
        LOG_NOTICE(c,
                   "%u The connected bucket is being deleted.. disconnecting",
                   c->getId());
        c->initateShutdown();
        return true;
    }

    return false;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    auto *c = reinterpret_cast<Connection *>(arg);
    if (c == nullptr) {
        LOG_WARNING(NULL, "event_handler: connection must be non-NULL");
        return;
    }

    auto *thr = c->getThread();
    if (thr == nullptr) {
        LOG_WARNING(c, "Internal error - connection without a thread found. - "
            "ignored");
        return;
    }

    LOCK_THREAD(thr);
    if (memcached_shutdown) {
        // Someone requested memcached to shut down.
        if (signal_idle_clients(thr, -1, false) == 0) {
            cb_assert(thr != nullptr);
            LOG_NOTICE(NULL, "Stopping worker thread %u", thr->index);
            c->eventBaseLoopbreak();
            return;
        }
    }

    /*
     * Remove the list from the list of pending io's (in case the
     * object was scheduled to run in the dispatcher before the
     * callback for the worker thread is executed.
     */
    thr->pending_io = list_remove(thr->pending_io, c);

    /* sanity */
    cb_assert(fd == c->getSocketDescriptor());

    if ((which & EV_TIMEOUT) == EV_TIMEOUT) {
        auto* mcbp = dynamic_cast<McbpConnection*>(c);

        if (mcbp != nullptr && (c->isAdmin() || c->isDCP() || c->isTAP())) {
            auto* mcbp = dynamic_cast<McbpConnection*>(c);
            if (c->isAdmin()) {
                LOG_NOTICE(c, "%u: Timeout for admin connection. (ignore)",
                           c->getId());
            } else if (c->isDCP()) {
                LOG_NOTICE(c, "%u: Timeout for DCP connection. (ignore)",
                           c->getId());
            } else if (c->isTAP()) {
                LOG_NOTICE(c, "%u: Timeout for TAP connection. (ignore)",
                           c->getId());
            }
            if (!mcbp->reapplyEventmask()) {
                c->initateShutdown();
            }
        } else {
            LOG_NOTICE(c, "%u: Shutting down idle client %s", c->getId(),
                       c->getDescription().c_str());
            c->initateShutdown();
        }
    }

    run_event_loop(c, which);

    if (memcached_shutdown) {
        // Someone requested memcached to shut down. If we don't have
        // any connections bound to this thread we can just shut down
        int connected = signal_idle_clients(thr, -1, true);
        if (connected == 0) {
            LOG_NOTICE(NULL, "Stopping worker thread %u", thr->index);
            event_base_loopbreak(thr->base);
        } else {
            LOG_NOTICE(NULL,
                       "Waiting for %d connected clients on worker thread %u",
                       connected, thr->index);
        }
    }

    UNLOCK_THREAD(thr);
}

/**
 * The listen_event_handler is the callback from libevent when someone is
 * connecting to one of the server sockets. It runs in the context of the
 * listen thread
 */
void listen_event_handler(evutil_socket_t, short which, void *arg) {
    auto *c = reinterpret_cast<ListenConnection *>(arg);
    if (c == nullptr) {
        LOG_WARNING(NULL, "listen_event_handler: internal error, "
            "arg must be non-NULL");
        return;
    }

    if (memcached_shutdown) {
        // Someone requested memcached to shut down. The listen thread should
        // be stopped immediately.
        LOG_NOTICE(NULL, "Stopping listen thread");
        c->eventBaseLoopbreak();
        return;
    }

    run_event_loop(c, which);
}

static void dispatch_event_handler(evutil_socket_t fd, short, void *) {
    char buffer[80];
    ssize_t nr = recv(fd, buffer, sizeof(buffer), 0);

    if (enable_common_ports.load()) {
        enable_common_ports.store(false);
        create_listen_sockets(false);
        LOG_NOTICE(NULL, "Initialization complete. Accepting clients.");
    }

    if (nr != -1 && is_listen_disabled()) {
        bool enable = false;
        {
            std::lock_guard<std::mutex> guard(listen_state.mutex);
            listen_state.count -= nr;
            if (listen_state.count <= 0) {
                enable = true;
                listen_state.disabled = false;
            }
        }
        if (enable) {
            Connection *next;
            for (next = listen_conn; next; next = next->getNext()) {
                auto* connection = dynamic_cast<ListenConnection*>(next);
                if (connection == nullptr) {
                    LOG_WARNING(next, "Internal error: tried to enable listen "
                        "on an incorrect connection object type");
                    continue;
                }

                connection->enable();
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
            LOG_WARNING(NULL, "getsockopt(SO_SNDBUF): %s", strerror(errno));
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
        LOG_DEBUG(NULL,
                  "<%d send buffer was %d, now %d", sfd, old_size, last_good);
    }
}

static SOCKET new_server_socket(struct addrinfo *ai, bool tcp_nodelay) {
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

    const struct linger ling = {0, 0};
    const int flags = 1;
    int error;

#if defined(WIN32)
    const char* ling_ptr = reinterpret_cast<const char*>(&ling);
    const char* flags_ptr = reinterpret_cast<const char*>(&flags);
#else
    const void* ling_ptr = reinterpret_cast<const char*>(&ling);
    const void* flags_ptr = reinterpret_cast<const void*>(&flags);
#endif

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
        error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, flags_ptr,
                           sizeof(flags));
        if (error != 0) {
            LOG_WARNING(NULL, "setsockopt(IPV6_V6ONLY): %s",
                        strerror(errno));
            safe_close(sfd);
            return INVALID_SOCKET;
        }
    }
#endif

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, flags_ptr, sizeof(flags));
    error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, flags_ptr,
                       sizeof(flags));
    if (error != 0) {
        LOG_WARNING(NULL, "setsockopt(SO_KEEPALIVE): %s", strerror(errno));
    }

    error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, ling_ptr, sizeof(ling));
    if (error != 0) {
        LOG_WARNING(NULL, "setsockopt(SO_LINGER): %s", strerror(errno));
    }

    if (tcp_nodelay) {
        error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, flags_ptr,
                           sizeof(flags));
        if (error != 0) {
            LOG_WARNING(NULL, "setsockopt(TCP_NODELAY): %s", strerror(errno));
        }
    }

    return sfd;
}

/**
 * Add a port to the list of interfaces we're listening to.
 *
 * We're supporting binding to the port number "0" to have the operating
 * system pick an available port we may use (and we'll report it back to
 * the user through the portnumber file.). If we have knowledge of the port,
 * update the port descriptor (ip4/ip6), if not go ahead and create a new entry
 *
 * @param interf the interface description used to create the port
 * @param port the port number in use
 * @param family the address family for the port
 */
static void add_listening_port(struct interface *interf, in_port_t port, sa_family_t family) {
    auto *descr = get_listening_port_instance(port);

    if (descr == nullptr) {
        listening_port newport;

        newport.port = port;
        newport.curr_conns = 1;
        newport.maxconns = interf->maxconn;

        if (interf->host != nullptr) {
            newport.host = interf->host;
        }
        if (interf->ssl.key == nullptr || interf->ssl.cert == nullptr) {
            newport.ssl.enabled = false;
        } else {
            newport.ssl.enabled = true;
            newport.ssl.key = interf->ssl.key;
            newport.ssl.cert = interf->ssl.cert;
        }
        newport.backlog = interf->backlog;

        if (family == AF_INET) {
            newport.ipv4 = true;
            newport.ipv6 = false;
        } else if (family == AF_INET6) {
            newport.ipv4 = false;
            newport.ipv6 = true;
        }

        newport.tcp_nodelay = interf->tcp_nodelay;
        newport.management = interf->management;
        newport.protocol = interf->protocol;

        stats.listening_ports.push_back(newport);
    } else {
        if (family == AF_INET) {
            descr->ipv4 = true;
        } else if (family == AF_INET6) {
            descr->ipv6 = true;
        }
        ++descr->curr_conns;
    }
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 */
static int server_socket(struct interface *interf) {
    SOCKET sfd;
    struct addrinfo hints;
    char port_buf[NI_MAXSERV];
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

    struct addrinfo *ai;
    int error = getaddrinfo(host, port_buf, &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        log_errcode_error(EXTENSION_LOG_WARNING, NULL,
                          "getaddrinfo(): %s", error);
#else
        if (error != EAI_SYSTEM) {
            LOG_WARNING(NULL, "getaddrinfo(): %s", gai_strerror(error));
        } else {
            LOG_WARNING(NULL, "getaddrinfo(): %s", strerror(error));
        }
#endif
        return 1;
    }

    for (struct addrinfo* next = ai; next; next = next->ai_next) {
        if ((sfd = new_server_socket(next, interf->tcp_nodelay)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

        in_port_t listenport = 0;
        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) == SOCKET_ERROR) {
            error = GetLastNetworkError();
            if (!is_addrinuse(error)) {
                log_errcode_error(EXTENSION_LOG_WARNING, nullptr,
                                  "Failed to bind to address: %s", error);
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            safe_close(sfd);
            continue;
        }

        success++;
        if (next->ai_addr->sa_family == AF_INET ||
             next->ai_addr->sa_family == AF_INET6) {
            union {
                struct sockaddr_in in;
                struct sockaddr_in6 in6;
            } my_sockaddr;
            socklen_t len = sizeof(my_sockaddr);
            if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                if (next->ai_addr->sa_family == AF_INET) {
                    listenport = ntohs(my_sockaddr.in.sin_port);
                } else {
                    listenport = ntohs(my_sockaddr.in6.sin6_port);
                }
            }
        }

        auto* lconn = conn_new_server(sfd, listenport, next->ai_addr->sa_family,
                                      *interf, main_base);
        if (lconn == nullptr) {
            FATAL_ERROR(EXIT_FAILURE, "Failed to create listening connection");
        }

        lconn->setNext(listen_conn);
        listen_conn = lconn;

        stats.daemon_conns++;
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
        add_listening_port(interf, listenport, next->ai_addr->sa_family);
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int server_sockets(bool management) {
    int ret = 0;

    if (management) {
        LOG_NOTICE(nullptr, "Enable management port(s)");
    } else {
        LOG_NOTICE(nullptr, "Enable user port(s)");
    }

    for (int ii = 0; ii < settings.num_interfaces; ++ii) {
        if (management && settings.interfaces[ii].management) {
            ret |= server_socket(settings.interfaces + ii);
        } else if (!management && !settings.interfaces[ii].management) {
            ret |= server_socket(settings.interfaces + ii);
        }
    }

    if (settings.stdin_listen) {
        dispatch_conn_new(fileno(stdin), 0);
    }

    return ret;
}

static void create_listen_sockets(bool management) {
    if (server_sockets(management)) {
        FATAL_ERROR(EX_OSERR, "Failed to create listening socket");
    }

    if (management && !settings.require_init) {
        // the client is not expecting us to update the port set at
        // later time, so enable all ports immediately
        if (server_sockets(false)) {
            FATAL_ERROR(EX_OSERR, "Failed to create listening socket");
        }
    }

    const char* portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
    if (portnumber_filename != nullptr) {
        std::string temp_portnumber_filename;
        temp_portnumber_filename.assign(portnumber_filename);
        temp_portnumber_filename.append(".lck");

        FILE* portnumber_file = nullptr;
        portnumber_file = fopen(temp_portnumber_filename.c_str(), "a");
        if (portnumber_file == nullptr) {
            FATAL_ERROR(EX_OSERR, "Failed to open \"%s\": %s",
                        temp_portnumber_filename.c_str(), strerror(errno));
        }

        unique_cJSON_ptr array(cJSON_CreateArray());

        for (auto* c = listen_conn; c!= nullptr; c = c->getNext()) {
            auto* lc = dynamic_cast<ListenConnection*>(c);
            if (lc == nullptr) {
                throw std::logic_error("server_sockets: listen_conn contains"
                                           " illegal objects: " +
                                       to_string(c->toJSON(), false));
            }
            cJSON_AddItemToArray(array.get(), lc->getDetails().release());
        }

        unique_cJSON_ptr root(cJSON_CreateObject());
        cJSON_AddItemToObject(root.get(), "ports", array.release());
        fprintf(portnumber_file, "%s\n", to_string(root, true).c_str());
        fclose(portnumber_file);
        LOG_NOTICE(nullptr, "Port numbers available in %s",
                   portnumber_filename);
        if (rename(temp_portnumber_filename.c_str(), portnumber_filename) == -1) {
            FATAL_ERROR(EX_OSERR, "Failed to rename \"%s\" to \"%s\": %s",
                        temp_portnumber_filename.c_str(), portnumber_filename,
                        strerror(errno));
        }
    }
}

#ifdef WIN32
// Unfortunately we don't have signal handlers on windows
static bool install_signal_handlers() {
    return true;
}

static void release_signal_handlers() {
}
#else

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

static struct event* sigusr1_event;
static struct event* sigterm_event;
static struct event* sigint_event;

static bool install_signal_handlers() {
    // SIGUSR1 - Used to dump connection stats
    sigusr1_event = evsignal_new(main_base, SIGUSR1,
                                 dump_connection_stat_signal_handler,
                                 nullptr);
    if (sigusr1_event == nullptr) {
        LOG_WARNING(nullptr, "Failed to allocate SIGUSR1 handler");
        return false;
    }

    if (event_add(sigusr1_event, nullptr) < 0) {
        LOG_WARNING(nullptr, "Failed to install SIGUSR1 handler");
        return false;

    }

    // SIGTERM - Used to shut down memcached cleanly
    sigterm_event = evsignal_new(main_base, SIGTERM, sigterm_handler, NULL);
    if (sigterm_event == NULL) {
        LOG_WARNING(nullptr, "Failed to allocate SIGTERM handler");
        return false;
    }

    if (event_add(sigterm_event, NULL) < 0) {
        LOG_WARNING(nullptr, "Failed to install SIGTERM handler");
        return false;
    }

    // SIGINT - Used to shut down memcached cleanly
    sigint_event = evsignal_new(main_base, SIGINT, sigterm_handler, NULL);
    if (sigint_event == NULL) {
        LOG_WARNING(nullptr, "Failed to allocate SIGINT handler");
        return false;
    }

    if (event_add(sigint_event, NULL) < 0) {
        LOG_WARNING(nullptr, "Failed to install SIGINT handler");
        return false;
    }

    return true;
}

static void release_signal_handlers() {
    event_free(sigusr1_event);
    event_free(sigint_event);
    event_free(sigterm_event);
}
#endif

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
    c->setEngineStorage(engine_data);
}

static void *get_engine_specific(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->getEngineStorage();
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
    uint8_t opcode = PROTOCOL_BINARY_CMD_INVALID;
    Connection *c = (Connection *)cookie;
    auto* mc = dynamic_cast<McbpConnection*>(c);
    if (mc != nullptr && mc->isEwouldblock()) {
        opcode = mc->binary_header.request.opcode;
    }
    return opcode;
}

static bool validate_session_cas(const uint64_t cas) {
    return session_cas.increment_session_counter(cas);
}

static void decrement_session_ctr(void) {
    session_cas.decrement_session_counter();
}

static ENGINE_ERROR_CODE reserve_cookie(const void *cookie) {
    Connection *c = (Connection *)cookie;
    c->incrementRefcount();
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE release_cookie(const void *cookie) {
    if (cookie == nullptr) {
        throw std::invalid_argument("release_cookie: 'cookie' must be non-NULL");
    }
    Connection *c = (Connection *)cookie;
    int notify;
    LIBEVENT_THREAD *thr;

    thr = c->getThread();
    cb_assert(thr);
    LOCK_THREAD(thr);
    c->decrementRefcount();

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

bool cookie_is_admin(const void *cookie) {
    if (settings.disable_admin) {
        return true;
    }
    if (cookie == nullptr) {
        throw std::invalid_argument("cookie_is_admin: 'cookie' must be non-NULL");
    }
    return reinterpret_cast<const Connection *>(cookie)->isAdmin();
}

static void cookie_set_priority(const void* cookie, CONN_PRIORITY priority) {
    if (cookie == nullptr) {
        throw std::invalid_argument("cookie_set_priority: 'cookie' must be non-NULL");
    }

    auto* c = (Connection*)(cookie);
    switch (priority) {
    case CONN_PRIORITY_HIGH:
        c->setPriority(Connection::Priority::High);
        return;
    case CONN_PRIORITY_MED:
        c->setPriority(Connection::Priority::Medium);
        return;
    case CONN_PRIORITY_LOW:
        c->setPriority(Connection::Priority::Low);
        return;
    }

    LOG_WARNING(c,
                "%u: cookie_set_priority: priority (which is %d) is not a "
                    "valid CONN_PRIORITY - closing connection", priority);
    c->initateShutdown();
}

static void count_eviction(const void *cookie, const void *key, int nkey) {
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

            ext_binprot->setup(setup_mcbp_lookup_cmd);
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
        LOG_WARNING(NULL, "You can't unregister a binary command handler!");
        break;
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

void shutdown_server(void) {
    memcached_shutdown = true;
    LOG_NOTICE(NULL, "Received shutdown request");
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
        core_api.realtime = mc_time_convert_to_real_time;
        core_api.abstime = mc_time_convert_to_abs_time;
        core_api.get_current_time = mc_time_get_current_time;
        core_api.parse_config = parse_config;
        core_api.shutdown = shutdown_server;
        core_api.get_config = get_config;

        server_cookie_api.store_engine_specific = store_engine_specific;
        server_cookie_api.get_engine_specific = get_engine_specific;
        server_cookie_api.is_datatype_supported = is_datatype_supported;
        server_cookie_api.is_mutation_extras_supported = is_mutation_extras_supported;
        server_cookie_api.get_opcode_if_ewouldblock_set = get_opcode_if_ewouldblock_set;
        server_cookie_api.validate_session_cas = validate_session_cas;
        server_cookie_api.decrement_session_ctr = decrement_session_ctr;
        server_cookie_api.notify_io_complete = notify_io_complete;
        server_cookie_api.reserve = reserve_cookie;
        server_cookie_api.release = release_cookie;
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

/* BUCKET FUNCTIONS */
void CreateBucketThread::create() {
    LOG_NOTICE(&connection, "%u Create bucket [%s]",
               connection.getId(), name.c_str());

    if (!BucketValidator::validateBucketName(name, error)) {
        LOG_WARNING(&connection,
                    "%u Create bucket [%s] failed - Invalid bucket name",
                    connection.getId(), name.c_str());
        result = ENGINE_EINVAL;
        return;
    }

    if (!BucketValidator::validateBucketType(type, error)) {
        LOG_WARNING(&connection,
                    "%u Create bucket [%s] failed - Invalid bucket type",
                    connection.getId(), name.c_str());
        result = ENGINE_EINVAL;
        return;
    }

    int ii;
    int first_free = -1;
    bool found = false;

    cb_mutex_enter(&buckets_lock);
    for (ii = 0; ii < settings.max_buckets && !found; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (first_free == -1 && all_buckets[ii].state == BucketState::None) {
            first_free = ii;
        }
        if (name == all_buckets[ii].name) {
            found = true;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }

    if (found) {
        result = ENGINE_KEY_EEXISTS;
        LOG_WARNING(&connection, "%u Create bucket [%s] failed - Already exists",
                   connection.getId(), name.c_str());
    } else if (first_free == -1) {
        result = ENGINE_E2BIG;
        LOG_WARNING(&connection,
                    "%u Create bucket [%s] failed - Too many buckets",
                    connection.getId(), name.c_str());
    } else {
        result = ENGINE_SUCCESS;
        ii = first_free;
        /*
         * split the creation of the bucket in two... so
         * we can release the global lock..
         */
        cb_mutex_enter(&all_buckets[ii].mutex);
        all_buckets[ii].state = BucketState::Creating;
        all_buckets[ii].type = type;
        strcpy(all_buckets[ii].name, name.c_str());
        try {
            all_buckets[ii].topkeys = new TopKeys(settings.topkeys_size);
        } catch (const std::bad_alloc &) {
            result = ENGINE_ENOMEM;
            LOG_WARNING(&connection,
                        "%u Create bucket [%s] failed - out of memory",
                        connection.getId(), name.c_str());        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }
    cb_mutex_exit(&buckets_lock);

    if (result != ENGINE_SUCCESS) {
        return;
    }

    auto &bucket = all_buckets[ii];

    /* People aren't allowed to use the engine in this state,
     * so we can do stuff without locking..
     */
    if (new_engine_instance(type, get_server_api,
                            (ENGINE_HANDLE**)&bucket.engine,
                            settings.extensions.logger)) {
        auto* engine = bucket.engine;
        cb_mutex_enter(&bucket.mutex);
        bucket.state = BucketState::Initializing;
        cb_mutex_exit(&bucket.mutex);

        try {
            result = engine->initialize(v1_handle_2_handle(engine),
                                        config.c_str());
        } catch (std::runtime_error& e) {
            LOG_WARNING(&connection, "%u - Failed to create bucket [%s]: %s",
                        connection.getId(), name.c_str(), e.what());
            result = ENGINE_FAILED;
        } catch (std::bad_alloc& e) {
            LOG_WARNING(&connection, "%u - Failed to create bucket [%s]: %s",
                        connection.getId(), name.c_str(), e.what());
            result = ENGINE_ENOMEM;
        }

        if (result == ENGINE_SUCCESS) {
            cb_mutex_enter(&bucket.mutex);
            bucket.state = BucketState::Ready;
            cb_mutex_exit(&bucket.mutex);
            LOG_NOTICE(&connection,
                        "%u - Bucket [%s] created successfully",
                        connection.getId(), name.c_str());
        } else {
            cb_mutex_enter(&bucket.mutex);
            bucket.state = BucketState::Destroying;
            cb_mutex_exit(&bucket.mutex);
            engine->destroy(v1_handle_2_handle(engine), false);

            cb_mutex_enter(&bucket.mutex);
            bucket.state = BucketState::None;
            bucket.name[0] = '\0';
            bucket.engine = nullptr;
            delete bucket.topkeys;
            bucket.topkeys = nullptr;
            cb_mutex_exit(&bucket.mutex);

            result = ENGINE_NOT_STORED;
        }
    } else {
        cb_mutex_enter(&bucket.mutex);
        bucket.state = BucketState::None;
        bucket.name[0] = '\0';
        bucket.engine = nullptr;
        delete bucket.topkeys;
        bucket.topkeys = nullptr;
        cb_mutex_exit(&bucket.mutex);

        LOG_WARNING(&connection,
                    "%u - Failed to create bucket [%s]: failed to create a "
                        "new engine instance",
                    connection.getId(), name.c_str());
        result = ENGINE_FAILED;
    }
}

void CreateBucketThread::run()
{
    setRunning();
    // Perform the task without having any locks. The task should be
    // scheduled in a pending state so the executor won't try to touch
    // the object until we're telling it that it is runnable
    create();
    std::lock_guard<std::mutex> guard(task->getMutex());
    task->makeRunnable();
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
            signal_idle_clients(me, ii, false);
        }
    }
}

void DestroyBucketThread::destroy() {
    ENGINE_ERROR_CODE ret = ENGINE_KEY_ENOENT;
    cb_mutex_enter(&buckets_lock);

    /*
     * The destroy function will have access to a connection if the
     * McbpDestroyBucketTask originated from delete_bucket_executor().
     * However if we are in the process of shuting down and the
     * McbpDestroyBucketTask originated from main() then connection
     * will be set to nullptr.
     */
    const auto* connection_id = (connection == nullptr) ? "<none>"
                                : std::to_string(connection->getId()).c_str();

    int idx = 0;
    for (int ii = 0; ii < settings.max_buckets; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (name == all_buckets[ii].name) {
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
    cb_mutex_exit(&buckets_lock);

    if (ret != ENGINE_SUCCESS) {
        auto code = engine_error_2_mcbp_protocol_error(ret);
        LOG_NOTICE(connection, "%s Delete bucket [%s]: %s",
                   connection_id, name.c_str(),
                   memcached_status_2_text(code));
        result = ret;
        return;
    }

    perform_callbacks(ON_DELETE_BUCKET, nullptr, &all_buckets[idx]);

    LOG_NOTICE(connection, "%s Delete bucket [%s]. Wait for clients to disconnect",
               connection_id, name.c_str());

    /* If this thread is connected to the requested bucket... release it */
    if (connection != nullptr && idx == connection->getBucketIndex()) {
        disassociate_bucket(connection);
    }

    /* Let all of the worker threads start invalidating connections */
    threads_initiate_bucket_deletion();

    /* Wait until all users disconnected... */
    cb_mutex_enter(&all_buckets[idx].mutex);
    while (all_buckets[idx].clients > 0) {
        LOG_NOTICE(connection,
                   "%u Delete bucket [%s]. Still waiting: %u clients connected",
                   connection_id, name.c_str(), all_buckets[idx].clients);
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

    /*
     * We cannot call assert_no_assocations(idx) because it iterates
     * over all connections and calls c->getBucketIndex().  The problem
     * is that a worker thread can call associate_initial_bucket() or
     * associate_bucket() at the same time.  This could lead to a call
     * to c->setBucketIndex(0) (the "no bucket"), which although safe,
     * raises a threadsanitizer warning.

     * Note, if associate_bucket() attempts to associate a connection
     * with a bucket that has been destroyed, or is in the process of
     * being destroyed, the association will fail because
     * BucketState != Ready.  See associate_bucket() for more details.
     */

    LOG_NOTICE(connection, "%s Delete bucket [%s]. Shut down the bucket",
               connection_id, name.c_str());

    all_buckets[idx].engine->destroy
        (v1_handle_2_handle(all_buckets[idx].engine), force);

    LOG_NOTICE(connection, "%s Delete bucket [%s]. Clean up allocated resources ",
               connection_id, name.c_str());

    /* Clean up the stats... */
    delete[]all_buckets[idx].stats;
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

    LOG_NOTICE(connection, "%s Delete bucket [%s] complete",
               connection_id, name.c_str());
    result = ENGINE_SUCCESS;
}

void DestroyBucketThread::run() {
    setRunning();
    destroy();
    std::lock_guard<std::mutex> guard(task->getMutex());
    task->makeRunnable();
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
                                  &handle,
                                  settings.extensions.logger));

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
            delete bucket.topkeys;
        }

        delete []bucket.stats;
    }
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
        LOG_WARNING(NULL, "Failed to open library \"%s\": %s\n",
                    soname, error_msg);
        free(error_msg);
        return false;
    }

    symbol = cb_dlsym(handle, "memcached_extensions_initialize", &error_msg);
    if (symbol == NULL) {
        LOG_WARNING(NULL,
                    "Could not find symbol \"memcached_extensions_"
                        "initialize\" in %s: %s\n",
                    soname, error_msg);
        free(error_msg);
        return false;
    }
    funky.voidptr = symbol;

    error = (*funky.initialize)(config, get_server_api);
    if (error != EXTENSION_SUCCESS) {
        LOG_WARNING(NULL,
                    "Failed to initalize extensions from %s. Error code: %d",
                    soname, error);
        cb_dlclose(handle);
        return false;
    }

    if (settings.verbose > 0) {
        LOG_INFO(NULL, "Loaded extensions from: %s", soname);
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
            FATAL_ERROR(EXIT_FAILURE,
                        "Failed to open parent process: %s",
                        cb_strerror(GetLastError()).c_str());
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
                                   "mc:parent_mon") != 0) {
            FATAL_ERROR(EXIT_FAILURE,
                        "Failed to open parent process: %s",
                        cb_strerror(GetLastError()).c_str());
        }
    }
}

static void set_max_filehandles(void) {
    struct rlimit rlim;

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        FATAL_ERROR(EX_OSERR, "Failed to getrlimit number of files");
    } else {
        const rlim_t maxfiles = settings.maxconns + (3 * (settings.num_threads + 2));
        rlim_t syslimit = rlim.rlim_cur;
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
                "system\nresource restrictions. Increase the number of file "
                "descriptors allowed\nto the memcached user process.\n"
                "The maximum number of connections is set to %d.\n";
            req = settings.maxconns;
            settings.maxconns = syslimit - (3 * (settings.num_threads + 2));
            if (settings.maxconns < 0) {
                FATAL_ERROR(EX_OSERR, "Failed to set rlimit to %d for "
                            "open files. Currently requesting %d more connections"
                            "than the syslimit of %d.  Try starting as root or "
                            "requesting smaller maxconns value.", rlim.rlim_max,
                            abs(settings.maxconns), syslimit);
            }
            LOG_WARNING(NULL, fmt, req, settings.maxconns);
        }
    }
}

#endif

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
            FATAL_ERROR(EXIT_FAILURE, "Unable to load extension %s "
                        "using the config %s",
                        settings.pending_extensions[ii].soname,
                        settings.pending_extensions[ii].config);
        }
    }
}

static std::terminate_handler default_terminate_handler;

// Replacement terminate_handler which prints a backtrace of the current stack
// before chaining to the default handler.
static void backtrace_terminate_handler() {
    char buffer[1024];

    if (print_backtrace_to_buffer("    ", buffer, sizeof(buffer))) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "*** Fatal error encountered during "
                                            "exception handling ***\n"
                                            "Call stack:\n%s",
                                        buffer);
    } else {
        fprintf(stderr, "*** Fatal error encountered during exception handling ***\n");
        fprintf(stderr, "Call stack:\n");
        print_backtrace_to_file(stderr);
        fflush(stderr);

        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "*** Fatal error encountered during "
                                            "exception handling ***\n"
                                            "Call stack exceeds 1k");
    }

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    std::abort();
}

/**
 * The log function used from SASL
 *
 * Try to remap the log levels to our own levels and put in the log
 * depending on the severity.
 */
static int sasl_log_callback(void*, int level, const char *message) {
    switch (level) {
    case CBSASL_LOG_ERR:
        LOG_WARNING(nullptr, "%s", message);
        break;
    case CBSASL_LOG_NOTE:
        LOG_NOTICE(nullptr, "%s", message);
        break;
    case CBSASL_LOG_FAIL:
    case CBSASL_LOG_DEBUG:
        LOG_DEBUG(nullptr, "%s", message);
        break;
    default:
        /* Ignore */
        ;
    }

    return CBSASL_OK;
}

static int sasl_getopt_callback(void*, const char*,
                                const char* option,
                                const char** result,
                                unsigned* len) {
    if (option == nullptr || result == nullptr || len == nullptr) {
        return CBSASL_BADPARAM;
    }

    std::string key(option);

    if (key == "hmac iteration count") {
        // Speed up the test suite by reducing the SHA1 hmac calculations
        // from 4k to 10
        if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
            *result = "10";
            *len = 2;
            return CBSASL_OK;
        }
    } else if (key == "sasl mechanisms") {
        if (settings.sasl_mechanisms != nullptr) {
            *result = settings.sasl_mechanisms;
            *len = strlen(settings.sasl_mechanisms);
            return CBSASL_OK;
        }
    }

    return CBSASL_FAIL;
}

static void initialize_sasl() {
    cbsasl_callback_t sasl_callbacks[3];
    int ii = 0;

    sasl_callbacks[ii].id = CBSASL_CB_LOG;
    sasl_callbacks[ii].proc = (int (*)(void))&sasl_log_callback;
    sasl_callbacks[ii].context = nullptr;
    sasl_callbacks[++ii].id = CBSASL_CB_GETOPT;
    sasl_callbacks[ii].proc = (int (*)(void))&sasl_getopt_callback;
    sasl_callbacks[ii].context = nullptr;
    sasl_callbacks[++ii].id = CBSASL_CB_LIST_END;
    sasl_callbacks[ii].proc = nullptr;
    sasl_callbacks[ii].context = nullptr;

    if (cbsasl_server_init(sasl_callbacks, "memcached") != CBSASL_OK) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to initialize SASL server");
    }
}

extern "C" int memcached_main(int argc, char **argv) {
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

    // Interpose our own C++ terminate handler to print backtrace upon failures
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);

    initialize_openssl();

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    init_alloc_hooks();

    /* init settings */
    settings_init();

    initialize_mbcp_lookup_map();

    if (memcached_initialize_stderr_logger(get_server_api) != EXTENSION_SUCCESS) {
        fprintf(stderr, "Failed to initialize log system\n");
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
    LOG_NOTICE(NULL, "Couchbase version %s starting.", get_server_version());

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
    if (settings.audit_file && configure_auditdaemon(settings.audit_file, NULL)
        != AUDIT_SUCCESS) {
        FATAL_ERROR(EXIT_FAILURE,
                    "FATAL: Failed to initialize audit "
                    "daemon with configuation file: %s",
                    settings.audit_file);
    }
    if (start_auditdaemon(&audit_extension_data) != AUDIT_SUCCESS) {
        FATAL_ERROR(EXIT_FAILURE, "FATAL: Failed to start audit daemon");
    }

    /* Initialize RBAC data */
    if (load_rbac_from_file(settings.rbac_file) != 0) {
        FATAL_ERROR(EXIT_FAILURE,
                    "FATAL: Failed to load RBAC configuration: %s",
                    (settings.rbac_file) ? settings.rbac_file :
                    "no file specified");
    }

    /* inform interested parties of initial verbosity level */
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);

    set_max_filehandles();

    /* Aggregate the maximum number of connections */
    calculate_maxconns();

    {
        char *errmsg;
        if (!initialize_engine_map(&errmsg, settings.extensions.logger)) {
            FATAL_ERROR(EXIT_FAILURE, "Unable to initialize engine "
                        "map: %s", errmsg);
        }
    }

    /* Initialize bucket engine */
    initialize_buckets();

    initialize_sasl();

    /* initialize main thread libevent instance */
    main_base = event_base_new();

    /* Initialize signal handlers (requires libevent). */
    if (!install_signal_handlers()) {
        FATAL_ERROR(EXIT_FAILURE, "Unable to install signal handlers");
    }

    /* initialize other stuff */
    stats_init();

#ifndef WIN32
    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        FATAL_ERROR(EX_OSERR, "Failed to ignore SIGPIPE; sigaction");
    }
#endif

    /* start up worker threads if MT mode */
    thread_init(settings.num_threads, main_base, dispatch_event_handler);

    executorPool.reset(new ExecutorPool(size_t(settings.num_threads)));

    /* Initialise memcached time keeping */
    mc_time_init(main_base);

    /* create the listening socket, bind it, and init */
    create_listen_sockets(true);

    /* Drop privileges no longer needed */
    drop_privileges();

    /* Optional parent monitor */
    setup_parent_monitor();

    if (!memcached_shutdown) {
        /* enter the event loop */
        if (settings.require_init) {
            LOG_NOTICE(nullptr,
                       "Accepting management clients to initialize buckets");
        } else {
            LOG_NOTICE(nullptr, "Initialization complete. Accepting clients.");
        }
        service_online = true;
        event_base_loop(main_base, 0);
        service_online = false;
    }

    LOG_NOTICE(NULL, "Initiating graceful shutdown.");

    cb_mutex_enter(&buckets_lock);
    /*
     * Start at one (not zero) because zero is reserved for "no bucket".
     * The "no bucket" has a state of BucketState::Ready but no name.
     */
    for (int ii = 1; ii < settings.max_buckets; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (all_buckets[ii].state == BucketState::Ready) {
            LOG_NOTICE(nullptr, "Scheduling McbpDestroyBucketTask for bucket %s",
                       all_buckets[ii].name);
            std::shared_ptr<Task> task = std::make_shared<McbpDestroyBucketTask>(
                                             all_buckets[ii].name, false, nullptr);
            std::lock_guard<std::mutex> guard(task->getMutex());
            reinterpret_cast<McbpDestroyBucketTask*>(task.get())->start();
            executorPool->schedule(task, false);
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }
    cb_mutex_exit(&buckets_lock);

    LOG_NOTICE(NULL, "Shutting down audit daemon");
    /* Close down the audit daemon cleanly */
    shutdown_auditdaemon();

    LOG_NOTICE(NULL, "Shutting down client worker threads");
    threads_shutdown();

    LOG_NOTICE(NULL, "Releasing client resources");
    close_all_connections();

    LOG_NOTICE(NULL, "Releasing bucket resources");
    cleanup_buckets();

    LOG_NOTICE(NULL, "Releasing thread resources");
    threads_cleanup();

    LOG_NOTICE(nullptr, "Shutting down executor pool");
    delete executorPool.release();

    LOG_NOTICE(NULL, "Releasing signal handlers");
    release_signal_handlers();

    LOG_NOTICE(NULL, "Shutting down SASL server");
    cbsasl_server_term();

    LOG_NOTICE(NULL, "Releasing connection objects");
    destroy_connections();

    LOG_NOTICE(NULL, "Shutting down engine map");
    shutdown_engine_map();

    LOG_NOTICE(NULL, "Removing breakpad");
    destroy_breakpad();

    LOG_NOTICE(NULL, "Releasing callbacks");
    free_callbacks();

    LOG_NOTICE(NULL, "Releasing settings");
    free_settings(&settings);

    LOG_NOTICE(NULL, "Shutting down OpenSSL");
    shutdown_openssl();

    LOG_NOTICE(NULL, "Shutting down libevent");
    event_base_free(main_base);

    LOG_NOTICE(NULL, "Shutdown complete.");
    return EXIT_SUCCESS;
}
