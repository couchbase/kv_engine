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
#include "memcached.h"
#include "alloc_hooks.h"
#include "buckets.h"
#include "cmdline.h"
#include "config_parse.h"
#include "connections.h"
#include "debug_helpers.h"
#include "doc_pre_expiry.h"
#include "enginemap.h"
#include "executor.h"
#include "executorpool.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "ioctl.h"
#include "libevent_locking.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "mcbp_executors.h"
#include "mcbp_topkeys.h"
#include "mcbp_validators.h"
#include "mcbpdestroybuckettask.h"
#include "memcached/audit_interface.h"
#include "memcached_openssl.h"
#include "parent_monitor.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "runtime.h"
#include "server_socket.h"
#include "session_cas.h"
#include "settings.h"
#include "stats.h"
#include "subdocument.h"
#include "timings.h"
#include "topkeys.h"
#include "tracing.h"
#include "utilities/engine_loader.h"
#include "utilities/protocol2text.h"
#include "utilities/terminate_handler.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <cbsasl/logging.h>
#include <cbsasl/mechanism.h>
#include <engines/default_engine.h>
#include <mcbp/mcbp.h>
#include <memcached/audit_interface.h>
#include <memcached/rbac.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_document_iface.h>
#include <memcached/server_log_iface.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <platform/interrupt.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/sysinfo.h>
#include <snappy-c.h>
#include <utilities/breakpad.h>
#include <gsl/gsl>

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <algorithm>
#include <memory>

#if HAVE_LIBNUMA
#include <numa.h>
#endif
#include <limits.h>
#include <math.h>
#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#include <sysexits.h>
#endif
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <vector>

/**
 * All of the buckets in couchbase is stored in this array.
 */
static std::mutex buckets_lock;
std::array<Bucket, COUCHBASE_MAX_NUM_BUCKETS + 1> all_buckets;

const char* getBucketName(const Connection* c) {
    return all_buckets[c->getBucketIndex()].name;
}

void bucketsForEach(std::function<bool(Bucket&, void*)> fn, void *arg) {
    std::lock_guard<std::mutex> all_bucket_lock(buckets_lock);
    for (Bucket& bucket : all_buckets) {
        bool do_break = false;
        std::lock_guard<std::mutex> guard(bucket.mutex);
        if (bucket.state == BucketState::Ready) {
            if (!fn(bucket, arg)) {
                do_break = true;
            }
        }
        if (do_break) {
            break;
        }
    }
}

std::atomic<bool> memcached_shutdown;
std::atomic<bool> service_online;

std::unique_ptr<ExecutorPool> executorPool;

/* Mutex for global stats */
std::mutex stats_mutex;

/*
 * forward declarations
 */
static void register_callback(EngineIface* eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void* cb_data);

static void create_listen_sockets(bool management);

/* stats */
static void stats_init(void);

/* defaults */
static void settings_init(void);

/** exported globals **/
struct stats stats;

/** file scope variables **/
std::vector<std::unique_ptr<ServerSocket>> listen_conn;
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

void disassociate_bucket(Connection& connection) {
    Bucket& b = all_buckets.at(connection.getBucketIndex());
    std::lock_guard<std::mutex> guard(b.mutex);
    b.clients--;

    connection.setBucketIndex(0);

    if (b.clients == 0 && b.state == BucketState::Destroying) {
        b.cond.notify_one();
    }
}

bool associate_bucket(Connection& connection, const char* name) {
    bool found = false;

    /* leave the current bucket */
    disassociate_bucket(connection);

    /* Try to associate with the named bucket */
    /* @todo add auth checks!!! */
    for (size_t ii = 1; ii < all_buckets.size() && !found; ++ii) {
        Bucket &b = all_buckets.at(ii);
        std::lock_guard<std::mutex> guard(b.mutex);
        if (b.state == BucketState::Ready && strcmp(b.name, name) == 0) {
            b.clients++;
            connection.setBucketIndex(gsl::narrow<int>(ii));
            audit_bucket_selection(connection);
            found = true;
        }
    }

    if (!found) {
        /* Bucket not found, connect to the "no-bucket" */
        Bucket &b = all_buckets.at(0);
        {
            std::lock_guard<std::mutex> guard(b.mutex);
            b.clients++;
        }
        connection.setBucketIndex(0);
    }

    return found;
}

void associate_initial_bucket(Connection& connection) {
    Bucket &b = all_buckets.at(0);
    {
        std::lock_guard<std::mutex> guard(b.mutex);
        b.clients++;
    }

    connection.setBucketIndex(0);

    if (is_default_bucket_enabled()) {
        associate_bucket(connection, "default");
    }
}

static void populate_log_level(void*) {
    // Lock the entire buckets array so that buckets can't be modified while
    // we notify them (blocking bucket creation/deletion)
    const auto val = settings.getLogLevel();

    std::lock_guard<std::mutex> all_bucket_lock(buckets_lock);
    for (auto& bucket : all_buckets) {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        if (bucket.state == BucketState::Ready) {
            bucket.getEngine()->set_log_level(val);
        }
    }
}

/* Perform all callbacks of a given type for the given connection. */
void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *void_cookie)
{
    cb_thread_t tid;

    switch (type) {
        /*
         * The following events operates on a connection which is passed in
         * as the cookie.
         */
    case ON_DISCONNECT: {
        const auto * cookie = reinterpret_cast<const Cookie *>(void_cookie);
        if (cookie == nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie is nullptr");
        }
        const auto bucket_idx = cookie->getConnection().getBucketIndex();
        if (bucket_idx == -1) {
            throw std::logic_error(
                    "perform_callbacks: connection (which is " +
                    std::to_string(cookie->getConnection().getId()) +
                    ") cannot be "
                    "disconnected as it is not associated with a bucket");
        }

        for (auto& handler : all_buckets[bucket_idx].engine_event_handlers[type]) {
            handler.cb(void_cookie, ON_DISCONNECT, data, handler.cb_data);
        }
        break;
    }
    case ON_LOG_LEVEL:
        if (void_cookie != nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie "
                "(which is " +
                std::to_string(reinterpret_cast<uintptr_t>(void_cookie)) +
                ") should be NULL for ON_LOG_LEVEL");
        }
        for (auto& handler : engine_event_handlers[type]) {
            handler.cb(void_cookie, ON_LOG_LEVEL, data, handler.cb_data);
        }

        if (service_online) {
            if (cb_create_thread(&tid, populate_log_level, nullptr, 1) == -1) {
                LOG_WARNING(
                        "Failed to create thread to notify engines about "
                        "changing log level");
            }
        }
        break;

    case ON_DELETE_BUCKET: {
        /** cookie is the bucket entry */
        auto* bucket = reinterpret_cast<const Bucket*>(void_cookie);
        for (auto& handler : bucket->engine_event_handlers[type]) {
            handler.cb(void_cookie, ON_DELETE_BUCKET, data, handler.cb_data);
        }
        break;
    }

    default:
        throw std::invalid_argument("perform_callbacks: type "
                "(which is " + std::to_string(type) +
                "is not a valid ENGINE_EVENT_TYPE");
    }
}

static void register_callback(EngineIface* eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void* cb_data) {
    size_t idx;
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
        for (idx = 0; idx < all_buckets.size(); ++idx) {
            if ((void*)eh == (void*)all_buckets[idx].getEngine()) {
                break;
            }
        }
        if (idx == all_buckets.size()) {
            throw std::invalid_argument("register_callback: eh (which is " +
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
    for (size_t idx = 0; idx < all_buckets.size(); ++idx) {
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

struct thread_stats* get_thread_stats(Connection* c) {
    cb_assert(c->getThread()->index < (settings.getNumWorkerThreads() + 1));
    auto& independent_stats = all_buckets[c->getBucketIndex()].stats;
    return &independent_stats.at(c->getThread()->index);
}

void stats_reset(Cookie& cookie) {
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        set_stats_reset_time();
    }
    stats.total_conns.reset();
    stats.rejected_conns.reset();
    threadlocal_stats_reset(cookie.getConnection().getBucket().stats);
    bucket_reset_stats(cookie);
}

static size_t get_number_of_worker_threads() {
    size_t ret;
    char *override = getenv("MEMCACHED_NUM_CPUS");
    if (override == NULL) {
        // No override specified; determine worker thread count based
        // on the CPU count:
        //     <5 cores: create 4 workers.
        //    >5+ cores: create #CPUs * 7/8.
        ret = Couchbase::get_available_cpu_count();

        if (ret > 4) {
            ret = (ret * 7) / 8;
        }
        if (ret < 4) {
            ret = 4;
        }
    } else {
        ret = std::stoull(override);
        if (ret == 0) {
            ret = 4;
        }
    }
    return ret;
}

static void breakpad_changed_listener(const std::string&, Settings &s) {
    cb::breakpad::initialize(s.getBreakpadSettings());
}

static void ssl_minimum_protocol_changed_listener(const std::string&, Settings &s) {
    set_ssl_protocol_mask(s.getSslMinimumProtocol());
}

static void ssl_cipher_list_changed_listener(const std::string&, Settings &s) {
    set_ssl_cipher_list(s.getSslCipherList());
}

static void verbosity_changed_listener(const std::string&, Settings &s) {
    auto logger = cb::logger::get();
    if (logger) {
        logger->set_level(settings.getLogLevel());
    }

    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
}

static void scramsha_fallback_salt_changed_listener(const std::string&,
                                                    Settings& s) {
    cb::sasl::server::set_scramsha_fallback_salt(s.getScramshaFallbackSalt());
}

static void opcode_attributes_override_changed_listener(const std::string&,
                                                        Settings& s) {
    unique_cJSON_ptr json(cJSON_Parse(s.getOpcodeAttributesOverride().c_str()));
    if (json) {
        cb::mcbp::sla::reconfigure(settings.getRoot(), *json);
    } else {
        cb::mcbp::sla::reconfigure(settings.getRoot());
    }
    LOG_INFO("SLA configuration changed to: {}",
             to_string(cb::mcbp::sla::to_json(), false));
}

static void interfaces_changed_listener(const std::string&, Settings &s) {
    for (const auto& ifc : s.getInterfaces()) {
        auto* port = get_listening_port_instance(ifc.port);
        if (port != nullptr) {
            if (port->maxconns != ifc.maxconn) {
                port->maxconns = ifc.maxconn;
            }

            if (port->backlog != ifc.backlog) {
                port->backlog = ifc.backlog;
            }

            if (port->tcp_nodelay != ifc.tcp_nodelay) {
                port->tcp_nodelay = ifc.tcp_nodelay;
            }
        }
    }
    s.calculateMaxconns();
}

#ifdef HAVE_LIBNUMA
/** Configure the NUMA policy for memcached. By default will attempt to set to
 *  interleaved polocy, unless the env var MEMCACHED_NUMA_MEM_POLICY is set to
 *  'disable'.
 *  @return A log message describing what action was taken.
 *
 */
static std::string configure_numa_policy() {
    if (numa_available() != 0) {
        return "Not available - not setting mem policy.";
    }

    // Attempt to set the default NUMA memory policy to interleaved,
    // unless overridden by our env var.
    const char* mem_policy_env = getenv("MEMCACHED_NUMA_MEM_POLICY");
    if (mem_policy_env != NULL && strcmp("disable", mem_policy_env) == 0) {
        return std::string("NOT setting memory allocation policy - disabled "
                "via MEMCACHED_NUMA_MEM_POLICY='") + mem_policy_env + "'";
    } else {
        errno = 0;
        numa_set_interleave_mask(numa_all_nodes_ptr);
        if (errno == 0) {
            return "Set memory allocation policy to 'interleave'";
        } else {
            return std::string("NOT setting memory allocation policy to "
                    "'interleave' - request failed: ") + cb_strerror();
        }
    }
}
#endif  // HAVE_LIBNUMA

static void settings_init(void) {
    // Set up the listener functions
    settings.addChangeListener("breakpad",
                               breakpad_changed_listener);
    settings.addChangeListener("ssl_minimum_protocol",
                               ssl_minimum_protocol_changed_listener);
    settings.addChangeListener("ssl_cipher_list",
                               ssl_cipher_list_changed_listener);
    settings.addChangeListener("verbosity", verbosity_changed_listener);
    settings.addChangeListener("interfaces", interfaces_changed_listener);
    settings.addChangeListener("scramsha_fallback_salt",
                               scramsha_fallback_salt_changed_listener);
    NetworkInterface default_interface;
    settings.addInterface(default_interface);

    settings.setBioDrainBufferSize(8192);

    settings.setVerbose(0);
    settings.setConnectionIdleTime(0); // Connection idle time disabled
    settings.setNumWorkerThreads(get_number_of_worker_threads());
    settings.setDatatypeJsonEnabled(true);
    settings.setDatatypeSnappyEnabled(true);
    settings.setRequestsPerEventNotification(50, EventPriority::High);
    settings.setRequestsPerEventNotification(5, EventPriority::Medium);
    settings.setRequestsPerEventNotification(1, EventPriority::Low);
    settings.setRequestsPerEventNotification(20, EventPriority::Default);

    /*
     * The max object size is 20MB. Let's allow packets up to 30MB to
     * be handled "properly" by returing E2BIG, but packets bigger
     * than that will cause the server to disconnect the client
     */
    settings.setMaxPacketSize(30 * 1024 * 1024);

    settings.setDedupeNmvbMaps(false);

    char *tmp = getenv("MEMCACHED_TOP_KEYS");
    settings.setTopkeysSize(20);
    if (tmp != NULL) {
        int count;
        if (safe_strtol(tmp, count)) {
            settings.setTopkeysSize(count);
        }
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
        if (env == nullptr) {
            settings.setSslCipherList("HIGH");
        } else {
            settings.setSslCipherList(env);
        }
    }

    settings.setSslMinimumProtocol("tlsv1");
    if (getenv("COUCHBASE_ENABLE_PRIVILEGE_DEBUG") != nullptr) {
        settings.setPrivilegeDebug(true);
    }

    settings.setTopkeysEnabled(true);
}

/**
 * The config file may have altered some of the default values we're
 * caching in other variables. This is the place where we'd propagate
 * such changes.
 *
 * This is also the place to initialize any additional files needed by
 * Memcached.
 */
static void update_settings_from_config(void)
{
    std::string root(DESTINATION_ROOT);

    if (!settings.getRoot().empty()) {
        root = settings.getRoot().c_str();
    }

    if (settings.getErrorMapsDir().empty()) {
        // Set the error map dir.
        std::string error_maps_dir(root + "/etc/couchbase/kv/error_maps");
        cb::io::sanitizePath(error_maps_dir);
        if (cb::io::isDirectory(error_maps_dir)) {
            settings.setErrorMapsDir(error_maps_dir);
        }
    }

    try {
        cb::mcbp::sla::reconfigure(root);
    } catch (const std::exception& e) {
        FATAL_ERROR(EXIT_FAILURE, e.what());
    }

    settings.addChangeListener("opcode_attributes_override",
                               opcode_attributes_override_changed_listener);
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

void disable_listen() {
    {
        std::lock_guard<std::mutex> guard(listen_state.mutex);
        listen_state.disabled = true;
        listen_state.count = 10;
        ++listen_state.num_disable;
    }

    for (auto& connection : listen_conn) {
        connection->disable();
    }
}

void safe_close(SOCKET sfd) {
    if (sfd != INVALID_SOCKET) {
        int rval;

        do {
            rval = evutil_closesocket(sfd);
        } while (rval == SOCKET_ERROR &&
                 cb::net::is_interrupted(cb::net::get_socket_error()));

        if (rval == SOCKET_ERROR) {
            std::string error = cb_strerror();
            LOG_WARNING("Failed to close socket {} ({})!!", (int)sfd, error);
        } else {
            stats.curr_conns.fetch_sub(1, std::memory_order_relaxed);
            if (is_listen_disabled()) {
                notify_dispatcher();
            }
        }
    }
}

static nlohmann::json get_bucket_details_UNLOCKED(const Bucket& bucket,
                                                  size_t idx) {
    if (bucket.state == BucketState::None) {
        return nlohmann::json();
    }

    nlohmann::json json;
    json["index"] = idx;
    switch (bucket.state.load()) {
    case BucketState::None:
        json["state"] = "none";
        break;
    case BucketState::Creating:
        json["state"] = "creating";
        break;
    case BucketState::Initializing:
        json["state"] = "initializing";
        break;
    case BucketState::Ready:
        json["state"] = "ready";
        break;
    case BucketState::Stopping:
        json["state"] = "stopping";
        break;
    case BucketState::Destroying:
        json["state"] = "destroying";
        break;
    }

    json["clients"] = bucket.clients;
    json["name"] = bucket.name;

    switch (bucket.type) {
    case BucketType::Unknown:
        json["type"] = "<<unknown>>";
        break;
    case BucketType::NoBucket:
        json["type"] = "no bucket";
        break;
    case BucketType::Memcached:
        json["type"] = "memcached";
        break;
    case BucketType::Couchstore:
        json["type"] = "couchstore";
        break;
    case BucketType::EWouldBlock:
        json["type"] = "ewouldblock";
        break;
    }

    return json;
}

nlohmann::json get_bucket_details(size_t idx) {
    Bucket& bucket = all_buckets.at(idx);
    std::lock_guard<std::mutex> guard(bucket.mutex);

    return get_bucket_details_UNLOCKED(bucket, idx);
}

/**
 * Check if the associated bucket is dying or not. There is two reasons
 * for why a bucket could be dying: It is currently being deleted, or
 * someone initiated a shutdown process.
 */
bool is_bucket_dying(Connection& c) {
    bool disconnect = memcached_shutdown;
    Bucket& b = all_buckets.at(c.getBucketIndex());

    if (b.state != BucketState::Ready) {
        disconnect = true;
    }

    if (disconnect) {
        LOG_INFO(
                "{}: The connected bucket is being deleted.. closing "
                "connection {}",
                c.getId(),
                c.getDescription());
        c.setState(StateMachine::State::closing);
        return true;
    }

    return false;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    auto* c = reinterpret_cast<Connection*>(arg);
    if (c == nullptr) {
        LOG_WARNING("event_handler: connection must be non-NULL");
        return;
    }

    auto *thr = c->getThread();
    if (thr == nullptr) {
        LOG_WARNING(
                "Internal error - connection without a thread found. - "
                "ignored");
        return;
    }

    // Remove the list from the list of pending io's (in case the
    // object was scheduled to run in the dispatcher before the
    // callback for the worker thread is executed.
    //
    {
        std::lock_guard<std::mutex> lock(thr->pending_io.mutex);
        thr->pending_io.map.erase(c);
    }

    TRACE_LOCKGUARD_TIMED(thr->mutex,
                          "mutex",
                          "event_handler::threadLock",
                          SlowMutexThreshold);

    if (memcached_shutdown) {
        // Someone requested memcached to shut down.
        if (signal_idle_clients(thr, -1, false) == 0) {
            cb_assert(thr != nullptr);
            LOG_INFO("Stopping worker thread {}", thr->index);
            c->eventBaseLoopbreak();
            return;
        }
    }

    /* sanity */
    cb_assert(fd == c->getSocketDescriptor());

    run_event_loop(c, which);

    if (memcached_shutdown) {
        // Someone requested memcached to shut down. If we don't have
        // any connections bound to this thread we can just shut down
        int connected = signal_idle_clients(thr, -1, true);
        if (connected == 0) {
            LOG_INFO("Stopping worker thread {}", thr->index);
            event_base_loopbreak(thr->base);
        } else {
            LOG_INFO("Waiting for {} connected clients on worker thread {}",
                     connected,
                     thr->index);
        }
    }
}

/**
 * The listen_event_handler is the callback from libevent when someone is
 * connecting to one of the server sockets. It runs in the context of the
 * listen thread
 */
void listen_event_handler(evutil_socket_t, short which, void *arg) {
    auto& c = *reinterpret_cast<ServerSocket*>(arg);

    if (memcached_shutdown) {
        // Someone requested memcached to shut down. The listen thread should
        // be stopped immediately to avoid new connections
        LOG_INFO("Stopping listen thread");
        event_base_loopbreak(main_base);
        return;
    }

    try {
        c.acceptNewClient();
    } catch (std::invalid_argument& e) {
        LOG_WARNING("{}: exception occurred while accepting clients: {}",
                    c.getSocket(),
                    e.what());
    }
}

static void dispatch_event_handler(evutil_socket_t fd, short, void *) {
    char buffer[80];
    ssize_t nr = cb::net::recv(fd, buffer, sizeof(buffer), 0);

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
            for (auto& connection : listen_conn) {
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

    /* Start with the default size. */
    if (cb::net::getsockopt(sfd,
                            SOL_SOCKET,
                            SO_SNDBUF,
                            reinterpret_cast<void*>(&old_size),
                            &intsize) != 0) {
        LOG_WARNING("getsockopt(SO_SNDBUF): {}", strerror(errno));
        return;
    }

    /* Binary-search for the real maximum. */
    int min = old_size;
    int max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        int avg = ((unsigned int)(min + max)) / 2;
        if (cb::net::setsockopt(sfd,
                                SOL_SOCKET,
                                SO_SNDBUF,
                                reinterpret_cast<void*>(&avg),
                                intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    LOG_DEBUG("<{} send buffer was {}, now {}", sfd, old_size, last_good);
}

static SOCKET new_server_socket(struct addrinfo *ai, bool tcp_nodelay) {
    SOCKET sfd;

    sfd = cb::net::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
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

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
        error = cb::net::setsockopt(sfd,
                                    IPPROTO_IPV6,
                                    IPV6_V6ONLY,
                                    reinterpret_cast<const void*>(&flags),
                                    sizeof(flags));
        if (error != 0) {
            LOG_WARNING("setsockopt(IPV6_V6ONLY): {}", strerror(errno));
            safe_close(sfd);
            return INVALID_SOCKET;
        }
    }
#endif

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_REUSEADDR,
                            reinterpret_cast<const void*>(&flags),
                            sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_REUSEADDR): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_KEEPALIVE,
                            reinterpret_cast<const void*>(&flags),
                            sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_KEEPALIVE): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_LINGER,
                            reinterpret_cast<const char*>(&ling),
                            sizeof(ling)) != 0) {
        LOG_WARNING("setsockopt(SO_LINGER): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (tcp_nodelay) {
        if (cb::net::setsockopt(sfd,
                                IPPROTO_TCP,
                                TCP_NODELAY,
                                reinterpret_cast<const void*>(&flags),
                                sizeof(flags)) != 0) {
            LOG_WARNING("setsockopt(TCP_NODELAY): {}",
                        cb_strerror(cb::net::get_socket_error()));
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
static void add_listening_port(const NetworkInterface *interf, in_port_t port, sa_family_t family) {
    std::lock_guard<std::mutex> guard(stats_mutex);
    auto *descr = get_listening_port_instance(port);

    if (descr == nullptr) {
        ListeningPort newport(port,
                              interf->host,
                              interf->tcp_nodelay,
                              interf->backlog,
                              interf->management);

        newport.curr_conns = 1;
        newport.maxconns = interf->maxconn;

        if (interf->ssl.key.empty() || interf->ssl.cert.empty()) {
            newport.ssl.enabled = false;
        } else {
            newport.ssl.enabled = true;
            newport.ssl.key = interf->ssl.key;
            newport.ssl.cert = interf->ssl.cert;
        }

        if (family == AF_INET) {
            newport.ipv4 = true;
            newport.ipv6 = false;
        } else if (family == AF_INET6) {
            newport.ipv4 = false;
            newport.ipv6 = true;
        }

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
 * @param true if we was able to set up at least one address on the interface
 *        false if we failed to set any addresses on the interface
 */
static bool server_socket(const NetworkInterface& interf) {
    SOCKET sfd;
    addrinfo hints = {};

    // Set to true when we create an IPv4 interface
    bool ipv4 = false;
    // Set to true when we create an IPv6 interface
    bool ipv6 = false;

    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (interf.ipv4 && interf.ipv6) {
        hints.ai_family = AF_UNSPEC;
    } else if (interf.ipv4) {
        hints.ai_family = AF_INET;
    } else if (interf.ipv6) {
        hints.ai_family = AF_INET6;
    } else {
        throw std::invalid_argument(
                "server_socket: can't create a socket without IPv4 or IPv6");
    }

    std::string port_buf = std::to_string(interf.port);

    const char* host = nullptr;
    if (!interf.host.empty() && interf.host != "*") {
        host = interf.host.c_str();
    }

    struct addrinfo *ai;
    int error = getaddrinfo(host, port_buf.c_str(), &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        LOG_WARNING("getaddrinfo(): {}", cb_strerror(error));
#else
        if (error != EAI_SYSTEM) {
            LOG_WARNING("getaddrinfo(): {}", gai_strerror(error));
        } else {
            LOG_WARNING("getaddrinfo(): {}", cb_strerror(error));
        }
#endif
        return false;
    }

    // getaddrinfo may return multiple entries for a given name/port pair.
    // Iterate over all of them and try to set up a listen object.
    // We need at least _one_ entry per requested configuration (IPv4/6) in
    // order to call it a success.
    for (struct addrinfo* next = ai; next; next = next->ai_next) {
        if ((sfd = new_server_socket(next, interf.tcp_nodelay)) ==
            INVALID_SOCKET) {
            // getaddrinfo can return "junk" addresses,
            continue;
        }

        in_port_t listenport = 0;
        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) == SOCKET_ERROR) {
            LOG_WARNING("Failed to bind to address: {}",
                        cb_strerror(cb::net::get_socket_error()));
            safe_close(sfd);
            continue;
        }

        // We've configured this port.
        if (next->ai_addr->sa_family == AF_INET) {
            // We have at least one entry
            ipv4 = true;
        } else if (next->ai_addr->sa_family == AF_INET6) {
            // We have at least one entry
            ipv6 = true;
        }

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

        listen_conn.emplace_back(std::make_unique<ServerSocket>(
                sfd, main_base, listenport, next->ai_addr->sa_family, interf));
        stats.daemon_conns++;
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
        add_listening_port(&interf, listenport, next->ai_addr->sa_family);
    }

    freeaddrinfo(ai);

    if (interf.ipv4 && !ipv4) {
        // Failed to create an IPv4 port
        LOG_CRITICAL(R"(Failed to create IPv4 port for "{}:{}")",
                     interf.host.empty() ? "*" : interf.host,
                     interf.port);
    }

    if (interf.ipv6 && !ipv6) {
        // Failed to create an IPv6 oprt
        LOG_CRITICAL(R"(Failed to create IPv6 port for "{}:{}")",
                     interf.host.empty() ? "*" : interf.host,
                     interf.port);
    }

    // Return success as long as we managed to create a listening port
    // for at least one protocol.
    return ipv4 || ipv6;
}

static bool server_sockets(bool management) {
    bool success = true;

    if (management) {
        LOG_INFO("Enable management port(s)");
    } else {
        LOG_INFO("Enable user port(s)");
    }

    for (auto& interface : settings.getInterfaces()) {
        if (management && interface.management) {
            if (!server_socket(interface)) {
                success = false;
            }
        } else if (!management && !interface.management) {
            if (!server_socket(interface)) {
                success = false;
            }
        }
    }

    return success;
}

static void create_listen_sockets(bool management) {
    if (!server_sockets(management)) {
        FATAL_ERROR(EX_OSERR, "Failed to create listening socket(s)");
    }

    if (management) {
        // the client is not expecting us to update the port set at
        // later time, so enable all ports immediately
        if (!server_sockets(false)) {
            FATAL_ERROR(EX_OSERR, "Failed to create listening socket(s)");
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
            FATAL_ERROR(EX_OSERR,
                        R"(Failed to open "{}": {})",
                        temp_portnumber_filename,
                        strerror(errno));
        }

        unique_cJSON_ptr array(cJSON_CreateArray());

        for (const auto& connection : listen_conn) {
            cJSON_AddItemToArray(array.get(),
                                 connection->getDetails().release());
        }

        unique_cJSON_ptr root(cJSON_CreateObject());
        cJSON_AddItemToObject(root.get(), "ports", array.release());
        fprintf(portnumber_file, "%s\n", to_string(root, true).c_str());
        fclose(portnumber_file);
        LOG_INFO("Port numbers available in {}", portnumber_filename);
        if (rename(temp_portnumber_filename.c_str(), portnumber_filename) == -1) {
            FATAL_ERROR(EX_OSERR,
                        R"(Failed to rename "{}" to "{}": {})",
                        temp_portnumber_filename.c_str(),
                        portnumber_filename,
                        strerror(errno));
        }
    }
}

static void sigint_handler() {
    shutdown_server();
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

static bool install_signal_handlers() {
    // SIGUSR1 - Used to dump connection stats
    sigusr1_event = evsignal_new(main_base, SIGUSR1,
                                 dump_connection_stat_signal_handler,
                                 nullptr);
    if (sigusr1_event == nullptr) {
        LOG_WARNING("Failed to allocate SIGUSR1 handler");
        return false;
    }

    if (event_add(sigusr1_event, nullptr) < 0) {
        LOG_WARNING("Failed to install SIGUSR1 handler");
        return false;

    }

    // SIGTERM - Used to shut down memcached cleanly
    sigterm_event = evsignal_new(main_base, SIGTERM, sigterm_handler, NULL);
    if (sigterm_event == NULL) {
        LOG_WARNING("Failed to allocate SIGTERM handler");
        return false;
    }

    if (event_add(sigterm_event, NULL) < 0) {
        LOG_WARNING("Failed to install SIGTERM handler");
        return false;
    }

    return true;
}

static void release_signal_handlers() {
    event_free(sigusr1_event);
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

static std::condition_variable shutdown_cv;
static std::mutex shutdown_cv_mutex;
static bool memcached_can_shutdown = false;
void shutdown_server(void) {

    std::unique_lock<std::mutex> lk(shutdown_cv_mutex);
    if (!memcached_can_shutdown) {
        // log and proceed to wait shutdown
        LOG_INFO("shutdown_server waiting for can_shutdown signal");
        shutdown_cv.wait(lk, []{return memcached_can_shutdown;});
    }
    memcached_shutdown = true;
    LOG_INFO("Received shutdown request");
    event_base_loopbreak(main_base);
}

void enable_shutdown(void) {
    std::unique_lock<std::mutex> lk(shutdown_cv_mutex);
    memcached_can_shutdown = true;
    shutdown_cv.notify_all();
}

struct ServerCoreApi : public ServerCoreIface {
    rel_time_t get_current_time() override {
        return mc_time_get_current_time();
    }

    rel_time_t realtime(rel_time_t exptime, cb::ExpiryLimit limit) override {
        return mc_time_convert_to_real_time(exptime, limit);
    }

    time_t abstime(rel_time_t exptime) override {
        return mc_time_convert_to_abs_time(exptime);
    }

    int parse_config(const char* str,
                     config_item* items,
                     FILE* error) override {
        return ::parse_config(str, items, error);
    }

    void shutdown() override {
        shutdown_server();
    }

    size_t get_max_item_iovec_size() override {
        return 1;
    }

    void trigger_tick() override {
        mc_time_clock_tick();
    }
};

struct ServerLogApi : public ServerLogIface {
    spdlog::logger* get_spdlogger() override {
        return cb::logger::get();
    }

    void set_level(spdlog::level::level_enum severity) override {
        switch (severity) {
        case spdlog::level::level_enum::trace:
            settings.setVerbose(2);
            break;
        case spdlog::level::level_enum::debug:
            settings.setVerbose(1);
            break;
        default:
            settings.setVerbose(0);
            break;
        }
    }
};

struct ServerDocumentApi : public ServerDocumentIface {
    ENGINE_ERROR_CODE pre_link(gsl::not_null<const void*> void_cookie,
                               item_info& info) override {
        // Sanity check that people aren't calling the method with a bogus
        // cookie
        auto* cookie =
                reinterpret_cast<Cookie*>(const_cast<void*>(void_cookie.get()));

        auto* context = cookie->getCommandContext();
        if (context != nullptr) {
            return context->pre_link_document(info);
        }

        return ENGINE_SUCCESS;
    }

    bool pre_expiry(item_info& itm_info) override {
        return document_pre_expiry(itm_info);
    }
};

struct ServerCallbackApi : public ServerCallbackIface {
    void register_callback(EngineIface* engine,
                           ENGINE_EVENT_TYPE type,
                           EVENT_CALLBACK cb,
                           const void* cb_data) override {
        ::register_callback(engine, type, cb, cb_data);
    }
    void perform_callbacks(ENGINE_EVENT_TYPE type,
                           const void* data,
                           const void* cookie) override {
        ::perform_callbacks(type, data, cookie);
    }
};

struct ServerCookieApi : public ServerCookieIface {
    void store_engine_specific(gsl::not_null<const void*> void_cookie,
                               void* engine_data) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        cookie->getConnection().setEngineStorage(engine_data);
    }

    void* get_engine_specific(gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().getEngineStorage();
    }

    bool is_datatype_supported(gsl::not_null<const void*> void_cookie,
                               protocol_binary_datatype_t datatype) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isDatatypeEnabled(datatype);
    }

    bool is_mutation_extras_supported(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isSupportsMutationExtras();
    }

    bool is_collections_supported(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isCollectionsSupported();
    }

    uint8_t get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        uint8_t opcode = PROTOCOL_BINARY_CMD_INVALID;
        if (cookie->isEwouldblock()) {
            try {
                opcode = cookie->getHeader().getOpcode();
            } catch (...) {
                // Don't barf out if the header isn't there
            }
        }
        return opcode;
    }

    bool validate_session_cas(uint64_t cas) override {
        return session_cas.increment_session_counter(cas);
    }

    void decrement_session_ctr() override {
        session_cas.decrement_session_counter();
    }

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        ::notify_io_complete(cookie, status);
    }

    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        cookie->getConnection().incrementRefcount();
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE release(gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        auto* c = &cookie->getConnection();
        int notify;
        FrontEndThread* thr;

        thr = c->getThread();
        cb_assert(thr);

        {
            TRACE_LOCKGUARD_TIMED(thr->mutex,
                                  "mutex",
                                  "release_cookie::threadLock",
                                  SlowMutexThreshold);

            c->decrementRefcount();

            /* Releasing the refererence to the object may cause it to change
             * state. (NOTE: the release call shall never be called from the
             * worker threads), so should put the connection in the pool of
             * pending IO and have the system retry the operation for the
             * connection
             */
            notify = add_conn_to_pending_io_list(c, ENGINE_SUCCESS);
        }

        /* kick the thread in the butt */
        if (notify) {
            notify_thread(*thr);
        }

        return ENGINE_SUCCESS;
    }

    void set_priority(gsl::not_null<const void*> void_cookie,
                      CONN_PRIORITY priority) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        auto* c = &cookie->getConnection();
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

        LOG_WARNING(
                "{}: ServerCookieApi::set_priority: priority (which is {}) is "
                "not a "
                "valid CONN_PRIORITY - closing connection {}",
                c->getId(),
                priority,
                c->getDescription());
        c->setState(StateMachine::State::closing);
    }

    CONN_PRIORITY get_priority(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        auto& conn = cookie->getConnection();
        const auto priority = conn.getPriority();
        switch (priority) {
        case Connection::Priority::High:
            return CONN_PRIORITY_HIGH;
        case Connection::Priority::Medium:
            return CONN_PRIORITY_MED;
        case Connection::Priority::Low:
            return CONN_PRIORITY_LOW;
        }

        LOG_WARNING(
                "{}: ServerCookieApi::get_priority: priority (which is {}) is "
                "not a "
                "valid CONN_PRIORITY. {}",
                conn.getId(),
                int(priority),
                conn.getDescription());
        return CONN_PRIORITY_MED;
    }

    bucket_id_t get_bucket_id(gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return bucket_id_t(cookie->getConnection().getBucketIndex());
    }

    uint64_t get_connection_id(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return uint64_t(&cookie->getConnection());
    }

    cb::rbac::PrivilegeAccess check_privilege(
            gsl::not_null<const void*> void_cookie,
            cb::rbac::Privilege privilege) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().checkPrivilege(
                privilege, const_cast<Cookie&>(*cookie));
    }

    protocol_binary_response_status engine_error2mcbp(
            gsl::not_null<const void*> void_cookie,
            ENGINE_ERROR_CODE code) override {
        const auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        auto& connection = cookie->getConnection();

        ENGINE_ERROR_CODE status = connection.remapErrorCode(code);
        if (status == ENGINE_DISCONNECT) {
            throw cb::engine_error(
                    cb::engine_errc::disconnect,
                    "engine_error2mcbp: " + std::to_string(connection.getId()) +
                            ": Disconnect client");
        }

        return engine_error_2_mcbp_protocol_error(status);
    }

    std::pair<uint32_t, std::string> get_log_info(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return std::make_pair(cookie->getConnection().getId(),
                              cookie->getConnection().getDescription());
    }

    void set_error_context(gsl::not_null<void*> void_cookie,
                           cb::const_char_buffer message) override {
        auto* cookie = reinterpret_cast<Cookie*>(void_cookie.get());
        cookie->setErrorContext(to_string(message));
    }
};

class ServerApi : public SERVER_HANDLE_V1 {
public:
    ServerApi() {
        hooks_api.add_new_hook = AllocHooks::add_new_hook;
        hooks_api.remove_new_hook = AllocHooks::remove_new_hook;
        hooks_api.add_delete_hook = AllocHooks::add_delete_hook;
        hooks_api.remove_delete_hook = AllocHooks::remove_delete_hook;
        hooks_api.get_extra_stats_size = AllocHooks::get_extra_stats_size;
        hooks_api.get_allocator_stats = AllocHooks::get_allocator_stats;
        hooks_api.get_allocation_size = AllocHooks::get_allocation_size;
        hooks_api.get_detailed_stats = AllocHooks::get_detailed_stats;
        hooks_api.release_free_memory = AllocHooks::release_free_memory;
        hooks_api.enable_thread_cache = AllocHooks::enable_thread_cache;
        hooks_api.get_allocator_property = AllocHooks::get_allocator_property;

        core = &core_api;
        callback = &callback_api;
        log = &server_log_api;
        cookie = &server_cookie_api;
        alloc_hooks = &hooks_api;
        document = &document_api;
    }

protected:
    ServerCoreApi core_api;
    ServerCookieApi server_cookie_api;
    ServerLogApi server_log_api;
    ServerCallbackApi callback_api;
    ServerAllocatorIface hooks_api;
    ServerDocumentApi document_api;
};

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
SERVER_HANDLE_V1* get_server_api() {
    static ServerApi rv;
    return &rv;
}

/* BUCKET FUNCTIONS */
void CreateBucketThread::create() {
    LOG_INFO("{} Create bucket [{}]", connection.getId(), name);

    if (!BucketValidator::validateBucketName(name, error)) {
        LOG_WARNING("{} Create bucket [{}] failed - Invalid bucket name",
                    connection.getId(),
                    name);
        result = ENGINE_EINVAL;
        return;
    }

    if (!BucketValidator::validateBucketType(type, error)) {
        LOG_WARNING("{} Create bucket [{}] failed - Invalid bucket type",
                    connection.getId(),
                    name);
        result = ENGINE_EINVAL;
        return;
    }

    size_t ii;
    size_t first_free = all_buckets.size();
    bool found = false;

    std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);
    for (ii = 0; ii < all_buckets.size() && !found; ++ii) {
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        if (first_free == all_buckets.size() &&
            all_buckets[ii].state == BucketState::None)
        {
            first_free = ii;
        }
        if (name == all_buckets[ii].name) {
            found = true;
        }
    }

    if (found) {
        result = ENGINE_KEY_EEXISTS;
        LOG_WARNING("{} Create bucket [{}] failed - Already exists",
                    connection.getId(),
                    name);
    } else if (first_free == all_buckets.size()) {
        result = ENGINE_E2BIG;
        LOG_WARNING("{} Create bucket [{}] failed - Too many buckets",
                    connection.getId(),
                    name);
    } else {
        result = ENGINE_SUCCESS;
        ii = first_free;
        /*
         * split the creation of the bucket in two... so
         * we can release the global lock..
         */
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        all_buckets[ii].state = BucketState::Creating;
        all_buckets[ii].type = type;
        strcpy(all_buckets[ii].name, name.c_str());
        try {
            all_buckets[ii].topkeys = new TopKeys(settings.getTopkeysSize());
        } catch (const std::bad_alloc &) {
            result = ENGINE_ENOMEM;
            LOG_WARNING("{} Create bucket [{}] failed - out of memory",
                        connection.getId(),
                        name);
        }
    }
    all_bucket_lock.unlock();

    if (result != ENGINE_SUCCESS) {
        return;
    }

    auto &bucket = all_buckets[ii];

    /* People aren't allowed to use the engine in this state,
     * so we can do stuff without locking..
     */
    auto* engine = new_engine_instance(type, name, get_server_api);
    if (engine == nullptr) {
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = BucketState::None;
            bucket.name[0] = '\0';
            bucket.setEngine(nullptr);
            delete bucket.topkeys;
            bucket.topkeys = nullptr;
        }

        LOG_WARNING(
                "{} - Failed to create bucket [{}]: failed to "
                "create a new engine instance",
                connection.getId(),
                name);
        result = ENGINE_FAILED;
        return;
    }

    bucket.setEngine(engine);
    {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucket.state = BucketState::Initializing;
    }

    try {
        result = engine->initialize(config.c_str());
    } catch (const std::runtime_error& e) {
        LOG_WARNING("{} - Failed to create bucket [{}]: {}",
                    connection.getId(),
                    name,
                    e.what());
        result = ENGINE_FAILED;
    } catch (const std::bad_alloc& e) {
        LOG_WARNING("{} - Failed to create bucket [{}]: {}",
                    connection.getId(),
                    name,
                    e.what());
        result = ENGINE_ENOMEM;
    }

    if (result == ENGINE_SUCCESS) {
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = BucketState::Ready;
        }
        LOG_INFO("{} - Bucket [{}] created successfully",
                 connection.getId(),
                 name);
        bucket.max_document_size = engine->getMaxItemSize();
    } else {
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = BucketState::Destroying;
        }
        engine->destroy(false);
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucket.state = BucketState::None;
        bucket.name[0] = '\0';
        bucket.setEngine(nullptr);
        delete bucket.topkeys;
        bucket.topkeys = nullptr;

        result = ENGINE_NOT_STORED;
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

void notify_thread_bucket_deletion(FrontEndThread& me) {
    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        bool destroy = false;
        {
            std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
            if (all_buckets[ii].state == BucketState::Destroying) {
                destroy = true;
            }
        }

        if (destroy) {
            signal_idle_clients(&me, gsl::narrow<int>(ii), false);
        }
    }
}

void DestroyBucketThread::destroy() {
    ENGINE_ERROR_CODE ret = ENGINE_KEY_ENOENT;
    std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);

    Connection* connection = nullptr;
    if (cookie != nullptr) {
        connection = &cookie->getConnection();
    }

    /*
     * The destroy function will have access to a connection if the
     * McbpDestroyBucketTask originated from delete_bucket_executor().
     * However if we are in the process of shuting down and the
     * McbpDestroyBucketTask originated from main() then connection
     * will be set to nullptr.
     */
    const std::string connection_id{(connection == nullptr)
            ? "<none>"
            : std::to_string(connection->getId())};

    size_t idx = 0;
    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        if (name == all_buckets[ii].name) {
            idx = ii;
            if (all_buckets[ii].state == BucketState::Ready) {
                ret = ENGINE_SUCCESS;
                all_buckets[ii].state = BucketState::Destroying;
            } else {
                ret = ENGINE_KEY_EEXISTS;
            }
        }
        if (ret != ENGINE_KEY_ENOENT) {
            break;
        }
    }
    all_bucket_lock.unlock();

    if (ret != ENGINE_SUCCESS) {
        auto code = engine_error_2_mcbp_protocol_error(ret);
        LOG_INFO("{} Delete bucket [{}]: {}",
                 connection_id,
                 name,
                 memcached_status_2_text(code));
        result = ret;
        return;
    }

    LOG_INFO(
            "{} Delete bucket [{}]. Notifying all registered "
            "ON_DELETE_BUCKET callbacks",
            connection_id,
            name);

    perform_callbacks(ON_DELETE_BUCKET, nullptr, &all_buckets[idx]);

    LOG_INFO("{} Delete bucket [{}]. Wait for clients to disconnect",
             connection_id,
             name);

    /* If this thread is connected to the requested bucket... release it */
    if (connection != nullptr && idx == size_t(connection->getBucketIndex())) {
        disassociate_bucket(*connection);
    }

    /* Let all of the worker threads start invalidating connections */
    threads_initiate_bucket_deletion();

    auto& bucket = all_buckets[idx];

    /* Wait until all users disconnected... */
    {
        std::unique_lock<std::mutex> guard(bucket.mutex);

        while (bucket.clients > 0) {
            LOG_INFO(
                    "{} Delete bucket [{}]. Still waiting: {} clients "
                    "connected",
                    connection_id.c_str(),
                    name.c_str(),
                    bucket.clients);
            /* drop the lock and notify the worker threads */
            guard.unlock();
            threads_notify_bucket_deletion();
            guard.lock();

            bucket.cond.wait_for(guard,
                                 std::chrono::milliseconds(1000),
                                 [&bucket] { return bucket.clients == 0; });
        }
    }

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

    LOG_INFO(
            "{} Delete bucket [{}]. Shut down the bucket", connection_id, name);

    bucket.getEngine()->destroy(force);

    LOG_INFO("{} Delete bucket [{}]. Clean up allocated resources ",
             connection_id,
             name);

    /* Clean up the stats... */
    threadlocal_stats_reset(bucket.stats);

    // Clear any registered event handlers
    for (auto& handler : bucket.engine_event_handlers) {
        handler.clear();
    }

    {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucket.state = BucketState::None;
        bucket.setEngine(nullptr);
        bucket.name[0] = '\0';
        delete bucket.topkeys;
        bucket.responseCounters.fill(0);
        bucket.topkeys = nullptr;
    }
    // don't need lock because all timing data uses atomics
    bucket.timings.reset();

    LOG_INFO("{} Delete bucket [{}] complete", connection_id, name);
    result = ENGINE_SUCCESS;
}

void DestroyBucketThread::run() {
    setRunning();
    destroy();
    std::lock_guard<std::mutex> guard(task->getMutex());
    task->makeRunnable();
}

static void initialize_buckets(void) {
    size_t numthread = settings.getNumWorkerThreads() + 1;
    for (auto &b : all_buckets) {
        b.stats.resize(numthread);
    }

    // To make the life easier for us in the code, index 0
    // in the array is "no bucket"
    auto &nobucket = all_buckets.at(0);
    nobucket.setEngine(new_engine_instance(
            BucketType::NoBucket, "<internal>", get_server_api));
    cb_assert(nobucket.getEngine());
    nobucket.type = BucketType::NoBucket;
    nobucket.state = BucketState::Ready;
}

static void cleanup_buckets(void) {
    for (auto &bucket : all_buckets) {
        bool waiting;

        do {
            waiting = false;
            {
                std::lock_guard<std::mutex> guard(bucket.mutex);
                switch (bucket.state.load()) {
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
            }
            if (waiting) {
                usleep(250);
            }
        } while (waiting);

        if (bucket.state == BucketState::Ready) {
            bucket.getEngine()->destroy(false);
            delete bucket.topkeys;
        }
    }
}

void delete_all_buckets() {
    /*
     * Delete all of the buckets one by one by using the executor.
     * We could in theory schedule all of them in parallel, but they
     * probably have some dirty items they need to write to disk so
     * instead of having all of the buckets step on the underlying IO
     * in parallel we'll do them sequentially.
     */

    /**
     * Create a specialized task I may use that just holds the
     * DeleteBucketThread object.
     */
    class DestroyBucketTask : public Task {
    public:
        DestroyBucketTask(const std::string& name_)
            : thread(name_, false, nullptr, this)
        {
            // empty
        }

        // start the bucket deletion
        // May throw std::bad_alloc if we're failing to start the thread
        void start() {
            thread.start();
        }

        Status execute() override {
            return Status::Finished;
        }

        DestroyBucketThread thread;
    };

    LOG_INFO("Stop all buckets");
    bool done;
    do {
        done = true;
        std::shared_ptr<Task> task;
        std::string name;

        std::unique_lock<std::mutex> all_bucket_lock(buckets_lock);
        /*
         * Start at one (not zero) because zero is reserved for "no bucket".
         * The "no bucket" has a state of BucketState::Ready but no name.
         */
        for (size_t ii = 1; ii < all_buckets.size() && done; ++ii) {
            std::lock_guard<std::mutex> bucket_guard(all_buckets[ii].mutex);
            if (all_buckets[ii].state == BucketState::Ready) {
                name.assign(all_buckets[ii].name);
                LOG_INFO("Scheduling delete for bucket {}", name);
                task = std::make_shared<DestroyBucketTask>(name);
                std::lock_guard<std::mutex> guard(task->getMutex());
                dynamic_cast<DestroyBucketTask&>(*task).start();
                executorPool->schedule(task, false);
                done = false;
            }
        }
        all_bucket_lock.unlock();

        if (task.get() != nullptr) {
            auto& dbt = dynamic_cast<DestroyBucketTask&>(*task);
            LOG_INFO("Waiting for delete of {} to complete", name);
            dbt.thread.waitForState(Couchbase::ThreadState::Zombie);
            LOG_INFO("Bucket {} deleted", name);
        }
    } while (!done);
}

static void set_max_filehandles(void) {
    const uint64_t maxfiles = settings.getMaxconns() +
                            (3 * (settings.getNumWorkerThreads() + 2)) +
                            1024;

    auto limit = cb::io::maximizeFileDescriptors(maxfiles);

    if (limit < maxfiles) {
        LOG_WARNING(
                "Failed to set the number of file descriptors "
                "to {} due to system resource restrictions. "
                "This may cause the system to misbehave once you reach a "
                "high connection count as the system won't be able open "
                "new files on the system. The maximum number of file "
                "descriptors is currently set to {}. The system "
                "is configured to allow {} number of client connections, "
                "and in addition to that the overhead of the worker "
                "threads is {}. Finally the backed database needs to "
                "open files to persist data.",
                int(maxfiles),
                int(limit),
                settings.getMaxconns(),
                (3 * (settings.getNumWorkerThreads() + 2)));
    }
}

/**
 * The log function used from SASL
 *
 * Try to remap the log levels to our own levels and put in the log
 * depending on the severity.
 */
static void sasl_log_callback(cb::sasl::logging::Level level,
                              const std::string& message) {
    switch (level) {
    case cb::sasl::logging::Level::Error:
        LOG_ERROR("{}", message);
        break;
    case cb::sasl::logging::Level::Warning:
        LOG_WARNING("{}", message);
        break;
    case cb::sasl::logging::Level::Notice:
        LOG_INFO("{}", message);
        break;
    case cb::sasl::logging::Level::Fail:
    case cb::sasl::logging::Level::Debug:
        LOG_DEBUG("{}", message);
        break;
    case cb::sasl::logging::Level::Trace:
        LOG_TRACE("{}", message);
        break;
    }
}

static void initialize_sasl() {
    using namespace cb::sasl;
    logging::set_log_callback(sasl_log_callback);
    server::initialize();
    if (mechanism::plain::authenticate("default", "") == Error::OK) {
        set_default_bucket_enabled(true);
    } else {
        set_default_bucket_enabled(false);
    }

    if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
        // Speed up the unit tests ;)
        server::set_hmac_iteration_count(10);
    }
}

extern "C" int memcached_main(int argc, char **argv) {
    // MB-14649 log() crash on windows on some CPU's
#ifdef _WIN64
    _set_FMA3_enable (0);
#endif

#ifdef HAVE_LIBNUMA
    // Configure NUMA policy as soon as possible (before any dynamic memory
    // allocation).
    const std::string numa_status = configure_numa_policy();
#endif
    std::unique_ptr<ParentMonitor> parent_monitor;

    try {
        cb::logger::createConsoleLogger();
    } catch (const std::exception& e) {
        std::cerr << "Failed to create logger object: " << e.what()
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    // Setup terminate handler as early as possible to catch crashes
    // occurring during initialisation.
    install_backtrace_terminate_handler();

    setup_libevent_locking();

    initialize_openssl();

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    AllocHooks::initialize();

    /* init settings */
    settings_init();

    initialize_mbcp_lookup_map();

    /* Parse command line arguments */
    try {
        parse_arguments(argc, argv);
    } catch (const std::exception& exception) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to initialize server: {}",
                    exception.what());
    }

    update_settings_from_config();

    cb::rbac::initialize();

    if (getenv("COUCHBASE_FORCE_ENABLE_XATTR") != nullptr) {
        settings.setXattrEnabled(true);
    }

    /* Initialize breakpad crash catcher with our just-parsed settings. */
    cb::breakpad::initialize(settings.getBreakpadSettings());

    /* Configure file logger, if specified as a settings object */
    if (settings.has.logger) {
        auto ret = cb::logger::initialize(settings.getLoggerConfig());
        if (ret) {
            FATAL_ERROR(
                    EXIT_FAILURE, "Failed to initialize logger: {}", ret.get());
        }
    }

    /* File-based logging available from this point onwards... */

    /* Logging available now extensions have been loaded. */
    LOG_INFO("Couchbase version {} starting.", get_server_version());

    LOG_INFO("Using SLA configuration: {}",
             to_string(cb::mcbp::sla::to_json(), false));

    if (settings.isStdinListenerEnabled()) {
        LOG_INFO("Enable standard input listener");
        start_stdin_listener(shutdown_server);
    }

#ifdef HAVE_LIBNUMA
    // Log the NUMA policy selected (now the logger is available).
    LOG_INFO("NUMA: {}", numa_status);
#endif

    if (!settings.has.rbac_file) {
        FATAL_ERROR(EXIT_FAILURE, "RBAC file not specified");
    }

    if (!cb::io::isFile(settings.getRbacFile())) {
        FATAL_ERROR(EXIT_FAILURE,
                    "RBAC [{}] does not exist",
                    settings.getRbacFile());
    }

    LOG_INFO("Loading RBAC configuration from [{}]", settings.getRbacFile());
    cb::rbac::loadPrivilegeDatabase(settings.getRbacFile());

    LOG_INFO("Loading error maps from [{}]", settings.getErrorMapsDir());
    try {
        settings.loadErrorMaps(settings.getErrorMapsDir());
    } catch (const std::exception& e) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to load error maps: {}", e.what());
    }

    LOG_INFO("Starting external authentication manager");
    externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
    externalAuthManager->start();

    initialize_audit();

    /* inform interested parties of initial verbosity level */
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);

    set_max_filehandles();

    /* Aggregate the maximum number of connections */
    settings.calculateMaxconns();

    if (getenv("MEMCACHED_CRASH_TEST") == NULL) {
        try {
            initialize_engine_map();
        } catch (const std::exception& error) {
            FATAL_ERROR(EXIT_FAILURE,
                        "Unable to initialize engine map: {}",
                        error.what());
        }
    } else {
        // The crash tests wants the system to generate a crash.
        // I tried to rethrow the exception instead of logging
        // the error, but for some reason the python test script
        // didn't like that..
        initialize_engine_map();
    }

    /* Initialize bucket engine */
    initialize_buckets();

    initialize_sasl();

    /* initialize main thread libevent instance */
    main_base = event_base_new();

    cb::console::set_sigint_handler(sigint_handler);

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
    thread_init(settings.getNumWorkerThreads(), main_base, dispatch_event_handler);

    executorPool =
            std::make_unique<ExecutorPool>(settings.getNumWorkerThreads());

    initializeTracing();
    TRACE_GLOBAL0("memcached", "Started");

    /*
     * MB-20034.
     * Now that all threads have been created, e.g. the audit thread, threads
     * associated with extensions and the workers, we can enable shutdown.
     */
    enable_shutdown();

    /* Initialise memcached time keeping */
    mc_time_init(main_base);

    /* create the listening socket, bind it, and init */
    create_listen_sockets(true);

    /* Optional parent monitor */
    char *env = getenv("MEMCACHED_PARENT_MONITOR");
    if (env != NULL) {
        LOG_INFO("Starting parent monitor");
        parent_monitor.reset(new ParentMonitor(std::stoi(env)));
    }

    if (!memcached_shutdown) {
        /* enter the event loop */
        LOG_INFO("Initialization complete. Accepting clients.");
        service_online = true;
        event_base_loop(main_base, 0);
        service_online = false;
    }

    LOG_INFO("Initiating graceful shutdown.");
    delete_all_buckets();

    if (parent_monitor.get() != nullptr) {
        LOG_INFO("Shutting down parent monitor");
        parent_monitor.reset();
    }

    LOG_INFO("Shutting down audit daemon");
    shutdown_audit();

    LOG_INFO("Shutting down client worker threads");
    threads_shutdown();

    LOG_INFO("Releasing server sockets");
    listen_conn.clear();

    LOG_INFO("Releasing client resources");
    close_all_connections();

    LOG_INFO("Releasing bucket resources");
    cleanup_buckets();

    LOG_INFO("Shutting down RBAC subsystem");
    cb::rbac::destroy();

    LOG_INFO("Releasing thread resources");
    threads_cleanup();

    LOG_INFO("Shutting down executor pool");
    executorPool.reset();

    LOG_INFO("Releasing signal handlers");
    release_signal_handlers();

    LOG_INFO("Shutting down external authentication service");
    externalAuthManager->shutdown();
    externalAuthManager->waitForState(Couchbase::ThreadState::Zombie);
    externalAuthManager.reset();

    LOG_INFO("Shutting down SASL server");
    cb::sasl::server::shutdown();

    LOG_INFO("Releasing connection objects");
    destroy_connections();

    LOG_INFO("Deinitialising tracing");
    deinitializeTracing();

    LOG_INFO("Shutting down engine map");
    shutdown_engine_map();

    LOG_INFO("Removing breakpad");
    cb::breakpad::destroy();

    LOG_INFO("Releasing callbacks");
    free_callbacks();

    LOG_INFO("Shutting down OpenSSL");
    shutdown_openssl();

    LOG_INFO("Shutting down libevent");
    event_base_free(main_base);

    LOG_INFO("Shutting down logger extension");
    // drop my handle to the logger
    cb::logger::reset();
    cb::logger::shutdown();

    return EXIT_SUCCESS;
}
