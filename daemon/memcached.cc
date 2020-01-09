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

#include "memcached.h"
#include "alloc_hooks.h"
#include "buckets.h"
#include "cmdline.h"
#include "connections.h"
#include "cookie.h"
#include "debug_helpers.h"
#include "enginemap.h"
#include "executor.h"
#include "executorpool.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "ioctl.h"
#include "libevent_locking.h"
#include "listening_port.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp_executors.h"
#include "mcbp_topkeys.h"
#include "mcbp_validators.h"
#include "mcbpdestroybuckettask.h"
#include "memcached/audit_interface.h"
#include "memcached_openssl.h"
#include "network_interface.h"
#include "opentracing.h"
#include "parent_monitor.h"
#include "protocol/mcbp/engine_errc_2_mcbp.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "runtime.h"
#include "server_socket.h"
#include "settings.h"
#include "ssl_utils.h"
#include "stats.h"
#include "topkeys.h"
#include "tracing.h"
#include "utilities/terminate_handler.h"

#include <cbsasl/logging.h>
#include <cbsasl/mechanism.h>
#include <event2/thread.h>
#include <mcbp/mcbp.h>
#include <memcached/rbac.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/interrupt.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/sysinfo.h>
#include <utilities/breakpad.h>
#include <gsl/gsl>

#include <cerrno>
#include <chrono>
#include <memory>
#include <thread>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#if HAVE_LIBNUMA
#include <numa.h>
#endif
#ifdef WIN32
#include <Winbase.h> // For SetDllDirectory
#else
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

/**
 * All of the buckets in couchbase is stored in this array.
 */
static std::mutex buckets_lock;
std::array<Bucket, cb::limits::TotalBuckets + 1> all_buckets;

void bucketsForEach(std::function<bool(Bucket&, void*)> fn, void *arg) {
    std::lock_guard<std::mutex> all_bucket_lock(buckets_lock);
    for (Bucket& bucket : all_buckets) {
        bool do_break = false;
        std::lock_guard<std::mutex> guard(bucket.mutex);
        if (bucket.state == Bucket::State::Ready) {
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

std::unique_ptr<cb::ExecutorPool> executorPool;

/* Mutex for global stats */
std::mutex stats_mutex;

/// The maximum number of file handles we may have. During startup
/// we'll try to increase the allowed number of file handles to the
/// limit specified for the current user.
static size_t max_file_handles;

/*
 * forward declarations
 */

static void create_listen_sockets();

/* stats */
static void stats_init();

/* defaults */
static void settings_init();

static bool server_socket(const std::string& tag,
                          const std::string& host,
                          in_port_t port,
                          bool system_port,
                          const std::string& sslkey,
                          const std::string& sslcert,
                          NetworkInterface::Protocol iv4,
                          NetworkInterface::Protocol iv6);

/** exported globals **/
struct stats stats;

/** file scope variables **/
std::vector<std::unique_ptr<ServerSocket>> listen_conn;
std::atomic_bool check_listen_conn;

static struct event_base *main_base;

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
static void set_stats_reset_time()
{
    time_t now = time(nullptr);
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
    Bucket& b = connection.getBucket();
    std::lock_guard<std::mutex> guard(b.mutex);
    b.clients--;

    connection.setBucketIndex(0);

    if (b.clients == 0 && b.state == Bucket::State::Destroying) {
        b.cond.notify_one();
    }
}

bool associate_bucket(Connection& connection, const char* name) {
    bool found = false;

    /* leave the current bucket */
    disassociate_bucket(connection);

    /* Try to associate with the named bucket */
    for (size_t ii = 1; ii < all_buckets.size() && !found; ++ii) {
        Bucket &b = all_buckets.at(ii);
        std::lock_guard<std::mutex> guard(b.mutex);
        if (b.state == Bucket::State::Ready && strcmp(b.name, name) == 0) {
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

static void populate_log_level() {
    const auto val = Settings::instance().getLogLevel();

    // Log the verbosity value set by the user and not the log level as they
    // are inverted
    LOG_INFO("Changing logging level to {}", Settings::instance().getVerbose());
    cb::logger::setLogLevels(val);
}

static void stats_init() {
    set_stats_reset_time();
    stats.conn_structs.reset();
    stats.total_conns.reset();
    stats.daemon_conns.reset();
    stats.rejected_conns.reset();
    stats.curr_conns.store(0, std::memory_order_relaxed);
}

struct thread_stats* get_thread_stats(Connection* c) {
    cb_assert(c->getThread().index <
              (Settings::instance().getNumWorkerThreads() + 1));
    auto& independent_stats = all_buckets[c->getBucketIndex()].stats;
    return &independent_stats.at(c->getThread().index);
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
    size_t ret = 0;
    char *override = getenv("MEMCACHED_NUM_CPUS");
    if (override) {
        try {
            ret = std::stoull(override);
        } catch (...) {
        }
    } else {
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
    }

    if (ret == 0) {
        ret = 4;
    }

    return ret;
}

/// We might not support as many connections as requested if
/// we don't have enough file descriptors available
static void recalculate_max_connections() {
    const auto maxconn = Settings::instance().getMaxConnections();
    const auto system =
            (3 * (Settings::instance().getNumWorkerThreads() + 2)) + 1024;
    const uint64_t maxfiles = maxconn + system;

    if (max_file_handles < maxfiles) {
        const auto newmax = max_file_handles - system;
        Settings::instance().setMaxConnections(newmax, false);
        LOG_WARNING(
                "max_connections is set higher than the available number of "
                "file descriptors available. Reduce max_connections to: {}",
                newmax);

        if (newmax > Settings::instance().getSystemConnections()) {
            LOG_WARNING(
                    "system_connections:{} > max_connections:{}. Reduce "
                    "system_connections to {}",
                    Settings::instance().getSystemConnections(),
                    newmax,
                    newmax / 2);
            Settings::instance().setSystemConnections(newmax / 2);
        }
    }
}

static void breakpad_changed_listener(const std::string&, Settings &s) {
    cb::breakpad::initialize(s.getBreakpadSettings());
}

static void verbosity_changed_listener(const std::string&, Settings &s) {
    auto logger = cb::logger::get();
    if (logger) {
        logger->set_level(Settings::instance().getLogLevel());
    }

    // MB-33637: Make the verbosity change in the calling thread.
    // This should be a relatively quick operation as we only block if
    // we are registering or unregistering new loggers. It also prevents
    // a race condition on shutdown where we could attempt to log
    // something but the logger has already been destroyed.
    populate_log_level();
}

static void scramsha_fallback_salt_changed_listener(const std::string&,
                                                    Settings& s) {
    cb::sasl::server::set_scramsha_fallback_salt(s.getScramshaFallbackSalt());
}

static void opcode_attributes_override_changed_listener(const std::string&,
                                                        Settings& s) {
    try {
        cb::mcbp::sla::reconfigure(
                Settings::instance().getRoot(),
                nlohmann::json::parse(s.getOpcodeAttributesOverride()));
    } catch (const std::exception&) {
        cb::mcbp::sla::reconfigure(Settings::instance().getRoot());
    }
    LOG_INFO("SLA configuration changed to: {}",
             cb::mcbp::sla::to_json().dump());
}

static void interfaces_changed_listener(const std::string&, Settings &s) {
    check_listen_conn = true;
    notify_dispatcher();
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
    if (mem_policy_env && strcmp("disable", mem_policy_env) == 0) {
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

static void settings_init() {
    auto& settings = Settings::instance();

    // Set up the listener functions
    settings.addChangeListener(
            "max_connections", [](const std::string&, Settings& s) -> void {
                recalculate_max_connections();
            });
    settings.addChangeListener("interfaces",
                                           interfaces_changed_listener);
    settings.addChangeListener(
            "scramsha_fallback_salt", scramsha_fallback_salt_changed_listener);
    settings.addChangeListener(
            "active_external_users_push_interval",
            [](const std::string&, Settings& s) -> void {
                if (externalAuthManager) {
                    externalAuthManager->setPushActiveUsersInterval(
                            s.getActiveExternalUsersPushInterval());
                }
            });

    settings.addChangeListener(
            "opentracing_config", [](const std::string&, Settings& s) -> void {
                auto config = s.getOpenTracingConfig();
                if (config) {
                    OpenTracing::updateConfig(*config);
                }
            });

    NetworkInterface default_interface;
    settings.addInterface(default_interface);
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
    if (tmp) {
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

    settings.addChangeListener(
            "num_reader_threads", [](const std::string&, Settings& s) -> void {
                auto val =
                        ThreadPoolConfig::ThreadCount(s.getNumReaderThreads());
                bucketsForEach(
                        [val](Bucket& b, void*) -> bool {
                            b.getEngine()->set_num_reader_threads(val);
                            return true;
                        },
                        nullptr);
            });
    settings.addChangeListener(
            "num_writer_threads", [](const std::string&, Settings& s) -> void {
                auto val =
                        ThreadPoolConfig::ThreadCount(s.getNumWriterThreads());
                bucketsForEach(
                        [val](Bucket& b, void*) -> bool {
                            b.getEngine()->set_num_writer_threads(val);
                            return true;
                        },
                        nullptr);
            });

    // We need to invalidate the SSL cache whenever some of the settings change
    settings.addChangeListener("client_cert_auth",
                               [](const std::string&, Settings&) -> void {
                                   invalidateSslCache();
                               });
    settings.addChangeListener("ssl_cipher_order",
                               [](const std::string&, Settings&) -> void {
                                   invalidateSslCache();
                               });
    settings.addChangeListener("ssl_cipher_list",
                               [](const std::string&, Settings&) -> void {
                                   invalidateSslCache();
                               });
    settings.addChangeListener("ssl_cipher_suites",
                               [](const std::string&, Settings&) -> void {
                                   invalidateSslCache();
                               });
    settings.addChangeListener("ssl_minimum_protocol",
                               [](const std::string&, Settings&) -> void {
                                   invalidateSslCache();
                               });
}

/**
 * The config file may have altered some of the default values we're
 * caching in other variables. This is the place where we'd propagate
 * such changes.
 *
 * This is also the place to initialize any additional files needed by
 * Memcached.
 */
static void update_settings_from_config()
{
    auto& settings = Settings::instance();
    std::string root(DESTINATION_ROOT);

    if (!settings.getRoot().empty()) {
        root = settings.getRoot();
#ifdef WIN32
        std::string libdir = root + "/lib";
        cb::io::sanitizePath(libdir);
        SetDllDirectory(libdir.c_str());
#endif
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

    settings.addChangeListener(
            "opcode_attributes_override",
            opcode_attributes_override_changed_listener);
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
        }
    }
}

static nlohmann::json get_bucket_details_UNLOCKED(const Bucket& bucket,
                                                  size_t idx) {
    nlohmann::json json;
    if (bucket.state != Bucket::State::None) {
        try {
            json["index"] = idx;
            json["state"] = to_string(bucket.state.load());
            json["clients"] = bucket.clients;
            json["name"] = bucket.name;
            json["type"] = to_string(bucket.type);
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to generate bucket details: {}", e.what());
        }
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
    auto& b = c.getBucket();

    if (b.state != Bucket::State::Ready) {
        disconnect = true;
    }

    if (disconnect) {
        LOG_INFO(
                "{}: The connected bucket is being deleted.. closing "
                "connection {}",
                c.getId(),
                c.getDescription());
        c.shutdown();
        return true;
    }

    return false;
}

static void create_portnumber_file(bool terminate) {
    auto filename = Settings::instance().getPortnumberFile();
    if (!filename.empty()) {
        nlohmann::json json;
        json["ports"] = nlohmann::json::array();

        for (const auto& connection : listen_conn) {
            json["ports"].push_back(connection->toJson());
        }

        std::string tempname;
        tempname.assign(filename);
        tempname.append(".lck");

        FILE* file = nullptr;
        file = fopen(tempname.c_str(), "a");
        if (file == nullptr) {
            LOG_CRITICAL(R"(Failed to open "{}": {})", tempname, cb_strerror());
            if (terminate) {
                exit(EXIT_FAILURE);
            }
            return;
        }

        fprintf(file, "%s\n", json.dump().c_str());
        fclose(file);
        if (cb::io::isFile(filename)) {
            cb::io::rmrf(filename);
        }

        LOG_INFO("Port numbers available in {}", filename);
        if (rename(tempname.c_str(), filename.c_str()) == -1) {
            LOG_CRITICAL(R"(Failed to rename "{}" to "{}": {})",
                         tempname,
                         filename,
                         cb_strerror());
            if (terminate) {
                exit(EXIT_FAILURE);
            }
            cb::io::rmrf(tempname);
        }
    }
}

/**
 * The listen_event_handler is the callback from libevent when someone is
 * connecting to one of the server sockets. It runs in the context of the
 * listen thread
 */
void listen_event_handler(evutil_socket_t, short, void *arg) {
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
    // Start by draining the notification pipe fist
    drain_notification_channel(fd);
    if (check_listen_conn) {
        invalidateSslCache();
        check_listen_conn = false;

        bool changes = false;
        auto interfaces = Settings::instance().getInterfaces();

        // Step one, enable all new ports
        bool success = true;
        for (const auto& interface : interfaces) {
            const bool useTag = interface.port == 0;
            bool ipv4 = interface.ipv4 != NetworkInterface::Protocol::Off;
            bool ipv6 = interface.ipv6 != NetworkInterface::Protocol::Off;

            for (const auto& connection : listen_conn) {
                const auto& descr = connection->getInterfaceDescription();

                if ((useTag && (descr.tag == interface.tag)) ||
                    (!useTag && (descr.port == interface.port))) {
                    if (descr.family == AF_INET) {
                        ipv4 = false;
                    } else {
                        ipv6 = false;
                    }

                    if (!ipv4 && !ipv6) {
                        // no need to search anymore as we've got both
                        break;
                    }
                }
            }

            if (ipv4) {
                // create an IPv4 interface
                changes = true;
                success &= server_socket(interface.tag,
                                         interface.host,
                                         interface.port,
                                         interface.system,
                                         interface.ssl.key,
                                         interface.ssl.cert,
                                         interface.ipv4,
                                         NetworkInterface::Protocol::Off);
            }

            if (ipv6) {
                // create an IPv6 interface
                changes = true;
                success &= server_socket(interface.tag,
                                         interface.host,
                                         interface.port,
                                         interface.system,
                                         interface.ssl.key,
                                         interface.ssl.cert,
                                         NetworkInterface::Protocol::Off,
                                         interface.ipv6);
            }
        }

        // Step two, shut down ports if we didn't fail opening new ports
        if (success) {
            for (auto iter = listen_conn.begin(); iter != listen_conn.end();
                 /* empty */) {
                auto& connection = *iter;
                const auto& descr = connection->getInterfaceDescription();
                // should this entry be here:
                bool drop = true;
                for (const auto& interface : interfaces) {
                    if (descr.tag.empty()) {
                        if (interface.port != descr.port) {
                            // port mismatch... look at the next
                            continue;
                        }
                    } else if (descr.tag != interface.tag) {
                        // Tag mismatch... look at the next
                        continue;
                    }

                    if ((descr.family == AF_INET &&
                         interface.ipv4 != NetworkInterface::Protocol::Off) ||
                        (descr.family == AF_INET6 &&
                         interface.ipv6 != NetworkInterface::Protocol::Off)) {
                        drop = false;
                    }

                    if (!drop) {
                        if (descr.sslKey != interface.ssl.key ||
                            descr.sslCert != interface.ssl.cert) {
                            // change the associated description
                            connection->updateSSL(interface.ssl.key,
                                                  interface.ssl.cert);
                        }
                    }

                    break;
                }
                if (drop) {
                    // erase returns the element following this one (or end())
                    changes = true;
                    iter = listen_conn.erase(iter);
                } else {
                    // look at the next element
                    ++iter;
                }
            }
        }

        if (changes) {
            create_portnumber_file(false);
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

static SOCKET new_server_socket(struct addrinfo* ai) {
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

    return sfd;
}

static bool server_socket(const std::string& tag,
                          const std::string& host,
                          in_port_t port,
                          bool system_port,
                          const std::string& sslkey,
                          const std::string& sslcert,
                          NetworkInterface::Protocol iv4,
                          NetworkInterface::Protocol iv6) {
    SOCKET sfd;
    addrinfo hints = {};

    // Set to true when we create an IPv4 interface
    bool ipv4 = false;
    // Set to true when we create an IPv6 interface
    bool ipv6 = false;

    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (iv4 != NetworkInterface::Protocol::Off &&
        iv6 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_UNSPEC;
    } else if (iv4 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_INET;
    } else if (iv6 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_INET6;
    } else {
        throw std::invalid_argument(
                "server_socket: can't create a socket without IPv4 or IPv6");
    }

    const char* host_buf = nullptr;
    if (!host.empty() && host != "*") {
        host_buf = host.c_str();
    }

    struct addrinfo *ai;
    int error =
            getaddrinfo(host_buf, std::to_string(port).c_str(), &hints, &ai);
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
        if (next->ai_addr->sa_family != AF_INET &&
            next->ai_addr->sa_family != AF_INET6) {
            // Ignore unsupported address families
            continue;
        }

        if ((sfd = new_server_socket(next)) == INVALID_SOCKET) {
            // getaddrinfo can return "junk" addresses,
            continue;
        }

        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) == SOCKET_ERROR) {
            const auto bind_error = cb::net::get_socket_error();
            auto name = cb::net::to_string(
                    reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                    static_cast<socklen_t>(next->ai_addrlen));
            LOG_WARNING(
                    "Failed to bind to {} - {}", name, cb_strerror(bind_error));
            safe_close(sfd);
            continue;
        }

        in_port_t listenport = port;
        if (listenport == 0) {
            // The interface description requested an ephemeral port to
            // be allocated. Pick up the real port number
            union {
                struct sockaddr_in in;
                struct sockaddr_in6 in6;
            } my_sockaddr{};
            socklen_t len = sizeof(my_sockaddr);
            if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len) == 0) {
                if (next->ai_addr->sa_family == AF_INET) {
                    listenport = ntohs(my_sockaddr.in.sin_port);
                } else {
                    listenport = ntohs(my_sockaddr.in6.sin6_port);
                }
            } else {
                const auto error = cb::net::get_socket_error();
                auto name = cb::net::to_string(
                        reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                        static_cast<socklen_t>(next->ai_addrlen));
                LOG_WARNING(
                        "Failed to look up the assigned port for the ephemeral "
                        "port: {} error: {}",
                        name,
                        cb_strerror(error));
                safe_close(sfd);
                continue;
            }
        } else {
            listenport = port;
        }

        // We've configured this port.
        if (next->ai_addr->sa_family == AF_INET) {
            // We have at least one entry
            ipv4 = true;
        } else {
            // We have at least one entry
            ipv6 = true;
        }

        auto inter = std::make_shared<ListeningPort>(tag,
                                                     host,
                                                     listenport,
                                                     next->ai_addr->sa_family,
                                                     system_port,
                                                     sslkey,
                                                     sslcert);
        listen_conn.emplace_back(
                std::make_unique<ServerSocket>(sfd, main_base, inter));
        stats.daemon_conns++;
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    }

    freeaddrinfo(ai);

    // Check if we successfully listened on all required protocols.
    bool required_proto_missing = false;

    // Check if the specified (missing) protocol was requested; if so log a
    // message, and if required return true.
    auto checkIfProtocolRequired =
            [host, port](const NetworkInterface::Protocol& protoMode,
                         const char* protoName) -> bool {
        if (protoMode != NetworkInterface::Protocol::Off) {
            // Failed to create a socket for this protocol; and it's not
            // disabled
            auto level = spdlog::level::level_enum::warn;
            if (protoMode == NetworkInterface::Protocol::Required) {
                level = spdlog::level::level_enum::critical;
            }
            CB_LOG_ENTRY(level,
                         R"(Failed to create {} {} socket for "{}:{}")",
                         to_string(protoMode),
                         protoName,
                         host.empty() ? "*" : host,
                         port);
        }
        return protoMode == NetworkInterface::Protocol::Required;
    };
    if (!ipv4) {
        required_proto_missing |= checkIfProtocolRequired(iv4, "IPv4");
    }
    if (!ipv6) {
        required_proto_missing |= checkIfProtocolRequired(iv6, "IPv6");
    }

    // Return success as long as we managed to create a listening port
    // for all non-optional protocols.
    return !required_proto_missing;
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param true if we was able to set up at least one address on the interface
 *        false if we failed to set any addresses on the interface
 */
static bool server_socket(const NetworkInterface& interf) {
    return server_socket(interf.tag,
                         interf.host,
                         interf.port,
                         interf.system,
                         interf.ssl.key,
                         interf.ssl.cert,
                         interf.ipv4,
                         interf.ipv6);
}

static bool server_sockets() {
    bool success = true;

    LOG_INFO("Enable port(s)");
    for (auto& interface : Settings::instance().getInterfaces()) {
        if (!server_socket(interface)) {
            success = false;
        }
    }

    return success;
}

static void create_listen_sockets() {
    if (!server_sockets()) {
        FATAL_ERROR(
                EXIT_FAILURE,
                "Failed to create required listening socket(s). Terminating.");
    }

    create_portnumber_file(true);
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

static void sigterm_handler(evutil_socket_t, short, void *) {
    shutdown_server();
}

static struct event* sigterm_event;

static bool install_signal_handlers() {
    // SIGTERM - Used to shut down memcached cleanly
    sigterm_event = evsignal_new(main_base, SIGTERM, sigterm_handler, nullptr);
    if (!sigterm_event) {
        LOG_WARNING("Failed to allocate SIGTERM handler");
        return false;
    }

    if (event_add(sigterm_event, nullptr) < 0) {
        LOG_WARNING("Failed to install SIGTERM handler");
        return false;
    }

    return true;
}

static void release_signal_handlers() {
    event_free(sigterm_event);
}
#endif

const char* get_server_version() {
    if (strlen(PRODUCT_VERSION) == 0) {
        return "unknown";
    } else {
        return PRODUCT_VERSION;
    }
}

static std::condition_variable shutdown_cv;
static std::mutex shutdown_cv_mutex;
static bool memcached_can_shutdown = false;
void shutdown_server() {

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

void enable_shutdown() {
    std::unique_lock<std::mutex> lk(shutdown_cv_mutex);
    memcached_can_shutdown = true;
    shutdown_cv.notify_all();
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
            all_buckets[ii].state == Bucket::State::None) {
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
        all_buckets[ii].state = Bucket::State::Creating;
        all_buckets[ii].type = type;
        strcpy(all_buckets[ii].name, name.c_str());
        try {
            all_buckets[ii].topkeys = std::make_unique<TopKeys>(
                    Settings::instance().getTopkeysSize());
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

    // People aren't allowed to use the engine in this state,
    // so we can do stuff without locking..
    try {
        bucket.setEngine(new_engine_instance(type, get_server_api));
    } catch (const cb::engine_error& exception) {
        bucket.reset();
        LOG_WARNING("{} - Failed to create bucket [{}]: {}",
                    connection.getId(),
                    name,
                    exception.what());
        result = ENGINE_ERROR_CODE(exception.code().value());
        return;
    }

    auto* engine = bucket.getEngine();
    {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        bucket.state = Bucket::State::Initializing;
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
            bucket.state = Bucket::State::Ready;
        }
        LOG_INFO("{} - Bucket [{}] created successfully",
                 connection.getId(),
                 name);
        bucket.max_document_size = engine->getMaxItemSize();
        bucket.supportedFeatures = engine->getFeatures();
    } else {
        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            bucket.state = Bucket::State::Destroying;
        }
        engine->destroy(false);
        bucket.reset();

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
            if (all_buckets[ii].state == Bucket::State::Ready) {
                ret = ENGINE_SUCCESS;
                all_buckets[ii].state = Bucket::State::Destroying;
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
        auto code = cb::mcbp::to_status(cb::engine_errc(ret));
        LOG_INFO("{} Delete bucket [{}]: {}",
                 connection_id,
                 name,
                 to_string(code));
        result = ret;
        return;
    }

    LOG_INFO("{} Delete bucket [{}]. Notifying engine", connection_id, name);

    all_buckets[idx].getEngine()->initiate_shutdown();
    all_buckets[idx].getEngine()->cancel_all_operations_in_ewb_state();

    LOG_INFO("{} Delete bucket [{}]. Engine ready for shutdown",
             connection_id,
             name);

    /* If this thread is connected to the requested bucket... release it */
    if (connection != nullptr && idx == size_t(connection->getBucketIndex())) {
        disassociate_bucket(*connection);
    }

    // Wait until all users disconnected...
    auto& bucket = all_buckets[idx];
    {
        std::unique_lock<std::mutex> guard(bucket.mutex);
        if (bucket.clients > 0) {
            LOG_INFO("{} Delete bucket [{}]. Wait for {} clients to disconnect",
                     connection_id,
                     name,
                     bucket.clients);

            // Signal clients bound to the bucket before waiting
            guard.unlock();
            iterate_all_connections([&bucket](Connection& connection) {
                if (&connection.getBucket() == &bucket) {
                    connection.signalIfIdle();
                }
            });
            guard.lock();
        }

        using std::chrono::seconds;
        using std::chrono::steady_clock;

        auto nextLog = steady_clock::now() + seconds(30);
        nlohmann::json prevDump;

        // We need to disconnect all of the clients before we can delete the
        // bucket. We try to log stuff while we're waiting in order to
        // know why stuff isn't complete. We'll do it in the following way:
        //
        //       1. Don't log anything the first 30 seconds. Then
        //          dump the state of all stuck connections.
        //       2. Don't og anything for the next 30 seconds. Then
        //          dump the state of all stuck connections which
        //          changed since the previous dump.
        //       3. goto 2.
        //
        while (bucket.clients > 0) {
            bucket.cond.wait_for(guard, seconds(1), [&bucket] {
                return bucket.clients == 0;
            });

            if (bucket.clients == 0) {
                break;
            }

            if (steady_clock::now() < nextLog) {
                guard.unlock();
                iterate_all_connections([&bucket](Connection& connection) {
                    if (&connection.getBucket() == &bucket) {
                        connection.signalIfIdle();
                    }
                });
                bucket.getEngine()->cancel_all_operations_in_ewb_state();
                guard.lock();
                continue;
            }

            nextLog = steady_clock::now() + seconds(30);

            // drop the lock and notify the worker threads
            guard.unlock();

            nlohmann::json current;
            iterate_all_connections([&bucket, &current](Connection& conn) {
                if (&conn.getBucket() == &bucket) {
                    conn.signalIfIdle();
                    current[std::to_string(conn.getId())] = conn.toJSON();
                }
            });
            auto diff = current;

            // remove all connections which didn't change
            for (auto it = prevDump.begin(); it != prevDump.end(); ++it) {
                auto entry = diff.find(it.key());
                if (entry != diff.end()) {
                    if (it.value().dump() == entry->dump()) {
                        diff.erase(entry);
                    }
                }
            }

            prevDump = std::move(current);
            if (diff.empty()) {
                LOG_INFO(
                        R"({} Delete bucket [{}]. Still waiting: {} clients connected (state is unchanged).)",
                        connection_id,
                        name,
                        bucket.clients);
            } else {
                LOG_INFO(
                        R"({} Delete bucket [{}]. Still waiting: {} clients connected: {})",
                        connection_id,
                        name,
                        bucket.clients,
                        diff.dump());
            }

            guard.lock();
        }
    }

    LOG_INFO(
            "{} Delete bucket [{}]. Shut down the bucket", connection_id, name);

    bucket.getEngine()->destroy(force);

    LOG_INFO("{} Delete bucket [{}]. Clean up allocated resources ",
             connection_id,
             name);
    bucket.reset();

    LOG_INFO("{} Delete bucket [{}] complete", connection_id, name);
    result = ENGINE_SUCCESS;
}

void DestroyBucketThread::run() {
    setRunning();
    destroy();
    std::lock_guard<std::mutex> guard(task->getMutex());
    task->makeRunnable();
}

void initialize_buckets() {
    size_t numthread = Settings::instance().getNumWorkerThreads() + 1;
    for (auto &b : all_buckets) {
        b.stats.resize(numthread);
    }

    // To make the life easier for us in the code, index 0
    // in the array is "no bucket"
    auto &nobucket = all_buckets.at(0);
    try {
        nobucket.setEngine(
                new_engine_instance(BucketType::NoBucket, get_server_api));
    } catch (const std::exception& exception) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to create the internal bucket \"No bucket\": {}",
                    exception.what());
    }
    nobucket.max_document_size = nobucket.getEngine()->getMaxItemSize();
    nobucket.supportedFeatures = nobucket.getEngine()->getFeatures();
    nobucket.type = BucketType::NoBucket;
    nobucket.state = Bucket::State::Ready;
}

void cleanup_buckets() {
    for (auto &bucket : all_buckets) {
        bool waiting;

        do {
            waiting = false;
            {
                std::lock_guard<std::mutex> guard(bucket.mutex);
                switch (bucket.state.load()) {
                case Bucket::State::Stopping:
                case Bucket::State::Destroying:
                case Bucket::State::Creating:
                case Bucket::State::Initializing:
                    waiting = true;
                    break;
                default:
                        /* Empty */
                        ;
                }
            }
            if (waiting) {
                std::this_thread::sleep_for(std::chrono::microseconds(250));
            }
        } while (waiting);

        if (bucket.state == Bucket::State::Ready) {
            bucket.getEngine()->destroy(false);
            bucket.reset();
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
        explicit DestroyBucketTask(const std::string& name_)
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
         * The "no bucket" has a state of Bucket::State::Ready but no name.
         */
        for (size_t ii = 1; ii < all_buckets.size() && done; ++ii) {
            std::lock_guard<std::mutex> bucket_guard(all_buckets[ii].mutex);
            if (all_buckets[ii].state == Bucket::State::Ready) {
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

        if (task) {
            auto& dbt = dynamic_cast<DestroyBucketTask&>(*task);
            LOG_INFO("Waiting for delete of {} to complete", name);
            dbt.thread.waitForState(Couchbase::ThreadState::Zombie);
            LOG_INFO("Bucket {} deleted", name);
        }
    } while (!done);
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
    set_default_bucket_enabled(
            mechanism::plain::authenticate("default", "") == Error::OK);

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

    max_file_handles = cb::io::maximizeFileDescriptors(
            std::numeric_limits<uint32_t>::max());

#ifdef WIN32
    evthread_use_windows_threads();
#else
    evthread_use_pthreads();
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
        Settings::instance().setXattrEnabled(true);
    }

    /* Configure file logger, if specified as a settings object */
    if (Settings::instance().has.logger) {
        auto ret =
                cb::logger::initialize(Settings::instance().getLoggerConfig());
        if (ret) {
            FATAL_ERROR(
                    EXIT_FAILURE, "Failed to initialize logger: {}", ret.get());
        }
    }

    /* File-based logging available from this point onwards... */
    Settings::instance().addChangeListener("verbosity",
                                           verbosity_changed_listener);

    /* Logging available now extensions have been loaded. */
    LOG_INFO("Couchbase version {} starting.", get_server_version());

    /// Initialize breakpad crash catcher with our just-parsed settings
    Settings::instance().addChangeListener("breakpad",
                                           breakpad_changed_listener);
    cb::breakpad::initialize(Settings::instance().getBreakpadSettings());

    LOG_INFO("Using SLA configuration: {}", cb::mcbp::sla::to_json().dump());

    auto opentracingconfig = Settings::instance().getOpenTracingConfig();
    if (opentracingconfig) {
        OpenTracing::updateConfig(*opentracingconfig);
    }

    if (Settings::instance().isStdinListenerEnabled()) {
        LOG_INFO("Enable standard input listener");
        start_stdin_listener(shutdown_server);
    }

#ifdef HAVE_LIBNUMA
    // Log the NUMA policy selected (now the logger is available).
    LOG_INFO("NUMA: {}", numa_status);
#endif

    if (!Settings::instance().has.rbac_file) {
        FATAL_ERROR(EXIT_FAILURE, "RBAC file not specified");
    }

    if (!cb::io::isFile(Settings::instance().getRbacFile())) {
        FATAL_ERROR(EXIT_FAILURE,
                    "RBAC [{}] does not exist",
                    Settings::instance().getRbacFile());
    }

    LOG_INFO("Loading RBAC configuration from [{}]",
             Settings::instance().getRbacFile());
    try {
        cb::rbac::loadPrivilegeDatabase(Settings::instance().getRbacFile());
    } catch (const std::exception& exception) {
        // We can't run without a privilege database
        FATAL_ERROR(EXIT_FAILURE, exception.what());
    }

    LOG_INFO("Loading error maps from [{}]",
             Settings::instance().getErrorMapsDir());
    try {
        Settings::instance().loadErrorMaps(
                Settings::instance().getErrorMapsDir());
    } catch (const std::exception& e) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to load error maps: {}", e.what());
    }

    LOG_INFO("Starting external authentication manager");
    externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
    externalAuthManager->setPushActiveUsersInterval(
            Settings::instance().getActiveExternalUsersPushInterval());
    externalAuthManager->start();

    initialize_audit();

    // inform interested parties of initial verbosity level
    populate_log_level();

    recalculate_max_connections();

    if (getenv("MEMCACHED_CRASH_TEST")) {
        create_crash_instance();
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
        FATAL_ERROR(EXIT_FAILURE, "Failed to ignore SIGPIPE; sigaction");
    }
#endif

    /* create the listening socket(s), bind, and init. Note this is done
     * before starting worker threads; so _if_ any required sockets fail then
     * terminating is simpler as we don't need to shutdown the workers.
     */
    create_listen_sockets();

    /* start up worker threads if MT mode */
    thread_init(Settings::instance().getNumWorkerThreads(),
                main_base,
                dispatch_event_handler);

    executorPool = std::make_unique<cb::ExecutorPool>(
            Settings::instance().getNumWorkerThreads());

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

    // Optional parent monitor
    {
        const int parent = Settings::instance().getParentIdentifier();
        if (parent != -1) {
            LOG_INFO("Starting parent monitor");
            parent_monitor = std::make_unique<ParentMonitor>(parent);
        }
    }

    if (!memcached_shutdown) {
        /* enter the event loop */
        LOG_INFO("Initialization complete. Accepting clients.");
        event_base_loop(main_base, 0);
    }

    LOG_INFO("Initiating graceful shutdown.");
    delete_all_buckets();

    if (parent_monitor) {
        LOG_INFO("Shutting down parent monitor");
        parent_monitor.reset();
    }

    LOG_INFO("Shutting down audit daemon");
    shutdown_audit();

    LOG_INFO("Shutting down client worker threads");
    threads_shutdown();

    LOG_INFO("Releasing server sockets");
    listen_conn.clear();

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

    LOG_INFO("Release cached SSL contexts");
    invalidateSslCache();

    LOG_INFO("Shutting down SASL server");
    cb::sasl::server::shutdown();

    LOG_INFO("Deinitialising tracing");
    deinitializeTracing();

    LOG_INFO("Shutting down all engines");
    shutdown_all_engines();

    LOG_INFO("Removing breakpad");
    cb::breakpad::destroy();

    LOG_INFO("Shutting down OpenSSL");
    shutdown_openssl();

    LOG_INFO("Shutting down libevent");
    event_base_free(main_base);

    if (OpenTracing::isEnabled()) {
        OpenTracing::shutdown();
    }

    LOG_INFO("Shutting down logger extension");
    cb::logger::shutdown();

    return EXIT_SUCCESS;
}
