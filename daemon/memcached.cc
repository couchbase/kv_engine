/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Sun Microsystems
 * Portions Copyright (c) 2003 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt,
 * licenses/BSD-3-Clause-Sun-Microsystems.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
#include "memcached.h"
#include "buckets.h"
#include "cmdline.h"
#include "cookie.h"
#include "enginemap.h"
#include "environment.h"
#include "error_map_manager.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "libevent_locking.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp_executors.h"
#include "network_interface.h"
#include "network_interface_manager.h"
#include "nobucket_taskable.h"
#include "opentelemetry.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "settings.h"
#include "stats.h"
#include "tracing.h"
#include "utilities/terminate_handler.h"
#include <cbsasl/logging.h>
#include <cbsasl/mechanism.h>
#include <executor/executorpool.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <folly/Chrono.h>
#include <folly/CpuId.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/Unistd.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/mcbp.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/backtrace.h>
#include <platform/dirutils.h>
#include <platform/interrupt.h>
#include <platform/process_monitor.h>
#include <platform/scope_timer.h>
#include <platform/strerror.h>
#include <platform/sysinfo.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <statistics/prometheus.h>
#include <utilities/breakpad.h>
#include <utilities/openssl_utils.h>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <memory>
#include <thread>

#if HAVE_LIBNUMA
#include <numa.h>
#endif

namespace cb::serverless {
Config& Config::instance() {
    static Config instance;
    return instance;
}
} // namespace cb::serverless

std::atomic<bool> memcached_shutdown;
std::atomic<bool> sigint;
std::atomic<bool> sigterm;

bool is_memcached_shutting_down() {
    return memcached_shutdown;
}

void shutdown_server() {
    memcached_shutdown = true;
}

/*
 * forward declarations
 */

/* stats */
static void stats_init();

/* defaults */
static void settings_init();

/** exported globals **/
struct stats stats;

/** file scope variables **/

std::unique_ptr<folly::EventBase> main_base;

void stop_memcached_main_base() {
    if (main_base) {
        main_base->terminateLoopSoon();
    }
}

static folly::Synchronized<std::string, std::mutex> reset_stats_time;
/**
 * MB-12470 requests an easy way to see when (some of) the statistics
 * counters were reset. This functions grabs the current time and tries
 * to format it to the current timezone by using ctime_r/s (which adds
 * a newline at the end for some obscure reason which we'll need to
 * strip off).
 */
static void setStatsResetTime() {
    time_t now = time(nullptr);
    std::array<char, 80> reset_time;
#ifdef WIN32
    ctime_s(reset_time.data(), reset_time.size(), &now);
#else
    ctime_r(&now, reset_time.data());
#endif
    char* ptr = strchr(reset_time.data(), '\n');
    if (ptr) {
        *ptr = '\0';
    }
    reset_stats_time.lock()->assign(reset_time.data());
}

std::string getStatsResetTime() {
    return *reset_stats_time.lock();
}

void disconnect_bucket(Bucket& bucket, Cookie* cookie) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer<SpanStopwatch> timer(
            std::forward_as_tuple(cookie, Code::DisassociateBucket));
    cb::tracing::MutexSpan guard(cookie,
                                 bucket.mutex,
                                 Code::BucketLockWait,
                                 Code::BucketLockHeld,
                                 std::chrono::milliseconds(5));

    if (--bucket.clients == 0 && bucket.state == Bucket::State::Destroying) {
        bucket.cond.notify_one();
    }
}

void disassociate_bucket(Connection& connection, Cookie* cookie) {
    disconnect_bucket(connection.getBucket(), cookie);
    connection.setBucketIndex(0, cookie);
}

bool associate_bucket(Connection& connection,
                      const char* name,
                      Cookie* cookie) {
    // leave the current bucket
    disassociate_bucket(connection, cookie);

    std::size_t idx = 0;
    /* Try to associate with the named bucket */
    for (size_t ii = 1; ii < all_buckets.size(); ++ii) {
        Bucket &b = all_buckets.at(ii);
        if (b.state == Bucket::State::Ready) {
            // Lock and rerun the test
            cb::tracing::MutexSpan guard(cookie,
                                         b.mutex,
                                         cb::tracing::Code::BucketLockWait,
                                         cb::tracing::Code::BucketLockHeld,
                                         std::chrono::milliseconds(5));
            if (b.state == Bucket::State::Ready && strcmp(b.name, name) == 0) {
                b.clients++;
                idx = ii;
                break;
            }
        }
    }

    if (idx != 0) {
        connection.setBucketIndex(gsl::narrow<int>(idx), cookie);
        audit_bucket_selection(connection, cookie);
    } else {
        // Bucket not found, connect to the "no-bucket"
        Bucket &b = all_buckets.at(0);
        {
            cb::tracing::MutexSpan guard(cookie,
                                         b.mutex,
                                         cb::tracing::Code::BucketLockWait,
                                         cb::tracing::Code::BucketLockHeld,
                                         std::chrono::milliseconds(5));
            b.clients++;
        }
        connection.setBucketIndex(0, cookie);
    }

    return idx != 0;
}

bool associate_bucket(Cookie& cookie, const char* name) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer<SpanStopwatch> timer(
            std::forward_as_tuple(cookie, Code::AssociateBucket));
    return associate_bucket(cookie.getConnection(), name, &cookie);
}

void associate_initial_bucket(Connection& connection) {
    Bucket &b = all_buckets.at(0);
    {
        std::lock_guard<std::mutex> guard(b.mutex);
        b.clients++;
    }

    connection.setBucketIndex(0);
}

static void populate_log_level() {
    const auto val = Settings::instance().getLogLevel();

    // Log the verbosity value set by the user and not the log level as they
    // are inverted
    LOG_INFO("Changing logging level to {}", Settings::instance().getVerbose());
    cb::logger::setLogLevels(val);
}

static void stats_init() {
    setStatsResetTime();
    stats.conn_structs.reset();
    stats.total_conns.reset();
    stats.rejected_conns.reset();
    stats.curr_conns.store(0, std::memory_order_relaxed);
}

static bool prometheus_auth_callback(const std::string& user,
                                     const std::string& password) {
    if (cb::sasl::mechanism::plain::authenticate(user, password) !=
        cb::sasl::Error::OK) {
        return false;
    }
    try {
        auto ctx = cb::rbac::createContext({user, cb::rbac::Domain::Local},
                                           "" /* no bucket */);
        return ctx
                .check(cb::rbac::Privilege::Stats,
                       std::nullopt /* no scope */,
                       std::nullopt /* no collection */)
                .success();
    } catch (const cb::rbac::Exception&) {
        return false;
    }
}

nlohmann::json prometheus_init(const std::pair<in_port_t, sa_family_t>& config,
                               cb::prometheus::AuthCallback authCB) {
    auto port = cb::prometheus::initialize(config, std::move(authCB));

    using cb::prometheus::IncludeTimestamps;
    cb::prometheus::addEndpoint("/_prometheusMetrics",
                                "kv_", /* metric prefix */
                                IncludeTimestamps::Yes,
                                server_prometheus_stats_low);
    cb::prometheus::addEndpoint("/_prometheusMetricsNoTS",
                                "kv_",
                                IncludeTimestamps::No,
                                server_prometheus_stats_low);
    cb::prometheus::addEndpoint("/_prometheusMetricsHigh",
                                "kv_",
                                IncludeTimestamps::Yes,
                                server_prometheus_stats_high);
    cb::prometheus::addEndpoint("/_prometheusMetricsHighNoTS",
                                "kv_",
                                IncludeTimestamps::No,
                                server_prometheus_stats_high);
    if (isServerlessDeployment()) {
        cb::prometheus::addEndpoint("/_metering",
                                    "", /* no prefix */
                                    IncludeTimestamps::No,
                                    server_prometheus_metering);
    }

    return port;
}

struct thread_stats* get_thread_stats(Connection* c) {
    auto& independent_stats = c->getBucket().stats;
    return &independent_stats.at(c->getThread().index);
}

void stats_reset(Cookie& cookie) {
    setStatsResetTime();
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
    auto& settings = Settings::instance();

    const auto desiredMaxConnections = settings.getMaxConnections();

    // File descriptors reserved for worker threads
    const auto workerThreadFds = (3 * (settings.getNumWorkerThreads() + 2));

    auto totalReserved =
            workerThreadFds + environment.memcached_reserved_file_descriptors;

    // We won't allow engine_file_descriptors to change for now.
    // @TODO allow this to be dynamic
    if (environment.engine_file_descriptors == 0) {
        totalReserved += environment.min_engine_file_descriptors;
    } else {
        totalReserved += environment.engine_file_descriptors;
    }

    const uint64_t maxfiles = desiredMaxConnections + totalReserved;

    if (environment.max_file_descriptors < maxfiles) {
        const auto newmax = environment.max_file_descriptors - totalReserved;
        settings.setMaxConnections(newmax, false);
        LOG_WARNING(
                "max_connections is set higher than the available number of "
                "file descriptors available. Reduce max_connections to: {}",
                newmax);

        if (settings.getSystemConnections() > newmax) {
            LOG_WARNING(
                    "system_connections:{} > max_connections:{}. Reduce "
                    "system_connections to {}",
                    settings.getSystemConnections(),
                    newmax,
                    newmax / 2);
            settings.setSystemConnections(newmax / 2);
        }

        // @TODO allow this to be dynamic
        if (environment.engine_file_descriptors == 0) {
            environment.engine_file_descriptors =
                    environment.min_engine_file_descriptors;
        }
    } else if (environment.engine_file_descriptors == 0) {
        environment.engine_file_descriptors =
                environment.max_file_descriptors -
                (workerThreadFds +
                 environment.memcached_reserved_file_descriptors +
                 desiredMaxConnections);
        // @TODO send down to the engine(s)
    }

    LOG_INFO("recalculate_max_connections: {}",
             nlohmann::json{
                     {"max_fds", environment.max_file_descriptors.load()},
                     {"max_connections", settings.getMaxConnections()},
                     {"system_connections", settings.getSystemConnections()},
                     {"engine_fds", environment.engine_file_descriptors.load()}}
                     .dump());
}

static void breakpad_changed_listener(const std::string&, Settings &s) {
    cb::breakpad::initialize(s.getBreakpadSettings(), s.getLoggerConfig());
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
            "scramsha_fallback_salt", scramsha_fallback_salt_changed_listener);
    settings.addChangeListener(
            "active_external_users_push_interval",
            [](const std::string&, Settings& s) -> void {
                if (externalAuthManager) {
                    externalAuthManager->setPushActiveUsersInterval(
                            s.getActiveExternalUsersPushInterval());
                }
            });

    settings.setVerbose(0);
    settings.setNumWorkerThreads(get_number_of_worker_threads());

    if (getenv("COUCHBASE_ENABLE_PRIVILEGE_DEBUG") != nullptr) {
        settings.setPrivilegeDebug(true);
    }

    settings.addChangeListener(
            "num_storage_threads", [](const std::string&, Settings& s) -> void {
                auto val = ThreadPoolConfig::StorageThreadCount(
                        s.getNumStorageThreads());
                BucketManager::instance().forEach([val](Bucket& b) -> bool {
                    b.getEngine().set_num_storage_threads(val);
                    return true;
                });
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
    }

    if (settings.getErrorMapsDir().empty()) {
        // Set the error map dir.
        auto path = std::filesystem::path(root) / "etc" / "couchbase" / "kv" /
                    "error_maps";
        if (cb::io::isDirectory(path.generic_string())) {
            settings.setErrorMapsDir(path.generic_string());
        }
    }

    try {
        if (settings.has.opcode_attributes_override) {
            cb::mcbp::sla::reconfigure(
                    Settings::instance().getRoot(),
                    nlohmann::json::parse(
                            settings.getOpcodeAttributesOverride()));
        } else {
            cb::mcbp::sla::reconfigure(root);
        }
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
        c.setTerminationReason("The connected bucket is being deleted");
        return true;
    }

    return false;
}

static void sigint_handler() {
    sigint = true;
    memcached_shutdown = true;
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
    sigterm = true;
    memcached_shutdown = true;
}

static struct event* sigterm_event;

static bool install_signal_handlers() {
    // SIGTERM - Used to shut down memcached cleanly
    sigterm_event = evsignal_new(
            main_base->getLibeventBase(), SIGTERM, sigterm_handler, nullptr);
    if (!sigterm_event) {
        LOG_WARNING_RAW("Failed to allocate SIGTERM handler");
        return false;
    }

    if (event_add(sigterm_event, nullptr) < 0) {
        LOG_WARNING_RAW("Failed to install SIGTERM handler");
        return false;
    }

    return true;
}

static void release_signal_handlers() {
    event_free(sigterm_event);
}
#endif

const char* get_server_version() {
    return PRODUCT_VERSION;
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
            bucket.destroyEngine(false);
            bucket.reset();
        }
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
    try {
        LOG_INFO_RAW("Initialize SASL");
        using namespace cb::sasl;
        logging::set_log_callback(sasl_log_callback);
        server::initialize();

        if (!getenv("MEMCACHED_UNIT_TESTS")) {
            // Speed up the unit tests ;)
            server::set_hmac_iteration_count(10);
        }
    } catch (std::exception& e) {
        FATAL_ERROR(EXIT_FAILURE,
                    fmt::format("Failed to initialize SASL: {}", e.what()));
    }
}

static void startExecutorPool() {
    auto& settings = Settings::instance();

    LOG_INFO(
            "Start executor pool with backend:Folly readers:{} writers:{} "
            "auxIO:{} nonIO:{}",
            settings.getNumReaderThreads(),
            settings.getNumWriterThreads(),
            settings.getNumAuxIoThreads(),
            settings.getNumNonIoThreads());

    ExecutorPool::create(
            ExecutorPool::Backend::Folly,
            0,
            ThreadPoolConfig::ThreadCount(settings.getNumReaderThreads()),
            ThreadPoolConfig::ThreadCount(settings.getNumWriterThreads()),
            settings.getNumAuxIoThreads(),
            settings.getNumNonIoThreads());
    ExecutorPool::get()->registerTaskable(NoBucketTaskable::instance());

    // MB-47484 Set up the settings callback for the executor pool now that
    // it is up'n'running
    settings.addChangeListener(
            "num_reader_threads", [](const std::string&, Settings& s) -> void {
                auto val =
                        ThreadPoolConfig::ThreadCount(s.getNumReaderThreads());
                // Update the ExecutorPool
                ExecutorPool::get()->setNumReaders(val);
            });
    settings.addChangeListener(
            "num_writer_threads", [](const std::string&, Settings& s) -> void {
                auto val =
                        ThreadPoolConfig::ThreadCount(s.getNumWriterThreads());
                // Update the ExecutorPool
                ExecutorPool::get()->setNumWriters(val);
                // Notify all buckets of the recent change
                BucketManager::instance().forEach([val](Bucket& b) -> bool {
                    b.getEngine().set_num_writer_threads(val);
                    return true;
                });
            });
    settings.addChangeListener(
            "num_auxio_threads", [](const std::string&, Settings& s) -> void {
                auto val = s.getNumAuxIoThreads();
                ExecutorPool::get()->setNumAuxIO(val);
            });
    settings.addChangeListener(
            "num_nonio_threads", [](const std::string&, Settings& s) -> void {
                auto val = s.getNumNonIoThreads();
                ExecutorPool::get()->setNumNonIO(val);
                BucketManager::instance().forEach([val](Bucket& b) -> bool {
                    b.getEngine().set_num_nonio_threads(val);
                    return true;
                });
            });
}

nlohmann::json cb::serverless::Config::to_json() const {
    nlohmann::json json;
    json["max_connections_per_bucket"] =
            maxConnectionsPerBucket.load(std::memory_order_acquire);
    json["read_compute_unit_size"] =
            readComputeUnitSize.load(std::memory_order_acquire);
    json["write_compute_unit_size"] =
            writeComputeUnitSize.load(std::memory_order_acquire);
    return json;
}

void cb::serverless::Config::update_from_json(const nlohmann::json& json) {
    maxConnectionsPerBucket.store(
            json.value("max_connections_per_bucket",
                       cb::serverless::MaxConnectionsPerBucket),
            std::memory_order_release);
    readComputeUnitSize.store(json.value("read_compute_unit_size",
                                         cb::serverless::ReadComputeUnitSize),
                              std::memory_order_release);
    writeComputeUnitSize.store(json.value("write_compute_unit_size",
                                          cb::serverless::WriteComputeUnitSize),
                               std::memory_order_release);
}

static void initialize_serverless_config() {
    const auto serverless =
            std::filesystem::path{Settings::instance().getRoot()} / "etc" /
            "couchbase" / "kv" / "serverless" / "configuration.json";
    auto& config = cb::serverless::Config::instance();
    if (exists(serverless)) {
        LOG_INFO("Using serverless static configuration from: {}",
                 serverless.generic_string());
        try {
            config.update_from_json(nlohmann::json::parse(
                    cb::io::loadFile(serverless.generic_string())));
        } catch (const std::exception& e) {
            LOG_WARNING("Failed to read serverless configuration: {}",
                        e.what());
        }
    }
    LOG_INFO("Serverless static configuration: {}", config.to_json().dump());
}

int memcached_main(int argc, char** argv) {
    // MB-14649 log() crash on windows on some CPU's
#ifdef _WIN64
    _set_FMA3_enable (0);
#endif

#ifdef HAVE_LIBNUMA
    // Configure NUMA policy as soon as possible (before any dynamic memory
    // allocation).
    const std::string numa_status = configure_numa_policy();
#endif

    environment.max_file_descriptors = cb::io::maximizeFileDescriptors(
            std::numeric_limits<uint32_t>::max());

    std::unique_ptr<ProcessMonitor> parent_monitor;

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

    // Set libevent to use our allocator before we do anything with libevent
    event_set_mem_functions(cb_malloc, cb_realloc, cb_free);

    setup_libevent_locking();

    initialize_openssl();
    cb::net::initialize();

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

    /* Configure file logger, if specified as a settings object */
    if (Settings::instance().has.logger) {
        auto ret =
                cb::logger::initialize(Settings::instance().getLoggerConfig());
        if (ret) {
            FATAL_ERROR(
                    EXIT_FAILURE, "Failed to initialize logger: {}", ret.value());
        }
    }

    /* File-based logging available from this point onwards... */
    Settings::instance().addChangeListener("verbosity",
                                           verbosity_changed_listener);

    // Logging available now extensions have been loaded.
    LOG_INFO("Couchbase version {} starting.", get_server_version());
    LOG_INFO("Process identifier: {}", getpid());

    if (folly::kIsSanitizeAddress) {
        LOG_INFO_RAW("Address sanitizer enabled");
    }

    if (folly::kIsSanitizeThread) {
        LOG_INFO_RAW("Thread sanitizer enabled");
    }

#if CB_DEVELOPMENT_ASSERTS
    LOG_INFO_RAW("Development asserts enabled");
#endif

#if defined(__x86_64__) || defined(_M_X64)
    if (!folly::CpuId().sse42()) {
        // MB-44941: hw crc32 is required, and is part of SSE4.2. If the CPU
        // does not support it, memcached would later crash with invalid
        // instruction exceptions.
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to initialise - CPU with SSE4.2 extensions is "
                    "required - terminating.");
    }
#endif

    try {
        cb::backtrace::initialize();
    } catch (const std::exception& e) {
        LOG_WARNING("Failed to initialize backtrace support: {}", e.what());
    }

    // Call recalculate_max_connections to make sure we put log the number
    // of file descriptors in the logfile
    recalculate_max_connections();
    // set up a callback to update it as part of parsing the configuration
    Settings::instance().addChangeListener(
            "max_connections", [](const std::string&, Settings& s) -> void {
                recalculate_max_connections();
            });

    /// Initialize breakpad crash catcher with our just-parsed settings
    Settings::instance().addChangeListener("breakpad",
                                           breakpad_changed_listener);
    cb::breakpad::initialize(Settings::instance().getBreakpadSettings(),
                             Settings::instance().getLoggerConfig());

    // Simple benchmark of clock performance - the system clock can have a
    // significant impact on opration latency (given we time all sorts of
    // things), so printing this at startup allows us to determine
    // if we have a "slow" clock or not..
    const auto fineClock =
            cb::estimateClockOverhead<std::chrono::steady_clock>();
    LOG_INFO("Fine clock overhead: {}", fineClock.overhead);
    const auto coarseClock =
            cb::estimateClockOverhead<folly::chrono::coarse_steady_clock>();
    LOG_INFO("Coarse clock overhead: {})", coarseClock.overhead);
    // Note: benchmark measurementPeriod is the same across all tested clocks,
    // to just report once.
    LOG_INFO("(Clock measurement period: {})", coarseClock.measurementPeriod);

    LOG_INFO("Using SLA configuration: {}", cb::mcbp::sla::to_json().dump());

    if (isServerlessDeployment()) {
        initialize_serverless_config();
    }

    if (Settings::instance().isStdinListenerEnabled()) {
        LOG_INFO_RAW("Enable standard input listener");
        start_stdin_listener([]() {
            LOG_INFO_RAW(
                    "Gracefully shutting down due to shutdown message or "
                    "stdin closure");
            shutdown_server();
        });
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

    try {
        const auto errormapPath =
                std::filesystem::path{Settings::instance().getErrorMapsDir()};
        LOG_INFO("Loading error maps from [{}]", errormapPath.generic_string());
        ErrorMapManager::initialize(errormapPath);
    } catch (const std::exception& e) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to load error maps: {}", e.what());
    }

    if (Settings::instance().has.prometheus_config) {
        try {
            prometheus_init(Settings::instance().getPrometheusConfig(),
                            prometheus_auth_callback);
        } catch (const std::exception& exception) {
            // Error message already formatted in the exception
            FATAL_ERROR(EXIT_FAILURE, "{}", exception.what());
        }
    } else {
        LOG_WARNING_RAW("Prometheus config not specified");
    }

    LOG_INFO_RAW("Starting external authentication manager");
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

    LOG_INFO_RAW("Initialize bucket manager");
    BucketManager::instance();

    initialize_sasl();

    /* initialize main thread libevent instance */
    main_base = std::make_unique<folly::EventBase>();

    cb::console::set_sigint_handler(sigint_handler);

    /* Initialize signal handlers (requires libevent). */
    if (!install_signal_handlers()) {
        FATAL_ERROR(EXIT_FAILURE, "Unable to install signal handlers");
    }

    /* initialize other stuff */
    stats_init();

#ifndef WIN32
    // ignore SIGPIPE signals
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to ignore SIGPIPE");
    }
#endif

    LOG_INFO_RAW("Starting network interface manager");
    networkInterfaceManager = std::make_unique<NetworkInterfaceManager>(
            *main_base, prometheus_auth_callback);
    networkInterfaceManager->createBootstrapInterface();

    /* start up worker threads if MT mode */
    worker_threads_init();

    LOG_INFO(R"(Starting Phosphor tracing with config: "{}")",
             Settings::instance().getPhosphorConfig());
    initializeTracing(Settings::instance().getPhosphorConfig());
    TRACE_GLOBAL0("memcached", "Started");

    startExecutorPool();

    // Schedule the StaleTraceRemover
    startStaleTraceDumpRemover(std::chrono::minutes(1),
                               std::chrono::minutes(5));

    /* Initialise memcached time keeping */
    mc_time_init(main_base->getLibeventBase());

    // Optional parent monitor
    {
        const int parent = Settings::instance().getParentIdentifier();
        if (parent != -1) {
            LOG_INFO_RAW("Starting parent monitor");
            parent_monitor = ProcessMonitor::create(
                    parent,
                    [parent](auto&) {
                        std::cerr << "Parent process " << parent
                                  << " died. Exiting" << std::endl;
                        std::cerr.flush();
                        std::_Exit(EXIT_FAILURE);
                    },
                    "mc:parent_mon");
        }
    }

    if (!memcached_shutdown) {
        /* enter the event loop */
        LOG_INFO_RAW("Initialization complete. Accepting clients.");
        main_base->loopForever();
    }

    LOG_INFO("Initiating graceful shutdown. sigint:{} sigterm:{}",
             sigint,
             sigterm);
    BucketManager::instance().destroyAll();

    if (parent_monitor) {
        LOG_INFO_RAW("Shutting down parent monitor");
        parent_monitor.reset();
    }

    // Shut down Prometheus (it adds its own log message)
    cb::prometheus::shutdown();

    LOG_INFO_RAW("Shutting down audit daemon");
    shutdown_audit();

    LOG_INFO_RAW("Shutting down client worker threads");
    threads_shutdown();

    LOG_INFO_RAW("Releasing server sockets");
    networkInterfaceManager.reset();

    LOG_INFO_RAW("Releasing bucket resources");
    cleanup_buckets();

    LOG_INFO_RAW("Shutting down RBAC subsystem");
    cb::rbac::destroy();

    LOG_INFO_RAW("Shutting down executor pool");
    ExecutorPool::get()->unregisterTaskable(NoBucketTaskable::instance(),
                                            false);
    ExecutorPool::shutdown();

    LOG_INFO_RAW("Releasing signal handlers");
    release_signal_handlers();

    LOG_INFO_RAW("Shutting down external authentication service");
    externalAuthManager->shutdown();
    externalAuthManager->waitForState(Couchbase::ThreadState::Zombie);
    externalAuthManager.reset();

    LOG_INFO_RAW("Shutting down SASL server");
    cb::sasl::server::shutdown();

    LOG_INFO_RAW("Deinitialising tracing");
    deinitializeTracing();

    LOG_INFO_RAW("Shutting down all engines");
    shutdown_all_engines();

    LOG_INFO_RAW("Removing breakpad");
    cb::breakpad::destroy();

    LOG_INFO_RAW("Shutting down OpenSSL");
    shutdown_openssl();

    LOG_INFO_RAW("Shutting down event base");
    main_base.reset();

    if (OpenTelemetry::isEnabled()) {
        LOG_INFO_RAW("Shutting down OpenTelemetry");
        OpenTelemetry::shutdown();
    }

    LOG_INFO_RAW("Shutting down logger extension");
    cb::logger::shutdown();

    ErrorMapManager::shutdown();

    return EXIT_SUCCESS;
}
