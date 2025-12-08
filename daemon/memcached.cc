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
#include "bucket_manager.h"
#include "buckets.h"
#include "cmdline.h"
#include "concurrency_semaphores.h"
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
#include "network_interface_manager_thread.h"
#include "nobucket_taskable.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "settings.h"
#include "stats.h"
#include "stdin_check.h"
#include "tracing.h"
#include "utilities/terminate_handler.h"
#include <cbsasl/mechanism.h>
#include <dek/manager.h>
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
#include <platform/json_log_conversions.h>
#include <platform/process_monitor.h>
#include <platform/scope_timer.h>
#include <platform/strerror.h>
#include <platform/sysinfo.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <statistics/prometheus.h>
#include <utilities/breakpad.h>
#include <utilities/global_concurrency_semaphores.h>
#include <utilities/magma_support.h>
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

using namespace std::chrono_literals;

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
GlobalStatistics global_statistics;

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

static void populate_log_level() {
    const auto val = Settings::instance().getLogLevel();

    // Log the verbosity value set by the user and not the log level as they
    // are inverted
    LOG_INFO_CTX("Changing logging level",
                 {"level", Settings::instance().getVerbose()});
    cb::logger::setLogLevels(val);
}

static void stats_init() {
    setStatsResetTime();
    global_statistics.conn_structs.reset();
    global_statistics.total_conns.reset();
    global_statistics.rejected_conns.reset();
    global_statistics.curr_conns = 0;
    global_statistics.curr_conn_closing = 0;
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

struct thread_stats* get_thread_stats(Connection* c) {
    auto& independent_stats = c->getBucket().stats;
    return &independent_stats.at(c->getThread().index);
}

void stats_reset(Cookie& cookie) {
    setStatsResetTime();
    global_statistics.total_conns.reset();
    global_statistics.rejected_conns.reset();
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
        LOG_WARNING_CTX("Reducing max_connections",
                        {"reason",
                         "max_connections is set higher than the available "
                         "number of file descriptors available"},
                        {"to", newmax});

        if (settings.getSystemConnections() > newmax) {
            LOG_WARNING_CTX("Reducing system_connections",
                            {"from", settings.getSystemConnections()},
                            {"to", newmax / 2},
                            {"max_connections", newmax});
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

    LOG_INFO_CTX("recalculate_max_connections",
                 {"max_fds", environment.max_file_descriptors.load()},
                 {"max_connections", settings.getMaxConnections()},
                 {"system_connections", settings.getSystemConnections()},
                 {"engine_fds", environment.engine_file_descriptors.load()});
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

static void opcode_attributes_override_changed_listener(const std::string&,
                                                        Settings& s) {
    try {
        cb::mcbp::sla::reconfigure(
                Settings::instance().getRoot(),
                nlohmann::json::parse(s.getOpcodeAttributesOverride()));
    } catch (const std::exception&) {
        cb::mcbp::sla::reconfigure(Settings::instance().getRoot());
    }
    LOG_INFO_CTX("SLA configuration changed", cb::mcbp::sla::to_json());
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
        return "NOT setting memory allocation policy - disabled "
               "via MEMCACHED_NUMA_MEM_POLICY='disable'";
    }
    errno = 0;
    numa_set_interleave_mask(numa_all_nodes_ptr);
    if (errno == 0) {
        return "Set memory allocation policy to 'interleave'";
    }
    return std::string(
                   "NOT setting memory allocation policy to "
                   "'interleave' - request failed: ") +
           cb_strerror();
}
#endif  // HAVE_LIBNUMA

static void settings_init() {
    auto& settings = Settings::instance();

    // Set up the listener functions
    settings.addChangeListener(
            "abrupt_shutdown_timeout", [](const std::string&, Settings& s) {
                abrupt_shutdown_timeout_changed(s.getAbruptShutdownTimeout());
            });

    settings.addChangeListener(
            "scramsha_fallback_salt", [](const std::string&, Settings& s) {
                cb::sasl::pwdb::UserFactory::setScramshaFallbackSalt(
                        s.getScramshaFallbackSalt());
            });
    settings.addChangeListener(
            "scramsha_fallback_iteration_count",
            [](const std::string&, Settings& s) {
                cb::sasl::pwdb::UserFactory::setDefaultScramShaIterationCount(
                        s.getScramshaFallbackIterationCount());
            });
    settings.addChangeListener(
            "active_external_users_push_interval",
            [](const std::string&, Settings& s) -> void {
                if (externalAuthManager) {
                    externalAuthManager->setPushActiveUsersInterval(
                            s.getActiveExternalUsersPushInterval());
                }
            });
    settings.addChangeListener(
            "external_auth_slow_duration",
            [](const std::string&, Settings& s) -> void {
                if (externalAuthManager) {
                    externalAuthManager->setExternalAuthSlowDuration(
                            s.getExternalAuthSlowDuration());
                }
            });
    settings.addChangeListener(
            "external_auth_request_timeout",
            [](const std::string&, Settings& s) -> void {
                if (externalAuthManager) {
                    externalAuthManager->setExternalAuthRequestTimeout(
                            s.getExternalAuthRequestTimeout());
                }
            });
    settings.setVerbose(0);
    settings.setNumWorkerThreads(get_number_of_worker_threads());

    settings.addChangeListener(
            "num_storage_threads", [](const std::string&, Settings& s) -> void {
                auto val = ThreadPoolConfig::StorageThreadCount(
                        s.getNumStorageThreads());
                BucketManager::instance().forEach([val](Bucket& b) -> bool {
                    if (b.type != BucketType::ClusterConfigOnly) {
                        b.getEngine().set_num_storage_threads(val);
                    }
                    return true;
                });
            });

    settings.addChangeListener(
            "max_concurrent_authentications", [](const auto&, auto& s) {
                ConcurrencySemaphores::instance().authentication.setCapacity(
                        s.getMaxConcurrentAuthentications());
            });

    settings.addChangeListener("fusion_migration_rate_limit",
                               [](const auto&, auto& s) {
                                   magma::Magma::SetFusionMigrationRateLimit(
                                           s.getFusionMigrationRateLimit());
                               });
    settings.addChangeListener("fusion_sync_rate_limit",
                               [](const auto&, auto& s) {
                                   magma::Magma::SetFusionSyncRateLimit(
                                           s.getFusionSyncRateLimit());
                               });
    settings.addChangeListener(
            "fusion_num_uploader_threads", [](const auto&, auto& s) {
                magma::Magma::SetNumThreads(magma::Magma::FusionUploader,
                                            s.getFusionNumUploaderThreads());
            });
    settings.addChangeListener(
            "fusion_num_migrator_threads", [](const auto&, auto& s) {
                magma::Magma::SetNumThreads(magma::Magma::FusionMigrator,
                                            s.getFusionNumMigratorThreads());
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
        FATAL_ERROR_CTX(EXIT_FAILURE,
                        "Failed to load SLA configuration",
                        {"error", e.what()});
    }

    settings.addChangeListener(
            "opcode_attributes_override",
            opcode_attributes_override_changed_listener);
    cb::serverless::setEnabled(Settings::instance().getDeploymentModel() ==
                               DeploymentModel::Serverless);
    settings.addChangeListener(
            "external_auth_service_scram_support",
            [](const std::string&, Settings& s) -> void {
                cb::sasl::server::set_using_external_auth_service(
                        s.doesExternalAuthServiceSupportScram());
            });
}

static bool safe_close(SOCKET sfd) {
    if (sfd == INVALID_SOCKET) {
        return false;
    }

    int rval;

    do {
        rval = evutil_closesocket(sfd);
    } while (rval == SOCKET_ERROR &&
             cb::net::is_interrupted(cb::net::get_socket_error()));

    if (rval == SOCKET_ERROR) {
        std::string error = cb_strerror();
        LOG_WARNING_CTX(
                "Failed to close socket", {"fd", (int)sfd}, {"error", error});
        return false;
    }
    return true;
}

void close_server_socket(SOCKET sfd) {
    safe_close(sfd);
}

void close_client_socket(SOCKET sfd) {
    if (safe_close(sfd)) {
        --global_statistics.curr_conns;
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
        c.shutdown();
        if (memcached_shutdown) {
            c.setTerminationReason("The system is shutting down");
        } else {
            c.setTerminationReason("The connected bucket is being deleted");
        }
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

static void initialize_sasl() {
    try {
        LOG_INFO_RAW("Initialize SASL");
        cb::sasl::server::initialize();
        if (Settings::instance().doesExternalAuthServiceSupportScram()) {
            LOG_INFO_RAW("Proxy SCRAM-SHA for unknown users to external users");
        }
        cb::sasl::server::set_using_external_auth_service(
                Settings::instance().doesExternalAuthServiceSupportScram());
    } catch (std::exception& e) {
        FATAL_ERROR_CTX(
                EXIT_FAILURE, "Failed to initialize SASL", {"error", e.what()});
    }
}

static void startExecutorPool() {
    auto& settings = Settings::instance();

    ExecutorPool::create(
            ExecutorPool::Backend::Folly,
            0,
            ThreadPoolConfig::ThreadCount(settings.getNumReaderThreads()),
            ThreadPoolConfig::ThreadCount(settings.getNumWriterThreads()),
            ThreadPoolConfig::AuxIoThreadCount(settings.getNumAuxIoThreads()),
            ThreadPoolConfig::NonIoThreadCount(settings.getNumNonIoThreads()),
            ThreadPoolConfig::IOThreadsPerCore(
                    settings.getNumIOThreadsPerCore()));
    ExecutorPool::get()->registerTaskable(NoBucketTaskable::instance());
    ExecutorPool::get()->setDefaultTaskable(NoBucketTaskable::instance());

    auto* pool = ExecutorPool::get();
    LOG_INFO_CTX("Started executor pool",
                 {"backend", pool->getName()},
                 {"readers", pool->getNumReaders()},
                 {"writers", pool->getNumWriters()},
                 {"auxIO", pool->getNumAuxIO()},
                 {"nonIO", pool->getNumNonIO()});

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
                BucketManager::instance().forEach([](Bucket& b) -> bool {
                    // Notify all buckets of the recent change
                    if (b.type != BucketType::ClusterConfigOnly) {
                        b.getEngine().notify_num_writer_threads_changed();
                    }
                    return true;
                });
            });
    settings.addChangeListener(
            "num_auxio_threads", [](const std::string&, Settings& s) -> void {
                auto val = ThreadPoolConfig::AuxIoThreadCount(
                        s.getNumAuxIoThreads());
                ExecutorPool::get()->setNumAuxIO(val);
                BucketManager::instance().forEach([](Bucket& b) -> bool {
                    // Notify all buckets of the recent change
                    if (b.type != BucketType::ClusterConfigOnly) {
                        b.getEngine().notify_num_auxio_threads_changed();
                    }
                    return true;
                });
            });
    settings.addChangeListener(
            "num_nonio_threads", [](const std::string&, Settings& s) -> void {
                auto val = ThreadPoolConfig::NonIoThreadCount(
                        s.getNumNonIoThreads());
                ExecutorPool::get()->setNumNonIO(val);
            });
    settings.addChangeListener(
            "num_io_thread_per_core", [](const std::string&, Settings& s) {
                // Update the ExecutorPool with the new coefficient, then
                // recalculate the number of AuxIO threads.
                auto* executorPool = ExecutorPool::get();
                executorPool->setNumIOThreadPerCore(
                        ThreadPoolConfig::IOThreadsPerCore{
                                s.getNumIOThreadsPerCore()});
                executorPool->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{
                        s.getNumAuxIoThreads()});
                // Notify engines of change in AuxIO thread count.
                BucketManager::instance().forEach([](Bucket& b) {
                    if (b.type != BucketType::ClusterConfigOnly) {
                        b.getEngine().notify_num_auxio_threads_changed();
                    }
                    return true;
                });
            });

    if (pool->getNumAuxIO() > 3) {
        GlobalConcurrencySemaphores::instance().download_snapshot.setCapacity(
                pool->getNumAuxIO() - 2);
    }
}

static void initialize_serverless_config() {
    const auto serverless =
            std::filesystem::path{Settings::instance().getRoot()} / "etc" /
            "couchbase" / "kv" / "serverless" / "configuration.json";
    auto& config = cb::serverless::Config::instance();
    if (exists(serverless)) {
        LOG_INFO_CTX("Using serverless static configuration",
                     {"path", serverless.generic_string()});
        try {
            config.update_from_json(
                    nlohmann::json::parse(cb::io::loadFile(serverless)));
        } catch (const std::exception& e) {
            LOG_WARNING_CTX("Failed to read serverless configuration",
                            {"error", e.what()});
        }
    }
    LOG_INFO_CTX("Serverless static configuration", nlohmann::json(config));
}

void disconnect_clients() {
    size_t num_clients = 0;
    auto next_log = cb::time::steady_clock::now() + std::chrono::minutes(1);
    bool first_time = true;
    do {
        num_clients = 0;
        bool dump_details = next_log < cb::time::steady_clock::now();
        if (dump_details) {
            next_log = cb::time::steady_clock::now() + std::chrono::minutes(1);
        }
        iterate_all_connections([&num_clients, &dump_details](auto& c) {
            ++num_clients;
            if (c.maybeInitiateShutdown("System is shutting down", false) &&
                dump_details) {
                LOG_INFO_CTX("Waiting for connection to shut down",
                             {"details", c.to_json()});
            }
        });
        if (num_clients) {
            if (first_time || dump_details) {
                first_time = false;
                LOG_INFO_CTX("Waiting for all clients to disconnect",
                             {"connected", num_clients});
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    } while (num_clients != 0);
    LOG_INFO_RAW("All clients disconnected");
}

int memcached_main(int argc, char** argv) {
    // Pause the process on startup (makes attaching debugger easier).
#ifndef WIN32
    if (getenv("MEMCACHED_DEBUG_STOP")) {
        raise(SIGSTOP);
    }
#endif

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

    cb::net::initialize();

    // We don't have the infrastructure to pass the keys on via stdin
    // so lets just use an environment variable for now
    if (getenv("MEMCACHED_UNIT_TESTS")) {
        if (const char* env = getenv("BOOTSTRAP_DEK"); env != nullptr) {
            try {
                cb::dek::Manager::instance().reset(nlohmann::json::parse(env));
            } catch (const std::exception& exception) {
                FATAL_ERROR_CTX(
                        EXIT_FAILURE,
                        "Failed to initialize unit tests bootstrap keys",
                        {"error", exception.what()});
            }
        }
    }

    /* init settings */
    settings_init();

    initialize_mbcp_lookup_map();

    /* Parse command line arguments */
    try {
        parse_arguments(argc, argv);
    } catch (const std::exception& exception) {
        FATAL_ERROR_CTX(EXIT_FAILURE,
                        "Failed to initialize server",
                        {"error", exception.what()});
    }

    update_settings_from_config();

    cb::rbac::initialize();

    /* Configure file logger, if specified as a settings object */
    if (Settings::instance().has.logger) {
        auto ret =
                cb::logger::initialize(Settings::instance().getLoggerConfig());
        if (ret) {
            FATAL_ERROR_CTX(EXIT_FAILURE,
                            "Failed to initialize logger",
                            {"error", ret.value()});
        }
    }

    /* File-based logging available from this point onwards... */
    Settings::instance().addChangeListener("verbosity",
                                           verbosity_changed_listener);

    // Logging available now extensions have been loaded.
    LOG_INFO_CTX("Couchbase version starting",
                 {"version", get_server_version()});
    LOG_INFO_CTX("Process identifier", {"pid", getpid()});

    if (folly::kIsSanitizeAddress) {
        LOG_INFO_RAW("Address sanitizer enabled");
    }

    if (folly::kIsSanitizeThread) {
        LOG_INFO_RAW("Thread sanitizer enabled");
    }

#if CB_DEVELOPMENT_ASSERTS
    LOG_INFO_RAW("Development asserts enabled");
    if (cb_malloc_is_using_arenas() == 0) {
        throw std::runtime_error(
                "memory tracking is not detected when calling "
                "cb_malloc_is_using_arenas");
    }
#endif

    {
        nlohmann::json encryption = nlohmann::json::object();
        cb::dek::Manager::instance().iterate(
                [&encryption](auto entity, const auto& ks) {
                    encryption[cb::dek::format_as(entity)] =
                            cb::crypto::toLoggableJson(ks);
                });
        if (!encryption.empty()) {
            LOG_INFO_CTX("Data encryption", {"config", encryption});
        }
    }

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
        LOG_WARNING_CTX("Failed to initialize backtrace support",
                        {"error", e.what()});
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
    const auto fineClockResolution =
            cb::estimateClockResolution<std::chrono::steady_clock>();
    LOG_INFO_CTX("Clock",
                 {"type", "fine"},
                 {"resolution", fineClockResolution},
                 {"overhead", fineClock.overhead},
                 {"period", fineClock.measurementPeriod});
    const auto coarseClock =
            cb::estimateClockOverhead<folly::chrono::coarse_steady_clock>();
    const auto coarseClockResolution =
            cb::estimateClockResolution<folly::chrono::coarse_steady_clock>();
    // Note: benchmark measurementPeriod is the same across all tested clocks,
    // to just report once.
    LOG_INFO_CTX("Clock",
                 {"type", "coarse"},
                 {"resolution", coarseClockResolution},
                 {"overhead", coarseClock.overhead},
                 {"period", coarseClock.measurementPeriod});

    LOG_INFO_CTX("Using SLA configuration", cb::mcbp::sla::to_json());

    if (cb::serverless::isEnabled()) {
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
    LOG_INFO_CTX("NUMA", {"policy", numa_status});
#endif

    if (!Settings::instance().has.rbac_file) {
        FATAL_ERROR(EXIT_FAILURE, "RBAC file not specified");
    }

    if (!cb::io::isFile(Settings::instance().getRbacFile())) {
        FATAL_ERROR_CTX(EXIT_FAILURE,
                        "RBAC file does not exist",
                        {"path", Settings::instance().getRbacFile()});
    }

    LOG_INFO_CTX("Loading RBAC configuration",
                 {"path", Settings::instance().getRbacFile()});
    try {
        cb::rbac::loadPrivilegeDatabase(Settings::instance().getRbacFile());
    } catch (const std::exception& exception) {
        // We can't run without a privilege database
        FATAL_ERROR_CTX(EXIT_FAILURE,
                        "Failed to load RBAC database",
                        {"error", exception.what()});
    }

    try {
        const auto errormapPath =
                std::filesystem::path{Settings::instance().getErrorMapsDir()};
        LOG_INFO_CTX("Loading error maps",
                     {"path", errormapPath.generic_string()});
        ErrorMapManager::initialize(errormapPath);
    } catch (const std::exception& e) {
        FATAL_ERROR_CTX(
                EXIT_FAILURE, "Failed to load error maps", {"error", e.what()});
    }

    LOG_INFO_RAW("Starting external authentication manager");
    externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
    externalAuthManager->setPushActiveUsersInterval(
            Settings::instance().getActiveExternalUsersPushInterval());
    externalAuthManager->setExternalAuthSlowDuration(
            Settings::instance().getExternalAuthSlowDuration());
    externalAuthManager->setExternalAuthRequestTimeout(
            Settings::instance().getExternalAuthRequestTimeout());
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

    auto nim_thread = std::make_unique<NetworkInterfaceManagerThread>(
            prometheus_auth_callback);
    worker_threads_init();

    LOG_INFO_CTX("Starting Phosphor tracing",
                 {"config", Settings::instance().getPhosphorConfig()});
    initializeTracing(Settings::instance().getPhosphorConfig());
    TRACE_GLOBAL0("memcached", "Started");

    startExecutorPool();

    // Schedule the StaleTraceRemover
    startStaleTraceDumpRemover(std::chrono::minutes(1),
                               std::chrono::minutes(5));

    /* enable system clock monitoring */
    cb::time::UptimeClock::instance().configureSystemClockCheck(
            std::chrono::seconds(60), std::chrono::seconds(1));
    /* enable steady clock monitoring, this will warn if the time thread was
       delayed (assuming steady clock does what it should!)*/
    cb::time::UptimeClock::instance().configureSteadyClockCheck(
            std::chrono::milliseconds(100));
    /* Create memcached time Regulator and begin periodic ticking */
    cb::time::Regulator::createAndRun(*main_base, 100ms);

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

    LOG_INFO_RAW("Initialization complete. Accepting clients.");
    nim_thread->start();

    if (!memcached_shutdown) {
        main_base->loopForever();
    }

    LOG_INFO_CTX("Initiating graceful shutdown",
                 {"sigint", sigint},
                 {"sigterm", sigterm});

    // We are shutting down the server, and at this time we still accept
    // new connections, but they are immediately closed and not dispatched
    // to the worker threads
    //
    // In this mode we won't accept new commands to be started, but we
    // wait for all commands currently being executed to complete.
    //
    // Start by deleting all buckets, and as part of initiating bucket
    // deletion we'll cancel all sync writes and disconnect those connections
    BucketManager::instance().destroyAll();

    // There may however be connections which are not associated with a bucket,
    // so we should wait for all of those to disconnect as well
    LOG_INFO_RAW("Disconnect all clients");
    disconnect_clients();

    // At this time we should have no connections left, so we can go ahead
    // and shut down the parent monitor
    if (parent_monitor) {
        LOG_INFO_RAW("Shutting down parent monitor");
        parent_monitor.reset();
    }

    // Shut down Prometheus (it adds its own log message)
    cb::prometheus::shutdown();

    // There is no ongoing commands, so it is safe to shut down the
    // network interface manager thread. This will also close the listening
    // socket. We've been closing the newly created connections immediately
    // since we detected the start of the shutdown sequence.
    LOG_INFO_RAW("Shutting down network interface manager thread");
    nim_thread->shutdown();
    nim_thread->waitForState(Couchbase::ThreadState::Zombie);
    nim_thread.reset();

    LOG_INFO_RAW("Shutting down client worker threads");
    threads_shutdown();

    LOG_INFO_RAW("Releasing bucket resources");
    BucketManager::instance().shutdown();

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

    LOG_INFO_RAW("Shutting down audit daemon");
    shutdown_audit();

    LOG_INFO_RAW("Removing breakpad");
    cb::breakpad::destroy();

    LOG_INFO_RAW("Shutting down event base");
    main_base.reset();

    LOG_INFO_RAW("Shutting down logger extension");
    cb::logger::shutdown();

    ErrorMapManager::shutdown();

    return EXIT_SUCCESS;
}
