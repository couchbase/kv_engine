/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "settings.h"
#include "log_macros.h"
#include "ssl_utils.h"
#include <fmt/chrono.h>
#include <mcbp/mcbp.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/trace_config.h>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <platform/json_log_conversions.h>
#include <platform/timeutils.h>
#include <utilities/fusion_support.h>
#include <utilities/json_utilities.h>
#include <utilities/logtags.h>
#include <algorithm>
#include <cstring>
#include <regex>
#include <system_error>

Settings::Settings() = default;
Settings::~Settings() = default;

Settings::Settings(const nlohmann::json& json) {
    reconfigure(json);
}

Settings& Settings::instance() {
    static Settings settings;
    return settings;
}

std::string storageThreadConfig2String(int val) {
    if (val == 0) {
        return "default";
    }

    return std::to_string(val);
}

std::string threadConfig2String(int val) {
    if (val == -1) {
        return "disk_io_optimized";
    }

    return storageThreadConfig2String(val);
}

static int parseThreadConfigSpec(const std::string& variable,
                                 const std::string& spec) {
    if (spec == "disk_io_optimized") {
        return -1;
    }
    if (spec == "default" || spec == "balanced") {
        // balanced is the new name for the default value. We continue
        // to support default for backwards compatibility.
        return 0;
    }
    int32_t val;
    if (!safe_strtol(spec, val)) {
        throw std::invalid_argument(
                variable +
                R"( must be specified as a numeric value, "balanced" or "disk_io_optimized")");
    }

    return val;
}

void Settings::setMaxConcurrentAuthentications(size_t val) {
    if (val < 1) {
        throw std::invalid_argument(
                "max_concurrent_authentications must be at least 1");
    }
    max_concurrent_authentications.store(val, std::memory_order_release);
    has.max_concurrent_authentications = true;
    notify_changed("max_concurrent_authentications");
}

void Settings::setDcpDisconnectWhenStuckTimeout(std::chrono::seconds val) {
    dcp_disconnect_when_stuck_timeout_seconds.store(val,
                                                    std::memory_order_release);
    has.dcp_disconnect_when_stuck_timeout_seconds = true;
    notify_changed("dcp_disconnect_when_stuck_timeout_seconds");
}

void Settings::setDcpDisconnectWhenStuckNameRegexFromBase64(
        const std::string& val) {
    // The string from the JSON is base64 encoded, decode and store the regex so
    // that it is immediately usable from the getter.
    setDcpDisconnectWhenStuckNameRegex(cb::base64::decode(val));
}

void Settings::setDcpDisconnectWhenStuckNameRegex(std::string val) {
    // This must be usable by std::regex, which will throw if not.
    try {
        std::regex regex(val);
    } catch (const std::exception& e) {
        // Log that we are ignoring the bad regex. If we throw here ns_server
        // may still push the "bad" config, and this throw would cause an outage
        // if we restart.
        LOG_WARNING(
                "dcp_disconnect_when_stuck_name_regex ignoring \"{}\" and "
                "using \"{}\". std::regex rejected the new value because "
                "\"{}\"",
                val,
                *dcp_disconnect_when_stuck_name_regex.lock(),
                e.what());
        return;
    }
    dcp_disconnect_when_stuck_name_regex.lock()->assign(std::move(val));
    has.dcp_disconnect_when_stuck_name_regex = true;
    notify_changed("dcp_disconnect_when_stuck_name_regex");
}

void Settings::reconfigure(const nlohmann::json& json) {
    // Nuke the default interface added to the system in settings_init and
    // use the ones in the configuration file (this is a bit messy).
    interfaces.wlock()->clear();

    for (const auto& obj : json.items()) {
        const auto key = obj.key();
        const auto value = obj.value();
        using namespace std::string_view_literals;

        if (key == "rbac_file"sv) {
            setRbacFile(value.get<std::string>());
        } else if (key == "audit_file"sv) {
            setAuditFile(value.get<std::string>());
        } else if (key == "deployment_model"sv) {
            if (value.get<std::string>() == "serverless") {
                setDeploymentModel(DeploymentModel::Serverless);
            }
        } else if (key == "error_maps_dir"sv) {
            setErrorMapsDir(value.get<std::string>());
        } else if (key == "enable_deprecated_bucket_autoselect"sv) {
            setDeprecatedBucketAutoselectEnabled(value.get<bool>());
        } else if (key == "quota_sharing_pager_concurrency_percentage") {
            auto val = value.get<int>();
            if (val <= 0 || val > 100) {
                throw std::invalid_argument(
                        "\"quota_sharing_pager_concurrency_percentage\" must "
                        "be a valid non-zero percentage");
            }
            setQuotaSharingPagerConcurrencyPercentage(val);
        } else if (key == "quota_sharing_pager_sleep_time_ms") {
            auto val = value.get<int>();
            if (val <= 0) {
                throw std::invalid_argument(
                        "\"quota_sharing_pager_sleep_time_ms\" must "
                        "be a valid duration in milliseconds");
            }
            setQuotaSharingPagerSleepTime(std::chrono::milliseconds(val));
        } else if (key == "threads"sv) {
            setNumWorkerThreads(value.get<size_t>());
        } else if (key == "interfaces") {
            if (value.type() != nlohmann::json::value_t::array) {
                cb::throwJsonTypeError("\"interfaces\" must be an array");
            }
            for (const auto& o : value) {
                auto ifc = o.get<NetworkInterface>();
                if (ifc.port == 0 && ifc.tag.empty()) {
                    throw std::invalid_argument(
                            "Ephemeral ports must have a tag");
                }
                addInterface(ifc);
            }
        } else if (key == "logger"sv) {
            setLoggerConfig(value.get<cb::logger::Config>());
        } else if (key == "default_reqs_per_event"sv) {
            setRequestsPerEventNotification(value.get<int>(),
                                            EventPriority::Default);
        } else if (key == "reqs_per_event_high_priority"sv) {
            setRequestsPerEventNotification(value.get<int>(),
                                            EventPriority::High);
        } else if (key == "reqs_per_event_med_priority"sv) {
            setRequestsPerEventNotification(value.get<int>(),
                                            EventPriority::Medium);
        } else if (key == "reqs_per_event_low_priority"sv) {
            setRequestsPerEventNotification(value.get<int>(),
                                            EventPriority::Low);
        } else if (key == "command_time_slice"sv) {
            setCommandTimeSlice(
                    std::chrono::milliseconds(value.get<uint32_t>()));
        } else if (key == "verbosity"sv) {
            setVerbose(value.get<int>());
        } else if (key == "connection_idle_time"sv) {
            setConnectionIdleTime(value.get<unsigned int>());
        } else if (key == "datatype_json"sv) {
            setDatatypeJsonEnabled(value.get<bool>());
        } else if (key == "datatype_snappy"sv) {
            setDatatypeSnappyEnabled(value.get<bool>());
        } else if (key == "root"sv) {
            auto dir = value.get<std::string>();

            if (!cb::io::isDirectory(dir)) {
                throw std::system_error(
                        std::make_error_code(
                                std::errc::no_such_file_or_directory),
                        "'root': '" + dir + "'");
            }

            setRoot(dir);
        } else if (key == "breakpad"sv) {
            auto settings = value.get<cb::breakpad::Settings>();
            settings.validate();
            setBreakpadSettings(settings);
        } else if (key == "max_packet_size"sv) {
            setMaxPacketSize(value.get<uint32_t>() * uint32_t(1024) *
                             uint32_t(1024));
        } else if (key == "max_send_queue_size"sv) {
            setMaxSendQueueSize(value.get<size_t>() * 1_MiB);
        } else if (key == "max_so_sndbuf_size"sv) {
            setMaxSoSndbufSize(value.get<uint32_t>());
        } else if (key == "max_connections"sv) {
            setMaxConnections(value.get<size_t>());
        } else if (key == "system_connections"sv) {
            setSystemConnections(value.get<size_t>());
        } else if (key == "sasl_mechanisms"sv) {
            setSaslMechanisms(value.get<std::string>());
        } else if (key == "ssl_sasl_mechanisms"sv) {
            setSslSaslMechanisms(value.get<std::string>());
        } else if (key == "stdin_listener"sv) {
            setStdinListenerEnabled(value.get<bool>());
        } else if (key == "clustermap_push_notifications_enabled"sv) {
            setClustermapPushNotificationsEnabled(value.get<bool>());
        } else if (key == "dedupe_nmvb_maps"sv) {
            setDedupeNmvbMaps(value.get<bool>());
        } else if (key == "tcp_keepalive_idle"sv) {
            setTcpKeepAliveIdle(std::chrono::seconds(value.get<uint32_t>()));
        } else if (key == "tcp_keepalive_interval"sv) {
            setTcpKeepAliveInterval(
                    std::chrono::seconds(value.get<uint32_t>()));
        } else if (key == "tcp_keepalive_probes"sv) {
            setTcpKeepAliveProbes(value.get<uint32_t>());
        } else if (key == "tcp_user_timeout"sv) {
            setTcpUserTimeout(std::chrono::seconds(value.get<uint32_t>()));
        } else if (key == "tcp_unauthenticated_user_timeout"sv) {
            setTcpUnauthenticatedUserTimeout(
                    std::chrono::seconds(value.get<uint32_t>()));
        } else if (key == "xattr_enabled"sv) {
            setXattrEnabled(value.get<bool>());
        } else if (key == "client_cert_auth"sv) {
            auto config = cb::x509::ClientCertConfig::create(value);
            reconfigureClientCertAuth(std::move(config));
        } else if (key == "collections_enabled"sv) {
            setCollectionsEnabled(value.get<bool>());
        } else if (key == "opcode_attributes_override"sv) {
            setOpcodeAttributesOverride(value.dump());
        } else if (key == "num_reader_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumReaderThreads(value.get<int>());
            } else {
                const auto val = value.get<std::string>();
                setNumReaderThreads(
                        parseThreadConfigSpec("num_reader_threads", val));
            }
        } else if (key == "num_writer_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumWriterThreads(value.get<int>());
            } else {
                const auto val = value.get<std::string>();
                setNumWriterThreads(
                        parseThreadConfigSpec("num_writer_threads", val));
            }
        } else if (key == "num_storage_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumStorageThreads(value.get<int>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumStorageThreads(static_cast<int>(
                        ThreadPoolConfig::StorageThreadCount::Default));
            } else {
                throw std::invalid_argument(fmt::format(
                        "Value to set number of storage threads must be an "
                        "unsigned integer or \"default\"! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "num_auxio_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumAuxIoThreads(value.get<int>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumAuxIoThreads(static_cast<int>(
                        ThreadPoolConfig::AuxIoThreadCount::Default));
            } else {
                throw std::invalid_argument(fmt::format(
                        "Value to set number of AuxIO threads must be an "
                        "unsigned integer or \"default\"! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "num_nonio_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumNonIoThreads(value.get<int>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumNonIoThreads(static_cast<int>(
                        ThreadPoolConfig::NonIoThreadCount::Default));
            } else {
                throw std::invalid_argument(fmt::format(
                        "Value to set number of NonIO threads must be an "
                        "unsigned integer or \"default\"!! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "num_io_threads_per_core"sv) {
            if (value.is_number_unsigned()) {
                setNumIOThreadsPerCore(value.get<int>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumIOThreadsPerCore(std::underlying_type_t<
                                       ThreadPoolConfig::IOThreadsPerCore>(
                        ThreadPoolConfig::IOThreadsPerCore::Default));
            } else {
                throw std::invalid_argument(fmt::format(
                        "Number of IO threads per core must be an "
                        "unsigned integer or \"default\"!! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "not_locked_returns_tmpfail"sv) {
            if (value.is_boolean()) {
                setNotLockedReturnsTmpfail(value.get<bool>());
            } else {
                throw std::invalid_argument(
                        fmt::format("not_locked_returns_tmpfail must be a "
                                    "boolean! Value:'{}'",
                                    value.dump()));
            }
        } else if (key == "tracing_enabled"sv) {
            setTracingEnabled(value.get<bool>());
        } else if (key == "scramsha_fallback_salt"sv) {
            // Try to base64 decode it to validate that it is a legal value..
            auto salt = value.get<std::string>();
            cb::base64::decode(salt);
            setScramshaFallbackSalt(salt);
        } else if (key == "scramsha_fallback_iteration_count"sv) {
            setScramshaFallbackIterationCount(value.get<int>());
        } else if (key == "external_auth_service"sv) {
            setExternalAuthServiceEnabled(value.get<bool>());
        } else if (key == "external_auth_service_scram_support"sv) {
            setExternalAuthServiceScramSupport(value.get<bool>());
        } else if (key == "active_external_users_push_interval"sv) {
            switch (value.type()) {
            case nlohmann::json::value_t::number_unsigned:
                setActiveExternalUsersPushInterval(
                        std::chrono::seconds(value.get<int>()));
                break;
            case nlohmann::json::value_t::string:
                setActiveExternalUsersPushInterval(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                cb::text2time(value.get<std::string>())));
                break;
            default:
                cb::throwJsonTypeError(
                        "\"active_external_users_push_interval\" must be a "
                        "number or string");
            }
        } else if (key == "external_auth_slow_duration"sv) {
            switch (value.type()) {
            case nlohmann::json::value_t::number_unsigned:
                setExternalAuthSlowDuration(
                        std::chrono::seconds(value.get<int>()));
                break;
            case nlohmann::json::value_t::string:
                setExternalAuthSlowDuration(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                cb::text2time(value.get<std::string>())));
                break;
            default:
                cb::throwJsonTypeError(
                        "\"external_auth_slow_duration\" must be a "
                        "number or string");
            }
        } else if (key == "external_auth_request_timeout"sv) {
            switch (value.type()) {
            case nlohmann::json::value_t::number_unsigned:
                setExternalAuthRequestTimeout(
                        std::chrono::seconds(value.get<int>()));
                break;
            case nlohmann::json::value_t::string:
                setExternalAuthRequestTimeout(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                cb::text2time(value.get<std::string>())));
                break;
            default:
                cb::throwJsonTypeError(
                        "\"external_auth_request_timeout\" must be a "
                        "number or string");
            }
        } else if (key == "max_concurrent_commands_per_connection"sv) {
            setMaxConcurrentCommandsPerConnection(value.get<size_t>());
        } else if (key == "fusion_migration_rate_limit"sv) {
            if (isFusionSupportEnabled()) {
                setFusionMigrationRateLimit(value.get<size_t>());
            } else {
                LOG_WARNING_CTX("Ignore fusion_migration_rate_limit",
                                {"value", value.dump()},
                                {"reason", "Fusion support is not enabled"});
            }
        } else if (key == "fusion_sync_rate_limit") {
            if (isFusionSupportEnabled()) {
                setFusionSyncRateLimit(value.get<size_t>());
            } else {
                LOG_WARNING_CTX("Ignore fusion_sync_rate_limit",
                                {"value", value.dump()},
                                {"reason", "Fusion support is not enabled"});
            }
        } else if (key == "phosphor_config"sv) {
            auto config = value.get<std::string>();
            // throw an exception if the config is invalid
            phosphor::TraceConfig::fromString(config);
            setPhosphorConfig(config);
        } else if (key == "prometheus"sv) {
            if (!value.contains("port")) {
                throw std::invalid_argument(
                        "\"prometheus.port\" must be present");
            }
            if (!value.contains("family")) {
                throw std::invalid_argument(
                        "\"prometheus.family\" must be present");
            }
            const auto port = value["port"].get<in_port_t>();
            const auto val = value["family"].get<std::string>();
            sa_family_t family;
            if (val == "inet") {
                family = AF_INET;
            } else if (val == "inet6") {
                family = AF_INET6;
            } else {
                throw std::invalid_argument(
                        R"("prometheus.family" must be "inet" or "inet6")");
            }
            setPrometheusConfig({port, family});
        } else if (key == "portnumber_file"sv) {
            setPortnumberFile(value.get<std::string>());
        } else if (key == "parent_identifier"sv) {
            setParentIdentifier(value.get<int>());
        } else if (key == "allow_localhost_interface"sv) {
            setAllowLocalhostInterface(value.get<bool>());
        } else if (key == "free_connection_pool_size"sv ||
                   key == "connection_limit_mode"sv) {
            // Ignore
        } else if (key == "max_client_connection_details"sv) {
            setMaxClientConnectionDetails(value.get<size_t>());
        } else if (key == "max_concurrent_authentications"sv) {
            setMaxConcurrentAuthentications(value.get<size_t>());
        } else if (key == "slow_prometheus_scrape_duration"sv) {
            setSlowPrometheusScrapeDuration(
                    std::chrono::duration<float>(value.get<float>()));
        } else if (key == "dcp_disconnect_when_stuck_timeout_seconds"sv) {
            setDcpDisconnectWhenStuckTimeout(
                    std::chrono::seconds(value.get<size_t>()));
        } else if (key == "dcp_disconnect_when_stuck_name_regex"sv) {
            setDcpDisconnectWhenStuckNameRegexFromBase64(
                    value.get<std::string>());
        } else if (key == "subdoc_multi_max_paths"sv) {
            setSubdocMultiMaxPaths(value.get<size_t>());
        } else if (key == "abrupt_shutdown_timeout"sv) {
            setAbruptShutdownTimeout(
                    std::chrono::milliseconds(value.get<size_t>()));
        } else if (key == "file_fragment_max_chunk_size"sv) {
            setFileFragmentMaxChunkSize(value.get<size_t>());
        } else if (key == "dcp_consumer_max_marker_version"sv) {
            setDcpConsumerMaxMarkerVersion(std::stod(value.get<std::string>()));
        } else if (key == "dcp_snapshot_marker_hps_enabled"sv) {
            setDcpSnapshotMarkerHPSEnabled(value.get<bool>());
        } else if (key == "dcp_snapshot_marker_purge_seqno_enabled"sv) {
            setDcpSnapshotMarkerPurgeSeqnoEnabled(value.get<bool>());
        } else if (key == "magma_blind_write_optimisation_enabled"sv) {
            setMagmaBlindWriteOptimisationEnabled(value.get<bool>());
        } else {
            LOG_WARNING_CTX("Ignoring unknown key in config", {"key", key});
        }
    }
}

void Settings::setOpcodeAttributesOverride(const std::string& value) {
    if (!value.empty()) {
        // Verify the content...
        cb::mcbp::sla::reconfigure(nlohmann::json::parse(value), false);
    }

    opcode_attributes_override.wlock()->assign(value);
    has.opcode_attributes_override = true;
    notify_changed("opcode_attributes_override");
}

void Settings::setNumIOThreadsPerCore(int val) {
    num_io_threads_per_core.store(val, std::memory_order_release);
    has.num_io_threads_per_core = true;
    notify_changed("num_io_threads_per_core");
}

void Settings::updateSettings(const Settings& other, bool apply) {
    if (other.has.deployment_model &&
        other.deployment_model != deployment_model) {
        throw std::invalid_argument(
                "deployment_model can't be changed dynamically");
    }

    if (other.has.rbac_file) {
        if (other.rbac_file != rbac_file) {
            throw std::invalid_argument("rbac_file can't be changed dynamically");
        }
    }
    if (other.has.threads) {
        if (other.num_threads != num_threads) {
            throw std::invalid_argument("threads can't be changed dynamically");
        }
    }

    if (other.has.audit) {
        if (other.audit_file != audit_file) {
            throw std::invalid_argument("audit can't be changed dynamically");
        }
    }
    if (other.has.datatype_json) {
        if (other.datatype_json != datatype_json) {
            throw std::invalid_argument(
                    "datatype_json can't be changed dynamically");
        }
    }
    if (other.has.root) {
        if (other.root != root) {
            throw std::invalid_argument("root can't be changed dynamically");
        }
    }
    if (other.has.stdin_listener) {
        if (other.stdin_listener.load() != stdin_listener.load()) {
            throw std::invalid_argument(
                    "stdin_listener can't be changed dynamically");
        }
    }

    if (other.has.logger) {
        if (other.logger_settings != logger_settings)
            throw std::invalid_argument(
                    "logger configuration can't be changed dynamically");
    }

    if (other.has.error_maps) {
        if (other.error_maps_dir != error_maps_dir) {
            throw std::invalid_argument(
                    "error_maps_dir can't be changed dynamically");
        }
    }

    if (other.has.parent_identifier) {
        if (other.parent_identifier != parent_identifier) {
            throw std::invalid_argument(
                    "parent_monitor_file can't be changed dynamically");
        }
    }

    if (other.has.portnumber_file) {
        if (other.portnumber_file != portnumber_file) {
            throw std::invalid_argument(
                    "portnumber_file can't be changed dynamically");
        }
    }

    // All non-dynamic settings has been validated. If we're not supposed
    // to update anything we can bail out.
    if (!apply) {
        return;
    }

    // Ok, go ahead and update the settings!!
    if (other.has.abrupt_shutdown_timeout) {
        if (other.getAbruptShutdownTimeout() != getAbruptShutdownTimeout()) {
            LOG_INFO_CTX("Change abrupt shutdown timeout",
                         {"from", getAbruptShutdownTimeout()},
                         {"to", other.getAbruptShutdownTimeout()});
            setAbruptShutdownTimeout(other.getAbruptShutdownTimeout());
        }
    }

    if (other.has.tcp_keepalive_idle) {
        if (other.getTcpKeepAliveIdle() != getTcpKeepAliveIdle()) {
            LOG_INFO_CTX("Change TCP_KEEPIDLE time",
                         {"from", getTcpKeepAliveIdle()},
                         {"to", other.getTcpKeepAliveIdle()});
            setTcpKeepAliveIdle(other.getTcpKeepAliveIdle());
        }
    }

    if (other.has.tcp_keepalive_interval) {
        if (other.getTcpKeepAliveInterval() != getTcpKeepAliveInterval()) {
            LOG_INFO_CTX("Change TCP_KEEPINTVL interval",
                         {"from", getTcpKeepAliveInterval()},
                         {"to", other.getTcpKeepAliveInterval()});
            setTcpKeepAliveInterval(other.getTcpKeepAliveInterval());
        }
    }

    if (other.has.tcp_keepalive_probes) {
        if (other.tcp_keepalive_probes != tcp_keepalive_probes) {
            LOG_INFO_CTX("Change TCP_KEEPCNT",
                         {"from", getTcpKeepAliveProbes()},
                         {"to", other.getTcpKeepAliveProbes()});
            setTcpKeepAliveProbes(other.getTcpKeepAliveProbes());
        }
    }

    if (other.has.tcp_user_timeout) {
        if (other.getTcpUserTimeout() != getTcpUserTimeout()) {
            using namespace std::chrono;
            LOG_INFO_CTX("Change TCP_USER_TIMEOUT",
                         {"from", getTcpUserTimeout()},
                         {"to", other.getTcpUserTimeout()});
            setTcpUserTimeout(other.getTcpUserTimeout());
        }
    }

    if (other.has.tcp_unauthenticated_user_timeout) {
        if (other.getTcpUnauthenticatedUserTimeout() !=
            getTcpUnauthenticatedUserTimeout()) {
            using namespace std::chrono;
            LOG_INFO_CTX("Change TCP_USER_TIMEOUT for unauthenticated users",
                         {"from", getTcpUnauthenticatedUserTimeout()},
                         {"to", other.getTcpUnauthenticatedUserTimeout()});
            setTcpUnauthenticatedUserTimeout(
                    other.getTcpUnauthenticatedUserTimeout());
        }
    }

    if (other.has.clustermap_push_notifications_enabled) {
        if (other.isClustermapPushNotificationsEnabled() !=
            isClustermapPushNotificationsEnabled()) {
            LOG_INFO_CTX("Change clustermap push notifications",
                         {"from",
                          isClustermapPushNotificationsEnabled() ? "enabled"
                                                                 : "disabled"},
                         {"to",
                          other.isClustermapPushNotificationsEnabled()
                                  ? "enabled"
                                  : "disabled"});
            setClustermapPushNotificationsEnabled(
                    other.isClustermapPushNotificationsEnabled());
        }
    }

    if (other.has.datatype_snappy) {
        if (other.datatype_snappy != datatype_snappy) {
            std::string curr_val_str = datatype_snappy ? "true" : "false";
            std::string other_val_str = other.datatype_snappy ? "true" : "false";
            LOG_INFO_CTX("Change datatype_snappy",
                         {"from", curr_val_str},
                         {"to", other_val_str});
            setDatatypeSnappyEnabled(other.datatype_snappy);
        }
    }

    if (other.has.enable_deprecated_bucket_autoselect &&
        other.enable_deprecated_bucket_autoselect !=
                enable_deprecated_bucket_autoselect) {
        LOG_INFO_CTX("Change deprecated bucket autoselect",
                     {"enabled", other.enable_deprecated_bucket_autoselect});
        setDeprecatedBucketAutoselectEnabled(
                other.enable_deprecated_bucket_autoselect);
    }

    if (other.has.verbose) {
        if (other.verbose != verbose) {
            LOG_INFO_CTX("Change verbosity level",
                         {"from", verbose.load()},
                         {"to", other.verbose.load()});
            setVerbose(other.verbose.load());
        }
    }

    if (other.has.reqs_per_event_high_priority) {
        if (other.reqs_per_event_high_priority !=
            reqs_per_event_high_priority) {
            LOG_INFO_CTX("Change high priority iterations per event",
                         {"from", reqs_per_event_high_priority},
                         {"to", other.reqs_per_event_high_priority});
            setRequestsPerEventNotification(other.reqs_per_event_high_priority,
                                            EventPriority::High);
        }
    }
    if (other.has.reqs_per_event_med_priority) {
        if (other.reqs_per_event_med_priority != reqs_per_event_med_priority) {
            LOG_INFO_CTX("Change medium priority iterations per event",
                         {"from", reqs_per_event_med_priority},
                         {"to", other.reqs_per_event_med_priority});
            setRequestsPerEventNotification(other.reqs_per_event_med_priority,
                                            EventPriority::Medium);
        }
    }
    if (other.has.reqs_per_event_low_priority) {
        if (other.reqs_per_event_low_priority != reqs_per_event_low_priority) {
            LOG_INFO_CTX("Change low priority iterations per event",
                         {"from", reqs_per_event_low_priority},
                         {"to", other.reqs_per_event_low_priority});
            setRequestsPerEventNotification(other.reqs_per_event_low_priority,
                                            EventPriority::Low);
        }
    }
    if (other.has.default_reqs_per_event) {
        if (other.default_reqs_per_event != default_reqs_per_event) {
            LOG_INFO_CTX("Change default iterations per event",
                         {"from", default_reqs_per_event},
                         {"to", other.default_reqs_per_event});
            setRequestsPerEventNotification(other.default_reqs_per_event,
                                            EventPriority::Default);
        }
    }

    if (other.has.command_time_slice) {
        if (other.getCommandTimeSlice() != getCommandTimeSlice()) {
            LOG_INFO_CTX("Change command time slice",
                         {"from", getCommandTimeSlice()},
                         {"to", other.getCommandTimeSlice()});
            setCommandTimeSlice(other.getCommandTimeSlice());
        }
    }

    if (other.has.connection_idle_time) {
        if (other.connection_idle_time != connection_idle_time) {
            LOG_INFO_CTX("Change connection idle time",
                         {"from", connection_idle_time.load()},
                         {"to", other.connection_idle_time.load()});
            setConnectionIdleTime(other.connection_idle_time);
        }
    }
    if (other.has.max_packet_size) {
        if (other.max_packet_size != max_packet_size) {
            LOG_INFO_CTX("Change max packet size",
                         {"from", max_packet_size},
                         {"to", other.max_packet_size});
            setMaxPacketSize(other.max_packet_size);
        }
    }
    if (other.has.max_send_queue_size) {
        if (other.max_send_queue_size != max_send_queue_size) {
            LOG_INFO_CTX("Change max packet size",
                         {"from", max_send_queue_size / (1_MiB)},
                         {"to", other.max_send_queue_size / (1_MiB)});
            setMaxSendQueueSize(other.max_send_queue_size);
        }
    }

    if (other.has.max_so_sndbuf_size) {
        if (other.max_so_sndbuf_size != max_so_sndbuf_size) {
            LOG_INFO_CTX("Change max SO_SNDBUF",
                         {"from", max_so_sndbuf_size},
                         {"to", other.max_so_sndbuf_size});
            setMaxSoSndbufSize(other.max_so_sndbuf_size);
        }
    }

    if (other.has.client_cert_auth) {
        const auto m = client_cert_mapper.to_string();
        const auto o = other.client_cert_mapper.to_string();

        if (m != o) {
            // @todo we should log them as JSON; not strings in json format
            LOG_INFO_CTX("Change SSL client auth", {"from", m}, {"to", o});
            // TODO MB-30041: Remove when we migrate settings
            nlohmann::json json = nlohmann::json::parse(o);
            auto config = cb::x509::ClientCertConfig::create(json);
            reconfigureClientCertAuth(std::move(config));
        }
    }

    if (other.has.dedupe_nmvb_maps) {
        if (other.dedupe_nmvb_maps != dedupe_nmvb_maps) {
            LOG_INFO_CTX("Change deduplication of NMVB maps",
                         {"enabled", other.dedupe_nmvb_maps.load()});
            setDedupeNmvbMaps(other.dedupe_nmvb_maps.load());
        }
    }

    if (other.has.max_connections) {
        if (other.max_connections != max_connections) {
            LOG_INFO_CTX("Change max connections",
                         {"from", max_connections},
                         {"to", other.max_connections});
            setMaxConnections(other.max_connections);
        }
    }

    if (other.has.system_connections) {
        if (other.system_connections != system_connections) {
            LOG_INFO_CTX("Change system connections",
                         {"from", system_connections},
                         {"to", other.system_connections});
            setSystemConnections(other.system_connections);
        }
    }

    if (other.has.max_client_connection_details) {
        if (other.max_client_connection_details !=
            max_client_connection_details) {
            LOG_INFO_CTX("Change max client connection details",
                         {"from", max_client_connection_details},
                         {"to", other.max_client_connection_details});
            setMaxClientConnectionDetails(other.max_client_connection_details);
        }
    }

    if (other.has.max_concurrent_authentications) {
        if (other.max_concurrent_authentications !=
            max_concurrent_authentications) {
            LOG_INFO_CTX("Change max concurrent authentications",
                         {"from", max_concurrent_authentications},
                         {"to", other.max_concurrent_authentications});
            setMaxConcurrentAuthentications(
                    other.max_concurrent_authentications);
        }
    }

    if (other.has.xattr_enabled) {
        if (other.xattr_enabled != xattr_enabled) {
            LOG_INFO_CTX("Change XATTR",
                         {"enabled", other.xattr_enabled.load()});
            setXattrEnabled(other.xattr_enabled.load());
        }
    }

    if (other.has.collections_enabled) {
        if (other.collections_enabled != collections_enabled) {
            LOG_INFO_CTX("Change collections_enabled",
                         {"enabled", other.collections_enabled.load()});
            setCollectionsEnabled(other.collections_enabled.load());
        }
    }

    if (other.has.breakpad) {
        bool changed = false;
        auto& b1 = breakpad;
        const auto& b2 = other.breakpad;

        if (b2.enabled != b1.enabled) {
            LOG_INFO_CTX("Change breakpad", {"enabled", b2.enabled});
            b1.enabled = b2.enabled;
            changed = true;
        }

        if (b2.minidump_dir != b1.minidump_dir) {
            LOG_INFO_CTX("Change minidump directory",
                         {"from", b1.minidump_dir},
                         {"to", b2.minidump_dir});
            b1.minidump_dir = b2.minidump_dir;
            changed = true;
        }

        if (b2.content != b1.content) {
            LOG_INFO_CTX("Change minidump content",
                         {"from", b1.content},
                         {"to", b2.content});
            b1.content = b2.content;
            changed = true;
        }

        if (changed) {
            notify_changed("breakpad");
        }
    }

    if (other.has.opcode_attributes_override) {
        auto current = getOpcodeAttributesOverride();
        auto proposed = other.getOpcodeAttributesOverride();

        if (proposed != current) {
            LOG_INFO_CTX("Change opcode attributes",
                         {"from", current},
                         {"to", proposed});
            setOpcodeAttributesOverride(proposed);
        }
    }

    if (other.has.tracing_enabled) {
        if (other.isTracingEnabled() != isTracingEnabled()) {
            LOG_INFO_CTX("Change tracing support",
                         {"enabled", other.isTracingEnabled()});
        }
        setTracingEnabled(other.isTracingEnabled());
    }

    if (other.has.scramsha_fallback_salt) {
        const auto o = other.getScramshaFallbackSalt();
        const auto m = getScramshaFallbackSalt();

        if (o != m) {
            LOG_INFO_CTX("Change scram fallback salt",
                         {"from", cb::UserDataView(m)},
                         {"to", cb::UserDataView(o)});
            setScramshaFallbackSalt(o);
        }
    }

    if (other.has.scramsha_fallback_iteration_count) {
        const auto o = other.getScramshaFallbackIterationCount();
        const auto m = getScramshaFallbackIterationCount();
        if (o != m) {
            LOG_INFO_CTX("Change scram fallback iteration count",
                         {"from", m},
                         {"to", o});
            setScramshaFallbackIterationCount(o);
        }
    }

    if (other.has.sasl_mechanisms) {
        auto mine = getSaslMechanisms();
        auto others = other.getSaslMechanisms();
        if (mine != others) {
            LOG_INFO_CTX("Change SASL mechanisms on normal connections",
                         {"from", mine},
                         {"to", others});
            setSaslMechanisms(others);
        }
    }

    if (other.has.ssl_sasl_mechanisms) {
        auto mine = getSslSaslMechanisms();
        auto others = other.getSslSaslMechanisms();
        if (mine != others) {
            LOG_INFO_CTX("Change SASL mechanisms on SSL connections",
                         {"from", mine},
                         {"to", others});
            setSslSaslMechanisms(others);
        }
    }

    if (other.has.external_auth_service) {
        if (isExternalAuthServiceEnabled() !=
            other.isExternalAuthServiceEnabled()) {
            LOG_INFO_CTX(
                    "Change external authentication service",
                    {"from",
                     isExternalAuthServiceEnabled() ? "enabled" : "disabled"},
                    {"to",
                     other.isExternalAuthServiceEnabled() ? "enabled"
                                                          : "disabled"});
            setExternalAuthServiceEnabled(other.isExternalAuthServiceEnabled());
        }
    }

    if (other.has.external_auth_service_scram_support) {
        if (doesExternalAuthServiceSupportScram() !=
            other.doesExternalAuthServiceSupportScram()) {
            LOG_INFO_CTX(
                    "Change external authentication SCRAM support",
                    {"from",
                     doesExternalAuthServiceSupportScram() ? "enabled"
                                                           : "disabled"},
                    {"to",
                     other.doesExternalAuthServiceSupportScram() ? "enabled"
                                                                 : "disabled"});
            setExternalAuthServiceScramSupport(
                    other.doesExternalAuthServiceSupportScram());
        }
    }

    if (other.has.active_external_users_push_interval) {
        if (getActiveExternalUsersPushInterval() !=
            other.getActiveExternalUsersPushInterval()) {
            LOG_INFO_CTX("Change push interval for external users list",
                         {"from", getActiveExternalUsersPushInterval()},
                         {"to", other.getActiveExternalUsersPushInterval()});
            setActiveExternalUsersPushInterval(
                    other.getActiveExternalUsersPushInterval());
        }
    }

    if (other.has.external_auth_slow_duration) {
        if (getExternalAuthSlowDuration() !=
            other.getExternalAuthSlowDuration()) {
            LOG_INFO_CTX(
                    "Change slow duration for external users authentication",
                    {"from", getExternalAuthSlowDuration()},
                    {"to", other.getExternalAuthSlowDuration()});
            setExternalAuthSlowDuration(other.getExternalAuthSlowDuration());
        }
    }

    if (other.has.external_auth_request_timeout) {
        if (getExternalAuthRequestTimeout() !=
            other.getExternalAuthRequestTimeout()) {
            LOG_INFO_CTX(
                    "Change request timeout for external users authentication",
                    {"from", getExternalAuthRequestTimeout()},
                    {"to", other.getExternalAuthRequestTimeout()});
            setExternalAuthRequestTimeout(
                    other.getExternalAuthRequestTimeout());
        }
    }

    if (other.has.allow_localhost_interface) {
        if (other.allow_localhost_interface != allow_localhost_interface) {
            LOG_INFO_CTX(
                    "Change allow localhost interface",
                    {"from",
                     isLocalhostInterfaceAllowed() ? "enabled" : "disabled"},
                    {"to",
                     other.isLocalhostInterfaceAllowed() ? "enabled"
                                                         : "disabled"});
            setAllowLocalhostInterface(other.isLocalhostInterfaceAllowed());
        }
    }

    if (other.has.max_concurrent_commands_per_connection) {
        if (other.getMaxConcurrentCommandsPerConnection() !=
            getMaxConcurrentCommandsPerConnection()) {
            LOG_INFO_CTX(
                    "Change max number of concurrent commands per connection",
                    {"from", getMaxConcurrentCommandsPerConnection()},
                    {"to", other.getMaxConcurrentCommandsPerConnection()});
            setMaxConcurrentCommandsPerConnection(
                    other.getMaxConcurrentCommandsPerConnection());
        }
    }

    if (other.has.fusion_migration_rate_limit) {
        if (other.getFusionMigrationRateLimit() !=
            getFusionMigrationRateLimit()) {
            LOG_INFO_CTX("Change fusion migration rate limit",
                         {"from", getFusionMigrationRateLimit()},
                         {"to", other.getFusionMigrationRateLimit()});
            setFusionMigrationRateLimit(other.getFusionMigrationRateLimit());
        }
    }

    if (other.has.fusion_sync_rate_limit) {
        if (other.getFusionSyncRateLimit() != getFusionSyncRateLimit()) {
            LOG_INFO_CTX("Change fusion sync rate limit",
                         {"from", getFusionSyncRateLimit()},
                         {"to", other.getFusionSyncRateLimit()});
            setFusionSyncRateLimit(other.getFusionSyncRateLimit());
        }
    }

    if (other.has.num_reader_threads &&
        other.getNumReaderThreads() != getNumReaderThreads()) {
        LOG_INFO_CTX("Change number of reader threads",
                     {"from", threadConfig2String(getNumReaderThreads())},
                     {"to", threadConfig2String(other.getNumReaderThreads())});
        setNumReaderThreads(other.getNumReaderThreads());
    }

    if (other.has.num_writer_threads &&
        other.getNumWriterThreads() != getNumWriterThreads()) {
        LOG_INFO_CTX("Change number of writer threads",
                     {"from", threadConfig2String(getNumWriterThreads())},
                     {"to", threadConfig2String(other.getNumWriterThreads())});
        setNumWriterThreads(other.getNumWriterThreads());
    }

    if (other.has.num_auxio_threads &&
        other.getNumAuxIoThreads() != getNumAuxIoThreads()) {
        LOG_INFO_CTX("Change number of AuxIO threads",
                     {"from", getNumAuxIoThreads()},
                     {"to", other.getNumAuxIoThreads()});
        setNumAuxIoThreads(other.getNumAuxIoThreads());
    }

    if (other.has.num_nonio_threads &&
        other.getNumNonIoThreads() != getNumNonIoThreads()) {
        LOG_INFO_CTX("Change number of NonIO threads",
                     {"from", getNumNonIoThreads()},
                     {"to", other.getNumNonIoThreads()});
        setNumNonIoThreads(other.getNumNonIoThreads());
    }

    if (other.has.num_io_threads_per_core) {
        const auto oldTPC = getNumIOThreadsPerCore();
        const auto newTPC = other.getNumIOThreadsPerCore();
        if (oldTPC != newTPC) {
            LOG_INFO_CTX("Change number of IO threads per core",
                         {"from", oldTPC},
                         {"to", newTPC});
            setNumIOThreadsPerCore(newTPC);
        }
    }

    if (other.has.quota_sharing_pager_concurrency_percentage &&
        other.getQuotaSharingPagerConcurrencyPercentage() !=
                getQuotaSharingPagerConcurrencyPercentage()) {
        LOG_INFO_CTX("Change pager concurrency percentage for quota sharing",
                     {"from", getQuotaSharingPagerConcurrencyPercentage()},
                     {"to", other.getQuotaSharingPagerConcurrencyPercentage()});
        setQuotaSharingPagerConcurrencyPercentage(
                other.getQuotaSharingPagerConcurrencyPercentage());
    }

    if (other.has.quota_sharing_pager_sleep_time_ms &&
        other.getQuotaSharingPagerSleepTime() !=
                getQuotaSharingPagerSleepTime()) {
        LOG_INFO_CTX("Change pager sleep time for quota sharing",
                     {"from", getQuotaSharingPagerSleepTime()},
                     {"to", other.getQuotaSharingPagerSleepTime()});
        setQuotaSharingPagerSleepTime(other.getQuotaSharingPagerSleepTime());
    }

    if (other.has.prometheus_config) {
        auto nval = *other.prometheus_config.rlock();
        if (nval != *prometheus_config.rlock()) {
            switch (nval.second) {
            case AF_INET:
                LOG_INFO_CTX("Change prometheus port",
                             {"protocol", "IPv4"},
                             {"to", nval.first});
                break;
            case AF_INET6:
                LOG_INFO_CTX("Change prometheus port",
                             {"protocol", "IPv6"},
                             {"to", nval.first});
                break;
            default:
                LOG_INFO_RAW("Disable prometheus port");
                nval.first = 0;
                nval.second = 0;
            }
            setPrometheusConfig(nval);
        }
    }

    if (other.has.num_storage_threads &&
        other.getNumStorageThreads() != getNumStorageThreads()) {
        LOG_INFO_CTX(
                "Change number of storage threads",
                {"from", storageThreadConfig2String(getNumStorageThreads())},
                {"to",
                 storageThreadConfig2String(other.getNumStorageThreads())});
        setNumStorageThreads(other.getNumStorageThreads());

    }

    if (other.has.not_locked_returns_tmpfail) {
        if (other.getNotLockedReturnsTmpfail() !=
            getNotLockedReturnsTmpfail()) {
            LOG_INFO_CTX("Change not_locked_returns_tmpfail",
                         {"from", getNotLockedReturnsTmpfail()},
                         {"to", other.getNotLockedReturnsTmpfail()});
            setNotLockedReturnsTmpfail(other.getNotLockedReturnsTmpfail());
        }
    }

    if (other.has.phosphor_config) {
        const auto o = other.getPhosphorConfig();
        const auto m = getPhosphorConfig();
        if (o != m) {
            LOG_INFO_CTX("Change Phosphor config", {"from", m}, {"to", o});
            setPhosphorConfig(o);
        }
    }

    if (other.getSlowPrometheusScrapeDuration() !=
        getSlowPrometheusScrapeDuration()) {
        const auto o = getSlowPrometheusScrapeDuration();
        const auto n = other.getSlowPrometheusScrapeDuration();
        LOG_INFO_CTX("Changing slow prometheus scrape duration",
                     {"from", o},
                     {"to", n});
        setSlowPrometheusScrapeDuration(n);
    }

    if (other.has.dcp_disconnect_when_stuck_name_regex) {
        // Both these values are the decoded values, not base64 encoded
        const auto newValue = other.getDcpDisconnectWhenStuckNameRegex();
        const auto current = getDcpDisconnectWhenStuckNameRegex();
        if (newValue != current) {
            LOG_INFO_CTX("Change dcp_disconnect_when_stuck_name_regex",
                         {"from", current},
                         {"to", newValue});
            // Use the protected setter which validates newValue
            setDcpDisconnectWhenStuckNameRegex(std::move(newValue));
        }
    }

    if (other.has.dcp_disconnect_when_stuck_timeout_seconds) {
        const auto newValue = other.getDcpDisconnectWhenStuckTimeout();
        const auto current = getDcpDisconnectWhenStuckTimeout();
        if (newValue != current) {
            LOG_INFO_CTX("Change dcp_disconnect_when_stuck_timeout_seconds",
                         {"from", current.count()},
                         {"to", newValue.count()});
            setDcpDisconnectWhenStuckTimeout(newValue);
        }
    }

    if (other.has.dcp_consumer_max_marker_version) {
        const auto newValue = other.getDcpConsumerMaxMarkerVersion();
        const auto current = getDcpConsumerMaxMarkerVersion();
        if (newValue != current) {
            LOG_INFO_CTX("Change dcp_consumer_max_marker_version",
                         {"from", current},
                         {"to", newValue});
            setDcpConsumerMaxMarkerVersion(newValue);
        }
    }

    if (other.has.subdoc_multi_max_paths) {
        if (other.getSubdocMultiMaxPaths() != getSubdocMultiMaxPaths()) {
            LOG_INFO_CTX("Change subdoc_multi_max_paths",
                         {"from", getSubdocMultiMaxPaths()},
                         {"to", other.getSubdocMultiMaxPaths()});
            setSubdocMultiMaxPaths(other.getSubdocMultiMaxPaths());
        }
    }

    if (other.has.file_fragment_max_chunk_size) {
        if (other.getFileFragmentMaxChunkSize() !=
            getFileFragmentMaxChunkSize()) {
            LOG_INFO_CTX("Change file_fragment_max_chunk_size",
                         {"from", getFileFragmentMaxChunkSize()},
                         {"to", other.getFileFragmentMaxChunkSize()});
            setFileFragmentMaxChunkSize(other.getFileFragmentMaxChunkSize());
        }
    }

    if (other.has.magma_blind_write_optimisation_enabled) {
        if (other.isMagmaBlindWriteOptimisationEnabled() !=
            isMagmaBlindWriteOptimisationEnabled()) {
            LOG_INFO_CTX("Change magma_blind_write_optimisation_enabled",
                         {"from", isMagmaBlindWriteOptimisationEnabled()},
                         {"to", other.isMagmaBlindWriteOptimisationEnabled()});
            setMagmaBlindWriteOptimisationEnabled(
                    other.isMagmaBlindWriteOptimisationEnabled());
        }
    }
}

spdlog::level::level_enum Settings::getLogLevel() const {
    switch (getVerbose()) {
    case 0:
        return spdlog::level::level_enum::info;
    case 1:
        return spdlog::level::level_enum::debug;
    default:
        return spdlog::level::level_enum::trace;
    }
}

void Settings::notify_changed(const std::string& key) {
    auto iter = change_listeners.find(key);
    if (iter != change_listeners.end()) {
        for (auto& listener : iter->second) {
            listener(key, *this);
        }
    }
}

std::string Settings::getSaslMechanisms() const {
    return std::string{*sasl_mechanisms.rlock()};
}

void Settings::setSaslMechanisms(std::string mechanisms) {
    sasl_mechanisms = std::move(mechanisms);
    has.sasl_mechanisms = true;
    notify_changed("sasl_mechanisms");
}

std::string Settings::getSslSaslMechanisms() const {
    return std::string{*ssl_sasl_mechanisms.rlock()};
}

void Settings::setSslSaslMechanisms(std::string mechanisms) {
    ssl_sasl_mechanisms = std::move(mechanisms);
    has.ssl_sasl_mechanisms = true;
    notify_changed("ssl_sasl_mechanisms");
}

size_t Settings::getMaxConcurrentCommandsPerConnection() const {
    return max_concurrent_commands_per_connection.load(
            std::memory_order_consume);
}

void Settings::setMaxConcurrentCommandsPerConnection(size_t num) {
    max_concurrent_commands_per_connection.store(num,
                                                 std::memory_order_release);
    has.max_concurrent_commands_per_connection = true;
    notify_changed("max_concurrent_commands_per_connection");
}

void Settings::setSubdocMultiMaxPaths(size_t val) {
    // Can only change this for internal R&D, SDKs are generally hardcoded with
    // the default so this would be pointless/dangerous to change outside of a
    // controlled test.
    if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
        subdoc_multi_max_paths.store(val, std::memory_order_release);
        has.subdoc_multi_max_paths = true;
        notify_changed("subdoc_multi_max_paths");
    } else {
        LOG_WARNING_RAW(
                "Settings::setSubdocMultiMaxPaths: ignoring request to change");
    }
}