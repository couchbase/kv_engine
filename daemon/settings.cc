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
#include <mcbp/mcbp.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/trace_config.h>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <platform/timeutils.h>
#include <utilities/json_utilities.h>
#include <utilities/logtags.h>
#include <algorithm>
#include <cstring>
#include <system_error>

std::string to_string(ConnectionLimitMode mode) {
    switch (mode) {
    case ConnectionLimitMode::Disconnect:
        return "disconnect";
    case ConnectionLimitMode::Recycle:
        return "recycle";
    }
    throw std::invalid_argument("Invalid ConnectionLimitMode: " +
                                std::to_string(int(mode)));
}

std::ostream& operator<<(std::ostream& os, const ConnectionLimitMode& mode) {
    return os << to_string(mode);
}

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

static int parseStorageThreadConfigSpec(const std::string& variable,
                                        const std::string& spec) {
    if (spec == "default") {
        return 0;
    }

    uint64_t val;
    if (!safe_strtoull(spec, val)) {
        throw std::invalid_argument(
                variable +
                R"( must be specified as a numeric value or "default")");
    }
    return val;
}

static int parseThreadConfigSpec(const std::string& variable,
                                 const std::string& spec) {
    if (spec == "disk_io_optimized") {
        return -1;
    }

    try {
        return parseStorageThreadConfigSpec(variable, spec);
    } catch (std::invalid_argument& e) {
        std::string message{e.what()};
        message.append(R"( or "disk_io_optimized")");
        throw std::invalid_argument(message);
    }
}

void Settings::reconfigure(const nlohmann::json& json) {
    // Nuke the default interface added to the system in settings_init and
    // use the ones in the configuration file (this is a bit messy).
    interfaces.wlock()->clear();

    for (const auto& obj : json.items()) {
        const auto key = obj.key();
        const auto value = obj.value();
        using namespace std::string_view_literals;

        if (key == "always_collect_trace_info"sv) {
            setAlwaysCollectTraceInfo(value.get<bool>());
        } else if (key == "rbac_file"sv) {
            setRbacFile(value.get<std::string>());
        } else if (key == "privilege_debug"sv) {
            setPrivilegeDebug(value.get<bool>());
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
        } else if (key == "event_framework"sv) {
            auto val = value.get<std::string>();
            if (val == "bufferevent") {
                setEventFramework(EventFramework::Bufferevent);
            } else if (val == "folly") {
                setEventFramework(EventFramework::Folly);
            } else {
                throw std::invalid_argument(
                        R"("event_framework" must be "bufferevent" or "folly")");
            }
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
            setMaxSendQueueSize(value.get<size_t>() * 1024 * 1024);
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
#ifdef __linux__
            setTcpUserTimeout(std::chrono::seconds(value.get<uint32_t>()));
#else
            if (value.get<uint32_t>()) {
                LOG_WARNING_RAW("TCP_USER_TIMEOUT is only supported on Linux");
            }
#endif
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
                setNumReaderThreads(value.get<size_t>());
            } else {
                const auto val = value.get<std::string>();
                setNumReaderThreads(
                        parseThreadConfigSpec("num_reader_threads", val));
            }
        } else if (key == "num_writer_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumWriterThreads(value.get<size_t>());
            } else {
                const auto val = value.get<std::string>();
                setNumWriterThreads(
                        parseThreadConfigSpec("num_writer_threads", val));
            }
        } else if (key == "num_storage_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumStorageThreads(value.get<size_t>());
            } else {
                setNumStorageThreads(parseStorageThreadConfigSpec(
                        "num_storage_threads", value.get<std::string>()));
            }
        } else if (key == "num_auxio_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumAuxIoThreads(value.get<size_t>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumAuxIoThreads(0);
            } else {
                throw std::invalid_argument(fmt::format(
                        "Value to set number of AuxIO threads must be an "
                        "unsigned integer or \"default\"! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "num_nonio_threads"sv) {
            if (value.is_number_unsigned()) {
                setNumNonIoThreads(value.get<size_t>());
            } else if (value.is_string() &&
                       value.get<std::string>() == "default") {
                setNumNonIoThreads(0);
            } else {
                throw std::invalid_argument(fmt::format(
                        "Value to set number of NonIO threads must be an "
                        "unsigned integer or \"default\"!! Value:'{}'",
                        value.dump()));
            }
        } else if (key == "tracing_enabled"sv) {
            setTracingEnabled(value.get<bool>());
        } else if (key == "scramsha_fallback_salt"sv) {
            // Try to base64 decode it to validate that it is a legal value..
            auto salt = value.get<std::string>();
            cb::base64::decode(salt);
            setScramshaFallbackSalt(salt);
        } else if (key == "external_auth_service"sv) {
            setExternalAuthServiceEnabled(value.get<bool>());
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
        } else if (key == "max_concurrent_commands_per_connection"sv) {
            setMaxConcurrentCommandsPerConnection(value.get<size_t>());
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
        } else if (key == "free_connection_pool_size"sv) {
            setFreeConnectionPoolSize(value.get<size_t>());
        } else if (key == "connection_limit_mode"sv) {
            const auto str = value.get<std::string>();
            if (str == "disconnect") {
                setConnectionLimitMode(ConnectionLimitMode::Disconnect);
            } else if (str == "recycle") {
                setConnectionLimitMode(ConnectionLimitMode::Recycle);
            } else {
                throw std::invalid_argument(
                        R"(connection_limit_mode must be "disconnect" or "recycle")");
            }
        } else {
            LOG_WARNING(R"(Unknown key "{}" in config ignored.)", key);
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
    if (other.has.tcp_keepalive_idle) {
        if (other.getTcpKeepAliveIdle() != getTcpKeepAliveIdle()) {
            LOG_INFO("Change TCP_KEEPIDLE time from {}s to {}s",
                     getTcpKeepAliveIdle().count(),
                     other.getTcpKeepAliveIdle().count());
            setTcpKeepAliveIdle(other.getTcpKeepAliveIdle());
        }
    }

    if (other.has.tcp_keepalive_interval) {
        if (other.getTcpKeepAliveInterval() != getTcpKeepAliveInterval()) {
            LOG_INFO("Change TCP_KEEPINTVL interval from {}s to {}s",
                     getTcpKeepAliveInterval().count(),
                     other.getTcpKeepAliveInterval().count());
            setTcpKeepAliveInterval(other.getTcpKeepAliveInterval());
        }
    }

    if (other.has.tcp_keepalive_probes) {
        if (other.tcp_keepalive_probes != tcp_keepalive_probes) {
            LOG_INFO("Change TCP_KEEPCNT from {} to {}",
                     getTcpKeepAliveProbes(),
                     other.getTcpKeepAliveProbes());
            setTcpKeepAliveProbes(other.getTcpKeepAliveProbes());
        }
    }

    if (other.has.tcp_user_timeout) {
        if (other.getTcpUserTimeout() != getTcpUserTimeout()) {
            using namespace std::chrono;
            LOG_INFO("Change TCP_USER_TIMEOUT from {}s to {}s",
                     duration_cast<seconds>(getTcpUserTimeout()).count(),
                     duration_cast<seconds>(other.getTcpUserTimeout()).count());
            setTcpUserTimeout(other.getTcpUserTimeout());
        }
    }

    if (other.has.event_framework) {
        if (other.event_framework != event_framework) {
            LOG_INFO("Change event framework from {} to {}",
                     getEventFramework(),
                     other.getEventFramework());
            setEventFramework(other.getEventFramework());
        }
    }

    if (other.has.always_collect_trace_info) {
        if (other.alwaysCollectTraceInfo() != alwaysCollectTraceInfo()) {
            if (other.alwaysCollectTraceInfo()) {
                LOG_INFO_RAW("Always collect trace information");
            } else {
                LOG_INFO_RAW(
                        "Only collect trace information if the client asks for "
                        "it");
            }
            setAlwaysCollectTraceInfo(other.alwaysCollectTraceInfo());
        }
    }

    if (other.has.datatype_snappy) {
        if (other.datatype_snappy != datatype_snappy) {
            std::string curr_val_str = datatype_snappy ? "true" : "false";
            std::string other_val_str = other.datatype_snappy ? "true" : "false";
            LOG_INFO("Change datatype_snappy from {} to {}",
                     curr_val_str,
                     other_val_str);
            setDatatypeSnappyEnabled(other.datatype_snappy);
        }
    }

    if (other.has.enable_deprecated_bucket_autoselect &&
        other.enable_deprecated_bucket_autoselect !=
                enable_deprecated_bucket_autoselect) {
        LOG_INFO("{}able deprecated bucket autoselect",
                 other.enable_deprecated_bucket_autoselect ? "En" : "Dis");
        setDeprecatedBucketAutoselectEnabled(
                other.enable_deprecated_bucket_autoselect);
    }

    if (other.has.verbose) {
        if (other.verbose != verbose) {
            LOG_INFO("Change verbosity level from {} to {}",
                     verbose.load(),
                     other.verbose.load());
            setVerbose(other.verbose.load());
        }
    }

    if (other.has.reqs_per_event_high_priority) {
        if (other.reqs_per_event_high_priority !=
            reqs_per_event_high_priority) {
            LOG_INFO("Change high priority iterations per event from {} to {}",
                     reqs_per_event_high_priority,
                     other.reqs_per_event_high_priority);
            setRequestsPerEventNotification(other.reqs_per_event_high_priority,
                                            EventPriority::High);
        }
    }
    if (other.has.reqs_per_event_med_priority) {
        if (other.reqs_per_event_med_priority != reqs_per_event_med_priority) {
            LOG_INFO(
                    "Change medium priority iterations per event from {} to {}",
                    reqs_per_event_med_priority,
                    other.reqs_per_event_med_priority);
            setRequestsPerEventNotification(other.reqs_per_event_med_priority,
                                            EventPriority::Medium);
        }
    }
    if (other.has.reqs_per_event_low_priority) {
        if (other.reqs_per_event_low_priority != reqs_per_event_low_priority) {
            LOG_INFO("Change low priority iterations per event from {} to {}",
                     reqs_per_event_low_priority,
                     other.reqs_per_event_low_priority);
            setRequestsPerEventNotification(other.reqs_per_event_low_priority,
                                            EventPriority::Low);
        }
    }
    if (other.has.default_reqs_per_event) {
        if (other.default_reqs_per_event != default_reqs_per_event) {
            LOG_INFO("Change default iterations per event from {} to {}",
                     default_reqs_per_event,
                     other.default_reqs_per_event);
            setRequestsPerEventNotification(other.default_reqs_per_event,
                                            EventPriority::Default);
        }
    }
    if (other.has.connection_idle_time) {
        if (other.connection_idle_time != connection_idle_time) {
            LOG_INFO("Change connection idle time from {} to {}",
                     connection_idle_time.load(),
                     other.connection_idle_time.load());
            setConnectionIdleTime(other.connection_idle_time);
        }
    }
    if (other.has.max_packet_size) {
        if (other.max_packet_size != max_packet_size) {
            LOG_INFO("Change max packet size from {} to {}",
                     max_packet_size,
                     other.max_packet_size);
            setMaxPacketSize(other.max_packet_size);
        }
    }
    if (other.has.max_send_queue_size) {
        if (other.max_send_queue_size != max_send_queue_size) {
            LOG_INFO("Change max packet size from {}MB to {}MB",
                     max_send_queue_size / (1024 * 1024),
                     other.max_send_queue_size / (1024 * 1024));
            setMaxSendQueueSize(other.max_send_queue_size);
        }
    }

    if (other.has.client_cert_auth) {
        const auto m = client_cert_mapper.to_string();
        const auto o = other.client_cert_mapper.to_string();

        if (m != o) {
            LOG_INFO(
                    R"(Change SSL client auth from "{}" to "{}")", m, o);
            // TODO MB-30041: Remove when we migrate settings
            nlohmann::json json = nlohmann::json::parse(o);
            auto config = cb::x509::ClientCertConfig::create(json);
            reconfigureClientCertAuth(std::move(config));
        }
    }

    if (other.has.dedupe_nmvb_maps) {
        if (other.dedupe_nmvb_maps != dedupe_nmvb_maps) {
            LOG_INFO("{} deduplication of NMVB maps",
                     other.dedupe_nmvb_maps.load() ? "Enable" : "Disable");
            setDedupeNmvbMaps(other.dedupe_nmvb_maps.load());
        }
    }

    if (other.has.max_connections) {
        if (other.max_connections != max_connections) {
            LOG_INFO(R"(Change max connections from {} to {})",
                     max_connections,
                     other.max_connections);
            setMaxConnections(other.max_connections);
        }
    }

    if (other.has.system_connections) {
        if (other.system_connections != system_connections) {
            LOG_INFO(R"(Change system connections from {} to {})",
                     system_connections,
                     other.system_connections);
            setSystemConnections(other.system_connections);
        }
    }

    if (other.has.free_connection_pool_size) {
        if (other.free_connection_pool_size != free_connection_pool_size) {
            LOG_INFO("Change free connections pool size from {} to {}",
                     free_connection_pool_size,
                     other.free_connection_pool_size);
            setFreeConnectionPoolSize(other.free_connection_pool_size);
        }
    } else {
        // Auto tune to 1%
        const auto old_size = getFreeConnectionPoolSize();
        const size_t new_size = getMaxUserConnections() / 100;
        if (old_size != new_size) {
            LOG_INFO("Change free connections pool size from {} to {}",
                     old_size,
                     new_size);
            setFreeConnectionPoolSize(new_size);
        }
    }

    if (other.has.connection_limit_mode &&
        other.getConnectionLimitMode() != getConnectionLimitMode()) {
        if (other.getConnectionLimitMode() == ConnectionLimitMode::Recycle) {
            LOG_INFO(
                    "Change connection limit mode from {} to {} with a pool "
                    "size of {}",
                    getConnectionLimitMode(),
                    other.getConnectionLimitMode(),
                    getFreeConnectionPoolSize());
        } else {
            LOG_INFO("Change connection limit mode from {} to {}",
                     getConnectionLimitMode(),
                     other.getConnectionLimitMode());
        }
        setConnectionLimitMode(other.getConnectionLimitMode());
    }

    if (other.has.xattr_enabled) {
        if (other.xattr_enabled != xattr_enabled) {
            LOG_INFO("{} XATTR",
                     other.xattr_enabled.load() ? "Enable" : "Disable");
            setXattrEnabled(other.xattr_enabled.load());
        }
    }

    if (other.has.collections_enabled) {
        if (other.collections_enabled != collections_enabled) {
            LOG_INFO("{} collections_enabled",
                     other.collections_enabled.load() ? "Enable" : "Disable");
            setCollectionsEnabled(other.collections_enabled.load());
        }
    }

    if (other.has.breakpad) {
        bool changed = false;
        auto& b1 = breakpad;
        const auto& b2 = other.breakpad;

        if (b2.enabled != b1.enabled) {
            LOG_INFO("{} breakpad", b2.enabled ? "Enable" : "Disable");
            b1.enabled = b2.enabled;
            changed = true;
        }

        if (b2.minidump_dir != b1.minidump_dir) {
            LOG_INFO(
                    R"(Change minidump directory from "{}" to "{}")",
                    b1.minidump_dir,
                    b2.minidump_dir);
            b1.minidump_dir = b2.minidump_dir;
            changed = true;
        }

        if (b2.content != b1.content) {
            LOG_INFO("Change minidump content from {} to {}",
                     b1.content,
                     b2.content);
            b1.content = b2.content;
            changed = true;
        }

        if (changed) {
            notify_changed("breakpad");
        }
    }

    if (other.has.privilege_debug) {
        if (other.privilege_debug != privilege_debug) {
            bool value = other.isPrivilegeDebug();
            LOG_INFO("{} privilege debug", value ? "Enable" : "Disable");
            setPrivilegeDebug(value);
        }
    }

    if (other.has.opcode_attributes_override) {
        auto current = getOpcodeAttributesOverride();
        auto proposed = other.getOpcodeAttributesOverride();

        if (proposed != current) {
            LOG_INFO(
                    R"(Change opcode attributes from "{}" to "{}")",
                    current,
                    proposed);
            setOpcodeAttributesOverride(proposed);
        }
    }

    if (other.has.tracing_enabled) {
        if (other.isTracingEnabled() != isTracingEnabled()) {
            LOG_INFO("{} tracing support",
                     other.isTracingEnabled() ? "Enable" : "Disable");
        }
        setTracingEnabled(other.isTracingEnabled());
    }

    if (other.has.scramsha_fallback_salt) {
        const auto o = other.getScramshaFallbackSalt();
        const auto m = getScramshaFallbackSalt();

        if (o != m) {
            LOG_INFO(R"(Change scram fallback salt from {} to {})",
                     cb::UserDataView(m),
                     cb::UserDataView(o));
            setScramshaFallbackSalt(o);
        }
    }

    if (other.has.sasl_mechanisms) {
        auto mine = getSaslMechanisms();
        auto others = other.getSaslMechanisms();
        if (mine != others) {
            LOG_INFO(
                    R"(Change SASL mechanisms on normal connections from "{}" to "{}")",
                    mine,
                    others);
            setSaslMechanisms(others);
        }
    }

    if (other.has.ssl_sasl_mechanisms) {
        auto mine = getSslSaslMechanisms();
        auto others = other.getSslSaslMechanisms();
        if (mine != others) {
            LOG_INFO(
                    R"(Change SASL mechanisms on SSL connections from "{}" to "{}")",
                    mine,
                    others);
            setSslSaslMechanisms(others);
        }
    }

    if (other.has.external_auth_service) {
        if (isExternalAuthServiceEnabled() !=
            other.isExternalAuthServiceEnabled()) {
            LOG_INFO(
                    R"(Change external authentication service from "{}" to "{}")",
                    isExternalAuthServiceEnabled() ? "enabled" : "disabled",
                    other.isExternalAuthServiceEnabled() ? "enabled"
                                                         : "disabled");
            setExternalAuthServiceEnabled(other.isExternalAuthServiceEnabled());
        }
    }

    if (other.has.active_external_users_push_interval) {
        if (getActiveExternalUsersPushInterval() !=
            other.getActiveExternalUsersPushInterval()) {
            LOG_INFO(
                    R"(Change push interval for external users list from {}s to {}s)",
                    std::chrono::duration_cast<std::chrono::seconds>(
                            getActiveExternalUsersPushInterval())
                            .count(),
                    std::chrono::duration_cast<std::chrono::seconds>(
                            other.getActiveExternalUsersPushInterval())
                            .count());
            setActiveExternalUsersPushInterval(
                    other.getActiveExternalUsersPushInterval());
        }
    }

    if (other.has.allow_localhost_interface) {
        if (other.allow_localhost_interface != allow_localhost_interface) {
            LOG_INFO(R"(Change allow localhost interface from "{}" to "{}")",
                     isLocalhostInterfaceAllowed() ? "enabled" : "disabled",
                     other.isLocalhostInterfaceAllowed() ? "enabled"
                                                         : "disabled");
            setAllowLocalhostInterface(other.isLocalhostInterfaceAllowed());
        }
    }

    if (other.has.max_concurrent_commands_per_connection) {
        if (other.getMaxConcurrentCommandsPerConnection() !=
            getMaxConcurrentCommandsPerConnection()) {
            LOG_INFO(
                    "Change max number of concurrent commands per connection "
                    "from {} to {}",
                    other.getMaxConcurrentCommandsPerConnection(),
                    getMaxConcurrentCommandsPerConnection());
            setMaxConcurrentCommandsPerConnection(
                    other.getMaxConcurrentCommandsPerConnection());
        }
    }

    if (other.has.num_reader_threads &&
        other.getNumReaderThreads() != getNumReaderThreads()) {
        LOG_INFO("Change number of reader threads from: {} to {}",
                 threadConfig2String(getNumReaderThreads()),
                 threadConfig2String(other.getNumReaderThreads()));
        setNumReaderThreads(other.getNumReaderThreads());
    }

    if (other.has.num_writer_threads &&
        other.getNumWriterThreads() != getNumWriterThreads()) {
        LOG_INFO("Change number of writer threads from: {} to {}",
                 threadConfig2String(getNumWriterThreads()),
                 threadConfig2String(other.getNumWriterThreads()));
        setNumWriterThreads(other.getNumWriterThreads());
    }

    if (other.has.num_auxio_threads &&
        other.getNumAuxIoThreads() != getNumAuxIoThreads()) {
        LOG_INFO("Change number of AuxIO threads from: {} to {}",
                 getNumAuxIoThreads(),
                 other.getNumAuxIoThreads());
        setNumAuxIoThreads(other.getNumAuxIoThreads());
    }

    if (other.has.num_nonio_threads &&
        other.getNumNonIoThreads() != getNumNonIoThreads()) {
        LOG_INFO("Change number of NonIO threads from: {} to {}",
                 getNumNonIoThreads(),
                 other.getNumNonIoThreads());
        setNumNonIoThreads(other.getNumNonIoThreads());
    }

    if (other.has.prometheus_config) {
        auto nval = *other.prometheus_config.rlock();
        if (nval != *prometheus_config.rlock()) {
            switch (nval.second) {
            case AF_INET:
                LOG_INFO("Change prometheus port to IPv4 port {}", nval.first);
                break;
            case AF_INET6:
                LOG_INFO("Change prometheus port to IPv6 port {}", nval.first);
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
        LOG_INFO("Change number of storage threads from: {} to {}",
                 storageThreadConfig2String(getNumStorageThreads()),
                 storageThreadConfig2String(other.getNumStorageThreads()));
        setNumStorageThreads(other.getNumStorageThreads());

    }

    if (other.has.phosphor_config) {
        const auto o = other.getPhosphorConfig();
        const auto m = getPhosphorConfig();
        if (o != m) {
            LOG_INFO(R"(Change Phosphor config from "{}" to "{}")", o, m);
            setPhosphorConfig(o);
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

void Settings::setSaslMechanisms(const std::string& mechanisms) {
    std::string mechs;
    std::transform(mechanisms.begin(),
                   mechanisms.end(),
                   std::back_inserter(mechs),
                   toupper);
    sasl_mechanisms.wlock()->assign(mechs);
    has.sasl_mechanisms = true;
    notify_changed("sasl_mechanisms");
}

std::string Settings::getSslSaslMechanisms() const {
    return std::string{*ssl_sasl_mechanisms.rlock()};
}

void Settings::setSslSaslMechanisms(const std::string& mechanisms) {
    std::string mechs;
    std::transform(mechanisms.begin(),
                   mechanisms.end(),
                   std::back_inserter(mechs),
                   toupper);
    ssl_sasl_mechanisms.wlock()->assign(mechs);
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

static std::string to_string(const EventFramework& framework) {
    switch (framework) {
    case EventFramework::Bufferevent:
        return "bufferevent";
    case EventFramework::Folly:
        return "folly";
    }
    return "Invalid EventFramework: " + std::to_string(int(framework));
}

std::ostream& operator<<(std::ostream& os, const EventFramework& framework) {
    return os << to_string(framework);
}
