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
#include <gsl/gsl-lite.hpp>
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
#include <regex>
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

/**
 * Handle deprecated tags in the settings by simply ignoring them
 */
static void ignore_entry(Settings&, const nlohmann::json&) {
}

static void handle_always_collect_trace_info(Settings& s,
                                             const nlohmann::json& obj) {
    s.setAlwaysCollectTraceInfo(obj.get<bool>());
}

/**
 * Handle the "rbac_file" tag in the settings
 *
 * ns_server don't synchronize updates to the files with memcached
 * (see MB-38270) we can't really check for the file existence as
 * we may race with ns_server trying to install a new version of the
 * file.
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_rbac_file(Settings& s, const nlohmann::json& obj) {
    s.setRbacFile(obj.get<std::string>());
}

/**
 * Handle the "privilege_debug" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_privilege_debug(Settings& s, const nlohmann::json& obj) {
    s.setPrivilegeDebug(obj.get<bool>());
}

/**
 * Handle the "audit_file" tag in the settings
 *
 * ns_server don't synchronize updates to the files with memcached
 * (see MB-38270) we can't really check for the file existence as
 * we may race with ns_server trying to install a new version of the
 * file.
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_audit_file(Settings& s, const nlohmann::json& obj) {
    s.setAuditFile(obj.get<std::string>());
}

static void handle_error_maps_dir(Settings& s, const nlohmann::json& obj) {
    s.setErrorMapsDir(obj.get<std::string>());
}

/**
 * Handle the "threads" tag in the settings
 *
 *  The value must be an integer value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_threads(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"threads\" must be an unsigned int");
    }
    s.setNumWorkerThreads(gsl::narrow_cast<size_t>(obj.get<unsigned int>()));
}

static void handle_scramsha_fallback_salt(Settings& s,
                                          const nlohmann::json& obj) {
    // Try to base64 decode it to validate that it is a legal value..
    auto salt = obj.get<std::string>();
    cb::base64::decode(salt);
    s.setScramshaFallbackSalt(salt);
}

static void handle_phosphor_config(Settings& s, const nlohmann::json& obj) {
    auto config = obj.get<std::string>();
    // throw an exception if the config is invalid
    phosphor::TraceConfig::fromString(config);
    s.setPhosphorConfig(config);
}

static void handle_external_auth_service(Settings& s,
                                         const nlohmann::json& obj) {
    s.setExternalAuthServiceEnabled(obj.get<bool>());
}

static void handle_active_external_users_push_interval(
        Settings& s, const nlohmann::json& obj) {
    switch (obj.type()) {
    case nlohmann::json::value_t::number_unsigned:
        s.setActiveExternalUsersPushInterval(
                std::chrono::seconds(obj.get<int>()));
        break;
    case nlohmann::json::value_t::string:
        s.setActiveExternalUsersPushInterval(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        cb::text2time(obj.get<std::string>())));
        break;
    default:
        cb::throwJsonTypeError(R"("active_external_users_push_interval" must
                                be a number or string)");
    }
}

static void handle_max_concurrent_commands_per_connection(
        Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("max_concurrent_commands_per_connection" must be a positive number)");
    }
    s.setMaxConcurrentCommandsPerConnection(obj.get<size_t>());
}

/**
 * Handle the "tracing_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_tracing_enabled(Settings& s, const nlohmann::json& obj) {
    s.setTracingEnabled(obj.get<bool>());
}

/// Handle the "enforce_tenant_limits_enabled" tag in the settings. Throws
/// an exception for incorrect datatype
static void handle_enforce_tenant_limits_enabled(Settings& s,
                                                 const nlohmann::json& obj) {
    s.setEnforceTenantLimitsEnabled(obj.get<bool>());
}

/**
 * Handle the "stdin_listener" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_stdin_listener(Settings& s, const nlohmann::json& obj) {
    s.setStdinListenerEnabled(obj.get<bool>());
}

/**
 * Handle "default_reqs_per_event", "reqs_per_event_high_priority",
 * "reqs_per_event_med_priority" and "reqs_per_event_low_priority" tag in
 * the settings
 *
 *  The value must be a integer value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_reqs_event(Settings& s,
                              const nlohmann::json& obj,
                              EventPriority priority,
                              const std::string& msg) {
    // Throw if not an unsigned int. Bool values can be converted to an int
    // in an nlohmann::json.get<unsigned int>() so we need to check this
    // explicitly.
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(msg + " must be an unsigned int");
    }

    s.setRequestsPerEventNotification(gsl::narrow<int>(obj.get<unsigned int>()),
                                      priority);
}

static void handle_default_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(s, obj, EventPriority::Default, "default_reqs_per_event");
}

static void handle_high_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::High, "reqs_per_event_high_priority");
}

static void handle_med_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::Medium, "reqs_per_event_med_priority");
}

static void handle_low_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::Low, "reqs_per_event_low_priority");
}

static void handle_tcp_keepalive_idle(Settings& s, const nlohmann::json& obj) {
    s.setTcpKeepAliveIdle(std::chrono::seconds(obj.get<uint32_t>()));
}
static void handle_tcp_keepalive_interval(Settings& s,
                                          const nlohmann::json& obj) {
    s.setTcpKeepAliveInterval(std::chrono::seconds(obj.get<uint32_t>()));
}
static void handle_tcp_keepalive_probes(Settings& s,
                                        const nlohmann::json& obj) {
    s.setTcpKeepAliveProbes(obj.get<uint32_t>());
}

/**
 * Handle the "verbosity" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_verbosity(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"verbosity\" must be an unsigned int");
    }
    s.setVerbose(gsl::narrow<int>(obj.get<unsigned int>()));
}

/**
 * Handle the "connection_idle_time" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_connection_idle_time(Settings& s,
                                        const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                "\"connection_idle_time\" must be an unsigned "
                "int");
    }
    s.setConnectionIdleTime(obj.get<unsigned int>());
}

/**
 * Handle the "datatype_snappy" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_datatype_json(Settings& s, const nlohmann::json& obj) {
    s.setDatatypeJsonEnabled(obj.get<bool>());
}

/**
 * Handle the "datatype_snappy" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_datatype_snappy(Settings& s, const nlohmann::json& obj) {
    s.setDatatypeSnappyEnabled(obj.get<bool>());
}

/**
 * Handle the "root" tag in the settings
 *
 * The value must be a string that points to a directory that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_root(Settings& s, const nlohmann::json& obj) {
    auto dir = obj.get<std::string>();

    if (!cb::io::isDirectory(dir)) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                "'root': '" + dir + "'");
    }

    s.setRoot(dir);
}

/**
 * Handle the "get_max_packet_size" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_max_packet_size(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"max_packet_size\" must be an unsigned int");
    }
    s.setMaxPacketSize(gsl::narrow<uint32_t>(obj.get<unsigned int>()) * 1024 *
                       1024);
}

static void handle_max_send_queue_size(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("max_send_queue_size" must be an unsigned number)");
    }
    s.setMaxSendQueueSize(obj.get<size_t>() * 1024 * 1024);
}

static void handle_max_connections(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("max_connections" must be a positive number)");
    }
    s.setMaxConnections(obj.get<size_t>());
}

static void handle_system_connections(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("system_connections" must be a positive number)");
    }
    s.setSystemConnections(obj.get<size_t>());
}

void handle_free_connection_pool_size(Settings& s, const nlohmann::json& obj) {
    s.setFreeConnectionPoolSize(obj.get<size_t>());
}

static void handle_connection_limit_mode(Settings& s,
                                         const nlohmann::json& obj) {
    const auto str = obj.get<std::string>();
    if (str == "disconnect") {
        s.setConnectionLimitMode(ConnectionLimitMode::Disconnect);
    } else if (str == "recycle") {
        s.setConnectionLimitMode(ConnectionLimitMode::Recycle);
    } else {
        throw std::invalid_argument(
                R"(connection_limit_mode must be "disconnect" or "recycle")");
    }
}

static void handle_max_client_connection_details(Settings& s,
                                                 const nlohmann::json& obj) {
    s.setMaxClientConnectionDetails(obj.get<size_t>());
}

/**
 * Handle the "sasl_mechanisms" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_sasl_mechanisms(Settings& s, const nlohmann::json& obj) {
    s.setSaslMechanisms(obj.get<std::string>());
}

/**
 * Handle the "ssl_sasl_mechanisms" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_sasl_mechanisms(Settings& s, const nlohmann::json& obj) {
    s.setSslSaslMechanisms(obj.get<std::string>());
}

/**
 * Handle the "dedupe_nmvb_maps" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_dedupe_nmvb_maps(Settings& s, const nlohmann::json& obj) {
    s.setDedupeNmvbMaps(obj.get<bool>());
}

/**
 * Handle the "xattr_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_xattr_enabled(Settings& s, const nlohmann::json& obj) {
    s.setXattrEnabled(obj.get<bool>());
}

/**
 * Handle the "client_cert_auth" tag in the settings
 *
 *  The value must be a string value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_client_cert_auth(Settings& s, const nlohmann::json& obj) {
    auto config = cb::x509::ClientCertConfig::create(obj);
    s.reconfigureClientCertAuth(std::move(config));
}

/**
 * Handle the "collections_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_collections_enabled(Settings& s, const nlohmann::json& obj) {
    s.setCollectionsPrototype(obj.get<bool>());
}

static void handle_opcode_attributes_override(Settings& s,
                                              const nlohmann::json& obj) {
    if (obj.type() != nlohmann::json::value_t::object) {
        throw std::invalid_argument(
                R"("opcode_attributes_override" must be an object)");
    }
    s.setOpcodeAttributesOverride(obj.dump());
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

static void handle_num_reader_threads(Settings& s,  const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumReaderThreads(obj.get<size_t>());
    } else {
        const auto val = obj.get<std::string>();
        s.setNumReaderThreads(parseThreadConfigSpec("num_reader_threads", val));
    }
}

static void handle_num_writer_threads(Settings& s,  const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumWriterThreads(obj.get<size_t>());
    } else {
        const auto val = obj.get<std::string>();
        s.setNumWriterThreads(parseThreadConfigSpec("num_writer_threads", val));
    }
}

static void handle_num_auxio_threads(Settings& s, const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumAuxIoThreads(obj.get<size_t>());
    } else if (obj.is_string() && obj.get<std::string>() == "default") {
        s.setNumAuxIoThreads(
                static_cast<int>(ThreadPoolConfig::AuxIoThreadCount::Default));
    } else {
        throw std::invalid_argument(
                fmt::format("Value to set number of AuxIO threads must be an "
                            "unsigned integer or \"default\"! Value:'{}'",
                            obj.dump()));
    }
}

static void handle_num_nonio_threads(Settings& s, const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumNonIoThreads(obj.get<size_t>());
    } else if (obj.is_string() && obj.get<std::string>() == "default") {
        s.setNumNonIoThreads(static_cast<int>(ThreadPoolConfig::NonIoThreadCount::Default));
    } else {
        throw std::invalid_argument(
                fmt::format("Value to set number of NonIO threads must be an "
                            "unsigned integer or \"default\"!! Value:'{}'",
                            obj.dump()));
    }
}

static void handle_num_io_threads_per_core(Settings& s, const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumIOThreadsPerCore(obj.get<int>());
    } else if (obj.is_string() && obj.get<std::string>() == "default") {
        s.setNumIOThreadsPerCore(
                std::underlying_type_t<ThreadPoolConfig::IOThreadsPerCore>(
                        ThreadPoolConfig::IOThreadsPerCore::Default));
    } else {
        throw std::invalid_argument(
                fmt::format("Number of IO threads per core must be an "
                            "unsigned integer or \"default\"!! Value:'{}'",
                            obj.dump()));
    }
}

static void handle_num_storage_threads(Settings& s, const nlohmann::json& obj) {
    if (obj.is_number_unsigned()) {
        s.setNumStorageThreads(obj.get<size_t>());
    } else {
        const auto val = obj.get<std::string>();
        s.setNumStorageThreads(
                parseStorageThreadConfigSpec("num_storage_threads", val));
    }
}

static void handle_logger(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_object()) {
        cb::throwJsonTypeError(R"("opcode_attributes_override" must be an
                              object)");
    }
    cb::logger::Config config(obj);
    s.setLoggerConfig(config);
}

static void handle_portnumber_file(Settings& s, const nlohmann::json& obj) {
    s.setPortnumberFile(obj.get<std::string>());
}

static void handle_parent_identifier(Settings& s, const nlohmann::json& obj) {
    s.setParentIdentifier(obj.get<int>());
}

/**
 * Handle the "interfaces" tag in the settings
 *
 *  The value must be an array
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_interfaces(Settings& s, const nlohmann::json& obj) {
    if (obj.type() != nlohmann::json::value_t::array) {
        cb::throwJsonTypeError("\"interfaces\" must be an array");
    }

    for (const auto& o : obj) {
        if (o.type() != nlohmann::json::value_t::object) {
            throw std::invalid_argument(
                    "Elements in the \"interfaces\" array must be objects");
        }
        NetworkInterface ifc(o);
        if (ifc.port == 0 && ifc.tag.empty()) {
            throw std::invalid_argument("Ephemeral ports must have a tag");
        }
        s.addInterface(ifc);
    }
}

static void handle_breakpad(Settings& s, const nlohmann::json& obj) {
    cb::breakpad::Settings breakpad(obj);
    s.setBreakpadSettings(breakpad);
}

static void handle_prometheus(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_object()) {
        cb::throwJsonTypeError(R"("prometheus" must be an object)");
    }
    auto iter = obj.find("port");
    if (iter == obj.end() || !iter->is_number()) {
        throw std::invalid_argument(
                R"("prometheus.port" must be present and a number)");
    }
    const auto port = iter->get<in_port_t>();
    iter = obj.find("family");
    if (iter == obj.end() || !iter->is_string()) {
        throw std::invalid_argument(
                R"("prometheus.family" must be present and a string)");
    }
    const auto val = iter->get<std::string>();
    sa_family_t family;
    if (val == "inet") {
        family = AF_INET;
    } else if (val == "inet6") {
        family = AF_INET6;
    } else {
        throw std::invalid_argument(
                R"("prometheus.family" must be "inet" or "inet6")");
    }

    s.setPrometheusConfig({port, family});
}

static void handle_whitelist_localhost_interface(Settings& s,
                                                 const nlohmann::json& obj) {
    s.setWhitelistLocalhostInterface(obj.get<bool>());
}

static void handle_dcp_disconnect_when_stuck_timeout_seconds(
        Settings& s, const nlohmann::json& obj) {
    s.setDcpDisconnectWhenStuckTimeout(std::chrono::seconds(obj.get<size_t>()));
}

static void handle_dcp_disconnect_when_stuck_name_regex(
        Settings& s, const nlohmann::json& obj) {
    s.setDcpDisconnectWhenStuckNameRegexFromBase64(obj.get<std::string>());
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
    auto decoded = cb::base64::decode(val);
    setDcpDisconnectWhenStuckNameRegex(std::string{
            reinterpret_cast<const char*>(decoded.data()), decoded.size()});
}

void Settings::setDcpDisconnectWhenStuckNameRegex(std::string val) {
    // This must be a usable by std::regex, which will throw if not.
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
    // use the ones in the configuration file.. (this is a bit messy)
    interfaces.wlock()->clear();

    struct settings_config_tokens {
        /**
         * The key in the configuration
         */
        std::string key;

        /**
         * A callback method used by the Settings object when we're parsing
         * the config attributes.
         *
         * @param settings the Settings object to update
         * @param obj the current object in the configuration we're looking at
         * @throws nlohmann::json::exception if the json cannot be parsed
         * @throws std::invalid_argument for other json input errors
         */
        void (*handler)(Settings& settings, const nlohmann::json& obj);
    };

    std::vector<settings_config_tokens> handlers = {
            {"admin", ignore_entry},
            {"always_collect_trace_info", handle_always_collect_trace_info},
            {"rbac_file", handle_rbac_file},
            {"privilege_debug", handle_privilege_debug},
            {"audit_file", handle_audit_file},
            {"error_maps_dir", handle_error_maps_dir},
            {"threads", handle_threads},
            {"interfaces", handle_interfaces},
            {"logger", handle_logger},
            {"default_reqs_per_event", handle_default_reqs_event},
            {"reqs_per_event_high_priority", handle_high_reqs_event},
            {"reqs_per_event_med_priority", handle_med_reqs_event},
            {"reqs_per_event_low_priority", handle_low_reqs_event},
            {"verbosity", handle_verbosity},
            {"connection_idle_time", handle_connection_idle_time},
            {"datatype_json", handle_datatype_json},
            {"datatype_snappy", handle_datatype_snappy},
            {"root", handle_root},
            {"breakpad", handle_breakpad},
            {"max_packet_size", handle_max_packet_size},
            {"max_send_queue_size", handle_max_send_queue_size},
            {"max_connections", handle_max_connections},
            {"system_connections", handle_system_connections},
            {"free_connection_pool_size", handle_free_connection_pool_size},
            {"connection_limit_mode", handle_connection_limit_mode},
            {"max_client_connection_details",
             handle_max_client_connection_details},
            {"sasl_mechanisms", handle_sasl_mechanisms},
            {"ssl_sasl_mechanisms", handle_ssl_sasl_mechanisms},
            {"stdin_listener", handle_stdin_listener},
            {"dedupe_nmvb_maps", handle_dedupe_nmvb_maps},
            {"tcp_keepalive_idle", handle_tcp_keepalive_idle},
            {"tcp_keepalive_interval", handle_tcp_keepalive_interval},
            {"tcp_keepalive_probes", handle_tcp_keepalive_probes},
            {"xattr_enabled", handle_xattr_enabled},
            {"client_cert_auth", handle_client_cert_auth},
            {"collections_enabled", handle_collections_enabled},
            {"opcode_attributes_override", handle_opcode_attributes_override},
            {"num_reader_threads", handle_num_reader_threads},
            {"num_writer_threads", handle_num_writer_threads},
            {"num_storage_threads", handle_num_storage_threads},
            {"num_auxio_threads", handle_num_auxio_threads},
            {"num_nonio_threads", handle_num_nonio_threads},
            {"num_io_threads_per_core", handle_num_io_threads_per_core},
            {"tracing_enabled", handle_tracing_enabled},
            {"enforce_tenant_limits_enabled",
             handle_enforce_tenant_limits_enabled},
            {"scramsha_fallback_salt", handle_scramsha_fallback_salt},
            {"external_auth_service", handle_external_auth_service},
            {"active_external_users_push_interval",
             handle_active_external_users_push_interval},
            {"max_concurrent_commands_per_connection",
             handle_max_concurrent_commands_per_connection},
            {"phosphor_config", handle_phosphor_config},
            {"prometheus", handle_prometheus},
            {"portnumber_file", handle_portnumber_file},
            {"parent_identifier", handle_parent_identifier},
            {"whitelist_localhost_interface",
             handle_whitelist_localhost_interface},
            {"dcp_disconnect_when_stuck_timeout_seconds",
             handle_dcp_disconnect_when_stuck_timeout_seconds},
            {"dcp_disconnect_when_stuck_name_regex",
             handle_dcp_disconnect_when_stuck_name_regex}};

    for (const auto& obj : json.items()) {
        bool found = false;
        for (auto& handler : handlers) {
            if (handler.key == obj.key()) {
                handler.handler(*this, obj.value());
                found = true;
                break;
            }
        }

        if (!found) {
            LOG_WARNING(R"(Unknown key "{}" in config ignored.)", obj.key());
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

    if (other.getConnectionLimitMode() != getConnectionLimitMode()) {
        if (other.getConnectionLimitMode() == ConnectionLimitMode::Recycle) {
            setConnectionLimitMode(ConnectionLimitMode::Recycle);
            setFreeConnectionPoolSize(other.getFreeConnectionPoolSize());
            LOG_INFO(
                    "Change connection limit mode from disconnect to recycle "
                    "with a pool size of {}",
                    getFreeConnectionPoolSize());
        } else {
            setConnectionLimitMode(ConnectionLimitMode::Disconnect);
            setFreeConnectionPoolSize(0);
            LOG_INFO_RAW(
                    "Change connection limit mode from recycle to disconnect");
        }
    } else if (getConnectionLimitMode() == ConnectionLimitMode::Recycle &&
               other.getFreeConnectionPoolSize() !=
                       getFreeConnectionPoolSize()) {
        LOG_INFO("Change free connections pool size from {} to {}",
                 getFreeConnectionPoolSize(),
                 other.getFreeConnectionPoolSize());
        setFreeConnectionPoolSize(other.getFreeConnectionPoolSize());
    }

    if (other.has.max_client_connection_details) {
        if (other.max_client_connection_details !=
            max_client_connection_details) {
            LOG_INFO("Change max client connection details from {} to {}",
                     max_client_connection_details,
                     other.max_client_connection_details);
            setMaxClientConnectionDetails(other.max_client_connection_details);
        }
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
            setCollectionsPrototype(other.collections_enabled.load());
        }
    }

    if (other.has.enforce_tenant_limits_enabled) {
        if (other.enforce_tenant_limits_enabled !=
            enforce_tenant_limits_enabled) {
            LOG_INFO("{} tenant resource control",
                     other.enforce_tenant_limits_enabled.load() ? "Enable"
                                                                : "Disable");
            setEnforceTenantLimitsEnabled(
                    other.enforce_tenant_limits_enabled.load());
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
                     to_string(b1.content),
                     to_string(b2.content));
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

    if (other.has.whitelist_localhost_interface) {
        if (other.whitelist_localhost_interface !=
            whitelist_localhost_interface) {
            LOG_INFO(
                    R"(Change whitelist of localhost interface from "{}" to "{}")",
                    isLocalhostInterfaceWhitelisted() ? "enabled" : "disabled",
                    other.isLocalhostInterfaceWhitelisted() ? "enabled"
                                                            : "disabled");
            setWhitelistLocalhostInterface(
                    other.isLocalhostInterfaceWhitelisted());
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

    if (other.has.num_io_threads_per_core) {
        const auto oldTPC = getNumIOThreadsPerCore();
        const auto newTPC = other.getNumIOThreadsPerCore();
        if (oldTPC != newTPC) {
            LOG_INFO("Change number of IO threads per core from: {} to {}",
                     oldTPC,
                     newTPC);
            setNumIOThreadsPerCore(newTPC);
        }
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

    if (other.has.dcp_disconnect_when_stuck_name_regex) {
        // Both these values are the decoded values, not base64 encoded
        const auto newValue = other.getDcpDisconnectWhenStuckNameRegex();
        const auto current = getDcpDisconnectWhenStuckNameRegex();
        if (newValue != current) {
            LOG_INFO(
                    R"(Change dcp_disconnect_when_stuck_name_regex from "{}" to "{}")",
                    current,
                    newValue);
            // Use the protected setter which validates newValue
            setDcpDisconnectWhenStuckNameRegex(std::move(newValue));
        }
    }

    if (other.has.dcp_disconnect_when_stuck_timeout_seconds) {
        const auto newValue = other.getDcpDisconnectWhenStuckTimeout();
        const auto current = getDcpDisconnectWhenStuckTimeout();
        if (newValue != current) {
            LOG_INFO(
                    R"(Change dcp_disconnect_when_stuck_timeout_seconds from {}s to {}s)",
                    current.count(),
                    newValue.count());
            setDcpDisconnectWhenStuckTimeout(newValue);
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
