/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "client_cert_config.h"
#include "logger/logger_config.h"
#include "network_interface.h"

#include <memcached/engine.h>
#include <relaxed_atomic.h>
#include <utilities/breakpad_settings.h>

#include <folly/Synchronized.h>

#include <atomic>
#include <cstdarg>
#include <deque>
#include <functional>
#include <map>
#include <string>
#include <vector>

enum class EventFramework : uint8_t { Bufferevent, Folly };
std::ostream& operator<<(std::ostream& os, const EventFramework& framework);

enum class EventPriority {
    High,
    Medium,
    Low,
    Default
};

/// The various deployment models the server may be in
enum class DeploymentModel {
    /// The good old Couchbase server as we know and love
    Normal,
    /// The serverless mode
    Serverless
};

/// The different modes of operation when we hit the max number of connections
enum class ConnectionLimitMode {
    /// Disconnect new connections when we hit the max connections (default)
    Disconnect,
    /// Recycle one of the least recently used connections when we hit the
    /// max connections
    Recycle
};
std::string to_string(ConnectionLimitMode mode);
std::ostream& operator<<(std::ostream& os, const ConnectionLimitMode& mode);

/**
 * Globally accessible settings as derived from the commandline / JSON config
 * file.
 *
 * Typically only the singleton instance (accessible via instance() is used.
 */
class Settings {
public:
    Settings();
    ~Settings();

    /**
     * Create and initialize a settings object from the given JSON
     *
     * @param json
     * @throws nlohmann::json::exception for parsing errors
     * @throws std::invalid_argument for JSON structure errors
     */
    explicit Settings(const nlohmann::json& json);

    Settings(const Settings&) = delete;

    /// Returns a reference to the singleton instance of Settings.
    static Settings& instance();

    /**
     * Allow the global settings instance to be changed by loading the
     * JSON configuration file.
     *
     * The global Settings instance should have configured the
     * change listeners already so that it may update the internal cached
     * values (if used).
     *
     * @param other
     */
    void reconfigure(const nlohmann::json& json);

    bool alwaysCollectTraceInfo() const {
        return always_collect_trace_info.load(std::memory_order_consume);
    }

    void setAlwaysCollectTraceInfo(bool value) {
        always_collect_trace_info.store(value, std::memory_order_release);
        has.always_collect_trace_info = true;
        notify_changed("always_collect_trace_info");
    }

    /**
     * Get the name of the file containing the RBAC data
     *
     * @return the absolute path to the rbac file
     */
    const std::string& getRbacFile() const {
        return rbac_file;
    }

    /**
     * Set the name of the file containing the RBAC data
     *
     * @param file the absolute path to the rbac file
     */
    void setRbacFile(std::string file) {
        has.rbac_file = true;
        rbac_file = std::move(file);
    }

    /**
     * If enabled we'll always return true from the privilege checks
     *
     * @return true if we're running in privilege debug mode.
     */
    bool isPrivilegeDebug() const {
        return privilege_debug.load(std::memory_order_relaxed);
    }

    /**
     * Set if privilege mode is enabled or not
     * @param enable
     */
    void setPrivilegeDebug(bool enable) {
        has.privilege_debug = true;
        privilege_debug.store(enable, std::memory_order_relaxed);
        notify_changed("privilege_debug");
    }

    /**
     * Get the number of frontend worker threads
     *
     * @return the configured amount of worker threads
     */
    size_t getNumWorkerThreads() const {
        return num_threads;
    }

    /**
     * Set the number of frontend worker threads
     *
     * @param num the new number of threads
     */
    void setNumWorkerThreads(size_t num) {
        has.threads = true;
        num_threads = num;
        notify_changed("threads");
    }

    /**
     * Add a new interface definition to the list of interfaces provided
     * by the server.
     *
     * @param ifc the interface to add
     */
    void addInterface(const NetworkInterface& ifc) {
        interfaces.wlock()->push_back(ifc);
        has.interfaces = true;
        notify_changed("interfaces");
    }

    /**
     * Get vector containing all of the interfaces the system should
     * listen on
     *
     * @return the vector of interfaces
     */
    std::vector<NetworkInterface> getInterfaces() const {
        return std::vector<NetworkInterface>{*interfaces.rlock()};
    }

    /**
     * Should we use standard input listener?
     *
     * @return true if enabled, false otherwise
     */
    bool isStdinListenerEnabled() const {
        return stdin_listener.load();
    }

    /**
     * Set the mode for the standard input listener
     *
     * @param enabled the new value
     */
    void setStdinListenerEnabled(bool enabled) {
        stdin_listener.store(enabled);
        has.stdin_listener = true;
        notify_changed("stdin_listener");
    }

    cb::logger::Config getLoggerConfig() const {
        auto config = logger_settings;
        // log_level is synthesised from settings.verbose.
        config.log_level = getLogLevel();
        return config;
    };

    void setLoggerConfig(const cb::logger::Config& config) {
        has.logger = true;
        logger_settings = config;
        notify_changed("logger");
    };

    /**
     * Get the name of the file containing the audit configuration
     *
     * @return the name of the file containing audit configuration
     */
    const std::string& getAuditFile() const {
        return audit_file;
    }

    /**
     * Set the name of the file containing the audit configuration
     *
     * @param file the new name of the file containing audit configuration
     */
    void setAuditFile(std::string file) {
        has.audit = true;
        audit_file = std::move(file);
        notify_changed("audit_file");
    }

    const std::string& getErrorMapsDir() const {
        return Settings::error_maps_dir;
    }

    void setErrorMapsDir(std::string dir) {
        has.error_maps = true;
        error_maps_dir = std::move(dir);
        notify_changed("error_maps_dir");
    }

    /**
     * Get the verbosity level for the node
     *
     * @return the verbosity level
     */
    int getVerbose() const {
        return verbose.load();
    }

    /**
     * Set the verbosity level to use
     *
     * @param value the new value for the verbosity level
     */
    void setVerbose(int value) {
        verbose.store(value);
        has.verbose = true;
        notify_changed("verbosity");
    }

    // Return the log level as defined by the current verbosity.
    spdlog::level::level_enum getLogLevel() const;

    /**
     * Get the idle time for a connection. Connections that stays idle
     * longer than this limit is automatically disconnected.
     *
     * @return the idle time in seconds
     */
    size_t getConnectionIdleTime() const {
        return connection_idle_time;
    }

    /**
     * Set the connection idle time
     *
     * @param value the number of seconds connections should stay idle
     */
    void setConnectionIdleTime(size_t value) {
        Settings::connection_idle_time = value;
        has.connection_idle_time = true;
        notify_changed("connection_idle_time");
    }

    /**
     * Get the root directory of the couchbase installation
     *
     * This allows the process to locate files in <code>etc/security</code> and
     * other locations relative to the installation root
     *
     * @return
     */
    const std::string& getRoot() const {
        return root;
    }

    /**
     * Set the root directory for the Couchbase installation
     *
     * @param root The root directory of the installation
     */
    void setRoot(std::string value) {
        root = std::move(value);
        has.root = true;
        notify_changed("root");
    }

    size_t getMaxConnections() const {
        return max_connections.load(std::memory_order_consume);
    }

    void setMaxConnections(size_t num, bool notify = true) {
        max_connections.store(num, std::memory_order_release);
        has.max_connections = true;
        if (notify) {
            notify_changed("max_connections");
        }
    }

    size_t getSystemConnections() const {
        return system_connections.load(std::memory_order_consume);
    }

    void setSystemConnections(size_t num) {
        system_connections.store(num, std::memory_order_release);
        has.system_connections = true;
        notify_changed("system_connections");
    }

    size_t getMaxUserConnections() const {
        return getMaxConnections() - getSystemConnections();
    }

    size_t getFreeConnectionPoolSize() const {
        if (connection_limit_mode == ConnectionLimitMode::Recycle) {
            const auto ret =
                    free_connection_pool_size.load(std::memory_order_acquire);
            if (!ret) {
                return getMaxUserConnections() / 100;
            }

            return ret;
        }
        return 0;
    }

    void setFreeConnectionPoolSize(size_t num) {
        free_connection_pool_size.store(num, std::memory_order_release);
        has.free_connection_pool_size = true;
        notify_changed("free_connection_pool_size");
    }

    ConnectionLimitMode getConnectionLimitMode() const {
        return connection_limit_mode.load(std::memory_order_acquire);
    }

    void setConnectionLimitMode(ConnectionLimitMode mode) {
        connection_limit_mode.store(mode, std::memory_order_release);
        has.connection_limit_mode = true;
        notify_changed("connection_limit_mode");
    }

    size_t getMaxConcurrentCommandsPerConnection() const;

    void setMaxConcurrentCommandsPerConnection(size_t num);

    /**
     * Set the number of request to handle per notification from the
     * event library
     *
     * @param num the number of requests to handle
     * @param priority The priority to update
     */
    void setRequestsPerEventNotification(int num, EventPriority priority) {
        switch (priority) {
        case EventPriority::High:
            reqs_per_event_high_priority = num;
            has.reqs_per_event_high_priority = true;
            notify_changed("reqs_per_event_high_priority");
            return;
        case EventPriority::Medium:
            reqs_per_event_med_priority = num;
            has.reqs_per_event_med_priority = true;
            notify_changed("reqs_per_event_med_priority");
            return;
        case EventPriority::Low:
            reqs_per_event_low_priority = num;
            has.reqs_per_event_low_priority = true;
            notify_changed("reqs_per_event_low_priority");
            return;
        case EventPriority::Default:
            default_reqs_per_event = num;
            has.default_reqs_per_event = true;
            notify_changed("default_reqs_per_event");
            return;
        }
        throw std::invalid_argument(
            "setRequestsPerEventNotification: Unknown priority");
    }

    /**
     * Get the number of requests to handle per callback from the event
     * library
     *
     * @param priority the priority of interest
     * @return the number of request to handle per callback
     */
    int getRequestsPerEventNotification(EventPriority priority) const {
        switch (priority) {
        case EventPriority::High:
            return reqs_per_event_high_priority;
        case EventPriority::Medium:
            return reqs_per_event_med_priority;
        case EventPriority::Low:
            return reqs_per_event_low_priority;
        case EventPriority::Default:
            return default_reqs_per_event;
        }
        throw std::invalid_argument(
            "setRequestsPerEventNotification: Unknown priority");
    }

    /**
     * Is PROTOCOL_BINARY_DATATYPE_JSON supported or not
     *
     * @return true if clients may use JSON support
     */
    bool isDatatypeJsonEnabled() const {
        return datatype_json;
    }

    /**
     * Is PROTOCOL_BINARY_DATATYPE_SNAPPY supported or not
     *
     * @return true if clients may use snappy support
     */
    bool isDatatypeSnappyEnabled() const {
        return datatype_snappy;
    }

    /**
     * Set if PROTOCOL_BINARY_DATATYPE_JSON support should be enabled or not
     *
     * @param enabled true if clients should be able to use json support
     */
    void setDatatypeJsonEnabled(bool enabled) {
        datatype_json = enabled;
        has.datatype_json = true;
        notify_changed("datatype_json");
    }

    /**
     * Set if PROTOCOL_BINARY_DATATYPE_SNAPPY support should be enabled or
     * not
     *
     * @param enabled true if clients should be able to use snappy support
     */
    void setDatatypeSnappyEnabled(bool enabled) {
        datatype_snappy = enabled;
        has.datatype_snappy = true;
        notify_changed("datatype_snappy");
    }

    /**
     * Get the maximum size of a packet the system should try to inspect.
     * Packets exceeding this limit will cause the client to be disconnected
     *
     * @return the maximum size in bytes
     */
    uint32_t getMaxPacketSize() const {
        return max_packet_size;
    }

    /**
     * Set the maximum size of a packet the system should try to inspect.
     * Packets exceeding this limit will cause the client to be disconnected
     *
     * @param max the new maximum size in bytes
     */
    void setMaxPacketSize(uint32_t max) {
        max_packet_size = max;
        has.max_packet_size = true;
        notify_changed("max_packet_size");
    }

    /// get the max number of bytes we want in the send queue before we
    /// stop executing new commands for the client until it drains the
    /// socket
    size_t getMaxSendQueueSize() const {
        return max_send_queue_size.load(std::memory_order_acquire);
    }

    /// Set the new maximum number of bytes we want to store in the send
    /// queue for the client before we stop executing new commands (or
    /// add data to a DCP pipe) for a given client to avoid clients
    /// reserving gigabytes of memory in the server
    void setMaxSendQueueSize(size_t max) {
        max_send_queue_size.store(max, std::memory_order_release);
        has.max_send_queue_size = true;
        notify_changed("max_send_queue_size");
    }

    void reconfigureClientCertAuth(
            std::unique_ptr<cb::x509::ClientCertConfig> config) {
        client_cert_mapper.reconfigure(std::move(config));
        has.client_cert_auth = true;
        notify_changed("client_cert_auth");
    }

    std::pair<cb::x509::Status, std::string> lookupUser(X509* cert) const {
        return client_cert_mapper.lookupUser(cert);
    }

    /**
     * Get the list of available SASL Mechanisms
     *
     * @return all SASL mechanisms the client may use
     */
    std::string getSaslMechanisms() const;

    /**
     * Set the list of available SASL Mechanisms
     *
     * @param sasl_mechanisms the new list of sasl mechanisms
     */
    void setSaslMechanisms(const std::string& sasl_mechanisms);

    /**
     * Get the list of available SASL Mechanisms to use for SSL
     *
     * @return all SASL mechanisms the client may use
     */
    std::string getSslSaslMechanisms() const;

    /**
     * Set the list of available SASL Mechanisms to use for SSL connections
     *
     * @param sasl_mechanisms the new list of sasl mechanisms
     */
    void setSslSaslMechanisms(const std::string& ssl_sasl_mechanisms);

    /**
     * Should the server return the cluster map it has already sent a
     * client as part of the payload of <em>not my vbucket</em> errors.
     *
     * @return true if the node should deduplicate such maps
     */
    bool isDedupeNmvbMaps() const {
        return dedupe_nmvb_maps.load();
    }

    /**
     * Set if the server should return the cluster map it has already sent a
     * client as part of the payload of <em>not my vbucket</em> errors.
     *
     * @param enable true if the node should deduplicate such maps
     */
    void setDedupeNmvbMaps(bool enable) {
        dedupe_nmvb_maps.store(enable);
        has.dedupe_nmvb_maps = true;
        notify_changed("dedupe_nmvb_maps");
    }

    /**
     * Get the breakpad settings
     *
     * @return the settings used for Breakpad
     */
    const cb::breakpad::Settings& getBreakpadSettings() const {
        return breakpad;
    }

    /**
     * Update the settings used by breakpad
     *
     * @param breakpad the new breakpad settings
     */
    void setBreakpadSettings(const cb::breakpad::Settings& config) {
        breakpad = config;
        has.breakpad = true;
        notify_changed("breakpad");
    }

    /**
     * Update this settings object with the properties explicitly set in
     * the other object
     *
     * @param other the object to copy the settings from
     * @param apply apply the changes if there is no errors during validation
     * @throws std::exception if an error occurs (like trying to change
     *              a value which isn't dynamically updateable)
     */
    void updateSettings(const Settings& other, bool apply = true);

    /**
     * Note that it is not safe to add new listeners after we've spun up
     * new threads as we don't try to lock the object.
     *
     * the use case for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    void addChangeListener(
            const std::string& key,
            std::function<void(const std::string&, Settings&)> listener) {
        change_listeners[key].push_back(listener);
    }

    /**
     * May clients enable the XATTR feature?
     *
     * @return true if xattrs may be used
     */
    bool isXattrEnabled() const {
        return xattr_enabled.load();
    }

    /**
     * Set if the server should allow the use of xattrs
     *
     * @param enable true if the system may use xattrs
     */
    void setXattrEnabled(bool enable) {
        xattr_enabled.store(enable);
        has.xattr_enabled = true;
        notify_changed("xattr_enabled");
    }

    /// Set time until the first probe is sent
    void setTcpKeepAliveIdle(std::chrono::seconds val) {
        tcp_keepalive_idle.store(val, std::memory_order_release);
        has.tcp_keepalive_idle = true;
        notify_changed("tcp_keepalive_idle");
    }

    /// Get the time before the first probe is sent
    std::chrono::seconds getTcpKeepAliveIdle() const {
        return tcp_keepalive_idle.load(std::memory_order_acquire);
    }

    /// Set the interval between the probes is sent
    void setTcpKeepAliveInterval(std::chrono::seconds val) {
        tcp_keepalive_interval.store(val, std::memory_order_release);
        has.tcp_keepalive_interval = true;
        notify_changed("tcp_keepalive_interval");
    }

    /// Get the interval between the probes is sent
    std::chrono::seconds getTcpKeepAliveInterval() const {
        return tcp_keepalive_interval.load(std::memory_order_acquire);
    }

    /// Set the number of probes sent before the connection is marked as dead
    void setTcpKeepAliveProbes(uint32_t val) {
        tcp_keepalive_probes.store(val, std::memory_order_release);
        has.tcp_keepalive_probes = true;
        notify_changed("tcp_keepalive_probes");
    }

    /// Get the number of probes sent before the connection is marked as dead
    uint32_t getTcpKeepAliveProbes() const {
        return tcp_keepalive_probes.load(std::memory_order_acquire);
    }

    /// Set time to use for TCP_USER_TIMEOUT
    void setTcpUserTimeout(std::chrono::milliseconds val) {
        tcp_user_timeout.store(val, std::memory_order_release);
        has.tcp_user_timeout = true;
        notify_changed("tcp_user_timeout");
    }
    /// Get the time configured for TCP_USER_TIMEOUT
    std::chrono::milliseconds getTcpUserTimeout() const {
        return tcp_user_timeout.load(std::memory_order_acquire);
    }

    /**
     * Collections prototype means certain work-in-progress parts of collections
     * are enabled/disabled and also means DCP auto-enables collections for
     * replication streams (as opposed to ns_server requesting it).
     *
     * @return true if the collections prototype should be enabled
     */
    bool isCollectionsEnabled() const {
        return collections_enabled.load();
    }

    /**
     * Set if the server should enable collection support
     *
     * @param enable true if the system should enable collections
     */
    void setCollectionsEnabled(bool enable) {
        collections_enabled.store(enable);
        has.collections_enabled = true;
        notify_changed("collections_enabled");
    }

    std::string getOpcodeAttributesOverride() const {
        return std::string{*opcode_attributes_override.rlock()};
    }

    void setOpcodeAttributesOverride(const std::string& value);

    bool isTracingEnabled() const {
        return tracing_enabled.load(std::memory_order_acquire);
    }

    void setTracingEnabled(bool enabled) {
        Settings::tracing_enabled.store(enabled, std::memory_order_release);
        has.tracing_enabled = true;
        notify_changed("tracing_enabled");
    }

    void setScramshaFallbackSalt(const std::string& value) {
        scramsha_fallback_salt.wlock()->assign(value);
        has.scramsha_fallback_salt = true;
        notify_changed("scramsha_fallback_salt");
    }

    std::string getScramshaFallbackSalt() const {
        return std::string{*scramsha_fallback_salt.rlock()};
    }

    void setExternalAuthServiceEnabled(bool enable) {
        external_auth_service.store(enable, std::memory_order_release);
        has.external_auth_service = true;
        notify_changed("external_auth_service");
    }

    bool isExternalAuthServiceEnabled() const {
        return external_auth_service.load(std::memory_order_acquire);
    }

    std::chrono::microseconds getActiveExternalUsersPushInterval() const {
        return active_external_users_push_interval.load(
                std::memory_order_acquire);
    }

    void setActiveExternalUsersPushInterval(
            const std::chrono::microseconds interval) {
        active_external_users_push_interval.store(interval,
                                                  std::memory_order_release);
        has.active_external_users_push_interval = true;
        notify_changed("active_external_users_push_interval");
    }

    std::string getPortnumberFile() const {
        return portnumber_file;
    }

    void setPortnumberFile(std::string val) {
        portnumber_file = std::move(val);
        has.portnumber_file = true;
        notify_changed("portnumber_file");
    }

    int getParentIdentifier() const {
        return parent_identifier;
    }

    void setParentIdentifier(int val) {
        parent_identifier = val;
        has.parent_identifier = true;
        notify_changed("parent_identifier");
    }

    int getNumReaderThreads() const {
        return num_reader_threads.load(std::memory_order_acquire);
    }

    void setNumReaderThreads(int val) {
        num_reader_threads.store(val, std::memory_order_release);
        has.num_reader_threads = true;
        notify_changed("num_reader_threads");
    }

    int getNumWriterThreads() const {
        return num_writer_threads.load(std::memory_order_acquire);
    }

    void setNumWriterThreads(size_t val) {
        num_writer_threads.store(val, std::memory_order_release);
        has.num_writer_threads = true;
        notify_changed("num_writer_threads");
    }

    int getNumAuxIoThreads() const {
        return num_auxio_threads.load(std::memory_order_acquire);
    }

    void setNumAuxIoThreads(size_t val) {
        num_auxio_threads.store(val, std::memory_order_release);
        has.num_auxio_threads = true;
        notify_changed("num_auxio_threads");
    }

    int getNumNonIoThreads() const {
        return num_nonio_threads.load(std::memory_order_acquire);
    }

    void setNumNonIoThreads(size_t val) {
        num_nonio_threads.store(val, std::memory_order_release);
        has.num_nonio_threads = true;
        notify_changed("num_nonio_threads");
    }

    std::pair<in_port_t, sa_family_t> getPrometheusConfig() {
        return *prometheus_config.rlock();
    }

    void setPrometheusConfig(std::pair<in_port_t, sa_family_t> config) {
        *prometheus_config.wlock() = std::move(config);
        has.prometheus_config = true;
        notify_changed("prometheus_config");
    }

    int getNumStorageThreads() const {
        return num_storage_threads.load(std::memory_order_acquire);
    }

    void setNumStorageThreads(size_t val) {
        num_storage_threads.store(val, std::memory_order_release);
        has.num_storage_threads = true;
        notify_changed("num_storage_threads");
    }

    void setPhosphorConfig(std::string value) {
        *phosphor_config.wlock() = std::move(value);
        has.phosphor_config = true;
        notify_changed("phosphor_config");
    }

    std::string getPhosphorConfig() const {
        return std::string{*phosphor_config.rlock()};
    }

    bool isLocalhostInterfaceAllowed() const {
        return allow_localhost_interface.load(std::memory_order_acquire);
    }

    void setAllowLocalhostInterface(bool val) {
        allow_localhost_interface.store(val, std::memory_order_release);
        has.allow_localhost_interface = true;
        notify_changed("allow_localhost_interface");
    }

    DeploymentModel getDeploymentModel() const {
        return deployment_model;
    }

    bool isDeprecatedBucketAutoselectEnabled() {
        return enable_deprecated_bucket_autoselect.load(
                std::memory_order_acquire);
    }

    void setDeprecatedBucketAutoselectEnabled(bool val) {
        enable_deprecated_bucket_autoselect.store(val,
                                                  std::memory_order_release);
        has.enable_deprecated_bucket_autoselect = true;
        notify_changed("enable_deprecated_bucket_autoselect");
    }

    EventFramework getEventFramework() const {
        return event_framework.load(std::memory_order_acquire);
    }

    void setEventFramework(EventFramework val) {
        event_framework.store(val, std::memory_order_release);
        has.event_framework = true;
        notify_changed("event_framework");
    }

    std::size_t getMaxClientConnectionDetails() const {
        return max_client_connection_details.load(std::memory_order_acquire);
    }

    void setMaxClientConnectionDetails(size_t val) {
        max_client_connection_details.store(val, std::memory_order_release);
        has.max_client_connection_details = true;
        notify_changed("max_client_connection_details");
    }

    int getQuotaSharingPagerConcurrencyPercentage() const {
        return quota_sharing_pager_concurrency_percentage;
    }

    void setQuotaSharingPagerConcurrencyPercentage(int val) {
        quota_sharing_pager_concurrency_percentage = val;
        has.quota_sharing_pager_concurrency_percentage = true;
        notify_changed("quota_sharing_pager_concurrency_percentage");
    }

protected:
    /// The file containing audit configuration
    std::string audit_file;

    /// Location of error maps sent to the client
    std::string error_maps_dir;

    /// The file containing the RBAC user data
    std::string rbac_file;

    /// The root directory of the installation
    std::string root;

    /// Number of worker (without dispatcher) libevent threads to run
    size_t num_threads = 0;

    /// The name of the file to store portnumber information
    /// May also be set in environment (cannot change)
    std::string portnumber_file;

    /// The number of seconds a client may be idle before it is disconnected
    cb::RelaxedAtomic<size_t> connection_idle_time{0};

    /// The maximum amount of data we want to keep in the send buffer for
    /// for a client until we pause execution of new commands until the
    /// client drain the buffer. The motivation for the limit is to avoid
    /// having one client consume gigabytes of memory. By default the
    /// limit is set to 40MB (2x the max document size)
    std::atomic<size_t> max_send_queue_size{40 * 1024 * 1024};

    /// ssl client authentication
    cb::x509::ClientCertMapper client_cert_mapper;

    /// Configuration of the logger
    cb::logger::Config logger_settings;

    /// Breakpad crash catcher settings
    cb::breakpad::Settings breakpad;

    /// Array of interface settings we are listening on
    folly::Synchronized<std::vector<NetworkInterface>> interfaces;

    /// The available sasl mechanism list
    folly::Synchronized<std::string> sasl_mechanisms;

    /// The available sasl mechanism list to use over SSL
    folly::Synchronized<std::string> ssl_sasl_mechanisms;

    /// Any settings to override opcode attributes
    folly::Synchronized<std::string> opcode_attributes_override;

    /// The salt to return to users we don't know about
    folly::Synchronized<std::string> scramsha_fallback_salt;

    folly::Synchronized<std::pair<in_port_t, sa_family_t>> prometheus_config;

    folly::Synchronized<std::string> phosphor_config{
            "buffer-mode:ring;buffer-size:20971520;enabled-categories:*"};

    std::atomic<std::chrono::microseconds> active_external_users_push_interval{
            std::chrono::minutes(5)};

    /// The maximum number of connections allowed
    std::atomic<size_t> max_connections{60000};

    /// The pool of connections reserved for system usage
    std::atomic<size_t> system_connections{5000};

    /// A sloppy number of how many free connections we want to have.
    /// Once we accept a client and the use count is higher than the
    /// free connection pool number we'll try to disconnect clients.
    /// Note that this is a _sloppy_ number. We might end up disconnecting
    /// _MORE_ clients as we don't try to synchronize between threads
    std::atomic<size_t> free_connection_pool_size{0};

    std::atomic<ConnectionLimitMode> connection_limit_mode{
            ConnectionLimitMode::Disconnect};

    /// The maximum number of client ip addresses we should keep track of
    std::atomic<size_t> max_client_connection_details{0};

    /// The maximum number of commands each connection may have before
    /// blocking execution
    std::atomic<std::size_t> max_concurrent_commands_per_connection{32};

    /**
     * Note that it is not safe to add new listeners after we've spun up
     * new threads as we don't try to lock the object.
     *
     * the usecase for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    std::map<std::string,
             std::deque<std::function<void(const std::string&, Settings&)>>>
            change_listeners;

    /// The identifier to use for the parent monitor
    int parent_identifier = -1;

    /// level of versosity to log at.
    std::atomic_int verbose{0};

    /**
     * Maximum number of io events to process based on the priority of the
     * connection
     */
    int reqs_per_event_high_priority = 50;
    int reqs_per_event_med_priority = 5;
    int reqs_per_event_low_priority = 1;
    int default_reqs_per_event = 20;

    /// To prevent us from reading (and allocating) an insane amount of
    /// data off the network we'll ignore (and disconnect clients) that
    /// tries to send packets bigger than this max_packet_size. The current
    /// Max document size is 20MB so by using 30MB we'll get the correct
    /// E2BIG error message for connections going a bit bigger (and not
    /// a quiet disconnect)
    uint32_t max_packet_size{30 * 1024 * 1024};

    std::atomic<int> num_reader_threads{0};
    std::atomic<int> num_writer_threads{0};
    std::atomic<int> num_auxio_threads{0};
    std::atomic<int> num_nonio_threads{0};

    /// Number of storage backend threads
    std::atomic<int> num_storage_threads{0};

    /// The number of seconds before keepalive kicks in
    std::atomic<std::chrono::seconds> tcp_keepalive_idle{
            std::chrono::seconds{360}};
    /// The number of seconds between each probe sent in keepalive
    std::atomic<std::chrono::seconds> tcp_keepalive_interval{
            std::chrono::seconds{10}};
    /// The number of missing probes before the connection is considered dead
    std::atomic<uint32_t> tcp_keepalive_probes{3};

    /// The number of milliseconds for tcp user timeout (0 == disable)
    std::atomic<std::chrono::milliseconds> tcp_user_timeout{
            std::chrono::seconds{0}};

    /// Should the server always collect trace information for commands
    std::atomic_bool always_collect_trace_info{true};

    /// Is privilege debug enabled or not
    std::atomic_bool privilege_debug{false};

    /// is datatype json enabled?
    bool datatype_json = true;

    /// is datatype snappy enabled?
    std::atomic_bool datatype_snappy{true};

    /// Should we deduplicate the cluster maps from the Not My VBucket messages
    std::atomic_bool dedupe_nmvb_maps{false};

    /// May xattrs be used or not
    std::atomic_bool xattr_enabled{false};

    /// Should collections be enabled
    std::atomic_bool collections_enabled{true};

    /// Is tracing enabled or not
    std::atomic_bool tracing_enabled{true};

    /// Use standard input listener
    std::atomic_bool stdin_listener{true};

    /// Should we allow for using the external authentication service or not
    std::atomic_bool external_auth_service{false};

    /// If set "localhost" connections will not be deleted as part
    /// of server cleanup. This setting should only be used for unit
    /// tests
    std::atomic_bool allow_localhost_interface{true};

    void setDeploymentModel(DeploymentModel val) {
        deployment_model.store(val, std::memory_order_release);
        has.deployment_model = true;
    }
    std::atomic<DeploymentModel> deployment_model{DeploymentModel::Normal};

    /// The event framework to use
    std::atomic<EventFramework> event_framework{EventFramework::Bufferevent};

    std::atomic_bool enable_deprecated_bucket_autoselect{false};

    /// The number of concurrent paging visitors to use for quota sharing,
    /// expressed as a fraction of the number of NonIO threads.
    std::atomic_int quota_sharing_pager_concurrency_percentage{50};

    void notify_changed(const std::string& key);

public:
    /**
     * Flags for each of the above config options, indicating if they were
     * specified in a parsed config file. This is public because I haven't
     * had the time to update all of the unit tests to use the appropriate
     * getter/setter pattern
     */
    struct {
        bool always_collect_trace_info = false;
        bool rbac_file = false;
        bool privilege_debug = false;
        bool threads = false;
        bool interfaces = false;
        bool logger = false;
        bool audit = false;
        bool reqs_per_event_high_priority = false;
        bool reqs_per_event_med_priority = false;
        bool reqs_per_event_low_priority = false;
        bool default_reqs_per_event = false;
        bool deployment_model = false;
        bool enable_deprecated_bucket_autoselect = false;
        bool verbose = false;
        bool connection_idle_time = false;
        bool datatype_json = false;
        bool datatype_snappy = false;
        bool root = false;
        bool breakpad = false;
        bool max_packet_size = false;
        bool max_send_queue_size = false;
        bool client_cert_auth = false;
        bool sasl_mechanisms = false;
        bool ssl_sasl_mechanisms = false;
        bool dedupe_nmvb_maps = false;
        bool error_maps = false;
        bool xattr_enabled = false;
        bool collections_enabled = false;
        bool opcode_attributes_override = false;
        bool tracing_enabled = false;
        bool stdin_listener = false;
        bool scramsha_fallback_salt = false;
        bool external_auth_service = false;
        bool tcp_keepalive_idle = false;
        bool tcp_keepalive_interval = false;
        bool tcp_keepalive_probes = false;
        bool tcp_user_timeout = false;
        bool active_external_users_push_interval = false;
        bool max_connections = false;
        bool system_connections = false;
        bool free_connection_pool_size = false;
        bool connection_limit_mode = false;
        bool max_client_connection_details = false;
        bool max_concurrent_commands_per_connection = false;
        bool opentracing_config = false;
        bool num_reader_threads = false;
        bool num_writer_threads = false;
        bool num_auxio_threads = false;
        bool num_nonio_threads = false;
        bool num_storage_threads = false;
        bool portnumber_file = false;
        bool parent_identifier = false;
        bool prometheus_config = false;
        bool phosphor_config = false;
        bool allow_localhost_interface = false;
        bool event_framework = false;
        bool quota_sharing_pager_concurrency_percentage = false;
    } has;
};
