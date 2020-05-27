/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include "client_cert_config.h"
#include "logger/logger_config.h"
#include "network_interface.h"
#include "opentracing_config.h"

#include <memcached/engine.h>
#include <platform/dynamic.h>
#include <relaxed_atomic.h>
#include <utilities/breakpad_settings.h>

#include <folly/Synchronized.h>

#include <atomic>
#include <cstdarg>
#include <deque>
#include <map>
#include <string>
#include <vector>

enum class EventPriority {
    High,
    Medium,
    Low,
    Default
};

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
     * Load the error map from the given directory
     * @param path Path to load from.
     * @throw exception if the file does not exist or cannot be read/parsed
     */
    void loadErrorMaps(const std::string& path);

    /**
     * Get the error map of the requested version
     * @param version the maximum version of the error map. The returned map
     *        may be a lower version, but never a higher one.
     * @return The text of the error map, or the empty string if no error maps
     *         are loaded.
     */
    const std::string& getErrorMap(size_t version) const;

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

    /**
     * Get the list of SSL ciphers to use for TLS < 1.3
     *
     * @return the list of available SSL ciphers to use
     */
    std::string getSslCipherList() const {
        return *ssl_cipher_list.rlock();
    }

    /**
     * Set the list of SSL ciphers the node may use for TLS < 1.3
     *
     * @param ssl_cipher_list the new list of SSL ciphers
     */
    void setSslCipherList(std::string list);

    /**
     * Get the list of SSL ciphers suites to use for TLS > 1.2
     *
     * @return the list of available SSL ciphers to use
     */
    std::string getSslCipherSuites() const {
        return *ssl_cipher_suites.rlock();
    }

    /**
     * Set the list of SSL ciphers the node may use for TLS > 1.2
     *
     * @param ssl_cipher_list the new list of SSL ciphers
     */
    void setSslCipherSuites(std::string suites);

    bool isSslCipherOrder() const {
        return ssl_cipher_order.load(std::memory_order_acquire);
    }

    void setSslCipherOrder(bool ordered);

    /// get the configured SSL protocol mask
    long getSslProtocolMask()const {
        return ssl_protocol_mask.load();
    }

    /**
     * Get the minimum SSL protocol the node use
     *
     * @return the minimum ssl protocol
     */
    const std::string& getSslMinimumProtocol() const {
        return ssl_minimum_protocol;
    }

    /**
     * Set the minimum SSL Protocol the node accepts
     *
     * @param ssl_minimum_protocol the new minimum SSL protocol
     */
    void setSslMinimumProtocol(std::string protocol);

    void reconfigureClientCertAuth(
            std::unique_ptr<cb::x509::ClientCertConfig>& config) {
        client_cert_mapper.reconfigure(config);
        has.client_cert_auth = true;
        notify_changed("client_cert_auth");
    }

    /**
     * Get the ssl client auth
     *
     * @return the value of the ssl client auth
     */
    cb::x509::Mode getClientCertMode() const {
        return client_cert_mapper.getMode();
    }

    std::pair<cb::x509::Status, std::string> lookupUser(X509* cert) const {
        return client_cert_mapper.lookupUser(cert);
    }

    /**
     * Get the number of topkeys to track
     *
     * @return the number of keys to track
     */
    int getTopkeysSize() const {
        return topkeys_size;
    }

    /**
     * Set the number of keys to track
     *
     * @param size the new limit
     */
    void setTopkeysSize(int size) {
        topkeys_size = size;
        has.topkeys_size = true;
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
     * the usecase for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    void addChangeListener(const std::string& key,
                           void (* listener)(const std::string& key,
                                             Settings& obj)) {
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
    void setCollectionsPrototype(bool enable) {
        collections_enabled.store(enable);
        has.collections_enabled = true;
        notify_changed("collections_enabled");
    }

    std::string getOpcodeAttributesOverride() const {
        return std::string{*opcode_attributes_override.rlock()};
    }

    void setOpcodeAttributesOverride(const std::string& value);

    bool isTopkeysEnabled() const {
        return topkeys_enabled.load(std::memory_order_acquire);
    }

    void setTopkeysEnabled(bool enabled) {
        Settings::topkeys_enabled.store(enabled, std::memory_order_release);
        has.topkeys_enabled = true;
        notify_changed("topkeys_enabled");
    }

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

    /**
     * Get the (optional) OpenTracing configuration.
     *
     * Given that this setting contains multiple settings which is closely
     * related it is put in a struct and we'll use a shared_ptr to allow
     * replacing all of them at the same time (without having to deal with
     * locking if we read one or change another etc)
     */
    std::shared_ptr<OpenTracingConfig> getOpenTracingConfig() const {
        return opentracing_config;
    }

    /**
     * Replace the OpenTracing configuration with the new setting and fire
     * the listener. Shared_ptr is being used to avoid having to deal with
     * locking of the individual members.
     */
    void setOpenTracingConfig(std::shared_ptr<OpenTracingConfig> config) {
        opentracing_config = config;
        has.opentracing_config = true;
        notify_changed("opentracing_config");
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

    std::pair<in_port_t, sa_family_t> getPrometheusConfig() {
        return *prometheus_config.rlock();
    }

    void setPrometheusConfig(std::pair<in_port_t, sa_family_t> config) {
        *prometheus_config.wlock() = std::move(config);
        has.prometheus_config = true;
        notify_changed("prometheus_config");
    }

protected:
    /// Should the server always collect trace information for commands
    std::atomic_bool always_collect_trace_info{false};

    /**
     * The file containing the RBAC user data
     */
    std::string rbac_file;

    /**
     * Is privilege debug enabled or not
     */
    std::atomic_bool privilege_debug{false};

    /**
     * Number of worker (without dispatcher) libevent threads to run
     * */
    size_t num_threads = 0;

    /// Array of interface settings we are listening on
    folly::Synchronized<std::vector<NetworkInterface>> interfaces;

    /**
     * Configuration of the logger
     */
    cb::logger::Config logger_settings;

    /**
     * The file containing audit configuration
     */
    std::string audit_file;

    /**
     * Location of error maps sent to the client
     */
    std::string error_maps_dir;

    /**
     * level of versosity to log at.
     */
    std::atomic_int verbose{0};

    /**
     * The number of seconds a client may be idle before it is disconnected
     */
    cb::RelaxedAtomic<size_t> connection_idle_time{0};

    /**
     * The root directory of the installation
     */
    std::string root;

    /**
     * is datatype json/snappy enabled?
     */
    bool datatype_json = false;
    std::atomic_bool datatype_snappy{false};

    /**
     * Maximum number of io events to process based on the priority of the
     * connection
     */
    int reqs_per_event_high_priority = 0;
    int reqs_per_event_med_priority = 0;
    int reqs_per_event_low_priority = 0;
    int default_reqs_per_event = 0;

    /**
     * Breakpad crash catcher settings
     */
    cb::breakpad::Settings breakpad;

    /**
     * To prevent us from reading (and allocating) an insane amount of
     * data off the network we'll ignore (and disconnect clients) that
     * tries to send packets bigger than this max_packet_size. See
     * the man page for more information.
     */
    uint32_t max_packet_size = 0;

    /// The maximum amount of data we want to keep in the send buffer for
    /// for a client until we pause execution of new commands until the
    /// client drain the buffer. The motivation for the limit is to avoid
    /// having one client consume gigabytes of memory. By default the
    /// limit is set to 40MB (2x the max document size)
    std::atomic<size_t> max_send_queue_size{40 * 1024 * 1024};

    /// The SSL cipher list to use for TLS < 1.3
    folly::Synchronized<std::string> ssl_cipher_list;

    /// The SSL cipher suites to use for TLS > 1.3
    folly::Synchronized<std::string> ssl_cipher_suites;

    /// if we should use the ssl cipher ordering
    std::atomic_bool ssl_cipher_order{true};

    /**
     * The minimum ssl protocol to use (by default this is TLS1)
     */
    std::string ssl_minimum_protocol;
    std::atomic_long ssl_protocol_mask{0};

    /**
     * ssl client authentication
     */
    cb::x509::ClientCertMapper client_cert_mapper;

    /**
     * The number of topkeys to track
     */
    int topkeys_size = 0;

    /// The available sasl mechanism list
    folly::Synchronized<std::string> sasl_mechanisms;

    /// The available sasl mechanism list to use over SSL
    folly::Synchronized<std::string> ssl_sasl_mechanisms;

    /**
     * Should we deduplicate the cluster maps from the Not My VBucket messages
     */
    std::atomic_bool dedupe_nmvb_maps{false};

    /**
     * Map of version -> string for error maps
     */
    std::vector<std::string> error_maps;

    /**
     * May xattrs be used or not
     */
    std::atomic_bool xattr_enabled{false};

    /**
     * Should collections be enabled
     */
    std::atomic_bool collections_enabled{true};

    /// Any settings to override opcode attributes
    folly::Synchronized<std::string> opcode_attributes_override;

    /**
     * Is topkeys enabled or not
     */
    std::atomic_bool topkeys_enabled{false};

    /**
     * Is tracing enabled or not
     */
    std::atomic_bool tracing_enabled{true};

    /**
     * Use standard input listener
     */
    std::atomic_bool stdin_listener{true};

    /**
     * Should we allow for using the external authentication service or not
     */
    std::atomic_bool external_auth_service{false};

    std::atomic<std::chrono::microseconds> active_external_users_push_interval{
            std::chrono::minutes(5)};

    /// The maximum number of connections allowed
    std::atomic<size_t> max_connections{60000};

    /// The pool of connections reserved for system usage
    std::atomic<size_t> system_connections{5000};

    /// The configuration used by OpenTracing
    std::shared_ptr<OpenTracingConfig> opentracing_config;

    /// The maximum number of commands each connection may have before
    /// blocking execution
    std::atomic<std::size_t> max_concurrent_commands_per_connection{32};

    /// The name of the file to store portnumber information
    /// May also be set in environment (cannot change)
    std::string portnumber_file;
    /// The identifier to use for the parent monitor
    int parent_identifier = -1;
    /// The salt to return to users we don't know about
    folly::Synchronized<std::string> scramsha_fallback_salt;

    /**
     * Note that it is not safe to add new listeners after we've spun up
     * new threads as we don't try to lock the object.
     *
     * the usecase for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    std::map<std::string,
             std::deque<void (*)(const std::string& key, Settings& obj)>>
            change_listeners;

    void notify_changed(const std::string& key);

    std::atomic<int> num_reader_threads{0};
    std::atomic<int> num_writer_threads{0};

    folly::Synchronized<std::pair<in_port_t, sa_family_t>> prometheus_config;

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
        bool verbose = false;
        bool connection_idle_time = false;
        bool datatype_json = false;
        bool datatype_snappy = false;
        bool root = false;
        bool breakpad = false;
        bool max_packet_size = false;
        bool max_send_queue_size = false;
        bool ssl_cipher_list = false;
        bool ssl_cipher_order = false;
        bool ssl_cipher_suites = false;
        bool ssl_minimum_protocol = false;
        bool client_cert_auth = false;
        bool topkeys_size = false;
        bool sasl_mechanisms = false;
        bool ssl_sasl_mechanisms = false;
        bool dedupe_nmvb_maps = false;
        bool error_maps = false;
        bool xattr_enabled = false;
        bool collections_enabled = false;
        bool opcode_attributes_override = false;
        bool topkeys_enabled = false;
        bool tracing_enabled = false;
        bool stdin_listener = false;
        bool scramsha_fallback_salt = false;
        bool external_auth_service = false;
        bool active_external_users_push_interval = false;
        bool max_connections = false;
        bool system_connections = false;
        bool max_concurrent_commands_per_connection = false;
        bool opentracing_config = false;
        bool num_reader_threads = false;
        bool num_writer_threads = false;
        bool portnumber_file = false;
        bool parent_identifier = false;
        bool prometheus_config = false;
    } has;
};
