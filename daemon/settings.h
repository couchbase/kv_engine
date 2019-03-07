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

#include "config.h"

#include "client_cert_config.h"
#include "logger/logger_config.h"
#include "network_interface.h"
#include <utilities/breakpad_settings.h>

#include <memcached/engine.h>
#include <platform/dynamic.h>
#include <relaxed_atomic.h>
#include <atomic>
#include <cstdarg>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <vector>

enum class EventPriority {
    High,
    Medium,
    Low,
    Default
};

/* When adding a setting, be sure to update process_stat_settings */
/**
 * Globally accessible settings as derived from the commandline / JSON config
 * file.
 */
class Settings {
public:
    Settings();
    /**
     * Create and initialize a settings object from the given JSON
     *
     * @param json
     * @throws nlohmann::json::exception for parsing errors
     * @throws std::invalid_argument for JSON structure errors
     */
    explicit Settings(const nlohmann::json& json);

    Settings(const Settings&) = delete;


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
     * @param rbac_file the absolute path to the rbac file
     */
    void setRbacFile(const std::string& rbac_file) {
        has.rbac_file = true;
        Settings::rbac_file = rbac_file;
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
     * @param num_threads the new number of threads
     */
    void setNumWorkerThreads(size_t num_threads) {
        has.threads = true;
        Settings::num_threads = num_threads;
        notify_changed("threads");
    }

    /**
     * Add a new interface definition to the list of interfaces provided
     * by the server.
     *
     * @param ifc the interface to add
     */
    void addInterface(const NetworkInterface& ifc) {
        interfaces.push_back(ifc);
        has.interfaces = true;
        notify_changed("interfaces");
    }

    /**
     * Get vector containing all of the interfaces the system should
     * listen on
     *
     * @return the vector of interfaces
     */
    const std::vector<NetworkInterface>& getInterfaces() const {
        return interfaces;
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
     * @param audit_file the new name of the file containing audit configuration
     */
    void setAuditFile(const std::string& audit_file) {
        has.audit = true;
        Settings::audit_file = audit_file;
        notify_changed("audit_file");
    }

    const std::string& getErrorMapsDir() const {
        return Settings::error_maps_dir;
    }

    void setErrorMapsDir(const std::string& dir) {
        has.error_maps = true;
        Settings::error_maps_dir = dir;
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
    const size_t getConnectionIdleTime() const {
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
    void setRoot(const std::string& root) {
        Settings::root = root;
        has.root = true;
        notify_changed("root");
    }

    size_t getMaxConnections() const {
        return max_connections.load(std::memory_order_consume);
    }

    void setMaxConnections(size_t max_connections, bool notify = true) {
        Settings::max_connections.store(max_connections,
                                        std::memory_order_release);
        has.max_connections = true;
        if (notify) {
            notify_changed("max_connections");
        }
    }

    size_t getSystemConnections() const {
        return system_connections.load(std::memory_order_consume);
    }

    void setSystemConnections(size_t system_connections) {
        Settings::system_connections.store(system_connections,
                                           std::memory_order_release);
        has.system_connections = true;
        notify_changed("system_connections");
    }

    size_t getMaxUserConnections() const {
        return getMaxConnections() - getSystemConnections();
    }

    /**
     * Set the number of request to handle per notification from the
     * event library
     *
     * @param num the number of requests to handle
     * @param priority The priority to update
     */
    void setRequestsPerEventNotification(int num,
                                         const EventPriority& priority) {
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
    int getRequestsPerEventNotification(const EventPriority& priority) const {
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
     * Get the size of the OpenSSL BIO buffers
     *
     * @return the size (in bytes) of the OpenSSL BIOs
     */
    unsigned int getBioDrainBufferSize() const {
        return bio_drain_buffer_sz;
    }

    /**
     * Set the size of the OpenSSL BIO buffers
     *
     * @param bio_drain_buffer_sz the new size in bytes
     */
    void setBioDrainBufferSize(unsigned int bio_drain_buffer_sz) {
        Settings::bio_drain_buffer_sz = bio_drain_buffer_sz;
        has.bio_drain_buffer_sz = true;
        notify_changed("bio_drain_buffer_sz");
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
     * @param max_packet_size the new maximum size in bytes
     */
    void setMaxPacketSize(uint32_t max_packet_size) {
        Settings::max_packet_size = max_packet_size;
        has.max_packet_size = true;
        notify_changed("max_packet_size");
    }

    /**
     * Get the list of SSL ciphers to use
     *
     * @return the list of available SSL ciphers to use
     */
    const std::string& getSslCipherList() const {
        return ssl_cipher_list;
    }

    /**
     * Set the list of SSL ciphers the node may use
     *
     * @param ssl_cipher_list the new list of SSL ciphers
     */
    void setSslCipherList(const std::string& ssl_cipher_list) {
        Settings::ssl_cipher_list = ssl_cipher_list;
        has.ssl_cipher_list = true;
        notify_changed("ssl_cipher_list");
    }

    bool isSslCipherOrder() const {
        return ssl_cipher_order.load(std::memory_order_acquire);
    }

    void setSslCipherOrder(bool ssl_cipher_order) {
        Settings::ssl_cipher_order.store(ssl_cipher_order,
                                         std::memory_order_release);
        has.ssl_cipher_order = true;
        notify_changed("ssl_cipher_order");
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
    void setSslMinimumProtocol(const std::string& ssl_minimum_protocol) {
        Settings::ssl_minimum_protocol = ssl_minimum_protocol;
        has.ssl_minimum_protocol = true;
        notify_changed("ssl_minimum_protocol");
    }

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
    const cb::x509::Mode getClientCertMode() {
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
     * @param topkeys_size the new limit
     */
    void setTopkeysSize(int topkeys_size) {
        Settings::topkeys_size = topkeys_size;
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
    const bool isDedupeNmvbMaps() const {
        return dedupe_nmvb_maps.load();
    }

    /**
     * Set if the server should return the cluster map it has already sent a
     * client as part of the payload of <em>not my vbucket</em> errors.
     *
     * @param dedupe_nmvb_maps true if the node should deduplicate such maps
     */
    void setDedupeNmvbMaps(const bool& dedupe_nmvb_maps) {
        Settings::dedupe_nmvb_maps.store(dedupe_nmvb_maps);
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
    void setBreakpadSettings(const cb::breakpad::Settings& breakpad) {
        Settings::breakpad = breakpad;
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
    const bool isXattrEnabled() const {
        return xattr_enabled.load();
    }

    /**
     * Set if the server should allow the use of xattrs
     *
     * @param xattr_enabled true if the system may use xattrs
     */
    void setXattrEnabled(const bool xattr_enabled) {
        Settings::xattr_enabled.store(xattr_enabled);
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
     * @param collections_enabled true if the system should enable collections
     */
    void setCollectionsPrototype(const bool collections_enabled) {
        Settings::collections_enabled.store(collections_enabled);
        has.collections_enabled = collections_enabled;
        notify_changed("collections_enabled");
    }

    const std::string getOpcodeAttributesOverride() const {
        std::lock_guard<std::mutex> guard(
                Settings::opcode_attributes_override.mutex);
        return std::string{opcode_attributes_override.value};
    }

    void setOpcodeAttributesOverride(
            const std::string& opcode_attributes_override);

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

    void setScramshaFallbackSalt(std::string value) {
        {
            std::lock_guard<std::mutex> guard(scramsha_fallback_salt.mutex);
            has.scramsha_fallback_salt = true;
            scramsha_fallback_salt.value = std::move(value);
        }
        notify_changed("scramsha_fallback_salt");
    }

    std::string getScramshaFallbackSalt() const {
        std::lock_guard<std::mutex> guard(scramsha_fallback_salt.mutex);
        return std::string{scramsha_fallback_salt.value};
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

protected:

    /**
     * The file containing the RBAC user data
     */
    std::string rbac_file;

    /**
     * Is privilege debug enabled or not
     */
    std::atomic_bool privilege_debug;

    /**
     * Number of worker (without dispatcher) libevent threads to run
     * */
    size_t num_threads;

    /**
     * Array of interface settings we are listening on
     */
    std::vector<NetworkInterface> interfaces;

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
    std::atomic_int verbose;

    /**
     * The number of seconds a client may be idle before it is disconnected
     */
    cb::RelaxedAtomic<size_t> connection_idle_time;

    /**
     * The root directory of the installation
     */
    std::string root;

    /**
     * size of the SSL bio buffers
     */
    unsigned int bio_drain_buffer_sz;

    /**
     * is datatype json/snappy enabled?
     */
    bool datatype_json;
    std::atomic_bool datatype_snappy;

    /**
     * Maximum number of io events to process based on the priority of the
     * connection
     */
    int reqs_per_event_high_priority;
    int reqs_per_event_med_priority;
    int reqs_per_event_low_priority;
    int default_reqs_per_event;

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
    uint32_t max_packet_size;

    /**
     * The SSL cipher list to use
     */
    std::string ssl_cipher_list;

    /// if we should use the ssl cipher ordering
    std::atomic_bool ssl_cipher_order{true};

    /**
     * The minimum ssl protocol to use (by default this is TLS1)
     */
    std::string ssl_minimum_protocol;

    /**
     * ssl client authentication
     */
    cb::x509::ClientCertMapper client_cert_mapper;

    /**
     * The number of topkeys to track
     */
    int topkeys_size;

    /**
     * The available sasl mechanism list
     */
    struct {
        mutable std::mutex mutex;
        std::string value;
    } sasl_mechanisms;

    /**
     * The available sasl mechanism list to use over SSL
     */
    struct {
        mutable std::mutex mutex;
        std::string value;
    } ssl_sasl_mechanisms;

    /**
     * Should we deduplicate the cluster maps from the Not My VBucket messages
     */
    std::atomic_bool dedupe_nmvb_maps;

    /**
     * Map of version -> string for error maps
     */
    std::vector<std::string> error_maps;

    /**
     * May xattrs be used or not
     */
    std::atomic_bool xattr_enabled;

    /**
     * Should collections be enabled (off by default as it's a work in progress)
     */
    std::atomic_bool collections_enabled;

    /**
     * Any settings to override opcode attributes
     */
    struct {
        mutable std::mutex mutex;
        std::string value;
    } opcode_attributes_override;

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
    std::atomic_bool external_auth_service;

    std::atomic<std::chrono::microseconds> active_external_users_push_interval{
            std::chrono::minutes(5)};

    /// The maximum number of connections allowed
    std::atomic<size_t> max_connections{60000};

    /// The pool of connections reserved for system usage
    std::atomic<size_t> system_connections{5000};

public:
    /**
     * Flags for each of the above config options, indicating if they were
     * specified in a parsed config file. This is public because I haven't
     * had the time to update all of the unit tests to use the appropriate
     * getter/setter pattern
     */
    struct {
        bool rbac_file;
        bool privilege_debug;
        bool threads;
        bool interfaces;
        bool logger;
        bool audit;
        bool reqs_per_event_high_priority;
        bool reqs_per_event_med_priority;
        bool reqs_per_event_low_priority;
        bool default_reqs_per_event;
        bool verbose;
        bool connection_idle_time;
        bool bio_drain_buffer_sz;
        bool datatype_json;
        bool datatype_snappy;
        bool root;
        bool breakpad;
        bool max_packet_size;
        bool ssl_cipher_list;
        bool ssl_cipher_order;
        bool ssl_minimum_protocol;
        bool client_cert_auth;
        bool topkeys_size;
        bool sasl_mechanisms;
        bool ssl_sasl_mechanisms;
        bool dedupe_nmvb_maps;
        bool error_maps;
        bool xattr_enabled;
        bool collections_enabled;
        bool opcode_attributes_override;
        bool topkeys_enabled;
        bool tracing_enabled;
        bool stdin_listener;
        bool scramsha_fallback_salt;
        bool external_auth_service;
        bool active_external_users_push_interval = false;
        bool max_connections = false;
        bool system_connections = false;
    } has;

protected:

    struct {
        mutable std::mutex mutex;
        std::string value;
    } scramsha_fallback_salt;

    /**
     * Note that it is not safe to add new listeners after we've spun up
     * new threads as we don't try to lock the object.
     *
     * the usecase for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    std::map<std::string, std::deque<void (*)(const std::string& key, Settings& obj)> > change_listeners;

    void notify_changed(const std::string& key);
};

extern Settings settings;
