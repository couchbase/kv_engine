/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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

#include <atomic>
#include <cJSON_utils.h>
#include <cstdarg>
#include <deque>
#include <map>
#include <memcached/engine.h>
#include <platform/dynamic.h>
#include <relaxed_atomic.h>
#include <string>
#include <vector>

/**
 * An enumeration with constants for the various protocols supported
 * on the server.
 */
enum class Protocol : uint8_t {
    /** The memcached binary protocol used in Couchbase < 4.0 */
    Memcached,
    /** The (new and upcomming) Greenstack protocol */
    Greenstack
};

const char* to_string(const Protocol& protocol);


struct interface {
    interface()
        : maxconn(1000),
          backlog(1024),
          port(11211),
          ipv6(true),
          ipv4(true),
          tcp_nodelay(true),
          management(false),
          protocol(Protocol::Memcached) {
    }

    interface(const cJSON* json);

    std::string host;
    struct {
        std::string key;
        std::string cert;
    } ssl;
    int maxconn;
    int backlog;
    in_port_t port;
    bool ipv6;
    bool ipv4;
    bool tcp_nodelay;
    bool management;
    Protocol protocol;
};

/* pair of shared object name and config for an extension to be loaded. */
struct extension_settings {
    extension_settings() {}
    extension_settings(const cJSON* json) {
        cJSON* obj = cJSON_GetObjectItem(const_cast<cJSON*>(json), "module");
        if (obj == nullptr) {
            throw std::invalid_argument("extension_settings: mandatory element module not found");
        }
        if (obj->type != cJSON_String) {
            throw std::invalid_argument("extension_settings: \"module\" must be a string");
        }
        soname.assign(obj->valuestring);

        obj = cJSON_GetObjectItem(const_cast<cJSON*>(json), "config");
        if (obj != nullptr) {
            if (obj->type != cJSON_String) {
                throw std::invalid_argument("extension_settings: \"config\" must be a string");
            }
            config.assign(obj->valuestring);
        }
    }

    std::string soname;
    std::string config;
};

/**
 * What information should breakpad minidumps contain?
 */
enum class BreakpadContent {
    /**
     * Default content (threads+stack+env+arguments)
     */
    Default
};

/**
 * Settings for Breakpad crash catcher.
 */
class BreakpadSettings {
public:
    /**
     * Default constructor initialize the object to be in a disabled state
     */
    BreakpadSettings()
        : enabled(false),
          content(BreakpadContent::Default) {
    }

    /**
     * Initialize the Breakpad object from the specified JSON structure
     * which looks like:
     *
     *     {
     *         "enabled" : true,
     *         "minidump_dir" : "/var/crash",
     *         "content" : "default"
     *     }
     *
     * @param json The json to parse
     * @throws std::invalid_argument if the json dosn't look as expected
     */
    BreakpadSettings(const cJSON* json);

    bool isEnabled() const {
        return enabled;
    }

    void setEnabled(bool enabled) {
        BreakpadSettings::enabled = enabled;
    }

    const std::string& getMinidumpDir() const {
        return minidump_dir;
    }

    void setMinidumpDir(const std::string& minidump_dir) {
        BreakpadSettings::minidump_dir = minidump_dir;
    }

    BreakpadContent getContent() const {
        return content;
    }

    void setContent(BreakpadContent content) {
        BreakpadSettings::content = content;
    }

protected:
    bool enabled;
    std::string minidump_dir;
    BreakpadContent content;
};

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
     * @throws std::invalid_argument if for syntax errors
     */
    Settings(const unique_cJSON_ptr& json);

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
    void reconfigure(const unique_cJSON_ptr& json);

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
    int getNumWorkerThreads() const {
        return num_threads;
    }

    /**
     * Set the number of frontend worker threads
     *
     * @param num_threads the new number of threads
     */
    void setNumWorkerThreads(int num_threads) {
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
    void addInterface(const struct interface& ifc) {
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
    const std::vector<struct interface>& getInterfaces() const {
        return interfaces;
    }

    /**
     * Add a new extension to be loaded to the internal list of extensions
     *
     * @param ext the extension to add
     */
    void addPendingExtension(const struct extension_settings& ext) {
        pending_extensions.push_back(ext);
        has.extensions = true;
        notify_changed("extensions");
    }

    /**
     * Get the list of extensions to load
     *
     * @return the vector containing all of the modules to load
     */
    const std::vector<extension_settings>& getPendingExtensions() const {
        return pending_extensions;
    }

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

public:
    /**
     * Get the error map of the requested version
     * @param version the maximum version of the error map. The returned map
     *        may be a lower version, but never a higher one.
     * @return The text of the error map, or the empty string if no error maps
     *         are loaded.
     */
    const std::string& getErrorMap(size_t version) const;

    /**
     * Is SASL required or not
     *
     * When SASL is required all connections _MUST_ perform SASL authentication.
     *
     * @return true if the server is configured to always use SASL auth
     */
    bool isRequireSasl() const {
        return require_sasl;
    }

    /**
     * Set if SASL is required or not
     *
     * When SASL is required all connections _MUST_ perform SASL authentication.
     *
     * @param require_sasl true if the server should refuse to execute commands
     *                     unless the connection performs a successful AUTH
     */
    void setRequireSasl(bool require_sasl) {
        Settings::require_sasl = require_sasl;
        has.require_sasl = true;
        notify_changed("require_sasl");
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

    /**
     * Get the aggregated number of max connections allowed
     *
     * @return the sum of maxconns specified for all interfaces
     */
    int getMaxconns() const {
        return maxconns;
    }

    /**
     * Calculate the aggregated count of all connections
     */
    void calculateMaxconns() {
        maxconns = 0;
        for (auto& ifc : interfaces) {
            maxconns += ifc.maxconn;
        }
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
     * Is datatype supported or not
     *
     * @return true if clients may use datatype support
     */
    bool isDatatypeSupport() const {
        return datatype;
    }

    /**
     * Set if datatype support should be enabled or not
     *
     * @param datatype true if clients should be able to use datatype support
     */
    void setDatatypeSupport(bool datatype) {
        Settings::datatype = datatype;
        has.datatype = true;
        notify_changed("datatype_support");
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
     * Should the server wait for an init message before opening up all
     * non-management tagged interfaces (and disconnect all "non-admin"
     * clients
     *
     * @return true if the system should wait for init complete message
     */
    bool isRequireInit() const {
        return require_init;
    }

    /**
     * Set the mode if the server should allow all clients immediately or
     * wait for ns_server to notify the note that it is done initializing
     * the buckets etc.
     *
     * @param require_init true if the node require an init complete message
     */
    void setRequireInit(bool require_init) {
        Settings::require_init = require_init;
        has.require_init = true;
        notify_changed("require_init");
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
     * Should the server listen on stdin for commands or not
     * (This is used for unit testing)
     *
     * @return true if the node should listen on clients on stdin
     */
    bool isStdinListen() const {
        return stdin_listen;
    }

    /**
     * Set if the node should listen for commands on stdin or not
     *
     * @param stdin_listen true if commands should be accepted on stdin
     */
    void setStdinListen(bool stdin_listen) {
        Settings::stdin_listen = stdin_listen;
        has.stdin_listen = true;
        notify_changed("stdin_listen");
    }

    /**
     * Should the process exit when the connection close
     * (This is used for testing)
     *
     * @return true if the process should exit on connection close
     */
    bool isExitOnConnectionClose() const {
        return exit_on_connection_close;
    }

    /**
     * Set if the process should exit on connection close or not
     * (This is used for testing)
     *
     * @param exit_on_connection_close true if the process should exit on
     *                                 connection close
     */
    void setExitOnConnectionClose(bool exit_on_connection_close) {
        Settings::exit_on_connection_close = exit_on_connection_close;
        has.exit_on_connection_close = true;
        notify_changed("exit_on_connection_close");
    }

    /**
     * Get the list of available SASL Mechanisms
     *
     * @return all SASL mechanisms the client may use
     */
    const std::string& getSaslMechanisms() const {
        return sasl_mechanisms;
    }

    /**
     * Set the list of available SASL Mechanisms
     *
     * @param sasl_mechanisms the new list of sasl mechanisms
     */
    void setSaslMechanisms(const std::string& sasl_mechanisms) {
        Settings::sasl_mechanisms = sasl_mechanisms;
        has.sasl_mechanisms = true;
        notify_changed("sasl_mechanisms");
    }

    /**
     * Get the list of available SASL Mechanisms to use for SSL
     *
     * @return all SASL mechanisms the client may use
     */
    const std::string& getSslSaslMechanisms() const {
        return ssl_sasl_mechanisms;
    }

    /**
     * Set the list of available SASL Mechanisms to use for SSL connections
     *
     * @param sasl_mechanisms the new list of sasl mechanisms
     */
    void setSslSaslMechanisms(const std::string& ssl_sasl_mechanisms) {
        Settings::ssl_sasl_mechanisms = ssl_sasl_mechanisms;
        has.ssl_sasl_mechanisms = true;
        notify_changed("ssl_sasl_mechanisms");
    }

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
    const BreakpadSettings& getBreakpadSettings() const {
        return breakpad;
    }

    /**
     * Update the settings used by breakpad
     *
     * @param breakpad the new breakpad settings
     */
    void setBreakpadSettings(const BreakpadSettings& breakpad) {
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

    static void logit(EXTENSION_LOG_LEVEL level, const char *fmt, ...);

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
    int num_threads;

    /**
     * Array of interface settings we are listening on
     */
    std::vector<struct interface> interfaces;

    /**
     * Array of extensions and their settings to be loaded
     */
    std::vector<struct extension_settings> pending_extensions;

    /**
     * The file containing audit configuration
     */
    std::string audit_file;

    /**
     * Location of error maps sent to the client
     */
    std::string error_maps_dir;

    /**
     * require SASL auth
     */
    bool require_sasl;

    /**
     * level of versosity to log at.
     */
    std::atomic_int verbose;

    /**
     * The number of seconds a client may be idle before it is disconnected
     */
    Couchbase::RelaxedAtomic<size_t> connection_idle_time;

    /**
     * The root directory of the installation
     */
    std::string root;

    /**
     * size of the SSL bio buffers
     */
    unsigned int bio_drain_buffer_sz;

    /**
     * is datatype support enabled?
     */
    bool datatype;

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
    BreakpadSettings breakpad;

    /**
     * To prevent us from reading (and allocating) an insane amount of
     * data off the network we'll ignore (and disconnect clients) that
     * tries to send packets bigger than this max_packet_size. See
     * the man page for more information.
     */
    uint32_t max_packet_size;

    /**
     * Require init message from ns_server
     */
    bool require_init;

    /**
     * The SSL cipher list to use
     */
    std::string ssl_cipher_list;

    /**
     * The minimum ssl protocol to use (by default this is TLS1)
     */
    std::string ssl_minimum_protocol;

    /**
     * The number of topkeys to track
     */
    int topkeys_size;

    /**
     * Listen on stdin (reply to stdout)
     */
    bool stdin_listen;

    /**
     * When *any* connection closes, terminate the process.
     * Intended for afl-fuzz runs.
     */
    bool exit_on_connection_close;

    /**
     * The available sasl mechanism list
     */
    std::string sasl_mechanisms;

    /**
     * The available sasl mechanism list to use over SSL
     */
    std::string ssl_sasl_mechanisms;

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
        bool extensions;
        bool audit;
        bool require_sasl;
        bool reqs_per_event_high_priority;
        bool reqs_per_event_med_priority;
        bool reqs_per_event_low_priority;
        bool default_reqs_per_event;
        bool verbose;
        bool connection_idle_time;
        bool bio_drain_buffer_sz;
        bool datatype;
        bool root;
        bool breakpad;
        bool max_packet_size;
        bool require_init;
        bool ssl_cipher_list;
        bool ssl_minimum_protocol;
        bool topkeys_size;
        bool stdin_listen;
        bool exit_on_connection_close;
        bool sasl_mechanisms;
        bool ssl_sasl_mechanisms;
        bool dedupe_nmvb_maps;
        bool error_maps;
        bool xattr_enabled;
    } has;

protected:
    /**
     * Note that it is not safe to add new listeners after we've spun up
     * new threads as we don't try to lock the object.
     *
     * the usecase for this is to be able to have logic elsewhere to update
     * state if a settings change.
     */
    std::map<std::string, std::deque<void (*)(const std::string& key, Settings& obj)> > change_listeners;

    void notify_changed(const std::string& key);

    /*************************************************************************
     * These settings are not exposed to the user, and are either derived from
     * the above, or not directly configurable:
     */
protected:
    int maxconns;           /* Total number of permitted connections. Derived
                               from sum of all individual interfaces */

public:
    /* linked lists of all loaded extensions */
    struct {
        EXTENSION_DAEMON_DESCRIPTOR *daemons;
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *binary;
    } extensions;
};

extern Settings settings;
