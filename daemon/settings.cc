/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"

#include <cstring>
#include <platform/dirutils.h>
#include "settings.h"
#include "ssl_utils.h"

// the global entry of the settings object
Settings settings;


/**
 * Initialize all members to "null" to preserve backwards
 * compatibility with the previous versions.
 */
Settings::Settings()
    : num_threads(0),
      require_sasl(false),
      bio_drain_buffer_sz(0),
      datatype(false),
      reqs_per_event_high_priority(0),
      reqs_per_event_med_priority(0),
      reqs_per_event_low_priority(0),
      default_reqs_per_event(00),
      max_packet_size(0),
      require_init(false),
      topkeys_size(0),
      stdin_listen(false),
      exit_on_connection_close(false),
      maxconns(0) {

    verbose.store(0);
    connection_idle_time.reset();
    dedupe_nmvb_maps.store(false);

    memset(&has, 0, sizeof(has));
    memset(&extensions, 0, sizeof(extensions));
}

Settings::Settings(const unique_cJSON_ptr& json)
    : Settings() {
    reconfigure(json);
}

/**
 * Handle the "admin" tag in the settings.
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_admin(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"admin\" must be a string");
    }
    s.setAdmin(obj->valuestring);
}

static void throw_missing_file_exception(const std::string &key, cJSON* obj) {
    std::string message("\"");
    message.append(key);
    message.append("\":");
    if (obj->valuestring == nullptr) {
        message.append("null");
    } else {
        message.append("\"");
        message.append(obj->valuestring);
        message.append("\"");
    }
    message.append(" does not exists");
    throw std::invalid_argument(message);
}

/**
 * Handle the "audit_file" tag in the settings
 *
 *  The value must be a string that points to a file that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_audit_file(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"audit_file\" must be a string");
    }

    if (!cb::io::isFile(obj->valuestring)) {
        throw_missing_file_exception("audit_file", obj);
    }

    s.setAuditFile(obj->valuestring);
}

/**
 * Handle the "threads" tag in the settings
 *
 *  The value must be an integer value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_threads(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"threads\" must be an integer");
    }

    s.setNumWorkerThreads(obj->valueint);
}

/**
 * Handle the "require_init" tag in the settings
 *
 *  The value must be a  value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_require_init(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setRequireInit(true);
    } else if (obj->type == cJSON_False) {
        s.setRequireInit(false);
    } else {
        throw std::invalid_argument(
            "\"require_init\" must be a boolean value");
    }
}

/**
 * Handle the "require_sasl" tag in the settings
 *
 *  The value must be a  value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_require_sasl(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setRequireSasl(true);
    } else if (obj->type == cJSON_False) {
        s.setRequireSasl(false);
    } else {
        throw std::invalid_argument(
            "\"require_sasl\" must be a boolean value");
    }
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
static void handle_reqs_event(Settings& s, cJSON* obj) {
    std::string name(obj->string);

    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"" + name + "\" must be an integer");
    }

    EventPriority priority;

    if (name == "default_reqs_per_event") {
        priority = EventPriority::Default;
    } else if (name == "reqs_per_event_high_priority") {
        priority = EventPriority::High;
    } else if (name == "reqs_per_event_med_priority") {
        priority = EventPriority::Medium;
    } else if (name == "reqs_per_event_low_priority") {
        priority = EventPriority::Low;
    } else {
        throw std::invalid_argument("Invalid key specified: " + name);
    }
    s.setRequestsPerEventNotification(obj->valueint, priority);
}

/**
 * Handle the "verbosity" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_verbosity(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"verbosity\" must be an integer");
    }
    s.setVerbose(obj->valueint);
}

/**
 * Handle the "connection_idle_time" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_connection_idle_time(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(
            "\"connection_idle_time\" must be an integer");
    }
    s.setConnectionIdleTime(obj->valueint);
}

/**
 * Handle the "bio_drain_buffer_sz" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_bio_drain_buffer_sz(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(
            "\"bio_drain_buffer_sz\" must be an integer");
    }
    s.setBioDrainBufferSize(obj->valueint);
}

/**
 * Handle the "datatype_support" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_datatype_support(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setDatatypeSupport(true);
    } else if (obj->type == cJSON_False) {
        s.setDatatypeSupport(false);
    } else {
        throw std::invalid_argument(
            "\"datatype_support\" must be a boolean value");
    }
}

/**
 * Handle the "root" tag in the settings
 *
 * The value must be a string that points to a directory that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_root(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"root\" must be a string");
    }

    if (!cb::io::isDirectory(obj->valuestring)) {
        throw_missing_file_exception("root", obj);
    }

    s.setRoot(obj->valuestring);
}

/**
 * Handle the "ssl_cipher_list" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_cipher_list(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"ssl_cipher_list\" must be a string");
    }
    s.setSslCipherList(obj->valuestring);
}

/**
 * Handle the "ssl_minimum_protocol" tag in the settings
 *
 * The value must be a string containing one of the following:
 *    tlsv1, tlsv1.1, tlsv1_1, tlsv1.2, tlsv1_2
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_minimum_protocol(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument(
            "\"ssl_minimum_protocol\" must be a string");
    }

    try {
        decode_ssl_protocol(obj->valuestring);
    } catch (std::exception& e) {
        throw std::invalid_argument(
            "\"ssl_minimum_protocol\"" + std::string(e.what()));
    }
    s.setSslMinimumProtocol(obj->valuestring);
}

/**
 * Handle the "get_max_packet_size" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_max_packet_size(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(
            "\"max_packet_size\" must be an integer");
    }
    s.setMaxPacketSize(obj->valueint * 1024 * 1024);
}

/**
 * Handle the "stdin_listen" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_stdin_listen(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setStdinListen(true);
    } else if (obj->type == cJSON_False) {
        s.setStdinListen(false);
    } else {
        throw std::invalid_argument(
            "\"stdin_listen\" must be a boolean value");
    }
}

/**
 * Handle the "exit_on_connection_close" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_exit_on_connection_close(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setExitOnConnectionClose(true);
    } else if (obj->type == cJSON_False) {
        s.setExitOnConnectionClose(false);
    } else {
        throw std::invalid_argument(
            "\"exit_on_connection_close\" must be a boolean value");
    }
}

/**
 * Handle the "sasl_mechanisms" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_sasl_mechanisms(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"sasl_mechanisms\" must be a string");
    }
    s.setSaslMechanisms(obj->valuestring);
}

/**
 * Handle the "dedupe_nmvb_maps" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_dedupe_nmvb_maps(Settings& s, cJSON* obj) {
    if (obj->type == cJSON_True) {
        s.setDedupeNmvbMaps(true);
    } else if (obj->type == cJSON_False) {
        s.setDedupeNmvbMaps(false);
    } else {
        throw std::invalid_argument(
            "\"dedupe_nmvb_maps\" must be a boolean value");
    }
}

/**
 * Handle the "extensions" tag in the settings
 *
 *  The value must be an array
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_extensions(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Array) {
        throw std::invalid_argument("\"extensions\" must be an array");
    }

    for (auto* child = obj->child; child != nullptr; child = child->next) {
        if (child->type != cJSON_Object) {
            throw std::invalid_argument(
                "Elements in the \"extensions\" array myst be objects");
        }
        extension_settings ext(child);
        s.addPendingExtension(ext);
    }
}

/**
 * Handle the "interfaces" tag in the settings
 *
 *  The value must be an array
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_interfaces(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Array) {
        throw std::invalid_argument("\"interfaces\" must be an array");
    }

    for (auto* child = obj->child; child != nullptr; child = child->next) {
        if (child->type != cJSON_Object) {
            throw std::invalid_argument(
                "Elements in the \"interfaces\" array myst be objects");
        }
        interface ifc(child);
        s.addInterface(ifc);
    }
}

static void handle_breakpad(Settings& s, cJSON* obj) {
    if (obj->type != cJSON_Object) {
        throw std::invalid_argument("\"breakpad\" must be an object");
    }

    BreakpadSettings breakpad(obj);
    s.setBreakpadSettings(breakpad);
}

void Settings::reconfigure(const unique_cJSON_ptr& json) {
    // Nuke the default interface added to the system in settings_init and
    // use the ones in the configuration file.. (this is a bit messy)
    interfaces.clear();

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
         * @throws std::invalid_argument if it something is wrong with the
         *         entry
         */
        void (* handler)(Settings& settings, cJSON* obj);
    };

    std::vector<settings_config_tokens> handlers = {
        {"admin",                        handle_admin},
        {"audit_file",                   handle_audit_file},
        {"threads",                      handle_threads},
        {"interfaces",                   handle_interfaces},
        {"extensions",                   handle_extensions},
        {"require_init",                 handle_require_init},
        {"require_sasl",                 handle_require_sasl},
        {"default_reqs_per_event",       handle_reqs_event},
        {"reqs_per_event_high_priority", handle_reqs_event},
        {"reqs_per_event_med_priority",  handle_reqs_event},
        {"reqs_per_event_low_priority",  handle_reqs_event},
        {"verbosity",                    handle_verbosity},
        {"connection_idle_time",         handle_connection_idle_time},
        {"bio_drain_buffer_sz",          handle_bio_drain_buffer_sz},
        {"datatype_support",             handle_datatype_support},
        {"root",                         handle_root},
        {"ssl_cipher_list",              handle_ssl_cipher_list},
        {"ssl_minimum_protocol",         handle_ssl_minimum_protocol},
        {"breakpad",                     handle_breakpad},
        {"max_packet_size",              handle_max_packet_size},
        {"stdin_listen",                 handle_stdin_listen},
        {"exit_on_connection_close",     handle_exit_on_connection_close},
        {"sasl_mechanisms",              handle_sasl_mechanisms},
        {"dedupe_nmvb_maps",             handle_dedupe_nmvb_maps}
    };

    cJSON* obj = json->child;
    while (obj != nullptr) {
        std::string key(obj->string);
        bool found = false;
        for (auto& handler : handlers) {
            if (handler.key == key) {
                handler.handler(*this, obj);
                found = true;
                break;
            }
        }

        if (!found) {
            logit(EXTENSION_LOG_WARNING,
                  "Unknown token \"%s\" in config ignored.\n",
                  obj->string);
        }

        obj = obj->next;
    }
}

static void handle_interface_maxconn(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"maxconn\" must be a number");
    }

    ifc.maxconn = obj->valueint;
}

static void handle_interface_port(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"port\" must be a number");
    }

    ifc.port = in_port_t(obj->valueint);
}

static void handle_interface_host(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"host\" must be a string");
    }

    ifc.host.assign(obj->valuestring);
}

static void handle_interface_backlog(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument("\"backlog\" must be a number");
    }

    ifc.backlog = obj->valueint;
}

static void handle_interface_ipv4(struct interface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.ipv4 = true;
    } else if (obj->type == cJSON_False) {
        ifc.ipv4 = false;
    } else {
        throw std::invalid_argument("\"ipv4\" must be a boolean value");
    }
}

static void handle_interface_ipv6(struct interface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.ipv6 = true;
    } else if (obj->type == cJSON_False) {
        ifc.ipv6 = false;
    } else {
        throw std::invalid_argument("\"ipv6\" must be a boolean value");
    }
}

static void handle_interface_tcp_nodelay(struct interface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.tcp_nodelay = true;
    } else if (obj->type == cJSON_False) {
        ifc.tcp_nodelay = false;
    } else {
        throw std::invalid_argument("\"tcp_nodelay\" must be a boolean value");
    }
}

static void handle_interface_management(struct interface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.management = true;
    } else if (obj->type == cJSON_False) {
        ifc.management = false;
    } else {
        throw std::invalid_argument("\"management\" must be a boolean value");
    }
}

static void handle_interface_ssl(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Object) {
        throw std::invalid_argument("\"ssl\" must be an object");
    }
    auto* key = cJSON_GetObjectItem(obj, "key");
    auto* cert = cJSON_GetObjectItem(obj, "cert");
    if (key == nullptr || cert == nullptr) {
        throw std::invalid_argument(
            "\"ssl\" must contain both \"key\" and \"cert\"");
    }

    if (key->type != cJSON_String) {
        throw std::invalid_argument("\"ssl:key\" must be a key");
    }

    if (!cb::io::isFile(key->valuestring)) {
        throw_missing_file_exception("ssl:key", key);
    }

    if (cert->type != cJSON_String) {
        throw std::invalid_argument("\"ssl:cert\" must be a key");
    }

    if (!cb::io::isFile(cert->valuestring)) {
        throw_missing_file_exception("ssl:cert", cert);
    }

    ifc.ssl.key.assign(key->valuestring);
    ifc.ssl.cert.assign(cert->valuestring);
}

static void handle_interface_protocol(struct interface& ifc, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument("\"protocol\" must be a string");
    }

    std::string protocol(obj->valuestring);

    if (protocol == "memcached") {
        ifc.protocol = Protocol::Memcached;
    } else if (protocol == "greenstack") {
        ifc.protocol = Protocol::Greenstack;
    } else {
        throw std::invalid_argument(
            "\"protocol\" must be \"memcached\" or \"greenstack\"");
    }
}

interface::interface(const cJSON* json)
    : interface() {


    struct interface_config_tokens {
        /**
         * The key in the configuration
         */
        std::string key;

        /**
         * A callback method used by the interface object when we're parsing
         * the config attributes.
         *
         * @param ifc the interface object to update
         * @param obj the current object in the configuration we're looking at
         * @throws std::invalid_argument if it something is wrong with the
         *         entry
         */
        void (* handler)(struct interface& ifc, cJSON* obj);
    };

    std::vector<interface_config_tokens> handlers = {
        {"maxconn",     handle_interface_maxconn},
        {"port",        handle_interface_port},
        {"host",        handle_interface_host},
        {"backlog",     handle_interface_backlog},
        {"ipv4",        handle_interface_ipv4},
        {"ipv6",        handle_interface_ipv6},
        {"tcp_nodelay", handle_interface_tcp_nodelay},
        {"ssl",         handle_interface_ssl},
        {"management",  handle_interface_management},
        {"protocol",    handle_interface_protocol},
    };

    cJSON* obj = json->child;
    while (obj != nullptr) {
        std::string key(obj->string);
        bool found = false;
        for (auto& handler : handlers) {
            if (handler.key == key) {
                handler.handler(*this, obj);
                found = true;
                break;
            }
        }

        if (!found) {
            Settings::logit(EXTENSION_LOG_NOTICE,
                            "Unknown token \"%s\" in config ignored.\n",
                            obj->string);
        }

        obj = obj->next;
    }
}

void Settings::updateSettings(const Settings& other, bool apply) {
    // I need to figure out this one..
    if (other.has.admin) {
        if (other.admin != admin) {
            throw std::invalid_argument("admin can't be changed dynamically");
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
    if (other.has.require_sasl) {
        if (other.require_sasl != require_sasl) {
            throw std::invalid_argument(
                "require_sasl can't be changed dynamically");
        }
    }
    if (other.has.bio_drain_buffer_sz) {
        if (other.bio_drain_buffer_sz != bio_drain_buffer_sz) {
            throw std::invalid_argument(
                "bio_drain_buffer_sz can't be changed dynamically");
        }
    }
    if (other.has.datatype) {
        if (other.datatype != datatype) {
            throw std::invalid_argument(
                "datatype can't be changed dynamically");
        }
    }
    if (other.has.root) {
        if (other.root != root) {
            throw std::invalid_argument("root can't be changed dynamically");
        }
    }
    if (other.has.require_init) {
        if (other.require_init != require_init) {
            throw std::invalid_argument(
                "require_init can't be changed dynamically");
        }
    }
    if (other.has.topkeys_size) {
        if (other.topkeys_size != topkeys_size) {
            throw std::invalid_argument(
                "topkeys_size can't be changed dynamically");
        }
    }
    if (other.has.stdin_listen) {
        if (other.stdin_listen != stdin_listen) {
            throw std::invalid_argument(
                "stdin_listen can't be changed dynamically");
        }
    }
    if (other.has.exit_on_connection_close) {
        if (other.exit_on_connection_close != exit_on_connection_close) {
            throw std::invalid_argument(
                "exit_on_connection_close can't be changed dynamically");
        }
    }
    if (other.has.sasl_mechanisms) {
        if (other.sasl_mechanisms != sasl_mechanisms) {
            throw std::invalid_argument(
                "sasl_mechanisms can't be changed dynamically");
        }
    }

    if (other.has.interfaces) {
        if (other.interfaces.size() != interfaces.size()) {
            throw std::invalid_argument(
                "interfaces can't be changed dynamically");
        }

        // validate that we haven't changed stuff in the entries
        auto total = interfaces.size();
        for (std::vector<interface>::size_type ii = 0; ii < total; ++ii) {
            const auto& i1 = interfaces[ii];
            const auto& i2 = other.interfaces[ii];

            if (i1.port == 0 || i2.port == 0) {
                // we can't look at dynamic ports...
                continue;
            }

            // the following fields can't change
            if ((i1.host != i2.host) || (i1.port != i2.port) ||
                (i1.ipv4 != i2.ipv4) || (i1.ipv6 != i2.ipv6) ||
                (i1.protocol != i2.protocol) ||
                (i1.management != i2.management)) {
                throw std::invalid_argument(
                    "interfaces can't be changed dynamically");
            }
        }
    }

    if (other.has.extensions) {
        if (other.pending_extensions.size() != pending_extensions.size()) {
            throw std::invalid_argument(
                "extensions can't be changed dynamically");
        }

        // validate that we haven't changed stuff in the entries
        auto total = pending_extensions.size();
        for (std::vector<extension_settings>::size_type ii = 0;
             ii < total; ++ii) {
            const auto& e1 = pending_extensions[ii];
            const auto& e2 = other.pending_extensions[ii];

            if ((e1.config != e2.config) || (e1.soname != e2.soname)) {
                throw std::invalid_argument(
                    "extensions can't be changed dynamically");
            }
        }
    }

    // All non-dynamic settings has been validated. If we're not supposed
    // to update anything we can bail out.
    if (!apply) {
        return;
    }


    // Ok, go ahead and update the settings!!
    if (other.has.verbose) {
        if (other.verbose != verbose) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change verbosity level from %u to %u",
                  verbose.load(), other.verbose.load());
            setVerbose(other.verbose.load());
        }
    }

    if (other.has.reqs_per_event_high_priority) {
        if (other.reqs_per_event_high_priority !=
            reqs_per_event_high_priority) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change high priority iterations per event from %u to %u",
                  reqs_per_event_high_priority,
                  other.reqs_per_event_high_priority);
            setRequestsPerEventNotification(other.reqs_per_event_high_priority,
                                            EventPriority::High);
        }
    }
    if (other.has.reqs_per_event_med_priority) {
        if (other.reqs_per_event_med_priority != reqs_per_event_med_priority) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change medium priority iterations per event from %u to %u",
                  reqs_per_event_med_priority,
                  other.reqs_per_event_med_priority);
            setRequestsPerEventNotification(other.reqs_per_event_med_priority,
                                            EventPriority::Medium);
        }
    }
    if (other.has.reqs_per_event_low_priority) {
        if (other.reqs_per_event_low_priority != reqs_per_event_low_priority) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change low priority iterations per event from %u to %u",
                  reqs_per_event_low_priority,
                  other.reqs_per_event_low_priority);
            setRequestsPerEventNotification(other.reqs_per_event_low_priority,
                                            EventPriority::Low);
        }
    }
    if (other.has.default_reqs_per_event) {
        if (other.default_reqs_per_event != default_reqs_per_event) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change default iterations per event from %u to %u",
                  default_reqs_per_event,
                  other.default_reqs_per_event);
            setRequestsPerEventNotification(other.default_reqs_per_event,
                                            EventPriority::Default);
        }
    }
    if (other.has.connection_idle_time) {
        if (other.connection_idle_time != connection_idle_time) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change connection idle time from %u to %u",
                  connection_idle_time.load(),
                  other.connection_idle_time.load());
            setConnectionIdleTime(other.connection_idle_time);
        }
    }
    if (other.has.max_packet_size) {
        if (other.max_packet_size != max_packet_size) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change max packet size from %u to %u",
                  max_packet_size,
                  other.max_packet_size);
            setMaxPacketSize(other.max_packet_size);
        }
    }
    if (other.has.ssl_cipher_list) {
        if (other.ssl_cipher_list != ssl_cipher_list) {
            // this isn't safe!! an other thread could call stats settings
            // which would cause this to crash...
            logit(EXTENSION_LOG_NOTICE,
                  "Change SSL Cipher list from \"%s\" to \"%s\"",
                  ssl_cipher_list.c_str(), other.ssl_cipher_list.c_str());
            setSslCipherList(other.ssl_cipher_list);
        }
    }
    if (other.has.ssl_minimum_protocol) {
        if (other.ssl_minimum_protocol != ssl_minimum_protocol) {
            // this isn't safe!! an other thread could call stats settings
            // which would cause this to crash...
            logit(EXTENSION_LOG_NOTICE,
                  "Change SSL minimum protocol from \"%s\" to \"%s\"",
                  ssl_minimum_protocol.c_str(),
                  other.ssl_minimum_protocol.c_str());
            setSslMinimumProtocol(other.ssl_minimum_protocol);
        }
    }
    if (other.has.dedupe_nmvb_maps) {
        if (other.dedupe_nmvb_maps != dedupe_nmvb_maps) {
            logit(EXTENSION_LOG_NOTICE,
                  "%s deduplication of NMVB maps",
                  other.dedupe_nmvb_maps.load() ? "Enable" : "Disable");
            setDedupeNmvbMaps(other.dedupe_nmvb_maps.load());
        }
    }

    if (other.has.interfaces) {
        // validate that we haven't changed stuff in the entries
        auto total = interfaces.size();
        bool changed = false;
        for (std::vector<interface>::size_type ii = 0; ii < total; ++ii) {
            auto& i1 = interfaces[ii];
            const auto& i2 = other.interfaces[ii];

            if (i1.port == 0 || i2.port == 0) {
                // we can't look at dynamic ports...
                continue;
            }

            if (i2.maxconn != i1.maxconn) {
                logit(EXTENSION_LOG_NOTICE,
                      "Change max connections for %s:%u from %u to %u",
                      i1.host.c_str(), i1.port, i1.maxconn, i2.maxconn);
                i1.maxconn = i2.maxconn;
                changed = true;
            }

            if (i2.backlog != i1.backlog) {
                logit(EXTENSION_LOG_NOTICE,
                      "Change backlog for %s:%u from %u to %u",
                      i1.host.c_str(), i1.port, i1.backlog, i2.backlog);
                i1.backlog = i2.backlog;
                changed = true;
            }

            if (i2.tcp_nodelay != i1.tcp_nodelay) {
                logit(EXTENSION_LOG_NOTICE,
                      "%e TCP NODELAY for %s:%u",
                      i2.tcp_nodelay ? "Enable" : "Disable",
                      i1.host.c_str(), i1.port);
                i1.tcp_nodelay = i2.tcp_nodelay;
                changed = true;
            }

            if (i2.ssl.cert != i1.ssl.cert) {
                logit(EXTENSION_LOG_NOTICE,
                      "Change SSL Certificiate for %s:%u from %s to %s",
                      i1.host.c_str(), i1.port, i1.ssl.cert.c_str(),
                      i2.ssl.cert.c_str());
                i1.ssl.cert.assign(i2.ssl.cert);
                changed = true;
            }

            if (i2.ssl.key != i1.ssl.key) {
                logit(EXTENSION_LOG_NOTICE,
                      "Change SSL Key for %s:%u from %s to %s",
                      i1.host.c_str(), i1.port, i1.ssl.key.c_str(),
                      i2.ssl.key.c_str());
                i1.ssl.key.assign(i2.ssl.key);
                changed = true;
            }
        }

        if (changed) {
            notify_changed("interfaces");
        }
    }

    if (other.has.breakpad) {
        bool changed = false;
        auto& b1 = breakpad;
        const auto& b2 = other.breakpad;

        if (b2.isEnabled() != b1.isEnabled()) {
            logit(EXTENSION_LOG_NOTICE,
                  "%e breakpad",
                  b2.isEnabled() ? "Enable" : "Disable");
            b1.setEnabled((b2.isEnabled()));
            changed = true;
        }

        if (b2.getMinidumpDir() != b1.getMinidumpDir()) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change minidump directory from \"%s\" to \"%s\"",
                  b1.getMinidumpDir().c_str(),
                  b2.getMinidumpDir().c_str());
            b1.setMinidumpDir(b2.getMinidumpDir());
            changed = true;
        }

        if (b2.getContent() != b1.getContent()) {
            logit(EXTENSION_LOG_NOTICE,
                  "Change minidump content from %u to %u",
                  b1.getContent(),
                  b2.getContent());
            b1.setContent(b2.getContent());
            changed = true;
        }

        if (changed) {
            notify_changed("breakpad");
        }
    }
}

void Settings::logit(EXTENSION_LOG_LEVEL level, const char* fmt, ...) {
    auto logger = settings.extensions.logger;
    if (logger != nullptr) {
        char buffer[1024];

        va_list ap;
        va_start(ap, fmt);
        auto len = vsnprintf(buffer, sizeof(buffer), fmt, ap);
        va_end(ap);
        if (len < 0) {
            return;
        }
        buffer[sizeof(buffer) - 1] = '\0';

        logger->log(level, nullptr, "%s", buffer);
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

BreakpadSettings::BreakpadSettings(const cJSON* json) {
    auto* obj = cJSON_GetObjectItem(const_cast<cJSON*>(json), "enabled");
    if (obj == nullptr) {
        throw std::invalid_argument(
            "\"breakpad\" settings MUST contain \"enabled\" attribute");
    }
    if (obj->type == cJSON_True) {
        enabled = true;
    } else if (obj->type == cJSON_False) {
        enabled = false;
    } else {
        throw std::invalid_argument(
            "\"breakpad:enabled\" settings must be a boolean value");
    }

    obj = cJSON_GetObjectItem(const_cast<cJSON*>(json), "minidump_dir");
    if (obj == nullptr) {
        if (enabled) {
            throw std::invalid_argument(
                "\"breakpad\" settings MUST contain \"minidump_dir\" attribute when enabled");
        }
    } else if (obj->type != cJSON_String) {
        throw std::invalid_argument(
            "\"breakpad:minidump_dir\" settings must be a string");
    } else {
        minidump_dir.assign(obj->valuestring);
        if (enabled) {
            if (!cb::io::isDirectory(minidump_dir)) {
                throw_missing_file_exception("breakpad:minidump_dir", obj);
            }
        }
    }

    obj = cJSON_GetObjectItem(const_cast<cJSON*>(json), "content");
    if (obj != nullptr) {
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                "\"breakpad:content\" settings must be a string");
        }
        if (strcmp(obj->valuestring, "default") != 0) {
            throw std::invalid_argument(
                "\"breakpad:content\" settings must set to \"default\"");
        }
        content = BreakpadContent::Default;
    }
}
