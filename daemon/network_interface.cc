/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "network_interface.h"
#include "settings.h"

#include <logger/logger.h>
#include <platform/dirutils.h>
#include <gsl/gsl>
#include <vector>

static void handle_interface_maxconn(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(R"("maxconn" must be a number)");
    }

    ifc.maxconn = gsl::narrow<int>(obj->valueint);
}

static void handle_interface_port(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(R"("port" must be a number)");
    }

    ifc.port = in_port_t(obj->valueint);
}

static void handle_interface_host(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument(R"("host" must be a string)");
    }

    ifc.host.assign(obj->valuestring);
}

static void handle_interface_backlog(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Number) {
        throw std::invalid_argument(R"("backlog" must be a number)");
    }

    ifc.backlog = gsl::narrow<int>(obj->valueint);
}

/**
 * Set the given NetworkInterface::Protocol based on the value of `obj`,
 * or throw std::invalid_argument if obj is not a valid setting.
 */
static void handle_interface_protocol(NetworkInterface::Protocol& proto,
                                      const char* proto_name,
                                      const cJSON* obj) {
    if (obj->type == cJSON_String) {
        const std::string value(obj->valuestring);
        if (value == "required") {
            proto = NetworkInterface::Protocol::Required;
        } else if (value == "optional") {
            proto = NetworkInterface::Protocol::Optional;
        } else if (value == "off") {
            proto = NetworkInterface::Protocol::Off;
        } else {
            throw std::invalid_argument(
                    "\"" + std::string(proto_name) +
                    "\" has an unrecognized string value \"" + value + R"(")");
        }
        // Backwards compatibility - map True -> Optional, False -> Off
    } else if (obj->type == cJSON_True) {
        proto = NetworkInterface::Protocol::Optional;
    } else if (obj->type == cJSON_False) {
        proto = NetworkInterface::Protocol::Off;
    } else {
        throw std::invalid_argument("\"" + std::string(proto_name) +
                                    "\" must be a string or boolean value)");
    }
}

static void handle_interface_ipv4(NetworkInterface& ifc, cJSON* obj) {
    handle_interface_protocol(ifc.ipv4, "ipv4", obj);
}

static void handle_interface_ipv6(NetworkInterface& ifc, cJSON* obj) {
    handle_interface_protocol(ifc.ipv6, "ipv6", obj);
}

static void handle_interface_tcp_nodelay(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.tcp_nodelay = true;
    } else if (obj->type == cJSON_False) {
        ifc.tcp_nodelay = false;
    } else {
        throw std::invalid_argument(R"("tcp_nodelay" must be a boolean value)");
    }
}

static void handle_interface_management(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type == cJSON_True) {
        ifc.management = true;
    } else if (obj->type == cJSON_False) {
        ifc.management = false;
    } else {
        throw std::invalid_argument(R"("management" must be a boolean value)");
    }
}

static void handle_interface_ssl(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_Object) {
        throw std::invalid_argument(R"("ssl" must be an object)");
    }
    auto* key = cJSON_GetObjectItem(obj, "key");
    auto* cert = cJSON_GetObjectItem(obj, "cert");
    if (key == nullptr || cert == nullptr) {
        throw std::invalid_argument(
                R"("ssl" must contain both "key" and "cert")");
    }

    if (key->type != cJSON_String) {
        throw std::invalid_argument(R"("ssl:key" must be a key)");
    }
    ifc.ssl.key.assign(key->valuestring);

    if (!cb::io::isFile(key->valuestring)) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                R"("ssl:key":')" + ifc.ssl.key + "'");
    }

    if (cert->type != cJSON_String) {
        throw std::invalid_argument(R"("ssl:cert" must be a key)");
    }

    ifc.ssl.cert.assign(cert->valuestring);
    if (!cb::io::isFile(cert->valuestring)) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                R"("ssl:cert":')" + ifc.ssl.cert + "'");
    }
}

static void handle_interface_protocol(NetworkInterface& ifc, cJSON* obj) {
    if (obj->type != cJSON_String) {
        throw std::invalid_argument(R"("protocol" must be a string)");
    }

    if (strcmp(obj->valuestring, "memcached") != 0) {
        throw std::invalid_argument(R"("protocol" must be "memcached")");
    }
}

NetworkInterface::NetworkInterface(gsl::not_null<const cJSON*> json) {
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
        void (*handler)(NetworkInterface& ifc, cJSON* obj);
    };

    std::vector<interface_config_tokens> handlers = {
            {"maxconn", handle_interface_maxconn},
            {"port", handle_interface_port},
            {"host", handle_interface_host},
            {"backlog", handle_interface_backlog},
            {"ipv4", handle_interface_ipv4},
            {"ipv6", handle_interface_ipv6},
            {"tcp_nodelay", handle_interface_tcp_nodelay},
            {"ssl", handle_interface_ssl},
            {"management", handle_interface_management},
            {"protocol", handle_interface_protocol},
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
            LOG_INFO(R"(Unknown token "{}" in config ignored.)", obj->string);
        }

        obj = obj->next;
    }
}

std::string to_string(const NetworkInterface::Protocol& proto) {
    switch (proto) {
    case NetworkInterface::Protocol::Off:
        return "off";
    case NetworkInterface::Protocol::Optional:
        return "optional";
    case NetworkInterface::Protocol::Required:
        return "required";
    }
    return "<invalid>";
}
