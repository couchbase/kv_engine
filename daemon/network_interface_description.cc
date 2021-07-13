/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "network_interface_description.h"
#include <nlohmann/json.hpp>

/// Validate the interface description
static nlohmann::json validateInterfaceDescription(const nlohmann::json& json) {
    if (!json.is_object()) {
        throw std::invalid_argument("value must be JSON of type object");
    }

    bool host = false;
    bool port = false;
    bool family = false;
    bool type = false;

    for (const auto& kv : json.items()) {
        if (kv.key() == "host") {
            if (!kv.value().is_string()) {
                throw std::invalid_argument("host must be JSON of type string");
            }
            host = true;
        } else if (kv.key() == "port") {
            // Yack. One thought one could use is_number_unsigned and it would
            // return true for all positive integer values, but it actually
            // looks at the _datatype_ so it may return false for lets say
            // 0 and 11210. Perform the checks myself
            if (!kv.value().is_number_integer()) {
                throw std::invalid_argument("port must be JSON of type number");
            }
            if (kv.value().get<int>() < 0) {
                throw std::invalid_argument("port must be a positive number");
            }
            if (kv.value().get<int>() > std::numeric_limits<in_port_t>::max()) {
                throw std::invalid_argument(
                        "port must be [0," +
                        std::to_string(std::numeric_limits<in_port_t>::max()) +
                        "]");
            }
            port = true;
        } else if (kv.key() == "family") {
            if (!kv.value().is_string()) {
                throw std::invalid_argument(
                        "family must be JSON of type string");
            }
            const auto value = kv.value().get<std::string>();
            if (value != "inet" && value != "inet6") {
                throw std::invalid_argument(R"(family must "inet" or "inet6")");
            }
            family = true;
        } else if (kv.key() == "tls") {
            if (!kv.value().is_boolean()) {
                throw std::invalid_argument("tls must be JSON of type boolean");
            }
        } else if (kv.key() == "system") {
            if (!kv.value().is_boolean()) {
                throw std::invalid_argument(
                        "system must be JSON of type boolean");
            }
        } else if (kv.key() == "type") {
            if (!kv.value().is_string()) {
                throw std::invalid_argument("type must be JSON of type string");
            }
            const auto value = kv.value().get<std::string>();
            if (value != "mcbp" && value != "prometheus") {
                throw std::invalid_argument(
                        R"(type must "mcbp" or "prometheus")");
            }
            type = true;
        } else if (kv.key() == "tag") {
            if (!kv.value().is_string()) {
                throw std::invalid_argument("tag must be JSON of type string");
            }
        } else if (kv.key() == "uuid") {
            if (!kv.value().is_string()) {
                throw std::invalid_argument("uuid must be JSON of type string");
            }
        } else {
            throw std::invalid_argument("Unsupported JSON property " +
                                        kv.key());
        }
    }

    if (!host || !port || !family || !type) {
        throw std::invalid_argument(
                R"("host", "port", "family" and "type" must all be set)");
    }

    return json;
}

NetworkInterfaceDescription::NetworkInterfaceDescription(
        const nlohmann::json& json)
    : NetworkInterfaceDescription(validateInterfaceDescription(json), true) {
}

static sa_family_t toFamily(const std::string& family) {
    if (family == "inet") {
        return AF_INET;
    }
    if (family == "inet6") {
        return AF_INET6;
    }

    throw std::invalid_argument(
            "toFamily(): family must be 'inet' or 'inet6': " + family);
}

NetworkInterfaceDescription::NetworkInterfaceDescription(
        const nlohmann::json& json, bool)
    : host(json["host"].get<std::string>()),
      port(json["port"].get<in_port_t>()),
      family(toFamily(json["family"].get<std::string>())),
      system(!(json.find("system") == json.cend()) &&
             json["system"].get<bool>()),
      type(json["type"].get<std::string>() == "mcbp" ? Type::Mcbp
                                                     : Type::Prometheus),
      tag(json.find("tag") == json.cend() ? ""
                                          : json["tag"].get<std::string>()),
      tls(!(json.find("tls") == json.cend()) && json["tls"].get<bool>()),
      uuid(json.find("uuid") == json.cend() ? ""
                                            : json["uuid"].get<std::string>()) {
}
