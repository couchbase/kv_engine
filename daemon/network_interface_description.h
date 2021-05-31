/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>

class NetworkInterfaceDescription {
public:
    enum class Type { Mcbp, Prometheus };

    /**
     * Initialize the NetworkInterfaceDescription by parsing the provided
     * document.
     *
     * @param json a json document containing the spec
     * @throws std::invalid_arguments if the format of the JSON isn't correct
     */
    explicit NetworkInterfaceDescription(const nlohmann::json& json);

    std::string getHost() const {
        return host;
    }

    std::string getHostname() const {
        if (host == "*") {
            return family == AF_INET ? "0.0.0.0" : "::";
        }
        return host;
    }

    in_port_t getPort() const {
        return port;
    }
    void setPort(in_port_t next) {
        port = next;
    }
    sa_family_t getFamily() const {
        return family;
    }
    bool isSystem() const {
        return system;
    }
    Type getType() const {
        return type;
    }
    std::string getTag() const {
        return tag;
    }
    bool isTls() const {
        return tls;
    }

protected:
    NetworkInterfaceDescription(const nlohmann::json& json, bool);
    const std::string host;
    in_port_t port;
    const sa_family_t family;
    const bool system;
    const Type type;
    const std::string tag;
    const bool tls;
};
