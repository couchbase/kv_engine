/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "client_connection_map.h"

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <utilities/json_utilities.h>

/////////////////////////////////////////////////////////////////////////
// Implementation of the ConnectionMap class
/////////////////////////////////////////////////////////////////////////
MemcachedConnection& ConnectionMap::getConnection(bool ssl,
                                                  sa_family_t family,
                                                  in_port_t port) {
    for (auto& conn : connections) {
        if (conn->isSsl() == ssl && conn->getFamily() == family &&
            (port == 0 || conn->getPort() == port)) {
            return *conn;
        }
    }

    throw std::runtime_error("No connection matching the request");
}

MemcachedConnection& ConnectionMap::getConnection(const std::string& tag,
                                                  sa_family_t family) {
    for (auto& conn : connections) {
        if (conn->getTag() == tag && conn->getFamily() == family) {
            return *conn;
        }
    }

    throw std::runtime_error("No connection matching the request");
}

bool ConnectionMap::contains(bool ssl, sa_family_t family) {
    try {
        (void)getConnection(ssl, family, 0);
        return true;
    } catch (const std::runtime_error&) {
        return false;
    }
}

void ConnectionMap::initialize(const nlohmann::json& ports) {
    invalidate();
    addPorts(ports);
}

void ConnectionMap::addPorts(const nlohmann::json& ports) {
    auto array = ports.find("ports");
    if (array == ports.end()) {
        throw std::runtime_error("ports not found in portnumber file: " +
                                 ports.dump());
    }

    sa_family_t family;
    for (const auto& obj : *array) {
        auto host = cb::jsonGet<std::string>(obj, "host");
        auto port = cb::jsonGet<size_t>(obj, "port");
        if (port < 1 || port > std::numeric_limits<in_port_t>::max()) {
            throw std::runtime_error(
                    fmt::format("Port number must be in the range [1,{}]: {}",
                                std::numeric_limits<in_port_t>::max(),
                                obj.dump()));
        }

        auto fam = cb::jsonGet<std::string>(obj, "family");
        if (fam == "inet") {
            family = AF_INET;
        } else {
            family = AF_INET6;
        }

        bool tls = cb::jsonGet<bool>(obj, "tls");
        connections.push_back(std::make_unique<MemcachedConnection>(
                "", gsl::narrow_cast<in_port_t>(port), family, tls));
        if (obj.find("tag") != obj.cend()) {
            connections.back()->setTag(obj["tag"]);
        }
        if (family == AF_INET) {
            connections.back()->setName(host + ":" + std::to_string(port));
        } else {
            connections.back()->setName("[" + host +
                                        "]:" + std::to_string(port));
        }
        connections.back()->setServerInterfaceUuid(obj["uuid"]);
    }
}

void ConnectionMap::invalidate() {
    connections.resize(0);
}

void ConnectionMap::add(const nlohmann::json& description) {
    addPorts(description);
}

void ConnectionMap::remove(const std::string& uuid) {
    for (auto iter = connections.begin(); iter != connections.end(); iter++) {
        if ((*iter)->getServerInterfaceUuid() == uuid) {
            connections.erase(iter);
        }
    }
}
