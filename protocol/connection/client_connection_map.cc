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
#include "client_connection_map.h"

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
            return *conn.get();
        }
    }

    throw std::runtime_error("No connection matching the request");
}

MemcachedConnection& ConnectionMap::getConnection(const std::string& tag,
                                                  sa_family_t family) {
    for (auto& conn : connections) {
        if (conn->getTag() == tag && conn->getFamily() == family) {
            return *conn.get();
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
    auto array = ports.find("ports");
    if (array == ports.end()) {
        throw std::runtime_error("ports not found in portnumber file: " +
                                 ports.dump());
    }

    sa_family_t family;
    for (const auto& obj : *array) {
        auto fam = cb::jsonGet<std::string>(obj, "family");
        if (fam == "AF_INET") {
            family = AF_INET;
        } else {
            family = AF_INET6;
        }

        auto ssl = cb::jsonGet<bool>(obj, "ssl");
        auto port = static_cast<in_port_t>(cb::jsonGet<size_t>(obj, "port"));
        if (port == in_port_t(-1)) {
            throw std::runtime_error("port cannot be -1");
        }
        connections.push_back(
                std::make_unique<MemcachedConnection>("", port, family, ssl));
        connections.back()->setTag(cb::jsonGet<std::string>(obj, "tag"));
        connections.back()->setName(cb::jsonGet<std::string>(obj, "name"));
    }
}

void ConnectionMap::invalidate() {
    connections.resize(0);
}
