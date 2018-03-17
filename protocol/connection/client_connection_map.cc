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

bool ConnectionMap::contains(bool ssl, sa_family_t family) {
    try {
        (void)getConnection(ssl, family, 0);
        return true;
    } catch (const std::runtime_error&) {
        return false;
    }
}

void ConnectionMap::initialize(cJSON* ports) {
    invalidate();
    cJSON* array = cJSON_GetObjectItem(ports, "ports");
    if (array == nullptr) {
        std::string msg("ports not found in portnumber file: ");
        msg.append(to_string(ports, false));
        throw std::runtime_error(msg);
    }

    auto numEntries = cJSON_GetArraySize(array);
    sa_family_t family;
    for (int ii = 0; ii < numEntries; ++ii) {
        auto obj = cJSON_GetArrayItem(array, ii);
        auto fam = cJSON_GetObjectItem(obj, "family");
        if (strcmp(fam->valuestring, "AF_INET") == 0) {
            family = AF_INET;
        } else if (strcmp(fam->valuestring, "AF_INET6") == 0) {
            family = AF_INET6;
        } else {
            std::string msg("Unsupported network family: ");
            msg.append(to_string(obj, false));
            throw std::runtime_error(msg);
        }

        auto ssl = cJSON_GetObjectItem(obj, "ssl");
        if (ssl == nullptr) {
            std::string msg("ssl missing for entry: ");
            msg.append(to_string(obj, false));
            throw std::runtime_error(msg);
        }

        auto port = cJSON_GetObjectItem(obj, "port");
        if (port == nullptr) {
            std::string msg("port missing for entry: ");
            msg.append(to_string(obj, false));
            throw std::runtime_error(msg);
        }

        auto protocol = cJSON_GetObjectItem(obj, "protocol");
        if (protocol == nullptr) {
            std::string msg("protocol missing for entry: ");
            msg.append(to_string(obj, false));
            throw std::runtime_error(msg);
        }

        auto portval = static_cast<in_port_t>(port->valueint);
        bool useSsl = ssl->type == cJSON_True ? true : false;

        MemcachedConnection* connection;
        if (strcmp(protocol->valuestring, "memcached") != 0) {
            throw std::logic_error(
                    "ConnectionMap::initialize: Invalid value passed for "
                    "protocol: " +
                    std::string(protocol->valuestring));
        }

        connection = new MemcachedConnection("", portval, family, useSsl);
        connections.push_back(std::unique_ptr<MemcachedConnection>{connection});
    }
}

void ConnectionMap::invalidate() {
    connections.resize(0);
}
