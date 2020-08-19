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
#pragma once

#include "client_connection.h"

#include <nlohmann/json_fwd.hpp>
#include <functional>

class ConnectionMap {
public:
    /**
     * Initialize the connection map with connections matching the ports
     * opened from Memcached
     */
    void initialize(const nlohmann::json& ports);

    /**
     * Invalidate all of the connections
     */
    void invalidate();

    /**
     * Get a connection object matching the given attributes
     *
     * @param ssl If ssl should be enabled or not
     * @param family the network family (IPv4 / IPv6)
     * @param port (optional) The specific port number to use..
     * @return A connection object to use
     * @throws std::runtime_error if the request can't be served
     */
    MemcachedConnection& getConnection(bool ssl,
                                       sa_family_t family = AF_INET,
                                       in_port_t port = 0);

    /// Get a connection mapped to the given tag
    MemcachedConnection& getConnection(const std::string& tag,
                                       sa_family_t family = AF_INET);

    /**
     * Just get a connection to the server (protocol / ssl etc
     * doesn't matter)
     *
     * @return A connection to the server
     */
    MemcachedConnection& getConnection() const {
        return *connections.front().get();
    }

    /**
     * Do we have a connection matching the requested attributes
     */
    bool contains(bool ssl, sa_family_t family);

    /**
     * Iterate over all of the connections
     */
    void iterate(std::function<void(const MemcachedConnection&)> fn) const {
        for (auto& connection : connections) {
            fn(*connection);
        }
    }

private:
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};
