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

    /// Add a new entry to the map
    void add(const nlohmann::json& description);

    /// Remove the entry with the given UUID
    void remove(const std::string& uuid);

protected:
    void addPorts(const nlohmann::json& ports);

    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};
