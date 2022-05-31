/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/portability/Unistd.h>
#include <folly/portability/Windows.h>
#include <nlohmann/json_fwd.hpp>
#include <protocol/connection/client_connection_map.h>
#include <sys/types.h>
#include <filesystem>
#include <memory>
#include <string>

class MemcachedConnection;

namespace cb::test {

/**
 * The node class represents a single node in the system. It is responsible
 * for starting and stopping an external memcached process.
 */
class Node {
public:
    virtual ~Node();

    virtual std::unique_ptr<MemcachedConnection> getConnection() const = 0;

    const std::filesystem::path directory;

    /**
     * Get the map of connections to the node. Given that the node was
     * started with just ephemereal ports the cluster needs the ability
     * to generate the cluster map for clients to fetch the port numbers
     *
     * @return A map containing all of the ports the server provides
     */
    virtual const ConnectionMap& getConnectionMap() const = 0;

    /// get the current configuration stored in "memcached.json"
    virtual nlohmann::json& getConfig() = 0;

    /// write the current configuration to "memcached.json"
    virtual void writeConfig() const = 0;

    /**
     * Create a new instance
     *
     * @param directory The base directory for the node (and where all
     *                  databases should live)
     * @param id a textual identifier to use for the node
     * @param configCallback callback to update the configuration before
     *                       starting the node
     */
    static std::unique_ptr<Node> create(
            std::filesystem::path directory,
            const std::string& id,
            std::function<void(std::string_view, nlohmann::json&)>
                    configCallback);

protected:
    explicit Node(std::filesystem::path dir);
};

} // namespace cb::test
