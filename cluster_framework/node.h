/*
 *     Copyright 2019 Couchbase, Inc
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

#include <folly/portability/Unistd.h>
#include <folly/portability/Windows.h>
#include <protocol/connection/client_connection_map.h>
#include <sys/types.h>
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

    bool isRunning() const;

    virtual std::unique_ptr<MemcachedConnection> getConnection() const = 0;

    const std::string directory;

    /**
     * Get the map of connections to the node. Given that the node was
     * started with just ephemereal ports the cluster needs the ability
     * to generate the cluster map for clients to fetch the port numbers
     *
     * @return A map containing all of the ports the server provides
     */
    virtual const ConnectionMap& getConnectionMap() const = 0;

    /**
     * Create a new instance
     *
     * @param directory The base directory for the node (and where all
     *                  databases should live)
     * @param id a textual identifier to use for the node
     */
    static std::unique_ptr<Node> create(const std::string& directory,
                                        const std::string& id);

protected:
    explicit Node(std::string dir);

#ifdef WIN32
    mutable HANDLE child = INVALID_HANDLE_VALUE;
#else
    mutable pid_t child = 0;
#endif
};

} // namespace cb::test
