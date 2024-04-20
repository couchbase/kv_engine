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

#include "dcp_packet_filter.h"
#include "utilities/test_manifest.h"

#include <memcached/vbucket.h>
#include <nlohmann/json_fwd.hpp>
#include <filesystem>
#include <memory>
#include <optional>

class MemcachedConnection;

namespace cb::test {

class Node;
class Bucket;
class AuthProviderService;

/**
 * The Cluster class represents a running cluster
 *
 * See readme.md for information on how to use the cluster
 *
 */
class Cluster {
public:
    virtual ~Cluster();

    /**
     * Create a Couchbase bucket
     *
     * @param name The name of the bucket to create
     * @param attributes A JSON object containing properties for the
     *                   bucket.
     * @param packet_filter An optional packet filter which is called for
     *                      with all of the packets going over the replication
     *                      streams for the bucket _before_ it is passed to
     *                      the other side. It is the content of the vector
     *                      which is put on the stream to the other end,
     *                      so the callback is free to inspect, modify or drop
     *                      the entire packet.
     * @param setup should we set up replication?
     * @return a bucket object representing the bucket
     */
    [[nodiscard]] virtual std::shared_ptr<Bucket> createBucket(
            const std::string& name,
            const nlohmann::json& attributes,
            DcpPacketFilter packet_filter = {},
            bool setup = true) = 0;

    /**
     * Delete the named bucket
     *
     * @param name the bucket to delete
     */
    virtual void deleteBucket(const std::string& name) = 0;

    /**
     * Lookup the named bucket
     *
     * @param name The name of the bucket
     * @return THe handle to the named bucket (if exist)
     */
    [[nodiscard]] virtual std::shared_ptr<Bucket> getBucket(
            const std::string& name) const = 0;

    /**
     * Get a connection to the specified node (note that node index starts
     * at 0)
     *
     * @param node the node number
     * @return a connection towards the specified node
     */
    [[nodiscard]] virtual std::unique_ptr<MemcachedConnection> getConnection(
            size_t node) const = 0;

    /**
     * Fetch the size of the cluster
     *
     * @return the number of nodes this cluster is built up of
     */
    [[nodiscard]] virtual size_t size() const = 0;

    /// Get the auth provider
    [[nodiscard]] virtual AuthProviderService& getAuthProviderService() = 0;

    CollectionsManifest collections;

    /// Get a JSON representation for the pool (whats typically returned
    /// via rest when requesting "http://172.0.0.1:8091/pools"
    [[nodiscard]] virtual nlohmann::json to_json() const = 0;

    /// Iterate over all of the nodes in the cluster
    virtual void iterateNodes(
            std::function<void(const Node&)> visitor) const = 0;

    /**
     * Factory method to create a cluster
     *
     * @param nodes The number of nodes in the cluster
     * @param directory an optional directory of where to create the nodes
     *                  (by default `pwd/cluster_XXXXX` where XXXXX is a
     *                  unique number)
     * @param configCallback config used to populate configuration on
     *                       a per node basis. The method is called with the
     *                       node ID and the current configuration.
     * @return a handle to the newly created cluster
     */
    [[nodiscard]] static std::unique_ptr<Cluster> create(
            size_t nodes,
            std::optional<std::string> directory = {},
            const std::function<void(std::string_view, nlohmann::json&)>&
                    configCallback = {});

    /// Get a JSON representation for the cluster before the
    /// cluster is initialized
    [[nodiscard]] static nlohmann::json getUninitializedJson();

    /**
     * Utility function to try to remove a file or directory with retries
     * (on windows you cannot remove a file if someone have it open and we
     * have some race conditions in our tests doing that)
     *
     * @param path The file or directory to remove
     * @param errorcallback A callback with the error. If a callback is
     *                      provided the function will retry until the callback
     *                      returns false
     * @return The last error message if we failed to remove the requested
     *
     */
    static void removeWithRetry(
            const std::filesystem::path& path,
            std::function<bool(const std::exception&)> errorcallback = {});
};

} // namespace cb::test
