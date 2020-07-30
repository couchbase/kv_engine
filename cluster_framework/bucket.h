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

#include "dcp_packet_filter.h"

#include <memcached/vbucket.h>
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>

class MemcachedConnection;

namespace cb::test {

class Cluster;
class DcpReplicator;

struct ReplicationConfig;

/**
 * The Bucket represents a bucket defined in the cluster. It owns the
 * full vbucket map of the cluster, and a thread which runs all DCP
 * replication streams for that bucket.
 *
 * @todo add support for rebalance and moving vbuckets around
 */
class Bucket {
public:
    /**
     * Create a bucket
     *
     * @param cluster The cluster the bucket belongs to
     * @param name The name of the bucket
     * @param vbuckets The number of vbuckets
     * @param replicas The number of replicas
     * @param packet_filter An optional packet filter which is called for
     *                      with all of the packets going over the replication
     *                      streams for the bucket _before_ it is passed to
     *                      the other side. It is the content of the vector
     *                      which is put on the stream to the other end,
     *                      so the callback is free to inspect, modify or drop
     *                      the entire packet.
     */
    Bucket(const Cluster& cluster,
           std::string name,
           size_t vbuckets,
           size_t replicas,
           DcpPacketFilter packet_filter);

    virtual ~Bucket();

    std::string getName() const {
        return name;
    }

    std::string getUuid() const {
        return uuid;
    }

    /**
     * Get the VBucket map for the bucket
     *
     * The vbucket maps is a two dimentional array looking like:
     *
     *     vb  |   A   |   R   |   R   |   R   |
     *      #  | node# | node# | node# | node# |
     *
     * The node# is the index into the clusters array of nodes.
     *
     */
    const std::vector<std::vector<int>>& getVbucketMap() const {
        return vbucketmap;
    }

    /**
     * Get a connection to the node which is responsible for the specified
     * vbucket (and type). Note that the connection needs authentication
     * and select bucket before it may be used.
     *
     * @param vbucket The interesting vbucket
     * @param state The state of the vbucket (active or replica)
     * @param replica_number If state == replica, the replica number to look up
     * @return a connection to the node responsible for the requested vbucket
     * @throws std::invalid_argument if any of the provided arguments is
     *                               invalid (unknown vbucket, invalid vbucket
     *                               state (not active or replica) or an
     *                               invalid replica number).
     * @throws std::system_error if an error occurs on the socket
     * @throws ConnectionError if an error occurs while trying to apply features
     */
    std::unique_ptr<MemcachedConnection> getConnection(
            Vbid vbucket,
            vbucket_state_t state = vbucket_state_active,
            size_t replica_number = 0);

    /**
     * Create all of the replication streams
     */
    void setupReplication();

    /**
     * Create specific replication streams as given by specifics
     *
     * @param specifics Vector of connections to set up only the desired
     *                  connections between the given nodes and with the given
     *                  features.
     */
    void setupReplication(const std::vector<ReplicationConfig>& specifics);

    void shutdownReplication();

    /// Set the connection manifest for the bucket (creates / deletes
    /// scopes and collections.
    void setCollectionManifest(nlohmann::json next);

    /// Get the collection manifest currently being used
    nlohmann::json getCollectionManifest() const {
        return manifest;
    }

protected:
    const Cluster& cluster;
    const std::string name;
    const std::string uuid;

    std::vector<std::vector<int>> vbucketmap;
    std::unique_ptr<DcpReplicator> replicators;
    DcpPacketFilter packet_filter;

    nlohmann::json manifest;
};

} // namespace cb::test
