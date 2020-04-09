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

#include "bucket.h"
#include "cluster.h"
#include "dcp_replicator.h"

#include <platform/uuid.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>
#include <utility>

namespace cb::test {

Bucket::Bucket(const Cluster& cluster,
               std::string name,
               size_t vbuckets,
               size_t replicas,
               DcpPacketFilter packet_filter)
    : cluster(cluster),
      name(std::move(name)),
      uuid(::to_string(cb::uuid::random())),
      packet_filter(std::move(packet_filter)) {
    auto nodes = cluster.size();
    vbucketmap.resize(vbuckets);
    int ii = 0;
    for (size_t vb = 0; vb < vbuckets; ++vb) {
        vbucketmap[vb].resize(replicas + 1);
        for (size_t n = 0; n < (replicas + 1); ++n) {
            vbucketmap[vb][n] = ii++ % nodes;
        }
    }
}

Bucket::~Bucket() = default;

void Bucket::setupReplication() {
    setupReplication({});
}

void Bucket::setupReplication(const std::vector<ReplicationConfig>& specifics) {
    replicators =
            DcpReplicator::create(cluster, *this, packet_filter, specifics);
}

void Bucket::shutdownReplication() {
    replicators.reset();
}

std::unique_ptr<MemcachedConnection> Bucket::getConnection(
        Vbid vbucket, vbucket_state_t state, size_t replica_number) {
    if (vbucket.get() > vbucketmap.size()) {
        throw std::invalid_argument("Bucket::getConnection: Invalid vbucket");
    }

    if (state == vbucket_state_active) {
        return cluster.getConnection(vbucketmap[vbucket.get()][0]);
    }

    if (state != vbucket_state_replica) {
        throw std::invalid_argument(
                "Bucket::getConnection: Unsupported vbucket state");
    }

    if ((replica_number + 1) > vbucketmap[0].size()) {
        throw std::invalid_argument(
                "Bucket::getConnection: Invalid replica number");
    }

    return cluster.getConnection(vbucketmap[vbucket.get()][replica_number + 1]);
}

void Bucket::setCollectionManifest(nlohmann::json next) {
    const auto payload = next.dump(2);
    for (size_t idx = 0; idx < cluster.size(); ++idx) {
        auto conn = cluster.getConnection(idx);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(name);
        auto rsp = conn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::CollectionsSetManifest, {}, payload});
        if (!rsp.isSuccess()) {
            throw ConnectionError(
                    "Bucket::setCollectionManifest: Failed to set Collection "
                    "manifest on n_" +
                            std::to_string(idx),
                    rsp);
        }
    }

    manifest = std::move(next);
}

} // namespace cb::test