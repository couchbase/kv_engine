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

#include "cluster.h"
#include "auth_provider_service.h"
#include "bucket.h"
#include "node.h"

#include <platform/dirutils.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>
#include <vector>

namespace cb {
namespace test {

Cluster::~Cluster() = default;

class ClusterImpl : public Cluster {
public:
    ClusterImpl() = delete;
    ClusterImpl(const ClusterImpl&) = delete;
    ClusterImpl(std::vector<std::unique_ptr<Node>>& nodes, std::string dir)
        : nodes(std::move(nodes)),
          directory(std::move(dir)),
          authProviderService(*this) {
    }

    ~ClusterImpl() override;

    std::shared_ptr<Bucket> createBucket(const std::string& name,
                                         const nlohmann::json& attributes,
                                         DcpPacketFilter packet_filter,
                                         bool setUpReplication) override;

    void deleteBucket(const std::string& name) override;

    std::shared_ptr<Bucket> getBucket(const std::string& name) const override {
        for (auto& bucket : buckets) {
            if (bucket->getName() == name) {
                return bucket;
            }
        }

        return std::shared_ptr<Bucket>();
    }

    std::unique_ptr<MemcachedConnection> getConnection(
            size_t node) const override {
        if (node < nodes.size()) {
            return nodes[node]->getConnection();
        }
        throw std::invalid_argument(
                "ClusterImpl::getConnection: Invalid node number");
    }

    size_t size() const override;

    AuthProviderService& getAuthProviderService() override {
        return authProviderService;
    }

protected:
    std::vector<std::unique_ptr<Node>> nodes;
    std::vector<std::shared_ptr<Bucket>> buckets;
    const std::string directory;
    AuthProviderService authProviderService;
};

std::shared_ptr<Bucket> ClusterImpl::createBucket(
        const std::string& name,
        const nlohmann::json& attributes,
        DcpPacketFilter packet_filter,
        bool setUpReplication) {
    size_t vbuckets = 1024;
    size_t replicas = std::min<size_t>(nodes.size() - 1, 3);

    nlohmann::json json = {{"max_size", 67108864},
                           {"backend", "couchdb"},
                           {"couch_bucket", name},
                           {"max_vbuckets", vbuckets},
                           {"data_traffic_enabled", false},
                           {"max_num_workers", 3},
                           {"conflict_resolution_type", "seqno"},
                           {"bucket_type", "persistent"},
                           {"item_eviction_policy", "value_only"},
                           {"max_ttl", 0},
                           {"ht_locks", 47},
                           {"compression_mode", "off"},
                           {"failpartialwarmup", false},
                           {"max_num_shards", 4}};

    json.update(attributes);

    auto iter = json.find("max_vbuckets");
    if (iter != json.end()) {
        vbuckets = iter->get<size_t>();
    }

    iter = json.find("replicas");
    if (iter != json.end()) {
        replicas = iter->get<size_t>();
        // The underlying bucket don't know about replicas
        json.erase(iter);
        if (replicas > nodes.size() - 1) {
            throw std::invalid_argument(
                    "ClusterImpl::createBucket: Not enough nodes in the "
                    "cluster for " +
                    std::to_string(replicas) + " replicas");
        }
    }

    auto bucket = std::make_shared<Bucket>(
            *this, name, vbuckets, replicas, packet_filter);
    const auto& vbucketmap = bucket->getVbucketMap();
    json["uuid"] = bucket->getUuid();

    try {
        //  @todo I need at least the number of nodes to set active + n replicas
        for (std::size_t node_idx = 0; node_idx < nodes.size(); ++node_idx) {
            auto connection = nodes[node_idx]->getConnection();
            connection->connect();
            connection->authenticate("@admin", "password", "plain");
            connection->setFeatures("cluster_testapp",
                                    {{cb::mcbp::Feature::MUTATION_SEQNO,
                                      cb::mcbp::Feature::XATTR,
                                      cb::mcbp::Feature::XERROR,
                                      cb::mcbp::Feature::SELECT_BUCKET,
                                      cb::mcbp::Feature::JSON,
                                      cb::mcbp::Feature::SNAPPY}});
            std::string fname = nodes[node_idx]->directory + "/" + name;
            std::replace(fname.begin(), fname.end(), '\\', '/');
            json["dbname"] = fname;
            fname = json["dbname"].get<std::string>() + "/access.log";
            std::replace(fname.begin(), fname.end(), '\\', '/');
            json["alog_path"] = fname;
            std::string config;
            for (auto it = json.begin(); it != json.end(); ++it) {
                if (it.value().is_string()) {
                    config += it.key() + "=" + it.value().get<std::string>() +
                              ";";
                } else {
                    config += it.key() + "=" + it.value().dump() + ";";
                }
            }
            connection->createBucket(name, config, BucketType::Couchbase);
            connection->selectBucket(name);

            // iterate over all of the vbuckets and find that node number and
            // define the vbuckets
            for (std::size_t vbucket = 0; vbucket < vbucketmap.size();
                 ++vbucket) {
                std::vector<std::string> chain;
                for (int ii : vbucketmap[vbucket]) {
                    chain.emplace_back("n_" + std::to_string(ii));
                }
                nlohmann::json topology = {
                        {"topology", nlohmann::json::array({chain})}};
                for (std::size_t ii = 0; ii < vbucketmap[vbucket].size();
                     ++ii) {
                    if (vbucketmap[vbucket][ii] == int(node_idx)) {
                        // This is me
                        if (ii == 0) {
                            connection->setVbucket(Vbid{uint16_t(vbucket)},
                                                   vbucket_state_active,
                                                   topology);
                        } else {
                            connection->setVbucket(Vbid{uint16_t(vbucket)},
                                                   vbucket_state_replica,
                                                   {});
                        }
                    }
                }
            }

            // Call enable traffic
            const auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::EnableTraffic});

            if (!rsp.isSuccess()) {
                throw ConnectionError("Failed to enable traffic",
                                      rsp.getStatus());
            }
        }

        if (setUpReplication) {
            bucket->setupReplication();
        }
        buckets.push_back(bucket);
        return bucket;
    } catch (const ConnectionError& e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        for (auto& b : nodes) {
            auto connection = b->getConnection();
            connection->connect();
            connection->authenticate("@admin", "password", "plain");
            try {
                connection->deleteBucket(name);
            } catch (const std::exception&) {
            }
        }
    }

    return {};
}

void ClusterImpl::deleteBucket(const std::string& name) {
    for (auto iter = buckets.begin(); iter != buckets.end(); ++iter) {
        if ((*iter)->getName() == name) {
            // The DCP replicators throws an exception if they get a
            // read error (in the case of others holding a reference
            // to the bucket class)...
            (*iter)->shutdownReplication();
            buckets.erase(iter);
            break;
        }
    }

    // I should wait for the bucket being closed on all nodes..
    for (auto& n : nodes) {
        auto connection = n->getConnection();
        connection->connect();
        connection->authenticate("@admin", "password", "plain");
        connection->deleteBucket(name);
        // And nuke the files for the database on that node..
        std::string bucketdir = n->directory + "/" + name;
        cb::io::sanitizePath(bucketdir);
        cb::io::rmrf(bucketdir);
    }
}

size_t ClusterImpl::size() const {
    return nodes.size();
}
ClusterImpl::~ClusterImpl() {
    buckets.clear();
    bool cleanup = true;

    for (auto& n : nodes) {
        std::string minidump_dir = n->directory + "/crash";
        cb::io::sanitizePath(minidump_dir);
        if (cb::io::isDirectory(minidump_dir)) {
            auto files = cb::io::findFilesWithPrefix(minidump_dir, "");
            cleanup &= files.empty();
        }
    }

    nodes.clear();
    // @todo I should make this configurable?
    if (cleanup && cb::io::isDirectory(directory)) {
        cb::io::rmrf(directory);
    }
}

std::unique_ptr<Cluster> Cluster::create(size_t num_nodes) {
    auto pattern = cb::io::getcwd() + "/cluster_";
    cb::io::sanitizePath(pattern);
    auto clusterdir = cb::io::mkdtemp(pattern);

    std::vector<std::unique_ptr<Node>> nodes;
    for (size_t n = 0; n < num_nodes; ++n) {
        const std::string id = "n_" + std::to_string(n);
        std::string nodedir = clusterdir + "/" + id;
        cb::io::sanitizePath(nodedir);
        cb::io::mkdirp(nodedir);
        nodes.emplace_back(Node::create(nodedir, id));
    }

    return std::make_unique<ClusterImpl>(nodes, clusterdir);
}

} // namespace test
} // namespace cb
