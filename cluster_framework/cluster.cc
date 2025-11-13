/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cluster.h"
#include "auth_provider_service.h"
#include "bucket.h"
#include "node.h"

#include <boost/filesystem/operations.hpp>
#include <cbcrypto/key_store.h>
#include <folly/Synchronized.h>
#include <platform/file_sink.h>
#include <platform/uuid.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace cb::test {

std::string format_as(BucketPersistenceBackend backend) {
    switch (backend) {
    case BucketPersistenceBackend::Couchstore:
        return "couchdb";
    case BucketPersistenceBackend::Magma:
        return "magma";
    }
    Expects(false && "Unexpected backend");
}

void to_json(nlohmann::json& j, const BucketPersistenceBackend backend) {
    j = format_as(backend);
}

Cluster::~Cluster() = default;

class ClusterImpl : public Cluster {
public:
    ClusterImpl() = delete;
    ClusterImpl(const ClusterImpl&) = delete;
    ClusterImpl(std::vector<std::unique_ptr<Node>>& nod,
                std::filesystem::path dir);
    ~ClusterImpl() override;
    void setBucketPersistenceBackend(
            BucketPersistenceBackend backend_) override {
        backend = backend_;
    }
    BucketPersistenceBackend getBucketPersistenceBackend() const override {
        return backend;
    }
    [[nodiscard]] std::shared_ptr<Bucket> createBucket(
            const std::string& name,
            const nlohmann::json& attributes,
            DcpPacketFilter packet_filter,
            bool setUpReplication) override;
    void deleteBucket(const std::string& name) override;
    [[nodiscard]] std::shared_ptr<Bucket> getBucket(
            const std::string& name) const override;
    [[nodiscard]] std::unique_ptr<MemcachedConnection> getConnection(
            size_t node) const override;
    [[nodiscard]] size_t size() const override;
    [[nodiscard]] AuthProviderService& getAuthProviderService() override;
    [[nodiscard]] nlohmann::json to_json() const override;
    void iterateNodes(std::function<void(const Node&)> visitor) const override;
    void changeConfig(std::function<void(nlohmann::json&)> callback) override;

protected:
    /**
     * Helper function to iterate over all the nodes in the cluster (performs
     * the same operation as iterateNodes, but that's a virtual function which
     * shouldn't be used in constructors/destructors)
     *
     * @param visitor the callback method for each node
     */
    void forAllNodes(std::function<void(const Node&)> visitor) const;

    /**
     * Helper function to create the bucket on a single node
     *
     * @param node The node to create the bucket on
     * @param bucket The bucket to create
     * @param config The config used to create the bucket
     */
    void createBucketOnNode(const Node& node,
                            Bucket& bucket,
                            std::string_view config);

    folly::Synchronized<std::vector<std::unique_ptr<Node>>> nodes;
    folly::Synchronized<std::vector<std::shared_ptr<Bucket>>> buckets;
    const std::filesystem::path directory;
    AuthProviderService authProviderService;
    const std::string uuid;
    std::atomic<int64_t> revno{1};
    static constexpr int64_t epoch = 1;
    BucketPersistenceBackend backend = BucketPersistenceBackend::Couchstore;
};

ClusterImpl::ClusterImpl(std::vector<std::unique_ptr<Node>>& nod,
                         std::filesystem::path dir)
    : nodes(std::move(nod)),
      directory(std::move(dir)),
      authProviderService(*this),
      uuid(::to_string(cb::uuid::random())) {
    auto [ipv4, ipv6] = cb::net::getIpAddresses(true);
    (void)ipv6; // currently not used
    const auto& hostname = ipv4.front();

    nlohmann::json manifest = {
            {"rev", 0},
            {"clusterCapabilities", nlohmann::json::object()},
            {"clusterCapabilitiesVer", {1, 0}}};
    // Build the CCCP map
    forAllNodes([&manifest, &hostname](const auto& node) {
        node.getConnectionMap().iterate([&manifest, &hostname](const auto& c) {
            if (c.getFamily() == AF_INET) {
                manifest["nodesExt"].emplace_back(nlohmann::json{
                        {"services",
                         {{"mgmt", 6666}, {"kv", c.getPort()}, {"capi", 6666}}},
                        {"hostname", hostname}});
            }
        });
    });

    // And finally store the CCCP map on the nodes:
    const auto globalmap = manifest.dump();
    forAllNodes([this, &globalmap](const auto& node) {
        auto connection = node.getConnection();
        connection->connect();
        connection->authenticate("@admin");
        connection->setAgentName("cluster_testapp");
        connection->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                                 cb::mcbp::Feature::XATTR,
                                 cb::mcbp::Feature::XERROR,
                                 cb::mcbp::Feature::SELECT_BUCKET,
                                 cb::mcbp::Feature::JSON,
                                 cb::mcbp::Feature::SNAPPY});
        auto rsp = connection->execute(BinprotSetClusterConfigCommand{
                globalmap, epoch, revno.load(), {}});
        if (!rsp.isSuccess()) {
            fmt::print(stderr,
                       "Failed to set global CCCP on node {}: {}",
                       node.getId(),
                       rsp.getDataView());
        }
    });
}

std::shared_ptr<Bucket> ClusterImpl::getBucket(const std::string& name) const {
    return buckets.withRLock([&name](auto& vector) {
        for (auto& bucket : vector) {
            if (bucket->getName() == name) {
                return bucket;
            }
        }
        return std::shared_ptr<Bucket>{};
    });
}

std::unique_ptr<MemcachedConnection> ClusterImpl::getConnection(
        size_t node) const {
    return nodes.withRLock([node](auto& vector) {
        if (node < vector.size()) {
            return vector[node]->getConnection();
        }
        throw std::invalid_argument(
                fmt::format("ClusterImpl::getConnection: Node {} is outside "
                            "the legal range of [0,{}]",
                            node,
                            vector.size() - 1));
    });
}

AuthProviderService& ClusterImpl::getAuthProviderService() {
    return authProviderService;
}

void ClusterImpl::createBucketOnNode(const Node& node,
                                     Bucket& bucket,
                                     std::string_view config) {
    auto connection = node.getConnection();
    connection->connect();
    connection->authenticate("@admin");
    connection->setAgentName("cluster_testapp");
    connection->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                             cb::mcbp::Feature::XATTR,
                             cb::mcbp::Feature::XERROR,
                             cb::mcbp::Feature::SELECT_BUCKET,
                             cb::mcbp::Feature::JSON,
                             cb::mcbp::Feature::SNAPPY,
                             // CC onwards, enable collections
                             cb::mcbp::Feature::Collections});

    const auto dbname = node.directory / bucket.getName();

    auto deks = dbname / "deks";
    create_directories(deks);

    cb::crypto::KeyStore keystore;
    keystore.setActiveKey(cb::crypto::DataEncryptionKey::generate());
    keystore.iterateKeys([&deks](auto key) {
        // ns_server generates (encrypted) files with the content of the
        // key (and possibly more info). While creating snapshots we
        // have logic which tries to copy all these files into the
        // snapshot so we need a file
        cb::io::FileSink file(deks / fmt::format("{}.key.0", key->getId()));
        nlohmann::json json = *key;
        file.sink(json.dump(2));
        file.close();
    });

    nlohmann::json json = keystore;
    std::string escaped;
    for (const auto& c : json.dump()) {
        if (c == '=') {
            escaped.push_back('\\');
        }
        escaped.push_back(c);
    }

    connection->createBucket(
            bucket.getName(),
            fmt::format("{}dbname={};alog_path={};encryption={}",
                        config,
                        dbname.generic_string(),
                        (dbname / "access.log").generic_string(),
                        escaped),
            BucketType::Couchbase);
    connection->selectBucket(bucket.getName());

    // iterate over all vbuckets and find the node number and define the
    // vbuckets
    auto& vbucketmap = bucket.getVbucketMap();
    for (std::size_t vbucket = 0; vbucket < vbucketmap.size(); ++vbucket) {
        std::vector<std::string> chain;
        for (int ii : vbucketmap[vbucket]) {
            chain.emplace_back("n_" + std::to_string(ii));
        }
        nlohmann::json topology = {
                {"topology", nlohmann::json::array({chain})}};
        for (std::size_t ii = 0; ii < vbucketmap[vbucket].size(); ++ii) {
            if (vbucketmap[vbucket][ii] ==
                std::stoi(std::string(node.getId().substr(2)))) {
                // This is me
                if (ii == 0) {
                    connection->setVbucket(Vbid{uint16_t(vbucket)},
                                           vbucket_state_active,
                                           topology);
                } else {
                    connection->setVbucket(
                            Vbid{uint16_t(vbucket)}, vbucket_state_replica, {});
                }
            }
        }
    }

    // Call enable traffic
    auto rsp = connection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::EnableTraffic});

    if (!rsp.isSuccess()) {
        throw ConnectionError("Failed to enable traffic", rsp.getStatus());
    }

    rsp = connection->execute(
            BinprotSetClusterConfigCommand{bucket.getManifest().dump(2),
                                           epoch,
                                           revno.load(),
                                           bucket.getName()});
    if (!rsp.isSuccess()) {
        throw ConnectionError(
                fmt::format("Failed to push CCCP to {}", node.getId()),
                rsp.getStatus());
    }
}

std::shared_ptr<Bucket> ClusterImpl::createBucket(
        const std::string& name,
        const nlohmann::json& attributes,
        DcpPacketFilter packet_filter,
        bool setUpReplication) {
    size_t vbuckets = 1024;
    size_t replicas = std::min<size_t>(size() - 1, 3);

    nlohmann::json json = {{"max_size", 67108864},
                           {"backend", backend},
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
                           {"max_num_shards", 4},
                           // Keep the flow control buffer size reasonably low.
                           // That increases the likelihood of consumers sending
                           // buffer-ack messages and thus improves our ability
                           // to catch issues with mismatched acking.
                           // To give an idea with the current settings,
                           // 67108864 * 0.01 ~ 671088 bytes with 1 consumer.
                           // Then equally split across any other consumer.
                           {"dcp_consumer_buffer_ratio", "0.01"}};

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
        if (replicas > size() - 1) {
            throw std::invalid_argument(
                    fmt::format("ClusterImpl::createBucket: Not enough nodes "
                                "in the cluster for {} replicas",
                                replicas));
        }
    }

    revno++;
    auto bucket = std::make_shared<Bucket>(
            *this, name, vbuckets, replicas, packet_filter);
    json["uuid"] = bucket->getUuid();
    std::string config;
    for (auto it = json.begin(); it != json.end(); ++it) {
        if (it.value().is_string()) {
            config += it.key() + "=" + it.value().get<std::string>() + ";";
        } else {
            config += it.key() + "=" + it.value().dump() + ";";
        }
    }
    try {
        forAllNodes([this, &bucket, &config](const auto& node) {
            createBucketOnNode(node, *bucket, config);
        });

        if (setUpReplication) {
            bucket->setupReplication();
        }
        buckets.wlock()->push_back(bucket);
        return bucket;
    } catch (const ConnectionError& e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        forAllNodes([&name](const auto& node) {
            auto connection = node.getConnection();
            connection->connect();
            connection->authenticate("@admin");
            try {
                connection->deleteBucket(name);
            } catch (const std::exception&) {
            }
        });
    }

    return {};
}

void ClusterImpl::deleteBucket(const std::string& name) {
    // The DCP replicators throws an exception if they get a
    // read error (in the case of others holding a reference
    // to the bucket class)... Start by locating the bucket
    // and shut down replication for the bucket and then remove
    // it from our list of known buckets.
    buckets.withWLock([&name](auto& vector) {
        for (auto iter = vector.begin(); iter != vector.end(); ++iter) {
            if ((*iter)->getName() == name) {
                (*iter)->shutdownReplication();
                vector.erase(iter);
                break;
            }
        }
    });

    // Iterate over all the nodes and delete the bucket.
    // Note that failing to delete the bucket on one node would cause
    // an exception to be thrown and leave the bucket alive on the
    // remaining nodes. We're going to ignore that problem for now as this
    // is a testing framework and in that case the test would most likely
    // fail...
    forAllNodes([name](const auto& node) {
        auto connection = node.getConnection();
        connection->connect();
        connection->authenticate("@admin");
        connection->deleteBucket(name);
        // And nuke the files for the database on that node.
        removeWithRetry(node.directory / name);
    });
}

size_t ClusterImpl::size() const {
    return nodes.rlock()->size();
}

ClusterImpl::~ClusterImpl() {
    // Delete all buckets (tears down replication)
    buckets.wlock()->clear();
    bool cleanup = true;

    forAllNodes([&cleanup](const auto& node) {
        const auto minidump_dir = node.directory / "crash";
        for (const auto& p :
             std::filesystem::directory_iterator(minidump_dir)) {
            if (is_regular_file(p)) {
                cleanup = false;
                break;
            }
        }
    });

    // clear the nodes array (causing the destructor of the nodes
    // to be called and shut down the memcached processes.)
    nodes.wlock()->clear();
    // @todo I should make this configurable?
    if (cleanup && exists(directory) && is_directory(directory)) {
        // I'm seeing a lot of failures on windows where file removal returns
        // EINVAL (when using ::remove) and EIO (when using DeleteFile).
        // From the looks of it EIO could be that someone else is holding
        // the file open.
        // As part of testing this patch it would typically fail every
        // now and then, and when it failed it would typically succeed within
        // 1-4 times of retrying.. Allow a few more to avoid failures if the
        // server is loaded and slow...
        removeWithRetry(directory);
    }
}

nlohmann::json ClusterImpl::to_json() const {
    auto ret = getUninitializedJson();
    ret["uuid"] = uuid;
    nlohmann::json pool;
    pool["name"] = "default";
    pool["uri"] = "/pools/default?uuid=" + uuid;
    pool["streamingUri"] = "/poolsStreaming/default?uuid=" + uuid;
    ret["pools"].emplace_back(std::move(pool));
    return ret;
}

void ClusterImpl::iterateNodes(std::function<void(const Node&)> visitor) const {
    forAllNodes(std::move(visitor));
}

void ClusterImpl::forAllNodes(std::function<void(const Node&)> visitor) const {
    nodes.withRLock([&visitor](auto& vector) {
        for (const auto& node : vector) {
            visitor(*node);
        }
    });
}

void ClusterImpl::changeConfig(std::function<void(nlohmann::json&)> callback) {
    nodes.withRLock([&callback](auto& vector) {
        for (auto& node : vector) {
            callback(node->getConfig());
            node->writeConfig();
            auto connection = node->getConnection();
            connection->connect();
            connection->authenticate("@admin");
            connection->reloadConfig();
        }
    });
}

static std::filesystem::path createTemporaryDirectory() {
    const auto cwd = absolute(std::filesystem::path{"."}).parent_path();
    for (;;) {
        auto candidate =
                cwd /
                boost::filesystem::unique_path("cluster_test_%%%%%%").string();
        if (create_directories(candidate)) {
            return candidate;
        }
    }
}

std::unique_ptr<Cluster> Cluster::create(
        size_t num_nodes,
        std::optional<std::string> directory,
        const std::function<void(std::string_view, nlohmann::json&)>&
                configCallback) {
    const std::filesystem::path root =
            directory.has_value() ? *directory
                                  : createTemporaryDirectory().string();
    std::vector<std::unique_ptr<Node>> nodes;
    for (size_t n = 0; n < num_nodes; ++n) {
        const std::string id = "n_" + std::to_string(n);
        auto nodedir = root / id;
        create_directories(nodedir);
        nodes.emplace_back(Node::create(nodedir, id, configCallback));
    }

    return std::make_unique<ClusterImpl>(nodes, root);
}

nlohmann::json Cluster::getUninitializedJson() {
    nlohmann::json ret;
    ret["isEnterprise"] = true;
    ret["allowedServices"].push_back("kv");
    ret["isIPv6"] = false;
    ret["isDeveloperPreview"] = true;
    ret["uuid"] = "";
    ret["pools"] = nlohmann::json::array();
    ret["implementationVersion"] = "7.0.0-0000-enterprise";
    return ret;
}

void Cluster::removeWithRetry(
        const std::filesystem::path& path,
        std::function<bool(const std::exception&)> errorcallback) {
    int retry = 100;
    if (!errorcallback) {
        errorcallback = [&retry](const std::exception& e) {
            std::cerr << "WARNING: Failed to delete directory: " << e.what()
                      << std::endl
                      << "         Sleep 20ms before retry" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return --retry > 0;
        };
    }

    do {
        try {
            std::filesystem::remove_all(path);
            return;
        } catch (const std::exception& e) {
            if (!errorcallback(e)) {
                return;
            }
        }
    } while (true);
}

} // namespace cb::test
