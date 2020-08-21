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

#ifdef WIN32
#include <winsock2.h>

#include <iphlpapi.h>
#pragma comment(lib, "IPHLPAPI.lib")
#else
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#endif

#include <platform/dirutils.h>
#include <platform/uuid.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <array>
#include <iostream>
#include <string>
#include <system_error>
#include <vector>

namespace cb::test {

Cluster::~Cluster() = default;

class ClusterImpl : public Cluster {
public:
    ClusterImpl() = delete;
    ClusterImpl(const ClusterImpl&) = delete;
    ClusterImpl(std::vector<std::unique_ptr<Node>>& nod, std::string dir)
        : nodes(std::move(nod)),
          directory(std::move(dir)),
          authProviderService(*this),
          uuid(::to_string(cb::uuid::random())) {
        manifest = {{"rev", 0},
                    {"clusterCapabilities", nlohmann::json::object()},
                    {"clusterCapabilitiesVer", {1, 0}}};
        auto [ipv4, ipv6] = cb::test::Cluster::getIpAddresses();
        (void)ipv6; // currently not used
        const auto& hostname = ipv4.front();
        for (const auto& n : nodes) {
            n->getConnectionMap().iterate(
                    [this, &hostname](const MemcachedConnection& c) {
                        if (c.getFamily() == AF_INET) {
                            manifest["nodesExt"].emplace_back(
                                    nlohmann::json{{"services",
                                                    {{"mgmt", 6666},
                                                     {"kv", c.getPort()},
                                                     {"capi", 6666}}},
                                                   {"hostname", hostname}});
                        }
                    });
        }

        // And finally store the CCCP map on the nodes:
        const auto globalmap = manifest.dump(2);
        for (const auto& n : nodes) {
            auto connection = n->getConnection();
            connection->connect();
            connection->authenticate("@admin", "password", "plain");
            connection->setAgentName("cluster_testapp");
            connection->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                                     cb::mcbp::Feature::XATTR,
                                     cb::mcbp::Feature::XERROR,
                                     cb::mcbp::Feature::SELECT_BUCKET,
                                     cb::mcbp::Feature::JSON,
                                     cb::mcbp::Feature::SNAPPY});
            auto rsp = connection->execute(
                    BinprotSetClusterConfigCommand{0, globalmap, 0, ""});
            if (!rsp.isSuccess()) {
                std::cerr << "Failed to set global CCCP version: "
                          << rsp.getDataString() << std::endl;
            }
        }
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

    nlohmann::json to_json() const override;

    void iterateNodes(std::function<void(const Node&)> visitor) const override;

    nlohmann::json getGlobalClusterMap() override {
        return manifest;
    }

protected:
    std::vector<std::unique_ptr<Node>> nodes;
    std::vector<std::shared_ptr<Bucket>> buckets;
    const std::string directory;
    AuthProviderService authProviderService;
    const std::string uuid;
    nlohmann::json manifest;
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
            connection->setAgentName("cluster_testapp");
            connection->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                                     cb::mcbp::Feature::XATTR,
                                     cb::mcbp::Feature::XERROR,
                                     cb::mcbp::Feature::SELECT_BUCKET,
                                     cb::mcbp::Feature::JSON,
                                     cb::mcbp::Feature::SNAPPY});
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
            auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::EnableTraffic});

            if (!rsp.isSuccess()) {
                throw ConnectionError("Failed to enable traffic",
                                      rsp.getStatus());
            }

            rsp = connection->execute(BinprotSetClusterConfigCommand{
                    0, bucket->getManifest().dump(2), 1, name});
            if (!rsp.isSuccess()) {
                throw ConnectionError("Failed to push CCCP", rsp.getStatus());
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
        std::string bucketdir = cb::io::sanitizePath(n->directory + "/" + name);
        cb::io::rmrf(bucketdir);
    }
}

size_t ClusterImpl::size() const {
    return nodes.size();
}

std::pair<bool, std::string> rmrf_nothrow(const std::string& path) {
    try {
        cb::io::rmrf(path);
    } catch (const std::exception& e) {
        return {false, std::string{e.what()}};
    }
    return {true, {}};
}

ClusterImpl::~ClusterImpl() {
    buckets.clear();
    bool cleanup = true;

    for (auto& n : nodes) {
        const auto minidump_dir = cb::io::sanitizePath(n->directory + "/crash");
        if (cb::io::isDirectory(minidump_dir)) {
            auto files = cb::io::findFilesWithPrefix(minidump_dir, "");
            cleanup &= files.empty();
        }
    }

    nodes.clear();
    // @todo I should make this configurable?
    if (cleanup && cb::io::isDirectory(directory)) {
        // I'm seeing a lot of failures on windows where file removal returns
        // EINVAL (when using ::remove) and EIO (when using DeleteFile).
        // From the looks of it EIO could be that someone else is holding
        // the file open.
        // As part of testing this patch it would typically fail every
        // now and then, and when it failed it would typically succeed within
        // 1-4 times of retrying.. Allow a few more to avoid failures if the
        // server is loaded and slow...
        for (int retry = 0; retry < 100; ++retry) {
            auto [success, msg] = rmrf_nothrow(directory);
            if (success) {
                break;
            }
            std::cerr << "WARNING: Failed to delete directory. " << msg
                      << std::endl
                      << "         Sleep 20ms before retry" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
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
    for (const auto& n : nodes) {
        visitor(*n);
    }
}

std::unique_ptr<Cluster> Cluster::create(size_t num_nodes,
                                         std::optional<std::string> directory) {
    if (!directory) {
        const auto pattern =
                cb::io::sanitizePath(cb::io::getcwd() + "/cluster_");
        directory = cb::io::mkdtemp(pattern);
    }

    std::vector<std::unique_ptr<Node>> nodes;
    for (size_t n = 0; n < num_nodes; ++n) {
        const std::string id = "n_" + std::to_string(n);
        const auto nodedir = cb::io::sanitizePath(*directory + "/" + id);
        cb::io::mkdirp(nodedir);
        nodes.emplace_back(Node::create(nodedir, id));
    }

    return std::make_unique<ClusterImpl>(nodes, *directory);
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

std::pair<std::vector<std::string>, std::vector<std::string>>
Cluster::getIpAddresses() {
    std::array<char, 1024> buffer;
    std::pair<std::vector<std::string>, std::vector<std::string>> ret;

#ifdef WIN32
    std::vector<char> blob(1024 * 1024);
    auto* addresses = reinterpret_cast<IP_ADAPTER_ADDRESSES*>(blob.data());
    ULONG dataSize = blob.size();
    auto rw = GetAdaptersAddresses(
            AF_UNSPEC,
            GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST |
                    GAA_FLAG_SKIP_DNS_SERVER | GAA_FLAG_SKIP_FRIENDLY_NAME,
            nullptr,
            addresses,
            &dataSize);
    if (rw != ERROR_SUCCESS) {
        throw std::system_error(
                rw, std::system_category(), "GetAdaptersAddresses()");
    }
    for (auto* iff = addresses; iff != nullptr; iff = iff->Next) {
        for (auto* addr = iff->FirstUnicastAddress; addr != nullptr;
             addr = addr->Next) {
            auto family = addr->Address.lpSockaddr->sa_family;
            if (family == AF_INET) {
                inet_ntop(AF_INET,
                          &reinterpret_cast<sockaddr_in*>(
                                   addr->Address.lpSockaddr)
                                   ->sin_addr,
                          buffer.data(),
                          buffer.size());
                std::string address(buffer.data());
                if (address != "127.0.0.1") {
                    // Ignore localhost address
                    ret.first.emplace_back(std::move(address));
                }
            } else if (family == AF_INET6) {
                inet_ntop(AF_INET6,
                          &reinterpret_cast<sockaddr_in6*>(
                                   addr->Address.lpSockaddr)
                                   ->sin6_addr,
                          buffer.data(),
                          buffer.size());
                std::string address(buffer.data());
                if (address != "::1") {
                    // Ignore localhost address
                    ret.second.emplace_back(std::move(address));
                }
            } else {
                // Skip all other types of addresses
                continue;
            }
        }
    }
#else
    ifaddrs* interfaces;
    if (getifaddrs(&interfaces) != 0) {
        throw std::system_error(errno, std::system_category(), "getifaddrs()");
    }

    if (interfaces == nullptr) {
        return {};
    }

    // Iterate over all the interfaces and pick out the IPv4 and IPv6 addresses
    for (auto* ifa = interfaces; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == AF_INET) {
            inet_ntop(AF_INET,
                      &reinterpret_cast<sockaddr_in*>(ifa->ifa_addr)->sin_addr,
                      buffer.data(),
                      buffer.size());
            std::string address(buffer.data());
            if (address != "127.0.0.1") {
                // Ignore localhost address
                ret.first.emplace_back(std::move(address));
            }
        } else if (ifa->ifa_addr->sa_family == AF_INET6) {
            inet_ntop(
                    AF_INET6,
                    &reinterpret_cast<sockaddr_in6*>(ifa->ifa_addr)->sin6_addr,
                    buffer.data(),
                    buffer.size());
            std::string address(buffer.data());
            if (address != "::1") {
                // Ignore localhost address
                ret.second.emplace_back(std::move(address));
            }
        }
    }

    freeifaddrs(interfaces);
#endif

    return ret;
}

} // namespace cb::test
