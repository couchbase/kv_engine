/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

std::unique_ptr<cb::test::Cluster> cb::test::ClusterTest::cluster;

std::ostream& cb::test::operator<<(std::ostream& os,
                                   const cb::test::MemStats& stats) {
    return os << "{current: " << stats.current / 1024.0 / 1024.0
              << " MiB, lower: " << stats.lower / 1024.0 / 1024.0
              << " MiB, upper: " << stats.upper / 1024.0 / 1024.0 << " MiB}";
}

void cb::test::ClusterTest::StartCluster() {
    MemcachedConnection::setLookupUserPasswordFunction(
            [](const std::string& user) -> std::string {
                if (user == "@admin") {
                    return "password";
                }

                if (cluster) {
                    auto ue =
                            cluster->getAuthProviderService().lookupUser(user);
                    if (ue) {
                        return ue->password;
                    }
                }

                return {};
            });

    cluster = Cluster::create(4);
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void cb::test::ClusterTest::SetUpTestCase() {
    if (!cluster) {
        std::cerr << "Cluster not running" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    try {
        createDefaultBucket();
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void cb::test::ClusterTest::TearDownTestCase() {
    if (cluster->getBucket("default")) {
        cluster->deleteBucket("default");
    }
}

void cb::test::ClusterTest::ShutdownCluster() {
    cluster.reset();
}

void cb::test::ClusterTest::createDefaultBucket() {
    auto bucket = cluster->createBucket("default",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    if (!bucket) {
        throw std::runtime_error("Failed to create default bucket");
    }

    cluster->collections = {};
    // Add fruit, vegetable to default scope
    // Add a second scope and collection
    cluster->collections.add(CollectionEntry::fruit)
            .add(CollectionEntry::vegetable);
    cluster->collections.add(ScopeEntry::customer);
    cluster->collections.add(CollectionEntry::customer1, ScopeEntry::customer);
    cluster->collections.add(ScopeEntry::maxScope);
    cluster->collections.add(CollectionEntry::maxCollection,
                             ScopeEntry::maxScope);
    bucket->setCollectionManifest(cluster->collections.getJson());
}

void cb::test::ClusterTest::SetUp() {
    Test::SetUp();
}

void cb::test::ClusterTest::TearDown() {
    Test::TearDown();
}

void cb::test::ClusterTest::getReplica(MemcachedConnection& conn,
                                       const Vbid vbid,
                                       const std::string& key) {
    BinprotResponse rsp;
    do {
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetReplica);
        cmd.setVBucket(vbid);
        cmd.setKey(key);

        rsp = conn.execute(cmd);
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess())
            << rsp.getStatus() << ": " << rsp.getDataView();
}

cb::test::MemStats cb::test::ClusterTest::getMemStats(
        MemcachedConnection& conn) {
    const auto stats = conn.stats("memory");
    return MemStats{
            stats["mem_used"].get<size_t>(),
            stats["ep_mem_low_wat"].get<size_t>(),
            stats["ep_mem_high_wat"].get<size_t>(),
    };
}

void cb::test::ClusterTest::setFlushParam(MemcachedConnection& conn,
                                          const std::string& paramName,
                                          const std::string& paramValue) {
    const auto cmd = BinprotSetParamCommand(
            cb::mcbp::request::SetParamPayload::Type::Flush,
            paramName,
            paramValue);
    const auto resp = conn.execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus())
            << resp.getDataView();
}

void cb::test::ClusterTest::setMemWatermarks(MemcachedConnection& conn,
                                             size_t memLowWat,
                                             size_t memHighWat) {
    setFlushParam(conn, "mem_low_wat", std::to_string(memLowWat));
    setFlushParam(conn, "mem_high_wat", std::to_string(memHighWat));
    auto stats = getMemStats(conn);
    EXPECT_EQ(stats.lower, memLowWat);
    EXPECT_EQ(stats.upper, memHighWat);
}
