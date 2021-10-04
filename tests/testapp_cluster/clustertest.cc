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

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

std::unique_ptr<cb::test::Cluster> cb::test::ClusterTest::cluster;

void cb::test::ClusterTest::SetUpTestCase() {
    cluster = Cluster::create(4);
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
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
    cluster.reset();
}

void cb::test::ClusterTest::createDefaultBucket() {
    auto bucket = cluster->createBucket("default",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    if (!bucket) {
        throw std::runtime_error("Failed to create default bucket");
    }

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
                                       Vbid vbid,
                                       const std::string& key) {
    BinprotResponse rsp;
    do {
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetReplica);
        cmd.setVBucket(Vbid(0));
        cmd.setKey(key);

        rsp = conn.execute(cmd);
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess());
}
