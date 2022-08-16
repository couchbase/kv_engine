/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/portability/GTest.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <thread>
#include <vector>

namespace cb::test {

void setClusterConfig(cb::test::Cluster& cluster,
                      uint64_t token,
                      const std::string& bucketName,
                      const std::string& config,
                      int64_t revision) {
    for (std::size_t ii = 0; ii < cluster.size(); ++ii) {
        auto admin = cluster.getConnection(ii);
        admin->authenticate("@admin", "password");

        // Verify that the bucket isn't there
        auto rsp = admin->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, bucketName});
        ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
                << "The bucket should not exist";

        rsp = admin->execute(BinprotSetClusterConfigCommand{
                token, config, 1, revision, bucketName});
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << std::endl
                                     << rsp.getDataJson();

        // Verify that the bucket is there
        rsp = admin->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, bucketName});
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    }
}

TEST(ConfigOnlyTest, SetClusterConfigCreatesBucket) {
    const std::string dummy_clustermap = R"({"rev":1000})";
    setClusterConfig(*cluster, 0, "cluster-config", dummy_clustermap, 1000);

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    // MB-53379: Clients with Collections enabled can't select a "config-only"
    // bucket. Enable collections to verify that it works
    admin->setFeature(cb::mcbp::Feature::Collections, true);
    admin->selectBucket("cluster-config");

    admin->setXerrorSupport(false);
    auto rsp =
            admin->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Stat});
    ASSERT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
    admin->setXerrorSupport(true);
    rsp = admin->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Stat});
    ASSERT_EQ(cb::mcbp::Status::EConfigOnly, rsp.getStatus());

    // But I should be able to read the cluster config
    rsp = admin->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_EQ(dummy_clustermap, rsp.getDataString());

    // And it should be possible to upgrade the dummy bucket to a real bucket
    auto bucket = cluster->createBucket("cluster-config",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    ASSERT_TRUE(bucket) << "Failed to create the bucket";

    // And my connection shouldn't have been removed
    nlohmann::json json;
    admin->stats([&json](auto, auto v) { json = nlohmann::json::parse(v); },
                 "connections self");
    EXPECT_FALSE(json.empty());

    // The cluster config should have been changed as part of creating
    // the bucket
    rsp = admin->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_NE(dummy_clustermap, rsp.getDataString());

    // Delete the bucket
    cluster->deleteBucket("cluster-config");
}

/// Verify that we can delete a "config-only" bucket
TEST(ConfigOnlyTest, DeleteClusterConfigBucket) {
    const std::string bucketname = "cluster-config";
    const std::string dummy_clustermap = R"({"rev":1000})";
    setClusterConfig(*cluster, 0, bucketname, dummy_clustermap, 1000);

    auto conn = cluster->getConnection(0);
    conn->authenticate("@admin", "password");
    // Select bucket will fail if the wasn't successfully created
    // and we want to have a connection in the bucket to verify that
    // we can still delete the bucket even if we've got connected clients
    conn->selectBucket(bucketname);

    // DeleteBucket is not of the legal commands to execute in such a bucket
    conn->setAutoRetryTmpfail(false);
    auto rsp = conn->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteBucket, bucketname});
    ASSERT_EQ(cb::mcbp::Status::EConfigOnly, rsp.getStatus());

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    // Delete will throw exception if it fails for some reason
    admin->deleteBucket(bucketname);
}

} // namespace cb::test
