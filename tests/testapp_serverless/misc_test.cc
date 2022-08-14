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
#include <serverless/config.h>
#include <deque>

namespace cb::test {

/// Verify that the detailed stat requests provide all the fields we expect
TEST(MiscTest, TestBucketDetailedStats) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");

    nlohmann::json bucket;
    admin->stats(
            [&bucket](const auto& k, const auto& v) {
                bucket = nlohmann::json::parse(v);
            },
            "bucket_details bucket-0");
    EXPECT_EQ(13, bucket.size());
    EXPECT_NE(bucket.end(), bucket.find("state"));
    EXPECT_NE(bucket.end(), bucket.find("clients"));
    EXPECT_NE(bucket.end(), bucket.find("name"));
    EXPECT_NE(bucket.end(), bucket.find("type"));
    EXPECT_NE(bucket.end(), bucket.find("ru"));
    EXPECT_NE(bucket.end(), bucket.find("wu"));
    EXPECT_NE(bucket.end(), bucket.find("num_throttled"));
    EXPECT_NE(bucket.end(), bucket.find("throttle_limit"));
    EXPECT_NE(bucket.end(), bucket.find("throttle_wait_time"));
    EXPECT_NE(bucket.end(), bucket.find("num_commands"));
    EXPECT_NE(bucket.end(), bucket.find("num_commands_with_metered_units"));
    EXPECT_NE(bucket.end(), bucket.find("num_metered_dcp_messages"));
    EXPECT_NE(bucket.end(), bucket.find("num_rejected"));
}

/// Verify that when a bucket gets created the throttle limit gets set to
/// the default value
TEST(MiscTest, TestDefaultThrottleLimit) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto bucket = cluster->createBucket("TestDefaultThrottleLimit",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    if (!bucket) {
        throw std::runtime_error(
                "Failed to create bucket: TestDefaultThrottleLimit");
    }
    std::size_t limit;
    admin->stats(
            [&limit](const auto& k, const auto& v) {
                nlohmann::json json = nlohmann::json::parse(v);
                limit = json["throttle_limit"].get<size_t>();
            },
            "bucket_details TestDefaultThrottleLimit");
    cluster->deleteBucket("TestDefaultThrottleLimit");
    EXPECT_EQ(cb::serverless::DefaultThrottleLimit, limit);
}

/// Verify that the user can't create too many bucket connections (and
/// that system-internal connections may continue to connect to a bucket)
TEST(MiscTest, MaxConnectionPerBucket) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto getNumClients = [&admin]() -> std::size_t {
        size_t num_clients = 0;
        admin->stats(
                [&num_clients](const auto& k, const auto& v) {
                    nlohmann::json json = nlohmann::json::parse(v);
                    num_clients = json["clients"].get<size_t>();
                },
                "bucket_details bucket-0");
        return num_clients;
    };

    std::deque<std::unique_ptr<MemcachedConnection>> connections;
    bool done = false;
    BinprotResponse rsp;
    do {
        auto conn = cluster->getConnection(0);
        conn->authenticate("bucket-0", "bucket-0");
        rsp = conn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
        if (rsp.isSuccess()) {
            connections.emplace_back(std::move(conn));
            ASSERT_LE(getNumClients(), MaxConnectionsPerBucket);
        } else {
            ASSERT_EQ(cb::mcbp::Status::RateLimitedMaxConnections,
                      rsp.getStatus());
            // Without XERROR E2BIG should be returned
            conn->setXerrorSupport(false);
            rsp = conn->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
            ASSERT_FALSE(rsp.isSuccess());
            ASSERT_EQ(cb::mcbp::Status::E2big, rsp.getStatus());
            done = true;
        }
    } while (!done);

    // But we should be allowed to connect internal users
    for (int ii = 0; ii < 5; ++ii) {
        auto conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("bucket-0");
        connections.emplace_back(std::move(conn));
    }
    EXPECT_EQ(MaxConnectionsPerBucket + 4, getNumClients());
}

/// Verify that we may set the bucket in a state where the client can no
/// longer set more data in a bucket
TEST(MiscTest, StopClientDataIngress) {
    auto writeDoc = [](MemcachedConnection& conn) {
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    };

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("bucket-0");

    auto bucket0 = admin->clone();
    bucket0->authenticate("bucket-0", "bucket-0");
    bucket0->selectBucket("bucket-0");

    // store a document
    writeDoc(*bucket0);

    // Disable client ingress
    auto rsp =
            admin->execute(SetBucketDataLimitExceededCommand{"bucket-0", true});
    EXPECT_TRUE(rsp.isSuccess());

    // fail to store a document
    try {
        writeDoc(*bucket0);
        FAIL() << "Should not be able to store a document";
    } catch (ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::BucketSizeLimitExceeded, error.getReason());
    }
    // Succeeds to store a document in bucket-1
    auto bucket1 = admin->clone();
    bucket1->authenticate("bucket-1", "bucket-1");
    bucket1->selectBucket("bucket-1");
    writeDoc(*bucket1);

    // enable client ingress
    rsp = admin->execute(SetBucketDataLimitExceededCommand{"bucket-0", false});
    EXPECT_TRUE(rsp.isSuccess());

    // succeed to store a document
    writeDoc(*bucket0);
}

/// Verify that the memcached buckets is not supported in serverless
/// configuration.
TEST(MiscTest, MemcachedBucketNotSupported) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto rsp = admin->execute(BinprotCreateBucketCommand{
            "NotSupported", "default_engine.so", ""});
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

} // namespace cb::test
