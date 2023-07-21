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
#include <cluster_framework/node.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <platform/split_string.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <serverless/config.h>
#include <deque>
#include <thread>

using namespace std::string_view_literals;

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
    EXPECT_EQ(15, bucket.size());
    EXPECT_NE(bucket.end(), bucket.find("state"));
    EXPECT_NE(bucket.end(), bucket.find("clients"));
    EXPECT_NE(bucket.end(), bucket.find("name"));
    EXPECT_NE(bucket.end(), bucket.find("type"));
    EXPECT_EQ("Success", bucket["data_ingress_status"]);
    EXPECT_NE(bucket.end(), bucket.find("ru"));
    EXPECT_NE(bucket.end(), bucket.find("wu"));
    EXPECT_NE(bucket.end(), bucket.find("num_throttled"));
    EXPECT_NE(bucket.end(), bucket.find("throttle_reserved"));
    EXPECT_NE(bucket.end(), bucket.find("throttle_hard_limit"));
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
    std::size_t reserved;
    std::size_t hard_limit;
    admin->stats(
            [&reserved, &hard_limit](const auto& k, const auto& v) {
                nlohmann::json json = nlohmann::json::parse(v);
                auto getLimit = [&json](auto key) -> std::size_t {
                    const nlohmann::json& entry = json.at(key);
                    if (entry.is_number()) {
                        return entry.get<size_t>();
                    } else if (entry.is_string() &&
                               entry.get<std::string>() == "unlimited") {
                        return std::numeric_limits<std::size_t>::max();
                    } else {
                        throw std::runtime_error(
                                fmt::format(R"(json["{}"] unknown type: {})",
                                            key,
                                            json.dump()));
                    }
                };

                reserved = getLimit("throttle_reserved");
                hard_limit = getLimit("throttle_hard_limit");
            },
            "bucket_details TestDefaultThrottleLimit");
    cluster->deleteBucket("TestDefaultThrottleLimit");
    EXPECT_EQ(cb::serverless::DefaultThrottleReservedUnits, reserved);
    EXPECT_EQ(cb::serverless::DefaultThrottleHardLimit, hard_limit);
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
    for (auto reason : {cb::mcbp::Status::BucketSizeLimitExceeded,
                        cb::mcbp::Status::BucketResidentRatioTooLow,
                        cb::mcbp::Status::BucketDataSizeTooBig,
                        cb::mcbp::Status::BucketDiskSpaceTooLow}) {
        auto rsp = admin->execute(
                SetBucketDataLimitExceededCommand{"bucket-0", reason});
        EXPECT_TRUE(rsp.isSuccess());

        admin->stats(
                [reason](auto k, auto v) {
                    auto json = nlohmann::json::parse(v);
                    EXPECT_EQ(::to_string(reason), json["data_ingress_status"]);
                },
                "bucket_details bucket-0");

        // fail to store a document
        try {
            writeDoc(*bucket0);
            FAIL() << "Should not be able to store a document when mode is set "
                      "to "
                   << reason;
        } catch (ConnectionError& error) {
            EXPECT_EQ(reason, error.getReason());
        }
        // Succeeds to store a document in bucket-1
        auto bucket1 = admin->clone();
        bucket1->authenticate("bucket-1", "bucket-1");
        bucket1->selectBucket("bucket-1");
        writeDoc(*bucket1);

        // enable client ingress
        rsp = admin->execute(SetBucketDataLimitExceededCommand{
                "bucket-0", cb::mcbp::Status::Success});
        EXPECT_TRUE(rsp.isSuccess());

        // succeed to store a document
        writeDoc(*bucket0);
    }
}

TEST(MiscTest, StopClientDataIngressLockedByNsServer) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");

    auto regulator = admin->clone();
    regulator->authenticate("@admin", "password");
    regulator->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);

    using cb::mcbp::Status;

    // The regulator can only set the state to BucketSizeLimitExceeded
    auto rsp = regulator->execute(SetBucketDataLimitExceededCommand{
            "bucket-0", Status::BucketSizeLimitExceeded});
    ASSERT_EQ(Status::Success, rsp.getStatus());
    // And may clear it if it is set to BucketSIzdeLimitExceeded
    rsp = regulator->execute(
            SetBucketDataLimitExceededCommand{"bucket-0", Status::Success});
    ASSERT_EQ(Status::Success, rsp.getStatus());

    for (auto reason : {Status::BucketResidentRatioTooLow,
                        Status::BucketDataSizeTooBig,
                        Status::BucketDiskSpaceTooLow}) {
        // Verify that we can't set the states reserved to ns_server
        rsp = regulator->execute(
                SetBucketDataLimitExceededCommand{"bucket-0", reason});
        ASSERT_EQ(Status::Locked, rsp.getStatus());

        // Verify that we can't change the state once ns_server set the state:
        rsp = admin->execute(
                SetBucketDataLimitExceededCommand{"bucket-0", reason});
        ASSERT_EQ(Status::Success, rsp.getStatus());

        rsp = regulator->execute(SetBucketDataLimitExceededCommand{
                "bucket-0", Status::BucketSizeLimitExceeded});
        ASSERT_EQ(Status::Locked, rsp.getStatus());

        // Verify that we can't clear the state if ns_server set the state
        rsp = regulator->execute(
                SetBucketDataLimitExceededCommand{"bucket-0", Status::Success});
        ASSERT_EQ(Status::Locked, rsp.getStatus());
    }

    // Clean up; set the state back to allow data ingress
    rsp = admin->execute(
            SetBucketDataLimitExceededCommand{"bucket-0", Status::Success});
    ASSERT_EQ(Status::Success, rsp.getStatus());
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

TEST(MiscTest, TraceInfoEnabled) {
    auto admin = cluster->getConnection(2);
    admin->authenticate("@admin", "password");
    admin->selectBucket("bucket-1");
    Document doc;
    doc.info.id = "TraceInfoEnabled";
    auto vbmap = cluster->getBucket("bucket-1")->getVbucketMap();
    admin->mutate(doc, Vbid{2}, MutationType::Add);

    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{10};
    std::string filename;
    cluster->iterateNodes([&filename](const Node& node) {
        if (node.getId() == "n_2") {
            filename = (node.directory / "log" / "memcached_log.000000.txt")
                               .generic_string();
        };
    });

    nlohmann::json entry;
    do {
        auto content = cb::io::loadFile(filename);
        auto lines = cb::string::split(content, '\n');
        for (const auto& line : lines) {
            const auto keyword = " Slow operation: "sv;
            auto index = line.find(keyword);
            if (index != std::string_view::npos) {
                try {
                    auto json = nlohmann::json::parse(
                            line.substr(index + keyword.size()));
                    if (json["packet"]["key"] == "<ud>TraceInfoEnabled</ud>") {
                        entry = std::move(json);
                    }
                } catch (const std::exception&) {
                }
            }
        }
        if (entry.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
    } while (entry.empty() && std::chrono::steady_clock::now() < timeout);
    ASSERT_FALSE(entry.empty())
            << "Timed out searching for the slow command log entry";
    ASSERT_NE(entry.end(), entry.find("trace"));
    EXPECT_NE(std::string::npos,
              entry["trace"].get<std::string>().find("json_validate"));
}

} // namespace cb::test
