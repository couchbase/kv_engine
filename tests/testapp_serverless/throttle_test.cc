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

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/portability/GTest.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <thread>
#include <vector>

namespace cb::test {

TEST(ThrottleTest, OpsAreThrottled) {
    auto func = [](const std::string& name) {
        auto conn = cluster->getConnection(0);
        conn->authenticate(name, name);
        conn->selectBucket(name);
        conn->setReadTimeout(std::chrono::seconds{3});

        Document document;
        document.info.id = "OpsAreThrottled";
        document.value = "This is the awesome document";

        // store a document
        conn->mutate(document, Vbid{0}, MutationType::Set);

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 4096; ++i) { // Run 4k mutations
            conn->get(document.info.id, Vbid{0});
        }
        auto end = std::chrono::steady_clock::now();
        EXPECT_LT(
                std::chrono::seconds{2},
                std::chrono::duration_cast<std::chrono::seconds>(end - start));

        nlohmann::json stats;
        conn->authenticate("@admin", "password");
        conn->stats(
                [&stats](const auto& k, const auto& v) {
                    stats = nlohmann::json::parse(v);
                },
                std::string{"bucket_details "} + name);
        ASSERT_FALSE(stats.empty());
        ASSERT_LE(3, stats["num_throttled"]);
        // it's hard to compare this with a "real value"; but it should at
        // least be non-zero
        ASSERT_NE(0, stats["throttle_wait_time"]);
    };

    std::vector<std::thread> threads;
    for (int ii = 0; ii < 5; ++ii) {
        threads.emplace_back(
                std::thread{[func, name = "bucket-" + std::to_string(ii)]() {
                    func(name);
                }});
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST(ThrottleTest, NonBlockingThrottlingMode) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("bucket-0", "bucket-0");
    conn->selectBucket("bucket-0");
    conn->setFeature(cb::mcbp::Feature::NonBlockingThrottlingMode, true);
    conn->setReadTimeout(std::chrono::seconds{30});

    Document document;
    document.info.id = "NonBlockingThrottlingMode";
    document.value = "This is the awesome document";

    // store a document (this command may be throttled for a while,
    // depending on the previous tests...
    conn->mutate(document, Vbid{0}, MutationType::Set);

    BinprotResponse rsp;
    do {
        rsp = conn->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Get,
                                                  "NonBlockingThrottlingMode"});
    } while (rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::EWouldThrottle, rsp.getStatus());
    // We should have an indication for how long we should wait until
    // the next "tick"
    EXPECT_LT(0, rsp.getDataJson()["next_tick_us"].get<int>());
    EXPECT_GT(1000000, rsp.getDataJson()["next_tick_us"].get<int>());
}

TEST(ThrottleTest, DeleteBucketWhileThrottling) {
    // Setup a new user/Bucket for this test as we want to delete the Bucket at
    // the end of the test.
    std::string rbac = R"({
"buckets": {
  "testBucket": {
    "privileges": [
      "Read",
      "SimpleStats",
      "Stats",
      "Insert",
      "Delete",
      "Upsert",
      "DcpProducer",
      "DcpStream"
    ]
  }
},
"privileges": ["Stats"],
"domain": "external"
})";
    cluster->getAuthProviderService().upsertUser(
            {"testBucket", "testBucket", nlohmann::json::parse(rbac)});

    auto bucket = cluster->createBucket("testBucket",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    bucket->setThrottleLimit(1);

    auto conn = cluster->getConnection(0);
    conn->authenticate("testBucket", "testBucket");
    conn->selectBucket("testBucket");
    conn->setReadTimeout(std::chrono::seconds{30});

    // store a document (this command may be throttled for a while,
    // depending on the previous tests)
    Document document;
    document.info.id = "document";
    document.value = std::string(1024, 'x');
    conn->mutate(document, Vbid{0}, MutationType::Set);

    // Setup a second connection on which we will do our gets. We will have auth
    // issues when trying to grab stats later otherwise...
    auto conn1 = cluster->getConnection(0);
    conn1->authenticate("testBucket", "testBucket");
    conn1->selectBucket("testBucket");

    // Sent a few gets to the server, don't wait for responses as they should
    // get throttled
    for (auto i = 0; i < 10; i++) {
        BinprotCommand command =
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "document"};
        conn1->sendCommand(command);
    }

    // Sanity stats checks
    nlohmann::json stats;
    conn->authenticate("@admin", "password");
    conn->stats([&stats](const auto& k,
                         const auto& v) { stats = nlohmann::json::parse(v); },
                "bucket_details testBucket");
    ASSERT_FALSE(stats.empty());
    ASSERT_LT(0, stats["num_throttled"]);

    // This is the test; before the fix we would hang/timeout this call as we
    // could not delete the Bucket with throttled cookies in the systme
    cluster->deleteBucket("testBucket");
}

} // namespace cb::test
