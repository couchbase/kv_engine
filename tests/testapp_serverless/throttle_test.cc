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

} // namespace cb::test
