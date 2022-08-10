/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_drain.h"
#include "serverless_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>
#include <protocol/connection/async_client_connection.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <serverless/config.h>
#include <deque>

namespace cb::test {

class DcpTest : public ::testing::Test {
public:
protected:
    static void SetUpTestCase() {
        auto conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("dcp");

        for (size_t ii = 0; ii < NumDocuments; ++ii) {
            Document doc;
            doc.info.id = "Document-" + std::to_string(ii);
            doc.value = "Hello World";
            conn->mutate(doc, Vbid{0}, MutationType::Set);
        }
    }
    static constexpr size_t NumDocuments = 4096;
    // Each document is 1 RU
    static constexpr size_t NumReadUnits = 4096;
};

/// Verify that we can run DCP without throttling or metering
TEST_F(DcpTest, DcpDrainNoMeterNoThrottle) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("dcp");

    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details dcp");

    DcpDrain instance(admin->getFamily() == AF_INET ? "127.0.0.1" : "::1",
                      std::to_string(admin->getPort()),
                      "@admin",
                      "password",
                      "dcp");
    instance.drain();
    EXPECT_EQ(NumDocuments, instance.getNumMutations());
    EXPECT_EQ(NumReadUnits, instance.getRu());

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details dcp");

    // Verify that we didn't meter
    EXPECT_EQ(before["ru"].get<size_t>(), after["ru"].get<size_t>());

    // Verify that we didn't throttle
    EXPECT_EQ(before["num_throttled"].get<size_t>(),
              after["num_throttled"].get<size_t>());
}

/// Verify that DCP gets throttled and metered
TEST_F(DcpTest, DcpDrainMeteredAndThrottled) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("dcp");

    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details dcp");

    DcpDrain instance(admin->getFamily() == AF_INET ? "127.0.0.1" : "::1",
                      std::to_string(admin->getPort()),
                      "dcp",
                      "dcp",
                      "dcp");

    auto start = std::chrono::steady_clock::now();
    instance.drain();
    auto stop = std::chrono::steady_clock::now();
    // No matter what the test should take _at least_ 2 seconds due to
    // throttling..
    EXPECT_LT(std::chrono::seconds{2},
              std::chrono::duration_cast<std::chrono::seconds>(stop - start));
    EXPECT_EQ(NumDocuments, instance.getNumMutations());
    EXPECT_EQ(NumReadUnits, instance.getRu());

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details dcp");

    EXPECT_EQ(NumReadUnits,
              after["ru"].get<size_t>() - before["ru"].get<size_t>());

    // Verify that the server throttled us (this may fail on a super-slow
    // system. Lets just disable the check on sanitizers..)
    if (!folly::kIsSanitize) {
        EXPECT_NE(after["num_throttled"].get<size_t>(),
                  before["num_throttled"].get<size_t>());
    }
}

} // namespace cb::test