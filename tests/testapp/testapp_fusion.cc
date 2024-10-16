/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <gmock/gmock.h>
#include <nlohmann/json.hpp>

class FusionTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(FusionTest, Stats) {
    nlohmann::json res;
    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion");
    });
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("sub_cmd"));
    EXPECT_EQ("N/A", res["sub_cmd"]);
    ASSERT_TRUE(res.contains("vbid"));
    EXPECT_EQ("N/A", res["vbid"]);

    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion a");
    });
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("sub_cmd"));
    EXPECT_EQ("a", res["sub_cmd"]);
    ASSERT_TRUE(res.contains("vbid"));
    EXPECT_EQ("N/A", res["vbid"]);

    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion a 0 extra-arg");
        });
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }

    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion a b");
        });
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }
}
