/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#ifdef USE_FUSION

#include "testapp_client_test.h"

#include <gmock/gmock.h>
#include <nlohmann/json.hpp>

class FusionTest : public TestappClientTest {
protected:
    static void SetUpTestCase() {
        const std::string dbPath = mcd_env->getDbPath();
        const auto bucketConfig = fmt::format(
                "magma_fusion_logstore_uri={};magma_fusion_metadatastore_uri={"
                "}",
                "local://" + dbPath + "/logstore",
                "local://" + dbPath + "/metadatastore");
        doSetUpTestCaseWithConfiguration(generate_config(), bucketConfig);
    }

    void SetUp() override {
        rebuildUserConnection(false);
        if (userConnection->statsMap("")["ep_backend"] != "magma") {
            GTEST_SKIP();
        }
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(FusionTest, AggregatedStats) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isNotSupported());
    }
}

TEST_P(FusionTest, InvalidStat) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion someInvalidStat 0");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }
}

TEST_P(FusionTest, Stat_SyncInfo) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion sync_info a");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }

    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion sync_info 0");
    });
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("logSeqno"));
    EXPECT_EQ(0, res["logSeqno"]);
    ASSERT_TRUE(res.contains("logTerm"));
    EXPECT_EQ(0, res["logTerm"]);
    ASSERT_TRUE(res.contains("version"));
    EXPECT_EQ(1, res["version"]);
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes) {
    nlohmann::json res;
    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion active_guest_volumes 0");
    });

    // @todo MB-63679: Actual values will be populated once we have MountKVStore
    ASSERT_TRUE(res.is_array());
}

#endif // USE_FUSION
