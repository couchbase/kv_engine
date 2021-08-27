/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <nlohmann/json.hpp>

class CollectionsTest : public TestappClientTest {
    void SetUp() override {
        TestappClientTest::SetUp();
        // Default engine does not support changing the collection configuration
        if (!mcd_env->getTestBucket().supportsCollections() ||
            mcd_env->getTestBucket().getName() == "default_engine") {
            return;
        }
        auto& conn = getAdminConnection();
        conn.selectBucket(bucketName);
        auto response = conn.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::CollectionsSetManifest,
                {},
                R"({"uid" : "7f",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})"});

        // The manifest sticks, so if we set it again with the same uid, that
        // is invalid, just assert erange and carry on
        if (!response.isSuccess()) {
            ASSERT_EQ(cb::mcbp::Status::Erange, response.getStatus());
        }
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         CollectionsTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

// Check that an unknown scope/collection error returns the expected JSON
TEST_P(CollectionsTest, ManifestUidInResponse) {
    std::string expectedUid = "7f";
    if (!mcd_env->getTestBucket().supportsCollections()) {
        return;
    }
    if (mcd_env->getTestBucket().getName() == "default_engine") {
        expectedUid = "0";
    }

    auto& conn = getConnection();

    // Force the unknown collection error and check the JSON
    auto response = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::CollectionsGetID, "_default.error"});
    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::UnknownCollection, response.getStatus());
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(response.getDataString());
    } catch (const nlohmann::json::exception& e) {
        FAIL() << "Cannot parse json response:" << response.getDataString()
               << " e:" << e.what();
    }

    auto itr = parsed.find("manifest_uid");
    EXPECT_NE(parsed.end(), itr);
    EXPECT_EQ(expectedUid, itr->get<std::string>());
}
