/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
        conn.selectBucket("default");
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
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
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
