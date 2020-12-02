/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

class ErrmapTest : public TestappClientTest {
public:
    static bool validateJson(const nlohmann::json& json, size_t reqversion);
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         ErrmapTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

bool ErrmapTest::validateJson(const nlohmann::json& json, size_t reqversion) {
    // Validate the JSON
    auto version = json.find("version");

    EXPECT_NE(json.end(), version);
    EXPECT_GE(reqversion, version->get<int>());

    auto rev = json.find("revision");
    EXPECT_NE(json.end(), rev);
    EXPECT_EQ(4, rev->get<int>());

    return !::testing::Test::HasFailure();
}

TEST_P(ErrmapTest, GetErrmapOk) {
    BinprotGetErrorMapCommand cmd;

    const uint16_t version = 1;
    cmd.setVersion(version);

    const auto resp = getConnection().execute(cmd);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    ASSERT_GT(resp.getBodylen(), 0);
    EXPECT_TRUE(
            validateJson(nlohmann::json::parse(resp.getDataString()), version));
}

TEST_P(ErrmapTest, GetErrmapAnyVersion) {
    BinprotGetErrorMapCommand cmd;
    const uint16_t version = 256;

    cmd.setVersion(version);
    const auto resp = getConnection().execute(cmd);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    ASSERT_GT(resp.getBodylen(), 0);

    const auto json = nlohmann::json::parse(resp.getDataString());
    ASSERT_TRUE(validateJson(json, version));
    ASSERT_EQ(1, json["version"].get<int>());
}

TEST_P(ErrmapTest, GetErrmapBadversion) {
    BinprotGetErrorMapCommand cmd;
    cmd.setVersion(0);
    const auto resp = getConnection().execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}
