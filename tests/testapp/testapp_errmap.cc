/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
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
