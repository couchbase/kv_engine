/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <cctype>
#include <limits>
#include <thread>
#include "testapp_arithmetic.h"

class ClusterConfigTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        // Make sure we've specified a session token
        setClusterSessionToken(0xdeadbeef);
    }

    BinprotResponse setClusterConfig(uint64_t token,
                                     const std::string& config) {
        auto& conn = getAdminConnection();
        conn.selectBucket("default");
        BinprotResponse response;
        conn.executeCommand(BinprotSetClusterConfigCommand{token, config},
                            response);
        return response;
    }

    void test_MB_17506(bool dedupe);
};

void ClusterConfigTest::test_MB_17506(bool dedupe) {
    // First set the correct deduplication mode
    auto* value = cJSON_GetObjectItem(memcached_cfg.get(), "dedupe_nmvb_maps");
    ASSERT_NE(nullptr, value);
    if (dedupe) {
        cJSON_ReplaceItemInObject(
                memcached_cfg.get(), "dedupe_nmvb_maps", cJSON_CreateTrue());
    } else {
        cJSON_ReplaceItemInObject(
                memcached_cfg.get(), "dedupe_nmvb_maps", cJSON_CreateFalse());
    }
    reconfigure();

    const std::string clustermap{R"({"rev":100})"};

    // Make sure we have a cluster configuration installed
    auto response = setClusterConfig(token, clustermap);
    EXPECT_TRUE(response.isSuccess());

    auto& conn = getConnection();
    BinprotGetCommand command;
    command.setKey("foo");
    command.setVBucket(1);

    // Execute the first get command. This one should _ALWAYS_ contain a map
    conn.executeCommand(command, response);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, response.getStatus());
    EXPECT_EQ(clustermap, response.getDataString());

    // Execute it one more time..
    conn.executeCommand(command, response);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, response.getStatus());

    if (dedupe) {
        EXPECT_TRUE(response.getDataString().empty())
                << "Expected an empty stream, got [" << response.getDataString()
                << "]";
    } else {
        EXPECT_EQ(clustermap, response.getDataString());
    }
}

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        ClusterConfigTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl),
                        ::testing::PrintToStringParamName());

TEST_P(ClusterConfigTest, SetClusterConfigWithIncorrectSessionToken) {
    auto response = setClusterConfig(0xcafebeef, R"({"rev":100})");
    EXPECT_FALSE(response.isSuccess()) << "Should not be allowed to set "
                                          "cluster config with invalid session "
                                          "token";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, response.getStatus());
}

TEST_P(ClusterConfigTest, SetClusterConfigWithCorrectTokenInvalidPayload) {
    auto response = setClusterConfig(token, R"({"foo":"bar"})");
    EXPECT_FALSE(response.isSuccess())
            << "Should not be allowed to set cluster config invalid payload";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, response.getStatus());
}

TEST_P(ClusterConfigTest, SetClusterConfigWithCorrectToken) {
    auto response = setClusterConfig(token, R"({"rev":100})");
    EXPECT_TRUE(response.isSuccess()) << "Should be allowed to set cluster "
                                         "config with the correct session "
                                         "token";
}

TEST_P(ClusterConfigTest, GetClusterConfig) {
    const std::string config{R"({"rev":100})"};
    auto response = setClusterConfig(token, config);
    ASSERT_TRUE(response.isSuccess());

    BinprotGenericCommand cmd{PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, "", ""};
    auto& conn = getConnection();
    conn.executeCommand(cmd, response);
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(config, response.getDataString());
}

TEST_P(ClusterConfigTest, test_MB_17506_no_dedupe) {
    test_MB_17506(false);
}

TEST_P(ClusterConfigTest, test_MB_17506_dedupe) {
    test_MB_17506(true);
}
