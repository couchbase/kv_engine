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


class ErrmapTest : public TestappClientTest {
public:
    static bool validateJson(cJSON* json, size_t reqversion);
    static bool validateJson(const char* s, size_t n, size_t reqversion);
    MemcachedBinprotConnection& getBinprotConnection() {
        return dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        ErrmapTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

bool ErrmapTest::validateJson(cJSON *json, size_t reqversion) {
    // Validate the JSON
    cJSON *jversion = cJSON_GetObjectItem(json, "version");

    EXPECT_NE(nullptr, jversion);
    if (jversion != nullptr) {
        EXPECT_LE(jversion->valueint, reqversion);
    }

    cJSON *jrev = cJSON_GetObjectItem(json, "revision");
    EXPECT_NE(nullptr, jrev);
    if (jrev != nullptr) {
        EXPECT_EQ(1, jrev->valueint);
    }
    return !::testing::Test::HasFailure();
}

bool ErrmapTest::validateJson(const char *s, size_t len, size_t reqversion) {
    unique_cJSON_ptr ptr(cJSON_Parse(std::string(s, len).c_str()));
    EXPECT_NE(nullptr, ptr);
    return validateJson(ptr.get(), reqversion);
}

TEST_P(ErrmapTest, GetErrmapOk) {
    BinprotGetErrorMapCommand cmd;
    BinprotGetErrorMapResponse resp;

    const uint16_t version = 1;
    cmd.setVersion(version);

    getBinprotConnection().executeCommand(cmd, resp);

    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    ASSERT_GT(resp.getBodylen(), 0);
    EXPECT_TRUE(validateJson(reinterpret_cast<const char *>(resp.getPayload()),
                             resp.getBodylen(), version));
}

TEST_P(ErrmapTest, GetErrmapAnyVersion) {
    BinprotGetErrorMapCommand cmd;
    BinprotResponse resp;
    const uint16_t version = 256;

    cmd.setVersion(version);
    getBinprotConnection().executeCommand(cmd, resp);

    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    ASSERT_GT(resp.getBodylen(), 0);

    std::string raw_json(reinterpret_cast<const char *>(resp.getPayload()),
                         resp.getBodylen());
    unique_cJSON_ptr ptr(cJSON_Parse(raw_json.c_str()));

    ASSERT_TRUE(validateJson(ptr.get(), version));
    cJSON *jversion = cJSON_GetObjectItem(ptr.get(), "version");
    ASSERT_EQ(1, jversion->valueint);
}

TEST_P(ErrmapTest, GetErrmapBadversion) {
    BinprotGetErrorMapCommand cmd;
    BinprotResponse resp;
    cmd.setVersion(0);
    getBinprotConnection().executeCommand(cmd, resp);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, resp.getStatus());
}
