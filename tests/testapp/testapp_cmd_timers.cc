/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <platform/compress.h>
#include <protocol/connection/client_connection.h>
#include <algorithm>

class CmdTimerTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        auto& conn = getAdminConnection();
        conn.createBucket("rbac_test", "", BucketType::Memcached);

        conn.selectBucket("default");
        BinprotResponse response;

        // Reset the command timers before we start
        conn.executeCommand(
                BinprotGenericCommand{PROTOCOL_BINARY_CMD_STAT, "reset"},
                response);

        // We just need to have a command we can check the numbers of
        conn.executeCommand(BinprotGenericCommand{PROTOCOL_BINARY_CMD_SCRUB},
                            response);

        conn.selectBucket("rbac_test");
        conn.executeCommand(BinprotGenericCommand{PROTOCOL_BINARY_CMD_SCRUB},
                            response);
        conn.reconnect();
    }

    void TearDown() override {
        auto& conn = getAdminConnection();
        conn.deleteBucket("rbac_test");
        TestappClientTest::TearDown();
    }

protected:
    /**
     * Get the number of operations in the payload
     * @param payload the JSON returned from the server
     * @return The number of operations we found in there
     */
    size_t getNumberOfOps(const std::string& payload) {
        unique_cJSON_ptr json(cJSON_Parse(payload.c_str()));
        if (!json) {
            throw std::invalid_argument("Failed to parse payload: " + payload);
        }

        size_t ret = 0;
        for (auto* obj = json.get()->child; obj != nullptr; obj = obj->next) {
            if (obj->type == cJSON_Number) {
                ret += obj->valueint;
            } else if (obj->type == cJSON_Array) {
                for (auto* ent = obj->child; ent != nullptr; ent = ent->next) {
                    if (ent->type == cJSON_Number) {
                        ret += ent->valueint;
                    } else {
                        throw std::invalid_argument("Expected numbers, got " +
                                                    std::to_string(ent->type));
                    }
                }
            }
        }

        return ret;
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        CmdTimerTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

/**
 * Test that we return the aggreate of all the buckets we've got access
 * to if we request with no key (and no selected bucket) or by using
 * the special bucket "/all/"
 */
TEST_P(CmdTimerTest, AllBuckets) {
    auto& c = getAdminConnection();
    BinprotResponse response;

    // Admin should have full access
    for (const auto& bucket : {"", "/all/"}) {
        c.executeCommand(
                BinprotGetCmdTimerCommand{bucket, PROTOCOL_BINARY_CMD_SCRUB},
                response);
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(2, getNumberOfOps(response.getDataString()));
    }

    // Smith only have acces to the bucket rbac_test
    c.authenticate("smith", "smithpassword", "PLAIN");
    for (const auto& bucket : {"", "/all/"}) {
        c.executeCommand(
                BinprotGetCmdTimerCommand{bucket, PROTOCOL_BINARY_CMD_SCRUB},
                response);
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    }
    c.reconnect();
}

/**
 * Jones only have access to the bucket rbac_test, but is missing the
 * simple-stats privilege
 */
TEST_P(CmdTimerTest, NoAccess) {
    auto& c = getConnection();

    c.authenticate("jones", "jonespassword", "PLAIN");
    for (const auto& bucket : {"", "/all/", "rbac_test", "default"}) {
        BinprotResponse response;
        c.executeCommand(
                BinprotGetCmdTimerCommand{bucket, PROTOCOL_BINARY_CMD_SCRUB},
                response);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, response.getStatus());
    }

    // Make sure it doesn't work for the "current selected bucket"
    c.selectBucket("rbac_test");
    BinprotResponse response;
    c.executeCommand(BinprotGetCmdTimerCommand{"", PROTOCOL_BINARY_CMD_SCRUB},
                     response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, response.getStatus());
    c.reconnect();
}

TEST_P(CmdTimerTest, CurrentBucket) {
    auto& c = getAdminConnection();
    c.selectBucket("rbac_test");

    BinprotResponse response;
    for (const auto& bucket : {"", "rbac_test"}) {
        c.executeCommand(
                BinprotGetCmdTimerCommand{bucket, PROTOCOL_BINARY_CMD_SCRUB},
                response);
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    }
    c.reconnect();
}

/**
 * We should get no access for unknown buckets
 */
TEST_P(CmdTimerTest, NonexistentBucket) {
    auto& c = getConnection();
    BinprotResponse response;
    c.executeCommand(BinprotGetCmdTimerCommand{"asdfasdfasdf",
                                               PROTOCOL_BINARY_CMD_SCRUB},
                     response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EACCESS, response.getStatus());
}

/**
 * A nonauth user should be automatically connected to the default bucket
 * (yes, we still have that in our unit tests).
 */
TEST_P(CmdTimerTest, DefaultBucket) {
    auto& c = getConnection();
    c.reconnect();
    BinprotResponse response;
    c.executeCommand(BinprotGetCmdTimerCommand{"", PROTOCOL_BINARY_CMD_SCRUB},
                     response);
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
}
