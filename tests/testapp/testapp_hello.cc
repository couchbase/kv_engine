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
#include <algorithm>

/**
 * The maximum number of character the core preserves for the
 * agent name for each connection
 */
const size_t MaxSavedAgentName = 32;

/**
 * The maximum number of character the core preserves for the
 * connection identifier for each connection
 */
const size_t MaxSavedConnectionId = 33;

class HelloTest : public TestappTest {
protected:
    /**
     * Get a fresh connection to the server (with no features set)
     */
    MemcachedConnection& getConnection() override {
        auto& ret = connectionMap.getConnection(false, AF_INET);
        ret.reconnect();
        return ret;
    }
};

/**
 * Verify that the first 32 bytes is stored in the server if the key
 * isn't json
 */
TEST_F(HelloTest, AgentName) {
    auto& conn = getConnection();
    const std::string agentname{
            "AgentInformation - c21fee83af4e7943/c21fee83af4e7943"};
    BinprotHelloCommand cmd(agentname);
    BinprotHelloResponse resp;
    conn.executeCommand(cmd, resp);
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (auto* conn = stats.get()->child; conn != nullptr; conn = conn->next) {
        unique_cJSON_ptr json(cJSON_Parse(conn->valuestring));
        ASSERT_NE(nullptr, json.get());
        // the _this_ pointer should at least be there
        ASSERT_NE(nullptr, cJSON_GetObjectItem(json.get(), "connection"));
        auto* ptr = cJSON_GetObjectItem(json.get(), "agent_name");
        if (ptr != nullptr) {
            ASSERT_EQ(cJSON_String, ptr->type);
            ASSERT_EQ(agentname.substr(0, MaxSavedAgentName), ptr->valuestring);
            found = true;
            break;
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + to_string(stats);
}

/**
 * Verify that we can set agent information via JSON
 */
TEST_F(HelloTest, JsonAgentInformation) {
    auto& conn = getConnection();
    BinprotHelloCommand cmd(
            R"({"a":"AgentInformation","i":"c21fee83af4e7943/c21fee83af4e7943"})");
    BinprotHelloResponse resp;
    conn.executeCommand(cmd, resp);
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (auto* conn = stats.get()->child; conn != nullptr; conn = conn->next) {
        unique_cJSON_ptr json(cJSON_Parse(conn->valuestring));
        ASSERT_NE(nullptr, json.get());
        // the _this_ pointer should at least be there
        ASSERT_NE(nullptr, cJSON_GetObjectItem(json.get(), "connection"));
        auto* ptr = cJSON_GetObjectItem(json.get(), "agent_name");
        if (ptr != nullptr) {
            ASSERT_EQ(cJSON_String, ptr->type);
            if (strcmp("AgentInformation", ptr->valuestring) == 0) {
                // We should have the uuid here!
                auto* id = cJSON_GetObjectItem(json.get(), "connection_id");
                ASSERT_NE(nullptr, id);
                EXPECT_EQ(cJSON_String, id->type);
                EXPECT_STREQ("c21fee83af4e7943/c21fee83af4e7943",
                             id->valuestring);
                found = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + to_string(stats);
}

/**
 * Verify that we can set agent information via JSON, and that
 * the server correctly truncates the values if they're too long
 */
TEST_F(HelloTest, JsonAgentInformationStringsTruncated) {
    auto& conn = getConnection();
    const std::string agent =
            "AgentInformation which is longer than what we're going to save "
            "for it";
    const std::string cid =
            "Id which is longer than what we're going to store for it... Ok?";

    ASSERT_LE(MaxSavedAgentName, agent.size());
    ASSERT_LE(MaxSavedConnectionId, cid.size());

    BinprotHelloCommand cmd(R"({"a":")" + agent + R"(","i":")" + cid + R"("})");
    BinprotHelloResponse resp;
    conn.executeCommand(cmd, resp);
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (auto* conn = stats.get()->child; conn != nullptr; conn = conn->next) {
        unique_cJSON_ptr json(cJSON_Parse(conn->valuestring));
        ASSERT_NE(nullptr, json.get());
        // the _this_ pointer should at least be there
        ASSERT_NE(nullptr, cJSON_GetObjectItem(json.get(), "connection"));
        auto* ptr = cJSON_GetObjectItem(json.get(), "agent_name");
        if (ptr != nullptr) {
            ASSERT_EQ(cJSON_String, ptr->type);
            if (agent.substr(0, MaxSavedAgentName) == ptr->valuestring) {
                // We should have the uuid here!
                auto* id = cJSON_GetObjectItem(json.get(), "connection_id");
                ASSERT_NE(nullptr, id);
                EXPECT_EQ(cJSON_String, id->type);
                EXPECT_EQ(cid.substr(0, MaxSavedConnectionId), id->valuestring);
                ;
                found = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + to_string(stats);
}
