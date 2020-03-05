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
    auto& conn = getAdminConnection();
    const std::string agentname{
            "AgentInformation - c21fee83af4e7943/c21fee83af4e7943"};
    BinprotHelloCommand cmd(agentname);
    const auto resp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (const auto& c : stats) {
        ASSERT_NE(c.end(), c.find("connection"));
        auto agent = c.find("agent_name");
        if (agent != c.end()) {
            auto agentStr = agent->get<std::string>();
            ASSERT_EQ(agentname.substr(0, MaxSavedAgentName), agentStr);
            found = true;
            break;
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + stats.dump();
}

/**
 * Verify that we can set agent information via JSON
 */
TEST_F(HelloTest, JsonAgentInformation) {
    auto& conn = getAdminConnection();
    BinprotHelloCommand cmd(
            R"({"a":"AgentInformation","i":"c21fee83af4e7943/c21fee83af4e7943"})");
    const auto resp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (const auto& c : stats) {
        ASSERT_NE(c.end(), c.find("connection"));
        auto agent = c.find("agent_name");
        if (agent != c.end()) {
            auto agentStr = agent->get<std::string>();
            if (agentStr == "AgentInformation") {
                // We should have the uuid here
                EXPECT_EQ("c21fee83af4e7943/c21fee83af4e7943",
                          c["connection_id"].get<std::string>());
                found = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + stats.dump();
}

/**
 * Verify that we can set agent information via JSON, and that
 * the server correctly truncates the values if they're too long
 */
TEST_F(HelloTest, JsonAgentInformationStringsTruncated) {
    auto& conn = getAdminConnection();
    const std::string agentname =
            "AgentInformation which is longer than what we're going to save "
            "for it";
    const std::string cid =
            "Id which is longer than what we're going to store for it... Ok?";

    ASSERT_LE(MaxSavedAgentName, agentname.size());
    ASSERT_LE(MaxSavedConnectionId, cid.size());

    BinprotHelloCommand cmd(R"({"a":")" + agentname + R"(","i":")" + cid +
                            R"("})");
    const auto resp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_TRUE(resp.isSuccess());

    auto stats = conn.stats("connections");
    bool found = false;

    // look over all of the entries and verify that it's set :)
    // validate that at least thats true:
    for (const auto& c : stats) {
        ASSERT_NE(c.end(), c.find("connection"));
        auto agent = c.find("agent_name");
        if (agent != c.end()) {
            if (agentname.substr(0, MaxSavedAgentName) ==
                agent->get<std::string>()) {
                // We should have the uuid here!
                EXPECT_EQ(cid.substr(0, MaxSavedConnectionId),
                          c["connection_id"].get<std::string>());
                found = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found) << "connection not found in stats: " + stats.dump();
}

/// Verify that the server gives me AltRequestSupport
TEST_F(HelloTest, AltRequestSupport) {
    BinprotHelloCommand cmd("AltRequestSupport");
    cmd.enableFeature(cb::mcbp::Feature::AltRequestSupport);
    const auto rsp = BinprotHelloResponse(getConnection().execute(cmd));
    ASSERT_TRUE(rsp.isSuccess());
    const auto& features = rsp.getFeatures();
    ASSERT_EQ(1, features.size());
    ASSERT_EQ(cb::mcbp::Feature::AltRequestSupport, features[0]);
}

/// Verify that the server gives me SyncReplication
TEST_F(HelloTest, SyncReplication) {
    BinprotHelloCommand cmd("SyncReplication");
    cmd.enableFeature(cb::mcbp::Feature::SyncReplication);
    const auto rsp = BinprotHelloResponse(getConnection().execute(cmd));
    ASSERT_TRUE(rsp.isSuccess());
    const auto& features = rsp.getFeatures();
    ASSERT_EQ(1, features.size());
    ASSERT_EQ(cb::mcbp::Feature::SyncReplication, features[0]);
}

TEST_F(HelloTest, Collections) {
    BinprotHelloCommand cmd("Collections");
    cmd.enableFeature(cb::mcbp::Feature::Collections);
    const auto rsp = BinprotHelloResponse(getConnection().execute(cmd));

    ASSERT_TRUE(rsp.isSuccess());
    if (GetTestBucket().supportsCollections()) {
        const auto& features = rsp.getFeatures();
        ASSERT_EQ(1, features.size());
        ASSERT_EQ(cb::mcbp::Feature::Collections, features[0]);
    } else {
        const auto& features = rsp.getFeatures();
        ASSERT_EQ(0, features.size());
    }
}
