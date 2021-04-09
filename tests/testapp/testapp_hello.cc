/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

    bool found = false;
    conn.stats(
            [&found, &agentname](const std::string& key,
                                 const std::string& value) {
                ASSERT_EQ("0", key);
                ASSERT_FALSE(value.empty());
                auto json = nlohmann::json::parse(value);
                auto agentStr = json["agent_name"].get<std::string>();
                ASSERT_EQ(agentname.substr(0, MaxSavedAgentName), agentStr);
                found = true;
            },
            "connections self");

    ASSERT_TRUE(found)
            << "connection self did not return the current connection";
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

    bool found = false;
    conn.stats(
            [&found](const std::string& key, const std::string& value) {
                ASSERT_EQ("0", key);
                ASSERT_FALSE(value.empty());
                auto json = nlohmann::json::parse(value);
                ASSERT_EQ("AgentInformation",
                          json["agent_name"].get<std::string>());
                ASSERT_EQ("c21fee83af4e7943/c21fee83af4e7943",
                          json["connection_id"].get<std::string>());
                found = true;
            },
            "connections self");
    ASSERT_TRUE(found)
            << "connection self did not return the current connection";
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

    bool found = false;
    conn.stats(
            [&found, &agentname, &cid](const std::string& key,
                                       const std::string& value) {
                ASSERT_EQ("0", key);
                ASSERT_FALSE(value.empty());
                auto json = nlohmann::json::parse(value);
                ASSERT_EQ(agentname.substr(0, MaxSavedAgentName),
                          json["agent_name"].get<std::string>());
                ASSERT_EQ(cid.substr(0, MaxSavedConnectionId),
                          json["connection_id"].get<std::string>());
                found = true;
            },
            "connections self");
    ASSERT_TRUE(found)
            << "connection self did not return the current connection";
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
