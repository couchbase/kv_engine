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

#include <nlohmann/json.hpp>
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

        // Reset the command timers before we start
        conn.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Stat, "reset"});

        // We just need to have a command we can check the numbers of
        conn.execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Scrub});

        conn.selectBucket("rbac_test");
        conn.execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Scrub});
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
     *
     * @param payload the JSON returned from the server
     * @return The number of operations we found in there
     */
    size_t getNumberOfOps(const std::string& payload) {
        nlohmann::json json = nlohmann::json::parse(payload);
        if (json.is_null()) {
            throw std::invalid_argument("Failed to parse payload: " + payload);
        }

        auto ret = json["total"].get<size_t>();

        return ret;
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
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

    // Admin should have full access
    for (const auto& bucket : {"", "/all/"}) {
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(2, getNumberOfOps(response.getDataString()));
    }

    // Smith only have acces to the bucket rbac_test
    c.authenticate("smith", "smithpassword", "PLAIN");
    for (const auto& bucket : {"", "/all/"}) {
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
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
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    }

    // Make sure it doesn't work for the "current selected bucket"
    c.selectBucket("rbac_test");
    const auto response = c.execute(
            BinprotGetCmdTimerCommand{"", cb::mcbp::ClientOpcode::Scrub});
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    c.reconnect();
}

TEST_P(CmdTimerTest, CurrentBucket) {
    auto& c = getAdminConnection();
    c.selectBucket("rbac_test");

    for (const auto& bucket : {"", "rbac_test"}) {
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
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
    const auto response = c.execute(BinprotGetCmdTimerCommand{
            "asdfasdfasdf", cb::mcbp::ClientOpcode::Scrub});
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
}

/**
 * A nonauth user should be automatically connected to the default bucket
 * (yes, we still have that in our unit tests).
 */
TEST_P(CmdTimerTest, DefaultBucket) {
    auto& c = getConnection();
    c.reconnect();
    const auto response = c.execute(
            BinprotGetCmdTimerCommand{"", cb::mcbp::ClientOpcode::Scrub});
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
}

/**
 * Attempting to fetch timings for an empty histogram should succeed (but return
 * no samples)
 */
TEST_P(CmdTimerTest, EmptySuccess) {
    auto& c = getAdminConnection();
    c.execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Stat, "reset"});
    const auto response = c.execute(
            BinprotGetCmdTimerCommand{"default", cb::mcbp::ClientOpcode::Set});
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(0, getNumberOfOps(response.getDataString()));
}
