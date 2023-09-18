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

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>
#include <algorithm>

class CmdTimerTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        adminConnection->createBucket("rbac_test", "", BucketType::Memcached);

        // Reset the command timers before we start
        adminConnection->selectBucket(bucketName);
        adminConnection->stats("reset");

        // We just need to have a command we can check the numbers of
        adminConnection->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Scrub});

        adminConnection->executeInBucket("rbac_test", [](auto& c) {
            c.execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Scrub});
        });
    }

    void TearDown() override {
        adminConnection->deleteBucket("rbac_test");
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
 * Test that we return the aggregate of all the buckets we've got access to
 * if we request with the special bucket "/all/"
 */
TEST_P(CmdTimerTest, AllBuckets) {

    // Admin should have full access
    auto response = adminConnection->execute(
            BinprotGetCmdTimerCommand{"/all/", cb::mcbp::ClientOpcode::Scrub});
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(2, getNumberOfOps(response.getDataString()));

    // Smith only has access to the bucket rbac_test - should only see numbers
    // from that.
    auto& c = getConnection();
    c.authenticate("smith", "smithpassword", "PLAIN");

    response = c.execute(
            BinprotGetCmdTimerCommand{"/all/", cb::mcbp::ClientOpcode::Scrub});
    EXPECT_TRUE(response.isSuccess());
    EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    c.reconnect();
}

/**
 * Test that timings for commands not associated with a bucket can be returned
 * either by not associating with a bucket or by explicitly asking for
 * "@no bucket@", as long as appropriate privs are present.
 */
TEST_P(CmdTimerTest, NoBucket) {
    // Perform an operation against "@no bucket@" so we can request its timing
    // stats in a moment.
    adminConnection->unselectBucket();
    adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Scrub});

    // Admin should have full access - either by unselecting any bucket
    // and issuing a request with the empty string, or by explicilty asking for
    // "@no bucket@".
    for (auto bucket : {"", "@no bucket@"}) {
        SCOPED_TRACE(fmt::format("for bucket '{}'", bucket));
        auto response = adminConnection->execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    }

    // Smith attempting to access no-bucket should fail.
    auto& c = getConnection();
    c.authenticate("smith", "smithpassword", "PLAIN");

    for (auto bucket : {"", "@no bucket@"}) {
        SCOPED_TRACE(fmt::format("for bucket '{}'", bucket));
        auto response = c.execute(BinprotGetCmdTimerCommand{
                bucket, cb::mcbp::ClientOpcode::Scrub});
        EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
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
    for (const auto& bucket : {"", "/all/", "rbac_test", bucketName.c_str()}) {
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
    adminConnection->executeInBucket("rbac_test", [this](auto& c) {
        for (const auto& bucket : {"", "rbac_test"}) {
            const auto response = c.execute(BinprotGetCmdTimerCommand{
                    bucket, cb::mcbp::ClientOpcode::Scrub});
            EXPECT_TRUE(response.isSuccess());
            EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
        }
    });
}

/**
 * We removed support for getting command timings for not the current bucket
 * as it was broken and unused
 */
TEST_P(CmdTimerTest, NotCurrentBucket) {
    adminConnection->executeInBucket(bucketName, [this](auto& c) {
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                "rbac_test", cb::mcbp::ClientOpcode::Scrub});
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::NotSupported, response.getStatus());
    });
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
 * Attempting to fetch timings for an empty histogram should succeed (but return
 * no samples)
 */
TEST_P(CmdTimerTest, EmptySuccess) {
    adminConnection->executeInBucket(bucketName, [this](auto& c) {
        c.stats("reset");
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucketName, cb::mcbp::ClientOpcode::Set});
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(0, getNumberOfOps(response.getDataString()));
    });
}
