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
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>
#include <algorithm>

/**
 * The CmdTimerTest operates on 3 buckets: "@no bucket", "default" and
 * "rbac_test".
 * "admin" user should have access to all buckets.
 * "luke" should have access to "default" and "rbac_test"
 * "smith" should have access to "rbac_test"
 * "jones" don't have stats access to any of the buckets.
 *
 * Each of these buckets contain 1 noop.
 */
class CmdTimerTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        TestappClientTest::SetUpTestCase();
        adminConnection->createBucket("rbac_test", "", BucketType::Memcached);

        for (const auto bucket : {"@no bucket@", "default", "rbac_test"}) {
            adminConnection->executeInBucket(bucket, [](auto& c) {
                c.execute(BinprotGenericCommand{opcode});
            });
        }
    }

    static void TearDownTestCase() {
        adminConnection->deleteBucket("rbac_test");
        TestappClientTest::TearDownTestCase();
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

    /// The command we want to get the timings for.
    static constexpr auto opcode = cb::mcbp::ClientOpcode::Noop;
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
            BinprotGetCmdTimerCommand{"/all/", opcode});
    EXPECT_TRUE(response.isSuccess());
    // One noop in "@no bucket@"; one in "default" and one in "rbac_test"
    EXPECT_EQ(3, getNumberOfOps(response.getDataString()));

    // Smith only has access to the bucket rbac_test - should only see numbers
    // from that.
    auto& c = getConnection();
    c.authenticate("smith");

    response = c.execute(BinprotGetCmdTimerCommand{"/all/", opcode});
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
    // Admin should have full access - either by unselecting any bucket
    // and issuing a request with the empty string, or by explicilty asking for
    // "@no bucket@".
    for (auto bucket : {"", "@no bucket@"}) {
        SCOPED_TRACE(fmt::format("for bucket '{}'", bucket));
        auto response = adminConnection->execute(
                BinprotGetCmdTimerCommand{bucket, opcode});
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    }

    // Smith attempting to access no-bucket should fail.
    auto& c = getConnection();
    c.authenticate("smith");

    for (auto bucket : {"", "@no bucket@"}) {
        SCOPED_TRACE(fmt::format("for bucket '{}'", bucket));
        auto response = c.execute(BinprotGetCmdTimerCommand{bucket, opcode});
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

    c.authenticate("jones");
    for (const auto& bucket : {"", "/all/", "rbac_test", bucketName.c_str()}) {
        const auto response =
                c.execute(BinprotGetCmdTimerCommand{bucket, opcode});
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    }

    // Make sure it doesn't work for the "current selected bucket"
    c.selectBucket("rbac_test");
    const auto response = c.execute(BinprotGetCmdTimerCommand{"", opcode});
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    c.reconnect();
}

TEST_P(CmdTimerTest, CurrentBucket) {
    adminConnection->executeInBucket("rbac_test", [this](auto& c) {
        for (const auto& bucket : {"", "rbac_test"}) {
            const auto response =
                    c.execute(BinprotGetCmdTimerCommand{bucket, opcode});
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
        const auto response =
                c.execute(BinprotGetCmdTimerCommand{"rbac_test", opcode});
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::NotSupported, response.getStatus());
    });
}

/**
 * We should get no access for unknown buckets
 */
TEST_P(CmdTimerTest, NonexistentBucket) {
    auto& c = getConnection();
    const auto response =
            c.execute(BinprotGetCmdTimerCommand{"asdfasdfasdf", opcode});
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
}

/**
 * Attempting to fetch timings for an empty histogram should succeed (but return
 * no samples)
 */
TEST_P(CmdTimerTest, EmptySuccess) {
    adminConnection->executeInBucket(bucketName, [this](auto& c) {
        const auto response = c.execute(BinprotGetCmdTimerCommand{
                bucketName, cb::mcbp::ClientOpcode::Set});
        EXPECT_TRUE(response.isSuccess());
        EXPECT_EQ(0, getNumberOfOps(response.getDataString()));
    });
}

/**
 * Jones only have access to the bucket rbac_test, but is missing the
 * simple-stats privilege. It should not be able to run the stats command
 * through impersonate unless he is granted the extra privilege.
 */
TEST_P(CmdTimerTest, ImpersonateNoAccess) {
    using namespace cb::mcbp::request;
    auto conn = adminConnection->clone();
    conn->authenticate("almighty");

    conn->executeInBucket("rbac_test", [](auto& conn) {
        auto cmd = BinprotGetCmdTimerCommand{"", opcode};
        cmd.addFrameInfo(ImpersonateUserFrameInfo("jones"));
        const auto response = conn.execute(cmd);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    });

    conn->executeInBucket("rbac_test", [](auto& conn) {
        auto cmd = BinprotGetCmdTimerCommand{"/all/", opcode};
        cmd.addFrameInfo(ImpersonateUserFrameInfo("jones"));
        const auto response = conn.execute(cmd);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
    });

    // But we may grant the user the extra privilege...
    conn->executeInBucket("rbac_test", [this](auto& conn) {
        auto cmd = BinprotGetCmdTimerCommand{"", opcode};
        cmd.addFrameInfo(ImpersonateUserFrameInfo("jones"));
        cmd.addFrameInfo(ImpersonateUserExtraPrivilegeFrameInfo(
                cb::rbac::Privilege::SimpleStats));
        const auto response = conn.execute(cmd);
        EXPECT_TRUE(response.isSuccess()) << response.getStatus();
        EXPECT_EQ(1, getNumberOfOps(response.getDataString()));
    });

    // But we may grant the user the extra privilege (should also work for
    // all, but you still won't have access to "@no bucket@")
    conn->executeInBucket("rbac_test", [this](auto& conn) {
        auto cmd = BinprotGetCmdTimerCommand{"/all/", opcode};
        cmd.addFrameInfo(ImpersonateUserFrameInfo("jones"));
        cmd.addFrameInfo(ImpersonateUserExtraPrivilegeFrameInfo(
                cb::rbac::Privilege::SimpleStats));
        const auto response = conn.execute(cmd);
        EXPECT_TRUE(response.isSuccess()) << response.getStatus();
        EXPECT_EQ(2, getNumberOfOps(response.getDataString()));
    });

    // But we may grant the user the extra privileges and also get "@no bucket@"
    conn->executeInBucket("rbac_test", [this](auto& conn) {
        auto cmd = BinprotGetCmdTimerCommand{"/all/", opcode};
        cmd.addFrameInfo(ImpersonateUserFrameInfo("jones"));
        cmd.addFrameInfo(ImpersonateUserExtraPrivilegeFrameInfo(
                cb::rbac::Privilege::SimpleStats));
        cmd.addFrameInfo(ImpersonateUserExtraPrivilegeFrameInfo(
                cb::rbac::Privilege::Stats));
        const auto response = conn.execute(cmd);
        EXPECT_TRUE(response.isSuccess()) << response.getStatus();
        EXPECT_EQ(3, getNumberOfOps(response.getDataString()));
    });
}
