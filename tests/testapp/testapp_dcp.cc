/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * In this file you'll find unit tests related to the DCP subsystem
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <xattr/blob.h>
#include <xattr/utils.h>

class DcpTest : public TestappClientTest {

};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         DcpTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

/**
 * Make sure that the rollback sequence number in the response isn't being
 * stripped / replaced with an error object
 */
TEST_P(DcpTest, MB24145_RollbackShouldContainSeqno) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());

    BinprotDcpStreamRequestCommand streamReq;
    streamReq.setDcpStartSeqno(1);
    conn.sendCommand(streamReq);
    conn.recvResponse(rsp);
    ASSERT_EQ(cb::mcbp::Status::Rollback, rsp.getStatus());

    auto data = rsp.getData();
    ASSERT_EQ(sizeof(uint64_t), data.size());
    auto* value = reinterpret_cast<const uint64_t*>(data.data());
    EXPECT_EQ(0, *value);

}

TEST_P(DcpTest, UnorderedExecutionNotSupported) {
    // Verify that it isn't possible to run a DCP open command
    // on a connection which is set to unordered execution mode.
    // Ideally we should have verified each of the available DCP
    // packets, but our test framework does not have methods to
    // create all of them. The DCP validators does however
    // all call a common validator method to check this
    // restriction. Once the full DCP test suite is implemented
    // we should extend this test to validate all of the
    // various commands.
    auto& conn = getConnection();
    conn.setUnorderedExecutionMode(ExecutionMode::Unordered);
    conn.sendCommand(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to select bucket
TEST_P(DcpTest, MB35904_DcpCantSelectBucket) {
    auto& conn = getAdminConnection();
    conn.selectBucket(bucketName);
    auto rsp = conn.execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());

    rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SelectBucket, name});
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to perform SASL AUTH
TEST_P(DcpTest, MB35928_DcpCantReauthenticate) {
    auto& conn = getAdminConnection();
    conn.selectBucket(bucketName);
    auto rsp = conn.execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());

    rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
            << "SASL LIST MECH should fail";

    try {
        conn.authenticate("@admin", "password", "plain");
        FAIL() << "DCP connections should not be able to reauthenticate";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, error.getReason())
                << "SASL AUTH should fail";
    }
}

TEST_P(DcpTest, CantDcpOpenTwice) {
    auto& conn = getAdminConnection();
    conn.selectBucket(bucketName);
    auto rsp = conn.execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());

    rsp = conn.execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_FALSE(rsp.isSuccess());
    auto json = nlohmann::json::parse(rsp.getDataString());
    EXPECT_EQ("The connection is already opened as a DCP connection",
              json["error"]["context"]);
}
