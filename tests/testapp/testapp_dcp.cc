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
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(DcpTest, TestDcpOpenCantBeProducerAndConsumer) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{
            "ewb_internal:1",
            cb::mcbp::request::DcpOpenPayload::Producer |
                    cb::mcbp::request::DcpOpenPayload::Notifier});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(DcpTest, TestDcpNotfierCantBeNoValue) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{
            "ewb_internal:1",
            cb::mcbp::request::DcpOpenPayload::NoValue |
                    cb::mcbp::request::DcpOpenPayload::Notifier});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(DcpTest, TestDcpNotfierCantIncludeXattrs) {
    auto& conn = getConnection();

    conn.sendCommand(BinprotDcpOpenCommand{
            "ewb_internal:1",
            cb::mcbp::request::DcpOpenPayload::IncludeXattrs |
                    cb::mcbp::request::DcpOpenPayload::Notifier});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

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

TEST_P(DcpTest, DISABLED_UnorderedExecutionNotSupported) {
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
    conn.selectBucket("default");
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
    conn.selectBucket("default");
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
