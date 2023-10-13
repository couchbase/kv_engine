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
protected:
    void SetUp() override {
        if (mcd_env->getTestBucket().getName() == "default_engine") {
            GTEST_SKIP() << "Skipping as DCP not supported";
        }
        TestappClientTest::SetUp();
    }

    std::unique_ptr<MemcachedConnection> setupProducerWithStream(
            const std::string& name) {
        BinprotResponse rsp;
        auto producerConn = getAdminConnection().clone(true);
        producerConn->authenticate("@admin", mcd_env->getPassword("@admin"));
        producerConn->selectBucket(bucketName);
        producerConn->sendCommand(BinprotDcpOpenCommand{
                name, cb::mcbp::request::DcpOpenPayload::Producer});
        producerConn->recvResponse(rsp);
        EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

        BinprotDcpStreamRequestCommand streamReq;
        streamReq.setDcpReserved(0);
        streamReq.setDcpStartSeqno(0);
        streamReq.setDcpEndSeqno(0xffffffff);
        streamReq.setDcpVbucketUuid(0);
        streamReq.setDcpSnapStartSeqno(0);
        streamReq.setDcpSnapEndSeqno(0xfffffff);
        streamReq.setVBucket(Vbid(0));
        producerConn->sendCommand(streamReq);
        // Instead of calling recvResponse(), which requires a Response and
        // throws on Request sent by DCP, use recvFrame and ignore any DCP
        // messages.
        Frame frame;
        producerConn->recvFrame(frame);
        while (cb::mcbp::is_server_magic(frame.getMagic())) {
            producerConn->recvFrame(frame,
                                    cb::mcbp::ClientOpcode::Invalid,
                                    std::chrono::seconds(5));
        }
        if (cb::mcbp::is_client_magic(frame.getMagic())) {
            EXPECT_TRUE(cb::mcbp::isStatusSuccess(
                    frame.getResponse()->getStatus()));
        }

        return producerConn;
    }
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
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);
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
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);
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
    auto conn = adminConnection->clone();
    conn->authenticate("@admin", mcd_env->getPassword("@admin"));
    conn->setDatatypeJson(true);
    conn->selectBucket(bucketName);
    auto rsp = conn->execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());

    rsp = conn->execute(
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
    auto conn = adminConnection->clone();
    conn->authenticate("@admin", mcd_env->getPassword("@admin"));
    conn->setDatatypeJson(true);
    conn->selectBucket(bucketName);
    auto rsp = conn->execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());

    rsp = conn->execute(BinprotDcpOpenCommand{
            "ewb_internal:1", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_FALSE(rsp.isSuccess());
    auto json = nlohmann::json::parse(rsp.getDataString());
    EXPECT_EQ("The connection is already opened as a DCP connection",
              json["error"]["context"]);
}

// Basic smoke test for "dcp" and "dcpagg" stat group - check they can be
// retrieved (regression tests for MB-48816).
TEST_P(DcpTest, DcpStats) {
    auto& conn = getConnection();
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);
    conn.sendCommand(BinprotDcpOpenCommand{
            "testapp_dcp", cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());

    auto stats = userConnection->stats("dcp");
    EXPECT_TRUE(stats.contains("ep_dcp_dead_conn_count"))
            << "dcp stats: " << stats.dump(2);
}

TEST_P(DcpTest, DcpAggStats) {
    auto& conn = getConnection();
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);
    conn.sendCommand(BinprotDcpOpenCommand{
            "testapp_dcp", cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());

    userConnection->stats("dcpagg _");
}

TEST_P(DcpTest, DcpStreamStats) {
    auto& conn = getAdminConnection();
    conn.selectBucket(bucketName);
    conn.sendCommand(BinprotDcpOpenCommand{
            "testapp_dcp", cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());

    auto producerConn = setupProducerWithStream("testapp_dcp");

    auto stats = userConnection->stats("dcp");
    EXPECT_TRUE(stats.contains("eq_dcpq:testapp_dcp:stream_0_flags"))
            << "dcp stats: " << stats.dump(2);
}
