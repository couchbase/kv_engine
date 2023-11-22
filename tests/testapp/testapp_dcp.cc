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

#include "testapp_client_test.h"

class DcpTest : public TestappClientTest {
protected:
    void SetUp() override {
        TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
        TestappClientTest::SetUp();
    }

    static auto getTestName() {
        return ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    auto createProducerConnection() {
        using cb::mcbp::Feature;
        auto connection = getAdminConnection().clone(
                true,
                {Feature::JSON, Feature::SNAPPY, Feature::SnappyEverywhere},
                getTestName());
        connection->authenticate("@admin", mcd_env->getPassword("@admin"));
        connection->selectBucket(bucketName);

        const auto rsp = connection->execute(BinprotDcpOpenCommand{
                getTestName(), cb::mcbp::request::DcpOpenPayload::Producer});
        EXPECT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                     << rsp.getDataView();
        return connection;
    }

    auto setupProducerWithStream() {
        auto producerConn = createProducerConnection();

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
    const auto conn = createProducerConnection();
    BinprotDcpStreamRequestCommand streamReq;
    streamReq.setDcpStartSeqno(0xdeadbeef);
    const auto rsp = conn->execute(streamReq);
    ASSERT_EQ(cb::mcbp::Status::Rollback, rsp.getStatus());

    const auto data = rsp.getData();
    ASSERT_EQ(sizeof(uint64_t), data.size());
    const auto* value = reinterpret_cast<const uint64_t*>(data.data());
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
    conn.sendCommand(
            BinprotDcpOpenCommand{"UnorderedExecutionNotSupported",
                                  cb::mcbp::request::DcpOpenPayload::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to select bucket
TEST_P(DcpTest, MB35904_DcpCantSelectBucket) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SelectBucket, name});
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to perform SASL AUTH
TEST_P(DcpTest, MB35928_DcpCantReauthenticate) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
            << "SASL LIST MECH should fail";
    try {
        conn->authenticate("@admin", "password");
        FAIL() << "DCP connections should not be able to reauthenticate";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, error.getReason())
                << "SASL AUTH should fail";
    }
}

TEST_P(DcpTest, CantDcpOpenTwice) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(BinprotDcpOpenCommand{
            "CantDcpOpenTwice", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_FALSE(rsp.isSuccess());
    const auto json = nlohmann::json::parse(rsp.getDataString());
    EXPECT_EQ("The connection is already opened as a DCP connection",
              json["error"]["context"]);
}

// Basic smoke test for "dcp" and "dcpagg" stat group - check they can be
// retrieved (regression tests for MB-48816).
TEST_P(DcpTest, DcpStats) {
    const auto conn = createProducerConnection();
    const auto stats = userConnection->stats("dcp");
    EXPECT_TRUE(stats.contains("ep_dcp_dead_conn_count"))
            << "dcp stats: " << stats.dump(2);
}

TEST_P(DcpTest, DcpAggStats) {
    const auto conn = createProducerConnection();
    userConnection->stats("dcpagg _");
}

TEST_P(DcpTest, DcpStreamStats) {
    const auto conn = setupProducerWithStream();
    const auto stats = userConnection->stats("dcp");
    EXPECT_TRUE(stats.contains(
            fmt::format("eq_dcpq:{}:stream_0_flags", getTestName())))
            << "dcp stats: " << stats.dump(2);
}
