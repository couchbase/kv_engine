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
#include <nlohmann/json.hpp>

class DcpTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappClientTest::SetUp();
    }

    static auto getTestName() {
        return ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    auto createProducerConnection(
            const std::function<void(MemcachedConnection&)>& on_auth_callback =
                    {}) {
        using cb::mcbp::Feature;
        auto connection = getAdminConnection().clone(
                true,
                {Feature::JSON, Feature::SNAPPY, Feature::SnappyEverywhere},
                getTestName());
        connection->authenticate("@admin");
        if (on_auth_callback) {
            on_auth_callback(*connection);
        }
        connection->selectBucket(bucketName);

        const auto rsp = connection->execute(BinprotDcpOpenCommand{
                getTestName(), cb::mcbp::DcpOpenFlag::Producer});
        EXPECT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                     << rsp.getDataView();
        return connection;
    }

    auto setupProducerWithStream(
            const std::function<void(MemcachedConnection&)>& on_auth_callback =
                    {}) {
        auto producerConn = createProducerConnection(on_auth_callback);

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

    const auto data = rsp.getDataView();
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
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    conn.setUnorderedExecutionMode(ExecutionMode::Unordered);
    conn.sendCommand(BinprotDcpOpenCommand{"UnorderedExecutionNotSupported",
                                           cb::mcbp::DcpOpenFlag::Producer});

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
        conn->authenticate("@admin");
        FAIL() << "DCP connections should not be able to reauthenticate";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, error.getReason())
                << "SASL AUTH should fail";
    }
}

TEST_P(DcpTest, CantDcpOpenTwice) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(BinprotDcpOpenCommand{
            "CantDcpOpenTwice", cb::mcbp::DcpOpenFlag::Producer});
    ASSERT_FALSE(rsp.isSuccess());
    const auto json = rsp.getDataJson();
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

/// Verify that we log unclean DCP disconnects
TEST_P(DcpTest, MB60706) {
    nlohmann::json json;

    std::string value(2048 * 1024, 'a');

    for (int ii = 0; ii < 10; ++ii) {
        store_document(fmt::format("MB60706_{}", ii), value, 0, 0, false);
    }

    auto conn = setupProducerWithStream([&json](auto& c) {
        c.stats([&json](auto k, auto v) { json = nlohmann::json::parse(v); },
                "connections self");
    });

    // Wait as long as the backfill have started
    Frame frame;
    do {
        conn->recvFrame(frame);
    } while (frame.getHeader()->isResponse() ||
             frame.getRequest()->getClientOpcode() !=
                     cb::mcbp::ClientOpcode::DcpMutation);

    conn->close();
    const auto id = json["socket"].get<int>();
    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{10};
    bool found = false;
    std::string search_prefix =
            fmt::format(R"(INFO Releasing DCP connection )", id);
    json.clear();
    do {
        bool found_hello = false;
        mcd_env->iterateLogLines([&id,
                                  &found,
                                  &found_hello,
                                  &search_prefix,
                                  &json](auto line) {
            if (found_hello) {
                auto idx = line.find(search_prefix);
                found = idx != std::string_view::npos;
                if (found) {
                    std::string info(line.substr(idx + search_prefix.length()));
                    json = nlohmann::json::parse(info)["description"];
                    // stop searching
                    return false;
                }
            } else {
                auto idx = line.find("HELO");
                if (idx != std::string_view::npos) {
                    auto ctx =
                            nlohmann::json::parse(line.substr(line.find('{')));
                    found_hello = ctx["conn_id"] == id &&
                                  ctx["client"]["a"] == "MB60706/McbpSsl";
                }
            }
            return true;
        });
        if (found) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    } while (std::chrono::steady_clock::now() < timeout);
    ASSERT_TRUE(found) << "Did not locate unclean shutdown";
#ifdef __linux__
    EXPECT_TRUE(json.contains("SIOCINQ"));
    EXPECT_TRUE(json.contains("SIOCOUTQ"));
#endif
    EXPECT_TRUE(json.contains("sendqueue"));
    ASSERT_TRUE(json["sendqueue"].is_object());
    const auto& sendq = json["sendqueue"];
    EXPECT_TRUE(sendq.contains("actual"));
    EXPECT_TRUE(sendq.contains("last"));
    EXPECT_TRUE(sendq.contains("size"));
    EXPECT_TRUE(sendq.contains("term"));
    EXPECT_TRUE(sendq["term"].get<bool>());
    EXPECT_TRUE(json.contains("socket_options"));
    EXPECT_TRUE(json.contains("blocked_send_queue_duration"));
    ASSERT_TRUE(json["socket_options"].is_object());
}
