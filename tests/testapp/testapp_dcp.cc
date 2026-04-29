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
#include <mcbp/protocol/dcp_cache_transfer_buffer.h>
#include <nlohmann/json.hpp>
#include <map>

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
        EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus() << std::endl
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

/**
 * Verify a producer can stream the cache via a CacheTransfer (CTS) stream.
 *
 * Stores a number of documents, then opens a producer connection and issues a
 * stream-request with DcpAddStreamFlag::CacheTransfer | ActiveVbOnly. The
 * start_seqno doubles as the maxSeqno used by CacheTransferStream to filter
 * eligible items - we set it to the vbucket's high-seqno so every stored doc
 * is eligible. end_seqno is set equal to start_seqno so the stream ends after
 * the cache-transfer rather than switching to a regular ActiveStream.
 *
 * The test then reads DCP frames and verifies that the DcpCacheTransfer
 * message(s) contain every stored key/value before the stream is ended.
 */
TEST_P(DcpTest, CacheTransferStream) {
    std::map<std::string, std::string> expected;
    for (int ii = 0; ii < 5; ++ii) {
        auto key = fmt::format("CacheTransferStream_{}", ii);
        auto value = fmt::format("value_{}", ii);
        store_document(key, value);
        expected.emplace(std::move(key), std::move(value));
    }

    // Determine the vbucket high-seqno and UUID so we can set start_seqno
    // (the CacheTransfer maxSeqno) such that every stored item is in scope,
    // and supply a valid UUID so the producer does not request a rollback.
    uint64_t highSeqno{0};
    uint64_t vbUuid{0};
    userConnection->stats(
            [&highSeqno, &vbUuid](auto& k, auto& v) {
                if (k == "vb_0:high_seqno") {
                    highSeqno = std::stoull(v);
                } else if (k == "vb_0:uuid") {
                    vbUuid = std::stoull(v);
                }
            },
            "vbucket-details 0");
    ASSERT_GE(highSeqno, expected.size());
    ASSERT_NE(0, vbUuid);

    const auto conn = createProducerConnection();

    BinprotDcpStreamRequestCommand streamReq;
    streamReq.setDcpFlags(cb::mcbp::DcpAddStreamFlag::CacheTransfer |
                          cb::mcbp::DcpAddStreamFlag::ActiveVbOnly);
    streamReq.setDcpReserved(0);
    streamReq.setDcpStartSeqno(highSeqno);
    streamReq.setDcpEndSeqno(highSeqno);
    streamReq.setDcpVbucketUuid(vbUuid);
    streamReq.setDcpSnapStartSeqno(highSeqno);
    streamReq.setDcpSnapEndSeqno(highSeqno);
    streamReq.setVBucket(Vbid(0));
    conn->sendCommand(streamReq);

    std::map<std::string, std::string> received;
    bool seenStreamReqResponse = false;
    bool seenCacheTransfer = false;
    bool streamEnded = false;
    while (!streamEnded) {
        Frame frame;
        conn->recvFrame(frame,
                        cb::mcbp::ClientOpcode::Invalid,
                        std::chrono::seconds(10));
        if (cb::mcbp::is_server_magic(frame.getMagic())) {
            continue;
        }
        if (frame.getHeader()->isResponse()) {
            ASSERT_FALSE(seenStreamReqResponse);
            ASSERT_TRUE(
                    cb::mcbp::isStatusSuccess(frame.getResponse()->getStatus()))
                    << "Stream request failed: "
                    << frame.getResponse()->getStatus();
            seenStreamReqResponse = true;
            continue;
        }

        const auto* req = frame.getRequest();
        switch (req->getClientOpcode()) {
        case cb::mcbp::ClientOpcode::DcpCacheTransfer: {
            seenCacheTransfer = true;
            cb::mcbp::DcpCacheTransferBuffer buffer(req->getValueString());
            for (auto it = buffer.begin(); it != buffer.end(); ++it) {
                ASSERT_FALSE(it.hasError())
                        << "DcpCacheTransferBuffer iteration error: "
                        << it.getError().dump();
                // Strip the 1-byte default collection prefix from the wire key.
                auto wireKey = it->getKey();
                ASSERT_FALSE(wireKey.empty());
                received.emplace(std::string{wireKey.substr(1)},
                                 std::string{it->getValue()});
            }
            break;
        }
        case cb::mcbp::ClientOpcode::DcpStreamEnd:
            streamEnded = true;
            break;
        default:
            // Ignore other DCP messages (e.g. DcpNoop, DcpControl).
            break;
        }
    }

    EXPECT_TRUE(seenStreamReqResponse);
    EXPECT_TRUE(seenCacheTransfer);
    EXPECT_EQ(expected, received);
}

/// Verify that we log unclean DCP disconnects. Disabled as part of the
/// change to reduce the number of notifications (this changes the scheduling
/// order in the test)
TEST_P(DcpTest, MB60706) {
    nlohmann::json json;

    std::string value(2_MiB, 'a');

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
        // This test needs to wait for the 1Mib documents, not documents from
        // other tests
        if (frame.getRequest()->getClientOpcode() ==
                    cb::mcbp::ClientOpcode::DcpMutation &&
            frame.getRequest()->getValue().size() == value.size()) {
            // DCP has started sending data
            conn->close();
            // Delete bucket forces the message we're looking for.
            adminConnection->deleteBucket(bucketName);
            break;
        }
    } while (frame.getHeader()->isRequest());

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
    EXPECT_TRUE(json["io_layer"].contains("SIOCINQ"));
    EXPECT_TRUE(json["io_layer"].contains("SIOCOUTQ"));
#endif
    EXPECT_TRUE(json["io_layer"].contains("input_len"));
    EXPECT_TRUE(json["io_layer"].contains("output_len"));
    EXPECT_TRUE(json["io_layer"].contains("EV_READ"));
    EXPECT_TRUE(json["io_layer"].contains("EV_WRITE"));
    ASSERT_TRUE(json["io_layer"].contains("socket_options"));
    ASSERT_TRUE(json["io_layer"]["socket_options"].is_object());
    EXPECT_TRUE(json.contains("sendqueue"));
    ASSERT_TRUE(json["sendqueue"].is_object());
    const auto& sendq = json["sendqueue"];
    EXPECT_TRUE(sendq.contains("last"));
    EXPECT_TRUE(sendq.contains("size"));
    EXPECT_TRUE(sendq.contains("term"));
    EXPECT_TRUE(sendq["term"].get<bool>());
    EXPECT_TRUE(json.contains("blocked_send_queue_duration"));
}
