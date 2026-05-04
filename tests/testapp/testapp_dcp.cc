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
#include <engines/ep/src/dcp/dcp-types.h>
#include <mcbp/codec/frameinfo.h>
#include <mcbp/protocol/dcp_cache_transfer_buffer.h>
#include <nlohmann/json.hpp>
#include <map>

using namespace cb::mcbp;
using namespace cb::mcbp::request;
using namespace cb::mcbp::subdoc;
using cb::durability::Level;
using KeyAndSnapshotPair =
        std::pair<std::string, std::optional<DcpSnapshotMarkerV1Payload>>;
using KeyAndSnapshotVector = std::vector<KeyAndSnapshotPair>;

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
                    {},
            DcpOpenFlag flags = DcpOpenFlag::None,
            const std::function<void(MemcachedConnection&)>&
                    on_stream_created_callback = {}) {
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
                getTestName(), DcpOpenFlag::Producer | flags});
        EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus() << std::endl
                                     << rsp.getDataView();
        if (on_stream_created_callback) {
            on_stream_created_callback(*connection);
        }
        return connection;
    }

    auto setupProducerWithStream(
            const std::function<void(MemcachedConnection&)>& on_auth_callback =
                    {},
            DcpOpenFlag openFlags = DcpOpenFlag::None,
            DcpAddStreamFlag streamFlags = DcpAddStreamFlag::None,
            uint64_t uuid = 0,
            uint64_t startSeqno = 0,
            const std::function<void(MemcachedConnection&)>&
                    on_stream_created_callback = {}) {
        auto producerConn = createProducerConnection(
                on_auth_callback, openFlags, on_stream_created_callback);

        BinprotDcpStreamRequestCommand streamReq;
        streamReq.setDcpReserved(0);
        streamReq.setDcpStartSeqno(startSeqno);
        streamReq.setDcpEndSeqno(std::numeric_limits<uint64_t>::max());
        streamReq.setDcpVbucketUuid(uuid);
        streamReq.setDcpSnapStartSeqno(startSeqno);
        streamReq.setDcpSnapEndSeqno(std::numeric_limits<uint64_t>::max());
        streamReq.setVBucket(Vbid(0));
        streamReq.setDcpFlags(streamFlags);

        producerConn->sendCommand(streamReq);
        // Instead of calling recvResponse(), which requires a Response and
        // throws on Request sent by DCP, use recvFrame and ignore any DCP
        // messages.
        Frame frame;
        producerConn->recvFrame(frame);
        while (is_server_magic(frame.getMagic())) {
            producerConn->recvFrame(
                    frame, ClientOpcode::Invalid, std::chrono::seconds(5));
        }
        if (is_client_magic(frame.getMagic())) {
            auto* rsp = frame.getResponse();
            if (!isStatusSuccess(rsp->getStatus()) ||
                rsp->getStatus() == Status::Rollback) {
                throw std::runtime_error(
                        fmt::format("DCP_STREAM_REQ failed: {}",
                                    rsp->to_json(true).dump()));
            }
        }

        return producerConn;
    }

    void verifyMb26074(bool ephemeral);

    auto upsert(std::string key,
                bool durable = false,
                std::string extra_value = {}) {
        BinprotSubdocMultiMutationCommand cmd(
                key,
                {{.opcode = ClientOpcode::SubdocDictUpsert,
                  .flags = PathFlag::Mkdir_p | PathFlag::XattrPath,
                  .path = "_sys.trusted",
                  .value = "false"},
                 {.opcode = ClientOpcode::SubdocDictUpsert,
                  .flags = PathFlag::Mkdir_p,
                  .path = "user.name",
                  .value = "\"John Doe\""},
                 {.opcode = ClientOpcode::SubdocDictUpsert,
                  .flags = PathFlag::Mkdir_p,
                  .path = "user.extra",
                  .value = fmt::format("\"{}\"", extra_value)}},
                DocFlag::Mkdoc);
        if (durable) {
            cmd.addFrameInfo(
                    DurabilityFrameInfo{Level::MajorityAndPersistOnMaster});
        }

        const auto rsp = BinprotSubdocMultiMutationResponse{
                userConnection->execute(cmd)};
        if (rsp.isSuccess()) {
            return true;
        }
        if (rsp.getStatus() == Status::Etmpfail) {
            return false;
        }
        throw std::runtime_error(fmt::format(
                "Failed to store document with system xattr: {} - {}",
                rsp.getStatus(),
                rsp.getDataView()));
    };

    auto remove(std::string key, bool durable = false) {
        BinprotRemoveCommand cmd{std::move(key)};
        if (durable) {
            cmd.addFrameInfo(
                    DurabilityFrameInfo{Level::MajorityAndPersistOnMaster});
        }

        const auto rsp = userConnection->execute(cmd);
        ASSERT_EQ(Status::Success, rsp.getStatus())
                << "Failed to delete document" << std::endl;
    };

    std::pair<KeyAndSnapshotVector, KeyAndSnapshotVector> drainStream(
            DcpOpenFlag open_flag,
            DcpAddStreamFlag stream_flag,
            uint64_t start_seqno);
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
    ASSERT_EQ(Status::Rollback, rsp.getStatus());

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
                                           DcpOpenFlag::Producer});

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to select bucket
TEST_P(DcpTest, MB35904_DcpCantSelectBucket) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(
            BinprotGenericCommand{ClientOpcode::SelectBucket, name});
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(Status::NotSupported, rsp.getStatus());
}

/// DCP connections should not be able to perform SASL AUTH
TEST_P(DcpTest, MB35928_DcpCantReauthenticate) {
    const auto conn = createProducerConnection();
    const auto rsp =
            conn->execute(BinprotGenericCommand{ClientOpcode::SaslListMechs});
    EXPECT_EQ(Status::NotSupported, rsp.getStatus())
            << "SASL LIST MECH should fail";
    try {
        conn->authenticate("@admin");
        FAIL() << "DCP connections should not be able to reauthenticate";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(Status::NotSupported, error.getReason())
                << "SASL AUTH should fail";
    }
}

TEST_P(DcpTest, CantDcpOpenTwice) {
    const auto conn = createProducerConnection();
    const auto rsp = conn->execute(
            BinprotDcpOpenCommand{"CantDcpOpenTwice", DcpOpenFlag::Producer});
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
    streamReq.setDcpFlags(DcpAddStreamFlag::CacheTransfer |
                          DcpAddStreamFlag::ActiveVbOnly);
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
        conn->recvFrame(frame, ClientOpcode::Invalid, std::chrono::seconds(10));
        if (is_server_magic(frame.getMagic())) {
            continue;
        }
        if (frame.getHeader()->isResponse()) {
            ASSERT_FALSE(seenStreamReqResponse);
            ASSERT_TRUE(isStatusSuccess(frame.getResponse()->getStatus()))
                    << "Stream request failed: "
                    << frame.getResponse()->getStatus();
            seenStreamReqResponse = true;
            continue;
        }

        const auto* req = frame.getRequest();
        switch (req->getClientOpcode()) {
        case ClientOpcode::DcpCacheTransfer: {
            seenCacheTransfer = true;
            DcpCacheTransferBuffer buffer(req->getValueString());
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
        case ClientOpcode::DcpStreamEnd:
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

// We need our own namespace to avoid name clash with the DcpProducer class
// in ep-engine
namespace testapp {

class DcpProducer {
public:
    DcpProducer(MemcachedConnection& conn,
                std::string agent,
                const std::string& bucket)
        : connection(conn.clone(
                  true,
                  {Feature::JSON, Feature::SNAPPY, Feature::SnappyEverywhere},
                  std::move(agent))) {
        connection->authenticate("@admin");
        connection->selectBucket(bucket);
    }

    void open(std::string dcpname, DcpOpenFlag flags) {
        auto rsp = connection->execute(BinprotDcpOpenCommand{
                std::move(dcpname), DcpOpenFlag::Producer | flags});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    fmt::format("DCP_STREAM_REQ failed: {} - {}",
                                rsp.getStatus(),
                                rsp.getDataView()));
        }

        std::vector<std::pair<std::string_view, std::string>> controls = {
                {DcpControlKeys::SetPriority, "high"},
                {DcpControlKeys::SupportsCursorDroppingVulcan, "true"},
                {DcpControlKeys::SupportsHifiMfu, "true"},
                {DcpControlKeys::SendStreamEndOnClientCloseStream, "true"},
                {DcpControlKeys::EnableExpiryOpcode, "true"},
                {DcpControlKeys::SetNoopInterval, "360"},
                {DcpControlKeys::EnableNoop, "true"},
                {DcpControlKeys::ConnectionBufferSize, "128"}};

        for (const auto& [key, value] : controls) {
            rsp = connection->execute(
                    BinprotDcpControlCommand{std::string{key}, value});
            if (!rsp.isSuccess()) {
                fmt::print(stderr,
                           "Failed to set {} to {}: {}",
                           key,
                           value,
                           rsp.getStatus());
            }
        }
    }

    void addStream(uint64_t uuid,
                   uint64_t startSeqno = 0,
                   uint64_t snapStartSeqno = 0,
                   uint64_t snapEndSeqno = std::numeric_limits<uint64_t>::max(),
                   DcpAddStreamFlag flags = DcpAddStreamFlag::None) {
        BinprotDcpStreamRequestCommand streamReq;
        streamReq.setDcpReserved(0);
        streamReq.setDcpStartSeqno(startSeqno);
        streamReq.setDcpEndSeqno(std::numeric_limits<uint64_t>::max());
        streamReq.setDcpVbucketUuid(uuid);
        streamReq.setDcpSnapStartSeqno(snapStartSeqno);
        streamReq.setDcpSnapEndSeqno(snapEndSeqno);
        streamReq.setVBucket(Vbid(0));
        streamReq.setDcpFlags(flags);
        connection->sendCommand(streamReq);

        Frame frame;
        connection->recvFrame(frame);
        while (is_server_magic(frame.getMagic())) {
            connection->recvFrame(
                    frame, ClientOpcode::Invalid, std::chrono::seconds(5));
        }
        if (is_client_magic(frame.getMagic())) {
            auto* rsp = frame.getResponse();
            if (!isStatusSuccess(rsp->getStatus()) ||
                rsp->getStatus() == Status::Rollback) {
                throw std::runtime_error(
                        fmt::format("DCP_STREAM_REQ failed: {}",
                                    rsp->to_json(true).dump()));
            }
        }
    }

    void drive(
            std::function<std::pair<bool, bool>(const Request&)> on_message) {
        Frame frame;
        do {
            connection->recvFrame(frame);
            if (is_request(frame.getMagic())) {
                const auto& req = *frame.getRequest();

                if (req.getClientOpcode() == ClientOpcode::DcpNoop) {
                    BinprotCommandResponse rsp{ClientOpcode::DcpNoop,
                                               req.getOpaque()};
                    connection->sendCommand(rsp);
                    continue;
                }

                auto [stop, send_ack] = on_message(*frame.getRequest());
                pending_buffer_size += getDcpBufferSize(*frame.getRequest());
                if (send_ack) {
                    connection->sendCommand(
                            BinprotDcpBufferAck{pending_buffer_size});
                    pending_buffer_size = 0;
                }

                if (stop) {
                    return;
                }
            }
        } while (true);
    }

protected:
    static std::size_t getDcpBufferSize(const Request& request) {
        switch (request.getClientOpcode()) {
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpOsoSnapshot:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::DcpPrepare:
        case ClientOpcode::DcpSeqnoAcknowledged:
        case ClientOpcode::DcpCommit:
        case ClientOpcode::DcpAbort:
        case ClientOpcode::DcpSeqnoAdvanced:
        case ClientOpcode::DcpCacheTransfer:
            return request.getBodylen() + sizeof(Header);
        default:
            return 0;
        }
    }

    std::unique_ptr<MemcachedConnection> connection;
    std::size_t pending_buffer_size{0};
};
} // namespace testapp

std::pair<KeyAndSnapshotVector, KeyAndSnapshotVector> DcpTest::drainStream(
        DcpOpenFlag open_flag,
        DcpAddStreamFlag stream_flag,
        uint64_t start_seqno) {
    uint64_t uuid = 0xdeadbeef;
    userConnection->stats(
            [&uuid](auto k, auto v) {
                if (k == "vb_0:uuid") {
                    uuid = std::stoull(v);
                }
            },
            "vbucket-details 0");

    testapp::DcpProducer producer(*userConnection, getTestName(), bucketName);
    producer.open(getTestName(), open_flag);
    producer.addStream(uuid,
                       start_seqno,
                       start_seqno,
                       std::numeric_limits<uint64_t>::max(),
                       stream_flag);

    std::optional<DcpSnapshotMarkerV1Payload> snapshot;

    std::vector<KeyAndSnapshotPair> keys;
    std::vector<KeyAndSnapshotPair> deletions;

    producer.drive([&](const Request& req) {
        if (req.getClientOpcode() == ClientOpcode::DcpStreamEnd) {
            return std::make_pair(true, true);
        }

        if (req.getClientOpcode() == ClientOpcode::DcpSnapshotMarker) {
            Expects(req.getExtlen() == sizeof(DcpSnapshotMarkerV1Payload));
            snapshot = *reinterpret_cast<const DcpSnapshotMarkerV1Payload*>(
                    req.getExtdata().data());
        } else if (req.getClientOpcode() == ClientOpcode::DcpDeletion) {
            deletions.emplace_back(req.getKeyString(), snapshot);
        } else if (req.getClientOpcode() == ClientOpcode::DcpMutation) {
            keys.emplace_back(req.getKeyString(), snapshot);
        }
        return std::make_pair(false, true);
    });
    return {std::move(keys), std::move(deletions)};
}

static bool isDiskSnapshot(const DcpSnapshotMarkerFlag flag) {
    return static_cast<uint32_t>(DcpSnapshotMarkerFlag::Disk) ==
           (static_cast<uint32_t>(flag) &
            static_cast<uint32_t>(DcpSnapshotMarkerFlag::Disk));
}

void DcpTest::verifyMb26074(bool ephemeral) {
    const auto extra_bucket_config =
            fmt::format("checkpoint_max_size=512{}",
                        ephemeral ? ";bucket_type=ephemeral" : "");

    mcd_env->getTestBucket().reloadBucket(*adminConnection,
                                          bucketName,
                                          TestBucket::BucketCreateMode::Clean,
                                          extra_bucket_config);
    rebuildUserConnection(isTlsEnabled());
    prepare(*userConnection);
    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SELECT_BUCKET,
             cb::mcbp::Feature::SubdocReplaceBodyWithXattr,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON,
             cb::mcbp::Feature::Collections}};
    userConnection->setFeatures(features);
    userConnection->selectBucket(bucketName);

    // key is name and this must be a collection ket (default collection is
    // fine)
    name.insert(0, 1, '\0');
    upsert(name, !ephemeral);
    remove(name, !ephemeral);
    upsert(fmt::format("{}-1", name), !ephemeral);

    // Verify that we skip the delete when we tell it to do so from an
    // initial backfill
    {
        auto [mutations, deletions] =
                drainStream(DcpOpenFlag::SkipDeletesInInitialBackfill,
                            DcpAddStreamFlag::ToLatest,
                            0);

        EXPECT_TRUE(deletions.empty())
                << "Expected not to see delete in DCP stream with "
                   "SkipDeletesInInitialBackfill, but it was present";
        ASSERT_EQ(1, mutations.size());
        EXPECT_EQ(fmt::format("{}-1", name), mutations[0].first);
    }

    // Verify that we don't skip the delete when we tell it to do a normal
    // initial backfill
    {
        auto [mutations, deletions] =
                drainStream({}, DcpAddStreamFlag::ToLatest, 0);

        ASSERT_EQ(1, deletions.size());
        EXPECT_EQ(name, deletions[0].first);
        ASSERT_TRUE(deletions[0].second.has_value());
        EXPECT_TRUE(isDiskSnapshot(deletions[0].second->getFlags()));
        ASSERT_EQ(1, mutations.size());
        EXPECT_EQ(fmt::format("{}-1", name), mutations[0].first);
    }

    // Verify that we don't skip the delete when we tell it to do so, but
    // we don't start from seqno 0
    {
        auto [mutations, deletions] =
                drainStream(DcpOpenFlag::SkipDeletesInInitialBackfill,
                            DcpAddStreamFlag::ToLatest,
                            1);

        ASSERT_EQ(1, deletions.size());
        EXPECT_EQ(name, deletions[0].first);
        ASSERT_TRUE(deletions[0].second.has_value());
        EXPECT_TRUE(isDiskSnapshot(deletions[0].second->getFlags()));
        ASSERT_EQ(1, mutations.size());
        EXPECT_EQ(fmt::format("{}-1", name), mutations[0].first);
    }
}

TEST_P(DcpTest, MB26074) {
    verifyMb26074(false);
    // Recreate the bucket with the normal state to avoid messing up the next
    // test
    mcd_env->getTestBucket().reloadBucket(
            *adminConnection, bucketName, TestBucket::BucketCreateMode::Clean);
    userConnection.reset();
}

TEST_P(DcpTest, MB26074_Ephemeral) {
    verifyMb26074(true);
    // Recreate the bucket with the normal state to avoid messing up the next
    // test
    mcd_env->getTestBucket().reloadBucket(
            *adminConnection, bucketName, TestBucket::BucketCreateMode::Clean);
    userConnection.reset();
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
                    ClientOpcode::DcpMutation &&
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
