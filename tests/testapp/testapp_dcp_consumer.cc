/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include <xattr/utils.h>

enum class OutOfMem { Yes, No };

std::string to_string(OutOfMem value) {
    switch (value) {
    case OutOfMem::Yes:
        return "OutOfMemYes";
    case OutOfMem::No:
        return "OutOfMemNo";
    }
    folly::assume_unreachable();
}

class DcpConsumerAckTest
    : public TestappTest,
      public ::testing::WithParamInterface<::testing::tuple<TransportProtocols,
                                                            XattrSupport,
                                                            ClientJSONSupport,
                                                            ClientSnappySupport,
                                                            OutOfMem>> {
public:
    static void SetUpTestCase() {
        // Enable consumer control so we can force buffering.
        // Also configure 0 bytes for consumers buffers, so that every input
        // will generate an ACK.
        doSetUpTestCaseWithConfiguration(
                generate_config(),
                "dcp_consumer_buffer_ratio=0.0;"
                "dcp_consumer_flow_control_ack_ratio=0.0");
    }

    void SetUp() override {
        TestappTest::SetUp();
        // Test has to run on a connection which can create a consumer and the
        // test fixtures must use the same connection as the consumer is
        // associated with a connection.
        conn = adminConnection->clone();
        conn->authenticate("@admin");
        conn->selectBucket(bucketName);

        // Enable all the datatypes we may need and collections
        using cb::mcbp::Feature;
        conn->setFeatures({Feature::JSON,
                           Feature::SNAPPY,
                           Feature::XATTR,
                           Feature::Collections,
                           Feature::MUTATION_SEQNO,
                           Feature::XERROR});

        // vbucket must be replica for addStream
        conn->setVbucket(Vbid{0}, vbucket_state_replica, {/*no json*/});

        auto& bucket = mcd_env->getTestBucket();
        if (testOutOfMem()) {
            bucket.setMutationMemRatio(*conn, "0.0");
        } else {
            bucket.setMutationMemRatio(*conn, "1.0");
        }

        consumerName = "consumer:" + name;
        setupConsumer(consumerName, {});
        setupConsumerStream(Vbid(0), {{0xdeadbeefull, 0}});

        // Setup a Document
        doc.info.id =
                DocKeyView::makeWireEncodedString(CollectionID::Default, "key");
        doc.info.cas = nextCas();
        generateDocumentValue(getValue());
    }

    void setupConsumer(
            std::string_view name,
            const std::vector<std::pair<std::string, std::string>>& controls) {
        conn->dcpOpenConsumer(name);

        for (const auto& [key, value] : controls) {
            conn->dcpControl(key, value);
        }
    }

    void setupConsumerStream(
            Vbid id,
            const std::vector<std::pair<uint64_t, uint64_t>>& failovers) {
        conn->dcpAddStream(id);

        // After AddStream the consumer will send back a number of control
        // messages and one GetErrorMap (for producer version detection).
        // We will ack success to all of these commands and stop when we see
        // the stream-request. The stream-request is given success and our dummy
        // failover table.
        Frame frame;
        auto stepDcp = [&frame, this]() {
            conn->recvFrame(frame);
            EXPECT_EQ(cb::mcbp::Magic::ClientRequest, frame.getMagic());
            return frame.getRequest();
        };

        while (true) {
            const auto* request = stepDcp();
            if (request->getClientOpcode() ==
                cb::mcbp::ClientOpcode::DcpStreamReq) {
                conn->dcpStreamRequestResponse(request->getOpaque(), failovers);
                break;
            }
            conn->sendCommand(BinprotCommandResponse{request->getClientOpcode(),
                                                     request->getOpaque()});
        }

        // And finally AddStream response now that the stream is ready.
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_TRUE(rsp.isSuccess());
    }

    std::string getValue() const {
        auto rv = memcached_cfg.dump();

        if (!testJson()) {
            // Same length, but not json
            std::ranges::replace(rv, '{', 'q');
            std::ranges::replace(rv, '}', 'r');
        }
        return rv;
    }

    std::string getVeryCompressibleValue() const {
        nlohmann::json value;
        value["aaa"] = std::string(500, 'a');
        std::string rv = value.dump();
        if (!testJson()) {
            // Same length, but not json
            std::ranges::replace(rv, '{', 'q');
            std::ranges::replace(rv, '}', 'r');
        }
        return rv;
    }

    std::string getSmallValue() const {
        nlohmann::json value;
        value["k"] = "v";
        std::string rv = value.dump();
        if (!testJson()) {
            // Same length, but not json
            std::ranges::replace(rv, '{', 'q');
            std::ranges::replace(rv, '}', 'r');
        }
        return rv;
    }

    void generateDocumentValue(const std::string& value,
                               const std::string& xattrKey = "_system_key",
                               int xattrCount = 1) {
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        if (testXattr()) {
            std::unordered_map<std::string, std::string> xattrMap;
            for (int ii = 0; ii < xattrCount; ii++) {
                auto index = std::to_string(ii);
                xattrMap.emplace(xattrKey + index, index);
            }

            doc.value = cb::xattr::make_wire_encoded_string(value, xattrMap);
            doc.info.datatype = cb::mcbp::Datatype::Xattr;
        } else {
            doc.value = value;
        }

        if (testJson()) {
            doc.info.datatype =
                    cb::mcbp::Datatype(uint8_t(doc.info.datatype) |
                                       uint8_t(cb::mcbp::Datatype::JSON));
        }

        if (testSnappy()) {
            doc.compress();
        }
    }

    bool testXattr() const {
        return ::testing::get<1>(GetParam()) == XattrSupport::Yes;
    }
    bool testJson() const {
        return ::testing::get<2>(GetParam()) == ClientJSONSupport::Yes;
    }

    bool testSnappy() const {
        return ::testing::get<3>(GetParam()) == ClientSnappySupport::Yes;
    }

    bool testOutOfMem() const {
        return ::testing::get<4>(GetParam()) == OutOfMem::Yes;
    }

    size_t getUnackedBytes() const {
        const auto dcpStats = conn->stats("dcp");
        const auto statName =
                "eq_dcpq:" + consumerName + ":stream_0_unacked_bytes";
        return dcpStats[statName].get<size_t>();
    }

    static uint64_t nextSeqno() {
        return seqno++;
    }

    static uint64_t nextCas() {
        return cas++;
    }

    std::unique_ptr<MemcachedConnection> conn;
    Document doc;
    std::string consumerName;
    static uint64_t seqno;
    static uint64_t cas;
};

uint64_t DcpConsumerAckTest::seqno{1};
uint64_t DcpConsumerAckTest::cas{1};

struct ToStringCombinedTestName {
    std::string operator()(
            const ::testing::TestParamInfo<::testing::tuple<TransportProtocols,
                                                            XattrSupport,
                                                            ClientJSONSupport,
                                                            ClientSnappySupport,
                                                            OutOfMem>>& info)
            const {
        std::string rv = to_string(::testing::get<0>(info.param)) + "_" +
                         to_string(::testing::get<1>(info.param)) + "_" +
                         to_string(::testing::get<2>(info.param)) + "_" +
                         to_string(::testing::get<3>(info.param)) + "_" +
                         to_string(::testing::get<4>(info.param));
        return rv;
    }
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        DcpConsumerAckTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No),
                           ::testing::Values(OutOfMem::Yes, OutOfMem::No)),
        ToStringCombinedTestName());

TEST_P(DcpConsumerAckTest, Basic) {
    const auto markerBytes = conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, {} /*flags*/);

    auto& bucket = mcd_env->getTestBucket();
    if (testOutOfMem()) {
        ASSERT_EQ(markerBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(markerBytes);

    if (testOutOfMem()) {
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    const auto mutationBytes =
            conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno());

    if (testOutOfMem()) {
        ASSERT_EQ(mutationBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(mutationBytes);

    if (testOutOfMem()) {
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    // Do a delete with no value
    doc.value = {};
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    doc.info.cas = nextCas();
    const auto delBytes = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    if (testOutOfMem()) {
        ASSERT_EQ(delBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(delBytes);
}

TEST_P(DcpConsumerAckTest, DeleteWithValue) {
    const auto markerBytes = conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, {} /*flags*/);

    auto& bucket = mcd_env->getTestBucket();
    if (testOutOfMem()) {
        ASSERT_EQ(markerBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(markerBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    const auto mutationBytes =
            conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno());

    if (testOutOfMem()) {
        ASSERT_EQ(mutationBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(mutationBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    doc.info.cas = nextCas();
    const auto delBytes = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        // In the compressed + buffer only mode this would trigger a variant of
        // MB-47318, but the ack value would be smaller than we sent (which is
        // worse as may eventually lead to a indefinite pause)
        if (testOutOfMem()) {
            ASSERT_EQ(delBytes, getUnackedBytes());
            bucket.setMutationMemRatio(*conn, "1.0");
        }
        conn->recvDcpBufferAck(delBytes);
    }
}

// Similar to previous test but use a highly compressible 'body'
TEST_P(DcpConsumerAckTest, DeleteWithCompressibleValue) {
    generateDocumentValue(getVeryCompressibleValue());

    const auto markerBytes = conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, {} /*flags*/);

    auto& bucket = mcd_env->getTestBucket();
    if (testOutOfMem()) {
        ASSERT_EQ(markerBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(markerBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    const auto mutationBytes =
            conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno());

    if (testOutOfMem()) {
        ASSERT_EQ(mutationBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(mutationBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    doc.info.cas = nextCas();
    const auto delBytes = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        // In the compressed + buffer only mode this would trigger a variant of
        // MB-47318, but the ack value would be smaller than we sent (which is
        // worse as may eventually lead to a indefinite pause)
        if (testOutOfMem()) {
            ASSERT_EQ(delBytes, getUnackedBytes());
            bucket.setMutationMemRatio(*conn, "1.0");
        }
        conn->recvDcpBufferAck(delBytes);
    }
}

// Similar to previous test but use many highly compressible 'xattr' i.e. the
// majority of the value is the xattr data.
// MB-47318: Issue detected that a buffered delete can ack more than we sent.
// This can occur when a value is compressed and DCP buffers the delete, the
// delete triggers value sanitisation code and results in an ACK using the
// decompressed size, which this test forces to be much larger than what we
// sent.
TEST_P(DcpConsumerAckTest, DeleteWithManyCompressibleXattrs) {
    // The xattr key/value will be repeating characters, which will compress
    // well. These are also system keys so they are retained by sanitisation.
    std::string xattrKey = "_" + std::string(5, 'a');
    generateDocumentValue(getSmallValue(), xattrKey, 10);

    const auto markerBytes = conn->dcpSnapshotMarkerV2(
            1 /*opaque */, seqno /*start*/, seqno + 2 /*end*/, {} /*flags*/);

    auto& bucket = mcd_env->getTestBucket();
    if (testOutOfMem()) {
        ASSERT_EQ(markerBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(markerBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    const auto mutationBytes =
            conn->dcpMutation(doc, 1 /*opaque*/, nextSeqno());

    if (testOutOfMem()) {
        ASSERT_EQ(mutationBytes, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "1.0");
    }
    conn->recvDcpBufferAck(mutationBytes);

    if (testOutOfMem()) {
        ASSERT_EQ(0, getUnackedBytes());
        bucket.setMutationMemRatio(*conn, "0.0");
    }

    doc.info.cas = nextCas();
    const auto delBytes = conn->dcpDeletionV2(doc, 1 /*opaque*/, nextSeqno());

    // Json values are only legal when combined with xattr
    if (testJson() && !testXattr()) {
        BinprotResponse rsp;
        conn->recvResponse(rsp);
        ASSERT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    } else {
        // In the compressed + buffer only mode this would trigger MB-47318. The
        // ack was larger than we sent.
        if (testOutOfMem()) {
            ASSERT_EQ(delBytes, getUnackedBytes());
            bucket.setMutationMemRatio(*conn, "1.0");
        }
        conn->recvDcpBufferAck(delBytes);
    }
}