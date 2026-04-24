/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/response.h"
#include "item.h"
#include "kv_bucket_test.h"
#include "test_helpers.h"

#include <folly/portability/GTest.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey_view.h>

/**
 * Fixture for DcpResponse tests that need a live engine to own items — e.g.
 * DcpCacheTransfer whose cb::unique_item_ptr uses cb::ItemDeleter{engine}
 * during destruction.
 */
class DcpResponseEngineTest : public KVBucketTest {
protected:
    /// Allocate a new Item wrapped in a cb::unique_item_ptr whose deleter
    /// dispatches back to the fixture's engine on destruction.
    cb::unique_item_ptr makeItem(const std::string& key,
                                 const std::string& value) {
        auto* item = new Item(makeStoredDocKey(key),
                              0 /*flags*/,
                              0 /*exptime*/,
                              value.data(),
                              value.size());
        return cb::unique_item_ptr{item, cb::ItemDeleter{engine.get()}};
    }
};

TEST(DcpResponseTest, DcpCommit_getMessageSize) {
    std::string key("key"); // tests will see 'key\0'
    auto sdkey = makeStoredDocKey(key, CollectionID{0x5555});
    cb::mcbp::unsigned_leb128<CollectionIDType> lebEncoded(0x5555);
    auto collectionAwareSize = sizeof(cb::mcbp::Request) +
                               sizeof(cb::mcbp::request::DcpCommitPayload) +
                               key.size() + lebEncoded.size();

    auto collectionUnAwareSize = sizeof(cb::mcbp::Request) +
                                 sizeof(cb::mcbp::request::DcpCommitPayload) +
                                 key.size();

    auto rsp1 = std::make_unique<CommitSyncWriteConsumer>(
            0, Vbid(99), 1 /*prepare*/, 2 /*commit*/, sdkey);

    EXPECT_EQ(collectionAwareSize, rsp1->getMessageSize());

    auto rsp2 = std::make_unique<CommitSyncWriteConsumer>(
            0,
            Vbid(99),
            1 /*prepare*/,
            2 /*commit*/,
            sdkey.makeDocKeyWithoutCollectionID());

    EXPECT_EQ(collectionUnAwareSize, rsp2->getMessageSize());

    auto rsp3 =
            std::make_unique<CommitSyncWrite>(0,
                                              Vbid(99),
                                              1 /*prepare*/,
                                              2 /*commit*/,
                                              sdkey,
                                              DocKeyEncodesCollectionId::Yes);

    EXPECT_EQ(collectionAwareSize, rsp3->getMessageSize());

    auto rsp4 = std::make_unique<CommitSyncWrite>(
            0,
            Vbid(99),
            1 /*prepare*/,
            2 /*commit*/,
            sdkey.makeDocKeyWithoutCollectionID(),
            DocKeyEncodesCollectionId::No);

    EXPECT_EQ(collectionUnAwareSize, rsp4->getMessageSize());
}

TEST(DcpResponseTest, DcpAbort_getMessageSize) {
    std::string key("key"); // tests will see 'key\0'
    auto sdkey = makeStoredDocKey(key, CollectionID{0x5555});
    cb::mcbp::unsigned_leb128<CollectionIDType> lebEncoded(0x5555);
    auto collectionAwareSize = sizeof(cb::mcbp::Request) +
                               sizeof(cb::mcbp::request::DcpAbortPayload) +
                               key.size() + lebEncoded.size();

    auto collectionUnAwareSize = sizeof(cb::mcbp::Request) +
                                 sizeof(cb::mcbp::request::DcpAbortPayload) +
                                 key.size();

    auto rsp1 = std::make_unique<AbortSyncWriteConsumer>(
            0, Vbid(99), sdkey, 1 /*prepare*/, 2 /*abort*/
    );

    EXPECT_EQ(collectionAwareSize, rsp1->getMessageSize());

    auto rsp2 = std::make_unique<AbortSyncWriteConsumer>(
            0,
            Vbid(99),
            sdkey.makeDocKeyWithoutCollectionID(),
            1 /*prepare*/,
            2 /*abort*/);

    EXPECT_EQ(collectionUnAwareSize, rsp2->getMessageSize());

    auto rsp3 =
            std::make_unique<AbortSyncWrite>(0,
                                             Vbid(99),
                                             sdkey,
                                             1 /*prepare*/,
                                             2 /*abort*/,
                                             DocKeyEncodesCollectionId::Yes);

    EXPECT_EQ(collectionAwareSize, rsp3->getMessageSize());

    auto rsp4 = std::make_unique<AbortSyncWrite>(
            0,
            Vbid(99),

            sdkey.makeDocKeyWithoutCollectionID(),
            1 /*prepare*/,
            2 /*abort*/,
            DocKeyEncodesCollectionId::No);

    EXPECT_EQ(collectionUnAwareSize, rsp4->getMessageSize());
}

TEST(DcpResponseTest, DcpSnapshotMarker_getMessageSize) {
    auto smV1 = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            {}, // highCompletedSeqno
            {},
            {}, // maxVisibleSeqno
            std::nullopt,
            {}); // sid

    const auto smV1_size =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload);

    EXPECT_EQ(smV1_size, smV1.getMessageSize());

    auto smV2_0_high_completed_seqno = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            6, // highCompletedSeqno
            {},
            {}, // maxVisibleSeqno
            std::nullopt,
            {}); // sid

    auto smV2_0_max_visible_seqno = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            {}, // highCompletedSeqno
            {},
            6, // maxVisibleSeqno
            std::nullopt,
            {}); // sid

    const auto smV2_0_size =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);

    EXPECT_EQ(smV2_0_size, smV2_0_high_completed_seqno.getMessageSize());
    EXPECT_EQ(smV2_0_size, smV2_0_max_visible_seqno.getMessageSize());
}

TEST_F(DcpResponseEngineTest, DcpCacheTransfer_getMessageSize) {
    // Empty-items, no streamId: just the Request header.
    {
        DcpCacheTransfer dct(1 /*opaque*/, {} /*items*/, Vbid(2), {} /*sid*/);
        EXPECT_EQ(sizeof(cb::mcbp::Request), dct.getMessageSize());
        EXPECT_EQ(dct.getMessageSize(), dct.getApproximateSize());
        EXPECT_EQ(Vbid(2), dct.getVBucket());
        EXPECT_EQ(1u, dct.getOpaque());
        EXPECT_EQ(DcpResponse::Event::CacheTransfer, dct.getEvent());
    }

    // Empty-items, with streamId: header + framing extras for the sid.
    {
        DcpCacheTransfer dct(
                1 /*opaque*/, {} /*items*/, Vbid(2), cb::mcbp::DcpStreamId(7));
        EXPECT_EQ(sizeof(cb::mcbp::Request) +
                          sizeof(cb::mcbp::DcpStreamIdFrameInfo),
                  dct.getMessageSize());
    }

    // Two items, no streamId.
    {
        const std::string key1 = "key1";
        const std::string value1 = "value1";
        const std::string key2 = "longerKey2";
        const std::string value2; // empty value allowed

        std::vector<cb::ItemWithCacheHint> items;
        items.push_back({makeItem(key1, value1), 0 /*cacheHint*/});
        items.push_back({makeItem(key2, value2), 0 /*cacheHint*/});

        DcpCacheTransfer dct(
                1 /*opaque*/, std::move(items), Vbid(2), {} /*sid*/);

        const auto expected =
                sizeof(cb::mcbp::Request) +
                (2 * sizeof(cb::mcbp::request::DcpCacheTransferPayload)) +
                makeStoredDocKey(key1).size() + value1.size() +
                makeStoredDocKey(key2).size() + value2.size();
        EXPECT_EQ(expected, dct.getMessageSize());
        EXPECT_EQ(dct.getMessageSize(), dct.getApproximateSize());
        EXPECT_EQ(2u, dct.getItems().size());
    }

    // One item, with streamId: header + framing extras + per-item payload.
    {
        const std::string key = "k1";
        const std::string value = "v1";

        std::vector<cb::ItemWithCacheHint> items;
        items.push_back({makeItem(key, value), 0 /*cacheHint*/});

        DcpCacheTransfer dct(1 /*opaque*/,
                             std::move(items),
                             Vbid(2),
                             cb::mcbp::DcpStreamId(7));

        const auto expected =
                sizeof(cb::mcbp::Request) +
                sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                sizeof(cb::mcbp::request::DcpCacheTransferPayload) +
                makeStoredDocKey(key).size() + value.size();
        EXPECT_EQ(expected, dct.getMessageSize());
    }
}

TEST(DcpResponseTest, DcpSnapshotMarker_with_sid_getMessageSize) {
    auto smV1 = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            {}, // highCompletedSeqno
            {},
            {}, // maxVisibleSeqno
            std::nullopt,
            cb::mcbp::DcpStreamId(6)); // sid

    const auto smV1_size =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
            sizeof(cb::mcbp::DcpStreamIdFrameInfo);

    EXPECT_EQ(smV1_size, smV1.getMessageSize());

    auto smV2_0_high_completed_seqno = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            6, // highCompletedSeqno
            {},
            {}, // maxVisibleSeqno
            std::nullopt,
            cb::mcbp::DcpStreamId(7)); // sid

    auto smV2_0_max_visible_seqno = SnapshotMarker(
            1, // opaque
            Vbid(2), // vbucket
            3, // start_seqno
            4, // end_seqno
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
            {}, // highCompletedSeqno
            {},
            6, // maxVisibleSeqno
            std::nullopt,
            cb::mcbp::DcpStreamId(7)); // sid

    const auto smV2_0_size =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value) +
            sizeof(cb::mcbp::DcpStreamIdFrameInfo);

    EXPECT_EQ(smV2_0_size, smV2_0_high_completed_seqno.getMessageSize());
    EXPECT_EQ(smV2_0_size, smV2_0_max_visible_seqno.getMessageSize());
}
