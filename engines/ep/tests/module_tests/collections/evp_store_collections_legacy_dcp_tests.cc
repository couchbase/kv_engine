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

/**
 * Tests for legacy DCP streams
 */

#include "kv_bucket.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/test_helpers.h"

class CollectionsLegacyDcpTest : public CollectionsDcpParameterizedTest {
public:
    CollectionsLegacyDcpTest() = default;

    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        producers = std::make_unique<CollectionsDcpTestProducers>();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

// The basic test for MB-47437, a legacy DCP stream is backfilled, but the
// vbucket high-seqno is not a default collection item. As there is no DCP seqno
// advance available we have to set the snapshot.end lower than the disk's end
TEST_P(CollectionsLegacyDcpTest, default_collection_is_not_vbucket_highseqno) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Perform a create of fruit via the bucket level
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    // Store documents - importantly the final documents are !defaultCollection
    store_item(vbid, StoredDocKey{"d1", CollectionEntry::defaultC}, "value");
    store_item(vbid, StoredDocKey{"d2", CollectionEntry::defaultC}, "value");
    store_item(vbid, StoredDocKey{"one", CollectionEntry::fruit}, "value");
    store_item(vbid, StoredDocKey{"two", CollectionEntry::fruit}, "value");
    flushVBucketToDiskIfPersistent(vbid, 5);

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    createDcpObjects({});

    // Snapshot must not be past the high-seqno of the default collection
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(3, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d1", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(2, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d2", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(3, producers->last_byseqno);

    // And no more!
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

// Similar to the previous test, but the high-seqno of the default collection
// is a pending (not visible item). The legacy client does not support sync
// writes.
TEST_P(CollectionsLegacyDcpTest,
       default_collection_is_not_vbucket_highseqno_with_pending) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Store documents - importantly the final documents are !defaultCollection
    const int items = 5;
    store_items(items,
                vbid,
                StoredDocKey{"d1_", CollectionEntry::defaultC},
                "value");
    flushVBucketToDiskIfPersistent(vbid, items);

    CollectionsManifest cm;
    // Must change collections config
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    // And put a pending item at the high-seqno of default so that the MB-47437
    // code has to do the extra scanning.
    auto item = makePendingItem(StoredDocKey{"d2", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    store_item(vbid, StoredDocKey{"one", CollectionEntry::fruit}, "value");
    store_item(vbid, StoredDocKey{"two", CollectionEntry::fruit}, "value");

    flushVBucketToDiskIfPersistent(vbid, 4);

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    createDcpObjects({});

    // Snapshot must not be past the high-seqno of the default collection
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(items, producers->last_snap_end_seqno);

    for (int i = 0; i < items; i++) {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ("d1_" + std::to_string(i), producers->last_key);
        EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
        EXPECT_EQ(i + 1, producers->last_byseqno);
        EXPECT_EQ("value", producers->last_value);
    }

    // And no more!
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

// Check when the default collection hasn't been written the backfill exits
// and we switch to in-memory
TEST_P(CollectionsLegacyDcpTest, default_collection_is_empty) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    // Store some documents, none in the default collection
    store_item(vbid, StoredDocKey{"one", CollectionEntry::fruit}, "value");
    store_item(vbid, StoredDocKey{"two", CollectionEntry::fruit}, "value");

    flushVBucketToDiskIfPersistent(vbid, 3);

    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    createDcpObjects({});
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Stream does nothing yet, and is now in-memory. The scheduled backfill
    // finds no default collection data exists and ends early
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid, false);
    EXPECT_TRUE(vb0Stream->isInMemory());
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    // Stream continues
    store_item(vbid, StoredDocKey{"one", CollectionEntry::defaultC}, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(4, producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_MEMORY, producers->last_flags & MARKER_FLAG_MEMORY);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("one", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(4, producers->last_byseqno);
}

// Check that if the default collection was dropped in-between streamRequest
// and the backfill creation, the stream correctly ends. This covers changes
// in MB-47437 where we find no stats for the default collection when figuring
// out the disk snapshot.
TEST_P(CollectionsLegacyDcpTest,
       default_dropped_after_successful_stream_request) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Store some documents
    store_item(vbid, StoredDocKey{"one", CollectionEntry::defaultC}, "value");
    store_item(vbid, StoredDocKey{"two", CollectionEntry::defaultC}, "value");

    flushVBucketToDiskIfPersistent(vbid, 2);

    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    auto dropDefault = [this]() {
        CollectionsManifest cm;
        setCollections(cookie, cm.remove(CollectionEntry::defaultC));
        flushVBucketToDiskIfPersistent(vbid, 1);
    };

    // Create a mock stream as we can inject drop of default collection in
    // between the stream request being successful and the stream setting up
    // the backfill
    auto mockStream = producer->mockActiveStreamRequest(
            /* flags */ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeletedUserXattrs::No,
            {},
            dropDefault);
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // If we step the stream, nothing is produced. Internally the backfill tried
    // to run, found no default collection data and bailed. We should not be an
    // in-memory stream
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid, false);
    EXPECT_TRUE(vb0Stream->isInMemory());
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    // Now the rest of the stream runs and first thing we see is the end of the
    // the default collection -> stream ends
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
}

TEST_P(CollectionsLegacyDcpTest, sync_replication_stream_is_ok) {
    VBucketPtr vb = store->getVBucket(vbid);

    store_item(vbid, StoredDocKey{"d1", CollectionEntry::defaultC}, "value");
    store_item(vbid, StoredDocKey{"d2", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    ensureDcpWillBackfill();
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);

    createDcpObjects({}, false, 0, true /*sync replication*/);

    // end stream reason
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);

    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(2, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d1", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(1, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d2", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(2, producers->last_byseqno);
    // And no more!
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    // There is a quirk of this though - if the stream now gets cursor dropped
    // it can't catch-up. This case only covers a bucket with collections
    // applied and a legacy client trying to stream with sync-replication. There
    // is no expectation that clients of that nature exist.

    CollectionsManifest cm;
    // create a collection and store something else
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    store_item(vbid, StoredDocKey{"d3", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 2);
    ensureDcpWillBackfill();

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());
    vb0Stream->handleSlowStream();
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
    // This isn't pretty, but a legacy stream can't be given a more descriptive
    // end stream reason. We would log the issue with warnings
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    EXPECT_EQ(producers->last_end_status,
              cb::mcbp::DcpStreamEndStatus::BackfillFail);
}

// The backfill code for MB-47437 doesn't yet work (and may never) if the bucket
// has other collections (or at least a manifest-uid of != 0). The backfill
// code aborts in that case and forces the stream to close.
// This case only covers a bucket with collections applied and a legacy client
// trying to stream with sync-replication. There is no expectation that clients
// of that nature exist.
TEST_P(CollectionsLegacyDcpTest, sync_replication_stream_ends) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Perform a create of fruit via the bucket level
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    // Store something
    store_item(vbid, StoredDocKey{"d1", CollectionEntry::defaultC}, "value");

    flushVBucketToDiskIfPersistent(vbid, 2);

    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);

    createDcpObjects({}, false, 0, true /*sync replication*/);

    // This isn't pretty, but a legacy stream can't be given a more descriptive
    // end stream reason. We would log the issue with warnings
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    EXPECT_EQ(producers->last_end_status,
              cb::mcbp::DcpStreamEndStatus::BackfillFail);
}

// No magma support or ephemeral
// @todo: MB-48037 include magma support
INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         CollectionsLegacyDcpTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
