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

#include "collections/vbucket_manifest_handles.h"
#include "kv_bucket.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/test_helpers.h"

class CollectionsLegacyDcpTest : public CollectionsDcpParameterizedTest {
public:
    CollectionsLegacyDcpTest() = default;

    void SetUp() override {
        STParameterizedBucketTest::SetUp();
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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
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
    ASSERT_TRUE(store_items(items,
                            vbid,
                            StoredDocKey{"d1_", CollectionEntry::defaultC},
                            "value"));
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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, true /*sync replication*/);

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
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, true /*sync replication*/);

    // This isn't pretty, but a legacy stream can't be given a more descriptive
    // end stream reason. We would log the issue with warnings
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    EXPECT_EQ(producers->last_end_status,
              cb::mcbp::DcpStreamEndStatus::BackfillFail);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_has_no_commits_default_high_seqno_one_non_inf) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    auto item = makePendingItem(StoredDocKey{"d1", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    item = makePendingItem(StoredDocKey{"f1", CollectionEntry::fruit}, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    store_item(vbid, StoredDocKey{"f2", CollectionEntry::fruit}, "value");

    flushVBucketToDiskIfPersistent(vbid, 4);
    auto highSeqno = vb->getHighSeqno();

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, false, highSeqno);
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_has_no_commits_default_high_seqno_four_non_inf) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    auto diskKey = StoredDocKey{"f1", CollectionEntry::fruit};
    auto item = makePendingItem(diskKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    store_item(vbid, StoredDocKey{"f2", CollectionEntry::fruit}, "value");

    item = makePendingItem(StoredDocKey{"d1", CollectionEntry::defaultC},
                           "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    flushVBucketToDiskIfPersistent(vbid, 4);
    auto highSeqno = vb->getHighSeqno();

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, false, highSeqno);
    // Stream does nothing yet, and is now in-memory. The scheduled backfill
    // finds no default collection data exists and ends early
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    store_item(vbid, StoredDocKey{"f3", CollectionEntry::fruit}, "value");

    // Stream continues
    store_item(vbid, StoredDocKey{"d2", CollectionEntry::defaultC}, "value");
    highSeqno = vb->getHighSeqno();
    uint64_t rollbackSeqno;
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->streamRequest(0,
                                    1, // opaque
                                    vbid,
                                    0, // start_seqno
                                    highSeqno, // end_seqno
                                    0, // vbucket_uuid,
                                    0, // snap_start_seqno,
                                    0, // snap_end_seqno,
                                    &rollbackSeqno,
                                    [](const std::vector<vbucket_failover_t>&) {
                                        return cb::engine_errc::success;
                                    },
                                    {}));
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid, false);
    EXPECT_TRUE(vb0Stream->isInMemory());
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(highSeqno, producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_MEMORY, producers->last_flags & MARKER_FLAG_MEMORY);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d2", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(highSeqno, producers->last_byseqno);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_has_no_items_but_fruit_does_inf_and_non_inf) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"f2", CollectionEntry::fruit}, "value");
    auto item = makePendingItem(StoredDocKey{"d1", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 3);
    auto highSeqno = vb->getHighSeqno();

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({});
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid, false);
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, false, highSeqno);
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd, false);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_one_prepare_no_collections_sync_write) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto item = makePendingItem(StoredDocKey{"d1", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
    createDcpObjects({}, OutOfOrderSnapshots::No, 0, true /*sync replication*/);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(1, producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_DISK, producers->last_flags & MARKER_FLAG_DISK);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpPrepare);
    EXPECT_EQ("d1", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(1, producers->last_byseqno);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_high_seqno_prepare_max_visable_seqno_less_than_hcs) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid);

    store_item(vbid, StoredDocKey{"d1", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid);

    const auto keyd1Seqno = vb->getHighSeqno();
    {
        auto key = StoredDocKey{"f1", CollectionEntry::fruit};
        auto item = makePendingItem(key, "value");
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*item, cookie));
        flushVBucketToDiskIfPersistent(vbid);

        EXPECT_EQ(
                cb::engine_errc::success,
                vb->commit(
                        key, vb->getHighSeqno(), {}, vb->lockCollections(key)));
    }
    auto item = makePendingItem(StoredDocKey{"d2", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    flushVBucketToDiskIfPersistent(vbid, 2);

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
    createDcpObjects({},
                     OutOfOrderSnapshots::No,
                     0,
                     false /*sync replication*/,
                     keyd1Seqno);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(keyd1Seqno, producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_DISK, producers->last_flags & MARKER_FLAG_DISK);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d1", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(keyd1Seqno, producers->last_byseqno);
    // Ensure there's no more items
    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest, default_collection_no_items_in_memory_only) {
    VBucketPtr vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, false, vb->getHighSeqno());
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest, default_collection_one_items_in_memory_only) {
    VBucketPtr vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    store_item(vbid, StoredDocKey{"d", CollectionEntry::defaultC}, "value");
    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));

    createDcpObjects(
            {}, OutOfOrderSnapshots::No, 0, false, vb->getHighSeqno() / 2);
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(2, producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_MEMORY, producers->last_flags & MARKER_FLAG_MEMORY);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(2, producers->last_byseqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest,
       default_collection_prepare_item_in_memory_only) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));

    auto item = makePendingItem(StoredDocKey{"d", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    createDcpObjects({}, OutOfOrderSnapshots::No, 0, false, vb->getHighSeqno());
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsLegacyDcpTest,
       ensure_backfill_continues_after_collection_drop) {
    VBucketPtr vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"f2", CollectionEntry::fruit}, "value");
    auto item = makePendingItem(StoredDocKey{"d1", CollectionEntry::defaultC},
                                "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 3);
    auto highSeqno = vb->getHighSeqno();

    item = makePendingItem(StoredDocKey{"d2", CollectionEntry::defaultC},
                           "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    // Now DCP with backfill
    ensureDcpWillBackfill();

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);

    createDcpObjects({},
                     OutOfOrderSnapshots::No,
                     0,
                     false /*sync replication*/,
                     highSeqno);
    setCollections(cookie, cm.remove(CollectionEntry::defaultC));

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid, false);
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
}

TEST_P(CollectionsDcpParameterizedTest, stream_end_in_memory_stream) {
    auto vb = store->getVBucket(vbid);

    ASSERT_TRUE(store_items(
            3, vbid, StoredDocKey{"d", CollectionEntry::defaultC}, "value"));
    auto highSeqno = vb->getHighSeqno();
    createDcpObjects({},
                     OutOfOrderSnapshots::No,
                     0,
                     false /*sync replication*/,
                     highSeqno);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(highSeqno, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d0", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(1, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d1", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(2, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d2", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(highSeqno, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsDcpParameterizedTest,
       stream_end_in_memory_stream_with_one_item_in_default_collection) {
    auto vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value");

    createDcpObjects(
            {}, OutOfOrderSnapshots::No, 0, false /*sync replication*/, 4);

    store_item(vbid, StoredDocKey{"d", CollectionEntry::defaultC}, "value");
    auto highSeqno = vb->getHighSeqno();

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(highSeqno, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ("d", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(highSeqno, producers->last_byseqno);

    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    store_item(vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value");

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

TEST_P(CollectionsDcpParameterizedTest,
       stream_end_in_memory_stream_with_one_item_in_default_collection_sync_write) {
    auto vb = store->getVBucket(vbid);
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value");
    createDcpObjects(
            {}, OutOfOrderSnapshots::No, 0, true /*sync replication*/, 5);
    {
        auto key = StoredDocKey{"d", CollectionEntry::defaultC};
        auto item = makePendingItem(key, "value");
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*item, cookie));
        EXPECT_EQ(
                cb::engine_errc::success,
                vb->commit(
                        key, vb->getHighSeqno(), {}, vb->lockCollections(key)));
    }
    auto highSeqno = vb->getHighSeqno();

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(highSeqno, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpPrepare);
    EXPECT_EQ("d", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(highSeqno - 1, producers->last_byseqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpCommit);
    EXPECT_EQ("cid:0x0:d", producers->last_key);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
    EXPECT_EQ(highSeqno, producers->last_commit_seqno);
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    store_item(vbid, StoredDocKey{"d", CollectionEntry::fruit}, "value");

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(producers->last_end_status, cb::mcbp::DcpStreamEndStatus::Ok);
}

// No ephemeral support
INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         CollectionsLegacyDcpTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
