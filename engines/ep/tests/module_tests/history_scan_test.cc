/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "collections/collections_dcp_test.h"
#include "dcp/backfill_by_seqno_disk.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_magma_kvstore.h"
#include "tests/mock/mock_stream.h"
#include "tests/module_tests/dcp_producer_config.h"
#include "tests/module_tests/dcp_stream_request_config.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <memcached/protocol_binary.h>

using namespace cb::mcbp;
using namespace cb::mcbp::request;
using namespace mcbp::systemevent;

// HistoryScanTest sub-classes collections DCP to give access to useful
// utilities for testing the "change stream" backfill feature.
class HistoryScanTest : public CollectionsDcpParameterizedTest {
public:
    void SetUp() override {
        // Configure a large history window. All of the tests which require
        // duplicates require that magma retains all of the test mutations.
        // MockMagmaKVStore then allows for arbitrary positioning of the history
        // window.
        config_string += "history_retention_bytes=104857600";

        CollectionsDcpParameterizedTest::SetUp();
        // To allow tests to set where history begins, use MockMagmaKVStore
        replaceMagmaKVStore();
    }

    void setHistoryStartSeqno(uint64_t seqno) {
        auto* kvstore = store->getRWUnderlying(vbid);
        dynamic_cast<MockMagmaKVStore&>(*kvstore).historyStartSeqno = seqno;
    }

    void validateSnapshot(Vbid vbucket,
                          uint64_t snap_start_seqno,
                          uint64_t snap_end_seqno,
                          uint32_t flags,
                          std::optional<uint64_t> highCompletedSeqno,
                          std::optional<uint64_t> maxVisibleSeqno,
                          std::optional<uint64_t> timestamp,
                          cb::mcbp::DcpStreamId sid,
                          const std::vector<Item>& items);
};

void HistoryScanTest::validateSnapshot(
        Vbid vbucket,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        uint32_t flags,
        std::optional<uint64_t> highCompletedSeqno,
        std::optional<uint64_t> maxVisibleSeqno,
        std::optional<uint64_t> timestamp,
        cb::mcbp::DcpStreamId sid,
        const std::vector<Item>& items) {
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(producers->last_vbucket, vbucket);
    EXPECT_EQ(producers->last_snap_start_seqno, snap_start_seqno);
    EXPECT_EQ(producers->last_snap_end_seqno, snap_end_seqno);
    EXPECT_EQ(producers->last_flags, flags);
    EXPECT_EQ(producers->last_stream_id, sid);
    EXPECT_EQ(producers->last_high_completed_seqno, highCompletedSeqno);
    EXPECT_EQ(producers->last_max_visible_seqno, maxVisibleSeqno);
    EXPECT_EQ(producers->last_timestamp, timestamp);

    for (const auto& item : items) {
        EXPECT_TRUE(item.getOperation() == queue_op::system_event ||
                    item.getOperation() == queue_op::mutation);
        if (item.getOperation() == queue_op::system_event) {
            stepAndExpect(ClientOpcode::DcpSystemEvent);
            EXPECT_EQ(producers->last_stream_id, sid);
            EXPECT_EQ(producers->last_vbucket, vbucket);
            EXPECT_EQ(producers->last_byseqno, item.getBySeqno());
            EXPECT_EQ(producers->last_collection_id,
                      item.getKey().getCollectionID());

        } else {
            if (item.isDeleted()) {
                stepAndExpect(ClientOpcode::DcpDeletion);

            } else {
                stepAndExpect(ClientOpcode::DcpMutation);
            }
            EXPECT_EQ(producers->last_key, item.getKey().c_str());
            EXPECT_EQ(producers->last_vbucket, vbucket);
            EXPECT_EQ(producers->last_byseqno, item.getBySeqno());
            EXPECT_EQ(producers->last_revseqno, item.getRevSeqno());
            EXPECT_EQ(producers->last_value, item.getValueView());
            EXPECT_EQ(producers->last_stream_id, sid);
            EXPECT_EQ(producers->last_datatype, item.getDataType());
            EXPECT_EQ(producers->last_collection_id,
                      item.getKey().getCollectionID());
        }
    }
}

// Basic functionality, unique mutations.
// All of the mutations fit in the history retention window.
// Validate that 1 disk snapshot is produced and that it is marked as history
// and duplicates
TEST_P(HistoryScanTest, basic_unique) {
    std::vector<Item> items;
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("a", CollectionID::Default), "val-a"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("b", CollectionID::Default), "val-b"));
    flush_vbucket_to_disk(vbid, items.size());
    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
    // Expect a single marker which state "history"
    validateSnapshot(vbid,
                     0,
                     2,
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     2 /*mvs*/,
                     {},
                     {},
                     items);
}

TEST_P(HistoryScanTest, basic_duplicates) {
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items;
    // Create a "dummy" Item that marks where the system-event is expected
    items.emplace_back(makeStoredDocKey("ignored", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       1);
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v0"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v1"));
    flush_vbucket_to_disk(vbid, 3 /* create-collection and mutations*/);
    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
    validateSnapshot(vbid,
                     0,
                     3,
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     3 /*mvs*/,
                     {},
                     {},
                     items);
}

// Test a backfill which is a resume of a stream, i.e. start >0
// In this test 3 items are written and then history is mocked to begin at
// seqno:2 a DCP stream request then starts within the history window.
TEST_P(HistoryScanTest, stream_start_within_history_window_unique_keys) {
    std::vector<Item> items;
    setHistoryStartSeqno(2);

    store_item(vbid, makeStoredDocKey("a", CollectionID::Default), "val-a");
    // Stream-request will expect the items stored from here.
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("b", CollectionID::Default), "val-b"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("c", CollectionID::Default), "val-c"));
    flush_vbucket_to_disk(vbid, items.size() + 1);
    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    auto vb = store->getVBucket(vbid);

    createDcpObjects(DcpProducerConfig{},
                     // Request says start=1, recall that stream-request uses
                     // the "last received seqno" as input. So here start=1 will
                     // result in the backfill sending inclusive of 2.
                     DcpStreamRequestConfig{vbid,
                                            0, // flags
                                            1, // opaque
                                            1, // 1 results in backfill from 2
                                            ~0ull, // no end
                                            1, // snap start 1
                                            1, // snap end 2
                                            vb->failovers->getLatestUUID(),
                                            std::string_view{},
                                            cb::engine_errc::success});

    runBackfill();
    // Expect a single marker which states "history"
    validateSnapshot(vbid,
                     1, // snap start
                     3, // snap end
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     3 /*mvs*/,
                     {},
                     {},
                     items);
}

TEST_P(HistoryScanTest, basic_duplicates_and_deletes) {
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items;
    // Create a "dummy" Item that marks where the system-event is expected
    items.emplace_back(makeStoredDocKey("ignored", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       1);
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v0"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v1"));
    flush_vbucket_to_disk(vbid, 3 /* create-collection and mutations*/);
    items.emplace_back(store_deleted_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), {}));
    flush_vbucket_to_disk(vbid, 1);
    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
    validateSnapshot(vbid,
                     0,
                     4,
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     4 /*mvs*/,
                     {},
                     {},
                     items);
}

// same test idea as stream_start_within_history_window_unique_keys but with
// duplicate versions of k0
TEST_P(HistoryScanTest, stream_start_within_history_window_duplicate_keys) {
    setHistoryStartSeqno(3);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));

    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v1");
    // Stream-request will expect from here.
    std::vector<Item> items;
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v2"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v3"));
    flush_vbucket_to_disk(vbid, items.size() + 2);

    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    auto vb = store->getVBucket(vbid);

    createDcpObjects(DcpProducerConfig{},
                     DcpStreamRequestConfig{vbid,
                                            0, // flags
                                            1, // opaque
                                            2, // last received seqno
                                            ~0ull,
                                            2, // snap {2,2}
                                            2, // ...
                                            vb->failovers->getLatestUUID(),
                                            std::string_view{},
                                            cb::engine_errc::success});

    runBackfill();

    // This test skips stream from 0, so replica skips create collection from
    // stream - create it explicitly so the test can push the mutations
    auto replica = store->getVBucket(replicaVB);
    EXPECT_TRUE(replica);
    replica->checkpointManager->createSnapshot(
            1, 1, 0, CheckpointType::Memory, 3);
    replica->replicaCreateCollection(
            Collections::ManifestUid(cm.getUid()),
            {ScopeID::Default, CollectionEntry::vegetable},
            "vegetable",
            {},
            CanDeduplicate::No,
            1);

    // Expect a single marker which states "history"
    validateSnapshot(vbid,
                     2,
                     4,
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     4 /*mvs*/,
                     {},
                     {},
                     items);
}

// Test that a scan which starts below the history window delivers two snapshots
TEST_P(HistoryScanTest, TwoSnapshots) {
    // history will begin at seqno 4, which means a backfill returns two
    // snapshot markers so that a DCP client can see when history begins
    setHistoryStartSeqno(4);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items1, items2;

    // items1 represents the first snapshot, only the crate of vegetable will
    // exist in this snapshot. The second history snapshot will have the 'k0'
    // keys (both versions).
    items1.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                        vbid,
                        queue_op::system_event,
                        0,
                        1);
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v0");
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v1");
    flush_vbucket_to_disk(vbid, 3);

    // history begins here...
    items2.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v2"));
    items2.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v3"));
    flush_vbucket_to_disk(vbid, 2);

    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    auto vbR = store->getVBucket(replicaVB);
    auto vbA = store->getVBucket(vbid);

    // Two back to back disk snapshots are generated.
    // Both snapshots state they encompass the entire disk range.
    validateSnapshot(vbid,
                     0,
                     5,
                     MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     5 /*mvs*/,
                     {},
                     {},
                     items1);

    EXPECT_EQ(items1.back().getBySeqno(), vbR->getHighSeqno());
    EXPECT_EQ(0, vbR->checkpointManager->getSnapshotInfo().range.getStart());
    EXPECT_EQ(vbA->getHighSeqno(),
              vbR->checkpointManager->getSnapshotInfo().range.getEnd());

    SCOPED_TRACE("History Snapshot");
    validateSnapshot(vbid,
                     0,
                     5,
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     5 /*mvs*/,
                     {},
                     {},
                     items2);

    EXPECT_EQ(vbA->getHighSeqno(), vbR->getHighSeqno());
    EXPECT_EQ(items2.back().getBySeqno(), vbR->getHighSeqno());
    EXPECT_EQ(0, vbR->checkpointManager->getSnapshotInfo().range.getStart());
    EXPECT_EQ(vbA->getHighSeqno(),
              vbR->checkpointManager->getSnapshotInfo().range.getEnd());
}

// Test OSO switches to history
TEST_P(HistoryScanTest, OSOThenHistory) {
    // Setup (which calls writeTwoCollections), then call writeTwoCollectios
    // to generate some duplicates (history)
    setupTwoCollections();
    auto highSeqno = writeTwoCollectios(true);

    ensureDcpWillBackfill();

    // Filter on vegetable collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["a"]})"}},
                     OutOfOrderSnapshots::Yes,
                     0,
                     false,
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    // see comment in CollectionsOSODcpTest.basic
    consumer->snapshotMarker(1, replicaVB, 0, highSeqno, 0, 0, highSeqno);

    // Manually step the producer and inspect all callbacks
    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    EXPECT_EQ("vegetable", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};

    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(k, producers->last_key) << producers->last_byseqno;
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    }

    // Now we get the end message
    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    auto vb = store->getVBucket(vbid);
    // Now we get the second snapshot, which is history
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(vbid, producers->last_vbucket);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getPersistenceSeqno(), producers->last_snap_end_seqno);
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK | MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS,
              producers->last_flags);

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);

    // And all keys in seq order. writeTwoCollectios created in order b, d, a, c
    std::array<std::string, 4> keySeqnoOrder = {{"b", "d", "a", "c"}};
    for (auto& k : keySeqnoOrder) {
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    }
    // twice.. as we wrote them twice
    for (auto& k : keySeqnoOrder) {
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    }
}

// A dropped collection can still exist inside the history window, test it is
// not observable by DCP change stream when backfilling
TEST_P(HistoryScanTest, BackfillWithDroppedCollection) {
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items;

    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v0");
    flush_vbucket_to_disk(vbid, 1 + 1);
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v1");
    flush_vbucket_to_disk(vbid, 1);

    // Now store 1 item to default (which will be in the snapshot)
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("default", CollectionID::Default), "val-a"));
    // Add a system_event in the next seqno position
    items.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       items.back().getBySeqno() + 1);
    // now drop the vegetable collection
    setCollections(cookie, cm.remove(CollectionEntry::vegetable));
    flush_vbucket_to_disk(vbid, 2);

    ensureDcpWillBackfill();

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
    validateSnapshot(vbid,
                     0,
                     items.back().getBySeqno(),
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     items.back().getBySeqno() /*mvs*/,
                     {},
                     {},
                     items);
}

TEST_P(HistoryScanTest, DoubleSnapshotMarker_MB_55590) {
    // Disable history and move the high-seqno forwards
    store->setHistoryRetentionBytes(0);

    store_item(vbid, makeStoredDocKey("key"), "v0");
    delete_item(vbid, makeStoredDocKey("key"));
    flush_vbucket_to_disk(vbid, 1);

    // Now purge tombstones, there is no data from start-seqno 0
    runCompaction(vbid, 0, true);

    // Now enable history and flush some items
    store->setHistoryRetentionBytes(1024 * 1024 * 100);

    // Now store 1 item to default (which will be in the snapshot)
    std::vector<Item> items;
    items.emplace_back(store_item(vbid, makeStoredDocKey("key"), "v0"));

    flush_vbucket_to_disk(vbid, 1);

    items.emplace_back(store_item(vbid, makeStoredDocKey("key"), "v1"));

    flush_vbucket_to_disk(vbid, 1);

    ensureDcpWillBackfill();

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    // Single snapshot produced. With MB-55590 a double marker was sent
    validateSnapshot(vbid,
                     0,
                     items.back().getBySeqno(),
                     MARKER_FLAG_HISTORY |
                             MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS |
                             MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                     0 /*hcs*/,
                     items.back().getBySeqno() /*mvs*/,
                     {},
                     {},
                     items);
}

// Tests which don't need executing in two eviction modes
class HistoryScanTestSingleEvictionMode : public HistoryScanTest {};

// Test covers state machine transitions when a history scanHistory gets false
// from markDiskSnapshot
TEST_P(HistoryScanTestSingleEvictionMode, HistoryScanFailMarkDiskSnapshot) {
    // Store an items, create new checkpoint and flush so we have something to
    // backfill from disk
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flushAndRemoveCheckpoints(vbid);

    // Create producer now we have items only on disk.
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test-producer", 0 /*flags*/, false /*startTask*/);
    ASSERT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::ChangeStreams, "true"));

    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto stream =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               DCP_ADD_STREAM_FLAG_DISKONLY,
                                               0,
                                               *vb,
                                               0,
                                               1,
                                               0,
                                               0,
                                               0,
                                               IncludeValue::Yes,
                                               IncludeXattrs::Yes,
                                               IncludeDeletedUserXattrs::No,
                                               std::string{});

    ASSERT_TRUE(stream->areChangeStreamsEnabled());
    stream->setActive();

    // Create our own backfill to test
    auto backfill = std::make_unique<DCPBackfillBySeqnoDisk>(
            *engine->getKVBucket(), stream, 1, vb->getPersistenceSeqno());
    EXPECT_EQ(backfill_state_init, backfill->getState());
    EXPECT_EQ(backfill_success, backfill->run());
    EXPECT_EQ(backfill_state_scanning_history_snapshot, backfill->getState());
    // stream will error markDiskSnapshot
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Ok);
    EXPECT_EQ(backfill_finished, backfill->run());
}

INSTANTIATE_TEST_SUITE_P(HistoryScanTests,
                         HistoryScanTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        HistoryScanTests,
        HistoryScanTestSingleEvictionMode,
        ::testing::Values("bucket_type=persistent:"
                          "backend=magma:"
                          "item_eviction_policy=full_eviction"),
        STParameterizedBucketTest::PrintToStringParamName);