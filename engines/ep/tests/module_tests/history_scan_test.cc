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
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "collections/collections_dcp_test.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill_by_seqno_disk.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
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

        // Disable OSO backfill auto-selection to simplify most of the
        // functional tests - set to always.
        config_string += ";dcp_oso_backfill=enabled";

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
                    item.getOperation() == queue_op::mutation ||
                    item.getOperation() == queue_op::pending_sync_write ||
                    item.getOperation() == queue_op::abort_sync_write);
        if (item.getOperation() == queue_op::system_event) {
            stepAndExpect(ClientOpcode::DcpSystemEvent);
            EXPECT_EQ(producers->last_stream_id, sid);
            EXPECT_EQ(producers->last_vbucket, vbucket);
            EXPECT_EQ(producers->last_byseqno, item.getBySeqno());
            EXPECT_EQ(producers->last_collection_id,
                      item.getKey().getCollectionID());

        } else {
            if (item.isPending()) {
                stepAndExpect(ClientOpcode::DcpPrepare);
            } else if (item.isAbort()) {
                stepAndExpect(ClientOpcode::DcpAbort);
            } else if (item.isDeleted()) {
                stepAndExpect(ClientOpcode::DcpDeletion);
            } else {
                stepAndExpect(ClientOpcode::DcpMutation);
            }
            EXPECT_EQ(producers->last_key,
                      std::string_view{
                              item.getKey().makeDocKeyWithoutCollectionID()});
            EXPECT_EQ(producers->last_vbucket, vbucket);
            EXPECT_EQ(producers->last_byseqno, item.getBySeqno());
            if (!item.isAbort()) {
                // no rev on abort
                EXPECT_EQ(producers->last_revseqno, item.getRevSeqno());
            }
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
                     OutOfOrderSnapshots::No,
                     {},
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
                     {},
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
                                            {}, // flags
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
                     {},
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
                                            {}, // flags
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
            1, 1, 0, CheckpointType::Memory, 1);
    replica->replicaCreateCollection(
            Collections::ManifestUid(cm.getUid()),
            {ScopeID::Default, CollectionEntry::vegetable},
            "vegetable",
            {},
            Collections::Metered::No,
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

    // Two more keys to create more checkpoints so we could trigger eager
    // checkpoint removal to cover MB-56452
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v4");
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v5");

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    // Run the backfill task, which has a number of steps to complete. Note that
    // to reproduce an issue from MB-56452 run the backfill manually so a flush
    // can be interleaved. This makes a checkpoint eligible for removal during
    // the markDisksnaphot callbacks
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);

    // flush after the snapshot is opened
    flush_vbucket_to_disk(vbid, 2);

    // backfill:scan()
    runNextTask(lpAuxioQ);

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
    // note: test hasn't replicated the final mutations, so - 2 for some expects
    EXPECT_EQ(vbA->getHighSeqno() - 2,
              vbR->checkpointManager->getSnapshotInfo().range.getEnd());

    CB_SCOPED_TRACE("History Snapshot");
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

    // note: test hasn't replicated the final mutations, so - 2 for some expects
    EXPECT_EQ(vbA->getHighSeqno() - 2, vbR->getHighSeqno());
    EXPECT_EQ(items2.back().getBySeqno(), vbR->getHighSeqno());
    EXPECT_EQ(0, vbR->checkpointManager->getSnapshotInfo().range.getStart());
    EXPECT_EQ(vbA->getHighSeqno() - 2,
              vbR->checkpointManager->getSnapshotInfo().range.getEnd());

    // MB-56452: A bug with cursor registration and eager checkpoint removal
    // meant that the stream was back to backfilling... it should be in-memory
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    EXPECT_TRUE(stream->isInMemory());
}

// Test OSO switches to history
TEST_P(HistoryScanTest, OSOThenHistory) {
    // Setup 2 collections, 1 with history and use writeTwoCollections to
    // populate
    CollectionsManifest cm(CollectionEntry::fruit);
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    flush_vbucket_to_disk(vbid, 2);
    writeTwoCollectios(false);
    auto highSeqno = writeTwoCollectios(true);

    ensureDcpWillBackfill();

    // Filter on vegetable collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["a"]})"}},
                     OutOfOrderSnapshots::Yes,
                     {},
                     false,
                     ~0ull,
                     ChangeStreams::Yes);

    // In this test scan and scanHistory must be invoked.
    // 1) scan
    runBackfill();
    // 2) scanHistory
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

    // At this point this test has flushed a mixture of history=true and false
    // items - check the flush statistic (system events will also be marked
    // with history=true if the collection has such a configuration)
    auto& epVB = dynamic_cast<EPVBucket&>(*store->getVBucket(vbid));
    EXPECT_EQ(4, epVB.getHistoricalItemsFlushed());

    ensureDcpWillBackfill();

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     {},
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
    std::vector<Item> items;

    // this flush will now enable history
    items.emplace_back(store_item(vbid, makeStoredDocKey("key"), "v0"));

    flush_vbucket_to_disk(vbid, 1);

    // Enable history on a collection and flush some two items into it
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    flush_vbucket_to_disk(vbid, 1);
    items.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       4);

    items.emplace_back(store_item(
            vbid, makeStoredDocKey("key", CollectionEntry::vegetable), "v0"));

    flush_vbucket_to_disk(vbid, 1);

    items.emplace_back(store_item(
            vbid, makeStoredDocKey("key", CollectionEntry::vegetable), "v1"));

    flush_vbucket_to_disk(vbid, 1);

    ensureDcpWillBackfill();

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    // In this test scan and scanHistory must be invoked.
    // Note: In this test the NonHistory snapshot is empty, so in the end a
    // single History snapshot is sent
    // 1) scan
    runBackfill();
    // 2) scanHistory
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

TEST_P(HistoryScanTest, DoubleSnapshotMarker_CursorDrop) {
    // Disable history and move the high-seqno forwards
    store->setHistoryRetentionBytes(0);

    auto& vb = *store->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    manager.clear();
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    const std::string value("v");
    store_item(vbid, makeStoredDocKey("nonHistoryKey"), value);
    flush_vbucket_to_disk(vbid, 1);
    ASSERT_EQ(1, manager.getHighSeqno());

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
                     true,
                     ~0ull,
                     ChangeStreams::Yes);
    ASSERT_TRUE(producer);
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isInMemory());
    ASSERT_EQ(0, stream->getLastReadSeqno());

    // The stream gets the in-memory data
    ASSERT_EQ(0, producers->last_byseqno);
    runCheckpointProcessor();
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ(1, producers->last_byseqno);

    // History disabled so far
    auto& underlying = *store->getRWUnderlying(vbid);
    ASSERT_EQ(0, underlying.getHistoryStartSeqno(vbid));

    // Bump highSeqno
    store_item(vbid, makeStoredDocKey("nonHistoryKey"), value);
    flush_vbucket_to_disk(vbid, 1);
    ASSERT_EQ(2, manager.getHighSeqno());

    // Prepare for checkpoint removal
    moveHelperCursorToCMEnd();
    manager.createNewCheckpoint();
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // Simulate memory pressure, cursor is dropped and checkpoints removed.
    // Note: After this step the stream has to go to disk for resuming from
    // seqno:2
    ASSERT_TRUE(stream->handleSlowStream());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // Now enable history..
    store->setHistoryRetentionBytes(1024 * 1024 * 100);
    // ..and flush some items
    store_item(vbid, makeStoredDocKey("historyKeyA"), value);
    store_item(vbid, makeStoredDocKey("historyKeyB"), value);
    flush_vbucket_to_disk(vbid, 2);
    ASSERT_EQ(4, manager.getHighSeqno());
    ASSERT_EQ(3, underlying.getHistoryStartSeqno(vbid));

    // Move the stream, it jumps to backfill
    ASSERT_TRUE(stream->isInMemory());
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
    ASSERT_TRUE(stream->isBackfilling());

    // Note: In this test the NonHistory snapshot is non-empty, so we get a
    // first marker for the NonHistory snapshot..
    runBackfill(); // scan
    // ..and then a second marker for the History snapshot.
    //
    // Note: Before the fix for MB-58548 this step throws an exception at
    // ::markDiskSnapshot(), caused by that the stream doesn't consider that
    // two markers with the same seqno range are sent when a backfill spans a
    // nonHistory+History range
    //
    // libc++abi: terminating due to uncaught exception of type
    // std::logic_error: ActiveStream::markDiskSnapshot:sent snapshot marker to
    // client with snap start <= previous snap end vb:0 lastSentSnapStart:2
    // lastSentSnapEnd:4 snapStart:2 snapEnd:4 sid:sid:none producer
    // name:test_producer lastReadSeqno:1 curChkSeqno:5
    runBackfill(); // scanHistory
}

// A dropped collection can still exist inside the history window, test it is
// not observable by DCP change stream when backfilling
// MB-55557
TEST_P(HistoryScanTest, BackfillWithDroppedCollectionAndPurge) {
    // Stop creating history
    store->setHistoryRetentionBytes(0);
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items;
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v0");
    flush_vbucket_to_disk(vbid, 1 + 1);

    // This first "vegetable" item isn't "history" until retention is configured
    auto& epVB = dynamic_cast<EPVBucket&>(*store->getVBucket(vbid));
    EXPECT_EQ(0, epVB.getHistoricalItemsFlushed());

    // Now history begins here
    store->setHistoryRetentionBytes(1024 * 1024 * 100);
    setHistoryStartSeqno(3);
    store_item(vbid, makeStoredDocKey("b", CollectionEntry::vegetable), "v0");
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(1, epVB.getHistoricalItemsFlushed());

    store_item(vbid, makeStoredDocKey("b", CollectionEntry::vegetable), "v1");
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(2, epVB.getHistoricalItemsFlushed());

    store_item(vbid, makeStoredDocKey("b", CollectionEntry::vegetable), "v2");
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(3, epVB.getHistoricalItemsFlushed());

    // Now store 1 item to default (which will be in the snapshot)
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("default", CollectionID::Default), "val-a"));
    items.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       items.back().getBySeqno() + 1);
    setCollections(cookie, cm.remove(CollectionEntry::vegetable));
    flush_vbucket_to_disk(vbid, 2);

    // At this point this test has flushed a mixture of history=true and false
    // items - check the flush statistic
    EXPECT_EQ(4, epVB.getHistoricalItemsFlushed());
    runCollectionsEraser(vbid);
    ensureDcpWillBackfill();
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);
    // Run the full NonHistory+History backfill
    runBackfill();
    runBackfill();

    // Prior to fixing MB-55557 this snapshot included the history of the
    // dropped vegetable collection
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

// Issue found in MB-55837 and observed as a collection item count difference in
// MB-55817
TEST_P(HistoryScanTest, MB_55837_incorrect_item_count) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));

    std::vector<Item> items;
    items.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       1);

    // Store 2 versions of k0
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v0"));
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "v1"));

    // Next store 2 versions of k1, but using prepare/commit.
    auto vb = store->getVBucket(vbid);
    auto key = StoredDocKey{"turnip", CollectionEntry::vegetable};
    store_item(
            vbid,
            key,
            "v0",
            0,
            {cb::engine_errc::sync_write_pending},
            PROTOCOL_BINARY_RAW_BYTES,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout::Infinity()));
    {
        folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
        ASSERT_EQ(
                cb::engine_errc::success,
                vb->commit(rlh, key, 4, {}, vb->lockCollections(key), nullptr));
    }

    // put a "committed" item in at seqno:5
    items.emplace_back(make_item(vbid, key, "v0"));
    items.back().setBySeqno(5);
    items.back().setRevSeqno(1);
    items.back().setDataType(PROTOCOL_BINARY_RAW_BYTES);

    store_item(
            vbid,
            key,
            "v1",
            0,
            {cb::engine_errc::sync_write_pending},
            PROTOCOL_BINARY_RAW_BYTES,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout::Infinity()));

    {
        folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
        ASSERT_EQ(
                cb::engine_errc::success,
                vb->commit(rlh, key, 6, {}, vb->lockCollections(key), nullptr));
    }

    // put a "committed" item in at seqno:7
    items.emplace_back(make_item(vbid, key, "v1"));
    items.back().setBySeqno(7);
    items.back().setRevSeqno(2);
    items.back().setDataType(PROTOCOL_BINARY_RAW_BYTES);

    // Flush everything in 1 batch and check the item counts
    flush_vbucket_to_disk(vbid, 7);

    // 2 keys are in the collection (although we have history available for 4
    // versions)
    EXPECT_EQ(2,
              vb->lockCollections().getItemCount(CollectionEntry::vegetable));
    EXPECT_EQ(2, vb->getNumItems());

    ensureDcpWillBackfill();
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
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
                     6 /*hcs*/,
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
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test-producer",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false /*startTask*/);
    ASSERT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::ChangeStreams, "true"));

    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto stream =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               DcpAddStreamFlag::DiskOnly,
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
    EXPECT_EQ(DCPBackfill::State::Create, backfill->getState());
    // stream will error markDiskSnapshot
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Ok);
    EXPECT_EQ(backfill_finished, backfill->run());
}

// This test covers MB-56565 where eager checkpoint removal can lead to a DCP
// stream being stuck perpetually in backfilling, even though there's no memory
// pressure. The bug occurred because DCP's own cursor re-registration path
// would trigger the removal of the checkpoint it needed to regain in-memory
// streaming.
TEST_P(HistoryScanTest, MB_56565) {
    producers->consumer = nullptr;
    CollectionsManifest cm;
    // Create 1 history enabled collection
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    flush_vbucket_to_disk(vbid, 1);
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);
    // Move DCP away from seqno:0 and receive the first message.
    notifyAndStepToCheckpoint();
    stepAndExpect(ClientOpcode::DcpSystemEvent);
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    EXPECT_TRUE(stream->isInMemory());
    // Now drop the DCP cursor and check we can recover to in-memory
    stream->handleSlowStream();
    // Now push the vbucket along, flush an call ensureDcpWillBackfill ensuring
    // that DCP can't switch straight back to in-memory
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "s2");
    flush_vbucket_to_disk(vbid, 1);
    ensureDcpWillBackfill();
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "s3");
    flush_vbucket_to_disk(vbid, 1);
    // DCP step - memory->backfilling
    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
    ASSERT_TRUE(stream->isBackfilling());
    // Now push the vbucket along again and importantly flush after the snapshot
    // is opened so DCP backfill cannot get these two items and importantly that
    // the persistence cursor is on seqno:5 so that the checkpoint for seqno:4
    // is currently unreferenced.
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "s4");
    store_item(vbid, makeStoredDocKey("k0", CollectionEntry::vegetable), "s5");
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(1, stream->getLastReadSeqno());
    auto cursor = stream->getCursor().lock();
    ASSERT_TRUE(cursor);
    EXPECT_EQ(3, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(3, (*cursor->getCheckpoint())->getMinimumCursorSeqno());
    // backfill:create and scan
    runNextTask(lpAuxioQ);
    // flush after the snapshot is opened to move persistence cursor to the end
    flush_vbucket_to_disk(vbid, 2);
    // For this MB, markDiskSnapshot has opened a snapshot that ends with seqno
    // 3 and wants to register a cursor from seqno 3. As markDiskSnapshot
    // calls registerCursor(3) the CP manager can initially satisfy the request
    // but internally eager checkpoint removal is triggered and the checkpoint
    // for seqno 4 is removed. The rest of the registerCursor code then cannot
    // satisfy from seqno 3 and a second backfill is scheduled. This entire loop
    // can be perpetually cycled if the flusher just keeps the persistence
    // cursor ahead so each markDiskSnapshot removes the checkpoint it needs.
    EXPECT_EQ(3, stream->getLastReadSeqno());
    cursor = stream->getCursor().lock();
    ASSERT_TRUE(cursor);
    EXPECT_EQ(4, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(4, (*cursor->getCheckpoint())->getMinimumCursorSeqno());
    // backfill:finished()
    runNextTask(lpAuxioQ);
    EXPECT_TRUE(stream->isBackfilling());
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(2, producers->last_snap_start_seqno);
    EXPECT_EQ(3, producers->last_snap_end_seqno);
    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(2, producers->last_byseqno);
    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(3, producers->last_byseqno);
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
    // Stream would remain in backfilling with MB-56565
    EXPECT_TRUE(stream->isInMemory());
    // run checkpoint processor for in-memory continuation
    notifyAndStepToCheckpoint();
    EXPECT_EQ(4, producers->last_snap_start_seqno);
    EXPECT_EQ(4, producers->last_snap_end_seqno);
    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(4, producers->last_byseqno);
    // CDC: new snapshot for seq:5
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(5, producers->last_snap_start_seqno);
    EXPECT_EQ(5, producers->last_snap_end_seqno);
    stepAndExpect(ClientOpcode::DcpMutation);
    EXPECT_EQ(5, producers->last_byseqno);
}

TEST_P(HistoryScanTest, GetHistoryTimeNow) {
    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    // vb:0 we can get a time
    EXPECT_TRUE(config.magmaCfg.GetHistoryTimeNow(0));

    // vb:200 not-my-vbucket, cannot determine a time
    EXPECT_FALSE(config.magmaCfg.GetHistoryTimeNow(200));
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
