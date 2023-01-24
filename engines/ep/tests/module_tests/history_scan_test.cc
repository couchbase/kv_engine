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
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_magma_kvstore.h"
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
        CollectionsDcpParameterizedTest::SetUp();
        // To allow tests to set where history begins, use MockMagmaKVStore
        replaceMagmaKVStore();
        // @todo: Setup to retain history using setHistoryRetentionBytes
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
    // The entire disk is "history", from seqno 1
    setHistoryStartSeqno(1);

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

// Following test cannot be enabled until magma history support exists
TEST_P(HistoryScanTest, DISABLED_basic_duplicates) {
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items;
    // Create a "dummy" Item that marks where the system-event is expected
    items.emplace_back(makeStoredDocKey("a", CollectionEntry::vegetable),
                       vbid,
                       queue_op::system_event,
                       0,
                       1);
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v0"));
    // temp flush in two batches as dedup is still on in the flusher
    flush_vbucket_to_disk(vbid, 1 + 1);
    items.emplace_back(store_item(
            vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v1"));
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

    createDcpObjects(DcpProducerConfig{"test_producer",
                                       OutOfOrderSnapshots::No,
                                       true,
                                       ChangeStreams::Yes,
                                       IncludeXattrs::Yes,
                                       IncludeDeleteTime::Yes},
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

// Test that a scan which starts below the history window delivers two snapshots
TEST_P(HistoryScanTest, TwoSnapshots) {
    // history will begin at seqno 4, which means a backfill returns two
    // snapshot markers so that a DCP client can see when history begins
    setHistoryStartSeqno(4);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable, {}, true));
    std::vector<Item> items1, items2;

    // items1 represents the first snapshot, only the crate of vegetable will
    // exist in this snapshot. The second history snapshot will have the 'a'
    // keys (both versions).
    items1.emplace_back(makeStoredDocKey("", CollectionEntry::vegetable),
                        vbid,
                        queue_op::system_event,
                        0,
                        1);
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v0");
    flush_vbucket_to_disk(vbid, 1 + 1);
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v1");
    flush_vbucket_to_disk(vbid, 1);

    // Now we must force history to begin from the next flush
    items2.emplace_back(store_item(
            vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "v2"));
    flush_vbucket_to_disk(vbid, 1);
    // @todo: switch to a using key 'a' when magma history support is available
    // then we can verify two versions of 'a' are returned
    items2.emplace_back(store_item(
            vbid, makeStoredDocKey("b", CollectionEntry::vegetable), "v3"));
    flush_vbucket_to_disk(vbid, 1);

    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     0,
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();

    VBucketPtr vbR = store->getVBucket(replicaVB);
    VBucketPtr vbA = store->getVBucket(vbid);

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
    setHistoryStartSeqno(1);

    // This writes to fruit and vegetable
    auto highSeqno = setupTwoCollections().second;

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

    uint64_t txHighSeqno = 0;
    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};

    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(k, producers->last_key) << producers->last_byseqno;
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
        txHighSeqno = std::max(txHighSeqno, producers->last_byseqno.load());
    }

    // Now we get the end message
    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    // Now we get the second snapshot
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);

    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK | MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS,
              producers->last_flags);
    stepAndExpect(ClientOpcode::DcpSystemEvent);

    // And all keys in seq order. Setup created in order b, d, a, c
    std::array<std::string, 4> keySeqnoOrder = {{"b", "d", "a", "c"}};
    for (auto& k : keySeqnoOrder) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        stepAndExpect(ClientOpcode::DcpMutation);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    }
}

INSTANTIATE_TEST_SUITE_P(HistoryScanTests,
                         HistoryScanTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);