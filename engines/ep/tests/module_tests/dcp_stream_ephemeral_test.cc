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

#include "dcp_stream_ephemeral_test.h"

#include "checkpoint_utils.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill-manager.h"
#include "dcp/backfill_memory.h"
#include "dcp/response.h"
#include "ephemeral_bucket.h"
#include "ephemeral_vb.h"
#include "test_helpers.h"

#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"

/* MB-24159 - Test to confirm a dcp stream backfill from an ephemeral bucket
 * over a range which includes /no/ items doesn't cause the producer to
 * segfault.
 */

TEST_P(EphemeralStreamTest, backfillGetsNoItems) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    store_item(vbid, "key", "value1");
    store_item(vbid, "key", "value2");

    auto evb = std::shared_ptr<EphemeralVBucket>(
            std::dynamic_pointer_cast<EphemeralVBucket>(vb0));
    DCPBackfillMemoryBuffered dcpbfm(evb, stream, 1, 1);
    dcpbfm.run();
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

TEST_P(EphemeralStreamTest, bufferedMemoryBackfillPurgeGreaterThanStart) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    auto evb = std::shared_ptr<EphemeralVBucket>(
            std::dynamic_pointer_cast<EphemeralVBucket>(vb0));

    // Force the purgeSeqno because it's easier than creating and
    // deleting items
    evb->setPurgeSeqno(3);

    // Backfill with start != 1 and start != end and start < purge
    DCPBackfillMemoryBuffered dcpbfm(evb, stream, 2, 4);
    dcpbfm.run();
    EXPECT_TRUE(stream->isDead());
}

/* Checks that DCP backfill in Ephemeral buckets does not have duplicates in
 a snaphsot */
TEST_P(EphemeralStreamTest, EphemeralBackfillSnapshotHasNoDuplicates) {
    auto* evb = dynamic_cast<EphemeralVBucket*>(vb0.get());

    /* Add 4 items */
    const int numItems = 4;
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        store_item(vbid, key, "value");
    }

    /* Update "key1" before range read cursors are on vb */
    store_item(vbid, "key1", "value1");

    /* Add fake range read cursor on vb and update items */
    {
        auto itr = evb->makeRangeIterator(/*isBackfill*/ true);
        /* update 'key2' and 'key3' */
        store_item(vbid, "key2", "value1");
        store_item(vbid, "key3", "value1");
    }

    /* update key2 once again with a range iterator again so that it has 2 stale
     values */
    {
        auto itr = evb->makeRangeIterator(/*isBackfill*/ true);
        /* update 'key2' */
        store_item(vbid, "key2", "value1");
    }

    removeCheckpoint(numItems);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);

    // transitionStateToBackfilling should set isBackfillTaskRunning to true
    // which will not be reset until the task finishes which we will use to
    // block this thread.
    stream->transitionStateToBackfilling();

    /* Wait for the backfill task to complete */
    {
        std::chrono::microseconds uSleepTime(128);
        while (stream->public_isBackfillTaskRunning()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Verify that only 4 items are read in the backfill (no duplicates) */
    EXPECT_EQ(numItems, stream->getNumBackfillItems());

    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

// Ephemeral only
INSTANTIATE_TEST_SUITE_P(Ephemeral,
                         EphemeralStreamTest,
                         ::testing::Values("ephemeral"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

/**
 * Test verifies that Backfill skips a "stale item with replacement" in the case
 * where the stale item is at RangeItr::begin. Test for Normal writes.
 */
TEST_P(STActiveStreamEphemeralTest, MB_43847_NormalWrite) {
    // We need to re-create the stream in a condition that triggers a backfill
    stream.reset();
    producer.reset();

    auto& vb = dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    store_item(vbid, key, value);
    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());

    // Cover seqno 1 with a range-read, then queue an update.
    // We cannot touch seqnos within the range-read, so we append a new OSV at
    // the end of the SeqList.
    {
        const auto rangeIt = vb.makeRangeIterator(false /*isBackfill*/);
        ASSERT_TRUE(rangeIt);

        store_item(vbid,
                   key,
                   value,
                   0 /*exptime*/,
                   {cb::engine_errc::success},
                   PROTOCOL_BINARY_RAW_BYTES,
                   {},
                   false /*deleted*/);
    }
    EXPECT_EQ(2, vb.getHighSeqno());
    // Note: This would be 1 with no range-read
    EXPECT_EQ(2, vb.getSeqListNumItems());

    // Steps to ensure backfill when we re-create the stream in the following
    manager.createNewCheckpoint();
    ASSERT_EQ(2, list.size());
    const auto openCkptId = manager.getOpenCheckpointId();
    ASSERT_EQ(1, manager.removeClosedUnrefCheckpoints(vb));
    // No new checkpoint created
    ASSERT_EQ(openCkptId, manager.getOpenCheckpointId());
    ASSERT_EQ(1, list.size());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Re-create producer and stream
    recreateProducerAndStream(vb, 0 /*flags*/);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    // Drive the backfill - execute
    auto& bfm = producer->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());

    // Backfill::create - Verify SnapMarker{start:0, end:2}
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(2, snapMarker.getEndSeqno());

    // Verify only s:2 sent at Backfill::scan.
    // Before the fix backfill contains also s:1.
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(2, *resp->getBySeqno());
    ASSERT_EQ(0, readyQ.size());
}

/**
 * Test verifies that Backfill skips a "stale item with replacement" in the case
 * where the stale item is at RangeItr::begin. Test for Sync writes.
 */
TEST_P(STActiveStreamEphemeralTest, MB_43847_SyncWrite) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // We need to re-create the stream in a condition that triggers a backfill
    stream.reset();
    producer.reset();

    auto& vb = dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(0, vb.getSeqListNumItems());
    ASSERT_EQ(0, vb.getSeqListNumStaleItems());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());

    // SyncWrite and Commit
    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    store_item(vbid,
               key,
               value,
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements(),
               false /*deleted*/);
    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());
    EXPECT_EQ(0, vb.getSeqListNumStaleItems());
    EXPECT_EQ(cb::engine_errc::success,
              vb.commit(key, 1 /*prepareSeqno*/, {}, vb.lockCollections(key)));
    EXPECT_EQ(2, vb.getHighSeqno());
    EXPECT_EQ(2, vb.getSeqListNumItems());
    EXPECT_EQ(0, vb.getSeqListNumStaleItems());

    // Cover seqnos [1, 2] with a range-read, then queue another Prepare.
    {
        const auto rangeIt = vb.makeRangeIterator(false /*isBackfill*/);
        ASSERT_TRUE(rangeIt);

        store_item(vbid,
                   key,
                   value,
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending},
                   PROTOCOL_BINARY_RAW_BYTES,
                   cb::durability::Requirements(),
                   false /*deleted*/);
    }
    EXPECT_EQ(3, vb.getHighSeqno());
    EXPECT_EQ(3, vb.getSeqListNumItems());
    EXPECT_EQ(1, vb.getSeqListNumStaleItems());

    // Steps to ensure backfill when we re-create the stream in the following
    manager.createNewCheckpoint();
    ASSERT_EQ(3, list.size());
    EXPECT_EQ(3, manager.removeClosedUnrefCheckpoints(vb));
    ASSERT_EQ(1, list.size());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Re-create producer and stream
    recreateProducerAndStream(vb, 0 /*flags*/);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_TRUE(stream->public_supportSyncReplication());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    // Drive the backfill - execute
    auto& bfm = producer->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());

    // Backfill::create - Verify SnapMarker{start:0, end:3}
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(3, snapMarker.getEndSeqno());

    // Verify only seqnos [2, 3] sent at Backfill::scan.
    // Before the fix backfill contains also s:1.
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(2, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(2, *resp->getBySeqno());
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(3, *resp->getBySeqno());
    ASSERT_EQ(0, readyQ.size());
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         STActiveStreamEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

void STPassiveStreamEphemeralTest::SetUp() {
    config_string = "ephemeral_metadata_purge_age=0";
    SingleThreadedPassiveStreamTest::SetUp();
}

/**
 * The test verifies that the SeqList::numDeletedItems counter is correctly
 * incremented when a Replica vbucket receives multiple deletions for the same
 * key in a row. The test verifies both normal and sync deletions.
 */
void STPassiveStreamEphemeralTest::test_MB_44139(
        const std::optional<cb::durability::Requirements>& durReqs) {
    // Test relies on that the HTCleaner does its work when it runs
    ASSERT_EQ(0, engine->getConfiguration().getEphemeralMetadataPurgeAge());

    auto& vb = dynamic_cast<EphemeralVBucket&>(*store->getVBucket(vbid));
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(0, vb.getSeqListNumItems());
    ASSERT_EQ(0, vb.getSeqListNumDeletedItems());

    // Receive marker for a Disk snapshot
    // Note: For the SyncDel test replica receives a single Snap{Disk, 1, 5},
    // while for the NormalDel test Snap{Disk, 1, 2} + Snap{Memory, 3, 5}
    const uint32_t opaque = 1;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       1 /*start*/,
                                       durReqs ? 5 : 2 /*end*/,
                                       MARKER_FLAG_CHK | MARKER_FLAG_DISK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    const auto keyA = DocKey("keyA", DocKeyEncodesCollectionId::No);
    if (durReqs) {
        // Receive SyncDelete:1
        const std::string value("value");
        cb::const_byte_buffer valueBuf{
                reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->prepare(opaque,
                                    keyA,
                                    valueBuf,
                                    0 /*priv_bytes*/,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    0 /*cas*/,
                                    vbid,
                                    0 /*flags*/,
                                    1 /*bySeqno*/,
                                    0 /*revSeqno*/,
                                    0 /*exp*/,
                                    0 /*lockTime*/,
                                    0 /*nru*/,
                                    DocumentState::Deleted,
                                    durReqs->getLevel()));
        EXPECT_EQ(1, vb.getHighSeqno());
        EXPECT_EQ(1, vb.getSeqListNumItems());
        EXPECT_EQ(1, vb.getSeqListNumDeletedItems());
    } else {
        // Receive DEL:2
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->deletion(opaque,
                                     keyA,
                                     {} /*value*/,
                                     0 /*priv_bytes*/,
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0 /*cas*/,
                                     vbid,
                                     2 /*seqno*/,
                                     0 /*revSeqno*/,
                                     {} /*meta*/));
        EXPECT_EQ(2, vb.getHighSeqno());
        EXPECT_EQ(1, vb.getSeqListNumItems());
        EXPECT_EQ(1, vb.getSeqListNumDeletedItems());
    }

    // Note: In the NormalDel test replica receives 2 DELs in a row, so by logic
    // the second one must be in a second snapshot.
    if (!durReqs) {
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->snapshotMarker(opaque,
                                           vbid,
                                           3 /*start*/,
                                           5 /*end*/,
                                           MARKER_FLAG_CHK | MARKER_FLAG_MEMORY,
                                           {} /*HCS*/,
                                           {} /*maxVisibleSeqno*/));
    }

    // Receive DEL:4 while there is a range-read in place (eg, TombstonePurger
    // is running).
    // Note: For replica is legal to receive 2 DELs in a row for the same
    // key, as mutations in the middle may have been deduplicated
    {
        const auto range = vb.makeRangeIterator(false /*backfill*/);

        EXPECT_EQ(cb::engine_errc::success,
                  consumer->deletion(opaque,
                                     keyA,
                                     {} /*value*/,
                                     0 /*priv_bytes*/,
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0 /*cas*/,
                                     vbid,
                                     4 /*seqno*/,
                                     0 /*revSeqno*/,
                                     {} /*meta*/));
    }

    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(2, vb.getSeqListNumItems());
    // Core check: Before the fix num-deleted-items stays 1.
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // Receive MUT:5 for a different key.
    // This step is necessary for the TombstonePurger to touch DEL:4, as it
    // would skip it if it's the latest item in the SeqList
    const auto keyB = DocKey("keyB", DocKeyEncodesCollectionId::No);
    const std::string value("value");
    cb::const_byte_buffer valueBuf{
            reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyB,
                                 valueBuf,
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 5 /*seqno*/,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));
    EXPECT_EQ(5, vb.getHighSeqno());
    EXPECT_EQ(3, vb.getSeqListNumItems());
    EXPECT_EQ(2, vb.getSeqListNumDeletedItems());

    // Before the fix this throws as we try to decrement num-deleted-item to -1
    runEphemeralHTCleaner();

    EXPECT_EQ(5, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getSeqListNumItems());
    EXPECT_EQ(0, vb.getSeqListNumDeletedItems());
}

TEST_P(STPassiveStreamEphemeralTest, MB_44139_NormalDel) {
    test_MB_44139({});
}

TEST_P(STPassiveStreamEphemeralTest, MB_44139_SyncDel) {
    test_MB_44139(cb::durability::Requirements());
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         STPassiveStreamEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);