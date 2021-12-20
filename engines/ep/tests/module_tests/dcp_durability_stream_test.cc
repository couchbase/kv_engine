/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_durability_stream_test.h"
#include <engines/ep/src/ephemeral_vb.h>
#include <engines/ep/tests/mock/mock_dcp.h>
#include <engines/ep/tests/mock/mock_ephemeral_vb.h>

#include "../../src/dcp/backfill-manager.h"
#include "checkpoint.h"
#include "checkpoint_test_impl.h"
#include "checkpoint_utils.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "durability/active_durability_monitor.h"
#include "durability/durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_bucket.h"
#include "kvstore/kvstore.h"
#include "test_helpers.h"
#include "vbucket_state.h"
#include "vbucket_utils.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_replicationthrottle.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityActiveStreamTest::SetUp() {
    SingleThreadedActiveStreamTest::SetUp();
    setUp(false /*startCheckpointProcessorTask*/);
}

void DurabilityActiveStreamTest::TearDown() {
    SingleThreadedActiveStreamTest::TearDown();
}

void DurabilityActiveStreamTest::setUp(bool startCheckpointProcessorTask,
                                       bool persist) {
    setVBucketState(vbid,
                    vbucket_state_active,
                    {{"topology", nlohmann::json::array({{active, replica}})}});

    if (isPersistent() && persist) {
        // Trigger the flusher to flush state to disk.
        const auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
        EXPECT_EQ(0, res.numFlushed);
    }

    // Enable SyncReplication and flow-control (Producer BufferLog)
    setupProducer({{"enable_sync_writes", "true"},
                   {"connection_buffer_size", "52428800"},
                   {"consumer_name", "test_consumer"}},
                  startCheckpointProcessorTask);
    ASSERT_TRUE(stream->public_supportSyncReplication());
}

void DurabilityActiveStreamTest::testSendDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};
    {
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }
    vb->notifyActiveDMOfLocalSyncWrite();

    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We do in HT stats
    EXPECT_EQ(1, vb->ht.getNumItems());

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    const auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_EQ(value, (*it)->getValue()->to_s());

    // We must have ckpt-start + Prepare
    auto outstandingItemsResult = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outstandingItemsResult.items.size());
    ASSERT_EQ(queue_op::checkpoint_start,
              outstandingItemsResult.items.at(0)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write,
              outstandingItemsResult.items.at(1)->getOperation());
    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outstandingItemsResult);

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a SnapshotMarker (+ a Prepare)
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    // Only a prepare exists, so maxVisible remains 0
    EXPECT_EQ(
            0,
            dynamic_cast<SnapshotMarker&>(*resp).getMaxVisibleSeqno().value_or(
                    ~0));

    // Simulate the Replica ack'ing the SnapshotMarker's bytes
    auto bytesOutstanding = producer->getBytesOutstanding();
    ASSERT_GT(bytesOutstanding, 0);
    producer->ackBytesOutstanding(bytesOutstanding);
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a DCP_PREPARE
    ASSERT_EQ(1, stream->public_readyQSize());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(prepareSeqno, *resp->getBySeqno());
    auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_EQ(value, prepare.getItem()->getValue()->to_s());
    EXPECT_EQ(cas, prepare.getItem()->getCas());

    // The expected size of a DCP_PREPARE is 57 + key-size + value-size.
    // Note that the base-size=57 is similar to the one of a DCP_MUTATION
    // (55), + 1 for delete-flag, + 3 for durability-requirements, - 2 for
    // missing optional-extra-length.
    bytesOutstanding =
            57 + key.makeDocKeyWithoutCollectionID().size() + value.size();
    ASSERT_EQ(bytesOutstanding, producer->getBytesOutstanding());
    // Simulate the Replica ack'ing the Prepare's bytes
    producer->ackBytesOutstanding(bytesOutstanding);
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ empty now
    ASSERT_EQ(0, stream->public_readyQSize());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_FALSE(resp);
}

TEST_P(DurabilityActiveStreamTest, SendDcpPrepare) {
    testSendDcpPrepare();
}

void DurabilityActiveStreamTest::testSendCompleteSyncWrite(Resolution res) {
    // First, we need to enqueue a Prepare.
    testSendDcpPrepare();
    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");
    const uint64_t prepareSeqno = 1;
    {
        ASSERT_FALSE(vb->ht.findForRead(key).storedValue);
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
    }

    // Now we proceed with testing the Commit/Abort of that Prepare
    auto& ckptMgr = *vb->checkpointManager;

    // The seqno of the Committed/Aborted item
    const auto completedSeqno = prepareSeqno + 1;

    // Used later to verify tha tabort queued into a new checkpoint
    const auto openId = ckptMgr.getOpenCheckpointId();

    switch (res) {
    case Resolution::Commit: {
        // FirstChain on Active has been set to {active, replica}. Given that
        // active has already implicitly ack'ed (as we have queued a Level
        // Majority Prepare), simulating a SeqnoAck received from replica
        // satisfies Durability Requirements and triggers Commit. So, the
        // following indirectly calls VBucket::commit
        simulateStreamSeqnoAck(replica, prepareSeqno);
        // Note: At FE we have an exact item count only at persistence.
        if (ephemeral() || !fullEviction()) {
            EXPECT_EQ(1, vb->getNumItems());
        } else {
            EXPECT_EQ(0, vb->getNumItems());
        }
        ASSERT_TRUE(vb->ht.findForWrite(key).storedValue);
        const auto sv = vb->ht.findForRead(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaPrepare,
                  sv.storedValue->getCommitted());
        ASSERT_EQ(completedSeqno, sv.storedValue->getBySeqno());
        break;
    }
    case Resolution::Abort:
        // Simulate timeout, indirectly calls VBucket::abort
        vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                     std::chrono::milliseconds(1000));
        EXPECT_EQ(0, vb->getNumItems());
        break;
    }
    vb->processResolvedSyncWrites();

    // Verify state of the checkpoint(s).
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    if (res == Resolution::Abort) {
        // Note: We avoid de-duplication of durability-items (Prepare/Abort)
        // by:
        // (1) closing the open checkpoint (the one that contains the Prepare)
        // (2) creating a new open checkpoint
        // (3) queueing the Commit/Abort in the new open checkpoint
        ASSERT_GT(ckptMgr.getOpenCheckpointId(), openId);
    }

    const auto* ckpt = ckptList.back().get();
    EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    EXPECT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    it++;

    switch (res) {
    case Resolution::Commit:
        // For Commit, Prepare is in the same checkpoint.
        ASSERT_EQ(2, ckpt->getNumItems());
        EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
        it++;
        EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
        EXPECT_TRUE((*it)->getValue()) << "Commit should carry a value";
        break;
    case Resolution::Abort:
        // For Abort, the prepare is in the previous checkpoint.
        ASSERT_EQ(1, ckpt->getNumItems());
        EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
        EXPECT_FALSE((*it)->getValue()) << "Abort should carry no value";
        break;
    }

    // Fetch items via DCP stream.
    auto outstandingItemsResult = stream->public_getOutstandingItems(*vb);
    uint64_t expectedVisibleSeqno = 0;
    switch (res) {
    case Resolution::Commit:
        expectedVisibleSeqno = 2;
        ASSERT_EQ(1, outstandingItemsResult.items.size())
                << "Expected 1 item (Commit)";
        EXPECT_EQ(queue_op::commit_sync_write,
                  outstandingItemsResult.items.at(0)->getOperation());
        break;
    case Resolution::Abort:
        ASSERT_EQ(2, outstandingItemsResult.items.size())
                << "Expected 2 items (CkptStart, Abort)";
        EXPECT_EQ(queue_op::checkpoint_start,
                  outstandingItemsResult.items.at(0)->getOperation());
        EXPECT_EQ(queue_op::abort_sync_write,
                  outstandingItemsResult.items.at(1)->getOperation());
        break;
    }

    // readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());

    // Push items into readyQ
    stream->public_processItems(outstandingItemsResult);

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain SnapshotMarker
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(expectedVisibleSeqno, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_FALSE(marker.getHighCompletedSeqno().has_value());
    EXPECT_EQ(2, marker.getEndSeqno());

    // Simulate the Replica ack'ing the SnapshotMarker's bytes
    auto bytesOutstanding = producer->getBytesOutstanding();
    ASSERT_GT(bytesOutstanding, 0);
    producer->ackBytesOutstanding(bytesOutstanding);
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain DCP_COMMIT/DCP_ABORT
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    switch (res) {
    case Resolution::Commit: {
        EXPECT_EQ(DcpResponse::Event::Commit, resp->getEvent());
        const auto& commit = dynamic_cast<CommitSyncWrite&>(*resp);
        EXPECT_EQ(key, commit.getKey());
        EXPECT_EQ(completedSeqno, *commit.getBySeqno());
        break;
    }
    case Resolution::Abort: {
        EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());
        const auto& abort = dynamic_cast<AbortSyncWrite&>(*resp);
        EXPECT_EQ(key, abort.getKey());
        EXPECT_EQ(prepareSeqno, abort.getPreparedSeqno());
        EXPECT_EQ(completedSeqno, abort.getAbortSeqno());
        break;
    }
    }

    // The expected size of a DCP_COMMT / DCP_ABORT is:
    // + 24 (header)
    // + 8  (prepare seqno)
    // + 8 (Commit/Abort seqno)
    // + key size (without the collection-ID)
    EXPECT_EQ(24 + 8 + 8 + key.makeDocKeyWithoutCollectionID().size(),
              producer->getBytesOutstanding());

    // readyQ empty now
    EXPECT_EQ(0, stream->public_readyQSize());
    resp = stream->public_popFromReadyQ();
    EXPECT_FALSE(resp);
}

cb::engine_errc DurabilityActiveStreamTest::simulateStreamSeqnoAck(
        const std::string& consumerName, uint64_t preparedSeqno) {
    auto result = stream->seqnoAck(consumerName, preparedSeqno);
    if (result == cb::engine_errc::success) {
        engine->getVBucket(vbid)->processResolvedSyncWrites();
    }
    return result;
}

/*
 * This test checks that the ActiveStream::readyQ contains the right DCP
 * messages during the journey of a Committed sync-write.
 */
TEST_P(DurabilityActiveStreamTest, SendDcpCommit) {
    testSendCompleteSyncWrite(Resolution::Commit);
}

/*
 * This test checks that the ActiveStream::readyQ contains the right DCP
 * messages during the journey of an Aborted sync-write.
 */
TEST_P(DurabilityActiveStreamTest, SendDcpAbort) {
    testSendCompleteSyncWrite(Resolution::Abort);
}

TEST_P(DurabilityActiveStreamTest, BackfillDurabilityLevel) {
    startCheckpointTask();
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));

    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());

    // Required at for transitioning to backfill at EPBucket
    flushVBucketToDiskIfPersistent(vbid, 1 /*num expected flushed*/);

    stream.reset();
    removeCheckpoint(*vb, 1);

    recreateStream(*vb);
    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state
    auto& bfm = producer->getBFM();
    bfm.backfill(); // create
    bfm.backfill(); // scan

    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(2, readyQ.size());

    auto resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    EXPECT_EQ(0, marker.getMaxVisibleSeqno().value_or(~0));

    EXPECT_EQ(0, *marker.getHighCompletedSeqno());
    EXPECT_EQ(0, marker.getStartSeqno());
    EXPECT_EQ(1, marker.getEndSeqno());

    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prep = static_cast<MutationResponse&>(*resp);
    const auto respItem = prep.getItem();
    EXPECT_EQ(cb::durability::Level::Majority,
              respItem->getDurabilityReqs().getLevel());
    EXPECT_TRUE(respItem->getDurabilityReqs().getTimeout().isInfinite());
}

TEST_P(DurabilityActiveStreamTest, AbortWithBackfillPrepare) {
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Drop our stream early, we want to remove checkpoints from the checkpoint
    // manager
    stream.reset();
    auto vb = engine->getVBucket(vbid);

    auto& ckptMgr = *vb->checkpointManager;

    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0,
              stats.itemsRemovedFromCheckpoints +
                      stats.itemsExpelledFromCheckpoints);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    auto prepareSeqno = vb->getHighSeqno();

    const auto openId = ckptMgr.getOpenCheckpointId();
    EXPECT_EQ(cb::engine_errc::success,
              vb->abort(key,
                        prepareSeqno,
                        {} /*abortSeqno*/,
                        vb->lockCollections(key)));
    // Verify abort queued into a new checkpoint
    EXPECT_GT(ckptMgr.getOpenCheckpointId(), openId);

    auto expected = 1;
    if (store->getOneROUnderlying()
                ->getStorageProperties()
                .hasAutomaticDeduplication()) {
        expected++;
    }
    flushVBucketToDiskIfPersistent(vbid, expected);

    // Ensure we have removed the prepare to backfill it
    ASSERT_EQ(1,
              stats.itemsRemovedFromCheckpoints +
                      stats.itemsExpelledFromCheckpoints);

    // Test that we actually dropped the checkpoint by number of items
    ASSERT_EQ(2, ckptMgr.getNumItems());

    // Create our new stream to plant our checkpoint cursor at the abort
    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, 0 /*flags*/, 0 /*opaque*/, *vb);

    stream->setActive();
    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();
    const auto& readyQ = stream->public_readyQ();

    // Just 2 items, SnapshotMarker and Mutation. Prepare has been de-duped by
    // Abort
    ASSERT_EQ(2, readyQ.size());

    // First item is a snapshot marker
    auto resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    EXPECT_EQ(0, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_EQ(1, *marker.getHighCompletedSeqno());
    EXPECT_EQ(0, marker.getStartSeqno());
    EXPECT_EQ(2, marker.getEndSeqno());

    // We don't receive the prepare, just the abort as it de-dupes the
    // prepare.
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());

    producer->cancelCheckpointCreatorTask();
}

// MB-37103: Check that a backfill where the High Completed Seqno is zero
// correctly populates the snapshot marker.
TEST_P(DurabilityActiveStreamTest, BackfillHCSZero) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    auto item = makeCommittedItem(makeStoredDocKey("key"), "value");
    VBQueueItemCtx ctx;
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));

    // Required at for transitioning to backfill at EPBucket
    flushVBucketToDiskIfPersistent(vbid, 1 /*num expected flushed*/);

    // remove the stream and the checkpoint to force a backfill
    stream.reset();
    removeCheckpoint(*vb, 1);
    recreateStream(*vb);

    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state
    auto& bfm = producer->getBFM();
    bfm.backfill(); // create
    bfm.backfill(); // scan

    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(2, readyQ.size()) << "Expected SnapshotMarker and Mutation";

    // First item should be a snapshot marker, with HCS==0.
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    const auto& marker = static_cast<SnapshotMarker&>(*resp);
    ASSERT_TRUE(marker.getHighCompletedSeqno().has_value());
    EXPECT_EQ(0, *marker.getHighCompletedSeqno());
}

TEST_P(DurabilityActiveStreamTest, RemoveUnknownSeqnoAckAtDestruction) {
    testSendDcpPrepare();
    flushVBucketToDiskIfPersistent(vbid, 1);

    // We don't include prepares in the numItems stat (should not exist in here)
    auto vb = engine->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumItems());

    // Our topology gives replica name as "replica" an our producer/stream has
    // name "test_producer". Simulate a seqno ack by calling the vBucket level
    // function.
    ASSERT_NE(producer->getConsumerName(), replica);
    simulateStreamSeqnoAck(producer->getConsumerName(), 1);

    // An unknown seqno ack should not have committed the item
    EXPECT_EQ(0, vb->getNumItems());

    // Disconnect the ActiveStream
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);

    // Attempt to ack the seqno again. The stream is dead so we should not
    // process the ack although we return SUCCESS to avoid tearing down any
    // connections. We verify that the seqno ack does not exist in the map
    // by performing the topology change that would commit the prepare if it
    // did.
    EXPECT_EQ(cb::engine_errc::success,
              simulateStreamSeqnoAck(producer->getConsumerName(), 1));

    // If the seqno ack still existed in the queuedSeqnoAcks map then it would
    // result in a commit on topology change
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array(
                      {{"active", "replica1", producer->getConsumerName()}})}});

    EXPECT_EQ(0, vb->getNumItems());
}

TEST_P(DurabilityActiveStreamTest, RemoveCorrectQueuedAckAtStreamSetDead) {
    testSendDcpPrepare();
    flushVBucketToDiskIfPersistent(vbid, 1);

    // We don't include prepares in the numItems stat (should not exist in here)
    auto vb = engine->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumItems());

    // Our topology gives replica name as "replica" an our producer/stream has
    // name "test_producer". Simulate a seqno ack by calling the vBucket level
    // function.
    simulateStreamSeqnoAck(producer->getConsumerName(), 1);

    // Disconnect the ActiveStream. Should remove the queued seqno ack
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);

    // remove the stream and the checkpoint to force a backfill
    stream.reset();
    removeCheckpoint(*vb, 1);

    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, 0 /*flags*/, 0 /*opaque*/, *vb);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    stream->setActive();

    // Process items to ensure that lastSentSeqno is GE the seqno that we will
    // ack
    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();
    EXPECT_EQ(2, stream->public_readyQSize());
    stream->consumeBackfillItems(*producer, 2);

    // Should not throw a monotonic exception as the ack should have been
    // removed by setDead.
    simulateStreamSeqnoAck(producer->getConsumerName(), 1);

    producer->cancelCheckpointCreatorTask();
}

void DurabilityActiveStreamTest::setUpSendSetInsteadOfCommitTest() {
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0,
              stats.itemsRemovedFromCheckpoints +
                      stats.itemsExpelledFromCheckpoints);

    auto vb = engine->getVBucket(vbid);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    // Seqno 1 - First prepare (the consumer streams this)
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Seqno 2 - Followed by a commit (the consumer does not get this)
    vb->commit(key, vb->getHighSeqno(), {}, vb->lockCollections(key));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto mutationResult =
            persistent() ? MutationStatus::WasClean : MutationStatus::WasDirty;
    // Seqno 3 - A prepare that is deduped
    EXPECT_EQ(mutationResult, public_processSet(*vb, *item, ctx));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Seqno 4 - A commit that the consumer would receive when reconnecting with
    // seqno 1
    vb->commit(key, vb->getHighSeqno(), {}, vb->lockCollections(key));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Create a new checkpoint here to ensure that we always remove the correct
    // number of items from the checkpoints that we remove at the end of this
    // function.
    vb->checkpointManager->createNewCheckpoint();

    // Seqno 5 - A prepare to dedupe the prepare at seqno 3.
    EXPECT_EQ(mutationResult, public_processSet(*vb, *item, ctx));
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(2, vb->ht.getNumItems());

    // Drop the stream cursor so that we can drop the closed checkpoints
    EXPECT_TRUE(stream->handleSlowStream());
    auto& mockCkptMgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
    auto expectedCursors = persistent() ? 1 : 0;
    ASSERT_EQ(expectedCursors, mockCkptMgr.getNumOfCursors());

    // Need ensure that some items have been remove from checkpoint so that we
    // can backfill from disk
    ASSERT_GT(stats.itemsRemovedFromCheckpoints +
                      stats.itemsExpelledFromCheckpoints,
              0);
}

TEST_P(DurabilityActiveStreamTest, SendSetInsteadOfCommitForReconnectWindow) {
    setUpSendSetInsteadOfCommitTest();

    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");

    // Disconnect and resume from our prepare
    stream = std::make_shared<MockActiveStream>(engine.get(),
                                                producer,
                                                0 /*flags*/,
                                                0 /*opaque*/,
                                                *vb,
                                                1 /*st_seqno*/,
                                                ~0 /*en_seqno*/,
                                                0x0 /*vb_uuid*/,
                                                1 /*snap_start_seqno*/,
                                                ~1 /*snap_end_seqno*/);

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    auto& bfm = producer->getBFM();
    bfm.backfill();
    // First backfill only sends the SnapshotMarker so repeat
    bfm.backfill();

    // Stream::readyQ must contain SnapshotMarker
    ASSERT_EQ(3, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    EXPECT_EQ(4, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_EQ(3, *marker.getHighCompletedSeqno());
    EXPECT_EQ(1, marker.getStartSeqno());
    EXPECT_EQ(5, marker.getEndSeqno());

    // Followed by a mutation instead of a commit
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    const auto& set = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, set.getItem()->getKey());
    EXPECT_EQ(4, set.getItem()->getBySeqno());

    // Followed by a prepare
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_TRUE(prepare.getItem()->isPending());
    EXPECT_EQ(5, prepare.getItem()->getBySeqno());
}

TEST_P(DurabilityActiveStreamTest, SendSetInsteadOfCommitForNewVB) {
    setUpSendSetInsteadOfCommitTest();

    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");

    // Disconnect and resume from our prepare
    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, 0 /*flags*/, 0 /*opaque*/, *vb);

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    auto& bfm = producer->getBFM();
    bfm.backfill();
    // First backfill only sends the SnapshotMarker so repeat
    bfm.backfill();

    // Stream::readyQ must contain SnapshotMarker
    ASSERT_EQ(3, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    EXPECT_EQ(4, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_EQ(3, marker.getHighCompletedSeqno().value_or(~0));
    EXPECT_EQ(0, marker.getStartSeqno());
    EXPECT_EQ(5, marker.getEndSeqno());

    // Followed by a mutation instead of a commit
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    const auto& set = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, set.getItem()->getKey());
    EXPECT_EQ(4, set.getItem()->getBySeqno());

    // Followed by a prepare
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_TRUE(prepare.getItem()->isPending());
    EXPECT_EQ(5, prepare.getItem()->getBySeqno());
}

/**
 * This test checks that we can deal with a seqno ack from a replica going
 * "backwards" when it shuts down and warms back up. This can happen when a
 * replica acks a Majority level prepare that has not yet been persisted before
 * it shuts down. When it warms up, it will ack the persisted HPS.
 */
TEST_P(DurabilityActiveStreamTest,
       ActiveDealsWithNonMonotonicSeqnoAckOnReconnect) {
    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    { // Locking scope for collections handle
        auto item = makePendingItem(
                key,
                value,
                cb::durability::Requirements(cb::durability::Level::Majority,
                                             cb::durability::Timeout(1)));
        VBQueueItemCtx ctx;
        ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(),
                                           nullptr /*cookie*/};
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }
    vb->notifyActiveDMOfLocalSyncWrite();

    auto items = stream->getOutstandingItems(*vb);
    stream->public_processItems(items);
    stream->consumeBackfillItems(*producer, 1);
    stream->public_nextQueuedItem(*producer);

    EXPECT_EQ(cb::engine_errc::success, simulateStreamSeqnoAck(replica, 1));
    EXPECT_EQ(1, vb->getHighPreparedSeqno());
    EXPECT_EQ(1, vb->getHighCompletedSeqno());

    // Move the DCP cursor
    items = stream->getOutstandingItems(*vb);
    stream->public_processItems(items);

    flushVBucketToDiskIfPersistent(vbid, 2);
    removeCheckpoint(*vb, 2);

    { // Locking scope for collections handle
        auto item = makePendingItem(
                key,
                value,
                cb::durability::Requirements(cb::durability::Level::Majority,
                                             cb::durability::Timeout(1)));
        VBQueueItemCtx ctx;
        ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(),
                                           nullptr /*cookie*/};
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }
    vb->notifyActiveDMOfLocalSyncWrite();

    items = stream->getOutstandingItems(*vb);
    stream->public_processItems(items);
    stream->consumeBackfillItems(*producer, 3);
    stream->public_nextQueuedItem(*producer);

    EXPECT_EQ(cb::engine_errc::success, simulateStreamSeqnoAck(replica, 3));
    EXPECT_EQ(3, vb->getHighPreparedSeqno());
    EXPECT_EQ(3, vb->getHighCompletedSeqno());

    // remove the stream and the checkpoint to force a backfill
    stream.reset();
    flushVBucketToDiskIfPersistent(vbid, 2);
    removeCheckpoint(*vb, 2);

    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, 0 /*flags*/, 0 /*opaque*/, *vb);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    stream->setActive();

    // Process items to ensure that lastSentSeqno is GE the seqno that we will
    // ack
    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();

    // We should only expect the snapshot marker and completed item. As we don't
    // send completed prepares from backfill.
    EXPECT_EQ(2, stream->public_readyQSize());

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    auto marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(4, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_EQ(3, marker.getHighCompletedSeqno().value_or(~0));
    EXPECT_EQ(4, marker.getEndSeqno());

    stream->consumeBackfillItems(*producer, 1);

    EXPECT_EQ(cb::engine_errc::success, simulateStreamSeqnoAck(replica, 1));

    producer->cancelCheckpointCreatorTask();
}

/**
 * Test that a prepare completed whilst we have an active backfill is still
 * sent to a consumer to ensure that when we transition to in memory streaming
 * we don't fail as we don't have a corresponding prepare
 */
TEST_P(DurabilityActiveStreamTest,
       BackfillSnapshotSendsFreshlyCompletedPrepare) {
    // 1) Prepare
    auto vb = engine->getVBucket(vbid);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    flushVBucketToDiskIfPersistent(vbid);
    vb->notifyActiveDMOfLocalSyncWrite();

    // 2) Start backfill but don't process items
    // Remove the stream and the checkpoint to force a backfill
    stream.reset();
    removeCheckpoint(*vb, 1);

    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, 0 /*flags*/, 0 /*opaque*/, *vb);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    stream->setActive();
    auto& bfm = producer->getBFM();
    bfm.backfill();

    // 3) Complete prepare
    EXPECT_EQ(cb::engine_errc::success,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1));
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid);

    // 4) Process backfill, prepare seen
    bfm.backfill();
    ASSERT_EQ(2, stream->public_readyQSize());

    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    const auto& marker = static_cast<SnapshotMarker&>(*resp);
    ASSERT_TRUE(marker.getHighCompletedSeqno().has_value());
    EXPECT_EQ(0, *marker.getHighCompletedSeqno());

    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
}

TEST_P(DurabilityActiveStreamTest, DiskSnapshotSendsHCSWithSyncRepSupport) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = makePendingItem(key, value);

    auto ctx = VBQueueItemCtx();
    ctx.durability = DurabilityItemCtx{item->getDurabilityReqs()};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    vb->notifyActiveDMOfLocalSyncWrite();
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          replica,
                          1 /*prepareSeqno*/);
    vb->processResolvedSyncWrites();

    flushVBucketToDiskIfPersistent(vbid, 1);
    stream.reset();
    removeCheckpoint(*vb, 2);
    recreateStream(*vb);

    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state.
    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a SnapshotMarker
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    auto& marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_CHK);
    ASSERT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    ASSERT_TRUE(marker.getHighCompletedSeqno());
    EXPECT_EQ(1, *marker.getHighCompletedSeqno());
    EXPECT_EQ(2, marker.getMaxVisibleSeqno().value_or(~0));
    EXPECT_EQ(2, marker.getEndSeqno());

    producer->cancelCheckpointCreatorTask();
}

void DurabilityPassiveStreamTest::SetUp() {
    enableSyncReplication = true;
    SingleThreadedPassiveStreamTest::SetUp();
    ASSERT_TRUE(consumer->isSyncReplicationEnabled());
}

void DurabilityPassiveStreamTest::TearDown() {
    SingleThreadedPassiveStreamTest::TearDown();
}

TEST_P(DurabilityPassiveStreamTest, SendSeqnoAckOnStreamAcceptance) {
    // 1) Put something in the vBucket as we won't send a seqno ack if there are
    // no items
    testReceiveDcpPrepare();

    consumer->closeAllStreams();
    uint32_t opaque = 0;
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    EXPECT_EQ(3, stream->public_readyQ().size());
    auto resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamReq, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::AddStream, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SeqnoAcknowledgement, resp->getEvent());
    const auto& ack = static_cast<SeqnoAcknowledgement&>(*resp);
    EXPECT_EQ(1, ack.getPreparedSeqno());
}

/**
 * This test demonstrates what happens to the acked seqno on the replica when
 * we shutdown and restart having previously acked a seqno that is not flushed.
 * This can cause the acked seqno to go "backwards".
 */
TEST_P(DurabilityPassiveStreamPersistentTest,
       ReplicaSeqnoAckNonMonotonicIfBounced) {
    // 1) Receive 2 majority prepares but only flush 1
    auto key = makeStoredDocKey("key");
    makeAndReceiveSnapMarkerAndDcpPrepare(key, 0 /*cas*/, 1 /*seqno*/);

    // Flush only the first prepare so when we warmup later we won't have the
    // second
    flushVBucketToDiskIfPersistent(vbid, 1);

    key = makeStoredDocKey("key2");
    makeAndReceiveSnapMarkerAndDcpPrepare(key, 0 /*cas*/, 2 /*seqno*/);

    // 2) Check that we have acked twice, once for each prepare, as each is in
    // it's own snapshot
    ASSERT_EQ(2, stream->public_readyQ().size());
    auto resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SeqnoAcknowledgement, resp->getEvent());
    auto ack = static_cast<SeqnoAcknowledgement&>(*resp);
    EXPECT_EQ(1, ack.getPreparedSeqno());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SeqnoAcknowledgement, resp->getEvent());
    ack = static_cast<SeqnoAcknowledgement&>(*resp);
    EXPECT_EQ(2, ack.getPreparedSeqno());

    // 3) Shutdown and warmup again
    consumer->closeAllStreams();
    consumer.reset();
    resetEngineAndWarmup();

    // 4) Test that the stream now sends a seqno ack of 1 when we reconnect
    uint32_t opaque = 0;
    // Recreate the consumer
    consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    consumer->enableSyncReplication();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    ASSERT_EQ(3, stream->public_readyQ().size());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamReq, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::AddStream, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::SeqnoAcknowledgement, resp->getEvent());
    ack = static_cast<SeqnoAcknowledgement&>(*resp);
    EXPECT_EQ(1, ack.getPreparedSeqno());
}

void DurabilityPassiveStreamPersistentTest::testDiskSnapshotHCSPersisted() {
    testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
            2 /*snapStart*/, 4 /*snapEnd*/, DocumentState::Alive);

    flushVBucketToDiskIfPersistent(vbid, 2);
    {
        auto vb = store->getVBucket(vbid);
        EXPECT_EQ(2, vb->getHighCompletedSeqno());
    }

    // Reset and warmup to check persistence
    consumer->closeAllStreams();
    consumer.reset();
    resetEngineAndWarmup();

    // Recreate the consumer so that our normal test TearDown will work
    setupConsumerAndPassiveStream();
    {
        auto vb = store->getVBucket(vbid);
        EXPECT_EQ(2, vb->getHighCompletedSeqno());
        EXPECT_EQ(4, vb->getHighSeqno());
    }
}

TEST_P(DurabilityPassiveStreamPersistentTest, DiskSnapshotHCSPersisted) {
    testDiskSnapshotHCSPersisted();
}

uint64_t DurabilityPassiveStreamPersistentTest::getPersistedHCS() {
    auto* rwUnderlying = store->getRWUnderlying(vbid);
    const auto* persistedVbState = rwUnderlying->getCachedVBucketState(vbid);
    return persistedVbState->persistedCompletedSeqno;
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       DiskSnapshotHCSNotPersistedBeforeSnapEnd) {
    // Test to ensure the highCompletedSeqno sent as part of a disk snapshot
    // is not persisted by the replica until the entire snapshot is flushed
    // to disk.

    // Send a disk snapshot marker with a HCS of 4
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          5 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          4 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // nothing has been completed yet
    ASSERT_EQ(0, getPersistedHCS());

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";

    using namespace cb::durability;
    auto prepare = makePendingItem(
            key, value, Requirements(Level::Majority, Timeout::Infinity()));
    prepare->setBySeqno(1);
    prepare->setCas(999);

    // Receive prepare at seqno 1
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      prepare,
                      stream->getOpaque(),
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // the HCS should not have advanced yet as we have not completed the disk
    // snapshot. NB: The HCS was received with the snapshot marker, but the
    // replica should not persist that value to disk yet.
    EXPECT_EQ(0, getPersistedHCS());

    // Receive mutation at seqno 2 which completes the prepare
    auto mutation = makeCommittedItem(key, value);
    mutation->setBySeqno(2);
    mutation->setCas(999);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      mutation,
                      stream->getOpaque(),
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // mutation does not advance the HCS
    EXPECT_EQ(0, getPersistedHCS());

    // receive an abort (seqno 4) for a deduped prepare (seqno 3)
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      stream->getOpaque(),
                      vbid,
                      makeStoredDocKey("abortKey"),
                      3 /*prepare*/,
                      4 /*abort*/)));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(0, getPersistedHCS());

    auto unrelated = makeCommittedItem(makeStoredDocKey("unrelatedKey"), value);
    unrelated->setBySeqno(5);
    unrelated->setCas(999);

    // Receive unrelated committed item at seqno 5
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      unrelated,
                      stream->getOpaque(),
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // We should flush the HCS when we flush the last item in the snapshot
    EXPECT_EQ(4, getPersistedHCS());
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       DiskSnapshotHCSIgnoredIfWeaklyMonotonic) {
    testDiskSnapshotHCSPersisted();

    EXPECT_EQ(2, getPersistedHCS());
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          6 /*snapStart*/,
                          7 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          2 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("unrelated");
    auto item = makeCommittedItem(key, "unrelated");
    item->setBySeqno(7);

    // Send the logical commit
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      0 /*opaque*/,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // We don't flush any items but we will run the flusher which will advance
    // use out of the checkpoint. Should not throw any pre-condition due to
    // monotonicity.
    flushVBucketToDiskIfPersistent(vbid, 1);
}

// MB-37103: Receiving a disk snapshot marker with a HCS of zero should be
// correctly accepted - HCS==0 is valid, for example when no SyncWrites have
// occurred on this vBucket.
TEST_P(DurabilityPassiveStreamPersistentTest, DiskSnapshotHCSZeroAccepted) {
    // Emulate a snapshot marker of a single (non-Sync) mutation.
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          1 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto item = makeCommittedItem(makeStoredDocKey("key"), "unrelated");
    item->setBySeqno(1);

    // Send the mutation
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      0 /*opaque*/,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Test: check we can successfully flush to disk with HCS=0.
    flushVBucketToDiskIfPersistent(vbid, 1);
}

TEST_P(DurabilityPassiveStreamTest,
       NoSeqnoAckOnStreamAcceptanceIfNotSupported) {
    consumer->disableSyncReplication();

    // 1) Put something in the vBucket as we won't send a seqno ack if there are
    // no items
    testReceiveDcpPrepare();

    consumer->closeAllStreams();
    uint32_t opaque = 0;
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    ASSERT_EQ(2, stream->public_readyQ().size());
    auto resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamReq, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::AddStream, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    EXPECT_FALSE(resp);
}

void DurabilityPassiveStreamTest::
        testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
                uint64_t snapStart,
                uint64_t snapEnd,
                DocumentState docState,
                bool clearCM) {
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& ckptMgr = *vb->checkpointManager;
    if (clearCM) {
        // Clear everything in CM, start from one single empty open checkpoint
        ckptMgr.clear(0 /*seqno*/);
    }
    uint32_t opaque = 1;

    SnapshotMarker marker(opaque,
                          vbid,
                          snapStart,
                          snapEnd,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          snapStart /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto item = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    item->setBySeqno(snapStart);
    item->setCas(999);

    // Send the prepare
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      item,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    item = makeCommittedItem(key, "committed");
    item->setBySeqno(snapEnd);

    if (docState == DocumentState::Deleted) {
        item->setDeleted(DeleteSource::Explicit);
        item->replaceValue({});
    }

    // Send the logical commit
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Test the HashTable state
    {
        // findForUpdate will return both pending and committed perspectives
        auto res = vb->ht.findForUpdate(key);
        ASSERT_TRUE(res.committed);
        EXPECT_EQ(snapEnd, res.committed->getBySeqno());
        if (docState == DocumentState::Alive) {
            EXPECT_TRUE(res.committed->getValue());
        }
        if (persistent()) {
            EXPECT_FALSE(res.pending);
        } else {
            ASSERT_TRUE(res.pending);
            EXPECT_EQ(snapStart, res.pending->getBySeqno());
            EXPECT_EQ(CommittedState::PrepareCommitted,
                      res.pending->getCommitted());
        }
    }

    // Test the checkpoint manager state
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);

    const auto* ckpt = ckptList.back().get();
    EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    EXPECT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    it++;

    ASSERT_EQ(2, ckpt->getNumItems());
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    it++;

    // The logical commit is a mutation in the checkpoint manager, not a commit.
    EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveMutationInsteadOfCommitWhenStreamingFromDisk) {
    testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
            2 /*snapStart*/, 4 /*snapEnd*/, DocumentState::Alive);
}

void DurabilityPassiveStreamTest::
        receiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDiskMutationFirst(
                DocumentState docState) {
    uint32_t opaque = 1;
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          3 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "mutation");
    item->setBySeqno(1);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
            2 /*snapStart*/, 4 /*snapEnd*/, docState);
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveMutationInsteadOfCommitOnTopOfMutation) {
    receiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDiskMutationFirst(
            DocumentState::Alive);
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveDeletionInsteadOfCommitOnTopOfMutation) {
    receiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDiskMutationFirst(
            DocumentState::Deleted);
}

void DurabilityPassiveStreamTest::
        testReceiveMutationOrDeletionInsteadOfCommitForReconnectWindowWithPrepareLast(
                DocumentState docState) {
    // 1) Receive DCP Prepare
    auto key = makeStoredDocKey("key");
    uint64_t prepareSeqno = 1;
    uint64_t cas = 0;
    makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, prepareSeqno);

    // 2) Fake disconnect and reconnect, importantly, this sets up the valid
    // window for ignoring DCPAborts.
    consumer->closeAllStreams();
    uint32_t opaque = 0;
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    // 3) Receive overwriting set instead of commit
    uint64_t streamStartSeqno = 4;
    SnapshotMarker marker(opaque,
                          vbid,
                          streamStartSeqno /*snapStart*/,
                          streamStartSeqno /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    const std::string value = "overwritingValue";
    auto item = makeCommittedItem(key, value);
    item->setBySeqno(streamStartSeqno);

    if (docState == DocumentState::Deleted) {
        item->setDeleted(DeleteSource::Explicit);
        item->replaceValue({});
    }

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 4) Verify doc state
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // tell the DM that this snapshot was persisted
    vb->setPersistenceSeqno(streamStartSeqno);
    vb->notifyPersistenceToDurabilityMonitor();

    {
        // findForUpdate will return both pending and committed perspectives
        auto res = vb->ht.findForUpdate(key);
        if (persistent()) {
            EXPECT_FALSE(res.pending);
        } else {
            ASSERT_TRUE(res.pending);
            EXPECT_EQ(1, res.pending->getBySeqno());
            EXPECT_EQ(CommittedState::PrepareCommitted,
                      res.pending->getCommitted());
        }
        ASSERT_TRUE(res.committed);
        EXPECT_EQ(4, res.committed->getBySeqno());
        EXPECT_EQ(CommittedState::CommittedViaMutation,
                  res.committed->getCommitted());
        if (docState == DocumentState::Alive) {
            ASSERT_TRUE(res.committed->getValue());
            EXPECT_EQ(value, res.committed->getValue()->to_s());
        }
    }

    // Should have removed all sync writes
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    // We should now be able to do a sync write to a different key
    key = makeStoredDocKey("newkey");
    makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, 10);
    prepareSeqno = vb->getHighSeqno();
    marker = SnapshotMarker(
            opaque,
            vbid,
            streamStartSeqno + 2 /*snapStart*/,
            streamStartSeqno + 2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(key, prepareSeqno, {}, vb->lockCollections(key)));
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveMutationInsteadOfCommitForReconnectWindowWithPrepareLast) {
    testReceiveMutationOrDeletionInsteadOfCommitForReconnectWindowWithPrepareLast(
            DocumentState::Alive);
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveAbortOnTopOfCommittedDueToDedupedPrepare) {
    uint32_t opaque = 0;
    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            1 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    const std::string value = "overwritingValue";
    auto item = makeCommittedItem(key, value);
    item->setBySeqno(1);
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    marker = SnapshotMarker(opaque,
                            vbid,
                            1 /*snapStart*/,
                            ~1 /*snapEnd*/,
                            dcp_marker_flag_t::MARKER_FLAG_DISK,
                            0 /*HCS*/,
                            {} /*maxVisibleSeqno*/,
                            {}, // timestamp
                            {} /*streamId*/);
    stream->processMarker(&marker);

    EXPECT_EQ(
            cb::engine_errc::success,
            stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                    opaque, vbid, key, 3 /*prepareSeqno*/, 4 /*abortSeqno*/)));
}

TEST_P(DurabilityPassiveStreamTest, SeqnoAckAtSnapshotEndReceived) {
    // The consumer receives mutations {s:1, s:2, s:3}, with only s:2 durable
    // with Level:Majority. We have to check that we do send a SeqnoAck, but
    // only when the Replica receives the snapshot-end mutation.

    // The consumer receives the snapshot-marker
    uint32_t opaque = 0;
    const uint64_t snapEnd = 3;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  snapEnd,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {} /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    const std::string value("value");

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    using namespace cb::durability;
    const uint64_t swSeqno = 2;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      swSeqno,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    // readyQ still empty, we have not received the snap-end mutation yet
    EXPECT_EQ(0, readyQ.size());

    // snapshot-end
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      snapEnd, vbid, value, opaque)));
    // Verify that we have the expected SeqnoAck in readyQ now
    ASSERT_EQ(1, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
              readyQ.front()->getEvent());
    const auto* seqnoAck =
            static_cast<const SeqnoAcknowledgement*>(readyQ.front().get());
    EXPECT_EQ(swSeqno, seqnoAck->getPreparedSeqno());
}

TEST_P(DurabilityPassiveStreamPersistentTest, SeqnoAckAtPersistedSnapEnd) {
    // The consumer receives mutations {s:1, s:2, s:3} in the snapshot:[1, 4],
    // with only s:2 durable with Level:PersistToMajority.
    // We have to check that we do send a SeqnoAck for s:2, but only after:
    // (1) the snapshot-end mutation (s:4) is received
    // (2) the complete snapshot is persisted

    // The consumer receives the snapshot-marker [1, 4]
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  4 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {} /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    const std::string value("value");

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    const int64_t swSeqno = 2;
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      swSeqno,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::PersistToMajority,
                                   Timeout::Infinity()))));
    // No SeqnoAck, HPS has not moved as Level:PersistToMajority requires to be
    // persisted for being locally-satisfied
    EXPECT_EQ(0, readyQ.size());

    // Flush (in the middle of the snapshot, which can happen at replica)
    flushVBucketToDiskIfPersistent(vbid, 2 /*expectedNumFlushed*/);
    // No SeqnoAck, HPS has not moved as Level:PersistToMajority Prepare has
    // been persisted but at any Level we require that the complete snapshot is
    // received before moving the HPS into the snapshot.
    EXPECT_EQ(0, readyQ.size());

    // Non-durable s:3 received
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));
    // No ack yet, we have not yet received the complete snapshot
    EXPECT_EQ(0, readyQ.size());

    // Non-durable s:4 (snapshot-end) received
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      4 /*seqno*/, vbid, value, opaque)));
    // No ack yet, we have received the snap-end mutation but we have not yet
    // persisted the complete snapshot
    EXPECT_EQ(0, readyQ.size());

    // Flush, complete snapshot persisted after this call
    flushVBucketToDiskIfPersistent(vbid, 2 /*expectedNumFlushed*/);

    // HPS must have moved to the (already) persisted s:2 and we must have a
    // SeqnoAck with payload HPS in readyQ.
    // Note that s:3 and s:4 (which is a non-sync write) don't affect HPS, which
    // is set to the last locally-satisfied Prepare.
    ASSERT_EQ(1, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
              readyQ.front()->getEvent());
    const auto* seqnoAck =
            static_cast<const SeqnoAcknowledgement*>(readyQ.front().get());
    EXPECT_EQ(swSeqno, seqnoAck->getPreparedSeqno());
}

/**
 * The test simulates a Replica receiving:
 *
 * snapshot-marker [1, 10] -> no-ack
 * s:1 non-durable -> no ack
 * s:2 Level:Majority -> no ack
 * s:3 non-durable -> no ack
 * s:4 Level:MajorityAndPersistOnMaster -> no ack
 * s:5 non-durable -> no ack
 * s:6 Level:PersistToMajority (durability-fence) -> no ack
 * s:7 Level-Majority -> no ack
 * s:8 Level:MajorityAndPersistOnMaster -> no ack
 * s:9 non-durable -> no ack
 * s:9 non-durable -> no ack
 * s:10 non-durable (snapshot-end) -> ack (HPS=4)
 *
 * Last step: flusher persists all -> ack (HPS=8)
 */
TEST_P(DurabilityPassiveStreamPersistentTest, DurabilityFence) {
    const auto& readyQ = stream->public_readyQ();
    auto checkSeqnoAckInReadyQ = [this, &readyQ](int64_t seqno) -> void {
        ASSERT_EQ(1, readyQ.size());
        ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
                  readyQ.front()->getEvent());
        const auto& seqnoAck =
                static_cast<const SeqnoAcknowledgement&>(*readyQ.front());
        EXPECT_EQ(seqno, seqnoAck.getPreparedSeqno());
        // Clear readyQ
        ASSERT_TRUE(stream->public_popFromReadyQ());
        ASSERT_FALSE(readyQ.size());
    };

    // snapshot-marker [1, 10] -> no-ack
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  10 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {} /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {});
    stream->processMarker(&snapshotMarker);
    EXPECT_EQ(0, readyQ.size());

    // s:1 non-durable -> no ack
    const std::string value("value");
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:2 Level:Majority -> no ack
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      2 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:3 non-durable -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:4 Level:MajorityAndPersistOnMaster -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      4 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::MajorityAndPersistOnMaster,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:5 non-durable -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      5 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:6 Level:PersistToMajority -> no ack (durability-fence)
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      6 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::PersistToMajority,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:7 Level-Majority -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      7 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:8 Level:MajorityAndPersistOnMaster -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      8 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::MajorityAndPersistOnMaster,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:9 non-durable -> no ack
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      9 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:10 non-durable (snapshot-end) -> ack (HPS=4, durability-fence at 6)
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationConsumerMessage(10, vbid, value, opaque)));
    checkSeqnoAckInReadyQ(4 /*HPS*/);

    // Flusher persists all -> ack (HPS=8)
    flushVBucketToDiskIfPersistent(vbid, 10 /*expectedNumFlushed*/);
    checkSeqnoAckInReadyQ(8 /*HPS*/);
}

queued_item DurabilityPassiveStreamTest::makeAndReceiveDcpPrepare(
        const StoredDocKey& key,
        uint64_t cas,
        uint64_t seqno,
        cb::durability::Level level) {
    using namespace cb::durability;
    queued_item qi = makePendingItem(
            key, "value", Requirements(level, Timeout::Infinity()));
    qi->setBySeqno(seqno);
    qi->setCas(cas);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      0 /*opaque*/,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    return qi;
}

queued_item DurabilityPassiveStreamTest::makeAndReceiveSnapMarkerAndDcpPrepare(
        const StoredDocKey& key,
        uint64_t cas,
        uint64_t seqno,
        cb::durability::Level level,
        uint64_t snapshotMarkerFlags,
        std::optional<uint64_t> hcs) {
    // The consumer receives snapshot-marker [seqno, seqno]
    uint32_t opaque = 0;
    makeAndProcessSnapshotMarker(opaque, seqno, snapshotMarkerFlags, hcs);

    return makeAndReceiveDcpPrepare(key, cas, seqno, level);
}

queued_item DurabilityPassiveStreamTest::makeAndReceiveCommittedItem(
        const StoredDocKey& key,
        uint64_t cas,
        uint64_t seqno,
        std::optional<DeleteSource> deleted) {
    queued_item qi = makeCommittedItem(key, "value");
    qi->setBySeqno(seqno);
    qi->setCas(cas);

    if (deleted) {
        qi->setDeleted(*deleted);
    }

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      0 /*opaque*/,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    return qi;
}

void DurabilityPassiveStreamTest::makeAndProcessSnapshotMarker(
        uint32_t opaque,
        uint64_t seqno,
        uint64_t snapshotMarkerFlags,
        const std::optional<uint64_t>& hcs) {
    SnapshotMarker marker(opaque,
                          vbid,
                          seqno,
                          seqno,
                          snapshotMarkerFlags,
                          hcs /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {} /*timestamp*/,
                          {} /*streamId*/);
    stream->processMarker(&marker);
}

void DurabilityPassiveStreamTest::testReceiveDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const uint64_t cas = 999;
    const uint64_t prepareSeqno = 1;
    auto qi = makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, prepareSeqno);

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        EXPECT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        EXPECT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        EXPECT_EQ(cas, sv.storedValue->getCas());
    }
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);

    const auto* ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected prepared item.
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(*qi, **it) << "Item in Checkpoint doesn't match queued_item";

    EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());
    // Level:Majority + snap-end received -> HPS has moved
    EXPECT_EQ(1, vb->getDurabilityMonitor().getHighPreparedSeqno());
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpPrepare) {
    testReceiveDcpPrepare();
}

void DurabilityPassiveStreamTest::testReceiveDuplicateDcpPrepare(
        uint64_t prepareSeqno) {
    // Consumer receives [1, 1] snapshot with just a prepare
    testReceiveDcpPrepare();

    // The consumer now "disconnects" then "re-connects" and misses a commit for
    // the given key at seqno 2. It instead receives the following snapshot
    // [3, 3] for the same key containing a prepare, followed by a second
    // snapshot [4, 4] with the corresponding commit.
    uint32_t opaque = 0;

    // Fake disconnect and reconnect, importantly, this sets up the valid window
    // for replacing the old prepare.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    ASSERT_TRUE(stream->isActive());
    // At Replica we don't expect multiple Durability items (for the same key)
    // within the same snapshot. That is because the Active prevents that for
    // avoiding de-duplication.
    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Commit. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and creating a new open one for queueing the Commit.
    SnapshotMarker marker(opaque,
                          vbid,
                          prepareSeqno /*snapStart*/,
                          prepareSeqno /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          1 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    const std::string value("value");
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;

    const uint64_t cas = 999;
    queued_item qi(new Item(key,
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            cas /*cas*/,
                            prepareSeqno,
                            vbid));
    qi->setPendingSyncWrite(Requirements(Level::Majority, Timeout::Infinity()));

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    auto commitSeqno = prepareSeqno + 1;
    marker = SnapshotMarker(
            opaque,
            vbid,
            commitSeqno /*snapStart*/,
            commitSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                      opaque, vbid, prepareSeqno, commitSeqno, key)));
}

void DurabilityPassiveStreamTest::testReceiveMultipleDuplicateDcpPrepares() {
    // This simulates the state in which the active has:
    // PRE1 PRE2 PRE3 CMT1 CMT2 CMT3 PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    // the replica sees:
    // PRE1 PRE2 PRE3 ||Disconnect|| PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    // All 3 duplicate prepares should be accepted by
    // allowedDuplicatePrepareSeqnos

    // NB: A mix of prepares levels is used intentionally - they allow
    // us to test that duplicate prepares are permitted:
    // - regardless of level of the replaced prepare
    // - regardless of persistence of the replaced prepare
    // - regardless of the HPS
    // They should only be rejected if they would replace a prepare with a seqno
    // outside the "allowed window". This window is specified as the range
    // [highCompletedSeqno+1, highSeqno] at the time the snapshot marker
    // is received.
    // No prepare with seqno <= highCompletedSeqno should ever be replaced,
    // because it has already been completed and should not be being tracked any
    // more No prepare with seqno > highSeqno (latest seqno seen by VB) should
    // be replaced, because these were received *after* the snapshot marker.
    const uint64_t cas = 999;
    uint64_t seqno = 1;
    std::vector<StoredDocKey> keys = {makeStoredDocKey("key1"),
                                      makeStoredDocKey("key2"),
                                      makeStoredDocKey("key3")};

    // Send the first prepare for each of three keys
    // PRE1 PRE2 PRE3 CMT1 CMT2 CMT3 PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    // ^^^^ ^^^^ ^^^^
    std::vector<queued_item> queued_items;
    queued_items.push_back(makeAndReceiveSnapMarkerAndDcpPrepare(
            keys[0], cas, seqno++, cb::durability::Level::Majority));
    queued_items.push_back(makeAndReceiveSnapMarkerAndDcpPrepare(
            keys[1],
            cas,
            seqno++,
            cb::durability::Level::MajorityAndPersistOnMaster));
    queued_items.push_back(makeAndReceiveSnapMarkerAndDcpPrepare(
            keys[2], cas, seqno++, cb::durability::Level::PersistToMajority));

    // The consumer now "disconnects" then "re-connects" and misses the commits
    // at seqnos 4, 5, 6.
    // PRE1 PRE2 PRE3 CMT1 CMT2 CMT3 PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    //                xxxx xxxx xxxx
    // It instead receives the following snapshot [7, 9] containing prepares
    // (for the same 3 keys), followed by a second snapshot [10, 12] with the
    // corresponding commits.
    uint32_t opaque = 0;

    // Fake disconnect and reconnect, importantly, this sets up the valid window
    // for replacing the old prepare.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    ASSERT_TRUE(stream->isActive());
    const auto hcs = store->getVBucket(vbid)->getHighCompletedSeqno();
    // At Replica we don't expect multiple Durability items (for the same key)
    // within the same snapshot. That is because the Active prevents that for
    // avoiding de-duplication.
    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Commit. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and creating a new open one for queueing the Commit.
    SnapshotMarker marker(opaque,
                          vbid,
                          7 /*snapStart*/,
                          9 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          hcs /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // Do second prepare for each of three keys
    // PRE1 PRE2 PRE3 CMT1 CMT2 CMT3 PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    //                               ^^^^ ^^^^ ^^^^
    seqno = 7;
    for (const auto& key : keys) {
        queued_items.push_back(makeAndReceiveDcpPrepare(
                key, cas, seqno++, cb::durability::Level::Majority));
    }

    marker = SnapshotMarker(
            opaque,
            vbid,
            10 /*snapStart*/,
            12 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // Commit each of the keys
    // PRE1 PRE2 PRE3 CMT1 CMT2 CMT3 PRE1 PRE2 PRE3 CMT1 CMT2 CMT3
    //                                              ^^^^ ^^^^ ^^^^

    uint64_t prepareSeqno = 7;
    seqno = 10;
    for (const auto& key : keys) {
        ASSERT_EQ(cb::engine_errc::success,
                  stream->messageReceived(
                          std::make_unique<CommitSyncWriteConsumer>(
                                  opaque, vbid, prepareSeqno++, seqno++, key)));
    }
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDuplicateDcpPrepare) {
    testReceiveDuplicateDcpPrepare(3);
}

TEST_P(DurabilityPassiveStreamTest, ReceiveMultipleDuplicateDcpPrepares) {
    testReceiveMultipleDuplicateDcpPrepares();
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDuplicateDcpPrepareRemoveFromSet) {
    testReceiveDuplicateDcpPrepare(3);

    const std::string value("value");
    auto key = makeStoredDocKey("key");
    const uint64_t cas = 999;
    queued_item qi(new Item(key,
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            cas /*cas*/,
                            3,
                            vbid));
    using namespace cb::durability;
    qi->setPendingSyncWrite(Requirements(Level::Majority, Timeout::Infinity()));

    uint32_t opaque = 0;
    ASSERT_EQ(cb::engine_errc::out_of_range,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDuplicateDcpPrepareRemoveFromPDM) {
    testReceiveDuplicateDcpPrepare(3);

    auto vb = engine->getVBucket(vbid);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vb);
    if (persistent()) {
        EXPECT_EQ(1, pdm.getNumTracked());

        // As we have a disk checkpoint we need to receive snap end and persist
        // the snap
        pdm.notifySnapshotEndReceived(4);
        flushVBucketToDiskIfPersistent(vbid, 3);
    }

    ASSERT_EQ(0, pdm.getNumTracked());
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       ReceiveDuplicateDcpPrepareRemoveIgnoresCompletedPrepares) {
    // Recreates the behaviour seen in MB-35933. Test to ensure a valid
    // duplicate prepare will replace the correct SyncWrite in trackedWrites, in
    // the presence of older completed prepares which have not yet been removed
    // because they have not been persisted.

    // Consumer receives prepare, abort, prepare ||| prepare for same key at
    // seqnos 1, 2, 3, 5 with a disconnect/reconnect at |||. This is a valid
    // sequence of ops, as the completion of the prepare at seqno 3 may have
    // been deduped. The prepare at seqno 5 needs to correctly erase the prepare
    // at seqno 3 in trackedWrites, but should NOT touch seqno 1.

    // All prepares are PersistToMajority

    auto vb = engine->getVBucket(vbid);
    const auto& dm = vb->getDurabilityMonitor();
    const auto key = makeStoredDocKey("key");

    EXPECT_EQ(0, dm.getNumTracked());

    // first prepare
    uint64_t cas = 999;
    using namespace cb::durability;
    makeAndReceiveSnapMarkerAndDcpPrepare(
            key, cas, 1, Level::PersistToMajority);

    EXPECT_EQ(1, dm.getNumTracked());
    // abort. Completes the first prepare, but the prepare will *not* be removed
    // from tracked writes as it will not be persisted (vbucket not flushed
    // during test)
    uint32_t opaque = 0;

    SnapshotMarker marker(
            opaque,
            vbid,
            2 /*snapStart*/,
            2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);
    stream->messageReceived(
            std::make_unique<AbortSyncWriteConsumer>(opaque, vbid, key, 1, 2));

    EXPECT_EQ(1, dm.getNumTracked());

    // second prepare
    makeAndReceiveSnapMarkerAndDcpPrepare(
            key, cas, 3, Level::PersistToMajority);

    EXPECT_EQ(2, dm.getNumTracked());

    auto* sv = vb->ht.findOnlyPrepared(key).storedValue;

    // confirm the second prepare is present in the hashtable
    ASSERT_TRUE(sv);
    ASSERT_EQ(sv->getBySeqno(), 3);

    // The consumer now "disconnects" then "re-connects" and misses a
    // commit/abort for the given key at seqno 4. It instead receives a snapshot
    // starting with *another* prepare for the given key - the commit/abort was
    // deduped. Importantly, this sets up the valid window for replacing the old
    // prepare.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    ASSERT_TRUE(stream->isActive());

    // third prepare
    // receiving this prepare failed an `Expects` prior to the fix for MB-35933
    // as it attempted to replace the first, completed prepare in error.
    EXPECT_NO_THROW(makeAndReceiveSnapMarkerAndDcpPrepare(
            key,
            cas,
            5,
            Level::PersistToMajority,
            MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            0 /*HCS*/));

    EXPECT_EQ(2, dm.getNumTracked());
}

void DurabilityPassiveStreamPersistentTest::
        testCompletedPrepareSkippedAtOutOfOrderCompletion(
                CheckpointType firstCkptType) {
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& ckptMgr = *vb->checkpointManager;
    ckptMgr.clear(0 /*seqno*/);

    const auto receiveSnapshot = [this, &vb, &ckptMgr](
                                         CheckpointType type,
                                         uint64_t snapStart,
                                         uint64_t snapEnd) -> void {
        uint32_t flags = 0;
        using namespace cb::durability;
        Requirements reqs;

        const auto timeout = Timeout::Infinity();
        if (type == CheckpointType::Memory) {
            flags = dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK;
            reqs = Requirements(Level::PersistToMajority, timeout);
        } else {
            flags = dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK;
            reqs = Requirements(Level::Majority, timeout);
        }

        uint32_t opaque = 1;
        SnapshotMarker marker(opaque,
                              vbid,
                              snapStart,
                              snapEnd,
                              flags,
                              snapStart /*HCS*/,
                              {} /*maxVisibleSeqno*/,
                              {}, // timestamp
                              {} /*streamId*/);
        stream->processMarker(&marker);

        const auto key = makeStoredDocKey("key");
        const auto value = "value";
        auto item = makePendingItem(key, value, reqs);
        item->setBySeqno(snapStart);

        // Replica receives PRE
        EXPECT_EQ(cb::engine_errc::success,
                  stream->messageReceived(
                          std::make_unique<MutationConsumerMessage>(
                                  item,
                                  opaque,
                                  IncludeValue::Yes,
                                  IncludeXattrs::Yes,
                                  IncludeDeleteTime::No,
                                  IncludeDeletedUserXattrs::Yes,
                                  DocKeyEncodesCollectionId::No,
                                  nullptr /*ext-metadata*/,
                                  cb::mcbp::DcpStreamId{})));

        // Replica receives logical CMT
        std::unique_ptr<DcpResponse> cmtMsg;
        if (type == CheckpointType::Memory) {
            cmtMsg = std::make_unique<CommitSyncWriteConsumer>(
                    opaque,
                    vbid,
                    snapStart /*prepareSeqno*/,
                    snapEnd /*commitSeqno*/,
                    key);
        } else {
            item = makeCommittedItem(key, value);
            item->setBySeqno(snapEnd);
            cmtMsg = std::make_unique<MutationConsumerMessage>(
                    std::move(item),
                    opaque,
                    IncludeValue::Yes,
                    IncludeXattrs::Yes,
                    IncludeDeleteTime::No,
                    IncludeDeletedUserXattrs::Yes,
                    DocKeyEncodesCollectionId::No,
                    nullptr /*ext-metadata*/,
                    cb::mcbp::DcpStreamId{});
        }
        EXPECT_EQ(cb::engine_errc::success,
                  stream->messageReceived(std::move(cmtMsg)));

        // Check HT
        {
            const auto res = vb->ht.findForUpdate(key);
            ASSERT_TRUE(res.committed);
            EXPECT_EQ(snapEnd, res.committed->getBySeqno());
            EXPECT_TRUE(res.committed->getValue());
            EXPECT_FALSE(res.pending);
        }
        // Check CM
        const auto& ckptList =
                CheckpointManagerTestIntrospector::public_getCheckpointList(
                        ckptMgr);
        const auto* ckpt = ckptList.back().get();
        EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
        ASSERT_EQ(1, ckpt->getNumMetaItems());
        ASSERT_EQ(2, ckpt->getNumItems());
        auto it = ckpt->begin();
        EXPECT_EQ(queue_op::empty, (*it)->getOperation());
        it++;
        EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
        it++;
        EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
        EXPECT_EQ(snapStart, (*it)->getBySeqno());
        it++;
        if (type == CheckpointType::Memory) {
            EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
        } else {
            EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
        }
        EXPECT_EQ(snapEnd, (*it)->getBySeqno());
    };

    // Receive a first snapshot with PRE:1(key) + CMT:2 such that PRE:1 is not
    // removed from tracking at completion
    receiveSnapshot(firstCkptType, 1, 2);

    // Receives a second Disk snapshot so with PRE:3(key) + CMT:4.
    // The PDM throws at this step before the fix in MB-37063.
    receiveSnapshot(CheckpointType::Disk, 3, 4);
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       CompletedPrepareSkippedAtOutOfOrderCompletion_Memory) {
    testCompletedPrepareSkippedAtOutOfOrderCompletion(
            CheckpointType::Memory /*firstCkptType*/);
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       CompletedPrepareSkippedAtOutOfOrderCompletion_Disk) {
    testCompletedPrepareSkippedAtOutOfOrderCompletion(
            CheckpointType::Disk /*firstCkptType*/);
}

TEST_P(DurabilityPassiveStreamTest, DeDupedPrepareWindowDoubleDisconnect) {
    testReceiveDcpPrepare();

    // Send another prepare for our second sequential prepare to overwrite.
    auto key = makeStoredDocKey("key1");
    const uint64_t cas = 999;
    uint64_t prepareSeqno = 2;
    makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, prepareSeqno);

    // The consumer now "disconnects" then "re-connects" and misses a commit for
    // the given key at seqno 3.
    uint32_t opaque = 0;

    // Fake disconnect and reconnect, importantly, this sets up the valid window
    // for replacing the old prepare.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);
    ASSERT_TRUE(stream->isActive());

    // Receive a snapshot marker [4, 4] for what would be a sequential prepare
    // on the same key This should set the valid sequential prepare window for
    // the vBucket (just seqno 4).
    // At Replica we don't expect multiple Durability items (for the same key)
    // within the same snapshot. That is because the Active prevents that for
    // avoiding de-duplication.
    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Commit. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and creating a new open one for queueing the Commit.
    SnapshotMarker marker(
            opaque,
            vbid,
            5 /*snapStart*/,
            5 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // Now disconnect again.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);
    ASSERT_TRUE(stream->isActive());

    // We should now expand the previously existing sequential prepare window to
    // seqno 4 and 5.
    marker = SnapshotMarker(
            opaque,
            vbid,
            5 /*snapStart*/,
            6 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            0 /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // We should now successfully overwrite the existing prepares at seqno 1
    // and seqno 2 with new prepares that exist at seqno 4 and seqno 5.
    key = makeStoredDocKey("key");
    const std::string value("value");
    using namespace cb::durability;
    prepareSeqno = 5;
    queued_item qi(new Item(key,
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            cas /*cas*/,
                            prepareSeqno,
                            vbid));
    qi->setPendingSyncWrite(Requirements(Level::Majority, Timeout::Infinity()));

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    key = makeStoredDocKey("key1");
    prepareSeqno = 6;
    qi = queued_item(new Item(key,
                              0 /*flags*/,
                              0 /*expiry*/,
                              value.c_str(),
                              value.size(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              cas /*cas*/,
                              prepareSeqno,
                              vbid));
    qi->setPendingSyncWrite(Requirements(Level::Majority, Timeout::Infinity()));

    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

void DurabilityPassiveStreamTest::testReceiveDcpPrepareCommit() {
    // First, simulate the Consumer receiving a Prepare
    testReceiveDcpPrepare();

    auto vb = engine->getVBucket(vbid);
    const uint64_t prepareSeqno = 1;

    // Record CAS for comparison with the later Commit.
    uint64_t cas;
    auto key = makeStoredDocKey("key");
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }

    // At Replica we don't expect multiple Durability items (for the same key)
    // within the same snapshot. That is because the Active prevents that for
    // avoiding de-duplication.
    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Commit. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and creating a new open one for queueing the Commit.
    auto& manager = *vb->checkpointManager;
    const auto openId = manager.getOpenCheckpointId();
    uint32_t opaque = 0;
    auto commitSeqno = prepareSeqno + 1;
    SnapshotMarker marker(
            opaque,
            vbid,
            commitSeqno,
            commitSeqno,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);
    flushVBucketToDiskIfPersistent(vbid, 1);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    // Now simulate the Consumer receiving Commit for that Prepare
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                      opaque, vbid, prepareSeqno, commitSeqno, key)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Ephemeral keeps the prepare in the hash table whilst ep modifies the
    // existing prepare
    if (persistent()) {
        EXPECT_EQ(1, vb->ht.getNumItems());
    } else {
        EXPECT_EQ(2, vb->ht.getNumItems());
    }

    {
        const auto sv = vb->ht.findForWrite(key).storedValue;
        EXPECT_TRUE(sv);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
        ASSERT_TRUE(vb->ht.findForWrite(key).storedValue);
    }

    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is Commit and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_TRUE((*it)->getValue());
    EXPECT_EQ("value", (*it)->getValue()->to_s());
    EXPECT_EQ(commitSeqno, (*it)->getBySeqno());
    EXPECT_EQ(cas, (*it)->getCas());

    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

/*
 * This test checks that a DCP Consumer receives and processes correctly a
 * DCP_PREPARE followed by a DCP_COMMIT message.
 */
TEST_P(DurabilityPassiveStreamTest, ReceiveDcpCommit) {
    testReceiveDcpPrepareCommit();
}

/*
 * This test checks that a DCP Consumer receives and processes correctly the
 * following sequence (to the same key):
 * - DCP_PREPARE
 * - DCP_COMMIT
 * - DCP_PREPARE
 */
TEST_P(DurabilityPassiveStreamTest, ReceiveDcpPrepareCommitPrepare) {
    // First setup the first DCP_PREPARE and DCP_COMMIT.
    testReceiveDcpPrepareCommit();

    auto vb = engine->getVBucket(vbid);
    auto& manager = *vb->checkpointManager;
    const auto openId = manager.getOpenCheckpointId();

    // Process the 2nd Prepare.
    auto key = makeStoredDocKey("key");
    const uint64_t cas = 1234;
    const uint64_t prepare2ndSeqno = 3;
    makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, prepare2ndSeqno);
    flushVBucketToDiskIfPersistent(vbid, 1);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    auto ckptIt = ckptList.begin();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, (*ckptIt)->getState());
    ASSERT_EQ(1, (*ckptIt)->getNumItems());

    // 2 Items in HashTable.
    EXPECT_EQ(2, vb->ht.getNumItems())
            << "Should have one Committed and one Prepared items in HashTable";
}

void DurabilityPassiveStreamTest::testReceiveDcpAbort() {
    // First, simulate the Consumer receiving a Prepare
    testReceiveDcpPrepare();
    auto vb = engine->getVBucket(vbid);
    uint64_t prepareSeqno = 1;
    const auto key = makeStoredDocKey("key");

    // Now simulate the Consumer receiving Abort for that Prepare
    uint32_t opaque = 0;
    auto abortReceived = [this, opaque, &key](
                                 uint64_t prepareSeqno,
                                 uint64_t abortSeqno) -> cb::engine_errc {
        return stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                opaque, vbid, key, prepareSeqno, abortSeqno));
    };

    // Check a negative first: at Replica we don't expect multiple Durable
    // items within the same checkpoint. That is to avoid Durable items de-dupe
    // at Producer.
    uint64_t abortSeqno = prepareSeqno + 1;
    auto thrown{false};
    try {
        abortReceived(prepareSeqno, abortSeqno);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("duplicate item") !=
                    std::string::npos);
        thrown = true;
    }
    if (!thrown) {
        FAIL();
    }

    auto& manager = *vb->checkpointManager;
    const auto openId = manager.getOpenCheckpointId();

    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Abort. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and cretaing a new open one for queueing the Abort.
    SnapshotMarker marker(
            opaque,
            vbid,
            3 /*snapStart*/,
            4 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);
    flushVBucketToDiskIfPersistent(vbid, 1);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    // The consumer receives an Abort for the previous Prepare.
    // Note: The call to abortReceived() above throws /after/
    //     PassiveStream::last_seqno has been incremented, so we need to
    //     abortSeqno to bypass cb::engine_errc::out_of_range checks.
    abortSeqno++;
    prepareSeqno++;
    ASSERT_EQ(cb::engine_errc::success,
              abortReceived(prepareSeqno, abortSeqno));

    EXPECT_EQ(0, vb->getNumItems());
    // Ephemeral keeps the completed prepare in the HashTable
    if (persistent()) {
        EXPECT_EQ(0, vb->ht.getNumItems());
    } else {
        EXPECT_EQ(1, vb->ht.getNumItems());
    }
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_FALSE(sv.storedValue);
    }

    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is Abort and carries no value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_FALSE((*it)->getValue());
    EXPECT_EQ(abortSeqno, (*it)->getBySeqno());

    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpAbort) {
    testReceiveDcpAbort();
}

/**
 * Test that we do not accept an abort without a corresponding prepare if we
 * are receiving an in-memory snapshot.
 */
TEST_P(DurabilityPassiveStreamTest, ReceiveAbortWithoutPrepare) {
    uint32_t opaque = 0;

    SnapshotMarker marker(
            opaque,
            vbid,
            2 /*snapStart*/,
            2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key1");
    auto prepareSeqno = 1;
    auto abortSeqno = 2;
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque, vbid, key, prepareSeqno, abortSeqno)));
}

/**
 * Test that we can accept an abort without a correponding prepare if we
 * are receiving a disk snapshot and the prepare seqno is greater than or
 * equal to the snapshot start seqno.
 */
TEST_P(DurabilityPassiveStreamTest, ReceiveAbortWithoutPrepareFromDisk) {
    uint32_t opaque = 0;
    auto prepareSeqno = 3;
    auto abortSeqno = 4;

    SnapshotMarker marker(opaque,
                          vbid,
                          3 /*snapStart*/,
                          4 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          prepareSeqno,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key1");
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque, vbid, key, prepareSeqno, abortSeqno)));
}

/**
 * This test is for a scenario in which we may receive an abort with a prepare
 * seqno outside of the snapshot. We're only consider backfill snapshots so
 * aborts will deduplicate prepares. This could happen if a replica  streams
 * a disk backfill as two snapshots due to a disconnect. This was seen after a
 * rollback but that's not necessary for the test. We'll use the following
 * snapshot:
 *
 *  [Start:1 - End:10]
 *  At seqno 5 we have an abort with prepare seqno of 3
 *
 *  Should we disconnect at whatever item is/isn't at seqno 4 then we shouldn't
 *  tear down the connection if the seqno of the abort is earlier than the first
 *  seqno of the snapshot.
 */
TEST_P(DurabilityPassiveStreamTest,
       DiskSnapCatchupAbortWithPrepareSeqnoBeforeSnap) {
    uint32_t opaque = 0;
    auto prepareSeqno = 3;
    auto abortSeqno = 5;

    // 1) Receive a disk snapshot
    SnapshotMarker marker(opaque,
                          vbid,
                          0 /*snapStart*/,
                          10 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          prepareSeqno,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Pretend we disconnected by just sending another snapshot marker, all
    // the state required gets updated when we process the snapshot marker
    marker = SnapshotMarker(
            opaque,
            vbid,
            5 /*snapStart*/,
            10 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            prepareSeqno,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 3) And abort
    auto key = makeStoredDocKey("key1");
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque, vbid, key, prepareSeqno, abortSeqno)));
}

void DurabilityPassiveStreamTest::setUpHandleSnapshotEndTest() {
    auto key1 = makeStoredDocKey("key1");
    uint64_t cas = 1;
    uint64_t prepareSeqno = 1;
    makeAndReceiveSnapMarkerAndDcpPrepare(key1, cas, prepareSeqno);

    uint32_t opaque = 0;
    SnapshotMarker marker(
            opaque,
            vbid,
            prepareSeqno + 1 /*snapStart*/,
            prepareSeqno + 2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto key2 = makeStoredDocKey("key2");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key2, "value2", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setCas(1);
    pending->setBySeqno(2);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    ASSERT_EQ(true, stream->getCurSnapshotPrepare());
}

TEST_P(DurabilityPassiveStreamTest, HandleSnapshotEndOnCommit) {
    setUpHandleSnapshotEndTest();
    uint32_t opaque;
    auto key = makeStoredDocKey("key1");

    // Commit the original prepare (setup receives two prepares so we need to
    // bump our seqno by 2).
    auto prepareSeqno = 1;
    auto commitSeqno = prepareSeqno + 2;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                      opaque, vbid, prepareSeqno, commitSeqno, key)));

    // We should have unset (acked the second prepare) the bool flag if we
    // handled the snapshot end
    EXPECT_EQ(false, stream->getCurSnapshotPrepare());
}

TEST_P(DurabilityPassiveStreamTest, HandleSnapshotEndOnAbort) {
    setUpHandleSnapshotEndTest();
    uint32_t opaque;
    auto key = makeStoredDocKey("key1");

    // Abort the original prepare
    auto abortSeqno = 3;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque, vbid, key, 1 /*prepareSeqno*/, abortSeqno)));

    // We should have unset (acked the second prepare) the bool flag if we
    // handled the snapshot end
    EXPECT_EQ(false, stream->getCurSnapshotPrepare());
}

TEST_P(DurabilityPassiveStreamTest, ReceiveBackfilledDcpCommit) {
    // Need to use actual opaque of the stream as we hit the consumer level
    // function.
    uint32_t opaque = 1;
    const uint64_t prepareSeqno = 1;

    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          prepareSeqno /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto prepare = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    prepare->setBySeqno(prepareSeqno);
    prepare->setCas(999);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      prepare,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Hit the consumer level function (not the stream level) for additional
    // error checking.
    EXPECT_EQ(cb::engine_errc::success,
              consumer->commit(opaque, vbid, key, prepare->getBySeqno(), 2));
}

TEST_P(DurabilityPassiveStreamTest, AllowsDupePrepareNamespaceInCheckpoint) {
    uint32_t opaque = 0;
    const uint64_t prepareSeqno = 1;

    // 1) Send disk snapshot marker
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          prepareSeqno /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Send prepare
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(prepareSeqno);
    pending->setCas(1);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    auto vb = engine->getVBucket(vbid);
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vb);
    ASSERT_EQ(1, pdm.getNumTracked());

    // 3) Send commit - should not throw
    auto commitSeqno = pending->getBySeqno() + 1;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                      opaque, vbid, pending->getBySeqno(), commitSeqno, key)));
    flushVBucketToDiskIfPersistent(vbid, 2);
    ASSERT_EQ(0, pdm.getNumTracked());

    // 5) Send next in memory snapshot
    marker = SnapshotMarker(
            opaque,
            vbid,
            3 /*snapStart*/,
            4 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamID*/);
    stream->processMarker(&marker);

    // 6) Send prepare
    pending->setBySeqno(commitSeqno + 1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    ASSERT_EQ(1, pdm.getNumTracked());

    // 7) Send commit - allowed to exist in same checkpoint
    commitSeqno = pending->getBySeqno() + 1;

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                      opaque, vbid, pending->getBySeqno(), commitSeqno, key)));
    EXPECT_EQ(0, pdm.getNumTracked());
}

/**
 * This is a valid scenario that we can get in and must deal with.
 *
 * 1) Replica is streaming from the active and receives a partial snapshot:
 *        [1:PRE(k1), 2:NOT RECEIVED(k2)]
 *    Importantly, not receiving the item at seqno 2 means that we do not move
 *    the HPS as we never received the snapshot end so this blocks us from
 *    removing 1:PRE at step 3a. It does not matter what sort of item we have
 *    at seqno 2.
 *
 * 2) Replica disconnects and reconnects which sets the
 *    allowedDuplicatePrepareSeqnos window to 1
 *
 * 3) Replica receives the following disk snapshot:
 *        [4:PRE(k1), 5:MUT(k1)]
 *    We have deduped the initial prepare and the commit at seqno 3.
 *
 *    a) 4:PRE(k1)
 *       We replace 1:PRE in the HashTable with 4:PRE and add 4:PRE to
 *       trackedWrites in the PDM.
 *       This prepare logically completes 1:PRE in the PDM but 1:PRE is not
 *       removed from trackedWrites as the HPS used in the fence to remove the
 *       SyncWrites is still 0 and won't be moved until the snapshot end.
 *    b) 5:MUT(k1)
 *       We find 4:PRE in the HashTable and use this seqno when we attempt to
 *       complete the SyncWrite in the PDM. The PDM starts searching for the
 *       SyncWrite to complete at the beginning of trackedWrites as we are
 *       in a disk snapshot and must allow out of order completion. We then find
 *       the trackedWrite for 1:PRE that still exists in the PDM.
 */
TEST_P(DurabilityPassiveStreamTest, MismatchingPreInHTAndPdm) {
    using namespace cb::durability;

    // 1) Consumer receives [1, 2] snapshot marker but only 1:PRE.
    uint32_t opaque = 0;
    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    std::string value("value1");
    const uint64_t cas = 999;
    queued_item qi = makePendingItem(
            key, value, Requirements(Level::Majority, Timeout::Infinity()));
    qi->setBySeqno(1);
    qi->setCas(cas);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    auto vb = engine->getVBucket(vbid);
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vb);
    ASSERT_EQ(1, pdm.getNumTracked());

    // 2) Disconnect and reconnect (sets allowedDuplicatePrepareSeqnos to {1}).
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    // 3a) Receive 4:PRE
    ASSERT_TRUE(stream->isActive());
    const uint64_t preSeqno = 4;
    marker = SnapshotMarker(
            opaque,
            vbid,
            1 /*snapStart*/,
            5 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            preSeqno /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    value = "value4";
    qi = queued_item(new Item(key,
                              0 /*flags*/,
                              0 /*expiry*/,
                              value.c_str(),
                              value.size(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              cas /*cas*/,
                              preSeqno,
                              vbid));
    qi->setPendingSyncWrite(Requirements(Level::Majority, Timeout::Infinity()));
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // We remove the SyncWrite corresponding to 1:PRE when we receive 4:PRE
    // even though we have not reached the snap end and moved the HPS because
    // we do not want to keep duplicate keys in trackedWrites.
    EXPECT_EQ(1, pdm.getNumTracked());

    // 3b) Receive 5:PRE
    value = "value5";
    auto item = makeCommittedItem(key, value);
    item->setBySeqno(5);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Persist and notify the PDM as 4:PRE requires persistence to complete due
    // to possibly deduping a persist level prepare. We flush 3 items because
    // the two prepares are in different checkpoint types.
    flushVBucketToDiskIfPersistent(vbid, 3);

    EXPECT_EQ(0, pdm.getNumTracked());
}

// Test covers issue seen in MB-35062, we must be able to tolerate a prepare
// and a delete, the replica must accept both when in a disk snapshot
TEST_P(DurabilityPassiveStreamTest, BackfillPrepareDelete) {
    uint32_t opaque = 0;

    // Send a snapshot which is
    // seq:1 prepare(key)
    // seq:3 delete(key)
    // In this case seq:2 was the commit and is now represented by del seq:3
    const uint64_t preSeqno = 1;

    // 1) Send disk snapshot marker
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          3 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          preSeqno /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Send prepare
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(preSeqno);
    pending->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 3) Send delete of the prepared key
    auto deleted = makeCommittedItem(key, {});
    deleted->setDeleted(DeleteSource::Explicit);
    deleted->setBySeqno(3);
    queued_item qi{deleted};
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      deleted,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Expect two items in the flush, prepare and delete
    flushVBucketToDiskIfPersistent(vbid, 2);

    // The vbucket must now be upto seqno 3
    auto vb = engine->getVBucket(vbid);
    EXPECT_EQ(3, vb->getHighSeqno());
}

TEST_P(DurabilityPassiveStreamTest,
       DiskSnapshotDontRemoveUncompletedPrepareFromPreviousSnapshot) {
    uint32_t opaque = 0;
    auto vb = engine->getVBucket(vbid);

    // 1) Send memory snapshot marker
    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            1 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Send prepare for keyA
    auto keyA = makeStoredDocKey("keyA");
    using namespace cb::durability;
    auto pending = makePendingItem(
            keyA, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(1);
    pending->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 3) Send disk snapshot marker
    const uint64_t keyBPrepareSeqno = 3;
    marker = SnapshotMarker(
            opaque,
            vbid,
            2 /*snapStart*/,
            5 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            keyBPrepareSeqno /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 4) Send prepare and commit for keyB before we see the commit for keyA
    auto key = makeStoredDocKey("keyB");
    pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(keyBPrepareSeqno);
    pending->setCas(3);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    auto committed = makeCommittedItem(key, {});
    committed->setBySeqno(4);
    auto qi = queued_item{committed};
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      committed,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 5) Flush before we receive the completion for keyA
    flushVBucketToDiskIfPersistent(vbid, 2);

    // 6) Completion of keyA should not throw an exception due to keyA not being
    // found in the PDM
    committed = makeCommittedItem(keyA, {});
    committed->setBySeqno(5);
    qi = queued_item{committed};
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      committed,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

/**
 * We have to treat all prepares in a disk snapshot as requiring persistence
 * due to them possibly de-duplicating a PersistToMajority SyncWrite. As this is
 * the case, we can keep completed SyncWrite objects in the PDM trackedWrites
 * even after one has been completed (a disk snapshot contains a prepare and a
 * commit for the same key). These prepares are logically completed and should
 * not trigger any sanity check exceptions due to having multiple prepares for
 * the same key in trackedWrites.
 */
TEST_P(DurabilityPassiveStreamTest, CompletedDiskPreIsIgnoredBySanityChecks) {
    uint32_t opaque = 0;
    const uint64_t preSeqno = 1;
    // 1) Receive disk snapshot marker
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          preSeqno /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Receive prepare
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(preSeqno);
    pending->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 3) Receive overwriting set instead of commit
    const std::string value = "commit";
    auto item = makeCommittedItem(key, value);
    item->setBySeqno(2);
    item->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(item),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 4) Receive memory snapshot marker
    marker = SnapshotMarker(
            opaque,
            vbid,
            3 /*snapStart*/,
            3 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 5) Receive prepare
    pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(3);
    pending->setCas(2);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

TEST_P(DurabilityPassiveStreamTest,
       CompletedPersistPreIsIgnoredBySanityChecks) {
    uint32_t opaque = 0;

    // 1) Receive disk snapshot marker
    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Receive prepare
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key,
            "value",
            Requirements(Level::PersistToMajority, Timeout::Infinity()));
    pending->setBySeqno(1);
    pending->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 3) Receive commit
    ASSERT_EQ(
            cb::engine_errc::success,
            stream->messageReceived(std::make_unique<CommitSyncWriteConsumer>(
                    opaque, vbid, 1 /*prepareSeqno*/, 2 /*commitSeqno*/, key)));

    // 4) Receive memory snapshot marker
    marker = SnapshotMarker(
            opaque,
            vbid,
            3 /*snapStart*/,
            3 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 5) Receive prepare
    pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(3);
    pending->setCas(2);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

void DurabilityPassiveStreamTest::testPrepareCompletedAtAbort(
        cb::durability::Level level, Resolution res, bool flush) {
    auto vb = engine->getVBucket(vbid);
    auto& ht = vb->ht;
    const auto& ckptMgr = *vb->checkpointManager;
    const auto& dm = vb->getDurabilityMonitor();

    auto checkDM = [&dm](size_t numTracked, int64_t hps, int64_t hcs) -> void {
        EXPECT_EQ(numTracked, dm.getNumTracked());
        EXPECT_EQ(hps, dm.getHighPreparedSeqno());
        EXPECT_EQ(hcs, dm.getHighCompletedSeqno());
    };

    const auto key = makeStoredDocKey("key1");

    // Checking the state of HT, CM and DM at every meaningful step,
    // all empty at this point.
    ASSERT_EQ(0, ht.getNumItems());
    {
        const auto htRes = ht.findForUpdate(key);
        ASSERT_FALSE(htRes.pending);
        ASSERT_FALSE(htRes.committed);
    }
    ASSERT_EQ(0, ckptMgr.getHighSeqno());
    {
        SCOPED_TRACE("");
        checkDM(0 /*numTracked*/, 0 /*HPS*/, 0 /*HCS*/);
    }

    // First we check the behaviour at receiving a memory snapshot
    const uint32_t opaque = 0;
    auto prepareSeqno = 1;
    auto snapEnd = prepareSeqno;
    SnapshotMarker marker(opaque,
                          vbid,
                          prepareSeqno /*snapStart*/,
                          snapEnd,
                          MARKER_FLAG_MEMORY | MARKER_FLAG_CHK /*flags*/,
                          {} /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // Replica receives PRE(key)
    using namespace cb::durability;
    const auto prepare =
            makePendingItem(key, "value", {level, Timeout::Infinity()});
    prepare->setBySeqno(prepareSeqno);
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      prepare,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr /*ext-meta*/,
                      cb::mcbp::DcpStreamId{})));

    // MB-36735: This is added for covering both when:
    // 1) The flusher has persisted the snapshot containing the prepare when
    //     the unprepared Abort is received
    // 2) The flusher has not persisted the entire snapshot
    //
    // In the second case (!flush) the Prepare is not locally-satisfied and so
    // still tracked in the PDM (as in PDM we can only remove up to the HPS).
    // In that scenario, before this fix the PDM throws when it processes the
    // unprepared Abort, as it does not expect to find an already-Completed
    // Prepare in tracked-writes.
    if (flush) {
        vb->setPersistenceSeqno(snapEnd);
        vb->notifyPersistenceToDurabilityMonitor();
    }

    EXPECT_EQ(1, ht.getNumItems());
    {
        const auto htRes = ht.findForUpdate(key);
        ASSERT_TRUE(htRes.pending);
        EXPECT_EQ(prepareSeqno, htRes.pending->getBySeqno());
        EXPECT_TRUE(htRes.pending->isPending());
        ASSERT_FALSE(htRes.committed);
    }
    EXPECT_EQ(prepareSeqno, ckptMgr.getHighSeqno());
    if (persistent() && !flush && level == Level::PersistToMajority) {
        SCOPED_TRACE("");
        checkDM(1 /*numTracked*/, 0 /*HPS*/, 0 /*HCS*/);
    } else {
        SCOPED_TRACE("");
        checkDM(1 /*numTracked*/, 1 /*HPS*/, 0 /*HCS*/);
    }

    // Replica receives a legal Commit/Abort for PRE(key).
    // Note: Active queues Durability items into different checkpoints for
    // avoiding deduplication, so Replica never expects Prepare+Commit/Abort
    // within the same snapshot.
    auto completeSeqno = 2;
    snapEnd = completeSeqno + 10;
    marker = SnapshotMarker(opaque,
                            vbid,
                            completeSeqno /*snapStart*/,
                            snapEnd,
                            MARKER_FLAG_CHK | MARKER_FLAG_MEMORY /*flags*/,
                            {} /*HCS*/,
                            {} /*maxVisibleSeqno*/,
                            {}, // timestamp
                            {} /*streamId*/);
    stream->processMarker(&marker);

    std::unique_ptr<DcpResponse> msg;
    switch (res) {
    case Resolution::Commit: {
        msg = std::make_unique<CommitSyncWriteConsumer>(
                opaque, vbid, prepareSeqno, completeSeqno, key);
        break;
    }
    case Resolution::Abort: {
        msg = std::make_unique<AbortSyncWriteConsumer>(
                opaque, vbid, key, prepareSeqno, completeSeqno);
        break;
    }
    }
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::move(msg)));

    {
        const auto htRes = ht.findForUpdate(key);

        switch (res) {
        case Resolution::Commit: {
            ASSERT_TRUE(htRes.committed);
            EXPECT_EQ(CommittedState::CommittedViaPrepare,
                      htRes.committed->getCommitted());
            EXPECT_EQ(completeSeqno, htRes.committed->getBySeqno());
            if (persistent()) {
                EXPECT_EQ(1, ht.getNumItems());
                ASSERT_FALSE(htRes.pending);
            } else {
                // Keeping the Prepare SV in HT for Ephemeral.
                EXPECT_EQ(2, ht.getNumItems());
                ASSERT_TRUE(htRes.pending);
                EXPECT_TRUE(htRes.pending->isPrepareCompleted());
                // SV is turned into a PrepareCommitted, seqno is still
                // prepareSeqno.
                EXPECT_EQ(CommittedState::PrepareCommitted,
                          htRes.pending->getCommitted());
                EXPECT_EQ(prepareSeqno, htRes.pending->getBySeqno());
            }
            break;
        }
        case Resolution::Abort: {
            ASSERT_FALSE(htRes.committed);
            if (persistent()) {
                EXPECT_EQ(0, ht.getNumItems());
                ASSERT_FALSE(htRes.pending);
            } else {
                // Keeping the Prepare SV in HT for Ephemeral.
                EXPECT_EQ(1, ht.getNumItems());
                ASSERT_TRUE(htRes.pending);
                EXPECT_TRUE(htRes.pending->isPrepareCompleted());
                // SV is turned into a PrepareAborted, seqno is set to
                // abortSeqno.
                EXPECT_EQ(CommittedState::PrepareAborted,
                          htRes.pending->getCommitted());
                EXPECT_EQ(completeSeqno, htRes.pending->getBySeqno());
            }
            break;
        }
        }
    }
    EXPECT_EQ(completeSeqno, ckptMgr.getHighSeqno());
    using namespace cb::durability;
    if (persistent() && !flush && level == Level::PersistToMajority) {
        // Completed Prepare still tracked in PDM as HPS has never covered it.
        SCOPED_TRACE("");
        checkDM(1 /*numTracked*/, 0 /*HPS*/, 1 /*HCS*/);
    } else {
        SCOPED_TRACE("");
        checkDM(0 /*numTracked*/, 1 /*HPS*/, 1 /*HCS*/);
    }

    // Here the important part of the test begins.
    // From this point onward, we have a Completed (Committed / Aborted) Prepare
    // in the HT, but no in-flight Prepare for key.
    // Before the fix, at VBucket::abort for Ephemeral we wrongly process the
    // Abort as if an in-flight Prepare is present in the HT (and at some point
    // we throw).

    // Unprepared Abort unexpected at this point as Replica is still receiving
    // a memory snapshot.
    // Note: This throws before the fix for MB-36735 if !flush, read comment
    // above.
    ASSERT_EQ(cb::engine_errc::invalid_arguments,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque,
                      vbid,
                      key,
                      10 /*prepareSeqno*/,
                      11 /*abortSeqno*/)));

    // Now Replica receives a disk snapshot
    marker = SnapshotMarker(opaque,
                            vbid,
                            13 /*snapStart*/,
                            20 /*snapEnd*/,
                            MARKER_FLAG_CHK | MARKER_FLAG_DISK /*flags*/,
                            15 /*HCS*/,
                            {} /*maxVisibleSeqno*/,
                            {}, // timestamp
                            {} /*streamId*/);
    stream->processMarker(&marker);

    auto abortSeqno = 16;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
                      opaque, vbid, key, 15 /*prepareSeqno*/, abortSeqno)));

    {
        const auto htRes = ht.findForUpdate(key);

        switch (res) {
        case Resolution::Commit: {
            ASSERT_TRUE(htRes.committed);
            EXPECT_EQ(CommittedState::CommittedViaPrepare,
                      htRes.committed->getCommitted());
            EXPECT_EQ(completeSeqno, htRes.committed->getBySeqno());
            if (persistent()) {
                EXPECT_EQ(1, ht.getNumItems());
                ASSERT_FALSE(htRes.pending);
            } else {
                // Keeping the Prepare SV in HT for Ephemeral.
                EXPECT_EQ(2, ht.getNumItems());
            }
            break;
        }
        case Resolution::Abort: {
            ASSERT_FALSE(htRes.committed);
            if (persistent()) {
                EXPECT_EQ(0, ht.getNumItems());
                ASSERT_FALSE(htRes.pending);
            } else {
                // Keeping the Prepare SV in HT for Ephemeral.
                EXPECT_EQ(1, ht.getNumItems());
            }
            break;
        }
        }
        if (ephemeral()) {
            // The old PrepareCommitted SV is turned into a PrepareAborted,
            // seqno is set to abortSeqno.
            ASSERT_TRUE(htRes.pending);
            EXPECT_TRUE(htRes.pending->isPrepareCompleted());
            EXPECT_EQ(CommittedState::PrepareAborted,
                      htRes.pending->getCommitted());
            EXPECT_EQ(abortSeqno, htRes.pending->getBySeqno());
        }
    }
    EXPECT_EQ(abortSeqno, ckptMgr.getHighSeqno());
    if (persistent() && !flush && level == Level::PersistToMajority) {
        // Completed Prepare still tracked in PDM as HPS has never covered it.
        SCOPED_TRACE("");
        checkDM(1 /*numTracked*/, 0 /*HPS*/, 1 /*HCS*/);
    } else {
        SCOPED_TRACE("");
        checkDM(0 /*numTracked*/, 1 /*HPS*/, 1 /*HCS*/);
    }
}

TEST_P(DurabilityPassiveStreamTest, MajorityPrepareAbortedAtAbort) {
    testPrepareCompletedAtAbort(cb::durability::Level::Majority,
                                Resolution::Abort);
}

TEST_P(DurabilityPassiveStreamTest,
       MajorityAndPersistOnMasterPrepareAbortedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(
            cb::durability::Level::MajorityAndPersistOnMaster,
            Resolution::Abort);
}

TEST_P(DurabilityPassiveStreamTest, PersistToMajorityPrepareAbortedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(cb::durability::Level::PersistToMajority,
                                Resolution::Abort);
}

TEST_P(DurabilityPassiveStreamTest,
       PersistToMajorityPrepareAbortedAndFlushedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(
            cb::durability::Level::PersistToMajority,
            Resolution::Abort,
            true /*flusher persists the snapshot containing the prepare*/);
}

TEST_P(DurabilityPassiveStreamTest, MajorityPrepareCommittedAtAbort) {
    testPrepareCompletedAtAbort(cb::durability::Level::Majority,
                                Resolution::Commit);
}

TEST_P(DurabilityPassiveStreamTest,
       MajorityAndPersistOnMasterPrepareCommittedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(
            cb::durability::Level::MajorityAndPersistOnMaster,
            Resolution::Commit);
}

TEST_P(DurabilityPassiveStreamTest, PersistToMajorityPrepareCommittedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(cb::durability::Level::PersistToMajority,
                                Resolution::Commit);
}

TEST_P(DurabilityPassiveStreamTest,
       PersistToMajorityPrepareCommittedAndFlushedAtAbort) {
    // @todo: Skip the test until MB-36772 is done (Replica should reject
    //  PersistTo prepare)
    if (ephemeral()) {
        return;
    }
    testPrepareCompletedAtAbort(
            cb::durability::Level::PersistToMajority,
            Resolution::Commit,
            true /*flusher persists the snapshot containing the prepare*/);
}

TEST_P(DurabilityPassiveStreamTest, AllowedDuplicatePreparesSetOnDiskSnap) {
    auto key = makeStoredDocKey("key");
    auto cas = 1;
    auto prepareSeqno = 1;
    makeAndReceiveSnapMarkerAndDcpPrepare(key, cas, prepareSeqno);

    const uint32_t opaque = 0;
    SnapshotMarker marker(opaque,
                          vbid,
                          3 /*snapStart*/,
                          3,
                          MARKER_FLAG_DISK | MARKER_FLAG_CHK /*flags*/,
                          1 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    stream->processMarker(&marker);

    using namespace cb::durability;
    queued_item qi = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    qi->setBySeqno(3);
    qi->setCas(3);

    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

void DurabilityPassiveStreamTest::testPrepareDeduplicationCorrectlyResetsHPS(
        cb::durability::Level level) {
    using namespace cb::durability;
    if (ephemeral()) {
        ASSERT_EQ(Level::Majority, level);
    }

    auto keyA = makeStoredDocKey("keyA");
    makeAndReceiveSnapMarkerAndDcpPrepare(
            keyA, 1 /*cas*/, 1 /*prepareSeqno*/, level);
    auto keyB = makeStoredDocKey("keyB");
    makeAndReceiveSnapMarkerAndDcpPrepare(
            keyB, 2 /*cas*/, 2 /*prepareSeqno*/, level);
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);

    // Expected state in PDM:
    //
    // P(keyA):1    P(keyB):2
    //              ^
    //              HPS:2
    const auto& vb = *store->getVBucket(vbid);
    const auto& pdm = dynamic_cast<const PassiveDurabilityMonitor&>(
            vb.getDurabilityMonitor());
    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(2, pdm.getHighestTrackedSeqno());
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());

    // Replica receives a disk snapshot now, key duplicates legal in PDM
    const uint32_t opaque = 0;
    SnapshotMarker marker(opaque,
                          vbid,
                          3 /*snapStart*/,
                          3 /*snapEnd*/,
                          MARKER_FLAG_DISK | MARKER_FLAG_CHK /*flags*/,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {} /*timestamp*/,
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // Before the fix, the next steps will end up with the following invalid
    // state, which is a pre-requirement for the next steps to fail:
    //
    // P(keyA):1    x    P(keyB):3
    //              ^
    //              HPS:2
    //
    // At fix, this is the state:
    //
    // P(keyA):1    x    P(keyB):3
    // ^
    // HPS:2

    auto qi = makePendingItem(
            keyB, "value", Requirements(level, Timeout::Infinity()));
    qi->setBySeqno(3);
    qi->setCas(3);
    // Receiving the snap-end seqno. Before the fix, this is where we fail on
    // Ephemeral, as we callback into the PDM for updating the HPS from
    // PassiveStream::handleSnapshotEnd().
    // The PDM throws as we break the monotonicity invariant on HPS (set to
    // seqno:2) by trying to reset it to PDM::trackedWrites::begin (ie, seqno:1)
    EXPECT_EQ(cb::engine_errc::success,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    if (persistent()) {
        EXPECT_EQ(2, pdm.getNumTracked());
        EXPECT_EQ(3, pdm.getHighestTrackedSeqno());
        EXPECT_EQ(2, pdm.getHighPreparedSeqno());

        // At flush we persist the full disk snapshot and we call back into the
        // PDM for moving the HPS. Before the fix the PDM throws here the same
        // as already described for Ephemeral.
        flush_vbucket_to_disk(vbid, 1 /*expected_num_flushed*/);
    }

    // Final state at fix
    //
    // P(keyA):1    x    P(keyB):3
    //                   ^
    //                   HPS:3
    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighestTrackedSeqno());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
}

TEST_P(DurabilityPassiveStreamTest, PrepareDedupCorrectlyResetsHPS_Majority) {
    testPrepareDeduplicationCorrectlyResetsHPS(cb::durability::Level::Majority);
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       PrepareDedupCorrectlyResetsHPS_MajorityAndPersistOnMaster) {
    testPrepareDeduplicationCorrectlyResetsHPS(
            cb::durability::Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityPassiveStreamPersistentTest,
       PrepareDedupCorrectlyResetsHPS_PersistToMajority) {
    testPrepareDeduplicationCorrectlyResetsHPS(
            cb::durability::Level::PersistToMajority);
}

// Regression test for MB-41024: check that if a Prepare is received when under
// memory pressure and it is initially rejected and queued, that the snapshot
// end is not notified twice to PDM.
TEST_P(DurabilityPassiveStreamTest,
       MB_41024_PrepareAtSnapEndWithMemoryPressure) {
    if (ephemeralFailNewData()) {
        // This configuration disconnects if a replication stream would
        // go over the high watermark - so test not applicable.
        return;
    }
    // Set replicationThrottleThreshold to zero so a single mutation can push us
    // over the limit (limit = replicationThrottleThreshold * bucket quota).
    engine->getConfiguration().setReplicationThrottleThreshold(0);

    // Send initial snapshot marrker.
    uint32_t opaque = 0;
    auto prepareSeqno = 1;
    // Require a disk snapshot so PassiveStream::handleSnapshotEnd() notifies
    // the PDM even if we haven't successfully processed a Prepare.
    makeAndProcessSnapshotMarker(
            opaque, prepareSeqno, MARKER_FLAG_DISK | MARKER_FLAG_CHK);

    // Setup - send prepare. This should initially be rejected, by virtue
    // of it value size which would make mem_used greater than
    // bucket quota * replicationThrottleThreshold.
    std::string largeValue(1024 * 1024 * 10, '\0');
    using namespace cb::durability;
    auto prepare =
            makePendingItem(makeStoredDocKey("key"),
                            largeValue,
                            Requirements(Level::Majority, Timeout::Infinity()));
    prepare->setBySeqno(prepareSeqno);

    // We *don't* want the replication throttle to kick in before we attempt
    // to process the prepare, so configure the MockReplicationThrottle to
    // just return "Process" when called during the messageReceived call
    // below.
    using namespace ::testing;
    EXPECT_CALL(engine->getMockReplicationThrottle(), getStatus())
            .WillOnce(Return(ReplicationThrottle::Status::Process));

    // Message received by stream. Too large for current mem_used so should
    // be buffered and return TMPFAIL
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      prepare,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      IncludeDeletedUserXattrs::Yes,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Increase replicationThrottleThreshold to allow mutation to be processed.
    engine->getConfiguration().setReplicationThrottleThreshold(100);

    // Test: now process the buffered message. This would previously throw a
    // Monotonic logic_error exception when attempting to push the same
    // seqno to the PDM::receivedSnapshotEnds
    uint32_t processedBytes = 0;
    EXPECT_EQ(all_processed,
              stream->processBufferedMessages(processedBytes, 1));
}

TEST_P(DurabilityPassiveStreamPersistentTest, BufferDcpCommit) {
    auto key = makeStoredDocKey("bufferDcp");

    // Messages go into the consumer so we update flow-control
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(stream->getOpaque(),
                                       vbid,
                                       1,
                                       1,
                                       MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                                       0,
                                       0));

    auto ackBytes = consumer->getFlowControl().getFreedBytes();
    EXPECT_EQ(sizeof(protocol_binary_request_header) +
                      sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                      sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value),
              ackBytes);
    EXPECT_EQ(cb::engine_errc::success,
              consumer->prepare(stream->getOpaque(),
                                key,
                                {}, // value (none)
                                0, // priv_bytes
                                0, // datatype
                                0, // cas
                                vbid,
                                0, // flags
                                1, // seqno
                                1, // rev
                                0, // expiration
                                0, // lock
                                0, // nru
                                DocumentState::Alive,
                                cb::durability::Level::Majority));

    // Add +2 as producer/consumer account for an extra 2 bytes (MB-46634)
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpPreparePayload) + key.size() + 2;
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    // Force consumer to buffer
    auto& stats = engine->getEpStats();
    stats.replicationThrottleThreshold = 0;
    const size_t size = stats.getMaxDataSize();
    stats.setMaxDataSize(1);
    ASSERT_EQ(ReplicationThrottle::Status::Pause,
              engine->getReplicationThrottle().getStatus());

    // Now buffer commit
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(stream->getOpaque(),
                                       vbid,
                                       2,
                                       2,
                                       MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                                       0,
                                       0));

    // No change, snapshot is now buffered
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    EXPECT_EQ(cb::engine_errc::success,
              consumer->commit(stream->getOpaque(),
                               vbid,
                               key,
                               1 /*prepare*/,
                               2 /*commit*/));

    // No change, commit is now buffered
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    // undo the adjustments so that processing of buffered items will work
    stats.replicationThrottleThreshold = 99;
    engine->getEpStats().setMaxDataSize(size);

    // And process buffered items
    EXPECT_EQ(more_to_process, consumer->processBufferedItems());
    EXPECT_EQ(all_processed, consumer->processBufferedItems());

    // Snapshot and commit processed
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpCommitPayload) + key.size();
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());
}

TEST_P(DurabilityPassiveStreamPersistentTest, BufferDcpAbort) {
    auto key = makeStoredDocKey("bufferDcp");

    // Messages go into the consumer so we update flow-control
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(stream->getOpaque(),
                                       vbid,
                                       1,
                                       1,
                                       MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                                       0,
                                       0));

    auto ackBytes = consumer->getFlowControl().getFreedBytes();
    EXPECT_EQ(sizeof(protocol_binary_request_header) +
                      sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                      sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value),
              ackBytes);
    EXPECT_EQ(cb::engine_errc::success,
              consumer->prepare(stream->getOpaque(),
                                key,
                                {}, // value (none)
                                0, // priv_bytes
                                0, // datatype
                                0, // cas
                                vbid,
                                0, // flags
                                1, // seqno
                                1, // rev
                                0, // expiration
                                0, // lock
                                0, // nru
                                DocumentState::Alive,
                                cb::durability::Level::Majority));

    // Add +2 as producer/consumer account for an extra 2 bytes (MB-46634)
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpPreparePayload) + key.size() + 2;
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    // Force consumer to buffer
    auto& stats = engine->getEpStats();
    stats.replicationThrottleThreshold = 0;
    const size_t size = stats.getMaxDataSize();
    stats.setMaxDataSize(1);
    ASSERT_EQ(ReplicationThrottle::Status::Pause,
              engine->getReplicationThrottle().getStatus());

    // Now buffer abort
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(stream->getOpaque(),
                                       vbid,
                                       2,
                                       2,
                                       MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                                       0,
                                       0));

    // No change, snapshot is now buffered
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    EXPECT_EQ(cb::engine_errc::success,
              consumer->abort(stream->getOpaque(),
                              vbid,
                              key,
                              1 /*prepare*/,
                              2 /*abort*/));

    // No change, commit is now buffered
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());

    // undo the adjustments so that processing of buffered items will work
    stats.replicationThrottleThreshold = 99;
    engine->getEpStats().setMaxDataSize(size);

    // And process buffered items
    EXPECT_EQ(more_to_process, consumer->processBufferedItems());
    EXPECT_EQ(all_processed, consumer->processBufferedItems());

    // Snapshot and commit processed
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);
    ackBytes += sizeof(protocol_binary_request_header) +
                sizeof(cb::mcbp::request::DcpAbortPayload) + key.size();
    EXPECT_EQ(ackBytes, consumer->getFlowControl().getFreedBytes());
}

void DurabilityPromotionStreamTest::SetUp() {
    // Set up as a replica
    DurabilityPassiveStreamTest::SetUp();
}

void DurabilityPromotionStreamTest::TearDown() {
    // Tear down as active
    DurabilityActiveStreamTest::TearDown();
}

void DurabilityPromotionStreamTest::registerCursorAtCMStartIfEphemeral() {
    if (ephemeral()) {
        auto vb = store->getVBucket(vbid);
        ASSERT_TRUE(vb);
        auto& manager = static_cast<CheckpointManager&>(*vb->checkpointManager);
        const auto dcpCursor =
                manager.registerCursorBySeqno(
                               "a cursor", 0, CheckpointCursor::Droppable::Yes)
                        .cursor.lock();
        ASSERT_TRUE(dcpCursor);
    }
}

void DurabilityPromotionStreamTest::testDiskCheckpointStreamedAsDiskSnapshot() {
    // 1) Receive a prepare followed by a commit in a disk checkpoint as a
    // replica then flush it
    DurabilityPassiveStreamTest::
            testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
                    2 /*snapStart*/, 4 /*snapEnd*/, DocumentState::Alive);

    // Note: We need to plant a DCP cursor on ephemeral for preventing
    // checkpoint removal at replica promotion
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    if (ephemeral()) {
        const auto dcpCursor =
                ckptMgr.registerCursorBySeqno(
                               "dcp", 0, CheckpointCursor::Droppable::Yes)
                        .cursor.lock();
        ASSERT_TRUE(dcpCursor);
    }

    // Remove the Consumer and PassiveStream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    const auto checkOpenCheckpoint = [&ckptMgr](
                                             CheckpointType expectedCkptType,
                                             uint64_t expectedSnapStart,
                                             uint64_t expectedSnapEnd) -> void {
        ASSERT_EQ(expectedCkptType,
                  getSuperCheckpointType(ckptMgr.getOpenCheckpointType()));
        auto currSnap = ckptMgr.getSnapshotInfo();
        ASSERT_EQ(expectedSnapStart, currSnap.range.getStart());
        ASSERT_EQ(expectedSnapEnd, currSnap.range.getEnd());
    };

    // Still in Checkpoint snap{Disk, 2, 4} (created at the step (1) of the
    // test)
    {
        SCOPED_TRACE("");
        checkOpenCheckpoint(CheckpointType::Disk, 2, 4);
    }

    // 3) Set up the Producer and ActiveStream
    DurabilityActiveStreamTest::setUp(true /*startCheckpointProcessorTask*/,
                                      false /*persist*/);

    // The vbstate-change must have:
    // - closed the checkpoint snap{2, 4, Disk}
    // - created a new checkpoint snap{4, 4, Memory}
    {
        SCOPED_TRACE("");
        checkOpenCheckpoint(CheckpointType::Memory, 4, 4);
    }

    // 4) Write Mutation(5) + Prepare(6) + Commit(7) to a different key in the
    // new Memory checkpoint.
    const auto key = makeStoredDocKey("differentKey");
    const std::string value = "value";
    const auto item = makeCommittedItem(key, value);
    using namespace cb::durability;
    const auto prepare = makePendingItem(
            key, value, Requirements(Level::Majority, Timeout::Infinity()));
    {
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(*item, cookie, *engine, {}, cHandle));
        ASSERT_EQ(5, vb->getHighSeqno());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
        ASSERT_EQ(6, vb->getHighSeqno());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->commit(key,
                             6 /*prepare-seqno*/,
                             {} /*commit-seqno*/,
                             cHandle));
        ASSERT_EQ(7, vb->getHighSeqno());
    }

    // Commit(7) queued in a new checkpoint
    {
        SCOPED_TRACE("");
        checkOpenCheckpoint(CheckpointType::Memory, 7, 7);
    }

    auto& stream = DurabilityActiveStreamTest::stream;
    ASSERT_TRUE(stream->public_supportSyncReplication());

    // 5) Test the checkpoint and stream output.

    auto outItems = stream->public_getOutstandingItems(*vb);
    if (isPersistent()) {
        ASSERT_THAT(
                outItems.items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::checkpoint_end)));
        outItems = stream->public_getOutstandingItems(*vb);
    }
    // Simulate running the checkpoint processor task
    // We must have ckpt-start + Prepare + Mutation + ckpt-end
    // Note: Here we are testing also that CheckpointManager::getItemsForCursor
    //   returns only items from contiguous checkpoints of the same type.
    //   Given that in CM we have checkpoints 1_Disk + 2_Memory, then the next
    //   returns only the entire 1_Disk.
    ASSERT_EQ(4, outItems.items.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write,
              outItems.items.at(1)->getOperation());
    ASSERT_EQ(queue_op::mutation, outItems.items.at(2)->getOperation());

    // We create a new checkpoint as a result of the state change
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(3)->getOperation());

    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a SnapshotMarker + Prepare + Mutation
    ASSERT_EQ(3, stream->public_readyQSize());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);

    // Snapshot marker must have the disk flag set, not the memory flag
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    const auto& markerDisk = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, markerDisk.getFlags());
    // HCS must be sent for CheckpointType:Disk.
    // Note: HCS=2 as PRE:2 added and committed at step (1) of the test
    const auto hcs = markerDisk.getHighCompletedSeqno();
    ASSERT_TRUE(hcs);
    EXPECT_EQ(2, *hcs);

    // readyQ must contain a DCP_PREPARE
    ASSERT_EQ(2, stream->public_readyQSize());
    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Simulate running the checkpoint processor task again, now we process
    // the second and the third checkpoints (both type:memory)
    outItems = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(8, outItems.items.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    // this set_vbucket_state is from creating a new failover table entry as
    // part of changing to active in the middle of this test
    ASSERT_EQ(queue_op::set_vbucket_state,
              outItems.items.at(1)->getOperation());
    // this set_vbucket_state is from changing to active in the middle of this
    // test
    ASSERT_EQ(queue_op::set_vbucket_state,
              outItems.items.at(2)->getOperation());
    ASSERT_EQ(queue_op::mutation, outItems.items.at(3)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write,
              outItems.items.at(4)->getOperation());
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(5)->getOperation());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(6)->getOperation());
    ASSERT_EQ(queue_op::commit_sync_write,
              outItems.items.at(7)->getOperation());

    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);
    // readyQ must contain:
    //   - SnapshotMarker, Mutation, Prepare
    //   - SnapshotMarker, Commit
    ASSERT_EQ(5, stream->public_readyQSize());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(4, stream->public_readyQSize());

    // Snapshot marker should now be memory flag
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* markerMemory = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, markerMemory->getFlags());
    // HCS not set for CheckpointType:Memory.
    ASSERT_FALSE(markerMemory->getHighCompletedSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(5, *resp->getBySeqno());
    ASSERT_EQ(3, stream->public_readyQSize());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(6, *resp->getBySeqno());
    ASSERT_EQ(2, stream->public_readyQSize());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    ASSERT_EQ(1, stream->public_readyQSize());
    markerMemory = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, markerMemory->getFlags());
    // HCS not set for CheckpointType:Memory.
    ASSERT_FALSE(markerMemory->getHighCompletedSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Commit, resp->getEvent());
    EXPECT_EQ(7, *resp->getBySeqno());
    EXPECT_EQ(6, dynamic_cast<CommitSyncWrite&>(*resp).getPreparedSeqno());
    ASSERT_EQ(0, stream->public_readyQSize());

    producer->cancelCheckpointCreatorTask();
}

TEST_P(DurabilityPromotionStreamTest,
       DiskCheckpointStreamedAsDiskSnapshotReplica) {
    // Should already be replica
    testDiskCheckpointStreamedAsDiskSnapshot();
}

TEST_P(DurabilityPromotionStreamTest,
       DiskCheckpointStreamedAsDiskSnapshotPending) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_pending);
    testDiskCheckpointStreamedAsDiskSnapshot();
}

/**
 * The test builds up an Active that has the following checkpoints:
 *   Memory{M:1} + Disk{PRE:2, M:3} + Memory{set-vbs:4, M:4}
 * and verifies that the MARKER_FLAG_CHK is set in all SnapshotMarkers sent to
 * a Replica.
 */
void DurabilityPromotionStreamTest::
        testCheckpointMarkerAlwaysSetAtSnapTransition() {
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& ckptMgr = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    ckptMgr.clear(0 /*seqno*/);

    // Note: We need to plant a DCP cursor on ephemeral for preventing
    // checkpoint removal at replica promotion
    registerCursorAtCMStartIfEphemeral();
    auto baseNumberOfCheckpoints = vb->checkpointManager->getNumCheckpoints();

    // 1) Replica receives a first MemoryCheckpoint - snap{1, 1} + M:1
    uint32_t opaque = 1;
    const auto seqno = 1;
    SnapshotMarker marker(
            opaque,
            vbid,
            seqno /*snapStart*/,
            seqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    auto* passiveStream = DurabilityPassiveStreamTest::stream;
    passiveStream->processMarker(&marker);

    const std::string value = "value";
    auto item = makeCommittedItem(makeStoredDocKey("key1"), value);
    item->setBySeqno(seqno);

    EXPECT_EQ(cb::engine_errc::success,
              passiveStream->messageReceived(
                      std::make_unique<MutationConsumerMessage>(
                              std::move(item),
                              opaque,
                              IncludeValue::Yes,
                              IncludeXattrs::Yes,
                              IncludeDeleteTime::No,
                              IncludeDeletedUserXattrs::Yes,
                              DocKeyEncodesCollectionId::No,
                              nullptr /*ext-metadata*/,
                              cb::mcbp::DcpStreamId{})));

    ASSERT_EQ(baseNumberOfCheckpoints + 1, ckptMgr.getNumCheckpoints());
    auto currSnap = ckptMgr.getSnapshotInfo();
    ASSERT_EQ(CheckpointType::Memory, ckptMgr.getOpenCheckpointType());
    ASSERT_EQ(seqno, currSnap.range.getStart());
    ASSERT_EQ(seqno, currSnap.range.getEnd());

    // 2) Replica receives PRE:2 and M:3 (logic CMT:3) in a disk checkpoint
    const auto diskSnapStart = 2;
    const auto diskSnapEnd = 3;
    DurabilityPassiveStreamTest::
            testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
                    diskSnapStart,
                    diskSnapEnd,
                    DocumentState::Alive,
                    false /*clearCM*/);

    ASSERT_EQ(baseNumberOfCheckpoints + 2, ckptMgr.getNumCheckpoints());
    currSnap = ckptMgr.getSnapshotInfo();
    ASSERT_EQ(CheckpointType::Disk, ckptMgr.getOpenCheckpointType());
    ASSERT_EQ(diskSnapStart, currSnap.range.getStart());
    ASSERT_EQ(diskSnapEnd, currSnap.range.getEnd());

    // Remove PassiveStream and Consumer
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    // 3) Simulate vbstate-change Replica->Active (we have also a Producer and
    // ActiveStream from this point onward)
    DurabilityActiveStreamTest::setUp(true /*startCheckpointProcessorTask*/,
                                      false /*persist*/);
    ASSERT_TRUE(producer);
    auto* activeStream = DurabilityActiveStreamTest::stream.get();
    ASSERT_TRUE(activeStream);

    // The vbstate-change must have:
    // - closed the checkpoint snap{Disk, 2, 3}
    // - created a new checkpoint snap{Memory, 3, 3}
    ASSERT_EQ(baseNumberOfCheckpoints + 3, ckptMgr.getNumCheckpoints());
    currSnap = ckptMgr.getSnapshotInfo();
    ASSERT_EQ(CheckpointType::Memory, ckptMgr.getOpenCheckpointType());
    ASSERT_EQ(3, currSnap.range.getStart());
    ASSERT_EQ(3, currSnap.range.getEnd());

    // 3) Queue M:4 in the new MemoryCheckpoint.
    item = makeCommittedItem(makeStoredDocKey("key2"), value);
    {
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(*item, cookie, *engine, {}, cHandle));
        ASSERT_EQ(4, vb->getHighSeqno());
    }

    // We are Active, currSnapEnd will be set to 5 in the open checkpoint.
    ASSERT_EQ(baseNumberOfCheckpoints + 3, ckptMgr.getNumCheckpoints());
    currSnap = ckptMgr.getSnapshotInfo();
    ASSERT_EQ(CheckpointType::Memory, ckptMgr.getOpenCheckpointType());
    ASSERT_EQ(4, currSnap.range.getStart());
    ASSERT_EQ(4, currSnap.range.getEnd());

    ASSERT_TRUE(activeStream->public_supportSyncReplication());

    // 4) Test the 2 steps of the CheckpointProcessorTask, ie:
    // - get items from the CheckpointManager (inspect items here)
    // - push items into the Stream::readyQ (inspect queue here)
    //
    // Note: CheckpointManager::getItemsForCursor returns only items from
    //   contiguous checkpoints of the same type. Given that in CM we have
    //   checkpoints Memory{M:1} + Disk{PRE:2, M:3} + Memory{set-vbs:4, M:4},
    //   then each "run" of the CheckpointProcessorTask will process only one
    //   checkpoint.

    // Stream::readyQ still empty
    ASSERT_EQ(0, activeStream->public_readyQSize());

    // First CheckpointProcessorTask run
    // Get items from CM, expect Memory{M:1}:
    //   ckpt-start + ckpt-end + ckpt-start + M:1 + ckpt-end
    auto outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_THAT(outItems.items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::checkpoint_end),
                                     HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::checkpoint_end)));
    // Check the seqno of the mutation
    ASSERT_EQ(1, outItems.items.at(3)->getBySeqno());

    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);
    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a SnapshotMarker + Mutation
    ASSERT_EQ(2, activeStream->public_readyQSize());
    // SnapshotMarker must be Memory + CheckpointFlag
    auto resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* markerMemory = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, markerMemory->getFlags());
    // HCS not set for CheckpointType:Memory.
    ASSERT_FALSE(markerMemory->getHighCompletedSeqno());
    ASSERT_EQ(1, activeStream->public_readyQSize());
    resp = activeStream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(0, activeStream->public_readyQSize());

    // Second CheckpointProcessorTask run
    // Get items from CM, expect Disk{PRE:2, M:3}:
    //   ckpt-start + PRE:2 + M:3 + ckpt-end
    //
    // !! NOTE: This is the important part of the test !!
    //   Before this patch we do not get any ckpt-start from CM
    outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_EQ(4, outItems.items.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write,
              outItems.items.at(1)->getOperation());
    const auto prepareSeqno = 2;
    ASSERT_EQ(prepareSeqno, outItems.items.at(1)->getBySeqno());
    ASSERT_EQ(queue_op::mutation, outItems.items.at(2)->getOperation());
    ASSERT_EQ(3, outItems.items.at(2)->getBySeqno());
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(3)->getOperation());

    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);

    // readyQ must contain a SnapshotMarker + Prepare + Mutation
    ASSERT_EQ(3, activeStream->public_readyQSize());
    // SnapshotMarker must be Disk + CheckpointFlag
    //
    // !! NOTE: This is the important part of the test !!
    //   Before this patch the SnapshotMarker here does not have the
    //   MARKER_FLAG_CHK because we do not cover the case of (Memory->Disk
    //   snapshot transition).
    //   That means that a Replica will not create a new Checkpoint at receiving
    //   the marker and will just queue (or at least it will try to) the
    //   snapshot into the open Checkpoint, which may even be of different type.
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    const auto& markerDisk = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, markerDisk.getFlags());
    // HCS must be sent for CheckpointType:Disk.
    // Note: HCS=2 as PRE:2 added and committed at step (2) of the test
    const auto hcs = markerDisk.getHighCompletedSeqno();
    ASSERT_TRUE(hcs);
    EXPECT_EQ(prepareSeqno, *hcs);
    ASSERT_EQ(2, activeStream->public_readyQSize());
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    ASSERT_EQ(1, activeStream->public_readyQSize());
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(0, activeStream->public_readyQSize());

    // Third CheckpointProcessorTask run
    // Get items from CM, expect Memory{set-vbs:4, M:4}:
    //   ckpt-start + set-vbs:4 + M:4 + ckpt-end
    outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_EQ(4, outItems.items.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    // this set_vbucket_state is from creating a new failover table entry as
    // part of changing to active in the middle of this test
    ASSERT_EQ(queue_op::set_vbucket_state,
              outItems.items.at(1)->getOperation());
    // this set_vbucket_state is from changing to active in the middle of this
    // test
    ASSERT_EQ(queue_op::set_vbucket_state,
              outItems.items.at(2)->getOperation());
    ASSERT_EQ(queue_op::mutation, outItems.items.at(3)->getOperation());
    ASSERT_EQ(4, outItems.items.at(3)->getBySeqno());

    // Stream::readyQ still empty
    ASSERT_EQ(0, activeStream->public_readyQSize());
    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);
    // readyQ must contain a SnapshotMarker + Mutation
    ASSERT_EQ(2, activeStream->public_readyQSize());

    // SnapshotMarker must be Memory + CheckpointFlag
    //
    // !! NOTE: This is the important part of the test !!
    //   The marker must have the MARKER_FLAG_CHK. Note that we covered this
    //   case (Disk -> Memory snapshot transition) even before this patch.
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(1, activeStream->public_readyQSize());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    markerMemory = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, markerMemory->getFlags());
    // HCS not set for CheckpointType:Memory.
    ASSERT_FALSE(markerMemory->getHighCompletedSeqno());

    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(4, *resp->getBySeqno());
    ASSERT_EQ(0, activeStream->public_readyQSize());

    producer->cancelCheckpointCreatorTask();
}

TEST_P(DurabilityPromotionStreamTest,
       CheckpointMarkerAlwaysSetAtSnapTransition) {
    testCheckpointMarkerAlwaysSetAtSnapTransition();
}

TEST_P(DurabilityPromotionStreamTest,
       testCheckpointMarkerAlwaysSetAtSnapTransition_Pending) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_pending);
    testCheckpointMarkerAlwaysSetAtSnapTransition();
}

/**
 * Test introduced in MB-36971. The test sets up a Replica with 2 Disk
 * Checkpoints, turns it into Active and checks that the correct HCS is always
 * included in each SnapshotMarker when the Active streams.
 *
 * The test covers also MB-37063. At setup we simulate the Replica receiving
 * two Disk Checkpoints in a row. Both checkpoints contain PRE + CMT for the
 * same key. The flusher has never run when Replica processes the second CMT
 * in the second snapshot. Before the fix for MB-37063 that would trigger an
 * exception from PassiveDurabilityMonitor::completeSyncWrite().
 */
void DurabilityPromotionStreamTest::
        testActiveSendsHCSAtDiskSnapshotSentFromMemory() {
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& ckptMgr = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    const auto checkOpenCheckpoint = [&ckptMgr](
                                             CheckpointType expectedCkptType,
                                             uint64_t expectedSnapStart,
                                             uint64_t expectedSnapEnd) -> void {
        ASSERT_EQ(expectedCkptType,
                  getSuperCheckpointType(ckptMgr.getOpenCheckpointType()));
        auto currSnap = ckptMgr.getSnapshotInfo();
        ASSERT_EQ(expectedSnapStart, currSnap.range.getStart());
        ASSERT_EQ(expectedSnapEnd, currSnap.range.getEnd());
    };

    // Note: We need to plant a DCP cursor on ephemeral for preventing
    // checkpoint removal
    registerCursorAtCMStartIfEphemeral();
    auto baseNumberOfCheckpoints = vb->checkpointManager->getNumCheckpoints();

    // 1) Replica receives PRE:1 and M:2 (logic CMT:2) in a disk checkpoint
    {
        SCOPED_TRACE("");
        DurabilityPassiveStreamTest::
                testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
                        1 /*diskSnapStart*/,
                        2 /*diskSnapEnd*/,
                        DocumentState::Alive);

        ASSERT_EQ(baseNumberOfCheckpoints + 1, ckptMgr.getNumCheckpoints());
        checkOpenCheckpoint(CheckpointType::Disk, 1, 2);
    }

    // 2) Replica receives PRE:3 and M:4 (logic CMT:4) in a second disk
    // checkpoint
    {
        SCOPED_TRACE("");
        DurabilityPassiveStreamTest::
                testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
                        3 /*diskSnapStart*/,
                        4 /*diskSnapEnd*/,
                        DocumentState::Alive,
                        false /*clearCM*/);

        ASSERT_EQ(baseNumberOfCheckpoints + 2, ckptMgr.getNumCheckpoints());
        checkOpenCheckpoint(CheckpointType::Disk, 3, 4);
    }

    // Remove PassiveStream and Consumer
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    // 3) Simulate vbstate-change Replica->Active (we have also a Producer and
    // ActiveStream from this point onward)
    DurabilityActiveStreamTest::setUp(true /*startCheckpointProcessorTask*/,
                                      false /*persist*/);
    ASSERT_TRUE(producer);
    auto* activeStream = DurabilityActiveStreamTest::stream.get();
    ASSERT_TRUE(activeStream);

    // The vbstate-change must have closed open Disk checkpoint and created a
    // new Memory one.
    // Note: checking that jus for ensuring that we have closed the last Disk
    // Checkpoint, but the new Memory checkpoint is no used in the following)
    ASSERT_EQ(baseNumberOfCheckpoints + 3, ckptMgr.getNumCheckpoints());
    {
        SCOPED_TRACE("");
        checkOpenCheckpoint(CheckpointType::Memory, 4, 4);
    }

    // 3) Test the 2 steps of the CheckpointProcessorTask, ie:
    // - get items from the CheckpointManager (inspect items here)
    // - push items into the Stream::readyQ (inspect queue here and verify that
    //   we include the correct HCS in the SnapshotMarker for ecvery Disk Snap)
    //
    // Note: CheckpointManager::getItemsForCursor never returns multiple
    //   Disk Checkpoints. Given that in CM we have Disk{PRE:1, M:2} +
    //   Disk{PRE:3, M:4}, then a single run of the CheckpointProcessorTask
    //   will process only one checkpoint.

    // Stream::readyQ still empty
    ASSERT_EQ(0, activeStream->public_readyQSize());

    // CheckpointProcessorTask runs
    auto outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outItems.items.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(1)->getOperation());

    // Get items from CM, expect Disk{PRE:1, M:2}:
    //   {CS, PRE:1, M:2, CE}
    outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_EQ(4, outItems.items.size());

    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    auto prepare = outItems.items.at(1);
    ASSERT_EQ(queue_op::pending_sync_write, prepare->getOperation());
    ASSERT_EQ(1, prepare->getBySeqno());
    auto mutation = outItems.items.at(2);
    ASSERT_EQ(queue_op::mutation, mutation->getOperation());
    ASSERT_EQ(2, mutation->getBySeqno());
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(3)->getOperation());

    ASSERT_EQ(0, activeStream->public_readyQSize());
    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);
    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain SnapshotMarker{Disk, HCS:1} + PRE:1 + M:2
    ASSERT_EQ(3, activeStream->public_readyQSize());

    auto resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, marker->getFlags());
    EXPECT_EQ(1, *marker->getHighCompletedSeqno());
    ASSERT_TRUE(resp);
    resp = activeStream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(1, *resp->getBySeqno());
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    ASSERT_EQ(2, *resp->getBySeqno());

    // CheckpointProcessorTask runs again
    // Get items from CM, expect Disk{PRE:3, M:4}:
    //   {CS, PRE:3, M:4, CE}
    outItems = activeStream->public_getOutstandingItems(*vb);
    ASSERT_EQ(4, outItems.items.size());

    ASSERT_EQ(queue_op::checkpoint_start, outItems.items.at(0)->getOperation());
    prepare = outItems.items.at(1);
    ASSERT_EQ(queue_op::pending_sync_write, prepare->getOperation());
    ASSERT_EQ(3, prepare->getBySeqno());
    mutation = outItems.items.at(2);
    ASSERT_EQ(queue_op::mutation, mutation->getOperation());
    ASSERT_EQ(4, mutation->getBySeqno());
    ASSERT_EQ(queue_op::checkpoint_end, outItems.items.at(3)->getOperation());

    ASSERT_EQ(0, activeStream->public_readyQSize());
    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);

    // readyQ must contain SnapshotMarker{Disk, HCS:3} + PRE:3 + M:4
    ASSERT_EQ(3, activeStream->public_readyQSize());

    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, marker->getFlags());
    EXPECT_EQ(3, *marker->getHighCompletedSeqno());
    ASSERT_TRUE(resp);
    resp = activeStream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(3, *resp->getBySeqno());
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    ASSERT_EQ(4, *resp->getBySeqno());

    ASSERT_EQ(0, activeStream->public_readyQSize());

    producer->cancelCheckpointCreatorTask();
}

TEST_P(DurabilityPromotionStreamTest,
       ActiveSendsHCSAtDiskSnapshotSentFromMemory) {
    testActiveSendsHCSAtDiskSnapshotSentFromMemory();
}

TEST_P(DurabilityPromotionStreamTest,
       ActiveSendsHCSAtDiskSnapshotSentFromMemory_Pending) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_pending);
    testActiveSendsHCSAtDiskSnapshotSentFromMemory();
}

// Test that the following scenario streams the correct snapshot marker from
// newly promoted active -> replica.
// 1) Receive DCP disk snapshot from 1-1
// 2) Receive DCP mutation at seqno 1
// 3) Receive DCP disk snapshot from 2-3
// 4) Receive DCP abort at seqno 3 (seqno 2 was a de-duped prepare)
// 5) Stream snap marker of 0-1 for snapshot at 1
// 6) Stream mutation at seqno 1
// 7) Stream snap mark of 2-3 for snapshot at 3 (previously this was 3-3).
// Steps 1 and 2 are required as the first snapshot marker sent will be adjusted
// to send a snapStart of the stream request start if lower than the intended
// snapStart.
TEST_P(DurabilityPromotionStreamTest,
       DiskCheckpointStreamsSnapStartFromCheckpoint) {
    // Note: We need to plant a DCP cursor on ephemeral for preventing
    // checkpoint removal
    registerCursorAtCMStartIfEphemeral();

    auto opaque = 0;
    // 1)
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          1 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          0 /*HCS*/,
                          1 /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);
    DurabilityPassiveStreamTest::stream->processMarker(&marker);

    // 2)
    auto mutation = makeCommittedItem(makeStoredDocKey("dummy"), "value");
    mutation->setBySeqno(1);
    mutation->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              DurabilityPassiveStreamTest::stream->messageReceived(
                      std::make_unique<MutationConsumerMessage>(
                              mutation,
                              0 /*opaque*/,
                              IncludeValue::Yes,
                              IncludeXattrs::Yes,
                              IncludeDeleteTime::No,
                              IncludeDeletedUserXattrs::Yes,
                              DocKeyEncodesCollectionId::No,
                              nullptr,
                              cb::mcbp::DcpStreamId{})));

    // 3)
    marker = SnapshotMarker(
            0 /*opaque*/,
            vbid,
            2,
            3,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            3 /*HCS*/,
            1 /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    DurabilityPassiveStreamTest::stream->processMarker(&marker);

    // 4)
    auto key = makeStoredDocKey("key");
    EXPECT_EQ(cb::engine_errc::success,
              DurabilityPassiveStreamTest::stream->messageReceived(
                      std::make_unique<AbortSyncWriteConsumer>(
                              0 /*opaque*/,
                              vbid,
                              key,
                              2 /*prepareSeqno*/,
                              3 /*abortSeqno*/)));

    // Remove PassiveStream and Consumer
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    // Simulate vbstate-change Replica->Active (we have also a Producer and
    // ActiveStream from this point onward)
    DurabilityActiveStreamTest::setUp(true /*startCheckpointProcessorTask*/,
                                      false /*persist*/);
    ASSERT_TRUE(producer);
    auto* activeStream = DurabilityActiveStreamTest::stream.get();
    ASSERT_TRUE(activeStream);

    // CheckpointProcessorTask runs
    // Get items from CM, expect Disk{M:1} as only one Disk checkpoint can be
    // retrieved at a time
    auto vb = store->getVBucket(vbid);
    auto outItems = activeStream->public_getOutstandingItems(*vb);

    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);

    if (!isPersistent()) {
        // Call public_getOutstandingItems() again as the last call will have
        // only looked at the empty checkpoint which has been kept around due to
        // the cursor placed by registerCursorAtCMStartIfEphemeral().
        outItems = activeStream->public_getOutstandingItems(*vb);
        activeStream->public_processItems(outItems);
    }

    // readyQ also contain SnapshotMarker
    ASSERT_EQ(2, activeStream->public_readyQSize());

    // 5)
    auto resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* m = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, m->getFlags());
    EXPECT_EQ(0, m->getStartSeqno());

    // 6)
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    // CheckpointProcessorTask runs
    // Get items from CM, expect Disk{A:3}
    outItems = activeStream->public_getOutstandingItems(*vb);
    activeStream->public_processItems(outItems);

    // 7)
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    m = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_DISK | MARKER_FLAG_CHK, m->getFlags());

    // Before the fix this snapshot marker had a start seqno of 3 rather than 2
    EXPECT_EQ(2, m->getStartSeqno());
}

// 1) Receive DCP memory checkpoint snapshot from 1-1
// 2) Receive DCP mutation at seqno 1
// 3) Receive DCP memory checkpoint snapshot from 2-3
// 4) Receive DCP mutation at seqno 3 (seqno 2 was de-duped)
// 5) Stream snap marker of 0-1 for snapshot at 1
// 6) Stream mutation at seqno 1
// 7) Stream snap marker of 2-3 for snapshot at 3 (previously this was 3-3).
TEST_P(DurabilityPromotionStreamTest,
       MemoryCheckpointStreamsSnapStartFromCheckpoint) {
    // Note: We need to plant a DCP cursor on ephemeral for preventing
    // checkpoint removal
    registerCursorAtCMStartIfEphemeral();

    auto opaque = 0;
    // 1)
    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            1 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            1 /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    DurabilityPassiveStreamTest::stream->processMarker(&marker);

    // 2)
    auto mutation = makeCommittedItem(makeStoredDocKey("dummy"), "value");
    mutation->setBySeqno(1);
    mutation->setCas(1);
    EXPECT_EQ(cb::engine_errc::success,
              DurabilityPassiveStreamTest::stream->messageReceived(
                      std::make_unique<MutationConsumerMessage>(
                              mutation,
                              0 /*opaque*/,
                              IncludeValue::Yes,
                              IncludeXattrs::Yes,
                              IncludeDeleteTime::No,
                              IncludeDeletedUserXattrs::Yes,
                              DocKeyEncodesCollectionId::No,
                              nullptr,
                              cb::mcbp::DcpStreamId{})));

    // 3)
    marker = SnapshotMarker(
            0 /*opaque*/,
            vbid,
            2,
            3,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            1 /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    DurabilityPassiveStreamTest::stream->processMarker(&marker);

    // 4)
    mutation = makeCommittedItem(makeStoredDocKey("dummy2"), "value");
    mutation->setBySeqno(3);
    mutation->setCas(3);
    EXPECT_EQ(cb::engine_errc::success,
              DurabilityPassiveStreamTest::stream->messageReceived(
                      std::make_unique<MutationConsumerMessage>(
                              mutation,
                              0 /*opaque*/,
                              IncludeValue::Yes,
                              IncludeXattrs::Yes,
                              IncludeDeleteTime::No,
                              IncludeDeletedUserXattrs::Yes,
                              DocKeyEncodesCollectionId::No,
                              nullptr,
                              cb::mcbp::DcpStreamId{})));

    // Remove PassiveStream and Consumer
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    // Simulate vbstate-change Replica->Active (we have also a Producer and
    // ActiveStream from this point onward)
    DurabilityActiveStreamTest::setUp(true /*startCheckpointProcessorTask*/,
                                      false /*persist*/);
    ASSERT_TRUE(producer);
    auto* activeStream = DurabilityActiveStreamTest::stream.get();
    ASSERT_TRUE(activeStream);

    // CheckpointProcessorTask runs
    // Get items from CM, expect Disk{M:1} as only one Disk checkpoint can be
    // retrieved at a time
    auto vb = store->getVBucket(vbid);
    auto outItems = activeStream->public_getOutstandingItems(*vb);

    // Push items into the Stream::readyQ
    activeStream->public_processItems(outItems);

    // readyQ also contain SnapshotMarker
    ASSERT_EQ(4, activeStream->public_readyQSize());

    // 5)
    auto resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* m = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, m->getFlags());
    EXPECT_EQ(0, m->getStartSeqno());
    EXPECT_EQ(1, m->getEndSeqno());

    // 6)
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    // CheckpointProcessorTask runs
    // Get items from CM, expect Disk{A:3}
    outItems = activeStream->public_getOutstandingItems(*vb);
    activeStream->public_processItems(outItems);

    // 7)
    resp = activeStream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    m = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, m->getFlags());

    // Before the fix this snapshot marker had a start seqno of 3 rather than 2
    EXPECT_EQ(2, m->getStartSeqno());
    EXPECT_EQ(3, m->getEndSeqno());
}

/**
 * Test fixture for durability-related PassiveStream tests where the Producer
 * is a downlevel version and doesn't support SyncWrites.
 */
class DurabilityPassiveStreamDownlevelProducerTest
    : public SingleThreadedPassiveStreamTest {
    // Impl identical to SingleThreadedPassiveStreamTest, just in own class
    // for clarity.
};

// MB-37161: When receiving a disk snapshot from a pre-MadHatter Producer, it
// will not sent a HCS (given it knows nothing of SyncWrites). This should _not_
// be treated as a non-present HCS by the mad-hatter consumer, effectively it's
// a HCS of zero (no SyncWrites have yet been completed).
TEST_P(DurabilityPassiveStreamDownlevelProducerTest,
       ReceiveBackfilledSnapshotPreSyncReplication) {
    uint32_t opaque = 1;

    SnapshotMarker marker(
            opaque,
            vbid,
            1 /*snapStart*/,
            1 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK,
            {} /*HCS missing as pre-MH producer will not send it*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    // Prior to the fix this would fail an Expects()
    stream->processMarker(&marker);
}

void DurabilityActiveStreamTest::testBackfillNoSyncWriteSupport(
        DocumentState docState, cb::durability::Level level) {
    if (!(persistent() || level == cb::durability::Level::Majority)) {
        return;
    }
    // drop the sync-write aware producer and stream created in setup
    producer = producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test_producer->test_consumer",
                                              0,
                                              false /*startTask*/);
    stream.reset();
    // Store
    //   Mutation
    //   Prepare
    //   Abort
    auto vb = engine->getVBucket(vbid);
    auto mutation = makeCommittedItem(makeStoredDocKey("mutation"), "value");

    {
        auto cHandle = vb->lockCollections(mutation->getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(*mutation, cookie, *engine, {}, cHandle));
    }
    flushVBucketToDiskIfPersistent(vbid, 1);

    const auto prepare =
            makePendingItem(makeStoredDocKey("prepare"), "value", {level, {}});

    {
        auto cHandle = vb->lockCollections(prepare->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
    }
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 2);

    ASSERT_EQ(cb::engine_errc::success,
              vb->abort(prepare->getKey(),
                        prepare->getBySeqno(),
                        {},
                        vb->lockCollections(prepare->getKey())));

    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // Create NON sync repl DCP stream
    stream = producer->mockActiveStreamRequest(
            /* flags */ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0);

    ASSERT_FALSE(stream->public_supportSyncReplication());

    stream->transitionStateToBackfilling();

    auto& manager = producer->getBFM();

    EXPECT_EQ(backfill_success, manager.backfill()); // create
    EXPECT_EQ(backfill_success, manager.backfill()); // scan
    EXPECT_EQ(backfill_finished, manager.backfill()); // nothing else to run

    ASSERT_EQ(2, stream->public_readyQSize());

    auto item = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, item->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*item);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());

    item = stream->public_nextQueuedItem(*producer);

    EXPECT_EQ(DcpResponse::Event::Mutation, item->getEvent());
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Alive_Majority) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Alive, Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Alive_MajorityAndPersist) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Alive,
                                   Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Alive_Persist) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Alive,
                                   Level::PersistToMajority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Delete_Majority) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Deleted, Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Delete_MajorityAndPersist) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Deleted,
                                   Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillPrepareNoSyncWriteSupport_Delete_Persist) {
    using cb::durability::Level;
    testBackfillNoSyncWriteSupport(DocumentState::Deleted,
                                   Level::PersistToMajority);
}

void DurabilityActiveStreamTest::testEmptyBackfillNoSyncWriteSupport(
        DocumentState docState, cb::durability::Level level) {
    if (!(persistent() || level == cb::durability::Level::Majority)) {
        return;
    }
    // drop the sync-write aware producer and stream created in setup
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 0,
                                                 true /*startTask*/);
    stream.reset();
    // Store
    //   Prepare
    //   Abort
    auto key = makeStoredDocKey("key");

    const auto prepare = makePendingItem(key, "value", {level, {}});

    auto vb = engine->getVBucket(vbid);
    {
        auto cHandle = vb->lockCollections(prepare->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
    }
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    ASSERT_EQ(cb::engine_errc::success,
              vb->abort(key,
                        prepare->getBySeqno(),
                        {},
                        vb->lockCollections(prepare->getKey())));

    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // Create NON sync repl DCP stream
    auto stream = producer->mockActiveStreamRequest(
            /* flags */ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0);

    // nothing in checkpoint manager or ready queue
    EXPECT_EQ(0, stream->getItemsRemaining());

    stream->transitionStateToBackfilling();
    EXPECT_EQ(ActiveStream::StreamState::Backfilling, stream->getState());

    auto& manager = producer->getBFM();

    EXPECT_EQ(backfill_success, manager.backfill()); // create->done
    EXPECT_EQ(backfill_finished, manager.backfill()); // nothing else to run

    // nothing in checkpoint manager or ready queue
    EXPECT_EQ(0, stream->getItemsRemaining());

    ASSERT_EQ(0, stream->public_readyQSize());

    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    // nothing in checkpoint manager or ready queue
    EXPECT_EQ(0, stream->getItemsRemaining());
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Alive_Majority) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Alive, Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Alive_MajorityAndPersist) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Alive,
                                        Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Alive_Persist) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Alive,
                                        Level::PersistToMajority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Delete_Majority) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Deleted,
                                        Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Delete_MajorityAndPersist) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Deleted,
                                        Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotNoSyncWriteSupport_Delete_Persist) {
    using cb::durability::Level;
    testEmptyBackfillNoSyncWriteSupport(DocumentState::Deleted,
                                        Level::PersistToMajority);
}

void DurabilityActiveStreamTest::
        testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
                DocumentState docState, cb::durability::Level level) {
    if (!(persistent() || level == cb::durability::Level::Majority)) {
        return;
    }
    // drop the sync-write aware producer and stream created in setup
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 0,
                                                 true /*startTask*/);
    producer->createCheckpointProcessorTask();
    stream.reset();
    auto key = makeStoredDocKey("key");
    auto vb = engine->getVBucket(vbid);
    // Store Mutation
    auto mutation = store_item(vbid, key, "value");
    // drop the checkpoint to cause initial backfill
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // Create NON sync repl DCP stream
    auto stream = producer->mockActiveStreamRequest(
            /* flags */ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0);

    EXPECT_EQ(ActiveStream::StreamState::Backfilling, stream->getState());

    auto& manager = producer->getBFM();

    // backfill the mutation
    EXPECT_EQ(backfill_success, manager.backfill()); // init
    EXPECT_EQ(backfill_success, manager.backfill()); // create
    EXPECT_EQ(backfill_finished, manager.backfill()); // nothing else to run

    // snapshot marker + mutation
    EXPECT_EQ(2, stream->public_readyQSize());

    EXPECT_EQ(ActiveStream::StreamState::Backfilling, stream->getState());

    auto resp = stream->next(*producer);
    EXPECT_TRUE(resp);

    // snap marker
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());

    EXPECT_EQ(ActiveStream::StreamState::Backfilling, stream->getState());

    // receive the mutation. Last item from backfill, stream transitions to
    // in memory.
    resp = stream->next(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    // drop the cursor
    stream->handleSlowStream();

    // Store
    //   Prepare
    //   Abort
    const auto prepare = makePendingItem(key, "value", {level, {}});
    {
        auto cHandle = vb->lockCollections(prepare->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
    }
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    ASSERT_EQ(cb::engine_errc::success,
              vb->abort(key,
                        prepare->getBySeqno(),
                        {},
                        vb->lockCollections(prepare->getKey())));
    // remove checkpoint to ensure stream backfills from disk
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // stream transitions back to backfilling because the cursor was dropped
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    EXPECT_EQ(ActiveStream::StreamState::Backfilling, stream->getState());

    EXPECT_EQ(backfill_success, manager.backfill()); // create->done
    EXPECT_EQ(backfill_finished, manager.backfill()); // nothing else to run

    ASSERT_EQ(0, stream->public_readyQSize());

    // No items should have been added to the ready queue, they are all
    // aborts/prepares and the stream did not negotiate for sync writes
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    // Now test that in-memory streaming starts at the correct point -
    // after the prepare and abort.
    // mutation to stream from memory
    auto mutation2 = store_item(vbid, key, "value");

    MockDcpMessageProducers producers;

    runCheckpointProcessor(*producer, producers);

    // snapshot marker + mutation
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_TRUE(resp);

    // snap marker
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(4, snapMarker.getStartSeqno());
    EXPECT_EQ(4, snapMarker.getEndSeqno());

    // mutation
    resp = stream->next(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Alive_Majority) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(DocumentState::Alive,
                                                           Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Alive_MajorityAndPersist) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState::Alive, Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Alive_Persist) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState::Alive, Level::PersistToMajority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Delete_Majority) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState::Deleted, Level::Majority);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Delete_MajorityAndPersist) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState::Deleted, Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityActiveStreamTest,
       BackfillEmptySnapshotAfterCursorDroppingNoSyncWriteSupport_Delete_Persist) {
    using cb::durability::Level;
    testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState::Deleted, Level::PersistToMajority);
}

// Test that when multiple checkpoints are combined and we generate many markers
// the markers are correct
TEST_P(DurabilityActiveStreamTest, inMemoryMultipleMarkers) {
    auto vb = store->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    ASSERT_TRUE(producer);
    auto* activeStream = DurabilityActiveStreamTest::stream.get();
    ASSERT_TRUE(activeStream);

    auto prepare = makePendingItem(
            makeStoredDocKey("prep1"),
            "value",
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    {
        auto cHandle = vb->lockCollections(prepare->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
    }

    ckptMgr.createNewCheckpoint();

    auto item = makeCommittedItem(makeStoredDocKey("mut1"), "value");
    {
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }

    prepare = makePendingItem(
            makeStoredDocKey("prep2"),
            "value",
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout(1)));
    {
        auto cHandle = vb->lockCollections(prepare->getKey());
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*prepare, cookie, *engine, {}, cHandle));
    }

    // We should get items from two checkpoints which will make processItems
    // generate two markers
    auto items = stream->public_getOutstandingItems(*vb);
    stream->public_processItems(items);

    // marker, prepare, marker, mutation, prepare
    EXPECT_EQ(5, stream->public_readyQSize());
    auto resp = stream->next(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(snapMarker.getFlags() & MARKER_FLAG_CHK);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());
    EXPECT_EQ(0, snapMarker.getMaxVisibleSeqno().value_or(~0));

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(snapMarker.getFlags() & MARKER_FLAG_CHK);
    EXPECT_EQ(2, snapMarker.getStartSeqno());
    EXPECT_EQ(3, snapMarker.getEndSeqno());
    EXPECT_EQ(2, snapMarker.getMaxVisibleSeqno().value_or(~0));

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
}

void DurabilityPassiveStreamEphemeralTest::testLogicalCommitCorrectTypeSetup() {
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1,
                          3,
                          MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          0 /*HCS*/,
                          1 /*maxVisibleSeqno*/,
                          {} /*timestamp*/,
                          {} /*streamId*/);
    stream->processMarker(&marker);
}

void DurabilityPassiveStreamEphemeralTest::
        testPrepareCommitedInDiskSnapshotCorrectState(
                std::optional<DeleteSource> deleted) {
    testLogicalCommitCorrectTypeSetup();

    auto key = makeStoredDocKey("key");
    uint64_t cas = 0;
    uint64_t seqno = 1;
    makeAndReceiveDcpPrepare(key, cas++, seqno++);
    makeAndReceiveCommittedItem(key, cas, seqno, deleted);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto* ephVb = static_cast<EphemeralVBucket*>(vb.get());
    ASSERT_TRUE(ephVb);
    auto itr = ephVb->makeRangeIterator(true /*backfill*/);
    EXPECT_EQ(1, itr->getHighCompletedSeqno());
}

TEST_P(DurabilityPassiveStreamEphemeralTest,
       PrepareMutationInDiskSnapshotCorrectCommittedState) {
    testPrepareCommitedInDiskSnapshotCorrectState();
}

TEST_P(DurabilityPassiveStreamEphemeralTest,
       PrepareDeletionInDiskSnapshotCorrectCommittedState) {
    testPrepareCommitedInDiskSnapshotCorrectState(DeleteSource::Explicit);
}

void DurabilityPassiveStreamEphemeralTest::
        testAbortCommitedInDiskSnapshotCorrectState(
                std::optional<DeleteSource> deleted) {
    testLogicalCommitCorrectTypeSetup();

    auto key = makeStoredDocKey("key");
    uint64_t cas = 0;
    uint64_t seqno = 2;
    stream->messageReceived(std::make_unique<AbortSyncWriteConsumer>(
            0 /*opaque*/, vbid, key, 1, seqno++));
    makeAndReceiveCommittedItem(key, cas, seqno, deleted);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto* ephVb = static_cast<EphemeralVBucket*>(vb.get());
    ASSERT_TRUE(ephVb);
    auto itr = ephVb->makeRangeIterator(true /*backfill*/);
    EXPECT_EQ(1, itr->getHighCompletedSeqno());
}

TEST_P(DurabilityPassiveStreamEphemeralTest,
       AbortMutationInDiskSnapshotCorrectCommittedState) {
    testAbortCommitedInDiskSnapshotCorrectState();
}

TEST_P(DurabilityPassiveStreamEphemeralTest,
       AbortDeletionInDiskSnapshotCorrectCommittedState) {
    testAbortCommitedInDiskSnapshotCorrectState(DeleteSource::Explicit);
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         DurabilityActiveStreamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         DurabilityPassiveStreamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         DurabilityPassiveStreamDownlevelProducerTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         DurabilityPromotionStreamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Ephemeral,
                         DurabilityPassiveStreamEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        Persistent,
        DurabilityPassiveStreamPersistentTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
