/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "dcp_durability_stream_test.h"

#include "../../src/dcp/backfill-manager.h"
#include "checkpoint_utils.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "durability/durability_monitor.h"
#include "test_helpers.h"

#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityActiveStreamTest::SetUp() {
    SingleThreadedActiveStreamTest::SetUp();
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    setupProducer({{"enable_synchronous_replication", "true"}});
    ASSERT_TRUE(stream->public_supportSyncReplication());
}

void DurabilityActiveStreamTest::TearDown() {
    SingleThreadedActiveStreamTest::TearDown();
}

void DurabilityActiveStreamTest::testSendDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));

    auto prepareSeqno = 1;
    uint64_t cas;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
        cas = sv.storedValue->getCas();
    }

    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We do in HT stats
    EXPECT_EQ(1, vb->ht.getNumItems());
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
    auto outItems = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outItems.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.at(0)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write, outItems.at(1)->getOperation());
    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);
    // Stream::readyQ must contain SnapshotMarker + DCP_PREPARE
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    EXPECT_EQ(prepareSeqno, *resp->getBySeqno());
    auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_EQ(value, prepare.getItem()->getValue()->to_s());
    EXPECT_EQ(cas, prepare.getItem()->getCas());
    ASSERT_EQ(0, stream->public_readyQSize());
    resp = stream->public_popFromReadyQ();
    ASSERT_FALSE(resp);
}

TEST_P(DurabilityActiveStreamTest, SendDcpPrepare) {
    testSendDcpPrepare();
}

/*
 * This test checks that the ActiveStream::readyQ contains the right DCP
 * messages during the journey of an Aborted sync-write.
 */
TEST_P(DurabilityActiveStreamTest, SendDcpAbort) {
    // First, we need to enqueue a Prepare.
    testSendDcpPrepare();
    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");
    const uint64_t prepareSeqno = 1;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
    }

    // Now we proceed with testing the Abort of that Prepare
    auto& ckptMgr = *vb->checkpointManager;

    // Simulate timeout, indirectly calls VBucket::abort
    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(1000));

    // We don't account Abort in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    if (persistent()) {
        // We must have removed the Prepare from the HashTable and we don't have
        // any "abort" StoredValue
        EXPECT_EQ(0, vb->ht.getNumItems());
    } else {
        // Ephemeral allows completed prepares in the HashTable
        EXPECT_EQ(1, vb->ht.getNumItems());
    }
    // Note: We don't de-duplicate Prepare and Abort, so we have closed the open
    //     ckpt (the one containing the Prepare), created a new open ckpt and
    //     queued the Abort in the latter. So we must have 2 checkpoints now.
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED,
              ckptList.front()->getState());
    const auto* ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is Abort and doesn't carry any value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_FALSE((*it)->getValue());

    // We must have ckpt-start + Abort
    auto outItems = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outItems.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.at(0)->getOperation());
    ASSERT_EQ(queue_op::abort_sync_write, outItems.at(1)->getOperation());
    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);
    // Stream::readyQ must contain SnapshotMarker + DCP_ABORT
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());
    const auto& abort = static_cast<AbortSyncWrite&>(*resp);
    EXPECT_EQ(key, abort.getKey());
    EXPECT_EQ(prepareSeqno, abort.getPreparedSeqno());
    EXPECT_EQ(2, abort.getAbortSeqno());
    ASSERT_EQ(0, stream->public_readyQSize());
    resp = stream->public_popFromReadyQ();
    ASSERT_FALSE(resp);
}

TEST_P(DurabilityActiveStreamEphemeralTest, BackfillDurabilityLevel) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));

    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state
    auto& bfm = producer->getBFM();
    bfm.backfill();

    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(2, readyQ.size());

    // First item is a snapshot marker so just skip it
    auto resp = stream->public_popFromReadyQ();
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prep = static_cast<MutationResponse&>(*resp);
    const auto respItem = prep.getItem();
    EXPECT_EQ(cb::durability::Level::Majority,
              respItem->getDurabilityReqs().getLevel());
    EXPECT_TRUE(respItem->getDurabilityReqs().getTimeout().isInfinite());
}

TEST_P(DurabilityActiveStreamTest, BackfillCommit) {
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    auto vb = engine->getVBucket(vbid);

    auto& ckptMgr = *vb->checkpointManager;

    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    auto prepareSeqno = vb->getHighSeqno();
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->commit(key,
                         prepareSeqno,
                         {} /*commitSeqno*/,
                         vb->lockCollections(key)));

    flushVBucketToDiskIfPersistent(vbid, 2);

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(3, readyQ.size());

    // First item is a snapshot marker so just skip it
    auto resp = stream->public_popFromReadyQ();
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prepare = static_cast<MutationConsumerMessage&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_EQ(value, prepare.getItem()->getValue()->to_s());
    resp = stream->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::Commit, resp->getEvent());
    const auto& cmmt = static_cast<CommitSyncWrite&>(*resp);
    ASSERT_TRUE(cmmt.getBySeqno());
    EXPECT_EQ(2, *cmmt.getBySeqno());

    producer->cancelCheckpointCreatorTask();
}

TEST_P(DurabilityActiveStreamTest, BackfillAbort) {
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    auto vb = engine->getVBucket(vbid);

    auto& ckptMgr = *vb->checkpointManager;

    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->abort(key,
                        vb->getHighSeqno(),
                        {} /*abortSeqno*/,
                        vb->lockCollections(key)));

    flushVBucketToDiskIfPersistent(vbid, 1);

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();
    bfm.backfill();
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(2, readyQ.size());

    // First item is a snapshot marker so just skip it
    auto resp = stream->public_popFromReadyQ();
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());
    const auto& abrt = static_cast<AbortSyncWrite&>(*resp);
    ASSERT_TRUE(abrt.getBySeqno());
    EXPECT_EQ(2, *abrt.getBySeqno());
    EXPECT_EQ(1, abrt.getPreparedSeqno());

    producer->cancelCheckpointCreatorTask();
}

TEST_P(DurabilityActiveStreamTest, RemoveUnknownSeqnoAckAtDestruction) {
    auto vb = engine->getVBucket(vbid);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // We don't include prepares in the numItems stat (should not exist in here)
    if (fullEviction()) {
        // @TODO Durability (MB-34092): getNumItems should always be 0 here,
        //  not 1 in full-eviction
        EXPECT_EQ(1, vb->getNumItems());
    } else {
        EXPECT_EQ(0, vb->getNumItems());
    }

    // Our topology gives replica name as "replica" an our producer/stream has
    // name "test_producer". Simulate a seqno ack by calling the vBucket level
    // function.
    vb->seqnoAcknowledged("test_producer", 1);

    // An unknown seqno ack should not have committed the item
    if (fullEviction()) {
        // @TODO Durability (MB-34092): getNumItems should always be 0 here,
        //  not 1 in full-eviction
        EXPECT_EQ(1, vb->getNumItems());
    } else {
        EXPECT_EQ(0, vb->getNumItems());
    }

    // Disconnect the ActiveStream
    stream->setDead(END_STREAM_DISCONNECTED);

    // If the seqno ack still existed in the queuedSeqnoAcks map then it would
    // result in a commit on topology change
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array(
                      {{"active", "replica1", "test_producer"}})}});

    if (fullEviction()) {
        // @TODO Durability (MB-34092): getNumItems should always be 0 here,
        //  not 1 in full-eviction
        EXPECT_EQ(1, vb->getNumItems());
    } else {
        EXPECT_EQ(0, vb->getNumItems());
    }
}

void DurabilityActiveStreamTest::setUpSendSetInsteadOfCommitTest() {
    auto vb = engine->getVBucket(vbid);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
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

    // Need to close the previously existing checkpoints so that we can backfill
    // from disk
    bool newCkpt = false;
    auto size =
            mockCkptMgr.removeClosedUnrefCheckpoints(*vb, newCkpt, 5 /*limit*/);
    ASSERT_FALSE(newCkpt);
    ASSERT_EQ(4, size);
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
    stream = std::make_shared<MockActiveStream>(engine.get(),
                                                producer,
                                                0 /*flags*/,
                                                0 /*opaque*/,
                                                *vb,
                                                0 /*st_seqno*/,
                                                ~0 /*en_seqno*/,
                                                0x0 /*vb_uuid*/,
                                                0 /*snap_start_seqno*/,
                                                ~0 /*snap_end_seqno*/);

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

TEST_P(DurabilityActiveStreamTest, SendCommitForResumeIfPrepareReceived) {
    setUpSendSetInsteadOfCommitTest();

    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");

    // Disconnect and resume from our prepare. We resume from prepare 3 in this
    // case so that the producers data will just be [4: Commit, 5:Prepare].
    stream = std::make_shared<MockActiveStream>(engine.get(),
                                                producer,
                                                0 /*flags*/,
                                                0 /*opaque*/,
                                                *vb,
                                                3 /*st_seqno*/,
                                                ~0 /*en_seqno*/,
                                                0x0 /*vb_uuid*/,
                                                3 /*snap_start_seqno*/,
                                                ~3 /*snap_end_seqno*/);

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

    // Followed by a commit because the producer knows we are not missing a
    // prepare.
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Commit, resp->getEvent());
    const auto& commit = static_cast<CommitSyncWrite&>(*resp);
    EXPECT_EQ(key, commit.getKey());
    EXPECT_EQ(4, *commit.getBySeqno());
    EXPECT_EQ(3, commit.getPreparedSeqno());

    // Followed by a prepare
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_TRUE(prepare.getItem()->isPending());
    EXPECT_EQ(5, prepare.getItem()->getBySeqno());
}

void DurabilityPassiveStreamTest::SetUp() {
    SingleThreadedPassiveStreamTest::SetUp();
    consumer->enableSyncReplication();
    ASSERT_TRUE(consumer->isSyncReplicationEnabled());
}

void DurabilityPassiveStreamTest::TearDown() {
    SingleThreadedPassiveStreamTest::TearDown();
}

TEST_P(DurabilityPassiveStreamTest,
       ReceiveSetInsteadOfCommitForReconnectWindowWithPrepareLast) {
    // 1) Receive DCP Prepare
    auto key = makeStoredDocKey("key");
    uint64_t prepareSeqno = 1;
    uint64_t cas = 0;
    makeAndReceiveDcpPrepare(key, cas, prepareSeqno);

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
    SnapshotMarker marker(
            opaque,
            vbid,
            streamStartSeqno /*snapStart*/,
            streamStartSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    const std::string value = "overwritingValue";
    queued_item qi(new Item(key,
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            0 /*cas*/,
                            streamStartSeqno,
                            vbid));

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 5) Verify doc state
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    {
        // findForCommit will return both pending and committed perspectives
        auto res = vb->ht.findForCommit(key);
        EXPECT_FALSE(res.pending);
        ASSERT_TRUE(res.committed);
        EXPECT_EQ(4, res.committed->getBySeqno());
        EXPECT_EQ(CommittedState::CommittedViaMutation,
                  res.committed->getCommitted());
        EXPECT_TRUE(res.committed->getValue());
        EXPECT_EQ(value, res.committed->getValue()->to_s());
    }

    // Should have removed all sync writes
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    // We should now be able to do a sync write to a different key
    key = makeStoredDocKey("newkey");
    makeAndReceiveDcpPrepare(key, cas, 10);
    prepareSeqno = vb->getHighSeqno();
    marker = SnapshotMarker(
            opaque,
            vbid,
            streamStartSeqno + 2 /*snapStart*/,
            streamStartSeqno + 2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->commit(key, prepareSeqno, {}, vb->lockCollections(key)));
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
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
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    const std::string value("value");

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    using namespace cb::durability;
    const uint64_t swSeqno = 2;
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      swSeqno,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    // readyQ still empty, we have not received the snap-end mutation yet
    EXPECT_EQ(0, readyQ.size());

    // snapshot-end
    ASSERT_EQ(ENGINE_SUCCESS,
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

TEST_P(DurabilityPassiveStreamPersistentTest, SeqnoAckAtPersistedSeqno) {
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
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    const std::string value("value");

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    const int64_t swSeqno = 2;
    using namespace cb::durability;
    ASSERT_EQ(ENGINE_SUCCESS,
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
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));
    // No ack yet, we have not yet received the complete snapshot
    EXPECT_EQ(0, readyQ.size());

    // Non-durable s:4 (snapshot-end) received
    ASSERT_EQ(ENGINE_SUCCESS,
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
                                  {});
    stream->processMarker(&snapshotMarker);
    EXPECT_EQ(0, readyQ.size());

    // s:1 non-durable -> no ack
    const std::string value("value");
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:2 Level:Majority -> no ack
    using namespace cb::durability;
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      2 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:3 non-durable -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:4 Level:MajorityAndPersistOnMaster -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      4 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::MajorityAndPersistOnMaster,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:5 non-durable -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      5 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:6 Level:PersistToMajority -> no ack (durability-fence)
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      6 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::PersistToMajority,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:7 Level-Majority -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      7 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:8 Level:MajorityAndPersistOnMaster -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      8 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::MajorityAndPersistOnMaster,
                                   Timeout::Infinity()))));
    EXPECT_EQ(0, readyQ.size());

    // s:9 non-durable -> no ack
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      9 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // s:10 non-durable (snapshot-end) -> ack (HPS=4, durability-fence at 6)
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(
                      makeMutationConsumerMessage(10, vbid, value, opaque)));
    checkSeqnoAckInReadyQ(4 /*HPS*/);

    // Flusher persists all -> ack (HPS=8)
    flushVBucketToDiskIfPersistent(vbid, 10 /*expectedNumFlushed*/);
    checkSeqnoAckInReadyQ(8 /*HPS*/);
}

queued_item DurabilityPassiveStreamTest::makeAndReceiveDcpPrepare(
        const StoredDocKey& key, uint64_t cas, uint64_t seqno) {
    using namespace cb::durability;

    // The consumer receives snapshot-marker [seqno, seqno]
    uint32_t opaque = 0;
    SnapshotMarker marker(
            opaque,
            vbid,
            seqno,
            seqno,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    queued_item qi = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    qi->setBySeqno(seqno);
    qi->setCas(cas);

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      qi,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
    return qi;
}

void DurabilityPassiveStreamTest::testReceiveDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const uint64_t cas = 999;
    const uint64_t prepareSeqno = 1;
    auto qi = makeAndReceiveDcpPrepare(key, cas, prepareSeqno);

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
    SnapshotMarker marker(
            opaque,
            vbid,
            prepareSeqno /*snapStart*/,
            prepareSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
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

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
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
            {} /*streamId*/);
    stream->processMarker(&marker);

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<CommitSyncWrite>(
                      opaque, vbid, prepareSeqno, commitSeqno, key)));
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDuplicateDcpPrepare) {
    testReceiveDuplicateDcpPrepare(3);
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
    ASSERT_EQ(ENGINE_ERANGE,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));
}

TEST_P(DurabilityPassiveStreamTest, DeDupedPrepareWindowDoubleDisconnect) {
    testReceiveDcpPrepare();

    // Send another prepare for our second sequential prepare to overwrite.
    auto key = makeStoredDocKey("key1");
    const uint64_t cas = 999;
    uint64_t prepareSeqno = 2;
    makeAndReceiveDcpPrepare(key, cas, prepareSeqno);

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
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
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

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
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

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      std::move(qi),
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
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
    uint32_t opaque = 0;
    auto commitSeqno = prepareSeqno + 1;
    SnapshotMarker marker(
            opaque,
            vbid,
            commitSeqno,
            commitSeqno,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2 checkpoints
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(2, ckptList.size());
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, ckpt->getState());
    ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    // Now simulate the Consumer receiving Commit for that Prepare
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<CommitSyncWrite>(
                      opaque, vbid, prepareSeqno, commitSeqno, key)));

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

    // Process the 2nd Prepare.
    auto key = makeStoredDocKey("key");
    const uint64_t cas = 1234;
    const uint64_t prepare2ndSeqno = 3;
    makeAndReceiveDcpPrepare(key, cas, prepare2ndSeqno);

    // 3 checkpoints
    auto vb = engine->getVBucket(vbid);
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(3, ckptList.size());
    auto ckptIt = ckptList.begin();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, (*ckptIt)->getState());
    ASSERT_EQ(1, (*ckptIt)->getNumItems());

    ckptIt++;
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, (*ckptIt)->getState());
    ASSERT_EQ(1, (*ckptIt)->getNumItems());

    ckptIt++;
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
                                 uint64_t abortSeqno) -> ENGINE_ERROR_CODE {
        return stream->messageReceived(std::make_unique<AbortSyncWrite>(
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
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2 checkpoints
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(2, ckptList.size());
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, ckpt->getState());
    ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    // The consumer receives an Abort for the previous Prepare.
    // Note: The call to abortReceived() above throws /after/
    //     PassiveStream::last_seqno has been incremented, so we need to
    //     abortSeqno to bypass ENGINE_ERANGE checks.
    abortSeqno++;
    prepareSeqno++;
    ASSERT_EQ(ENGINE_SUCCESS, abortReceived(prepareSeqno, abortSeqno));

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

void DurabilityPassiveStreamTest::testReceiveDuplicateDcpAbort(
        const std::string& key,
        uint64_t prepareSeqno,
        ENGINE_ERROR_CODE expectedResult) {
    testReceiveDcpAbort();
    auto vb = engine->getVBucket(vbid);

    const uint64_t abortSeqno = 5;
    const auto docKey = makeStoredDocKey(key);
    uint32_t opaque = 0;

    // Fake disconnect and reconnect, importantly, this sets up the valid window
    // for ignoring DCPAborts.
    consumer->closeAllStreams();
    consumer->addStream(opaque, vbid, 0 /*flags*/);
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    stream->acceptStream(cb::mcbp::Status::Success, opaque);

    SnapshotMarker marker(
            opaque,
            vbid,
            abortSeqno /*snapStart*/,
            abortSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2 checkpoints
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(3, ckptList.size());
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, ckpt->getState());
    ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    EXPECT_EQ(expectedResult,
              stream->messageReceived(std::make_unique<AbortSyncWrite>(
                      opaque, vbid, docKey, prepareSeqno, abortSeqno)));

    EXPECT_EQ(0, vb->getNumItems());

    // Ephemeral keeps the completed prepare in the HashTable
    if (persistent()) {
        EXPECT_EQ(0, vb->ht.getNumItems());
    } else {
        EXPECT_EQ(1, vb->ht.getNumItems());
    }
    {
        const auto sv = vb->ht.findForWrite(docKey);
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
    ASSERT_EQ(0, ckpt->getNumItems());

    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpAbortIgnoreDeDuped) {
    testReceiveDuplicateDcpAbort("key", 4, ENGINE_SUCCESS);
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpAbortDuplicate) {
    testReceiveDuplicateDcpAbort("key", 3, ENGINE_EINVAL);
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpAbortDeDupedPrepareSeqnoTooHigh) {
    testReceiveDuplicateDcpAbort("key", 5, ENGINE_EINVAL);
}

TEST_P(DurabilityPassiveStreamTest, ReceiveDcpAbortIgnoreDeDupedNewKey) {
    testReceiveDuplicateDcpAbort("newkey", 4, ENGINE_SUCCESS);
}

void DurabilityPassiveStreamTest::setUpHandleSnapshotEndTest() {
    auto key1 = makeStoredDocKey("key1");
    uint64_t cas = 1;
    uint64_t prepareSeqno = 1;
    makeAndReceiveDcpPrepare(key1, cas, prepareSeqno);

    uint32_t opaque = 0;
    SnapshotMarker marker(
            opaque,
            vbid,
            prepareSeqno + 1 /*snapStart*/,
            prepareSeqno + 2 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto key2 = makeStoredDocKey("key2");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key2, "value2", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setCas(1);
    pending->setBySeqno(2);

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    ASSERT_EQ(true, stream->getCurSnapshotPrepare());
}

TEST_P(DurabilityPassiveStreamTest, HandleSnapshotEndOnCommit) {
    setUpHandleSnapshotEndTest();
    uint32_t opaque;
    auto key = makeStoredDocKey("key1");

    // Commit the original prepare
    auto prepareSeqno = 2;
    auto commitSeqno = prepareSeqno + 1;
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<CommitSyncWrite>(
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
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<AbortSyncWrite>(
                      opaque, vbid, key, 1 /*prepareSeqno*/, abortSeqno)));

    // We should have unset (acked the second prepare) the bool flag if we
    // handled the snapshot end
    EXPECT_EQ(false, stream->getCurSnapshotPrepare());
}

TEST_P(DurabilityPassiveStreamTest, ReceiveBackfilledDcpCommit) {
    // Need to use actual opaque of the stream as we hit the consumer level
    // function.
    uint32_t opaque = 1;

    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          {} /*streamId*/);
    stream->processMarker(&marker);

    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto prepare = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    prepare->setBySeqno(1);
    prepare->setCas(999);

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      prepare,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // Hit the consumer level function (not the stream level) for additional
    // error checking.
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->commit(opaque, vbid, key, prepare->getBySeqno(), 2));
}

TEST_P(DurabilityPassiveStreamTest, AllowsDupePrepareNamespaceInCheckpoint) {
    uint32_t opaque = 0;

    // 1) Send disk snapshot marker
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK,
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // 2) Send prepare
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    pending->setBySeqno(1);
    pending->setCas(1);

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 3) Send commit - should not throw
    auto commitSeqno = pending->getBySeqno() + 1;
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<CommitSyncWrite>(
                      opaque, vbid, pending->getBySeqno(), commitSeqno, key)));

    // 5) Send next in memory snapshot
    marker = SnapshotMarker(
            opaque,
            vbid,
            3 /*snapStart*/,
            4 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamID*/);
    stream->processMarker(&marker);

    // 6) Send prepare
    pending->setBySeqno(commitSeqno + 1);
    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                      pending,
                      opaque,
                      IncludeValue::Yes,
                      IncludeXattrs::Yes,
                      IncludeDeleteTime::No,
                      DocKeyEncodesCollectionId::No,
                      nullptr,
                      cb::mcbp::DcpStreamId{})));

    // 7) Send commit - allowed to exist in same checkpoint
    commitSeqno = pending->getBySeqno() + 1;

    EXPECT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(std::make_unique<CommitSyncWrite>(
                      opaque, vbid, pending->getBySeqno(), commitSeqno, key)));
}

INSTANTIATE_TEST_CASE_P(AllBucketTypes,
                        DurabilityActiveStreamTest,
                        STParameterizedBucketTest::allConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_CASE_P(AllBucketTypes,
                        DurabilityPassiveStreamTest,
                        STParameterizedBucketTest::allConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_CASE_P(
        AllBucketTypes,
        DurabilityPassiveStreamPersistentTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_CASE_P(AllBucketTypes,
                        DurabilityActiveStreamEphemeralTest,
                        STParameterizedBucketTest::ephConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);
