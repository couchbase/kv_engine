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

#include "evp_store_durability_test.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_utils.h"
#include "test_helpers.h"

class DurabilityEPBucketTest : public STParameterizedEPBucketTest {
    void SetUp() {
        STParameterizedEPBucketTest::SetUp();
        // Add an initial replication topology so we can accept SyncWrites.
        setVBucketToActiveWithValidTopology();
    }

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }
};

/*
 * Testcases related to persistence of durability related items
 * (aka SyncWrites)
 */

TEST_P(DurabilityEPBucketTest, PersistPrepare) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));
    auto& vb = *getEPBucket().getVBucket(vbid);
    ASSERT_EQ(1, vb.getNumItems());
    auto pending = makePendingItem(key, "valueB");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));

    const auto& ckptMgr = *getEPBucket().getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(2, ckptMgr.getNumItemsForPersistence());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // Committed and Pending will be split into two checkpoints:
    ASSERT_EQ(2, ckptList.size());

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(2, stats.diskQueueSize);

    // Item must be flushed
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(2) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    // Item must have been removed from the disk queue
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);

    // The item count must not increase when flushing Pending SyncWrites
    EXPECT_EQ(1, vb.getNumItems());

    // Check the Prepare on disk
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*prepare*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_FALSE(gv.item->isDeleted());
}

TEST_P(DurabilityEPBucketTest, PersistAbort) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *getEPBucket().getVBucket(vbid);
    ASSERT_EQ(0, vb.getNumItems());

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    // A Prepare doesn't account in curr-items
    ASSERT_EQ(0, vb.getNumItems());

    {
        auto res = vb.ht.findForWrite(key);
        ASSERT_TRUE(res.storedValue);
        ASSERT_EQ(CommittedState::Pending, res.storedValue->getCommitted());
        ASSERT_EQ(1, res.storedValue->getBySeqno());
    }
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.diskQueueSize);
    const auto& ckptMgr = *getEPBucket().getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, ckptMgr.getNumItemsForPersistence());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.front()->getState());
    ASSERT_EQ(1, ckptList.front()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.front()->end()))->getOperation() ==
                      queue_op::pending_sync_write);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       1 /*prepareSeqno*/,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // 2 different checkpoints)
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::abort_sync_write);
    EXPECT_EQ(2, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(2, stats.diskQueueSize);

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(1) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);

    // At persist-dedup, the Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
}

/// Test that if a single key is prepared, aborted & re-prepared it is the
/// second Prepare which is kept on disk.
TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepare) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *getEPBucket().getVBucket(vbid);

    // First prepare and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare.
    auto pending2 = makePendingItem(key, "value2");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending2, cookie));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *getEPBucket().getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::pending_sync_write);
    EXPECT_EQ(3, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(1) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    // At persist-dedup, the 2nd Prepare survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_FALSE(gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno(), gv.item->getBySeqno());
}

/// Test that if a single key is prepared, aborted re-prepared & re-aborted it
/// is the second Abort which is kept on disk.
TEST_P(DurabilityEPBucketTest, PersistPrepareAbortx2) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *getEPBucket().getVBucket(vbid);

    // First prepare and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare and abort.
    auto pending2 = makePendingItem(key, "value2");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending2, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending2->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *getEPBucket().getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(4, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::abort_sync_write);
    EXPECT_EQ(4, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Abort are in the same key-space and hence are
    //       deduplicated at Flush.
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(1) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    // At persist-dedup, the 2nd Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno() + 1, gv.item->getBySeqno());
}

TEST_P(DurabilityEPBucketTest, ActiveLocalNotifyPersistedSeqno) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    const cb::durability::Requirements reqs = {
            cb::durability::Level::PersistToMajority, 0 /*timeout*/};

    for (uint8_t seqno = 1; seqno <= 3; seqno++) {
        auto item = makePendingItem(
                makeStoredDocKey("key" + std::to_string(seqno)), "value", reqs);
        ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    }

    const auto& vb = getEPBucket().getVBucket(vbid);
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);

    auto checkPending = [&ckptList]() -> void {
        ASSERT_EQ(1, ckptList.size());
        const auto& ckpt = *ckptList.front();
        EXPECT_EQ(3, ckpt.getNumItems());
        for (const auto& qi : ckpt) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
            }
        }
    };

    // No replica has ack'ed yet
    checkPending();

    // Replica acks disk-seqno
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged("replica", 3 /*preparedSeqno*/));
    // Active has not persisted, so Durability Requirements not satisfied yet
    checkPending();

    // Flusher runs on Active. This:
    // - persists all pendings
    // - and notifies local DurabilityMonitor of persistence
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(3) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    // When seqno:1 is persisted:
    //
    // - the Flusher notifies the local DurabilityMonitor
    // - seqno:1 is satisfied, so it is committed
    // - the open checkpoint containes the seqno:1:prepare, so it is closed and
    //     seqno:1:committed is enqueued in a new open checkpoint (that is how
    //     we avoid SyncWrite de-duplication currently)
    // - the next committed seqnos are enqueued into the same open checkpoint
    //
    // So after Flush we have 2 checkpoints: the first (closed) containing only
    // pending SWs and the second (open) containing only committed SWs
    ASSERT_EQ(2, ckptList.size());

    // Remove the closed checkpoint (that makes the check on Committed easier)
    bool newOpenCkptCreated{false};
    vb->checkpointManager->removeClosedUnrefCheckpoints(*vb,
                                                        newOpenCkptCreated);
    ASSERT_FALSE(newOpenCkptCreated);

    // Durability Requirements satisfied, all committed
    ASSERT_EQ(1, ckptList.size());
    const auto& ckpt = *ckptList.front();
    EXPECT_EQ(3, ckpt.getNumItems());
    for (const auto& qi : ckpt) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
        }
    }
}

// Test cases which run against all enabled storage backends.
INSTANTIATE_TEST_CASE_P(AllBackends,
                        DurabilityEPBucketTest,
                        DurabilityEPBucketTest::allConfigValues(),
                        STParameterizedEPBucketTestPrintName());
