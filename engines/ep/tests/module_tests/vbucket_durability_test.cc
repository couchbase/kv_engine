/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "vbucket_durability_test.h"

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "test_helpers.h"
#include "thread_gate.h"

#include "../mock/mock_durability_monitor.h"

#include <folly/portability/GMock.h>
#include <thread>

using namespace std::string_literals;

void VBucketDurabilityTest::SetUp() {
    VBucketTest::SetUp();
    ht = &vbucket->ht;
    ckptMgr = vbucket->checkpointManager.get();
    vbucket->setState(
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{active, replica}})}});
    // Note: MockDurabilityMonitor is used only for accessing the base
    //     class protected members, it doesn't change the base class layout
    monitor = reinterpret_cast<MockDurabilityMonitor*>(
            vbucket->durabilityMonitor.get());
    ASSERT_GT(monitor->public_getReplicationChainSize(), 0);
}

size_t VBucketDurabilityTest::storeSyncWrites(
        const std::vector<SyncWriteSpec>& seqnos) {
    if (seqnos.empty()) {
        throw std::logic_error(
                "VBucketDurabilityTest::addSyncWrites: seqnos list is empty");
    }

    // @todo: For now this function is supposed to be called once per test,
    //     expand if necessary
    ht->clear();
    ckptMgr->clear(*vbucket, 0 /*lastBySeqno*/);

    // In general we need to test SyncWrites at sparse seqnos. To achieve that
    // we have 2 options (e.g., if we want to add SyncWrites with seqnos
    // {1, 3, 5}):
    // 1) We can use the VBucket::set interface. Given that bySeqno is
    //     auto-generated then we have to add non-sync mutations with seqnos
    //     {2, 4}.
    // 2) We can call directly VBucket::processSet and provide our bySeqno. We
    //     need to set the Checkpoint snapshot boundaries manually in this case
    //     (e.g., [1, 5]), the set fails otherwise.
    // I go with the latter way, that is the reason of the the following call.
    ckptMgr->createSnapshot(seqnos.front().seqno, seqnos.back().seqno);
    EXPECT_EQ(1, ckptMgr->getNumCheckpoints());

    size_t numStored = ht->getNumItems();
    size_t numCkptItems = ckptMgr->getNumItems();
    size_t numTracked = monitor->public_getNumTracked();
    for (auto write : seqnos) {
        auto item = Item(makeStoredDocKey("key" + std::to_string(write.seqno)),
                         0 /*flags*/,
                         0 /*exp*/,
                         "value",
                         5 /*valueSize*/,
                         PROTOCOL_BINARY_RAW_BYTES,
                         0 /*cas*/,
                         write.seqno);
        if (write.deletion) {
            item.setDeleted();
        }
        using namespace cb::durability;
        item.setPendingSyncWrite(Requirements(Level::Majority, 0 /*timeout*/));
        VBQueueItemCtx ctx;
        ctx.genBySeqno = GenerateBySeqno::No;
        ctx.durability = DurabilityItemCtx{item.getDurabilityReqs(), cookie};

        EXPECT_EQ(MutationStatus::WasClean,
                  public_processSet(item, 0 /*cas*/, ctx));

        EXPECT_EQ(++numStored, ht->getNumItems());
        EXPECT_EQ(++numTracked, monitor->public_getNumTracked());
        EXPECT_EQ(++numCkptItems, ckptMgr->getNumItems());
    }
    return numStored;
}

void VBucketDurabilityTest::testSyncWrites(
        const std::vector<SyncWriteSpec>& writes) {
    auto numStored = storeSyncWrites(writes);
    ASSERT_EQ(writes.size(), numStored);

    for (auto write : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));

        EXPECT_EQ(nullptr, ht->findForRead(key).storedValue);
        const auto sv = ht->findForWrite(key).storedValue;
        EXPECT_NE(nullptr, sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    EXPECT_EQ(numStored, ckptList.front()->getNumItems());
    for (const auto& qi : *ckptList.front()) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(CommittedState::Pending, qi->getCommitted());
            EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
        }
    }

    // Simulate flush + checkpoint-removal
    ckptMgr->clear(*vbucket, 0 /*lastBySeqno*/);

    // The active sends DCP_PREPARE messages to the replica, here I simulate
    // the replica DCP_SEQNO_ACK response
    vbucket->seqnoAcknowledged(
            replica, writes.back().seqno /*memorySeqno*/, 0 /*diskSeqno*/);

    for (auto write : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));

        const auto sv =
                ht->findForRead(key, TrackReference::Yes, WantsDeleted::Yes)
                        .storedValue;
        EXPECT_NE(nullptr, sv);
        EXPECT_NE(nullptr, ht->findForWrite(key).storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
        EXPECT_EQ(write.deletion, sv->isDeleted());
    }

    ASSERT_EQ(1, ckptList.size());
    EXPECT_EQ(numStored, ckptList.front()->getNumItems());
    for (const auto& qi : *ckptList.front()) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(CommittedState::CommittedViaPrepare, qi->getCommitted());
            EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
        }
    }
}

TEST_P(VBucketDurabilityTest, SyncWrites_ContinuousSeqnos) {
    testSyncWrites({1, 2, 3});
}

TEST_P(VBucketDurabilityTest, SyncWrites_ContinuousDeleteSeqnos) {
    testSyncWrites({{1, true}, {2, true}, {3, true}});
}

TEST_P(VBucketDurabilityTest, SyncWrites_SparseSeqnos) {
    testSyncWrites({1, 3, 10, 20, 30});
}

TEST_P(VBucketDurabilityTest, SyncWrites_SparseDeleteSeqnos) {
    testSyncWrites({{1, true}, {3, true}, {10, true}, {20, true}, {30, true}});
}

// Mix of Mutations and Deletions.
TEST_P(VBucketDurabilityTest, SyncWrites_SparseMixedSeqnos) {
    testSyncWrites({{1, true}, 3, {10, true}, {20, true}, 30});
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        VBucketDurabilityTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });

// Positive test for validateSetStateMeta 'topology' key - check that
// valid topology values are accepted.
TEST(VBucketDurabilityTest, validateSetStateMetaTopology) {
    using nlohmann::json;

    // Single chain, one node
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology", json::array({{"active"}})}}));

    // Single chain, two nodes.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology", json::array({{"active", "replica1"}})}}));

    // Single chain, three nodes.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology",
                        json::array({{"active", "replica1", "replica2"}})}}));

    // Single chain, four nodes.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta({{"topology",
                                              json::array({{"active",
                                                            "replica1",
                                                            "replica2",
                                                            "replica3"}})}}));

    // Single chain, four nodes, two undefined.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology",
                        json::array(
                                {{"active", "replica1", nullptr, nullptr}})}}));

    // Two chains, one node
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology", json::array({{"activeA"}, {"activeB"}})}}));

    // Two chains, two nodes.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta(
                      {{"topology",
                        json::array({{"activeA", "replicaA1"},
                                     {"activeB", "replicaB1"}})}}));

    // Two chains, three nodes.
    EXPECT_EQ(
            ""s,
            VBucket::validateSetStateMeta(
                    {{"topology",
                      json::array({{"activeA", "replicaA1", "replicaA2"},
                                   {"activeB", "replicaB1", "replicaB2"}})}}));

    // Two chains, four nodes.
    EXPECT_EQ(""s,
              VBucket::validateSetStateMeta({{"topology",
                                              json::array({{"activeA",
                                                            "replicaA1",
                                                            "replicaA2",
                                                            "replicaA3"},
                                                           {"activeB",
                                                            "replicaB1",
                                                            "replicaB2",
                                                            "replicaB3"}})}}));

    // Two chains, four nodes, 1 undefined in first; 2 in second.
    EXPECT_EQ(
            ""s,
            VBucket::validateSetStateMeta(
                    {{"topology",
                      json::array(
                              {{"activeA", "replicaA1", "replicaA2", nullptr},
                               {"activeB", "replicaB1", nullptr, nullptr}})}}));
}

TEST(VBucketDurabilityTest, validateSetStateMetaTopologyNegative) {
    using nlohmann::json;
    using testing::HasSubstr;

    // Too few (0) chains (empty json::array)
    EXPECT_THAT(VBucket::validateSetStateMeta({{"topology", json::array({})}}),
                HasSubstr("topology' must contain 1..2 elements"));

    // Too many (>2) chains
    EXPECT_THAT(
            VBucket::validateSetStateMeta(
                    {{"topology",
                      json::array({{"activeA"}, {"activeB"}, {"activeC"}})}}),
            HasSubstr("topology' must contain 1..2 elements"));

    // Two chains, second contains too many (5) nodes.
    EXPECT_THAT(
            VBucket::validateSetStateMeta({{"topology",
                                            json::array({{"active", "replica"},
                                                         {"active",
                                                          "replica1",
                                                          "replica2",
                                                          "replica3",
                                                          "replica4"}})}}),
            HasSubstr("chain[1] must contain 1..4 nodes"));

    // Incorrect structure - flat array not nested.
    EXPECT_THAT(VBucket::validateSetStateMeta(
                        {{"topology", json::array({"activeA", "replica"})}}),
                HasSubstr("chain[0] must be an array"));

    // Incorrect structure - elements are not strings.
    EXPECT_THAT(VBucket::validateSetStateMeta(
                        {{"topology",
                          json::array({{"activeA", "replicaA1"},
                                       {"activeB", 1.1}})}}),
                HasSubstr("chain[1] node[1] must be a string"));

    // Incorrect structure - first node (active) cannot be undefined (null).
    EXPECT_THAT(VBucket::validateSetStateMeta(
                        {{"topology",
                          json::array({{nullptr, "replicaA1"},
                                       {"activeB", "replicaB1"}})}}),
                HasSubstr("chain[0] node[0] (active) cannot be null"));
}

TEST_P(VBucketDurabilityTest, SetVBucketState_ClearTopologyAtReplica) {
    ASSERT_FALSE(vbucket->getReplicationTopology().is_null());
    vbucket->setState(vbucket_state_replica);
    ASSERT_TRUE(vbucket->getReplicationTopology().is_null());
}

TEST_P(VBucketDurabilityTest, MultipleReplicas) {
    const std::string active = "active";
    const std::string replica1 = "replica1";
    const std::string replica2 = "replica2";
    const std::string replica3 = "replica3";

    vbucket->setState(vbucket_state_active,
                      {{"topology",
                        nlohmann::json::array(
                                {{active, replica1, replica2, replica3}})}});

    ASSERT_EQ(1, storeSyncWrites({1} /*seqnos*/));

    auto key = makeStoredDocKey("key1");

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    auto checkPending = [this, &key, &ckptList]() -> void {
        EXPECT_EQ(nullptr, ht->findForRead(key).storedValue);
        const auto sv = ht->findForWrite(key).storedValue;
        EXPECT_NE(nullptr, sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
        EXPECT_EQ(1, ckptList.size());
        ASSERT_EQ(1, ckptList.front()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(CommittedState::Pending, qi->getCommitted());
                EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
            }
        }
    };

    auto checkCommitted = [this, &key, &ckptList]() -> void {
        const auto sv = ht->findForRead(key).storedValue;
        EXPECT_NE(nullptr, sv);
        EXPECT_NE(nullptr, ht->findForWrite(key).storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
        EXPECT_EQ(1, ckptList.size());
        EXPECT_EQ(1, ckptList.front()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(CommittedState::CommittedViaPrepare,
                          qi->getCommitted());
                EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
            }
        }
    };

    // No replica has ack'ed yet
    checkPending();

    // replica2 acks, Durability Requirements not satisfied yet
    vbucket->seqnoAcknowledged(replica2, 1 /*memSeqno*/, 0 /*diskSeqno*/);
    checkPending();

    // replica3 acks, Durability Requirements satisfied
    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear(*vbucket, 0 /*lastBySeqno*/);
    vbucket->seqnoAcknowledged(replica3, 1 /*memSeqno*/, 0 /*diskSeqno*/);
    checkCommitted();
}

TEST_P(VBucketDurabilityTest, PendingSkippedAtEjectionAndCommit) {
    ASSERT_EQ(1, storeSyncWrites({1} /*seqnos*/));

    auto key = makeStoredDocKey("key1");
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // Note: Replica has not ack'ed yet

    // HashTable state:
    // not visible at read
    EXPECT_FALSE(ht->findForRead(key).storedValue);
    // Note: Need to release the HashBucketLock before calling again the
    //     HT::find* functions below, deadlock otherwise
    {
        // visible at write
        auto storedItem = ht->findForWrite(key);
        ASSERT_TRUE(storedItem.storedValue);
        // item pending
        EXPECT_EQ(CommittedState::Pending,
                  storedItem.storedValue->getCommitted());
        // value is resident
        ASSERT_TRUE(storedItem.storedValue->getValue());
        EXPECT_EQ("value", storedItem.storedValue->getValue()->to_s());

        // CheckpointManager state:
        // 1 checkpoint
        ASSERT_EQ(1, ckptList.size());
        // empty-item
        const auto& ckpt = *ckptList.front();
        auto it = ckpt.begin();
        ASSERT_EQ(queue_op::empty, (*it)->getOperation());
        // 1 metaitem (checkpoint-start)
        it++;
        ASSERT_EQ(1, ckpt.getNumMetaItems());
        EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
        // 1 non-metaitem is pending and contains the expected value
        it++;
        ASSERT_EQ(1, ckpt.getNumItems());
        EXPECT_EQ(CommittedState::Pending, (*it)->getCommitted());
        EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
        EXPECT_EQ("value", (*it)->getValue()->to_s());

        // Need to clear the dirty flag to ensure that we are testing the right
        // thing, i.e. that the item is not ejected because it is Pending (not
        // because it is dirty).
        storedItem.storedValue->markClean();
        ASSERT_FALSE(ht->unlocked_ejectItem(storedItem.lock,
                                            storedItem.storedValue,
                                            GetParam() /*ejection-policy*/));
    }

    // HashTable state:
    // not visible at read
    EXPECT_FALSE(ht->findForRead(key).storedValue);
    // visible at write
    const auto* sv = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(sv);
    // item pending
    EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
    // value is still resident
    EXPECT_TRUE(sv->getValue());

    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear(*vbucket, 0 /*seqno*/);

    // Replica acks, Durability Requirements satisfied, Commit
    vbucket->seqnoAcknowledged(replica, 1 /*memSeqno*/, 0 /*diskSeqno*/);

    // HashTable state:
    // visible at read
    sv = ht->findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    // visible at write
    EXPECT_TRUE(ht->findForWrite(key).storedValue);
    // still pending
    EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
    // value is resident
    EXPECT_TRUE(sv->getValue());
    EXPECT_EQ("value", sv->getValue()->to_s());

    // CheckpointManager state:
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    // empty-item
    const auto& ckpt = *ckptList.front();
    auto it = ckpt.begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt.getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is committed and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt.getNumItems());
    EXPECT_EQ(CommittedState::CommittedViaPrepare, (*it)->getCommitted());
    EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
    EXPECT_EQ("value", (*it)->getValue()->to_s());
}

TEST_P(VBucketDurabilityTest, NonExistingKeyAtAbort) {
    auto noentKey = makeStoredDocKey("non-existing-key");
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              vbucket->abort(noentKey,
                             0 /*prepareSeqno*/,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(noentKey)));
}

TEST_P(VBucketDurabilityTest, NonPendingKeyAtAbort) {
    auto nonPendingKey = makeStoredDocKey("key1");
    auto nonPendingItem = make_item(vbucket->getId(), nonPendingKey, "value");
    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(nonPendingItem, 0 /*cas*/, VBQueueItemCtx()));
    EXPECT_EQ(1, ht->getNumItems());
    // Visible at read
    const auto* sv = ht->findForRead(nonPendingKey).storedValue;
    ASSERT_TRUE(sv);
    const int64_t bySeqno = 1;
    ASSERT_EQ(bySeqno, sv->getBySeqno());
    EXPECT_EQ(ENGINE_EINVAL,
              vbucket->abort(nonPendingKey,
                             bySeqno /*prepareSeqno*/,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(nonPendingKey)));
}

/*
 * This test checks that at abort:
 * 1) the Pending is removed from the HashTable
 * 2) a queue_op::abort_sync_write item is enqueued into the CheckpointManager
 * 3) the abort_sync_write is not added to the DurabilityMonitor
 */
TEST_P(VBucketDurabilityTest, AbortSyncWrite_Active) {
    ASSERT_EQ(1, storeSyncWrites({1} /*seqnos*/));

    auto key = makeStoredDocKey("key1");

    EXPECT_EQ(1, ht->getNumItems());
    // Not visible at read
    EXPECT_FALSE(ht->findForRead(key).storedValue);
    // Note: Need to release the HashBucketLock before calling VBucket::abort
    //     (which acquires the same HBL), deadlock otherwise
    {
        // Visible at write
        auto storedItem = ht->findForWrite(key);
        ASSERT_TRUE(storedItem.storedValue);
        // item pending
        EXPECT_EQ(CommittedState::Pending,
                  storedItem.storedValue->getCommitted());
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // CheckpointManager state:
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    // empty-item
    const auto* ckpt = ckptList.front().get();
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(CommittedState::Pending, (*it)->getCommitted());
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ("value", (*it)->getValue()->to_s());

    // The Pending is tracked by the DurabilityMonitor
    EXPECT_EQ(1, monitor->public_getNumTracked());

    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear(*vbucket, 0 /*seqno*/);

    // Note: abort-seqno must be provided only at Replica
    EXPECT_EQ(ENGINE_SUCCESS,
              vbucket->abort(key,
                             1 /*prepareSeqno*/,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(key)));

    // StoredValue has gone
    EXPECT_EQ(0, ht->getNumItems());
    EXPECT_FALSE(ht->findForRead(key).storedValue);
    EXPECT_FALSE(ht->findForWrite(key).storedValue);

    // CheckpointManager state:
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    // empty-item
    ckpt = ckptList.front().get();
    it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is a deleted durable-abort item with no value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_TRUE((*it)->isDeleted());
    EXPECT_FALSE((*it)->getValue());

    // The Aborted item is not added for tracking.
    // Note: The Pending has not been removed as we are testing at VBucket
    //     level, so num-tracked must be still 1.
    EXPECT_EQ(1, monitor->public_getNumTracked());
}

/*
 * Base multi-frontend-thread test for durable writes.
 * The test spawns 2 threads doing set-durable on a number of writes.
 * The aim of this test is to expose any VBucket/DurabilityMonitor
 * synchronization issue, hopefully caught by TSan at commit validation.
 *
 * This test is initially introduced for MB-33298, where we could end up with
 * queueing out-of-order seqnos into the DurabilityMonitor.
 * Note that for MB-33298:
 * - The kind of synchronization issue is not caught by TSan as it is
 *     logic-synchronization problem (details in the MB)
 * - The test is not deterministic for the specific case. The number of items is
 *     chosen such that the test always fails before the fix on local OSX, but
 *     there may be sporadic false negatives.
 *     In the specific case the problem is that, while exposing the original
 *     issue in a deterministic test is easy, after some trials it seems not
 *     feasible to have a deterministic test that fails before the fix and
 *     succeeds at fix. That is because the synchronization changes necessary
 *     at fix invalidate the test.
 */
TEST_P(VBucketDurabilityTest, ParallelSet) {
    const auto numThreads = 4;
    ThreadGate tg(numThreads);
    const auto threadLoad = 1000;

    auto load = [this, &tg, threadLoad](const std::string& prefix) -> void {
        tg.threadUp();
        for (auto seqno = 1; seqno <= threadLoad; seqno++) {
            auto item = Item(makeStoredDocKey(prefix + std::to_string(seqno)),
                             0 /*flags*/,
                             0 /*exp*/,
                             "value",
                             5 /*valueSize*/,
                             PROTOCOL_BINARY_RAW_BYTES);
            using namespace cb::durability;
            item.setPendingSyncWrite(Requirements());
            VBQueueItemCtx ctx;
            ctx.durability = DurabilityItemCtx{item.getDurabilityReqs(),
                                               nullptr /*cookie*/};

            EXPECT_EQ(MutationStatus::WasClean,
                      public_processSet(item, 0 /*cas*/, ctx));
        }
    };

    // Note: The use of key-prefix is to ensure that (most of) the loaded keys
    //     fall into different HastTable partitions.
    //     That way, front-end threads do not synchronize on HashBucketLock at
    //     VBucket::queueDirty, so the test stresses the internal queueDirty
    //     synchronization.
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; i++) {
        threads.push_back(
                std::thread(load, "key" + std::to_string(i) + "_" /*prefix*/));
    }
    for (auto& t : threads) {
        t.join();
    }
}
