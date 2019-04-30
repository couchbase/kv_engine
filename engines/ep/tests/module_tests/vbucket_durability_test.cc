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
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "vbucket_utils.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_ephemeral_vb.h"

#include <folly/portability/GMock.h>
#include <thread>

using namespace std::string_literals;

void VBucketDurabilityTest::SetUp() {
    ht = &vbucket->ht;
    ckptMgr = static_cast<MockCheckpointManager*>(
            vbucket->checkpointManager.get());
    vbucket->setState(
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{active, replica1}})}});
    ASSERT_EQ(2, vbucket->getActiveDM().getFirstChainSize());
}

void VBucketDurabilityTest::storeSyncWrites(
        const std::vector<SyncWriteSpec>& seqnos) {
    if (seqnos.empty()) {
        throw std::logic_error(
                "VBucketDurabilityTest::storeSyncWrites: seqnos list is empty");
    }

    // @todo: For now this function is supposed to be called once per test,
    //     expand if necessary
    ht->clear();
    ckptMgr->clear(*vbucket, 0);

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

    const auto preHTCount = ht->getNumItems();
    const auto preCMCount = ckptMgr->getNumItems();
    for (const auto& write : seqnos) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));
        // Use a Level::Majority with an Infinite timeout - these tests
        // don't rely on specific timeout values.
        using namespace cb::durability;
        auto reqs = Requirements{Level::Majority, Timeout::Infinity()};
        auto item = makePendingItem(key, "value", reqs);
        item->setBySeqno(write.seqno);
        if (write.deletion) {
            item->setDeleted();
        }
        if (vbucket->getState() != vbucket_state_active) {
            // Non-active Vbs have prepares added as "MaybeVisible"
            item->setPreparedMaybeVisible();
        }

        VBQueueItemCtx ctx;
        ctx.genBySeqno = GenerateBySeqno::No;
        ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(), cookie};
        ASSERT_EQ(MutationStatus::WasClean,
                  public_processSet(*item, 0 /*cas*/, ctx));
    }
    EXPECT_EQ(preHTCount + seqnos.size(), ht->getNumItems());
    EXPECT_EQ(preCMCount + seqnos.size(), ckptMgr->getNumItems());
}

void VBucketDurabilityTest::simulateLocalAck(uint64_t seqno) {
    vbucket->setPersistenceSeqno(seqno);
    vbucket->getActiveDM().notifyLocalPersistence();
}

void VBucketDurabilityTest::testAddPrepare(
        const std::vector<SyncWriteSpec>& writes) {
    {
        SCOPED_TRACE("");
        storeSyncWrites(writes);
    }

    for (auto write : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));

        EXPECT_EQ(nullptr, ht->findOnlyCommitted(key).storedValue);
        const auto sv = ht->findForWrite(key).storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_TRUE(sv->isPending());
        EXPECT_EQ(write.deletion, sv->isDeleted());
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    EXPECT_EQ(writes.size(), ckptList.front()->getNumItems());
    for (const auto& qi : *ckptList.front()) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
        }
    }
}

void VBucketDurabilityTest::testAddPrepareAndCommit(
        const std::vector<SyncWriteSpec>& writes) {
    testAddPrepare(writes);

    std::vector<uint64_t> cas;
    for (auto write : writes) {
        cas.push_back(
                ht->findForWrite(
                          makeStoredDocKey("key" + std::to_string(write.seqno)))
                        .storedValue->getCas());
    }

    // Simulate flush + checkpoint-removal
    ckptMgr->clear(*vbucket, ckptMgr->getHighSeqno());

    // Simulate replica and active seqno-ack
    vbucket->seqnoAcknowledged(replica1, writes.back().seqno);
    simulateLocalAck(writes.back().seqno);

    int i = 0;
    for (auto write : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));

        const auto sv =
                ht->findForRead(key, TrackReference::Yes, WantsDeleted::Yes)
                        .storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_NE(nullptr, ht->findForWrite(key).storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
        EXPECT_EQ(write.deletion, sv->isDeleted());
        EXPECT_EQ(cas[i], sv->getCas());
        i++;
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    EXPECT_EQ(writes.size(), ckptList.front()->getNumItems());
    for (const auto& qi : *ckptList.front()) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
        }
    }
}

TEST_P(VBucketDurabilityTest, Active_AddPrepareAndCommit_ContinuousSeqnos) {
    testAddPrepareAndCommit({1, 2, 3});
}

TEST_P(VBucketDurabilityTest,
       Active_AddPrepareAndCommit_ContinuousDeleteSeqnos) {
    testAddPrepareAndCommit({{1, true}, {2, true}, {3, true}});
}

TEST_P(VBucketDurabilityTest, Active_AddPrepareAndCommit_SparseSeqnos) {
    testAddPrepareAndCommit({1, 3, 10, 20, 30});
}

TEST_P(VBucketDurabilityTest, Active_AddPrepareAndCommit_SparseDeleteSeqnos) {
    testAddPrepareAndCommit(
            {{1, true}, {3, true}, {10, true}, {20, true}, {30, true}});
}

// Mix of Mutations and Deletions.
TEST_P(VBucketDurabilityTest, Active_AddPrepareAndCommit_SparseMixedSeqnos) {
    testAddPrepareAndCommit({{1, true}, 3, {10, true}, {20, true}, 30});
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(
        AllVBTypesAllEvictionModes,
        VBucketDurabilityTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_CASE_P(
        Ephemeral,
        EphemeralVBucketDurabilityTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        EPVBucketDurabilityTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

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

void VBucketDurabilityTest::testSetVBucketState_ClearTopology(
        vbucket_state_t state) {
    ASSERT_NE(nlohmann::json{}.dump(),
              vbucket->getReplicationTopology().dump());

    vbucket->setState(state);

    EXPECT_EQ(nlohmann::json{}.dump(),
              vbucket->getReplicationTopology().dump());
}

TEST_P(VBucketDurabilityTest, Replica_SetVBucketState_ClearTopology) {
    testSetVBucketState_ClearTopology(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest, Pending_SetVBucketState_ClearTopology) {
    testSetVBucketState_ClearTopology(vbucket_state_pending);
}

/**
 * Test that when the vbucket is active and topology changes (but is still
 * active) that existing tracked SyncWrites are not discarded.
 */
TEST_P(VBucketDurabilityTest, ActiveActive_SetVBucketState_KeepsTrackedWrites) {
    auto& monitor = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    storeSyncWrites({10, 20});
    ASSERT_EQ(2, monitor.getNumTracked());

    vbucket->setState(
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{active, replica1, replica2}})}});

    auto& monitor2 = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(2, monitor2.getNumTracked());
    EXPECT_EQ(20, monitor2.getHighPreparedSeqno());
}

/**
 * Test that when the vbucket is replica and changes to pending that existing
 * tracked SyncWrites are not discarded.
 */
TEST_P(VBucketDurabilityTest,
       ReplicaPending_SetVBucketState_KeepsTrackedWrites) {
    vbucket->setState(vbucket_state_replica);
    const auto& monitor =
            VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    storeSyncWrites({10, 20});
    ASSERT_EQ(2, monitor.getNumTracked());
    ASSERT_EQ(0, monitor.getHighPreparedSeqno());
    // Note: simulating a snapshot:[10, 30]
    vbucket->notifyPassiveDMOfSnapEndReceived(30 /*snapEnd*/);
    EXPECT_EQ(20, monitor.getHighPreparedSeqno());

    vbucket->setState(vbucket_state_pending);

    auto& monitor2 = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    EXPECT_EQ(2, monitor2.getNumTracked());
    EXPECT_EQ(20, monitor2.getHighPreparedSeqno());

    vbucket->setState(vbucket_state_replica);

    auto& monitor3 = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    EXPECT_EQ(2, monitor3.getNumTracked());
    EXPECT_EQ(20, monitor3.getHighPreparedSeqno());
}

TEST_P(VBucketDurabilityTest, Active_Commit_MultipleReplicas) {
    auto& monitor = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    monitor.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}}));
    ASSERT_EQ(4, monitor.getFirstChainSize());

    const int64_t preparedSeqno = 1;
    storeSyncWrites({preparedSeqno});
    ASSERT_EQ(1, monitor.getNumTracked());

    auto key = makeStoredDocKey("key1");

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    auto checkPending = [this, &key, &ckptList]() -> void {
        EXPECT_EQ(nullptr, ht->findForRead(key).storedValue);
        const auto sv = ht->findForWrite(key).storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
        EXPECT_EQ(1, ckptList.size());
        ASSERT_EQ(1, ckptList.front()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
            }
        }
    };

    auto checkCommitted = [this, &key, &ckptList]() -> void {
        const auto sv = ht->findForRead(key).storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_NE(nullptr, ht->findForWrite(key).storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
        EXPECT_EQ(1, ckptList.size());
        EXPECT_EQ(1, ckptList.front()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
            }
        }
    };

    // Simulate active seqno-ack
    simulateLocalAck(preparedSeqno);

    // No replica has ack'ed yet
    checkPending();

    // replica2 acks, Durability Requirements not satisfied yet
    vbucket->seqnoAcknowledged(replica2, preparedSeqno);
    checkPending();

    // replica3 acks, Durability Requirements satisfied
    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear(*vbucket, ckptMgr->getHighSeqno());
    vbucket->seqnoAcknowledged(replica3, preparedSeqno);
    checkCommitted();
}

TEST_P(VBucketDurabilityTest, Active_PendingSkippedAtEjectionAndCommit) {
    const int64_t preparedSeqno = 1;
    storeSyncWrites({preparedSeqno});
    ASSERT_EQ(1,
              VBucketTestIntrospector::public_getActiveDM(*vbucket)
                      .getNumTracked());

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
        EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
        EXPECT_EQ("value", (*it)->getValue()->to_s());

        // Need to clear the dirty flag to ensure that we are testing the right
        // thing, i.e. that the item is not ejected because it is Pending (not
        // because it is dirty).
        storedItem.storedValue->markClean();
        ASSERT_FALSE(ht->unlocked_ejectItem(
                storedItem.lock, storedItem.storedValue, getEvictionPolicy()));
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
    ckptMgr->clear(*vbucket, ckptMgr->getHighSeqno());

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(0 /*count*/, nullptr, ENGINE_EINVAL),
              swCompleteTrace);

    // Simulate replica and active seqno-ack
    vbucket->seqnoAcknowledged(replica1, preparedSeqno);
    simulateLocalAck(preparedSeqno);

    // Commit notified
    EXPECT_EQ(SWCompleteTrace(1 /*count*/, cookie, ENGINE_SUCCESS),
              swCompleteTrace);

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
    EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
    EXPECT_EQ("value", (*it)->getValue()->to_s());
}

TEST_P(VBucketDurabilityTest, NonExistingKeyAtAbort) {
    auto noentKey = makeStoredDocKey("non-existing-key");
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              vbucket->abort(noentKey,
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
    const int64_t bySeqno = 1001;
    ASSERT_EQ(bySeqno, sv->getBySeqno());
    EXPECT_EQ(ENGINE_EINVAL,
              vbucket->abort(nonPendingKey,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(nonPendingKey)));
}

/*
 * This test checks that at abort:
 * 1) the Pending is removed from the HashTable
 * 2) a queue_op::abort_sync_write item is enqueued into the CheckpointManager
 * 3) the abort_sync_write is not added to the DurabilityMonitor
 */
TEST_P(VBucketDurabilityTest, Active_AbortSyncWrite) {
    if (getVbType() == VBType::Ephemeral) {
        // @todo-durability: Implement abort.
        return;
    }
    storeSyncWrites({1} /*seqno*/);
    ASSERT_EQ(1,
              VBucketTestIntrospector::public_getActiveDM(*vbucket)
                      .getNumTracked());

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
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ("value", (*it)->getValue()->to_s());

    // The Pending is tracked by the DurabilityMonitor
    const auto& monitor = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(1, monitor.getNumTracked());

    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear(*vbucket, ckptMgr->getHighSeqno());

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(0 /*count*/, nullptr, ENGINE_EINVAL),
              swCompleteTrace);

    // Note: abort-seqno must be provided only at Replica
    EXPECT_EQ(ENGINE_SUCCESS,
              vbucket->abort(key,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(key),
                             cookie));

    // Abort notified
    EXPECT_EQ(SWCompleteTrace(1 /*count*/, cookie, ENGINE_SYNC_WRITE_AMBIGUOUS),
              swCompleteTrace);

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
    EXPECT_EQ(1, monitor.getNumTracked());
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
TEST_P(VBucketDurabilityTest, Active_ParallelSet) {
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

void VBucketDurabilityTest::testAddPrepareInPassiveDM(
        vbucket_state_t state, const std::vector<SyncWriteSpec>& writes) {
    vbucket->setState(state);
    const auto& monitor =
            VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, monitor.getNumTracked());
    testAddPrepare(writes);
    ASSERT_EQ(writes.size(), monitor.getNumTracked());
}

TEST_P(VBucketDurabilityTest, Replica_AddPrepareInPassiveDM) {
    testAddPrepareInPassiveDM(vbucket_state_replica, {1, 2, 3} /*seqnos*/);
}

TEST_P(VBucketDurabilityTest, Pending_AddPrepareInPassiveDM) {
    testAddPrepareInPassiveDM(vbucket_state_pending, {1, 2, 3} /*seqnos*/);
}

// Test adding a pending item and then committing it.
TEST_P(VBucketDurabilityTest, Commit) {
    auto key = makeStoredDocKey("key1");
    storeSyncWrites({1});

    // Check preconditions - pending item should be found as pending.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());

    // Test
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // Check postconditions - should only have one item for that key.
    auto readView = ht->findForRead(key).storedValue;
    auto writeView = ht->findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
    EXPECT_EQ(*readView, *writeView);
}

void VBucketDurabilityTest::testHTCommitExisting() {
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0, {}));
    ASSERT_EQ(1, ht->getNumItems());

    auto pending = makePendingItem(key, "valueB"s);
    VBQueueItemCtx ctx;
    ctx.durability = DurabilityItemCtx{pending->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*pending, 0, ctx));

    ASSERT_EQ(2, ht->getNumItems());

    // Check preconditions - item should be found as pending.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());

    // Test
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // Check postconditions - should only have one item for that key.
    auto readView = ht->findForRead(key).storedValue;
    auto writeView = ht->findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);

    EXPECT_EQ(1, ht->getNumItems());

    // Should be CommittedViaPrepare
    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
}

TEST_P(EPVBucketDurabilityTest, CommitExisting) {
    testHTCommitExisting();
}

TEST_P(EphemeralVBucketDurabilityTest, CommitExisting) {
    testHTCommitExisting();
    // Check that we have the expected items in the seqList.
    // 1 stale prepare
    // 2 items total (prepare + commit)
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());

    // Do a purge of the stale items and check result
    EXPECT_EQ(1, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(1, mockEphVb->public_getNumListItems());
}

TEST_P(EphemeralVBucketDurabilityTest, CommitExisting_RangeRead) {
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    mockEphVb->registerFakeReadRange(0, 1000);

    testHTCommitExisting();

    // Check that we have the expected items in the seqList.
    // 1 stale commit via mutation (because of the range read)
    // 1 stale prepare
    // 1 commit via prepare
    EXPECT_EQ(2, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(3, mockEphVb->public_getNumListItems());

    // Do a purge of the stale items and check result
    EXPECT_EQ(2, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(1, mockEphVb->public_getNumListItems());
}

// The test case doesn't really test anything new, it just demonstrates how
// prepares work in regards to the StaleItemDeleter.
TEST_P(EphemeralVBucketDurabilityTest, PrepareOnCommitted) {
    auto key = makeStoredDocKey("key1");
    auto item = makePendingItem(key, "value");
    VBQueueItemCtx ctx;
    ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*item, 0 /*cas*/, ctx));

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());

    // Test
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());

    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*item, 0 /*cas*/, ctx));

    // We have 1 stale item (the old prepare)
    // 2 non-stale items (committed + new prepare)
    EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(3, mockEphVb->public_getNumListItems());

    // Do a purge of the stale items and check result
    EXPECT_EQ(1, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

TEST_P(EPVBucketDurabilityTest, SyncDeleteCommit) {
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0, {}));

    // Do a SyncDelete
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{{cb::durability::Level::Majority, {}}, cookie};
    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSoftDelete(key, ctx).first);

    // Test - commit the pending SyncDelete.
    result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // Check postconditions:
    // 1. Upon commit, both read and write view should show same deleted item.
    auto* readView =
            ht->findForRead(key, TrackReference::Yes, WantsDeleted::Yes)
                    .storedValue;
    ASSERT_TRUE(readView);
    auto* writeView = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);

    EXPECT_EQ(readView, writeView);
    EXPECT_TRUE(readView->isDeleted());
    EXPECT_FALSE(readView->getValue());

    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());

    // Should currently have 1 item:
    EXPECT_EQ(1, ht->getNumItems());
}

// Negative test - check it is not possible to commit a non-pending item.
TEST_P(VBucketDurabilityTest, CommitNonPendingFails) {
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0, {}));

    // Check preconditions - item should be found as committed.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::CommittedViaMutation, result->getCommitted());

    // Test
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));
}

// Test that a normal set after a Committed SyncWrite is allowed and handled
// correctly.
TEST_P(VBucketDurabilityTest, MutationAfterCommit) {
    // Setup - Commit a SyncWrite into the HashTable.
    auto key = makeStoredDocKey("key1");
    storeSyncWrites({1});
    ASSERT_EQ(1, ht->getNumItems());

    // Check preconditions - item should be found as pending.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    auto readView = ht->findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    ASSERT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());

    // Test - attempt to update with a normal Mutation (should be allowed).
    auto committed = makeCommittedItem(key, "mutation"s);
    ASSERT_EQ(MutationStatus::WasDirty, public_processSet(*committed, 0, {}));

    // Check postconditions
    // 1. Should only have 1 item (and should be same)
    readView = ht->findForRead(key).storedValue;
    auto writeView = ht->findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);

    // Should be CommittedViaMutation
    EXPECT_EQ(CommittedState::CommittedViaMutation, readView->getCommitted());
}

/// Store a prepared SyncWrite, commit it and check counts.
TEST_P(VBucketDurabilityTest, StatsCommittedSyncWrite) {
    // Setup
    auto key = makeStoredDocKey("key");
    auto prepared = makePendingItem(key, "valueB"s);
    VBQueueItemCtx ctx;
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*prepared, 0, ctx));

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // Test
    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht->getNumItems());
    EXPECT_EQ(1, ht->getDatatypeCounts()[prepared->getDataType()]);
}

/// Store a prepared SyncDelete, commit it and check counts.
TEST_P(VBucketDurabilityTest, StatsCommittedSyncDelete) {
    // Setup
    auto key = makeStoredDocKey("key");
    auto prepared = makePendingItem(key, "prepared");
    prepared->setDeleted(DeleteSource::Explicit);
    VBQueueItemCtx ctx;
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*prepared, 0, ctx));

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // Test
    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht->getNumItems());
    EXPECT_EQ(1, ht->getNumDeletedItems());
    for (const auto& count : ht->getDatatypeCounts()) {
        EXPECT_EQ(0, count);
    }
}

// Test case for doing a sync write on top of a pre-existing item.
// Stats should be tracked as follows:
// 1) Pre-existing item created (no item -> committed item)
// 2) Pending sync write created (no pending sw for item -> pending sw for item)
// 3) Commit sync write (committed item -> [removed] + pending sw -> committed)
TEST_P(VBucketDurabilityTest, StatsCommittedSyncWritePreExisting) {
    // Setup - set a pre-existing item
    auto key = makeStoredDocKey("key");
    StoredDocKey existing = makeStoredDocKey("existing");
    auto item = makeCommittedItem(key, "value");
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*item, 0, {}));

    ASSERT_EQ(0, ht->getNumPreparedSyncWrites());

    // Setup - prepare a sync write
    auto prepared = makePendingItem(key, "prepared");
    VBQueueItemCtx ctx;
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*prepared, 0, ctx));
    ASSERT_EQ(1, ht->getNumPreparedSyncWrites());

    // Commit the sync write
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);

    // We should not throw due to underflow (if assertions are turned on)
    EXPECT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key, {}, vbucket->lockCollections(key)));

    // We should no longer have the prepared sync write (will only fail if
    // assertions are turned off)
    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
}

void VBucketDurabilityTest::testConvertPassiveDMToActiveDM(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    vbucket->setState(initialState);

    // Queue some Prepares into the PDM
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2, 3};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), pdm.getNumTracked());

    // VBState transitions from Replica to Active
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active, replica1}})}});
    vbucket->setState(vbucket_state_active, topology);
    // The old PDM is an instance of ADM now. All Prepares are retained.
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(seqnos.size(), adm.getNumTracked());
    EXPECT_EQ(std::unordered_set<int64_t>({1, 2, 3}), adm.getTrackedSeqnos());

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(0 /*count*/, nullptr, ENGINE_EINVAL),
              swCompleteTrace);

    // Check that any attempts to read keys which are have Prepared SyncWrites
    // against them fail with SyncWriteReCommitting (until they are committed).
    for (const auto& spec : seqnos) {
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(WantsDeleted::No,
                                               TrackReference::No,
                                               QueueExpired::No,
                                               vbucket->lockCollections(key));
        ASSERT_TRUE(result.storedValue);
        EXPECT_TRUE(result.storedValue->isPreparedMaybeVisible());
    }

    // Check that the SyncWrite journey now proceeds to completion as expected
    ASSERT_EQ(seqnos.back().seqno, adm.getHighPreparedSeqno());
    ASSERT_EQ(0, adm.getNodeWriteSeqno(replica1));
    size_t expectedNumTracked = seqnos.size();
    for (const auto s : seqnos) {
        adm.seqnoAckReceived(replica1, s.seqno);
        EXPECT_EQ(--expectedNumTracked, adm.getNumTracked());
        // Nothing to notify, we don't know anything about the oldADM->client
        // connection.
        EXPECT_EQ(SWCompleteTrace(0 /*count*/, nullptr, ENGINE_EINVAL),
                  swCompleteTrace);
    }

    // After commit() check that the keys are now accessible and appear as
    // committed.
    for (const auto& spec : seqnos) {
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(WantsDeleted::No,
                                               TrackReference::No,
                                               QueueExpired::No,
                                               vbucket->lockCollections(key));
        ASSERT_TRUE(result.storedValue);
        EXPECT_TRUE(result.storedValue->isCommitted());
    }
}

TEST_P(VBucketDurabilityTest, Replica_ConvertPassiveDMToActiveDM) {
    testConvertPassiveDMToActiveDM(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest, Pending_ConvertPassiveDMToActiveDM) {
    testConvertPassiveDMToActiveDM(vbucket_state_pending);
}

TEST_P(VBucketDurabilityTest, IgnoreAckAtTakeoverDead) {
    // Queue some Prepares into the PDM
    const auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    ASSERT_EQ(0, adm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2, 3};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), adm.getNumTracked());

    // VBState transitions from Replica to Active
    vbucket->setState(vbucket_state_dead, nlohmann::json{});

    EXPECT_EQ(ENGINE_SUCCESS, vbucket->seqnoAcknowledged("replica1", 3));
    // We won't have crashed and we will have ignored the ack
    EXPECT_EQ(seqnos.size(), adm.getNumTracked());
}

void VBucketDurabilityTest::testCompleteSWInPassiveDM(vbucket_state_t state,
                                                      Resolution res) {
    const std::vector<SyncWriteSpec>& writes{1, 2, 3}; // seqnos
    testAddPrepareInPassiveDM(state, writes);
    ASSERT_EQ(writes.size(), vbucket->durabilityMonitor->getNumTracked());

    // This is just for an easier inspection of the CheckpointManager below
    ckptMgr->clear(*vbucket, ckptMgr->getHighSeqno());

    // Commit all + check HT
    // Note: At replica, snapshots are defined at PassiveStream level, need to
    //     do it manually here given that we are testing at VBucket level.
    ckptMgr->createSnapshot(writes.back().seqno + 1, writes.back().seqno + 100);
    for (auto write : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));

        switch (res) {
        case Resolution::Commit: {
            EXPECT_EQ(ENGINE_SUCCESS,
                      vbucket->commit(key,
                                      write.seqno + 10 /*commitSeqno*/,
                                      vbucket->lockCollections(key)));

            const auto sv = ht->findForRead(key).storedValue;
            EXPECT_TRUE(sv);
            EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
            EXPECT_TRUE(ht->findForWrite(key).storedValue);

            break;
        }
        case Resolution::Abort: {
            EXPECT_EQ(ENGINE_SUCCESS,
                      vbucket->abort(key,
                                     write.seqno + 10 /*abortSeqno*/,
                                     vbucket->lockCollections(key)));

            EXPECT_FALSE(ht->findForRead(key).storedValue);
            EXPECT_FALSE(ht->findForWrite(key).storedValue);

            break;
        }
        }
    }

    // Check CM
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    EXPECT_EQ(writes.size(), ckptList.front()->getNumItems());
    const auto expectedOp =
            (res == Resolution::Commit ? queue_op::commit_sync_write
                                       : queue_op::abort_sync_write);
    for (const auto& qi : *ckptList.front()) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(expectedOp, qi->getOperation());
        }
    }

    // Check DM
    EXPECT_EQ(0, vbucket->durabilityMonitor->getNumTracked());
}

TEST_P(VBucketDurabilityTest, Replica_Commit) {
    testCompleteSWInPassiveDM(vbucket_state_replica, Resolution::Commit);
}

TEST_P(VBucketDurabilityTest, Pending_Commit) {
    testCompleteSWInPassiveDM(vbucket_state_pending, Resolution::Commit);
}

TEST_P(EPVBucketDurabilityTest, Replica_Abort) {
    testCompleteSWInPassiveDM(vbucket_state_replica, Resolution::Abort);
}

TEST_P(EPVBucketDurabilityTest, Pending_Abort) {
    testCompleteSWInPassiveDM(vbucket_state_pending, Resolution::Abort);
}
