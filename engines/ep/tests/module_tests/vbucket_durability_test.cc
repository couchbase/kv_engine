/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucket_durability_test.h"

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "collections/vbucket_manifest_handles.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "kvstore/persistence_callback.h"
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
    auto meta = nlohmann::json{
            {"topology", nlohmann::json::array({{active, replica1}})}};
    vbucket->setState(vbucket_state_active, &meta);
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
    ckptMgr->clear(0);

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
    ckptMgr->createSnapshot(seqnos.front().seqno,
                            seqnos.back().seqno,
                            {} /*HCS*/,
                            CheckpointType::Memory,
                            0);

    const auto preHTCount = ht->getNumItems();
    const auto preCMCount = ckptMgr->getNumItems();
    for (const auto& write : seqnos) {
        auto key = makeStoredDocKey("key" + std::to_string(write.seqno));
        using namespace cb::durability;
        auto reqs = Requirements{write.level, write.timeout};
        auto item = makePendingItem(key, "value", reqs);
        item->setBySeqno(write.seqno);
        if (write.deletion) {
            item->setDeleted();
        }
        if (vbucket->getState() != vbucket_state_active) {
            // Non-active Vbs have prepares added as "MaybeVisible"
            item->setPreparedMaybeVisible();
        }

        VBQueueItemCtx ctx{CanDeduplicate::Yes};
        ctx.genBySeqno = GenerateBySeqno::No;
        ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(), cookie};
        ASSERT_EQ(MutationStatus::WasClean,
                  public_processSet(*item, 0 /*cas*/, ctx));
    }
    EXPECT_EQ(preHTCount + seqnos.size(), ht->getNumItems());
    EXPECT_EQ(preCMCount + seqnos.size(), ckptMgr->getNumItems());
}

void VBucketDurabilityTest::simulateSetVBState(vbucket_state_t to,
                                               const nlohmann::json& meta) {
    auto* metaPtr = meta.empty() ? nullptr : &meta;
    vbucket->setState(to, metaPtr);
    vbucket->processResolvedSyncWrites();
}

void VBucketDurabilityTest::simulateLocalAck(uint64_t seqno) {
    vbucket->setPersistenceSeqno(seqno);
    vbucket->notifyPersistenceToDurabilityMonitor();
    vbucket->processResolvedSyncWrites();
}

void VBucketDurabilityTest::testAddPrepare(
        const std::vector<SyncWriteSpec>& writes) {
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    const auto initialNumItems = ckptList.back()->getNumItems();

    {
        CB_SCOPED_TRACE("");
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

    EXPECT_EQ(initialNumItems + writes.size(), ckptList.back()->getNumItems());

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
    ckptMgr->clear();

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);
    const auto initialNumItems = ckptList.front()->getNumItems();

    // Simulate replica and active seqno-ack
    vbucket->seqnoAcknowledged(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            replica1,
            writes.back().seqno);
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

    ASSERT_EQ(1, ckptMgr->getNumCheckpoints());
    EXPECT_EQ(initialNumItems + writes.size(), ckptList.front()->getNumItems());
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

TEST_P(VBucketDurabilityTest, CommitSyncWriteThenWriteToSameKey) {
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "valueB");

    using namespace cb::durability;
    auto reqs = Requirements{Level::Majority, Timeout::Infinity()};
    pending->setPendingSyncWrite(reqs);

    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{reqs, cookie};

    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    auto item = makeCommittedItem(key, "value");
    ctx.durability.reset();
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSet(*item, 0 /*cas*/, ctx));

    {
        std::shared_lock rlh(vbucket->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  lastSeqno + 1,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key),
                                  cookie));
    }

    // Do a normal mutation
    EXPECT_EQ(MutationStatus::WasDirty,
              public_processSet(*item, 0 /*cas*/, ctx));

    // And another prepare
    ctx.durability = DurabilityItemCtx{reqs, cookie};
    auto status = getVbType() == VBType::Persistent ? MutationStatus::WasClean
                                                    : MutationStatus::WasDirty;
    EXPECT_EQ(status, public_processSet(*pending, 0 /*cas*/, ctx));
}

TEST_P(VBucketDurabilityTest, CommitSyncWriteLoop) {
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "valueB");

    using namespace cb::durability;
    auto reqs = Requirements{Level::Majority, Timeout::Infinity()};
    pending->setPendingSyncWrite(reqs);

    // Do 3 iterations. Why? The 1st and 2nd iterations take different paths
    // (add vs update) so we want to verify everything is correct with the 3rd.
    for (int i = 0; i < 3; i++) {
        VBQueueItemCtx ctx{CanDeduplicate::Yes};
        ctx.durability = DurabilityItemCtx{reqs, cookie};

        // Do the prepare (should be clean/dirty)
        ASSERT_NE(MutationStatus::IsPendingSyncWrite,
                  public_processSet(*pending, 0 /*cas*/, ctx));
        auto item = makeCommittedItem(key, "value" + std::to_string(i));

        // Check that we block normal set
        ctx.durability.reset();
        ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
                  public_processSet(*item, 0 /*cas*/, ctx));

        std::shared_lock rlh(vbucket->getStateLock());
        // And commit
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  lastSeqno + i * 2 + 1,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key),
                                  cookie));
    }
}

TEST_P(VBucketDurabilityTest, AbortSyncWriteLoop) {
    ht->clear();
    ckptMgr->clear(0);
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "valueB");

    using namespace cb::durability;
    auto reqs = Requirements{Level::Majority, Timeout::Infinity()};
    pending->setPendingSyncWrite(reqs);

    for (int i = 0; i < 10; i++) {
        VBQueueItemCtx ctx{CanDeduplicate::Yes};
        ctx.durability = DurabilityItemCtx{reqs, cookie};

        // Do the prepare (should be clean/dirty)
        ASSERT_NE(MutationStatus::IsPendingSyncWrite,
                  public_processSet(*pending, 0 /*cas*/, ctx));
        auto item = makeCommittedItem(key, "value" + std::to_string(i));

        // Check that we block normal set
        ctx.durability.reset();
        ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
                  public_processSet(*item, 0 /*cas*/, ctx));

        {
            std::shared_lock rlh(vbucket->getStateLock());
            // And commit
            ASSERT_EQ(cb::engine_errc::success,
                      vbucket->abort(rlh,
                                     key,
                                     1 + (i * 2),
                                     {},
                                     vbucket->lockCollections(key),
                                     cookie));
        }

        // Abort shouldn't result in any operation counts being incremented.
        EXPECT_EQ(0, vbucket->opsCreate);
        EXPECT_EQ(0, vbucket->opsUpdate);
        EXPECT_EQ(0, vbucket->opsDelete);
    }
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        VBucketDurabilityTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        Ephemeral,
        EphemeralVBucketDurabilityTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
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
    ASSERT_NE(nlohmann::json{}.dump(), vbucket->getReplicationTopology());

    vbucket->setState(state);

    EXPECT_EQ(nlohmann::json{}.dump(), vbucket->getReplicationTopology());
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

    simulateLocalAck(20);
    EXPECT_EQ(20, monitor.getHighPreparedSeqno());

    auto meta = nlohmann::json{
            {"topology",
             nlohmann::json::array({{active, replica1, replica2}})}};
    vbucket->setState(vbucket_state_active, &meta);

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

    auto checkPending = [this, &key, &ckptList, preparedSeqno]() -> void {
        EXPECT_EQ(nullptr, ht->findForRead(key).storedValue);
        const auto sv = ht->findForWrite(key).storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());

        ASSERT_EQ(2, ckptList.back()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
                EXPECT_EQ(preparedSeqno, qi->getBySeqno());
                EXPECT_EQ("value", qi->getValue()->to_string_view());
            }
        }
    };

    auto checkCommitted = [this, &key, &ckptList, preparedSeqno]() -> void {
        const auto sv = ht->findForRead(key).storedValue;
        ASSERT_NE(nullptr, sv);
        EXPECT_NE(nullptr, ht->findForWrite(key).storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());

        EXPECT_EQ(2, ckptList.back()->getNumItems());
        for (const auto& qi : *ckptList.front()) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
                EXPECT_GT(qi->getBySeqno() /*commitSeqno*/, preparedSeqno);
                EXPECT_EQ(preparedSeqno, qi->getPrepareSeqno());
                EXPECT_EQ("value", qi->getValue()->to_string_view());
            }
        }
    };

    // Simulate active seqno-ack
    simulateLocalAck(preparedSeqno);

    // No replica has ack'ed yet
    checkPending();

    // replica2 acks, Durability Requirements not satisfied yet
    vbucket->seqnoAcknowledged(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            replica2,
            preparedSeqno);
    checkPending();

    // replica3 acks, Durability Requirements satisfied
    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear();
    vbucket->seqnoAcknowledged(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            replica3,
            preparedSeqno);
    vbucket->processResolvedSyncWrites();

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
        EXPECT_EQ("value",
                  storedItem.storedValue->getValue()->to_string_view());

        // CheckpointManager state:
        // empty-item
        const auto& ckpt = *ckptList.back();
        auto it = ckpt.begin();
        ASSERT_EQ(queue_op::empty, (*it)->getOperation());
        // 1 metaitem (checkpoint-start)
        it++;
        ASSERT_EQ(2, ckpt.getNumItems());
        EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
        // 1 non-metaitem is pending and contains the expected value
        it++;
        EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
        EXPECT_EQ("value", (*it)->getValue()->to_string_view());

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
    ckptMgr->clear();

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(
                      0 /*count*/, nullptr, cb::engine_errc::invalid_arguments),
              swCompleteTrace);

    // Simulate replica and active seqno-ack
    vbucket->seqnoAcknowledged(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            replica1,
            preparedSeqno);
    simulateLocalAck(preparedSeqno);

    // Commit notified
    EXPECT_EQ(SWCompleteTrace(1 /*count*/, cookie, cb::engine_errc::success),
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
    EXPECT_EQ("value", sv->getValue()->to_string_view());

    // CheckpointManager state:
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    // empty-item
    const auto& ckpt = *ckptList.front();
    auto it = ckpt.begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(2, ckpt.getNumItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is committed and contains the expected value
    it++;
    EXPECT_EQ(queue_op::commit_sync_write, (*it)->getOperation());
    EXPECT_EQ("value", (*it)->getValue()->to_string_view());
}

TEST_P(VBucketDurabilityTest, NonExistingKeyAtAbort) {
    auto noentKey = makeStoredDocKey("non-existing-key");
    std::shared_lock rlh(vbucket->getStateLock());
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vbucket->abort(rlh,
                             noentKey,
                             0 /*prepareSeqno*/,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(noentKey)));
}

TEST_P(VBucketDurabilityTest, NonPendingKeyAtAbort) {
    auto nonPendingKey = makeStoredDocKey("key1");
    auto nonPendingItem = make_item(vbucket->getId(), nonPendingKey, "value");
    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(nonPendingItem, 0 /*cas*/));
    EXPECT_EQ(1, ht->getNumItems());
    // Visible at read
    const auto* sv = ht->findForRead(nonPendingKey).storedValue;
    ASSERT_TRUE(sv);
    const int64_t bySeqno = lastSeqno + 1;
    ASSERT_EQ(bySeqno, sv->getBySeqno());
    std::shared_lock rlh(vbucket->getStateLock());
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              vbucket->abort(rlh,
                             nonPendingKey,
                             bySeqno /*prepareSeqno*/,
                             {} /*abortSeqno*/,
                             vbucket->lockCollections(nonPendingKey)));
}

TEST_P(VBucketDurabilityTest, NonExistingKeyAtAbortReplica) {
    vbucket->setState(vbucket_state_replica);
    // create memory snapshot initially
    ckptMgr->createSnapshot(lastSeqno + 1,
                            lastSeqno + 2,
                            {} /*HCS*/,
                            CheckpointType::Memory,
                            lastSeqno);

    auto key = makeStoredDocKey("key1");

    // nothing in the hashtable
    ASSERT_FALSE(ht->findOnlyCommitted(key).storedValue);
    ASSERT_FALSE(ht->findOnlyPrepared(key).storedValue);

    const int64_t prepareSeqno = lastSeqno + 1;
    const int64_t abortSeqno = lastSeqno + 2;

    ASSERT_GT(abortSeqno, ckptMgr->getHighSeqno());

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // try abort without preceding prepare, should fail
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  vbucket->abort(rlh,
                                 key,
                                 prepareSeqno /*prepareSeqno*/,
                                 abortSeqno /*abortSeqno*/,
                                 vbucket->lockCollections(key)));
    }

    // flip to a disk snapshot instead
    ckptMgr->createSnapshot(lastSeqno + 1,
                            lastSeqno + 2,
                            prepareSeqno /*HCS*/,
                            CheckpointType::Disk,
                            lastSeqno);

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // now abort should be accepted because the prepare can have validly
        // been deduped.
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->abort(rlh,
                                 key,
                                 prepareSeqno /*prepareSeqno*/,
                                 abortSeqno /*abortSeqno*/,
                                 vbucket->lockCollections(key)));
    }

    // no committed item still
    EXPECT_FALSE(ht->findOnlyCommitted(key).storedValue);
    const auto* abortedSv = ht->findOnlyPrepared(key).storedValue;

    if (std::get<0>(GetParam()) == VBucketTestBase::VBType::Ephemeral) {
        // completed sv is correctly stored in the hashtable
        EXPECT_TRUE(abortedSv);
        EXPECT_TRUE(abortedSv->isPrepareCompleted());
        EXPECT_EQ(abortSeqno, abortedSv->getBySeqno());
    }

    // abort was queued
    EXPECT_EQ(abortSeqno, ckptMgr->getHighSeqno());
}

TEST_P(VBucketDurabilityTest, NonPendingKeyAtAbortReplica) {
    vbucket->setState(vbucket_state_replica);
    // create memory snapshot initially
    ckptMgr->createSnapshot(lastSeqno + 1,
                            lastSeqno + 3,
                            {} /*HCS*/,
                            CheckpointType::Memory,
                            lastSeqno);

    auto key = makeStoredDocKey("key1");

    // nothing in the hashtable
    ASSERT_FALSE(ht->findOnlyCommitted(key).storedValue);
    ASSERT_FALSE(ht->findOnlyPrepared(key).storedValue);

    auto nonPendingItem = make_item(vbucket->getId(), key, "value");
    nonPendingItem.setBySeqno(lastSeqno + 1);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.genBySeqno = GenerateBySeqno::No;
    // store a normal item
    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(nonPendingItem, 0 /*cas*/, ctx));

    EXPECT_EQ(1, ht->getNumItems());
    // item is found in hashtable
    const auto* sv = ht->findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isCommitted());
    ASSERT_FALSE(ht->findOnlyPrepared(key).storedValue);

    const int64_t committedSeqno = sv->getBySeqno();
    EXPECT_EQ(lastSeqno + 1, committedSeqno);
    const int64_t prepareSeqno = committedSeqno + 1;
    const int64_t abortSeqno = committedSeqno + 2;

    ASSERT_GT(abortSeqno, ckptMgr->getHighSeqno());

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // try abort without preceding prepare, should fail
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  vbucket->abort(rlh,
                                 key,
                                 prepareSeqno /*prepareSeqno*/,
                                 abortSeqno /*abortSeqno*/,
                                 vbucket->lockCollections(key)));
    }

    EXPECT_EQ(lastSeqno + 1, ckptMgr->getMaxVisibleSeqno());

    // flip to a disk snapshot instead
    ckptMgr->createSnapshot(lastSeqno + 1,
                            lastSeqno + 3,
                            prepareSeqno /*HCS*/,
                            CheckpointType::Disk,
                            lastSeqno);

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // now abort should be accepted because the prepare can have validly
        // been deduped.
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->abort(rlh,
                                 key,
                                 prepareSeqno /*prepareSeqno*/,
                                 abortSeqno /*abortSeqno*/,
                                 vbucket->lockCollections(key)));
    }

    const auto* committedSv = ht->findOnlyCommitted(key).storedValue;
    const auto* abortedSv = ht->findOnlyPrepared(key).storedValue;

    // original committed item untouched
    EXPECT_EQ(sv, committedSv);
    if (std::get<0>(GetParam()) == VBucketTestBase::VBType::Ephemeral) {
        // completed sv is correctly stored in the hashtable
        EXPECT_TRUE(abortedSv);
        EXPECT_TRUE(abortedSv->isPrepareCompleted());
        EXPECT_EQ(abortSeqno, abortedSv->getBySeqno());
    }

    // abort was queued
    EXPECT_EQ(abortSeqno, ckptMgr->getHighSeqno());
}

/*
 * This test checks that at abort:
 * 1) the Pending is removed from the HashTable
 * 2) a queue_op::abort_sync_write item is enqueued into the CheckpointManager
 * 3) the abort_sync_write is not added to the DurabilityMonitor
 */
TEST_P(VBucketDurabilityTest, Active_AbortSyncWrite) {
    const int64_t preparedSeqno = 1;
    storeSyncWrites({preparedSeqno});
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
        EXPECT_EQ(CommittedState::Pending,
                  storedItem.storedValue->getCommitted());
        EXPECT_EQ(preparedSeqno, storedItem.storedValue->getBySeqno());
    }

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // CheckpointManager state:
    // empty-item
    const auto* ckpt = ckptList.back().get();
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(2, ckpt->getNumItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected value
    it++;
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ(preparedSeqno, (*it)->getBySeqno());
    EXPECT_EQ("value", (*it)->getValue()->to_string_view());

    // The Pending is tracked by the DurabilityMonitor
    const auto& monitor = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(1, monitor.getNumTracked());

    // Note: ensure 1 Ckpt in CM, easier to inspect the CkptList after Commit
    ckptMgr->clear();

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(
                      0 /*count*/, nullptr, cb::engine_errc::invalid_arguments),
              swCompleteTrace);

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // Note: abort-seqno must be provided only at Replica
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->abort(rlh,
                                 key,
                                 1 /*prepareSeqno*/,
                                 {} /*abortSeqno*/,
                                 vbucket->lockCollections(key),
                                 cookie));
    }

    // Abort notified
    EXPECT_EQ(
            SWCompleteTrace(
                    1 /*count*/, cookie, cb::engine_errc::sync_write_ambiguous),
            swCompleteTrace);

    // StoredValue has gone
    // Ephemeral keeps completed prepare in HashTable
    if (getVbType() == VBType::Persistent) {
        EXPECT_EQ(0, ht->getNumItems());
    } else {
        EXPECT_EQ(1, ht->getNumItems());
    }
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
    ASSERT_EQ(2, ckpt->getNumItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is a deleted durable-abort item with no value
    it++;
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_TRUE((*it)->isDeleted());
    EXPECT_FALSE((*it)->getValue());
    EXPECT_GT((*it)->getBySeqno(), preparedSeqno);
    EXPECT_EQ(preparedSeqno, (*it)->getPrepareSeqno());

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
            VBQueueItemCtx ctx{CanDeduplicate::Yes};
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
    ASSERT_EQ(0, monitor.getHighCompletedSeqno());
    testAddPrepare(writes);
    ASSERT_EQ(writes.size(), monitor.getNumTracked());
    ASSERT_EQ(0, monitor.getHighCompletedSeqno());
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

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // Test
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  result->getBySeqno(),
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    // Check postconditions - should only have one item for that key.
    auto readView = ht->findForRead(key).storedValue;
    auto writeView = ht->findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
    EXPECT_EQ(*readView, *writeView);
}

void VBucketDurabilityTest::testHTCommitExisting() {
    ht->clear(false);
    ckptMgr->clear(0);

    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0));
    ASSERT_EQ(1, ht->getNumItems());

    auto pending = makePendingItem(key, "valueB"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{pending->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*pending, 0, ctx));

    ASSERT_EQ(2, ht->getNumItems());

    // Check preconditions - item should be found as pending.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());

    {
        std::shared_lock rlh(vbucket->getStateLock());
        // Test
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  2,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    // Check postconditions - should only have one item for that key.
    auto readView = ht->findForRead(key).storedValue;
    auto writeView = ht->findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);

    // Should be CommittedViaPrepare
    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
}

TEST_P(EPVBucketDurabilityTest, CommitExisting) {
    testHTCommitExisting();
    EXPECT_EQ(1, ht->getNumItems());
}

TEST_P(EphemeralVBucketDurabilityTest, CommitExisting) {
    testHTCommitExisting();

    // Prepare still exists in the hash table
    EXPECT_EQ(2, ht->getNumItems());
    auto key = makeStoredDocKey("key");
    auto res = ht->findForUpdate(key);
    EXPECT_TRUE(res.pending->isPrepareCompleted());

    // Check that we have the expected items in the seqList.
    // 2 items total (prepare + commit)
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

TEST_P(EphemeralVBucketDurabilityTest, CommitExisting_RangeRead) {
    // Do a commit (which will remain unchanged due to range read)
    testHTCommitExisting();

    auto key = makeStoredDocKey("key");

    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    {
        // take a range read to cause stale items
        auto range = mockEphVb->registerFakeSharedRangeLock(0, 1000);

        // Now do a commit on top of the existing commit (within the range read)
        auto pending = makePendingItem(key, "valueC"s);
        VBQueueItemCtx ctx{CanDeduplicate::Yes};
        ctx.durability =
                DurabilityItemCtx{pending->getDurabilityReqs(), cookie};
        ASSERT_EQ(MutationStatus::WasClean,
                  public_processSet(*pending, 0, ctx));

        // 1 stale prepare, 2 non-stale (commit + new prepare)
        EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
        EXPECT_EQ(3, mockEphVb->public_getNumListItems());

        // range read released at end of scope
    }

    // range lock released - stale items can be purged
    EXPECT_EQ(1, mockEphVb->purgeStaleItems());

    // Prepare would exist outside the range read so we would not hit the append
    // case if we just committed now. Grab another range read to cover the
    // prepare so that we can test commit under range read.
    {
        auto range = mockEphVb->registerFakeSharedRangeLock(0, 1000);
        std::shared_lock rlh(vbucket->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  4,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));

        // Check that we have the expected items in the seqList.
        // 1 stale commit (because of the range read)
        // 1 completed prepare
        // 1 commit via prepare
        EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
        EXPECT_EQ(3, mockEphVb->public_getNumListItems());

        // range read released at end of scope
    }

    // range lock released - stale items can be purged

    // Do a purge of the stale items and check result
    EXPECT_EQ(1, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

// The test case doesn't really test anything new, it just demonstrates how
// prepares work in regards to the StaleItemDeleter.
TEST_P(EphemeralVBucketDurabilityTest, PrepareOnCommitted) {
    auto key = makeStoredDocKey("key1");
    auto item = makePendingItem(key, "value");
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{item->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*item, 0 /*cas*/, ctx));

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::Pending, result->getCommitted());

    // Test
    std::shared_lock rlh(vbucket->getStateLock());
    ASSERT_EQ(cb::engine_errc::success,
              vbucket->commit(rlh,
                              key,
                              result->getBySeqno(),
                              {},
                              CommitType::Majority,
                              vbucket->lockCollections(key)));

    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());

    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSet(*item, 0 /*cas*/, ctx));

    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

void VBucketDurabilityTest::testHTSyncDeleteCommit() {
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0));

    // Because we called public_processSet (which calls VBucket::set) we skip
    // the call to update the collections stats using the notifyCtx. This is
    // fine for persistent buckets because we never flush anything, but for
    // Ephemeral buckets we need to manually poke the collections item counter
    // or we will throw when we attempt to decrement it below 0.
    if (getVbType() == VBType::Ephemeral) {
        auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
        VBNotifyCtx notifyCtx;
        notifyCtx.setItemCountDifference(1);
        mockEphVb->public_doCollectionsStats(vbucket->lockCollections(key),
                                             notifyCtx);
    }

    // Do a SyncDelete
    auto* writeView = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSoftDelete(key, ctx).first);
    EXPECT_EQ(0, vbucket->opsDelete)
            << "opsDelete should not be incremented by prepared SyncDelete";

    // Test - commit the pending SyncDelete.
    writeView = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);

    auto* readView = ht->findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    EXPECT_FALSE(readView->isDeleted());
    EXPECT_TRUE(readView->getValue());

    ASSERT_EQ(CommittedState::Pending, writeView->getCommitted());
    std::shared_lock rlh(vbucket->getStateLock());
    ASSERT_EQ(cb::engine_errc::success,
              vbucket->commit(rlh,
                              key,
                              writeView->getBySeqno(),
                              {},
                              CommitType::Majority,
                              vbucket->lockCollections(key)));

    // Check postconditions:
    // 1. Upon commit, both read and write view should show same deleted item.
    readView = ht->findForRead(key, TrackReference::Yes, WantsDeleted::Yes)
                       .storedValue;
    ASSERT_TRUE(readView);
    writeView = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);

    EXPECT_EQ(readView, writeView);
    EXPECT_TRUE(readView->isDeleted());
    EXPECT_FALSE(readView->getValue());

    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
}

TEST_P(EPVBucketDurabilityTest, SyncDeleteCommit) {
    testHTSyncDeleteCommit();

    // Should just have commit
    EXPECT_EQ(1, ht->getNumItems());
}

void VBucketDurabilityTest::testSyncDeleteUpdateMaxDelRevSeqno(Resolution res) {
    // before doing any ops, the max del rel should be zero
    EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());

    doSyncWriteAndCommit();

    // not changed by a non-delete op
    EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());
    auto key = makeStoredDocKey("key");

    auto sv = ht->findOnlyCommitted(key).storedValue;
    ASSERT_TRUE(sv);
    // stored item should have rev seqno one greater than the seen max
    EXPECT_EQ(1, sv->getRevSeqno());

    // prepare a sync delete
    auto prepared = makePendingItem(key, "prepared");
    prepared->setDeleted(DeleteSource::Explicit);
    auto preparedSeqno = vbucket->getHighSeqno() + 1;
    prepared->setBySeqno(preparedSeqno);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*prepared, 0, ctx));

    // Storing the prepare doesn't touch the max del rev, the item isn't
    // actually deleted yet!
    EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());

    sv = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(sv);

    if (res == Resolution::Commit) {
        std::shared_lock rlh(vbucket->getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  preparedSeqno,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));

        // committed sync delete has rev seqno one greater than the item it
        // deleted, and has updated the max deleted rev seqno to that value
        EXPECT_EQ(2, ht->getMaxDeletedRevSeqno());

        doSyncWriteAndCommit();

        // still not changed by a non-delete op
        EXPECT_EQ(2, ht->getMaxDeletedRevSeqno());

        sv = ht->findOnlyCommitted(key).storedValue;
        ASSERT_TRUE(sv);
        // the new item again has rev seqno one greater than max del rev.
        // MB-48179: this would fail and instead be 1 as the max del rev had
        //           not been changed
        EXPECT_EQ(3, sv->getRevSeqno());
    } else {
        {
            std::shared_lock rlh(vbucket->getStateLock());
            EXPECT_EQ(cb::engine_errc::success,
                      vbucket->abort(rlh,
                                     key,
                                     preparedSeqno,
                                     {},
                                     vbucket->lockCollections(key)));
        }

        // an aborted sync delete didn't delete anything, the max del rev
        // should be untouched
        EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());

        doSyncWriteAndCommit();

        // still not changed by a non-delete op
        EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());

        sv = ht->findOnlyCommitted(key).storedValue;
        ASSERT_TRUE(sv);
        // the new item is one greater than the _original_ stored value,
        // not the aborted sync delete prepare.
        EXPECT_EQ(2, sv->getRevSeqno());
    }
}

TEST_P(EPVBucketDurabilityTest, SyncDeleteIncreasesMaxDeletedRevSeqno) {
    // MB-48179: test that a committed sync delete increases the
    // maxDeletedRevSeqno. If it does not, the rev seqno might be seen to
    // go backwards.

    testSyncDeleteUpdateMaxDelRevSeqno(Resolution::Commit);
}

TEST_P(EPVBucketDurabilityTest,
       AbortedSyncDeleteDoesntIncreaseMaxDeletedRevSeqno) {
    // MB-48179: secondary test just to validate that aborts don't alter
    // the max deleted rev seqno tracked in the hashtable.

    testSyncDeleteUpdateMaxDelRevSeqno(Resolution::Abort);
}

TEST_P(EphemeralVBucketDurabilityTest, SyncDeleteCommit) {
    testHTSyncDeleteCommit();

    // Should have completed prepare and commit
    EXPECT_EQ(2, ht->getNumItems());

    // Check that we have the expected items in the seqList.
    // 1 completed prepare
    // 1 commit
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

TEST_P(EphemeralVBucketDurabilityTest, SyncDeleteCommit_RangeRead) {
    testHTCommitExisting();

    // Manually bump the collections doc count as we are going to hit an
    // internal function
    auto key = makeStoredDocKey("key");
    VBNotifyCtx notifyCtx;
    notifyCtx.setItemCountDifference(1);
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    mockEphVb->public_doCollectionsStats(vbucket->lockCollections(key),
                                         notifyCtx);

    // Make our prepare outside of the range read
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSoftDelete(key, ctx).first);

    // Do the SyncDelete Commit in a range read
    {
        auto range = mockEphVb->registerFakeSharedRangeLock(0, 1000);
        std::shared_lock rlh(vbucket->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  4 /*prepareSeqno*/,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));

        // Check that we have the expected items in the seqList.
        // 1 stale value
        // 1 completed prepare
        // 1 value (committed)
        EXPECT_EQ(1, mockEphVb->public_getNumStaleItems());
        EXPECT_EQ(3, mockEphVb->public_getNumListItems());
    }

    // range lock released - stale items can be purged

    // Do a purge of the stale items and check result. Can't remove everything
    // from the seqList
    EXPECT_EQ(1, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(2, mockEphVb->public_getNumListItems());
}

// Negative test - check it is not possible to commit a non-pending item.
TEST_P(VBucketDurabilityTest, CommitNonPendingFails) {
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*committed, 0));

    // Check preconditions - item should be found as committed.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    ASSERT_EQ(CommittedState::CommittedViaMutation, result->getCommitted());

    // Test
    std::shared_lock rlh(vbucket->getStateLock());
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vbucket->commit(rlh,
                              key,
                              result->getBySeqno(),
                              {},
                              CommitType::Majority,
                              vbucket->lockCollections(key)));
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
    {
        std::shared_lock rlh(vbucket->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  result->getBySeqno(),
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    auto readView = ht->findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    ASSERT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());

    // Test - attempt to update with a normal Mutation (should be allowed).
    auto committed = makeCommittedItem(key, "mutation"s);
    ASSERT_EQ(MutationStatus::WasDirty, public_processSet(*committed, 0));

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

void VBucketDurabilityTest::doSyncWriteAndCommit() {
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(MutationStatus::WasClean, doPrepareSyncSet(key, "\"valueB\""s));
    ASSERT_EQ(1, ht->getNumPreparedSyncWrites());
    auto preparedSeqno = vbucket->getHighSeqno();

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    std::shared_lock rlh(vbucket->getStateLock());
    ASSERT_EQ(cb::engine_errc::success,
              vbucket->commit(rlh,
                              key,
                              preparedSeqno,
                              {},
                              CommitType::Majority,
                              vbucket->lockCollections(key)));
}

// MB-36393: Test that a SyncDelete after a SyncWrite correctly sets the value
// of the prepare (and committed) SyncDelete to empty.
// (Original MB only affects Ephemeral, but we should expect no value for a
// SyncDelete irrespective so instantiate for both bucket types.)
TEST_P(VBucketDurabilityTest, SyncWriteSyncDeleteEmptyValue) {
    // Setup - prepare & commit a SyncWrite so there's a previous Prepared
    // value, prepare a SyncDelete.
    // then
    doSyncWriteAndCommit();

    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSoftDelete(key, ctx).first);

    // Test: Check the prepared item has a zero length, raw value.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);

    EXPECT_TRUE(result->isDeleted());
    EXPECT_EQ(0, result->valuelen());
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, result->getDatatype());
}

TEST_P(EPVBucketDurabilityTest, StatsCommittedSyncWrite) {
    doSyncWriteAndCommit();

    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht->getNumItems());
}

TEST_P(EphemeralVBucketDurabilityTest, StatsCommittedSyncWrite) {
    doSyncWriteAndCommit();

    EXPECT_EQ(1, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(2, ht->getNumItems());
}

void VBucketDurabilityTest::doSyncDelete() {
    // Setup
    auto key = makeStoredDocKey("key");
    auto prepared = makePendingItem(key, "prepared");
    prepared->setDeleted(DeleteSource::Explicit);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*prepared, 0, ctx));

    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    std::shared_lock rlh(vbucket->getStateLock());
    ASSERT_EQ(cb::engine_errc::success,
              vbucket->commit(rlh,
                              key,
                              lastSeqno + 1,
                              {},
                              CommitType::Majority,
                              vbucket->lockCollections(key)));
}

MutationStatus VBucketDurabilityTest::doPrepareSyncSet(const StoredDocKey& key,
                                                       std::string value) {
    auto prepared = makePendingItem(key, value);
    prepared->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);

    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    return public_processSet(*prepared, 0, ctx);
}

AddStatus VBucketDurabilityTest::doPrepareSyncAdd(const StoredDocKey& key,
                                                  std::string value) {
    auto prepared = makePendingItem(key, value);
    prepared->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    return public_processAdd(*prepared, ctx);
}

TEST_P(EPVBucketDurabilityTest, StatsCommittedSyncDelete) {
    doSyncDelete();

    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht->getNumItems());
    EXPECT_EQ(1, ht->getNumDeletedItems());
}

TEST_P(EphemeralVBucketDurabilityTest, StatsCommittedSyncDelete) {
    doSyncDelete();

    EXPECT_EQ(1, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(2, ht->getNumItems());
    EXPECT_EQ(1, ht->getNumDeletedItems());
}

// Test case for doing a sync write on top of a pre-existing item.
// Stats should be tracked as follows:
// 1) Pre-existing item created (no item -> committed item)
// 2) Pending sync write created (no pending sw for item -> pending sw for item)
// 3) Commit sync write (committed item -> [removed] + pending sw -> committed)
TEST_P(EPVBucketDurabilityTest, StatsCommittedSyncWritePreExisting) {
    // Setup - set a pre-existing item
    auto key = makeStoredDocKey("key");
    StoredDocKey existing = makeStoredDocKey("existing");
    auto item = makeCommittedItem(key, "value");
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*item, 0));

    doSyncWriteAndCommit();

    // We should no longer have the prepared sync write (will only fail if
    // assertions are turned off)
    EXPECT_EQ(0, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht->getNumItems());
}

TEST_P(EphemeralVBucketDurabilityTest, StatsCommittedSyncWritePreExisting) {
    // Setup - set a pre-existing item
    auto key = makeStoredDocKey("key");
    StoredDocKey existing = makeStoredDocKey("existing");
    auto item = makeCommittedItem(key, "value");
    ASSERT_EQ(MutationStatus::WasClean, public_processSet(*item, 0));

    doSyncWriteAndCommit();

    // We should no longer have the prepared sync write (will only fail if
    // assertions are turned off)
    EXPECT_EQ(1, ht->getNumPreparedSyncWrites());
    EXPECT_EQ(2, ht->getNumItems());
}

void VBucketDurabilityTest::testConvertPassiveDMToActiveDM(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    vbucket->setState(initialState);

    // Queue some Prepares into the PDM
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2, 3};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), pdm.getNumTracked());

    // Notify the snapshot end to move the PDM HPS so that the ADM HPS will be
    // correct post topology change
    pdm.notifySnapshotEndReceived(3);

    // VBState transitions from Replica to Active
    const nlohmann::json meta(
            {{"topology", nlohmann::json::array({{active, replica1}})}});
    vbucket->setState(vbucket_state_active, &meta);
    // The old PDM is an instance of ADM now. All Prepares are retained.
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(seqnos.size(), adm.getNumTracked());
    ASSERT_EQ(seqnos.back().seqno, adm.getHighPreparedSeqno());
    EXPECT_EQ(std::unordered_set<int64_t>({1, 2, 3}), adm.getTrackedSeqnos());

    // Client never notified yet
    ASSERT_EQ(SWCompleteTrace(
                      0 /*count*/, nullptr, cb::engine_errc::invalid_arguments),
              swCompleteTrace);

    // Check that any attempts to read keys which are have Prepared SyncWrites
    // against them fail with SyncWriteReCommitting (until they are committed).
    for (const auto& spec : seqnos) {
        std::shared_lock rlh(vbucket->getStateLock());
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(rlh,
                                               WantsDeleted::No,
                                               TrackReference::No,
                                               vbucket->lockCollections(key));
        ASSERT_TRUE(result.storedValue);
        EXPECT_TRUE(result.storedValue->isPreparedMaybeVisible());
    }

    // Check that the SyncWrite journey now proceeds to completion as expected
    ASSERT_EQ(0, adm.getNodeWriteSeqno(replica1));
    size_t expectedNumTracked = seqnos.size();
    for (const auto& s : seqnos) {
        adm.seqnoAckReceived(replica1, s.seqno);
        vbucket->processResolvedSyncWrites();

        EXPECT_EQ(--expectedNumTracked, adm.getNumTracked());
        // Nothing to notify, we don't know anything about the oldADM->client
        // connection.
        EXPECT_EQ(SWCompleteTrace(0 /*count*/,
                                  nullptr,
                                  cb::engine_errc::invalid_arguments),
                  swCompleteTrace);
    }

    // After commit() check that the keys are now accessible and appear as
    // committed.
    for (const auto& spec : seqnos) {
        std::shared_lock rlh(vbucket->getStateLock());
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(rlh,
                                               WantsDeleted::No,
                                               TrackReference::No,
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

void VBucketDurabilityTest::testConvertPDMToADMWithNullTopologySetup(
        vbucket_state_t initialState, std::vector<SyncWriteSpec>& writes) {
    ASSERT_TRUE(vbucket);
    vbucket->setState(initialState);

    // Queue some Prepares into the PDM
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());

    testAddPrepare(writes);
    ASSERT_EQ(writes.size(), pdm.getNumTracked());

    // Persist only 2 of the prepares
    vbucket->setPersistenceSeqno(2);
    ASSERT_EQ(2, vbucket->getPersistenceSeqno());

    // Only move the HPS at snapshot boundary
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    // Still got a persist level prepare we need to persist
    const_cast<PassiveDurabilityMonitor&>(pdm).notifySnapshotEndReceived(
            writes.back().seqno);
    EXPECT_EQ(writes.size(), pdm.getNumTracked());
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // VBState transitions from Replica to Active with a null topology
    vbucket->setState(vbucket_state_active, {});
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    EXPECT_EQ(2, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
    EXPECT_EQ(writes.size(), adm.getNumTracked());
}

void VBucketDurabilityTest::testConvertPDMToADMWithNullTopology(
        vbucket_state_t initialState) {
    std::vector<SyncWriteSpec> writes{1, 2};

    testConvertPDMToADMWithNullTopologySetup(initialState, writes);

    // ns_server then sets the topology
    simulateSetVBState(
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{active}, {active}})}});

    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    // And we commit our prepares
    EXPECT_EQ(2, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
}

TEST_P(VBucketDurabilityTest, Replica_ConvertPDMToADMWithNullTopology) {
    testConvertPDMToADMWithNullTopology(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest, Pending_ConvertPDMToADMWithNullTopology) {
    testConvertPDMToADMWithNullTopology(vbucket_state_pending);
}

void VBucketDurabilityTest::
        testConvertPDMToADMWithNullTopologyPersistAfterTopologyChange(
                vbucket_state_t initialState) {
    std::vector<SyncWriteSpec> writes{1, 2};
    writes.emplace_back(
            3, false /*deletion*/, cb::durability::Level::PersistToMajority);

    testConvertPDMToADMWithNullTopologySetup(initialState, writes);

    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    // ns_server then sets the topology
    simulateSetVBState(vbucket_state_active,
                       {{"topology", nlohmann::json::array({{active}})}});

    // And we commit our prepares
    EXPECT_EQ(2, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(1, adm.getNumTracked());

    vbucket->setPersistenceSeqno(3);
    adm.notifyLocalPersistence();
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()));
    EXPECT_EQ(3, adm.getHighPreparedSeqno());
    EXPECT_EQ(3, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
}

TEST_P(EPVBucketDurabilityTest,
       Replica_ConvertPDMToADMWithNullTopologyPersistAfter) {
    testConvertPDMToADMWithNullTopologyPersistAfterTopologyChange(
            vbucket_state_replica);
}

TEST_P(EPVBucketDurabilityTest,
       Pending_ConvertPDMToADMWithNullTopologyPersistAfter) {
    testConvertPDMToADMWithNullTopologyPersistAfterTopologyChange(
            vbucket_state_pending);
}

void VBucketDurabilityTest::
        testConvertPDMToADMWithNullTopologyPersistBeforeTopologyChange(
                vbucket_state_t initialState) {
    std::vector<SyncWriteSpec> writes{1, 2};
    writes.emplace_back(
            3, false /*deletion*/, cb::durability::Level::PersistToMajority);

    testConvertPDMToADMWithNullTopologySetup(initialState, writes);

    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    vbucket->setPersistenceSeqno(3);
    adm.notifyLocalPersistence();

    // ns_server then sets the topology
    simulateSetVBState(vbucket_state_active,
                       {{"topology", nlohmann::json::array({{active}})}});

    EXPECT_EQ(3, adm.getHighPreparedSeqno());
    EXPECT_EQ(3, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
}

TEST_P(EPVBucketDurabilityTest,
       Replica_ConvertPDMToADMWithNullTopologyPersistBefore) {
    testConvertPDMToADMWithNullTopologyPersistBeforeTopologyChange(
            vbucket_state_replica);
}

TEST_P(EPVBucketDurabilityTest,
       Pending_ConvertPDMToADMWithNullTopologyPersistBefore) {
    testConvertPDMToADMWithNullTopologyPersistBeforeTopologyChange(
            vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPDMToADMWithNullTopologyPostDiskSnap(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    simulateSetVBState(initialState);
    const auto baseSeqno = vbucket->getHighSeqno();

    ckptMgr->createSnapshot(baseSeqno + 3,
                            baseSeqno + 3,
                            0,
                            CheckpointType::Disk,
                            baseSeqno + 3);

    // Queue some Prepares into the PDM
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{baseSeqno + 1, baseSeqno + 2};

    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), pdm.getNumTracked());

    // Store an unrelated item
    setOne(makeStoredDocKey("committedItem"));
    ASSERT_EQ(baseSeqno + 3, vbucket->getHighSeqno());

    // "Persist" them too and notify the PDM.
    pdm.notifySnapshotEndReceived(baseSeqno + 3);
    vbucket->setPersistenceSeqno(baseSeqno + 3);
    pdm.notifyLocalPersistence();
    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(baseSeqno + 2, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // VBState transitions from Replica to Active with a null topology
    simulateSetVBState(vbucket_state_active, {});
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    EXPECT_EQ(baseSeqno + 2, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
    EXPECT_EQ(2, adm.getNumTracked());

    // ns_server then sets the topology
    simulateSetVBState(vbucket_state_active,
                       {{"topology", nlohmann::json::array({{active}})}});

    // Note: transitioning from a null topology resets the HPS, see ADM code for
    // details.
    EXPECT_EQ(baseSeqno + 3, adm.getHighPreparedSeqno());
    EXPECT_EQ(baseSeqno + 2, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());

    // Adding a SyncWrite does not update the HPS
    auto key = makeStoredDocKey("newPrepare");
    auto newPrepare = makePendingItem(key, "value");
    newPrepare->setBySeqno(baseSeqno + 4);
    ht->set(*newPrepare.get());
    adm.addSyncWrite(nullptr /*cookie*/, newPrepare);
    EXPECT_EQ(baseSeqno + 3, adm.getHighPreparedSeqno());
    EXPECT_EQ(baseSeqno + 2, adm.getHighCompletedSeqno());
    EXPECT_EQ(1, adm.getNumTracked());

    adm.checkForCommit();
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()));
    EXPECT_EQ(baseSeqno + 4, adm.getHighPreparedSeqno());
    EXPECT_EQ(baseSeqno + 4, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
}

TEST_P(VBucketDurabilityTest,
       Replica_ConvertPDMToADMWithNullTopologyPostDiskSnap) {
    testConvertPDMToADMWithNullTopologyPostDiskSnap(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest,
       Pending_ConvertPDMToADMWithNullTopologyPostDiskSnap) {
    testConvertPDMToADMWithNullTopologyPostDiskSnap(vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPassiveDMToActiveDMUnpersistedPrepare(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    simulateSetVBState(initialState);

    // Create 1 persist level prepare
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    std::vector<SyncWriteSpec> writes;
    using namespace cb::durability;
    writes.emplace_back(1, false /*deletion*/, Level::PersistToMajority);
    testAddPrepare(writes);
    ASSERT_EQ(1, pdm.getNumTracked());

    pdm.notifySnapshotEndReceived(2);

    // Still tracking the prepare because we must persist it
    ASSERT_EQ(1, pdm.getNumTracked());

    // VBState transitions from Replica to Active
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active}})}});
    simulateSetVBState(vbucket_state_active, topology);

    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    // Still tracking prepare and HPS as 0 as we have not persisted yet
    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighPreparedSeqno());

    // Fake persistence and check again
    vbucket->setPersistenceSeqno(1);
    adm.notifyLocalPersistence();
    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(1, adm.getHighPreparedSeqno());
}

TEST_P(EPVBucketDurabilityTest,
       Replica_ConvertPassiveDMToActiveDMWithUnpersistedPrepare) {
    testConvertPassiveDMToActiveDMUnpersistedPrepare(vbucket_state_replica);
}

TEST_P(EPVBucketDurabilityTest,
       Pending_ConvertPassiveDMToActiveDMWithUnpersistedPrepare) {
    testConvertPassiveDMToActiveDMUnpersistedPrepare(vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPDMToADMMidSnapSetup(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    simulateSetVBState(initialState);

    // Create 1 prepare
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2};
    testAddPrepare(seqnos);
    ASSERT_EQ(2, pdm.getNumTracked());

    // Tell the PDM to commit the prepare
    auto key = makeStoredDocKey("key1");
    ckptMgr->extendOpenCheckpoint(4, 4);
    std::shared_lock rlh(vbucket->getStateLock());
    vbucket->commit(rlh,
                    key,
                    1 /*prepareSeqno*/,
                    3 /*commitSeqno*/,
                    CommitType::Majority,
                    vbucket->lockCollections(key));

    // Skip the receiving of the snap end
    ASSERT_EQ(2, pdm.getNumTracked());
    // We can only move HPS on snap end
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());
    // We can move HCS before snap end
    ASSERT_EQ(1, pdm.getHighCompletedSeqno());
}

void VBucketDurabilityTest::testConvertPDMToADMMidSnapSetupPersistBeforeChange(
        vbucket_state_t initialState) {
    testConvertPDMToADMMidSnapSetup(initialState);

    // Persist everything.
    // @TODO MB-35308: Remove this comment
    // Note, HPS post topology change will be equal to the high persisted seqno
    // in this case as we want to advance the HPS when we make a topoology
    // change if we are awaiting persistence of a prepare.
    vbucket->setPersistenceSeqno(3);

    // VBState transitions from Replica to Active with a topology that should be
    // able to commit all in-flight SyncWrites
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active}})}});
    simulateSetVBState(vbucket_state_active, topology);

    // The old PDM is an instance of ADM now. All Prepares are retained.
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(3, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(4, vbucket->getHighSeqno());
}

TEST_P(VBucketDurabilityTest,
       Replica_ConvertPDMToADMMidSnapPersistBeforeChange) {
    testConvertPDMToADMMidSnapSetupPersistBeforeChange(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest,
       Pending_ConvertPDMToADMMidSnapPersistBeforeChange) {
    testConvertPDMToADMMidSnapSetupPersistBeforeChange(vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPDMToADMMidSnapSetupPersistAfterChange(
        vbucket_state_t initialState) {
    testConvertPDMToADMMidSnapSetup(initialState);
    // VBState transitions from Replica to Active with a topology that should be
    // able to commit all in-flight SyncWrites
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active}})}});
    simulateSetVBState(vbucket_state_active, topology);
    // The old PDM is an instance of ADM now. All Prepares are retained.
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);

    // Remove completed from trackedWrites at topology change
    EXPECT_EQ(1, adm.getNumTracked());
    adm.checkForCommit();
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()));

    // HPS is equal to seqno of last prepare
    EXPECT_EQ(2, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(4, vbucket->getHighSeqno());
}

TEST_P(EPVBucketDurabilityTest,
       Replica_ConvertPDMToADMMidSnapPersistAfterChange) {
    testConvertPDMToADMMidSnapSetupPersistAfterChange(vbucket_state_replica);
}

TEST_P(EPVBucketDurabilityTest,
       Pending_ConvertPDMToADMMidSnapPersistAfterChange) {
    testConvertPDMToADMMidSnapSetupPersistAfterChange(vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPDMToADMMidSnapAllPreparesCompleted(
        vbucket_state_t initialState) {
    testConvertPDMToADMMidSnapSetup(initialState);

    // Commit the other outstanding preparea
    ckptMgr->extendOpenCheckpoint(5, 5);
    auto key = makeStoredDocKey("key2");
    {
        std::shared_lock rlh(vbucket->getStateLock());
        vbucket->commit(rlh,
                        key,
                        2 /*prepareSeqno*/,
                        4 /*commitSeqno*/,
                        CommitType::Majority,
                        vbucket->lockCollections(key));
    }
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(2, pdm.getHighCompletedSeqno());

    vbucket->setPersistenceSeqno(4);
    // VBState transitions from Replica to Active with a topology that should be
    // able to commit all in-flight SyncWrites
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active}})}});
    simulateSetVBState(vbucket_state_active, topology);
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(4, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(4, vbucket->getHighSeqno());

    key = makeStoredDocKey("newPrepare");
    auto newPrepare = makePendingItem(key, "value");
    newPrepare->setBySeqno(5);

    // Adding a SyncWrite does not update the HPS
    ht->set(*newPrepare.get());
    adm.addSyncWrite(nullptr /*cookie*/, newPrepare);
    EXPECT_EQ(4, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());
    EXPECT_EQ(1, adm.getNumTracked());

    adm.checkForCommit();
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()));
    EXPECT_EQ(5, adm.getHighPreparedSeqno());
    EXPECT_EQ(5, adm.getHighCompletedSeqno());
    EXPECT_EQ(0, adm.getNumTracked());
}

TEST_P(VBucketDurabilityTest,
       Replica_ConvertPDMToADMMidSnapPersistAllPreparesCompleted) {
    testConvertPDMToADMMidSnapAllPreparesCompleted(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest,
       Pending_ConvertPDMToADMMidSnapPersistAllPreparesCompleted) {
    testConvertPDMToADMMidSnapAllPreparesCompleted(vbucket_state_pending);
}

void VBucketDurabilityTest::testConvertPassiveDMToActiveDMNoPrepares(
        vbucket_state_t initialState) {
    ASSERT_TRUE(vbucket);
    simulateSetVBState(initialState);

    // Create 1 prepare
    const auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    ASSERT_EQ(0, pdm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1};
    testAddPrepare(seqnos);
    ASSERT_EQ(1, pdm.getNumTracked());
    auto key = makeStoredDocKey("key1");

    // Tell the PDM to commit the prepare
    auto& nonConstPdm = const_cast<PassiveDurabilityMonitor&>(pdm);
    nonConstPdm.completeSyncWrite(
            key, PassiveDurabilityMonitor::Resolution::Commit, 1);
    // Won't remove from trackedWrites until snap end
    nonConstPdm.notifySnapshotEndReceived(2);
    ASSERT_EQ(0, pdm.getNumTracked());

    // VBState transitions from Replica to Active
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active, replica1}})}});
    simulateSetVBState(vbucket_state_active, topology);
    // The old PDM is an instance of ADM now. All Prepares are retained.
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(0, adm.getNumTracked());

    // The seqno ack for some other replica should not throw due to acked seqno
    // being greater than lastTrackedSeqno.
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 1));
}

TEST_P(VBucketDurabilityTest,
       Replica_ConvertPassiveDMToActiveDMEmptyTrackedWrites) {
    testConvertPassiveDMToActiveDMNoPrepares(vbucket_state_replica);
}

TEST_P(VBucketDurabilityTest,
       Pending_ConvertPassiveDMToActiveDMEmptyTrackedWrites) {
    testConvertPassiveDMToActiveDMNoPrepares(vbucket_state_replica);
}

// Test that conversion from ActiveDM to PassiveDM with in-flight trackedWrites
// calculated HPS correctly.
// MB-35332: During rebalance a node may go from being active to replica for
// a vBucket, resulting in ActiveDM -> PassiveDM conversion. If the ActiveDM
// had prepares which were already locally ack'd, after PassiveDM conversion
// the highPreparedSeqno iterator was in the wrong position; resulting in
// the highPreparedSeqno.lastWriteSeqno going backwards (breaking Monotonic
// invariant).
TEST_P(VBucketDurabilityTest, ConvertActiveDMToPassiveDMPreparedSyncWrites) {
    // Setup: queue two Prepares into the ADM (needs >1 to expose the bug
    // where HPS.iterator has incorrect position, resulting in attempting to
    // set HPS.lastWriteSeqno to a lower value then it was (2 -> 1).
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    ASSERT_EQ(0, adm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2};
    testAddPrepare(seqnos);
    // checkForCommit will be called after every normal vBucket op and will
    // set the HPS for us
    adm.checkForCommit();

    // Test: Convert to PassiveDM (via dead as ns_server can do).
    simulateSetVBState(vbucket_state_dead);
    simulateSetVBState(vbucket_state_replica);

    // Check: seqnos on newly-created PassiveDM.
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Test(2): Simulate a new snapshot being received (i.e. from a DcpConsumer)
    // and the snapshot end being reached which triggers
    // updateHighPreparedSeqno()
    // - which shouldn't move HPS backwards.
    auto key = makeStoredDocKey("key3");
    auto pending = makePendingItem(key, "replica_value"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    using namespace cb::durability;
    ctx.durability = DurabilityItemCtx{
            Requirements{Level::Majority, Timeout::Infinity()}, cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    pdm.notifySnapshotEndReceived(3);
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Test(3): Now commit the prepared items.
    auto resolutionCommit = PassiveDurabilityMonitor::Resolution::Commit;
    pdm.completeSyncWrite(makeStoredDocKey("key1"), resolutionCommit, 1);
    EXPECT_EQ(1, pdm.getHighCompletedSeqno());
    pdm.completeSyncWrite(makeStoredDocKey("key2"), resolutionCommit, 2);
    EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    pdm.completeSyncWrite(makeStoredDocKey("key3"), resolutionCommit, 3);
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
}

// Test that conversion from ActiveDM to PassiveDM with in-flight trackedWrites
// including at least one completed is handled correctly.
TEST_P(VBucketDurabilityTest, ConvertActiveDMToPassiveDMCompletedSyncWrites) {
    // Setup: queue three Prepares into the ADM, then complete the seqno:1.
    // (We want to end up with at least two SyncWrites in PDM - it only
    // contains uncompleted SyncWrites (i.e. seqno 2 & 3).
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    ASSERT_EQ(0, adm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2, 3};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), adm.getNumTracked());
    // checkForCommit will be called after every normal vBucket op and will
    // set the HPS for us
    adm.checkForCommit();

    // Setup: Commit the first Prepare (so we can advance HCS to non-zero and
    // test it below).
    adm.seqnoAckReceived(replica1, 1);
    vbucket->processResolvedSyncWrites();
    ASSERT_EQ(2, adm.getNumTracked());
    ASSERT_EQ(3, adm.getHighPreparedSeqno());
    ASSERT_EQ(1, adm.getHighCompletedSeqno());

    // Test: Convert to PassiveDM (via dead as ns_server can do).
    simulateSetVBState(vbucket_state_dead);
    simulateSetVBState(vbucket_state_replica);

    // Check: state on newly created PassiveDM.
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(1, pdm.getHighCompletedSeqno());

    // Test(2): Commit the remaining outstanding prepares.
    auto resolutionCommit = PassiveDurabilityMonitor::Resolution::Commit;
    pdm.completeSyncWrite(makeStoredDocKey("key2"), resolutionCommit, 2);
    EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    pdm.completeSyncWrite(makeStoredDocKey("key3"), resolutionCommit, 3);
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumTracked());
}

/**
 * Check that converting from Replica to Active back to Replica correctly
 * preserves completed SyncWrites in trackedWrites which cannot yet be removed
 * (if persistMajority and not locally persisted).
 *
 * Scenario:
 * Replica (PassiveDM):
 *     1:prepare(persistToMajority)
 *     2:prepare(majority)
 *     3:commit(1) -> cannot locally remove 1 as not persisted yet.
 *     4:commit(2) -> cannot locally remove 2 as seqno:1 not persisted yet
 *                    (in-order commit).
 *     -> trackedWrites=[1,2]
 *        HPS=0
 *        HCS=2  (want HCS higher than the first element in the trackedWrites)
 *
 * Convert to ADM (null topology):
 *     -> trackedWrites=[1,2]
 *        HPS=0
 *        HCS=2
 *     (i.e. same as previous PassiveDM)
 *
 * Convert to PDM:
 *     State should be same as it was:
 *     -> trackedWrites=[1,2]
 *        HPS=0
 *        HCS=2
 * notifyLocalPersistence -> can remove 1 and 2:
 *     -> trackedWrites=[]
 *        HPS=2
 *        HCS=2
 */
TEST_P(EPVBucketDurabilityTest, ReplicaToActiveToReplica) {
    // Setup: PassiveDM with
    // 1:PRE(persistMajority), 2:PRE(majority), 3:COMMIT(1), 4:COMMIT(2)
    simulateSetVBState(vbucket_state_replica);
    using namespace cb::durability;
    std::vector<SyncWriteSpec> seqnos{{1, false, Level::PersistToMajority}, 2};
    testAddPrepare(seqnos);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    pdm.completeSyncWrite(makeStoredDocKey("key1"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          1);
    pdm.completeSyncWrite(makeStoredDocKey("key2"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          2);
    pdm.notifySnapshotEndReceived(4);

    // Sanity: Check PassiveDM state as expected - HPS is still zero as haven't
    // locally prepared the persistMajority, but globally that's been
    // committed (HCS=1).
    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());
    ASSERT_EQ(2, pdm.getHighCompletedSeqno());

    // Setup(2): Convert to ActiveDM (null topology).
    simulateSetVBState(vbucket_state_active);
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(2, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());

    // Test: Convert back to PassiveDM.
    simulateSetVBState(vbucket_state_replica);
    {
        auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
        EXPECT_EQ(2, pdm.getNumTracked());
        EXPECT_EQ(0, pdm.getHighPreparedSeqno());
        EXPECT_EQ(2, pdm.getHighCompletedSeqno());

        // Test(2): Check that notification of local persistence will remove
        // the completed items and advance HPS.

        // @todo MB-35366: This notifySnapshotEndReceived *shouldn't* be
        // necessary; calling notifyPersistenceToDurabilityMonitor (via
        // simulateLocalAck) should be sufficient to prepare.
        pdm.notifySnapshotEndReceived(4);
        simulateLocalAck(4);

        EXPECT_EQ(0, pdm.getNumTracked());
        EXPECT_EQ(2, pdm.getHighPreparedSeqno());
        EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    }
}

/**
 * Check that converting from Replica to Active back to Replica correctly
 * preserves completed SyncWrites in trackedWrites which cannot yet be removed
 * (if persistMajority and not locally persisted).
 *
 * Scenario:
 * Replica (PassiveDM):
 *     1:prepare(persistToMajority)
 *     2:prepare(majority)
 *     3:commit(1) -> cannot locally remove 1 as not persisted yet.
 *     4:commit(2) -> cannot locally remove 2 as seqno:1 not persisted yet
 *                    (in-order commit).
 *     -> trackedWrites=[1,2]
 *        HPS=0
 *        HCS=2  (want HCS higher than the first element in the trackedWrites)
 *
 * Convert to ADM (null topology):
 *     -> trackedWrites=[1,2]
 *        HPS=0
 *        HCS=2
 *     (i.e. same as previous PassiveDM)
 *
 * Persist seqnos 1:
 *     Calls notifyLocalPersistence, but no-op as null topology.
 *
 * Convert to PDM:
 *     (seqnos 1 has been persisted previously...)
 *     State should be updated to reflect completion of seqno:1:
 *     -> trackedWrites=[2]
 *        HPS=2
 *        HCS=2
 */
TEST_P(EPVBucketDurabilityTest, ReplicaToActiveToReplica2) {
    // Setup: PassiveDM with
    // 1:PRE(persistMajority), 2:PRE(majority), 3:COMMIT(1), 4:COMMIT(2)
    vbucket->setState(vbucket_state_replica);
    using namespace cb::durability;
    std::vector<SyncWriteSpec> seqnos{{1, false, Level::PersistToMajority}, 2};
    testAddPrepare(seqnos);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
    pdm.completeSyncWrite(makeStoredDocKey("key1"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          1);
    pdm.completeSyncWrite(makeStoredDocKey("key2"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          2);
    pdm.notifySnapshotEndReceived(4);

    // Sanity: Check PassiveDM state as expected - HPS is still zero as haven't
    // locally prepared the persistMajority, but globally that's been
    // committed (HCS=1).
    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());
    ASSERT_EQ(2, pdm.getHighCompletedSeqno());

    // Setup(2): Convert to ActiveDM (null topology).
    vbucket->setState(vbucket_state_active);
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    EXPECT_EQ(2, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());

    // Setup(3): Persist seqno 1 (so locally prepared now), but no-op
    // as without topology.
    simulateLocalAck(1);
    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(2, adm.getHighPreparedSeqno());
    EXPECT_EQ(2, adm.getHighCompletedSeqno());

    // Test: Convert back to PassiveDM. Should remove completed
    // SyncWrites from trackedWrites as have been persisted.

    // Test: Convert back to PassiveDM.
    vbucket->setState(vbucket_state_replica);
    {
        auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);
        EXPECT_EQ(0, pdm.getNumTracked());
        EXPECT_EQ(2, pdm.getHighPreparedSeqno());
        EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    }
}

// Test that a double set_vb_state with identical state & topology is handled
// correctly.
// MB-35189: ns_server can send such set_vb_state messages, and in the
// aforemented MB the nodes in the replicaiton chains were not correctly
// positioned.
TEST_P(VBucketDurabilityTest, ActiveDM_DoubleSetVBState) {
    // Setup: queue two Prepares into the ADM
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    ASSERT_EQ(0, adm.getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), adm.getNumTracked());
    // checkForCommit will be called after every normal vBucket op and will
    // set the HPS for us
    adm.checkForCommit();
    ASSERT_EQ(seqnos.back().seqno, adm.getHighPreparedSeqno());

    // Test: (re)set the topology to the same state.
    const nlohmann::json topology(
            {{"topology", nlohmann::json::array({{active, replica1}})}});
    simulateSetVBState(vbucket_state_active, topology);

    // Validate: Client never notified yet (still awaiting replica1 ACK).
    ASSERT_EQ(SWCompleteTrace(
                      0 /*count*/, nullptr, cb::engine_errc::invalid_arguments),
              swCompleteTrace);

    // Validate: Check that the SyncWrite journey now proceeds to completion as
    // expected when replica1 acks.
    ASSERT_EQ(seqnos.back().seqno, adm.getHighPreparedSeqno());
    ASSERT_EQ(0, adm.getNodeWriteSeqno(replica1));
    size_t expectedNumTracked = seqnos.size();
    for (const auto& s : seqnos) {
        adm.seqnoAckReceived(replica1, s.seqno);
        vbucket->processResolvedSyncWrites();
        EXPECT_EQ(--expectedNumTracked, adm.getNumTracked());
    }
    // Client should be notified.
    EXPECT_EQ(SWCompleteTrace(2 /*count*/, cookie, cb::engine_errc::success),
              swCompleteTrace);

    // After commit() check that the keys are now accessible and appear as
    // committed.
    for (const auto& spec : seqnos) {
        std::shared_lock rlh(vbucket->getStateLock());
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(rlh,
                                               WantsDeleted::No,
                                               TrackReference::No,
                                               vbucket->lockCollections(key));
        ASSERT_TRUE(result.storedValue);
        EXPECT_TRUE(result.storedValue->isCommitted());
    }
}

TEST_P(EPVBucketDurabilityTest,
       ActiveDM_SecondChainNodeWriteSeqnoMaintainedOntopologyChange) {
    // Set a topology with 2 chains - i.e. approaching the end of a replica swap
    // rebalance
    nlohmann::json topology({{"topology",
                              nlohmann::json::array({{active, replica1},
                                                     {active, replica2}})}});
    simulateSetVBState(vbucket_state_active, topology);

    // Setup: queue two Prepares into the ADM
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vbucket);
    ASSERT_EQ(0, adm.getNumTracked());
    using namespace cb::durability;
    const std::vector<SyncWriteSpec> seqnos{
            {1, false, Level::PersistToMajority}, 2};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), adm.getNumTracked());
    // checkForCommit will be called after every normal vBucket op and will
    // set the HPS for us
    adm.checkForCommit();

    // Prepare at seqno 1 requires persistence so we have not moved HPS
    ASSERT_EQ(0, adm.getHighPreparedSeqno());
    ASSERT_EQ(2, adm.getNumTracked());

    // We have acked replica2
    adm.seqnoAckReceived(replica2, 2);
    ASSERT_EQ(2, adm.getNodeWriteSeqno(replica2));
    ASSERT_EQ(2, adm.getNumTracked());

    // Complete the "rebalance" by settings the topology to a single chain with
    // the new replica
    topology = nlohmann::json(
            {{"topology", nlohmann::json::array({{active, replica2}})}});
    simulateSetVBState(vbucket_state_active, topology);

    // Nothing has yet been committed because the active must persist
    EXPECT_EQ(2, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighPreparedSeqno());

    // The node write seqno is transferred in the topology change
    EXPECT_EQ(2, adm.getNodeWriteSeqno(replica2));

    // Notify persistence and commit our prepares
    vbucket->setPersistenceSeqno(2);
    adm.notifyLocalPersistence();
    EXPECT_EQ(0, adm.getNumTracked());

    // Client should be notified once processed.
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()));
    EXPECT_EQ(SWCompleteTrace(2 /*count*/, cookie, cb::engine_errc::success),
              swCompleteTrace);

    // After commit() check that the keys are now accessible and appear as
    // committed.
    for (const auto& spec : seqnos) {
        std::shared_lock rlh(vbucket->getStateLock());
        auto key = makeStoredDocKey("key"s + std::to_string(spec.seqno));
        auto result = vbucket->fetchValidValue(rlh,
                                               WantsDeleted::No,
                                               TrackReference::No,
                                               vbucket->lockCollections(key));
        ASSERT_TRUE(result.storedValue);
        EXPECT_TRUE(result.storedValue->isCommitted());
    }
}

TEST_P(VBucketDurabilityTest, IgnoreAckAtTakeoverDead) {
    ASSERT_EQ(0, vbucket->getDurabilityMonitor().getNumTracked());
    const std::vector<SyncWriteSpec> seqnos{1, 2, 3};
    testAddPrepare(seqnos);
    ASSERT_EQ(seqnos.size(), vbucket->getDurabilityMonitor().getNumTracked());

    // VBState transitions from Replica to Active
    simulateSetVBState(vbucket_state_dead, nlohmann::json{});

    EXPECT_EQ(cb::engine_errc::success,
              vbucket->seqnoAcknowledged(std::shared_lock<folly::SharedMutex>(
                                                 vbucket->getStateLock()),
                                         "replica1",
                                         3));
    // We won't have crashed and we will have ignored the ack
    EXPECT_EQ(seqnos.size(), vbucket->getDurabilityMonitor().getNumTracked());
}

void VBucketDurabilityTest::setupPendingDelete(StoredDocKey key) {
    // Perform a regular mutation (so we have something to delete).
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht->set(*committed));
    ASSERT_EQ(1, ht->getNumItems());

    // Test: Now delete it via a SyncDelete.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasDirty,
              public_processSoftDelete(key, ctx).first);

    // Check postconditions:
    // 1. Original item should still be the same (when looking up via
    // findForRead):
    auto* readView = ht->findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    EXPECT_FALSE(readView->isDeleted());
    EXPECT_EQ(committed->getValue(), readView->getValue());

    // 2. Pending delete should be visible via findForWrite:
    auto* writeView = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);
    EXPECT_TRUE(writeView->isDeleted());
    EXPECT_EQ(CommittedState::Pending, writeView->getCommitted());
    EXPECT_NE(*readView, *writeView);

    // Should currently have 2 items:
    EXPECT_EQ(2, ht->getNumItems());
}

// Test that we cannot do a normal set on top of a pending SyncWrite
TEST_P(VBucketDurabilityTest, DenyReplacePendingWithCommitted) {
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "pending"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    // Attempt setting the item again with a committed value.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSet(*committed, 0 /*cas*/, ctx));
}

// Test that we cannot do a pending SyncWrite on top of a pending SyncWrite
TEST_P(VBucketDurabilityTest, DenyReplacePendingWithPending) {
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "pending"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    // Attempt setting the item again with a committed value.
    auto pending2 = makePendingItem(key, "pending2"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSet(*pending2, 0 /*cas*/, ctx));
}

// Positive test - check that an item can have a pending delete added
// (SyncDelete).
TEST_P(VBucketDurabilityTest, SyncDeletePending) {
    // Perform a regular mutation (so we have something to delete).
    auto key = makeStoredDocKey("key");
    setupPendingDelete(key);
}

// Negative test - check that if a key has a pending SyncDelete it cannot
// otherwise be modified.
TEST_P(VBucketDurabilityTest, PendingSyncDeleteToPendingWriteFails) {
    auto key = makeStoredDocKey("key");
    setupPendingDelete(key);

    // Test - attempt to mutate a key which has a pending SyncDelete against it
    // with a pending SyncWrite.
    auto pending = makePendingItem(key, "pending"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.genBySeqno = GenerateBySeqno::No;
    ctx.durability = DurabilityItemCtx{pending->getDurabilityReqs(), cookie};
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSet(*pending, 0 /*cas*/, ctx));
}

// Negative test - check that if a key has a pending SyncWrite it cannot
// be SyncDeleted
TEST_P(VBucketDurabilityTest, PendingSyncWriteToPendingDeleteFails) {
    auto key = makeStoredDocKey("key");

    // Test - attempt to mutate a key which has a pending SyncWrite against it
    // with a pending SyncDelete.
    auto pending = makePendingItem(key, "pending"s);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSoftDelete(key, ctx).first);
}

// Negative test - check that if a key has a pending SyncDelete it cannot
// otherwise be modified.
TEST_P(VBucketDurabilityTest, PendingSyncDeleteToPendingDeleteFails) {
    auto key = makeStoredDocKey("key");
    setupPendingDelete(key);

    // Test - attempt to mutate a key which has a pending SyncDelete against it
    // with a pending SyncDelete.
    auto result = ht->findForWrite(key).storedValue;
    ASSERT_TRUE(result);

    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{
            cb::durability::Requirements{cb::durability::Level::Majority, {}},
            cookie};
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite,
              public_processSoftDelete(key, ctx).first);
}

TEST_P(VBucketDurabilityTest, TouchAfterCommitIsNormalMutation) {
    // MB-36698: Touch failed when updating an item stored through a
    // sync write.
    // Touch read the existing queue_op::commit_sync_write, updated the
    // exptime, and queueDirty'd it again. This was interpreted as a
    // commit, and an expects was thrown because durability requirements were
    // not provided (needed for prepareSeqno).
    // Touch does not support durability, so it is reasonable to alter the
    // stored item to be a plain mutation; it logically *is* a mutation of a
    // previously committed value.
    auto key = makeStoredDocKey("key");
    doSyncWriteAndCommit();

    using namespace std::chrono;
    auto expiry = system_clock::now() + seconds(10);

    // prior to fix, fails expects when queueDirty-ing a
    // queue_op::commit_sync_write after fix, succeeds, stores the item as a
    // normal mutation.
    auto [status, gv] =
            public_getAndUpdateTtl(key, system_clock::to_time_t(expiry));

    ASSERT_EQ(MutationStatus::WasClean, status);
    EXPECT_EQ(CommittedState::CommittedViaMutation, gv.item->getCommitted());
}

void VBucketDurabilityTest::testCompleteSWInPassiveDM(vbucket_state_t state,
                                                      Resolution res) {
    const std::vector<SyncWriteSpec>& writes{1, 2, 3}; // seqnos
    testAddPrepareInPassiveDM(state, writes);
    ASSERT_EQ(writes.size(), vbucket->durabilityMonitor->getNumTracked());

    // This is just for an easier inspection of the CheckpointManager below
    ckptMgr->clear();

    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*vbucket);

    // Commit all + check HT + check PassiveDM::completedSeqno
    // Note: At replica, snapshots are defined at PassiveStream level, need to
    //     do it manually here given that we are testing at VBucket level.
    ckptMgr->createSnapshot(writes.back().seqno + 1,
                            writes.back().seqno + 100,
                            {} /*HCS*/,
                            CheckpointType::Memory,
                            0);
    for (auto prepare : writes) {
        auto key = makeStoredDocKey("key" + std::to_string(prepare.seqno));

        switch (res) {
        case Resolution::Commit: {
            std::shared_lock rlh(vbucket->getStateLock());
            EXPECT_EQ(cb::engine_errc::success,
                      vbucket->commit(rlh,
                                      key,
                                      prepare.seqno,
                                      prepare.seqno + 10 /*commitSeqno*/,
                                      CommitType::Majority,
                                      vbucket->lockCollections(key)));

            const auto sv = ht->findForRead(key).storedValue;
            EXPECT_TRUE(sv);
            EXPECT_EQ(CommittedState::CommittedViaPrepare, sv->getCommitted());
            EXPECT_TRUE(ht->findForWrite(key).storedValue);

            break;
        }
        case Resolution::Abort: {
            std::shared_lock rlh(vbucket->getStateLock());
            EXPECT_EQ(cb::engine_errc::success,
                      vbucket->abort(rlh,
                                     key,
                                     prepare.seqno,
                                     prepare.seqno + 10 /*abortSeqno*/,
                                     vbucket->lockCollections(key)));

            EXPECT_FALSE(ht->findForRead(key).storedValue);
            EXPECT_FALSE(ht->findForWrite(key).storedValue);

            break;
        }
        }

        // HCS moves
        EXPECT_EQ(prepare.seqno, pdm.getHighCompletedSeqno());
    }

    // Check CM
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // empty-item
    const auto& ckpt = *ckptList.back();
    auto it = ckpt.begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // cs + 3 non-metaitem are Committed or Aborted
    ASSERT_EQ(1 + writes.size(), ckpt.getNumItems());
    const auto expectedOp =
            (res == Resolution::Commit ? queue_op::commit_sync_write
                                       : queue_op::abort_sync_write);
    for (const auto& prepare : writes) {
        it++;
        EXPECT_EQ(expectedOp, (*it)->getOperation());
        EXPECT_GT((*it)->getBySeqno() /*commitSeqno*/, prepare.seqno);
        EXPECT_EQ(prepare.seqno, (*it)->getPrepareSeqno());
        if (expectedOp == queue_op::commit_sync_write) {
            EXPECT_EQ("value", (*it)->getValue()->to_string_view());
        }
    }

    // Nothing removed from PassiveDM as HPS has never moved (we have not
    // notified the PDM of snapshot-end received). Remember that we cannot
    // remove Prepares before the HPS (even the completed ones).
    EXPECT_EQ(writes.size(), vbucket->durabilityMonitor->getNumTracked());
    EXPECT_EQ(writes.back().seqno, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.notifySnapshotEndReceived(4 /*snap-end*/);

    // All removed from PassiveDM as HPS has moved up to covering all the
    // completed Prepares.
    EXPECT_EQ(0, vbucket->durabilityMonitor->getNumTracked());
    EXPECT_EQ(writes.back().seqno, pdm.getHighCompletedSeqno());
    EXPECT_EQ(writes.back().seqno, pdm.getHighPreparedSeqno());
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

TEST_P(VBucketDurabilityTest, Pending_Abort) {
    testCompleteSWInPassiveDM(vbucket_state_pending, Resolution::Abort);
}

void VBucketDurabilityTest::testConvertADMMakesPreparesMaybeVisible(
        vbucket_state_t toState, bool expectPreparedMaybeVisible) {
    const int64_t preparedSeqno = 1;
    storeSyncWrites({preparedSeqno});
    ASSERT_EQ(1,
              VBucketTestIntrospector::public_getActiveDM(*vbucket)
                      .getNumTracked());

    vbucket->setState(toState);

    auto prepareKey = makeStoredDocKey("key1");
    auto htRes = vbucket->ht.findForUpdate(prepareKey);

    ASSERT_TRUE(htRes.pending);
    if (expectPreparedMaybeVisible) {
        EXPECT_EQ(CommittedState::PreparedMaybeVisible,
                  htRes.pending->getCommitted());
    } else {
        EXPECT_NE(CommittedState::PreparedMaybeVisible,
                  htRes.pending->getCommitted());
    }
}

TEST_P(VBucketDurabilityTest, PreparesMaybeVisibleOnActiveToReplicaTransition) {
    testConvertADMMakesPreparesMaybeVisible(vbucket_state_replica, true);
}

TEST_P(VBucketDurabilityTest, PreparesMaybeVisibleOnActiveToDeadTransition) {
    testConvertADMMakesPreparesMaybeVisible(vbucket_state_dead, true);
}

TEST_P(VBucketDurabilityTest, PreparesMaybeVisibleOnActiveToPendingTransition) {
    testConvertADMMakesPreparesMaybeVisible(vbucket_state_pending, true);
}

TEST_P(VBucketDurabilityTest, PreparesMaybeVisibleOnActiveToActiveTransition) {
    testConvertADMMakesPreparesMaybeVisible(vbucket_state_active, false);
}

TEST_P(EphemeralVBucketDurabilityTest, Replica_Abort) {
    testCompleteSWInPassiveDM(vbucket_state_replica, Resolution::Abort);

    // Check that we have the expected items in the seqList.
    // 3 prepare
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(3, mockEphVb->public_getNumListItems());
}

TEST_P(EphemeralVBucketDurabilityTest, Replica_Abort_RangeRead) {
    // Register our range read
    auto* mockEphVb = dynamic_cast<MockEphemeralVBucket*>(vbucket.get());
    {
        auto range = mockEphVb->registerFakeSharedRangeLock(0, 1000);

        testCompleteSWInPassiveDM(vbucket_state_replica, Resolution::Abort);

        // Check that we have the expected items in the seqList.
        // 3 stale prepare. We append to the seqList because of the range read.
        EXPECT_EQ(3, mockEphVb->public_getNumStaleItems());
        EXPECT_EQ(6, mockEphVb->public_getNumListItems());
    }

    // range lock released - stale items can be purged

    // Do a purge of the stale items and check result. We always keep the last
    // item so it is not expected that we purge everything
    EXPECT_EQ(3, mockEphVb->purgeStaleItems());
    EXPECT_EQ(0, mockEphVb->public_getNumStaleItems());
    EXPECT_EQ(3, mockEphVb->public_getNumListItems());
}

// Test that the combination of:
// 1) a Delete and a SyncAdd (which re-creates the key),
// 2) where the Delete is persisted after the SyncAdd Prepare is added to the HT
// Gets the correct vBucket item count.
// This is a regression test for MB-38197, where the persistence callback for
// the delete did not find a Committed item in the HashTable and hence did
// not decrement the number of on-disk items.
TEST_P(EPVBucketDurabilityTest,
       MB_38197_ItemCountDeletePersistedAfterPendingSet) {
    // Setup
    // (1) Create a doc so we can delete it.
    auto key = makeStoredDocKey("key");
    setOne(key);

    // (1.1) We need the persistence cursor to move through the current
    // checkpoint and find items to run the persistence callback on.
    // (we are essentially mocking the actual flusher).
    std::vector<queued_item> items;
    vbucket->checkpointManager->getItemsForPersistence(
            items, 1, std::numeric_limits<size_t>::max());
    ASSERT_EQ(2, items.size()) << "Expected setVBState and SET.";

    // (1.2) Mimic flusher by running the PCB for the store at (1)
    EPPersistenceCallback persistCb(global_stats, *vbucket);
    persistCb(*items.back(), FlushStateMutation::Insert);
    EXPECT_EQ(1, vbucket->getNumItems());

    // (1.3) Delete key. StoredValue will still be present in HT as Dirty.
    softDeleteOne(key, MutationStatus::WasClean);
    if (std::get<1>(GetParam()) == EvictionPolicy::Full) {
        EXPECT_EQ(1, vbucket->getNumItems()) << "With EvictionPolicy::Full";
    } else {
        EXPECT_EQ(0, vbucket->getNumItems()) << "With EvictionPolicy::Value";
    }
    EXPECT_EQ(1, vbucket->getNumInMemoryDeletes());

    // (2) Re-add k via a SyncWrite. This will replace the Committed, Deleted,
    // Dirty SV with a Prepared, Dirty SV.
    auto prepared = makePendingItem(key, R"("valueB")");
    prepared->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{prepared->getDurabilityReqs(), cookie};
    ASSERT_EQ(AddStatus::UnDel, public_processAdd(*prepared, ctx));
    ASSERT_EQ(1, ht->getNumPreparedSyncWrites());

    // Test
    // Run persistence callback for Delete. This should not find a Committed
    // item in the HashTable, but _should_ decrement the numTotalItems to zero.
    items.clear();
    vbucket->checkpointManager->getItemsForPersistence(
            items, 1, std::numeric_limits<size_t>::max());
    ASSERT_EQ(2, items.size()) << "Expected Delete and Prepared SyncWrite";
    persistCb(*items.at(0), FlushStateDeletion::Delete);

    EXPECT_EQ(0, vbucket->getNumItems())
            << "Should have zero items once delete persistence callback has "
               "run.";

    // Run persistence callback for prepared Add. Item count should be
    // unaffected (still zero) given this is not yet a Committed item.
    persistCb(*items.at(1), FlushStateMutation::Insert);

    EXPECT_EQ(0, vbucket->getNumItems())
            << "Should have zero items once set(pending) persistence callback "
               "has run.";
}

// Test that the combination of:
//   1) A SyncWrite, followed by
//   2) A Delete, followed by
//   3) A SyncAdd (which re-creates the key);
// where the persistence callback for the Commit of the SyncWrite happens
// after (3) results in the correct on-disk item count.
//
// This is a regression test for a second variant of MB-38197, where the
// persistence callback for the SET did not find a Committed item in the
// HashTable and hence did not increment the number of on-disk items.
TEST_P(EPVBucketDurabilityTest,
       MB_38197_ItemCountSyncWriteCommitPersistedAfterPendingDelete) {
    // Setup
    // (1) Prepare a SyncWrite
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(MutationStatus::WasClean, doPrepareSyncSet(key, "\"valueA\""));
    ASSERT_EQ(1, ht->getNumPreparedSyncWrites());

    // (1.1) Run persistence callback for Prepare (not important, but must
    // happen before Commit occurs).
    std::vector<queued_item> items;
    vbucket->checkpointManager->getItemsForPersistence(
            items, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(2, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::pending_sync_write, items.at(1)->getOperation());

    EPPersistenceCallback persistCb(global_stats, *vbucket);
    persistCb(*items.back(), FlushStateMutation::Insert);
    EXPECT_EQ(0, vbucket->getNumItems())
            << "Prepared SyncAdd should not increase numItems";

    // (1.2) Commit the SyncWrite
    auto preparedSeqno = vbucket->getHighSeqno();
    {
        std::shared_lock rlh(vbucket->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  preparedSeqno,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    // (1.3) Begin persistence of the SyncWrite (i.e. advance persistence
    // cursor) but don't _yet_ complete persistence.
    items.clear();
    vbucket->checkpointManager->getItemsForPersistence(
            items, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::commit_sync_write, items.at(0)->getOperation());

    // (1.4) Delete key. Note crucially this happens before the Commit of the
    // SyncWrite has run it's persistence callback.
    softDeleteOne(key, MutationStatus::WasDirty);
    EXPECT_EQ(0, vbucket->getNumTotalItems()) << "With EvictionPolicy::Full";
    EXPECT_EQ(1, vbucket->getNumInMemoryDeletes());

    // (1.5) Prepare a SyncAdd for the same key.
    // This will replace the committed, deleted, dirty item added at 1.4 in
    // HT with a prepare.
    ASSERT_EQ(AddStatus::UnDel, doPrepareSyncAdd(key, "\"valueB\""));
    ASSERT_EQ(1, ht->getNumPreparedSyncWrites());

    // TEST
    // Run the persistence callback for the Commit SyncWrite at (1.2).
    // This should increase the number of items on disk from 0 to 1.
    persistCb(*items.at(0), FlushStateMutation::Insert);
    EXPECT_EQ(1, vbucket->getNumTotalItems())
            << "Should have one item once set(Commit) persistence callback has "
               "run.";

    // Run persistence callback for Delete at (1.4). Item count should
    // be reduced to zero, but in fact underflowed in original MB, given missing
    // increment from previous Commit SyncAdd.
    items.clear();
    vbucket->checkpointManager->getItemsForPersistence(
            items, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(3, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_TRUE(items.at(1)->isDeleted());
    EXPECT_EQ(queue_op::pending_sync_write, items.at(2)->getOperation());
    persistCb(*items.at(1), FlushStateDeletion::Delete);

    EXPECT_EQ(0, vbucket->getNumItems())
            << "Should have zero items once delete persistence callback "
               "has run.";
}

// More targetted test for VBucket::processExpiredItem return value when we try
// to expire a committed item while there is a pending one.
TEST_P(VBucketDurabilityTest, DoNotExpireCommittedIfPending) {
    auto key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");
    item->setExpTime(5);
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*item, 0 /*cas*/));

    auto pending = makePendingItem(key, "value");
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.durability = DurabilityItemCtx{pending->getDurabilityReqs(), cookie};
    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(*pending, 0 /*cas*/, ctx));

    auto cHandle = vbucket->lockCollections(key);
    auto findUpdateResult = vbucket->ht.findForUpdate(key);
    auto [status, sv, notifyCtx] = public_processExpiredItem(
            findUpdateResult, cHandle, ExpireBy::Access);
    EXPECT_EQ(MutationStatus::IsPendingSyncWrite, status);
    EXPECT_EQ(findUpdateResult.committed, sv);
    EXPECT_EQ(0, notifyCtx.getSeqno());
    EXPECT_FALSE(notifyCtx.isNotifyReplication());
    EXPECT_FALSE(notifyCtx.isNotifyFlusher());
    EXPECT_EQ(0, notifyCtx.getItemCountDifference());

    EXPECT_EQ(0, vbucket->numExpiredItems);
}

TEST_P(VBucketDurabilityTest, SyncAddUsesCommittedValueRevSeqno) {
    // MB-48713: verify that sync add uses an existing committed item's
    // rev seqno, rather than that of any existing prepare.

    // before doing any ops, the max del rel should be zero
    ASSERT_EQ(0, ht->getMaxDeletedRevSeqno());

    auto key = makeStoredDocKey("key");
    std::string value = "value";

    // store the item initially
    if (getEvictionPolicy() == EvictionPolicy::Full) {
        EXPECT_EQ(AddStatus::AddTmpAndBgFetch, doPrepareSyncAdd(key, value));
        // this is a vbucket level test, fake a bgfetch not finding anything
        // on disk
        addOneTemp(key);
        // even though there definitely is not a deleted version of this
        // item on disk, this results in UnDel, as it is replacing a
        // temporary item (which is considered "committed") with a prepare
        // This seems wrong, but is not relevant to this test.
        // Expecting it anyway, so if that is changed this test will fail and
        // be updated
        EXPECT_EQ(AddStatus::UnDel, doPrepareSyncAdd(key, value));
    } else {
        EXPECT_EQ(AddStatus::Success, doPrepareSyncAdd(key, value));
    }

    {
        std::shared_lock rlh(vbucket->getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  vbucket->getHighSeqno() /* preparedSeqno */,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    // max del rev not changed by a non-delete op
    EXPECT_EQ(0, ht->getMaxDeletedRevSeqno());

    auto sv = ht->findOnlyCommitted(key).storedValue;
    ASSERT_TRUE(sv);
    // stored item should have rev seqno one greater than the seen max del rev
    EXPECT_EQ(1, sv->getRevSeqno());

    // non-sync delete the item
    ASSERT_EQ(MutationStatus::WasDirty, public_processSoftDelete(key).first);

    // committed, deleted item has rev seqno one greater than the item it
    // deleted, and has updated the max deleted rev seqno to that value
    sv = ht->findOnlyCommitted(key).storedValue;
    ASSERT_TRUE(sv);
    // stored item should have rev seqno one greater than the seen max del rev
    EXPECT_EQ(2, sv->getRevSeqno());
    EXPECT_EQ(2, ht->getMaxDeletedRevSeqno());

    if (persistent()) {
        // Persistent buckets correctly find that the prepare is replacing
        // an existing committed item, indicating that the add must
        // be logically an UnDel
        EXPECT_EQ(AddStatus::UnDel, doPrepareSyncAdd(key, value));
    } else {
        // Ephemeral does not check for the existence of a committed value
        // if a completed prepared value is found. Thus, Succcess for what is
        // logically an UnDel
        EXPECT_EQ(AddStatus::Success, doPrepareSyncAdd(key, value));
    }

    {
        std::shared_lock rlh(vbucket->getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vbucket->commit(rlh,
                                  key,
                                  vbucket->getHighSeqno() /* preparedSeqno */,
                                  {},
                                  CommitType::Majority,
                                  vbucket->lockCollections(key)));
    }

    // still not changed by a non-delete op
    EXPECT_EQ(2, ht->getMaxDeletedRevSeqno());

    sv = ht->findOnlyCommitted(key).storedValue;
    ASSERT_TRUE(sv);
    // the new item again has rev seqno one greater than max del rev.
    // MB-48713: this would fail and instead be 2 as the second sync add prepare
    // used the first prepare's revSeqno +1, instead of using the committed
    // values.
    EXPECT_EQ(3, sv->getRevSeqno());
}
