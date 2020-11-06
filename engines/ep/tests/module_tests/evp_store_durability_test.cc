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
#include "checkpoint.h"
#include "checkpoint_utils.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "durability/active_durability_monitor.h"
#include "durability/durability_completion_task.h"
#include "durability/durability_monitor.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "item.h"
#include "kv_bucket.h"
#include "src/internal.h" // this is couchstore/src/internal.h
#include "test_helpers.h"
#include "tests/test_fileops.h"
#include "thread_gate.h"
#include "vbucket_state.h"
#include "vbucket_utils.h"

#include <folly/portability/GMock.h>

#include <engines/ep/src/ephemeral_tombstone_purger.h>
#include <engines/ep/tests/mock/mock_ep_bucket.h>
#include <engines/ep/tests/mock/mock_ephemeral_bucket.h>
#include <engines/ep/tests/mock/mock_paging_visitor.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>

class DurabilityEPBucketTest : public STParameterizedBucketTest {
protected:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        // Add an initial replication topology so we can accept SyncWrites.
        setVBucketToActiveWithValidTopology();
    }

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }

    /// Test that a prepare of a SyncWrite / SyncDelete is correctly persisted
    /// to disk.
    void testPersistPrepare(DocumentState docState);

    /// Test that a prepare of a SyncWrite / SyncDelete, which is then aborted
    /// is correctly persisted to disk.
    void testPersistPrepareAbort(DocumentState docState);

    /**
     * Test that if a single key is prepared, aborted & re-prepared it is the
     * second Prepare which is kept on disk.
     * @param docState State to use for the second prepare (first is always
     *                 state alive).
     */
    void testPersistPrepareAbortPrepare(DocumentState docState);

    /**
     * Test that if a single key is prepared, aborted re-prepared & re-aborted
     * it is the second Abort which is kept on disk.
     * @param docState State to use for the second prepare (first is always
     *                 state alive).
     */
    void testPersistPrepareAbortX2(DocumentState docState);

    /**
     * Method to verify that a document of a given key is delete
     * @param vb the vbucket that should contain the deleted document
     * @param key the key of the document that should have been delete
     */
    void verifyDocumentIsDelete(VBucket& vb, StoredDocKey key);

    /**
     * Method to verify that a document is present in a vbucket
     * @param vb the vbucket that should contain the stored document
     * @param key the key of the stored document
     */
    void verifyDocumentIsStored(VBucket& vb, StoredDocKey key);

    /**
     * Method to create a SyncWrite by calling store, check the on disk
     * item count and collection count after calling the store
     * @param vb vbucket to perform the prepare SyncWrite to
     * @param pendingItem the new mutation that should be written to disk
     * @param expectedDiskCount expected number of items on disk after the
     * call to store
     * @param expectedCollectedCount expected number of items in the default
     * collection after the call to store
     */
    void performPrepareSyncWrite(VBucket& vb,
                                 queued_item pendingItem,
                                 uint64_t expectedDiskCount,
                                 uint64_t expectedCollectedCount);

    /**
     * Method to create a SyncDelete by calling delete on the vbucket, check
     * the on disk item count and collection count after calling the store
     * @param vb vbucket to perform the prepare SyncDelete to
     * @param key of the document to be deleted
     * @param expectedDiskCount expected number of items on disk after the
     * call to delete
     * @param expectedCollectedCount expected number of items in the default
     * collection after the call to delete
     */
    void performPrepareSyncDelete(VBucket& vb,
                                  StoredDocKey key,
                                  uint64_t expectedDiskCount,
                                  uint64_t expectedCollectedCount);

    /**
     * Method to perform a commit of a mutation for a given key and
     * check the on disk item count afterwards.
     * @param vb vbucket to perform the commit to
     * @param key StoredDockKey that we should be committing
     * @param prepareSeqno the prepare seqno of the commit
     * @param expectedDiskCount expected number of items on disk after the
     * commit
     * @param expectedCollectedCount expected number of items in the default
     * collection after the commit
     */
    void performCommitForKey(VBucket& vb,
                             StoredDocKey key,
                             uint64_t prepareSeqno,
                             uint64_t expectedDiskCount,
                             uint64_t expectedCollectedCount);

    /**
     * Method to perform an end to end SyncWrite by creating an document of
     * keyName with the value value and then performing a flush of the
     * prepare and committed mutations.
     * @param vb vbucket to perform the SyncWrite on
     * @param keyName name of the key to perform a SyncWrite too
     * @param value that should be written to the key
     */
    void testCommittedSyncWriteFlushAfterCommit(VBucket& vb,
                                                std::string keyName,
                                                std::string value);

    /**
     * Method to perform an end to end SyncDelete for the document with the key
     * keyName. We also flush the prepare and commit of the SyncDelete to disk
     * and then check that the value has been written to disk.
     * prepare and committed mutations.
     * @param vb vbucket to perform the SyncDelete on
     * @param keyName the name of the key to delete
     */
    void testSyncDeleteFlushAfterCommit(VBucket& vb, std::string keyName);

    /**
     * Method to verify a vbucket's on disk item count
     * @param vb VBucket reference that stores the on disk count that we want
     * to assert
     * @param expectedValue The value of the on disk item count that expect the
     * counter to be.
     */
    void verifyOnDiskItemCount(VBucket& vb, uint64_t expectedValue);

    /**
     * Method to verify collection's item count
     * @param vb VBucket reference that stores the on disk count that we want
     * to assert
     * @param cID The CollectionID of the collection that contains the on disk
     * count that we want to assert against the expectedValue
     * @param expectedValue The value of the on disk item count that expect the
     * counter to be.
     */
    void verifyCollectionItemCount(VBucket& vb,
                                   CollectionID cID,
                                   uint64_t expectedValue);

};

/**
 * Test fixtures for persistent bucket tests that only run under couchstore
 */
class DurabilityCouchstoreBucketTest : public DurabilityEPBucketTest {};

/**
 * Test fixture for Durability-related tests applicable to ephemeral and
 * persistent buckets with either eviction modes.
 */
class DurabilityBucketTest : public STParameterizedBucketTest {
protected:
    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }

    /**
     * Method to set the current vbucket to the replica state and runes the
     * persistence task
     */
    void setVBucketToReplicaAndPersistToDisk() {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_replica, {}, TransferVB::No);
    }

    template <typename F>
    void testDurabilityInvalidLevel(F& func);

    /**
     * MB-34770: Test that a Pending -> Active takeover (which has in-flight
     * prepared SyncWrites) is handled correctly when there isn't yet a
     * replication toplogy.
     * This is the case during takeover where the setvbstate(active) is sent
     * from the old active which doesn't know what the topology will be and
     * hence is null.
     */
    void testTakeoverDestinationHandlesPreparedSyncWrites(
            cb::durability::Level level);

    // Call a number of operations where we expect a sync_write in progress
    // error
    void checkForSyncWriteInProgess(Item& pendingItem) {
        auto* anotherClient = create_mock_cookie(engine.get());
        ASSERT_EQ(ENGINE_SYNC_WRITE_IN_PROGRESS,
                  store->set(pendingItem, anotherClient, {}));
        ASSERT_EQ(ENGINE_SYNC_WRITE_IN_PROGRESS,
                  store->replace(pendingItem, anotherClient, {}));
        destroy_mock_cookie(anotherClient);
    }

    /**
     * Add a prepared SyncWrite for the given key, then abort it.
     */
    void setupAbortedSyncWrite(const StoredDocKey& key) {
        auto prepared = makePendingItem(key, "value");
        ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepared, cookie));
        auto& vb = *store->getVBucket(vbid);
        ASSERT_EQ(ENGINE_SUCCESS,
                  vb.abort(key,
                           prepared->getBySeqno(),
                           {},
                           vb.lockCollections(key),
                           cookie));
    }

    /**
     * Add a prepared SyncDelete for the given key, then abort it.
     */
    void setupAbortedSyncDelete(const StoredDocKey& key) {
        uint64_t cas = 0;
        using namespace cb::durability;
        auto reqs = Requirements(Level::Majority, {});
        mutation_descr_t delInfo;
        ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING,
                  store->deleteItem(
                          key, cas, vbid, cookie, reqs, nullptr, delInfo));
        auto& vb = *store->getVBucket(vbid);
        ASSERT_EQ(ENGINE_SUCCESS,
                  vb.abort(key,
                           delInfo.seqno,
                           {},
                           vb.lockCollections(key),
                           cookie));
    }

    // When bloom filters are turned off, a temp item needs to be
    // inserted and the BGFetcher needs to run to get that item accepted.
    // Then, we need to come back to the original item and push that out
    // with the BGFetch before the pending item is accepted.
    ENGINE_ERROR_CODE addPendingItem(Item& itm, const void* cookie) {
        auto rc = store->add(itm, cookie);
        if (rc == ENGINE_EWOULDBLOCK && persistent() && fullEviction()) {
            runBGFetcherTask();
            rc = store->add(itm, cookie);
        }
        return rc;
    }

    /**
     * Method to get and check a replica's value
     */
    void checkReplicaValue(DocKey key,
                           std::string value,
                           get_options_t options) {
        auto getReplicaValue = store->getReplica(key, vbid, cookie, options);
        ASSERT_EQ(ENGINE_SUCCESS, getReplicaValue.getStatus());
        auto itemFromValue = *getReplicaValue.item;
        EXPECT_FALSE(itemFromValue.isPending());
        EXPECT_EQ(value,
                  std::string(itemFromValue.getData(),
                              itemFromValue.getNBytes()));
    }

    /**
     * Method to create a PendingMaybeVisible item to be stored in a replica
     * vbucket
     * @param key of the pending item
     * @param value of the pending item
     */
    void storePreparedMaybeVisibleItem(DocKey key, std::string& value) {
        using namespace cb::durability;
        auto& vb = *store->getVBucket(vbid);

        auto seqno = vb.getHighSeqno() + 1;

        vb.checkpointManager->createSnapshot(seqno,
                                             seqno,
                                             {} /*HCS*/,
                                             CheckpointType::Memory,
                                             vb.getHighSeqno());

        auto item = *makePendingItem(
                StoredDocKey(key),
                value,
                Requirements(Level::Majority, Timeout::Infinity()));
        item.setCas();
        item.setBySeqno(seqno);
        item.setPreparedMaybeVisible();

        EXPECT_EQ(ENGINE_SUCCESS, store->prepare(item, cookie));
    }

    /**
     * Test that prepares in the resolvedQueue of the ADM are returned to
     * trackedWrites when transitioning away from ADM
     */
    void testResolvedSyncWritesReturnedToTrackedWritesVBStateChange(
            vbucket_state_t newState);

    void takeoverSendsDurabilityAmbiguous(vbucket_state_t newState);

    enum class DocState : uint8_t { NOENT, RESIDENT, EJECTED };

    /**
     * Check that the correct replace semantic is still enforced if a prepare is
     * in-flight for the same doc:
     * 1) the replace is rejected with KEY_ENOENT if no committed doc exists
     *  (regardless of whether a prepare is in-flight or not)
     * 2) else, the set-phase of the replace is rejected with SW_IN_PROGRESS if
     *  a pending prepare exists in the HT
     */
    void testReplaceAtPendingSW(DocState docState);

    /**
     * Test that the Bucket Min Durability Level provided is valid this Bucket
     * instance (Persistent or Ephemeral).
     */
    void testSetMinDurabilityLevel(cb::durability::Level level);

    enum class EngineOp : uint8_t { Store, StoreIf, Remove };

    /**
     * Test that the Durability Level of a write is upgraded to the Bucket Min
     * Level.
     *
     * @param minLevel
     * @param writeLevel (optional)
     */
    void testUpgradeToMinDurabilityLevel(
            cb::durability::Level minLevel,
            std::optional<cb::durability::Level> writeLevel,
            EngineOp engineOp);

    /// Member to store the default options for GET and GET_REPLICA ops
    static const get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
};

class DurabilityEphemeralBucketTest : public STParameterizedBucketTest {
protected:
    template <typename F>
    void testPurgeCompletedPrepare(F& func);
};

/// Note - not single-threaded
class DurabilityRespondAmbiguousTest : public KVBucketTest {
protected:
    void SetUp() override {
        // The test should do the SetUp
    }
    void TearDown() override {
            // The test should do the TearDown
    };
};

class BackingStoreMaxVisibleSeqnoTest : public DurabilityBucketTest {
public:
    void SetUp() override {
        DurabilityBucketTest::SetUp();
        // The maxVisibleSeqno should only advance on mutations, deletions or
        // commits, not prepares or aborts.
        setVBucketToActiveWithValidTopology();

        vb = store->getVBucket(vbid);
        ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

        // no commits or mutations have occurred
        EXPECT_EQ(0, getMVS());
    }

    void TearDown() override {
        vb.reset();
        DurabilityBucketTest::TearDown();
    };

    uint64_t getMVS() {
        if(persistent()) {
            KVStore* rwUnderlying = store->getRWUnderlying(vbid);
            const auto* persistedVbState = rwUnderlying->getVBucketState(vbid);

            return persistedVbState->maxVisibleSeqno;
        } else {
            auto& evb = dynamic_cast<const EphemeralVBucket&>(*vb);
            return gsl::narrow_cast<uint64_t>(evb.getMaxVisibleSeqno());
        }
    }

    const StoredDocKey key = makeStoredDocKey("key");
    VBucketPtr vb;
};

static void validateHighAndVisibleSeqno(VBucket& vb,
                                        uint64_t expectedHigh,
                                        uint64_t expectedVisible) {
    auto& ckptMgr = *vb.checkpointManager;
    EXPECT_EQ(expectedHigh, ckptMgr.getHighSeqno());
    EXPECT_EQ(expectedVisible, ckptMgr.getMaxVisibleSeqno());
    if (vb.getState() == vbucket_state_active) {
        EXPECT_EQ(expectedHigh, ckptMgr.getSnapshotInfo().range.getEnd());
        EXPECT_EQ(expectedVisible, ckptMgr.getVisibleSnapshotEndSeqno());
    }
}

void DurabilityEPBucketTest::testPersistPrepare(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));
    auto& vb = *store->getVBucket(vbid);
    flushVBucketToDiskIfPersistent(vbid, 1);
    ASSERT_EQ(1, vb.getNumItems());
    auto pending = makePendingItem(key, "valueB");
    if (docState == DocumentState::Deleted) {
        pending->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, ckptMgr.getNumItemsForPersistence());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // Committed and Pending will be split in one checkpoint
    ASSERT_EQ(1, ckptList.size());

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.diskQueueSize);

    // Item must be flushed
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Item must have been removed from the disk queue
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);

    // The item count must not increase when flushing Pending SyncWrites
    EXPECT_EQ(1, vb.getNumItems());
    EXPECT_EQ(1, vb.opsCreate) << "pending op increased opsCreate?";
    EXPECT_EQ(0, vb.opsUpdate) << "pending op increased opsUpdate?";

    // @TODO RocksDB
    // @TODO Durability
    // TSan sporadically reports a data race when calling store->get below when
    // running this test under RocksDB. Manifests for both full and value
    // eviction but only seen after adding full eviction variants for this test.
    // Might be the case that running the couchstore full eviction variant
    // beforehand is breaking something.
#ifdef THREAD_SANITIZER
    auto bucketType = std::get<0>(GetParam());
    if (bucketType == "persistentRocksdb") {
        return;
    }
#endif

    // Check the committed item on disk.
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(*committed, *gv.item);

    // Check the Prepare on disk
    DiskDocKey prefixedKey(key, true /*prepare*/);
    gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_EQ(docState == DocumentState::Deleted, gv.item->isDeleted());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareWrite) {
    testPersistPrepare(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareDelete) {
    testPersistPrepare(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbort(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getNumItems());

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    if (docState == DocumentState::Deleted) {
        pending->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
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
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
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
    validateHighAndVisibleSeqno(vb, 2, 0);

    EXPECT_EQ(2, vb.getHighSeqno());
    EXPECT_EQ(0, vb.getMaxVisibleSeqno());

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);
    EXPECT_EQ(0, vb.opsCreate); // nothing committed
    EXPECT_EQ(0, vb.opsUpdate); // nothing updated
    EXPECT_EQ(0, vb.opsDelete); // nothing deleted

    // At persist-dedup, the Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_NE(0, gv.item->getDeleteTime());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareWriteAbort) {
    testPersistPrepareAbort(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareDeleteAbort) {
    testPersistPrepareAbort(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbortPrepare(
        DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // First prepare (always a SyncWrite) and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare.
    auto pending2 = makePendingItem(key, "value2");
    if (docState == DocumentState::Deleted) {
        pending2->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending2, cookie));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::pending_sync_write);
    EXPECT_EQ(3, ckptMgr.getNumItemsForPersistence());
    validateHighAndVisibleSeqno(vb, 3, 0);

    EXPECT_EQ(3, vb.getHighSeqno());
    EXPECT_EQ(0, vb.getMaxVisibleSeqno());

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Prepare survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_EQ(docState == DocumentState::Deleted, gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno(), gv.item->getBySeqno());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepare) {
    testPersistPrepareAbortPrepare(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepareDelete) {
    testPersistPrepareAbortPrepare(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbortX2(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // First prepare and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare and abort.
    auto pending2 = makePendingItem(key, "value2");
    if (docState == DocumentState::Deleted) {
        pending2->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending2, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending2->getBySeqno(),
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(4, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::abort_sync_write);
    EXPECT_EQ(4, ckptMgr.getNumItemsForPersistence());
    validateHighAndVisibleSeqno(vb, 4, 0);

    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(0, vb.getMaxVisibleSeqno());

    // Note: Prepare and Abort are in the same key-space and hence are
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno() + 1, gv.item->getBySeqno());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortx2) {
    testPersistPrepareAbortX2(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepareDeleteAbort) {
    testPersistPrepareAbortX2(DocumentState::Deleted);
}

/// Test persistence of a prepared & committed SyncWrite, followed by a
/// prepared & committed SyncDelete.
TEST_P(DurabilityEPBucketTest, PersistSyncWriteSyncDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager but they
    // can exist in a single checkpoint
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(2, ckptList.back()->getNumItems());
    EXPECT_EQ(2, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(0, vb.opsDelete);

    // prepare SyncDelete and commit.
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    EXPECT_EQ(1, ckptMgr.getNumItemsForPersistence());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // Counts shouldn't change when preparing.
    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(0, vb.opsDelete);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        delInfo.seqno,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(2, ckptList.back()->getNumItems());
    EXPECT_EQ(1, ckptMgr.getNumItemsForPersistence());
    validateHighAndVisibleSeqno(vb, 4, 4);

    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(4, vb.getMaxVisibleSeqno());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Prepare and Commit survive.
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isCommitted());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(delInfo.seqno + 1, gv.item->getBySeqno());

    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    if (!isRocksDB()) {
        // TODO: opsDelete not updated correctly under RocksDB as persistence
        // callback doesn't know if the document previously existed or not.
        EXPECT_EQ(1, vb.opsDelete);
    }
}

/// Test SyncDelete on top of SyncWrite
TEST_P(DurabilityBucketTest, SyncWriteSyncDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    EXPECT_EQ(0, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(0, vb.opsDelete);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager (achieved
    // by inserting them into different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(2, ckptList.back()->getNumItems());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(0, vb.opsDelete);

    // prepare SyncDelete and commit.
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;

    EXPECT_EQ(1, vb.getNumItems());

    // Ephemeral keeps the completed prepare
    if (persistent()) {
        EXPECT_EQ(0, vb.ht.getNumPreparedSyncWrites());
    } else {
        EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());
    }
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    checkForSyncWriteInProgess(*pending);

    EXPECT_EQ(1, vb.getNumItems());
    EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());
    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(0, vb.opsDelete);

    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());

    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        3 /*prepareSeqno*/,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));
    validateHighAndVisibleSeqno(vb, 4, 4);

    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(4, vb.getMaxVisibleSeqno());

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(1, vb.opsCreate);
    EXPECT_EQ(0, vb.opsUpdate);
    EXPECT_EQ(1, vb.opsDelete);

    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(2, ckptList.back()->getNumItems());
}

// Test SyncDelete followed by a SyncWrite, where persistence of
// SyncDelete's Commit is delayed until SyncWrite prepare in HashTable (checking
// correct HashTable item is removed)
// Regression test for MB-34810.
TEST_P(DurabilityBucketTest, SyncDeleteSyncWriteDelayedPersistence) {
    // Setup: Add an initial value (so we can SyncDelete it).
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // Setup: prepare SyncDelete
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    validateHighAndVisibleSeqno(vb, 2, 1);
    // Setup: Persist SyncDelete prepare.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Setup: commit SyncDelete (but no flush yet).
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        2 /*prepareSeqno*/,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    validateHighAndVisibleSeqno(vb, 3, 3);

    // Setuo: Prepare SyncWrite
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    validateHighAndVisibleSeqno(vb, 4, 3);

    // Test: flush items to disk. The flush of the Committed SyncDelete will
    // attempt to remove that item from the HashTable; check the correct item
    // is removed (Committed SyncDelete, not prepared SyncWrite).
    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites())
            << "SyncWrite prepare should still exist";

    EXPECT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        4 /*prepareSeqno*/,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)))
            << "SyncWrite commit should be possible";
    validateHighAndVisibleSeqno(vb, 5, 5);
}

/// Test delete on top of SyncWrite
TEST_P(DurabilityBucketTest, SyncWriteDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager (achieved
    // by inserting them into different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(2, ckptList.back()->getNumItems());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Perform regular delete.
    uint64_t cas = 0;
    mutation_descr_t delInfo;

    EXPECT_EQ(1, vb.getNumItems());

    auto expectedNumPrepares = persistent() ? 0 : 1;
    EXPECT_EQ(expectedNumPrepares, vb.ht.getNumPreparedSyncWrites());
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key, cas, vbid, cookie, {}, nullptr, delInfo));

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(expectedNumPrepares, vb.ht.getNumPreparedSyncWrites());

    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
}

TEST_P(DurabilityBucketTest, SyncWriteComparesToCorrectCas) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable write to same key

    auto committed = makeCommittedItem(key, "some_other_value");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // get cas
    uint64_t cas = store->get(key, vbid, cookie, {}).item->getCas();

    // now do another SyncWrite with a cas
    pending = makePendingItem(key, "new_value");
    pending->setCas(cas);

    // Should succeed - has correct cas
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
}

TEST_P(DurabilityEphemeralBucketTest, SyncAddChecksCorrectSVExists) {
    // MB-35979: test to ensure a durable add op does not erroneously succeed
    // when the item does exist, in the presence of a completed prepare.
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite  and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable write
    auto committed = makeCommittedItem(key, "some_other_value");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // now do a SyncAdd. Should FAIL as the item exists
    // This was seen to succeed due to a bug in VBucket::processAdd
    pending = makePendingItem(key, "new_value");
    ASSERT_EQ(ENGINE_NOT_STORED, store->add(*pending, cookie));
}

TEST_P(DurabilityEphemeralBucketTest, SyncAddChecksCorrectExpiry) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable write with expiry
    auto committed = makeCommittedItem(key, "some_other_value");

    using namespace std::chrono;
    auto expiry = system_clock::now() + seconds(1);
    committed->setExpTime(system_clock::to_time_t(expiry));

    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // time travel to when the item has definitely expired
    TimeTraveller cooper(10);

    // now do a SyncAdd. Should succeed, as the item has expired.
    pending = makePendingItem(key, "new_value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->add(*pending, cookie));
}

TEST_P(DurabilityEphemeralBucketTest, SyncReplaceChecksCorrectSVExists) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite  and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable delete
    mutation_descr_t delInfo;
    uint64_t cas = 0;
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key, cas, vbid, cookie, {}, nullptr, delInfo));

    // now do a SyncReplace. Should FAIL as the item was deleted
    pending = makePendingItem(key, "new_value");
    ASSERT_EQ(ENGINE_KEY_ENOENT, store->replace(*pending, cookie));
}

TEST_P(DurabilityEphemeralBucketTest, SyncReplaceChecksCorrectExpiry) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable write with expiry
    auto committed = makeCommittedItem(key, "some_other_value");

    using namespace std::chrono;
    auto expiry = system_clock::now() + seconds(1);
    committed->setExpTime(system_clock::to_time_t(expiry));

    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // time travel to when the item has definitely expired
    TimeTraveller abe(10);

    // now do a SyncReplace. Should fail, as the item has expired.
    pending = makePendingItem(key, "new_value");
    ASSERT_EQ(ENGINE_KEY_ENOENT, store->replace(*pending, cookie));
}

TEST_P(DurabilityEphemeralBucketTest, SyncWriteChecksCorrectExpiry) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        pending->getBySeqno(),
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    vb.processResolvedSyncWrites();

    // Non-durable write with expiry
    auto committed = makeCommittedItem(key, "some_other_value");

    using namespace std::chrono;
    auto expiry = system_clock::now() + seconds(1);
    committed->setExpTime(system_clock::to_time_t(expiry));

    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    // get cas
    uint64_t cas = store->get(key, vbid, cookie, {}).item->getCas();

    // time travel to when the item has definitely expired
    TimeTraveller cooper(10);

    // now do a SyncWrite with a cas - the item has expired so it should
    // return not found, not invalid cas
    pending = makePendingItem(key, "new_value");
    pending->setCas(cas);
    ASSERT_EQ(ENGINE_KEY_ENOENT, store->set(*pending, cookie));
}

void DurabilityEPBucketTest::verifyOnDiskItemCount(VBucket& vb,
                                                   uint64_t expectedValue) {
    // skip for rocksdb as it treats every mutation as an insertion
    // and so we would expect a different item count compared with couchstore
    auto bucketType = std::get<0>(GetParam());
    if (bucketType == "persistentRocksdb") {
        return;
    }
    EXPECT_EQ(expectedValue, vb.getNumTotalItems());
}

void DurabilityEPBucketTest::verifyCollectionItemCount(VBucket& vb,
                                                       CollectionID cID,
                                                       uint64_t expectedValue) {
    // skip for rocksdb as it dose not perform item counting for collections
    auto bucketType = std::get<0>(GetParam());
    if (bucketType == "persistentRocksdb") {
        return;
    }
    {
        auto rh = vb.lockCollections();
        EXPECT_EQ(expectedValue, rh.getItemCount(cID));
    }
}

void DurabilityEPBucketTest::verifyDocumentIsStored(VBucket& vb,
                                                    StoredDocKey key) {
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    ASSERT_FALSE(gv.item->isDeleted());
    ASSERT_TRUE(gv.item->isCommitted());
}

void DurabilityEPBucketTest::verifyDocumentIsDelete(VBucket& vb,
                                                    StoredDocKey key) {
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    ASSERT_TRUE(gv.item->isDeleted());
    ASSERT_TRUE(gv.item->isCommitted());
}

void DurabilityEPBucketTest::performPrepareSyncWrite(
        VBucket& vb,
        queued_item pendingItem,
        uint64_t expectedDiskCount,
        uint64_t expectedCollectedCount) {
    auto cID = pendingItem->getKey().getCollectionID();
    // First prepare SyncWrite and commit for test_doc.
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pendingItem, cookie));
    verifyOnDiskItemCount(vb, expectedDiskCount);
    verifyCollectionItemCount(vb, cID, expectedCollectedCount);
}

void DurabilityEPBucketTest::performPrepareSyncDelete(
        VBucket& vb,
        StoredDocKey key,
        uint64_t expectedDiskCount,
        uint64_t expectedCollectedCount) {
    mutation_descr_t delInfo;
    uint64_t cas = 0;
    auto reqs =
            cb::durability::Requirements(cb::durability::Level::Majority, {});
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    verifyOnDiskItemCount(vb, expectedDiskCount);
    verifyCollectionItemCount(
            vb, key.getCollectionID(), expectedCollectedCount);
}

void DurabilityEPBucketTest::performCommitForKey(
        VBucket& vb,
        StoredDocKey key,
        uint64_t prepareSeqno,
        uint64_t expectedDiskCount,
        uint64_t expectedCollectedCount) {
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        prepareSeqno,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));
    verifyOnDiskItemCount(vb, expectedDiskCount);
    verifyCollectionItemCount(
            vb, key.getCollectionID(), expectedCollectedCount);
}

void DurabilityEPBucketTest::testCommittedSyncWriteFlushAfterCommit(
        VBucket& vb, std::string keyName, std::string value) {
    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey(keyName);
    auto keyCollectionID = key.getCollectionID();
    auto pending = makePendingItem(key, value);

    auto initOnDiskCount = vb.getNumTotalItems();
    uint64_t currentCollectionCount = 0;
    {
        auto rh = vb.lockCollections();
        currentCollectionCount = rh.getItemCount(keyCollectionID);
    }

    performPrepareSyncWrite(
            vb, pending, initOnDiskCount, currentCollectionCount);
    auto prepareSeqno = vb.getHighSeqno();
    performCommitForKey(
            vb, key, prepareSeqno, initOnDiskCount, currentCollectionCount);

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // check the value is correctly set on disk
    verifyDocumentIsStored(vb, key);
}

void DurabilityEPBucketTest::testSyncDeleteFlushAfterCommit(
        VBucket& vb, std::string keyName) {
    auto key = makeStoredDocKey(keyName);
    auto keyCollectionID = key.getCollectionID();

    auto initOnDiskCount = vb.getNumTotalItems();
    uint64_t currentCollectionCount = 0;
    {
        auto rh = vb.lockCollections();
        currentCollectionCount = rh.getItemCount(keyCollectionID);
    }

    performPrepareSyncDelete(vb, key, initOnDiskCount, currentCollectionCount);
    auto prepareSeqno = vb.getHighSeqno();
    performCommitForKey(
            vb, key, prepareSeqno, initOnDiskCount, currentCollectionCount);

    // flush the prepare and commit mutations to disk
    flushVBucketToDiskIfPersistent(vbid, 2);

    // check the value is correctly deleted on disk
    verifyDocumentIsDelete(vb, key);
}

/// Test persistence of a prepared & committed SyncWrite, a second prepared
/// & committed SyncWrite, followed by a prepared & committed SyncDelete.
TEST_P(DurabilityEPBucketTest, PersistSyncWriteSyncWriteSyncDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, 0, 0);

    // prepare SyncWrite and commit.
    testCommittedSyncWriteFlushAfterCommit(vb, "key", "value");
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, 0, 1);

    // Second prepare SyncWrite and commit.
    testCommittedSyncWriteFlushAfterCommit(vb, "key", "value2");
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, 0, 1);

    // prepare SyncDelete and commit.
    auto key = makeStoredDocKey("key");
    performPrepareSyncDelete(vb, key, 1, 1);
    auto prepareSeqno = vb.getHighSeqno();

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, key.getCollectionID(), 1);

    performCommitForKey(vb, key, prepareSeqno, 1, 1);

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, key.getCollectionID(), 0);

    // At persist-dedup, the 2nd Prepare and Commit survive.
    verifyDocumentIsDelete(vb, key);
}

/**
 * Test to check that our on disk count and collections count are tracked
 * correctly and do not underflow.
 *
 * This test does two rounds of SyncWrite then SyncDelete of a document with
 * the same key called "test_doc". Before the fix for MB-34094 and MB-34120 we
 * would expect our on disk counters to underflow and throw and exception.
 *
 * Note in this version of the test we flush after each commit made.
 */
TEST_P(DurabilityEPBucketTest,
       PersistSyncWriteSyncDeleteTwiceFlushAfterEachCommit) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, 0, 0);

    // First prepare SyncWrite and commit for test_doc.
    testCommittedSyncWriteFlushAfterCommit(vb, "test_doc", "{ \"run\": 1 }");
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, 0, 1);

    // First prepare SyncDelete and commit.
    testSyncDeleteFlushAfterCommit(vb, "test_doc");
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, 0, 0);

    // Second prepare SyncWrite and commit.
    testCommittedSyncWriteFlushAfterCommit(vb, "test_doc", "{ \"run\": 2 }");
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, 0, 1);

    // Second prepare SyncDelete and commit.
    testSyncDeleteFlushAfterCommit(vb, "test_doc");
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, 0, 0);
}

/**
 * Test to check that our on disk count and collections count are track
 * correctly and do not underflow.
 *
 * This test does two rounds of SyncWrite then SyncDelete of a document with
 * the same key called "test_doc". Before the fix for MB-34094 and MB-34120 we
 * would expect our on disk counters to underflow and throw and exception.
 *
 * Note in this version of the test we flush after all commits an prepares
 * have been made.
 */
TEST_P(DurabilityEPBucketTest,
       PersistSyncWriteSyncDeleteTwiceFlushAfterAllMutations) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    using namespace cb::durability;
    auto& vb = *store->getVBucket(vbid);
    auto* kvstore = vb.getShard()->getROUnderlying();

    std::string keyName("test_doc");
    auto key = makeStoredDocKey(keyName);
    auto keyCollectionID = key.getCollectionID();
    auto pending = makePendingItem(key, "{ \"run\": 1 }");

    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    // First prepare SyncWrite and commit for test_doc.
    performPrepareSyncWrite(vb, pending, 0, 0);
    auto prepareSeqno = vb.getHighSeqno();
    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    // check the value is correctly set on disk
    auto gv = kvstore->get(DiskDocKey(key), Vbid(0));
    ASSERT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // First prepare SyncDelete and commit.
    performPrepareSyncDelete(vb, key, 0, 0);
    prepareSeqno = vb.getHighSeqno();
    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    // check the value is correctly deleted on disk
    gv = kvstore->get(DiskDocKey(key), Vbid(0));
    ASSERT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Second prepare SyncWrite and commit.
    pending = makePendingItem(key, "{ \"run\": 2 }");
    performPrepareSyncWrite(vb, pending, 0, 0);
    prepareSeqno = vb.getHighSeqno();
    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    // check the value is correctly set on disk
    gv = kvstore->get(DiskDocKey(key), Vbid(0));
    ASSERT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Second prepare SyncDelete and commit.
    performPrepareSyncDelete(vb, key, 0, 0);
    prepareSeqno = vb.getHighSeqno();
    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    // flush the prepare and commit mutations to disk
    flushVBucketToDiskIfPersistent(vbid, 2);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    // check the value is correctly deleted on disk
    verifyDocumentIsDelete(vb, key);
}

/**
 * Test to check that our on disk count and collections count are track
 * correctly and do not underflow.
 *
 * This test does two rounds of SyncWrite then SyncDelete of a document with
 * the same key called "test_doc". Before the fix for MB-34094 and MB-34120 we
 * would expect our on disk counters to underflow and throw and exception.
 *
 * Note in this version of the test we flush after each commit and prepare made.
 */
TEST_P(DurabilityEPBucketTest,
       PersistSyncWriteSyncDeleteTwiceFlushAfterEachMutation) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    using namespace cb::durability;
    auto& vb = *store->getVBucket(vbid);

    std::string keyName("test_doc");
    auto key = makeStoredDocKey(keyName);
    auto keyCollectionID = key.getCollectionID();
    auto pending = makePendingItem(key, "{ \"run\": 1 }");

    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    // First prepare SyncWrite and commit for test_doc.
    performPrepareSyncWrite(vb, pending, 0, 0);
    auto prepareSeqno = vb.getHighSeqno();

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, keyCollectionID, 1);

    // check the value is correctly set on disk
    verifyDocumentIsStored(vb, key);

    // First prepare SyncDelete and commit.
    performPrepareSyncDelete(vb, key, 1, 1);
    prepareSeqno = vb.getHighSeqno();

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, keyCollectionID, 1);

    performCommitForKey(vb, key, prepareSeqno, 1, 1);

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    // check the value is correctly deleted on disk
    verifyDocumentIsDelete(vb, key);

    // Second prepare SyncWrite and commit.
    pending = makePendingItem(key, "{ \"run\": 2 }");
    performPrepareSyncWrite(vb, pending, 0, 0);
    prepareSeqno = vb.getHighSeqno();

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    performCommitForKey(vb, key, prepareSeqno, 0, 0);

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, keyCollectionID, 1);

    // check the value is correctly set on disk
    verifyDocumentIsStored(vb, key);

    // Second prepare SyncDelete and commit.
    performPrepareSyncDelete(vb, key, 1, 1);
    prepareSeqno = vb.getHighSeqno();

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 1);
    verifyCollectionItemCount(vb, keyCollectionID, 1);

    performCommitForKey(vb, key, prepareSeqno, 1, 1);

    flushVBucketToDiskIfPersistent(vbid, 1);
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, keyCollectionID, 0);

    // check the value is correctly deleted on disk
    verifyDocumentIsDelete(vb, key);
}

/**
 * Test to check that our on disk count and collections count are track
 * correctly and do not underflow.
 *
 * This test does 3 rounds of SyncWrite then SyncDelete of a document with
 * for a set of documents "test_doc-{0..9}". Before the fix for MB-34094 and
 * MB-34120 we would expect our on disk counters to underflow and throw and
 * exception.
 *
 * This performs multiple runs with ten documents as this allows us to perform
 * a sanity test that when we are setting and deleting more than one document
 * that our on disk accounting remain consistent.
 */
TEST_P(DurabilityEPBucketTest, PersistSyncWriteSyncDeleteTenDocs3Times) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    using namespace cb::durability;
    std::string keyName("test_doc-");
    auto& vb = *store->getVBucket(vbid);

    const uint32_t numberOfRuns = 3;
    const uint32_t numberOfDocks = 10;

    // Perform multiple runs of the creating and deletion of documents named
    // "test_doc-{0..9}".
    for (uint32_t j = 0; j < numberOfRuns; j++) {
        // Set and then delete ten documents names "test_doc-{0..9}"
        for (uint32_t i = 0; i < numberOfDocks; i++) {
            // prepare SyncWrite and commit.
            testCommittedSyncWriteFlushAfterCommit(
                    vb,
                    keyName + std::to_string(i),
                    "{ \"run\":" + std::to_string(j) + " }");
            verifyOnDiskItemCount(vb, 1);
            verifyCollectionItemCount(vb, 0, 1);

            // prepare SyncDelete and commit.
            testSyncDeleteFlushAfterCommit(vb, keyName + std::to_string(i));
            verifyOnDiskItemCount(vb, 0);
            verifyCollectionItemCount(vb, 0, 0);
        }
    }
}

/// Test to check that after 20 SyncWrites and then 20 SyncDeletes
/// that on disk count is 0.
/// Sanity test to make sure our accounting is consitant when we create
/// Multiple documents on disk.
TEST_P(DurabilityEPBucketTest, PersistSyncWrite20SyncDelete20) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    using namespace cb::durability;
    std::string keyName("test_doc-");
    auto& vb = *store->getVBucket(vbid);

    const uint32_t numberOfDocks = 20;
    // SyncWrite numberOfDocks docs
    for (uint32_t i = 0; i < numberOfDocks; i++) {
        // prepare SyncWrite and commit.
        testCommittedSyncWriteFlushAfterCommit(
                vb, keyName + std::to_string(i), R"({ "Hello": "World" })");

        {
            SCOPED_TRACE("flush sync write: " + std::to_string(i));
            verifyOnDiskItemCount(vb, i + 1);
            verifyCollectionItemCount(vb, 0, i + 1);
        }
    }
    // SyncDelete Docs
    for (uint32_t i = 0; i < numberOfDocks; i++) {
        testSyncDeleteFlushAfterCommit(vb, keyName + std::to_string(i));
        {
            SCOPED_TRACE("flush sync delete: " + std::to_string(i));
            verifyOnDiskItemCount(vb, numberOfDocks - i - 1);
            verifyCollectionItemCount(vb, 0, numberOfDocks - i - 1);
        }
    }
    verifyOnDiskItemCount(vb, 0);
    verifyCollectionItemCount(vb, 0, 0);
}

TEST_P(DurabilityEPBucketTest, ActiveLocalNotifyPersistedSeqno) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    const cb::durability::Requirements reqs = {
            cb::durability::Level::PersistToMajority, {}};

    for (uint8_t seqno = 1; seqno <= 3; seqno++) {
        auto item = makePendingItem(
                makeStoredDocKey("key" + std::to_string(seqno)), "value", reqs);
        ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
    }

    const auto& vb = store->getVBucket(vbid);
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
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      3 /*preparedSeqno*/));
    // Active has not persisted, so Durability Requirements not satisfied yet
    checkPending();

    // Flusher runs on Active. This:
    // - persists all pendings
    // - and notifies local DurabilityMonitor of persistence
    flushVBucketToDiskIfPersistent(vbid, 3);
    vb->processResolvedSyncWrites();

    // When seqno:1 is persisted:
    //
    // - the Flusher notifies the local DurabilityMonitor
    // - seqno:1 is satisfied, so it is committed
    // - the next committed seqnos are enqueued into the same open checkpoint
    ASSERT_EQ(1, ckptList.size());
    const auto& ckpt = *ckptList.front();
    EXPECT_EQ(6, ckpt.getNumItems());
    for (const auto& qi : ckpt) {
        if (!qi->isCheckPointMetaItem()) {
            queue_op op;
            if (qi->getBySeqno() / 4 == 0) {
                // The first three non-meta items/seqnos are prepares
                op = queue_op::pending_sync_write;
            } else {
                // The rest (last 3) are commits
                op = queue_op::commit_sync_write;
            }
            EXPECT_EQ(op, qi->getOperation());
        }
    }
}

TEST_P(DurabilityBucketTest, SetDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->set(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->set(*item, cookie));
}

TEST_P(DurabilityBucketTest, AddDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->add(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->add(*item, cookie));
}

TEST_P(DurabilityBucketTest, ReplaceDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->replace(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->replace(*item, cookie));
}

TEST_P(DurabilityBucketTest, DeleteDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");

    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    cb::durability::Requirements durabilityRequirements;
    durabilityRequirements.setLevel(cb::durability::Level::Majority);
    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                durabilityRequirements,
                                nullptr,
                                mutation_descr));

    durabilityRequirements.setLevel(cb::durability::Level::None);
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                durabilityRequirements,
                                nullptr,
                                mutation_descr));
}

template <typename F>
void DurabilityBucketTest::testDurabilityInvalidLevel(F& func) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    auto pending = makePendingItem(key, "value", reqs);
    EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));

    reqs = Requirements(Level::MajorityAndPersistOnMaster, {});
    pending = makePendingItem(key, "value", reqs);
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    }

    reqs = Requirements(Level::PersistToMajority, {});
    pending = makePendingItem(key, "value", reqs);
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    }
}

void DurabilityBucketTest::testTakeoverDestinationHandlesPreparedSyncWrites(
        cb::durability::Level level) {
    // Setup: VBucket into pending state with one Prepared SyncWrite.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_pending);

    auto& vb = *store->getVBucket(vbid);
    vb.checkpointManager->createSnapshot(
            1, 1, {} /*HCS*/, CheckpointType::Memory, 0);
    using namespace cb::durability;
    auto requirements = Requirements(level, Timeout::Infinity());
    auto pending =
            makePendingItem(makeStoredDocKey("key"), "value", requirements);
    pending->setCas(1);
    pending->setBySeqno(1);
    ASSERT_EQ(ENGINE_SUCCESS, store->prepare(*pending, nullptr));
    ASSERT_EQ(1, vb.getDurabilityMonitor().getNumTracked());

    // Test: Change to active via takeover (null topology),
    // then persist (including the prepared item above). This will trigger
    // the flusher to call back into ActiveDM telling it high prepared seqno
    // has advanced.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(
                      vbid, vbucket_state_active, {}, TransferVB::Yes));
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb.getDurabilityMonitor().getNumTracked())
            << "Should have 1 prepared SyncWrite if active+null topology";

    // Test: Set the topology (as ns_server does), by specifying just
    // a single node in topology should now be able to commit the prepare.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"active"}})}};
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_active, &meta));
    vb.processResolvedSyncWrites();

    // Given the prepare was already persisted to disk above when we first
    // changed to active, once a valid topology is set then SyncWrite should
    // be committed immediately irrespective of level.
    EXPECT_EQ(0, vb.getDurabilityMonitor().getNumTracked())
            << "Should have committed the SyncWrite if active+valid topology";
    // Should be able to flush Commit to disk.
    flushVBucketToDiskIfPersistent(vbid, 1);
}

TEST_P(DurabilityBucketTest, SetDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->set(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, AddDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->add(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, ReplaceDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->replace(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, DeleteDurabilityInvalidLevel) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    using namespace cb::durability;
    auto durabilityRequirements = Requirements(Level::Majority, {});

    auto del = [this](cb::durability::Requirements requirements)
            -> ENGINE_ERROR_CODE {
        auto key = makeStoredDocKey("key");
        uint64_t cas = 0;
        mutation_descr_t mutation_descr;
        return store->deleteItem(
                key, cas, vbid, cookie, requirements, nullptr, mutation_descr);
    };
    EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));

    durabilityRequirements =
            Requirements(Level::MajorityAndPersistOnMaster, {});
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    }

    durabilityRequirements = Requirements(Level::PersistToMajority, {});
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    }
}

/// MB_34012: Test that add() returns DurabilityImpossible if there's already a
/// SyncWrite in progress against a key, instead of returning EEXISTS as add()
/// would normally if it found an existing item. (Until the first SyncWrite
/// completes there's no user-visible value for the key.
TEST_P(DurabilityBucketTest, AddIfAlreadyExistsSyncWriteInProgress) {
    setVBucketToActiveWithValidTopology();

    // Setup: Add the first prepared SyncWrite.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, addPendingItem(*pending, cookie));

    // Test: Attempt to add a second prepared SyncWrite (different cookie i.e.
    // client).
    MockCookie secondClient(engine.get());
    auto pending2 = makePendingItem(key, "value2");
    EXPECT_EQ(ENGINE_SYNC_WRITE_IN_PROGRESS,
              store->add(*pending2, &secondClient));
}

/// MB-35042: Test that SyncDelete returns SYNC_WRITE_IN_PROGRESS if there's
/// already a SyncDelete in progress against a key, instead of returning
/// KEY_ENOENT as delete() would normally if it didn't find an existing item.
TEST_P(DurabilityBucketTest, DeleteIfDeleteInProgressSyncWriteInProgress) {
    setVBucketToActiveWithValidTopology();

    // Setup: Create a document, then start a SyncDelete.
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));
    uint64_t cas = 0;
    mutation_descr_t mutInfo;
    cb::durability::Requirements reqs{cb::durability::Level::Majority, {}};
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, mutInfo));

    // Test: Attempt to perform a second SyncDelete (different cookie i.e.
    // client).
    MockCookie secondClient(engine.get());
    cas = 0;
    EXPECT_EQ(ENGINE_SYNC_WRITE_IN_PROGRESS,
              store->deleteItem(
                      key, cas, vbid, &secondClient, reqs, nullptr, mutInfo));
}

/// MB-35042: Test that SyncDelete returns SYNC_WRITE_IN_PROGRESS if there's
/// already a SyncWrite in progress against a key, instead of returning
/// KEY_ENOENT as delete() would normally if it found a deleted item in the
/// HashTable.
TEST_P(DurabilityBucketTest, DeleteIfSyncWriteInProgressSyncWriteInProgress) {
    setVBucketToActiveWithValidTopology();

    // Setup: start a SyncWrite.
    auto key = makeStoredDocKey("key");
    auto committed = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*committed, cookie));

    // Test: Attempt to perform a second SyncDelete (different cookie i.e.
    // client).
    MockCookie secondClient(engine.get());
    uint64_t cas = 0;
    mutation_descr_t mutInfo;
    cb::durability::Requirements reqs{cb::durability::Level::Majority, {}};
    EXPECT_EQ(ENGINE_SYNC_WRITE_IN_PROGRESS,
              store->deleteItem(
                      key, cas, vbid, &secondClient, reqs, nullptr, mutInfo));
}

/// MB-35303: Test that after a SyncWrite Prepare is Aborted, a subsequent
/// SyncAdd succeeds (the abort doesn't block it).
TEST_P(DurabilityBucketTest, SyncAddAfterAbortedSyncWrite) {
    setVBucketToActiveWithValidTopology();

    // Setup: start a SyncWrite and then abort it.
    auto key = makeStoredDocKey("key");
    setupAbortedSyncWrite(key);

    // Test: Attempt to perform a SyncAdd. Should succeed as initial SyncWrite
    // was aborted.
    auto prepared2 = makePendingItem(key, "value2");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, addPendingItem(*prepared2, cookie));

    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(
            ENGINE_SUCCESS,
            vb.commit(
                    key, prepared2->getBySeqno(), {}, vb.lockCollections(key)));
}

/// MB-35303: Test that after a SyncWrite Prepare is Aborted, a subsequent
/// SyncReplace fails (the document doesn't exist yet so cannot replace).
TEST_P(DurabilityBucketTest, SyncReplaceAfterAbortedSyncWrite) {
    setVBucketToActiveWithValidTopology();

    // Setup: start a SyncWrite and then abort it.
    auto key = makeStoredDocKey("key");
    setupAbortedSyncWrite(key);

    // Test: Attempt to perform a SyncReplace. Should fails as initial SyncWrite
    // was aborted.
    auto prepared2 = makePendingItem(key, "value2");
    if (persistent() && fullEviction() && !bloomFilterEnabled()) {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->replace(*prepared2, cookie));
        runBGFetcherTask();
    }
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(*prepared2, cookie));
}

/// MB-35303: Test that after a SyncDelete Prepare is Aborted, a subsequent
/// SyncReplace succeeds (the abort doesn't block it).
TEST_P(DurabilityBucketTest, SyncReplaceAfterAbortedSyncDelete) {
    setVBucketToActiveWithValidTopology();

    // Setup: Create an item, start a SyncDelete and then abort it.
    auto key = makeStoredDocKey("key");
    auto mutation = makeCommittedItem(key, "value");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*mutation, cookie));
    // prepare and then abort a SyncDelete.
    setupAbortedSyncDelete(key);

    // Test: Attempt to perform a SyncReplace. Should succeed as SyncDelete was
    // aborted (so item still exists).
    auto prepared2 = makePendingItem(key, "value2");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->replace(*prepared2, cookie));
    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(
            ENGINE_SUCCESS,
            vb.commit(
                    key, prepared2->getBySeqno(), {}, vb.lockCollections(key)));
}

/// MB-35303: Test that after a SyncDelete Prepare is Aborted, a subsequent
/// SyncDelete succeeds (the abort doesn't block it).
TEST_P(DurabilityBucketTest, SyncDeleteAfterAbortedSyncDelete) {
    setVBucketToActiveWithValidTopology();

    // Setup: Create an item, start a SyncDelete and then abort it.
    auto key = makeStoredDocKey("key");
    auto mutation = makeCommittedItem(key, "value");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*mutation, cookie));
    // prepare and then abort a SyncDelete.
    setupAbortedSyncDelete(key);

    // Test: Attempt to perform another SyncDelete. Should succeed as initial
    // SyncDelete was aborted.
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    // Test: Should be able to Commit also.
    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(ENGINE_SUCCESS,
              vb.commit(key, delInfo.seqno, {}, vb.lockCollections(key)));

    // Item should no longer exist.
    auto gv = store->get(key, vbid, cookie, get_options_t());
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
}

/**
 * Test that the DurabilityCompletionTask correctly deals with a vBucket going
 * away.
 */
TEST_P(DurabilityBucketTest, RunCompletionTaskNoVBucket) {
    setVBucketToActiveWithValidTopology();

    auto task = std::make_shared<DurabilityCompletionTask>(*engine);
    if (persistent()) {
        auto* mockStore = static_cast<MockEPBucket*>(store);
        mockStore->setDurabilityCompletionTask(task);
    } else {
        auto* mockStore = static_cast<MockEphemeralBucket*>(store);
        mockStore->setDurabilityCompletionTask(task);
    }

    // Schedule the task so that we can run it later
    task_executor->schedule(task);

    // Make pending
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(key, "value");

    Vbid vbid = Vbid(0);
    pending->setVBucketId(vbid);

    // Store it
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    { // Scope for vbptr
        auto vb = store->getVBucket(vbid);
        auto& dm = vb->getDurabilityMonitor();

        EXPECT_EQ(0, dm.getHighCompletedSeqno());
        EXPECT_EQ(1, dm.getNumTracked());

        {
            auto rlh = folly::SharedMutex::ReadHolder(vb->getStateLock());
            vb->seqnoAcknowledged(rlh, "replica", 1);
        }

        // Not completed yet as we have not run the task
        EXPECT_EQ(0, vb->getHighCompletedSeqno());
        EXPECT_EQ(0, dm.getNumTracked());
    }

    // Delete the vBucket
    store->deleteVBucket(vbid, nullptr);

    // When the task runs, it should not segfault due to the vBucket having
    // been deleted.
    auto& taskQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(taskQ, task->getDescription());
}

void DurabilityBucketTest::takeoverSendsDurabilityAmbiguous(
        vbucket_state_t newState) {
    setVBucketToActiveWithValidTopology();
    using namespace cb::durability;

    // Store two keys, key1 is acknowledged, key2 is not.
    auto key1 = makeStoredDocKey("ack-me");
    auto pending1 = makePendingItem(key1, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending1, cookie));

    auto key2 = makeStoredDocKey("don't-ack-me");
    auto pending2 = makePendingItem(key2, "value");
    auto cookie2 = create_mock_cookie(engine.get());
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending2, cookie2));

    auto vb = store->getVBucket(vbid);
    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          pending1->getBySeqno());

    // We don't send ENGINE_SYNC_WRITE_PENDING to clients
    auto mockCookie = cookie_to_mock_cookie(cookie);
    auto mockCookie2 = cookie_to_mock_cookie(cookie2);

    EXPECT_EQ(ENGINE_SUCCESS, mockCookie->status);
    EXPECT_EQ(ENGINE_SUCCESS, mockCookie2->status);

    // Set state to dead
    EXPECT_EQ(ENGINE_SUCCESS, store->setVBucketState(vbid, newState));

    // We have set state to dead but we have not yet run the notification task
    EXPECT_EQ(ENGINE_SUCCESS, mockCookie->status);
    EXPECT_EQ(ENGINE_SUCCESS, mockCookie2->status);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(lpAuxioQ);

    // We should have told client the SyncWrite is ambiguous
    EXPECT_EQ(ENGINE_SYNC_WRITE_AMBIGUOUS, mockCookie->status);
    EXPECT_EQ(ENGINE_SYNC_WRITE_AMBIGUOUS, mockCookie2->status);

    destroy_mock_cookie(cookie2);
}

TEST_P(DurabilityBucketTest, TakeoverSendsDurabilityAmbiguous_replica) {
    takeoverSendsDurabilityAmbiguous(vbucket_state_replica);
}

TEST_P(DurabilityBucketTest, TakeoverSendsDurabilityAmbiguous_pending) {
    takeoverSendsDurabilityAmbiguous(vbucket_state_pending);
}

TEST_P(DurabilityBucketTest, TakeoverSendsDurabilityAmbiguous_dead) {
    takeoverSendsDurabilityAmbiguous(vbucket_state_dead);
}

TEST_F(DurabilityRespondAmbiguousTest, RespondAmbiguousNotificationDeadLock) {
    // Anecdotally this takes between 0.5 and 1s to run on my dev machine
    // (MB Pro 2017 - PCIe SSD). The test typically hits the issue on the 1st
    // run but sometimes takes up to 5. I didn't want to increase the number
    // of iterations as the test will obviously take far longer to run. If
    // this test ever causes a timeout - a deadlock issue (probably in the
    // RespondAmbiguousNotification task) is present.
    for (int i = 0; i < 100; i++) {
        KVBucketTest::SetUp();

        // We need a mock cookie which won't signal the engine when it
        // disconnects as we try to use it after the engine is deleted (
        // the full core will delete the cookie before the engine is killed)
        destroy_mock_cookie(cookie);
        cookie = create_mock_cookie();
        auto meta = nlohmann::json{
                {"topology", nlohmann::json::array({{"active", "replica"}})}};
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, vbucket_state_active, &meta));

        auto key = makeStoredDocKey("key");
        using namespace cb::durability;
        auto pending = makePendingItem(key, "value");

        // Store it
        EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

        // We don't send ENGINE_SYNC_WRITE_PENDING to clients
        auto mockCookie = cookie_to_mock_cookie(cookie);
        EXPECT_EQ(ENGINE_SUCCESS, mockCookie->status);

        // Set state to dead - this will schedule the task
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, vbucket_state_dead));

        // Deleting the vBucket will set the deferred deletion flag that
        // causes deadlock when the RespondAmbiguousNotification task is
        // destroyed as part of shutdown but is the last owner of the vBucket
        // (attempts to schedule destruction and tries to recursively lock a
        // mutex)
        {
            auto ptr = store->getVBucket(vbid);
            store->deleteVBucket(vbid, nullptr);
        }

        engine->getDcpConnMap().manageConnections();

        // Should deadlock here in ~SynchronousEPEngine
        engine.reset();

        // The RespondAmbiguousNotification task requires our cookie to still be
        // valid so delete it only after it has been destroyed
        destroy_mock_cookie(cookie);

        ExecutorPool::shutdown();

        // Cleanup any files we created.
        cb::io::rmrf(test_dbname);
    }
}

// Test that if a SyncWrite times out, then a subsequent SyncWrite which
// _should_ fail does indeed fail.
// (Regression test for part of MB-34367 - after using notify_IO_complete
// to report the SyncWrite was timed out with status eambiguous, the outstanding
// cookie context was not correctly cleared.
TEST_P(DurabilityBucketTest, MutationAfterTimeoutCorrect) {
    setVBucketToActiveWithValidTopology();

    // Setup: make pending item and store it; then abort it (at VBucket) level.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    uint64_t cas;
    ASSERT_EQ(ENGINE_EWOULDBLOCK,
              engine->store(cookie,
                            pending.get(),
                            cas,
                            StoreSemantics::Set,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive,
                            false));
    ASSERT_TRUE(engine->getEngineSpecific(cookie))
            << "Expected engine specific to be set for cookie after "
               "EWOULDBLOCK";

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       pending->getBySeqno(),
                       {},
                       vb.lockCollections(key),
                       cookie));

    // Test: Attempt another SyncWrite, which _should_ fail (in this case just
    // use replace against the same non-existent key).
    auto rc = engine->store(cookie,
                            pending.get(),
                            cas,
                            StoreSemantics::Replace,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive,
                            false);

    if (rc == ENGINE_EWOULDBLOCK && persistent() && fullEviction()) {
        runBGFetcherTask();
        rc = engine->store(cookie,
                           pending.get(),
                           cas,
                           StoreSemantics::Replace,
                           pending->getDurabilityReqs(),
                           DocumentState::Alive,
                           false);
    }
    EXPECT_EQ(ENGINE_KEY_ENOENT, rc);
}

// Test a durable set with CAS works when evicted. This checks that the set
// requires at least 3 attempst.
// 1) set -> ewouldblock, set needs item meta for cas check
// 2) set -> ewouldblock, set pending durability
// 3) set -> success (not included in the test)
TEST_P(DurabilityBucketTest, DurableEvictedSetWithCas) {
    if (!fullEviction()) {
        return;
    }

    setVBucketToActiveWithValidTopology();

    // 1) Non durable store with CAS
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value-1");
    uint64_t cas = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->store(cookie,
                            committed.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));

    // flush so that the hash-table allows eviction
    flushVBucketToDiskIfPersistent(vbid, 1);

    // and evict the key
    evict_key(vbid, key);

    // 2 Now do a durable SET with CAS
    auto pending = makePendingItem(key, "value-2");
    pending->setCas(cas);
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              engine->store(cookie,
                            pending.get(),
                            cas,
                            StoreSemantics::Set,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive,
                            false));

    // Must fetch at least the meta-data to process the SET with CAS
    runBGFetcherTask();

    // Now the set is accepted and pending durability
    // Prior to the resolution of MB-35932 this was returning SUCCESS
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              engine->store(cookie,
                            pending.get(),
                            cas,
                            StoreSemantics::Set,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive,
                            false));
}

TEST_P(DurabilityBucketTest,
       TakeoverDestinationHandlesPreparedSyncWriteMajority) {
    testTakeoverDestinationHandlesPreparedSyncWrites(
            cb::durability::Level::Majority);
}

TEST_P(DurabilityBucketTest,
       TakeoverDestinationHandlesPreparedyncWritePersistToMajority) {
    testTakeoverDestinationHandlesPreparedSyncWrites(
            cb::durability::Level::PersistToMajority);
}

// MB-34453: Block SyncWrites if there are more than this many replicas in the
// chain as we cannot guarantee no dataloss in a particular failover+rollback
// scenario.
TEST_P(DurabilityBucketTest, BlockSyncWritesIfMoreThan2Replicas) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array(
                      {{"active", "replica1", "replica2", "replica3"}})}});

    auto pre1 = makePendingItem(makeStoredDocKey("set"), "value");
    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->set(*pre1, cookie));

    auto pre2 = makePendingItem(makeStoredDocKey("add"), "value");
    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->add(*pre2, cookie));

    auto pre3 = makePendingItem(makeStoredDocKey("replace"), "value");
    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->replace(*pre3, cookie));
}

class FailOnExpiryCallback : public Callback<Item&, time_t&> {
public:
    void callback(Item& item, time_t& time) override {
        FAIL() << "Item was expired, nothing should be eligible for expiry";
    }
};

TEST_P(DurabilityEPBucketTest, DoNotExpirePendingItem) {
    /* MB-34768: the expiry time field of deletes has two uses - expiry time,
     * and deletion time (for use by the tombstone purger). This is true for
     * SyncDelete Prepares do too - BUT SyncDelete Prepares are not treated
     * as deleted (they are not tombstones yet) but are ALSO not eligible
     * for expiry, despite the expiry time field being set. Check that
     * compaction does not misinterpret the state of the prepare and try to
     * expire it.
     */
    setVBucketToActiveWithValidTopology();
    using namespace cb::durability;

    auto key1 = makeStoredDocKey("key1");
    auto req = Requirements(Level::Majority, Timeout(1000));

    auto key = makeStoredDocKey("key");
    // Store item normally
    queued_item qi{new Item(key, 0, 0, "value", 5)};
    EXPECT_EQ(ENGINE_SUCCESS, store->set(*qi, cookie));

    // attempt to sync delete it
    auto pending = makePendingItem(key, "value", req);
    pending->setDeleted(DeleteSource::Explicit);
    // expiry time is set *now*
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    flushVBucketToDiskIfPersistent(vbid, 2);

    CompactionConfig config;
    auto cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);

    cctx->expiryCallback = std::make_shared<FailOnExpiryCallback>();

    // Jump slightly forward, to ensure the new current time
    // is > expiry time of the delete
    TimeTraveller tt(1);

    auto* kvstore = store->getOneRWUnderlying();

    // Compact. Nothing should be expired
    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
    }

    // Check the committed item on disk.
    auto gv = kvstore->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(*qi, *gv.item);

    // Check the Prepare on disk
    DiskDocKey prefixedKey(key, true /*prepare*/);
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_TRUE(gv.item->isDeleted());
}

// @TODO Rocksdb when we have manual compaction/compaction filtering this test
// should be made to pass.
TEST_P(DurabilityEPBucketTest,
       DontRemoveUnCommittedPreparesAtCompaction) {
    if (isRocksDB()) {
        return;
    }
    setVBucketToActiveWithValidTopology();
    using namespace cb::durability;

    auto key = makeStoredDocKey("key");
    auto req = Requirements(Level::Majority, Timeout(1000));
    auto pending = makePendingItem(key, "value", req);
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    auto dummyKey = makeStoredDocKey("dummyKey");
    auto dummy = makeCommittedItem(dummyKey, "dummyValue");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(*dummy, cookie));

    flushVBucketToDiskIfPersistent(vbid, 2);

    CompactionConfig config;
    auto cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);
    cctx->expiryCallback = std::make_shared<FailOnExpiryCallback>();

    auto* kvstore = store->getOneRWUnderlying();

    // Sanity - prepare exists before compaction
    DiskDocKey prefixedKey(key, true /*prepare*/);
    auto gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    if (isMagma()) {
        // Magma doesn't track number of prepares
        EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(1, kvstore->getItemCount(vbid));
    } else {
        EXPECT_EQ(1, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(2, kvstore->getItemCount(vbid));
    }

    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
    }

    // Check the Prepare on disk
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    if (isMagma()) {
        // Magma doesn't track number of prepares
        EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(1, kvstore->getItemCount(vbid));
    } else {
        // Check onDiskPrepares is updated correctly too.
        EXPECT_EQ(1, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(2, kvstore->getItemCount(vbid));
    }

    auto vb = store->getVBucket(vbid);
    vb.reset();
    resetEngineAndWarmup();
    kvstore = store->getOneRWUnderlying();

    if (isMagma()) {
        // Magma doesn't track number of prepares
        EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(1, kvstore->getItemCount(vbid));
    } else {
        EXPECT_EQ(2, kvstore->getItemCount(vbid));
        EXPECT_EQ(1, kvstore->getVBucketState(vbid)->onDiskPrepares);
    }
}

// @TODO Rocksdb when we have manual compaction/compaction filtering this test
// should be made to pass.
TEST_P(DurabilityEPBucketTest, RemoveCommittedPreparesAtCompaction) {
    if (isRocksDB()) {
        return;
    }

    setVBucketToActiveWithValidTopology();
    using namespace cb::durability;

    auto key = makeStoredDocKey("key");
    auto req = Requirements(Level::Majority, Timeout(1000));
    auto pending = makePendingItem(key, "value", req);
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    auto vb = store->getVBucket(vbid);
    vb->commit(key,
               1 /*prepareSeqno*/,
               {} /*commitSeqno*/,
               vb->lockCollections(key));

    flushVBucketToDiskIfPersistent(vbid, 2);

    CompactionConfig config;
    auto cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);
    cctx->expiryCallback = std::make_shared<FailOnExpiryCallback>();

    auto* kvstore = store->getOneRWUnderlying();

    // Sanity - prepare exists before compaction
    DiskDocKey prefixedKey(key, true /*prepare*/);
    auto gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    if (isMagma()) {
        // Magma doesn't track number of prepares
        EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(1, kvstore->getItemCount(vbid));
    } else {
        EXPECT_EQ(1, kvstore->getVBucketState(vbid)->onDiskPrepares);
        EXPECT_EQ(2, kvstore->getItemCount(vbid));
    }

    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
    }
    // Check the committed item on disk.
    gv = kvstore->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ("value", gv.item->getValue()->to_s());

    // Check the Prepare on disk
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Check onDiskPrepares is updated correctly after compaction.
    EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
    EXPECT_EQ(1, kvstore->getItemCount(vbid));

    vb.reset();
    resetEngineAndWarmup();
    kvstore = store->getOneRWUnderlying();
    EXPECT_EQ(1, kvstore->getItemCount(vbid));
    EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
}

TEST_P(DurabilityEPBucketTest, RemoveAbortedPreparesAtCompaction) {
    if (isRocksDB()) {
        return;
    }

    setVBucketToActiveWithValidTopology();
    using namespace cb::durability;

    auto key = makeStoredDocKey("key");
    auto req = Requirements(Level::Majority, Timeout(1000));
    auto pending = makePendingItem(key, "value", req);
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    // Flush prepare
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    vb->abort(key,
              1 /*prepareSeqno*/,
              {} /*commitSeqno*/,
              vb->lockCollections(key));

    // We can't purge the last item so write a dummy
    auto dummyKey = makeStoredDocKey("dummy");
    auto dummyItem = makeCommittedItem(dummyKey, "dummyValue");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(*dummyItem, cookie));

    // Flush Abort and dummy
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto* kvstore = store->getOneRWUnderlying();
    EXPECT_EQ(1, kvstore->getItemCount(vbid));
    EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);

    CompactionConfig config;
    auto cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);
    cctx->expiryCallback = std::make_shared<FailOnExpiryCallback>();

    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
    }

    // Check the Abort on disk. We won't remove it until the purge interval has
    // passed because we need it to ensure we can resume a replica that had an
    // outstanding prepare within the purge interval.
    DiskDocKey prefixedKey(key, true /*prepare*/);
    auto gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    config.purge_before_ts = std::numeric_limits<uint64_t>::max();
    cctx = std::make_shared<CompactionContext>(Vbid(0), config, 0);
    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
    }

    // Now the Abort should be gone
    gv = kvstore->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
    EXPECT_EQ(1, kvstore->getItemCount(vbid));
    EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);

    vb.reset();
    resetEngineAndWarmup();
    kvstore = store->getOneRWUnderlying();
    EXPECT_EQ(1, kvstore->getItemCount(vbid));
    EXPECT_EQ(0, kvstore->getVBucketState(vbid)->onDiskPrepares);
}

TEST_P(DurabilityCouchstoreBucketTest, MB_36739) {
    // Replace RW kvstore and use a gmocked ops so we an inject failure
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);

    // Inject one fsync error when writing the pending mutation
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    setVBucketToActiveWithValidTopology();
    vbucket_state vbs = *store->getRWUnderlying(vbid)->getVBucketState(vbid);
    using namespace cb::durability;

    auto key = makeStoredDocKey("key");
    auto req = Requirements(Level::Majority, Timeout(1000));
    auto pending = makePendingItem(key, "value", req);
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    // Flush prepare, expect fail, then success on retry
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(0, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::No, res.wakeupCkptRemover);
    EXPECT_EQ(1, engine->getEpStats().commitFailed);
    EXPECT_EQ(0, engine->getEpStats().flusherCommits);
    EXPECT_EQ(vbs, *store->getRWUnderlying(vbid)->getVBucketState(vbid));

    res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::No, res.wakeupCkptRemover);
    EXPECT_EQ(1, engine->getEpStats().commitFailed);
    EXPECT_EQ(1, engine->getEpStats().flusherCommits);

    // Now expect that the vbucket state has been mutated by the flush
    vbucket_state newState =
            *store->getRWUnderlying(vbid)->getVBucketState(vbid);
    EXPECT_NE(vbs, newState);
    EXPECT_EQ(1, newState.persistedPreparedSeqno);
}

template <typename F>
void DurabilityEphemeralBucketTest::testPurgeCompletedPrepare(F& func) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    EXPECT_EQ(ENGINE_SUCCESS, func(vb, key));

    EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());

    TimeTraveller avenger(10000000);

    EphemeralVBucket::HTTombstonePurger purger(0);
    auto& evb = dynamic_cast<EphemeralVBucket&>(vb);
    purger.setCurrentVBucket(evb);
    evb.ht.visit(purger);

    EXPECT_EQ(0, vb.ht.getNumPreparedSyncWrites());
}

TEST_P(DurabilityEphemeralBucketTest, PurgeCompletedPrepare) {
    auto op = [this](VBucket& vb, StoredDocKey key) -> ENGINE_ERROR_CODE {
        return vb.commit(key,
                         1 /*prepareSeqno*/,
                         {} /*commitSeqno*/,
                         vb.lockCollections(key));
    };
    testPurgeCompletedPrepare(op);
}

TEST_P(DurabilityEphemeralBucketTest, PurgeCompletedAbort) {
    auto op = [this](VBucket& vb, StoredDocKey key) -> ENGINE_ERROR_CODE {
        return vb.abort(key,
                        1 /*prepareSeqno*/,
                        {} /*abortSeqno*/,
                        vb.lockCollections(key));
    };
    testPurgeCompletedPrepare(op);
}

// Test to confirm that prepares in state PrepareCommitted are not expired
TEST_P(DurabilityEphemeralBucketTest, CompletedPreparesNotExpired) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    const Vbid active_vb = Vbid(0);
    auto vb = engine->getVBucket(active_vb);

    const std::string value(1024, 'x'); // 1KB value to use for documents.

    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "value");

    using namespace std::chrono;
    auto expiry = system_clock::now() + seconds(1);

    item->setExpTime(system_clock::to_time_t(expiry));

    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    ASSERT_EQ(ENGINE_SUCCESS,
              vb->commit(key,
                         1 /*prepareSeqno*/,
                         {} /*commitSeqno*/,
                         vb->lockCollections(key)));

    TimeTraveller hgwells(10);

    std::shared_ptr<std::atomic<bool>> available;

    Configuration& cfg = engine->getConfiguration();
    std::unique_ptr<MockPagingVisitor> pv = std::make_unique<MockPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            EvictionRatios{0.0 /* active&pending */,
                           0.0 /* replica */}, // evict nothing
            available,
            EXPIRY_PAGER,
            false,
            VBucketFilter(),
            cfg.getItemEvictionAgePercentage(),
            cfg.getItemEvictionFreqCounterAgeThreshold());

    {
        auto pending = vb->ht.findForUpdate(key).pending;
        ASSERT_TRUE(pending);
        ASSERT_TRUE(pending->isCompleted());
        ASSERT_EQ(pending->getCommitted(), CommittedState::PrepareCommitted);
    }

    pv->setCurrentBucket(vb);
    for (int ii = 0; ii <= Item::initialFreqCount; ii++) {
        pv->setFreqCounterThreshold(0);
        vb->ht.visit(*pv);
        pv->update();
    }

    {
        auto pending = vb->ht.findForUpdate(key).pending;
        EXPECT_TRUE(pending);
        EXPECT_TRUE(pending->isCompleted());
    }
}

// Highlighted in MB-34997 was a situation where a vb state change meant that
// the new PDM had no knowledge of outstanding prepares that existed before the
// state change. This is fixed in VBucket by transferring the outstanding
// prepares from the ADM to the new PDM in such a switch over. This test
// demonstrates the issue and exercises the fix.
TEST_P(DurabilityBucketTest, ActiveToReplicaAndCommit) {
    setVBucketToActiveWithValidTopology();

    // seqno:1 A prepare, that does not commit yet.
    auto key = makeStoredDocKey("crikey");
    auto pending = makePendingItem(key, "pending");

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->set(*makePendingItem(makeStoredDocKey("crikey2"), "value2"),
                       cookie));

    flushVBucketToDiskIfPersistent(vbid, 2);
    validateHighAndVisibleSeqno(*store->getVBucket(vbid), 2, 0);

    // Now switch over to being a replica, via dead for realism
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead, {});

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica, {});
    auto& vb = *store->getVBucket(vbid);

    // Now drive the VB as if a passive stream is receiving data.
    vb.checkpointManager->createSnapshot(
            1, 3, {} /*HCS*/, CheckpointType::Memory, 0 /*MVS*/);

    // seqno:3 A new prepare
    auto key1 = makeStoredDocKey("crikey3");
    auto pending3 = makePendingItem(
            key1,
            "pending",
            {cb::durability::Level::Majority, cb::durability::Timeout(5000)});
    pending3->setCas(1);
    pending3->setBySeqno(3);
    EXPECT_EQ(ENGINE_SUCCESS, store->prepare(*pending3, cookie));
    // Trigger update of HPS (normally called by PassiveStream).
    vb.notifyPassiveDMOfSnapEndReceived(3);

    // seqno:4 the prepare at seqno:1 is committed
    vb.checkpointManager->createSnapshot(
            4, 4, {} /*HCS*/, CheckpointType::Memory, 0);
    ASSERT_EQ(ENGINE_SUCCESS, vb.commit(key, 1, 4, vb.lockCollections(key)));
}

TEST_P(DurabilityBucketTest, CasCheckMadeForNewPrepare) {
    setVBucketToActiveWithValidTopology();

    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "committed");

    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));

    auto pending = makePendingItem(key, "pending");
    pending->setCas(123);
    EXPECT_EQ(ENGINE_KEY_EEXISTS, store->set(*pending, cookie));

    pending->setCas(committed->getCas());
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));
}

TEST_P(DurabilityBucketTest, CompletedPreparesDoNotPreventDelWithMetaReplica) {
    // Test that a completed prepare does not prevent a deleteWithMeta
    // from correctly deleting the committed value.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica, {});
    const Vbid vbid = Vbid(0);
    auto vbucket = engine->getVBucket(vbid);

    uint64_t seqno = 1;
    // PREPARE
    vbucket->checkpointManager->createSnapshot(
            seqno, seqno, {}, CheckpointType::Memory, 0);

    const std::string value(1024, 'x'); // 1KB value to use for documents.

    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(
            key,
            "value",
            {cb::durability::Level::Majority, cb::durability::Timeout(1)});

    item->setCas();
    item->setBySeqno(seqno);

    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->setWithMeta(*item,
                                   0,
                                   &seqno,
                                   cookie,
                                   *engine,
                                   CheckConflicts::No,
                                   true,
                                   GenerateBySeqno::No,
                                   GenerateCas::No,
                                   vbucket->lockCollections(key)));

    ++seqno;
    // COMMIT
    vbucket->checkpointManager->createSnapshot(
            seqno, seqno, {}, CheckpointType::Memory, seqno);

    ASSERT_EQ(ENGINE_SUCCESS,
              vbucket->commit(key,
                              seqno - 1 /*prepareSeqno*/,
                              seqno /*commitSeqno*/,
                              vbucket->lockCollections(key)));

    // Check completed prepare is present
    if (!persistent()) {
        auto pending = vbucket->ht.findForUpdate(key).pending;
        ASSERT_TRUE(pending);
        ASSERT_TRUE(pending->isCompleted());
        ASSERT_EQ(pending->getCommitted(), CommittedState::PrepareCommitted);
    }

    ++seqno;
    // Try to deleteWithMeta
    vbucket->checkpointManager->createSnapshot(
            seqno, seqno, {}, CheckpointType::Memory, seqno);
    // expect the seqnos to be  @ the commit (which is seqno - 1)
    validateHighAndVisibleSeqno(*store->getVBucket(vbid), seqno - 1, seqno - 1);

    uint64_t cas = 0;
    ItemMetaData metadata;
    EXPECT_EQ(ENGINE_SUCCESS,
              vbucket->deleteWithMeta(cas,
                                      nullptr,
                                      cookie,
                                      *engine,
                                      CheckConflicts::No,
                                      metadata,
                                      GenerateBySeqno::No,
                                      GenerateCas::No,
                                      seqno /*seqno*/,
                                      vbucket->lockCollections(key),
                                      DeleteSource::TTL));

    EXPECT_FALSE(vbucket->ht.findForRead(key).storedValue);
}

/**
 * Test that we return a committed value when GetReplica requests a key
 * in the hashtable.
 * 1. Perform a set using normal mutation
 * 2. Switch vbucket to replica status
 * 3. Create a pending item for the key in the hashtable
 * 4. Check Get returns not my vbucket
 * 5. Perform a GetReplica again on the key and we should see the original
 * value and not the pending one.
 * 6. Switch vbucket to active
 * 7. Commit the pending item
 * 8. Check the commit worked by getting the committed item that was pending
 * 9. Switch vbucket to replica
 * 10. Check that GetReplica returns the once pending value.
 */
TEST_P(DurabilityBucketTest, GetReplicaWithPendingSyncWriteOnKey) {
    setVBucketToActiveWithValidTopology();

    // 1. Perform a set using normal mutation
    auto key = makeStoredDocKey("key");
    std::string initItemValue("value");
    store_item(vbid, key, initItemValue);

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    // 2. Switch vbucket to replica status
    setVBucketToReplicaAndPersistToDisk();

    // 3. Create a pending item for the key in the hashtable
    std::string pendingValue("pendingValue");
    storePreparedMaybeVisibleItem(key, pendingValue);

    // 4. Check Get returns not my vbucket
    auto getValue = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, getValue.getStatus());
    // 5. Perform a GetReplica again on the key and we should see the original
    // value and not the pending one.
    checkReplicaValue(key, initItemValue, options);

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    // 6. Switch vbucket to active
    setVBucketToActiveWithValidTopology();

    // 7. Commit the pending item. Can't just call the VBucket::commit function
    // here as we need the DM to be in the correct state.
    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(1, vb.getDurabilityMonitor().getNumTracked());
    vb.seqnoAcknowledged(
            folly::SharedMutex::ReadHolder(vb.getStateLock()), "replica", 2);
    vb.notifyActiveDMOfLocalSyncWrite();
    vb.processResolvedSyncWrites();
    EXPECT_EQ(0, vb.getDurabilityMonitor().getNumTracked());
    EXPECT_EQ(2, vb.getHighCompletedSeqno());

    // 8. Check the commit worked by getting the committed item that was pending
    auto getValueOfCommit = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, getValueOfCommit.getStatus());
    auto itemFromValue = *getValueOfCommit.item;
    EXPECT_FALSE(itemFromValue.isPending());
    EXPECT_TRUE(itemFromValue.isCommitted());
    EXPECT_EQ(pendingValue,
              std::string(itemFromValue.getData(), itemFromValue.getNBytes()));

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    // 9. Switch vbucket to replica
    setVBucketToReplicaAndPersistToDisk();
    // 10. Check that GetReplica returns the once pending value.
    checkReplicaValue(key, pendingValue, options);
}

/**
 * Test that we return a committed value from disk when GetReplica requests a
 * key that is committed value is evicted to disk but that has a prepared value
 * in the hashtable.
 * 1. Perform a set using normal mutation
 * 2. Evict the value and key to disk
 * 3. Switch vbucket to replica status
 * 4. Create a pending item for the key in the hashtable
 * 5. Check Get returns not my vbucket
 * 6. Check that without running bgfetch task that we get ewouldblock when
 * performing a GetReplica
 * 7. Run the bgfetch task
 * 8. Perform a GetReplica again on the key and we should see the original
 * value and not the pending one.
 */
TEST_P(DurabilityBucketTest, GetReplicaWithAnEvictedPendingSyncWriteOnKey) {
    if (!persistent()) {
        return;
    }
    setVBucketToActiveWithValidTopology();

    std::string initItemValue("value");
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, initItemValue);

    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    setVBucketToReplicaAndPersistToDisk();

    std::string pendingValue("pendingValue");
    storePreparedMaybeVisibleItem(key, pendingValue);

    auto getValue = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, getValue.getStatus());

    auto getReplicaValue = store->getReplica(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, getReplicaValue.getStatus());

    runBGFetcherTask();

    checkReplicaValue(key, initItemValue, options);
}

/**
 * Test to check that we return a committed item after a SyncWrite when
 * performing Get and GetReplica ops
 * 1. Create key and item and store as normal mutation
 * 2. Check we can access it
 * 3. Switch vbucket to a replica
 * 4. Perform Get to vbucket this should return not my vbucket
 * 5. Perform GetReplica to vbucket this should succeed with the committed value
 * being returned.
 */
TEST_P(DurabilityBucketTest, GetReplicaWithCommitedSyncWriteOnKey) {
    setVBucketToActiveWithValidTopology();

    std::string initItemValue("value");
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, initItemValue);

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    auto getValue = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, getValue.getStatus());
    EXPECT_FALSE(getValue.item->isPending());
    EXPECT_EQ(
            initItemValue,
            std::string(getValue.item->getData(), getValue.item->getNBytes()));

    EXPECT_EQ(ENGINE_SUCCESS,
              store->set(*makeCommittedItem(key, "CommittedItem"), cookie));

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    setVBucketToReplicaAndPersistToDisk();

    auto getSyncWriteValue = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, getSyncWriteValue.getStatus());

    checkReplicaValue(key, "CommittedItem", options);
}

/**
 * Test to check that we return the correct status codes when performing Get and
 * GetReplica ops
 * 1. Create key and item and store as normal mutation
 * 2. Switch vbucket to a replica
 * 3. Perform Get to vbucket this should return not my vbucket
 * 4. Perform GetReplica to vbucket this should succeed
 */
TEST_P(DurabilityBucketTest, GetAndGetReplica) {
    setVBucketToActiveWithValidTopology();

    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    setVBucketToReplicaAndPersistToDisk();

    auto getValue = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, getValue.getStatus());

    checkReplicaValue(key, "value", options);
}

/**
 * Test to check that we return the correct status codes if we don't honor
 * states when performing Get and GetReplica ops
 * 1. Create key and item and store as normal mutation
 * 2. Switch vbucket to a replica
 * 3. Perform Get to vbucket this should succeed as we're not honoring stats
 * 4. Perform GetReplica to vbucket this should succeed
 */
TEST_P(DurabilityBucketTest, GetAndGetReplicaDontHonorStates) {
    setVBucketToActiveWithValidTopology();

    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    setVBucketToReplicaAndPersistToDisk();

    auto getValue =
            store->get(key,
                       vbid,
                       cookie,
                       static_cast<get_options_t>(options ^ HONOR_STATES));
    EXPECT_EQ(ENGINE_SUCCESS, getValue.getStatus());
    EXPECT_FALSE(getValue.item->isPending());
    EXPECT_EQ(
            std::string("value"),
            std::string(getValue.item->getData(), getValue.item->getNBytes()));

    checkReplicaValue(key, "value", options);
}

// If a vbucket is changed from active when there are SyncWrites which have been
// resolved (i.e. we have decided to commit / abort) but *not* yet completed,
// then we need to put them back into trackedWrites. Previously we would
// complete them but this is incorrect as the DCP Stream will have already been
// set to dead so no Abort or Commit message will make it to the replica. In the
// case where this node becomes a replica, this node could have received a "re"
// Commit for the same key without having a Prepare in the DurabilityMonitor.
void DurabilityBucketTest::
        testResolvedSyncWritesReturnedToTrackedWritesVBStateChange(
                vbucket_state_t newState) {
    setVBucketToActiveWithValidTopology();

    // Setup: Make pending item, and simulate sufficient ACKs so it's in the
    // ResolvedQueue (but not yet Committed).
    using namespace cb::durability;
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING,
              store->set(*makePendingItem(makeStoredDocKey("key"),
                                          "value",
                                          {Level::Majority, Timeout(10000)}),
                         cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // ACK, locally and remotely, but *don't* process the resolved Queue yet.
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();

    // SyncWrite should now be in ResolvedQueue, but not yet Committed.
    auto key = makeStoredDocKey("key");
    {
        const auto sv = vb->ht.findForSyncWrite(key).storedValue;
        ASSERT_TRUE(sv);
        ASSERT_EQ(CommittedState::Pending, sv->getCommitted());
    }
    ASSERT_EQ(1, vb->getHighSeqno());

    // We have 0 items in trackedWrites (but 1 in the resolvedQueue)
    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    // Test: Change vbstate to non-active (dead which is what a takeover would
    // do. This should result in the resolved SyncWrite getting completed.
    store->setVBucketState(vbid, newState);

    // Check that the item is still pending in the HashTable. It will actually
    // be PreparedMaybeVisible as we have transitioned from active to non-active
    {
        const auto sv = vb->ht.findForWrite(key).storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::PreparedMaybeVisible, sv->getCommitted());
    }
    EXPECT_EQ(1, vb->getHighSeqno());

    const auto& dm = vb->getDurabilityMonitor();
    EXPECT_EQ(1, dm.getNumTracked());
    EXPECT_EQ(1, dm.getHighPreparedSeqno());
    EXPECT_EQ(0, dm.getHighCompletedSeqno());

    // Set us back to active so that we can check a few things in regards to the
    // state of the SynWrite objects.
    setVBucketToActiveWithValidTopology();
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vb);

    // A dead vbucket will keep the ADM but setting the topology will move
    // writes from trackedWrites to the completed queue as we will not touch the
    // ackCount. Deal with this separately so we can continue testing other
    // states
    if (newState == vbucket_state_dead) {
        EXPECT_EQ(0, adm.getNumTracked());
        EXPECT_EQ(1, adm.getHighPreparedSeqno());
        EXPECT_EQ(0, adm.getHighCompletedSeqno());

        adm.processCompletedSyncWriteQueue();
        EXPECT_EQ(1, adm.getHighCompletedSeqno());
        return;
    }

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(1, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());

    // We should not transfer the ack count
    adm.checkForCommit();
    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(1, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());

    // Or the cookies
    auto cookies = adm.getCookiesForInFlightSyncWrites();
    EXPECT_TRUE(cookies.empty());

    // We SHOULD have set the timeout to infinite
    adm.processTimeout(std::chrono::steady_clock::now() +
                       std::chrono::seconds(70));
    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(1, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
}

TEST_P(DurabilityBucketTest,
       ResolvedSyncWritesReturnedToTrackedWritesAtReplica) {
    testResolvedSyncWritesReturnedToTrackedWritesVBStateChange(
            vbucket_state_replica);
}

TEST_P(DurabilityBucketTest,
       ResolvedSyncWritesReturnedToTrackedWritesAtPending) {
    testResolvedSyncWritesReturnedToTrackedWritesVBStateChange(
            vbucket_state_pending);
}

TEST_P(DurabilityBucketTest, ResolvedSyncWritesReturnedToTrackedWritesAtDead) {
    testResolvedSyncWritesReturnedToTrackedWritesVBStateChange(
            vbucket_state_dead);
}

TEST_P(DurabilityBucketTest, getMetaReturnsRecommitInProgress) {
    // check that getMeta respects recommit in progress
    setVBucketToActiveWithValidTopology();

    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(key, "value");
    prepare->setPreparedMaybeVisible();
    store->set(*prepare, cookie);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto res = getMeta(vbid,
                       key,
                       cookie,
                       itemMeta,
                       deleted,
                       datatype,
                       false /* do not expect ewouldblock */);

    // Verify that GetMeta failed with recommit in progress
    ASSERT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, res);
}

TEST_P(DurabilityEPBucketTest, ActivePersistedDurabilitySeqnosAdvanceOnSyncWrites) {
    // In general, for an active VB the HPS and PPS will both be the seqno
    // of the most recent persisted prepare.
    // (exceptions being a recently promoted replica, or if the active is
    // ever changed to be able to persist partial snapshots).
    setVBucketToActiveWithValidTopology();
    // Store pending item, and simulate sufficient ACKs
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    store = engine->getKVBucket();
    KVStore* rwUnderlying = store->getRWUnderlying(vbid);
    const auto* persistedVbState = rwUnderlying->getVBucketState(vbid);
    auto& pcs = persistedVbState->persistedCompletedSeqno;
    auto& pps = persistedVbState->persistedPreparedSeqno;
    auto& hps = persistedVbState->highPreparedSeqno;

    // everything should be zero for now, no syncwrites
    // have occurred
    EXPECT_EQ(0, pcs);
    EXPECT_EQ(0, pps);
    EXPECT_EQ(0, hps);

    using namespace cb::durability;
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING,
              store->set(*makePendingItem(makeStoredDocKey("key"),
                                          "value",
                                          {Level::Majority, Timeout(10000)}),
                         cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // the prepare has not yet been completed, but has been
    // persisted to disk. the pps and hps should both
    // advance, and should be equal as the vb was active
    // at the time these values were flushed to disk.
    EXPECT_EQ(0, pcs);
    EXPECT_EQ(1, pps);
    EXPECT_EQ(1, hps);

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // the commit has been persisted, which should update the PCS
    EXPECT_EQ(1, pcs);
    EXPECT_EQ(1, pps);
    EXPECT_EQ(1, hps);
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, Mutation) {
    using namespace cb::durability;
    // mutation (add)
    ASSERT_EQ(ENGINE_SUCCESS,
              store->set(*makeCommittedItem(key, "value"), cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on mutation
    EXPECT_EQ(1, getMVS());

    // mutation (replace)
    ASSERT_EQ(ENGINE_SUCCESS,
              store->set(*makeCommittedItem(key, "value"), cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on mutation
    EXPECT_EQ(2, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, Deletion) {
    using namespace cb::durability;

    // mutation
    ASSERT_EQ(ENGINE_SUCCESS,
              store->set(*makeCommittedItem(key, "value"), cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on mutation
    EXPECT_EQ(1, getMVS());

    // deletion
    uint64_t cas = 0;
    mutation_descr_t delInfo;

    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key, cas, vbid, cookie, {}, nullptr, delInfo));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on deletion
    EXPECT_EQ(2, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, Expiry) {
    using namespace cb::durability;
    // mutation
    auto mutation = makeCommittedItem(key, "value");

    mutation->setExpTime(1);
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*mutation, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    TimeTraveller susan{2};

    // perform get to confirm expired and trigger writing a deletion
    auto gv = store->get(key, vbid, cookie, {});
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on expiry
    EXPECT_EQ(2, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, PrepareCommit) {
    using namespace cb::durability;

    // test with prepare & commit
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->set(*makePendingItem(
                               key, "value", {Level::Majority, Timeout(10000)}),
                       cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // prepare should not move maxVisibleSeqno
    EXPECT_EQ(0, getMVS());

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // commit should move maxVisibleSeqno
    EXPECT_EQ(2, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, PrepareAbort) {
    using namespace cb::durability;

    // test with prepare & abort
    ASSERT_EQ(
            ENGINE_SYNC_WRITE_PENDING,
            store->set(*makePendingItem(
                               key, "value", {Level::Majority, Timeout(10000)}),
                       cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // prepare should not move maxVisibleSeqno
    EXPECT_EQ(0, getMVS());

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // time out to abort
    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(10001));
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // abort should not maxVisibleSeqno
    EXPECT_EQ(0, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, PrepareDeleteCommit) {
    using namespace cb::durability;

    // test with prepare & commit
    auto pendingItem =
            makePendingItem(key, "value", {Level::Majority, Timeout(10000)});
    pendingItem->setDeleted(DeleteSource::Explicit);
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pendingItem, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // prepare should not move maxVisibleSeqno
    EXPECT_EQ(0, getMVS());

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // commit should move maxVisibleSeqno
    EXPECT_EQ(2, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, PrepareDeleteAbort) {
    using namespace cb::durability;

    // test with prepare & abort
    auto pendingItem =
            makePendingItem(key, "value", {Level::Majority, Timeout(10000)});
    pendingItem->setDeleted(DeleteSource::Explicit);
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pendingItem, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // prepare should not move maxVisibleSeqno
    EXPECT_EQ(0, getMVS());

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // time out to abort
    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(10001));
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // abort should not maxVisibleSeqno
    EXPECT_EQ(0, getMVS());
}

TEST_P(BackingStoreMaxVisibleSeqnoTest, PrepareCommitExpire) {
    using namespace cb::durability;

    // test with prepare & commit and expire
    auto prepare =
            makePendingItem(key, "value", {Level::Majority, Timeout(10000)});

    prepare->setExpTime(1);

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // prepare should not move maxVisibleSeqno
    EXPECT_EQ(0, getMVS());

    ASSERT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // commit should move maxVisibleSeqno
    EXPECT_EQ(2, getMVS());

    TimeTraveller barbara{2};

    // perform get to confirm expired and trigger writing a deletion
    auto gv = store->get(key, vbid, cookie, {});
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // should move on expiry
    EXPECT_EQ(3, getMVS());
}

TEST_P(DurabilityEPBucketTest, PrematureEvictionOfDirtyCommit) {
    using namespace cb::durability;

    // 1) Persist a prepare and complete it (without persisting the commit)
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(
            key, "value", {Level::PersistToMajority, Timeout(30)});
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    // 2) Evict the commit (manually as it's easier than running the pager)
    // Can't evict the item yet as it has been marked dirty. Before the fix the
    // item would be evicted successfully and a subsequent get would perform a
    // BGFetch and return KEY_ENOENT.
    const char* msg;
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, store->evictKey(key, vbid, &msg));

    // 3) Get returns the value without BGFetch (and not KEY_ENOENT)
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
}

TEST_P(DurabilityEPBucketTest, PrematureEvictionOfDirtyCommitExistingCommit) {
    using namespace cb::durability;

    // 1) Persist a prepare and commit to test that we don't perform a stale
    // read
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(
            key, "staleValue", {Level::PersistToMajority, Timeout(30)});
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Persist a prepare and complete it (without persisting the commit)
    prepare = makePendingItem(
            key, "value", {Level::PersistToMajority, Timeout(30)});
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // ACK, locally and remotely
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      3 /*preparedSeqno*/));
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();

    ASSERT_EQ(0, vb->getDurabilityMonitor().getNumTracked());

    // 3) Evict the commit (manually as it's easier than running the pager)
    // Can't evict the item yet as it has been marked dirty. Before the fix the
    // item would be evicted successfully and a subsequent get would perform a
    // BGFetch and return KEY_ENOENT.
    const char* msg;
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, store->evictKey(key, vbid, &msg));

    // 4) Get returns the new value without BGFetch (and does not return a stale
    // value).
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ("value", gv.item->getValue()->to_s());
}

// @TODO Rocksdb when we have manual compaction/compaction filtering this test
// should be made to pass.
TEST_P(DurabilityCouchstoreBucketTest,
       CompactionOfPrepareDoesNotAddToBloomFilter) {
    using namespace cb::durability;

    // 1) Persist a prepare but don't complete it
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(
            key, "value", {Level::PersistToMajority, Timeout(30)});
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    ASSERT_EQ(0, vb->getNumOfKeysInFilter());

    // Run compaction now, don't expect it to purge anything, just want to
    // process the items and swap the BloomFilters
    runCompaction(vbid);

    // Should not have added the prepare to the filter
    EXPECT_EQ(0, vb->getNumOfKeysInFilter());

    // A get should complete and return KEY_ENOENT without BGFetch
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
}

TEST_P(DurabilityEPBucketTest, MB_40480) {
    using namespace cb::durability;

    // 1) Persist a prepare but don't complete it yet
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(
        key, "original", {Level::PersistToMajority, Timeout(10000)});

    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Now abort our prepare but don't persist it yet
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
    std::chrono::milliseconds(10001));
    vb->processResolvedSyncWrites();

    EXPECT_EQ(1, vb->getSyncWriteAbortedCount());

    // 3) Grab the abort for persistence (in EPBucket::flushVBucket) but don't
    // actually complete the flush yet (i.e. call the commitCallback(s)).
    // state.
    ThreadGate tg1(2);
    ThreadGate tg2(2);

    auto* kvstore = store->getRWUnderlying(vbid);
    kvstore->setPostFlushHook([&tg1, &tg2]() {
        tg1.threadUp();
        tg2.threadUp();
    });

    std::thread flusher {[this](){
        flushVBucketToDiskIfPersistent(vbid, 1);
    }};

    tg1.threadUp();

    // 4) Do another prepare now on the same key so that the HashTable item is
    // updated
    prepare = makePendingItem(
            key, "can't delete", {Level::PersistToMajority, Timeout(30)});
    prepare->setDeleted();
    ASSERT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*prepare, cookie));

    // 5) Release the flusher thread stuck waiting to flush the abort.
    tg2.threadUp();
    flusher.join();

    // We should now have called the PersistenceCallback for the abort. Before
    // this bug was fixed we would delete the latest prepare from the HashTable
    // here causing "prepare not found" errors when we attempt to complete
    // this prepare.
    const auto res = vb->ht.findForUpdate(key);
    EXPECT_TRUE(res.pending);
    EXPECT_FALSE(res.committed);
}

TEST_P(DurabilityBucketTest, ObserveReturnsErrorIfRecommitInProgress) {
    // check that Observe respects recommit in progress
    setVBucketToActiveWithValidTopology();

    std::string keyMaybeVisible = "maybeVisible";
    std::string keyCommitted = "committed";

    // store a maybe visible prepare
    auto prepare = makePendingItem(makeStoredDocKey(keyMaybeVisible), "value");
    prepare->setPreparedMaybeVisible();
    store->set(*prepare, cookie);

    // store a committed item
    auto committed = makeCommittedItem(makeStoredDocKey(keyCommitted), "value");
    store->set(*committed, cookie);

    const auto dummyAddResponse = [](std::string_view key,
                                     std::string_view extras,
                                     std::string_view body,
                                     uint8_t datatype,
                                     cb::mcbp::Status status,
                                     uint64_t cas,
                                     const void* cookie) { return true; };

    auto requestPtr = createObserveRequest({keyCommitted});
    auto res = engine->observe(cookie, *requestPtr, dummyAddResponse);
    EXPECT_EQ(ENGINE_SUCCESS, res);

    // Verify that observing a maybe visble prepare causes
    // the entire Observe to fail
    requestPtr = createObserveRequest({keyMaybeVisible});
    res = engine->observe(cookie, *requestPtr, dummyAddResponse);
    EXPECT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, res);

    // a request with one prepared maybe visible key should still
    // fail the entire request
    requestPtr = createObserveRequest({keyMaybeVisible, keyCommitted});
    res = engine->observe(cookie, *requestPtr, dummyAddResponse);
    EXPECT_EQ(ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS, res);
}

void DurabilityBucketTest::testReplaceAtPendingSW(DocState docState) {
    if (!persistent()) {
        return;
    }

    // 1 replica node in topology, no SyncWrite will be ever completed unless
    // manually ack'ed.
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto key = makeStoredDocKey("key");
    auto item = makeCommittedItem(key, "value");

    ASSERT_TRUE(store);
    auto& vb = dynamic_cast<EPVBucket&>(*store->getVBucket(vbid));

    switch (docState) {
    case DocState::NOENT:
        break;
    case DocState::RESIDENT: {
        ASSERT_EQ(ENGINE_SUCCESS, store->set(*item, cookie));
        flush_vbucket_to_disk(vbid, 1 /*expectedNumFlused*/);
        break;
    }
    case DocState::EJECTED: {
        ASSERT_EQ(ENGINE_SUCCESS, store->set(*item, cookie));
        flush_vbucket_to_disk(vbid, 1 /*expectedNumFlused*/);
        {
            auto res = vb.ht.findForWrite(key);
            ASSERT_TRUE(res.storedValue);
        }
        auto cHandle = vb.lockCollections(key);
        ASSERT_TRUE(cHandle.valid());
        const auto buffer = std::make_unique<const char[]>(128);
        const char* msg = buffer.get();
        ASSERT_EQ(cb::mcbp::Status::Success, vb.evictKey(&msg, cHandle));
        ASSERT_TRUE(std::strcmp("Ejected.", msg) == 0);
        break;
    }
    }

    // Pending mutation for the same key
    auto pending = makePendingItem(key, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*pending, cookie));

    ENGINE_ERROR_CODE expectedRes;

    {
        const auto res = vb.ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);

        // Verify committed state in HT and residency
        switch (docState) {
        case DocState::NOENT: {
            ASSERT_FALSE(res.committed);
            ASSERT_EQ(0, vb.getNumTotalItems());
            if (fullEviction() && !bloomFilterEnabled()) {
                expectedRes = ENGINE_EWOULDBLOCK;
            } else {
                expectedRes = ENGINE_KEY_ENOENT;
            }
            break;
        }
        case DocState::RESIDENT: {
            ASSERT_TRUE(res.committed);
            ASSERT_EQ(1, vb.getNumTotalItems());
            expectedRes = ENGINE_SYNC_WRITE_IN_PROGRESS;
            break;
        }
        case DocState::EJECTED: {
            if (fullEviction()) {
                // The committed item is fully ejected, the bloom filter must
                // trigger a bg-fetch
                ASSERT_FALSE(res.committed);
                expectedRes = ENGINE_EWOULDBLOCK;
            } else {
                ASSERT_TRUE(res.committed);
                expectedRes = ENGINE_SYNC_WRITE_IN_PROGRESS;
            }
            ASSERT_EQ(1, vb.getNumTotalItems());
            break;
        }
        }
    }

    EXPECT_EQ(expectedRes, store->replace(*item, cookie));
}

TEST_P(DurabilityBucketTest, ReplaceAtPendingSW_DocEnoent) {
    testReplaceAtPendingSW(DocState::NOENT);
}

TEST_P(DurabilityBucketTest, ReplaceAtPendingSW_DocResident) {
    testReplaceAtPendingSW(DocState::RESIDENT);
}

TEST_P(DurabilityBucketTest, ReplaceAtPendingSW_DocEjected) {
    testReplaceAtPendingSW(DocState::EJECTED);
}

TEST_P(DurabilityBucketTest, SetMinDurabilityLevel_UnknownLevel) {
    auto& config = engine->getConfiguration();
    try {
        config.setDurabilityMinLevel("non-existing-level");
    } catch (const std::range_error& e) {
        ASSERT_TRUE(std::string(e.what()).find(
                            "Validation Error, durability_min_level") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

void DurabilityBucketTest::testSetMinDurabilityLevel(
        cb::durability::Level level) {
    using namespace cb::durability;

    ASSERT_EQ(Level::None, store->getMinDurabilityLevel());

    const auto checkSuccessfulSet = [this, level]() -> void {
        ASSERT_EQ(ENGINE_SUCCESS, store->setMinDurabilityLevel(level));
        ASSERT_EQ(level, store->getMinDurabilityLevel());
    };

    switch (level) {
    case Level::None:
    case Level::Majority: {
        checkSuccessfulSet();
        break;
    }
    case Level::MajorityAndPersistOnMaster:
    case Level::PersistToMajority: {
        if (persistent()) {
            checkSuccessfulSet();
        } else {
            ASSERT_EQ(ENGINE_DURABILITY_INVALID_LEVEL,
                      store->setMinDurabilityLevel(level));
        }
        break;
    }
    }
}

TEST_P(DurabilityBucketTest, SetMinDurabilityLevel_None) {
    testSetMinDurabilityLevel(cb::durability::Level::None);
}

TEST_P(DurabilityBucketTest, SetMinDurabilityLevel_Majority) {
    testSetMinDurabilityLevel(cb::durability::Level::Majority);
}

TEST_P(DurabilityBucketTest, SetMinDurabilityLevel_MajorityAndPersistOnMaster) {
    testSetMinDurabilityLevel(
            cb::durability::Level::MajorityAndPersistOnMaster);
}

TEST_P(DurabilityBucketTest, SetMinDurabilityLevel_PersistToMajority) {
    testSetMinDurabilityLevel(cb::durability::Level::PersistToMajority);
}

void DurabilityBucketTest::testUpgradeToMinDurabilityLevel(
        cb::durability::Level minLevel,
        std::optional<cb::durability::Level> writeLevel,
        EngineOp engineOp) {
    using namespace cb::durability;

    // Do not execute Ephemeral for persistence levels
    if (ephemeral()) {
        if (minLevel > Level::Majority) {
            return;
        }
        if (writeLevel && *writeLevel > Level::Majority) {
            return;
        }
    }

    // * SETUP *
    // Avoid that Prepares are committed as soon as queued
    setVBucketToActiveWithValidTopology();
    // If we are testing Remove, the we need to add the document that is deleted
    // later in the test.
    // Need to perform this step before we set any MinLevel > None, the insert
    // would be turned into a SyncWrite otherwise
    const auto key = makeStoredDocKey("key");
    if (engineOp == EngineOp::Remove) {
        ASSERT_EQ(Level::None, store->getMinDurabilityLevel());
        const auto item = makeCommittedItem(key, "value");
        uint64_t cas = 0;
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->store(cookie,
                                item.get(),
                                cas,
                                StoreSemantics::Set,
                                {} /*durReqs*/,
                                DocumentState::Alive,
                                false /*preserveTtl*/));
    }
    // Set the bucket min-level that we want to test
    ASSERT_EQ(ENGINE_SUCCESS, store->setMinDurabilityLevel(minLevel));
    ASSERT_EQ(minLevel, store->getMinDurabilityLevel());

    // * PRE-CONDITIONS *
    auto& vb = *store->getVBucket(vbid);
    auto& ht = vb.ht;
    {
        const auto res = ht.findForUpdate(key);
        ASSERT_EQ(engineOp != EngineOp::Remove, res.committed == nullptr);
        ASSERT_FALSE(res.pending);
    }

    // * TEST - write the document *
    const auto item = makeCommittedItem(key, "value");
    // Durability Requirements for the write:
    // - Reqs{writeLevel, some-timeout} if writeLevel provided
    // - No reqs (ie, normal write) otherwise
    const auto timeout = Timeout(54321);
    const auto reqs =
            writeLevel ? std::optional<Requirements>({*writeLevel, timeout})
                       : std::optional<Requirements>();
    switch (engineOp) {
    case EngineOp::Store: {
        uint64_t cas = 0;
        ASSERT_EQ(ENGINE_EWOULDBLOCK,
                  engine->store(cookie,
                                item.get(),
                                cas,
                                StoreSemantics::Set,
                                reqs,
                                DocumentState::Alive,
                                false));
        break;
    }
    case EngineOp::StoreIf: {
        const cb::StoreIfPredicate predicate =
                [](const std::optional<item_info>&, cb::vbucket_info) {
                    return cb::StoreIfStatus::Continue;
                };
        const auto res = engine->store_if(cookie,
                                          item.get(),
                                          0 /*cas*/,
                                          StoreSemantics::Set,
                                          predicate,
                                          reqs,
                                          DocumentState::Alive,
                                          false);
        ASSERT_EQ(cb::engine_errc::would_block, res.status);
        break;
    }
    case EngineOp::Remove: {
        uint64_t cas = 0;
        mutation_descr_t info;
        ASSERT_EQ(ENGINE_EWOULDBLOCK,
                  engine->remove(cookie, key, cas, vbid, reqs, info));
        break;
    }
    }

    // * POST-CONDITIONS - item must be queued in CM with the expected DurReqs *
    ASSERT_TRUE(engine->getEngineSpecific(cookie));
    {
        const auto res = ht.findForUpdate(key);
        ASSERT_EQ(engineOp != EngineOp::Remove, res.committed == nullptr);
        ASSERT_TRUE(res.pending);
    }

    auto& manager = *vb.checkpointManager;
    const auto expectedHighSeqno = engineOp != EngineOp::Remove ? 1 : 2;
    ASSERT_EQ(expectedHighSeqno, manager.getHighSeqno());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, ckptList.size());
    const auto& ckpt = *ckptList.front();
    ASSERT_EQ(2, ckpt.getNumMetaItems());
    const auto expectedNumItems = engineOp != EngineOp::Remove ? 1 : 2;
    ASSERT_EQ(expectedNumItems, ckpt.getNumItems());
    // Skip empty-item, checkpoint-start and set-vbstate
    auto it = ckpt.begin();
    it++;
    it++;
    it++;
    // We must have the committed from the insert if we are testing a deletion
    if (engineOp == EngineOp::Remove) {
        EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
        EXPECT_EQ(1, (*it)->getBySeqno());
        EXPECT_FALSE((*it)->isDeleted());
        it++;
    }
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ(expectedHighSeqno, (*it)->getBySeqno());
    EXPECT_EQ(engineOp == EngineOp::Remove, (*it)->isDeleted());

    const auto& itemReqs = (*it)->getDurabilityReqs();

    Level expectedLevel;
    Timeout expectedTimeout;
    if (!writeLevel) {
        // NormalWrite -> minLevel >= writeLevel by logic, so we expect
        // always whatever minLevel
        expectedLevel = minLevel;
        // No user-timeout for the original NormalWrite
        expectedTimeout = Timeout();
    } else {
        // SyncWrite -> we expect the max(minLevel, writeLevel)
        expectedLevel = (*writeLevel > minLevel ? *writeLevel : minLevel);
        // Timeout unchanged
        expectedTimeout = timeout;
    }
    EXPECT_EQ(expectedLevel, itemReqs.getLevel());
    EXPECT_EQ(expectedTimeout.get(), itemReqs.getTimeout().get());
}

// Representative of any combination of {minLevel, writeLevel} with
// (minLevel < writeLevel)
// Note: In this scenario, by definition the write is a SyncWrite
TEST_P(DurabilityBucketTest, UpgradeToMinLevel_None_Majority_Store) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::None /*minLevel*/,
                                    {Level::Majority} /*writeLevel*/,
                                    EngineOp::Store);
}
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_None_MajorityAndPersistOnMaster_StoreIf) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::None,
                                    {Level::MajorityAndPersistOnMaster},
                                    EngineOp::StoreIf);
}
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_MajorityAndPersistOnMaster_PersistToMajority_Remove) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::MajorityAndPersistOnMaster,
                                    {Level::PersistToMajority},
                                    EngineOp::Remove);
}

// Representative of any combination of {minLevel, writeLevel} with
// (minLevel > writeLevel) AND the write is a NormalWrite
TEST_P(DurabilityBucketTest, UpgradeToMinLevel_Majority_None_Store) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::Majority, {}, EngineOp::Store);
}
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_MajorityAndPersistOnMaster_None_StoreIf) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(
            Level::MajorityAndPersistOnMaster, {}, EngineOp::StoreIf);
}
TEST_P(DurabilityBucketTest, UpgradeToMinLevel_PersistToMajority_None_Remove) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(
            Level::PersistToMajority, {}, EngineOp::Remove);
}

// Representative of any combination of {minLevel, writeLevel} with
// (minLevel > writeLevel) AND the write is a SyncWrite
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_PersistToMajority_Majority_Store) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::MajorityAndPersistOnMaster,
                                    Level::Majority,
                                    EngineOp::Store);
}
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_PersistToMajority_MajorityAndPersistOnMaster_StoreIf) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(Level::PersistToMajority,
                                    Level::MajorityAndPersistOnMaster,
                                    EngineOp::StoreIf);
}
TEST_P(DurabilityBucketTest,
       UpgradeToMinLevel_PersistToMajority_Majority_Remove) {
    using namespace cb::durability;
    testUpgradeToMinDurabilityLevel(
            Level::PersistToMajority, Level::Majority, EngineOp::Remove);
}

TEST_P(DurabilityBucketTest, PrepareDoesNotExpire) {
    using namespace cb::durability;

    // Avoid that Prepares are committed as soon as queued
    setVBucketToActiveWithValidTopology();

    auto& vb = *store->getVBucket(vbid);
    auto& ht = vb.ht;
    const auto key = makeStoredDocKey("key");
    {
        const auto res = ht.findForUpdate(key);
        ASSERT_FALSE(res.committed);
        ASSERT_FALSE(res.pending);
    }

    // Load a SyncWrite with exptime != 0
    const auto item = makePendingItem(
            key, "value", Requirements(Level::Majority, Timeout::Infinity()));
    item->setExpTime(ep_real_time() + 3600);
    item->setPreparedMaybeVisible();
    uint64_t cas = 0;
    ASSERT_EQ(ENGINE_EWOULDBLOCK,
              engine->store(cookie,
                            item.get(),
                            cas,
                            StoreSemantics::Set,
                            item->getDurabilityReqs(),
                            DocumentState::Alive,
                            false /*preserveTTL*/));

    {
        const auto res = ht.findForRead(key);
        ASSERT_TRUE(res.storedValue);
        EXPECT_TRUE(res.storedValue->isPreparedMaybeVisible());
        EXPECT_FALSE(res.storedValue->isDeleted());
    }

    // Now access the StoredValue via the expiry code path, must NOT expire has
    // TTL not reached
    const auto checkNotExpired = [&vb, &key]() -> void {
        auto res = vb.fetchValidValue(WantsDeleted::No,
                                      TrackReference::No,
                                      QueueExpired::Yes,
                                      vb.lockCollections(key));
        ASSERT_TRUE(res.storedValue);
        EXPECT_TRUE(res.storedValue->isPreparedMaybeVisible());
        EXPECT_FALSE(res.storedValue->isDeleted());
    };
    checkNotExpired();

    // TimeTravel to expire
    TimeTraveller tt(3601);

    // Again, must NOT expire as the still in pending Prepare state.
    checkNotExpired();

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key, 1 /*prepareSeqno*/, {}, vb.lockCollections(key)));

    // Note: The next call to VBucket::fetchValidValue needs the test engine in
    // ObjectRegistry as it makes a call to
    // ObjectRegistry::getCurrentEngine()->getServerApi()->doc->pre_expiry(item)
    ObjectRegistry::onSwitchThread(engine.get());

    // Item committed, TTL must kick in
    auto res = vb.fetchValidValue(WantsDeleted::Yes,
                                  TrackReference::No,
                                  QueueExpired::Yes,
                                  vb.lockCollections(key));
    ASSERT_TRUE(res.storedValue);
    EXPECT_TRUE(res.storedValue->isCommitted());
    EXPECT_TRUE(res.storedValue->isDeleted());
    EXPECT_EQ(DeleteSource::TTL, res.storedValue->getDeletionSource());
}

// Test cases which run against couchstore
INSTANTIATE_TEST_SUITE_P(AllBackends,
                         DurabilityCouchstoreBucketTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run against all persistent storage backends.
INSTANTIATE_TEST_SUITE_P(
        AllBackends,
        DurabilityEPBucketTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run against all ephemeral.
INSTANTIATE_TEST_SUITE_P(AllBackends,
                         DurabilityEphemeralBucketTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run against all configurations.
INSTANTIATE_TEST_SUITE_P(AllBackends,
                         DurabilityBucketTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

// maxVisibleSeqno tests run against all persistent storage backends.
INSTANTIATE_TEST_SUITE_P(AllBackends,
                         BackingStoreMaxVisibleSeqnoTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
