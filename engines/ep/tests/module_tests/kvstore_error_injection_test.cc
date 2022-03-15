/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../couchstore/src/internal.h"
#include "../mock/mock_magma_kvstore.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "test_helpers.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/test_fileops.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <folly/portability/GMock.h>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

/**
 * Error injector interface with implementations for each KVStore that we care
 * to test. This class/test fixture serves a different purpose to the
 * MockKVStore which we use to test Bucket/VBucket code. The purpose of the
 * ErrorInjector class/test fixture is to test changes in KVStore et. al. by
 * injecting errors after we run as much KVStore code as possible.
 */
class ErrorInjector {
public:
    virtual ~ErrorInjector() = default;

    /**
     * Make the next KVStore::commit (flush) operation fail
     */
    virtual void failNextCommit() = 0;

    /**
     * Make the next KVStore::snapshotVBucket (flush vbstate only) operation
     * fail
     */
    virtual void failNextSnapshotVBucket() = 0;
};

class CouchKVStoreErrorInjector : public ErrorInjector {
public:
    CouchKVStoreErrorInjector(KVBucketTest& test)
        : ops(create_default_file_ops()) {
        test.replaceCouchKVStore(ops);
    }

    void failNextCommit() override {
        using namespace testing;
        EXPECT_CALL(ops, sync(_, _))
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .WillRepeatedly(Return(COUCHSTORE_SUCCESS));
    }

    void failNextSnapshotVBucket() override {
        using namespace testing;
        EXPECT_CALL(ops, sync(_, _))
                .WillOnce(Return(COUCHSTORE_ERROR_WRITE))
                .WillRepeatedly(Return(COUCHSTORE_SUCCESS));
    }

protected:
    ::testing::NiceMock<MockOps> ops;
};

#ifdef EP_USE_MAGMA
class MagmaKVStoreErrorInjector : public ErrorInjector {
public:
    MagmaKVStoreErrorInjector(KVBucketTest& test) {
        test.replaceMagmaKVStore();
        kvstore = dynamic_cast<MockMagmaKVStore*>(
                test.store->getRWUnderlying(test.vbid));
    }

    void failNextCommit() override {
        kvstore->saveDocsErrorInjector = [this](VB::Commit& cmt,
                                                kvstats_ctx& ctx) -> int {
            kvstore->saveDocsErrorInjector = nullptr;
            return magma::Status::IOError;
        };
    }

    void failNextSnapshotVBucket() override {
        kvstore->snapshotVBucketErrorInjector = [this]() {
            kvstore->snapshotVBucketErrorInjector = nullptr;
            return false;
        };
    }

    MockMagmaKVStore* kvstore;
};
#endif

class KVStoreErrorInjectionTest : public STParamPersistentBucketTest {
public:
    void SetUp() override {
        STParamPersistentBucketTest::SetUp();
        createErrorInjector();
    }

    void createErrorInjector() {
        if (isCouchstore()) {
            errorInjector = std::make_unique<CouchKVStoreErrorInjector>(*this);
#ifdef EP_USE_MAGMA
        } else if (isMagma()) {
            errorInjector = std::make_unique<MagmaKVStoreErrorInjector>(*this);
#endif
        } else {
            throw std::invalid_argument(
                    "KVStoreErrorInjectionTest::createErrorInjector invalid "
                    "backend");
        }
    }

    /**
     * All the tests below check that we don't lose any item, any vbstate and
     * that we update flush-stats properly when flush fails and we re-attempt
     * the flush later.
     *
     * @param vbDeletion Some tests get this additional arg to verify that all
     *  goes as expected when the flusher processes VBuckets set for deferred
     *  deletion
     */
    void testFlushFailureStatsAtDedupedNonMetaItems(bool vbDeletion = false);
    void testFlushFailureAtPersistDelete(bool vbDeletion = false);

protected:
    std::unique_ptr<ErrorInjector> errorInjector;
};

TEST_P(KVStoreErrorInjectionTest, ItemCountsAndCommitFailure_MB_41321) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    auto stats =
            vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(1, stats.highSeqno);
    EXPECT_NE(0, stats.diskSize);

    // Delete our key
    delete_item(vbid, key);

    // Expect the flush of our delete to fail twice. This would see an underflow
    // exception before the fix for MB-41321 as we would decrement the item
    // count from 1 to 0 and then try for -1
    auto flushAndExpectFailure = [this](int expectedCommitFailed) {
        auto flushResult = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        EXPECT_EQ(EPBucket::MoreAvailable::Yes, flushResult.moreAvailable);
        EXPECT_EQ(0, flushResult.numFlushed);
        EXPECT_EQ(EPBucket::WakeCkptRemover::No, flushResult.wakeupCkptRemover);
        EXPECT_EQ(expectedCommitFailed, engine->getEpStats().commitFailed);
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // validate the default collection hasn't changed
        auto stats = vb->getManifest()
                             .lock(CollectionID::Default)
                             .getPersistedStats();
        EXPECT_EQ(1, stats.itemCount);
        EXPECT_EQ(1, stats.highSeqno);
        EXPECT_NE(0, stats.diskSize);
    };

    errorInjector->failNextCommit();
    flushAndExpectFailure(1);

    errorInjector->failNextCommit();
    flushAndExpectFailure(2);

    // Now a successful flush which will update the stats
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::No, res.wakeupCkptRemover);
    EXPECT_EQ(2, engine->getEpStats().commitFailed);
    stats = vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(2, stats.highSeqno);

    if (isMagma()) {
        // Magma does not track tombstones in disk size as compaction may visit
        // stale values so it's not possible to decrement on purge
        EXPECT_EQ(0, stats.diskSize);
    } else {
        EXPECT_GT(stats.diskSize, 0); // tombstone data remains
    }
}

TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPersistingCollectionChange) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm(CollectionEntry::dairy);
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    // Check nothing persisted to disk, only default collection exists
    auto* kvstore = store->getRWUnderlying(vbid);
    auto [s1, m1] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s1);
    EXPECT_EQ(1, m1.collections.size());
    const Collections::CollectionMetaData defaultState;
    EXPECT_EQ(defaultState, m1.collections[0].metaData);
    EXPECT_EQ(0, m1.collections[0].startSeqno);
    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    {
        errorInjector->failNextCommit();
        EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
                  epBucket.flushVBucket(vbid));
        // Flush stats not updated
        EXPECT_EQ(1, vb->dirtyQueueSize);
    }

    // Check nothing persisted to disk, only default collection exists
    auto [s2, m2] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s2);
    EXPECT_EQ(1, m2.collections.size());
    EXPECT_EQ(defaultState, m2.collections[0].metaData);
    EXPECT_EQ(0, m2.collections[0].startSeqno);

    // This flush succeeds
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb->dirtyQueueSize);

    auto [s3, m3] = kvstore->getCollectionsManifest(vbid);
    ASSERT_TRUE(s3);
    EXPECT_EQ(2, m3.collections.size());

    Collections::CollectionMetaData dairyState{ScopeID::Default,
                                               CollectionEntry::dairy,
                                               CollectionEntry::dairy.name,
                                               {/*no ttl*/}};
    // no ordering of returned collections, both default and dairy must exist
    for (const auto& c : m3.collections) {
        if (c.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(c.metaData, defaultState);
            EXPECT_EQ(0, c.startSeqno);
        } else {
            EXPECT_EQ(c.metaData, dairyState);
            EXPECT_EQ(1, c.startSeqno);
        }
    }
}

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 */
TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPersistNonMetaItems) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Active receives PRE(keyA):1, M(keyB):2, D(keyB):3
    // Note that the set of mutation is just functional to testing that we write
    // to disk all the required vbstate entries at flush
    const std::string valueA = "valueA";
    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   valueA,
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {cb::durability::Requirements()});
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "valueB",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    delete_item(vbid, makeStoredDocKey("keyB"));

    // M(keyB):2 deduplicated, just 2 items for cursor
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(2, vb.checkpointManager->getNumItemsForPersistence());
    EXPECT_EQ(2, vb.dirtyQueueSize);

    const auto checkPreFlushHTState = [&vb]() -> void {
        const auto resA = vb.ht.findForUpdate(makeStoredDocKey("keyA"));
        ASSERT_TRUE(resA.pending);
        ASSERT_FALSE(resA.pending->isDeleted());
        ASSERT_TRUE(resA.pending->isDirty());
        ASSERT_FALSE(resA.committed);

        const auto resB = vb.ht.findForUpdate(makeStoredDocKey("keyB"));
        ASSERT_FALSE(resB.pending);
        ASSERT_TRUE(resB.committed);
        ASSERT_TRUE(resB.committed->isDeleted());
        ASSERT_TRUE(resB.committed->isDirty());
    };
    checkPreFlushHTState();

    auto& kvStore = *store->getRWUnderlying(vbid);
    const auto checkCachedAndOnDiskVBState = [this, &kvStore](
                                                     uint64_t lastSnapStart,
                                                     uint64_t lastSnapEnd,
                                                     uint64_t highSeqno,
                                                     CheckpointType type,
                                                     uint64_t hps,
                                                     uint64_t hcs,
                                                     uint64_t maxDelRevSeqno) {
        const auto& cached = *kvStore.getCachedVBucketState(vbid);
        const auto& onDisk = kvStore.getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, onDisk.status);
        for (const auto& vbs : {cached, onDisk.state}) {
            EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
            EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
            EXPECT_EQ(highSeqno, vbs.highSeqno);
            EXPECT_EQ(type, vbs.checkpointType);
            EXPECT_EQ(hps, vbs.highPreparedSeqno);
            EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
            EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
        }
    };

    // This flush fails, we have not written anything to disk
    errorInjector->failNextCommit();
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(0 /*lastSnapStart*/,
                                    0 /*lastSnapEnd*/,
                                    0 /*highSeqno*/,
                                    CheckpointType::Memory,
                                    0 /*HPS*/,
                                    0 /*HCS*/,
                                    0 /*maxDelRevSeqno*/);
        checkPreFlushHTState();
    }

    // Check nothing persisted to disk
    auto kvstore = store->getRWUnderlying(vbid);
    const auto keyA = makeDiskDocKey("keyA", true);
    auto docA = kvstore->get(keyA, vbid);
    EXPECT_EQ(cb::engine_errc::no_such_key, docA.getStatus());
    ASSERT_FALSE(docA.item);
    const auto keyB = makeDiskDocKey("keyB");
    auto docB = kvstore->get(keyB, vbid);
    EXPECT_EQ(cb::engine_errc::no_such_key, docB.getStatus());
    ASSERT_FALSE(docB.item);

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    EXPECT_EQ(FlushResult(MoreAvailable::No, 2, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        // Notes: expected (snapStart = snapEnd) for complete snap flushed,
        //  which is always the case at Active
        checkCachedAndOnDiskVBState(3 /*lastSnapStart*/,
                                    3 /*lastSnapEnd*/,
                                    3 /*highSeqno*/,
                                    CheckpointType::Memory,
                                    1 /*HPS*/,
                                    0 /*HCS*/,
                                    2 /*maxDelRevSeqno*/);

        // Check HT state
        const auto resA = vb.ht.findForUpdate(makeStoredDocKey("keyA"));
        ASSERT_TRUE(resA.pending);
        ASSERT_FALSE(resA.pending->isDeleted());
        ASSERT_FALSE(resA.pending->isDirty());
        ASSERT_FALSE(resA.committed);

        const auto resB = vb.ht.findForUpdate(makeStoredDocKey("keyB"));
        ASSERT_FALSE(resB.pending);
        ASSERT_FALSE(resB.committed);
    }

    // Check persisted docs
    docA = kvstore->get(keyA, vbid);
    EXPECT_EQ(cb::engine_errc::success, docA.getStatus());
    ASSERT_TRUE(docA.item);
    ASSERT_GT(docA.item->getNBytes(), 0);
    EXPECT_EQ(std::string_view(valueA.c_str(), valueA.size()),
              std::string_view(docA.item->getData(), docA.item->getNBytes()));
    EXPECT_FALSE(docA.item->isDeleted());
    docB = kvstore->get(keyB, vbid);
    EXPECT_EQ(cb::engine_errc::success, docB.getStatus());
    EXPECT_EQ(0, docB.item->getNBytes());
    EXPECT_TRUE(docB.item->isDeleted());
}

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (2) by checking that we persist the new vbstate
 * when we re-attempt flush.
 *
 * The test verifies MB-37920 too. Ie, the cached vbstate is not updated if
 * persistence fails.
 */
TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPersistVBStateOnly_ErrorWrite) {
    auto& kvStore = *store->getRWUnderlying(vbid);
    const auto checkCachedAndOnDiskVBState =
            [this, &kvStore](vbucket_state_t expectedState) -> void {
        EXPECT_EQ(expectedState,
                  kvStore.getCachedVBucketState(vbid)->transition.state);
        auto diskState = kvStore.getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, diskState.status);
        EXPECT_EQ(expectedState, diskState.state.transition.state);
    };

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(vbucket_state_active);
    }

    const auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(0, vb.dirtyQueueSize);

    const auto checkSetVBStateItemForCursor = [&vb]() -> void {
        const auto& manager = *vb.checkpointManager;
        auto pos = CheckpointCursorIntrospector::getCurrentPos(
                *manager.getPersistenceCursor());
        ASSERT_EQ(queue_op::set_vbucket_state, (*(pos++))->getOperation());
    };

    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_replica));
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(vbucket_state_active);
        checkSetVBStateItemForCursor();
        EXPECT_EQ(1, vb.dirtyQueueSize);
    }

    // This flush fails, we have not written anything to disk
    errorInjector->failNextSnapshotVBucket();
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(1, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(vbucket_state_active);
        checkSetVBStateItemForCursor();
    }

    // This flush succeeds, we must write the new vbstate on disk
    // Note: set-vbstate items are not accounted in numFlushed
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(vbucket_state_replica);
    }
}

/**
 * Check that flush stats are updated only at flush success.
 * Covers the case where the number of items pulled from the CheckpointManager
 * is different (higher) than the actual number of items flushed. Ie, flusher
 * deduplication occurs.
 */
void KVStoreErrorInjectionTest::testFlushFailureStatsAtDedupedNonMetaItems(
        bool vbDeletion) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Do we want to test the case where the flusher is running on a vbucket set
    // set for deferred deletion?
    // Nothing changes in the logic of this test, just that we hit an additional
    // code-path where flush-stats are wrongly updated at flush failure
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    if (vbDeletion) {
        vb.setDeferredDeletion(true);
    }

    // Active receives M(keyA):1, M(keyA):2.
    // They are queued into different checkpoints. We enforce that as we want to
    // stress deduplication at flush-vbucket, so we just avoid checkpoint dedup.

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    manager.createNewCheckpoint();
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    const auto storedKey = makeStoredDocKey("keyA");
    const std::string value2 = "value2";
    {
        SCOPED_TRACE("");
        store_item(vbid,
                   storedKey,
                   value2,
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getNumItemsForPersistence());

    EXPECT_EQ(2, vb.dirtyQueueSize);

    const auto checkPreFlushHTState = [&vb, &storedKey]() -> void {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_TRUE(res.committed);
        ASSERT_FALSE(res.committed->isDeleted());
        ASSERT_TRUE(res.committed->isDirty());
    };
    checkPreFlushHTState();

    // This flush fails, we have not written anything to disk
    errorInjector->failNextCommit();
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb.dirtyQueueSize);
    // HT state
    checkPreFlushHTState();
    // No doc on disk
    auto kvstore = store->getRWUnderlying(vbid);
    const auto diskKey = makeDiskDocKey("keyA");
    auto doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(cb::engine_errc::no_such_key, doc.getStatus());
    ASSERT_FALSE(doc.item);

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    // Flusher deduplication, just 1 item flushed
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
              epBucket.flushVBucket(vbid));
    EXPECT_TRUE(vb.checkpointManager->hasClosedCheckpointWhichCanBeRemoved());
    // Flush stats updated
    EXPECT_EQ(0, vb.dirtyQueueSize);
    // HT state
    const auto res = vb.ht.findForUpdate(storedKey);
    ASSERT_FALSE(res.pending);
    ASSERT_TRUE(res.committed);
    ASSERT_FALSE(res.committed->isDeleted());
    ASSERT_FALSE(res.committed->isDirty());
    // doc persisted
    doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(cb::engine_errc::success, doc.getStatus());
    ASSERT_TRUE(doc.item);
    ASSERT_GT(doc.item->getNBytes(), 0);
    EXPECT_EQ(std::string_view(value2.c_str(), value2.size()),
              std::string_view(doc.item->getData(), doc.item->getNBytes()));
    EXPECT_FALSE(doc.item->isDeleted());

    // Cleanup: reset the flag to avoid that we schedule the actual deletion at
    //  TearDown, the ExecutorPool will be already gone at that point and the
    //  test will SegFault
    vb.setDeferredDeletion(false);
}

TEST_P(KVStoreErrorInjectionTest, FlushFailureStatsAtDedupedNonMetaItems) {
    testFlushFailureStatsAtDedupedNonMetaItems();
}

TEST_P(KVStoreErrorInjectionTest,
       FlushFailureStatsAtDedupedNonMetaItems_VBDeletion) {
    testFlushFailureStatsAtDedupedNonMetaItems(true);
}

/**
 * Check that when persisting a delete and the flush fails:
 *  - flush-stats are not updated
 *  - the (deleted) item is not removed from the HashTable
 */
void KVStoreErrorInjectionTest::testFlushFailureAtPersistDelete(
        bool vbDeletion) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    if (vbDeletion) {
        vb.setDeferredDeletion(true);
    }

    // Active receives M(keyA):1 and deletion, M is deduplicated.
    const auto storedKey = makeStoredDocKey("keyA");
    store_item(vbid,
               storedKey,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    delete_item(vbid, storedKey);

    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    // Mutation deduplicated, just deletion
    ASSERT_EQ(1, manager.getNumItemsForPersistence());

    // Pre-conditions:
    // - stats account for the deletion in the write queue
    // - the deletion is in the HashTable
    EXPECT_EQ(1, vb.dirtyQueueSize);
    const auto checkPreFlushHTState = [&vb, &storedKey]() -> void {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_TRUE(res.committed);
        ASSERT_TRUE(res.committed->isDeleted());
        ASSERT_TRUE(res.committed->isDirty());
    };
    checkPreFlushHTState();

    // Test: flush fails, we have not written anything to disk
    errorInjector->failNextCommit();
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));

    // Post-conditions:
    //  - no doc on disk
    //  - flush stats not updated
    //  - the deletion is still dirty in the HashTable
    auto kvstore = store->getRWUnderlying(vbid);
    const auto diskKey = makeDiskDocKey("keyA");
    auto doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(cb::engine_errc::no_such_key, doc.getStatus());
    ASSERT_FALSE(doc.item);
    EXPECT_EQ(1, vb.dirtyQueueSize);
    checkPreFlushHTState();

    // Check out that all goes well when we re-attemp the flush

    // This flush succeeds, we must write all the expected items on disk.
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Doc on disk, flush stats updated and deletion removed from the HT
    doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(cb::engine_errc::success, doc.getStatus());
    EXPECT_EQ(0, doc.item->getNBytes());
    EXPECT_TRUE(doc.item->isDeleted());
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_FALSE(res.committed);
    }

    // All done, nothing to flush
    ASSERT_EQ(0, manager.getNumItemsForPersistence());
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));

    vb.setDeferredDeletion(false);
}

TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPerstingDelete) {
    testFlushFailureAtPersistDelete();
}
TEST_P(KVStoreErrorInjectionTest, FlushFailureAtPerstingDelete_VBDeletion) {
    testFlushFailureAtPersistDelete(true);
}

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 */
TEST_P(KVStoreErrorInjectionTest, ResetPCursorAtPersistNonMetaItems) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Active receives PRE(keyA):1, M(keyB):2, D(keyB):3
    // Note that the set of mutation is just functional to testing that we write
    // to disk all the required vbstate entries at flush

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {cb::durability::Requirements()});
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {} /*dur-reqs*/,
                   true /*deleted*/);
    }

    // M(keyB):2 deduplicated, just 2 items for cursor
    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(2, vb->checkpointManager->getNumItemsForPersistence());
    EXPECT_EQ(2, vb->dirtyQueueSize);

    auto& kvStore = *store->getRWUnderlying(vbid);
    const auto checkCachedAndOnDiskVBState = [this, &kvStore](
                                                     uint64_t lastSnapStart,
                                                     uint64_t lastSnapEnd,
                                                     uint64_t highSeqno,
                                                     CheckpointType type,
                                                     uint64_t hps,
                                                     uint64_t hcs,
                                                     uint64_t maxDelRevSeqno) {
        const auto& cached = *kvStore.getCachedVBucketState(vbid);
        const auto& onDisk = kvStore.getPersistedVBucketState(vbid);
        ASSERT_EQ(KVStoreIface::ReadVBStateStatus::Success, onDisk.status);
        for (const auto& vbs : {cached, onDisk.state}) {
            EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
            EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
            EXPECT_EQ(highSeqno, vbs.highSeqno);
            EXPECT_EQ(type, vbs.checkpointType);
            EXPECT_EQ(hps, vbs.highPreparedSeqno);
            EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
            EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
        }
    };

    // This flush fails, we have not written anything to disk
    errorInjector->failNextCommit();
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb->dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkCachedAndOnDiskVBState(0 /*lastSnapStart*/,
                                    0 /*lastSnapEnd*/,
                                    0 /*highSeqno*/,
                                    CheckpointType::Memory,
                                    0 /*HPS*/,
                                    0 /*HCS*/,
                                    0 /*maxDelRevSeqno*/);
    }

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    EXPECT_EQ(FlushResult(MoreAvailable::No, 2, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb->dirtyQueueSize);
    {
        SCOPED_TRACE("");
        // Notes: expected (snapStart = snapEnd) for complete snap flushed,
        //  which is always the case at Active
        checkCachedAndOnDiskVBState(3 /*lastSnapStart*/,
                                    3 /*lastSnapEnd*/,
                                    3 /*highSeqno*/,
                                    CheckpointType::Memory,
                                    1 /*HPS*/,
                                    0 /*HCS*/,
                                    2 /*maxDelRevSeqno*/);
    }
}

INSTANTIATE_TEST_SUITE_P(
        CouchstoreOrMagma,
        KVStoreErrorInjectionTest,
        STParameterizedBucketTest::persistentNoNexusConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);