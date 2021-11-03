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

#include "evp_store_single_threaded_test.h"

#include "../couchstore/src/internal.h"
#include "../couchstore/tests/test_fileops.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_utils.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_vb.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "thread_gate.h"

#include <utilities/test_manifest.h>

#include <thread>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

class STParamCouchstoreBucketTest : public STParamPersistentBucketTest {};

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistNonMetaItems(
        couchstore_error_t failureCode) {
    using namespace testing;
    NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(_, _))
            .Times(AnyNumber())
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS commit
            .WillOnce(Return(failureCode))
            .WillRepeatedly(Return(COUCHSTORE_SUCCESS));

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

    auto& kvStore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
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
        for (const auto& vbs : {cached, onDisk}) {
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

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistNonMetaItems_ErrorWrite) {
    testFlushFailureAtPersistNonMetaItems(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistNonMetaItems_NoSuchFile) {
    testFlushFailureAtPersistNonMetaItems(COUCHSTORE_ERROR_NO_SUCH_FILE);
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
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistVBStateOnly(
        couchstore_error_t failureCode) {
    using namespace testing;
    NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(_, _))
            .Times(AnyNumber())
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS commit
            .WillOnce(Return(failureCode))
            .WillRepeatedly(Return(COUCHSTORE_SUCCESS));

    auto& kvStore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    const auto checkCachedAndOnDiskVBState =
            [this, &kvStore](vbucket_state_t expectedState) -> void {
        EXPECT_EQ(expectedState,
                  kvStore.getCachedVBucketState(vbid)->transition.state);
        EXPECT_EQ(expectedState,
                  kvStore.getPersistedVBucketState(vbid).transition.state);
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

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistVBStateOnly_ErrorWrite) {
    testFlushFailureAtPersistVBStateOnly(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistVBStateOnly_NoSuchFile) {
    testFlushFailureAtPersistVBStateOnly(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

/**
 * Check that flush stats are updated only at flush success.
 * Covers the case where the number of items pulled from the CheckpointManager
 * is different (higher) than the actual number of items flushed. Ie, flusher
 * deduplication occurs.
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureStatsAtDedupedNonMetaItems(
        couchstore_error_t failureCode, bool vbDeletion) {
    using namespace testing;
    NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(_, _))
            .Times(AnyNumber())
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS commit
            .WillOnce(Return(failureCode))
            .WillRepeatedly(Return(COUCHSTORE_SUCCESS));

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

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_ErrorWrite) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_NoSuchFile) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_VBDeletion) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_WRITE, true);
}

/**
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
TEST_P(STParamCouchstoreBucketTest,
       BucketCreationFlagClearedOnlyAtFlushSuccess_PersistVBStateOnly) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    ASSERT_FALSE(engine->getKVBucket()->getVBucket(vbid));

    auto meta = nlohmann::json{
            {"topology", nlohmann::json::array({{"active", "replica"}})}};
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);

    ASSERT_TRUE(vb->isBucketCreation());

    // This flush fails, the bucket creation flag must be still set
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(1, vb->dirtyQueueSize);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(1, vb->dirtyQueueSize);
    EXPECT_TRUE(vb->isBucketCreation());

    // This flush succeeds
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb->dirtyQueueSize);
    EXPECT_FALSE(vb->isBucketCreation());
}

/**
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
TEST_P(STParamCouchstoreBucketTest,
       BucketCreationFlagClearedOnlyAtFlushSuccess_PersistVBStateAndMutations) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    ASSERT_FALSE(engine->getKVBucket()->getVBucket(vbid));

    auto meta = nlohmann::json{
            {"topology", nlohmann::json::array({{"active", "replica"}})}};
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);

    ASSERT_TRUE(vb->isBucketCreation());

    store_item(vbid,
               makeStoredDocKey("key"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    // This flush fails, the bucket creation flag must be still set
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(2, vb->dirtyQueueSize);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(2, vb->dirtyQueueSize);
    EXPECT_TRUE(vb->isBucketCreation());

    // This flush succeeds
    // Note: the returned num-flushed does not account meta-items
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb->dirtyQueueSize);
    EXPECT_FALSE(vb->isBucketCreation());
}

/**
 * Check that when persisting a delete and the flush fails:
 *  - flush-stats are not updated
 *  - the (deleted) item is not removed from the HashTable
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistDelete(
        couchstore_error_t failureCode, bool vbDeletion) {
    using namespace testing;
    NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(_, _))
            .Times(AnyNumber())
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // boot commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS pre-commit
            .WillOnce(Return(COUCHSTORE_SUCCESS)) // setVBS commit
            .WillOnce(Return(failureCode))
            .WillRepeatedly(Return(COUCHSTORE_SUCCESS));

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

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_ErrorWrite) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_NoSuchFile) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_VBDeletion) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_WRITE, true);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPersistingCollectionChange) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);

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
        EXPECT_CALL(ops, open(testing::_, testing::_, testing::_, testing::_))
                .WillOnce(testing::Return(COUCHSTORE_ERROR_OPEN_FILE))
                .RetiresOnSaturation();
        EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
                  epBucket.flushVBucket(vbid));
        // Flush stats not updated
        EXPECT_EQ(1, vb->dirtyQueueSize);
    }
    ops.DelegateToFake();
    EXPECT_CALL(ops, open(testing::_, testing::_, testing::_, testing::_))
            .Times(::testing::AnyNumber())
            .RetiresOnSaturation();
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

TEST_P(STParamCouchstoreBucketTest, ItemCountsAndCommitFailure_MB_41321) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");
    auto pending = makeCommittedItem(key, "value");
    EXPECT_EQ(cb::engine_errc::success,
              store->set(*pending, cookie)); // items=1
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(1, res.numFlushed);

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    auto stats =
            vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(1, stats.highSeqno);
    EXPECT_NE(0, stats.diskSize);

    // Replace RW kvstore and use a gmocked ops so we can inject failure
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);

    // Inject fsync error when so we fail the delete
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillRepeatedly(testing::Return(COUCHSTORE_ERROR_WRITE));

    // Delete our key
    uint64_t cas = 0;
    mutation_descr_t delInfo;
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteItem(key, cas, vbid, cookie, {}, nullptr, delInfo));

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

    flushAndExpectFailure(1);
    flushAndExpectFailure(2);

    // Replace the CouchKVStore as we need a valid FileOps to finish
    replaceCouchKVStore(*couchstore_get_default_file_ops());

    // Now a successful flush which will update the stats
    res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::No, res.wakeupCkptRemover);
    EXPECT_EQ(2, engine->getEpStats().commitFailed);
    stats = vb->getManifest().lock(CollectionID::Default).getPersistedStats();
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(2, stats.highSeqno);
    EXPECT_GT(stats.diskSize, 0); // tombstone data remains
}

TEST_P(STParamCouchstoreBucketTest, MB_44098_compactionFailureLeavesNewFile) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Replace compaction completion function with one which throws
    dynamic_cast<MockEPBucket*>(store)->mockMakeCompactionContext =
            [](std::shared_ptr<CompactionContext> ctx) {
                ctx->completionCallback = [](CompactionContext& ctx) {
                    throw std::logic_error("forcing compaction to fail");
                };
                return ctx;
            };

    // Store some data into the vbucket (and flush)
    auto key = makeStoredDocKey("keyB");
    store_item(vbid, key, "value");

    // flush to vbid.couch.1
    flush_vbucket_to_disk(vbid, 1);

    // Run compaction which will fail - but must not leave vbid.couch.2
    runCompaction(vbid);

    // Now delete and recreate the vbucket, the new vb will use vbid.couch.2
    // and the test expects to not be able to read the key
    store->deleteVBucket(vbid, cookie);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // To demonstrate the MB without to much work, disable the bloomfilter.
    // Prior to the fix, we can fetch the key which was written before the
    // deleteVBucket/createVbucket
    store->getVBucket(vbid)->setFilterStatus(BFILTER_DISABLED);

    auto gv = store->get(key, vbid, cookie, QUEUE_BG_FETCH);

    if (gv.getStatus() == cb::engine_errc::would_block) {
        runBGFetcherTask();
        gv = store->get(key, vbid, cookie, {});
    }

    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
}

INSTANTIATE_TEST_SUITE_P(STParamCouchstoreBucketTest,
                         STParamCouchstoreBucketTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

TEST_P(STParamCouchstoreBucketTest, FlusherMarksCleanBySeqno) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Used to synchronize this-thread (which simulate a frontend thread) and
    // the flusher-thread below (which simulate the flusher running in a bg
    // thread) so that we produce the exec interleaving of a scenario that
    // allows the user reading a stale seqno from disk.
    // Before the fix, that is possible because he flusher marks-clean items in
    // in the HashTable by CAS. Fixed by using Seqno instead.
    // Note: The scenario showed here a perfectly legal case of XDCR setup where
    // 2 different source clusters replicate to the same destination cluster.
    ThreadGate tg{2};

    auto& kvstore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    kvstore.setPostFlushHook([&tg]() {
        // The hook is executed after we have flushed to disk but before we call
        // back into the PersistenceCallback. Here we use the hook only for
        // blocking the flusher and allowing a frontend write before it proceeds
        tg.threadUp();
    });

    const std::string key = "key";
    const auto setWithMeta = [this, &key](uint64_t cas,
                                          uint64_t revSeqno,
                                          uint64_t expectedSeqno) -> void {
        const std::string value = "value";
        const auto valBuf = cb::const_byte_buffer{
                reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        uint64_t opCas = 0;
        uint64_t seqno = 0;
        const auto res = engine->public_setWithMeta(
                vbid,
                engine->public_makeDocKey(cookie, key),
                valBuf,
                {cas, revSeqno, 0 /*flags*/, 0 /*exp*/},
                false /*isDeleted*/,
                PROTOCOL_BINARY_RAW_BYTES,
                opCas,
                &seqno,
                cookie,
                {vbucket_state_active} /*permittedVBStates*/,
                CheckConflicts::Yes,
                true /*allowExisting*/,
                GenerateBySeqno::Yes,
                GenerateCas::No,
                {} /*extendedMetaData*/);
        ASSERT_EQ(cb::engine_errc::success, res);
        EXPECT_EQ(cas, opCas); // Note: CAS is not regenerated
        EXPECT_EQ(expectedSeqno, seqno);
    };

    // This-thread issues the first setWithMeta(s:1) and then blocks.
    // It must resume only when the flusher has persisted but not yet executed
    // into the PersistenceCallback.
    const uint64_t cas = 0x0123456789abcdef;
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 1 /*revSeqno*/, 1 /*expectedSeqno*/);
    }
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(1, vb.checkpointManager->getNumItemsForPersistence());

    // Run the flusher in a bg-thread
    const auto flush = [this]() -> void {
        auto& epBucket = dynamic_cast<EPBucket&>(*store);
        const auto res = epBucket.flushVBucket(vbid);
        EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No), res);
    };
    auto flusher = std::thread(flush);

    // This-thread issues a second setWithMeta(s:2), but only when the the
    // flusher is blocked into the postFlushHook.
    while (tg.getCount() < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // setWithMeta(s:2) with same CAS and higher revSeqno, so s:2 wins conflict
    // resolution and the operation succeeds.
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 2 /*revSeqno*/, 2 /*expectedSeqno*/);
    }

    // Now I want the flusher to proceed and call into the PersistenceCallback.
    // Before the fix, the flusher uses CAS for identifying the StoredValue to
    // mark clean in the HashTable, so in this scenario that makes clean a
    // StoreValue (s:2) that has never been persisted.
    // Note: The flusher was already running before s:2 was queued for
    // persistence, so s:2 is not being persisted in this flusher run.
    tg.threadUp();
    flusher.join();

    // Now the ItemPager runs. In the HashTable we have s:2: it must be dirty
    // and so not eligible for eviction.
    auto& epVB = dynamic_cast<EPVBucket&>(vb);
    const auto docKey = makeStoredDocKey(key);
    {
        const auto readHandle = vb.lockCollections();
        auto res = vb.ht.findOnlyCommitted(docKey);
        ASSERT_TRUE(res.storedValue);
        ASSERT_EQ(2, res.storedValue->getBySeqno());
        EXPECT_TRUE(res.storedValue->isDirty());
        EXPECT_FALSE(epVB.pageOut(readHandle, res.lock, res.storedValue));
    }

    // Note: The flusher has never persisted s:2
    ASSERT_EQ(1, vb.getPersistenceSeqno());

    // Try a get, it must fetch s:2 from the HashTable
    const auto res = engine->get(*cookie, docKey, vbid, DocStateFilter::Alive);
    // Note: Before the fix we get EWOULDBLOCK as s:2 would be evicted
    ASSERT_EQ(cb::engine_errc::success, res.first);
    const auto* it = reinterpret_cast<const Item*>(res.second.get());
    EXPECT_EQ(2, it->getBySeqno());
}

TEST_P(STParamCouchstoreBucketTest, DeleteUpdatesPersistedDeletes) {
    store->setVBucketState(vbid, vbucket_state_active);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(0, vb->getNumPersistedDeletes());

    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyA"));

    flushVBucketToDiskIfPersistent(vbid, 1);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store.
    EXPECT_EQ(1, vb->getNumPersistedDeletes());
}

void STParamPersistentBucketTest::testCompactionPersistedDeletes(
        bool dropDeletes) {
    store->setVBucketState(vbid, vbucket_state_active);

    flushVBucketToDiskIfPersistent(vbid, 0);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    ASSERT_NE(0, vb->getFilterSize());

    // Stat should be correct and we should populate the cached value
    EXPECT_EQ(0, vb->getNumPersistedDeletes());

    // Persist first delete
    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyA"));

    store_item(vbid,
               makeStoredDocKey("keyB"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    delete_item(vbid, makeStoredDocKey("keyB"));

    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(2, vb->getNumPersistedDeletes());

    runCompaction(vbid, 0, dropDeletes);
}

TEST_P(STParamCouchstoreBucketTest, CompactionUpdatesPersistedDeletes) {
    testCompactionPersistedDeletes(true /*dropDeletes*/);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store. This won't be 0
    // even though we have 2 deletes as we keep the last item during a
    // compaction.
    EXPECT_EQ(1, vb->getNumPersistedDeletes());
}

TEST_P(STParamCouchstoreBucketTest, CompactionUpdatesBloomFilter) {
    engine->getConfiguration().setBfilterKeyCount(1);

    testCompactionPersistedDeletes(false /*dropDeletes*/);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Before the bug fix the stat would be wrong as we'd read from the RO
    // store but only update the cached value in the RW store.
    EXPECT_EQ(2, vb->getNumPersistedDeletes());

    auto expected = 29;
    if (fullEviction()) {
        expected = 10;
    }
    EXPECT_EQ(expected, vb->getFilterSize());
}

TEST_P(STParamCouchstoreBucketTest,
       RollbackCompletionCallbackStateAfterCompletionCallbackFailure) {
    replaceCouchKVStoreWithMock();

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto vb = store->getVBucket(vbid);
    auto newKey = makeStoredDocKey("key");
    auto item = makePendingItem(newKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(cb::engine_errc::success,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      1));

    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    size_t collectionSize = 0;
    {
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        EXPECT_LT(0, summary[CollectionID::Default].diskSize);
        collectionSize = summary[CollectionID::Default].diskSize;
    }

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*store);
    mockEPBucket.setPostCompactionCompletionHook(
            []() { throw std::runtime_error("oops"); });

    runCompaction(vbid);

    // Stats shouldn't change as we should abort the compaction
    EXPECT_EQ(0, vb->getPurgeSeqno());
    EXPECT_EQ(1, vb->getNumTotalItems());

    {
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        EXPECT_EQ(1, summary[CollectionID::Default].itemCount);
        EXPECT_EQ(collectionSize, summary[CollectionID::Default].diskSize);
    }
}

/**
 * MB-42224: The test verifies that a failure in the header-sync phase at
 * flush-vbucket causes couchstore auto-retry. Also, the test verifies that
 * relevant stats are correctly updated when finally persistence succeeds.
 */
TEST_P(STParamCouchstoreBucketTest, HeaderSyncFails) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Use mock ops to inject syscall failures
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    // We do 2 syncs per flush. First is for data sync, second is for header
    // sync. In this test we test we fail the second sync, which means that:
    // - All data (docs+local) will be flushed to the OSBC and sync'ed to disk
    // - New header flushed to OSBC but not sync'ed to disk
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::DoDefault())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::DoDefault());

    const auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getNumItems());
    auto* kvstore = store->getRWUnderlying(vbid);
    ASSERT_EQ(0, kvstore->getPersistedVBucketState(vbid).onDiskPrepares);
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(1, stats.flusherCommits);

    // Store a mutation (that is for checking our num-items)
    const auto keyM = makeStoredDocKey("keyM");
    const auto item = makeCommittedItem(keyM, "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(*item, cookie));

    // Store a prepare (that is for checking out on-dick-prepares)
    const auto key = makeStoredDocKey("key");
    const auto prepare =
            makePendingItem(key, "value", cb::durability::Requirements());
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie));

    // Try persistence, it fails the first attempt but couchstore auto-retry
    // succeeds at the second attempt.
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(2, res.numFlushed);
    EXPECT_EQ(1, stats.commitFailed); // This indicates that we failed once
    EXPECT_EQ(2, stats.flusherCommits);
    EXPECT_EQ(1, vb.getNumTotalItems());
    EXPECT_EQ(1, kvstore->getPersistedVBucketState(vbid).onDiskPrepares);
    EXPECT_EQ(1, kvstore->getCachedVBucketState(vbid)->onDiskPrepares);
}

/**
 * MB-42224: This test doesn't cover any fix, it just shows how
 * KVStore::snapshotVBuckets behaves if the sync-header fails at commit.
 */
TEST_P(STParamCouchstoreBucketTest, HeaderSyncFails_VBStateOnly) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto* kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);
    ASSERT_EQ(vbucket_state_active,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    ASSERT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(1, stats.flusherCommits);

    // Flush and verify nothing in the persistence queue.
    auto& bucket = dynamic_cast<EPBucket&>(*store);
    auto res = bucket.flushVBucket(vbid);
    using MoreAvailable = EPBucket::MoreAvailable;
    EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(0, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);

    // Use mock ops to inject syscall failures.
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::DoDefault()) // data
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE)) // header
            .WillRepeatedly(testing::DoDefault());
    kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);

    // Set new vbstate in memory only
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // Flush and verify failure
    res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);

    // Given that write-data has succeded, we have written the new vbstate to
    // the OS buffer cache, so we do see the new vbstate when we make a VFS read
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    // Note: Cached vbstate is updated only if commit succeeds, so we didn't
    // reach the point where we update it.
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // The next flush attempt succeeds, cached/on-disk vbstate aligned
    res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(2, stats.flusherCommits);
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getCachedVBucketState(vbid)->transition.state);
}

TEST_P(STParamCouchstoreBucketTest, FlushVBStateUpdatesCommitStats) {
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(0, stats.flusherCommits);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto* kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);
    EXPECT_EQ(0, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);

    // Use mock ops to inject syscall failures
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE)) // data
            .WillRepeatedly(testing::DoDefault());
    kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);

    // Set new vbstate in memory only
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // Flush and verify failure
    auto& bucket = dynamic_cast<EPBucket&>(*store);
    auto res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(1, stats.flusherCommits);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_active,
              kvstore->getCachedVBucketState(vbid)->transition.state);

    // The next flush attempt succeeds
    res = bucket.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(2, stats.flusherCommits);
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getPersistedVBucketState(vbid).transition.state);
    EXPECT_EQ(vbucket_state_replica,
              kvstore->getCachedVBucketState(vbid)->transition.state);
}

TEST_P(STParamCouchstoreBucketTest,
       RollBackToZeroAfterOnDiskPrepareReadFailure) {
    // set up mock KVStore so we can fail a file open
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto& vbucket = *engine->getVBucket(vbid);
    // Write a prepare so we have something for loadPreparedSyncWrites() to try
    // and load and 5 items to disk we will try to rollback to the last of these
    // items
    store_item(vbid,
               makeDiskDocKey("key123", true, CollectionID()).getDocKey(),
               "value",
               0,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               {{cb::durability::Level::Majority, cb::durability::Timeout{}}});

    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("key"), "value"));
    auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(6, res.numFlushed);

    // Add another 5 items to disk so we can tell EP Engine to roll these back
    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("key"), "value"));
    res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    EXPECT_EQ(5, res.numFlushed);
    EXPECT_EQ(11, vbucket.getHighSeqno());

    // Set the vbucket to a replica so we can rollback the data on disk
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Setup couchstore so that we fail a file open during
    // EPBucket::loadPreparedSyncWrites()
    EXPECT_CALL(ops, open(testing::_, testing::_, testing::_, testing::_))
            .WillOnce(testing::DoDefault())
            .WillOnce(testing::DoDefault())
            .WillOnce(testing::DoDefault())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_READ))
            .WillRepeatedly(testing::DoDefault());

    // Try and rollback to seqno 6, this should fail as we're unable to load
    // prepares on disk due to this we should rollback to seqno 0
    auto status = engine->getKVBucket()->rollback(vbid, 6);
    EXPECT_EQ(TaskStatus::Complete, status);
    auto& vbucketR = *engine->getVBucket(vbid);
    EXPECT_EQ(0, vbucketR.getHighSeqno());
    EXPECT_EQ(0, vbucketR.getNumItems());
}

TEST_P(STParamCouchstoreBucketTest,
       BootstrapProcedureLeavesNoCorruptedFileAtFailure) {
    using namespace testing;

    // Always fail the pwrite syscall for inducing the creation of an empty
    // couchstore file when the first flush fails.
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .Times(2)
            .WillRepeatedly(Return(COUCHSTORE_ERROR_WRITE));
    auto* kvstore = dynamic_cast<CouchKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(kvstore);

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.commitFailed);
    ASSERT_EQ(0, stats.flusherCommits);

    auto vb = store->getVBucket(vbid);
    ASSERT_FALSE(vb);

    // The store has never received a SetVBstate yet, verify no file on disk
    const auto verifyNoFile = [this, &kvstore]() -> void {
        bool fileNotFound = false;
        try {
            kvstore->getPersistedVBucketState(vbid);
        } catch (const std::logic_error& e) {
            ASSERT_THAT(e.what(), HasSubstr("openDB error:no such file"));
            fileNotFound = true;
        }
        ASSERT_TRUE(fileNotFound);
    };
    verifyNoFile();

    setVBucketState(vbid, vbucket_state_replica);
    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    verifyNoFile();

    // Flush fails
    auto& ep = dynamic_cast<EPBucket&>(*store);
    const auto res = ep.flushVBucket(vbid);
    EXPECT_EQ(MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(0, res.numFlushed);
    EXPECT_EQ(1, stats.commitFailed);
    EXPECT_EQ(0, stats.flusherCommits);

    // Before the fix we fail with "no header in non-empty file" if we try to
    // open the database at this point, as flush has failed but we have left an
    // empty file behind.
    // A practical example is a restart. We'll try to initialize KVStore and
    // fail, and we'll never recover automatically from that state.
    //
    // After the fix persistence never creates empty files, so we see "no such
    // file" if we try to read from disk here. At restart we are fine as we just
    // don't see any file.
    verifyNoFile();
}

TEST_P(STParamCouchstoreBucketTest,
       FlushStatsAtPersistNonMetaItems_CkptMgrSuccessPersistAgain) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    replaceCouchKVStore(ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    SCOPED_TRACE("");
    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    const auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    EXPECT_EQ(1, vb.dirtyQueueSize);

    auto kvstore = store->getRWUnderlying(vbid);
    kvstore->setPostFlushHook([this, &vb]() {
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   "biggerValue",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
        EXPECT_EQ(2, vb.dirtyQueueSize);
    });

    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(1, vb.dirtyQueueSize);

    // Reset hook, don't want to add another item on the successful flush
    kvstore->setPostFlushHook([]() {});

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    // Flusher deduplication, just 1 item flushed
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));

    // Flush stats updated
    EXPECT_EQ(0, vb.dirtyQueueSize);
    EXPECT_EQ(0, vb.dirtyQueueAge);
    EXPECT_EQ(0, vb.dirtyQueueMem);
    EXPECT_EQ(0, vb.dirtyQueuePendingWrites);
}
