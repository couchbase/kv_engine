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

#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_magma_kvstore.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"

#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#include "kvstore_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/thread_gate.h"
#include "vbucket.h"

#include <platform/cb_arena_malloc.h>
#include <utilities/test_manifest.h>

#include <folly/synchronization/Baton.h>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;

class STParamMagmaBucketTest : public STParamPersistentBucketTest {
private:
    // Default function to be called if no function is given to
    // setupForImplicitCompactionTest. This is a bit hacky but some of our gcc
    // versions error due to a 'is already defined' error if we use a default
    // empty lambda of [] () {}.
    static void doNothing(){};

public:
    /**
     * Setup for implicit compaction test that stores some items as specified
     * by a lambda function and runs an implicit compaction. Callers can then
     * test the post-compaction state.
     *
     * @param storeItemsForTest Lambda storing items we care about testing
     * @param postTimeTravelFn Labmda for doing things after the time travel
     * @param runPostCompactionFn Lambda to run during the post implicit
     * @param expectedCompactionStatus expected status of the implicit
     * compaction compaction hook
     */
    void setupForImplicitCompactionTest(
            std::function<void()> storeItemsForTest,
            std::function<void()> postTimeTravelFn = doNothing,
            std::function<void()> runPostCompactionFn = doNothing,
            magma::Status expectedCompactionStatus = magma::Status::OK());

    /**
     * Function to perform 15 writes so that the next flush will hit the
     * LSMMaxNumLevel0Tables threshold which will trigger implicit compaction
     */
    void performWritesForImplicitCompaction();

    void testDiskStateAfterCompactKVStore(std::function<void()> completionCb);

    /**
     * Execute rollback to given seqno and verify that the given key is seen in
     * the rollback callback.
     *
     * @param rollbackSeqno Seqno to rollback to
     * @param callbackKey Key that should be seen in the callback
     */
    void doRollbackAndVerifyCallback(int64_t rollbackSeqno,
                                     StoredDocKey callbackKey);
};

// We want to test what happens during an implicit magma compaction (in
// particular in regards to the CompactionConfig). Given that we call the same
// functions with a slightly different CompactionContext object we can just test
// this by creating the CompactionContext in the same way that we do for an
// implicit compaction and perform a normal compaction with this ctx.
// This test requires the full engine to ensure that we get correct timestamps
// for items as we delete them and all the required callbacks to perform
// compactions.
TEST_P(STParamMagmaBucketTest, implicitCompactionContext) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto firstDeletedKey = makeStoredDocKey("keyA");
    auto secondDeletedKey = makeStoredDocKey("keyB");

    store_item(vbid, firstDeletedKey, "value");
    delete_item(vbid, firstDeletedKey);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Time travel 5 days, we want to drop the tombstone for this when we
    // compact
    TimeTraveller timmy{60 * 60 * 24 * 5};

    // Add a second tombstone to check that we don't drop everything
    store_item(vbid, secondDeletedKey, "value");
    delete_item(vbid, secondDeletedKey);

    // And a dummy item because we can't drop the final seqno
    store_item(vbid, makeStoredDocKey("dummy"), "value");

    flushVBucketToDiskIfPersistent(vbid, 2);

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);

    // Assert the state of the first key on disk
    auto gv = magmaKVStore->get(DiskDocKey(firstDeletedKey), Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    ASSERT_TRUE(gv.item);
    ASSERT_TRUE(gv.item->isDeleted());

    // Assert the second of the first key on disk
    gv = magmaKVStore->get(DiskDocKey(secondDeletedKey), Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    ASSERT_TRUE(gv.item);
    ASSERT_TRUE(gv.item->isDeleted());

    // And compact
    auto cctx = magmaKVStore->makeImplicitCompactionContext(vbid);

    {
        auto vb = store->getLockedVBucket(vbid);
        EXPECT_EQ(CompactDBStatus::Success,
                  magmaKVStore->compactDB(vb.getLock(), cctx));
    }

    // Check the first key on disk - should not exist
    gv = magmaKVStore->get(DiskDocKey(firstDeletedKey), Vbid(0));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
    EXPECT_FALSE(gv.item);

    // Check the second key on disk - should be a tombstone
    gv = magmaKVStore->get(DiskDocKey(secondDeletedKey), Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_TRUE(gv.item);
    EXPECT_TRUE(gv.item->isDeleted());
}

TEST_P(STParamMagmaBucketTest, makeCompactionContextSetupAtWarmup) {
    // Need a vBucket state to make sure we can call makeCompactionContext
    // without throwing
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Reset so that we can warmup
    resetEngineAndEnableWarmup();

    // Remove the makeCompactionContextCallback function from the KVStore via
    // a mock to test that we set it correctly (as we always set it manually
    // in construction of the SynchronousEpEngine).
    auto mockBucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    mockBucket->removeMakeCompactionContextCallback();

    // Grab the KVStore and assert that the makeCompactionContextCallback isn't
    // currently set
    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);
    ASSERT_THROW(magmaKVStore->makeImplicitCompactionContext(vbid),
                 std::runtime_error);

    // Run warmup, and we should set the makeCompactionContextCallback in the
    // final stage
    runReadersUntilWarmedUp();
    EXPECT_NO_THROW(magmaKVStore->makeImplicitCompactionContext(vbid));
}

void STParamMagmaBucketTest::performWritesForImplicitCompaction() {
    for (int i = 0; i < 15; i++) {
        store_item(vbid, makeStoredDocKey("key" + std::to_string(i)), "value");
        flushVBucketToDiskIfPersistent(vbid, 1);
    }
}

void STParamMagmaBucketTest::setupForImplicitCompactionTest(
        std::function<void()> storeItemsForTest,
        std::function<void()> postTimeTravelFn,
        std::function<void()> runPostCompactionFn,
        magma::Status expectedCompactionStatus) {
    replaceMagmaKVStore();
    // Make sure the makeCompactionContextCallback function is set in the new
    // MagmaKVStore
    getEPBucket().primaryWarmupCompleted();

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->getPurgeSeqno());

    // Delete at least one thing to trigger the implicit compaction in cases
    // where we are testing with non-deleted items
    auto dummyKey = makeStoredDocKey("keyA");
    store_item(vbid, dummyKey, "value");
    delete_item(vbid, dummyKey);
    flushVBucketToDiskIfPersistent(vbid, 1);

    storeItemsForTest();

    // Check that the purge seqno is still 0, but we should have a tombstone on
    // disk
    EXPECT_EQ(0, vb->getPurgeSeqno());

    // Time travel 5 days, we want to drop the tombstone for this when we
    // compact
    TimeTraveller timmy{60 * 60 * 24 * 5};

    postTimeTravelFn();

    auto& bucket = dynamic_cast<EPBucket&>(*store);
    bucket.postPurgeSeqnoImplicitCompactionHook = runPostCompactionFn;

    auto deletedNotPurgedKey = makeStoredDocKey("keyB");
    // Add a tombstone to check that we don't drop everything
    store_item(vbid, deletedNotPurgedKey, "value");
    delete_item(vbid, deletedNotPurgedKey);

    // And a dummy item because we can't drop the final seqno
    store_item(vbid, makeStoredDocKey("dummy"), "value");
    // Flush the dummy value and second deleted value
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto magmaKVStore =
            dynamic_cast<MockMagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);

    // Create a new checkpoint so that we can perform implicit compaction on the
    // old checkpoint
    ASSERT_EQ(magma::Status::OK(), magmaKVStore->newCheckpoint(vbid));
    EXPECT_EQ(expectedCompactionStatus,
              magmaKVStore->runImplicitCompactKVStore(vbid));

    // Write and flush another value to cause a Sync in magma to occur which
    // will ensure that firstDeletedKey is no longer visible
    store_item(vbid, makeStoredDocKey("dummy2"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Assert that the deletedNotPurgedKey key is still a tombstone on disk as
    // it hasn't hit the purge threshold yet
    auto gv = magmaKVStore->get(DiskDocKey(deletedNotPurgedKey), vbid);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    ASSERT_TRUE(gv.item);
    ASSERT_TRUE(gv.item->isDeleted());
}

void STParamMagmaBucketTest::doRollbackAndVerifyCallback(
        int64_t rollbackSeqno, StoredDocKey callbackKey) {
    auto vb = store->getVBucket(vbid);
    auto* magmaKVStore =
            dynamic_cast<MockMagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);

    bool rollbackKeySeen{false};
    auto rollbackCb = [&callbackKey, &rollbackKeySeen](GetValue result) {
        if (result.item->getKey() == callbackKey) {
            rollbackKeySeen = true;
        }
    };

    // Ensure the key was not dropped via rollback callback
    auto rollbackResult = magmaKVStore->rollback(
            vbid,
            rollbackSeqno,
            std::make_unique<CustomRBCallback>(rollbackCb));
    ASSERT_TRUE(rollbackResult.success);
    ASSERT_TRUE(rollbackKeySeen);
}

/**
 * Test to check that we correctly update the in memory purge seqno when magma
 * performs an implicit compaction.
 */
TEST_P(STParamMagmaBucketTest, CheckImplicitCompactionUpdatePurgeSeqno) {
    uint64_t expectedPurgeSeqno;
    auto purgedKey = makeStoredDocKey("keyA");

    setupForImplicitCompactionTest([this, &expectedPurgeSeqno]() {
        auto vb = store->getVBucket(vbid);
        expectedPurgeSeqno = vb->getHighSeqno();
    });

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);
    // Assert that the first key no longer has a tomb stone
    auto gv = magmaKVStore->get(DiskDocKey(purgedKey), vbid);
    ASSERT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    // Ensure that the purge seqno has been set during the second flush to where
    // the first tombstone was
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(expectedPurgeSeqno, vb->getPurgeSeqno());
}

TEST_P(STParamMagmaBucketTest,
       CheckImplicitCompactionDoesNotUpdatePurgeSeqnoForPrepare) {
    uint64_t expectedPurgeSeqno;
    auto purgedKey = makeStoredDocKey("keyPrepare");

    setupForImplicitCompactionTest([this, &expectedPurgeSeqno, &purgedKey]() {
        auto vb = store->getVBucket(vbid);
        expectedPurgeSeqno = vb->getHighSeqno();

        store_item(vbid,
                   purgedKey,
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {cb::durability::Requirements()});
        flushVBucketToDiskIfPersistent(vbid, 1);

        EXPECT_EQ(cb::engine_errc::success,
                  vb->seqnoAcknowledged(
                          folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          vb->getHighSeqno() /*prepareSeqno*/));
        vb->processResolvedSyncWrites();
        flushVBucketToDiskIfPersistent(vbid, 1);
    });

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);
    // Assert that the first key no longer has a tomb stone
    auto gv = magmaKVStore->get(DiskDocKey(purgedKey, true /*prepare*/), vbid);
    ASSERT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    // Ensure that the purge seqno has been set during the second flush to where
    // the first tombstone was
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(expectedPurgeSeqno, vb->getPurgeSeqno());
}

TEST_P(STParamMagmaBucketTest, ImplicitCompactionDoesNotDropCollectionItems) {
    uint64_t expectedPurgeSeqno;
    auto purgedKey = makeStoredDocKey("keyA", CollectionEntry::fruit.getId());
    CollectionsManifest cm;

    setupForImplicitCompactionTest(
            [this, &expectedPurgeSeqno, &purgedKey, &cm]() {
                auto vb = store->getVBucket(vbid);
                expectedPurgeSeqno = vb->getHighSeqno();

                cm.add(CollectionEntry::fruit);
                vb->updateFromManifest(
                        folly::SharedMutex::ReadHolder(vb->getStateLock()),
                        makeManifest(cm));
                store_item(vbid, purgedKey, "value");
                flushVBucketToDiskIfPersistent(vbid, 2);
            },
            [this, &cm]() {
                // Now remove the collection (which won't get purged as this is
                // run after the time travel. This can't be purged or it moves
                // the purge seqno and invalidates the test
                auto vb = store->getVBucket(vbid);
                cm.remove(CollectionEntry::fruit);
                vb->updateFromManifest(
                        folly::SharedMutex::ReadHolder(vb->getStateLock()),
                        makeManifest(cm));
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);
    // Assert that the collection key tombstone is not dropped
    auto gv = magmaKVStore->get(DiskDocKey(purgedKey), vbid);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());

    // Ensure that the purge seqno has been set during the second flush to where
    // the first tombstone was
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(expectedPurgeSeqno, vb->getPurgeSeqno());
}

/**
 * Test that implicit/explicit compaction overlapping handles setting the purge
 * seqno correctly.
 */
TEST_P(STParamMagmaBucketTest,
       CheckExplicitAndImplicitCompactionUpdatePurgeSeqno) {
    // Re-set the engine and warmup adding the magma rollback test config
    // settings, so that we create a checkpoint at every flush
    resetEngineAndWarmup(magmaRollbackConfig);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->getPurgeSeqno());

    // Test will drop a collection and keep one for writing. Create fruit now
    // and later it will be dropped
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    store_item(vbid,
               makeStoredDocKey("f1", CollectionEntry::fruit.getId()),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Item in default collection (will be purged)
    auto firstDeletedKey = makeStoredDocKey("keyA");
    store_item(vbid, firstDeletedKey, "value");
    delete_item(vbid, firstDeletedKey);
    flushVBucketToDiskIfPersistent(vbid, 1);
    const auto expectedPurgeSeqno = vb->getHighSeqno();

    // Check that the purge seqno is still 0, but we should have a tombstone on
    // disk
    EXPECT_EQ(0, vb->getPurgeSeqno());

    // Time travel 5 days, we want to drop the tombstone for this when we
    // compact
    TimeTraveller timmy{60 * 60 * 24 * 5};

    ThreadGate tg(2);
    auto& bucket = dynamic_cast<EPBucket&>(*store);
    bucket.postPurgeSeqnoImplicitCompactionHook = [&tg]() -> void {
        tg.threadUp();
    };

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*store);
    mockEPBucket.setPostCompactionCompletionHook([&tg, this]() {
        // ensure we meet the LSMMaxNumLevel0Tables threshold
        performWritesForImplicitCompaction();

        auto secondDeletedKey = makeStoredDocKey("keyB");
        // Add a second tombstone to check that we don't drop everything
        store_item(vbid, secondDeletedKey, "value");
        delete_item(vbid, secondDeletedKey);

        // And a dummy item because we can't drop the final seqno
        store_item(vbid, makeStoredDocKey("dummy"), "value");
        // Flush the dummy value and second deleted value
        flushVBucketToDiskIfPersistent(vbid, 2);
        tg.threadUp();
    });

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);

    // Drop the fruit collection and run compaction - explicit compaction runs
    // and will interleave an implicit compaction from the completion hook
    cm.remove(CollectionEntry::fruit);
    vb->updateFromManifest(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                           makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    runCollectionsEraser(vbid);

    EXPECT_EQ(expectedPurgeSeqno, vb->getPurgeSeqno());
}

/**
 * Test to ensure we don't crash due to an exception being raised during an
 * implicit compaction. To test this just throw inside the
 * postPurgeSeqnoImplicitCompactionHook.
 */
TEST_P(STParamMagmaBucketTest, MB_48441) {
    setupForImplicitCompactionTest(
            []() {},
            []() {},
            []() { throw std::runtime_error("this should be caught"); },
            magma::Status(magma::Status::Internal,
                          "MagmaKVStore::compactionCallBack() threw:'this "
                          "should be caught'"));
}

TEST_P(STParamMagmaBucketTest, MagmaMemQuotaDynamicUpdate) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("magma_mem_quota_ratio", "0.1", msg));

    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    ASSERT_EQ(0.1f, config.getMagmaMemQuotaRatio());

    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("magma_mem_quota_ratio", "0.3", msg));
    EXPECT_EQ(0.3f, config.getMagmaMemQuotaRatio());
}

TEST_P(STParamMagmaBucketTest, MagmaSeqTreeDataBlocksSize) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "magma_seq_tree_data_block_size", "7777", msg));

    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    ASSERT_EQ(7777, config.getMagmaSeqTreeDataBlockSize());
}

TEST_P(STParamMagmaBucketTest, MagmaSeqTreeIndexBlocksSize) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "magma_seq_tree_index_block_size", "7777", msg));

    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    ASSERT_EQ(7777, config.getMagmaSeqTreeIndexBlockSize());
}

TEST_P(STParamMagmaBucketTest, MagmaKeyTreeDataBlocksSize) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "magma_key_tree_data_block_size", "7777", msg));

    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    ASSERT_EQ(7777, config.getMagmaKeyTreeDataBlockSize());
}

TEST_P(STParamMagmaBucketTest, MagmaKeyTreeIndexBlocksSize) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "magma_key_tree_index_block_size", "7777", msg));

    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(
            store->getRWUnderlying(vbid)->getConfig());
    ASSERT_EQ(7777, config.getMagmaKeyTreeIndexBlockSize());
}

/*
 * Test for MB-47566 to ensure that compaction running at the same time as a
 * vbucket being rolled back doesn't cause us to throw an underflow exception
 *
 * This test can also replicate the old functionality of compaction completion
 * callback to trigger an underflow. By setting runWithFix=false;
 */
TEST_P(STParamMagmaBucketTest, MB_47566) {
    const bool runWithFix = true;
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Add collection and write a documents to it. Then drop the collection so
    // when compaction runs we will drop an item.
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Switch the vbucket to a replica so we can perform rollback
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto& epBucket = dynamic_cast<EPBucket&>(*engine->getKVBucket());
    folly::Baton<> compactionStarted;
    folly::Baton<> rollbackCompleted;
    folly::Baton<> compactionCompleted;

    CompactionConfig config;
    // Set drop deletes so we get rid of the fruit collection's documents
    config.drop_deletes = true;
    // Create a compaction context and cache the real compaction completion
    // callback
    auto ctx = epBucket.makeCompactionContext(vbid, config, 0);
    auto realCompletionCallback = ctx->completionCallback;
    // Create a new compaction completion callback that wait for the main
    // thread to have performed the rollback before calling the real compaction
    // completion callback
    ctx->completionCallback = [this,
                               &compactionStarted,
                               &rollbackCompleted,
                               runWithFix,
                               realCompletionCallback](CompactionContext& ctx) {
        // notify that compaction has started
        compactionStarted.post();
        // now wait for rollback to run and finish before continuing
        rollbackCompleted.wait();
        if (runWithFix) {
            realCompletionCallback(ctx);
        } else {
            auto vb = engine->getVBucket(vbid);
            vb->maybeSetPurgeSeqno(ctx.getRollbackPurgeSeqno());
            vb->decrNumTotalItems(ctx.stats.collectionsItemsPurged);
        }
    };

    // Lambda to perform the compaction on another thread
    auto tPerformCompaction = [this, ctx, &epBucket, &compactionCompleted]() {
        ObjectRegistry::onSwitchThread(engine.get());
        auto* kvstore = epBucket.getRWUnderlying(vbid);
        auto vb = store->getLockedVBucket(vbid, std::try_to_lock);
        auto& lock = vb.getLock();
        CompactDBStatus result;
        // Ensure that the compaction doesn't throw
        // note - this may unlock the provided mutex.
        EXPECT_NO_THROW(result = kvstore->compactDB(lock, ctx));
        // rollback reset the vbucket, but it occurred after all items
        // had been visited, so it did not cancel the compaction early.
        EXPECT_EQ(CompactDBStatus::Success, result);
        // compaction is done, any underflow should have happened by now.
        compactionCompleted.post();
    };
    // Start compaction thread
    std::thread compactionThread(tPerformCompaction);
    // Wait till the compaction task is about to compact
    compactionStarted.wait();
    // Now that compaction has been started reset the vbucket by rolling back to
    // seqno 0
    EXPECT_NE(TaskStatus::Abort, epBucket.rollback(vbid, 0));
    // notify the compaction so it continues running
    rollbackCompleted.post();
    // Wait for compaction to finish
    compactionCompleted.wait();
    // Wait for compaction thread to complete
    compactionThread.join();
}

/**
 * Test that when we fail a CompactKVStore call we update stats appropriately
 */
TEST_P(STParamMagmaBucketTest, FailCompactKVStoreCall) {
    replaceMagmaKVStore();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    setCollections(cookie, cm.add(CollectionEntry::meat));
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            1, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));
    ASSERT_TRUE(store_items(
            1, vbid, StoredDocKey{"m", CollectionEntry::meat}, "value"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // 2 items, 1 in each collection
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(2, vb->getNumTotalItems());

    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    setCollections(cookie, cm.remove(CollectionEntry::meat));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Still 2 items, waiting for purge
    ASSERT_EQ(2, vb->getNumTotalItems());

    auto* kvstore = store->getRWUnderlying(vbid);
    ASSERT_TRUE(kvstore);
    auto& magmaKVStore = static_cast<MockMagmaKVStore&>(*kvstore);

    // "Fail" the second compaction. This is a sort of "soft" failure as the
    // magma portion works but we're going to pretend that it doesn't and
    // skip updating state based on that.
    bool first = true;
    magmaKVStore.setCompactionStatusHook([&first](magma::Status& status) {
        if (first) {
            first = false;
        } else {
            status = magma::Status(magma::Status::Code::IOError, "bad");
        }
    });

    // Compaction for one KVStore passes and another "fails". We'll check the
    // dropped collections local doc after to determine that it has been set
    // successfully.
    runCompaction(vbid);

    // 1 Dropped collection remains, the one that "failed"
    auto [status, dc] = kvstore->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_FALSE(dc.empty());

    // Our hook wouldn't do anything now, but reset it anyway for simplicity
    // and run the compaction again allowing the other collection to compact.
    magmaKVStore.setCompactionStatusHook([](magma::Status&) {});
    runCompaction(vbid);

    // Dropped collections all gone now
    ASSERT_EQ(1, engine->getEpStats().compactionFailed);
    std::tie(status, dc) = kvstore->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_TRUE(dc.empty());
}

TEST_P(STParamMagmaBucketTest, FailPrepareCompactKVStoreCall) {
    replaceMagmaKVStore();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            1, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));
    flushVBucketToDiskIfPersistent(vbid, 1);

    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto* kvstore = store->getRWUnderlying(vbid);
    ASSERT_TRUE(kvstore);
    auto& magmaKVStore = static_cast<MockMagmaKVStore&>(*kvstore);

    // "Fail" the prepare compaction. This is a sort of "soft" failure as the
    // magma portion works but we're going to pretend that it doesn't and
    // skip updating state based on that.
    bool first = true;
    magmaKVStore.setCompactionStatusHook([&first](magma::Status& status) {
        if (first) {
            first = false;
            status = magma::Status(magma::Status::Code::IOError, "bad");
        }
    });

    // Compaction for prepare namespace "fails". We'll check the
    // dropped collections local doc after to determine that it has been set
    // successfully.
    runCompaction(vbid);

    // Dropped collection remains, as the compaction should have aborted
    auto [status, dc] = kvstore->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_FALSE(dc.empty());

    // Our hook wouldn't do anything now, but reset it anyway for simplicity
    // and run the compaction again allowing the other collection to compact.
    magmaKVStore.setCompactionStatusHook([](magma::Status&) {});
    runCompaction(vbid);

    // Dropped collections all gone now
    ASSERT_EQ(1, engine->getEpStats().compactionFailed);
    std::tie(status, dc) = kvstore->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_TRUE(dc.empty());
}

/**
 * MB-50061:
 * Test that we correctly track the document count when we update an item in a
 * collection at an interesting point during compaction. The issue before this
 * fix was as follows:
 *
 * 1) Drop a collection
 * 2) Start purging the compaction and read the dropped stats then pause
 * 3) Add a new generation of that collection
 * 4) Add an item to the new generation of the collection that existed in the
 *    older generation of the collection which adjusts the dropped stats count
 * 5) Finish the compaction which applies a delta of the count read at 2 rather
 *    than the count that should exist after 4.
 */
TEST_P(STParamMagmaBucketTest, UpdateDroppCollStatAfterReadBeforeCompact) {
    replaceMagmaKVStore();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto* kvstore = store->getRWUnderlying(vbid);
    auto& magmaKVStore = static_cast<MockMagmaKVStore&>(*kvstore);
    ASSERT_TRUE(kvstore);
    EXPECT_EQ(1, magmaKVStore.getItemCount(vbid));

    // Need to create a new checkpoint so that the compaction on the prepare
    // namespace that we do before any other collection namespaces does not
    // also visit the items in the fruit collection. This will force the fruit
    // collection item into a different SSTable which won't be in the prepare
    // range and so won't be visited during that CompactKVStore call.
    ASSERT_TRUE(magmaKVStore.newCheckpoint(vbid));

    auto key = makeStoredDocKey("fruitKey", CollectionEntry::fruit);
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(2, magmaKVStore.getItemCount(vbid));

    ASSERT_TRUE(magmaKVStore.newCheckpoint(vbid));

    cm.remove(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, magmaKVStore.getItemCount(vbid));
    ASSERT_EQ(1, vb->getNumTotalItems());

    bool workdone = false;
    magmaKVStore.setPreCompactKVStoreHook(
            [this, &cm, &vb, &key, &magmaKVStore, &workdone]() {
                if (workdone) {
                    return;
                }
                workdone = true;
                // Collection resurrection during compaction and we write
                // a "logical" insert which needs to adjust the dropped
                // stats
                cm.add(CollectionEntry::fruit);
                vb->updateFromManifest(
                        folly::SharedMutex::ReadHolder(vb->getStateLock()),
                        makeManifest(cm));
                flushVBucketToDiskIfPersistent(vbid, 1);
                EXPECT_EQ(2, magmaKVStore.getItemCount(vbid));
                EXPECT_EQ(1, vb->getNumTotalItems());

                store_item(vbid, key, "v2");
                flushVBucketToDiskIfPersistent(vbid, 1);
                // Before compaction expect to count for each store of fruitKey
                // getItemCount includes fruit system event too
                EXPECT_EQ(3, magmaKVStore.getItemCount(vbid));
                EXPECT_EQ(2, vb->getNumTotalItems());
            });

    runCompaction(vbid);
    EXPECT_EQ(2, magmaKVStore.getItemCount(vbid));

    // The "logical" insert from the hook still exists so doc count should be 1
    // still
    EXPECT_EQ(1, vb->getNumTotalItems());

    auto [status, count] = magmaKVStore.getDroppedCollectionItemCount(
            vbid, CollectionEntry::fruit);
    EXPECT_EQ(0, count);

    // Remove the collection again, we should be able to clean up everything
    // and get the item count back down to 0.
    cm.remove(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, magmaKVStore.getItemCount(vbid));

    std::tie(status, count) = magmaKVStore.getDroppedCollectionItemCount(
            vbid, CollectionEntry::fruit);
    EXPECT_EQ(1, count);

    magmaKVStore.setPreCompactKVStoreHook([]() {});
    runCompaction(vbid);
    // MB-50519: This was negative prior to fixing
    EXPECT_EQ(0, magmaKVStore.getItemCount(vbid));

    std::tie(status, count) = magmaKVStore.getDroppedCollectionItemCount(
            vbid, CollectionEntry::fruit);
    EXPECT_EQ(0, count);

    EXPECT_EQ(0, vb->getNumTotalItems());
}

void STParamMagmaBucketTest::testDiskStateAfterCompactKVStore(
        std::function<void()> compactionCallback) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            1, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            1, vbid, StoredDocKey{"f", CollectionEntry::fruit}, "value"));
    flushVBucketToDiskIfPersistent(vbid, 1);

    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& epBucket = dynamic_cast<EPBucket&>(*engine->getKVBucket());
    auto* kvstore = epBucket.getRWUnderlying(vbid);

    CompactionConfig config;
    // Create a compaction context and cache the real compaction completion
    // callback
    auto ctx = epBucket.makeCompactionContext(vbid, config, 0);
    auto realCompletionCallback = ctx->completionCallback;
    ctx->completionCallback = [this,
                               &realCompletionCallback,
                               &kvstore,
                               &compactionCallback](CompactionContext& ctx) {
        compactionCallback();
        realCompletionCallback(ctx);
    };

    auto vb = store->getLockedVBucket(vbid, std::try_to_lock);
    auto& lock = vb.getLock();

    ASSERT_EQ(1, kvstore->getItemCount(vbid));
    ObjectRegistry::onSwitchThread(engine.get());
    kvstore->compactDB(lock, ctx);
}

TEST_P(STParamMagmaBucketTest, ConsistentStateAfterCompactKVStoreCallDocCount) {
    auto* kvstore = store->getRWUnderlying(vbid);
    testDiskStateAfterCompactKVStore([this, &kvstore]() {
        // This is the test. After we run CompactKVStore but before we
        // complete the compaction we call the completionCallback.
        // Before the fix local docs and item count were not up to date
        // at this point.
        EXPECT_EQ(0, kvstore->getItemCount(vbid));
    });

    EXPECT_EQ(0, kvstore->getItemCount(vbid));
}

/**
 * MB-47487: Test to ensure we dont drop tombstones whose keys can be restored
 * via rollback. If the tombstone is dropped, magma will not restore the key to
 * the kv_engine hashTable via the rollback callback ie. the key will go missing
 * in case of valueEviction.
 */
TEST_P(STParamMagmaBucketTest, ImplicitCompactionTombstoneRollback) {
    auto rollbackKey = makeStoredDocKey("rollbackKey");
    int64_t highSeqno{0};
    MockMagmaKVStore* magmaKVStore;
    setupForImplicitCompactionTest(
            [this, &rollbackKey, &highSeqno, &magmaKVStore]() {
                auto vb = store->getVBucket(vbid);
                magmaKVStore = dynamic_cast<MockMagmaKVStore*>(
                        store->getRWUnderlying(vbid));
                ASSERT_TRUE(magmaKVStore);

                store_item(vbid, rollbackKey, "value");
                flushVBucketToDiskIfPersistent(vbid, 1);

                // Store highSeqno to rollback to and create a checkpoint
                highSeqno = vb->getHighSeqno();
                ASSERT_TRUE(magmaKVStore->newCheckpoint(vbid));

                // Delete the key
                delete_item(vbid, rollbackKey);
                flushVBucketToDiskIfPersistent(vbid, 1);

                // Create a checkpoint to force a flush
                ASSERT_TRUE(magmaKVStore->newCheckpoint(vbid));
            },
            []() {},
            [this, &rollbackKey, &magmaKVStore]() {
                // Assert that the rollbackKey's tombstone is still around
                auto gv = magmaKVStore->get(DiskDocKey(rollbackKey), vbid);
                ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
            });

    doRollbackAndVerifyCallback(highSeqno, rollbackKey);
}

/**
 * MB-47487: Test to ensure we dont drop completed prepares which can be
 * restored via rollback
 */
TEST_P(STParamMagmaBucketTest, ImplicitCompactionCompletedPrepareRollback) {
    auto purgedKey = makeStoredDocKey("keyPrepare");
    int64_t highSeqno{0};
    MockMagmaKVStore* magmaKVStore;
    setupForImplicitCompactionTest(
            [this, &purgedKey, &highSeqno, &magmaKVStore]() {
                auto vb = store->getVBucket(vbid);
                magmaKVStore = dynamic_cast<MockMagmaKVStore*>(
                        store->getRWUnderlying(vbid));
                ASSERT_TRUE(magmaKVStore);

                store_item(vbid,
                           purgedKey,
                           "value",
                           0 /*exptime*/,
                           {cb::engine_errc::sync_write_pending} /*expected*/,
                           PROTOCOL_BINARY_RAW_BYTES,
                           {cb::durability::Requirements()});
                flushVBucketToDiskIfPersistent(vbid, 1);

                // Store highSeqno to rollback to and create a checkpoint
                // We will be rolling back to undo the complete drop via
                // implicit compaction
                highSeqno = vb->getHighSeqno();
                ASSERT_TRUE(magmaKVStore->newCheckpoint(vbid));

                // Complete the prepare
                EXPECT_EQ(cb::engine_errc::success,
                          vb->seqnoAcknowledged(
                                  folly::SharedMutex::ReadHolder(
                                          vb->getStateLock()),
                                  "replica",
                                  vb->getHighSeqno() /*prepareSeqno*/));
                vb->processResolvedSyncWrites();
                flushVBucketToDiskIfPersistent(vbid, 1);

                // Create a checkpoint to force a flush
                ASSERT_TRUE(magmaKVStore->newCheckpoint(vbid));
            },
            []() {},
            [this, &purgedKey, &magmaKVStore]() {
                // Assert that the prepare is still around
                auto gv = magmaKVStore->get(
                        DiskDocKey(purgedKey, true /*prepare*/), vbid);
                ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
            });

    doRollbackAndVerifyCallback(highSeqno, purgedKey);
}

/**
 * Test that disk state is "consistent" if we crash in the middle of a
 * compaction after a CompactKVStore call but before we finalize local doc
 * updates. Before, dropped collection stats were tracking in local docs which
 * remained and caused us to underflow counters on a subsequent compaction.
 */
TEST_P(STParamMagmaBucketTest,
       ConsistentStateAfterCompactKVStoreCallDroppedStats) {
    testDiskStateAfterCompactKVStore(
            [this]() { throw std::runtime_error("oops"); });

    resetEngineAndWarmup();

    replaceMagmaKVStore();

    // Run the compaction again and before the fix we'd underflow the vBucket
    // doc count
    runCompaction(vbid);

    auto* kvstore = store->getRWUnderlying(vbid);
    EXPECT_EQ(0, engine->getEpStats().compactionFailed);

    EXPECT_EQ(0, kvstore->getItemCount(vbid));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems());

    // Dropped collection stats should be gone now.
    auto* magmaKVStore =
            dynamic_cast<MockMagmaKVStore*>(store->getRWUnderlying(vbid));
    auto stats = magmaKVStore->public_getMagmaDbStats(vbid);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(stats->droppedCollectionCounts.empty());
}

// Raised as MB-49472 following work on MB-48659
TEST_P(STParamMagmaBucketTest, ResurrectCollectionDuringCompaction) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Create and drop fruit (with some items in it)
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Drop collection
    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 3) Start purge, add the delete of the deleted stats doc
    CompactionConfig config;
    auto& epBucket = dynamic_cast<EPBucket&>(*engine->getKVBucket());
    auto ctx = epBucket.makeCompactionContext(vbid, config, 0);
    auto realCompletionCallback = ctx->completionCallback;

    ctx->completionCallback = [this, &realCompletionCallback, &cm](
                                      CompactionContext& ctx) {
        // Recreate and drop again
        setCollections(cookie, cm.add(CollectionEntry::fruit));
        flushVBucketToDiskIfPersistent(vbid, 1);

        store_item(vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "v1");
        flushVBucketToDiskIfPersistent(vbid, 1);

        setCollections(cookie, cm.remove(CollectionEntry::fruit));
        flushVBucketToDiskIfPersistent(vbid, 1);

        // Doing these two steps in the completionCallback should work
        realCompletionCallback(ctx);
    };

    auto vb = store->getLockedVBucket(vbid, std::try_to_lock);
    auto* kvstore = epBucket.getRWUnderlying(vbid);

    {
        ObjectRegistry::onSwitchThread(engine.get());
        EXPECT_EQ(CompactDBStatus::Success,
                  kvstore->compactDB(vb.getLock(), ctx));
    }

    auto [status, dropped] = kvstore->getDroppedCollections(vbid);
    EXPECT_TRUE(status);
    EXPECT_EQ(1, dropped.size());
    EXPECT_EQ(CollectionEntry::fruit.getId(), dropped.front().collectionId);

    ctx = epBucket.makeCompactionContext(vbid, config, 0);
    ctx->completionCallback = realCompletionCallback;
    {
        ObjectRegistry::onSwitchThread(engine.get());
        EXPECT_EQ(CompactDBStatus::Success,
                  kvstore->compactDB(vb.getLock(), ctx));
    }

    std::tie(status, dropped) = kvstore->getDroppedCollections(vbid);
    EXPECT_TRUE(status);
    EXPECT_TRUE(dropped.empty());
}

#if defined(HAVE_JEMALLOC)
// Test reproduces issue of MB-55711 where we see the memory domains get out
// of sync. A free of data on the wrong domain.
TEST_P(STParamMagmaBucketTest, getDbFileInfo_MemoryDomainLeak) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid, makeStoredDocKey("keyA"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto primaryDomainSz = cb::ArenaMalloc::getPreciseAllocated(
            engine->getArenaMallocClient(), cb::MemoryDomain::Primary);
    auto secondaryDomainSz = cb::ArenaMalloc::getPreciseAllocated(
            engine->getArenaMallocClient(), cb::MemoryDomain::Secondary);
    auto memUsed = engine->getEpStats().getPreciseTotalMemoryUsed();
    EXPECT_NE(0, primaryDomainSz);
    EXPECT_NE(0, secondaryDomainSz);
    EXPECT_NE(0, memUsed);

    auto* kvstore = store->getRWUnderlying(vbid);
    ASSERT_TRUE(kvstore);
    kvstore->getDbFileInfo(vbid);
    EXPECT_EQ(
            primaryDomainSz,
            cb::ArenaMalloc::getPreciseAllocated(engine->getArenaMallocClient(),
                                                 cb::MemoryDomain::Primary));
    EXPECT_EQ(
            secondaryDomainSz,
            cb::ArenaMalloc::getPreciseAllocated(engine->getArenaMallocClient(),
                                                 cb::MemoryDomain::Secondary));
    EXPECT_EQ(memUsed, engine->getEpStats().getPreciseTotalMemoryUsed());
}
#endif

INSTANTIATE_TEST_SUITE_P(STParamMagmaBucketTest,
                         STParamMagmaBucketTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
