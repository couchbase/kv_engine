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
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/thread_gate.h"
#include <utilities/test_manifest.h>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

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
};

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 *
 * @TODO MB-38377: With proper magma IO error injection we should turn off
 * background threads and use the IO error injection instead of mock functions.
 */
TEST_P(STParamMagmaBucketTest, ResetPCursorAtPersistNonMetaItems) {
    replaceMagmaKVStore();

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

    auto& kvStore =
            dynamic_cast<MockMagmaKVStore&>(*store->getRWUnderlying(vbid));
    const auto checkCachedAndOnDiskVBState = [this, &kvStore](
                                                     uint64_t lastSnapStart,
                                                     uint64_t lastSnapEnd,
                                                     uint64_t highSeqno,
                                                     CheckpointType type,
                                                     uint64_t hps,
                                                     uint64_t hcs,
                                                     uint64_t maxDelRevSeqno) {
        const auto& cached = *kvStore.getCachedVBucketState(vbid);
        const auto& onDisk = kvStore.readVBStateFromDisk(vbid).vbstate;
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
    kvStore.saveDocsErrorInjector = [](VB::Commit& cmt,
                                       kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };
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
    kvStore.saveDocsErrorInjector = nullptr;
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
        EXPECT_TRUE(magmaKVStore->compactDB(vb.getLock(), cctx));
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
    getEPBucket().warmupCompleted();

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

TEST_P(STParamMagmaBucketTest,
       CheckImplicitCompactionDoesNotUpdatePurgeSeqnoForLogicallyDeletedItem) {
    uint64_t expectedPurgeSeqno;
    auto purgedKey = makeStoredDocKey("keyA", CollectionEntry::fruit.getId());
    CollectionsManifest cm;

    setupForImplicitCompactionTest(
            [this, &expectedPurgeSeqno, &purgedKey, &cm]() {
                auto vb = store->getVBucket(vbid);
                expectedPurgeSeqno = vb->getHighSeqno();

                cm.add(CollectionEntry::fruit);
                vb->updateFromManifest(makeManifest(cm));
                store_item(vbid, purgedKey, "value");
                flushVBucketToDiskIfPersistent(vbid, 2);
            },
            [this, &cm]() {
                // Now remove the collection (which won't get purged as this is
                // run after the time travel. This can't be purged or it moves
                // the purge seqno and invalidates the test
                auto vb = store->getVBucket(vbid);
                cm.remove(CollectionEntry::fruit);
                vb->updateFromManifest(makeManifest(cm));
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);
    // Assert that the collection key no longer has a tomb stone
    auto gv = magmaKVStore->get(DiskDocKey(purgedKey), vbid);
    ASSERT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

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
    vb->updateFromManifest(makeManifest(cm));
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
    vb->updateFromManifest(makeManifest(cm));
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
    ThreadGate compactionGate(2);
    ThreadGate compactionCompletionGate(2);

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
                               &compactionCompletionGate,
                               runWithFix,
                               realCompletionCallback](CompactionContext& ctx) {
        compactionCompletionGate.threadUp();
        if (runWithFix) {
            realCompletionCallback(ctx);
        } else {
            auto vb = engine->getVBucket(vbid);
            vb->maybeSetPurgeSeqno(ctx.getRollbackPurgeSeqno());
            vb->decrNumTotalItems(ctx.stats.collectionsItemsPurged);
        }
    };

    // Lambda to perform the compaction on another thread
    auto tPerformCompaction = [this,
                               ctx,
                               &epBucket,
                               &compactionGate,
                               &compactionCompletionGate]() {
        ObjectRegistry::onSwitchThread(engine.get());
        auto* kvstore = epBucket.getRWUnderlying(vbid);
        auto vb = store->getLockedVBucket(vbid, std::try_to_lock);
        auto& lock = vb.getLock();
        compactionGate.threadUp();
        bool result = false;
        // Ensure that the compaction doesn't throw and completes successfully
        EXPECT_NO_THROW(result = kvstore->compactDB(lock, ctx));
        EXPECT_TRUE(result);
        // if for some reason the compaction failed and the compaction callback
        // wasn't called ensure we don't hang the main thread by calling
        // threadUp()
        if (!result && !compactionCompletionGate.isComplete()) {
            compactionCompletionGate.threadUp();
        }
    };
    // Start compaction thread
    std::thread compactionThread(tPerformCompaction);
    // Wait till the compaction task is about to compact
    compactionGate.threadUp();
    // Now that compaction has been started reset the vbucket by rolling back to
    // seqno 0
    EXPECT_NE(TaskStatus::Abort, epBucket.rollback(vbid, 0));
    // After we've run rollback let the compaction callback run
    compactionCompletionGate.threadUp();
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

    // Compaction for one KVStore passes and another "fails". Given we update
    // stats with the dropped stats docs on success we'll check the vb item
    // count to check how many items were "purged".
    runCompaction(vbid);
    EXPECT_EQ(1, vb->getNumTotalItems());

    // Our hook wouldn't do anything now, but reset it anyway for simplicity
    // and run the compaction again allowing the other collection to compact.
    magmaKVStore.setCompactionStatusHook([](magma::Status&) {});
    runCompaction(vbid);

    // Items all gone, before the fix 1 would remain
    EXPECT_EQ(0, vb->getNumTotalItems());
    EXPECT_EQ(1, magmaKVStore.getKVStoreStat().numCompactionFailure);
}

INSTANTIATE_TEST_SUITE_P(STParamMagmaBucketTest,
                         STParamMagmaBucketTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
