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

#include "checkpoint_remover_test.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_utils.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/response.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <climits>

void CheckpointRemoverTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    // Note: By MB-47386 the default max_checkpoints has be set to 10 but we
    // still run this testsuite with the original max_checkpoints=2. Reason is
    // that a bunch of (logic) tests inspect the inner checkpoint queues and
    // rely on max_checkpoints=2 for knowing where to find items. Note that
    // max_checkpoints doesn't have any logic-change implication.
    config_string +=
            "max_vbuckets=8;checkpoint_remover_task_count=2;max_checkpoints=2";

    STParameterizedBucketTest::SetUp();
}

/**
 * Checks that CheckpointRemoverTask orders vbuckets to visit by "highest
 * checkpoint mem-usage" order. Also verifies that vbuckets are sharded across
 * tasks.
 */
TEST_P(CheckpointRemoverTest, CheckpointRemoverVBucketOrder) {
    const auto numVBuckets = 5;
    for (uint16_t vbid = 0; vbid < numVBuckets; ++vbid) {
        setVBucketStateAndRunPersistTask(Vbid(vbid), vbucket_state_active);
        // Note: higher the vbid higher the num of items loaded.
        for (uint16_t seqno = 0; seqno < vbid; ++seqno) {
            store_item(Vbid(vbid),
                       makeStoredDocKey("key_" + std::to_string(vbid) + "_" +
                                        std::to_string(seqno)),
                       "value");
        }
    }

    auto& config = engine->getConfiguration();
    const auto numRemovers = 2;
    ASSERT_EQ(numRemovers, config.getCheckpointRemoverTaskCount());

    for (uint8_t removerId = 0; removerId < numRemovers; ++removerId) {
        const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
                *engine, engine->getEpStats(), removerId);

        // std::vector<std::pair<Vbid, size_t>>
        const auto vbuckets = remover->getVbucketsSortedByChkMem();

        // Usual modulo computation for shards, expected:
        // - vbids {0, 2, 4} -> removerId 0
        // - vbid {1, 3} -> removerId 1
        // .. and all in descending checkpoint mem-usage order
        if (removerId == 0) {
            ASSERT_EQ(3, vbuckets.size());
            EXPECT_EQ(4, vbuckets.at(0).first.get());
            EXPECT_EQ(2, vbuckets.at(1).first.get());
            EXPECT_EQ(0, vbuckets.at(2).first.get());
            EXPECT_GE(vbuckets.at(0).second, vbuckets.at(1).second);
            EXPECT_GE(vbuckets.at(1).second, vbuckets.at(2).second);
        } else {
            ASSERT_EQ(2, vbuckets.size());
            EXPECT_EQ(3, vbuckets.at(0).first.get());
            EXPECT_EQ(1, vbuckets.at(1).first.get());
            EXPECT_GE(vbuckets.at(0).second, vbuckets.at(1).second);
        }
    }
}

/**
 * Test CheckpointManager correctly returns which cursors we are eligible to
 * drop. We should not be allowed to drop any cursors in a checkpoint when the
 * persistence cursor is present.
 */
TEST_P(CheckpointRemoverEPTest, CursorsEligibleToDrop) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    // We should have one checkpoint which is for the state change
    ASSERT_EQ(1, checkpointManager->getNumCheckpoints());
    // We should only have one cursor, which is for persistence
    ASSERT_EQ(1, checkpointManager->getNumOfCursors());

    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);

    // The persistence cursor is still within the current checkpoint,
    // so we should not be allowed to drop any cursors at this time
    auto cursors = checkpointManager->getListOfCursorsToDrop();
    ASSERT_EQ(0, cursors.size());

    // Create a DCP stream for the vBucket, and check that we now have 2 cursors
    // registered
    createDcpStream(*producer);
    ASSERT_EQ(2, checkpointManager->getNumOfCursors());

    // Insert a few items to the vBucket so we create a new checkpoint
    const auto value = std::string(
            checkpointManager->getCheckpointConfig().getCheckpointMaxSize() / 4,
            'x');
    for (size_t i = 0; checkpointManager->getNumCheckpoints() < 2; ++i) {
        store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), value);
    }

    // We should now have 2 checkpoints for this vBucket
    ASSERT_EQ(2, checkpointManager->getNumCheckpoints());

    // Run the persistence task for this vBucket, this should advance the
    // persistence cursor out of the first checkpoint
    flushVBucket(vbid);

    // We should now be eligible to drop the user created DCP stream from the
    // checkpoint
    cursors = checkpointManager->getListOfCursorsToDrop();
    ASSERT_EQ(1, cursors.size());
    ActiveStream& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
    ASSERT_EQ(activeStream.getCursor().lock(), cursors[0].lock());
}

// Test that we correctly determine whether to trigger memory recovery.
TEST_P(CheckpointRemoverEPTest, MemoryRecoveryTrigger) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    const size_t bucketQuota = 100_MiB;
    auto& config = engine->getConfiguration();
    config.setMaxSize(bucketQuota);
    auto& stats = engine->getEpStats();
    EXPECT_EQ(bucketQuota, stats.getMaxDataSize());

    // No item stored, no memory condition that triggers mem-recovery
    const auto checkpointMemoryLimit =
            bucketQuota * store->getCheckpointMemoryRatio();
    EXPECT_LT(stats.getCheckpointManagerEstimatedMemUsage(),
              checkpointMemoryLimit);
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);
    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());

    // Place a cursor to prevent eager checkpoint removal
    auto& manager = static_cast<MockCheckpointManager&>(
            *engine->getVBucket(vbid)->checkpointManager);
    const auto dcpCursor =
            manager.registerCursorBySeqno(
                           "dcp", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock();
    ASSERT_TRUE(dcpCursor);

    // Now store some items so that the mem-usage in checkpoint crosses the
    // Checkpoint Quota but the overall mem-usage in the system doesn't hit the
    // global LWM.
    size_t numItems = 0;
    do {
        const auto value = std::string(bucketQuota / 10, 'x');
        auto item =
                make_item(vbid,
                          makeStoredDocKey("key_" + std::to_string(++numItems)),
                          value,
                          0 /*exp*/,
                          PROTOCOL_BINARY_RAW_BYTES);
        store->set(item, cookie);
    } while (stats.getCheckpointManagerEstimatedMemUsage() <
             checkpointMemoryLimit);

    // Need to flush to disk for full-eviction buckets so getNumItems() returns
    // the correct value.
    flushVBucket(vbid);
    auto vb = store->getVBucket(vbid);
    EXPECT_GT(vb->getNumItems(), 0);
    EXPECT_EQ(numItems, vb->getNumItems());
    EXPECT_GT(stats.getCheckpointManagerEstimatedMemUsage(),
              checkpointMemoryLimit);
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);

    // Checkpoint mem-recovery must trigger (regardless of any LWM)
    EXPECT_GT(store->getRequiredCMMemoryReduction(), 0);
}

// Test that we correctly determine when to stop memory recovery.
TEST_P(CheckpointRemoverEPTest, MemoryRecoveryEnd) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    scheduleCheckpointRemoverTask();

    const size_t bucketQuota = 100_MiB;
    auto& config = engine->getConfiguration();
    config.setMaxSize(bucketQuota);
    // set the max checkpoint size excessively high - need the test to
    // trigger expelling rather than checkpoint removal; avoid creating
    // multiple checkpoints.
    config.setCheckpointMaxSize(bucketQuota);

    auto& stats = engine->getEpStats();
    ASSERT_EQ(bucketQuota, stats.getMaxDataSize());

    // No item stored, no memory condition that triggers mem-recovery
    const auto checkpointMemoryLimit =
            bucketQuota * store->getCheckpointMemoryRatio();
    ASSERT_LT(stats.getCheckpointManagerEstimatedMemUsage(),
              checkpointMemoryLimit);
    ASSERT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);
    ASSERT_EQ(0, store->getRequiredCMMemoryReduction());

    // Now store some items so that the mem-usage in checkpoint crosses the
    // Checkpoint Quota
    size_t numItems = 0;
    do {
        const auto value = std::string(bucketQuota / 100, 'x');
        auto item =
                make_item(vbid,
                          makeStoredDocKey("key_" + std::to_string(++numItems)),
                          value,
                          0 /*exp*/,
                          PROTOCOL_BINARY_RAW_BYTES);
        store->set(item, cookie);
    } while (stats.getCheckpointManagerEstimatedMemUsage() <
             checkpointMemoryLimit);

    flushVBucketToDiskIfPersistent(vbid, gsl::narrow_cast<int>(numItems));

    auto vb = store->getVBucket(vbid);
    ASSERT_GT(vb->getNumItems(), 0);
    ASSERT_EQ(numItems, vb->getNumItems());
    ASSERT_GT(stats.getCheckpointManagerEstimatedMemUsage(),
              checkpointMemoryLimit);
    ASSERT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);

    ASSERT_TRUE(store->isCMMemoryReductionRequired());

    ASSERT_EQ(stats.itemsExpelledFromCheckpoints, 0);

    // run the remover to trigger expelling
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIO, "CheckpointMemRecoveryTask:0");
    runNextTask(nonIO, "CheckpointMemRecoveryTask:1");

    // some items should have been expelled
    EXPECT_GT(stats.itemsExpelledFromCheckpoints, 0);

    const auto checkpointMemoryRatio = store->getCheckpointMemoryRatio();
    const auto checkpointQuota = stats.getMaxDataSize() * checkpointMemoryRatio;
    const auto usage = stats.getCheckpointManagerEstimatedMemUsage();

    const auto lowerRatio = store->getCheckpointMemoryRecoveryLowerMark();
    const auto lowerMark = checkpointQuota * lowerRatio;
    // we are now below the low mark
    EXPECT_LE(usage, lowerMark);

    // and no longer need to reduce checkpoint memory
    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());
}

void CheckpointRemoverTest::testExpellingOccursBeforeCursorDropping(
        MemRecoveryMode mode) {
    // 1) Get enough checkpoint metadata to trigger expel
    // 2) doesn't hit maxDataSize first
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();

    auto vb = store->getVBuckets().getBucket(vbid);
    auto* manager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<CheckpointCursor> cursor;
    if (mode != MemRecoveryMode::ItemExpelWithoutCursor) {
        producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
        createDcpStream(*producer);
        ActiveStream& activeStream =
                reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
        cursor = activeStream.getCursor().lock();
    }

    config.setChkExpelEnabled(true);
    config.setMaxSize(100_MiB);
    // Disable the mem-based checkpoint creation in this test, we would end up
    // doing straight CheckpointRemoval rather than ItemExpel/CursorDrop
    config.setCheckpointMaxSize(std::numeric_limits<size_t>::max());
    const auto chkptMemRecoveryLimit =
            config.getMaxSize() * store->getCheckpointMemoryRatio() *
            store->getCheckpointMemoryRecoveryUpperMark();
    auto& stats = engine->getEpStats();
    stats.mem_low_wat.store(1);

    int ii = 0;
    const auto value = std::string(1_MiB, 'x');
    while (stats.getCheckpointManagerEstimatedMemUsage() <
           chkptMemRecoveryLimit) {
        std::string doc_key = "key_" + std::to_string(ii);
        store_item(vbid, makeStoredDocKey(doc_key), value);
        ++ii;
    }
    flushVBucketToDiskIfPersistent(vbid, ii);

    if (mode == MemRecoveryMode::CursorDrop) {
        // Force checkpoint closing/creation - CursorDrop wouldn't kick in
        // otherwise
        manager->createNewCheckpoint();
    }

    const auto inititalNumCheckpoints = stats.getNumCheckpoints();
    EXPECT_GT(inititalNumCheckpoints, 0);

    const auto memToClear = store->getRequiredCMMemoryReduction();
    EXPECT_GT(memToClear, 0);

    if (mode == MemRecoveryMode::ItemExpelWithCursor) {
        // Advance cursor so ItemExpel can occur.
        std::vector<queued_item> items;
        manager->getNextItemsForDcp(*cursor, items);
    }

    const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);
    remover->run();
    getCkptDestroyerTask(vbid)->run();

    switch (mode) {
    case MemRecoveryMode::ItemExpelWithCursor:
    case MemRecoveryMode::ItemExpelWithoutCursor:
        EXPECT_EQ(stats.getNumCheckpoints(), inititalNumCheckpoints);
        break;
    case MemRecoveryMode::CursorDrop:
        EXPECT_LT(stats.getNumCheckpoints(), inititalNumCheckpoints);
        break;
    }

    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());
}

void CheckpointRemoverTest::testGetBytesToFree(
        KVBucket& bucket,
        std::shared_ptr<CheckpointMemRecoveryTask> remover,
        bool triggeredByHWM) {
    if (triggeredByHWM) {
        // Should free to bucket LWM.
        auto expectedBytesToFree = bucket.getPageableMemCurrent() -
                                   bucket.getPageableMemLowWatermark();
        auto target = CheckpointMemRecoveryTask::ReductionTarget::BucketLWM;
        auto bytesToFree = remover->getBytesToFree(target);
        EXPECT_EQ(expectedBytesToFree, bytesToFree);
    } else {
        // Should free to checkpoint lower mark.
        auto target =
                CheckpointMemRecoveryTask::ReductionTarget::CheckpointLowerMark;
        auto bytesToFree = remover->getBytesToFree(target);
        EXPECT_EQ(bucket.getRequiredCMMemoryReduction(), bytesToFree);
    }
}

// Test that we correctly apply expelling before cursor dropping.
TEST_P(CheckpointRemoverTest, expelButNoCursorDrop) {
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.memFreedByCheckpointRemoval);
    ASSERT_EQ(0, stats.memFreedByCheckpointItemExpel);

    testExpellingOccursBeforeCursorDropping(
            MemRecoveryMode::ItemExpelWithCursor);
    EXPECT_NE(0, stats.itemsExpelledFromCheckpoints);
    EXPECT_EQ(0, stats.cursorsDropped);
    EXPECT_EQ(0, stats.memFreedByCheckpointRemoval);
    EXPECT_GT(stats.memFreedByCheckpointItemExpel, 0);
}

// Test that we correctly trigger cursor dropping when have checkpoint
// with cursor at the start and so cannot use expelling.
TEST_P(CheckpointRemoverTest, notExpelButCursorDrop) {
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.memFreedByCheckpointItemExpel);
    ASSERT_EQ(0, stats.memFreedByCheckpointRemoval);

    testExpellingOccursBeforeCursorDropping(MemRecoveryMode::CursorDrop);
    EXPECT_EQ(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(1, engine->getEpStats().cursorsDropped);
    EXPECT_EQ(0, stats.memFreedByCheckpointItemExpel);
    EXPECT_GT(stats.memFreedByCheckpointRemoval, 0);
}

// Test that we correctly apply expelling when there are zero cursors (e.g.
// ephemeral)
TEST_P(CheckpointRemoverTest, expelWithoutCursor) {
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.memFreedByCheckpointRemoval);
    ASSERT_EQ(0, stats.memFreedByCheckpointItemExpel);

    testExpellingOccursBeforeCursorDropping(
            MemRecoveryMode::ItemExpelWithoutCursor);
    EXPECT_NE(0, stats.itemsExpelledFromCheckpoints);
    EXPECT_EQ(0, stats.cursorsDropped);
    EXPECT_EQ(0, stats.memFreedByCheckpointRemoval);
    EXPECT_GT(stats.memFreedByCheckpointItemExpel, 0);
}

TEST_P(CheckpointRemoverTest, MemRecoveryByCheckpointCreation) {
    setVBucketStateAndRunPersistTask(Vbid(0), vbucket_state_active);
    setVBucketStateAndRunPersistTask(Vbid(1), vbucket_state_active);

    auto& config = engine->getConfiguration();
    config.setChkExpelEnabled(true);
    config.setMaxSize(100_MiB);

    ASSERT_EQ(0, store->getRequiredCMMemoryReduction());

    // Compute paylaod size such that we enter a TempOOM phase when we store
    // the second item.
    const size_t valueSize =
            config.getMaxSize() * config.getCheckpointMemoryRatio() *
                    config.getCheckpointMemoryRecoveryUpperMark() / 2 +
            1;
    const auto value = std::string(valueSize, 'x');
    // Store first item, no checkpoint OOM yet
    store_item(Vbid(0), makeStoredDocKey("keyA"), value);
    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());
    // Store second item, Checkpoint OOM
    store_item(Vbid(1), makeStoredDocKey("keyB"), value);
    ASSERT_GT(store->getRequiredCMMemoryReduction(), 0);

    // Move the cursors to the end of the open checkpoint. Step required to
    // allow checkpoint creation + cursor jumping into the new checkpoints in
    // the next steps
    flushVBucketToDiskIfPersistent(Vbid(0), 1);
    flushVBucketToDiskIfPersistent(Vbid(1), 1);

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.itemsExpelledFromCheckpoints);
    const auto initialRemoved = stats.itemsRemovedFromCheckpoints;

    // Mem-recovery is expected to:
    // 1. Create a new checkpoint on at least 1 vbucket
    // 2. Move the cursors from the closed checkpoint to the open one
    // 3. Remove the closed (and now unred) checkpoint
    const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);
    remover->run();

    // That allows to remove checkpoints and recover from OOM
    // Before the fix, nothing removed from checkpoints and mem-reduction still
    // required at this point
    EXPECT_EQ(0, stats.itemsExpelledFromCheckpoints);
    EXPECT_GT(stats.itemsRemovedFromCheckpoints, initialRemoved);
    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());
}

// Without the fix, there is a data race in
// CheckpointManager::takeAndResetCursors which did not take a queueLock,
// and could mutate the CheckpointManager while it is being accessed,
// e.g. in CheckpointManager::getListOfCursorsToDrop.
TEST_P(CheckpointRemoverTest, MB59601) {
    if (!isPersistent()) {
        GTEST_SKIP();
    }

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();
    config.setChkExpelEnabled(false);
    config.setMaxSize(100_MiB);
    // Disable the mem-based checkpoint creation in this test, we would end up
    // doing straight CheckpointRemoval rather than ItemExpel/CursorDrop
    config.setCheckpointMaxSize(std::numeric_limits<size_t>::max());
    const auto chkptMemRecoveryLimit =
            config.getMaxSize() * store->getCheckpointMemoryRatio() *
            store->getCheckpointMemoryRecoveryUpperMark();
    auto& stats = engine->getEpStats();
    stats.mem_low_wat.store(1);

    int numItems = 0;
    const std::string value(1_MiB, 'x');
    while (stats.getCheckpointManagerEstimatedMemUsage() <
           chkptMemRecoveryLimit) {
        auto docKey = "key_" + std::to_string(++numItems);
        store_item(vbid, makeStoredDocKey(docKey), value);
    }
    flushVBucketToDiskIfPersistent(vbid, numItems);

    // VB needs to be replica to rollback
    store->setVBucketState(vbid, vbucket_state_replica);

    EXPECT_GT(stats.getNumCheckpoints(), 0);
    EXPECT_GT(store->getRequiredCMMemoryReduction(), 0);

    /// Synchronises just before accessing and mutating CM::cursors
    ThreadGate tg(2);
    std::thread bgThread;

    auto& oldManager = *store->getVBucket(vbid)->checkpointManager;
    oldManager.takeAndResetCursorsHook = [this, &tg, &bgThread]() {
        // Note: takeAndResetCursorsHook is executed *after* the new VBucket
        // has already been created

        auto& newManager = *store->getVBucket(vbid)->checkpointManager;
        newManager.getListOfCursorsToDropHook = [&tg]() { tg.threadUp(); };
        bgThread = std::thread([this]() {
            auto remover = std::make_shared<CheckpointMemRecoveryTask>(
                    *engine, engine->getEpStats(), 0);
            remover->run();
        });

        tg.threadUp();
    };

    store->rollback(vbid, 0);
    bgThread.join();
}

// Test written for MB-36366. With the fix removed this test failed because
// post expel, we continued onto cursor dropping.
// MB-36447 - unreliable test, disabling for now
TEST_P(CheckpointRemoverEPTest, DISABLED_noCursorDropWhenTargetMet) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();
    const auto& task = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);

    auto vb = store->getVBuckets().getBucket(vbid);
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);

    bool isLastMutation;
    ActiveStream& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));

    config.setChkExpelEnabled(true);
    const size_t maxSize = 100000;
    config.setMaxSize(maxSize);
    config.setCheckpointMemoryRatio(0.35);
    // This value is forced to 1 so expel/cursor drop becomes eligible
    engine->getEpStats().mem_low_wat.store(1);

    int ii = 0;
    while (engine->getEpStats().getPreciseTotalMemoryUsed() <
           (maxSize * 0.75)) {
        // using small keys and values
        std::string doc_key = std::to_string(ii);
        store_item(vbid, makeStoredDocKey(doc_key), "a");
        ++ii;
    }

    // Create a second checkpoint add an item and flush, this moves the
    // persistence cursor into the second checkpoint making dcp cursor eligible
    // for dropping.
    checkpointManager->createNewCheckpoint();

    store_item(vbid, makeStoredDocKey("another"), "value");

    // We should now have 2 checkpoints for this vBucket
    ASSERT_EQ(2, checkpointManager->getNumCheckpoints());

    // Flush all and leave DCP behind
    flush_vbucket_to_disk(vbid, ii + 1);

    // Move the DCP cursor along so expelling can do some work
    {
        auto cursor = activeStream.getCursor().lock();

        // Move the cursor past 80% of the items added.
        for (int jj = 0; jj < ii * 0.8; ++jj) {
            checkpointManager->nextItem(cursor.get(), isLastMutation);
        }
    }

    // We expect expelling to have kicked in, but not cursor dropping
    task->run();
    EXPECT_NE(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(0, engine->getEpStats().cursorsDropped);
    EXPECT_TRUE(activeStream.getCursor().lock().get());
}

std::vector<queued_item> CheckpointRemoverEPTest::getItemsWithCursor(
        const std::string& name, uint64_t startBySeqno, bool expectBackfill) {
    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    auto regRes = cm->registerCursorBySeqno(
            "SomeName", startBySeqno, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(expectBackfill, regRes.tryBackfill);
    auto cursor = regRes.takeCursor().lock();

    std::vector<queued_item> items;
    cm->getNextItemsForDcp(*cursor, items);

    cm->removeCursor(*cursor);

    return items;
}

void CheckpointRemoverEPTest::createCheckpointAndEnsureOldRemoved(
        CheckpointManager& manager) {
    const auto openId = manager.getOpenCheckpointId();
    manager.createNewCheckpoint();
    flush_vbucket_to_disk(vbid, 0);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_GT(manager.getOpenCheckpointId(), openId);
}

/**
 * @todo MB-51295: Remove this test and the related code path in ItemExpel.
 * The test verifies that ItemExpel defers mem-recovery to checkpoint removal
 * if (a) there's more than 1 checkpoint and (b) the oldest checkpoint is
 * unreferenced. That is an impossible state by Eager checkpoint removal, as
 * closed/unref checkpoints are removed as soon as they become unref.
 * I just disable the test for now.
 */
TEST_P(CheckpointRemoverEPTest,
       DISABLED_expelsOnlyIfOldestCheckpointIsReferenced) {
    // Check to confirm checkpoint expelling will only run if there are cursors
    // in the oldest checkpoint. If there are not, the entire checkpoint should
    // be closed

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    /* adding three items because expelUnreferencedCheckpointItems will find the
     * earliest cursor, then step backwards while the item it points to
     * satisfies any of: A: its seqno is equal to the highSeqno of the
     * checkpoint B: it is a meta item C: it is preceded by another item with
     * the same seqno (meta items can share the same seqno as a subsequent item)
     *
     * If the dummy item which starts the checkpoint is reached, expelling bails
     * out early as nothing can be expelled.
     *
     * If it stops on an item which is not the dummy, this is then used as the
     * *last* item to expel.
     *
     * To actually expel anything we need three items. If we had one or two
     * items, all items in the checkpoint would then satisfy one of the above
     * cases, the dummy item would be reached and nothing would be expelled.
     */

    for (int i = 0; i < 3; i++) {
        store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), "value");
    }

    cm->createNewCheckpoint();

    for (int i = 3; i < 6; i++) {
        store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), "value");
    }

    flush_vbucket_to_disk(vbid, 6);

    // Persistence cursor advanced to the end, the first checkpoint is now
    // unreferenced; trying to expel should do nothing. Expelling from the first
    // checkpoint would be inefficient as it can be dropped as a whole
    // checkpoint, and no other checkpoint can be expelled without leaving
    // "holes" in the data a cursor would read.

    size_t beforeCount =
            getItemsWithCursor("Cursor1", 0, /* expect tryBackfill*/ true)
                    .size();
    cm->expelUnreferencedCheckpointItems();
    size_t afterCount = getItemsWithCursor("Cursor2", 0, true).size();

    EXPECT_EQ(beforeCount, afterCount);

    // Now, put a cursor in the first checkpoint. Now, expelling should remove
    // items from it as it is the oldest checkpoint.

    auto regRes = cm->registerCursorBySeqno(
            "Cursor3", 0, CheckpointCursor::Droppable::Yes);
    auto cursor = regRes.takeCursor().lock();

    /* items in first checkpoint
     *
     *   dummy     << cursor starts here
     *   chk start    |
     *   vb state     |
     *   key_0        V
     *   key_1     << advance to here
     *   key_2
     *   chk end
     */

    while (cursor->getKey().to_string() != "cid:0x0:key_1") {
        cm->incrCursor(*cursor);
    }

    // Can now expel the 2 items in (ckpt_start, cursor]
    auto result = cm->expelUnreferencedCheckpointItems();

    EXPECT_EQ(3, result.count);

    /* items in first checkpoint
     *
     *   dummy
     *   chk start
     *   key_2
     *   chk end
     */

    afterCount = getItemsWithCursor("Cursor4", 0, true).size();

    EXPECT_EQ(beforeCount - 3, afterCount);
}

TEST_P(CheckpointRemoverEPTest, earliestCheckpointSelectedCorrectly) {
    // MB-35812 - Confirm that checkpoint expelling correctly selects the
    // earliest cursor, and that the cursor is in the oldest reffed checkpoint.

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    createCheckpointAndEnsureOldRemoved(*cm);

    // queue a single item into checkpoint
    store_item(vbid, makeStoredDocKey("key_1"), "value");
    // queue a set vbstate meta item into checkpoint
    cm->queueSetVBState();

    cm->createNewCheckpoint();

    // persist, moves the persistence cursor to the new checkpoint start
    // to move it "out of the way" for this test
    flush_vbucket_to_disk(vbid, 1);

    /*
     * Checkpoint manager structure
     *                 seqno
     *  - dummy          1   << cursors start here
     *  - chptStart      1
     *  - item key_1     1
     *  - set_vb_state   2   << CursorB
     *  - chkptEnd       2
     *  -------
     *  - dummy          2   ** cursors skip this dummy
     *  - ckptStart      2   << CursorA
     */

    // Put a cursor in the second checkpoint
    auto regResA = cm->registerCursorBySeqno(
            "CursorA", 0, CheckpointCursor::Droppable::Yes);
    auto cursorA = regResA.takeCursor().lock();
    for (int i = 0; i < 5; i++) {
        cm->incrCursor(*cursorA);
    }

    // Put a cursor on the *last* item of the first checkpoint
    auto regResB = cm->registerCursorBySeqno(
            "CursorB", 0, CheckpointCursor::Droppable::Yes);
    auto cursorB = regResB.takeCursor().lock();
    for (int i = 0; i < 3; i++) {
        cm->incrCursor(*cursorB);
    }

    // Now, the items pointed to by each cursor both have the *same*
    // by seqno, but the cursors are in different checkpoints

    // Checkpoint expelling `Expects` that the earliest cursor
    // will be in the earliest reffed checkpoint.
    // This test is seen to fail prior to the fix for MB-35812
    // as the cursors were sorted only by seqno and CursorA would
    // be selected, despite not being in the oldest checkpoint.
    EXPECT_NO_THROW(cm->expelUnreferencedCheckpointItems());
}

/*
 * Ensure that ItemExpel correctly marks keyIndex entries for SyncWrite when a
 * prepare is expelled.
 *
 * MB-36338: expelling would incorrectly mark a keyIndex entry for a sync
 * write as non-sync write if it was the last item to be expelled, as the
 * value it checked was that of the dummy item rather than of the real item.
 */
TEST_P(CheckpointRemoverEPTest, ItemExpellingInvalidatesKeyIndexCorrectly) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    createCheckpointAndEnsureOldRemoved(*cm);

    // expelling will not remove items preceded by an item with the same seqno
    // (in the case, the checkpoint start meta item)
    // pad to allow the following prepare to be expelled
    store_item(vbid, makeStoredDocKey("padding1"), "value");

    auto prepareKey = makeStoredDocKey("key_1");

    // Queue a prepare
    auto prepare = makePendingItem(prepareKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie, nullptr /*StoreIfPredicate*/));

    {
        std::shared_lock rlh(vb->getStateLock());
        // Commit - we need this step as in the following we want to SyncWrite
        // again for the same key
        EXPECT_EQ(cb::engine_errc::success,
                  vb->commit(rlh,
                             prepareKey,
                             2,
                             {},
                             CommitType::Majority,
                             vb->lockCollections(prepareKey),
                             cookie));
    }

    EXPECT_EQ(1, cm->getNumCheckpoints());

    // Persist to move our cursor so that we can expel the prepare.
    // Note: ItemExpel can remove all items in a checkpoint, also high-seqno and
    // items pointed by cursors
    flushVBucketToDiskIfPersistent(vbid, 3);

    // expel from the checkpoint. This will invalidate keyIndex entries
    // for all expelled items.
    auto result = cm->expelUnreferencedCheckpointItems();
    EXPECT_EQ(3, result.count);

    const auto prepareCkptId = cm->getOpenCheckpointId();
    auto prepare2 = makePendingItem(prepareKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare2, cookie, nullptr /*StoreIfPredicate*/));

    // Queueing second prepare into the same checkpoint must fail as it would
    // dedupe the existing prepare. The old prepare was expelled but we know it
    // existed as the keyIndex still keeps track of that. So, the new prepare is
    // queued into a new open checkpoint. In the end we still 1 checkpoint as
    // the old one was removed.
    EXPECT_EQ(1, cm->getNumCheckpoints());
    EXPECT_GT(cm->getOpenCheckpointId(), prepareCkptId);
}

TEST_P(CheckpointRemoverEPTest, MB_48233) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Run the remover one first time. Before the fix this step leaves the
    // Task::available flag set to false, which prevent any further execution
    // of the removal logic at the next runs.
    const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);
    remover->run();

    auto& config = engine->getConfiguration();
    config.setChkExpelEnabled(true);
    config.setMaxSize(100_MiB);

    // Load to OOM. At the same time, makes just 1 item eligible for expelling.
    // Purpose here is:
    //  1. Hit OOM
    //  2. Give the MemoryRecoveryTask some item to expel
    //  3. Prevent eager checkpoint removal. (1) wouldn't be verified otherwise
    const auto value = std::string(1_MiB, 'x');
    auto ret = cb::engine_errc::success;
    for (size_t seqno = 1; ret != cb::engine_errc::no_memory; ++seqno) {
        auto item = make_item(
                vbid, makeStoredDocKey("key_" + std::to_string(seqno)), value);
        ret = store->set(item, cookie);

        if (seqno == 2) {
            flushVBucket(vbid);
        }
    }
    ASSERT_TRUE(KVBucket::isCheckpointMemoryStateFull(
            store->getCheckpointMemoryState()));

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0,
              stats.itemsExpelledFromCheckpoints +
                      stats.itemsRemovedFromCheckpoints);

    // Run the remover a second time.
    remover->run();

    // Before the fix a second execution just returns by Task::available=false,
    // so we wouldn't remove anything
    ASSERT_GT(stats.itemsExpelledFromCheckpoints +
                      stats.itemsRemovedFromCheckpoints,
              0);
}

/**
 * @todo MB-51295: Remove, this is a Lazy only test.
 * With Eager, CursorDrop is the only case where the CheckpointMemRecoveryTask
 * is expected to remove checkpoints.
 */
TEST_P(CheckpointRemoverEPTest, DISABLED_CheckpointRemovalWithoutCursorDrop) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();

    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);
    auto& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
    auto cursor = activeStream.getCursor().lock();

    config.setChkExpelEnabled(true);
    config.setMaxSize(100_MiB);
    const auto value = std::string(1_MiB, 'x');
    auto ret = cb::engine_errc::success;
    for (size_t i = 0; ret != cb::engine_errc::no_memory; ++i) {
        auto item = make_item(
                vbid, makeStoredDocKey("key_" + std::to_string(i)), value);
        ret = store->set(item, cookie);
    }

    // Create a new checkpoint to move all cursors into it
    manager.createNewCheckpoint();

    flushVBucket(vbid);
    const auto initialNumItems = vb->getNumItems();

    // Move the DCP cursor to make some checkpoints eligible for removal without
    // dropping the cursor
    {
        std::vector<queued_item> items;
        manager.getNextItemsForDcp(*cursor, items);
    }

    ASSERT_NE(0, store->getRequiredCMMemoryReduction());
    ASSERT_EQ(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    ASSERT_EQ(0, engine->getEpStats().itemsRemovedFromCheckpoints);
    ASSERT_EQ(0, engine->getEpStats().cursorsDropped);

    ASSERT_GT(store->getRequiredCMMemoryReduction(), 0);
    const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);
    remover->run();
    getCkptDestroyerTask(vbid)->run();

    EXPECT_EQ(0, store->getRequiredCMMemoryReduction());
    EXPECT_EQ(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(initialNumItems,
              engine->getEpStats().itemsRemovedFromCheckpoints);
    EXPECT_EQ(0, engine->getEpStats().cursorsDropped);
}

TEST_P(CheckpointRemoverTest, CursorMoveWakesDestroyer) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Schedule the Destroyer tasks, ready for use when it's time to wake it up.
    scheduleCheckpointDestroyerTasks();

    auto vb = engine->getVBucket(vbid);
    auto& cm = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    auto& epstats = engine->getEpStats();
    const auto initialMemUsed = epstats.getCheckpointManagerEstimatedMemUsage();
    const auto initialMemUsedCM = cm.getMemUsage();

    // The test covers both persistent/ephemeral
    auto dcpCursor =
            cm.registerCursorBySeqno("dcp", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock();
    ASSERT_TRUE(dcpCursor);

    // cs + vbs for persistent buckets
    ASSERT_EQ(ephemeral() ? 1 : 2, cm.getNumOpenChkItems());
    // Store an item
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    EXPECT_EQ(1, cm.getNumCheckpoints());
    EXPECT_EQ(ephemeral() ? 2 : 3, cm.getNumOpenChkItems());

    // Create new open checkpoint
    cm.createNewCheckpoint();
    EXPECT_EQ(2, cm.getNumCheckpoints());
    EXPECT_EQ(1, cm.getNumOpenChkItems());

    // Memory usage should be higher than it started
    const auto preDetachGlobalMemUsage =
            epstats.getCheckpointManagerEstimatedMemUsage();
    EXPECT_GT(preDetachGlobalMemUsage, initialMemUsed);
    const auto peakMemUsedCM = cm.getMemUsage();
    EXPECT_GT(cm.getMemUsage(), initialMemUsedCM);

    // The destroyer doesn't own anything yet, so should have no mem usage
    const auto& destroyer = getCkptDestroyerTask(vbid);
    EXPECT_EQ(0, destroyer->getMemoryUsage());
    EXPECT_EQ(0, destroyer->getNumCheckpoints());

    // Move cursors out of the old checkpoint.
    // That makes the old checkpoint closed/unref and queues it for destruction.
    // This operation is also expected to wake up the Destroyer - we run that
    // a few line down here.
    {
        std::vector<queued_item> items;
        cm.getNextItemsForDcp(*dcpCursor, items);
    }
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, cm.getNumCheckpoints());

    // The checkpoints should have been disassociated from their manager, so the
    // tracked memory usage should have decreased..
    EXPECT_LE(cm.getMemUsage(), initialMemUsedCM);
    // .. and now the checkpoint mem usage is accounted against the destroyer
    EXPECT_EQ(peakMemUsedCM - cm.getMemUsage(), destroyer->getMemoryUsage());
    EXPECT_EQ(1, destroyer->getNumCheckpoints());
    // Also the counter in EPStats accounts only checkpoints owned by CM, so it
    // must be already updated now that checkpoints are owned by the destroyer
    const auto postDetachGlobalMemUsage =
            epstats.getCheckpointManagerEstimatedMemUsage();
    EXPECT_LT(postDetachGlobalMemUsage, preDetachGlobalMemUsage);

    // Run the Destroyer
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIO, "Destroying closed unreferenced checkpoints");

    // The checkpoints have been released, the Destroyer should have no
    // checkpoint memory associated.
    // Note that the EPStats counter has already been updated so it must not
    // change again now.
    EXPECT_EQ(postDetachGlobalMemUsage,
              epstats.getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(cm.getMemUsage(),
              epstats.getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(0, destroyer->getMemoryUsage());
    EXPECT_EQ(0, destroyer->getNumCheckpoints());
}

TEST_P(CheckpointRemoverTest, CheckpointCreationSchedulesDcpStep) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    scheduleCheckpointRemoverTask();

    // We set checkpoint max size = CM upper-mark.
    //
    // That's for simplifying setting up the required scenario.
    // We need to end up in a state where:
    //  1. Some items are queued and fill up the open checkpoint
    //  2. The next item queued would create a new checkpoint and queue there
    //  3. But, step (1) has pushed the CM mem-usage to the upper-mark, so
    //     checkpoint mem recovery triggers
    //  4. Step (3) finds that the open checkpoint is full and creates a new
    //     checkpoint.
    //
    // In this test we need to verify that step (4) notifies the DCP frontend
    // if related cursors have jumped into the new open checkpoint.

    const auto& stats = engine->getEpStats();
    auto& config = engine->getConfiguration();
    config.setCheckpointMaxSize(store->getCMRecoveryUpperMarkBytes());
    ASSERT_EQ(store->getCMRecoveryUpperMarkBytes(),
              engine->getCheckpointConfig().getCheckpointMaxSize());

    auto vb = engine->getVBucket(vbid);
    auto& cm = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    // The test covers both persistent/ephemeral
    ASSERT_EQ(persistent() ? 1 : 0, cm.getNumOfCursors());
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);
    ASSERT_EQ(persistent() ? 2 : 1, cm.getNumOfCursors());
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto dcpCursor = stream->getCursor().lock();
    ASSERT_TRUE(dcpCursor);

    // Store items until checkpoint mem recovery required
    ASSERT_FALSE(store->isCMMemoryReductionRequired());
    // Note: Sizing the value so that surely one single queued items makes the
    // checkpoint logically full. That ensures that we exit the for-loop in a
    // state where the following CheckpointMemRecoveryTask run (see below) will
    // trigger checkpoint creation.
    const std::string value(
            engine->getCheckpointConfig().getCheckpointMaxSize(), 'v');
    auto item = make_item(vbid, makeStoredDocKey("key"), value);
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    ASSERT_TRUE(store->isCMMemoryReductionRequired());
    ASSERT_EQ(1, cm.getNumCheckpoints());
    const auto ckptId = cm.getOpenCheckpointId();
    ASSERT_EQ(ckptId, (*dcpCursor->getCheckpoint())->getId());

    // Move DCP cursor to end of checkpoint and push to stream readyQ
    stream->nextCheckpointItemTask();

    // Move the Producer to settled
    ASSERT_TRUE(producer->getReadyQueue().exists(vbid));
    while (producer->public_getNextItem()) {
    }
    ASSERT_FALSE(producer->getReadyQueue().exists(vbid));

    // Move persistence cursor
    if (persistent()) {
        flushVBucket(vbid);
    }

    // The next queued item would trigger checkpoint creation, but memory
    // recovery runs first and creates the checkpoint
    ASSERT_EQ(0, stats.itemsRemovedFromCheckpoints);
    store->wakeUpCheckpointMemRecoveryTask();
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIO, "CheckpointMemRecoveryTask:0");
    runNextTask(nonIO, "CheckpointMemRecoveryTask:1");
    ASSERT_GT(stats.itemsRemovedFromCheckpoints, 0);

    // New checkpoint created
    EXPECT_EQ(1, cm.getNumCheckpoints());
    EXPECT_EQ(ckptId + 1, cm.getOpenCheckpointId());
    // Cursor has moved into the new checkpoint
    EXPECT_EQ(ckptId + 1, (*dcpCursor->getCheckpoint())->getId());
    EXPECT_EQ(queue_op::empty, (*dcpCursor->getPos())->getOperation());
    EXPECT_EQ(0, dcpCursor->getDistance());

    // Verify that the stream has been notified.
    EXPECT_TRUE(producer->getReadyQueue().exists(vbid));
}

TEST_P(CheckpointRemoverTest, NoCMRecoveryIfPendingDeallocEnough) {
    // We setup a state where checkpoint mem-usage hits the CMQuota upper_mark,
    // so checkpoint mem-releasing is necessary for exiting the TempOOM phase.
    // But, memory usage is split half in CM and half in Destroyer. That means
    // that deallocating checkpoints owned by Destroyers is enough for pushing
    // the overall checkpoint mem-usage below the lower_mark.
    // In that scenario we don't want to perform any emergency mem-recovery in
    // CM.

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Note: We make both tasks available to run in the ExecutorPool, then in
    // the following we can verify what's woken up and what's not.
    scheduleCheckpointRemoverTask();
    scheduleCheckpointDestroyerTasks();

    auto vb = engine->getVBucket(vbid);
    auto& cm = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    ASSERT_EQ(1, cm.getNumCheckpoints());

    // The test covers both persistent/ephemeral
    ASSERT_EQ(persistent() ? 1 : 0, cm.getNumOfCursors());
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);
    ASSERT_EQ(persistent() ? 2 : 1, cm.getNumOfCursors());
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto dcpCursor = stream->getCursor().lock();
    ASSERT_TRUE(dcpCursor);

    // Store items until checkpoint mem recovery required
    ASSERT_FALSE(store->isCMMemoryReductionRequired());
    const auto& ckptConfig = engine->getCheckpointConfig();
    ASSERT_EQ(2, ckptConfig.getMaxCheckpoints());
    const auto valueSize = ckptConfig.getCheckpointMaxSize();
    ASSERT_GT(valueSize, 0);
    const std::string value(valueSize, 'v');
    for (size_t i = 0; !store->isCMMemoryReductionRequired(); ++i) {
        auto item = make_item(
                vbid, makeStoredDocKey("key" + std::to_string(i)), value);
        EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    }
    ASSERT_EQ(2, cm.getNumCheckpoints());

    // Move persistence cursor - In the following we'll play with only the DCP
    // cursor for setting up the required memory state
    if (persistent()) {
        flushVBucket(vbid);
    }

    // DCP cursor hasn't move yet
    ASSERT_LT((*dcpCursor->getCheckpoint())->getId(), cm.getOpenCheckpointId());
    // No checkpoint removed
    ASSERT_EQ(2, cm.getNumCheckpoints());
    const auto& bucket = *engine->getKVBucket();
    ASSERT_EQ(0, bucket.getNumCheckpointsPendingDestruction());

    // Run DCP - cursor processes the full first checkpoint and jumps into the
    // second one
    stream->nextCheckpointItemTask();
    EXPECT_EQ(cm.getOpenCheckpointId(), (*dcpCursor->getCheckpoint())->getId());
    EXPECT_EQ(queue_op::empty, (*dcpCursor->getPos())->getOperation());
    EXPECT_EQ(0, dcpCursor->getDistance());

    // DCP cursor jump has removed the closed/unref checkpoint from CM to
    // Destroyer
    ASSERT_EQ(1, cm.getNumCheckpoints());
    ASSERT_EQ(1, bucket.getNumCheckpointsPendingDestruction());

    // *Test mem-recovery behaviour*
    // CM + Destroyer mem usage has hit the upper_mark, but half of the
    // allocation is in Destroyer. That means that the amount releasable by
    // Destroyer is enough for pushing the overall checkpoint mem-usage down to
    // lower_mark. Thus, we don't need to run any CM mem-recovery.
    const auto& stats = engine->getEpStats();
    ASSERT_GE(stats.getCheckpointManagerEstimatedMemUsage() +
                      bucket.getCheckpointPendingDestructionMemoryUsage(),
              bucket.getCMRecoveryUpperMarkBytes());
    ASSERT_GT(bucket.getCheckpointPendingDestructionMemoryUsage(),
              bucket.getCMRecoveryUpperMarkBytes() -
                      bucket.getCMRecoveryLowerMarkBytes());
    ASSERT_EQ(0, bucket.getRequiredCMMemoryReduction());

    // *Test frontend behaviour*
    // We hit CMQuota and we are waiting for checkpoint deallocation. We have to
    // reject operations.
    auto item = make_item(vbid, makeStoredDocKey("rejected-key"), value);
    EXPECT_EQ(cb::engine_errc::no_memory, store->set(item, cookie));

    // Recover
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    ASSERT_GT(bucket.getCheckpointPendingDestructionMemoryUsage(), 0);
    runNextTask(nonIO, "Destroying closed unreferenced checkpoints");
    ASSERT_EQ(0, bucket.getCheckpointPendingDestructionMemoryUsage());

    // Note: There was only the Destroyer in the queue, no CM recovery task
    // woken up in the previous steps
    try {
        runNextTask(nonIO);
        FAIL() << "Expected nothing ready to run in the NonIO queue";
    } catch (const std::logic_error& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("failed fetchNextTask"));
    }

    // Now we have recovered from TempOOM
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
}

TEST_P(CheckpointRemoverTest, UpdateNumDestroyers) {
    auto& config = engine->getConfiguration();
    const auto initialNumDestroyers = config.getCheckpointDestructionTasks();
    ASSERT_GT(initialNumDestroyers, 0);
    EXPECT_EQ(initialNumDestroyers, store->getNumCheckpointDestroyers());

    const auto newNumDestroyers = initialNumDestroyers + 1;
    config.setCheckpointDestructionTasks(newNumDestroyers);
    EXPECT_EQ(newNumDestroyers, store->getNumCheckpointDestroyers());
}

TEST_P(CheckpointRemoverTest, UpdateNumDestroyers_LowerThanMin) {
    auto& config = engine->getConfiguration();
    try {
        config.setCheckpointDestructionTasks(0);
    } catch (const std::range_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr(
                            "Validation Error, checkpoint_destruction_tasks "
                            "takes values between 1"));
        return;
    }
    FAIL();
}

TEST_P(CheckpointRemoverTest, UpdateNumDestroyers_TaskRunning) {
    auto& config = engine->getConfiguration();
    const auto initialNumDestroyers = config.getCheckpointDestructionTasks();
    ASSERT_GT(initialNumDestroyers, 0);
    EXPECT_EQ(initialNumDestroyers, store->getNumCheckpointDestroyers());

    // No checkpoint to destroy
    auto destroyer = getCkptDestroyerTask(vbid);
    ASSERT_TRUE(destroyer);
    ASSERT_EQ(0, destroyer->getNumCheckpoints());
    ASSERT_EQ(0, destroyer->getMemoryUsage());

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& manager = *vb->checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());

    // Schedule checkpoint destruction
    const auto openId = manager.getOpenCheckpointId();
    manager.createNewCheckpoint();
    if (isPersistent()) {
        std::vector<queued_item> items;
        manager.getNextItemsForPersistence(items);
    }
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(openId + 1, manager.getOpenCheckpointId());
    EXPECT_GT(destroyer->getNumCheckpoints(), 0);
    EXPECT_GT(destroyer->getMemoryUsage(), 0);
    EXPECT_EQ(destroyer->getMemoryUsage(),
              store->getCheckpointPendingDestructionMemoryUsage());

    // Reset the Destroyer task pool while the existing task is running
    destroyer->runHook = [this, &config]() {
        config.setCheckpointDestructionTasks(2);
        EXPECT_EQ(2, store->getNumCheckpointDestroyers());
        EXPECT_EQ(0, store->getCheckpointPendingDestructionMemoryUsage());
    };
    destroyer->run();

    // Verify that the new pool has been created and that the old task doesn't
    // belong to it
    auto* newDestroyer0 = getCkptDestroyerTask(Vbid(0)).get();
    auto* newDestroyer1 = getCkptDestroyerTask(Vbid(1)).get();
    EXPECT_NE(newDestroyer0, newDestroyer1);
    EXPECT_NE(destroyer.get(), newDestroyer0);
    EXPECT_NE(destroyer.get(), newDestroyer1);
}

// Snooze and wakeup again if the lower_mark has not been reached
TEST_P(CheckpointRemoverTest, WakeupAgainIfReductionRequired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& config = engine->getConfiguration();
    config.setMaxSize(1_MiB); // 1MB
    config.setCheckpointMemoryRatio(0); // will always require reduction

    const auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);

    // Reduction required
    ASSERT_GT(store->getRequiredCMMemoryReduction(), 0);

    // Not scheduled to run initially
    EXPECT_EQ(remover->getWaketime(),
              cb::time::steady_clock::time_point::max());
    EXPECT_EQ(remover->getState(), TASK_SNOOZED);

    // Reduction still required after first run
    remover->run();
    ASSERT_GT(store->getRequiredCMMemoryReduction(), 0);

    // Scheduled to run again
    EXPECT_NE(remover->getWaketime(),
              cb::time::steady_clock::time_point::max());
    EXPECT_EQ(remover->getState(), TASK_SNOOZED);

    // Update upper_mark above current usage
    engine->getKVBucket()->setCheckpointMemoryRatio(0.2);

    // No longer require reduction
    remover->run();
    ASSERT_EQ(store->getRequiredCMMemoryReduction(), 0);

    // Not scheduled to run again
    EXPECT_EQ(remover->getWaketime(),
              cb::time::steady_clock::time_point::max());
    EXPECT_EQ(remover->getState(), TASK_SNOOZED);
}

// If the CheckpointMemRecoveryTask is woken up by the checkpoint upper mark
// being exceeded, then it should attempt to free memory to reach the checkpoint
// lower mark
TEST_P(CheckpointRemoverTest, TriggeredByChkUpperMark) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& config = engine->getConfiguration();
    auto& stats = engine->getEpStats();
    auto& bucket = *engine->getKVBucket();

    const size_t bucketQuota = 100_MiB; // 100MB
    config.setMaxSize(bucketQuota);
    // Set checkpoint quota to a lower value to test the upper mark triggering
    config.setCheckpointMemoryRatio(0.2);
    EXPECT_EQ(bucketQuota, stats.getMaxDataSize());

    const auto checkpointMemoryLimit =
            bucketQuota * store->getCheckpointMemoryRatio();
    config.setCheckpointMaxSize(checkpointMemoryLimit);

    // Simulate memory usage above checkpoint upper mark
    size_t numItems = 0;
    do {
        const auto value = std::string(bucketQuota / 20, 'x');
        auto item =
                make_item(vbid,
                          makeStoredDocKey("key_" + std::to_string(++numItems)),
                          value,
                          0 /*exp*/,
                          PROTOCOL_BINARY_RAW_BYTES);
        store->set(item, cookie);
    } while (stats.getCheckpointManagerEstimatedMemUsage() <
             bucket.getCMRecoveryUpperMarkBytes());

    auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);

    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat);
    testGetBytesToFree(bucket, remover, false);
}

// If the CheckpointMemRecoveryTask is woken up by the total memory HWM
// being exceeded, then it should attempt to free memory as much
// memory as possible to reach the total memory LWM
TEST_P(CheckpointRemoverTest, TriggeredByBucketHWM) {
    if (persistent()) {
        // This test is only relevant for Ephemeral buckets
        GTEST_SKIP();
    }

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& config = engine->getConfiguration();
    auto& stats = engine->getEpStats();
    auto& bucket = *engine->getKVBucket();

    const size_t bucketQuota = 100_MiB; // 100MB
    config.setMaxSize(bucketQuota);
    config.setCheckpointMaxSize(bucketQuota);
    // Set high checkpoint quota as we just want to test the HWM triggering
    // and not the checkpoint upper_mark triggering the task.
    config.setCheckpointMemoryRatio(1);
    EXPECT_EQ(bucketQuota, stats.getMaxDataSize());

    // Simulate memory usage above HWM.
    size_t numItems = 0;
    const auto value = std::string(bucketQuota / 20, 'x');
    do {
        auto item =
                make_item(vbid,
                          makeStoredDocKey("key_" + std::to_string(++numItems)),
                          value,
                          0 /*exp*/,
                          PROTOCOL_BINARY_RAW_BYTES);
        store->set(item, cookie);
    } while (stats.getEstimatedTotalMemoryUsed() <
             (stats.mem_high_wat + value.size())); // Ensure above HWM

    flushAndRemoveCheckpoints(vbid);

    auto remover = std::make_shared<CheckpointMemRecoveryTask>(
            *engine, engine->getEpStats(), 0);

    // Only EphemeralMemRecoveryTask can trigger CheckpointMemRecoveryTask
    // when HWM is exceeded currently.
    const bool triggeredByHWM =
            engine->getConfiguration().isEphemeralMemRecoveryEnabled();
    EXPECT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat);
    testGetBytesToFree(bucket, remover, triggeredByHWM);
}

INSTANTIATE_TEST_SUITE_P(
        EphemeralOrPersistent,
        CheckpointRemoverTest,
        // Not necessary to test all the different KVStores, we just want to
        // check Persistent vs Ephemeral memory handling.
        STParameterizedBucketTest::ephAndCouchstoreConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Persistent,
                         CheckpointRemoverEPTest,
                         // Not necessary to test all the different KVStores,
                         // we just want to check Persistent memory handling.
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
