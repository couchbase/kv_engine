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

void CheckpointRemoverTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
}

size_t CheckpointRemoverTest::getMaxCheckpointItems(VBucket& vb) {
    return vb.checkpointManager->getCheckpointConfig().getCheckpointMaxItems();
}

/**
 * Check that the VBucketMap.getVBucketsSortedByChkMgrMem() returns the
 * correct ordering of vBuckets, sorted from largest memory usage to smallest.
 */
TEST_F(CheckpointRemoverEPTest, GetVBucketsSortedByChkMgrMem) {
    for (uint16_t i = 0; i < 3; i++) {
        setVBucketStateAndRunPersistTask(Vbid(i), vbucket_state_active);
        for (uint16_t j = 0; j < i; j++) {
            std::string doc_key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
            store_item(Vbid(i), makeStoredDocKey(doc_key), "value");
        }
    }

    auto map = store->getVBuckets().getVBucketsSortedByChkMgrMem();

    // The map should be 3 elements long, since we created 3 vBuckets
    ASSERT_EQ(3, map.size());

    for (size_t i = 1; i < map.size(); i++) {
        auto this_vbucket = map[i];
        auto prev_vbucket = map[i - 1];
        // This vBucket should have a greater memory usage than the one previous
        // to it in the map
        ASSERT_GE(this_vbucket.second, prev_vbucket.second);
    }
}

#ifndef WIN32
/**
 * Check that the CheckpointManager memory usage calculation is correct and
 * accurate based on the size of the checkpoints in it.
 *
 * Note: On WIN32 the memory usage base point seems to difficult to predict as
 * it varies slightly even on apparently unrelated changes in the Checkpoint
 * area. Covering other platforms is enough for ensuring that we are tracking
 * mem-usage correctly.
 */
TEST_F(CheckpointRemoverEPTest, CheckpointManagerMemoryUsage) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    // We should have one checkpoint which is for the state change
    ASSERT_EQ(1, checkpointManager->getNumCheckpoints());

    // The queue (toWrite) is implemented as std:list, therefore
    // when we add an item it results in the creation of 3 pointers -
    // forward ptr, backward ptr and ptr to object.
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);

    // Allocator used for tracking memory used by the CheckpointQueue
    checkpoint_index::allocator_type memoryTrackingAllocator;

    // Allocator used for tracking the memory usage of the keys in the
    // checkpoint indexes.
    checkpoint_index::key_type::allocator_type keyIndexKeyTrackingAllocator;

    // Emulate the Checkpoint preparedKeyIndex and committedKeyIndex so we can
    // determine the number of bytes that should be allocated during its use.
    checkpoint_index committedKeyIndex(memoryTrackingAllocator);
    checkpoint_index preparedKeyIndex(memoryTrackingAllocator);
    const auto iterator =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    *checkpointManager)
                    .begin();
    IndexEntry entry{iterator};

    // Check that the expected memory usage of the checkpoints is correct
    size_t expected_size = 0;
    for (auto& checkpoint :
         CheckpointManagerTestIntrospector::public_getCheckpointList(
                 *checkpointManager)) {
        // Add the overhead of the Checkpoint object
        expected_size += sizeof(Checkpoint);

        for (auto& itr : *checkpoint) {
            // Add the size of the item
            expected_size += itr->size();
            // Add the size of adding to the queue
            expected_size += perElementOverhead;
        }
    }

    ASSERT_EQ(expected_size, checkpointManager->getMemoryUsage());

    // Check that the new checkpoint memory usage is equal to the previous
    // amount plus the addition of the new item.
    Item item = store_item(vbid, makeStoredDocKey("key0"), "value");
    size_t new_expected_size = expected_size;
    // Add the size of the item
    new_expected_size += item.size();
    // Add the size of adding to the queue
    new_expected_size += perElementOverhead;
    // Add to the keyIndex
    committedKeyIndex.emplace(
            CheckpointIndexKeyType(item.getKey(), keyIndexKeyTrackingAllocator),
            entry);

    // As the metaKeyIndex, preparedKeyIndex and committedKeyIndex all share
    // the same allocator, retrieving the bytes allocated for the keyIndex,
    // will also include the bytes allocated for the other indexes.
    const size_t keyIndexSize =
            committedKeyIndex.get_allocator().getBytesAllocated() +
            keyIndexKeyTrackingAllocator.getBytesAllocated();
    ASSERT_EQ(new_expected_size + keyIndexSize,
              checkpointManager->getMemoryUsage());
}
#endif

/**
 * Test CheckpointManager correctly returns which cursors we are eligible to
 * drop. We should not be allowed to drop any cursors in a checkpoint when the
 * persistence cursor is present.
 */
TEST_F(CheckpointRemoverEPTest, CursorsEligibleToDrop) {
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

    // Insert items to the vBucket so we create a new checkpoint by going over
    // the max items limit by 10
    for (size_t i = 0; i < getMaxCheckpointItems(*vb) + 10; i++) {
        std::string doc_key = "key_" + std::to_string(i);
        store_item(vbid, makeStoredDocKey(doc_key), "value");
    }

    // We should now have 2 checkpoints for this vBucket
    ASSERT_EQ(2, checkpointManager->getNumCheckpoints());

    // Run the persistence task for this vBucket, this should advance the
    // persistence cursor out of the first checkpoint
    flush_vbucket_to_disk(vbid, getMaxCheckpointItems(*vb) + 10);

    // We should now be eligible to drop the user created DCP stream from the
    // checkpoint
    cursors = checkpointManager->getListOfCursorsToDrop();
    ASSERT_EQ(1, cursors.size());
    ActiveStream& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
    ASSERT_EQ(activeStream.getCursor().lock(), cursors[0].lock());
}

/**
 * Check that the memory of unreferenced checkpoints after we drop all cursors
 * in a checkpoint is equal to the size of the items that were contained within
 * it.
 */
TEST_F(CheckpointRemoverEPTest, CursorDropMemoryFreed) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    // We should have one checkpoint which is for the state change
    ASSERT_EQ(1, checkpointManager->getNumCheckpoints());
    // We should only have one cursor, which is for persistence
    ASSERT_EQ(1, checkpointManager->getNumOfCursors());

    auto initialSize = checkpointManager->getMemoryUsage();

    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);

    createDcpStream(*producer);

    // The queue (toWrite) is implemented as std:list, therefore
    // when we add an item it results in the creation of 3 pointers -
    // forward ptr, backward ptr and ptr to object.
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);

    // Allocator used for tracking memory used by the CheckpointQueue
    checkpoint_index::allocator_type memoryTrackingAllocator;

    // Allocator used for tracking the memory usage of the keys in the
    // checkpoint indexes.
    checkpoint_index::key_type::allocator_type keyIndexKeyTrackingAllocator;

    // Emulate the Checkpoint keyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    checkpoint_index keyIndex(memoryTrackingAllocator);
    // Grab the initial size of the keyIndex because on Windows an empty
    // std::unordered_map allocated 200 bytes.
    const auto initialKeyIndexSize =
            keyIndex.get_allocator().getBytesAllocated();
    const auto iterator =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    *checkpointManager)
                    .begin();
    IndexEntry entry{iterator};

    auto expectedFreedMemoryFromItems = initialSize;
    for (size_t i = 0; i < getMaxCheckpointItems(*vb); i++) {
        std::string doc_key = "key_" + std::to_string(i);
        Item item = store_item(vbid, makeStoredDocKey(doc_key), "value");
        expectedFreedMemoryFromItems += item.size();
        // Add the size of adding to the queue
        expectedFreedMemoryFromItems += perElementOverhead;
        // Add to the emulated keyIndex
        keyIndex.emplace(CheckpointIndexKeyType(item.getKey(),
                                                keyIndexKeyTrackingAllocator),
                         entry);
    }

    ASSERT_EQ(1, checkpointManager->getNumCheckpoints());
    ASSERT_EQ(getMaxCheckpointItems(*vb) + 2, checkpointManager->getNumItems());
    ASSERT_NE(0, expectedFreedMemoryFromItems);

    // Insert a new item, this will create a new checkpoint
    store_item(vbid, makeStoredDocKey("Banana"), "value");
    ASSERT_EQ(2, checkpointManager->getNumCheckpoints());

    // Run the persistence task for this vBucket, this should advance the
    // persistence cursor out of the first checkpoint
    flush_vbucket_to_disk(vbid, getMaxCheckpointItems(*vb) + 1);

    auto cursors = checkpointManager->getListOfCursorsToDrop();
    ASSERT_EQ(1, cursors.size());
    ActiveStream& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
    ASSERT_EQ(activeStream.getCursor().lock(), cursors[0].lock());

    // Needed to calculate the size of a checkpoint_end queued_item
    StoredDocKey key("checkpoint_end", CollectionID::System);
    queued_item chkptEnd(new Item(key, vbid, queue_op::checkpoint_end, 0, 0));

    // Add the size of the checkpoint end
    expectedFreedMemoryFromItems += chkptEnd->size();
    // Add the size of adding to the queue
    expectedFreedMemoryFromItems += perElementOverhead;

    const auto keyIndexSize = keyIndex.get_allocator().getBytesAllocated();
    expectedFreedMemoryFromItems +=
            (keyIndexSize - initialKeyIndexSize +
             keyIndexKeyTrackingAllocator.getBytesAllocated());

    // Manually handle the slow stream, this is the same logic as the checkpoint
    // remover task uses, just without the overhead of setting up the task
    auto memoryOverhead = checkpointManager->getMemoryOverhead();
    if (engine->getDcpConnMap().handleSlowStream(vbid,
                                                 cursors[0].lock().get())) {
        ASSERT_EQ(expectedFreedMemoryFromItems,
                  checkpointManager->getMemoryUsageOfUnrefCheckpoints());
        // Check that the memory of unreferenced checkpoints is greater than or
        // equal to the pre-cursor-dropped memory overhead.
        //
        // This is the least amount of memory we expect to be able to free,
        // as it is all internal and independent from the HashTable.
        ASSERT_GE(checkpointManager->getMemoryUsageOfUnrefCheckpoints(),
                  memoryOverhead);
    } else {
        ASSERT_FALSE(producer->isCursorDroppingEnabled());
    }

    // There should only be the one checkpoint cursor now for persistence
    ASSERT_EQ(1, checkpointManager->getNumOfCursors());
}

// Test that we correctly determine whether to trigger memory recovery.
TEST_F(CheckpointRemoverEPTest, MemoryRecoveryTrigger) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    const auto& task = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    const size_t bucketQuota = 1024 * 1024 * 100;
    auto& config = engine->getConfiguration();
    config.setMaxSize(bucketQuota);
    auto& stats = engine->getEpStats();
    EXPECT_EQ(bucketQuota, stats.getMaxDataSize());

    // No item stored, no memory condition that triggers mem-recovery
    const auto checkpointMemoryLimit =
            bucketQuota * store->getCheckpointMemoryRatio();
    EXPECT_LT(stats.getEstimatedCheckpointMemUsage(), checkpointMemoryLimit);
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);
    bool hasTriggered{false};
    size_t amountOfMemoryToClear{0};
    std::tie(hasTriggered, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_FALSE(hasTriggered);
    EXPECT_EQ(0, amountOfMemoryToClear);

    // Now store some items so that the mem-usage in checkpoint crosses the
    // checkpoint_upper_mark (default to 50% of the bucket quota as of writing),
    // but the overall mem-usage in the system doesn't hit the LWM.
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
    } while (stats.getEstimatedCheckpointMemUsage() < checkpointMemoryLimit);
    auto vb = store->getVBucket(vbid);
    EXPECT_GT(vb->getNumItems(), 0);
    EXPECT_EQ(numItems, vb->getNumItems());
    EXPECT_GT(stats.getEstimatedCheckpointMemUsage(), checkpointMemoryLimit);
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);

    // Checkpoint mem-recovery must trigger (regardless of any LWM)
    std::tie(hasTriggered, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_TRUE(hasTriggered);
    EXPECT_GT(amountOfMemoryToClear, 0);
}

void CheckpointRemoverEPTest::testExpellingOccursBeforeCursorDropping(
        bool moveCursor) {
    // 1) Get enough checkpoint metadata to trigger expel
    // 2) doesn't hit maxDataSize first
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();
    const auto& task = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    auto vb = store->getVBuckets().getBucket(vbid);
    auto* manager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    CheckpointConfig c(*engine.get());
    manager->resetConfig(c);

    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);

    ActiveStream& activeStream =
            reinterpret_cast<ActiveStream&>(*producer->findStream(vbid));
    auto cursor = activeStream.getCursor().lock();

    config.setChkExpelEnabled(true);
    config.setMaxSize(1024 * 1024 * 100);
    const auto chkptMemRecoveryLimit =
            config.getMaxSize() * store->getCheckpointMemoryRatio() *
            store->getCheckpointMemoryRecoveryUpperMark();
    auto& stats = engine->getEpStats();
    stats.mem_low_wat.store(1);

    int ii = 0;
    const auto value = std::string(1024 * 1024, 'x');
    while (stats.getEstimatedCheckpointMemUsage() < chkptMemRecoveryLimit) {
        std::string doc_key = "key_" + std::to_string(ii);
        store_item(vbid, makeStoredDocKey(doc_key), value);
        ++ii;
    }
    flush_vbucket_to_disk(vbid, ii);

    if (moveCursor) {
        std::vector<queued_item> items;
        manager->getNextItemsForCursor(cursor.get(), items);
    }

    bool shouldReduceMemory{false};
    size_t amountOfMemoryToClear{0};
    std::tie(shouldReduceMemory, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_TRUE(shouldReduceMemory);

    manager->removeClosedUnrefCheckpoints(*vb);

    task->run();
    manager->removeClosedUnrefCheckpoints(*vb);

    std::tie(shouldReduceMemory, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_FALSE(shouldReduceMemory);
}

// Test that we correctly apply expelling before cursor dropping.
TEST_F(CheckpointRemoverEPTest, expelButNoCursorDrop) {
    testExpellingOccursBeforeCursorDropping(true);
    EXPECT_NE(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(0, engine->getEpStats().cursorsDropped);
}

// Test that we correctly trigger cursor dropping when have checkpoint
// with cursor at the start and so cannot use expelling.
TEST_F(CheckpointRemoverEPTest, notExpelButCursorDrop) {
    engine->getConfiguration().setChkMaxItems(10);
    engine->getConfiguration().setMaxCheckpoints(20);
    testExpellingOccursBeforeCursorDropping(false);
    EXPECT_EQ(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(1, engine->getEpStats().cursorsDropped);
}

// Test written for MB-36366. With the fix removed this test failed because
// post expel, we continued onto cursor dropping.
// MB-36447 - unreliable test, disabling for now
TEST_F(CheckpointRemoverEPTest, DISABLED_noCursorDropWhenTargetMet) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();
    const auto& task = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    auto vb = store->getVBuckets().getBucket(vbid);
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    CheckpointConfig c(*engine.get());
    checkpointManager->resetConfig(c);

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
    auto regRes = cm->registerCursorBySeqno("SomeName", startBySeqno);
    EXPECT_EQ(expectBackfill, regRes.tryBackfill);
    auto cursor = regRes.cursor.lock();

    std::vector<queued_item> items;
    cm->getNextItemsForCursor(cursor.get(), items);

    cm->removeCursor(cursor.get());

    return items;
}

TEST_F(CheckpointRemoverEPTest, expelsOnlyIfOldestCheckpointIsReferenced) {
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

    cm->forceNewCheckpoint();

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

    auto regRes = cm->registerCursorBySeqno("Cursor3", 0);
    auto cursor = regRes.cursor.lock();

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

    // Can now expel the 2 items in (ckpt_start, cursor)
    auto result = cm->expelUnreferencedCheckpointItems();

    EXPECT_EQ(2, result.expelCount);

    /* items in first checkpoint
     *
     *   dummy
     *   chk start
     *   key_1
     *   key_2
     */

    afterCount = getItemsWithCursor("Cursor4", 0, true).size();

    EXPECT_EQ(beforeCount - 2, afterCount);
}

TEST_F(CheckpointRemoverEPTest, earliestCheckpointSelectedCorrectly) {
    // MB-35812 - Confirm that checkpoint expelling correctly selects the
    // earliest cursor, and that the cursor is in the oldest reffed checkpoint.

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    {
        // clear out the first checkpoint containing a set vbstate
        cm->forceNewCheckpoint();
        flush_vbucket_to_disk(vbid, 0);
        cm->removeClosedUnrefCheckpoints(*vb);
    }

    // queue a single item into checkpoint
    store_item(vbid, makeStoredDocKey("key_1"), "value");
    // queue a set vbstate meta item into checkpoint
    cm->queueSetVBState(*vb);

    cm->forceNewCheckpoint();

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
    auto regResA = cm->registerCursorBySeqno("CursorA", 0);
    auto cursorA = regResA.cursor.lock();
    for (int i = 0; i < 5; i++) {
        cm->incrCursor(*cursorA);
    }

    // Put a cursor on the *last* item of the first checkpoint
    auto regResB = cm->registerCursorBySeqno("CursorB", 0);
    auto cursorB = regResB.cursor.lock();
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
TEST_F(CheckpointRemoverEPTest, NewSyncWriteCreatesNewCheckpointIfCantDedupe) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    {
        // clear out the first checkpoint containing a set vbstate
        cm->forceNewCheckpoint();
        flush_vbucket_to_disk(vbid, 0);
        cm->removeClosedUnrefCheckpoints(*vb);
    }

    store_item(vbid, makeStoredDocKey("key_1"), "value");
    store_item(vbid, makeStoredDocKey("key_2"), "value");

    // Queue a prepare
    auto prepareKey = makeStoredDocKey("key_1");
    auto prepare = makePendingItem(prepareKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie, nullptr /*StoreIfPredicate*/));

    // Persist to move our cursor so that we can expel the prepare
    flushVBucketToDiskIfPersistent(vbid, 3);

    auto result = cm->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, result.expelCount);

    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(prepareKey,
                         3,
                         {},
                         vb->lockCollections(prepareKey),
                         cookie));

    // We should have opened a new checkpoint
    EXPECT_EQ(2, cm->getNumCheckpoints());
}

TEST_F(CheckpointRemoverEPTest, UseOpenCheckpointIfCanDedupeAfterExpel) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    {
        // clear out the first checkpoint containing a set vbstate
        cm->forceNewCheckpoint();
        flush_vbucket_to_disk(vbid, 0);
        cm->removeClosedUnrefCheckpoints(*vb);
    }

    store_item(vbid, makeStoredDocKey("key_1"), "value");
    store_item(vbid, makeStoredDocKey("key_2"), "value");

    // Queue a prepare
    auto prepareKey = makeStoredDocKey("key_1");
    auto prepare = makePendingItem(prepareKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie, nullptr /*StoreIfPredicate*/));

    // Persist to move our cursor so that we can expel the prepare
    flushVBucketToDiskIfPersistent(vbid, 3);

    auto result = cm->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, result.expelCount);

    store_item(vbid, makeStoredDocKey("key_2"), "value");

    EXPECT_EQ(1, cm->getNumCheckpoints());

    // We should have decremented numItems when we added again
    EXPECT_EQ(3, cm->getNumOpenChkItems());
}

TEST_F(CheckpointRemoverEPTest,
       CheckpointExpellingInvalidatesKeyIndexCorrectly) {
    /*
     * Ensure that expelling correctly marks invalidated keyIndex entries as
     * SyncWrite/non SyncWrite.
     * MB-36338: expelling would incorrectly mark a keyIndex entry for a sync
     * write as non-sync write if it was the last item to be expelled, as the
     * value it checked was that of the dummy item rather than of the real item.
     */
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto vb = engine->getVBucket(vbid);
    auto* cm = static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    {
        // clear out the first checkpoint containing a set vbstate
        cm->forceNewCheckpoint();
        flush_vbucket_to_disk(vbid, 0);
        cm->removeClosedUnrefCheckpoints(*vb);
    }

    // expelling will not remove items preceded by an item with the same seqno
    // (in the case, the checkpoint start meta item)
    // pad to allow the following prepare to be expelled
    store_item(vbid, makeStoredDocKey("padding1"), "value");

    auto prepareKey = makeStoredDocKey("key_1");

    // Queue a prepare
    auto prepare = makePendingItem(prepareKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie, nullptr /*StoreIfPredicate*/));

    // expelling will not remove items with seqno equal to the ckpt highSeqno
    // but the commit serves as padding, allowing the preceding prepare to be
    // expelled
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(prepareKey,
                         2,
                         {},
                         vb->lockCollections(prepareKey),
                         cookie));

    // Persist to move our cursor so that we can expel the prepare
    flushVBucketToDiskIfPersistent(vbid, 3);

    // expel from the checkpoint. This will invalidate keyIndex entries
    // for all expelled items.
    auto result = cm->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, result.expelCount);

    EXPECT_EQ(1, cm->getNumCheckpoints());

    auto prepare2 = makePendingItem(prepareKey, "value");

    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare2, cookie, nullptr /*StoreIfPredicate*/));

    // queueing second prepare should fail as it would dedupe the existing
    // prepare (even though it has been expelled) leading to a new
    // checkpoint being opened.
    EXPECT_EQ(2, cm->getNumCheckpoints());
}
