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

#include "checkpoint_remover_test.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_utils.h"
#include "dcp/response.h"
#include "test_helpers.h"

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

/**
 * Check that the VBucketMap.getVBucketsTotalCheckpointMemoryUsage() returns the
 * total memory usage of the checkpoints of all vbuckets.
 */
TEST_F(CheckpointRemoverEPTest, GetVBucketsTotalCheckpointMemoryUsage) {
    const auto numOfCheckpoints{3};
    for (uint16_t i = 0; i < numOfCheckpoints; i++) {
        setVBucketStateAndRunPersistTask(Vbid(i), vbucket_state_active);
        for (uint16_t j = 0; j < i; j++) {
            std::string doc_key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
            store_item(Vbid(i), makeStoredDocKey(doc_key), "value");
        }
        // Set to different vbucket states (active=1, replica=2 and pending=3).
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(Vbid(i), vbucket_state_t(i + 1), {}));
    }

    size_t totalCheckpointMemoryUsage{0};
    for (size_t i = 0; i < numOfCheckpoints; i++) {
        VBucketPtr b = store->getVBuckets().getBucket(Vbid(i));
        if (b) {
            totalCheckpointMemoryUsage += b->getChkMgrMemUsage();
        }
    }
    EXPECT_EQ(totalCheckpointMemoryUsage,
              store->getVBuckets().getVBucketsTotalCheckpointMemoryUsage());
}

/**
 * Check that the CheckpointManager memory usage calculation is correct and
 * accurate based on the size of the checkpoints in it.
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
    // Emulate the Checkpoint metaKeyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    meta_checkpoint_index metaKeyIndex(memoryTrackingAllocator);
    // Emulate the Checkpoint keyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    checkpoint_index keyIndex(memoryTrackingAllocator);
    ChkptQueueIterator iterator =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *checkpointManager)
                    .front()
                    ->begin();
    index_entry entry{iterator, 0};

    // Check that the expected memory usage of the checkpoints is correct
    size_t expected_size = 0;
    for (auto& checkpoint :
         CheckpointManagerTestIntrospector::public_getCheckpointList(
                 *checkpointManager)) {
        // Add the overhead of the Checkpoint object
        expected_size += sizeof(Checkpoint);
#ifdef WIN32
        // On windows for an empty list we still allocate space for
        // containing one element.
        expected_size += perElementOverhead;
#endif

        for (auto itr = checkpoint->begin(); itr != checkpoint->end(); ++itr) {
            // Add the size of the item
            expected_size += (*itr)->size();
            // Add the size of adding to the queue
            expected_size += perElementOverhead;
            // Add to the emulated metaKeyIndex
            metaKeyIndex.emplace((*itr)->getKey(), entry);
        }
    }

    const auto metaKeyIndexSize =
            *(metaKeyIndex.get_allocator().getBytesAllocated());
    ASSERT_EQ(expected_size + metaKeyIndexSize,
              checkpointManager->getMemoryUsage());

    // Check that the new checkpoint memory usage is equal to the previous
    // amount plus the addition of the new item.
    Item item = store_item(vbid, makeStoredDocKey("key0"), "value");
    size_t new_expected_size = expected_size;
    // Add the size of the item
    new_expected_size += item.size();
    // Add the size of adding to the queue
    new_expected_size += perElementOverhead;
    // Add to the keyIndex
    keyIndex.emplace(
            CheckpointIndexKey(item.getKey(),
                               item.isCommitted()
                                       ? CheckpointIndexKeyNamespace::Committed
                                       : CheckpointIndexKeyNamespace::Prepared),
            entry);

    // As the metaKeyIndex and keyIndex share the same allocator, retrieving
    // the bytes allocated for the keyIndex, will also include the bytes
    // allocated for the metaKeyIndex.
    size_t keyIndexSize = *(keyIndex.get_allocator().getBytesAllocated());
    ASSERT_EQ(new_expected_size + keyIndexSize,
              checkpointManager->getMemoryUsage());
}

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
    // Emulate the Checkpoint keyIndex so we can determine the number
    // of bytes that should be allocated during its use.
    checkpoint_index keyIndex(memoryTrackingAllocator);
    // Grab the initial size of the keyIndex because on Windows an empty
    // std::unordered_map allocated 200 bytes.
    const auto initialKeyIndexSize =
            *(keyIndex.get_allocator().getBytesAllocated());
    ChkptQueueIterator iterator =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *checkpointManager)
                    .front()
                    ->begin();
    index_entry entry{iterator, 0};

    auto expectedFreedMemoryFromItems = initialSize;
    // The initial setVBucketState is a meta item and will be dropped when we
    // drop the first checkpoint. It is slightly smaller than an item enqueued
    // in the keyIndex as it does not include the CheckpointIndexKeyNamespace
    expectedFreedMemoryFromItems -= sizeof(CheckpointIndexKeyNamespace);
    for (size_t i = 0; i < getMaxCheckpointItems(*vb); i++) {
        std::string doc_key = "key_" + std::to_string(i);
        Item item = store_item(vbid, makeStoredDocKey(doc_key), "value");
        expectedFreedMemoryFromItems += item.size();
        // Add the size of adding to the queue
        expectedFreedMemoryFromItems += perElementOverhead;
        // Add to the emulated keyIndex
        keyIndex.emplace(
                CheckpointIndexKey(
                        item.getKey(),
                        item.isCommitted()
                                ? CheckpointIndexKeyNamespace::Committed
                                : CheckpointIndexKeyNamespace::Prepared),
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
    // checkpoint_end is a meta item and has slightly smaller size than an item
    // enqueued in the keyIndex as it does not include the
    // CheckpointIndexKeyNamespace
    expectedFreedMemoryFromItems -= sizeof(CheckpointIndexKeyNamespace);

    // Add the size of the checkpoint end
    expectedFreedMemoryFromItems += chkptEnd->size();
    // Add the size of adding to the queue
    expectedFreedMemoryFromItems += perElementOverhead;
    // Add to the emulated keyIndex
    keyIndex.emplace(
            CheckpointIndexKey(chkptEnd->getKey(),
                               chkptEnd->isCommitted()
                                       ? CheckpointIndexKeyNamespace::Committed
                                       : CheckpointIndexKeyNamespace::Prepared),
            entry);

    const auto keyIndexSize = *(keyIndex.get_allocator().getBytesAllocated());
    expectedFreedMemoryFromItems += (keyIndexSize - initialKeyIndexSize);

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
TEST_F(CheckpointRemoverEPTest, memoryRecoverTriggerTest) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto& config = engine->getConfiguration();
    const auto& task = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            engine.get(),
            engine->getEpStats(),
            engine->getConfiguration().getChkRemoverStime());

    bool shouldTriggerMemoryRecovery{false};
    size_t amountOfMemoryToClear{0};

    /*
     * With a large max size (with no other changes) we should
     * conclude the cursor dropping is not required.
     */
    config.setMaxSize(10240);

    std::tie(shouldTriggerMemoryRecovery, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_FALSE(shouldTriggerMemoryRecovery);
    EXPECT_EQ(0, amountOfMemoryToClear);

    /*
     * Trigger condition for memory recovery:
     * the total memory used is greater than the upper threshold which is
     * a percentage of the quota, specified by cursor_dropping_upper_mark
     */
    config.setMaxSize(1024);

    std::tie(shouldTriggerMemoryRecovery, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_TRUE(shouldTriggerMemoryRecovery);
    EXPECT_LT(0, amountOfMemoryToClear);

    /*
     * Trigger condition for memory recovery:
     * the overall checkpoint memory usage goes above a certain % of the
     * bucket quota, specified by cursor_dropping_checkpoint_mem_upper_mark
     * and the checkpoint memory usage is above the memory low watermark.
     */
    engine->getEpStats().mem_low_wat.store(1);
    config.setCursorDroppingCheckpointMemUpperMark(1);

    std::tie(shouldTriggerMemoryRecovery, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_TRUE(shouldTriggerMemoryRecovery);
    EXPECT_LT(0, amountOfMemoryToClear);
}

void CheckpointRemoverEPTest::testExpelingOccursBeforeCursorDropping(
        bool moveCursor) {
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
    auto cursor = activeStream.getCursor().lock();

    config.setChkExpelEnabled(true);
    config.setMaxSize(200000);
    const auto chkptMemLimit =
            (config.getMaxSize() *
             config.getCursorDroppingCheckpointMemUpperMark()) /
            100;
    engine->getEpStats().mem_low_wat.store(1);

    int ii = 0;
    while (engine->getKVBucket()
                   ->getVBuckets()
                   .getVBucketsTotalCheckpointMemoryUsage() < chkptMemLimit) {
        std::string doc_key = "key_" + std::to_string(ii);
        store_item(vbid, makeStoredDocKey(doc_key), "value");
        ++ii;
    }
    flush_vbucket_to_disk(vbid, ii);

    if (moveCursor) {
        // Move the cursor past 90% of the items added.
        for (int jj = 0; jj < ii * 0.9; ++jj) {
            checkpointManager->nextItem(cursor.get(), isLastMutation);
        }
    }

    bool shouldReduceMemory{false};
    size_t amountOfMemoryToClear{0};
    std::tie(shouldReduceMemory, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_TRUE(shouldReduceMemory);

    bool newOpenCheckpointCreated;
    checkpointManager->removeClosedUnrefCheckpoints(*vb,
                                                    newOpenCheckpointCreated);

    task->run();
    checkpointManager->removeClosedUnrefCheckpoints(*vb,
                                                    newOpenCheckpointCreated);

    std::tie(shouldReduceMemory, amountOfMemoryToClear) =
            task->isReductionInCheckpointMemoryNeeded();
    EXPECT_FALSE(shouldReduceMemory);
}

// Test that we correctly apply expelling before cursor dropping.
TEST_F(CheckpointRemoverEPTest, expelButNoCursorDrop) {
    testExpelingOccursBeforeCursorDropping(true);
    EXPECT_NE(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(0, engine->getEpStats().cursorsDropped);
}

// Test that we correctly trigger cursor dropping when have checkpoint
// with cursor at the start and so cannot use expelling.
TEST_F(CheckpointRemoverEPTest, notExpelButCursorDrop) {
    engine->getConfiguration().setChkMaxItems(10);
    engine->getConfiguration().setMaxCheckpoints(20);
    testExpelingOccursBeforeCursorDropping(false);
    EXPECT_EQ(0, engine->getEpStats().itemsExpelledFromCheckpoints);
    EXPECT_EQ(1, engine->getEpStats().cursorsDropped);
}
