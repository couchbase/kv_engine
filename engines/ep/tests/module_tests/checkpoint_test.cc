/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_test.h"
#include "checkpoint_test_impl.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_utils.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "ep_types.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <utilities/test_manifest.h>
#include <thread>

#define DCP_CURSOR_PREFIX "dcp-client-"

void CheckpointTest::SetUp() {
    config.setCheckpointMaxSize(std::numeric_limits<size_t>::max());
    checkpoint_config = std::make_unique<CheckpointConfig>(config);
    VBucketTest::SetUp();
    createManager();
}

void CheckpointTest::TearDown() {
    VBucketTest::TearDown();
}

void CheckpointTest::createManager(int64_t lastSeqno, bool addCursor) {
    ASSERT_TRUE(vbucket);
    ASSERT_TRUE(checkpoint_config);
    range = {static_cast<uint64_t>(lastSeqno),
             static_cast<uint64_t>(lastSeqno)};
    vbucket->checkpointManager = std::make_unique<MockCheckpointManager>(
            global_stats,
            *vbucket,
            *checkpoint_config,
            lastSeqno,
            range.getStart(),
            range.getEnd(),
            lastSeqno, // setting maxVisibleSeqno to equal lastSeqno
            0, // lastPrepareSeqno
            /*flusher callback*/ nullptr);

    ASSERT_TRUE(vbucket);
    manager = static_cast<MockCheckpointManager*>(
            vbucket->checkpointManager.get());
    ASSERT_TRUE(manager);

    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_EQ(1, manager->getNumCheckpoints());

    if (!addCursor) {
        return;
    }

    // Set the proper test cursor
    if (persistent()) {
        cursor = manager->getPersistenceCursor();
    } else {
        cursor =
                manager->registerCursorBySeqno("test_cursor",
                                               0,
                                               CheckpointCursor::Droppable::Yes)
                        .takeCursor()
                        .lock()
                        .get();
    }
    ASSERT_TRUE(cursor);

    ASSERT_EQ(1, manager->getNumOfCursors());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumItemsForCursor(*cursor));
}

void CheckpointTest::resetManager() {
    manager = nullptr;
    vbucket->checkpointManager.reset();
}

bool CheckpointTest::queueNewItem(const std::string& key) {
    queued_item qi{new Item(makeStoredDocKey(key),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0)};
    qi->setQueuedTime(std::chrono::steady_clock::now());
    return manager->queueDirty(qi,
                               GenerateBySeqno::Yes,
                               GenerateCas::Yes,
                               /*preLinkDocCtx*/ nullptr);
}

bool CheckpointTest::queueNewCDCItem(const std::string& key) {
    queued_item qi{new Item(makeStoredDocKey(key),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0)};
    qi->setQueuedTime(std::chrono::steady_clock::now());
    qi->setCanDeduplicate(CanDeduplicate::No);
    return manager->queueDirty(qi,
                               GenerateBySeqno::Yes,
                               GenerateCas::Yes,
                               /*preLinkDocCtx*/ nullptr);
}

bool CheckpointTest::queueReplicatedItem(const std::string& key,
                                         int64_t seqno) {
    queued_item qi{new Item(makeStoredDocKey(key),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            seqno)};
    qi->setCas(1);
    return manager->queueDirty(qi,
                               GenerateBySeqno::No,
                               GenerateCas::No,
                               /*preLinkDocCtx*/ nullptr);
}

void CheckpointTest::advanceCursorToEndOfCheckpoints() {
    // Move the cursor past the empty checkpoint
    std::vector<queued_item> items;
    Expects(cursor);
    manager->getItemsForCursor(*cursor,
                               items,
                               std::numeric_limits<size_t>::max(),
                               std::numeric_limits<size_t>::max());
}

void ReplicaCheckpointTest::SetUp() {
    CheckpointTest::SetUp();
    // Move the cursor past the empty checkpoint
    advanceCursorToEndOfCheckpoints();
}

// Sanity check test fixture
TEST_P(CheckpointTest, CheckFixture) {
    // Initially have a single cursor (persistence).
    EXPECT_EQ(1, this->manager->getNumOfCursors());
    // cs item
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    // empty, cs,
    // ^
    EXPECT_EQ(1, manager->getNumItemsForCursor(*cursor));
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    ASSERT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1000, result.ranges.front().getEnd());
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
}

// Basic test of a single, open checkpoint.
TEST_P(CheckpointTest, OneOpenCkpt) {
    // Queue a set operation.
    queued_item qi(new Item(makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 20,
                            /*bySeq*/ 0));

    // No set_ops in queue, expect queueDirty to return true (increase
    // persistence queue size).
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    // cs + 1x op_set
    EXPECT_EQ(2, manager->getNumOpenChkItems());
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(20, qi->getRevSeqno());
    EXPECT_EQ(2, this->manager->getNumItemsForCursor(*cursor));
    EXPECT_EQ(1001, this->manager->getHighSeqno());
    EXPECT_EQ(1001, this->manager->getMaxVisibleSeqno());

    // Adding the same key again shouldn't increase the size.
    queued_item qi2(new Item(makeStoredDocKey("key1"),
                             this->vbucket->getId(),
                             queue_op::mutation,
                             /*revSeq*/ 21,
                             /*bySeq*/ 0));
    EXPECT_FALSE(manager->queueDirty(qi2,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::Yes,
                                     /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems());
    EXPECT_EQ(1002, qi2->getBySeqno());
    EXPECT_EQ(21, qi2->getRevSeqno());
    EXPECT_EQ(2, this->manager->getNumItemsForCursor(*cursor));
    EXPECT_EQ(1002, this->manager->getHighSeqno());
    EXPECT_EQ(1002, this->manager->getMaxVisibleSeqno());

    // Adding a different key should increase size.
    queued_item qi3(new Item(makeStoredDocKey("key2"),
                             this->vbucket->getId(),
                             queue_op::mutation,
                             /*revSeq*/ 0,
                             /*bySeq*/ 0));
    EXPECT_TRUE(manager->queueDirty(qi3,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(1003, qi3->getBySeqno());
    EXPECT_EQ(0, qi3->getRevSeqno());
    EXPECT_EQ(3, this->manager->getNumItemsForCursor(*cursor));
    EXPECT_EQ(1003, this->manager->getHighSeqno());
    EXPECT_EQ(1003, this->manager->getMaxVisibleSeqno());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1003, result.ranges.front().getEnd());
    EXPECT_EQ(3, items.size());
    EXPECT_EQ(1003, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation)));
}

// Test that enqueuing a single delete works.
TEST_P(CheckpointTest, Delete) {
    // Enqueue a single delete.
    queued_item qi{new Item{makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 10,
                            /*byseq*/ 0}};
    qi->setDeleted();
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    EXPECT_EQ(1, this->manager->getNumCheckpoints());  // Single open checkpoint.
    EXPECT_EQ(2, manager->getNumOpenChkItems()); // cs + 1x op_del
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(1001, this->manager->getHighSeqno());
    EXPECT_EQ(1001, this->manager->getMaxVisibleSeqno());
    EXPECT_EQ(10, qi->getRevSeqno());

    // Check that the items fetched matches what was enqueued.
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());

    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1001, result.ranges.back().getEnd());
    ASSERT_EQ(2, items.size());
    EXPECT_EQ(1001, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation)));
    EXPECT_TRUE(items[1]->isDeleted());
}

// Test with one open and one closed checkpoint.
TEST_P(CheckpointTest, OneOpenOneClosed) {
    // Add some items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    // cs + 2x op_set
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    const uint64_t ckpt_id1 = this->manager->getOpenCheckpointId();

    // Create a new checkpoint (closing the current open one).
    manager->createNewCheckpoint();
    const uint64_t ckpt_id2 = manager->getOpenCheckpointId();
    EXPECT_EQ(ckpt_id1 + 1, ckpt_id2)
            << "New checkpoint ID should differ from old";
    EXPECT_EQ(1, manager->getNumOpenChkItems()); // just cs

    // Add some items to the newly-opened checkpoint (note same keys as 1st
    // ckpt).
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    // cs + 2x op_set
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    // Examine the items - should be 2 lots of two keys + meta-items.
    EXPECT_EQ(7, manager->getNumItemsForCursor(*cursor));

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    EXPECT_EQ(2, result.ranges.size()); // 2 checkpoints returned items
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1002, result.ranges.front().getEnd());
    EXPECT_EQ(1003, result.ranges.back().getStart());
    EXPECT_EQ(1004, result.ranges.back().getEnd());
    EXPECT_EQ(1002, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);
    EXPECT_EQ(7, items.size());
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::checkpoint_end),
                                     HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation),
                                     HasOperation(queue_op::mutation)));
}

// Test demonstrates some of the basics behaviour of the MB-35003 changes.
// The CheckpointManager methods that return a CursorResult can return multiple
// snapshot ranges if the set of items returns spans multiple snapshots.
// The test also demonstrates a partial snapshot
TEST_P(ReplicaCheckpointTest, getItems_MultipleSnapshots) {
    // 1st Snapshot covers 1001, 1003, but item 1002 de-duped
    this->manager->createSnapshot(1001, 1003, {}, CheckpointType::Memory, 1003);
    EXPECT_TRUE(this->queueReplicatedItem("k1", 1001));
    EXPECT_TRUE(this->queueReplicatedItem("k2", 1003));

    // 2nd Snapshot covers 1004-1006 and all items are received
    // here we pretend that 1005 is hidden
    this->manager->createSnapshot(1004, 1006, {}, CheckpointType::Memory, 1005);

    for (auto i : {1004, 1005, 1006}) {
        EXPECT_TRUE(this->queueReplicatedItem("k" + std::to_string(i), i));
    }

    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(4, manager->getNumOpenChkItems());
    std::vector<queued_item> items;
    auto cursorResult =
            manager->getItemsForCursor(*cursor,
                                       items,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<size_t>::max());
    EXPECT_FALSE(cursorResult.moreAvailable);

    // Expect to see all of the items and two snapshot ranges
    EXPECT_EQ(2, cursorResult.ranges.size());
    // Still see the ranges 1001,1002 and 1003,1005
    EXPECT_EQ(1001, cursorResult.ranges[0].getStart());
    EXPECT_EQ(1003, cursorResult.ranges[0].getEnd());
    EXPECT_EQ(1004, cursorResult.ranges[1].getStart());
    EXPECT_EQ(1006, cursorResult.ranges[1].getEnd());
    EXPECT_EQ(8, items.size()); // cp start, 2 items, cp end, cp start 3 items
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1001, items.at(1)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(2)->getOperation());
    EXPECT_EQ(1003, items.at(2)->getBySeqno());
    EXPECT_EQ(queue_op::checkpoint_end, items.at(3)->getOperation());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(4)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(5)->getOperation());
    EXPECT_EQ(1004, items.at(5)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(6)->getOperation());
    EXPECT_EQ(1005, items.at(6)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(7)->getOperation());
    EXPECT_EQ(1006, items.at(7)->getBySeqno());
    EXPECT_EQ(1003, cursorResult.visibleSeqno);
    EXPECT_FALSE(cursorResult.highCompletedSeqno);
}

// However different types of snapshot don't get combined.
// Extended with dedicated checks and comments for the scenario hit in MB-55520.
TEST_P(ReplicaCheckpointTest, getItems_MemoryDiskSnapshots) {
    // 1st Snapshot covers 1001, 1003, but item 1002 de-duped
    this->manager->createSnapshot(1001, 1003, {}, CheckpointType::Memory, 1003);
    EXPECT_TRUE(this->queueReplicatedItem("k1", 1001));
    EXPECT_TRUE(this->queueReplicatedItem("k2", 1003));

    // 2nd Snapshot covers 1004-1006 and all items are received
    this->manager->createSnapshot(1004, 1006, 0, CheckpointType::Disk, 1006);

    for (auto i : {1004, 1005, 1006}) {
        EXPECT_TRUE(this->queueReplicatedItem("k" + std::to_string(i), i));
    }

    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(4, manager->getNumOpenChkItems());
    std::vector<queued_item> items;
    auto cursorResult =
            manager->getItemsForCursor(*cursor,
                                       items,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<size_t>::max());
    EXPECT_TRUE(cursorResult.moreAvailable);

    // Expect only the first snapshot
    EXPECT_EQ(1, cursorResult.ranges.size());
    // Only range 1001, 1003
    EXPECT_EQ(1001, cursorResult.ranges[0].getStart());
    EXPECT_EQ(1003, cursorResult.ranges[0].getEnd());
    EXPECT_EQ(4, items.size()); // cp start, 2 items, cp end
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1001, items.at(1)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(2)->getOperation());
    EXPECT_EQ(1003, items.at(2)->getBySeqno());
    EXPECT_EQ(queue_op::checkpoint_end, items.at(3)->getOperation());

    // Verify the other ItemsForCursor quantities
    EXPECT_EQ(1003, cursorResult.visibleSeqno);

    // !! VERY IMPORTANT AS COVERAGE FOR MB-55520 !!
    // Before the fix here we get CheckpointType::Disk && !HCS, which is the
    // illegal state that fails ActiveStream in MB-55520.
    EXPECT_EQ(CheckpointType::Memory, cursorResult.checkpointType);
    EXPECT_FALSE(cursorResult.highCompletedSeqno);
}

// Test checkpoint and cursor accounting - when checkpoints are closed the
// offset of cursors is updated as appropriate.
TEST_P(CheckpointTest, CursorOffsetOnCheckpointClose) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    // cs + 2x op_set
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    // Use the existing persistence cursor for this test:
    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor))
            << "Cursor should initially have 3 items pending";

    // Check de-dupe counting - after adding another item with the same key,
    // should still see two items.
    EXPECT_FALSE(this->queueNewItem("key1")) << "Adding a duplicate key to "
                                                "open checkpoint should not "
                                                "increase queue size";

    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor))
            << "Expected 3 items for cursor (cs + 2x op_set) after adding a "
               "duplicate.";

    // Create a new checkpoint (closing the current open one).
    this->manager->createNewCheckpoint();
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(5, manager->getNumItemsForCursor(*cursor))
            << "Expected 5 items for cursor after creating new checkpoint (ce "
               "+ cs added)";

    // Advance persistence cursor - first to get the 'checkpoint_start' meta
    // item, and a second time to get the a 'proper' mutation.
    bool isLastMutationItem;
    auto item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(4, manager->getNumItemsForCursor(*cursor))
            << "Expected 4 items for cursor after advancing one item";

    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor))
            << "Expected 3 item for cursor after advancing by 1";

    // Add two items to the newly-opened checkpoint. Same keys as 1st ckpt,
    // but cannot de-dupe across checkpoints.
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    EXPECT_EQ(5, manager->getNumItemsForCursor(*cursor))
            << "Expected 5 items for cursor after adding 2 more to new "
               "checkpoint";

    // Advance the cursor 'out' of the first checkpoint.
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);

    // Now at the end of the first checkpoint, move into the next checkpoint.
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);

    // Both previous checkpoints became closed/unref, so they were removed for
    // deallocation. This will cause the cursor offset to be recalculated.
    // Note: Closed/unref checkpoints already removed, attempting to remove them
    // manually is a NOP here
    EXPECT_GT(manager->getMemFreedByCheckpointRemoval(), 0);
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumItemsForCursor(*cursor));

    // Drain the remaining items.
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);

    EXPECT_EQ(0, manager->getNumItemsForCursor(*cursor));
}

TEST_P(CheckpointTest, ItemsForCheckpointCursor) {
    // We want to have items across 2 checkpoints.
    checkpoint_config->setCheckpointMaxSize(1);
    ASSERT_EQ(1, manager->getCheckpointConfig().getCheckpointMaxSize());
    ASSERT_EQ(2, manager->getCheckpointConfig().getMaxCheckpoints());

    // Add items such that we have 2 checkpoints
    for (size_t i = 0; manager->getNumCheckpoints() < 2; ++i) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }

    const auto& ckptList = manager->getCheckpointList();
    // We make computations on the num non-meta items in the following.
    // CHK1 has start+end
    // CHK2 has start
    const auto numItemsCkpt1 = ckptList.front()->getNumItems() - 2;
    const auto numItemsCkpt2 = ckptList.back()->getNumItems() - 1;

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    const auto test = [this, numItemsCkpt1, numItemsCkpt2](
                              CheckpointCursor* cursor) -> void {
        std::vector<queued_item> items;
        auto result =
                manager->getItemsForCursor(*cursor,
                                           items,
                                           std::numeric_limits<size_t>::max(),
                                           std::numeric_limits<size_t>::max());

        // We should have got (numItemsCkpt1 + numItemsCkpt2 + 3) items.
        // 3 additional are op_ckpt_start, op_ckpt_end and op_ckpt_start
        EXPECT_EQ(numItemsCkpt1 + numItemsCkpt2 + 3, items.size());
        EXPECT_EQ(2, result.ranges.size());
        EXPECT_EQ(1000, result.ranges.at(0).getStart());
        EXPECT_EQ(1000 + numItemsCkpt1, result.ranges.at(0).getEnd());
        EXPECT_EQ(1000 + numItemsCkpt1 + 1, result.ranges.at(1).getStart());
        EXPECT_EQ(1000 + numItemsCkpt1 + numItemsCkpt2,
                  result.ranges.at(1).getEnd());
        // Max visible seqno for the first checkpoint returned.. see definitions
        // in ItemsForCursor for details
        EXPECT_EQ(1000 + numItemsCkpt1, result.visibleSeqno);
        EXPECT_FALSE(result.highCompletedSeqno);
    };

    // Get items for persistence (EP test only)
    test(cursor);
    // Get items for DCP cursor (EP/Ephe)
    test(dcpCursor.takeCursor().lock().get());
}

// Test getItemsForDcp() when it is limited to fewer items than exist
// in total. Cursor should only advanced to the start of the 2nd checkpoint.
TEST_P(CheckpointTest, ItemsForCheckpointCursorLimited) {
    // We want to have items across 2 checkpoints.
    checkpoint_config->setCheckpointMaxSize(1);
    ASSERT_EQ(1, manager->getCheckpointConfig().getCheckpointMaxSize());
    ASSERT_EQ(2, manager->getCheckpointConfig().getMaxCheckpoints());

    // Add items such that we have 2 checkpoints
    for (size_t i = 0; manager->getNumCheckpoints() < 2; ++i) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }

    const auto& ckptList = manager->getCheckpointList();
    const auto numItemsCkpt1 = ckptList.front()->getNumItems() - 2;

    /* Get items for persistence. Specify a limit of 1 so we should only
     * fetch the first checkpoints' worth.
     */
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1000 + numItemsCkpt1, result.ranges.front().getEnd());
    EXPECT_EQ(numItemsCkpt1 + 2, items.size())
            << "Should have maxItems + 2 (ckpt start & end) items";
    EXPECT_EQ(2, (*cursor->getCheckpoint())->getId())
            << "Cursor should have moved into second checkpoint.";
}

// Limit returned to flusher is strict for Disk checkpoints
TEST_P(CheckpointTest, DiskCheckpointStrictItemLimit) {
    // Test only relevant for persistent buckets as it relates to the
    // persistence cursor
    if (!persistent()) {
        return;
    }

    checkpoint_config->setCheckpointMaxSize(1);
    ASSERT_EQ(1, manager->getCheckpointConfig().getCheckpointMaxSize());
    ASSERT_EQ(2, manager->getCheckpointConfig().getMaxCheckpoints());

    // Need to be an active vbucket to make sure we create a new checkpoint due
    // to the "max checkpoint items" limit being hit. As replica vbuckets aren't
    // responsible to checkpoint creation.
    vbucket->setState(vbucket_state_active);
    // Force the checkpoint to be a disk one
    CheckpointManagerTestIntrospector::setOpenCheckpointType(
            *manager, CheckpointType::Disk);

    // Add items such that we have 2 checkpoints
    for (size_t i = 0; manager->getNumCheckpoints() < 2; ++i) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }

    const auto& ckptList = manager->getCheckpointList();
    const auto numItemsCkpt1 = ckptList.front()->getNumItems() - 2;

    /* Get items for persistence. Specify a limit of 1 so we should only
     * fetch the first item
     */
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1000 + numItemsCkpt1, result.ranges.front().getEnd());
    EXPECT_EQ(1, items.size()) << "Should have 1 item";
    EXPECT_EQ(1, (*cursor->getCheckpoint())->getId())
            << "Cursor should not have moved into second checkpoint.";
}

// Test the checkpoint cursor movement
TEST_P(CheckpointTest, CursorMovement) {
    // 1 item / 1 checkpoint
    ASSERT_TRUE(queueNewItem("key"));

    ASSERT_EQ(1, manager->getNumCheckpoints());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    const auto numItems = ckptList.front()->getNumItems();
    ASSERT_EQ(2, numItems);
    ASSERT_EQ(1001, manager->getHighSeqno());

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor =
            manager->registerCursorBySeqno(dcp_cursor.c_str(),
                                           0,
                                           CheckpointCursor::Droppable::Yes)
                    .takeCursor();

    /* Get items for persistence cursor */
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    result.flushHandle.reset();

    /* We should have got (maxItems + op_ckpt_start) items. */
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1001, result.ranges.front().getEnd());

    /* Get items for DCP replication cursor */
    items.clear();
    result = this->manager->getNextItemsForDcp(*dcpCursor.lock(), items);
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000, result.ranges.front().getStart());
    EXPECT_EQ(1001, result.ranges.front().getEnd());

    const auto openId = manager->getOpenCheckpointId();
    manager->createNewCheckpoint();
    EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());

    /* Get items for persistence cursor, cs is available */
    EXPECT_EQ(1, manager->getNumItemsForCursor(*cursor));

    items.clear();
    result = manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());

    /* We should have got op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1002, items.front()->getBySeqno());
    EXPECT_EQ(1002, result.ranges.front().getStart());
    EXPECT_EQ(1002, result.ranges.front().getEnd());

    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(0, manager->getNumItemsForCursor(*cursor));

    /* Get items for DCP replication cursor */
    EXPECT_EQ(1, manager->getNumItemsForCursor(*dcpCursor.lock()))
            << "Expected to have no normal (only meta) items";
    items.clear();
    this->manager->getNextItemsForDcp(*dcpCursor.lock(), items);
    /* Expecting only 1 op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(0, manager->getNumItemsForCursor(*dcpCursor.lock()));
}

// MB-25056 - Regression test replicating situation where the seqno returned by
// registerCursorBySeqno minus one is greater than the input parameter
// startBySeqno but a backfill is not required.
TEST_P(CheckpointTest, MB25056_backfill_not_required) {
    std::vector<queued_item> items;
    this->vbucket->setState(vbucket_state_replica);

    ASSERT_TRUE(this->queueNewItem("key0"));
    // Add duplicate items, which should cause de-duplication to occur.
    for (unsigned int ii = 0; ii < 10; ii++) {
        ASSERT_FALSE(this->queueNewItem("key0"));
    }
    // [e:1 cs:1 m:1011)
    ASSERT_EQ(1011, manager->getHighSeqno());

    // Add a number of non duplicate items to the same checkpoint
    for (unsigned int ii = 1; ii < 10; ii++) {
        ASSERT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }
    // [e:1 cs:1 m:1011 m:1012 .. m:1020)
    ASSERT_EQ(1020, manager->getHighSeqno());

    // Register DCP replication cursor
    std::string dcp_cursor(DCP_CURSOR_PREFIX);
    // Request to register the cursor with a seqno that has been de-duped away
    CursorRegResult result = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 1005, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1011, result.nextSeqno)
            << "Returned seqno is not expected value.";
    EXPECT_FALSE(result.tryBackfill) << "Backfill is unexpectedly required.";
}

//
// It's critical that the HLC (CAS) is ordered with seqno generation
// otherwise XDCR may drop a newer bySeqno mutation because the CAS is not
// higher.
//
TEST_P(CheckpointTest, SeqnoAndHLCOrdering) {
    const int n_threads = 8;
    const int n_items = 1000;

    std::vector<std::thread> threads;

    // vector of pairs, first is seqno, second is CAS
    // just do a scatter gather over n_threads
    std::vector<std::vector<std::pair<uint64_t, uint64_t> > > threadData(n_threads);
    for (int ii = 0; ii < n_threads; ii++) {
        auto& threadsData = threadData[ii];
        threads.emplace_back([this, ii, n_items, &threadsData]() {
            std::string key = "key" + std::to_string(ii);
            for (int item  = 0; item < n_items; item++) {
                queued_item qi(
                        new Item(makeStoredDocKey(key + std::to_string(item)),
                                 this->vbucket->getId(),
                                 queue_op::mutation,
                                 /*revSeq*/ 0,
                                 /*bySeq*/ 0));
                EXPECT_TRUE(manager->queueDirty(qi,
                                                GenerateBySeqno::Yes,
                                                GenerateCas::Yes,
                                                /*preLinkDocCtx*/ nullptr));

                // Save seqno/cas
                threadsData.emplace_back(qi->getBySeqno(), qi->getCas());
            }
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(1, manager->getNumCheckpoints());
    // cs + mutations
    ASSERT_EQ(1 + n_threads * n_items, manager->getNumOpenChkItems());

    // Now combine the data and check HLC is increasing with seqno
    std::map<uint64_t, uint64_t> finalData;
    for (auto t : threadData) {
        for (auto pair : t) {
            EXPECT_EQ(finalData.end(), finalData.find(pair.first));
            finalData[pair.first] = pair.second;
        }
    }

    auto itr = finalData.begin();
    EXPECT_NE(itr, finalData.end());
    uint64_t previousCas = (itr++)->second;
    EXPECT_NE(itr, finalData.end());
    for (; itr != finalData.end(); itr++) {
        EXPECT_LT(previousCas, itr->second);
        previousCas = itr->second;
    }

    // Now a final check, iterate the checkpoint and also check for increasing
    // HLC.
    std::vector<queued_item> items;
    manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());

    // We should have got (op_ckpt_start + n_threads*n_items) items
    EXPECT_EQ(1 + n_threads * n_items, items.size());

    previousCas = items[1]->getCas();
    for (size_t ii = 2; ii < items.size(); ii++) {
        EXPECT_LT(previousCas, items[ii]->getCas());
        previousCas = items[ii]->getCas();
    }
}

// Test cursor is correctly updated when enqueuing a key which already exists
// in the checkpoint (and needs de-duping), where the cursor points at a
// meta-item at the head of the checkpoint:
//
//  Before:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET(key), 2:SET_VBSTATE() ]
//                                                               ^
//                                                            Cursor
//
//  After:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 2:SET_VBSTATE(), 2:SET(key) ]
//                                                     ^
//                                                   Cursor
//
TEST_P(CheckpointTest, CursorUpdateForExistingItemWithMetaItemAtHead) {
    // Setup the checkpoint and cursor.
    ASSERT_EQ(1, this->manager->getNumItems());
    ASSERT_TRUE(this->queueNewItem("key"));
    ASSERT_EQ(2, this->manager->getNumItems());
    manager->queueSetVBState();

    ASSERT_EQ(3, this->manager->getNumItems());

    // Advance persistence cursor so all items have been consumed.
    std::vector<queued_item> items;
    manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    ASSERT_EQ(3, items.size());
    ASSERT_EQ(0, manager->getNumItemsForCursor(*cursor));

    // Queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, manager->getNumItemsForCursor(*cursor));

    // Should have another item to read (new version of 'key')
    items.clear();
    manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, items.size());
}

// Test cursor is correctly updated when enqueuing a key which already exists
// in the checkpoint (and needs de-duping), where the cursor points at a
// meta-item *not* at the head of the checkpoint:
//
//  Before:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET_VBSTATE(key), 1:SET() ]
//                                                     ^
//                                                    Cursor
//
//  After:
//      Checkpoint [ 0:EMPTY(), 1:CKPT_START(), 1:SET_VBSTATE(key), 2:SET() ]
//                                                     ^
//                                                   Cursor
//
TEST_P(CheckpointTest, CursorUpdateForExistingItemWithNonMetaItemAtHead) {
    // Setup the checkpoint and cursor.
    ASSERT_EQ(1, this->manager->getNumItems());
    manager->queueSetVBState();
    ASSERT_EQ(2, this->manager->getNumItems());

    // Advance persistence cursor so all items have been consumed.
    std::vector<queued_item> items;
    manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    ASSERT_EQ(2, items.size());
    ASSERT_EQ(0, manager->getNumItemsForCursor(*cursor));

    // Queue a set (cursor will now be one behind).
    ASSERT_TRUE(this->queueNewItem("key"));
    ASSERT_EQ(1, manager->getNumItemsForCursor(*cursor));

    // Test: queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, manager->getNumItemsForCursor(*cursor));

    // Should an item to read (new version of 'key')
    items.clear();
    manager->getItemsForCursor(
            *cursor, items, 9999, std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(1002, items.at(0)->getBySeqno());
    EXPECT_EQ(makeStoredDocKey("key"), items.at(0)->getKey());
}

// Regression test for MB-21925 - when a duplicate key is queued and the
// persistence cursor is still positioned on the initial dummy key,
// should return SuccessExistingItem.
TEST_P(CheckpointTest,
       MB21925_QueueDuplicateWithPersistenceCursorOnInitialMetaItem) {
    // Need a manager starting from seqno zero.
    createManager(0);
    ASSERT_EQ(0, this->manager->getHighSeqno());
    ASSERT_EQ(1, this->manager->getNumItems())
            << "Should start with queue_op::empty on checkpoint.";

    // Add an item with some new key.
    ASSERT_TRUE(this->queueNewItem("key"));

    // Test - second item (duplicate key) should return false.
    EXPECT_FALSE(this->queueNewItem("key"));
}

/*
 * Test modified following formal removal of backfill queue. Now the test
 * demonstrates an initial disk backfill being received and completed and that
 * all items enter the checkpoint. On completion of the snapshot no new
 * checkpoint is created, only a new snapshot will do that.
 */
TEST_F(SingleThreadedCheckpointTest, CloseReplicaCheckpointOnDiskSnapshotEnd) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(ckptMgr);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    // We must have only 1 initial open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);
    // We must have only one cursor (the persistence cursor), as there is no
    // DCP producer for vbid
    EXPECT_EQ(ckptMgr->getNumOfCursors(), 1);
    // We must have only the checkpoint_start and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(2, ckptList.back()->getNumItems());
    EXPECT_EQ(2, ckptMgr->getNumItems());

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test-consumer");
    auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    0 /* flags */,
                    0 /* opaque */,
                    vbid,
                    0 /* startSeqno */,
                    std::numeric_limits<uint64_t>::max() /* endSeqno */,
                    0 /* vbUuid */,
                    0 /* snapStartSeqno */,
                    0 /* snapEndSeqno */,
                    0 /* vb_high_seqno */,
                    Collections::ManifestUid{} /* vb_manifest_uid */));

    uint64_t snapshotStart = 1;
    const uint64_t snapshotEnd = 10;

    uint32_t flags = dcp_marker_flag_t::MARKER_FLAG_DISK;

    // 1) the consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(0 /* opaque */,
                                  vbid,
                                  snapshotStart,
                                  snapshotEnd,
                                  flags,
                                  0 /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {});
    passiveStream->processMarker(&snapshotMarker);

    // We must have 1 open checkpoint with id=2
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 2);

    // 2) the consumer receives the mutations until (snapshotEnd -1)
    processMutations(*passiveStream, snapshotStart, snapshotEnd - 1);

    // We must have again 1 open checkpoint with id=2
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 2);
    EXPECT_EQ(1 + (snapshotEnd - 1), ckptMgr->getNumOpenChkItems());

    // 3) the consumer receives the snapshotEnd mutation
    processMutations(*passiveStream, snapshotEnd, snapshotEnd);

    // We must have again 1 open checkpoint with id=2
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 2);
    EXPECT_EQ(1 + snapshotEnd, ckptMgr->getNumOpenChkItems());

    // 4) the consumer receives a second snapshot-marker
    SnapshotMarker snapshotMarker2(0 /* opaque */,
                                   vbid,
                                   snapshotEnd + 1,
                                   snapshotEnd + 2,
                                   dcp_marker_flag_t::MARKER_FLAG_CHK,
                                   {} /*HCS*/,
                                   {} /*maxVisibleSeqno*/,
                                   {}, // timestamp
                                   {} /*SID*/);
    passiveStream->processMarker(&snapshotMarker2);
    EXPECT_EQ(ckptList.size(), 2);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 3);
    EXPECT_EQ(1, ckptMgr->getNumOpenChkItems());

    store->deleteVBucket(vb->getId(), cookie);
}

/*
 * Only if (mem_used > high_wat), then we expect that a Consumer closes the
 * open checkpoint and creates a new one when a PassiveStream receives the
 * snapshotEnd mutation for both:
 *     - memory-snapshot
 *     - disk-snapshot && vbHighSeqno > 0, which is processed as memory-snapshot
 *
 * Note that the test executes 4 combinations in total:
 *     {mem-snap, disk-snap} x {lowMemUsed, highMemUsed}
 *
 * **NOTE** as of MB-35764, the low and high mem used cases are expected to be
 * the same; the replica should not close the checkpoint until instructed
 * by the active (receiving a snapshot marker with the CHK flag set).
 * This set of tests could validly be removed now, but are being kept (for now)
 * to confirm the behaviour matches regardless of the mem_used
 */
void SingleThreadedCheckpointTest::closeReplicaCheckpointOnMemorySnapshotEnd(
        bool highMemUsed, uint32_t flags) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(ckptMgr);

    EPStats& stats = engine->getEpStats();
    if (highMemUsed) {
        // Simulate (mem_used > high_wat) by setting high_wat=0
        stats.mem_high_wat.store(0);
    }
    int openedCheckPoints = 1;
    // We must have only 1 open checkpoint
    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
    // We must have only one cursor (the persistence cursor), as there
    // is no DCP producer for vbid
    EXPECT_EQ(ckptMgr->getNumOfCursors(), 1);
    // We must have only the checkpoint-start and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(2, ckptMgr->getNumItems());
    EXPECT_EQ(2, ckptMgr->getNumOpenChkItems());

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test-consumer");
    auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    0 /* flags */,
                    0 /* opaque */,
                    vbid,
                    0 /* startSeqno */,
                    std::numeric_limits<uint64_t>::max() /* endSeqno */,
                    0 /* vbUuid */,
                    0 /* snapStartSeqno */,
                    0 /* snapEndSeqno */,
                    0 /* vb_high_seqno */,
                    Collections::ManifestUid{} /* vb_manifest_uid */));

    uint64_t snapshotStart = 1;
    const uint64_t snapshotEnd = 10;

    // Note: for a DcpConsumer only the vbHighSeqno=0 disk-snapshot
    //     exists (so it is the only disk-snapshot for which the
    //     consumer enqueues incoming mutation to the backfill-queue).
    //     All the subsequent disk-snapshots (vbHighSeqno>0) are
    //     actually processed as memory-snapshot, so the incoming
    //     mutations are queued to the mutable checkpoint. Here we are
    //     testing checkpoints, that is why for the disk-snapshot case:
    //     1) we process a first disk-snapshot; this sets the
    //     vbHighSeqno
    //         to something > 0; we don't care about the status of
    //         checkpoints here
    //     2) we carry on with processing a second disk-snapshot, which
    //         involves checkpoints
    int openCheckpointSize = snapshotEnd - snapshotStart;
    if (flags & dcp_marker_flag_t::MARKER_FLAG_DISK) {
        // Just process the first half of mutations as vbSeqno-0
        // disk-snapshot
        const uint64_t diskSnapshotEnd = (snapshotEnd - snapshotStart) / 2;
        SnapshotMarker snapshotMarker(0 /* opaque */,
                                      vbid,
                                      snapshotStart,
                                      diskSnapshotEnd,
                                      flags,
                                      0 /*HCS*/,
                                      {} /*maxVisibleSeqno*/,
                                      {}, // timestamp
                                      {} /*SID*/);
        passiveStream->processMarker(&snapshotMarker);
        processMutations(*passiveStream, snapshotStart, diskSnapshotEnd);
        snapshotStart = diskSnapshotEnd + 1;

        // checkpoint extended
        openCheckpointSize = diskSnapshotEnd;

        EXPECT_EQ(1 + openCheckpointSize, ckptMgr->getNumOpenChkItems());
    }

    // 1) the consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(0 /* opaque */,
                                  vbid,
                                  snapshotStart,
                                  snapshotEnd,
                                  flags,
                                  0 /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {} /*SID*/);
    passiveStream->processMarker(&snapshotMarker);

    // 2) the consumer receives the mutations until (snapshotEnd -1)
    processMutations(*passiveStream, snapshotStart, snapshotEnd - 1);

    if (flags & dcp_marker_flag_t::MARKER_FLAG_DISK) {
        // checkpoint contains intial backfill and second snapshot
        openCheckpointSize = snapshotEnd - 1;
    }

    // We must have exactly (snapshotEnd - snapshotStart) mutations in the
    // checkpoint + chk_start
    EXPECT_EQ(1 + openCheckpointSize, ckptMgr->getNumOpenChkItems());

    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());

    // 3) the consumer receives the snapshotEnd mutation
    processMutations(*passiveStream, snapshotEnd, snapshotEnd);

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *ckptMgr);

    if (highMemUsed) {
        // Check that (mem_used > high_wat) when we processed the
        // snapshotEnd mutation
        ASSERT_GT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load());

    } else {
        // Check that (mem_used < high_wat) when we processed the
        // snapshotEnd mutation
        ASSERT_LT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load());
    }

    // The consumer has received the snapshotEnd mutation, but
    // mem_used<high_wat, so we must still have 1 open checkpoint
    // that store chk_start + all mutations
    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
    EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.back()->getState());
    EXPECT_EQ(ckptList.back()->getNumItems(), snapshotEnd + 1);

    store->deleteVBucket(vb->getId(), cookie);
}

// MB-42780: Test disabled as already marked as "could validly be removed now"
// above + the test is now legally failing due to changes in the checkpoint-list
TEST_F(SingleThreadedCheckpointTest,
       DISABLED_CloseReplicaCheckpointOnMemorySnapshotEnd_HighMemDisk) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            true, dcp_marker_flag_t::MARKER_FLAG_DISK);
}

// MB-42780: Test disabled as already marked as "could validly be removed now"
// above + the test is now legally failing due to changes in the checkpoint-list
TEST_F(SingleThreadedCheckpointTest,
       DISABLED_CloseReplicaCheckpointOnMemorySnapshotEnd_Disk) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            false, dcp_marker_flag_t::MARKER_FLAG_DISK);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd_HighMem) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            true, dcp_marker_flag_t::MARKER_FLAG_MEMORY);
}

TEST_F(SingleThreadedCheckpointTest,
       CloseReplicaCheckpointOnMemorySnapshotEnd) {
    closeReplicaCheckpointOnMemorySnapshotEnd(
            false, dcp_marker_flag_t::MARKER_FLAG_MEMORY);
}

TEST_F(SingleThreadedCheckpointTest, CheckpointMaxSize_AutoSetup) {
    auto& config = engine->getConfiguration();
    const uint32_t _1GB = 1024 * 1024 * 1024;
    config.setMaxSize(_1GB);
    const auto ckptMemRatio = 0.4f;
    config.setCheckpointMemoryRatio(ckptMemRatio);
    const auto maxCheckpoints = 20;
    config.setMaxCheckpoints(maxCheckpoints);
    config.setCheckpointMaxSize(0); // 0 triggers auto-setup

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    ASSERT_EQ(_1GB, config.getMaxSize());
    ASSERT_EQ(ckptMemRatio, store->getCheckpointMemoryRatio());
    ASSERT_EQ(maxCheckpoints,
              manager.getCheckpointConfig().getMaxCheckpoints());

    const auto cmQuota = _1GB * ckptMemRatio;
    const auto numVBuckets = store->getVBuckets().getNumAliveVBuckets();
    ASSERT_GT(numVBuckets, 0);
    const auto expected = cmQuota / numVBuckets / maxCheckpoints;
    EXPECT_EQ(expected, manager.getCheckpointConfig().getCheckpointMaxSize());
}

TEST_F(SingleThreadedCheckpointTest,
       CheckpointMaxSize_NeverGreaterThanFlushMaxBytes) {
    auto& config = engine->getConfiguration();
    const size_t _1GB = 1024 * 1024 * 1024;
    const size_t _2GB = 2 * _1GB;
    ASSERT_EQ(_2GB, config.getFlushBatchMaxBytes());

    config.setMaxSize(_2GB);
    config.setCheckpointMemoryRatio(1.0f); // CMQuota takes Full Bucket Quota
    config.setMaxCheckpoints(2); // Two Checkpoints takes full CMQuota

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    EXPECT_EQ(2, ckptConfig.getMaxCheckpoints());
    EXPECT_EQ(_1GB, ckptConfig.getCheckpointMaxSize());

    const size_t _4GB = 4 * _1GB;
    engine->setMaxDataSize(_4GB); // triggers checkpoint's size auto-setup
    EXPECT_EQ(_2GB, ckptConfig.getCheckpointMaxSize());

    const size_t _5GB = 5 * _1GB;
    engine->setMaxDataSize(_5GB); // triggers checkpoint's size auto-setup
    // checkpoint_computed_max_size won't cross flush_batch_max_bytes
    EXPECT_EQ(_2GB, ckptConfig.getCheckpointMaxSize());
}

TEST_F(SingleThreadedCheckpointTest, CheckpointMaxSize_CapAtFlushMaxBytes) {
    auto& config = engine->getConfiguration();
    const size_t _1GB = 1024 * 1024 * 1024;
    const size_t _2GB = 2 * _1GB;
    ASSERT_EQ(_2GB, config.getFlushBatchMaxBytes());

    config.setMaxSize(_2GB);
    config.setCheckpointMemoryRatio(1.0f); // CMQuota takes Full Bucket Quota
    config.setMaxCheckpoints(2); // Two Checkpoints takes full CMQuota

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    EXPECT_EQ(2, ckptConfig.getMaxCheckpoints());
    EXPECT_EQ(_1GB, ckptConfig.getCheckpointMaxSize());

    config.setCheckpointMaxSize(1); // Set to 1
    EXPECT_EQ(1, ckptConfig.getCheckpointMaxSize());

    const size_t _5GB = 5 * _1GB;
    config.setMaxSize(_5GB);
    engine->setMaxDataSize(_5GB); // triggers checkpoint's size auto-setup
    // checkpoint_computed_max_size is capped at flush_batch_max_bytes
    // Note: Before the fix for MB-61238 checkpoint_computed_max_size is still 1
    EXPECT_EQ(_2GB, ckptConfig.getCheckpointMaxSize());
}

TEST_F(SingleThreadedCheckpointTest, MemUsageCheckpointCreation) {
    auto& config = engine->getConfiguration();
    config.setMaxSize(1024 * 1024 * 100);

    config.setMaxCheckpoints(20);
    // Note: This test also verifies that a value > 0 is just set (*)
    const uint32_t _10MB = 1024 * 1024 * 10;
    config.setCheckpointMaxSize(_10MB);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    ASSERT_EQ(20, ckptConfig.getMaxCheckpoints());
    ASSERT_EQ(_10MB, ckptConfig.getCheckpointMaxSize()); // (*)

    ASSERT_EQ(1, manager.getNumCheckpoints());

    const size_t numItems = 5;
    const std::string value(_10MB, '!');
    for (size_t i = 0; i < numItems; ++i) {
        auto item = makeCommittedItem(
                makeStoredDocKey("key" + std::to_string(i)), value, vbid);
        EXPECT_TRUE(manager.queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    }

    // Checkpoints must be created based on checkpoint_max_size.
    // Before enabling the feature all items were queued into a single
    // checkpoint.
    EXPECT_EQ(numItems, manager.getNumCheckpoints());
}

TEST_F(SingleThreadedCheckpointTest,
       MemUsageCheckpointCreation_CkptSizeSmallerThanItemSize) {
    auto& config = engine->getConfiguration();
    config.setMaxSize(1024 * 1024 * 100);

    config.setMaxCheckpoints(20);
    // Set checkpoint max size to something very low
    config.setCheckpointMaxSize(1);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    ASSERT_EQ(20, ckptConfig.getMaxCheckpoints());
    ASSERT_EQ(1, ckptConfig.getCheckpointMaxSize());

    // 1 empty checkpoint
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs + vbs

    // Value is much bigger than the checkpoint max size
    const std::string value(1024, '!');
    store_item(vbid, makeStoredDocKey("key1"), value);

    // Still, the item must be queued in the existing open checkpoint (ie, we
    // must not create another checkpoint).
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(3, manager.getNumOpenChkItems());

    // The next store must create a new checkpoint
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs + mut
}

std::shared_ptr<CheckpointCursor>
SingleThreadedCheckpointTest::testCursorDistance_Register() {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    // e:0 cs:0 vbs:1
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(0, manager.getHighSeqno());

    auto cursor = manager.registerCursorBySeqno(
                                 "cursor", 0, CheckpointCursor::Droppable::Yes)
                          .takeCursor()
                          .lock();
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(0, cursor->getDistance());

    // e:0 cs:0 vbs:1 m:1 m:2
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key1"), value);
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(4, manager.getNumOpenChkItems());
    EXPECT_EQ(2, manager.getHighSeqno());

    cursor = manager.registerCursorBySeqno(
                            "cursor", 1, CheckpointCursor::Droppable::Yes)
                     .takeCursor()
                     .lock();
    EXPECT_EQ(queue_op::mutation, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(3, cursor->getDistance());

    cursor = manager.registerCursorBySeqno(
                            "cursor", 2, CheckpointCursor::Droppable::Yes)
                     .takeCursor()
                     .lock();
    EXPECT_EQ(queue_op::mutation, (*cursor->getPos())->getOperation());
    EXPECT_EQ(2, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(4, cursor->getDistance());

    return cursor;
}

TEST_F(SingleThreadedCheckpointTest, CursorDistance_Register) {
    testCursorDistance_Register();
}

TEST_F(SingleThreadedCheckpointTest, CursorDistance_MoveToNewCheckpoint) {
    auto cursor = testCursorDistance_Register();

    // State here:
    // [e:0 cs:0 vbs:1 m:1 m:2)
    //                     ^

    auto& manager = *store->getVBuckets().getBucket(vbid)->checkpointManager;
    manager.createNewCheckpoint();
    std::vector<queued_item> out;
    manager.getNextItemsForDcp(*cursor, out);

    // [e:0 cs:0 vbs:1 m:1 m:2] [e:3 cs:3)
    //                               ^
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getHighSeqno());
    ASSERT_EQ(queue_op::checkpoint_start, (*cursor->getPos())->getOperation());
    ASSERT_EQ(3, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(1, cursor->getDistance());
}

TEST_F(SingleThreadedCheckpointTest, CursorDistance_Deduplication) {
    auto cursor = testCursorDistance_Register();

    // State here:
    // [e:0 cs:0 vbs:1 m:1 m:2)
    //                     ^

    // Dedup some item before the one pointed by cursor
    store_item(vbid, makeStoredDocKey("key1"), "value");

    // [e:0 cs:0 vbs:1 x m:2 m:3)
    //                   ^
    const auto& manager =
            *store->getVBuckets().getBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(4, manager.getNumOpenChkItems());
    ASSERT_EQ(3, manager.getHighSeqno());
    ASSERT_EQ(queue_op::mutation, (*cursor->getPos())->getOperation());
    ASSERT_EQ(2, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(3, cursor->getDistance());
}

TEST_F(SingleThreadedCheckpointTest, CursorDistance_Expel) {
    auto cursor = testCursorDistance_Register();

    // State here:
    // [e:1 cs:1 vbs:1 m:1 m:2)
    //                     ^
    ASSERT_EQ(4, cursor->getDistance());

    // Expel
    // [e:1 cs:1 x x x)
    //      ^
    ASSERT_EQ(3, flushAndExpelFromCheckpoints(vbid));

    const auto& manager =
            *store->getVBuckets().getBucket(vbid)->checkpointManager;
    ASSERT_EQ(2, manager.getHighSeqno());
    ASSERT_EQ(queue_op::checkpoint_start, (*cursor->getPos())->getOperation());
    ASSERT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(1, cursor->getDistance());
}

TEST_F(SingleThreadedCheckpointTest, CursorDistance_ResetCursor) {
    auto cursor = testCursorDistance_Register();

    // Just need to run with 1 cursor, let's keep the test simple
    auto& vb = *store->getVBuckets().getBucket(vbid);
    auto& manager = *vb.checkpointManager;
    manager.removeCursor(*manager.getPersistenceCursor());
    ASSERT_EQ(1, manager.getNumCursors());

    // State here:
    // [e:1 cs:1 vbs:1 m:1 m:2)
    //                     ^
    ASSERT_EQ(4, cursor->getDistance());

    auto newManager = std::make_unique<MockCheckpointManager>(
            engine->getEpStats(),
            vb,
            engine->getCheckpointConfig(),
            0,
            0 /*lastSnapStart*/,
            0 /*lastSnapEnd*/,
            0 /*maxVisible*/,
            0 /*maxPrepareSeqno*/,
            nullptr /*persistence callback*/);
    newManager->removeCursor(*newManager->getPersistenceCursor());

    ASSERT_EQ(1, manager.getNumCursors());
    ASSERT_EQ(0, newManager->getNumOfCursors());
    newManager->takeAndResetCursors(manager);
    EXPECT_EQ(0, manager.getNumCursors());
    EXPECT_EQ(1, newManager->getNumOfCursors());

    EXPECT_EQ(0, newManager->getHighSeqno());
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());

    // [e:1 cs:1 m:1 m:2)
    //  ^
    for (const auto& key : {"key1", "key2"}) {
        auto item = makeCommittedItem(makeStoredDocKey(key), "value");
        ASSERT_TRUE(newManager->queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    }
    EXPECT_EQ(2, newManager->getHighSeqno());
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());

    // [e:1 cs:1 m:1 m:2)
    //               ^
    std::vector<queued_item> items;
    newManager->getNextItemsForDcp(*cursor, items);
    ASSERT_EQ(3, items.size());
    EXPECT_EQ(3, cursor->getDistance());

    // [e:1 cs:1 x m:2)
    //             ^
    // Note: Before the fix for MB-49594, this call fails by:
    // - assertion failure within boost::list::splice() on debug builds
    // - Checkpoint::queueMemOverhead underflow on rel builds
    // - KV assertion failure on dev builds
    EXPECT_EQ(2, newManager->expelUnreferencedCheckpointItems().count);

    EXPECT_EQ(2, newManager->getHighSeqno());
    EXPECT_EQ(queue_op::checkpoint_start, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(1, cursor->getDistance());

    // Need to manually reset before newManager goes out of scope, newManager'll
    // be already destroyed when we'll try to decrement its cursor count.
    cursor.reset();
}

TEST_F(SingleThreadedCheckpointTest, CheckpointHighSeqno) {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;
    EXPECT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();
    flushVBucket(vbid);

    // [e:1 cs:1)
    EXPECT_EQ(2, manager.getOpenCheckpointId());
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems());
    EXPECT_EQ(0, manager.getHighSeqno());

    // [e:1 cs:1 m:1)
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key1"), value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(1, manager.getHighSeqno());

    // [e:1 cs:1 m:1 vbs:2)
    setVBucketState(vbid,
                    vbucket_state_active,
                    {{"topology", nlohmann::json::array({{"n0", "n1"}})}});
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    const auto queue =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    manager);
    const auto it = queue.back();
    EXPECT_EQ(queue_op::set_vbucket_state, it->getOperation());
    EXPECT_EQ(2, it->getBySeqno());
    // Before the fix, this returns 2
    EXPECT_EQ(1, checkpoint.getHighSeqno());

    // The following is just to show the behaviour at registerCursor(startSeqno)
    // in the case where some meta-items (and a set_vbtate) exists at
    // startSeqno+1.
    // The behaviour doesn't change before/after the patch that introduces the
    // test, but this is good to highlight that we actually skip the set_vbstate
    // item. Not a problem given that DCP doesn't stream set_vbstate.

    // [e:1 cs:1 m:1 vbs:2 ce:2] [e:2 cs:2)
    manager.createNewCheckpoint();
    EXPECT_EQ(3, manager.getOpenCheckpointId());
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems());
    EXPECT_EQ(1, manager.getHighSeqno());

    // [e:1 cs:1 m:1 vbs:2 ce:2] [e:2 cs:2)
    //                            ^
    auto cursor = manager.registerCursorBySeqno(
                                 "cursor", 1, CheckpointCursor::Droppable::Yes)
                          .takeCursor()
                          .lock();
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(2, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());
}

void SingleThreadedCheckpointTest::testMinimumCursorSeqno(
        ItemRemovalPath itemRemoval) {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();
    flushVBucket(vbid);

    // [e:1 cs:1)
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // [e:1 cs:1 m:1)
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key"), value);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    ASSERT_EQ(1, manager.getHighSeqno());
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    ASSERT_EQ(1, checkpoint.getMinimumCursorSeqno());
    ASSERT_EQ(1, checkpoint.getHighSeqno());

    switch (itemRemoval) {
    case ItemRemovalPath::None: {
        // [e:1 cs:1 m:1 m:2)
        store_item(vbid, makeStoredDocKey("different-key"), value);
        flushVBucket(vbid);

        EXPECT_EQ(1, checkpoint.getMinimumCursorSeqno());
        EXPECT_EQ(2, checkpoint.getHighSeqno());

        auto res = manager.registerCursorBySeqno(
                "cursor", 1, CheckpointCursor::Droppable::Yes);
        EXPECT_FALSE(res.tryBackfill);
        const auto cursor = res.takeCursor().lock();
        EXPECT_EQ(queue_op::mutation, (*cursor->getPos())->getOperation());
        EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
        EXPECT_EQ(2, cursor->getDistance());

        break;
    }
    // Following path both lead to
    // [e:1 cs:1 x m:2)
    case ItemRemovalPath::Dedup: {
        EXPECT_EQ(2, checkpoint.getNumItems());
        store_item(vbid, makeStoredDocKey("key"), value);
        flushVBucket(vbid);
        EXPECT_EQ(2, checkpoint.getNumItems());

        EXPECT_EQ(1, checkpoint.getMinimumCursorSeqno());
        EXPECT_EQ(2, checkpoint.getHighSeqno());

        // Note on the next checks: By passing seqno:1 in the request, the user
        // tells CM that it has got m:1 already.
        auto res = manager.registerCursorBySeqno(
                "cursor", 1, CheckpointCursor::Droppable::Yes);
        // So, backfill not needed.
        EXPECT_FALSE(res.tryBackfill);
        // Also, cursor positioned at checkpoint_start, so the next snapshot
        // generated for cursor won't have any CHK flag set. That is correct, as
        // if a DCP Consumer has got m:1 that implies that the same client had
        // also previously received a proper SnapshotMarker(CHK).
        // Actually placing the cursor at queue_op::empty would be wrong, as
        // that would generate a another (unnecessary) SnapshotMarker(CHK). The
        // effect at Consumer would be creating an additional/unnecessary
        // checkpoint in CM.
        // Note that I refer to DCP Consumer when the topic is the CHK flag as
        // that is an internal KV replication flag. External DCP clients aren't
        // expected to use that.
        const auto cursor = res.takeCursor().lock();
        EXPECT_EQ(queue_op::checkpoint_start,
                  (*cursor->getPos())->getOperation());
        EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
        EXPECT_EQ(1, cursor->getDistance());

        break;
    }
    case ItemRemovalPath::Expel: {
        EXPECT_EQ(2, checkpoint.getNumItems());
        store_item(vbid, makeStoredDocKey("different-key"), value);
        flushVBucket(vbid);
        EXPECT_EQ(3, checkpoint.getNumItems());
        EXPECT_EQ(2, manager.expelUnreferencedCheckpointItems().count);
        EXPECT_EQ(1, checkpoint.getNumItems());

        // We have expelled all the mutations
        EXPECT_FALSE(checkpoint.getMinimumCursorSeqno());
        EXPECT_FALSE(checkpoint.getHighSeqno());

        // Note: This is a very important point - Cursor registered at the same
        // position as in the Dedup case, but the Expel case requires a backfill

        auto res = manager.registerCursorBySeqno(
                "cursor", 1, CheckpointCursor::Droppable::Yes);
        EXPECT_TRUE(res.tryBackfill);
        const auto cursor = res.takeCursor().lock();
        EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
        EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
        EXPECT_EQ(0, cursor->getDistance());

        break;
    }
    }
}

TEST_F(SingleThreadedCheckpointTest, CheckpointMinimumCursorSeqno) {
    testMinimumCursorSeqno(ItemRemovalPath::None);
}

TEST_F(SingleThreadedCheckpointTest, CheckpointMinimumCursorSeqno_Dedup) {
    testMinimumCursorSeqno(ItemRemovalPath::Dedup);
}

TEST_F(SingleThreadedCheckpointTest, CheckpointMinimumCursorSeqno_Expel) {
    testMinimumCursorSeqno(ItemRemovalPath::Expel);
}

TEST_F(SingleThreadedCheckpointTest, RefCheckpointIsRemovedWhenClosedIfEmpty) {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;
    EXPECT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();
    flushVBucket(vbid);

    // [e:1 cs:1)
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // [e:1 cs:1 m:1)
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key"), value);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    ASSERT_EQ(1, manager.getHighSeqno());
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    ASSERT_EQ(1, checkpoint.getMinimumCursorSeqno());
    ASSERT_EQ(1, checkpoint.getHighSeqno());

    // Expel and make the checkpoint "empty", ie no mutation in it
    ASSERT_EQ(1, manager.getNumCursors());
    flushVBucket(vbid);
    EXPECT_EQ(1, manager.expelUnreferencedCheckpointItems().count);

    // Checkpoint referenced
    ASSERT_EQ(
            1,
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(manager)
                    .getNumCursorsInCheckpoint());

    // Verify that the closed/empty/referenced checkpoint is removed
    EXPECT_EQ(1, manager.getNumCheckpoints());
    const auto preId = manager.getOpenCheckpointId();
    manager.createNewCheckpoint();
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_GT(manager.getOpenCheckpointId(), preId);

    // The cursor is now in the new open checkpoint
    EXPECT_EQ(
            1,
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(manager)
                    .getNumCursorsInCheckpoint());
}

void SingleThreadedCheckpointTest::testRegisterCursorInCheckpointEmptyByExpel(
        bool extraMetaItem) {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();
    flushVBucket(vbid);

    // [e:1 cs:1)
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // [e:1 cs:1 m:1 m:2)
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key"), value);
    store_item(vbid, makeStoredDocKey("other-key"), value);
    flushVBucket(vbid);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(3, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getHighSeqno());
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    ASSERT_EQ(3, manager.getNumOpenChkItems());
    ASSERT_EQ(1, checkpoint.getMinimumCursorSeqno());
    ASSERT_EQ(2, checkpoint.getHighSeqno());

    // Expel
    ASSERT_EQ(3, checkpoint.getNumItems());
    ASSERT_EQ(2, manager.expelUnreferencedCheckpointItems().count);
    ASSERT_EQ(1, checkpoint.getNumItems());

    // We have expelled all the mutations
    // [e:1 cs:1 x x)
    ASSERT_FALSE(checkpoint.getMinimumCursorSeqno());
    ASSERT_FALSE(checkpoint.getHighSeqno());

    if (extraMetaItem) {
        // Add a topology just as a variation from the previous state, that's
        // for ensuring that the operation isn't skipped
        setVBucketState(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
        // cs + vbs
        ASSERT_EQ(2, checkpoint.getNumItems());
        ASSERT_FALSE(checkpoint.getHighSeqno());
        ASSERT_EQ(2, vb->getHighSeqno());
    }

    // Verify that we register the cursor successfully.
    // Before the fix for MB-53055 this step fails in the case where the
    // checkpoint is "empty" but contains some meta-item after checkpoint_start.
    //
    // Note: Checkpoint's highSeqno=2 and user passes lastProcessedSeqno=2,
    // backfill not needed
    auto res = manager.registerCursorBySeqno(
            "cursor_triggers_backfill", 2, CheckpointCursor::Droppable::Yes);
    EXPECT_FALSE(res.tryBackfill);
    auto cursor = res.takeCursor().lock();
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());

    // Note: Checkpoint's highSeqno=2 and user passes lastProcessedSeqno=1,
    // backfill needed
    res = manager.registerCursorBySeqno(
            "cursor_no_backfill", 1, CheckpointCursor::Droppable::Yes);
    EXPECT_TRUE(res.tryBackfill);
    cursor = res.takeCursor().lock();
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(1, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());
}

TEST_F(SingleThreadedCheckpointTest, RegisterCursor_CheckpointEmptyByExpel) {
    testRegisterCursorInCheckpointEmptyByExpel(false);
}

TEST_F(SingleThreadedCheckpointTest,
       RegisterCursor_CheckpointEmptyByExpel_ExtraMetaItem) {
    testRegisterCursorInCheckpointEmptyByExpel(true);
}

void SingleThreadedCheckpointTest::
        testRegisterCursorCheckpointEmptyByExpelMultipleCheckpoints(
                uint64_t startSeqno) {
    // Setup an active vbucket/cm like:
    //
    // [disk | e:1 cs:1 x x vbs:3) [memory | e:3 cs:3)
    //         ^
    //
    // Purpose of the test is verifying that we can successfully register a new
    // cursor in that state.
    // Desired state reachable by a replica->pending->active transition.

    setVBucketState(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();
    flushVBucket(vbid);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test-consumer");
    auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(*engine,
                                        consumer,
                                        "test-passive-stream",
                                        0,
                                        0,
                                        vbid,
                                        0,
                                        std::numeric_limits<uint64_t>::max(),
                                        0,
                                        0,
                                        0,
                                        0,
                                        Collections::ManifestUid{}));
    // Receive marker for disk checkpoint
    SnapshotMarker snapshotMarker(0,
                                  vbid,
                                  1, // start
                                  2, // end
                                  MARKER_FLAG_DISK,
                                  0,
                                  {},
                                  {},
                                  {});
    passiveStream->processMarker(&snapshotMarker);
    // Receive mutations
    processMutations(*passiveStream, 1 /*start*/, 2 /*end*/);
    flushVBucket(vbid);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(manager.getOpenCheckpointType(), CheckpointType::InitialDisk);
    EXPECT_EQ(3, manager.getNumOpenChkItems());
    EXPECT_EQ(3, manager.getOpenCheckpointId());

    // Expel
    ASSERT_EQ(2, manager.expelUnreferencedCheckpointItems().count);
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // We have expelled all the mutations
    // [e:1 cs:1 x x)
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    EXPECT_FALSE(checkpoint.getMinimumCursorSeqno());
    EXPECT_FALSE(checkpoint.getHighSeqno());
    EXPECT_EQ(1, manager.getNumOpenChkItems());
    EXPECT_EQ(3, manager.getOpenCheckpointId());
    ASSERT_EQ(2, checkpoint.getHighestExpelledSeqno());

    // Set vb to pending
    setVBucketState(vbid, vbucket_state_pending);
    // No change in checkpoint
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(manager.getOpenCheckpointType(), CheckpointType::InitialDisk);
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(3, manager.getOpenCheckpointId());

    // Set vb to active
    setVBucketState(vbid, vbucket_state_active, {}, TransferVB::Yes);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(manager.getOpenCheckpointType(), CheckpointType::Memory);
    EXPECT_EQ(4, manager.getOpenCheckpointId());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(4, manager.getOpenCheckpointId());

    // Verify that we register the cursor successfully.
    // Before the fix for MB-53570 this step fails by exception thrown.
    //
    // [disk | e:1 cs:1 x x vbs:3) [memory | e:3 cs:3)
    //                                       ^
    auto res = manager.registerCursorBySeqno(
            "cursor", startSeqno, CheckpointCursor::Droppable::Yes);

    if (startSeqno >= 2) {
        EXPECT_FALSE(res.tryBackfill);
    } else {
        EXPECT_TRUE(res.tryBackfill);
    }

    const auto cursor = res.takeCursor().lock();
    EXPECT_EQ(3, res.nextSeqno);
    EXPECT_EQ(4, (*cursor->getCheckpoint())->getId());
    EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    EXPECT_EQ(3, (*cursor->getPos())->getBySeqno());
    EXPECT_EQ(0, cursor->getDistance());
}

TEST_F(SingleThreadedCheckpointTest,
       RegisterCursorCheckpointEmptyByExpelMultipleCheckpoints) {
    testRegisterCursorCheckpointEmptyByExpelMultipleCheckpoints(2);
}

TEST_F(SingleThreadedCheckpointTest,
       RegisterCursorCheckpointEmptyByExpelMultipleCheckpoints_startZero) {
    testRegisterCursorCheckpointEmptyByExpelMultipleCheckpoints(0);
}

TEST_P(CheckpointTest, MB_58321) {
    manager->createNewCheckpoint();
    ASSERT_TRUE(this->queueNewItem("a")); // seqno:1001
    manager->createNewCheckpoint();
    ASSERT_TRUE(this->queueNewItem("b")); // seqno:1002
    auto res = manager->registerCursorBySeqno(
            "mb58321", 1001, CheckpointCursor::Droppable::Yes);

    EXPECT_EQ(1002, res.nextSeqno);
    EXPECT_FALSE(res.tryBackfill);

    auto cursor = res.takeCursor();
    std::vector<queued_item> items;
    auto result = manager->getNextItemsForDcp(*cursor.lock(), items);

    ASSERT_FALSE(items.empty());
    for (const auto& qi : items) {
        // Should be 1002 onwards
        ASSERT_GT(qi->getBySeqno(), 1001);
    }
}

TEST_F(SingleThreadedCheckpointTest, QueueSetVBStateSchedulesDcpStep) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = engine->getVBucket(vbid);
    auto& cm = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    ASSERT_EQ(1, cm.getNumOfCursors());
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);
    ASSERT_EQ(2, cm.getNumOfCursors());
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto dcpCursor = stream->getCursor().lock();
    ASSERT_TRUE(dcpCursor);

    ASSERT_EQ(1, cm.getNumCheckpoints());
    ASSERT_EQ(1, (*dcpCursor->getCheckpoint())->getId());
    EXPECT_EQ(queue_op::empty, (*dcpCursor->getPos())->getOperation());
    EXPECT_EQ(0, dcpCursor->getDistance());

    // NOTE: DCP stream created *after* cs+vbs queued in checkpoint.
    //   When this test is instroduced that part works already fine. Core test
    //   follows.
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(producer->getReadyQueue().exists(vbid));

    // Move DCP cursor to end of checkpoint and push to stream readyQ
    stream->nextCheckpointItemTask();
    // Move the Producer to settled
    ASSERT_TRUE(producer->getReadyQueue().exists(vbid));
    while (producer->public_getNextItem()) {
    }
    ASSERT_FALSE(producer->getReadyQueue().exists(vbid));
    ASSERT_EQ(0, stream->getItemsRemaining());

    // Core test
    // Queue another set-vbstate and verify that stream notified again
    setVBucketState(vbid,
                    vbucket_state_active,
                    {{"topology", nlohmann::json::array({{"n0", "n1"}})}});
    EXPECT_EQ(1, stream->getItemsRemaining());
    EXPECT_TRUE(producer->getReadyQueue().exists(vbid));
}

TEST_F(SingleThreadedCheckpointTest, GetItemsForCursor_BytesLimit) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto vb = engine->getVBucket(vbid);
    auto& cm = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    ASSERT_EQ(1, cm.getNumOfCursors());
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    createDcpStream(*producer);
    ASSERT_EQ(2, cm.getNumOfCursors());
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto dcpCursor = stream->getCursor().lock();
    ASSERT_TRUE(dcpCursor);

    ASSERT_EQ(1, cm.getNumCheckpoints());
    ASSERT_EQ(1, (*dcpCursor->getCheckpoint())->getId());
    EXPECT_EQ(queue_op::empty, (*dcpCursor->getPos())->getOperation());
    EXPECT_EQ(0, dcpCursor->getDistance());
    EXPECT_EQ(2, cm.getNumItemsForCursor(*dcpCursor)); // cs, vbs

    // [e:1 vbs:1 cs:1 m:1 m:2)
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("key1"), value);
    store_item(vbid, makeStoredDocKey("key2"), value);
    ASSERT_EQ(2, cm.getHighSeqno());
    ASSERT_EQ(4, cm.getNumOpenChkItems());
    EXPECT_EQ(4, cm.getNumItemsForCursor(*dcpCursor)); // cs, vbs, m, m

    // [e:1 vbs:1 cs:1 m:1 m:2 ce:3] [e:3 cs:3 m:3 m:4)
    cm.createNewCheckpoint();
    ASSERT_EQ(2, cm.getNumCheckpoints());
    store_item(vbid, makeStoredDocKey("key3"), value);
    store_item(vbid, makeStoredDocKey("key4"), value);
    ASSERT_EQ(4, cm.getHighSeqno());
    ASSERT_EQ(3, cm.getNumOpenChkItems());
    // cs, vbs, m, m, ce, cs, m, m
    EXPECT_EQ(8, cm.getNumItemsForCursor(*dcpCursor));
    EXPECT_EQ(1, (*dcpCursor->getCheckpoint())->getId());

    // Test - The next call returns only items from the first checkpoint
    std::vector<queued_item> items;
    const auto res = cm.getItemsForCursor(
            *dcpCursor, items, std::numeric_limits<size_t>::max(), 1);
    ASSERT_EQ(1, res.ranges.size());
    const auto range = res.ranges.at(0);
    EXPECT_EQ(0, range.getStart());
    EXPECT_EQ(2, range.getEnd());
    EXPECT_EQ(5, items.size()); // cs, vbs, m, m, ce
    // Also, cursor moved to the open checkpoint
    EXPECT_EQ(2, (*dcpCursor->getCheckpoint())->getId());
}

TEST_F(SingleThreadedCheckpointTest, ItemExpelResilientToVBucketRollback) {
    setVBucketState(vbid, vbucket_state_active);
    auto& vb = *store->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs

    // Note: We need at least 2 mutations for triggering ItemExpel, as we can't
    // expel items pointed from cursors
    store_item(vbid, makeStoredDocKey("key1"), "value");
    store_item(vbid, makeStoredDocKey("key2"), "value");
    ASSERT_EQ(2, vb.getHighSeqno());
    ASSERT_EQ(4, manager.getNumOpenChkItems());

    // Move the cursor, allow ItemExpel
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Simulate the rollback behaviour.
    // ItemExpel plants the expel-cursor in the checkpoint under processing and
    // releases the CM::lock. At that point VB::rollback can clear the CM
    // removing all checkpoint. When ItemExpel resumes it needs to be resilient
    // to that state.
    // The CM::expelHook executes in the section where ItemExpel has released
    // (and not yet re-acquired) the CM::lock.
    manager.expelHook = [&manager]() { manager.clear(); };

    // Before the fix for MB-56644 this step fails by exception triggered,
    // caused by that rollback has removed the checkpoint touched by ItemExpel
    manager.expelUnreferencedCheckpointItems();

    // Note: The CM was cleared, so mem-usage must track the correct allocation
    // for the single/empty checkpoint in CheckpointList
    auto config = CheckpointConfig(store->getEPEngine().getConfiguration());
    const auto emptyManager =
            CheckpointManager(store->getEPEngine().getEpStats(),
                              vb,
                              config,
                              0,
                              0,
                              0,
                              0,
                              0,
                              nullptr);
    EXPECT_EQ(emptyManager.getQueuedItemsMemUsage(),
              manager.getQueuedItemsMemUsage());
    EXPECT_EQ(emptyManager.getMemOverheadQueue(),
              manager.getMemOverheadQueue());
    EXPECT_EQ(0, manager.getMemOverheadIndex());
}

// Test that when the same client registers twice, the first cursor 'dies'
TEST_P(CheckpointTest, ReRegister) {
    ASSERT_EQ(1, manager->getNumOfCursors());

    const std::string name = "dcp-cursor";
    auto cursor1 = manager->registerCursorBySeqno(
                                  name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_NE(nullptr, cursor1.lock().get());
    EXPECT_EQ(2, manager->getNumOfCursors());

    auto cursor2 = this->manager
                           ->registerCursorBySeqno(
                                   name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_EQ(nullptr, cursor1.lock().get());
    EXPECT_NE(nullptr, cursor2.lock().get());
    EXPECT_EQ(2, manager->getNumOfCursors());
}

TEST_F(SingleThreadedCheckpointTest, ReRegister_OldAndNewInClosedCheckpoint) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* manager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(manager);

    ASSERT_EQ(1, manager->getNumOfCursors());

    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumOpenChkItems()); // cs, vbs
    ASSERT_EQ(0, manager->getHighSeqno());
    const std::string value = "value";
    store_item(vbid, makeStoredDocKey("key1"), value);
    EXPECT_EQ(1, manager->getHighSeqno());
    manager->createNewCheckpoint();
    ASSERT_EQ(2, manager->getNumCheckpoints());
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(2, manager->getHighSeqno());

    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    const auto& closed = *list.front();
    ASSERT_EQ(1, closed.getId());
    ASSERT_EQ(1, closed.getNumCursorsInCheckpoint());
    ASSERT_EQ(4, closed.getNumItems()); // cs, vbs, m, ce
    const auto& open = *list.back();
    ASSERT_EQ(2, open.getId());
    ASSERT_EQ(0, open.getNumCursorsInCheckpoint());
    ASSERT_EQ(2, open.getNumItems()); // cs, m

    const std::string name = "dcp-cursor";
    auto cursor1 = manager->registerCursorBySeqno(
                                  name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_TRUE(cursor1.lock());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(2, closed.getNumCursorsInCheckpoint());
    ASSERT_EQ(1, (*cursor1.lock()->getCheckpoint())->getId());

    // Advance the baseline cursor, moving it out of the closed checkpoint.
    // The checkpoint isn't removed as it is still referenced by dcp-cursor.
    {
        std::vector<queued_item> items;
        manager->getNextItemsForPersistence(items);
    }
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getOpenCheckpointId());
    ASSERT_EQ(2, (*manager->getPersistenceCursor()->getCheckpoint())->getId());
    EXPECT_EQ(1, closed.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(closed));
    EXPECT_EQ(1, open.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(open));

    // Verify that re-registering the same dcp-cursor into the same closed
    // checkpoint doesn't trigger checkpoint removal.
    auto cursor2 = manager->registerCursorBySeqno(
                                  name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_FALSE(cursor1.lock());
    EXPECT_TRUE(cursor2.lock());
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getOpenCheckpointId());
    EXPECT_EQ(1, (*cursor2.lock()->getCheckpoint())->getId());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(1, closed.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(closed));
    EXPECT_EQ(1, open.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(open));

    // No checkpoint at Destroyer
    auto& destroyer = *getCkptDestroyerTask(vbid);
    ASSERT_EQ(0, destroyer.getNumCheckpoints());

    // Moving dcp-cursor out of the closed checkpoint makes it unreferenced,
    // so it's removed.
    {
        std::vector<queued_item> items;
        manager->getItemsForCursor(*cursor2.lock(),
                                   items,
                                   std::numeric_limits<size_t>::max(),
                                   std::numeric_limits<size_t>::max());
    }
    EXPECT_EQ(0, manager->getNumItemsForCursor(*cursor2.lock()));
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getOpenCheckpointId());
    EXPECT_EQ(2, (*cursor2.lock()->getCheckpoint())->getId());
    EXPECT_EQ(2, open.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(open));

    // Release the removed checkpoint
    EXPECT_EQ(1, destroyer.getNumCheckpoints());
    destroyer.run();
    EXPECT_EQ(0, destroyer.getNumCheckpoints());
}

TEST_F(SingleThreadedCheckpointTest,
       ReRegister_OldInClosedAndNewInOpenCheckpoint) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* manager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(manager);

    ASSERT_EQ(1, manager->getNumOfCursors());

    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumOpenChkItems()); // cs, vbs
    ASSERT_EQ(0, manager->getHighSeqno());
    const std::string value = "value";
    store_item(vbid, makeStoredDocKey("key1"), value);
    EXPECT_EQ(1, manager->getHighSeqno());
    manager->createNewCheckpoint();
    ASSERT_EQ(2, manager->getNumCheckpoints());
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(2, manager->getHighSeqno());

    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    const auto& closed = *list.front();
    ASSERT_EQ(1, closed.getId());
    ASSERT_EQ(1, closed.getNumCursorsInCheckpoint());
    ASSERT_EQ(4, closed.getNumItems()); // cs, vbs,  m, ce
    const auto& open = *list.back();
    ASSERT_EQ(2, open.getId());
    ASSERT_EQ(0, open.getNumCursorsInCheckpoint());
    ASSERT_EQ(2, open.getNumItems()); // cs, m

    const std::string name = "dcp-cursor";
    auto cursor1 = manager->registerCursorBySeqno(
                                  name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_TRUE(cursor1.lock());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(2, closed.getNumCursorsInCheckpoint());
    ASSERT_EQ(1, (*cursor1.lock()->getCheckpoint())->getId());

    // No checkpoint at Destroyer
    auto& destroyer = *getCkptDestroyerTask(vbid);
    ASSERT_EQ(0, destroyer.getNumCheckpoints());

    // Advance the baseline cursor, moving it out of the closed checkpoint.
    // The checkpoint isn't removed as it is still referenced by dcp-cursor.
    {
        std::vector<queued_item> items;
        manager->getNextItemsForPersistence(items);
    }
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getOpenCheckpointId());
    ASSERT_EQ(2, (*manager->getPersistenceCursor()->getCheckpoint())->getId());
    EXPECT_EQ(1, closed.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(closed));
    EXPECT_EQ(1, open.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(open));

    EXPECT_EQ(0, destroyer.getNumCheckpoints());

    // Verify that re-registering the same dcp-cursor into the subsequent open
    // checkpoint makes the closed checkpoint unreferenced and triggers
    // checkpoint removal.
    auto cursor2 = manager->registerCursorBySeqno(
                                  name, 2, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_FALSE(cursor1.lock());
    EXPECT_TRUE(cursor2.lock());
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getOpenCheckpointId());
    EXPECT_EQ(2, (*cursor2.lock()->getCheckpoint())->getId());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(2, open.getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(open));

    // Release the removed checkpoint
    EXPECT_EQ(1, destroyer.getNumCheckpoints());
    destroyer.run();
    EXPECT_EQ(0, destroyer.getNumCheckpoints());
}

TEST_F(SingleThreadedCheckpointTest,
       ReRegister_OldAndNewInClosedSubsequentCheckpoints) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* manager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(manager);

    ASSERT_EQ(1, manager->getNumOfCursors());

    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumOpenChkItems()); // cs, vbs
    ASSERT_EQ(0, manager->getHighSeqno());
    const std::string value = "value";
    store_item(vbid, makeStoredDocKey("key1"), value);
    EXPECT_EQ(1, manager->getHighSeqno());
    manager->createNewCheckpoint();
    ASSERT_EQ(2, manager->getNumCheckpoints());
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(2, manager->getHighSeqno());
    manager->createNewCheckpoint();
    ASSERT_EQ(3, manager->getNumCheckpoints());
    store_item(vbid, makeStoredDocKey("key3"), value);
    EXPECT_EQ(3, manager->getHighSeqno());

    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    auto it = list.begin();
    ASSERT_EQ(1, (*it)->getId());
    ASSERT_EQ(1, (*it)->getNumCursorsInCheckpoint());
    ASSERT_EQ(4, (*it)->getNumItems()); // cs, vbs,  m, ce
    ++it;
    ASSERT_EQ(2, (*it)->getId());
    ASSERT_EQ(0, (*it)->getNumCursorsInCheckpoint());
    ASSERT_EQ(3, (*it)->getNumItems()); // cs, m, ce
    ++it;
    ASSERT_EQ(3, (*it)->getId());
    ASSERT_EQ(0, (*it)->getNumCursorsInCheckpoint());
    ASSERT_EQ(2, (*it)->getNumItems()); // cs, m

    const std::string name = "dcp-cursor";
    auto cursor1 = manager->registerCursorBySeqno(
                                  name, 0, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_TRUE(cursor1.lock());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(2, (list.front())->getNumCursorsInCheckpoint());
    ASSERT_EQ(1, (*cursor1.lock()->getCheckpoint())->getId());

    // No checkpoint at Destroyer
    auto& destroyer = *getCkptDestroyerTask(vbid);
    ASSERT_EQ(0, destroyer.getNumCheckpoints());

    // Advance the baseline cursor, moving it out of the closed checkpoint.
    // The checkpoint isn't removed as it is still referenced by dcp-cursor.
    {
        std::vector<queued_item> items;
        manager->getNextItemsForPersistence(items);
    }
    EXPECT_EQ(3, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getOpenCheckpointId());
    ASSERT_EQ(3, (*manager->getPersistenceCursor()->getCheckpoint())->getId());
    EXPECT_EQ(1, (list.front())->getNumCursorsInCheckpoint()); // closed
    EXPECT_FALSE(manager->isEligibleForRemoval(*list.front()));
    EXPECT_EQ(1, (list.back())->getNumCursorsInCheckpoint());
    EXPECT_FALSE(manager->isEligibleForRemoval(*list.back())); // open

    EXPECT_EQ(0, destroyer.getNumCheckpoints());

    // Verify that re-registering the same dcp-cursor into the subsequent closed
    // checkpoint makes the first closed checkpoint unreferenced and triggers
    // checkpoint removal.
    //
    // Note: This step covers MB-58692, where we hit a case of referenced
    // checkpoint removed at cursor-replacement. At bug, the subsequent steps
    // throw an assertion that verifies no-cursor in the removed checkpoints.
    auto cursor2 = manager->registerCursorBySeqno(
                                  name, 1, CheckpointCursor::Droppable::Yes)
                           .takeCursor();
    EXPECT_FALSE(cursor1.lock());
    EXPECT_TRUE(cursor2.lock());
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getOpenCheckpointId());
    EXPECT_EQ(2, (*cursor2.lock()->getCheckpoint())->getId());
    EXPECT_EQ(2, manager->getNumOfCursors());
    EXPECT_EQ(1, (list.front())->getNumCursorsInCheckpoint()); // closed
    EXPECT_FALSE(manager->isEligibleForRemoval(*list.front()));
    EXPECT_EQ(1, (list.back())->getNumCursorsInCheckpoint()); // open
    EXPECT_FALSE(manager->isEligibleForRemoval(*list.back()));

    // Release the removed checkpoint
    EXPECT_EQ(1, destroyer.getNumCheckpoints());
    destroyer.run();
    EXPECT_EQ(0, destroyer.getNumCheckpoints());
}

TEST_P(CheckpointTest, TakeAndResetCursors) {
    // The test runs with 2 cursors:
    // 1: CheckpointTest::cursor -> that is Persistence/DCP depending on the
    //                              bucket type
    // 2: Extra DCP cursor
    auto* dcpCursor =
            manager->registerCursorBySeqno(
                           "dcp_cursor", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock()
                    .get();
    ASSERT_EQ(2, manager->getNumOfCursors());

    const auto cursors = {cursor, dcpCursor};

    for (const auto* c : cursors) {
        ASSERT_NE(nullptr, c);
        // cs is available
        ASSERT_EQ(1, manager->getNumItemsForCursor(*c));
        ASSERT_EQ(0, c->getDistance());
    }

    // Store 1 item
    queueNewItem("key");

    for (const auto* c : cursors) {
        EXPECT_EQ(2, manager->getNumItemsForCursor(*c));
        EXPECT_EQ(0, c->getDistance());
    }

    // Move cursors
    for (auto* c : cursors) {
        std::vector<queued_item> items;
        manager->getItemsForCursor(
                *c, items, 9999, std::numeric_limits<size_t>::max());
        // ckpt_starts + mutation
        EXPECT_EQ(2, items.size());
        EXPECT_EQ(2, c->getDistance());
    }

    // Second manager
    auto manager2 = std::make_unique<MockCheckpointManager>(
            this->global_stats,
            *vbucket,
            *checkpoint_config,
            0,
            0 /*lastSnapStart*/,
            0 /*lastSnapEnd*/,
            0 /*maxVisible*/,
            0 /*maxPrepareSeqno*/,
            nullptr /*persistence callback*/);

    // Take cursors from the first CM and place them into the second CM..
    manager2->takeAndResetCursors(*manager);
    EXPECT_EQ(2, manager2->getNumOfCursors());
    EXPECT_EQ(0, manager->getNumOfCursors());
    // ..and destroy first CM
    resetManager();

    // The second CM's cursors are not affected by destroying the first CM
    EXPECT_EQ(1, manager2->getNumCheckpoints());
    EXPECT_EQ(2, manager2->getNumOfCursors());
    EXPECT_EQ(
            2,
            manager2->getCheckpointList().front()->getNumCursorsInCheckpoint());
    for (const auto* c : cursors) {
        EXPECT_EQ(1, manager2->getNumItemsForCursor(*c));
        EXPECT_EQ(queue_op::empty, (*c->getPos())->getOperation());
        EXPECT_EQ(0, c->getDistance());
    }
}

// Test that if we add 2 cursors with the same name the first one is removed.
TEST_P(CheckpointTest, DuplicateCheckpointCursor) {
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    // The persistent cursor means we have one cursor in the checkpoint
    ASSERT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a DCP cursor.
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    EXPECT_EQ(2, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a 2nd DCP cursor with the same name.
    auto dcpCursor2 = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    // Adding the 2nd DCP cursor should not have increased the number of
    // cursors in the checkpoint, as the previous one will have been removed
    // when the new one was added.
    EXPECT_EQ(2,ckptList.back()->getNumCursorsInCheckpoint());
}

// Test that if we add 2 cursors with the same name the first one is removed.
// even if the 2 cursors are in different checkpoints.
TEST_P(CheckpointTest, DuplicateCheckpointCursorDifferentCheckpoints) {
    config.setCheckpointMaxSize(1);
    checkpoint_config = std::make_unique<CheckpointConfig>(config);
    createManager();

    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *manager);
    // The persistent cursor means we have one cursor in the checkpoint
    ASSERT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());

    // Register a DCP cursor.
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    // Adding the following items will result in 2 checkpoints, with
    // both cursors in the first checkpoint.
    const size_t numItems = 2;
    for (size_t i = 0; i < numItems; ++i) {
        queueNewItem("key" + std::to_string(i));
    }
    EXPECT_EQ(2, ckptList.size());
    EXPECT_EQ(2, ckptList.front()->getNumCursorsInCheckpoint());

    // Register a 2nd DCP cursor with the same name but this time into the
    // 2nd checkpoint
    auto dcpCursor2 =
            manager->registerCursorBySeqno(dcp_cursor.c_str(),
                                           1000 + numItems,
                                           CheckpointCursor::Droppable::Yes);

    // Adding the 2nd DCP cursor should not have increased the number of
    // cursors as the previous cursor will have been removed when the new one
    // was added.  The persistence cursor will still be in the first
    // checkpoint however the dcpCursor will have been deleted from the first
    // checkpoint and adding to the 2nd checkpoint.
    EXPECT_EQ(1, ckptList.front()->getNumCursorsInCheckpoint());
    EXPECT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());
}

/**
 * MB-35589: We do not add keys to the indexes of Disk Checkpoints.
 *
 * Disk Checkpoints do not maintain a key index in the same way that Memory
 * Checkpoints do as we don't expect to perform de-duplication or de-duplication
 * sanity checks. This is also necessary as we cannot let a Disk Checkpoint
 * grow memory usage (after expelling) in a O(n) manner for heavy DGM use cases
 * as we would use a lot of memory for key indexes.
 */
TEST_P(CheckpointTest, NoKeyIndexInDiskCheckpoint) {
    manager->createSnapshot(0, 1000, 1000, CheckpointType::Disk, 1000);

    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    *manager);
    ASSERT_EQ(0,
              CheckpointManagerTestIntrospector::getCheckpointNumIndexEntries(
                      checkpoint));

    // Queue an item
    auto item = makeCommittedItem(makeStoredDocKey("key"), "value", vbid);
    EXPECT_TRUE(manager->queueDirty(
            item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    // Index still empty
    EXPECT_EQ(0,
              CheckpointManagerTestIntrospector::getCheckpointNumIndexEntries(
                      checkpoint));
}

// Test that can expel items and that we have the correct behaviour when we
// register cursors for items that have been expelled.
void CheckpointTest::testExpelCheckpointItems() {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // cs + mutations
    ASSERT_EQ(itemCount + 1, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount + 1, manager->getNumItemsForCursor(*cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1001 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     */
    const size_t expectedNumItemsPre = 1 + itemCount;
    EXPECT_EQ(expectedNumItemsPre, manager->getNumOpenChkItems());
    EXPECT_EQ(expectedNumItemsPre, manager->getNumItems());

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, expelResult.count);
    EXPECT_LT(0, expelResult.memory);
    EXPECT_EQ(2, this->global_stats.itemsExpelledFromCheckpoints);

    /*
     * We have expelled:
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1)
     *
     * Now the checkpoint looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start <<<<<<< persistenceCursor
     * 1003 - 3rd item (key 2)
     */
    if (persistent()) {
        const auto pos = manager->getPersistenceCursorPos();
        EXPECT_EQ(queue_op::checkpoint_start, (*pos)->getOperation());
        EXPECT_EQ(1001, (*pos)->getBySeqno());
    }

    // 1 mutation removed from checkpoint
    const size_t expectedNumItemsPost = 1 + (itemCount - expelResult.count);
    EXPECT_EQ(expectedNumItemsPost, manager->getNumOpenChkItems());
    EXPECT_EQ(expectedNumItemsPost, manager->getNumItems());

    // Try to register a DCP replication cursor from 1001 - an expelled item.
    std::string dcp_cursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult = manager->registerCursorBySeqno(
            dcp_cursor1.c_str(), 1001, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1003, regResult.nextSeqno);
    EXPECT_TRUE(regResult.tryBackfill);

    // Try to register a DCP cursor from 1002 - another expelled item
    std::string dcp_cursor2(DCP_CURSOR_PREFIX + std::to_string(2));
    regResult = manager->registerCursorBySeqno(
            dcp_cursor2.c_str(), 1002, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1003, regResult.nextSeqno);
    EXPECT_FALSE(regResult.tryBackfill);

    // Try to register a DCP cursor from 1003 - the first item still in chk
    std::string dcp_cursor3(DCP_CURSOR_PREFIX + std::to_string(3));
    regResult = manager->registerCursorBySeqno(
            dcp_cursor3.c_str(), 1003, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1004, regResult.nextSeqno);
    EXPECT_FALSE(regResult.tryBackfill);
}

TEST_P(CheckpointTest, ExpelCheckpointItemsMemory) {
    testExpelCheckpointItems();
}

TEST_P(ReplicaCheckpointTest, ExpelCheckpointItemsDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1000);
    testExpelCheckpointItems();
}

// Test that we correctly handle duplicates, where the initial version of the
// document has been expelled.
TEST_P(CheckpointTest, ExpelCheckpointItemsWithDuplicate) {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(1 + itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(1 + itemCount, manager->getNumItemsForCursor(*cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, expelResult.count);
    EXPECT_LT(0, expelResult.memory);
    EXPECT_EQ(2, global_stats.itemsExpelledFromCheckpoints);

    // 1 mutation removed
    EXPECT_EQ(1 + (itemCount - expelResult.count),
              this->manager->getNumOpenChkItems());

    /*
     * After expelling checkpoint now looks as follows:
     * 1001 - dummy Item
     * 1001 - checkpoint_start <<<<<<< cursor
     * 1003 - 3rd item (key 2)
     */
    auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::checkpoint_start, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    auto cursor2 = manager->registerCursorBySeqno(
            "MB-62961", 1002, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1003, cursor2.nextSeqno);
    EXPECT_FALSE(cursor2.tryBackfill) << *manager;
    // cs + mutation
    EXPECT_EQ(2,
              manager->getNumItemsForCursor(*cursor2.takeCursor().lock().get()))
            << *manager;
    // Add another item which has been expelled.
    // Should not find the duplicate and so will re-add.
    EXPECT_TRUE(this->queueNewItem("key0"));

    /*
     * Checkpoint now looks as follows:
     * 1001 - dummy Item
     * 1001 - checkpoint_start <<<<<<< cursor
     * 1003 - 3rd item (key2)
     * 1004 - 4th item (key0)  << The New item added >>
     */

    // The full checkpoint still contains the cs + 2 unique items added. The
    // second add for key0 de-dupes the first for key0 so we don't bump the
    // count. This mimics normal behaviour for an item de-duping an earlier
    // one in a checkpoint when there is no expelling going on.
    EXPECT_EQ(3, this->manager->getNumOpenChkItems());
}

// Test that when the first cursor we come across is pointing to the last
// item we do not evict this item.  Instead we walk backwards find the
// first non-meta item and evict from there.
void CheckpointTest::testExpelCursorPointingToLastItem() {
    if (!persistent()) {
        // Need at least one cursor (i.e. persistence cursor) to be able
        // to expel.
        return;
    }

    const int itemCount{2};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    // cs + mutations
    ASSERT_EQ(itemCount + 1, manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount + 1, manager->getNumItemsForCursor(*cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount + 1; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1001 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item
     * 1002 - 2nd item  <<<<<<< persistenceCursor
     */

    // Only expel seqno 1001 - the cursor points to item that
    // has the highest seqno for the checkpoint so we move the expel point back
    // one. That item isn't a metadata item nor is it's successor item
    // (1002) the same seqno as itself (1001) so can expel from there.
    const auto expelResult = manager->expelUnreferencedCheckpointItems();

    /*
     * Checkpoint now looks as follows:
     * 1001 - dummy item
     * 1001 - checkpoint start <<<<<<< persistenceCursor
     */

    EXPECT_EQ(2, expelResult.count);
    EXPECT_GT(expelResult.memory, 0);
    EXPECT_EQ(2, global_stats.itemsExpelledFromCheckpoints);

    const auto pos = manager->getPersistenceCursorPos();
    EXPECT_EQ(queue_op::checkpoint_start, (*pos)->getOperation());
    EXPECT_EQ(1001, (*pos)->getBySeqno());
}

TEST_P(CheckpointTest, ExpelCursorPointingToLastItemMemory) {
    testExpelCursorPointingToLastItem();
}

TEST_P(ReplicaCheckpointTest, ExpelCursorPointingToLastItemDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1000);
    testExpelCursorPointingToLastItem();
}

// Test that when the first cursor we come across is pointing to the checkpoint
// start we do not evict this item.  Instead we walk backwards and find the
// the dummy item, so do not expel any items.
void CheckpointTest::testExpelCursorPointingToChkptStart() {
    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.

    bool isLastMutationItem{true};
    auto item = this->manager->nextItem(
            this->manager->getPersistenceCursor(), isLastMutationItem);

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start  <<<<<<< persistenceCursor
     */

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.count);
    EXPECT_EQ(0, expelResult.memory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

TEST_P(CheckpointTest, ExpelCursorPointingToChkptStartMemory) {
    testExpelCursorPointingToChkptStart();
}

TEST_P(ReplicaCheckpointTest, ExpelCursorPointingToChkptStartDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1000);
    testExpelCursorPointingToChkptStart();
}

// Test that if we want to evict items from seqno X, but have a meta-data item
// also with seqno X, and a cursor is pointing to this meta data item, we do not
// evict.
void CheckpointTest::testDontExpelIfCursorAtMetadataItemWithSameSeqno() {
    const int itemCount{2};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Move the persistence cursor to the end to get it of the way.
    bool isLastMutationItem{true};
    for (auto ii = 0; ii < 3; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    // Add a cursor pointing to the dummy
    std::string dcpCursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult = manager->registerCursorBySeqno(
            dcpCursor1.c_str(), 1000, CheckpointCursor::Droppable::Yes);

    // Move the cursor forward one step so that it now points to the checkpoint
    // start.
    auto item = manager->nextItem(regResult.takeCursor().lock().get(),
                                  isLastMutationItem);

    // Add a cursor to point to the 1st mutation we added.  Note that when
    // registering the cursor we walk backwards from the checkpoint end until we
    // reach the item with the seqno we are requesting.  Hence we register the
    // cursor at the mutation and not the metadata item (checkpoint start) which
    // has the same seqno.
    std::string dcpCursor2(DCP_CURSOR_PREFIX + std::to_string(2));
    CursorRegResult regResult2 = manager->registerCursorBySeqno(
            dcpCursor2.c_str(), 1001, CheckpointCursor::Droppable::Yes);

    /*
     * Checkpoint now looks as follows:
     * 1001 - dummy item
     * 1001 - checkpoint start  <<<<<<< dcpCursor1
     * 1001 - 1st item  <<<<<<< dcpCursor2
     * 1002 - 2nd item  <<<<<<< persistenceCursor
     */

    // We should not expel any items due to dcpCursor1
    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.count);
    EXPECT_EQ(0, expelResult.memory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

TEST_P(CheckpointTest, testDontExpelIfCursorAtMetadataItemWithSameSeqnoMemory) {
    testDontExpelIfCursorAtMetadataItemWithSameSeqno();
}

TEST_P(CheckpointTest, testDontExpelIfCursorAtMetadataItemWithSameSeqnoDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1000);
    testDontExpelIfCursorAtMetadataItemWithSameSeqno();
}

// Test estimate for the amount of memory recovered by expelling is correct.
void CheckpointTest::testExpelCheckpointItemsMemoryRecovered() {
    const int itemCount{3};
    size_t sizeOfItem{0};

    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    const auto initialCMUsage = manager->getMemUsage();
    const auto initialCMQueuedItemsUsage = manager->getQueuedItemsMemUsage();

    for (auto ii = 0; ii < itemCount; ++ii) {
        std::string value("value");
        queued_item item(new Item(makeStoredDocKey("key" + std::to_string(ii)),
                                  0,
                                  0,
                                  value.c_str(),
                                  value.size(),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  0,
                                  -1,
                                  Vbid(0)));

        sizeOfItem = item->size();

        // Add the queued_item to the checkpoint
        manager->queueDirty(item,
                            GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            /*preLinkDocCtx*/ nullptr);
    }

    // cs + mutations
    ASSERT_EQ(itemCount + 1, manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount + 1, manager->getNumItemsForCursor(*cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < 3; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     *
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1) <<<<<<< Cursor
     * 1003 - 3rd item (key2)
     */

    // Get the memory usage before expelling
    const auto memUsageBeforeExpel = manager->getMemUsage();

    auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, expelResult.count);
    EXPECT_EQ(2, global_stats.itemsExpelledFromCheckpoints);

    /*
     * We have expelled:
     * 1001 - 1st item (key 0)
     * 1002 - 2nd item (key1)
     *
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start <<<<<<< Cursor
     * 1003 - 3rd item (key 2)
     */
    if (persistent()) {
        const auto pos = manager->getPersistenceCursorPos();
        EXPECT_EQ(queue_op::checkpoint_start, (*pos)->getOperation());
        EXPECT_EQ(1001, (*pos)->getBySeqno());
    }

    const size_t expectedMemoryRecovered =
            expelResult.count *
            (Checkpoint::per_item_queue_overhead + sizeOfItem);

    EXPECT_EQ(expectedMemoryRecovered, expelResult.memory);
    EXPECT_EQ(expectedMemoryRecovered,
              memUsageBeforeExpel - manager->getMemUsage());

    // Now verify the behaviour when Expel releases all the items in checkpoint
    manager->nextItem(cursor, isLastMutationItem);
    ASSERT_TRUE(isLastMutationItem);
    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start
     * 1003 - 3rd item (key 2) <<<<<<< Cursor
     */
    const auto id = manager->getOpenCheckpointId();
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumOpenChkItems());

    expelResult = manager->expelUnreferencedCheckpointItems();
    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start <<<<<<< Cursor
     */
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    EXPECT_EQ(1, expelResult.count);
    EXPECT_EQ(3, global_stats.itemsExpelledFromCheckpoints);

    // We expelled all the items, so we expect queue-usage back to initial val
    EXPECT_EQ(initialCMQueuedItemsUsage, manager->getQueuedItemsMemUsage());
    // Plus, CM total still accounts for the keys in the index - keys inserted
    // in the index when items queued, but not removed by Expel
    EXPECT_EQ(initialCMUsage + manager->getMemOverheadIndex(),
              manager->getMemUsage());

    // Verify that releasing the full checkpoint reverts CM's usage back to the
    // initial usage - Checkpoint removal release the keyIndex too
    manager->createNewCheckpoint();
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_GT(manager->getOpenCheckpointId(), id);
    EXPECT_EQ(initialCMUsage, manager->getMemUsage());
}

TEST_P(CheckpointTest, ExpelCheckpointItemsMemoryRecoveredMemory) {
    testExpelCheckpointItemsMemoryRecovered();
}

TEST_P(ReplicaCheckpointTest, ExpelCheckpointItemsMemoryRecoveredDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1000);
    testExpelCheckpointItemsMemoryRecovered();
}

TEST_P(CheckpointTest, InitialSnapshotDoesDoubleRefCheckpoint) {
    // Test to ensure that receiving an initial snapshot while
    // already holding cursors (in addition to the persistence cursor)
    // does not lead to a second increment of the checkpoint num cursors

    createManager(0);

    auto& cm = *this->manager;
    const auto& checkpointList = cm.getCheckpointList();

    ASSERT_EQ(1, checkpointList.size());
    ASSERT_EQ(1, checkpointList.front()->getNumCursorsInCheckpoint());
    auto cursor =
            cm.registerCursorBySeqno(
                      "test_cursor_name", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor();
    EXPECT_EQ(2, checkpointList.front()->getNumCursorsInCheckpoint());

    // first snapshot received
    cm.createSnapshot(1, 10, {/* hcs */}, CheckpointType::Memory, 10);
    EXPECT_EQ(2, checkpointList.size());
    // Ensure the number of cursors is still correct
    EXPECT_EQ(2, checkpointList.front()->getNumCursorsInCheckpoint());
}

TEST_P(CheckpointTest, MetaItemsSeqnoWeaklyMonotonicSetVbStateBeforeEnd) {
    createManager(0);
    auto& cm = *this->manager;

    // Queue a normal set
    queued_item qi(new Item(makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0));
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    // Queue a setVBucketState
    cm.queueSetVBState();

    // Close our checkpoint to create a checkpoint_end, another dummy item, and
    // a checkpoint_start
    cm.createNewCheckpoint();

    // Test: Iterate on all items and check that the seqnos are weakly monotonic
    auto regRes = cm.registerCursorBySeqno(
            "Cursor", 0, CheckpointCursor::Droppable::Yes);
    auto cursor = regRes.takeCursor().lock();
    std::vector<queued_item> items;
    cm.getNextItemsForDcp(*cursor, items);

    WeaklyMonotonic<uint64_t, ThrowExceptionPolicy> seqno(0);
    for (const auto& item : items) {
        seqno = static_cast<uint64_t>(item->getBySeqno());
    }
}

TEST_P(CheckpointTest, MetaItemsSeqnoWeaklyMonotonicSetVbStateAfterStart) {
    createManager(0);
    auto& cm = *this->manager;

    // Queue a setVBucketState
    cm.queueSetVBState();

    // Queue a normal set
    queued_item qi(new Item(makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0));
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    // Close our checkpoint to create a checkpoint_end, another dummy item, and
    // a checkpoint_start
    cm.createNewCheckpoint();

    // Test: Iterate on all items and check that the seqnos are weakly monotonic
    auto regRes = cm.registerCursorBySeqno(
            "Cursor", 0, CheckpointCursor::Droppable::Yes);
    auto cursor = regRes.takeCursor().lock();
    std::vector<queued_item> items;
    cm.getNextItemsForDcp(*cursor, items);

    WeaklyMonotonic<uint64_t, ThrowExceptionPolicy> seqno{0};
    for (const auto& item : items) {
        seqno = static_cast<uint64_t>(item->getBySeqno());
    }
}

TEST_P(CheckpointTest, CursorPlacedAtCkptStartSeqnoCorrectly) {
    createManager(0);
    auto& cm = *this->manager;

    // Queue a normal set
    queued_item qi(new Item(makeStoredDocKey("key1"),
                            this->vbucket->getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0));
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    // Close our checkpoint to create a checkpoint_end, another dummy item, and
    // a checkpoint_start
    cm.createNewCheckpoint();

    // Queue a normal set
    qi = queued_item(new Item(makeStoredDocKey("key1"),
                              this->vbucket->getId(),
                              queue_op::mutation,
                              /*revSeq*/ 0,
                              /*bySeq*/ 0));
    EXPECT_TRUE(manager->queueDirty(qi,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    // Try to register a cursor at seqno 2 (i.e. the first item in the second
    // checkpoint).
    auto regRes = cm.registerCursorBySeqno(
            "Cursor", 2, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(2, (*regRes.takeCursor().lock()->getCheckpoint())->getId());
}

TEST_P(CheckpointTest,
       GetItemsForPersistenceCursor_ThrowIfBackupCursorAlreadyExists) {
    if (!persistent()) {
        return;
    }

    std::vector<queued_item> items;
    auto res = manager->getNextItemsForPersistence(items);

    try {
        manager->getNextItemsForPersistence(items);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(
                std::string(e.what()).find("Backup cursor already exists") !=
                std::string::npos);
        return;
    }
    FAIL();
}

CheckpointManager::ItemsForCursor
CheckpointTest::testGetItemsForPersistenceCursor() {
    if (!persistent()) {
        return {};
    }

    // Pre-condition: the test-cursor is at the begin of the single open
    // checkpoint
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(1, manager->getNumOfCursors());
    const auto initialPos =
            *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::empty, initialPos->getOperation());

    // chk_start
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    // Enqueue 1 item
    EXPECT_TRUE(queueNewItem("keyA"));
    EXPECT_EQ(2, manager->getNumOpenChkItems());

    // Checkpoint shape now is (P/C stand for pCursor/pCursorCopy):
    // [E    CS    M)
    //  ^
    //  P

    // Pull it out from the CM without moving the cursor
    std::vector<queued_item> items;
    auto res = manager->getNextItemsForPersistence(items);

    // Check that we get all the expected
    EXPECT_TRUE(res.flushHandle);
    EXPECT_FALSE(res.moreAvailable);
    EXPECT_EQ(1, res.ranges.size());
    EXPECT_EQ(1000, res.ranges[0].getStart());
    EXPECT_EQ(1001, res.ranges[0].getEnd());
    EXPECT_EQ(2, items.size()); // checkpoint_start + 1 item
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1001, items.at(1)->getBySeqno());
    EXPECT_EQ(1001, res.visibleSeqno);
    EXPECT_FALSE(res.highCompletedSeqno);

    // This is the expected CM state
    // [E    CS    M:1)
    //  ^          ^
    //  B          P

    // Check that the pCursor has moved
    auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    // Check that we have created the backup persistence cursor.
    // The peristence cursor will be reset to that copy if necessary (ie,
    // KVStore::commit failure).
    EXPECT_EQ(2, manager->getNumOfCursors());
    const auto backupPCursor = manager->getBackupPersistenceCursor();
    pos = *CheckpointCursorIntrospector::getCurrentPos(*backupPCursor);
    EXPECT_EQ(initialPos, pos);
    EXPECT_EQ(queue_op::empty, pos->getOperation());

    return res;
}

TEST_P(CheckpointTest, GetItemsForPersistenceCursor_FlushSuccessScenario) {
    if (!persistent()) {
        return;
    }

    // This step tests preconditions and leaves the CM as:
    // [E    CS    M:1)
    //             ^
    //             P
    testGetItemsForPersistenceCursor();

    // Note: The previous step simulates the KVStore::commit successful path at
    // persistence
    ASSERT_EQ(1, manager->getNumOfCursors());
    ASSERT_TRUE(manager->getPersistenceCursor());
    const auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    ASSERT_EQ(queue_op::mutation, pos->getOperation());
    ASSERT_EQ(1001, pos->getBySeqno());

    // Now try to pull items out again and expect no items for pcursor as
    // the flush has succeded
    std::vector<queued_item> items;
    const auto res = manager->getNextItemsForPersistence(items);
    EXPECT_FALSE(res.moreAvailable);
    ASSERT_EQ(0, res.ranges.size());
    ASSERT_EQ(0, items.size());
}

TEST_P(CheckpointTest, GetItemsForPersistenceCursor_FlushFailureScenario) {
    if (!persistent()) {
        return;
    }

    // This step tests preconditions and leaves the CM as:
    // [E    CS    M:1)
    //  ^          ^
    //  B          P
    auto res = testGetItemsForPersistenceCursor();

    // This step simulates the KVStore::commit failure path at persistence
    ASSERT_TRUE(res.flushHandle);
    res.flushHandle->markFlushFailed(*vbucket);
    res.flushHandle.reset();

    // The previous step re-initializes pcursor, need to reset the test cursor
    ASSERT_EQ(1, manager->getNumOfCursors());
    cursor = manager->getPersistenceCursor();
    ASSERT_TRUE(cursor);

    // pcursor must be reset at the expected position
    const auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    ASSERT_EQ(queue_op::empty, pos->getOperation());

    // [E    CS    M:1)
    //  ^
    //  P

    // Now try to pull items out again and expect to retrieve all the items
    // + snap-range info.
    std::vector<queued_item> items;
    res = manager->getNextItemsForPersistence(items);

    ASSERT_TRUE(res.flushHandle);
    EXPECT_FALSE(res.moreAvailable);
    ASSERT_EQ(1, res.ranges.size());
    EXPECT_EQ(1000, res.ranges[0].getStart());
    EXPECT_EQ(1001, res.ranges[0].getEnd());
    ASSERT_EQ(2, items.size()); // checkpoint_start + 1 item
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1001, items.at(1)->getBySeqno());
    EXPECT_EQ(1001, res.visibleSeqno);
    EXPECT_FALSE(res.highCompletedSeqno);
}

TEST_P(CheckpointTest, NeverDropBackupPCursor) {
    if (!persistent()) {
        return;
    }

    // This step tests preconditions and leaves the CM as:
    // [E    CS    M:1)
    //             ^
    //             P
    testGetItemsForPersistenceCursor();

    // Now I want to get to:
    // [E:1    CS:1    M:1    M:2    CE:3]    [E:3    CS:3)
    //                 ^                              ^
    //                 B                              P

    // Step necessary to avoid pcursor to be moved to the new checkpoint at
    // CM::createNewCheckpoint below. This would invalidate the test as we need
    // the backupPCursor in a closed checkppoint.
    // @todo: we can remove this step when MB-37846 is resolved
    ASSERT_EQ(2, manager->getNumOpenChkItems());
    ASSERT_TRUE(queueNewItem("another-key"));
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(3, manager->getNumOpenChkItems());

    manager->createNewCheckpoint();
    ASSERT_EQ(2, manager->getNumCheckpoints());

    // Create backup-pcursor (and move pcursor)
    std::vector<queued_item> items;
    const auto res = manager->getNextItemsForPersistence(items);

    ASSERT_EQ(3, items.size());
    ASSERT_EQ(queue_op::mutation, items.at(0)->getOperation());
    ASSERT_EQ(1002, items.at(0)->getBySeqno());
    ASSERT_EQ(queue_op::checkpoint_end, items.at(1)->getOperation());
    ASSERT_EQ(1003, items.at(1)->getBySeqno());
    ASSERT_EQ(queue_op::checkpoint_start, items.at(2)->getOperation());
    ASSERT_EQ(1003, items.at(2)->getBySeqno());

    // Check expected position for pcursor
    const auto pPos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    ASSERT_EQ(queue_op::checkpoint_start, pPos->getOperation());
    ASSERT_EQ(1003, pPos->getBySeqno());

    // Check expected position for backup-pcursor.
    const auto bPos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager->getBackupPersistenceCursor());
    ASSERT_EQ(queue_op::mutation, bPos->getOperation());
    ASSERT_EQ(1001, bPos->getBySeqno());

    // We never drop pcursor and backup-pcursor.
    // Note: backup-pcursor is in a closed checkpoint, so it would be eligible
    //   for dropping if treated as a DCP cursor
    EXPECT_EQ(0, manager->getListOfCursorsToDrop().size());
}

/**
 * Test that the backup persistence cursor is correctly handled at deduplication
 * when it points to the item being dedup'ed.
 */
TEST_P(CheckpointTest,
       GetItemsForPersistenceCursor_FlushFailureScenario_Deduplication) {
    if (!persistent()) {
        return;
    }

    // Queue items
    // [E    CS    M(keyA):1)
    //  ^
    //  P

    // Flush - getItems
    // [E    CS    M(keyA):1)
    //  ^          ^
    //  B          P

    // Flush - success
    // [E    CS    M(keyA):1)
    //             ^
    //             P
    testGetItemsForPersistenceCursor();

    // Queue items
    // [E    CS    M(keyA):1    M(keyB):2)
    //             ^
    //             P
    ASSERT_TRUE(queueNewItem("keyB"));
    ASSERT_EQ(3, manager->getNumOpenChkItems());

    // Flush - getItems
    // [E    CS    M(keyA):1    M(keyB):2)
    //             ^            ^
    //             B            P
    std::vector<queued_item> items;
    auto res = manager->getNextItemsForPersistence(items);
    ASSERT_EQ(1, items.size());
    EXPECT_EQ(queue_op::mutation, items.at(0)->getOperation());
    EXPECT_EQ(1002, items.at(0)->getBySeqno());

    EXPECT_EQ(2, manager->getNumOfCursors());
    // Check pcursor
    auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1002, pos->getBySeqno());
    // Check backup pcursor
    pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager->getBackupPersistenceCursor());
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    // Queue items (it can happen in the middle of the flush as CM is unlocked)
    // [E    CS    x    M(keyB):2    M(keyA):3)
    //       ^          ^
    //       B          P
    ASSERT_TRUE(queueNewItem("keyA"));
    ASSERT_EQ(3, manager->getNumOpenChkItems());

    // pcursor has not moved
    pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1002, pos->getBySeqno());
    // backup cursor has moved backward
    pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager->getBackupPersistenceCursor());
    EXPECT_EQ(queue_op::checkpoint_start, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    // Flush - failure
    // [E    CS    x    M(keyB):2    M(keyA):3)
    //       ^
    //       P
    // This step simulates the KVStore::commit failure path at persistence
    res.flushHandle->markFlushFailed(*vbucket);
    res.flushHandle.reset();

    // The previous step re-initializes pcursor, need to reset the test cursor
    ASSERT_EQ(1, manager->getNumOfCursors());
    cursor = manager->getPersistenceCursor();
    ASSERT_TRUE(cursor);

    // backup cursor has been released
    ASSERT_FALSE(manager->getBackupPersistenceCursor());
    // pcursor reset to the expected position
    pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::checkpoint_start, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    // Re-attempt Flush - getItems
    // [E    CS    x    M(keyB):2    M(keyA):3)
    //                               ^
    //                               P
    items.clear();
    manager->getNextItemsForPersistence(items);
    ASSERT_EQ(2, items.size());
    EXPECT_EQ(queue_op::mutation, items.at(0)->getOperation());
    EXPECT_EQ(1002, items.at(0)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1003, items.at(1)->getBySeqno());

    // Flush - success
    // no backup cursor around
    ASSERT_FALSE(manager->getBackupPersistenceCursor());
    // pcursor at the expected position
    pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1003, pos->getBySeqno());
}

/*
 * This test replaces the old tests for MB-41283.
 * Since the fix for MB-42780, code in those test was simulating an invalid
 * scenario that is now prevented by strict assertion in the CheckpointManager.
 * Specifically, the CM now fails if the user tries to extend a Disk Checkpoint.
 */
TEST_P(CheckpointTest, CheckpointManagerForbidsMergingDiskSnapshot) {
    vbucket->setState(vbucket_state_replica);

    // Positive check first: extending Memory checkpoints is allowed
    manager->createSnapshot(1000, 2000, 0, CheckpointType::Memory, 2000);
    EXPECT_TRUE(queueReplicatedItem("keyA", 1001));
    manager->extendOpenCheckpoint(2001, 3000);

    // Negative check
    manager->createSnapshot(3001, 4000, 0, CheckpointType::Disk, 4000);
    try {
        manager->extendOpenCheckpoint(4001, 5000);
    } catch (const std::logic_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr("Cannot extend a Disk checkpoint"));
        return;
    }
    FAIL();
}

TEST_P(CheckpointTest, CheckpointItemToString) {
    auto item = manager->public_createCheckpointMetaItem(0, queue_op::empty);
    EXPECT_EQ("cid:0x1:empty", item->getKey().to_string());

    item = manager->public_createCheckpointMetaItem(0,
                                                    queue_op::checkpoint_start);
    EXPECT_EQ("cid:0x1:checkpoint_start", item->getKey().to_string());

    item = manager->public_createCheckpointMetaItem(
            0, queue_op::set_vbucket_state);
    EXPECT_EQ("cid:0x1:set_vbucket_state", item->getKey().to_string());

    item = manager->public_createCheckpointMetaItem(0,
                                                    queue_op::checkpoint_end);
    EXPECT_EQ("cid:0x1:checkpoint_end", item->getKey().to_string());

    auto disk = makeDiskDocKey("test_key");
    EXPECT_EQ("cid:0x0:test_key", disk.to_string());

    disk = makeDiskDocKey("test_key", true, CollectionID(99));
    EXPECT_EQ("pre:cid:0x63:test_key", disk.to_string());

    auto event =
            SystemEventFactory::makeCollectionEvent(CollectionID(99), {}, {});
    EXPECT_EQ("cid:0x1:0x0:0x63:_collection", event->getKey().to_string());
    event = SystemEventFactory::makeModifyCollectionEvent(
            CollectionID(999), {}, {});
    EXPECT_EQ("cid:0x1:0x2:0x3e7:_collection", event->getKey().to_string());
    event = SystemEventFactory::makeScopeEvent(ScopeID(99), {}, {});
    EXPECT_EQ("cid:0x1:0x1:0x63:_scope", event->getKey().to_string());
}

// Test class for closed/unref checkpoint removal
class CheckpointRemovalTest : public CheckpointTest {};

MATCHER(CheckpointMatcher, "") {
    // arg expected to be a Checkpoint
    const auto& [ckpt, expected] = arg;
    const auto& actualKeys =
            CheckpointManagerTestIntrospector::getNonMetaItemKeys(*ckpt);

    auto res = expected == actualKeys;
    if (!res) {
        *result_listener << "actual keys: ";
        for (const auto& key : actualKeys) {
            *result_listener << "\"" << key << "\""
                             << ", ";
        }
    }
    return res;
}
MATCHER_P(CheckpointMatcher, expected, "") {
    // arg expected to be a Checkpoint
    std::vector<std::string> actualKeys =
            CheckpointManagerTestIntrospector::getNonMetaItemKeys(arg);

    return expected == actualKeys;
}

TEST_P(CheckpointRemovalTest, CursorMovement) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor));

    using namespace testing;

    const auto openId = manager->getOpenCheckpointId();

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // remains in the old checkpoint, so it is still reffed - no checkpoint
        // removed
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());
        EXPECT_EQ(5, manager->getNumItemsForCursor(*cursor));
    }

    {
        // Advance cursor, moving it out of the closed ckpt.
        // checkpoint should be removed, as it is now unreffed.
        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(
                    *cursor, items, 1000, std::numeric_limits<size_t>::max());
        }
        EXPECT_EQ(0, manager->getNumItemsForCursor(*cursor));
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());
    }
}

TEST_P(CheckpointRemovalTest, NewClosedCheckpointMovesCursor) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor));

    const auto openId = manager->getOpenCheckpointId();

    {
        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(
                    *cursor, items, 1000, std::numeric_limits<size_t>::max());
        }
        EXPECT_EQ(3, manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId, manager->getOpenCheckpointId());
        EXPECT_EQ(0, manager->getNumItemsForCursor(*cursor));
    }

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // is advanced implicitly to the new checkpoint - closed checkpoint
        // should be removed
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_GT(manager->getOpenCheckpointId(), openId);
        EXPECT_EQ(1, manager->getNumItemsForCursor(*cursor));
        EXPECT_EQ(queue_op::empty, (*cursor->getPos())->getOperation());
    }
}

TEST_P(CheckpointRemovalTest, NewUnreffedClosedCheckpoint) {
    // remove the cursor now, so the newly closed checkpoint will be immediately
    // unreffed
    manager->removeCursor(*cursor);

    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    const auto openId = manager->getOpenCheckpointId();

    {
        // Create a new checkpoint, closing the current open one and removing
        // the old checkpoint as there are no cursors in the closed checkpoint
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        // just the open checkpoint left, closed was removed immediately
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());
    }
}

TEST_P(CheckpointRemovalTest, OnlyOldestCkptIsRemoved) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    const auto openId = manager->getOpenCheckpointId();

    {
        // Create a new checkpoint, closing the current open one.
        // Closed checkpoint not removed, cursor is present.
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());
    }

    // queue another item
    EXPECT_TRUE(this->queueNewItem("key3"));
    EXPECT_EQ(2, manager->getNumOpenChkItems());
    EXPECT_EQ(2, this->manager->getNumCheckpoints());

    {
        // Create a new checkpoint, closing the current open one.
        // The "middle" checkpoint has no cursors, but is not the oldest
        // checkpoint, so cannot trigger checkpoint removal
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(3, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 2, manager->getOpenCheckpointId());
    }

    {
        // Advance cursor into "middle" checkpoint. Oldest checkpoint can now
        // be removed, should trigger callback.
        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(
                    *cursor, items, 2, std::numeric_limits<size_t>::max());
        }
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 2, manager->getOpenCheckpointId());
        EXPECT_EQ(4, manager->getNumItemsForCursor(*cursor));
    }
}

TEST_P(CheckpointRemovalTest, RemoveCursorTriggersCkptRemoval) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(3, manager->getNumItemsForCursor(*cursor));

    const auto openId = manager->getOpenCheckpointId();

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // remains in the old checkpoint, so it is still reffed - closed ckpt
        // not removed
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 1, manager->getOpenCheckpointId());
        EXPECT_EQ(5, manager->getNumItemsForCursor(*cursor));
    }

    {
        // Queue an item and create one more checkpoint. Still can't be removed,
        // as the cursor still exists.
        EXPECT_TRUE(this->queueNewItem("key3"));
        this->manager->createNewCheckpoint();
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(3, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 2, manager->getOpenCheckpointId());
        // cs, mut, mut, ce, cs, mut, ce, cs
        EXPECT_EQ(8, manager->getNumItemsForCursor(*cursor));
    }

    {
        // Drop the cursor. 2 closed checkpoints have to be removed, as _both_
        // are now eligible for removal - the cursor was removed from the oldest
        // leaving it unreffed, and the "middle" checkpoint doesn't have any
        // cursors either.
        manager->removeCursor(*cursor);
        EXPECT_EQ(1, manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(openId + 2, manager->getOpenCheckpointId());
    }
}

// This test drives the checkpoint manager with the events of MB-47516. Trying
// to force the bug from higher level constructs is not possible in a single
// threaded test without a hook to inject expel at the right moment.
// The MB itself saw a pending vbucket receive a disk-snapshot and then a DCP
// takeover switches the VB to active. A set-vbstate which occurs when the take
// over stream is accepted places a set-vbstate meta-item as the last item in
// the open/disk checkpoint. Next as the takeover stream runs
// "KVBucket::setVbucketState" expel triggers in between two checkpoint manager
// calls leaving the closed checkpoint in a bad state.
TEST_P(ReplicaCheckpointTest, MB_47516) {
    // running for persistence only is a simplification of the test so we can
    // just use the persistence cursor to drive the issue
    if (!persistent()) {
        return;
    }

    // mimic the MB, note disk/memory doesn't really matter, or the range
    // of the snapshot - the issue is that a combination of renumbering the
    // vbstate item + expel allows registerCursor to operate incorrectly

    // 1) Receive a snapshot, two items is plenty for the test
    this->manager->createSnapshot(
            1001, 1002, 1002, CheckpointType::Disk, 1002);
    ASSERT_TRUE(this->queueReplicatedItem("k1001", 1001)); // 1001

    // 1.1) persist these, cursor now past k1001 and we can expel up to that
    // (included)
    std::vector<queued_item> items;
    manager->getNextItemsForPersistence(items);
    // we get the cp start and one mutation
    EXPECT_EQ(2, items.size());

    // 1.2) Add another mutation
    ASSERT_TRUE(this->queueReplicatedItem("k1002", 1002)); // 1002

    // 2) A set-vbstate needs to occur - this happens when a takeover stream is
    //    accepted and queues the new vbstate.
    manager->queueSetVBState();

    // 3) ... in older branches we would explicitly call setOpenCheckpointId.
    //    Which is what the vbstate change also did. In mad-hatter it was that
    //    function that 'damaged' the checkpoint, in cheshire-cat that function
    //    was fixed, but now that function no longer exists at all, but continue
    //    the test.

    // 4) Expel occurs in the middle of the state switch - between
    //    queueSetVBState and createNewCheckpoint. This is the second
    //    part of the MB that left the checkpoint in a bad state. registerCursor
    //    from this point can return the incorrect seqno
    auto expel = this->manager->expelUnreferencedCheckpointItems();
    // Only 1 mutation gets expelled
    EXPECT_EQ(1, expel.count);

    // Note in this test we don't need to call createNewCheckpoint, the damage
    // was done without.

    // Item 1001 is expelled - we should not get a cursor for it. So
    // ask for all data, we should be told to try backfill and be given a cursor
    // for the high-seqno which is still in the cp-manager
    auto cursor = manager->registerCursorBySeqno(
            "MB_47516", 0, CheckpointCursor::Droppable::Yes);
    EXPECT_TRUE(cursor.tryBackfill);
    EXPECT_EQ(1002, cursor.nextSeqno);
}

// In the case of open/closed checkpoints cursors placed at the high-seqno
// should not reference the closed checkpoint.
TEST_P(ReplicaCheckpointTest, MB_47551) {
    // 1) Receive a snapshot, two items is plenty for the test
    manager->createSnapshot(1001, 1002, 1002, CheckpointType::Disk, 1002);
    ASSERT_TRUE(queueReplicatedItem("k1001", 1001)); // 1001
    ASSERT_TRUE(queueReplicatedItem("k1002", 1002)); // 1002

    // No as if vb-state changed, new checkpoint
    manager->createNewCheckpoint();

    // 0, mid-way and high-seqno-1 request - expect the closed CP, data is
    // available
    for (uint64_t seqno : {0, 500, 1001}) {
        auto cursor = manager->registerCursorBySeqno(
                "MB-47551", seqno, CheckpointCursor::Droppable::Yes);
        if (seqno == 1001) {
            EXPECT_FALSE(cursor.tryBackfill) << seqno;
            EXPECT_EQ(1002, cursor.nextSeqno) << seqno;
        } else {
            EXPECT_TRUE(cursor.tryBackfill) << seqno;
            EXPECT_EQ(1001, cursor.nextSeqno) << seqno;
        }

        // Cursor should be in the closed checkpoint, it has the items we need
        EXPECT_EQ(2, (*cursor.takeCursor().lock()->getCheckpoint())->getId());
    }

    // But high-seqno should use the open CP
    auto cursor2 = manager->registerCursorBySeqno(
            "cursor2", 1002, CheckpointCursor::Droppable::Yes);

    // And we expect to be in the open checkpoint, so we don't hold the closed
    // one. Also, we don't need a backfill as we are simulating a DCP client
    // that has already got 1002 and the next seqno 1003 is in checkpoint.
    EXPECT_FALSE(cursor2.tryBackfill);
    EXPECT_EQ(1003, cursor2.nextSeqno);
    EXPECT_EQ(3, (*cursor2.takeCursor().lock()->getCheckpoint())->getId());
}

// This test exists to cover the case where cursor re-registration moves the
// cursor for a new checkpoint and importantly when eager removal is in play.
// Before ~Checkpoint had an Expects(getNumCursorsInCheckpoint() == 0); this
// test would lead to a use-after-free issue which no other unit test hit.
TEST_P(CheckpointTest, reRegisterCheckpointCursor) {
    // Add 4 item using CDC functionality - they will create new checkpoints
    // Note: seqno:1003 (k2) is added as MB-58302 breaks the original neo test
    // and this extra seqno helps demonstrate the fix in checkpoint_manager
    // is working. E.g. this test without the fix and it fails because a
    // tryBackfill is true (because eager removal discarded the CP we wanted)
    ASSERT_TRUE(this->queueNewCDCItem("k1")); // seqno:1001
    ASSERT_TRUE(this->queueNewCDCItem("k1")); // seqno:1002
    ASSERT_TRUE(this->queueNewCDCItem("k2")); // seqno:1003
    ASSERT_TRUE(this->queueNewCDCItem("k1")); // seqno:1004
    // But high-seqno should use the open CP
    auto cursor = manager->registerCursorBySeqno(
            "cursor", 1001, CheckpointCursor::Droppable::Yes);
    if (persistent()) {
        // Important progress the persistence cursor to ensure other checkpoints
        // are eligible for eager removal
        std::vector<queued_item> items;
        manager->getItemsForCursor(*manager->getPersistenceCursor(),
                                   items,
                                   std::numeric_limits<size_t>::max(),
                                   std::numeric_limits<size_t>::max());
    }
    // Note: A version of the code for MB-56565 triggers a new Expects when
    // registering the cursor at the "higher" seqno (+ eager removal)
    cursor = manager->registerCursorBySeqno(
            "cursor", 1002, CheckpointCursor::Droppable::Yes);
    EXPECT_FALSE(cursor.tryBackfill);
    EXPECT_EQ(1003, cursor.nextSeqno);
    EXPECT_EQ(2, (*cursor.takeCursor().lock()->getCheckpoint())->getId());
}

/**
 * The test verifies that we handle correctly the possible CheckpointRemoval
 * that might be triggered when the user attempts to register a cursor that
 * already exists in CM with the same name. Examples of that are:
 * - General external DCP clients
 * - Internal outbound streams when they jump memory<->backfill and re-register
 *   their cursor as part of that.
 */
TEST_P(CheckpointTest, RegisterDuplicateCursor_CheckpointRemoval) {
    // Just keep the test simple, 1 mutation -> 1 checkpoint
    config.setCheckpointMaxSize(1);
    checkpoint_config = std::make_unique<CheckpointConfig>(config);
    createManager();
    ASSERT_EQ(1, manager->getCheckpointConfig().getCheckpointMaxSize());

    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_EQ(1, manager->getNumItems()); // cs

    ASSERT_EQ(1000, manager->getHighSeqno());

    const std::string value = "value";
    for (size_t i = 0; i < 2; ++i) {
        queued_item item(new Item(makeStoredDocKey("key" + std::to_string(i)),
                                  0,
                                  0,
                                  value.c_str(),
                                  value.size(),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  0,
                                  -1,
                                  Vbid(0)));
        manager->queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr);
    }
    ASSERT_EQ(2, manager->getNumCheckpoints());
    ASSERT_EQ(5, manager->getNumItems()); // cs, m:1, ce, cs, m:2
    ASSERT_EQ(2, manager->getNumOpenChkItems());
    ASSERT_EQ(1002, manager->getHighSeqno());

    // Register the test cursor into the first checkpoint
    ASSERT_EQ(1, manager->getNumOfCursors());
    const std::string cursorName = "dcp-cursor";
    auto dcpCursor =
            manager->registerCursorBySeqno(
                           cursorName, 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock()
                    .get();
    ASSERT_TRUE(dcpCursor);
    ASSERT_EQ(2, manager->getNumOfCursors());
    const auto& ckptList = manager->getCheckpointList();
    ASSERT_EQ(ckptList.front(), *dcpCursor->getCheckpoint());
    ASSERT_EQ(CHECKPOINT_CLOSED, (*dcpCursor->getCheckpoint())->getState());

    const auto initialMemUsage = manager->getMemUsage();
    const auto firstCheckpointMemUsage = ckptList.front()->getMemUsage();

    // Move the baseline cursor so that it doesn't prevent checkpoint removal in
    // the next steps.
    {
        std::vector<queued_item> items;
        manager->getItemsForCursor(*cursor, items, 111000111, 111000111);
        ASSERT_EQ(ckptList.back(), *cursor->getCheckpoint());
    }

    // Now re-register the test cursor.
    // Note that for triggering CheckpointRemoval we need to re-register the
    // cursor at Open checkpoint.
    // Before the MB-56094 fix this step causes a number of errors, all caught
    // in the validation that follows as these quantities aren't updated:
    // - CM::numItems
    // - Global "num items removed from checkpoints"
    // - Multiple CM mem usage stats - They end up into a wrong CM::memUsage
    // - Global "mem freed by checkpoint removal"
    dcpCursor =
            manager->registerCursorBySeqno(
                           cursorName, 1002, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock()
                    .get();
    ASSERT_TRUE(dcpCursor);
    ASSERT_EQ(2, manager->getNumOfCursors());
    ASSERT_EQ(ckptList.back(), *dcpCursor->getCheckpoint());
    ASSERT_EQ(CHECKPOINT_OPEN, (*dcpCursor->getCheckpoint())->getState());

    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumItems()); // cs, m:2
    EXPECT_EQ(2, manager->getNumOpenChkItems());
    EXPECT_EQ(global_stats.itemsRemovedFromCheckpoints, 3); // cs, m:1, ce
    EXPECT_EQ(initialMemUsage - firstCheckpointMemUsage,
              manager->getMemUsage());
    EXPECT_GT(global_stats.memFreedByCheckpointRemoval, 0);
}

CheckpointManager::ExtractItemsResult CheckpointTest::extractItemsToExpel() {
    std::lock_guard<std::mutex> lh(manager->queueLock);
    return manager->extractItemsToExpel(lh);
}

void CheckpointTest::expelCursorSetup() {
    ASSERT_EQ(1000, manager->getHighSeqno());
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());

    // Queue 2 items
    EXPECT_TRUE(queueNewItem("key1"));
    EXPECT_TRUE(queueNewItem("key2"));
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(1002, manager->getHighSeqno());

    // Ensure that we have at least 1 cursor in the checkpoint (expel will yield
    // to checkpoint-removal otherwise) and ensure that we have some items
    // eligible for expelling. We would skip the code path under test otherwise.
    EXPECT_EQ(1, manager->getNumOfCursors());
    EXPECT_TRUE(cursor);
    std::vector<queued_item> out;
    manager->getItemsForCursor(
            *cursor, out, 9999, std::numeric_limits<size_t>::max());
    EXPECT_EQ(3, out.size()); // checkpoint_start + mutations

    // [e:1001 cs:1001 m:1001 m:1002)
    //                        ^
    const auto pos = *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::mutation, pos->getOperation());
    EXPECT_EQ(1002, pos->getBySeqno());
}

CheckpointManager::ExtractItemsResult
CheckpointTest::testExpelCursorRegistered() {
    expelCursorSetup();
    // [e:1001 cs:1001 m:1001 m:1002)
    //                        ^

    auto res = extractItemsToExpel();
    EXPECT_EQ(2, res.getNumItems());

    // [e:1001 cs:1001 x x)
    //  ^      ^
    const auto& expelCursor = res.getExpelCursor();
    auto pos = *CheckpointCursorIntrospector::getCurrentPos(expelCursor);
    EXPECT_EQ(queue_op::empty, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    pos = *cursor->getPos();
    EXPECT_EQ(queue_op::checkpoint_start, pos->getOperation());
    EXPECT_EQ(1001, pos->getBySeqno());

    return res;
}

TEST_P(CheckpointTest, ExpelCursor_Registered) {
    testExpelCursorRegistered();
}

TEST_P(CheckpointTest, ExpelCursor_Removed) {
    expelCursorSetup();
    // [e:1001 cs:1001 m:1001 m:1002)
    //                        ^
    ASSERT_EQ(1, manager->getNumCursors());

    // The operation registers the expel-cursor and removes it once done, so the
    // final numCursors must not change
    const auto res = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(2, res.count);
    EXPECT_EQ(1, manager->getNumCursors());
}

TEST_P(CheckpointTest, ExpelCursor_NeverDrop) {
    const auto res = testExpelCursorRegistered();
    // [e:1001 cs:1001 x x)
    //  ^      ^

    // We never drop cursors in the open checkpoint, so close the existing one
    // and ensure that the expel cursor is still in the closed checkpoint.
    // Note: cursors that are at the end of the checkpoint being closed are
    //  bumped to the new checkpoint. By logic that can never happen for the
    //  expel-cursor as it always points to the empty item.
    manager->createNewCheckpoint();
    // [e:1001 cs:1001 x x] [e:1003 cs:1003)
    //  ^                    ^
    ASSERT_EQ(2, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumCursors());
    const auto& expelCursor = res.getExpelCursor();
    const auto expelCursorPos =
            *CheckpointCursorIntrospector::getCurrentPos(expelCursor);
    EXPECT_EQ(queue_op::empty, expelCursorPos->getOperation());
    EXPECT_EQ(1001, expelCursorPos->getBySeqno());
    ASSERT_TRUE(cursor);
    const auto cursorPos =
            *CheckpointCursorIntrospector::getCurrentPos(*cursor);
    EXPECT_EQ(queue_op::empty, cursorPos->getOperation());
    EXPECT_EQ(1003, cursorPos->getBySeqno());

    const auto toDrop = manager->getListOfCursorsToDrop();
    EXPECT_EQ(0, toDrop.size());
}

// Check expel behaviour when the oldest checkpoint is an Open checkpoint
// without any cursors (for example an Ephemeral bucket without a persistence
// cursor).
TEST_P(EphemeralCheckpointTest, Expel_OpenCheckpointNoCursor_OneItem) {
    // Queue a single item - we should  be able to expel this
    ASSERT_TRUE(queueNewItem("key1"));
    ASSERT_EQ(2, manager->getNumOpenChkItems());
    ASSERT_EQ(1001, manager->getHighSeqno());

    {
        auto res = extractItemsToExpel();
        EXPECT_EQ(1, res.getNumItems());
    }

    // Nothing should have changed in CheckpointManager.
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    EXPECT_EQ(0, manager->getNumOfCursors());
}

TEST_P(EphemeralCheckpointTest, Expel_OpenCheckpointNoCursor_TwoItems) {
    // Queue two items - we should be able to expel the oldest one (but should
    // keep the younger one).
    ASSERT_TRUE(queueNewItem("key1"));
    ASSERT_TRUE(queueNewItem("key2"));
    ASSERT_EQ(3, manager->getNumOpenChkItems());
    ASSERT_EQ(1002, manager->getHighSeqno());

    // Two items should have been expelled, should be two less in CkptMgr.
    {
        auto res = extractItemsToExpel();
        EXPECT_EQ(2, res.getNumItems());
    }

    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    EXPECT_EQ(0, manager->getNumOfCursors());
}

TEST_P(CheckpointTest, MB_47134_vbstate_at_backup_cursor) {
    if (!persistent()) {
        GTEST_SKIP();
    }

    // Lambda to simulate the tracking of agg stats done at the begging of a
    // flush vbucket
    auto updateAggStats = [](std::vector<queued_item>& items) {
        AggregatedFlushStats aggStats;
        for (auto& item : items) {
            if (item->shouldPersist()) {
                aggStats.accountItem(*item);
            }
        }
        return aggStats;
    };

    // Check the initial state
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(0, vbucket->dirtyQueueSize);

    // Add items A, B and a set_vbucket_state so we have a meta item for the
    // backup pointer to point too
    ASSERT_TRUE(queueNewItem("A")); // A
    ASSERT_TRUE(queueNewItem("B")); // B
    manager->queueSetVBState();
    EXPECT_EQ(3, vbucket->dirtyQueueSize);
    // Simulate a successful flush of the 3 items
    {
        std::vector<queued_item> items;
        auto itemsForCursor = manager->getItemsForCursor(
                *cursor, items, 1000, std::numeric_limits<size_t>::max());
        ASSERT_EQ(2, manager->getNumCursors());
        auto aggStats = updateAggStats(items);
        EXPECT_FALSE(itemsForCursor.moreAvailable);
        vbucket->doAggregatedFlushStats(aggStats);
    }
    // Check that we accounted the successful flush
    EXPECT_EQ(0, vbucket->dirtyQueueSize);

    ASSERT_TRUE(queueNewItem("B")); // B
    ASSERT_TRUE(queueNewItem("C")); // C
    // Ensure the dirty queue size is currently 3 for B,C
    EXPECT_EQ(2, vbucket->dirtyQueueSize);

    // Now simulate a failed flush, with a new mutation of B being added to the
    // checkpoint
    {
        std::vector<queued_item> items;
        auto itemsForCursor = manager->getItemsForCursor(
                *cursor, items, 1000, std::numeric_limits<size_t>::max());
        ASSERT_EQ(2, manager->getNumCursors());

        // Check that the backup cursor is pointing to the set vb state that was
        // the last thing that we "persisted"
        const auto backupPCursor = manager->getBackupPersistenceCursor();
        auto backupPos =
                *CheckpointCursorIntrospector::getCurrentPos(*backupPCursor);
        ASSERT_TRUE(backupPos->isCheckPointMetaItem());
        ASSERT_EQ(queue_op::set_vbucket_state, backupPos->getOperation());

        updateAggStats(items);
        // Add new mutations
        ASSERT_TRUE(queueNewItem("A")); // A
        ASSERT_TRUE(queueNewItem("B")); // B
        // Mark the flush as having failed
        itemsForCursor.flushHandle->markFlushFailed(*vbucket);
        EXPECT_FALSE(itemsForCursor.moreAvailable);
        // Ensure the dirty queue size is currently 4 for B,C,A,B
        EXPECT_EQ(4, vbucket->dirtyQueueSize);
    }
    // Set the test cursor to the new persistence cursor as it has been changed
    // due to the failed "flush"
    cursor = manager->getPersistenceCursor();

    // Ensure the dirty queue is currently 3 for C,A,B to account for the
    // deduplication after the flush failure
    EXPECT_EQ(3, vbucket->dirtyQueueSize);

    ASSERT_TRUE(queueNewItem("D")); // D
    EXPECT_EQ(4, vbucket->dirtyQueueSize);
    // Now perform a successful flush of C,A,B,D
    {
        std::vector<queued_item> items;
        auto itemsForCursor = manager->getItemsForCursor(
                *cursor, items, 1000, std::numeric_limits<size_t>::max());
        ASSERT_EQ(2, manager->getNumCursors());
        auto aggStats = updateAggStats(items);
        EXPECT_FALSE(itemsForCursor.moreAvailable);
        // Ensure the dirty queue size is currently 4 for C,A,B,D
        EXPECT_EQ(4, vbucket->dirtyQueueSize);
        vbucket->doAggregatedFlushStats(aggStats);
    }
    EXPECT_EQ(0, vbucket->dirtyQueueSize);
}

/**
 * Test to ensure that the seqno returned by registerCursorBySeqno() is
 * consistent with the the items the manager returned from
 * getNextItemsForDcp() for the cursor that was registered. So
 * getNextItemsForDcp() doesn't return items with seqnos lower than the seqno
 * returned by registerCursorBySeqno()
 */
TEST_P(CheckpointTest, MB_53100_RegisterCursor) {
    // 1. Create three meta only checkpoints.
    manager->queueSetVBState(); // seqno: 1001
    manager->createNewCheckpoint();
    manager->queueSetVBState(); // seqno: 1001
    manager->createNewCheckpoint();
    manager->queueSetVBState(); // seqno: 1001
    manager->createNewCheckpoint();
    // 2. Simulate a couple set vbucket states as would happen after a takeover
    // stream and a few new mutations that we can stream
    manager->queueSetVBState(); // seqno: 1001
    manager->queueSetVBState(); // seqno: 1001
    ASSERT_TRUE(queueNewItem("A")); // key:A seqno: 1001
    ASSERT_TRUE(queueNewItem("B")); // key:B seqno: 1002

    // 3. Register at seqno 1001 as if we received a stream request from 1001
    auto result = manager->registerCursorBySeqno(
            "test", 1001, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1002, result.nextSeqno);
    // Create a WeaklyMonotonic var to track the current cursors seqno as it
    // would in the ActiveStream
    WeaklyMonotonic<uint64_t, ThrowExceptionPolicy> curSeqno;
    curSeqno = result.nextSeqno;

    // 4. Get items for cursor
    std::vector<queued_item> items;
    auto itemsForCursor =
            manager->getNextItemsForDcp(*result.takeCursor().lock(), items);
    // Ensure that we gets items and the first item is 1002 NOT 1001 as it would
    // have been before MB-53100 was fixed
    ASSERT_FALSE(items.empty());
    EXPECT_EQ(1002, (*items.begin())->getBySeqno());

    // Using the WeaklyMonotonic var, check that all the items have seqnos that
    // are weakly monotonic from the seqno that registerCursorBySeqno returned
    EXPECT_NO_THROW(std::for_each(
            items.begin(), items.end(), [&curSeqno](const queued_item& i) {
                curSeqno = i->getBySeqno();
            }));
}

/**
 * MB-55520
 * This test ensures that in a sequence of [Memory, Disk] checkpoints the CM
 * never returns checkpoints of different types from CM::getItemsForCursor().
 *
 * Similar to CheckpointTest.getItems_MemoryDiskSnapshot. I write a dedicated
 * test here for MB-55520 as this test highlights the aspects relative to the
 * particular failure seen in MB-55520.
 */
TEST_P(CheckpointTest, NeverMergeCheckpointsOfDifferentType) {
    {
        // Move cursor to end of checkpoint and allow checkpoint removal in the
        // next steps
        std::vector<queued_item> items;
        manager->getItemsForCursor(*cursor, items, 123456, 123456);
    }

    // First Memory snapshot
    manager->createSnapshot(1001, 1002, {}, CheckpointType::Memory, 1002);
    EXPECT_TRUE(queueReplicatedItem("key1", 1001));
    EXPECT_TRUE(queueReplicatedItem("key2", 1002));
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    // Second Disk snapshot
    this->manager->createSnapshot(1003, 1004, 0, CheckpointType::Disk, 1004);
    EXPECT_TRUE(queueReplicatedItem("key3", 1003));
    EXPECT_TRUE(queueReplicatedItem("key4", 1004));
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());

    std::vector<queued_item> items;
    const auto res = manager->getItemsForCursor(*cursor, items, 123456, 123456);
    EXPECT_TRUE(res.moreAvailable);

    // Expect only the first snapshot.
    // Before the fix we get the  2 snapshots merged.
    EXPECT_EQ(1, res.ranges.size());
    // Only range 1001, 1002
    EXPECT_EQ(1001, res.ranges[0].getStart());
    EXPECT_EQ(1002, res.ranges[0].getEnd());
    EXPECT_EQ(4, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
    EXPECT_EQ(queue_op::mutation, items.at(1)->getOperation());
    EXPECT_EQ(1001, items.at(1)->getBySeqno());
    EXPECT_EQ(queue_op::mutation, items.at(2)->getOperation());
    EXPECT_EQ(1002, items.at(2)->getBySeqno());
    EXPECT_EQ(queue_op::checkpoint_end, items.at(3)->getOperation());

    // Verify the other ItemsForCursor quantities
    EXPECT_EQ(1002, res.visibleSeqno);

    // !! VERY IMPORTANT AS COVERAGE FOR MB-55520 !!
    // Before the fix here we get CheckpointType::Disk && !HCS, which is the
    // illegal state that fails ActiveStream in MB-55520.
    EXPECT_EQ(CheckpointType::Memory, res.checkpointType);
    EXPECT_FALSE(res.highCompletedSeqno);
}

INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        CheckpointTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllEvictionModes,
        EphemeralCheckpointTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        ReplicaCheckpointTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        CheckpointRemovalTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllKeyLengths,
        CheckpointMemoryTrackingTest,
        ::testing::Values(CheckpointMemoryTrackingTest::shortKeyLength,
                          CheckpointMemoryTrackingTest::longKeyLength),
        CheckpointMemoryTrackingTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllKeyLengths,
        CheckpointIndexAllocatorMemoryTrackingTest,
        ::testing::Values(CheckpointMemoryTrackingTest::shortKeyLength,
                          CheckpointMemoryTrackingTest::longKeyLength),
        CheckpointMemoryTrackingTest::PrintToStringParamName);

// Test that the CheckpointManager's getMemUsage and EPStats' estimated mem
// usage both increase and decrease by the size of Checkpoint when a new empty
// checkpoint is created / destroyed
TEST_F(CheckpointMemoryTrackingTest, CheckpointManagerAccountsEmptyCheckpoint) {
    setVBucketState(vbid, vbucket_state_active);
    auto& stats = engine->getEpStats();
    auto vb = store->getVBuckets().getBucket(vbid);
    std::vector<queued_item> items;

    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    // C1 [empty, > checkpoint_start, setVBState]

    manager.getNextItemsForPersistence(items);
    // C1 [empty, checkpoint_start, > setVBState]

    // Force new checkpoint to get rid of the checkpoint that contains the
    // setVBState item. We don't need to move the cursor forward because the
    // cursor was already at the end of the previous checkpoint, and so is
    // automatically moved forward as an optimization

    manager.createNewCheckpoint();
    // C2 [> empty, checkpoint_start]

    const auto startMemUsage = manager.getMemUsage();
    const auto startEstimatedMemUsage =
            stats.getCheckpointManagerEstimatedMemUsage();

    manager.createNewCheckpoint();
    // C2 [> empty, checkpoint_start], C3 [empty, checkpoint_start]

    auto newCheckpoint = manager.getCheckpointList().front().get();
    auto newCheckpointOverhead = newCheckpoint->getQueuedItemsMemUsage() +
                                 newCheckpoint->getMemOverheadQueue();

    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(startMemUsage + sizeof(Checkpoint) + newCheckpointOverhead,
              manager.getMemUsage());
    EXPECT_EQ(
            startEstimatedMemUsage + sizeof(Checkpoint) + newCheckpointOverhead,
            stats.getCheckpointManagerEstimatedMemUsage());

    manager.getNextItemsForPersistence(items);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(startMemUsage, manager.getMemUsage());
    EXPECT_EQ(startEstimatedMemUsage,
              stats.getCheckpointManagerEstimatedMemUsage());
}

void CheckpointMemoryTrackingTest::testCheckpointManagerMemUsage() {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    const auto& openQueue =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    manager);
    const auto initialQueueSize = openQueue.size();
    // empty + ckpt_start + set_vbstate
    EXPECT_EQ(3, initialQueueSize);

    auto* checkpoint = manager.getCheckpointList().front().get();
    const auto& stats = engine->getEpStats();

    // pre-conditions
    const auto initialQueued = checkpoint->getQueuedItemsMemUsage();
    const auto initialQueueOverhead = checkpoint->getMemOverheadQueue();
    const auto initialIndex = checkpoint->getMemOverheadIndex();
    const auto initialMemOverhead = stats.getMemOverhead();
    auto checkpointOverhead = manager.getNumCheckpoints() * sizeof(Checkpoint);

    const auto initialMemOverheadAllocator =
            checkpoint->getMemOverheadAllocatorBytes();
    const auto initialQueuedOverheadAllocator =
            checkpoint->getWriteQueueAllocatorBytes();
    const auto initialIndexAllocator = checkpoint->getKeyIndexAllocatorBytes();

    // Some metaitems are already in the queue
    EXPECT_GT(initialQueued, 0);
    EXPECT_EQ(initialQueueSize * Checkpoint::per_item_queue_overhead,
              initialQueueOverhead);
    EXPECT_EQ(0, initialIndex);
    EXPECT_EQ(initialQueued + initialQueueOverhead + checkpointOverhead,
              stats.getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(initialQueued + initialQueueOverhead + checkpointOverhead,
              manager.getMemUsage());

    EXPECT_EQ(initialMemOverheadAllocator,
              sizeof(Checkpoint) + initialQueuedOverheadAllocator +
                      initialIndexAllocator);

    size_t itemsAlloc = 0;
    size_t itemOverheadAlloc = 0;
    size_t queueAlloc = 0;

    // The allocator accounts for
    // sizeof(StoredDocKeyT<MemoryTrackingAllocator>) + sizeof(IndexEntry)
    // for each item, manual stats only account for the latter.
    const size_t allocatorInsertionOverhead =
            checkpointIndexInsertionOverhead * numItems;
    const size_t manualInsertionOverhead = sizeof(IndexEntry) * numItems;

    // Manual stats account for SSO, but only the key.size() portion and not
    // sizeof(std::string). The allocator accounts for heap allocations only.
    size_t totalKeySize = 0;
    size_t totalKeyHeapAlloc = 0; // Subset of totalKeySize; only counts keys
                                  // that require heap allocation
    // In this way, the expected key sizes for the current behaviour of:
    // * Manual = totalKeySize
    // * Allocator = totalKeyHeapAlloc, plus some bytes for possible
    // overallocation as (capacity >= size)
    std::string key;
    for (size_t i = 0; i < numItems; ++i) {
        auto item = makeCommittedItem(
                makeStoredDocKey(createPaddedKeyString(i, GetParam())),
                "value",
                vbid);
        EXPECT_TRUE(vb->checkpointManager->queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));

        itemsAlloc += item->size();
        itemOverheadAlloc += (item->size() - item->getValMemSize());
        queueAlloc += Checkpoint::per_item_queue_overhead;

        const auto keySize = item->getKey().size();
        totalKeySize += keySize;
        if (GetParam() == CheckpointMemoryTrackingTest::longKeyLength) {
            totalKeyHeapAlloc += keySize;
        }
    }

    const size_t manuallyTrackedKeyIndexAlloc =
            totalKeySize + manualInsertionOverhead;
    const size_t allocatorTrackedKeyIndexAlloc =
            totalKeyHeapAlloc + allocatorInsertionOverhead;

    // Load post-conditions
    ASSERT_EQ(1, manager.getNumCheckpoints());
    // cs + vbs + mut(s)
    EXPECT_EQ(2 + numItems, manager.getNumOpenChkItems());
    EXPECT_EQ(initialQueueSize + numItems, openQueue.size());

    const auto queued = checkpoint->getQueuedItemsMemUsage();
    const auto queueOverhead = checkpoint->getMemOverheadQueue();
    const auto index = checkpoint->getMemOverheadIndex();
    checkpointOverhead = manager.getNumCheckpoints() * sizeof(Checkpoint);

    const auto memOverheadAllocator =
            checkpoint->getMemOverheadAllocatorBytes();
    const auto queuedOverheadAllocator =
            checkpoint->getWriteQueueAllocatorBytes();
    const auto indexAllocator = checkpoint->getKeyIndexAllocatorBytes();

    EXPECT_EQ(initialQueued + itemsAlloc, queued);
    EXPECT_EQ(initialQueueOverhead + queueAlloc, queueOverhead);
    EXPECT_EQ(initialIndex + manuallyTrackedKeyIndexAlloc, index);
    EXPECT_EQ(initialMemOverhead + queueAlloc + manuallyTrackedKeyIndexAlloc +
                      itemOverheadAlloc,
              stats.getMemOverhead());
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              stats.getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              manager.getMemUsage());

    // Since we only have one checkpoint, the CheckpointManager's sum should be
    // equal to the single Checkpoint's memory values
    EXPECT_EQ(queueOverhead, manager.getMemOverheadQueue());
    EXPECT_EQ(index, manager.getMemOverheadIndex());
    // Add the size of one Checkpoint to the queue and index overhead to
    // calculate the CM's memory overhead
    EXPECT_EQ(queueOverhead + index + sizeof(Checkpoint),
              manager.getMemOverhead());

    EXPECT_EQ(memOverheadAllocator,
              sizeof(Checkpoint) + queuedOverheadAllocator + indexAllocator);
    EXPECT_EQ(queuedOverheadAllocator,
              initialQueuedOverheadAllocator + queueAlloc);

    // We cannot easily pinpoint the exact allocation made by the allocator, but
    // we can expect a reasonable minimum value:
    // Memory allocated should increase by more than the sum of the key sizes
    // and the insertion overhead times the number of items.
    EXPECT_GE(indexAllocator,
              initialIndexAllocator + allocatorTrackedKeyIndexAlloc);
    // We can't upper bound this value without tightly coupling to the
    // underlying implementation of the checkpoint index structure, which isn't
    // suitable for this test.
}

TEST_P(CheckpointMemoryTrackingTest, CheckpointManagerMemUsage) {
    testCheckpointManagerMemUsage();
}

TEST_P(CheckpointMemoryTrackingTest,
       CheckpointManagerMemUsageMultipleCheckpoints) {
    int numCheckpoints = 2;
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    // Force each item to be in a new checkpoint
    engine->getCheckpointConfig().setCheckpointMaxSize(1);

    for (int key = 0; key < numCheckpoints; ++key) {
        store_item(vbid, makeStoredDocKey(std::to_string(key)), "");
    }

    ASSERT_GT(manager.getNumCheckpoints(), 1);

    size_t queueOverheadTotal = 0;
    size_t keyIndexOverheadTotal = 0;
    size_t memOverheadTotal = 0;
    for (auto& checkpoint : manager.getCheckpointList()) {
        queueOverheadTotal += checkpoint->getMemOverheadQueue();
        keyIndexOverheadTotal += checkpoint->getMemOverheadIndex();
        memOverheadTotal += checkpoint->getMemOverhead();
    }

    EXPECT_EQ(queueOverheadTotal, manager.getMemOverheadQueue());
    EXPECT_EQ(keyIndexOverheadTotal, manager.getMemOverheadIndex());
    EXPECT_EQ(memOverheadTotal, manager.getMemOverhead());
}

TEST_P(CheckpointMemoryTrackingTest, CheckpointManagerMemUsageAtExpelling) {
    testCheckpointManagerMemUsage();

    auto vb = store->getVBuckets().getBucket(vbid);
    const auto& stats = engine->getEpStats();
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    const auto initialNumItems = manager.getNumOpenChkItems();
    ASSERT_GT(initialNumItems, 0);
    const auto& openQueue =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    manager);
    const auto initialQueueSize = openQueue.size();
    EXPECT_GT(initialQueueSize, initialNumItems);

    auto& checkpoint = *manager.getCheckpointList().front();
    const auto initialQueued = checkpoint.getQueuedItemsMemUsage();
    const auto initialQueueOverhead = checkpoint.getMemOverheadQueue();
    const auto initialIndex = checkpoint.getMemOverheadIndex();
    const auto initialMemOverhead = stats.getMemOverhead();

    // Move the cursor to the first mutation, we want to expel up to that.
    // Skip ckpt-start and set-vbstate, and place the cursor on m:1.
    // As a collateral thing, I need to keep track of sizes of items that we are
    // going to expel, that's to make our final verification on memory counters.
    size_t setVBStateSize = 0;
    size_t setVBStateOverhead = 0;
    size_t m1Size = 0;
    size_t m1Overhead = 0;
    // While these are helpers for other checks later in the test.
    size_t emptySize = 0;
    size_t ckptStartSize = 0;

    auto& cursor = *manager.getPersistenceCursor();
    {
        const auto pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
        ASSERT_TRUE(pos->isEmptyItem());
        emptySize = pos->size();
    }

    for (auto i = 0; i < 3; ++i) {
        CheckpointCursorIntrospector::incrPos(cursor);
        const auto pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
        if (pos->getOperation() == queue_op::checkpoint_start) {
            ckptStartSize = pos->size();
        } else if (pos->getOperation() == queue_op::set_vbucket_state) {
            setVBStateSize = pos->size();
            setVBStateOverhead = setVBStateSize - pos->getValMemSize() +
                                 Checkpoint::per_item_queue_overhead;
        } else if (pos->getOperation() == queue_op::mutation &&
                   pos->getBySeqno() == 1) {
            m1Size = pos->size();
            m1Overhead = m1Size - pos->getValMemSize() +
                         Checkpoint::per_item_queue_overhead;
        }
    }

    ASSERT_GT(setVBStateSize, 0);
    ASSERT_GT(m1Size, 0);
    ASSERT_GT(emptySize, 0);
    ASSERT_GT(ckptStartSize, 0);
    {
        const auto pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
        ASSERT_FALSE(pos->isCheckPointMetaItem());
        ASSERT_EQ(1, pos->getBySeqno());
    }

    ASSERT_EQ(0, manager.getMemFreedByItemExpel());
    ASSERT_EQ(0, stats.memFreedByCheckpointItemExpel);

    // Expelling set-vbstate + m:1
    const auto numExpelled = manager.expelUnreferencedCheckpointItems().count;
    EXPECT_EQ(2, numExpelled);

    // Expel post-conditions
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(initialNumItems - numExpelled, manager.getNumOpenChkItems());
    EXPECT_EQ(initialQueueSize - numExpelled, openQueue.size());

    const auto queued = checkpoint.getQueuedItemsMemUsage();
    const auto queueOverhead = checkpoint.getMemOverheadQueue();
    const auto index = checkpoint.getMemOverheadIndex();
    const auto checkpointOverhead =
            manager.getNumCheckpoints() * sizeof(Checkpoint);
    // Initial - what we expelled
    EXPECT_EQ(initialQueued - setVBStateSize - m1Size, queued);
    EXPECT_EQ(initialQueueOverhead -
                      (numExpelled * Checkpoint::per_item_queue_overhead),
              queueOverhead);
    // Expel doesn't touch the key index
    EXPECT_EQ(initialIndex, index);
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              stats.getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              manager.getMemUsage());
    EXPECT_EQ(initialMemOverhead - setVBStateOverhead - m1Overhead,
              stats.getMemOverhead());

    EXPECT_GT(manager.getMemFreedByItemExpel(), 0);
    EXPECT_EQ(manager.getMemFreedByItemExpel(),
              stats.memFreedByCheckpointItemExpel);
}

TEST_P(CheckpointMemoryTrackingTest, CheckpointManagerMemUsageAtRemoval) {
    testCheckpointManagerMemUsage();

    // confirm that no items have been removed from the checkpoint manager
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.itemsRemovedFromCheckpoints);
    ASSERT_EQ(0, stats.memFreedByCheckpointRemoval);

    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    ASSERT_EQ(0, manager.getMemFreedByCheckpointRemoval());

    EXPECT_EQ(1, manager.getNumCheckpoints());
    const auto initialNumItems = manager.getNumOpenChkItems();
    ASSERT_GT(initialNumItems, 0);
    const auto& openQueue =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    manager);
    const auto initialQueueSize = openQueue.size();
    EXPECT_GT(initialQueueSize, initialNumItems);

    auto& cursor = *manager.getPersistenceCursor();
    auto pos = CheckpointCursorIntrospector::getCurrentPos(cursor);
    ASSERT_TRUE((*pos)->isEmptyItem());
    // These are helpers for other checks later in the test.
    const auto emptySize = (*pos)->size();
    ++pos;
    ASSERT_TRUE((*pos)->isCheckpointStart());
    const auto ckptStartSize = (*pos)->size();

    // The tracked mem-usage after expel should account only for the
    // empty + ckpt_start items in the single/open empty checkpoint
    const auto expectedFinalQueueAllocation = emptySize + ckptStartSize;
    const auto expectedFinalQueueOverheadAllocation =
            2 * Checkpoint::per_item_queue_overhead;
    const auto expectedFinalIndexAllocation = 0;

    auto checkpoint = manager.getCheckpointList().front().get();
    auto queued = checkpoint->getQueuedItemsMemUsage();
    auto queueOverhead = checkpoint->getMemOverheadQueue();
    auto index = checkpoint->getMemOverheadIndex();
    auto checkpointOverhead = manager.getNumCheckpoints() * sizeof(Checkpoint);
    ASSERT_GT(queued, expectedFinalQueueAllocation);
    ASSERT_GT(index, expectedFinalIndexAllocation);
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              engine->getEpStats().getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              manager.getMemUsage());

    manager.createNewCheckpoint();
    EXPECT_EQ(2, manager.getNumCheckpoints());
    // Move cursor to new checkpoint
    std::vector<queued_item> items;
    manager.getNextItemsForPersistence(items);
    // Verify cursor move did remove some checkpoints
    // Items removed: initial (cs + mut(s)) + ce
    EXPECT_EQ(initialNumItems + 1,
              engine->getEpStats().itemsRemovedFromCheckpoints);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems());

    // Checkpoint queued for destruction
    EXPECT_EQ(1, getCkptDestroyerTask(vbid)->getNumCheckpoints());
    // Run the destroyer task to recover the memory the checkpoints use
    runCheckpointDestroyer(vbid);

    checkpoint = manager.getCheckpointList().front().get();
    queued = checkpoint->getQueuedItemsMemUsage();
    queueOverhead = checkpoint->getMemOverheadQueue();
    index = checkpoint->getMemOverheadIndex();
    checkpointOverhead = manager.getNumCheckpoints() * sizeof(Checkpoint);
    EXPECT_EQ(expectedFinalQueueAllocation, queued);
    EXPECT_EQ(expectedFinalQueueOverheadAllocation, queueOverhead);
    EXPECT_EQ(expectedFinalIndexAllocation, index);
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              engine->getEpStats().getCheckpointManagerEstimatedMemUsage());
    EXPECT_EQ(queued + index + queueOverhead + checkpointOverhead,
              manager.getMemUsage());

    EXPECT_GT(manager.getMemFreedByCheckpointRemoval(), 0);
    EXPECT_EQ(manager.getMemFreedByCheckpointRemoval(),
              stats.memFreedByCheckpointRemoval);
}

TEST_P(CheckpointMemoryTrackingTest, Deduplication) {
    // Queue numItems mutations into the checkpoint
    testCheckpointManagerMemUsage();

    // Pre-condition: last key is in the queue
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    const auto ckptId = checkpoint.getId();
    // cs + vbs + muts
    ASSERT_EQ(1 + 1 + numItems, checkpoint.getNumItems());
    const auto& queue =
            CheckpointManagerTestIntrospector::public_getOpenCheckpointQueue(
                    manager);
    const auto lastKey = createPaddedKeyString(numItems - 1, GetParam());
    ASSERT_EQ("cid:0x0:" + lastKey, queue.back()->getKey().to_string());
    const auto preValueSize = queue.back()->getNBytes();

    // Pre-dedup mem state
    const auto initialTotal = manager.getMemUsage();
    const auto initialQueued = checkpoint.getQueuedItemsMemUsage();
    const auto initialQueueOverhead = checkpoint.getMemOverheadQueue();
    const auto initialIndexOverhead = checkpoint.getMemOverheadIndex();
    EXPECT_GT(initialQueued, 0);
    EXPECT_GT(initialQueueOverhead, 0);
    EXPECT_GT(initialIndexOverhead, 0);

    // Test - deduplicate item
    auto item = makeCommittedItem(makeStoredDocKey(lastKey),
                                  std::string(2 * preValueSize, 'x'),
                                  vbid);
    EXPECT_FALSE(manager.queueDirty(
            item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));

    // Post
    EXPECT_EQ(
            ckptId,
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(manager)
                    .getId());
    // cs + vbs + 10 muts
    EXPECT_EQ(1 + 1 + numItems, checkpoint.getNumItems());
    EXPECT_EQ(initialTotal + preValueSize, manager.getMemUsage());
    EXPECT_EQ(initialQueued + preValueSize,
              checkpoint.getQueuedItemsMemUsage());
    EXPECT_EQ(initialQueueOverhead, checkpoint.getMemOverheadQueue());
    EXPECT_EQ(initialIndexOverhead, checkpoint.getMemOverheadIndex());
}

TEST_P(CheckpointMemoryTrackingTest, BackgroundTaskIsNotified) {
    // Verify that eager checkpoint removal notifies the CheckpointDestroyerTask
    // to run ASAP.

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    scheduleCheckpointDestroyerTasks();

    const auto task = getCkptDestroyerTask(vbid);

    auto initialWaketime = task->getWaketime();

    // Add two items to the initial (open) checkpoint.
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    for (auto i : {1, 2}) {
        auto item = makeCommittedItem(
                makeStoredDocKey(createPaddedKeyString(i, GetParam())),
                "value",
                vbid);
        EXPECT_TRUE(manager.queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    }
    auto* cursor = manager.getPersistenceCursor();
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(4, manager.getNumOpenChkItems());
    EXPECT_EQ(4, manager.getNumItemsForCursor(*cursor));

    // task should not have been woken yet
    EXPECT_EQ(initialWaketime, task->getWaketime());

    // Create a new checkpoint, closing the current open one. The cursor
    // remains in the old checkpoint, so it is still reffed - callback
    // should not be triggered
    manager.createNewCheckpoint();
    EXPECT_EQ(1, manager.getNumOpenChkItems());
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(6, manager.getNumItemsForCursor(*cursor));

    // task should not have been woken yet
    EXPECT_EQ(initialWaketime, task->getWaketime());

    auto& epstats = engine->getEpStats();

    auto initialCMMemUsage = manager.getMemUsage();
    auto initialEPMemUsage = epstats.getCheckpointManagerEstimatedMemUsage();

    // the destroyer doesn't own anything yet, so should have no mem usage
    EXPECT_EQ(0, task->getMemoryUsage());
    EXPECT_EQ(0, task->getNumCheckpoints());

    // advance the cursor, unreffing the checkpoint. CheckpointDestroyerTask
    // should be notified and ownership of the checkpoint transferred.
    {
        std::vector<queued_item> items;
        manager.getNextItemsForPersistence(items);
    }
    // as soon as checkpoints are removed, the manager's memory usage should
    // decrease...
    EXPECT_LT(manager.getMemUsage(), initialCMMemUsage);
    // ... and the destroyer task's should increase by the same amount
    EXPECT_EQ(initialCMMemUsage - manager.getMemUsage(),
              task->getMemoryUsage());
    EXPECT_EQ(1, task->getNumCheckpoints());

    // Also the counter in EPStats accounts only checkpoints owned by CM, so it
    // must be already updated now that checkpoints are owned by the destroyer
    const auto postDetachEPMemUsage =
            epstats.getCheckpointManagerEstimatedMemUsage();
    EXPECT_LT(postDetachEPMemUsage, initialEPMemUsage);

    // now the task should be ready to run
    EXPECT_LE(task->getWaketime(), std::chrono::steady_clock::now());

    auto& nonIOQueue = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(nonIOQueue, "Destroying closed unreferenced checkpoints");

    // checkpoint has been destroyed, EPStats counter has already been updated
    // so it must not change again now
    EXPECT_EQ(postDetachEPMemUsage,
              epstats.getCheckpointManagerEstimatedMemUsage());
    // and so should the destroyers memory tracking
    EXPECT_EQ(0, task->getMemoryUsage());
    EXPECT_EQ(0, task->getNumCheckpoints());
}

TEST_P(CheckpointIndexAllocatorMemoryTrackingTest,
       keyIndexAllocatorAccountsForKey) {
    using Introspector = CheckpointManagerTestIntrospector;

    const auto keyLength = GetParam();

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    // Lambda function used to guarantee duplicate item queued is duplicate
    auto queueItem = [this, &manager, &keyLength]() {
        auto item =
                makeCommittedItem(makeStoredDocKey(std::string(keyLength, 'x'),
                                                   CollectionID::Default),
                                  "value",
                                  vbid);
        EXPECT_TRUE(manager.queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    };

    auto& checkpoint = Introspector::public_getOpenCheckpoint(manager);
    EXPECT_EQ(0, checkpoint.getKeyIndexAllocatorBytes());

    queueItem();

    // Expect reasonable values for the keyIndex allocation, which should be:
    // 1. Greater than or equal to the insertion overhead plus the size of the
    // key allocation on the heap
    EXPECT_GE(checkpoint.getKeyIndexAllocatorBytes(),
              checkpointIndexInsertionOverhead + keyLength);
    // 2. Less than or equal to the platform-specific overhead + insertion
    // overhead + the first element metadata overhead for Folly maps, plus the
    // size of the key. As std::string will likely overallocate for
    // alignment/optimization purposes, upper bound the raw size of the key by
    // some bytes.
    EXPECT_LE(checkpoint.getKeyIndexAllocatorBytes(),
              checkpointIndexInsertionOverhead + firstElemOverhead +
                      (keyLength + alignmentBytes));

    // Expel the item from the checkpoint. The keyIndex will still contain
    // the key/value, so its size should not change - expect the same value.
    const auto beforeExpel = checkpoint.getKeyIndexAllocatorBytes();
    checkpoint.expelItems(std::prev(checkpoint.end()), 3);
    EXPECT_EQ(beforeExpel, checkpoint.getKeyIndexAllocatorBytes());

    // Queue the same item again. As a duplicate of a key that is already in the
    // keyIndex, the memory usage should not change as nothing is inserted.
    const auto beforeDuplicateItem = checkpoint.getKeyIndexAllocatorBytes();
    queueItem();
    EXPECT_EQ(beforeDuplicateItem, checkpoint.getKeyIndexAllocatorBytes());
}

void ShardedCheckpointDestructionTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    config_string +=
            "checkpoint_destruction_tasks=" + std::to_string(GetParam());
    SingleThreadedKVBucketTest::SetUp();
}

TEST_P(ShardedCheckpointDestructionTest, ShardedBackgroundTaskIsNotified) {
    // Verify that eager checkpoint removal notifies the correct destroyer task

    // sanity check that the number of tasks that exist matches the config
    const auto locked = getCheckpointDestroyerTasks().rlock();
    ASSERT_EQ(GetParam(), locked->size());

    const size_t numVbuckets = 4;
    // setup 4 vbuckets
    for (size_t i = 0; i < numVbuckets; ++i) {
        setVBucketState(Vbid(i), vbucket_state_active);
    }

    // schedules all the destroyer tasks (number controlled by test param)
    scheduleCheckpointDestroyerTasks();

    // none of the tasks should be scheduled to run
    for (const auto& task : *locked) {
        EXPECT_EQ(task->getWaketime(),
                  std::chrono::steady_clock::time_point::max());
    }

    // queue an item, then destroy the checkpoint for each of the vbuckets
    for (size_t i = 0; i < numVbuckets; ++i) {
        auto currVbid = Vbid(i);

        auto vb = store->getVBuckets().getBucket(currVbid);
        auto& manager =
                static_cast<MockCheckpointManager&>(*vb->checkpointManager);

        auto item = makeCommittedItem(
                makeStoredDocKey("key" + std::to_string(i)), "value", currVbid);
        EXPECT_TRUE(manager.queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));

        manager.createNewCheckpoint();
        // advance the persistence cursor, CheckpointDestroyerTask should be
        // notified.
        {
            std::vector<queued_item> items;
            manager.getNextItemsForPersistence(items);
        }

        // The specific task this vbid is associated with should have been
        // scheduled to run
        EXPECT_LE(getCkptDestroyerTask(vbid)->getWaketime(),
                  std::chrono::steady_clock::now());
    }

    auto& nonIOQueue = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    // check that expected tasks have been notified, and run them
    for (size_t i = 0; i < locked->size(); ++i) {
        const auto& task = locked->at(i);
        if (numVbuckets > i) {
            // there are enough vbuckets that this task should definitely
            // have been triggered
            EXPECT_LE(task->getWaketime(), std::chrono::steady_clock::now());
            runNextTask(nonIOQueue,
                        "Destroying closed unreferenced checkpoints");
        } else {
            // there is a surplus of destroyers - no vbuckets will be allocated
            // to the excess tasks
            // e.g.,
            // task = vbid % numTasks
            //          4  %   5
            // none of the 4 vbuckets can map to the 5th task
            // this task should not have been woken.
            EXPECT_EQ(task->getWaketime(),
                      std::chrono::steady_clock::time_point::max());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(MultipleCheckpointDestroyerTests,
                         ShardedCheckpointDestructionTest,
                         ::testing::Values(
                                 // number of destroyer tasks
                                 1, // Degenerate case, same as pre-sharding
                                 2, // even distribution
                                 3, // uneven distribution
                                 4, // destroyer for each vb
                                 5 // more destroyers than vbuckets
                                 ),
                         ::testing::PrintToStringParamName());

TEST_F(CheckpointConfigTest, MaxCheckpoints_LowerThanMin) {
    auto& config = engine->getConfiguration();
    try {
        config.setMaxCheckpoints(1);
    } catch (const std::range_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr("Validation Error, max_checkpoints "
                                       "takes values between 2"));
        return;
    }
    FAIL();
}

TEST_F(CheckpointConfigTest, MaxCheckpoints) {
    auto& config = engine->getConfiguration();
    config.setMaxCheckpoints(1000);

    setVBucketState(vbid, vbucket_state_active);
    auto& manager = *store->getVBuckets().getBucket(vbid)->checkpointManager;

    EXPECT_EQ(1000, manager.getCheckpointConfig().getMaxCheckpoints());
}

void EphemeralCheckpointTest::SetUp() {
    CheckpointTest::SetUp();
    // Remove test-cursor - we want these tests to run in the same
    // configuration as an Ephemeral bucket normally does.
    ASSERT_TRUE(manager->removeCursor(*cursor));

    ASSERT_EQ(1000, manager->getHighSeqno());
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_EQ(0, manager->getNumOfCursors());
}

void CDCCheckpointTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    // Enable history retention
    config_string += "history_retention_bytes=10485760";
    SingleThreadedKVBucketTest::SetUp();

    CollectionsManifest manifest;
    manifest.add(CollectionEntry::fruit,
                 cb::NoExpiryLimit,
                 {} /*no history*/,
                 ScopeEntry::defaultS);
    manifest.add(CollectionEntry::historical,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, manifest);
    flushVBucketToDiskIfPersistent(vbid, 2);
    vb->checkpointManager->createNewCheckpoint();
}

TEST_F(CDCCheckpointTest, CollectionNotDeduped) {
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    ASSERT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumItems()); // cs
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs only, no mutation
    ASSERT_EQ(2, manager.getHighSeqno()); // 2 create-coll processed at SetUp

    // The test-cursor is at the begin of the single open checkpoint
    ASSERT_EQ(1, manager.getNumCursors());
    const auto pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager.getPersistenceCursor());
    ASSERT_EQ(queue_op::empty, pos->getOperation());

    // Normal in-memory deduplication for collection(history=false)
    const auto keyNonHistorical =
            makeStoredDocKey("key", CollectionEntry::fruit);
    const auto value = "value";
    store_item(vbid, keyNonHistorical, value);
    store_item(vbid, keyNonHistorical, value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems()); // cs + mut
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs + mut
    EXPECT_EQ(4, manager.getHighSeqno());

    // No in-memory deduplication for collection(history=true)
    manager.createNewCheckpoint();
    flushVBucket(vbid);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    const auto keyHistorical =
            makeStoredDocKey("key", CollectionEntry::historical);
    store_item(vbid, keyHistorical, value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems()); // cs + mut
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs + mut
    EXPECT_EQ(5, manager.getHighSeqno());
    // Now queue duplicate
    store_item(vbid, keyHistorical, value);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(5, manager.getNumItems()); // cs, m, ce, cs, m
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(6, manager.getHighSeqno());
}

/**
 * The test shows that the deduplication behaviour in CM might affects also
 * collections with history turned off in the case where mutations for those
 * collections interleave with mutations that belong to some
 * collection(history=true).
 */
TEST_F(CDCCheckpointTest, CollectionNotDeduped_Interleaved) {
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    ASSERT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumItems()); // cs
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    ASSERT_EQ(2, manager.getHighSeqno()); // 2 create-coll processed at SetUp

    ASSERT_EQ(1, manager.getNumCursors());
    const auto pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager.getPersistenceCursor());
    ASSERT_EQ(queue_op::empty, pos->getOperation());

    const auto keyNonHistorical =
            makeStoredDocKey("key", CollectionEntry::fruit);
    const auto keyHistorical =
            makeStoredDocKey("key", CollectionEntry::historical);
    const auto value = "value";

    // history=false
    store_item(vbid, keyNonHistorical, value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems()); // [cs mut)
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs + 1x mut
    EXPECT_EQ(3, manager.getHighSeqno());

    // history=true, but keyHistorical not in the checkpoint index, so no need
    // to dedup anything yet
    store_item(vbid, keyHistorical, value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(3, manager.getNumItems()); // [cs mutV mutF)
    EXPECT_EQ(3, manager.getNumOpenChkItems()); // cs + 2x mut
    EXPECT_EQ(4, manager.getHighSeqno());

    // history=false, keyNonHistorical in the index, deduped
    store_item(vbid, keyNonHistorical, value);
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(3, manager.getNumItems()); // [cs x mutF mutV)
    EXPECT_EQ(3, manager.getNumOpenChkItems()); // cs + 2x mut
    EXPECT_EQ(5, manager.getHighSeqno());

    // history=true, keyHistorical in the index, dedup
    store_item(vbid, keyHistorical, value);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(6, manager.getNumItems()); // [cs x mutF mutV ce] [cs mutF)
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs + 1x mut
    EXPECT_EQ(6, manager.getHighSeqno());

    // history=false, keyNonHistorical could be dedup but it doesn't, as now we
    // queue the new mutation for it into a different checkpoint
    store_item(vbid, keyNonHistorical, value);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(7, manager.getNumItems()); // [cs x mutF mutV ce] [cs mutF mutV)
    EXPECT_EQ(3, manager.getNumOpenChkItems()); // cs + 2x mut
    EXPECT_EQ(7, manager.getHighSeqno());
}

TEST_F(CDCCheckpointTest, DuplicateItemWhenPreviousExpelled) {
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    // State:
    //
    // [cs)
    ASSERT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumItems()); // cs
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getHighSeqno()); // 2 create-coll processed at SetUp

    // The persistence cursor is at the begin of the single open checkpoint.
    ASSERT_EQ(1, manager.getNumCursors());
    auto pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager.getPersistenceCursor());
    ASSERT_EQ(queue_op::empty, pos->getOperation());

    const auto keyA = makeStoredDocKey("keyA", CollectionEntry::historical);
    const auto value = "value";
    store_item(vbid, keyA, value);
    // State:
    //
    // [cs m(A):3)
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems());
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, mut
    EXPECT_EQ(3, manager.getHighSeqno());

    const auto keyB = makeStoredDocKey("keyB", CollectionEntry::historical);
    store_item(vbid, keyB, value);
    // State:
    //
    // [cs m(A):3 m(B):4) )
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(3, manager.getNumItems());
    EXPECT_EQ(3, manager.getNumOpenChkItems()); // cs, mut, mut
    EXPECT_EQ(4, manager.getHighSeqno());

    flushVBucket(vbid);
    pos = *CheckpointCursorIntrospector::getCurrentPos(
            *manager.getPersistenceCursor());
    ASSERT_EQ(queue_op::mutation, pos->getOperation());
    ASSERT_EQ(4, pos->getBySeqno());
    // State:
    //
    // [cs m(A):3 m(B):4) )
    //            ^

    const auto keyC = makeStoredDocKey("keyC", CollectionEntry::historical);
    store_item(vbid, keyC, value);
    // State:
    //
    // [cs m(A):3 m(B):4) m(C):5) )
    //            ^
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(4, manager.getNumItems());
    EXPECT_EQ(4, manager.getNumOpenChkItems()); // cs, mut, mut, mut
    EXPECT_EQ(5, manager.getHighSeqno());

    const auto expelRes = manager.expelUnreferencedCheckpointItems();
    ASSERT_EQ(2, expelRes.count); // Note: We do expel items pointed by cursors
    // State:
    //
    // [cs x m(C):5) )
    //       ^
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(5, manager.getHighSeqno());

    // Core test: New mutation for keyA MUST be queued into a new checkpoint
    const auto ckptId = manager.getOpenCheckpointId();
    store_item(vbid, keyA, value);
    // State:
    //
    // [cs x m(C):5) ce] [cs m(A):6) )
    //       ^
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(5, manager.getNumItems());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(6, manager.getHighSeqno());
    EXPECT_GT(manager.getOpenCheckpointId(), ckptId);
}

class CheckpointTestDedup : public CheckpointTest {
public:
    void SetUp() override {
        CheckpointTest::SetUp();

        // 1 closed checkpoint and one open checkpoint
        queueNewItem("k1"); // 1001
        queueNewItem("k2"); // 1002
        manager->createNewCheckpoint();
        queueNewItem("k1"); // 1003
        queueNewItem("k2"); // 1004
        queueNewItem("k3"); // 1005

        // c1 in closed checkpoint
        auto cr1 = manager->registerCursorBySeqno(
                "c1", 1001, CheckpointCursor::Droppable::Yes);
        c1 = cr1.takeCursor();
        // c2 and c3 at different positions in the open checkpoint
        auto cr2 = manager->registerCursorBySeqno(
                "c2", 1002, CheckpointCursor::Droppable::Yes);
        c2 = cr2.takeCursor();
        auto cr3 = manager->registerCursorBySeqno(
                "c3", 1004, CheckpointCursor::Droppable::Yes);
        c3 = cr3.takeCursor();
        if (persistent()) {
            // CS + k1 + k2 + CE + CS + k1 + k2 + k3
            EXPECT_EQ(initialPCurItems, manager->getNumItemsForPersistence());
        }
        // k2 + CE + CS + k1 + k2 + k3
        EXPECT_EQ(initialC1Items,
                  manager->getNumItemsForCursor(*c1.lock().get()));
        // CS + k1 + k2 + k3
        EXPECT_EQ(initialC2Items,
                  manager->getNumItemsForCursor(*c2.lock().get()));
        // k3
        EXPECT_EQ(initialC3Items,
                  manager->getNumItemsForCursor(*c3.lock().get()));
    }

    void setupExpel() {
        if (persistent()) {
            std::vector<queued_item> items;
            manager->getItemsForPersistence(items,
                                            std::numeric_limits<size_t>::max(),
                                            std::numeric_limits<size_t>::max());
            EXPECT_EQ(initialPCurItems, items.size());
        }

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(*c1.lock().get(),
                                       items,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<size_t>::max());
            EXPECT_EQ(initialC1Items, items.size());
        }

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(*c2.lock().get(),
                                       items,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<size_t>::max());
            EXPECT_EQ(initialC2Items, items.size());
        }

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(*c3.lock().get(),
                                       items,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<size_t>::max());
            EXPECT_EQ(initialC3Items, items.size());
        }

        // No items for any cursor
        EXPECT_EQ(0, manager->getNumItemsForPersistence());
        EXPECT_EQ(0, manager->getNumItemsForCursor(*c1.lock().get()));
        EXPECT_EQ(0, manager->getNumItemsForCursor(*c2.lock().get()));
        EXPECT_EQ(0, manager->getNumItemsForCursor(*c3.lock().get()));
        manager->maybeCreateNewCheckpoint();
    }
    Cursor c1, c2, c3;
    const size_t initialC1Items = 6;
    const size_t initialC2Items = 4;
    const size_t initialC3Items = 1;
    const size_t initialPCurItems = 8;
};

TEST_P(CheckpointTestDedup, dedupK1) {
    // Dedup k1
    // replaces k1@1003 -> k1@1006
    // As dedup processes cursors c3 cursor has already processed k1 and must
    // count an extra item. c3 cursor has it's item-remaining offset adjusted
    // All other cursors see no change in number of available items as dedup k1
    // is ahead of them
    queueNewItem("k1");

    // CS + k1 + k2 + CE + CS + k2 + k3 + k1
    EXPECT_EQ(initialPCurItems, manager->getNumItemsForPersistence());

    // k2 + CE + CS + k2 + k3 + k1
    EXPECT_EQ(initialC1Items, manager->getNumItemsForCursor(*c1.lock().get()));
    // CS + k2 + k3 + k1
    EXPECT_EQ(initialC2Items, manager->getNumItemsForCursor(*c2.lock().get()));
    // k3 + k1
    EXPECT_EQ(initialC3Items + 1,
              manager->getNumItemsForCursor(*c3.lock().get()));
}

TEST_P(CheckpointTestDedup, dedupK2) {
    // Dedup k2
    // replaces k2@1004 -> k2@1006
    // This hits a case in dedup cursor processing where a cursor is on the
    // deduplicated item. Checkpoint::queueDirty will decrement the cursor. c3
    // is decremented and now accounts the "new" item. All other cusors see no
    // change.
    queueNewItem("k2"); // replaces k2@1004 -> k2@1006

    // CS + k1 + k2 + CE + CS + k1 + k3 + k2
    EXPECT_EQ(initialPCurItems, manager->getNumItemsForPersistence());

    // k2 + CE + CS + k1 + k3 + k2
    EXPECT_EQ(initialC1Items, manager->getNumItemsForCursor(*c1.lock().get()));
    // CS + k1 + k3 + k2
    EXPECT_EQ(initialC2Items, manager->getNumItemsForCursor(*c2.lock().get()));
    // k3 + k2
    EXPECT_EQ(initialC3Items + 1,
              manager->getNumItemsForCursor(*c3.lock().get()));
}

TEST_P(CheckpointTestDedup, dedupK3) {
    // Dedup k3
    // replaces k3@1005 -> k3@1006
    // No change to any cursor, replacing k3 which is ahead of all cursors makes
    // no change to the items remaining.
    queueNewItem("k3");

    // CS + k1 + k2 + CE + CS + k1 + k2 + k3
    EXPECT_EQ(initialPCurItems, manager->getNumItemsForPersistence());

    // k2 + CE + CS + k1 + k2 + k3
    EXPECT_EQ(initialC1Items, manager->getNumItemsForCursor(*c1.lock().get()));
    // CS + k1 + k2 + k3
    EXPECT_EQ(initialC2Items, manager->getNumItemsForCursor(*c2.lock().get()));
    // k3
    EXPECT_EQ(initialC3Items, manager->getNumItemsForCursor(*c3.lock().get()));
}

TEST_P(CheckpointTestDedup, expelAndDedupK1) {
    setupExpel();
    manager->expelUnreferencedCheckpointItems();

    // k1 was in the checkpoint (but expelled). Dedup processing detects this
    // case and we must treat k1 like a new item, all cursors now count it.
    queueNewItem("k1"); // replaces k3@1005 -> k3@1006

    // All see 1 item, k1
    EXPECT_EQ(1, manager->getNumItemsForPersistence());

    EXPECT_EQ(1, manager->getNumItemsForCursor(*c1.lock().get()));
    EXPECT_EQ(1, manager->getNumItemsForCursor(*c2.lock().get()));
    EXPECT_EQ(1, manager->getNumItemsForCursor(*c3.lock().get()));
}

// test is hits the case of dedupK1 but for all cursors
TEST_P(CheckpointTestDedup, dedupK1AfterAllVisited) {
    setupExpel(); // this will move all cursors to end

    // Test just ensures all cursors see the new k1
    queueNewItem("k1");

    // All see 1 item, k1
    EXPECT_EQ(1, manager->getNumItemsForPersistence());
    EXPECT_EQ(1, manager->getNumItemsForCursor(*c1.lock().get()));
    EXPECT_EQ(1, manager->getNumItemsForCursor(*c2.lock().get()));
    EXPECT_EQ(1, manager->getNumItemsForCursor(*c3.lock().get()));
}

INSTANTIATE_TEST_SUITE_P(
        CheckpointTestDedup,
        CheckpointTestDedup,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent),
                ::testing::Values(EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);
