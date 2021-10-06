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
#include "tests/module_tests/test_helpers.h"
#include "thread_gate.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "programs/engine_testapp/mock_server.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <valgrind/valgrind.h>

#include <thread>

#define NUM_DCP_THREADS 3
#define NUM_DCP_THREADS_VG 2
#define NUM_SET_THREADS 4
#define NUM_SET_THREADS_VG 2

#define NUM_ITEMS 500

#define DCP_CURSOR_PREFIX "dcp-client-"

void CheckpointTest::SetUp() {
    // disable eager checkpoint removal - these tests cover the "original"
    // lazy behaviour, and should continue to guard it.
    // EagerCheckpointDisposalTest covers eager tests.
    config.parseConfiguration("checkpoint_removal_mode=lazy",
                              get_mock_server_api());
    recreateCheckpointConfig();
    VBucketTest::SetUp();
    createManager();
}

void CheckpointTest::TearDown() {
    VBucketTest::TearDown();
}

void CheckpointTest::recreateCheckpointConfig() {
    checkpoint_config = CheckpointConfig(
            DEFAULT_CHECKPOINT_PERIOD,
            DEFAULT_CHECKPOINT_ITEMS,
            config.getMaxCheckpoints(),
            true /*itemNumBasedNewCheckpoint*/,
            (getVbType() == VBType::Persistent ? true
                                               : false) /*persistenceEnabled*/,
            config.getCheckpointRemovalMode() ==
                            ::to_string(CheckpointRemoval::Eager)
                    ? CheckpointRemoval::Eager
                    : CheckpointRemoval::Lazy);
}

void CheckpointTest::createManager(int64_t lastSeqno) {
    ASSERT_TRUE(vbucket);
    range = {0, 0};
    vbucket->checkpointManager = std::make_unique<MockCheckpointManager>(
            global_stats,
            *vbucket,
            checkpoint_config,
            lastSeqno,
            range.getStart(),
            range.getEnd(),
            lastSeqno, // setting maxVisibleSeqno to equal lastSeqno
            /*flusher callback*/ nullptr);

    ASSERT_TRUE(vbucket);
    manager = static_cast<MockCheckpointManager*>(
            vbucket->checkpointManager.get());
    ASSERT_TRUE(manager);

    // Set the proper test cursor
    if (persistent()) {
        cursor = manager->getPersistenceCursor();
    } else {
        cursor =
                manager->registerCursorBySeqno("test_cursor",
                                               0,
                                               CheckpointCursor::Droppable::Yes)
                        .cursor.lock()
                        .get();
    }
    ASSERT_TRUE(cursor);

    ASSERT_EQ(1, manager->getNumOfCursors());
    ASSERT_EQ(0, manager->getNumOpenChkItems());
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(0, manager->getNumItemsForCursor(cursor));
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

// Sanity check test fixture
TEST_P(CheckpointTest, CheckFixture) {
    // Initially have a single cursor (persistence).
    EXPECT_EQ(1, this->manager->getNumOfCursors());
    EXPECT_EQ(0, this->manager->getNumOpenChkItems());
    // Should initially be zero items to persist.
    EXPECT_EQ(0, this->manager->getNumItemsForCursor(cursor));

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForCursor(cursor, items);
    ASSERT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(0, result.ranges.front().getEnd());
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
    // 1x op_set
    EXPECT_EQ(1, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(20, qi->getRevSeqno());
    EXPECT_EQ(1, this->manager->getNumItemsForCursor(cursor));
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
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1002, qi2->getBySeqno());
    EXPECT_EQ(21, qi2->getRevSeqno());
    EXPECT_EQ(1, this->manager->getNumItemsForCursor(cursor));
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
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    EXPECT_EQ(1003, qi3->getBySeqno());
    EXPECT_EQ(0, qi3->getRevSeqno());
    EXPECT_EQ(2, this->manager->getNumItemsForCursor(cursor));
    EXPECT_EQ(1003, this->manager->getHighSeqno());
    EXPECT_EQ(1003, this->manager->getMaxVisibleSeqno());

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForCursor(cursor, items);
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
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
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_del
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(1001, this->manager->getHighSeqno());
    EXPECT_EQ(1001, this->manager->getMaxVisibleSeqno());
    EXPECT_EQ(10, qi->getRevSeqno());

    // Check that the items fetched matches what was enqueued.
    std::vector<queued_item> items;
    auto result = manager->getNextItemsForCursor(cursor, items);

    EXPECT_EQ(0, result.ranges.front().getStart());
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
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    const uint64_t ckpt_id1 = this->manager->getOpenCheckpointId();

    // Create a new checkpoint (closing the current open one).
    const uint64_t ckpt_id2 = this->manager->createNewCheckpoint();
    EXPECT_NE(ckpt_id1, ckpt_id2) << "New checkpoint ID should differ from old";
    EXPECT_EQ(ckpt_id1, this->manager->getLastClosedCheckpointId());
    EXPECT_EQ(0, this->manager->getNumOpenChkItems()); // no items yet

    // Add some items to the newly-opened checkpoint (note same keys as 1st
    // ckpt).
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    // Examine the items - should be 2 lots of two keys.
    EXPECT_EQ(4, manager->getNumItemsForCursor(cursor));

    // Check that the items fetched matches the number we were told to expect.
    std::vector<queued_item> items;
    auto result = manager->getNextItemsForCursor(cursor, items);
    EXPECT_EQ(2, result.ranges.size()); // 2 checkpoints returned items
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(1002, result.ranges.front().getEnd());
    EXPECT_EQ(1002, result.ranges.back().getStart());
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
TEST_P(CheckpointTest, getItems_MultipleSnapshots) {
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
    EXPECT_EQ(3, this->manager->getNumOpenChkItems());
    std::vector<queued_item> items;
    auto cursorResult = manager->getItemsForCursor(cursor, items, 1000);
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

// However different types of snapshot don't get combined
TEST_P(CheckpointTest, getItems_MemoryDiskSnapshots) {
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
    EXPECT_EQ(3, this->manager->getNumOpenChkItems());
    std::vector<queued_item> items;
    auto cursorResult = manager->getItemsForCursor(cursor, items, 1000);
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
    EXPECT_EQ(1003, cursorResult.visibleSeqno);
    EXPECT_FALSE(cursorResult.highCompletedSeqno);
}

// Test the automatic creation of checkpoints based on the number of items.
TEST_P(CheckpointTest, ItemBasedCheckpointCreation) {
    // Size down the default number of items to create a new checkpoint and
    // recreate the manager
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    // Create one less than the number required to create a new checkpoint.
    queued_item qi;
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_EQ(ii, this->manager->getNumOpenChkItems());

        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
    }

    // Add one more - should create a new checkpoint.
    EXPECT_TRUE(this->queueNewItem("key_epoch"));
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_set

    // Fill up this checkpoint also - note loop for MIN_CHECKPOINT_ITEMS - 1
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS - 1; ii++) {
        EXPECT_EQ(ii + 1,
                  this->manager->getNumOpenChkItems()); /* +1 initial set */

        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));

        EXPECT_EQ(2, this->manager->getNumCheckpoints());
    }

    // Add one more - as we have hit maximum checkpoints should *not* create a
    // new one.
    EXPECT_TRUE(this->queueNewItem("key_epoch2"));
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(11, // 1x key_epoch, 9x key_X, 1x key_epoch2
              this->manager->getNumOpenChkItems());

    // Fetch the items associated with the persistence cursor. This
    // moves the single cursor registered outside of the initial checkpoint,
    // allowing a new open checkpoint to be created.
    EXPECT_EQ(1, this->manager->getNumOfCursors());
    std::vector<queued_item> items;
    auto result = manager->getNextItemsForCursor(cursor, items);
    result.flushHandle.reset();

    EXPECT_EQ(2, result.ranges.size());
    EXPECT_EQ(0, result.ranges.at(0).getStart());
    EXPECT_EQ(1010, result.ranges.at(0).getEnd());
    EXPECT_EQ(1010, result.ranges.at(1).getStart());
    EXPECT_EQ(1021, result.ranges.at(1).getEnd());
    EXPECT_EQ(1010, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);
    EXPECT_EQ(24, items.size());

    // Should still have the same number of checkpoints and open items.
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(11, this->manager->getNumOpenChkItems());

    // But adding a new item will create a new one.
    EXPECT_TRUE(this->queueNewItem("key_epoch3"));
    EXPECT_EQ(3, this->manager->getNumCheckpoints());
    EXPECT_EQ(1, this->manager->getNumOpenChkItems()); // 1x op_set
}

// Test checkpoint and cursor accounting - when checkpoints are closed the
// offset of cursors is updated as appropriate.
TEST_P(CheckpointTest, CursorOffsetOnCheckpointClose) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    // 2x op_set
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    // Use the existing persistence cursor for this test:
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor))
            << "Cursor should initially have two items pending";

    // Check de-dupe counting - after adding another item with the same key,
    // should still see two items.
    EXPECT_FALSE(this->queueNewItem("key1")) << "Adding a duplicate key to "
                                                "open checkpoint should not "
                                                "increase queue size";

    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor))
            << "Expected 2 items for cursor (2x op_set) after adding a "
               "duplicate.";

    // Create a new checkpoint (closing the current open one).
    this->manager->createNewCheckpoint();
    EXPECT_EQ(0, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor))
            << "Expected 2 items for cursor after creating new checkpoint";

    // Advance persistence cursor - first to get the 'checkpoint_start' meta
    // item, and a second time to get the a 'proper' mutation.
    bool isLastMutationItem;
    auto item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor))
            << "Expected 2 items for cursor after advancing one item";

    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    EXPECT_EQ(1, manager->getNumItemsForCursor(cursor))
            << "Expected 1 item for cursor after advancing by 1";

    // Add two items to the newly-opened checkpoint. Same keys as 1st ckpt,
    // but cannot de-dupe across checkpoints.
    for (auto ii : {1,2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    EXPECT_EQ(3, manager->getNumItemsForCursor(cursor))
            << "Expected 3 items for cursor after adding 2 more to new "
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

    // Both previous checkpoints are unreferenced. Close them. This will
    // cause the offset of this cursor to be recalculated.
    EXPECT_EQ(2, manager->removeClosedUnrefCheckpoints().count);

    EXPECT_EQ(1, this->manager->getNumCheckpoints());

    EXPECT_EQ(2, this->manager->getNumItemsForCursor(cursor));

    // Drain the remaining items.
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_FALSE(isLastMutationItem);
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(item->isCheckPointMetaItem());
    EXPECT_TRUE(isLastMutationItem);

    EXPECT_EQ(0, manager->getNumItemsForCursor(cursor));
}

// Test the getNextItemsForCursor()
TEST_P(CheckpointTest, ItemsForCheckpointCursor) {
    // We want to have items across 2 checkpoints. Size down the default number
    // of items to create a new checkpoint and recreate the manager.
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    /* Add items such that we have 2 checkpoints */
    queued_item qi;
    for (unsigned int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Check if we have desired number of checkpoints and desired number of
       items */
    EXPECT_EQ(2, this->manager->getNumCheckpoints());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    /* Get items for persistence*/
    std::vector<queued_item> items;
    auto result = this->manager->getNextItemsForCursor(cursor, items);

    /* We should have got (2 * MIN_CHECKPOINT_ITEMS + 3) items. 3 additional are
       op_ckpt_start, op_ckpt_end and op_ckpt_start */
    EXPECT_EQ(2 * MIN_CHECKPOINT_ITEMS + 3, items.size());
    EXPECT_EQ(2, result.ranges.size());
    EXPECT_EQ(0, result.ranges.at(0).getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.at(0).getEnd());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.at(1).getStart());
    EXPECT_EQ(1000 + 2 * MIN_CHECKPOINT_ITEMS, result.ranges.at(1).getEnd());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);

    /* Get items for DCP replication cursor */
    items.clear();
    result = this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(),
                                                  items);
    EXPECT_EQ(2 * MIN_CHECKPOINT_ITEMS + 3, items.size());
    EXPECT_EQ(2, result.ranges.size());
    EXPECT_EQ(0, result.ranges.at(0).getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.at(0).getEnd());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.at(1).getStart());
    EXPECT_EQ(1000 + 2 * MIN_CHECKPOINT_ITEMS, result.ranges.at(1).getEnd());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.visibleSeqno);
    EXPECT_FALSE(result.highCompletedSeqno);
}

// Test getNextItemsForCursor() when it is limited to fewer items than exist
// in total. Cursor should only advanced to the start of the 2nd checkpoint.
TEST_P(CheckpointTest, ItemsForCheckpointCursorLimited) {
    /* We want to have items across 2 checkpoints. Size down the default number
       of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    /* Add items such that we have 2 checkpoints */
    queued_item qi;
    for (unsigned int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ii++) {
        ASSERT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Verify we have desired number of checkpoints and desired number of
       items */
    ASSERT_EQ(2, this->manager->getNumCheckpoints());
    ASSERT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Get items for persistence. Specify a limit of 1 so we should only
     * fetch the first checkpoints' worth.
     */
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(cursor, items, 1);
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getEnd());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 2, items.size())
            << "Should have MIN_CHECKPOINT_ITEMS + 2 (ckpt start & end) items";
    EXPECT_EQ(2, cursor->getId())
            << "Cursor should have moved into second checkpoint.";
}

// Limit returned to flusher is strict for Disk checkpoints
TEST_P(CheckpointTest, DiskCheckpointStrictItemLimit) {
    // Test only relevant for persistent buckets as it relates to the
    // persistence cursor
    if (!persistent()) {
        return;
    }
    /* We want to have items across 2 checkpoints. Size down the default number
      of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    // Force the first checkpoint to be a disk one
    this->manager->createSnapshot(this->manager->getOpenSnapshotStartSeqno(),
                                  0,
                                  0 /*highCompletedSeqno*/,
                                  CheckpointType::Disk,
                                  0);

    /* Add items such that we have 2 checkpoints */
    queued_item qi;
    for (unsigned int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ii++) {
        ASSERT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Verify we have desired number of checkpoints and desired number of
       items */
    ASSERT_EQ(2, this->manager->getNumCheckpoints());
    ASSERT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Get items for persistence. Specify a limit of 1 so we should only
     * fetch the first item
     */
    std::vector<queued_item> items;
    auto result = manager->getItemsForCursor(cursor, items, 1);
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getEnd());
    EXPECT_EQ(1, items.size()) << "Should have 1 item";
    EXPECT_EQ(1, cursor->getId())
            << "Cursor should not have moved into second checkpoint.";
}

// Test the checkpoint cursor movement
TEST_P(CheckpointTest, CursorMovement) {
    /* We want to have items across 2 checkpoints. Size down the default number
     of items to create a new checkpoint and recreate the manager */
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    /* Add items such that we have 1 full (max items as per config) checkpoint.
       Adding another would open new checkpoint */
    queued_item qi;
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /* Check if we have desired number of checkpoints and desired number of
       items */
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS, this->manager->getNumOpenChkItems());

    /* Register DCP replication cursor */
    std::string dcp_cursor(DCP_CURSOR_PREFIX + std::to_string(1));
    auto dcpCursor = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 0, CheckpointCursor::Droppable::Yes);

    /* Get items for persistence cursor */
    std::vector<queued_item> items;
    auto result = manager->getNextItemsForCursor(cursor, items);
    result.flushHandle.reset();

    /* We should have got (MIN_CHECKPOINT_ITEMS + op_ckpt_start) items. */
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 1, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getEnd());

    /* Get items for DCP replication cursor */
    items.clear();
    result = this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(),
                                                  items);
    EXPECT_EQ(MIN_CHECKPOINT_ITEMS + 1, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.ranges.front().getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getEnd());

    uint64_t curr_open_chkpt_id = this->manager->getOpenCheckpointId();

    /* Run the checkpoint remover so that new open checkpoint is created */
    manager->removeClosedUnrefCheckpoints();
    EXPECT_EQ(curr_open_chkpt_id + 1, this->manager->getOpenCheckpointId());

    /* Get items for persistence cursor */
    EXPECT_EQ(0, manager->getNumItemsForCursor(cursor))
            << "Expected to have no normal (only meta) items";
    items.clear();
    result = manager->getNextItemsForCursor(cursor, items);

    /* We should have got op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getStart());
    EXPECT_EQ(1000 + MIN_CHECKPOINT_ITEMS, result.ranges.front().getEnd());

    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());

    /* Get items for DCP replication cursor */
    EXPECT_EQ(0, manager->getNumItemsForCursor(cursor))
            << "Expected to have no normal (only meta) items";
    items.clear();
    this->manager->getNextItemsForCursor(dcpCursor.cursor.lock().get(), items);
    /* Expecting only 1 op_ckpt_start item */
    EXPECT_EQ(1, items.size());
    EXPECT_EQ(queue_op::checkpoint_start, items.at(0)->getOperation());
}

// MB-25056 - Regression test replicating situation where the seqno returned by
// registerCursorBySeqno minus one is greater than the input parameter
// startBySeqno but a backfill is not required.
TEST_P(CheckpointTest, MB25056_backfill_not_required) {
    std::vector<queued_item> items;
    this->vbucket->setState(vbucket_state_replica);

    ASSERT_TRUE(this->queueNewItem("key0"));
    // Add duplicate items, which should cause de-duplication to occur.
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_FALSE(this->queueNewItem("key0"));
    }
    // Add a number of non duplicate items to the same checkpoint
    for (unsigned int ii = 1; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Register DCP replication cursor
    std::string dcp_cursor(DCP_CURSOR_PREFIX);
    // Request to register the cursor with a seqno that has been de-duped away
    CursorRegResult result = manager->registerCursorBySeqno(
            dcp_cursor.c_str(), 1005, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1011, result.seqno) << "Returned seqno is not expected value.";
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

    // configure so we can store a large number of items
    // configure with 1 checkpoint to ensure the time-based closing
    // does not split the items over many checkpoints and muddy the final
    // data checks.
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             n_threads * n_items,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

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
    manager->getNextItemsForCursor(cursor, items);

    /* We should have got (n_threads*n_items + op_ckpt_start) items. */
    EXPECT_EQ(n_threads * n_items + 1, items.size());

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
    manager->getNextItemsForCursor(cursor, items);
    ASSERT_EQ(3, items.size());
    ASSERT_EQ(0, manager->getNumItemsForCursor(cursor));

    // Queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, manager->getNumItemsForCursor(cursor));

    // Should have another item to read (new version of 'key')
    items.clear();
    manager->getNextItemsForCursor(cursor, items);
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
    manager->getNextItemsForCursor(cursor, items);
    ASSERT_EQ(2, items.size());
    ASSERT_EQ(0, manager->getNumItemsForCursor(cursor));

    // Queue a set (cursor will now be one behind).
    ASSERT_TRUE(this->queueNewItem("key"));
    ASSERT_EQ(1, manager->getNumItemsForCursor(cursor));

    // Test: queue an item with a duplicate key.
    this->queueNewItem("key");

    // Test: Should have one item for cursor (the one we just added).
    EXPECT_EQ(1, manager->getNumItemsForCursor(cursor));

    // Should an item to read (new version of 'key')
    items.clear();
    manager->getNextItemsForCursor(cursor, items);
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
    // We must have only the checkpoint-open and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(ckptList.back()->getNumItems(), 0);
    EXPECT_EQ(ckptMgr->getNumItems(), 2);

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

    // We must have 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);

    // 2) the consumer receives the mutations until (snapshotEnd -1)
    processMutations(*passiveStream, snapshotStart, snapshotEnd - 1);

    // We must have again 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);
    EXPECT_EQ(snapshotEnd - 1, ckptMgr->getNumOpenChkItems());

    // 3) the consumer receives the snapshotEnd mutation
    processMutations(*passiveStream, snapshotEnd, snapshotEnd);

    // We must have again 1 open checkpoint with id=1
    EXPECT_EQ(ckptList.size(), 1);
    EXPECT_EQ(ckptList.back()->getState(), checkpoint_state::CHECKPOINT_OPEN);
    EXPECT_EQ(ckptList.back()->getId(), 1);
    EXPECT_EQ(snapshotEnd, ckptMgr->getNumOpenChkItems());

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
    EXPECT_EQ(ckptList.back()->getId(), 2);
    EXPECT_EQ(0, ckptMgr->getNumOpenChkItems());

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
    // We must have only the checkpoint-open and the vbucket-state
    // meta-items in the open checkpoint
    EXPECT_EQ(ckptMgr->getNumItems(), 2);
    EXPECT_EQ(ckptMgr->getNumOpenChkItems(), 0);

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

        EXPECT_EQ(openCheckpointSize, ckptMgr->getNumOpenChkItems());
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

    // We must have exactly (snapshotEnd - snapshotStart) items in the
    // checkpoint
    EXPECT_EQ(openCheckpointSize, ckptMgr->getNumOpenChkItems());

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
    // that store all mutations
    EXPECT_EQ(openedCheckPoints, ckptMgr->getNumCheckpoints());
    EXPECT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.back()->getState());
    EXPECT_EQ(ckptList.back()->getNumItems(), snapshotEnd);

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
    const auto expected = cmQuota / store->getVBMapSize() / maxCheckpoints;
    EXPECT_EQ(expected, store->getCheckpointMaxSize());
}

TEST_F(SingleThreadedCheckpointTest, MemUsageCheckpointCreation) {
    auto& config = engine->getConfiguration();
    config.setMaxSize(1024 * 1024 * 100);

    // Disable item/time based checkpoint creation
    config.setItemNumBasedNewChk(false);
    config.setChkPeriod(3600);
    config.setMaxCheckpoints(20);
    // Note: This test also verifies that a value > 0 is just set (*)
    const uint32_t _10MB = 1024 * 1024 * 10;
    config.setCheckpointMaxSize(_10MB);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    ASSERT_FALSE(ckptConfig.isItemNumBasedNewCheckpoint());
    ASSERT_EQ(3600, ckptConfig.getCheckpointPeriod());
    ASSERT_EQ(20, ckptConfig.getMaxCheckpoints());
    ASSERT_EQ(_10MB, store->getCheckpointMaxSize()); // (*)

    ASSERT_EQ(1, manager.getNumCheckpoints());

    const size_t numItems = 5;
    const std::string value(_10MB, '!');
    for (size_t i = 1; i <= numItems; ++i) {
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

    // Disable item/time based checkpoint creation
    config.setItemNumBasedNewChk(false);
    config.setChkPeriod(3600);
    config.setMaxCheckpoints(20);
    // Set checkpoint max size to something very low
    config.setCheckpointMaxSize(1);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = *vb->checkpointManager;

    const auto& ckptConfig = manager.getCheckpointConfig();
    ASSERT_FALSE(ckptConfig.isItemNumBasedNewCheckpoint());
    ASSERT_EQ(3600, ckptConfig.getCheckpointPeriod());
    ASSERT_EQ(20, ckptConfig.getMaxCheckpoints());
    ASSERT_EQ(1, store->getCheckpointMaxSize());

    // 1 empty checkpoint
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Value is much bigger than the checkpoint max size
    const std::string value(1024, '!');
    store_item(vbid, makeStoredDocKey("key1"), value);

    // Still, the item must be queued in the existing open checkpoint (ie, we
    // must not create another checkpoint).
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems());

    // The next store must create a new checkpoint
    store_item(vbid, makeStoredDocKey("key2"), value);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems());
}

// Test that when the same client registers twice, the first cursor 'dies'
TEST_P(CheckpointTest, reRegister) {
    auto dcpCursor1 = manager->registerCursorBySeqno(
            "name", 0, CheckpointCursor::Droppable::Yes);
    EXPECT_NE(nullptr, dcpCursor1.cursor.lock().get());
    auto dcpCursor2 = this->manager->registerCursorBySeqno(
            "name", 0, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(nullptr, dcpCursor1.cursor.lock().get());
    EXPECT_NE(nullptr, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(2, this->manager->getNumOfCursors());
}

TEST_P(CheckpointTest, takeAndResetCursors) {
    auto dcpCursor1 = manager->registerCursorBySeqno(
            "name1", 0, CheckpointCursor::Droppable::Yes);
    auto dcpCursor2 = manager->registerCursorBySeqno(
            "name2", 0, CheckpointCursor::Droppable::Yes);
    auto dcpCursor3 = manager->registerCursorBySeqno(
            "name3", 0, CheckpointCursor::Droppable::Yes);

    EXPECT_EQ(0, manager->getNumItemsForCursor(cursor));
    this->queueNewItem("key");

    const auto* c1 = dcpCursor1.cursor.lock().get();
    const auto* c2 = dcpCursor2.cursor.lock().get();
    const auto* c3 = dcpCursor3.cursor.lock().get();
    EXPECT_NE(nullptr, c1);
    EXPECT_NE(nullptr, c2);
    EXPECT_NE(nullptr, c3);
    EXPECT_EQ(4, this->manager->getNumOfCursors());
    EXPECT_EQ(1, manager->getNumItemsForCursor(cursor)); // +key
    EXPECT_EQ(1,
              this->manager->getNumItemsForCursor(
                      dcpCursor2.cursor.lock().get()));

    // Second manager
    auto manager2 = std::make_unique<MockCheckpointManager>(
            this->global_stats,
            *vbucket,
            this->checkpoint_config,
            0,
            0 /*lastSnapStart*/,
            0 /*lastSnapEnd*/,
            0 /*maxVisible*/,
            nullptr /*persistence callback*/);

    manager2->takeAndResetCursors(*this->manager);

    EXPECT_EQ(c1, dcpCursor1.cursor.lock().get());
    EXPECT_EQ(c2, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(c3, dcpCursor3.cursor.lock().get());

    EXPECT_EQ(4, manager2->getNumOfCursors());
    EXPECT_EQ(0, this->manager->getNumOfCursors());

    // Destroy first checkpoint manager
    resetManager();

    EXPECT_EQ(c1, dcpCursor1.cursor.lock().get());
    EXPECT_EQ(c2, dcpCursor2.cursor.lock().get());
    EXPECT_EQ(c3, dcpCursor3.cursor.lock().get());

    EXPECT_EQ(4, manager2->getNumOfCursors());
    // Cursors move, but checkpoints don't
    EXPECT_EQ(0, manager2->getNumItemsForCursor(cursor));
    EXPECT_EQ(0,
              manager2->getNumItemsForCursor(dcpCursor2.cursor.lock().get()));

    EXPECT_EQ(1, manager2->getNumCheckpoints());
    EXPECT_EQ(
            4,
            manager2->getCheckpointList().front()->getNumCursorsInCheckpoint());
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
    // Size down the default number of items to create a new checkpoint and
    // recreate the manager
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             MIN_CHECKPOINT_ITEMS,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
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
    for (int ii = 0; ii < 2 * MIN_CHECKPOINT_ITEMS; ++ii) {
        this->queueNewItem("key" + std::to_string(ii));
    }
    EXPECT_EQ(2, ckptList.size());
    EXPECT_EQ(2, ckptList.front()->getNumCursorsInCheckpoint());

    // Register a 2nd DCP cursor with the same name but this time into the
    // 2nd checkpoint
    auto dcpCursor2 =
            manager->registerCursorBySeqno(dcp_cursor.c_str(),
                                           1000 + MIN_CHECKPOINT_ITEMS + 2,
                                           CheckpointCursor::Droppable::Yes);

    // Adding the 2nd DCP cursor should not have increased the number of
    // cursors as the previous cursor will have been removed when the new one
    // was added.  The persistence cursor will still be in the first
    // checkpoint however the dcpCursor will have been deleted from the first
    // checkpoint and adding to the 2nd checkpoint.
    EXPECT_EQ(1, ckptList.front()->getNumCursorsInCheckpoint());
    EXPECT_EQ(1, ckptList.back()->getNumCursorsInCheckpoint());
}

// Test that when adding duplicate queued_items (of the same size) it
// does not increase the size of the checkpoint.
TEST_P(CheckpointTest, dedupeMemoryTest) {
    // Get the intial size of the checkpoint.
    auto memoryUsage1 = this->manager->getMemoryUsage();

    ASSERT_TRUE(this->queueNewItem("key0"));

    // Get checkpoint size again after adding a queued_item.
    auto memoryUsage2 = this->manager->getMemoryUsage();
    EXPECT_LT(memoryUsage1, memoryUsage2);

    // Add duplicate items, which should cause de-duplication to occur
    // and so the checkpoint should not increase in size
    for (auto ii = 0; ii < MIN_CHECKPOINT_ITEMS; ++ii) {
        EXPECT_FALSE(this->queueNewItem("key0"));
    }

    // Get checkpoint size again after adding duplicate items.
    auto memoryUsage3 = this->manager->getMemoryUsage();
    EXPECT_EQ(memoryUsage2, memoryUsage3);

    // Add a number of non duplicate items to the same checkpoint so the
    // checkpoint should increase in size.
    for (auto ii = 1; ii < MIN_CHECKPOINT_ITEMS; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    // Get checkpoint size again after adding non-duplicate items.
    auto memoryUsage4 = this->manager->getMemoryUsage();
    EXPECT_LT(memoryUsage3, memoryUsage4);
}

// Test that the checkpoint memory stat is correctly maintained when
// de-duplication occurs and also when the checkpoint containing the
// mutation is removed.
TEST_P(CheckpointTest, checkpointMemoryTest) {
    // Get the intial size of the checkpoint.
    auto initialSize = this->manager->getMemoryUsage();

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
                    *this->manager)
                    .begin();
    IndexEntry entry{iterator};

    // Create a queued_item with a 'small' value
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey("key"),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    manager->queueDirty(qiSmall,
                        GenerateBySeqno::Yes,
                        GenerateCas::Yes,
                        /*preLinkDocCtx*/ nullptr);

    // The queue (toWrite) is implemented as std:list, therefore
    // when we add an item it results in the creation of 3 pointers -
    // forward ptr, backward ptr and ptr to object.
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);

    // Check that checkpoint size is the initial size plus the addition of
    // qiSmall.
    auto expectedSize = initialSize;
    // Add the size of the item
    expectedSize += qiSmall->size();
    // Add the size of adding to the queue
    expectedSize += perElementOverhead;
    // Add to the emulated keyIndex
    keyIndex.emplace(CheckpointIndexKeyType(qiSmall->getKey(),
                                            keyIndexKeyTrackingAllocator),
                     entry);

    auto keyIndexSize = keyIndex.get_allocator().getBytesAllocated();
    expectedSize += (keyIndexSize - initialKeyIndexSize);

    EXPECT_EQ(expectedSize, this->manager->getMemoryUsage());

    // Create a queued_item with a 'big' value
    std::string bigValue(1024, 'a');
    queued_item qiBig(new Item(makeStoredDocKey("key"),
                               0,
                               0,
                               bigValue.c_str(),
                               bigValue.size(),
                               PROTOCOL_BINARY_RAW_BYTES,
                               0,
                               -1,
                               Vbid(0)));

    // Add the queued_item to the checkpoint
    manager->queueDirty(qiBig,
                        GenerateBySeqno::Yes,
                        GenerateCas::Yes,
                        /*preLinkDocCtx*/ nullptr);

    // Check that checkpoint size is the initial size plus the addition of
    // qiBig.
    expectedSize = initialSize;
    // Add the size of the item
    expectedSize += qiBig->size();
    // Add the size of adding to the queue
    expectedSize += perElementOverhead;
    // Add to the keyIndex
    keyIndex.emplace(CheckpointIndexKeyType(qiBig->getKey(),
                                            keyIndexKeyTrackingAllocator),
                     entry);

    keyIndexSize = keyIndex.get_allocator().getBytesAllocated();
    expectedSize += (keyIndexSize - initialKeyIndexSize);

    EXPECT_EQ(expectedSize, this->manager->getMemoryUsage());

    bool isLastMutationItem;
    // Move cursor to checkpoint start
    auto item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(isLastMutationItem);
    // Move cursor to the mutation
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(isLastMutationItem);

    // Create a new checkpoint, which will close the old checkpoint
    // and move the persistence cursor to the new checkpoint.
    this->manager->createNewCheckpoint();

    // We are now in a position to remove the checkpoint that had the
    // mutation in it.
    EXPECT_EQ(1, manager->removeClosedUnrefCheckpoints().count);

    // Should be back to the initialSize
    EXPECT_EQ(initialSize, this->manager->getMemoryUsage());
}

// Test the tracking of memory overhead by adding a single element to the
// CheckpointQueue.
TEST_P(CheckpointTest, checkpointTrackingMemoryOverheadTest) {
    // Get the intial size of the checkpoint overhead.
    const auto initialOverhead = this->manager->getMemoryOverhead();

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
                    *this->manager)
                    .begin();
    IndexEntry entry{iterator};

    // Create a queued_item
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey("key"),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    manager->queueDirty(qiSmall,
                        GenerateBySeqno::Yes,
                        GenerateCas::Yes,
                        /*preLinkDocCtx*/ nullptr);

    // Re-measure the checkpoint overhead
    const auto updatedOverhead = this->manager->getMemoryOverhead();
    // Three pointers - forward, backward and pointer to item
    const auto perElementListOverhead = sizeof(uintptr_t) * 3;
    // Add entry into keyIndex
    keyIndex.emplace(CheckpointIndexKeyType(qiSmall->getKey(),
                                            keyIndexKeyTrackingAllocator),
                     entry);

    const auto keyIndexSize = keyIndex.get_allocator().getBytesAllocated();
    EXPECT_EQ(perElementListOverhead + (keyIndexSize - initialKeyIndexSize),
              updatedOverhead - initialOverhead);

    bool isLastMutationItem;
    // Move cursor to checkpoint start
    auto item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_FALSE(isLastMutationItem);
    // Move cursor to the mutation
    item = manager->nextItem(cursor, isLastMutationItem);
    EXPECT_TRUE(isLastMutationItem);

    // Create a new checkpoint, which will close the old checkpoint
    // and move the persistence cursor to the new checkpoint.
    this->manager->createNewCheckpoint();

    // We are now in a position to remove the checkpoint that had the
    // mutation in it.
    EXPECT_EQ(1, manager->removeClosedUnrefCheckpoints().count);

    // Should be back to the initialOverhead
    EXPECT_EQ(initialOverhead, this->manager->getMemoryOverhead());
}

TEST_P(CheckpointTest, checkpointTrackingMemoryOverheadHeapAllocatedKeyTest) {
    // Get the intial size of the checkpoint overhead.
    const auto initialOverhead = this->manager->getMemoryOverhead();

    // Create a queued_item with a big key. This size is an order of magnitude
    // bigger than our key index should be when it's empty so we can just check
    // if the overhead is at least this size to verify that we track key
    // allocations.
    auto keySize = 2000;
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey(std::string(keySize, 'x')),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    manager->queueDirty(qiSmall,
                        GenerateBySeqno::Yes,
                        GenerateCas::Yes,
                        /*preLinkDocCtx*/ nullptr);

    auto overhead = this->manager->getMemoryOverhead() - initialOverhead;
    EXPECT_LT(keySize, overhead);
}

/**
 * MB-35589: We do not add keys to the indexes of Disk Checkpoints.
 *
 * Disk Checkpoints do not maintain a key index in the same way that Memory
 * Checkpoints do as we don't expect to perform de-duplication or de-duplication
 * sanity checks. This is also necessary as we cannot let a Disk Checkpoint
 * grow memory usage (after expelling) in a O(n) manner for heavy DGM use cases
 * as we would use a lot of memory for key indexes. As such, test that we don't
 * add keys to the indexes of Disk Checkpoints by measuring memory usage.
 */
TEST_P(CheckpointTest, checkpointTrackingMemoryOverheadDiskCheckpointTest) {
    // Create checkpoint of type Disk
    this->manager->createSnapshot(0, 1000, 1000, CheckpointType::Disk, 1001);

    // Get the intial size of the checkpoint overhead.
    const auto initialOverhead = this->manager->getMemoryOverhead();

    auto keySize = 2000;
    std::string value("value");
    queued_item qiSmall(new Item(makeStoredDocKey(std::string(keySize, 'x')),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 Vbid(0)));

    // Add the queued_item to the checkpoint
    manager->queueDirty(qiSmall,
                        GenerateBySeqno::Yes,
                        GenerateCas::Yes,
                        /*preLinkDocCtx*/ nullptr);

    // The queue (toWrite) is implemented as std:list, therefore when we add an
    // item it results in the creation of 3 pointers - forward ptr, backward ptr
    // and ptr to object. This is tracked under memoryOverhead.
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);
    EXPECT_EQ(initialOverhead + perElementOverhead,
              this->manager->getMemoryOverhead());
}

// Test that can expel items and that we have the correct behaviour when we
// register cursors for items that have been expelled.
void CheckpointTest::testExpelCheckpointItems() {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, manager->getNumItemsForCursor(cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     */

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_LT(0, expelResult.memory);
    EXPECT_EQ(1, this->global_stats.itemsExpelledFromCheckpoints);

    /*
     * We have expelled:
     * 1001 - 1st item (key 0)
     *
     * Now the checkpoint looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key 2)
     */
    if (persistent()) {
        EXPECT_EQ(1002, (*manager->getPersistenceCursorPos())->getBySeqno());
    }

    // The full checkpoint still contains the 3 items added.
    EXPECT_EQ(itemCount, this->manager->getNumOpenChkItems());

    // Try to register a DCP replication cursor from 1001 - an expelled item.
    std::string dcp_cursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult = manager->registerCursorBySeqno(
            dcp_cursor1.c_str(), 1001, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1002, regResult.seqno);
    EXPECT_TRUE(regResult.tryBackfill);

    // Try to register a DCP replication cursor from 1002 - the first valid
    // in-checkpoint item.
    std::string dcp_cursor2(DCP_CURSOR_PREFIX + std::to_string(2));
    regResult = manager->registerCursorBySeqno(
            dcp_cursor2.c_str(), 1002, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1003, regResult.seqno);
    EXPECT_FALSE(regResult.tryBackfill);

    // Try to register a DCP replication cursor from 1003
    std::string dcp_cursor3(DCP_CURSOR_PREFIX + std::to_string(3));
    regResult = manager->registerCursorBySeqno(
            dcp_cursor3.c_str(), 1003, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(1004, regResult.seqno);
    EXPECT_FALSE(regResult.tryBackfill);
}

TEST_P(CheckpointTest, testExpelCheckpointItemsMemory) {
    testExpelCheckpointItems();
}

TEST_P(CheckpointTest, testExpelCheckpointItemsDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
    testExpelCheckpointItems();
}

// Test that we correctly handle duplicates, where the initial version of the
// document has been expelled.
TEST_P(CheckpointTest, expelCheckpointItemsWithDuplicateTest) {
    const int itemCount{3};

    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, this->manager->getNumItemsForCursor(cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_LT(0, expelResult.memory);
    EXPECT_EQ(1, this->global_stats.itemsExpelledFromCheckpoints);

    // Item count doens't change
    EXPECT_EQ(3, this->manager->getNumOpenChkItems());

    /*
     * After expelling checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1000 - checkpoint_start
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key 2)
     */
    if (persistent()) {
        EXPECT_EQ(1002, (*manager->getPersistenceCursorPos())->getBySeqno());
    }

    // Add another item which has been expelled.
    // Should not find the duplicate and so will re-add.
    EXPECT_TRUE(this->queueNewItem("key0"));

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1000 - checkpoint_start
     * 1002 - 2nd item (key1) <<<<<<< persistenceCursor
     * 1003 - 3rd item (key2)
     * 1004 - 4th item (key0)  << The New item added >>
     */

    // The full checkpoint still contains the 3 unique items added. The second
    // add for key0 de-dupes the first for key0 so we don't bump the count. This
    // mimics normal behaviour for an item de-duping an earlier one in a
    // checkpoint when there is no expelling going on.
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
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, manager->getNumItemsForCursor(cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < itemCount + 1; ++ii) {
        auto item = this->manager->nextItem(
                this->manager->getPersistenceCursor(), isLastMutationItem);
    }

    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1000 - checkpoint start
     * 1001 - 1st item
     * 1002 - 2nd item  <<<<<<< persistenceCursor
     */

    // Only expel seqno 1001 - the cursor points to item that
    // has the highest seqno for the checkpoint so we move the expel point back
    // one. That item isn't a metadata item nor is it's successor item
    // (1002) the same seqno as itself (1001) so can expel from there.
    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_GT(expelResult.memory, 0);
    EXPECT_EQ(1, this->global_stats.itemsExpelledFromCheckpoints);
}

TEST_P(CheckpointTest, testExpelCursorPointingToLastItemMemory) {
    testExpelCursorPointingToLastItem();
}

TEST_P(CheckpointTest, testExpelCursorPointingToLastItemDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
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

TEST_P(CheckpointTest, testExpelCursorPointingToChkptStartMemory) {
    testExpelCursorPointingToChkptStart();
}

TEST_P(CheckpointTest, testExpelCursorPointingToChkptStartDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
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
    auto item = manager->nextItem(regResult.cursor.lock().get(),
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
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
    testDontExpelIfCursorAtMetadataItemWithSameSeqno();
}

// Test that if we have a item after a mutation with the same seqno
// then we will move the expel point backwards to the mutation
// (and possibly further).
void CheckpointTest::testDoNotExpelIfHaveSameSeqnoAfterMutation() {
    this->checkpoint_config =
            CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                             /*maxItemsInCheckpoint*/ 1,
                             /*numCheckpoints*/ 2,
                             /*itemBased*/ true,
                             persistent() /*persistenceEnabled*/,
                             CheckpointRemoval::Lazy);
    createManager();

    // Add a meta data operation
    manager->queueSetVBState();

    const int itemCount{2};
    for (auto ii = 0; ii < itemCount; ++ii) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(ii)));
    }

    /*
     * First checkpoint (closed) is as follows:
     * 1000 - dummy item   <<<<<<< Cursor
     * 1001 - checkpoint start
     * 1001 - set VB state
     * 1001 - mutation
     * 1001 - checkpoint end
     *
     * Second checkpoint (open) is as follows:
     * 1001 - dummy item
     * 1002 - checkpoint start
     * 1002 - mutation
     */

    // Move the cursor to the second mutation.
    bool isLastMutationItem{false};
    for (auto ii = 0; ii < 6; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
    }

    std::string dcpCursor1(DCP_CURSOR_PREFIX + std::to_string(1));
    CursorRegResult regResult = manager->registerCursorBySeqno(
            dcpCursor1.c_str(), 1000, CheckpointCursor::Droppable::Yes);

    // Move the dcp cursor to the checkpoint end.
    for (auto ii = 0; ii < 4; ++ii) {
        auto item = this->manager->nextItem(regResult.cursor.lock().get(),
                                            isLastMutationItem);
    }

    /*
     * First checkpoint (closed) is as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - set VB state
     * 1001 - mutation
     * 1001 - checkpoint end  <<<<<<< dcpCursor1
     *
     * Second checkpoint (open) is as follows:
     * 1001 - dummy item
     * 1002 - checkpoint start
     * 1002 - mutation   <<<<<<< Cursor
     */

    // We should not expel any items due to dcpCursor1 as we end up
    // moving the expel point back to the dummy item.
    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.count);
    EXPECT_EQ(0, expelResult.memory);
    EXPECT_EQ(0, this->global_stats.itemsExpelledFromCheckpoints);
}

TEST_P(CheckpointTest, testDoNotExpelIfHaveSameSeqnoAfterMutationMemory) {
    testDoNotExpelIfHaveSameSeqnoAfterMutation();
}

TEST_P(CheckpointTest, testDoNotExpelIfHaveSameSeqnoAfterMutationDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
    testDoNotExpelIfHaveSameSeqnoAfterMutation();
}

// Test estimate for the amount of memory recovered by expelling is correct.
void CheckpointTest::testExpelCheckpointItemsMemoryRecovered() {
    const int itemCount{3};
    size_t sizeOfItem{0};

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

    ASSERT_EQ(1, this->manager->getNumCheckpoints()); // Single open checkpoint.
    ASSERT_EQ(itemCount, this->manager->getNumOpenChkItems());
    ASSERT_EQ(itemCount, manager->getNumItemsForCursor(cursor));
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
    const auto checkpointMemoryUsageBeforeExpel =
            this->manager->getMemoryUsage();

    const auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_EQ(1, global_stats.itemsExpelledFromCheckpoints);

    /*
     * We have expelled:
     * 1001 - 1st item (key 0)
     *
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start
     * 1002 - 2nd item (key 1) <<<<<<< Cursor
     * 1003 - 3rd item (key 2)
     */
    if (persistent()) {
        EXPECT_EQ(1002, (*manager->getPersistenceCursorPos())->getBySeqno());
    }

    // In our code the per-list-element memory overhead is computed as of 3
    const size_t checkpointListSaving =
            (3 * sizeof(uintptr_t)) * expelResult.count;
    // List saving + 1 mutation
    const size_t expectedMemoryRecovered = checkpointListSaving + sizeOfItem;

    EXPECT_EQ(expectedMemoryRecovered, expelResult.memory);
    EXPECT_EQ(expectedMemoryRecovered,
              checkpointMemoryUsageBeforeExpel - manager->getMemoryUsage());
}

TEST_P(CheckpointTest, testExpelCheckpointItemsMemoryRecoveredMemory) {
    testExpelCheckpointItemsMemoryRecovered();
}

TEST_P(CheckpointTest, testExpelCheckpointItemsMemoryRecoveredDisk) {
    manager->createSnapshot(0, 1000, 0, CheckpointType::Disk, 1001);
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
    cm.registerCursorBySeqno(
            "test_cursor_name", 0, CheckpointCursor::Droppable::Yes);
    EXPECT_EQ(2, checkpointList.front()->getNumCursorsInCheckpoint());

    // first snapshot received
    cm.createSnapshot(1, 10, {/* hcs */}, CheckpointType::Memory, 10);
    EXPECT_EQ(1, checkpointList.size());
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
    cm.forceNewCheckpoint();

    // Test: Iterate on all items and check that the seqnos are weakly monotonic
    auto regRes = cm.registerCursorBySeqno(
            "Cursor", 0, CheckpointCursor::Droppable::Yes);
    auto cursor = regRes.cursor.lock();
    std::vector<queued_item> items;
    cm.getItemsForCursor(cursor.get(), items, 10 /*approxLimit*/);

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
    cm.forceNewCheckpoint();

    // Test: Iterate on all items and check that the seqnos are weakly monotonic
    auto regRes = cm.registerCursorBySeqno(
            "Cursor", 0, CheckpointCursor::Droppable::Yes);
    auto cursor = regRes.cursor.lock();
    std::vector<queued_item> items;
    cm.getItemsForCursor(cursor.get(), items, 10 /*approxLimit*/);

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
    cm.forceNewCheckpoint();

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
    EXPECT_EQ(2, regRes.cursor.lock()->getId());
}

TEST_P(CheckpointTest,
       GetItemsForPersistenceCursor_ThrowIfBackupCursorAlreadyExists) {
    if (!persistent()) {
        return;
    }

    std::vector<queued_item> items;
    auto res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);

    try {
        manager->getItemsForCursor(cursor, items, 123 /*limit*/);
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

    // Enqueue 1 item
    EXPECT_TRUE(queueNewItem("keyA"));
    EXPECT_EQ(1, manager->getNumOpenChkItems());

    // Checkpoint shape now is (P/C stand for pCursor/pCursorCopy):
    // [E    CS    M)
    //  ^
    //  P

    // Pull it out from the CM without moving the cursor
    std::vector<queued_item> items;
    auto res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);

    // Check that we get all the expected
    EXPECT_TRUE(res.flushHandle);
    EXPECT_FALSE(res.moreAvailable);
    EXPECT_EQ(1, res.ranges.size());
    EXPECT_EQ(0, res.ranges[0].getStart());
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
    const auto res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);
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
    res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);

    ASSERT_TRUE(res.flushHandle);
    EXPECT_FALSE(res.moreAvailable);
    ASSERT_EQ(1, res.ranges.size());
    EXPECT_EQ(0, res.ranges[0].getStart());
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
    ASSERT_EQ(1, manager->getNumOpenChkItems());
    ASSERT_TRUE(queueNewItem("another-key"));
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(2, manager->getNumOpenChkItems());

    manager->createNewCheckpoint();
    ASSERT_EQ(2, manager->getNumCheckpoints());

    // Create backup-pcursor (and move pcursor)
    std::vector<queued_item> items;
    const auto res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);

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
    ASSERT_EQ(2, manager->getNumOpenChkItems());

    // Flush - getItems
    // [E    CS    M(keyA):1    M(keyB):2)
    //             ^            ^
    //             B            P
    std::vector<queued_item> items;
    auto res = manager->getItemsForCursor(cursor, items, 123 /*limit*/);
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
    ASSERT_EQ(2, manager->getNumOpenChkItems());

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
    manager->getItemsForCursor(cursor, items, 123 /*limit*/);
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
    auto item = this->manager->public_createCheckpointItem(
            0, Vbid(0), queue_op::empty);
    EXPECT_EQ("cid:0x1:empty", item->getKey().to_string());

    item = this->manager->public_createCheckpointItem(
            0, Vbid(0), queue_op::checkpoint_start);
    EXPECT_EQ("cid:0x1:checkpoint_start", item->getKey().to_string());

    item = this->manager->public_createCheckpointItem(
            0, Vbid(0), queue_op::set_vbucket_state);
    EXPECT_EQ("cid:0x1:set_vbucket_state", item->getKey().to_string());

    item = this->manager->public_createCheckpointItem(
            0, Vbid(0), queue_op::checkpoint_end);
    EXPECT_EQ("cid:0x1:checkpoint_end", item->getKey().to_string());

    auto disk = makeDiskDocKey("test_key");
    EXPECT_EQ("cid:0x0:test_key", disk.to_string());

    disk = makeDiskDocKey("test_key", true, CollectionID(99));
    EXPECT_EQ("pre:cid:0x63:test_key", disk.to_string());

    auto event =
            SystemEventFactory::makeCollectionEvent(CollectionID(99), {}, {});
    EXPECT_EQ("cid:0x1:0x0:0x63:_collection", event->getKey().to_string());
    event = SystemEventFactory::makeScopeEvent(ScopeID(99), {}, {});
    EXPECT_EQ("cid:0x1:0x1:0x63:_scope", event->getKey().to_string());
}

// sub class for eager unreffed checkpoint disposal related tests.
class EagerCheckpointDisposalTest : public CheckpointTest {
public:
    void SetUp() override {
        CheckpointTest::SetUp();

        // This test suite specifically tests eager checkpoint removal.
        // Ensure the checkpoint config is set to that.
        config.parseConfiguration("checkpoint_removal_mode=eager",
                                  get_mock_server_api());
        recreateCheckpointConfig();
    }
};

using MockCheckpointDisposer = testing::MockFunction<void(CheckpointList&&)>;

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

/**
 * GTest match a CheckpointList against a vector of vectors of keys
 * E.g.,
 *  MatchCheckpoint({
 *                      {"key1", "key2"},
 *                      {"key3"}
 *                  });
 * creates a matcher which expects to find two checkpoints, where the non-meta
 * items in the first have keys "key1", "key2", and in the second "key3"
 */
auto MatchCheckpointList(
        const std::vector<std::vector<std::string>>& expected) {
    return ::testing::Pointwise(CheckpointMatcher(), expected);
}

TEST_P(EagerCheckpointDisposalTest, CursorMovement) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor));

    using namespace testing;

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // remains in the old checkpoint, so it is still reffed - callback
        // should not be triggered
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(2, manager->getNumItemsForCursor(cursor));
    }

    {
        // Advance cursor, moving it out of the closed ckpt.
        // callback should be triggered, as it is now unreffed.
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(
                callback,
                Call(MatchCheckpointList({{"cid:0x0:key1", "cid:0x0:key2"}})))
                .Times(1);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(
                    cursor, items, std::numeric_limits<size_t>::max());
        }
        EXPECT_EQ(0, manager->getNumItemsForCursor(cursor));
    }
}

TEST_P(EagerCheckpointDisposalTest, NewClosedCheckpointMovesCursor) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor));

    using namespace testing;

    {
        // Advance cursor, moving it to the end of the checkpoint.
        // checkpoint still reffed, for now
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(
                    cursor, items, std::numeric_limits<size_t>::max());
        }
        EXPECT_EQ(2, this->manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(0, manager->getNumItemsForCursor(cursor));
    }

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // is advanced implicitly to the new checkpoint - callback should
        // be triggered, closed checkpoint should be removed
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(
                callback,
                Call(MatchCheckpointList({{"cid:0x0:key1", "cid:0x0:key2"}})))
                .Times(1);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
        EXPECT_EQ(0, manager->getNumItemsForCursor(cursor));
    }
}

TEST_P(EagerCheckpointDisposalTest, NewUnreffedClosedCheckpoint) {
    // remove the cursor now, so the newly closed checkpoint will be immediately
    // unreffed
    manager->removeCursor(cursor);

    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    using namespace testing;

    {
        // Create a new checkpoint, closing the current open one. Callback
        // should be triggered as there are no cursors in the closed checkpoint
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(
                callback,
                Call(MatchCheckpointList({{"cid:0x0:key1", "cid:0x0:key2"}})))
                .Times(1);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        // just the open checkpoint left, closed was removed immediately
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
    }
}

TEST_P(EagerCheckpointDisposalTest, OnlyOldestCkptTriggersCB) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());

    using namespace testing;

    {
        // Create a new checkpoint, closing the current open one.
        // No callback triggered, cursor is present.
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
    }

    // queue another item
    EXPECT_TRUE(this->queueNewItem("key3"));
    EXPECT_EQ(1, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, this->manager->getNumCheckpoints());

    {
        // Create a new checkpoint, closing the current open one.
        // The "middle" checkpoint has no cursors, but is not the oldest
        // checkpoint, so cannot trigger checkpoint removal
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(3, this->manager->getNumCheckpoints());
    }

    {
        // Advance cursor into "middle" checkpoint. Oldest checkpoint can now
        // be removed, should trigger callback.
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(
                callback,
                Call(MatchCheckpointList({{"cid:0x0:key1", "cid:0x0:key2"}})))
                .Times(1);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        {
            std::vector<queued_item> items;
            manager->getItemsForCursor(cursor, items, 2);
        }
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(1, manager->getNumItemsForCursor(cursor));
    }
}

TEST_P(EagerCheckpointDisposalTest, RemoveCursorTriggersCB) {
    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        EXPECT_TRUE(this->queueNewItem("key" + std::to_string(i)));
    }
    EXPECT_EQ(1, this->manager->getNumCheckpoints());
    EXPECT_EQ(2, this->manager->getNumOpenChkItems());
    EXPECT_EQ(2, manager->getNumItemsForCursor(cursor));

    using namespace testing;

    {
        // Create a new checkpoint, closing the current open one. The cursor
        // remains in the old checkpoint, so it is still reffed - callback
        // should not be triggered
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(2, this->manager->getNumCheckpoints());
        EXPECT_EQ(2, manager->getNumItemsForCursor(cursor));
    }

    {
        // Queue an item and create one more checkpoint. Still can't be removed,
        // as the cursor still exists.
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback, Call(_)).Times(0);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        EXPECT_TRUE(this->queueNewItem("key3"));
        this->manager->createNewCheckpoint();
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(3, this->manager->getNumCheckpoints());
        EXPECT_EQ(3, manager->getNumItemsForCursor(cursor));
    }

    {
        // Drop the cursor. Callback should be triggered, as _both_ checkpoints
        // are now eligible for removal - the cursor was removed from the oldest
        // leaving it unreffed, and the "middle" checkpoint doesn't have any
        // cursors either.
        StrictMock<MockCheckpointDisposer> callback;
        EXPECT_CALL(callback,
                    Call(MatchCheckpointList({{"cid:0x0:key1", "cid:0x0:key2"},
                                              {"cid:0x0:key3"}})))
                .Times(1);
        manager->setCheckpointDisposer(callback.AsStdFunction());

        manager->removeCursor(cursor);
        EXPECT_EQ(0, this->manager->getNumOpenChkItems());
        EXPECT_EQ(1, this->manager->getNumCheckpoints());
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
TEST_P(CheckpointTest, MB_47516) {
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
    ASSERT_TRUE(this->queueNewItem("k1001")); // 1001
    ASSERT_TRUE(this->queueNewItem("k1002")); // 1002

    // 1.1) persist these, cursor now past them and we can expel
    std::vector<queued_item> items;
    manager->getNextItemsForPersistence(items);
    // we get the cp start and our two items
    EXPECT_EQ(3, items.size());

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
    EXPECT_EQ(1002, cursor.seqno);
}

// In the case of open/closed checkpoints cursors placed at the high-seqno
// should not reference the closed checkpoint.
TEST_P(CheckpointTest, MB_47551) {
    // 1) Receive a snapshot, two items is plenty for the test
    this->manager->createSnapshot(1001, 1002, 1002, CheckpointType::Disk, 1002);
    ASSERT_TRUE(this->queueNewItem("k1001")); // 1001
    ASSERT_TRUE(this->queueNewItem("k1002")); // 1002

    // No as if vb-state changed, new checkpoint
    this->manager->createNewCheckpoint();

    // 0, mid-way and high-seqno-1 request - expect the closed CP, data is
    // available
    for (uint64_t seqno : {0, 500, 1001}) {
        auto cursor = manager->registerCursorBySeqno(
                "MB-47551", seqno, CheckpointCursor::Droppable::Yes);
        if (seqno == 1001) {
            EXPECT_FALSE(cursor.tryBackfill) << seqno;
            EXPECT_EQ(1002, cursor.seqno) << seqno;
        } else {
            EXPECT_TRUE(cursor.tryBackfill) << seqno;
            EXPECT_EQ(1001, cursor.seqno) << seqno;
        }

        // Cursor should be in the closed checkpoint, it has the items we need
        EXPECT_EQ(1, cursor.cursor.lock()->getId());
    }

    // But high-seqno should use the open CP
    auto cursor2 = manager->registerCursorBySeqno(
            "cursor2", 1002, CheckpointCursor::Droppable::Yes);

    // And we expect to be in the open checkpoint, so we don't hold the closed
    // one. Possibly don't need backfill=true, but DCP streams handle this case
    EXPECT_TRUE(cursor2.tryBackfill);
    EXPECT_EQ(1003, cursor2.seqno);
    EXPECT_EQ(2, cursor2.cursor.lock()->getId());
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
        AllVBTypesAllEvictionModes,
        EagerCheckpointDisposalTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

void CheckpointMemoryTrackingTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    config_string += "checkpoint_removal_mode=";
    config_string += to_string(GetParam());
    SingleThreadedCheckpointTest::SetUp();
}

void CheckpointMemoryTrackingTest::testEstimatedCheckpointMemUsage() {
    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItems());
    EXPECT_EQ(0, manager.getNumOpenChkItems());
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
    const auto initialIndex = checkpoint->getKeyIndexMemUsage();
    // Some metaitems are already in the queue
    EXPECT_GT(initialQueued, 0);
    EXPECT_EQ(0, initialIndex);
    EXPECT_EQ(initialQueued, stats.getEstimatedCheckpointMemUsage());
    EXPECT_EQ(initialQueued, manager.getEstimatedMemUsage());

    size_t itemsAlloc = 0;
    size_t keyIndexAlloc = 0;
    const size_t numItems = 10;
    for (size_t i = 1; i <= numItems; ++i) {
        auto item = makeCommittedItem(
                makeStoredDocKey("key" + std::to_string(i)), "value", vbid);
        EXPECT_TRUE(vb->checkpointManager->queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
        // Our estimated mem-usage must account for the queued item + the
        // allocation for the key-index
        itemsAlloc += item->size();
        keyIndexAlloc += item->getKey().size() + sizeof(IndexEntry);
    }

    // Load post-conditions
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(numItems, manager.getNumOpenChkItems());
    EXPECT_EQ(initialQueueSize + numItems, openQueue.size());

    const auto queued = checkpoint->getQueuedItemsMemUsage();
    const auto index = checkpoint->getKeyIndexMemUsage();
    EXPECT_EQ(initialQueued + itemsAlloc, queued);
    EXPECT_EQ(initialIndex + keyIndexAlloc, index);
    EXPECT_EQ(queued + index, stats.getEstimatedCheckpointMemUsage());
    EXPECT_EQ(queued + index, manager.getEstimatedMemUsage());
}

TEST_P(CheckpointMemoryTrackingTest, EstimatedCheckpointMemUsage) {
    testEstimatedCheckpointMemUsage();
}

TEST_P(CheckpointMemoryTrackingTest, EstimatedCheckpointMemUsageAtExpelling) {
    testEstimatedCheckpointMemUsage();

    auto vb = store->getVBuckets().getBucket(vbid);
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
    const auto initialIndex = checkpoint.getKeyIndexMemUsage();

    auto& cursor = *manager.getPersistenceCursor();
    auto pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
    ASSERT_TRUE(pos->isEmptyItem());

    // Move the cursor to the second mutation, we want to expel up to the first
    // one. Skip ckpt-start, set-vbstate, m:1 and place the cursor on m:2.
    // As a collateral thing, I need to keep track of sizes of items that we are
    // going to expel, that's to make our final verification on memory counters.
    size_t setVBStateSize = 0;
    size_t m1Size = 0;
    // While these are helpers for other checks later in the test.
    size_t emptySize = pos->size();
    size_t ckptStartSize = 0;

    for (auto i = 0; i < 4; ++i) {
        CheckpointCursorIntrospector::incrPos(cursor);
        const auto pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
        if (pos->getOperation() == queue_op::checkpoint_start) {
            ckptStartSize = pos->size();
        } else if (pos->getOperation() == queue_op::set_vbucket_state) {
            setVBStateSize = pos->size();
        } else if (pos->getOperation() == queue_op::mutation &&
                   pos->getBySeqno() == 1) {
            m1Size = pos->size();
        }
    }

    ASSERT_GT(setVBStateSize, 0);
    ASSERT_GT(m1Size, 0);
    ASSERT_GT(emptySize, 0);
    ASSERT_GT(ckptStartSize, 0);
    pos = (*CheckpointCursorIntrospector::getCurrentPos(cursor));
    ASSERT_FALSE(pos->isCheckPointMetaItem());
    ASSERT_EQ(2, pos->getBySeqno());

    // Expelling set-vbstate + m:1
    const auto numExpelled = manager.expelUnreferencedCheckpointItems().count;
    EXPECT_EQ(2, numExpelled);

    // Expel post-conditions
    EXPECT_EQ(1, manager.getNumCheckpoints());
    // Note: item-expel doesn't update nonmeta-item counters in checkpoint
    EXPECT_EQ(initialNumItems, manager.getNumOpenChkItems());
    // But the actual queue size is accurate
    EXPECT_EQ(initialQueueSize - numExpelled, openQueue.size());

    const auto queued = checkpoint.getQueuedItemsMemUsage();
    const auto index = checkpoint.getKeyIndexMemUsage();
    // Initial - what we expelled
    EXPECT_EQ(initialQueued - setVBStateSize - m1Size, queued);
    // Expel doesn't touch the key index
    EXPECT_EQ(initialIndex, index);
    EXPECT_EQ(queued + index,
              engine->getEpStats().getEstimatedCheckpointMemUsage());
    EXPECT_EQ(queued + index, manager.getEstimatedMemUsage());
}

TEST_P(CheckpointMemoryTrackingTest, EstimatedCheckpointMemUsageAtRemoval) {
    testEstimatedCheckpointMemUsage();

    // confirm that no items have been removed from the checkpoint manager
    ASSERT_EQ(0, engine->getEpStats().itemsRemovedFromCheckpoints);

    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    // set the eager checkpoint disposer to allow checkpoints to be destroyed
    // "inline" when removed. This avoids needing to drive background tasks.
    manager.setCheckpointDisposer(ImmediateCkptDisposer);
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
    const auto expectedFinalIndexAllocation = 0;

    auto checkpoint = manager.getCheckpointList().front().get();
    auto queued = checkpoint->getQueuedItemsMemUsage();
    auto index = checkpoint->getKeyIndexMemUsage();
    ASSERT_GT(queued, expectedFinalQueueAllocation);
    ASSERT_GT(index, expectedFinalIndexAllocation);
    EXPECT_EQ(queued + index,
              engine->getEpStats().getEstimatedCheckpointMemUsage());

    manager.createNewCheckpoint(true /*force*/);
    EXPECT_EQ(2, manager.getNumCheckpoints());
    // Move cursor to new checkpoint
    std::vector<queued_item> items;
    manager.getItemsForCursor(manager.getPersistenceCursor(),
                              items,
                              std::numeric_limits<size_t>::max());
    // Remove closed checkpoint (if eager checkpoint removal, this is
    // a no-op; the checkpoints have already been removed)
    manager.removeClosedUnrefCheckpoints();
    // rather than checking the result of removeClosedUnrefCheckpoints,
    // check the number of items removed according to the stats.
    // This avoids being dependent on eager vs lazy checkpoint removal
    EXPECT_EQ(initialNumItems,
              engine->getEpStats().itemsRemovedFromCheckpoints);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(0, manager.getNumOpenChkItems());

    // removeClosedUnrefCheckpoints queues checkpoints for destruction,
    // run the destroyer task to recover the memory the checkpoints use.
    runCheckpointDestroyer();

    checkpoint = manager.getCheckpointList().front().get();
    queued = checkpoint->getQueuedItemsMemUsage();
    index = checkpoint->getKeyIndexMemUsage();
    EXPECT_EQ(expectedFinalQueueAllocation, queued);
    EXPECT_EQ(expectedFinalIndexAllocation, index);
    EXPECT_EQ(queued + index,
              engine->getEpStats().getEstimatedCheckpointMemUsage());
    EXPECT_EQ(queued + index, manager.getEstimatedMemUsage());
}

TEST_P(CheckpointMemoryTrackingTest, BackgroundTaskIsNotified) {
    // Verify that eager checkpoint removal notifies the CheckpointDestroyerTask
    // to run ASAP.

    if (GetParam() != CheckpointRemoval::Eager) {
        GTEST_SKIP();
    }

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb->checkpointManager);

    scheduleCheckpointDestroyerTask();

    auto& task = store->getCkptDestroyerTask();

    auto initialWaketime = task.getWaketime();

    // Add two items to the initial (open) checkpoint.
    for (auto i : {1, 2}) {
        auto item = makeCommittedItem(
                makeStoredDocKey("key" + std::to_string(i)), "value", vbid);
        EXPECT_TRUE(manager.queueDirty(
                item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
    }
    auto* cursor = manager.getPersistenceCursor();
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumOpenChkItems());
    EXPECT_EQ(2, manager.getNumItemsForCursor(cursor));

    // task should not have been woken yet
    EXPECT_EQ(initialWaketime, task.getWaketime());

    // Create a new checkpoint, closing the current open one. The cursor
    // remains in the old checkpoint, so it is still reffed - callback
    // should not be triggered
    manager.createNewCheckpoint();
    EXPECT_EQ(0, manager.getNumOpenChkItems());
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(2, manager.getNumItemsForCursor(cursor));

    // task should not have been woken yet
    EXPECT_EQ(initialWaketime, task.getWaketime());

    auto& epstats = engine->getEpStats();

    auto initialCMMemUsage = manager.getEstimatedMemUsage();
    auto initialEPMemUsage = epstats.getEstimatedCheckpointMemUsage();

    // the destroyer doesn't own anything yet, so should have no mem usage
    EXPECT_EQ(0, task.getMemoryUsage());

    // advance the cursor, unreffing the checkpoint. CheckpointDestroyerTask
    // should be notified and ownership of the checkpoint transferred.
    {
        std::vector<queued_item> items;
        manager.getItemsForCursor(
                cursor, items, std::numeric_limits<size_t>::max());
    }
    // as soon as checkpoints are removed, the manager's memory usage should
    // decrease...
    EXPECT_LT(manager.getEstimatedMemUsage(), initialCMMemUsage);
    // ... and the destroyer task's should increase by the same amount
    EXPECT_EQ(initialCMMemUsage - manager.getEstimatedMemUsage(),
              task.getMemoryUsage());

    // As the checkpoints still exist, so they are still accounted for in
    // epstats
    EXPECT_EQ(initialEPMemUsage, epstats.getEstimatedCheckpointMemUsage());

    // now the task should be ready to run
    EXPECT_LE(task.getWaketime(), std::chrono::steady_clock::now());

    auto& nonIOQueue = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(nonIOQueue, "Destroying closed unreferenced checkpoints");

    // checkpoint has been destroyed, epstats counter should have decreased
    EXPECT_LT(epstats.getEstimatedCheckpointMemUsage(), initialEPMemUsage);
    // and so should the destroyers memory tracking
    EXPECT_EQ(0, task.getMemoryUsage());
}

INSTANTIATE_TEST_SUITE_P(EagerAndLazyCheckpointMemoryTrackingTests,
                         CheckpointMemoryTrackingTest,
                         ::testing::Values(CheckpointRemoval::Eager,
                                           CheckpointRemoval::Lazy));

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