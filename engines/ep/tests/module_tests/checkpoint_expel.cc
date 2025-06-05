/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_checkpoint_manager.h"
#include "checkpoint_test.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"

class CheckpointExpelTest : public CheckpointTest {};

TEST_P(CheckpointExpelTest, ExpelSetVBState) {
    const int itemCount{3};
    size_t sizeOfItem{0}, sizeOfSetVBState{0};

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

        const auto size = item->size();
        if (sizeOfItem) {
            // all should be same size, certainly for the test memory reduction
            // calculation.
            EXPECT_EQ(size, sizeOfItem);
        }
        sizeOfItem = size;

        // Add the queued_item to the checkpoint
        manager->queueDirty(item,
                            GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            /*preLinkDocCtx*/ nullptr);
    }

    const int setVBStates = 3;
    for (auto ii = 0; ii < setVBStates; ++ii) {
        const auto size = manager->queueSetVBState();
        if (sizeOfSetVBState) {
            // all should be same size, certainly for the test memory reduction
            // calculation.
            EXPECT_EQ(size, sizeOfSetVBState);
        }
        sizeOfSetVBState = size;
    }

    // cs + mutations
    ASSERT_EQ(1 + itemCount + setVBStates, manager->getNumOpenChkItems());
    ASSERT_EQ(1 + itemCount + setVBStates,
              manager->getNumItemsForCursor(*cursor));
    ASSERT_EQ(1000 + itemCount, this->manager->getHighSeqno());

    bool isLastMutationItem{true};
    for (auto ii = 0; ii < 3 + setVBStates; ++ii) {
        auto item = manager->nextItem(cursor, isLastMutationItem);
        ASSERT_FALSE(isLastMutationItem);
    }

    /*
     *
     * Checkpoint now looks as follows:
     * 1000 - dummy item
     * 1001 - checkpoint start
     * 1001 - 1st item (key0)
     * 1002 - 2nd item (key1)
     * 1003 - 3rd item (key2)
     * 1004 - 1st set-vb-state
     * 1004 - 2nd set-vb-state <<<<<<< Cursor
     * 1004 - 3rd set-vb-state
     */
    // Get the memory usage before expelling
    const auto memUsageBeforeExpel = manager->getMemUsage();
    auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(itemCount + (setVBStates - 1), expelResult.count);
    EXPECT_EQ(itemCount + (setVBStates - 1),
              global_stats.itemsExpelledFromCheckpoints);

    /*
     * We have expelled:
     * 1001 - 1st item (key 0)
     * 1002 - 2nd item (key1)
     * 1003 - 3rd item (key2)
     * 1004 - 1st set-vb-state
     * 1004 - 2nd set-vb-state
     *
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start <<<<<<< Cursor
     * 1004 - 3rd set-vb-state
     */
    if (persistent()) {
        const auto pos = manager->getPersistenceCursorPos();
        EXPECT_EQ(queue_op::checkpoint_start, (*pos)->getOperation());
        EXPECT_EQ(1001, (*pos)->getBySeqno());
    }

    const size_t perItem = Checkpoint::per_item_queue_overhead + sizeOfItem;
    const size_t perSetVBState =
            Checkpoint::per_item_queue_overhead + sizeOfSetVBState;
    const size_t expectedMemoryRecovered =
            itemCount * perItem + (setVBStates - 1) * perSetVBState;

    EXPECT_EQ(expectedMemoryRecovered,
              memUsageBeforeExpel - manager->getMemUsage())
            << *manager;

    // Now verify the behaviour when Expel releases all the items in checkpoint
    manager->nextItem(cursor, isLastMutationItem);
    ASSERT_TRUE(isLastMutationItem);
    /*
     * Checkpoint now looks as follows:
     * 1000 - dummy Item
     * 1001 - checkpoint start
     * 1004 - 3rd set-vb-state) <<<<<<< Cursor
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
    EXPECT_EQ(itemCount + setVBStates,
              global_stats.itemsExpelledFromCheckpoints);

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

class CheckpointExpelTestNoCursor : public CheckpointTest {
    void SetUp() override {
        config.setCheckpointMaxSize(std::numeric_limits<size_t>::max());
        checkpoint_config = std::make_unique<CheckpointConfig>(config);
        VBucketTest::SetUp();
        createManager(1000, false); // false=> no cursor registered
        ASSERT_EQ(0, manager->getNumOfCursors());
    }
};

// Expel test to cover the "lowestCursor==nullptr" case
TEST_P(CheckpointExpelTestNoCursor, ExpelSetVBState) {
    // Expel has detection for early return when "empty" and no cursor
    // see "lowestCursor" checks in expel code.
    ASSERT_EQ(1, manager->getNumOpenChkItems()); // cp_start is counted
    auto expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(0, expelResult.count);

    std::string value("value");
    auto item = makeCommittedItem(makeStoredDocKey("key"), value);

    EXPECT_TRUE(manager->queueDirty(item,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_EQ(item->size() + Checkpoint::per_item_queue_overhead,
              expelResult.memory);

    const auto size = manager->queueSetVBState();
    expelResult = manager->expelUnreferencedCheckpointItems();
    EXPECT_EQ(1, expelResult.count);
    EXPECT_EQ(size + Checkpoint::per_item_queue_overhead, expelResult.memory);
}

INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        CheckpointExpelTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);
INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        CheckpointExpelTestNoCursor,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);
