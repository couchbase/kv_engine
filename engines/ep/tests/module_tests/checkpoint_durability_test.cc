/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_test.h"
#include "checkpoint_test_impl.h"
#include "ep_vb.h"
#include "test_helpers.h"

#include <gmock/gmock-generated-matchers.h>
#include <tests/mock/mock_checkpoint_manager.h>

/**
 * Test fixture for Checkpoint tests related to durability.
 */
class CheckpointDurabilityTest : public CheckpointTest {
protected:
    // Helper method - test that the two queued ops first and second, when
    // queued in that order are not de-dupicated.
    // @param[out] items returned via persistence cursor after queuing both
    // items.
    void test_AvoidDeDuplication(queued_item first,
                                 queued_item second,
                                 std::vector<queued_item>& items);
};

void CheckpointDurabilityTest::test_AvoidDeDuplication(
        queued_item first,
        queued_item second,
        std::vector<queued_item>& items) {
    // Check expected starting state.
    ASSERT_EQ(1, manager->getNumCheckpoints());

    // Setup: enqueue a first item.
    ASSERT_TRUE(manager->queueDirty(*this->vbucket,
                                    first,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));
    ASSERT_EQ(1, manager->getNumCheckpoints());
    ASSERT_EQ(1, manager->getNumOpenChkItems());

    // Test: enqueue second item
    EXPECT_TRUE(manager->queueDirty(*this->vbucket,
                                    second,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    /*preLinkDocCtx*/ nullptr));

    manager->getNextItemsForPersistence(items);
}

// Check that an existing pending SyncWrite is not de-duplicated when a
// Committed SyncWrite (with the same key) is added to the CheckpointManager.
TEST_P(CheckpointDurabilityTest,
       AvoidDeDuplicationOfExistingPendingWithCommit) {
    auto pending = makePendingItem(makeStoredDocKey("durable"), "pending");

    auto committed = makeCommittedviaPrepareItem(makeStoredDocKey("durable"),
                                                 "committed");
    std::vector<queued_item> items;
    this->test_AvoidDeDuplication(pending, committed, items);
    EXPECT_THAT(
            items,
            testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                 HasOperation(queue_op::pending_sync_write),
                                 HasOperation(queue_op::commit_sync_write)));

    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems());
}

// Check that an existing Committed SyncWrite is not de-duplicated when a
// Pending SyncWrite (with the same key) is added to the CheckpointManager.
TEST_P(CheckpointDurabilityTest,
       AvoidDeDuplicationOfExistingCommitWithPending) {
    auto committed = makeCommittedviaPrepareItem(makeStoredDocKey("durable"),
                                                 "committed");
    auto pending = makePendingItem(makeStoredDocKey("durable"), "pending");

    std::vector<queued_item> items;
    this->test_AvoidDeDuplication(committed, pending, items);
    EXPECT_THAT(
            items,
            testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                 HasOperation(queue_op::commit_sync_write),
                                 HasOperation(queue_op::pending_sync_write)));

    // Verify: Should not have de-duplicated, should be in one checkpoint
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems());
}

// Check that an existing Committed SyncWrite is not de-duplicated when a
// non-SyncWrite (with the same key) is added to the CheckpointManager.
TEST_P(CheckpointDurabilityTest,
       AvoidDeDuplicationOfExistingCommitWithMutation) {
    auto committed = makeCommittedviaPrepareItem(makeStoredDocKey("durable"),
                                                 "committed");
    auto mutation = makeCommittedItem(makeStoredDocKey("durable"), "mutation");

    std::vector<queued_item> items;
    this->test_AvoidDeDuplication(committed, mutation, items);
    EXPECT_THAT(items,
                testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::commit_sync_write),
                                     HasOperation(queue_op::checkpoint_end),
                                     HasOperation(queue_op::checkpoint_start),
                                     HasOperation(queue_op::mutation)));
    // Verify: Should not have de-duplicated, should be in two checkpoints
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(1, manager->getNumOpenChkItems());
}

// Check that an existing Committed non-SyncWrite is not de-duplicated when a
// pending SyncWrite (with the same key) is added to the CheckpointManager.
TEST_P(CheckpointDurabilityTest,
       AvoidDeDuplicationOfExistingCommitWithPrepare) {
    auto mutation = makeCommittedItem(makeStoredDocKey("durable"), "mutation");
    auto pending = makePendingItem(makeStoredDocKey("durable"), "pending");

    std::vector<queued_item> items;
    this->test_AvoidDeDuplication(mutation, pending, items);
    EXPECT_THAT(
            items,
            testing::ElementsAre(HasOperation(queue_op::checkpoint_start),
                                 HasOperation(queue_op::mutation),
                                 HasOperation(queue_op::pending_sync_write)));

    // Verify: Should not have de-duplicated, should be in one checkpoint
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems());
}
