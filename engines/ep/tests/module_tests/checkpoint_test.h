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

#pragma once

#include "callbacks.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "evp_store_single_threaded_test.h"
#include "vbucket_test.h"
#include <checkpoint.h>
#include <folly/portability/GTest.h>

class MockCheckpointManager;

/**
 * Test fixture for Checkpoint tests.
 */
class CheckpointTest : public VBucketTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    // Creates a new item with the given key and queues it into the checkpoint
    // manager.
    bool queueNewItem(const std::string& key);

    // Creates a new item with the given key@seqno and queues it into the
    // checkpoint manager.
    bool queueReplicatedItem(const std::string& key, int64_t seqno);

    void createManager(int64_t lastSeqno = 1000);

    void resetManager();

    /**
     * Tests that CM::getItemsForCursor(pcursor) creates a backup pcursor at the
     * pcursor original position. Used as baseline step for other tests.
     *
     * @return the items for pcursor
     */
    CheckpointManager::ItemsForCursor testGetItemsForPersistenceCursor();

    // Test that can expel items and that we have the correct behaviour when we
    // register cursors for items that have been expelled.
    void testExpelCheckpointItems();

    // Test that when the first cursor we come across is pointing to the last
    // item we do not evict this item.  Instead we walk backwards find the
    // first non-meta item and evict from there.
    void testExpelCursorPointingToLastItem();

    // Test that when the first cursor we come across is pointing to the
    // checkpoint start we do not evict this item. Instead we walk backwards and
    // find the the dummy item, so do not expel any items.
    void testExpelCursorPointingToChkptStart();

    // Test that if we want to evict items from seqno X, but have a meta-data
    // item also with seqno X, and a cursor is pointing to this meta data item,
    // we do not evict.
    void testDontExpelIfCursorAtMetadataItemWithSameSeqno();

    // Test that if we have a item after a mutation with the same seqno then we
    // will move the expel point backwards to the mutation (and possibly
    // further).
    void testDoNotExpelIfHaveSameSeqnoAfterMutation();

    // Test estimate for the amount of memory recovered by expelling is correct.
    void testExpelCheckpointItemsMemoryRecovered();

    // Proxy to the related protected CM function
    CheckpointManager::ExtractItemsResult extractItemsToExpel();

    // Common base setup for expel-cursor tests
    void expelCursorSetup();

    // Tests that the extract-items step at ItemExpel registers the expel-cursor
    CheckpointManager::ExtractItemsResult testExpelCursorRegistered();

    // Calls getItemsForCursor() to advance the cursor to the end of the
    // checkpoints
    void advanceCursorToEndOfCheckpoints();

    // Owned by VBucket
    MockCheckpointManager* manager;
    // Owned by CheckpointManager
    CheckpointCursor* cursor;
};

/*
 * Test fixture for single-threaded Checkpoint tests
 */
class SingleThreadedCheckpointTest : public SingleThreadedKVBucketTest {
public:
    void closeReplicaCheckpointOnMemorySnapshotEnd(bool highMem,
                                                   uint32_t snapshotType);

    /**
     * Tests that the cursor distance is computed correctly when the cursor is
     * registered
     *
     * @return the new cursor
     */
    std::shared_ptr<CheckpointCursor> testCursorDistance_Register();

    enum class ItemRemovalPath { None, Dedup, Expel };

    /**
     * Verifies that the checkpoint tracks correctly the lower seqno that a
     * cursor would pick if registered at checkpoint's begin.
     *
     * @param itemRemoval Defines the scenario under test in the different cases
     *  of item removal
     */
    void testMinimumCursorSeqno(ItemRemovalPath itemRemoval);

    /**
     * Verifies that registering a cursor succeeds in checkpoints from which
     * all mutations have been expelled.
     * Helper function to cover also the case where the checkpoint contains
     * some meta-item after checkpoint_start (eg, set_vbstate is queued after
     * ItemExpel).
     *
     * @param extraMetaItem Whether we run the test on a checkpoint that
     *  contains a meta-item after checkpoint_start
     */
    void testRegisterCursorInCheckpointEmptyByExpel(bool extraMetaItem);
};

/**
 * Test fixture dedicated to the memory tracking of internal structures in
 * checkpoint. Parameterized over key length.
 */
class CheckpointMemoryTrackingTest : public SingleThreadedCheckpointTest,
                                     public ::testing::WithParamInterface<int> {
public:
    /**
     * Verify that the checkpoints mem-usage is tracked correctly
     * at queueing items into the checkpoints.
     */
    void testCheckpointManagerMemUsage();

    static std::string PrintToStringParamName(
            const testing::TestParamInfo<int>& info) {
        return "keyLength_" + std::to_string(info.param);
    }

    std::string createPaddedKeyString(const size_t& key,
                                      const int& desiredKeyLength) {
        const auto keyStr = std::to_string(key);

        EXPECT_GT(desiredKeyLength, keyStr.size())
                << "key '" << std::to_string(key)
                << "' cannot be padded to desiredKeyLength "
                << std::to_string(desiredKeyLength);

        return keyStr + std::string(desiredKeyLength - keyStr.size(), 'x');
    }

    // Number of items in the checkpoint for all memUsage tests.
    static const auto numItems = 10;

    // Small enough such that SSO applies for all platforms
    static const int shortKeyLength = 5;
    // Very long key that will be >> any possible non-key allocation; SSO will
    // definitely not apply.
    static const int longKeyLength = 1024;

    static constexpr size_t checkpointIndexInsertionOverhead =
            sizeof(StoredDocKeyT<MemoryTrackingAllocator>) + sizeof(IndexEntry);
};

/**
 * Test fixture dedicated to memory tracking of the checkpoint_index structure.
 */
class CheckpointIndexAllocatorMemoryTrackingTest
    : public CheckpointMemoryTrackingTest {
protected:
    // When the first element is inserted into a Folly map, there’s a small
    // overhead for the metadata of items which share the same
    // row. We cannot easily predict which row different items will be assigned
    // to, but we do know the first element will need to allocate a new row.
    // Therefore expect the below additional bytes to be allocated above the
    // bytes for the key+value.
#if WIN32
    const size_t firstElemOverhead = 32 + 16;
#else
    const size_t firstElemOverhead = 32;
#endif

    // std::string's allocator will also likely over-allocate as an
    // optimization, e.g. for alignment. This has been observed to be up to
    // multiples of 16 bytes on macOS, but this is environment dependent, so the
    // following value is used only as an upper bound.
    static const size_t alignmentBytes = 24;
};

/**
 * Test fixture for checking out that checkpoint configuration is applied
 * as expected.
 */
class CheckpointConfigTest : public SingleThreadedCheckpointTest {};

/**
 * Test fixture covering the creation of multiple CheckpointDestroyerTasks,
 * and ensuring work is distributed across them.
 */
class ShardedCheckpointDestructionTest
    : public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<size_t> {
public:
    void SetUp() override;
};

class EphemeralCheckpointTest : public CheckpointTest {
    void SetUp() override;
};

/**
 * Test fixture for tests that want to treat the checkpoint manager as if it
 * where for a replica vbucket.
 */
class ReplicaCheckpointTest : public CheckpointTest {
public:
    void SetUp() override;
};