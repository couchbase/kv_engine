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

    // Calls getNextItemsForCursor() to advance the cursor to the end of the
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
};

/**
 * Test fixture dedicated to the memory tracking of internal structures in
 * checkpoint.
 */
class CheckpointMemoryTrackingTest : public SingleThreadedCheckpointTest {
public:
    /**
     * Verify that the checkpoints mem-usage is tracked correctly
     * at queueing items into the checkpoints.
     */
    void testCheckpointManagerMemUsage();
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