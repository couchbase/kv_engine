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
#include "configuration.h"
#include "evp_store_single_threaded_test.h"
#include "stats.h"
#include "vbucket_test.h"

#include <engines/ep/src/ephemeral_vb.h>
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
};
