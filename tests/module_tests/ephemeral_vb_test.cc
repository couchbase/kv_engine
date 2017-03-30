/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * Tests specific to Ephemeral VBuckets.
 */

#include "config.h"

#include "../mock/mock_ephemeral_vb.h"
#include "failover-table.h"
#include "test_helpers.h"
#include "vbucket_test.h"

class EphemeralVBucketTest : public VBucketTest {
protected:
    void SetUp() {
        /* to test ephemeral vbucket specific stuff */
        mockEpheVB = new MockEphemeralVBucket(0,
                                              vbucket_state_active,
                                              global_stats,
                                              checkpoint_config,
                                              /*kvshard*/ nullptr,
                                              /*lastSeqno*/ 0,
                                              /*lastSnapStart*/ 0,
                                              /*lastSnapEnd*/ 0,
                                              /*table*/ nullptr,
                                              /*newSeqnoCb*/ nullptr,
                                              config,
                                              VALUE_ONLY);
        /* vbucket manages the life time of mockEpheVB and is a base test class
           ptr of owning type */
        vbucket.reset(mockEpheVB);
    }

    void TearDown() {
        vbucket.reset();
    }

    /* We want a ptr to MockEphemeralVBucket as we test ephemeral vbucket
       specific stuff in this class */
    MockEphemeralVBucket* mockEpheVB;

    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
};

// Verify that attempting to pageOut an item twice has no effect the second
// time.
TEST_F(EphemeralVBucketTest, DoublePageOut) {
    auto key = makeStoredDocKey("key");
    addOne(key, AddStatus::Success);
    ASSERT_EQ(1, vbucket->getNumItems());

    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;
    ASSERT_FALSE(storedVal->isDeleted());

    // Page out the item (once).
    EXPECT_TRUE(vbucket->pageOut(lock_sv.first, storedVal));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());

    // Attempt to page out again - should not be possible.
    EXPECT_FALSE(vbucket->pageOut(lock_sv.first, storedVal));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
}

TEST_F(EphemeralVBucketTest, SetItems) {
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    EXPECT_EQ(numItems, vbucket->getNumItems());
    EXPECT_EQ(numItems, vbucket->getHighSeqno());
}

TEST_F(EphemeralVBucketTest, UpdateItems) {
    /* Add 3 items and then update all of them */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    /* Update the items */
    setMany(keys, MutationStatus::WasDirty);

    EXPECT_EQ(numItems * 2, vbucket->getHighSeqno());
    EXPECT_EQ(numItems, vbucket->getNumItems());
}

TEST_F(EphemeralVBucketTest, SoftDelete) {
    /* Add 3 items and then delete all of them */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    /* soft delete all */
    softDeleteMany(keys, MutationStatus::WasDirty);

    EXPECT_EQ(numItems * 2, vbucket->getHighSeqno());
    EXPECT_EQ(0, vbucket->getNumItems());
}

TEST_F(EphemeralVBucketTest, Backfill) {
    /* Add 3 items and get them by backfill */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    auto res = mockEpheVB->inMemoryBackfill(1, numItems);
    EXPECT_EQ(ENGINE_SUCCESS, res.first);
    EXPECT_EQ(numItems, res.second.size());
}

TEST_F(EphemeralVBucketTest, UpdateDuringBackfill) {
    /* Add 5 items and then update all of them */
    const int numItems = 5;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    /* Set up a mock backfill by setting the range of the backfill */
    mockEpheVB->registerFakeReadRange(2, numItems - 1);

    /* Update the first, middle and last item in the range read and 2 items
       that are outside (before and after) range read */
    setOne(keys[0], MutationStatus::WasDirty);
    for (int i = 1; i < numItems - 1; ++i) {
        setOne(keys[i], MutationStatus::WasClean);
    }
    setOne(keys[numItems - 1], MutationStatus::WasDirty);

    /* Hash table must have only recent (updated) items */
    EXPECT_EQ(numItems, vbucket->getNumItems());

    /* High Seqno must be 2 * numItems */
    EXPECT_EQ(numItems * 2, vbucket->getHighSeqno());

    /* LinkedList must have 3 stale items */
    EXPECT_EQ(3, mockEpheVB->public_getNumStaleItems());

    EXPECT_EQ(numItems * 2 - /* since 2 items are deduped*/ 2,
              mockEpheVB->public_getNumListItems());
}

TEST_F(EphemeralVBucketTest, SoftDeleteDuringBackfill) {
    /* Add 5 items and then soft delete all of them */
    const int numItems = 5;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    /* Set up a mock backfill by setting the range of the backfill */
    mockEpheVB->registerFakeReadRange(2, numItems - 1);

    /* Update the first, middle and last item in the range read and 2 items
       that are outside (before and after) range read */
    softDeleteMany(keys, MutationStatus::WasDirty);

    /* Hash table must have only recent (updated) items */
    EXPECT_EQ(0, vbucket->getNumItems());

    /* High Seqno must be 2 * numItems */
    EXPECT_EQ(numItems * 2, vbucket->getHighSeqno());

    /* LinkedList must have 3 stale items */
    EXPECT_EQ(3, mockEpheVB->public_getNumStaleItems());

    EXPECT_EQ(numItems * 2 - /* since 2 items are deduped*/ 2,
              mockEpheVB->public_getNumListItems());
}
