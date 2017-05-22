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
#include "ep_time.h"
#include "failover-table.h"
#include "test_helpers.h"
#include "thread_gate.h"
#include "vbucket_test.h"

#include <thread>

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
    ASSERT_EQ(AddStatus::Success, addOne(key));
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


// Verify that we can pageOut deleted items which have a value associated with
// them - and afterwards the value is null.
TEST_F(EphemeralVBucketTest, PageOutAfterDeleteWithValue) {
    // Add an item which is marked as deleted, but has a body (e.g. system
    // XATTR).
    auto key = makeStoredDocKey("key");
    std::string value = "deleted value";
    Item item(key, 0, /*expiry*/0, value.data(), value.size());
    item.setDeleted();
    ASSERT_EQ(AddStatus::Success, public_processAdd(item));
    ASSERT_EQ(0, vbucket->getNumItems());

    // Check preconditions
    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;
    ASSERT_TRUE(storedVal->isDeleted());
    ASSERT_EQ(value, storedVal->getValue()->to_s());

    // Page it out.
    EXPECT_TRUE(vbucket->pageOut(lock_sv.first, storedVal));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
    EXPECT_FALSE(storedVal->getValue());
}

// NRU: check the seqlist has correct statistics for a create, pageout,
// and (re)create of the same key.
TEST_F(EphemeralVBucketTest, CreatePageoutCreate) {
    auto key = makeStoredDocKey("key");

    // Add a key, then page out.
    ASSERT_EQ(AddStatus::Success, addOne(key));
    {
        auto lock_sv = lockAndFind(key);
        EXPECT_TRUE(vbucket->pageOut(lock_sv.first, lock_sv.second));
    }
    // Sanity check - should have just the one deleted item.
    ASSERT_EQ(0, vbucket->getNumItems());
    ASSERT_EQ(1, mockEpheVB->getLL()->getNumDeletedItems());

    // Test: Set the key again.
    ASSERT_EQ(MutationStatus::WasDirty, setOne(key));

    EXPECT_EQ(1, vbucket->getNumItems());
    EXPECT_EQ(0, mockEpheVB->getLL()->getNumDeletedItems());

    // Finally for good measure, delete again and check the numbers are correct.
    {
        auto lock_sv = lockAndFind(key);
        EXPECT_TRUE(vbucket->pageOut(lock_sv.first, lock_sv.second));
    }
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_EQ(1, mockEpheVB->getLL()->getNumDeletedItems());
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
    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size());
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
    ASSERT_EQ(MutationStatus::WasDirty, setOne(keys[0]));
    for (int i = 1; i < numItems - 1; ++i) {
        ASSERT_EQ(MutationStatus::WasClean, setOne(keys[i]));
    }
    ASSERT_EQ(MutationStatus::WasDirty, setOne(keys[numItems - 1]));

    /* Hash table must have only recent (updated) items */
    EXPECT_EQ(numItems, vbucket->getNumItems());

    /* High Seqno must be 2 * numItems */
    EXPECT_EQ(numItems * 2, vbucket->getHighSeqno());

    /* LinkedList must have 3 stale items */
    EXPECT_EQ(3, mockEpheVB->public_getNumStaleItems());

    EXPECT_EQ(numItems * 2 - /* since 2 items are deduped*/ 2,
              mockEpheVB->public_getNumListItems());
}

TEST_F(EphemeralVBucketTest, GetAndUpdateTtl) {
    const int numItems = 2;

    /* Add 2 keys */
    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    ASSERT_EQ(numItems, vbucket->getNumItems());
    EXPECT_EQ(numItems, vbucket->getHighSeqno());
    EXPECT_EQ(0, mockEpheVB->public_getNumStaleItems());

    /* --- basic test --- */
    /* set the ttl of one item */
    GetValue gv1 = public_getAndUpdateTtl(keys[0], 100);

    /* New seqno should have been used */
    EXPECT_EQ(numItems + 1, vbucket->getHighSeqno());

    /* No.of items in the bucket should NOT change */
    EXPECT_EQ(numItems, vbucket->getNumItems());

    /* No.of items in the list should NOT change */
    EXPECT_EQ(numItems, mockEpheVB->public_getNumListItems());

    /* There should be NO stale items */
    EXPECT_EQ(0, mockEpheVB->public_getNumStaleItems());

    /* --- Repeat the above test with a similated ReadRange --- */
    mockEpheVB->registerFakeReadRange(1, numItems);
    GetValue gv2 = public_getAndUpdateTtl(keys[1], 101);

    /* New seqno should have been used */
    EXPECT_EQ(numItems + 2, vbucket->getHighSeqno());

    /* No.of items in the bucket should remain the same */
    EXPECT_EQ(numItems, vbucket->getNumItems());

    /* No.of items in the sequence list should inc by 1 */
    EXPECT_EQ(numItems + 1, mockEpheVB->public_getNumListItems());

    /* There should be 1 stale item */
    EXPECT_EQ(1, mockEpheVB->public_getNumStaleItems());

    auto seqNoVec = mockEpheVB->getLL()->getAllSeqnoForVerification();
    seqno_t prevSeqNo = 0;

    for (const auto& seqNo : seqNoVec) {
        EXPECT_GT(seqNo, prevSeqNo);
        prevSeqNo = seqNo;
    }
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

// EphemeralVB Tombstone Purging //////////////////////////////////////////////

class EphTombstoneTest : public EphemeralVBucketTest {
protected:
    void SetUp() override {
        EphemeralVBucketTest::SetUp();

        // Store three items to work with.
        keys = generateKeys(3);
        setMany(keys, MutationStatus::WasClean);
        ASSERT_EQ(3, vbucket->getNumItems());
    }
    std::vector<StoredDocKey> keys;
};

// Check an empty seqList is handled correctly.
TEST_F(EphTombstoneTest, ZeroElementPurge) {
    // Create a new empty VB (using parent class SetUp).
    EphemeralVBucketTest::SetUp();
    ASSERT_EQ(0, mockEpheVB->public_getNumListItems());

    EXPECT_EQ(0, mockEpheVB->purgeTombstones(0));
}

// Check a seqList with one element is handled correctly.
TEST_F(EphTombstoneTest, OneElementPurge) {
    // Create a new empty VB (using parent class SetUp).
    EphemeralVBucketTest::SetUp();
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("one")));
    ASSERT_EQ(1, mockEpheVB->public_getNumListItems());

    EXPECT_EQ(0, mockEpheVB->purgeTombstones(0));
}

// Check that nothing is purged if no items are stale.
TEST_F(EphTombstoneTest, NoPurgeIfNoneStale) {
    // Run purger - nothing should be removed.
    EXPECT_EQ(0, mockEpheVB->purgeTombstones(0));
    EXPECT_EQ(keys.size(), vbucket->getNumItems());
}

// Check that deletes are not purged if they are not old enough.
TEST_F(EphTombstoneTest, NoPurgeIfNoneOldEnough) {
    // Delete the first item "now"
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    ASSERT_EQ(2, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // Advance time by 5 seconds and run the EphTombstonePurger specifying a
    // purge_age of 10s - nothing
    // should be purged.
    TimeTraveller theTerminator(5);
    EXPECT_EQ(0, mockEpheVB->purgeTombstones(10));

    EXPECT_EQ(2, vbucket->getNumItems());
    EXPECT_EQ(1, vbucket->getNumInMemoryDeletes());
}

// Check that items should be purged when they are old enough.
TEST_F(EphTombstoneTest, OnePurgeIfDeletedItemOld) {
    // Delete the first item "now"
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    ASSERT_EQ(2, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // Delete the second item at time 30.
    TimeTraveller looper(30);
    softDeleteOne(keys.at(1), MutationStatus::WasDirty);
    ASSERT_EQ(1, vbucket->getNumItems());
    ASSERT_EQ(2, vbucket->getNumInMemoryDeletes());

    // and the third at time 60.
    TimeTraveller looper2(30);
    softDeleteOne(keys.at(2), MutationStatus::WasDirty);
    ASSERT_EQ(0, vbucket->getNumItems());
    ASSERT_EQ(3, vbucket->getNumInMemoryDeletes());

    // Run the EphTombstonePurger specifying a purge_age of 60s - only key0
    // should be purged.
    mockEpheVB->purgeTombstones(60);

    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_EQ(2, vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(4, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
    EXPECT_EQ(nullptr, findValue(keys.at(0)));
    EXPECT_NE(nullptr, findValue(keys.at(1)));
    EXPECT_NE(nullptr, findValue(keys.at(2)));
}

// Check that deleted items can be purged immediately.
TEST_F(EphTombstoneTest, ImmediateDeletedPurge) {
    // Advance to non-zero time.
    TimeTraveller jamesCole(10);

    // Delete the first item at 10s
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    ASSERT_EQ(2, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // Run the EphTombstonePurger specifying a purge_age of 0s - key0 should
    // be immediately purged.
    mockEpheVB->purgeTombstones(0);
    EXPECT_EQ(2, vbucket->getNumItems());
    EXPECT_EQ(0, vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(4, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
    EXPECT_EQ(nullptr, findValue(keys.at(0)));
    EXPECT_NE(nullptr, findValue(keys.at(1)));
    EXPECT_NE(nullptr, findValue(keys.at(2)));
}

// Check that alive, stale items have no constraint on age.
TEST_F(EphTombstoneTest, ImmediatePurgeOfAliveStale) {
    // Perform a mutation on the second element, with a (fake) Range Read in
    // place; causing the initial OSV to be marked as stale and a new OSV to
    // be added for that key.
    auto& seqList = mockEpheVB->getLL()->getSeqList();
    {
        std::lock_guard<std::mutex> rrGuard(
                mockEpheVB->getLL()->getRangeReadLock());
        mockEpheVB->registerFakeReadRange(1, 2);
        ASSERT_EQ(MutationStatus::WasClean, setOne(keys.at(1)));

        // Sanity check - our state is as expected:
        ASSERT_EQ(3, vbucket->getNumItems());
        ASSERT_EQ(4, seqList.size());
        auto staleIt = std::next(seqList.begin());
        auto newIt = seqList.rbegin();
        ASSERT_EQ(staleIt->getKey(), newIt->getKey());
        {
            std::lock_guard<std::mutex> writeGuard(
                    mockEpheVB->getLL()->getListWriteLock());
            ASSERT_TRUE(staleIt->isStale(writeGuard));
            ASSERT_FALSE(newIt->isStale(writeGuard));
        }

        // Attempt a purge - should not remove anything as read range is in
        // place.
        EXPECT_EQ(0, mockEpheVB->purgeTombstones(0));
        EXPECT_EQ(3, vbucket->getNumItems());
        EXPECT_EQ(4, seqList.size());
        EXPECT_EQ(0, vbucket->getPurgeSeqno());

        // Clear the ReadRange (so we can actually purge items) and retry the
        // purge which should now succeed.
        mockEpheVB->getLL()->resetReadRange();
    } // END rrGuard.

    EXPECT_EQ(1, mockEpheVB->purgeTombstones(0));
    EXPECT_EQ(3, vbucket->getNumItems());
    EXPECT_EQ(3, seqList.size());
}

// Test that deleted items purged out of order are handled correctly (and
// highestDeletedPurged is updated).
TEST_F(EphTombstoneTest, PurgeOutOfOrder) {
    // Delete the 3rd item.
    softDeleteOne(keys.at(2), MutationStatus::WasDirty);

    // Run the tombstone purger.
    mockEpheVB->getLL()->resetReadRange();
    ASSERT_EQ(1, mockEpheVB->purgeTombstones(0));
    ASSERT_EQ(2, vbucket->getNumItems());
    EXPECT_EQ(4, vbucket->getPurgeSeqno());

    // Delete the 1st item
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);

    // Run the tombstone purger. This should succeed, but with
    // highestDeletedPurged unchanged.
    ASSERT_EQ(1, mockEpheVB->purgeTombstones(0));
    ASSERT_EQ(1, vbucket->getNumItems());
    EXPECT_EQ(5, vbucket->getPurgeSeqno());
}

// Thread-safety test (intended to run via Valgrind / ASan / TSan) -
// perform sets and deletes on 2 additional threads while the purger
// runs constantly in the main thread.
TEST_F(EphTombstoneTest, ConcurrentPurge) {
    ThreadGate started(2);
    std::atomic<size_t> completed(0);

    auto writer = [this](
            ThreadGate& started, std::atomic<size_t>& completed, size_t id) {
        started.threadUp();
        for (size_t ii = 0; ii < 1000; ++ii) {
            auto key = makeStoredDocKey(std::to_string(id) + ":key_" +
                                        std::to_string(ii));
            Item item(key, /*flags*/ 0, /*expiry*/ 0, key.data(), key.size());
            public_processSet(item, item.getCas());
            softDeleteOne(key, MutationStatus::WasDirty);
        }
        ++completed;
    };
    std::thread fe1{writer, std::ref(started), std::ref(completed), 1};
    std::thread fe2{writer, std::ref(started), std::ref(completed), 2};

    size_t purged = 0;
    do {
        purged += mockEpheVB->purgeTombstones(0);
        std::this_thread::yield();
    } while (completed != 2);

    fe1.join();
    fe2.join();
}

// Test that on a double-delete (delete with a different value) the deleted time
// is updated correctly.
TEST_F(EphTombstoneTest, DoubleDeleteTimeCorrect) {
    // Delete the first item at +0s
    auto key = keys.at(0);
    softDeleteOne(key, MutationStatus::WasDirty);
    auto* delOSV = findValue(key)->toOrderedStoredValue();
    auto initialDelTime = delOSV->getDeletedTime();
    ASSERT_EQ(2, delOSV->getRevSeqno()) << "Should be initial set + 1";

    // Advance to non-zero time.
    const int timeJump = 10;
    TimeTraveller nonZero(timeJump);
    ASSERT_GE(ep_current_time(), initialDelTime + timeJump)
            << "Failed to advance at least " + std::to_string(timeJump) +
                       " seconds from when initial delete "
                       "occcured";

    // Delete the same key again (delete-with-Value), checking the deleted
    // time has changed.
    Item item(key, 0, 0, "deleted", strlen("deleted"));
    item.setDeleted();
    ASSERT_EQ(MutationStatus::WasDirty, public_processSet(item, item.getCas()));
    ASSERT_EQ(3, delOSV->getRevSeqno()) << "Should be initial set + 2";

    auto secondDelTime = delOSV->getDeletedTime();
    EXPECT_GE(secondDelTime, initialDelTime + timeJump);
}

TEST_F(EphemeralVBucketTest, UpdateUpdatesHighestDedupedSeqno) {
    /* Add 3 items and then update all of them */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    ASSERT_EQ(0, mockEpheVB->getLL()->getHighestDedupedSeqno());

    /* Update the items */
    setMany(keys, MutationStatus::WasDirty);

    EXPECT_EQ(6, mockEpheVB->getLL()->getHighestDedupedSeqno());
}

TEST_F(EphemeralVBucketTest, AppendUpdatesHighestDedupedSeqno) {
    /* Add 3 items and then update all of them */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    ASSERT_EQ(0, mockEpheVB->getLL()->getHighestDedupedSeqno());

    {
        auto itr = mockEpheVB->getLL()->makeRangeIterator();

        /* Update the items */
        setMany(keys, MutationStatus::WasClean);
    }

    ASSERT_EQ(6, mockEpheVB->getLL()->getHighestDedupedSeqno());
}

TEST_F(EphemeralVBucketTest, SnapshotHasNoDuplicates) {
    /* Add 2 items and then update all of them */
    const int numItems = 2;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    {
        auto itr = mockEpheVB->getLL()->makeRangeIterator();

        /* Update the items  */
        setMany(keys, MutationStatus::WasClean);
    }

    // backfill to infinity would include both the stale and updated versions,
    // ensure we receive the right number of items
    auto res = mockEpheVB->inMemoryBackfill(
            1, std::numeric_limits<seqno_t>::max());
    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size());
    EXPECT_EQ(numItems * 2, std::get<2>(res));
}

TEST_F(EphemeralVBucketTest, SnapshotIncludesNonDuplicateStaleItems) {
    /* Add 2 items and then update all of them */
    const int numItems = 2;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    {
        auto itr = mockEpheVB->getLL()->makeRangeIterator();

        /* Update the items  */
        setMany(keys, MutationStatus::WasClean);
    }

    // backfill to numItems would /not/ include both the stale and updated
    // versions, ensure we receive only one copy
    auto res = mockEpheVB->inMemoryBackfill(1, numItems);
    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size());
    EXPECT_EQ(numItems * 2, std::get<2>(res));
}

TEST_F(EphemeralVBucketTest, SnapshotHasNoDuplicatesWithInterveningItems) {
    // Add 3 items, begin a rangeRead, add 1 more item, then update the first 2
    const int numItems = 2;

    auto keysToUpdate = generateKeys(numItems);
    auto firstFillerKey = makeStoredDocKey(std::to_string(numItems + 1));
    auto secondFillerKey = makeStoredDocKey(std::to_string(numItems + 2));

    setMany(keysToUpdate, MutationStatus::WasClean);
    EXPECT_EQ(MutationStatus::WasClean, setOne(firstFillerKey));

    {
        auto itr = mockEpheVB->getLL()->makeRangeIterator();

        EXPECT_EQ(MutationStatus::WasClean, setOne(secondFillerKey));

        /* Update the items  */
        setMany(keysToUpdate, MutationStatus::WasClean);
    }

    // backfill to infinity would include both the stale and updated versions,
    // ensure we receive the right number of items
    auto res = mockEpheVB->inMemoryBackfill(
            1, std::numeric_limits<seqno_t>::max());
    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems * 2, std::get<1>(res).size()); // how many items returned
    EXPECT_EQ(numItems * 3, std::get<2>(res)); // extended end of readRange
}

TEST_F(EphemeralVBucketTest, SnapshotHasNoDuplicatesWithMultipleStale) {
    /* repeatedly update two items, ensure the backfill ignores all stale
     * versions */
    const int numItems = 2;
    const int updateIterations = 10;

    auto keys = generateKeys(numItems);

    setMany(keys, MutationStatus::WasClean);
    for (int i = 0; i < updateIterations; ++i) {
        /* Set up a mock backfill, cover all items */
        {
            auto itr = mockEpheVB->getLL()->makeRangeIterator();
            /* Update the items  */
            setMany(keys, MutationStatus::WasClean);
        }
    }

    // backfill to infinity would include both the stale and updated versions,
    // ensure we receive the right number of items
    auto res = mockEpheVB->inMemoryBackfill(
            1, std::numeric_limits<seqno_t>::max());
    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size()); // how many items returned
    EXPECT_EQ(numItems * (updateIterations + 1),
              std::get<2>(res)); // extended end of readRange
}

TEST_F(EphemeralVBucketTest, RangeReadStopsOnInvalidSeqno) {
    /* MB-24376: rangeRead has to stop if it encounters an OSV with a seqno of
     * -1; this item is definitely past the end of the rangeRead, and has not
     * yet had its seqno updated in queueDirty */
    const int numItems = 2;

    // store two items
    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    auto lastKey = makeStoredDocKey(std::to_string(numItems + 1));
    Item i(lastKey, 0, 0, lastKey.data(), lastKey.size());

    // set item with no queueItemCtx - will not be queueDirty'd, and will
    // keep seqno -1
    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(i, i.getCas(), false));

    EXPECT_EQ(-1, mockEpheVB->getLL()->getSeqList().back().getBySeqno());

    auto res = mockEpheVB->inMemoryBackfill(
            1, std::numeric_limits<seqno_t>::max());

    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size());
    EXPECT_EQ(numItems, std::get<2>(res));
}