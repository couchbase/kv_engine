/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Tests specific to Ephemeral VBuckets.
 */
#include "../mock/mock_ephemeral_vb.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_time.h"
#include "failover-table.h"
#include "item.h"
#include "test_helpers.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "thread_gate.h"
#include "vbucket_test.h"

#include <folly/portability/GTest.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <thread>

/**
 * Test fixture for VBucket-level tests specific to Ephemeral VBuckets.
 */
class EphemeralVBucketTest : public VBucketTestBase, public ::testing::Test {
public:
    EphemeralVBucketTest()
        : VBucketTestBase(VBType::Ephemeral, EvictionPolicy::Value) {
    }

protected:
    void SetUp() override {
        checkpoint_config = std::make_unique<CheckpointConfig>(config);
        mockEpheVB = new MockEphemeralVBucket(
                Vbid(0),
                vbucket_state_active,
                global_stats,
                *checkpoint_config,
                /*kvshard*/ nullptr,
                /*lastSeqno*/ 0,
                /*lastSnapStart*/ 0,
                /*lastSnapEnd*/ 0,
                /*table*/ nullptr,
                /*newSeqnoCb*/ nullptr,
                SyncWriteResolvedCallback{},
                NoopSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                ImmediateCkptDisposer,
                config,
                EvictionPolicy::Value,
                std::make_unique<Collections::VB::Manifest>(
                        std::make_shared<Collections::Manager>()));
        /* vbucket manages the life time of mockEpheVB and is a base test class
           ptr of owning type */
        vbucket.reset(mockEpheVB);
    }

    void TearDown() override {
        vbucket.reset();
    }

    /* We want a ptr to MockEphemeralVBucket as we test ephemeral vbucket
       specific stuff in this class */
    MockEphemeralVBucket* mockEpheVB;

    EPStats global_stats;
    std::unique_ptr<CheckpointConfig> checkpoint_config;
    Configuration config;
};

// Verify that attempting to pageOut an item twice has no effect the second
// time.
TEST_F(EphemeralVBucketTest, DoublePageOut) {
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(AddStatus::Success, addOne(key));
    ASSERT_EQ(1, vbucket->getNumItems());

    // Get a normal collections ReadHandle, no need to use a Caching one here
    // as we are only calling pageOut (which has to get the ManifestEntry
    // manually). We must take this before the HashBucketLock to prevent
    // lock order inversion warnings in TSan.
    auto readHandle = vbucket->lockCollections();
    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;
    ASSERT_FALSE(storedVal->isDeleted());

    // Before we page out the item we need to poke the collections stats to be
    // correct. This is because these vbucket test hit internal functions
    // (VBucket::processAdd) instead of the front end endpoints (VBucket::add)
    // where the stat counting is done using the VBNotifyCtx.
    // EphemeralVB::pageOut will delete the item so to prevent a stat counting
    // underflow we should increment it.
    ASSERT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
    readHandle.incrementItemCount(key.getCollectionID());
    ASSERT_EQ(1, readHandle.getItemCount(key.getCollectionID()));

    // Page out the item (once).
    EXPECT_TRUE(vbucket->pageOut(readHandle, lock_sv.first, storedVal, false));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
    EXPECT_EQ(0, readHandle.getItemCount(key.getCollectionID()));

    // We don't need to poke the collections stats again because the item is
    // already deleted (i.e. there will be no stat change).
    // Attempt to page out again - should not be possible.
    EXPECT_FALSE(vbucket->pageOut(readHandle, lock_sv.first, storedVal, false));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
    EXPECT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
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

    // Get a normal collections ReadHandle, no need to use a Caching one here
    // as we are only calling pageOut (which has to get the ManifestEntry
    // manually). We must take this before the HashBucketLock to prevent
    // lock order inversion warnings in TSan.
    auto readHandle = vbucket->lockCollections();
    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;

    // Check preconditions
    ASSERT_TRUE(storedVal->isDeleted());
    ASSERT_EQ(value, storedVal->getValue()->to_s());

    // Page it out.
    EXPECT_TRUE(vbucket->pageOut(readHandle, lock_sv.first, storedVal, false));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_TRUE(storedVal->isDeleted());
    EXPECT_FALSE(storedVal->getValue());
}

TEST_F(EphemeralVBucketTest, PageOutAfterCollectionsDrop) {
    auto key = makeStoredDocKey("key");
    std::string value = "value";
    Item item(key, 0, /*expiry*/ 0, value.data(), value.size());
    ASSERT_EQ(AddStatus::Success, public_processAdd(item));

    // Drop the collection
    CollectionsManifest cm;
    vbucket->updateFromManifest(
            makeManifest(cm.remove(CollectionEntry::defaultC)));

    auto readHandle = vbucket->lockCollections();
    auto lock_sv = lockAndFind(key);
    auto* storedVal = lock_sv.second;

    // Try and page it out, but with MB-43745 we would of seen an exception
    // here.
    EXPECT_FALSE(vbucket->pageOut(readHandle, lock_sv.first, storedVal, true));
    EXPECT_EQ(1, vbucket->getNumItems());
}

// NRU: check the seqlist has correct statistics for a create, pageout,
// and (re)create of the same key.
TEST_F(EphemeralVBucketTest, CreatePageoutCreate) {
    auto key = makeStoredDocKey("key");

    // Add a key, then page out.
    ASSERT_EQ(AddStatus::Success, addOne(key));
    {
        // Get a normal collections ReadHandle, no need to use a Caching one
        // here as we are only calling pageOut (which has to get the
        // ManifestEntry manually). We must take this before the HashBucketLock
        // to prevent lock order inversion warnings in TSan.
        auto readHandle = vbucket->lockCollections();

        // Before we page out the item we need to poke the collections stats to
        // be correct. This is because these vbucket test hit internal functions
        // (VBucket::processAdd) instead of the front end endpoints
        // (VBucket::add) where the stat counting is done using the VBNotifyCtx.
        // EphemeralVB::pageOut will delete the item so to prevent a stat
        // counting underflow we should increment it.
        ASSERT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
        readHandle.incrementItemCount(key.getCollectionID());
        ASSERT_EQ(1, readHandle.getItemCount(key.getCollectionID()));

        auto lock_sv = lockAndFind(key);
        EXPECT_TRUE(vbucket->pageOut(
                readHandle, lock_sv.first, lock_sv.second, false));
        EXPECT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
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
        // Get a normal collections ReadHandle, no need to use a Caching one
        // here as we are only calling pageOut (which has to get the
        // ManifestEntry manually). We must take this before the HashBucketLock
        // to prevent lock order inversion warnings in TSan.
        auto readHandle = vbucket->lockCollections();

        // Before we page out the item we need to poke the collections stats to
        // be correct. This is because these vbucket test hit internal functions
        // (VBucket::processAdd) instead of the front end endpoints
        // (VBucket::add) where the stat counting is done using the VBNotifyCtx.
        // EphemeralVB::pageOut will delete the item so to prevent a stat
        // counting underflow we should increment it.
        ASSERT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
        readHandle.incrementItemCount(key.getCollectionID());
        ASSERT_EQ(1, readHandle.getItemCount(key.getCollectionID()));

        auto lock_sv = lockAndFind(key);
        EXPECT_TRUE(vbucket->pageOut(
                readHandle, lock_sv.first, lock_sv.second, false));
        EXPECT_EQ(0, readHandle.getItemCount(key.getCollectionID()));
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

TEST_F(EphemeralVBucketTest, AddItems) {
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    addMany(keys, AddStatus::Success);
    EXPECT_EQ(numItems, vbucket->getNumItems());
    EXPECT_EQ(numItems, vbucket->getHighSeqno());
}

TEST_F(EphemeralVBucketTest, AddTempItem) {
    /* Add temp item */
    EXPECT_EQ(TempAddStatus::BgFetch, addOneTemp(makeStoredDocKey("one")));

    /* hash table contains the temp item */
    EXPECT_EQ(1, vbucket->getNumTempItems());

    /* linked list and ephemeral vb do not have the temp item */
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_EQ(0, mockEpheVB->public_getNumListItems());

    /* High seqno is still 0 */
    EXPECT_EQ(0, vbucket->getHighSeqno());
    EXPECT_EQ(0, mockEpheVB->public_getListHighSeqno());
}

TEST_F(EphemeralVBucketTest, AddTempItemAndUpdate) {
    const StoredDocKey k = makeStoredDocKey("one");

    /* Add temp item */
    EXPECT_EQ(TempAddStatus::BgFetch, addOneTemp(k));
    ASSERT_EQ(0, vbucket->getNumItems());

    /* Update the temp item (make it non-temp) */
    Item i(k, 0, /*expiry*/ 0, k.data(), k.size());
    EXPECT_EQ(MutationStatus::WasClean, setOne(k));

    /* hash table contains no temp item */
    EXPECT_EQ(0, vbucket->getNumTempItems());

    /* linked list and ephemeral vb have the 1 item */
    EXPECT_EQ(1, vbucket->getNumItems());
    EXPECT_EQ(1, mockEpheVB->public_getNumListItems());

    /* High seqno is 1 */
    EXPECT_EQ(1, vbucket->getHighSeqno());
    EXPECT_EQ(1, mockEpheVB->public_getListHighSeqno());
}

TEST_F(EphemeralVBucketTest, AddTempItemAndSoftDelete) {
    const StoredDocKey k = makeStoredDocKey("one");

    /* Add temp item */
    EXPECT_EQ(TempAddStatus::BgFetch, addOneTemp(k));
    ASSERT_EQ(1, vbucket->getNumTempItems());

    /* SoftDelete the temp item (make it non-temp) */
    softDeleteOne(k, MutationStatus::WasClean);

    /* hash table contains no temp item */
    EXPECT_EQ(0, vbucket->getNumTempItems());

    /* ephemeral vb has 0 items */
    EXPECT_EQ(0, vbucket->getNumItems());

    /* linked list has 1 deleted item */
    EXPECT_EQ(1, mockEpheVB->public_getNumListDeletedItems());
    EXPECT_EQ(1, mockEpheVB->public_getNumListItems());

    /* High seqno is 1 */
    EXPECT_EQ(1, vbucket->getHighSeqno());
    EXPECT_EQ(1, mockEpheVB->public_getListHighSeqno());
}

TEST_F(EphemeralVBucketTest, Backfill) {
    /* Add 3 items and get them by backfill */
    const int numItems = 3;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    const auto rangeItr = mockEpheVB->makeRangeIterator(true /*isBackfill*/);
    ASSERT_TRUE(rangeItr);
    EXPECT_EQ(numItems, rangeItr->count());
}

TEST_F(EphemeralVBucketTest, UpdateDuringBackfill) {
    /* Add 5 items and then update all of them */
    const int numItems = 5;

    auto keys = generateKeys(numItems);
    setMany(keys, MutationStatus::WasClean);

    /* Set up a mock backfill by setting the range of the backfill */
    auto range = mockEpheVB->registerFakeSharedRangeLock(2, numItems - 1);

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
    GetValue gv1 = public_getAndUpdateTtl(keys[0], 100).second;

    /* New seqno should have been used */
    EXPECT_EQ(numItems + 1, vbucket->getHighSeqno());

    /* No.of items in the bucket should NOT change */
    EXPECT_EQ(numItems, vbucket->getNumItems());

    /* No.of items in the list should NOT change */
    EXPECT_EQ(numItems, mockEpheVB->public_getNumListItems());

    /* There should be NO stale items */
    EXPECT_EQ(0, mockEpheVB->public_getNumStaleItems());

    /* --- Repeat the above test with a simulated ReadRange --- */
    auto range = mockEpheVB->registerFakeSharedRangeLock(1, numItems);
    GetValue gv2 = public_getAndUpdateTtl(keys[1], 101).second;

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
    auto range = mockEpheVB->registerFakeSharedRangeLock(2, numItems - 1);

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

        // Need mock time functions to be able to time travel
        initialize_time_functions(get_mock_server_api()->core);

        // Store three items to work with.
        keys = generateKeys(3);
        setMany(keys, MutationStatus::WasClean);
        ASSERT_EQ(3, vbucket->getNumItems());
    }
    std::vector<StoredDocKey> keys;
};

/**
 * MB-31175. We should not be able to delete a tombstone that has been deleted
 * after the HTTombstonePurger starts running as this could cause int underflow
 * and the subsequent deletion of the tombstone before the purgeAge
 */
TEST_F(EphTombstoneTest, DeleteAfterPurgeStarts) {
    // Delete an item after the task starts but before the purgeArge
    {
        TimeTraveller toTheFuture(1985);
        softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    }

    // Check the delete went through
    ASSERT_EQ(2, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // The item has been deleted in the future, run the tombstone purger
    // purgeAge is > delete time so we should only mark one item as stale
    EXPECT_EQ(0, mockEpheVB->markOldTombstonesStale(86400));
    EXPECT_EQ(0, mockEpheVB->public_getNumStaleItems());
    EXPECT_TRUE(findValue(keys.at(0)));
}

// Check an empty seqList is handled correctly.
TEST_F(EphTombstoneTest, ZeroElementPurge) {
    // Create a new empty VB (using parent class SetUp).
    EphemeralVBucketTest::SetUp();
    ASSERT_EQ(0, mockEpheVB->public_getNumListItems());

    EXPECT_EQ(0, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
}

// Check a seqList with one element is handled correctly.
TEST_F(EphTombstoneTest, OneElementPurge) {
    // Create a new empty VB (using parent class SetUp).
    EphemeralVBucketTest::SetUp();
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("one")));
    ASSERT_EQ(1, mockEpheVB->public_getNumListItems());

    EXPECT_EQ(0, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
}

// Check that nothing is purged if no items are stale.
TEST_F(EphTombstoneTest, NoPurgeIfNoneStale) {
    // Run purger - nothing should be removed.
    EXPECT_EQ(0, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
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
    EXPECT_EQ(0, mockEpheVB->markOldTombstonesStale(10));
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());

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

    // Purge 1/2: mark tombstones older than 60s as stale - only key0 should be
    // marked as stale.
    EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(60));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_EQ(2, vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(nullptr, findValue(keys.at(0)));
    EXPECT_NE(nullptr, findValue(keys.at(1)));
    EXPECT_NE(nullptr, findValue(keys.at(2)));

    // Purge 2/2: delete stale items.
    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(4, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
}

/* Do not purge the last (back of the list) deleted stale item */
TEST_F(EphTombstoneTest, DoNotPurgeLastDelete) {
    /* Advance to non-zero time. */
    TimeTraveller jamesCole(10);

    /* Delete a key, it will be the last element in the sequence list */
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    int expectedItems = keys.size() - 1 /*deleted key*/;
    ASSERT_EQ(expectedItems, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    /* Mark deleted items older than 0s as stale */
    EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(expectedItems, vbucket->getNumItems());
    EXPECT_EQ(0, vbucket->getNumInMemoryDeletes());

    /* Try to purge the stale delete, but it should not get purged as it is
     the last element in the list */
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(0, vbucket->getPurgeSeqno())
            << "Should not have purged the last list element";
}

/* Do not purge if stale item is the only item in the list */
TEST_F(EphTombstoneTest, DoNotPurgeTheOnlyElement) {
    /* Create a new empty VB (using parent class SetUp). */
    EphemeralVBucketTest::SetUp();
    const auto key = makeStoredDocKey("one");
    ASSERT_EQ(MutationStatus::WasClean, setOne(key));
    ASSERT_EQ(1, mockEpheVB->public_getNumListItems());

    softDeleteOne(key, MutationStatus::WasDirty);
    EXPECT_EQ(1, vbucket->getNumInMemoryDeletes());

    /* Advance to non-zero time. */
    TimeTraveller jamesCole(10);

    /* Mark stale and then try to purge */
    EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(0, vbucket->getNumItems());
    EXPECT_EQ(0, vbucket->getNumInMemoryDeletes());

    /* Try to purge, but should be unsuccessful as the stale element is
       the only element in the list and hence the last one */
    EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(0, vbucket->getPurgeSeqno());

    /* Check that you can add another element to the list after the purge try
       (to ensure that purgeStaleItems() did not screw up the list) */
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("two")));
    ASSERT_EQ(2, mockEpheVB->public_getNumListItems());
}

// Check that deleted items can be purged immediately.
TEST_F(EphTombstoneTest, ImmediateDeletedPurge) {
    // Advance to non-zero time.
    TimeTraveller jamesCole(10);

    // Delete the first item at 10s
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);

    setOne(makeStoredDocKey("last_key1"));
    int expectedItems = keys.size() - 1 /*deleted key*/ + 1 /*last_key*/;
    ASSERT_EQ(expectedItems, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // Purge 1/2: mark tombstones older than 0s as stale - key0 should be
    // immediately purged.
    EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(0));
    EXPECT_EQ(expectedItems, vbucket->getNumItems());
    EXPECT_EQ(0, vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(nullptr, findValue(keys.at(0)));
    EXPECT_NE(nullptr, findValue(keys.at(1)));
    EXPECT_NE(nullptr, findValue(keys.at(2)));

    // Purge 2/2: delete stale items.
    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(4, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
    EXPECT_NE(vbucket->getPurgeSeqno(), vbucket->getHighSeqno());
}

/**
 * It's not necessary to move the purge seqno when we purge an item in a
 * collection as the collection create/drop events acts as our tombstones
 */
TEST_F(EphTombstoneTest, CollectionErasureDoesNotMovePurgeSeqno) {
    CollectionsManifest cm;
    vbucket->updateFromManifest(
            makeManifest(cm.remove(CollectionEntry::defaultC)));

    EXPECT_EQ(3, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(0, vbucket->getPurgeSeqno());
}

/**
 * It's not necessary to move the purge seqno when we purge a deletion in a
 * collection as the collection create/drop events acts as our tombstones
 */
TEST_F(EphTombstoneTest, CollectionDeletionErasureDoesNotMovePurgeSeqno) {
    softDeleteOne(keys.at(1), MutationStatus::WasDirty);

    CollectionsManifest cm;
    vbucket->updateFromManifest(
            makeManifest(cm.remove(CollectionEntry::defaultC)));

    EXPECT_EQ(3, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(0, vbucket->getPurgeSeqno());
}

/**
 * It's not necessary to move the purge seqno when we replace a deletion as we
 * stream the full extent of the seqlist snapshot so the replacement should be
 * included
 */
TEST_F(EphTombstoneTest, ReplacedDeletePurgeDoesNotMovePurgeSeqno) {
    softDeleteOne(keys.at(1), MutationStatus::WasDirty);

    {
        auto range = mockEpheVB->registerFakeSharedRangeLock(1, 4);
        ASSERT_EQ(MutationStatus::WasClean, setOne(keys.at(1)));
    }

    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(0, vbucket->getPurgeSeqno());
}

// Check that alive, stale items have no constraint on age.
TEST_F(EphTombstoneTest, ImmediatePurgeOfAliveStale) {
    // Perform a mutation on the second element, with a (fake) Range Read in
    // place; causing the initial OSV to be marked as stale and a new OSV to
    // be added for that key.
    auto& seqList = mockEpheVB->getLL()->getSeqList();
    {
        auto range = mockEpheVB->registerFakeSharedRangeLock(1, 2);
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
        EXPECT_EQ(0, mockEpheVB->purgeStaleItems());
        EXPECT_EQ(3, vbucket->getNumItems());
        EXPECT_EQ(4, seqList.size());
        EXPECT_EQ(0, vbucket->getPurgeSeqno());

        // range released at end of scope (so we can actually purge items) and
        // retry the purge which should now succeed.
    } // END range

    // Scan sequenceList for stale items.
    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(3, vbucket->getNumItems());
    EXPECT_EQ(3, seqList.size());
}

// Test that deleted items purged out of order are handled correctly (and
// highestDeletedPurged is updated).
TEST_F(EphTombstoneTest, PurgeOutOfOrder) {
    // Delete the 3rd item.
    softDeleteOne(keys.at(2), MutationStatus::WasDirty);

    setOne(makeStoredDocKey("last_key1"));
    int expectedItems = keys.size() - 1 /*deleted key*/ + 1 /*last_key*/;

    // Run the tombstone purger.
    ASSERT_EQ(1, mockEpheVB->markOldTombstonesStale(0));
    ASSERT_EQ(expectedItems, vbucket->getNumItems());

    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(4, vbucket->getPurgeSeqno());

    // Delete the 1st item
    softDeleteOne(keys.at(0), MutationStatus::WasDirty);
    --expectedItems;

    setOne(makeStoredDocKey("last_key2"));
    expectedItems += 1;

    // Run the tombstone purger. This should succeed, but with
    // highestDeletedPurged unchanged.
    ASSERT_EQ(1, mockEpheVB->markOldTombstonesStale(0));
    ASSERT_EQ(expectedItems, vbucket->getNumItems());

    EXPECT_EQ(1, mockEpheVB->purgeStaleItems());
    EXPECT_EQ(6, vbucket->getPurgeSeqno());
    EXPECT_NE(vbucket->getPurgeSeqno(), vbucket->getHighSeqno());
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
        purged += mockEpheVB->markOldTombstonesStale(0);
        std::this_thread::yield();
    } while (completed != 2);

    fe1.join();
    fe2.join();
    ASSERT_GT(purged, 0);
}

// Test that on a double-delete (delete with a different value) the deleted time
// is updated correctly.
TEST_F(EphTombstoneTest, DoubleDeleteTimeCorrect) {
    // Delete the first item at +0s
    auto key = keys.at(0);
    softDeleteOne(key, MutationStatus::WasDirty);
    auto* delOSV = findValue(key)->toOrderedStoredValue();
    auto initialDelTime = delOSV->getCompletedOrDeletedTime();
    ASSERT_EQ(2, delOSV->getRevSeqno()) << "Should be initial set + 1";

    // Advance to non-zero time.
    const int timeJump = 10;
    TimeTraveller nonZero(timeJump);
    ASSERT_GE(ep_real_time(), initialDelTime + timeJump)
            << "Failed to advance at least " + std::to_string(timeJump) +
                       " seconds from when initial delete "
                       "occcured";

    // Delete the same key again (delete-with-Value), checking the deleted
    // time has changed.
    Item item(key, 0, 0, "deleted", strlen("deleted"));
    item.setDeleted();
    ASSERT_EQ(MutationStatus::WasDirty, public_processSet(item, item.getCas()));
    ASSERT_EQ(3, delOSV->getRevSeqno()) << "Should be initial set + 2";

    auto secondDelTime = delOSV->getCompletedOrDeletedTime();
    EXPECT_GE(secondDelTime, initialDelTime + timeJump);
}

// Check that tombstone purger runs fine in pause-resume mode
TEST_F(EphTombstoneTest, PurgePauseResume) {
    // Delete the second item
    softDeleteOne(keys.at(1), MutationStatus::WasDirty);
    ASSERT_EQ(keys.size() - 1, vbucket->getNumItems());
    ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

    // Add one key as we do not purge the last element
    setOne(makeStoredDocKey("last_key"));

    // Advance time to 30s
    TimeTraveller looper(30);

    // Purge 1/2: mark tombstones older than 10s as stale
    EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(10));
    EXPECT_EQ(keys.size(), vbucket->getNumItems());
    EXPECT_EQ(nullptr, findValue(keys.at(1)));

    // Purge 2/2: delete the stale item. Set the max purge duration to 0 to
    //            simulate pause-resume
    int numPurged = 0, numPaused = -1;
    while (numPurged != 1) {
        numPurged += mockEpheVB->purgeStaleItems([]() { return true; });
        ++numPaused;
    }
    EXPECT_EQ(keys.size() + 1, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
    EXPECT_GE(numPaused, 1)
            << "Test expected to simulate atleast one pause-resume";
}

TEST_F(EphTombstoneTest, PurgePauseResumeWithUpdateAtPausedPoint) {
    // Delete any stale items. Set the max purge duration to 0 to
    // simulate pause-resume
    int numPurged = 0, numPaused = -1;
    while (numPurged != 1) {
        numPurged += mockEpheVB->purgeStaleItems([]() { return true; });
        if (numPaused == -1) {
            // Delete the second item, we actually moving the element at
            // the paused point to end.
            softDeleteOne(keys.at(1), MutationStatus::WasDirty);
            ASSERT_EQ(keys.size() - 1, vbucket->getNumItems());
            ASSERT_EQ(1, vbucket->getNumInMemoryDeletes());

            // Add one key as we do not purge the last element
            setOne(makeStoredDocKey("last_key"));

            // Advance time to 30s
            TimeTraveller looper(30);

            // mark tombstones older than 10s as stale
            EXPECT_EQ(1, mockEpheVB->markOldTombstonesStale(10));
            EXPECT_EQ(keys.size(), vbucket->getNumItems());
            EXPECT_EQ(nullptr, findValue(keys.at(1)));
        }
        ++numPaused;
    }
    EXPECT_EQ(keys.size() + 1, vbucket->getPurgeSeqno())
            << "Should have purged up to 4th update (1st delete, after 3 sets)";
    EXPECT_GE(numPaused, 1)
            << "Test expected to simulate atleast one pause-resume";
}
