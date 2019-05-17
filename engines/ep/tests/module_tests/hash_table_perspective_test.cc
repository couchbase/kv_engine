/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "hash_table.h"
#include "hash_table_test.h"
#include "item.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"

using namespace std::string_literals;

/*
 * Tests related to using the HashTable via the Pending and Committed
 * perspectives, as used by Synchronous Writes.
 */
class HashTablePerspectiveTest : public HashTableTest {
public:
    HashTablePerspectiveTest()
        : ht(global_stats, makeFactory(), 5, 1),
          key("key", CollectionID::Default) {
    }

    /// Put a pending SyncDelete into the HashTable.
    void setupPendingDelete(StoredDocKey key) {
        auto committed = makeCommittedItem(key, "value"s);
        ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));
        { // locking scope.
            auto result = ht.findForWrite(key);
            ASSERT_TRUE(result.storedValue);
            auto deleted = ht.unlocked_softDelete(result.lock,
                                                  *result.storedValue,
                                                  /*onlyMarkDeleted*/ false,
                                                  DeleteSource::Explicit,
                                                  HashTable::SyncDelete::Yes);
            ASSERT_EQ(DeletionStatus::Success, deleted.status);
        }
    }

    HashTable ht;
    StoredDocKey key;
};

// Test that for each of the 4 possible states a key could be in
// (not present, committed only, pending only, committed + pending)
// that accessing via the Committed perspective gives the expected result.

// Test that we can add a Pending item to the HashTable; and then find it when
// using Pending perspective, but *not* via Committed.
TEST_F(HashTablePerspectiveTest, PendingItem) {
    auto i = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*i));

    // Should be able to get via findForWrite (Pending perspective)
    {
        auto item = ht.findForWrite(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
        EXPECT_EQ("pending"s, sv->getValue()->to_s());
    }

    // Should *not* be visible via findForRead (Committed perspective).
    {
        auto item = ht.findForRead(key);
        EXPECT_FALSE(item.storedValue);
    }

    del(ht, key);
}

// Test that we can add a Committed item to the HashTable; and then find it
// using both Committed and Pending perspective.
TEST_F(HashTablePerspectiveTest, CommittedItem) {
    auto i = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*i));

    // Should be able to get via findForWrite (Pending perspective)
    {
        auto item = ht.findForWrite(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::CommittedViaMutation, sv->getCommitted());
        EXPECT_EQ("committed"s, sv->getValue()->to_s());
    }

    // Should also be visible via Committed (Read) perspective.
    {
        auto item = ht.findForRead(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::CommittedViaMutation, sv->getCommitted());
        EXPECT_EQ("committed"s, sv->getValue()->to_s());
    }

    del(ht, key);
}

// Test that when both a pending and committed item exist; then Pending
// perspective returns the pending one and Committed the committed one.
TEST_F(HashTablePerspectiveTest, CorrectItemForEachPersisective) {
    // Setup -create both committed and pending items.
    // Attempt setting the item again with a committed value.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));

    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));

    // Test - check both perspectives find the correct item.
    {
        auto item = ht.findForWrite(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::Pending, sv->getCommitted());
        EXPECT_EQ("pending"s, sv->getValue()->to_s());
    }

    {
        auto item = ht.findForRead(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::CommittedViaMutation, sv->getCommitted());
        EXPECT_EQ("committed"s, sv->getValue()->to_s());
    }

    del(ht, key);
}

// Test that the normal set() method cannot be used to change a pending item
// to committed - commit() must be used.
TEST_F(HashTablePerspectiveTest, DenyReplacePendingWithCommitted) {
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));

    // Attempt setting the item again with a committed value.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite, ht.set(*committed));
}

// Test that the normal set() method cannot be used to change a pending item
// to another pending - commit() must be used.
TEST_F(HashTablePerspectiveTest, DenyReplacePendingWithPending) {
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));

    // Attempt setting the item again with a committed value.
    auto pending2 = makePendingItem(key, "pending2"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite, ht.set(*pending2));
}


// Positive test - check that an item can have a pending delete added
// (SyncDelete).
TEST_F(HashTablePerspectiveTest, SyncDeletePending) {
    // Perform a regular mutation (so we have something to delete).
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));
    ASSERT_EQ(1, ht.getNumItems());

    // Test: Now delete it via a SyncDelete.
    { // locking scope.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        EXPECT_EQ(DeletionStatus::Success,
                  ht.unlocked_softDelete(result.lock,
                                         *result.storedValue,
                                         /*onlyMarkDeleted*/ false,
                                         DeleteSource::Explicit,
                                         HashTable::SyncDelete::Yes)
                          .status);
    }

    // Check postconditions:
    // 1. Original item should still be the same (when looking up via
    // findForRead):
    auto* readView = ht.findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    EXPECT_FALSE(readView->isDeleted());
    EXPECT_EQ(committed->getValue(), readView->getValue());

    // 2. Pending delete should be visible via findForWrite:
    auto* writeView = ht.findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);
    EXPECT_TRUE(writeView->isDeleted());
    EXPECT_EQ(CommittedState::Pending, writeView->getCommitted());
    EXPECT_NE(*readView, *writeView);

    // Should currently have 2 items:
    EXPECT_EQ(2, ht.getNumItems());
}

// Negative test - check that if a key has a pending SyncDelete it cannot
// otherwise be modified.
TEST_F(HashTablePerspectiveTest, PendingSyncWriteToPendingDeleteFails) {
    setupPendingDelete(key);

    // Test - attempt to mutate a key which has a pending SyncDelete against it
    // with a pending SyncWrite.
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite, ht.set(*pending));
}

// Negative test - check that if a key has a pending SyncDelete it cannot
// otherwise be modified.
TEST_F(HashTablePerspectiveTest, PendingSyncDeleteToPendingDeleteFails) {
    setupPendingDelete(key);

    // Test - attempt to mutate a key which has a pending SyncDelete against it
    // with a pending SyncDelete.
    { // locking scope.
        auto result = ht.findForWrite(key);

        ASSERT_EQ(DeletionStatus::IsPendingSyncWrite,
                  ht.unlocked_softDelete(result.lock,
                                         *result.storedValue,
                                         /*onlyMarkDeleted*/ false,
                                         DeleteSource::Explicit,
                                         HashTable::SyncDelete::Yes)
                          .status);
    }
}

// Check that if a pending SyncWrite is added _before_ a Committed one (to the
// same key), then findforWrite finds the pending one.
// (While normally pending is added _after_ the existing Committed; during
// warmup we load pending first.)
TEST_F(HashTablePerspectiveTest, WarmupPendingAddedBeforeCommited) {
    // Setup - Insert pending then committed.
    auto pending = makePendingItem(key, "pending"s);
    pending->setBySeqno(2);
    ASSERT_EQ(
            MutationStatus::NotFound,
            ht.insertFromWarmup(*pending, false, false, EvictionPolicy::Value));

    auto committed = makeCommittedItem(key, "previous committed"s);
    committed->setBySeqno(1);
    ASSERT_EQ(MutationStatus::NotFound,
              ht.insertFromWarmup(
                      *committed, false, false, EvictionPolicy::Value));

    // Test - check that findForRead finds the committed one, and findForWrite
    // the pending one.
    auto* readView = ht.findForRead(key).storedValue;
    ASSERT_TRUE(readView);
    EXPECT_TRUE(readView->isCommitted());
    EXPECT_EQ(1, readView->getBySeqno());

    auto* writeView = ht.findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);
    EXPECT_TRUE(writeView->isPending());
    EXPECT_EQ(2, writeView->getBySeqno());
}

// CHeck that findOnlyCommitted only finds committed items.
TEST_F(HashTablePerspectiveTest, findOnlyCommitted) {
    // Setup -create both committed and pending items with same key, then
    // a pending item under another key.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));
    auto prepared = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*prepared));

    auto pendingKey = StoredDocKey("pending", CollectionID::Default);
    auto pending2 = makePendingItem(pendingKey, "pending2"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending2));

    // Test
    // 1) Check looking for a non-existing key finds nothing.
    {
        auto nonExistentKey = StoredDocKey("missing", CollectionID::Default);
        auto nonExistent = ht.findOnlyCommitted(nonExistentKey);
        EXPECT_FALSE(nonExistent.storedValue);
        EXPECT_TRUE(nonExistent.lock.getHTLock()) << "Mutex should be locked";
    }

    // 2) Check looking for the committed&pending key returns committed
    {
        auto actual = ht.findOnlyCommitted(key);
        ASSERT_TRUE(actual.storedValue);
        EXPECT_EQ(*committed, *actual.storedValue->toItem(Vbid{0}));
        EXPECT_TRUE(actual.lock.getHTLock()) << "Mutex should be locked";
    }

    // 3) Check looking for the pending key returns nothing
    {
        auto actual = ht.findOnlyCommitted(pendingKey);
        EXPECT_FALSE(actual.storedValue);
        EXPECT_TRUE(actual.lock.getHTLock()) << "Mutex should be locked";
    }
}

// CHeck that findOnlyPrepared only finds prepared items.
TEST_F(HashTablePerspectiveTest, findOnlyPrepared) {
    // Setup -create both committed and prepared items with same key, then
    // a committed item under another key.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));
    auto prepared = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*prepared));

    auto committedKey = StoredDocKey("committed", CollectionID::Default);
    auto committed2 = makeCommittedItem(committedKey, "committed2"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed2));

    // Test
    // 1) Check looking for a non-existing key finds nothing.
    {
        auto nonExistentKey = StoredDocKey("missing", CollectionID::Default);
        auto nonExistent = ht.findOnlyPrepared(nonExistentKey);
        EXPECT_FALSE(nonExistent.storedValue);
        EXPECT_TRUE(nonExistent.lock.getHTLock()) << "Mutex should be locked";
    }

    // 2) Check looking for the committed&prepared key returns prepared
    {
        auto actual = ht.findOnlyPrepared(key);
        ASSERT_TRUE(actual.storedValue);
        auto actualItem =
                actual.storedValue->toItem(Vbid{0},
                                           StoredValue::HideLockedCas::No,
                                           StoredValue::IncludeValue::Yes,
                                           prepared->getDurabilityReqs());
        EXPECT_EQ(*prepared, *actualItem);
        EXPECT_TRUE(actual.lock.getHTLock()) << "Mutex should be locked";
    }

    // 3) Check looking for the committed key returns nothing
    {
        auto actual = ht.findOnlyPrepared(committedKey);
        EXPECT_FALSE(actual.storedValue);
        EXPECT_TRUE(actual.lock.getHTLock()) << "Mutex should be locked";
    }
}
