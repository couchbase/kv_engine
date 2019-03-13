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
// using
// // Committed but and Pending perspective.
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

// Test adding a pending item and then committing it.
TEST_F(HashTablePerspectiveTest, Commit) {
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));

    { // locking scope.
        // Check preconditions - pending item should be found as pending.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        ASSERT_EQ(CommittedState::Pending, result.storedValue->getCommitted());

        // Test
        ht.commit(result.lock, *result.storedValue);
        EXPECT_EQ(CommittedState::CommittedViaPrepare,
                  result.storedValue->getCommitted());
    }

    // Check postconditions - should only have one item for that key.
    auto readView = ht.findForRead(key).storedValue;
    auto writeView = ht.findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);
}

// Test a normal set followed by a pending SyncWrite; then committing the
// pending SyncWrite which should replace the previous committed.
TEST_F(HashTablePerspectiveTest, CommitExisting) {
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));
    auto pending = makePendingItem(key, "valueB"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));
    ASSERT_EQ(2, ht.getNumItems());

    { // locking scope.
        // Check preconditions - item should be found as pending.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        ASSERT_EQ(CommittedState::Pending, result.storedValue->getCommitted());

        // Test
        ht.commit(result.lock, *result.storedValue);

        EXPECT_EQ(CommittedState::CommittedViaPrepare,
                  result.storedValue->getCommitted());
    }

    // Check postconditions - should only have one item for that key.
    auto readView = ht.findForRead(key).storedValue;
    auto writeView = ht.findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);

    EXPECT_EQ(1, ht.getNumItems());

    // Should be CommittedViaPrepare
    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());
}

// Negative test - check it is not possible to commit a non-pending item.
TEST_F(HashTablePerspectiveTest, CommitNonPendingFails) {
    auto committed = makeCommittedItem(key, "valueA"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*committed));

    { // locking scope.
        // Check preconditions - item should be found as committed.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        ASSERT_EQ(CommittedState::CommittedViaMutation,
                  result.storedValue->getCommitted());

        // Test
        EXPECT_THROW(ht.commit(result.lock, *result.storedValue),
                     std::invalid_argument);
    }
}

// Test that a normal set after a Committed SyncWrite is allowed and handled
// correctly.
TEST_F(HashTablePerspectiveTest, MutationAfterCommit) {
    // Setup - Commit a SyncWrite into the HashTable.
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*pending));
    ASSERT_EQ(1, ht.getNumItems());
    { // locking scope.
        // Check preconditions - item should be found as pending.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        ASSERT_EQ(CommittedState::Pending, result.storedValue->getCommitted());
        ht.commit(result.lock, *result.storedValue);

        ASSERT_EQ(CommittedState::CommittedViaPrepare,
                  result.storedValue->getCommitted());
    }

    // Test - attempt to update with a normal Mutation (should be allowed).
    auto committed = makeCommittedItem(key, "mutation"s);
    ASSERT_EQ(MutationStatus::WasDirty, ht.set(*committed));

    // Check postconditions
    // 1. Should only have 1 item (and should be same)
    auto readView = ht.findForRead(key).storedValue;
    auto writeView = ht.findForWrite(key).storedValue;
    EXPECT_TRUE(readView);
    EXPECT_TRUE(writeView);
    EXPECT_EQ(*readView, *writeView);

    // Should be CommittedViaMutation
    EXPECT_EQ(CommittedState::CommittedViaMutation, readView->getCommitted());
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

// Positive test - check that a pending sync delete can be committed.
TEST_F(HashTablePerspectiveTest, SyncDeleteCommit) {
    setupPendingDelete(key);

    // Test - commit the pending SyncDelete.
    { // locking scope.
        auto result = ht.findForWrite(key);
        ASSERT_TRUE(result.storedValue);
        ASSERT_EQ(CommittedState::Pending, result.storedValue->getCommitted());
        ht.commit(result.lock, *result.storedValue);
    }

    // Check postconditions:
    // 1. Upon commit, both read and write view should show same deleted item.
    auto* readView = ht.findForRead(key, TrackReference::Yes, WantsDeleted::Yes)
                             .storedValue;
    ASSERT_TRUE(readView);
    auto* writeView = ht.findForWrite(key).storedValue;
    ASSERT_TRUE(writeView);

    EXPECT_EQ(readView, writeView);
    EXPECT_TRUE(readView->isDeleted());
    EXPECT_FALSE(readView->getValue());

    EXPECT_EQ(CommittedState::CommittedViaPrepare, readView->getCommitted());

    // Should currently have 1 item:
    EXPECT_EQ(1, ht.getNumItems());
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
    ASSERT_EQ(MutationStatus::NotFound,
              ht.insertFromWarmup(*pending, false, false, VALUE_ONLY));

    auto committed = makeCommittedItem(key, "previous committed"s);
    committed->setBySeqno(1);
    ASSERT_EQ(MutationStatus::NotFound,
              ht.insertFromWarmup(*committed, false, false, VALUE_ONLY));

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
