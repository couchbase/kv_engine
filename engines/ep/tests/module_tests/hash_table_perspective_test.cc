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

    Item makeCommittedItem(StoredDocKey key, std::string value) {
        Item i(key, 0, 0, value.data(), value.size());
        return i;
    }

    HashTable ht;
    StoredDocKey key;
};

// Test that for each of the 4 possible states a key could be in
// (not present, committed only, pending only, committed + pending)
// that accessing via the Committed perspective gives the expected result.

using namespace std::string_literals;

// Test that we can add a Pending item to the HashTable; and then find it when
// using Pending perspective, but *not* via Committed.
TEST_F(HashTablePerspectiveTest, PendingItem) {
    auto i = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(i));

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
    ASSERT_EQ(MutationStatus::WasClean, ht.set(i));

    // Should be able to get via findForWrite (Pending perspective)
    {
        auto item = ht.findForWrite(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::Committed, sv->getCommitted());
        EXPECT_EQ("committed"s, sv->getValue()->to_s());
    }

    // Should also be visible via Committed (Read) perspective.
    {
        auto item = ht.findForRead(key);
        auto* sv = item.storedValue;
        ASSERT_TRUE(sv);
        EXPECT_EQ(CommittedState::Committed, sv->getCommitted());
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
    ASSERT_EQ(MutationStatus::WasClean, ht.set(committed));

    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(pending));

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
        EXPECT_EQ(CommittedState::Committed, sv->getCommitted());
        EXPECT_EQ("committed"s, sv->getValue()->to_s());
    }

    del(ht, key);
}

// Test that the normal set() method cannot be used to change a pending item
// to committed - commit() must be used.
TEST_F(HashTablePerspectiveTest, DenyReplacePendingWithCommitted) {
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(pending));

    // Attempt setting the item again with a committed value.
    auto committed = makeCommittedItem(key, "committed"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite, ht.set(committed));
}

// Test that the normal set() method cannot be used to change a pending item
// to another pending - commit() must be used.
TEST_F(HashTablePerspectiveTest, DenyReplacePendingWithPending) {
    auto pending = makePendingItem(key, "pending"s);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(pending));

    // Attempt setting the item again with a committed value.
    auto pending2 = makePendingItem(key, "pending2"s);
    ASSERT_EQ(MutationStatus::IsPendingSyncWrite, ht.set(pending2));
}
