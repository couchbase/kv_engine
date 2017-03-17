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

#include "config.h"

#include <gtest/gtest.h>
#include <platform/cb_malloc.h>

#include "../mock/mock_basic_ll.h"
#include "hash_table.h"
#include "item.h"
#include "linked_list.h"
#include "stats.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"

#include <limits>
#include <vector>

static EPStats global_stats;

class BasicLinkedListTest : public ::testing::Test {
public:
    BasicLinkedListTest() : ht(global_stats, makeFactory(), 2, 1) {
    }

    static std::unique_ptr<AbstractStoredValueFactory> makeFactory() {
        return std::make_unique<OrderedStoredValueFactory>(global_stats);
    }

protected:
    void SetUp() {
        basicLL = std::make_unique<MockBasicLinkedList>(global_stats);
    }

    void TearDown() {
        /* Like in a vbucket we want the list to be erased before HashTable is
           is destroyed. */
        basicLL.reset();
    }

    /**
     * Adds 'numItems' number of new items to the linked list, from startSeqno.
     * Items to have key as keyPrefixXX, XX being the seqno.
     *
     * Returns the vector of seqnos added.
     */
    std::vector<seqno_t> addNewItemsToList(seqno_t startSeqno,
                                           const std::string& keyPrefix,
                                           const int numItems) {
        const seqno_t last = startSeqno + numItems;
        const std::string val("data");
        OrderedStoredValue* sv;
        std::vector<seqno_t> expectedSeqno;

        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        for (seqno_t i = startSeqno; i < last; ++i) {
            StoredDocKey key = makeStoredDocKey(keyPrefix + std::to_string(i));
            Item item(key,
                      0,
                      0,
                      val.data(),
                      val.length(),
                      /*ext_meta*/ nullptr,
                      /*ext_len*/ 0,
                      /*theCas*/ 0,
                      /*bySeqno*/ i);
            EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

            sv = ht.find(key, TrackReference::Yes, WantsDeleted::No)
                         ->toOrderedStoredValue();
            basicLL->appendToList(lg, *sv);
            basicLL->updateHighSeqno(*sv);
            expectedSeqno.push_back(i);
        }
        return expectedSeqno;
    }

    /**
     * Updates an existing item with key == key and assigns it a seqno of
     * highSeqno + 1. To be called when there is no range read.
     */
    void updateItem(seqno_t highSeqno, const std::string& key) {
        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        OrderedStoredValue* osv = ht.find(makeStoredDocKey(key),
                                          TrackReference::No,
                                          WantsDeleted::Yes)
                                          ->toOrderedStoredValue();
        EXPECT_EQ(SequenceList::UpdateStatus::Success,
                  basicLL->updateListElem(lg, *osv));
        osv->setBySeqno(highSeqno + 1);
        basicLL->updateHighSeqno(*osv);
    }

    /**
     * Updates an existing item with key == key.
     * To be called when there is range read.
     */
    void updateItemDuringRangeRead(seqno_t highSeqno, const std::string& key) {
        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        OrderedStoredValue* osv = ht.find(makeStoredDocKey(key),
                                          TrackReference::No,
                                          WantsDeleted::Yes)
                                          ->toOrderedStoredValue();

        EXPECT_EQ(SequenceList::UpdateStatus::Append,
                  basicLL->updateListElem(lg, *osv));
    }

    /**
     * Deletes an existing item with key == key, puts it onto the linked list
     * and assigns it a seqno of highSeqno + 1.
     * To be called when there is no range read.
     */
    void softDeleteItem(seqno_t highSeqno, const std::string& key) {
        { /* hbl lock scope */
            auto hbl = ht.getLockedBucket(makeStoredDocKey(key));
            StoredValue* sv = ht.unlocked_find(makeStoredDocKey(key),
                                               hbl.getBucketNum(),
                                               WantsDeleted::Yes,
                                               TrackReference::No);

            ht.unlocked_softDelete(
                    hbl.getHTLock(), *sv, /* onlyMarkDeleted */ false);
        }

        updateItem(highSeqno, key);
    }

    /**
     * Release a StoredValue with 'key' from the hash table
     */
    StoredValue::UniquePtr releaseFromHashTable(const std::string& key) {
        auto hbl = ht.getLockedBucket(makeStoredDocKey(key));
        return ht.unlocked_release(hbl, makeStoredDocKey(key));
    }

    /* We need a HashTable because StoredValue is created only in the HashTable
       and then put onto the sequence list */
    HashTable ht;
    std::unique_ptr<MockBasicLinkedList> basicLL;
};

TEST_F(BasicLinkedListTest, SetItems) {
    const int numItems = 3;

    /* Add 3 new items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, std::string("key"), numItems);

    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, TestRangeRead) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;
    std::tie(status, items) = basicLL->rangeRead(1, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
}

TEST_F(BasicLinkedListTest, TestRangeReadTillInf) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;
    std::tie(status, items) =
            basicLL->rangeRead(1, std::numeric_limits<seqno_t>::max());

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
}

TEST_F(BasicLinkedListTest, TestRangeReadFromMid) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;
    std::tie(status, items) = basicLL->rangeRead(2, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems - 1, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
}

TEST_F(BasicLinkedListTest, TestRangeReadStopBeforeEnd) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now request for a range read of just 2 items */
    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;
    std::tie(status, items) = basicLL->rangeRead(1, numItems - 1);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems - 1, items.size());
    EXPECT_EQ(numItems - 1, items.back()->getBySeqno());
}

TEST_F(BasicLinkedListTest, TestRangeReadNegatives) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;

    /* Now do a range read with start > end */
    std::tie(status, items) = basicLL->rangeRead(2, 1);
    EXPECT_EQ(ENGINE_ERANGE, status);

    /* Now do a range read with start > highSeqno */
    std::tie(status, items) = basicLL->rangeRead(numItems + 1, numItems + 2);
    EXPECT_EQ(ENGINE_ERANGE, status);
}

TEST_F(BasicLinkedListTest, UpdateFirstElem) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Update the first item in the list */
    updateItem(numItems, keyPrefix + std::to_string(1));

    /* Check if the updated element has moved to the end */
    std::vector<seqno_t> expectedSeqno = {2, 3, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, UpdateMiddleElem) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Update a middle item in the list */
    updateItem(numItems, keyPrefix + std::to_string(numItems - 1));

    /* Check if the updated element has moved to the end */
    std::vector<seqno_t> expectedSeqno = {1, 3, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, UpdateLastElem) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Update the last item in the list */
    updateItem(numItems, keyPrefix + std::to_string(numItems));

    /* Check if the updated element has moved to the end */
    std::vector<seqno_t> expectedSeqno = {1, 2, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, WriteNewAfterUpdate) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Update an item in the list */
    updateItem(numItems, keyPrefix + std::to_string(numItems - 1));

    /* Add a new item after update */
    addNewItemsToList(
            numItems + /* +1 is update, another +1 for next */ 2, keyPrefix, 1);

    /* Check if the new element is added correctly */
    std::vector<seqno_t> expectedSeqno = {1, 3, 4, 5};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, UpdateDuringRangeRead) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    basicLL->registerFakeReadRange(1, numItems);

    /* Update an item in the list when a fake range read is happening */
    updateItemDuringRangeRead(numItems,
                              keyPrefix + std::to_string(numItems - 1));
}

TEST_F(BasicLinkedListTest, DeletedItem) {
    const std::string keyPrefix("key");
    const int numItems = 1;

    int numDeleted = basicLL->getNumDeletedItems();

    /* Add an item */
    addNewItemsToList(numItems, keyPrefix, 1);

    /* Delete the item */
    softDeleteItem(numItems, keyPrefix + std::to_string(numItems));

    /* Check if the delete is added correctly */
    std::vector<seqno_t> expectedSeqno = {numItems + 1};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
    EXPECT_EQ(numDeleted + 1, basicLL->getNumDeletedItems());
}

TEST_F(BasicLinkedListTest, MarkStale) {
    const std::string keyPrefix("key");
    const int numItems = 1;

    /* To begin with we expect 0 stale items */
    EXPECT_EQ(0, basicLL->getNumStaleItems());

    /* Add an item */
    addNewItemsToList(numItems, keyPrefix, 1);

    /* Release the item from the hash table */
    auto ownedSv = releaseFromHashTable(keyPrefix + std::to_string(numItems));
    OrderedStoredValue* nonOwnedSvPtr = ownedSv.get()->toOrderedStoredValue();
    size_t svSize = ownedSv->size();
    size_t svMetaDataSize = ownedSv->metaDataSize();

    /* Mark the item stale */
    basicLL->markItemStale(std::move(ownedSv));

    /* Check if the StoredValue is marked stale */
    EXPECT_TRUE(nonOwnedSvPtr->isStale());

    /* Check if the stale count incremented to 1 */
    EXPECT_EQ(1, basicLL->getNumStaleItems());

    /* Check if the total item count in the linked list is 1 */
    EXPECT_EQ(1, basicLL->getNumItems());

    /* Check memory usage of the list as it owns the stale item */
    EXPECT_EQ(svSize, basicLL->getMemorySize());
    EXPECT_EQ(svMetaDataSize, basicLL->getMetaDataMemorySize());
}
