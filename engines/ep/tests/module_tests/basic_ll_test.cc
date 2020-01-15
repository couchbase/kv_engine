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
#include <platform/cb_malloc.h>

#include "../mock/mock_basic_ll.h"
#include "hash_table.h"
#include "item.h"
#include "linked_list.h"
#include "stats.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"

#include <folly/portability/GTest.h>

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
                      PROTOCOL_BINARY_RAW_BYTES,
                      /*theCas*/ 0,
                      /*bySeqno*/ i);
            EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

            sv = ht.findForWrite(key).storedValue->toOrderedStoredValue();

            std::lock_guard<std::mutex> listWriteLg(
                    basicLL->getListWriteLock());
            basicLL->appendToList(lg, listWriteLg, *sv);
            basicLL->updateHighSeqno(listWriteLg, *sv);
            expectedSeqno.push_back(i);
        }
        return expectedSeqno;
    }

    /**
     * Adds one item without a seqno to the linked list
     */
    void addItemWithoutSeqno(const std::string& key) {
        const std::string val("data");

        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        StoredDocKey sKey = makeStoredDocKey(key);
        Item item(sKey, 0, 0, sKey.data(), sKey.size());

        EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

        OrderedStoredValue* sv =
                ht.findForWrite(sKey).storedValue->toOrderedStoredValue();

        std::lock_guard<std::mutex> listWriteLg(basicLL->getListWriteLock());
        basicLL->appendToList(lg, listWriteLg, *sv);
    }

    void addStaleItem(const std::string& key, seqno_t seqno) {
        const std::string val("data");

        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        /* add an item */
        StoredDocKey sKey = makeStoredDocKey(key);
        Item item(sKey,
                  0,
                  0,
                  val.data(),
                  val.length(),
                  PROTOCOL_BINARY_RAW_BYTES,
                  /*theCas*/ 0,
                  /*bySeqno*/ seqno);
        EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

        auto res = ht.findForWrite(sKey);
        ASSERT_TRUE(res.storedValue);
        auto* sv = res.storedValue->toOrderedStoredValue();
        std::lock_guard<std::mutex> listWriteLg(basicLL->getListWriteLock());
        basicLL->appendToList(lg, listWriteLg, *sv);
        basicLL->updateHighSeqno(listWriteLg, *sv);

        /* Mark stale */
        auto ownedSV = ht.unlocked_release(res.lock, res.storedValue);
        basicLL->markItemStale(listWriteLg, std::move(ownedSV), nullptr);
    }

    /**
     * Updates an existing item with key == key and assigns it a seqno of
     * highSeqno + 1. To be called when there is no range read.
     */
    void updateItem(seqno_t highSeqno, const std::string& key) {
        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        auto* sv = ht.findForWrite(makeStoredDocKey(key)).storedValue;
        ASSERT_TRUE(sv);
        auto* osv = sv->toOrderedStoredValue();

        std::lock_guard<std::mutex> listWriteLg(basicLL->getListWriteLock());
        EXPECT_EQ(SequenceList::UpdateStatus::Success,
                  basicLL->updateListElem(lg, listWriteLg, *osv));
        osv->setBySeqno(highSeqno + 1);
        basicLL->updateHighSeqno(listWriteLg, *osv);
    }

    /**
     * Updates an existing item with key == key.
     * To be called when there is range read.
     */
    void updateItemDuringRangeRead(seqno_t highSeqno, const std::string& key) {
        const std::string val("data");

        /* Get a fake sequence lock */
        std::mutex fakeSeqLock;
        std::lock_guard<std::mutex> lg(fakeSeqLock);

        auto docKey = makeStoredDocKey(key);
        auto res = ht.findForWrite(docKey);
        EXPECT_TRUE(res.storedValue);
        auto* osv = res.storedValue->toOrderedStoredValue();

        std::lock_guard<std::mutex> listWriteLg(basicLL->getListWriteLock());
        EXPECT_EQ(SequenceList::UpdateStatus::Append,
                  basicLL->updateListElem(lg, listWriteLg, *osv));

        /* Release the current sv from the HT */
        auto ownedSv = ht.unlocked_release(res.lock, res.storedValue);

        /* Add a new storedvalue for the append */
        Item itm(docKey,
                 0,
                 0,
                 val.data(),
                 val.length(),
                 PROTOCOL_BINARY_RAW_BYTES,
                 /*theCas*/ 0,
                 /*bySeqno*/ highSeqno + 1);
        auto* newSv = ht.unlocked_addNewStoredValue(res.lock, itm);
        basicLL->markItemStale(listWriteLg, std::move(ownedSv), newSv);

        basicLL->appendToList(
                lg, listWriteLg, *(newSv->toOrderedStoredValue()));
        basicLL->updateHighSeqno(listWriteLg, *(newSv->toOrderedStoredValue()));
    }

    /**
     * Deletes an existing item with key == key, puts it onto the linked list
     * and assigns it a seqno of highSeqno + 1.
     * To be called when there is no range read.
     */
    void softDeleteItem(seqno_t highSeqno, const std::string& key) {
        { /* hbl lock scope */
            auto result = ht.findForWrite(makeStoredDocKey(key));

            ht.unlocked_softDelete(result.lock,
                                   *result.storedValue,
                                   /* onlyMarkDeleted */ false,
                                   DeleteSource::Explicit);
        }

        updateItem(highSeqno, key);
    }

    /**
     * Release a StoredValue with 'key' from the hash table
     */
    StoredValue::UniquePtr releaseFromHashTable(const std::string& key) {
        auto res = ht.findForWrite(makeStoredDocKey(key));
        EXPECT_TRUE(res.storedValue);
        return ht.unlocked_release(res.lock, res.storedValue);
    }

    /**
     * Creates an optional 'RangeIterator'. Expected to create the optional
     * one always.
     */
    SequenceList::RangeIterator getRangeIterator() {
        auto itrOptional = basicLL->makeRangeIterator(true /*isBackfill*/);
        EXPECT_TRUE(itrOptional);
        return std::move(*itrOptional);
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
    std::vector<UniqueItemPtr> items;
    seqno_t endSeqno;
    std::tie(status, items, endSeqno) = basicLL->rangeRead(1, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
    EXPECT_EQ(numItems, endSeqno);
}

TEST_F(BasicLinkedListTest, TestRangeReadTillInf) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<UniqueItemPtr> items;
    seqno_t endSeqno;
    std::tie(status, items, endSeqno) =
            basicLL->rangeRead(1, std::numeric_limits<seqno_t>::max());

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
    EXPECT_EQ(numItems, endSeqno);
}

TEST_F(BasicLinkedListTest, TestRangeReadFromMid) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<UniqueItemPtr> items;
    seqno_t endSeqno;
    std::tie(status, items, endSeqno) = basicLL->rangeRead(2, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems - 1, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
    EXPECT_EQ(numItems, endSeqno);
}

TEST_F(BasicLinkedListTest, TestRangeReadStopBeforeEnd) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Now request for a range read of just 2 items */
    ENGINE_ERROR_CODE status;
    std::vector<UniqueItemPtr> items;
    seqno_t endSeqno;
    std::tie(status, items, endSeqno) = basicLL->rangeRead(1, numItems - 1);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems - 1, items.size());
    EXPECT_EQ(numItems - 1, items.back()->getBySeqno());
    EXPECT_EQ(numItems - 1, endSeqno);
}

TEST_F(BasicLinkedListTest, TestRangeReadNegatives) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    ENGINE_ERROR_CODE status;
    std::vector<UniqueItemPtr> items;

    /* Now do a range read with start > end */
    std::tie(status, items, std::ignore) = basicLL->rangeRead(2, 1);
    EXPECT_EQ(ENGINE_ERANGE, status);

    /* Now do a range read with start > highSeqno */
    std::tie(status, items, std::ignore) =
            basicLL->rangeRead(numItems + 1, numItems + 2);
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

    auto range = basicLL->registerFakeSharedRangeLock(1, numItems);

    /* Update an item in the list when a fake range read is happening */
    updateItemDuringRangeRead(numItems,
                              keyPrefix + std::to_string(numItems - 1));

    /* Check if the new element is added correctly */
    std::vector<seqno_t> expectedSeqno = {1, 2, 3, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, DeletedItem) {
    const std::string keyPrefix("key");
    const int numItems = 1;

    int numDeleted = basicLL->getNumDeletedItems();

    /* Add an item */
    addNewItemsToList(numItems, keyPrefix, 1);

    /* Delete the item */
    softDeleteItem(numItems, keyPrefix + std::to_string(numItems));
    basicLL->updateNumDeletedItems(false, true);

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
    OrderedStoredValue* nonOwnedSvPtr = ownedSv->toOrderedStoredValue();
    size_t svSize = ownedSv->size();
    size_t svMetaDataSize = ownedSv->metaDataSize();

    // obtain a replacement SV
    addNewItemsToList(numItems + 1, keyPrefix, 1);
    OrderedStoredValue* replacement =
            ht.findForWrite(makeStoredDocKey(keyPrefix +
                                             std::to_string(numItems + 1)))
                    .storedValue->toOrderedStoredValue();

    /* Mark the item stale */
    {
        std::lock_guard<std::mutex> writeGuard(basicLL->getListWriteLock());
        basicLL->markItemStale(writeGuard, std::move(ownedSv), replacement);
    }

    /* Check if the StoredValue is marked stale */
    {
        std::lock_guard<std::mutex> writeGuard(basicLL->getListWriteLock());
        EXPECT_TRUE(nonOwnedSvPtr->isStale(writeGuard));
    }

    /* Check if the stale count incremented to 1 */
    EXPECT_EQ(1, basicLL->getNumStaleItems());

    /* Check if the total item count in the linked list is 2 */
    EXPECT_EQ(2, basicLL->getNumItems());

    /* Check memory usage of the list as it owns the stale item */
    EXPECT_EQ(svSize, basicLL->getStaleValueBytes());
    EXPECT_EQ(svMetaDataSize, basicLL->getStaleMetadataBytes());
}

TEST_F(BasicLinkedListTest, RangeIterator) {
    const int numItems = 3;

    /* Add 3 new items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, std::string("key"), numItems);

    auto itr = getRangeIterator();

    std::vector<seqno_t> actualSeqno;

    /* Read all the items with the iterator */
    while (itr.curr() != itr.end()) {
        actualSeqno.push_back((*itr).getBySeqno());
        ++itr;
    }
    EXPECT_EQ(expectedSeqno, actualSeqno);
}

TEST_F(BasicLinkedListTest, RangeIteratorNoItems) {
    auto itr = getRangeIterator();
    /* Since there are no items in the list to iterate over, we expect itr start
       to be end */
    EXPECT_EQ(itr.curr(), itr.end());
}

TEST_F(BasicLinkedListTest, RangeIteratorSingleItem) {
    /* Add an item */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, std::string("key"), 1);

    auto itr = getRangeIterator();

    std::vector<seqno_t> actualSeqno;
    /* Read all the items with the iterator */
    while (itr.curr() != itr.end()) {
        actualSeqno.push_back((*itr).getBySeqno());
        ++itr;
    }
    EXPECT_EQ(expectedSeqno, actualSeqno);
}

TEST_F(BasicLinkedListTest, RangeIteratorOverflow) {
    const int numItems = 1;
    bool caughtOutofRangeExcp = false;

    /* Add an item */
    addNewItemsToList(1, std::string("key"), numItems);

    auto itr = getRangeIterator();

    /* Iterator till end */
    while (itr.curr() != itr.end()) {
        ++itr;
    }

    /* Try iterating beyond the end and expect exception to be thrown */
    try {
        ++itr;
    } catch (std::out_of_range&) {
        caughtOutofRangeExcp = true;
    }
    EXPECT_TRUE(caughtOutofRangeExcp);
}

TEST_F(BasicLinkedListTest, RangeIteratorDeletion) {
    const int numItems = 3;

    /* Add 3 new items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, std::string("key"), numItems);

    /* Check if second range reader can read items after the first one is
       deleted */
    for (int i = 0; i < 2; ++i) {
        auto itr = getRangeIterator();

        std::vector<seqno_t> actualSeqno;

        /* Read all the items with the iterator */
        while (itr.curr() != itr.end()) {
            actualSeqno.push_back((*itr).getBySeqno());
            ++itr;
        }
        EXPECT_EQ(expectedSeqno, actualSeqno);

        /* itr is deleted as each time we loop */
    }
}

TEST_F(BasicLinkedListTest, RangeIteratorAddNewItemDuringRead) {
    const int numItems = 3;

    /* Add 3 new items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, std::string("key"), numItems);

    {
        auto itr = getRangeIterator();

        std::vector<seqno_t> actualSeqno;

        /* Read one item */
        actualSeqno.push_back((*itr).getBySeqno());
        ++itr;

        /* Add a new item */
        addNewItemsToList(numItems + 1 /* start */, std::string("key"), 1);

        /* Read the other items */
        while (itr.curr() != itr.end()) {
            actualSeqno.push_back((*itr).getBySeqno());
            ++itr;
        }
        EXPECT_EQ(expectedSeqno, actualSeqno);

        /* itr is deleted */
    }

    /* Now create new iterator and if we can read all elements */
    expectedSeqno.push_back(numItems + 1);

    {
        auto itr = getRangeIterator();

        std::vector<seqno_t> actualSeqno;

        /* Read the other items */
        while (itr.curr() != itr.end()) {
            actualSeqno.push_back((*itr).getBySeqno());
            ++itr;
        }
        EXPECT_EQ(expectedSeqno, actualSeqno);
    }
}

TEST_F(BasicLinkedListTest, RangeIteratorUpdateItemDuringRead) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 new items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, keyPrefix, numItems);

    {
        auto itr = getRangeIterator();

        std::vector<seqno_t> actualSeqno;

        /* Read one item */
        actualSeqno.push_back((*itr).getBySeqno());
        ++itr;

        /* Update an item */
        updateItemDuringRangeRead(numItems /*highSeqno*/,
                                  keyPrefix + std::to_string(2));

        /* Read the other items */
        while (itr.curr() != itr.end()) {
            actualSeqno.push_back((*itr).getBySeqno());
            ++itr;
        }
        EXPECT_EQ(expectedSeqno, actualSeqno);

        /* itr is deleted */
    }

    /* Now create new iterator and if we can read all but duplicate elements */
    seqno_t exp[] = {1, 3, 4};
    expectedSeqno =
            std::vector<seqno_t>(exp, exp + sizeof(exp) / sizeof(seqno_t));

    {
        auto itr = getRangeIterator();
        std::vector<seqno_t> actualSeqno;

        /* Read the other items */
        while (itr.curr() != itr.end()) {
            actualSeqno.push_back((*itr).getBySeqno());
            ++itr;
        }
        EXPECT_EQ(expectedSeqno, actualSeqno);
    }
}

/* Creates 2 range iterators such that iterator2 is created after iterator1
   has read all items, and has hence released the rangeReadLock, but before
   iterator1 is deleted */
TEST_F(BasicLinkedListTest, MultipleRangeIterator_MB24474) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, keyPrefix, numItems);

    /* Create a 'RangeIterator' on the heap so that it can be deleted before
       the function scope ends */
    auto itr1Optional =
            std::make_unique<boost::optional<SequenceList::RangeIterator>>(
                    basicLL->makeRangeIterator(true /*isBackfill*/));
    auto itr1 = std::move(**itr1Optional);

    /* Read all items */
    std::vector<seqno_t> actualSeqno;
    for (; itr1.curr() != itr1.end(); ++(itr1)) {
        actualSeqno.push_back((*itr1).getBySeqno());
    }
    EXPECT_EQ(expectedSeqno, actualSeqno);

    /* Create another 'RangeIterator' after all items are read from itr1, but
       before itr1 is deleted */
    auto itr2 = getRangeIterator();
    itr1Optional.reset();

    /* Now read the items after itr1 is deleted */
    actualSeqno.clear();
    while (itr2.curr() != itr2.end()) {
        actualSeqno.push_back((*itr2).getBySeqno());
        ++itr2;
    }
    EXPECT_EQ(expectedSeqno, actualSeqno);
}

TEST_F(BasicLinkedListTest, ConcurrentRangeIterators) {
    const int numItems = 3;
    const std::string keyPrefix("key");

    /* Add 3 items */
    std::vector<seqno_t> expectedSeqno =
            addNewItemsToList(1, keyPrefix, numItems);

    // multiple iterators should be permitted
    auto itr1 = getRangeIterator();
    auto itr2 = getRangeIterator();

    std::vector<seqno_t> seqnos1;
    std::vector<seqno_t> seqnos2;

    /* Read all the items with the iterators */
    for (int i = 0; i < 3; i++) {
        seqnos1.push_back(itr1->getBySeqno());
        seqnos2.push_back(itr2->getBySeqno());
        ++itr1;
        ++itr2;
    }

    EXPECT_TRUE(itr1.curr() == itr1.end());
    EXPECT_TRUE(itr2.curr() == itr2.end());

    EXPECT_TRUE(seqnos1 == seqnos2);

    EXPECT_EQ(3, seqnos1.back());
}

TEST_F(BasicLinkedListTest, ConcurrentRangeReadShared) {
    auto guard1 = basicLL->tryLockSeqnoRangeShared(1, 2);
    auto guard2 = basicLL->tryLockSeqnoRangeShared(1, 2);

    EXPECT_TRUE(guard1);
    EXPECT_TRUE(guard2);
}

TEST_F(BasicLinkedListTest, ConcurrentRangeReadExclusive) {
    {
        auto guard1 = basicLL->tryLockSeqnoRangeShared(1, 2);
        auto guard2 = basicLL->tryLockSeqnoRange(1, 2);

        EXPECT_TRUE(guard1);
        EXPECT_FALSE(guard2);
    }

    {
        auto guard1 = basicLL->tryLockSeqnoRangeShared(1, 2);
        auto guard2 = basicLL->tryLockSeqnoRangeShared(1, 2);
        auto guard3 = basicLL->tryLockSeqnoRange(1, 2);

        EXPECT_TRUE(guard1);
        EXPECT_TRUE(guard2);
        EXPECT_FALSE(guard3);

        guard1.reset();

        guard3 = basicLL->tryLockSeqnoRange(1, 2);
        EXPECT_FALSE(guard3);

        guard2.reset();

        guard3 = basicLL->tryLockSeqnoRange(1, 2);
        EXPECT_TRUE(guard3);
    }

    {
        auto guard1 = basicLL->tryLockSeqnoRange(1, 2);
        auto guard2 = basicLL->tryLockSeqnoRangeShared(1, 2);

        EXPECT_TRUE(guard1);
        EXPECT_FALSE(guard2);
    }

    {
        auto guard1 = basicLL->tryLockSeqnoRange(1, 2);
        auto guard2 = basicLL->tryLockSeqnoRange(1, 2);

        EXPECT_TRUE(guard1);
        EXPECT_FALSE(guard2);

        guard1.reset();

        guard2 = basicLL->tryLockSeqnoRange(1, 2);
        EXPECT_TRUE(guard2);
    }
}

TEST_F(BasicLinkedListTest, RangeReadStopsOnInvalidSeqno) {
    /* MB-24376: rangeRead has to stop if it encounters an OSV with a seqno of
     * -1; this item is definitely past the end of the rangeRead, and has not
     * yet had its seqno updated in queueDirty */
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Add a key that does not yet have a vaild seqno (say -1) */
    addItemWithoutSeqno("key3");

    EXPECT_EQ(-1, basicLL->getSeqList().back().getBySeqno());

    auto res = basicLL->rangeRead(1, std::numeric_limits<seqno_t>::max());

    EXPECT_EQ(ENGINE_SUCCESS, std::get<0>(res));
    EXPECT_EQ(numItems, std::get<1>(res).size());
    EXPECT_EQ(numItems, std::get<2>(res));
}

/* 'EphemeralVBucket' (class that has the list) never calls the purge of last
   element, but the list must support generic purge (that is purge until any
   element). */
TEST_F(BasicLinkedListTest, PurgeTillLast) {
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Add a stale item */
    addStaleItem("stale", numItems + 1);
    EXPECT_EQ(numItems + 1, basicLL->getNumItems());
    EXPECT_EQ(1, basicLL->getNumStaleItems());

    /* Purge the last item */
    EXPECT_EQ(1, basicLL->purgeTombstones(numItems + 1));
    EXPECT_EQ(numItems, basicLL->getNumItems());
    EXPECT_EQ(0, basicLL->getNumStaleItems());

    /* Should be able to add elements to the list after the purger has run */
    addNewItemsToList(
            numItems + 2 /*startseqno*/, keyPrefix, 1 /*add one element*/);
    std::vector<seqno_t> expectedSeqno = {1, 2, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

/* 'EphemeralVBucket' (class that has the list) never calls the purge of the
   only element, but the list must support generic purge (that is purge until
   any element). */
TEST_F(BasicLinkedListTest, PurgeTheOnlyElement) {
    /* Add a stale item */
    addStaleItem("stale", 1);
    EXPECT_EQ(1, basicLL->getNumItems());
    EXPECT_EQ(1, basicLL->getNumStaleItems());

    /* Purge the only item */
    EXPECT_EQ(1, basicLL->purgeTombstones(1));
    EXPECT_EQ(0, basicLL->getNumItems());
    EXPECT_EQ(0, basicLL->getNumStaleItems());

    /* Should be able to add elements to the list after the purger has run */
    addNewItemsToList(2 /*startseqno*/, "key", 1 /*add one element*/);
    EXPECT_EQ(1, basicLL->getNumItems());
}

/* 'EphemeralVBucket' (class that has the list) never calls the purge of
   elements beyond the last element, but the list must support generic purge
   (that is purge until any element).
   This is a negative test case which checks that 'purgeTombstones' completes
   correctly even in the case of a wrong input */
TEST_F(BasicLinkedListTest, PurgeBeyondLast) {
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Add a stale item */
    addStaleItem("stale", numItems + 1);
    EXPECT_EQ(numItems + 1, basicLL->getNumItems());
    EXPECT_EQ(1, basicLL->getNumStaleItems());

    /* Purge beyond the last item */
    EXPECT_EQ(1, basicLL->purgeTombstones(numItems + 1000));
    EXPECT_EQ(numItems, basicLL->getNumItems());
    EXPECT_EQ(0, basicLL->getNumStaleItems());

    /* Should be able to add elements to the list after the purger has run */
    addNewItemsToList(
            numItems + 2 /*startseqno*/, keyPrefix, 1 /*add one element*/);
    std::vector<seqno_t> expectedSeqno = {1, 2, 4};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, UpdateDuringPurge) {
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Start the purger, in between send an update. Update is done at a point
       (first key here) in the linked list that is already visited by
       the purger, hence we do not expect the update to create a stale copy of
       the updated item */

    size_t timesShouldPauseCalled = 0;
    basicLL->purgeTombstones(numItems, {}, [&]() {
        /* By sending the update in the callback, we are simulating a
           scenario where an update happens in between the purge */
        if (timesShouldPauseCalled == 1) {
            /* update first key */
            updateItem(numItems, keyPrefix + std::to_string(1));
        }
        ++timesShouldPauseCalled;
        return false;
    });

    /* Update should succeed */
    EXPECT_EQ(numItems + 1, basicLL->getHighSeqno());
    /* Update should not create stale items */
    EXPECT_EQ(0, basicLL->getNumStaleItems());
}

TEST_F(BasicLinkedListTest, RangeIteratorRefusedDuringPurge) {
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    basicLL->purgeTombstones(numItems, {}, [&]() {
        auto itr = basicLL->makeRangeIterator(true /*isBackfill*/);

        // should not have been allowed to make a range iterator during purge
        EXPECT_FALSE(itr);
        return false;
    });
}

TEST_F(BasicLinkedListTest, PurgeEarlyExitsIfRangeIteratorExists) {
    const int numItems = 2;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    auto itr = basicLL->makeRangeIterator(true /*isBackfill*/);
    EXPECT_TRUE(itr);

    auto purgedCount = basicLL->purgeTombstones(numItems, {}, [&]() {
        // should not have tried to iterate the seqList,
        // so shouldPause (this lambda) should not be called
        ADD_FAILURE();
        return false;
    });

    EXPECT_EQ(0, purgedCount);
}

/* Run purge when the last item in the list does not yet have a seqno */
TEST_F(BasicLinkedListTest, PurgeWithItemWithoutSeqno) {
    const int numItems = 2;
    int expItems = numItems;
    const std::string keyPrefix("key");

    /* Add 2 new items */
    addNewItemsToList(1, keyPrefix, numItems);

    /* Add a stale item */
    addStaleItem("stale", numItems + 1);
    ++expItems;
    ASSERT_EQ(expItems, basicLL->getNumItems());
    ASSERT_EQ(1, basicLL->getNumStaleItems());

    /* Add an item which doesn't yet have a seqno. Such a scenario is possible
       when an item is added to the list, but seqno for it is yet to be
       generated */
    addItemWithoutSeqno("itemInMetaState");
    ++expItems;
    ASSERT_EQ(expItems, basicLL->getNumItems());

    /* Run purge */
    EXPECT_EQ(1, basicLL->purgeTombstones(numItems + 1));
    --expItems;
    EXPECT_EQ(expItems, basicLL->getNumItems());
    EXPECT_EQ(0, basicLL->getNumStaleItems());
}

TEST_F(BasicLinkedListTest, PurgePauseResume) {
    const int numItems = 4, numPurgeItems = 2;
    const std::string keyPrefix("key");

    /* Add some (numItems/2) new items */
    addNewItemsToList(1, keyPrefix, numItems / 2);

    /* Add a stale item */
    addStaleItem("stale", numItems / 2 + 1);

    /* Add some more (numItems/2) new items */
    addNewItemsToList(
            1 + 1 /* one stale item */ + numItems / 2, keyPrefix, numItems / 2);

    /* Add another stale item at the end */
    addStaleItem("stale", 1 + numItems + 1 /* one stale item */);

    ASSERT_EQ(numItems + numPurgeItems, basicLL->getNumItems());
    ASSERT_EQ(numPurgeItems, basicLL->getNumStaleItems());

    /* Purge the list. Set the max purge duration to 0 so that tombstone
     purging will pause */
    int purged = 0, numPaused = -1;

    /* Expect all items to be purged and atleast one pause-resume */
    while (purged != numPurgeItems) {
        purged += basicLL->purgeTombstones(
                numItems + numPurgeItems, {}, []() { return true; });
        ++numPaused;
    }
    EXPECT_EQ(0, basicLL->getNumStaleItems());
    EXPECT_GE(numPaused, 1);
    EXPECT_EQ(numItems, basicLL->getNumItems());

    /* Should be able to add elements to the list after the purger has run */
    addNewItemsToList(numItems + numPurgeItems + 1 /*startseqno*/,
                      keyPrefix,
                      1 /*add one element*/);
    std::vector<seqno_t> expectedSeqno = {1, 2, 4, 5, 7};
    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());
}

TEST_F(BasicLinkedListTest, PurgePauseResumeWithUpdate) {
    const int numItems = 2, numPurgeItems = 1;
    const std::string keyPrefix("key");

    /* Add a new item */
    addNewItemsToList(1 /*seqno*/, keyPrefix, 1);

    /* Add a stale item */
    addStaleItem("stale", 2 /*seqno*/);

    /* Add another item */
    addNewItemsToList(3 /*seqno*/, keyPrefix, 1);

    ASSERT_EQ(numItems + numPurgeItems, basicLL->getNumItems());
    ASSERT_EQ(numPurgeItems, basicLL->getNumStaleItems());

    /* Purge the list. Set the max purge duration to 0 so that tombstone
     purging will pause */
    int purged = 0, numPaused = -1;

    /* Expect all items to be purged and atleast one pause-resume */
    while (purged != numPurgeItems) {
        purged += basicLL->purgeTombstones(
                numItems + numPurgeItems, {}, []() { return true; });
        if (numPaused == -1) {
            /* During one pause, update some list element (last element here) */
            updateItem(numItems + numPurgeItems /*high seqno*/,
                       std::string("key3"));
        }
        ++numPaused;
    }
    EXPECT_EQ(0, basicLL->getNumStaleItems());
    EXPECT_GE(numPaused, 1);
    EXPECT_EQ(numItems, basicLL->getNumItems());
}

TEST_F(BasicLinkedListTest, PurgePauseResumeWithUpdateAtPausedPoint) {
    const int numItems = 4, numPurgeItems = 2;
    const std::string keyPrefix("key");

    /* Add some (numItems/2) new items */
    addNewItemsToList(1, keyPrefix, numItems / 2);

    /* Add a stale item */
    addStaleItem("stale", numItems / 2 + 1);

    /* Add some more (numItems/2) new items */
    addNewItemsToList(
            1 + 1 /* one stale item */ + numItems / 2, keyPrefix, numItems / 2);

    /* Add another stale item at the end */
    addStaleItem("stale", 1 + numItems + 1 /* one stale item */);

    ASSERT_EQ(numItems + numPurgeItems, basicLL->getNumItems());
    ASSERT_EQ(numPurgeItems, basicLL->getNumStaleItems());

    /* Purge the list. Set the max purge duration to 0 so that tombstone
     purging will pause */
    int purged = 0, numPaused = -1;

    /* Expect all items to be purged and atleast one pause-resume */
    while (purged != numPurgeItems) {
        purged += basicLL->purgeTombstones(
                numItems + numPurgeItems, {}, []() { return true; });
        if (numPaused == -1) {
            /* After first call to purgeTombstones() we know that the list is
             paused at seqno 2. Update that element */
            updateItem(numItems + numPurgeItems /*high seqno*/,
                       std::string("key2"));
        }
        ++numPaused;
    }
    EXPECT_EQ(0, basicLL->getNumStaleItems());
    EXPECT_GE(numPaused, 1);
    EXPECT_EQ(numItems, basicLL->getNumItems());
}
