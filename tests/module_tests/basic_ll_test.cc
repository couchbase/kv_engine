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
        basicLL = std::make_unique<MockBasicLinkedList>();
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
        std::vector<OrderedStoredValue*> sv(numItems);
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

            sv[i - 1] = ht.find(key, TrackReference::Yes, WantsDeleted::No)
                                ->toOrderedStoredValue();
            basicLL->appendToList(lg, *(sv[i - 1]));
            expectedSeqno.push_back(i);
        }
        return expectedSeqno;
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

    /* Update the high seqno */
    basicLL->updateHighSeqno(numItems);

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

    /* Update the high seqno */
    basicLL->updateHighSeqno(numItems);

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

    /* Update the high seqno */
    basicLL->updateHighSeqno(numItems);

    /* Now do a range read */
    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;
    std::tie(status, items) = basicLL->rangeRead(2, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, status);
    EXPECT_EQ(numItems - 1, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());
}

TEST_F(BasicLinkedListTest, TestRangeReadNegatives) {
    const int numItems = 3;

    /* Add 3 new items */
    addNewItemsToList(1, std::string("key"), numItems);

    /* Update the high seqno */
    basicLL->updateHighSeqno(numItems);

    ENGINE_ERROR_CODE status;
    std::vector<queued_item> items;

    /* Now do a range read with start > end */
    std::tie(status, items) = basicLL->rangeRead(2, 1);
    EXPECT_EQ(ENGINE_ERANGE, status);

    /* Now do a range read with start > highSeqno */
    std::tie(status, items) = basicLL->rangeRead(numItems + 1, numItems + 2);
    EXPECT_EQ(ENGINE_ERANGE, status);
}
