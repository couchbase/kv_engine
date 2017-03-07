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

static EPStats global_stats;

class BasicLinkedListTest : public ::testing::Test {
public:
    static std::unique_ptr<AbstractStoredValueFactory> makeFactory() {
        return std::make_unique<OrderedStoredValueFactory>(global_stats);
    }

protected:
    void SetUp() {
        basicLL = std::make_unique<MockBasicLinkedList>();
    }

    void TearDown() {
    }

    std::unique_ptr<MockBasicLinkedList> basicLL;
};

TEST_F(BasicLinkedListTest, SetItems) {
    /* Setup with 2 hash buckets and 1 lock. We need a HashTable because
       StoredValue are created only in the HashTable and then put
       onto the sequence list */
    HashTable ht(global_stats, makeFactory(), 2, 1);

    const int numItems = 3;
    OrderedStoredValue* sv[numItems];
    const std::string val("data");
    std::vector<seqno_t> expectedSeqno;

    /* Get a fake sequence lock */
    std::mutex fakeSeqLock;
    std::lock_guard<std::mutex> lg(fakeSeqLock);

    /* Add 3 new items */
    for (int i = 1; i <= numItems; ++i) {
        StoredDocKey key =
                makeStoredDocKey(std::string("key" + std::to_string(i)));
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

    EXPECT_EQ(expectedSeqno, basicLL->getAllSeqnoForVerification());

    /* Like in a vbucket we want the link list to be erased before HashTable is
       is destroyed. */
    basicLL.reset();
}
