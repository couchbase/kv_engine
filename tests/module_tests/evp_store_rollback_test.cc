/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
 * Tests for Rollback functionality in EPStore.
 */

#include "evp_store_test.h"

class RollbackTest : public EventuallyPersistentStoreTest
{
    void SetUp() override {
        EventuallyPersistentStoreTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);

        // For any rollback tests which actually want to rollback, we need
        // to ensure that we don't rollback more than 50% of the seqno count
        // as then the VBucket is just cleared (it'll instead expect a resync
        // from zero.
        // Therefore create 10 dummy items which we don't otherwise care
        // about (most of the Rollback test only work with a couple of
        // "active" items.
        const auto dummy_elements = size_t{5};
        for (size_t ii = 1; ii <= dummy_elements; ii++) {
            auto res = store_item(vbid, "dummy" + std::to_string(ii),
                                  "dummy");
            ASSERT_EQ(ii, res.getBySeqno());
        }
        ASSERT_EQ(dummy_elements, store->flushVBucket(vbid));
        initial_seqno = dummy_elements;
    }

protected:
    int64_t initial_seqno;
};

// Test rollback after modifying an item - regression test for MB-21587.
TEST_F(RollbackTest, MB21587_RollbackAfterMutation) {

    // Setup: Store an item then flush the vBucket (creating a checkpoint);
    // then update the item with a new value and create a second checkpoint.
    auto item_v1 = store_item(vbid, "a", "old");
    ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
    ASSERT_EQ(1, store->flushVBucket(vbid));

    auto item2 = store_item(vbid, "a", "new");
    ASSERT_EQ(initial_seqno + 2, item2.getBySeqno());
    ASSERT_EQ(1, store->flushVBucket(vbid));

    // Test - rollback to seqno of item_v1 and verify that the previous value
    // of the item has been restored.
    ASSERT_EQ(ENGINE_SUCCESS, store->rollback(vbid, initial_seqno + 1));
    auto result = store->get("a", vbid, nullptr, {});
    EXPECT_EQ(ENGINE_SUCCESS, result.getStatus());
    EXPECT_EQ(item_v1, *result.getValue())
        << "Fetched item after rollback should match item_v1";
    delete result.getValue();
}
