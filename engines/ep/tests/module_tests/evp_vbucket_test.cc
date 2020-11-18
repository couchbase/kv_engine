/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "evp_vbucket_test.h"
#include "bgfetcher.h"
#include "ep_vb.h"
#include "item.h"
#include "kv_bucket.h"
#include "kvshard.h"
#include "kvstore.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"

#include <platform/cb_malloc.h>

void EPVBucketTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
}

void EPVBucketTest::TearDown() {
    SingleThreadedKVBucketTest::TearDown();
}

size_t EPVBucketTest::public_queueBGFetchItem(
        const DocKey& key,
        std::unique_ptr<BGFetchItem> fetchItem,
        BgFetcher& bgFetcher) {
    return dynamic_cast<EPVBucket&>(*vbucket).queueBGFetchItem(
            key, std::move(fetchItem), bgFetcher);
}

/**
 * To test the BgFetcher with a VBucket we need references to an EPBucket and
 * a KVShard. The easiest way to create these is to create an entire engine...
 *
 * Measure performance of VBucket::getBGFetchItems - queue and then get
 * 100,000 items from the vbucket.
 */
TEST_P(EPVBucketTest, GetBGFetchItemsPerformance) {
    // Use the SynchronousEPEngine to create the necessary objects for our
    // bgFetcher
    auto mockEPBucket =
            engine->public_makeMockBucket(engine->getConfiguration());
    KVShard kvShard(*engine.get(), 0);
    BgFetcher bgFetcher(*mockEPBucket.get());

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto fetchItem = std::make_unique<FrontEndBGFetchItem>(
                nullptr, ValueFilter::VALUES_DECOMPRESSED);
        this->public_queueBGFetchItem(makeStoredDocKey(std::to_string(ii)),
                                      std::move(fetchItem),
                                      bgFetcher);
    }
    auto items = this->vbucket->getBGFetchItems();
}

// Test statistics after softDelete.
// Persistent VBucket only as Ephemeral cannot totally clear the VBucket; it
// must keep at least the last deleted seqno for correct tombstone handling
TEST_P(EPVBucketTest, SizeStatsSoftDel) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.getItemMemory());
    ASSERT_EQ(0, this->vbucket->ht.getCacheSize());
    size_t initialSize = this->global_stats.getCurrentSize();

    const StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean, this->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(k).first);
    this->public_deleteStoredValue(k);

    EXPECT_EQ(0, this->vbucket->ht.getItemMemory());
    EXPECT_EQ(0, this->vbucket->ht.getCacheSize());
    EXPECT_EQ(initialSize, this->global_stats.getCurrentSize());

    cb_free(someval);
}

// Test statistics after softDelete and flush.
// Persistent VBucket only as Ephemeral cannot totally clear the VBucket; it
// must keep at least the last deleted seqno for correct tombstone handling
TEST_P(EPVBucketTest, SizeStatsSoftDelFlush) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.getItemMemory());
    ASSERT_EQ(0, this->vbucket->ht.getCacheSize());
    size_t initialSize = this->global_stats.getCurrentSize();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean, this->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(k).first);
    this->vbucket->ht.clear();

    EXPECT_EQ(0, this->vbucket->ht.getItemMemory());
    EXPECT_EQ(0, this->vbucket->ht.getCacheSize());
    EXPECT_EQ(initialSize, this->global_stats.getCurrentSize());

    cb_free(someval);
}

// Check that counts of items and resident items are as expected when items are
// ejected from the HashTable.
// Persisent VBucket only as Ephemeral doesn't support explicit ejection (and
// even when items are removed they still exist as stale / deleted).
TEST_P(EPVBucketTest, EjectionResidentCount) {
    const auto eviction_policy = GetParam();
    ASSERT_EQ(0, this->vbucket->getNumItems());
    ASSERT_EQ(0, this->vbucket->getNumNonResidentItems());

    Item item(makeStoredDocKey("key"),
              /*flags*/ 0,
              /*exp*/ 0,
              /*data*/ nullptr,
              /*ndata*/ 0);

    EXPECT_EQ(MutationStatus::WasClean,
              this->public_processSet(item, item.getCas()));

    switch (eviction_policy) {
    case EvictionPolicy::Value:
        // We have accurate VBucket counts in value eviction:
        EXPECT_EQ(1, this->vbucket->getNumItems());
        break;
    case EvictionPolicy::Full:
        // In Full Eviction the vBucket count isn't accurate until the
        // flusher completes - hence just check count in HashTable.
        EXPECT_EQ(1, this->vbucket->ht.getNumItems());
        break;
    }
    EXPECT_EQ(0, this->vbucket->getNumNonResidentItems());

    auto stored_item = this->vbucket->ht.findForWrite(makeStoredDocKey("key"));
    EXPECT_NE(nullptr, stored_item.storedValue);
    // Need to clear the dirty flag to allow it to be ejected.
    stored_item.storedValue->markClean();
    EXPECT_TRUE(this->vbucket->ht.unlocked_ejectItem(
            stored_item.lock, stored_item.storedValue, eviction_policy));

    switch (eviction_policy) {
    case EvictionPolicy::Value:
        // After ejection, should still have 1 item in HashTable (meta-only),
        // and VBucket count should also be 1.
        EXPECT_EQ(1, this->vbucket->getNumItems());
        EXPECT_EQ(1, this->vbucket->getNumNonResidentItems());
        break;
    case EvictionPolicy::Full:
        // In Full eviction should be no items in HashTable (we fully
        // evicted it).
        EXPECT_EQ(0, this->vbucket->ht.getNumItems());
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(
        FullAndValueEviction,
        EPVBucketTest,
        ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full),
        [](const ::testing::TestParamInfo<EvictionPolicy>& info) {
            return to_string(info.param);
        });
