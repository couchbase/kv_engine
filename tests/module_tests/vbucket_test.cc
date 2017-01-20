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

#include "config.h"

#include <gtest/gtest.h>
#include <platform/cb_malloc.h>

#include "bgfetcher.h"
#include "item.h"
#include "makestoreddockey.h"
#include "vbucket.h"

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) { }
};

class MockVBucket : public VBucket {
public:
    MockVBucket(id_type i,
                vbucket_state_t newState,
                EPStats& st,
                CheckpointConfig& chkConfig,
                KVShard* kvshard,
                int64_t lastSeqno,
                uint64_t lastSnapStart,
                uint64_t lastSnapEnd,
                std::unique_ptr<FailoverTable> table,
                std::shared_ptr<Callback<id_type>> flusherCb,
                NewSeqnoCallback newSeqnoCb,
                Configuration& config,
                item_eviction_policy_t evictionPolicy,
                vbucket_state_t initState = vbucket_state_dead,
                uint64_t purgeSeqno = 0,
                uint64_t maxCas = 0)
        : VBucket(i,
                  vbucket_state_active,
                  st,
                  chkConfig,
                  kvshard,
                  lastSeqno,
                  lastSnapStart,
                  lastSnapEnd,
                  nullptr,
                  flusherCb,
                  nullptr,
                  config,
                  evictionPolicy,
                  initState,
                  purgeSeqno,
                  maxCas) {
    }

    MutationStatus public_processSet(Item& itm, const uint64_t cas) {
        int bucket_num(0);
        auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
        StoredValue* v =
                ht.unlocked_find(itm.getKey(), bucket_num, true, false);
        return processSet(lh, v, itm, cas, true, false);
    }
};

class VBucketTest
        : public ::testing::Test,
          public ::testing::WithParamInterface<item_eviction_policy_t> {
protected:
    void SetUp() {
        const auto eviction_policy = GetParam();
        vbucket.reset(new MockVBucket(0,
                                      vbucket_state_active,
                                      global_stats,
                                      checkpoint_config,
                                      /*kvshard*/ nullptr,
                                      /*lastSeqno*/ 1000,
                                      /*lastSnapStart*/ 0,
                                      /*lastSnapEnd*/ 0,
                                      /*table*/ nullptr,
                                      std::make_shared<DummyCB>(),
                                      /*newSeqnoCb*/ nullptr,
                                      config,
                                      eviction_policy));
    }

    void TearDown() {
        vbucket.reset();
    }

    std::unique_ptr<MockVBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
};

// Measure performance of VBucket::getBGFetchItems - queue and then get
// 10,000 items from the vbucket.
TEST_P(VBucketTest, GetBGFetchItemsPerformance) {
    BgFetcher fetcher(/*store*/nullptr, /*shard*/nullptr, global_stats);

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto* fetchItem = new VBucketBGFetchItem(/*cookie*/nullptr,
                                                 /*isMeta*/false);
        vbucket->queueBGFetchItem(makeStoredDocKey(std::to_string(ii)), fetchItem, &fetcher);
    }
    auto items = vbucket->getBGFetchItems();

    // Cleanup.
    for (auto& it : items) {
        for (auto* fetchItem : it.second.bgfetched_list) {
            delete fetchItem;
        }
    }
}

// Check the existence of bloom filter after performing a
// swap of existing filter with a temporary filter.
TEST_P(VBucketTest, SwapFilter) {
    vbucket->createFilter(1, 1.0);
    ASSERT_FALSE(vbucket->isTempFilterAvailable());
    ASSERT_NE("DOESN'T EXIST", vbucket->getFilterStatusString());
    vbucket->swapFilter();
    EXPECT_NE("DOESN'T EXIST", vbucket->getFilterStatusString());
}

class VBucketEvictionTest : public VBucketTest {};

// Check that counts of items and resident items are as expected when items are
// ejected from the HashTable.
TEST_P(VBucketEvictionTest, EjectionResidentCount) {
    const auto eviction_policy = GetParam();
    ASSERT_EQ(0, this->vbucket->getNumItems(eviction_policy));
    ASSERT_EQ(0, this->vbucket->getNumNonResidentItems(eviction_policy));

    Item item(makeStoredDocKey("key"), /*flags*/0, /*exp*/0,
              /*data*/nullptr, /*ndata*/0);

    EXPECT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(item, item.getCas()));

    EXPECT_EQ(1, this->vbucket->getNumItems(eviction_policy));
    EXPECT_EQ(0, this->vbucket->getNumNonResidentItems(eviction_policy));

    // TODO-MT: Should acquire lock really (ok given this is currently
    // single-threaded).
    auto* stored_item = this->vbucket->ht.find(makeStoredDocKey("key"));
    EXPECT_NE(nullptr, stored_item);
    // Need to clear the dirty flag to allow it to be ejected.
    stored_item->markClean();
    EXPECT_TRUE(this->vbucket->ht.unlocked_ejectItem(stored_item,
                                                     eviction_policy));

    // After ejection, should still have 1 item in VBucket, but also have
    // 1 non-resident item.
    EXPECT_EQ(1, this->vbucket->getNumItems(eviction_policy));
    EXPECT_EQ(1, this->vbucket->getNumNonResidentItems(eviction_policy));
}

// Regression test for MB-21448 - if an attempt is made to perform a CAS
// operation on a logically deleted item we should return NOT_FOUND
// (aka KEY_ENOENT) and *not* INVALID_CAS (aka KEY_EEXISTS).
TEST_P(VBucketEvictionTest, MB21448_UnlockedSetWithCASDeleted) {
    // Setup - create a key and then delete it.
    StoredDocKey key = makeStoredDocKey("key");
    Item item(key, 0, 0, "deleted", strlen("deleted"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(item, item.getCas()));
    ASSERT_EQ(MutationStatus::WasDirty, this->vbucket->ht.softDelete(key, 0));

    // Attempt to perform a set on a deleted key with a CAS.
    Item replacement(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(MutationStatus::NotFound,
              this->vbucket->public_processSet(replacement,
                                               /*cas*/ 10))
            << "When trying to replace-with-CAS a deleted item";
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        VBucketTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        VBucketEvictionTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });
