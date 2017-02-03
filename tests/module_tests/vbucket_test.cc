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

#include "../mock/mock_vbucket.h"
#include "bgfetcher.h"
#include "item.h"
#include "makestoreddockey.h"
#include "programs/engine_testapp/mock_server.h"
#include "vbucket.h"

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) { }
};

static std::vector<StoredDocKey> generateKeys(int num, int start = 0) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

static void addOne(MockVBucket& vb,
                   const StoredDocKey& k,
                   AddStatus expect,
                   int expiry = 0) {
    Item i(k, 0, expiry, k.data(), k.size());
    EXPECT_EQ(expect, vb.public_processAdd(i)) << "Failed to add key "
                                               << k.c_str();
}

static void addMany(MockVBucket& vb,
                    std::vector<StoredDocKey>& keys,
                    AddStatus expect) {
    for (const auto& k : keys) {
        addOne(vb, k, expect);
    }
}

static void verifyValue(MockVBucket& vb,
                        StoredDocKey& key,
                        const char* value,
                        bool trackReference,
                        bool wantDeleted) {
    StoredValue* v = vb.ht.find(key, trackReference, wantDeleted);
    EXPECT_NE(nullptr, v);
    value_t val = v->getValue();
    if (!value) {
        EXPECT_EQ(nullptr, val.get());
    } else {
        EXPECT_STREQ(value, val->to_s().c_str());
    }
}

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
    BgFetcher fetcher(/*store*/ nullptr, /*shard*/ nullptr, this->global_stats);

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto fetchItem = std::make_unique<VBucketBGFetchItem>(
                /*cookie*/ nullptr,
                /*isMeta*/ false);
        this->vbucket->queueBGFetchItem(makeStoredDocKey(std::to_string(ii)),
                                        std::move(fetchItem),
                                        &fetcher);
    }
    auto items = this->vbucket->getBGFetchItems();
}

// Check the existence of bloom filter after performing a
// swap of existing filter with a temporary filter.
TEST_P(VBucketTest, SwapFilter) {
    this->vbucket->createFilter(1, 1.0);
    ASSERT_FALSE(this->vbucket->isTempFilterAvailable());
    ASSERT_NE("DOESN'T EXIST", this->vbucket->getFilterStatusString());
    this->vbucket->swapFilter();
    EXPECT_NE("DOESN'T EXIST", this->vbucket->getFilterStatusString());
}

TEST_P(VBucketTest, Add) {
    const auto eviction_policy = GetParam();
    if (eviction_policy != VALUE_ONLY) {
        return;
    }
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    addMany(*this->vbucket, keys, AddStatus::Success);

    StoredDocKey missingKey = makeStoredDocKey("aMissingKey");
    EXPECT_FALSE(this->vbucket->ht.find(missingKey));

    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.find(key));
    }

    addMany(*this->vbucket, keys, AddStatus::Exists);
    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.find(key));
    }

    // Verify we can read after a soft deletion.
    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(keys[0], nullptr, 0));
    EXPECT_EQ(MutationStatus::NotFound,
              this->vbucket->public_processSoftDelete(keys[0], nullptr, 0));
    EXPECT_FALSE(this->vbucket->ht.find(keys[0]));

    Item i(keys[0], 0, 0, "newtest", 7);
    EXPECT_EQ(AddStatus::UnDel, this->vbucket->public_processAdd(i));
    EXPECT_EQ(nkeys, this->vbucket->ht.getNumItems());
}

TEST_P(VBucketTest, AddExpiry) {
    const auto eviction_policy = GetParam();
    if (eviction_policy != VALUE_ONLY) {
        return;
    }
    StoredDocKey k = makeStoredDocKey("aKey");

    addOne(*this->vbucket, k, AddStatus::Success, ep_real_time() + 5);
    addOne(*this->vbucket, k, AddStatus::Exists, ep_real_time() + 5);

    StoredValue* v = this->vbucket->ht.find(k);
    EXPECT_TRUE(v);
    EXPECT_FALSE(v->isExpired(ep_real_time()));
    EXPECT_TRUE(v->isExpired(ep_real_time() + 6));

    mock_time_travel(6);
    EXPECT_TRUE(v->isExpired(ep_real_time()));

    addOne(*this->vbucket, k, AddStatus::UnDel, ep_real_time() + 5);
    EXPECT_TRUE(v);
    EXPECT_FALSE(v->isExpired(ep_real_time()));
    EXPECT_TRUE(v->isExpired(ep_real_time() + 6));
}

/**
 * Test to check if an unlocked_softDelete performed on an
 * existing item with a new value results in a success
 */
TEST_P(VBucketTest, unlockedSoftDeleteWithValue) {
    const auto eviction_policy = GetParam();
    if (eviction_policy != VALUE_ONLY) {
        return;
    }

    // Setup - create a key and then delete it with a value.
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0, 0, "value", strlen("value"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(stored_item, stored_item.getCas()));

    StoredValue* v(this->vbucket->ht.find(key, /*trackReference*/ false));
    EXPECT_NE(nullptr, v);

    // Create an item and set its state to deleted
    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));
    deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(deleted_item, this->vbucket->ht, PreserveRevSeqno::Yes);

    ItemMetaData itm_meta;
    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(v->getKey(), v, 0));
    verifyValue(*this->vbucket,
                key,
                "deletedvalue",
                /*trackReference*/ true,
                /*wantsDeleted*/ true);
}

/**
 * Test to check if an unlocked_softDelete performed on a
 * deleted item without a value and with a value
 */
TEST_P(VBucketTest, updateDeletedItem) {
    const auto eviction_policy = GetParam();
    if (eviction_policy != VALUE_ONLY) {
        return;
    }

    // Setup - create a key and then delete it.
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0, 0, "value", strlen("value"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(stored_item, stored_item.getCas()));

    StoredValue* v(this->vbucket->ht.find(key, /*trackReference*/ false));
    EXPECT_NE(nullptr, v);

    ItemMetaData itm_meta;
    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(v->getKey(), v, 0));
    verifyValue(*this->vbucket,
                key,
                nullptr,
                /*trackReference*/ true,
                /*wantsDeleted*/ true);

    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));
    deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(deleted_item, this->vbucket->ht, PreserveRevSeqno::Yes);

    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(v->getKey(), v, 0));
    verifyValue(*this->vbucket,
                key,
                "deletedvalue",
                /*trackReference*/ true,
                /*wantDeleted*/ true);

    Item update_deleted_item(
            key, 0, 0, "updatedeletedvalue", strlen("updatedeletedvalue"));
    update_deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(update_deleted_item, this->vbucket->ht, PreserveRevSeqno::Yes);

    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(v->getKey(), v, 0));
    verifyValue(*this->vbucket,
                key,
                "updatedeletedvalue",
                /*trackReference*/ true,
                /*wantsDeleted*/ true);
}

TEST_P(VBucketTest, SizeStatsSoftDel) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.memSize.load());
    ASSERT_EQ(0, this->vbucket->ht.cacheSize.load());
    size_t initialSize = this->global_stats.currentSize.load();

    const StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(k, nullptr, 0));
    this->vbucket->public_deleteStoredValue(k);

    EXPECT_EQ(0, this->vbucket->ht.memSize.load());
    EXPECT_EQ(0, this->vbucket->ht.cacheSize.load());
    EXPECT_EQ(initialSize, this->global_stats.currentSize.load());

    cb_free(someval);
}

TEST_P(VBucketTest, SizeStatsSoftDelFlush) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.memSize.load());
    ASSERT_EQ(0, this->vbucket->ht.cacheSize.load());
    size_t initialSize = this->global_stats.currentSize.load();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean,
              this->vbucket->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(k, nullptr, 0));
    this->vbucket->ht.clear();

    EXPECT_EQ(0, this->vbucket->ht.memSize.load());
    EXPECT_EQ(0, this->vbucket->ht.cacheSize.load());
    EXPECT_EQ(initialSize, this->global_stats.currentSize.load());

    cb_free(someval);
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
    ASSERT_EQ(MutationStatus::WasDirty,
              this->vbucket->public_processSoftDelete(key, nullptr, 0));

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
