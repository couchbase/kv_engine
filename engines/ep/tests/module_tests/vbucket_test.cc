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

#include "vbucket_test.h"

#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "persistence_callback.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket_bgfetch_item.h"

#include <platform/cb_malloc.h>

void VBucketTest::SetUp() {
    const auto eviction_policy = GetParam();
    vbucket.reset(new EPVBucket(0,
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
    cookie = create_mock_cookie();
}

void VBucketTest::TearDown() {
    vbucket.reset();
    destroy_mock_cookie(cookie);
}

std::vector<StoredDocKey> VBucketTest::generateKeys(int num, int start) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num + start; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

queued_item VBucketTest::makeQueuedItem(const char *key) {
    std::string val("x");
    uint32_t flags = 0;
    time_t expiry = 0;
    return queued_item(new Item(makeStoredDocKey(key),
                                flags,
                                expiry,
                                val.c_str(),
                                val.size()));
}

AddStatus VBucketTest::addOne(const StoredDocKey& k, int expiry) {
    Item i(k, 0, expiry, k.data(), k.size());
    return public_processAdd(i);
}

TempAddStatus VBucketTest::addOneTemp(const StoredDocKey& k) {
    auto hbl_sv = lockAndFind(k);
    return vbucket->addTempStoredValue(hbl_sv.first, k);
}

void VBucketTest::addMany(std::vector<StoredDocKey>& keys, AddStatus expect) {
    for (const auto& k : keys) {
        EXPECT_EQ(expect, addOne(k));
    }
}

MutationStatus VBucketTest::setOne(const StoredDocKey& k, int expiry) {
    Item i(k, 0, expiry, k.data(), k.size());
    return public_processSet(i, i.getCas());
}

void VBucketTest::setMany(std::vector<StoredDocKey>& keys,
                          MutationStatus expect) {
    for (const auto& k : keys) {
        EXPECT_EQ(expect, setOne(k));
    }
}

void VBucketTest::softDeleteOne(const StoredDocKey& k, MutationStatus expect) {
    StoredValue* v(vbucket->ht.find(k, TrackReference::No, WantsDeleted::No));
    EXPECT_NE(nullptr, v);

    EXPECT_EQ(expect, public_processSoftDelete(v->getKey(), v, 0))
            << "Failed to soft delete key " << k.c_str();
}

void VBucketTest::softDeleteMany(std::vector<StoredDocKey>& keys,
                                 MutationStatus expect) {
    for (const auto& k : keys) {
        softDeleteOne(k, expect);
    }
}

StoredValue* VBucketTest::findValue(StoredDocKey& key) {
    return vbucket->ht.find(key, TrackReference::Yes, WantsDeleted::Yes);
}

void VBucketTest::verifyValue(StoredDocKey& key,
                              const char* value,
                              TrackReference trackReference,
                              WantsDeleted wantDeleted) {
    StoredValue* v = vbucket->ht.find(key, trackReference, wantDeleted);
    EXPECT_NE(nullptr, v);
    value_t val = v->getValue();
    if (!value) {
        EXPECT_EQ(nullptr, val.get().get());
    } else {
        EXPECT_STREQ(value, val->to_s().c_str());
    }
}

std::pair<HashTable::HashBucketLock, StoredValue*> VBucketTest::lockAndFind(
        const StoredDocKey& key) {
    auto hbl = vbucket->ht.getLockedBucket(key);
    auto* storedVal = vbucket->ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);
    return std::make_pair(std::move(hbl), storedVal);
}

MutationStatus VBucketTest::public_processSet(Item& itm, const uint64_t cas) {
    auto hbl_sv = lockAndFind(itm.getKey());
    VBQueueItemCtx queueItmCtx(GenerateBySeqno::Yes,
                               GenerateCas::No,
                               TrackCasDrift::No,
                               /*isBackfillItem*/ false,
                               /*preLinkDocumentContext_*/ nullptr);
    return vbucket
            ->processSet(hbl_sv.first,
                         hbl_sv.second,
                         itm,
                         cas,
                         true,
                         false,
                         queueItmCtx,
                         {/*no predicate*/})
            .first;
}

AddStatus VBucketTest::public_processAdd(Item& itm) {
    auto hbl_sv = lockAndFind(itm.getKey());
    VBQueueItemCtx queueItmCtx(GenerateBySeqno::Yes,
                               GenerateCas::No,
                               TrackCasDrift::No,
                               /*isBackfillItem*/ false,
                               /*preLinkDocumentContext_*/ nullptr);
    return vbucket
            ->processAdd(hbl_sv.first,
                         hbl_sv.second,
                         itm,
                         /*maybeKeyExists*/ true,
                         /*isReplication*/ false,
                         queueItmCtx,
                         vbucket->lockCollections(itm.getKey()))
            .first;
}

MutationStatus VBucketTest::public_processSoftDelete(const DocKey& key,
                                                     StoredValue* v,
                                                     uint64_t cas) {
    auto hbl = vbucket->ht.getLockedBucket(key);
    if (!v) {
        v = vbucket->ht.unlocked_find(
                key, hbl.getBucketNum(), WantsDeleted::No, TrackReference::No);
        if (!v) {
            return MutationStatus::NotFound;
        }
    }
    ItemMetaData metadata;
    metadata.revSeqno = v->getRevSeqno() + 1;
    MutationStatus status;
    std::tie(status, std::ignore, std::ignore) = vbucket->processSoftDelete(
            hbl,
            *v,
            cas,
            metadata,
            VBQueueItemCtx(GenerateBySeqno::Yes,
                           GenerateCas::Yes,
                           TrackCasDrift::No,
                           /*isBackfillItem*/ false,
                           /*preLinkDocCtx*/ nullptr),
            /*use_meta*/ false,
            /*bySeqno*/ v->getBySeqno());
    return status;
}

bool VBucketTest::public_deleteStoredValue(const DocKey& key) {
    auto hbl_sv = lockAndFind(key);
    if (!hbl_sv.second) {
        return false;
    }
    return vbucket->deleteStoredValue(hbl_sv.first, *hbl_sv.second);
}

GetValue VBucketTest::public_getAndUpdateTtl(const DocKey& key,
                                             time_t exptime) {
    auto hbl = lockAndFind(key);
    GetValue gv;
    MutationStatus status;
    std::tie(status, gv) = vbucket->processGetAndUpdateTtl(
            key, hbl.first, hbl.second, exptime);
    return gv;
}

size_t EPVBucketTest::public_queueBGFetchItem(
        const DocKey& key,
        std::unique_ptr<VBucketBGFetchItem> fetchItem,
        BgFetcher* bgFetcher) {
    return dynamic_cast<EPVBucket&>(*vbucket).queueBGFetchItem(
            key, std::move(fetchItem), bgFetcher);
}

class BlobTest : public Blob {
public:
    BlobTest() : Blob(0,0) {}
    static size_t getAllocationSize(size_t len){
        return Blob::getAllocationSize(len);
    }
};

TEST(BlobTest, basicAllocationSize){
    EXPECT_EQ(BlobTest::getAllocationSize(10), 19);

    // Expected to be 9 because 3 bytes of the data member array will not
    // be allocated because they will not be used.
    EXPECT_EQ(BlobTest::getAllocationSize(0), 9);
}

// Measure performance of VBucket::getBGFetchItems - queue and then get
// 10,000 items from the vbucket.
TEST_P(EPVBucketTest, GetBGFetchItemsPerformance) {
    BgFetcher fetcher(/*store*/ nullptr, /*shard*/ nullptr, this->global_stats);

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto fetchItem = std::make_unique<VBucketBGFetchItem>(cookie,
                                                              /*isMeta*/ false);
        this->public_queueBGFetchItem(
                makeStoredDocKey(std::to_string(ii)),
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
    addMany(keys, AddStatus::Success);

    StoredDocKey missingKey = makeStoredDocKey("aMissingKey");
    EXPECT_FALSE(this->vbucket->ht.find(
            missingKey, TrackReference::Yes, WantsDeleted::No));

    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.find(
                key, TrackReference::Yes, WantsDeleted::No));
    }

    addMany(keys, AddStatus::Exists);
    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.find(
                key, TrackReference::Yes, WantsDeleted::No));
    }

    // Verify we can read after a soft deletion.
    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(keys[0], nullptr, 0));
    EXPECT_EQ(MutationStatus::NotFound,
              this->public_processSoftDelete(keys[0], nullptr, 0));
    EXPECT_FALSE(this->vbucket->ht.find(
            keys[0], TrackReference::Yes, WantsDeleted::No));

    Item i(keys[0], 0, 0, "newtest", 7);
    EXPECT_EQ(AddStatus::UnDel, this->public_processAdd(i));
    EXPECT_EQ(nkeys, this->vbucket->ht.getNumItems());
}

TEST_P(VBucketTest, AddExpiry) {
    const auto eviction_policy = GetParam();
    if (eviction_policy != VALUE_ONLY) {
        return;
    }
    StoredDocKey k = makeStoredDocKey("aKey");

    ASSERT_EQ(AddStatus::Success, addOne(k, ep_real_time() + 5));
    EXPECT_EQ(AddStatus::Exists, addOne(k, ep_real_time() + 5));

    StoredValue* v =
            this->vbucket->ht.find(k, TrackReference::Yes, WantsDeleted::No);
    EXPECT_TRUE(v);
    EXPECT_FALSE(v->isExpired(ep_real_time()));
    EXPECT_TRUE(v->isExpired(ep_real_time() + 6));

    TimeTraveller biffTannen(6);
    EXPECT_TRUE(v->isExpired(ep_real_time()));

    EXPECT_EQ(AddStatus::UnDel, addOne(k, ep_real_time() + 5));
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
              this->public_processSet(stored_item, stored_item.getCas()));

    StoredValue* v(
            this->vbucket->ht.find(key, TrackReference::No, WantsDeleted::No));
    EXPECT_NE(nullptr, v);

    // Create an item and set its state to deleted
    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));
    deleted_item.setDeleted();

    auto prev_revseqno = v->getRevSeqno();
    deleted_item.setRevSeqno(prev_revseqno);

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSet(deleted_item, 0));
    verifyValue(key, "deletedvalue", TrackReference::Yes, WantsDeleted::Yes);
    EXPECT_EQ(prev_revseqno + 1, v->getRevSeqno());
}

/**
 * Test to check that if an item has expired, an incoming mutation
 * on that item, if in deleted state results in an invalid cas and
 * if not in deleted state, results in not found
 */
TEST_P(VBucketTest, updateExpiredItem) {
    // Setup - create a key
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0, ep_real_time() - 1, "value", strlen("value"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->public_processSet(stored_item, stored_item.getCas()));

    StoredValue* v = this->vbucket->ht.find(key, TrackReference::No, WantsDeleted::No);
    EXPECT_TRUE(v);
    EXPECT_TRUE(v->isExpired(ep_real_time()));

    auto cas = v->getCas();
    // Create an item and set its state to deleted.
    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));

    EXPECT_EQ(MutationStatus::NotFound,
              this->public_processSet(deleted_item, cas + 1));

    deleted_item.setDeleted();
    EXPECT_EQ(MutationStatus::InvalidCas,
              this->public_processSet(deleted_item, cas + 1));
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
              this->public_processSet(stored_item, stored_item.getCas()));

    StoredValue* v(
            this->vbucket->ht.find(key, TrackReference::No, WantsDeleted::No));
    EXPECT_NE(nullptr, v);

    ItemMetaData itm_meta;
    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(v->getKey(), v, 0));
    verifyValue(key, nullptr, TrackReference::Yes, WantsDeleted::Yes);
    EXPECT_EQ(0, this->vbucket->getNumItems());
    EXPECT_EQ(1, this->vbucket->getNumInMemoryDeletes());

    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));
    deleted_item.setDeleted();

    auto prev_revseqno = v->getRevSeqno();
    deleted_item.setRevSeqno(prev_revseqno);

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSet(deleted_item, 0));
    verifyValue(
                key,
                "deletedvalue",
                TrackReference::Yes,
                WantsDeleted::Yes);
    EXPECT_EQ(0, this->vbucket->getNumItems());
    EXPECT_EQ(1, this->vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(prev_revseqno + 1, v->getRevSeqno());

    Item update_deleted_item(
            key, 0, 0, "updatedeletedvalue", strlen("updatedeletedvalue"));
    update_deleted_item.setDeleted();

    prev_revseqno = v->getRevSeqno();
    update_deleted_item.setRevSeqno(prev_revseqno);

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSet(update_deleted_item, 0));
    verifyValue(
            key, "updatedeletedvalue", TrackReference::Yes, WantsDeleted::Yes);
    EXPECT_EQ(0, this->vbucket->getNumItems());
    EXPECT_EQ(1, this->vbucket->getNumInMemoryDeletes());
    EXPECT_EQ(prev_revseqno + 1, v->getRevSeqno());
}

TEST_P(VBucketTest, SizeStatsSoftDel) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.getItemMemory());
    ASSERT_EQ(0, this->vbucket->ht.getCacheSize());
    size_t initialSize = this->global_stats.getCurrentSize();

    const StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean,
              this->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(k, nullptr, 0));
    this->public_deleteStoredValue(k);

    EXPECT_EQ(0, this->vbucket->ht.getItemMemory());
    EXPECT_EQ(0, this->vbucket->ht.getCacheSize());
    EXPECT_EQ(initialSize, this->global_stats.getCurrentSize());

    cb_free(someval);
}

TEST_P(VBucketTest, SizeStatsSoftDelFlush) {
    this->global_stats.reset();
    ASSERT_EQ(0, this->vbucket->ht.getItemMemory());
    ASSERT_EQ(0, this->vbucket->ht.getCacheSize());
    size_t initialSize = this->global_stats.getCurrentSize();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char* someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(MutationStatus::WasClean,
              this->public_processSet(i, i.getCas()));

    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(k, nullptr, 0));
    this->vbucket->ht.clear();

    EXPECT_EQ(0, this->vbucket->ht.getItemMemory());
    EXPECT_EQ(0, this->vbucket->ht.getCacheSize());
    EXPECT_EQ(initialSize, this->global_stats.getCurrentSize());

    cb_free(someval);
}

// Check that getItemsForCursor() can impose a limit on items fetched, but
// that it always fetches complete checkpoints.
TEST_P(VBucketTest, GetItemsForCursor_Limit) {
    // Setup - Add two items each to three separate checkpoints.
    auto keys = generateKeys(2, 1);
    setMany(keys, MutationStatus::WasClean);
    this->vbucket->checkpointManager->createNewCheckpoint();
    keys = generateKeys(2, 3);
    setMany(keys, MutationStatus::WasClean);
    this->vbucket->checkpointManager->createNewCheckpoint();
    keys = generateKeys(2, 5);
    setMany(keys, MutationStatus::WasClean);

    // Check - Asking for 1 item should give us all items in first checkpoint
    // - 4 total (1x ckpt start, 2x mutation, 1x ckpt end).
    auto result = this->vbucket->getItemsToPersist(1);
    EXPECT_TRUE(result.moreAvailable);
    EXPECT_EQ(4, result.items.size());
    EXPECT_TRUE(result.items[0]->isCheckPointMetaItem());
    EXPECT_STREQ("1", result.items[1]->getKey().c_str());
    EXPECT_STREQ("2", result.items[2]->getKey().c_str());
    EXPECT_TRUE(result.items[3]->isCheckPointMetaItem());

    // Asking for 5 items should give us all items in second checkpoint and
    // third checkpoint - 7 total
    // (ckpt start, 2x mutation, ckpt_end, ckpt_start, 2x mutation)
    result = this->vbucket->getItemsToPersist(5);
    EXPECT_FALSE(result.moreAvailable);
    EXPECT_EQ(7, result.items.size());
    EXPECT_TRUE(result.items[0]->isCheckPointMetaItem());
    EXPECT_STREQ("3", result.items[1]->getKey().c_str());
    EXPECT_STREQ("4", result.items[2]->getKey().c_str());
    EXPECT_TRUE(result.items[3]->isCheckPointMetaItem());
    EXPECT_TRUE(result.items[4]->isCheckPointMetaItem());
    EXPECT_STREQ("5", result.items[5]->getKey().c_str());
    EXPECT_STREQ("6", result.items[6]->getKey().c_str());
}

// Check that getItemsToPersist() can correctly impose a limit on items fetched.
TEST_P(VBucketTest, GetItemsToPersist_Limit) {
    // Setup - Add items to reject, backfill and checkpoint manager.

    this->vbucket->rejectQueue.push(makeQueuedItem("1"));
    this->vbucket->rejectQueue.push(makeQueuedItem("2"));

    // Add 2 items to backfill queue
    this->vbucket->backfill.items.push(makeQueuedItem("3"));
    this->vbucket->backfill.items.push(makeQueuedItem("4"));

    // Add 2 items to checkpoint manager (in addition to initial
    // checkpoint_start).
    auto keys = generateKeys(2, 5);
    setMany(keys, MutationStatus::WasClean);

    // Test - fetch items in chunks spanning the different item sources.
    // Should get specified number (in correct order), and should always have
    // items available until the end.
    auto result = this->vbucket->getItemsToPersist(1);
    EXPECT_TRUE(result.moreAvailable);
    EXPECT_EQ(1, result.items.size());
    EXPECT_STREQ("1", result.items[0]->getKey().c_str());

    result = this->vbucket->getItemsToPersist(2);
    EXPECT_TRUE(result.moreAvailable);
    EXPECT_EQ(2, result.items.size());
    EXPECT_STREQ("2", result.items[0]->getKey().c_str());
    EXPECT_STREQ("3", result.items[1]->getKey().c_str());

    // Next call should read 1 item from backfill; and *all* items from
    // checkpoint (even through we only asked for 2 total), as it is not valid
    // to read partial checkpoint contents.
    result = this->vbucket->getItemsToPersist(2);
    EXPECT_FALSE(result.moreAvailable);
    EXPECT_EQ(4, result.items.size());
    EXPECT_STREQ("4", result.items[0]->getKey().c_str());
    EXPECT_TRUE(result.items[1]->isCheckPointMetaItem());
    EXPECT_STREQ("5", result.items[2]->getKey().c_str());
    EXPECT_STREQ("6", result.items[3]->getKey().c_str());
}

// Check that getItemsToPersist() correctly returns `moreAvailable` if we
// hit the CheckpointManager limit early.
TEST_P(VBucketTest, GetItemsToPersist_LimitCkptMoreAvailable) {
    // Setup - Add an item to checkpoint manager (in addition to initial
    // checkpoint_start).
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("1")));

    // Test - fetch items such that we have a limit for CheckpointManager of
    // zero. This should return moreAvailable=true.
    auto result = this->vbucket->getItemsToPersist(0);
    EXPECT_TRUE(result.moreAvailable);
    EXPECT_EQ(0, result.items.size());
}

class VBucketEvictionTest : public VBucketTest {};

// Check that counts of items and resident items are as expected when items are
// ejected from the HashTable.
TEST_P(VBucketEvictionTest, EjectionResidentCount) {
    const auto eviction_policy = GetParam();
    ASSERT_EQ(0, this->vbucket->getNumItems());
    ASSERT_EQ(0, this->vbucket->getNumNonResidentItems());

    Item item(makeStoredDocKey("key"), /*flags*/0, /*exp*/0,
              /*data*/nullptr, /*ndata*/0);

    EXPECT_EQ(MutationStatus::WasClean,
              this->public_processSet(item, item.getCas()));

    switch (eviction_policy) {
    case VALUE_ONLY:
        // We have accurate VBucket counts in value eviction:
        EXPECT_EQ(1, this->vbucket->getNumItems());
        break;
    case FULL_EVICTION:
        // In Full Eviction the vBucket count isn't accurate until the
        // flusher completes - hence just check count in HashTable.
        EXPECT_EQ(1, this->vbucket->ht.getNumItems());
        break;
    }
    EXPECT_EQ(0, this->vbucket->getNumNonResidentItems());

    // TODO-MT: Should acquire lock really (ok given this is currently
    // single-threaded).
    auto* stored_item = this->vbucket->ht.find(
            makeStoredDocKey("key"), TrackReference::Yes, WantsDeleted::No);
    EXPECT_NE(nullptr, stored_item);
    // Need to clear the dirty flag to allow it to be ejected.
    stored_item->markClean();
    EXPECT_TRUE(this->vbucket->ht.unlocked_ejectItem(stored_item,
                                                     eviction_policy));

    switch (eviction_policy) {
    case VALUE_ONLY:
        // After ejection, should still have 1 item in HashTable (meta-only),
        // and VBucket count should also be 1.
        EXPECT_EQ(1, this->vbucket->getNumItems());
        EXPECT_EQ(1, this->vbucket->getNumNonResidentItems());
        break;
    case FULL_EVICTION:
        // In Full eviction should be no items in HashTable (we fully
        // evicted it).
        EXPECT_EQ(0, this->vbucket->ht.getNumItems());
        break;
    }
}

// Regression test for MB-21448 - if an attempt is made to perform a CAS
// operation on a logically deleted item we should return NOT_FOUND
// (aka KEY_ENOENT) and *not* INVALID_CAS (aka KEY_EEXISTS).
TEST_P(VBucketEvictionTest, MB21448_UnlockedSetWithCASDeleted) {
    // Setup - create a key and then delete it.
    StoredDocKey key = makeStoredDocKey("key");
    Item item(key, 0, 0, "deleted", strlen("deleted"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->public_processSet(item, item.getCas()));
    ASSERT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(key, nullptr, 0));

    // Attempt to perform a set on a deleted key with a CAS.
    Item replacement(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(MutationStatus::NotFound,
              this->public_processSet(replacement,
                                               /*cas*/ 10))
            << "When trying to replace-with-CAS a deleted item";
}

class VBucketFullEvictionTest : public VBucketTest {};

TEST_P(VBucketFullEvictionTest, MB_30137) {
    auto k = makeStoredDocKey("key");
    queued_item qi(new Item(k, 0, 0, k.data(), k.size()));
    PersistenceCallback cb1(qi, qi->getCas());

    // (1) Store k
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*qi, qi->getCas()));

    // (1.1) Mimic flusher by running the PCB for the store at (1)
    EPTransactionContext tc1(global_stats, *vbucket);
    auto mr1 = std::make_pair(1, true);
    cb1.callback(tc1, mr1); // Using the create/update callback

    EXPECT_EQ(1, vbucket->getNumItems());

    // (2) Delete k
    softDeleteOne(k, MutationStatus::WasClean);

    // (3) Now set k again
    EXPECT_EQ(MutationStatus::WasDirty, public_processSet(*qi, qi->getCas()));

    // (3.1) Run the PCB for the delete/expiry (2)
    int value = 1;
    cb1.callback(tc1, value); // Using the delete callback

    // In FE mode, getNumItems is tracking disk items, so we should have 0 disk
    // items until the 'flush' of the second store (3)
    EXPECT_EQ(0, vbucket->getNumItems());

    // (4) run the create/update PCB again, the store (3) should look like a
    // create because of the delete at (2)
    cb1.callback(tc1, mr1); // Using the create/update callback

    EXPECT_EQ(1, vbucket->getNumItems());
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

// Test cases which run in Full Eviction only
INSTANTIATE_TEST_CASE_P(
        FullEviction,
        VBucketFullEvictionTest,
        ::testing::Values(FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            return "FULL_EVICTION";
        });

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        EPVBucketTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });
