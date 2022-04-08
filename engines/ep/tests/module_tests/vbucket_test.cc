/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "vbucket_test.h"

#include "../mock/mock_checkpoint_manager.h"
#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "ephemeral_vb.h"
#include "failover-table.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "kvstore/persistence_callback.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket_bgfetch_item.h"

#include "../mock/mock_ephemeral_vb.h"

#include <folly/portability/GMock.h>
#include <nlohmann/json.hpp>
#include <platform/cb_malloc.h>
#include <memory>

using namespace std::string_literals;

std::string VBucketTestBase::to_string(VBType vbType) {
    switch (vbType) {
    case VBType::Persistent:
        return "Persistent";
    case VBType::Ephemeral:
        return "Ephemeral";
    }
    folly::assume_unreachable();
}

VBucketTestBase::VBucketTestBase(VBType vbType,
                                 EvictionPolicy eviction_policy) {
    // Used for mem-checks at Replica VBuckets. Default=0 prevents any
    // processSet, returns NoMem. I set a production-like value.
    global_stats.replicationThrottleThreshold = 0.9;

    // MB-34453: Change sync_writes_max_allowed_replicas back to total
    // possible replicas given we want to still test against all replicas.
    std::string confString = "sync_writes_max_allowed_replicas=3;bucket_type=";
    confString += (vbType == VBType::Persistent ? "persistent" : "ephemeral");
    // Also, a bunch of tests rely on max_checkpoints=2.
    confString += ";max_checkpoints=2";
    config.parseConfiguration(confString.c_str(), get_mock_server_api());

    auto manifest = std::make_unique<Collections::VB::Manifest>(
            std::make_shared<Collections::Manager>());

    checkpoint_config = std::make_unique<CheckpointConfig>(config);

    switch (vbType) {
    case VBType::Persistent:
        vbucket = std::make_unique<EPVBucket>(
                vbid,
                vbucket_state_active,
                global_stats,
                *checkpoint_config,
                /*kvshard*/ nullptr,
                lastSeqno,
                range.getStart(),
                range.getEnd(),
                std::make_unique<FailoverTable>(1 /*capacity*/),
                /*flusher callback*/ nullptr,
                /*newSeqnoCb*/ nullptr,
                noOpSyncWriteResolvedCb,
                TracedSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                config,
                eviction_policy,
                std::move(manifest));
        break;
    case VBType::Ephemeral: {
        vbucket = std::make_unique<MockEphemeralVBucket>(
                vbid,
                vbucket_state_active,
                global_stats,
                *checkpoint_config,
                /*kvshard*/ nullptr,
                lastSeqno,
                range.getStart(),
                range.getEnd(),
                std::make_unique<FailoverTable>(1 /*capacity*/),
                /*newSeqnoCb*/ nullptr,
                noOpSyncWriteResolvedCb,
                TracedSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                config,
                eviction_policy,
                std::move(manifest));
        break;
    }
    }

    vbucket->checkpointManager = std::make_unique<MockCheckpointManager>(
            global_stats,
            *vbucket,
            *checkpoint_config,
            lastSeqno,
            range.getStart(),
            range.getEnd(),
            lastSeqno, // setting maxVisibleSeqno to equal lastSeqno
            /*flusher callback*/ nullptr);

    cookie = create_mock_cookie(nullptr);
}

VBucketTestBase::~VBucketTestBase() {
    vbucket.reset();
    destroy_mock_cookie(cookie);
}

std::vector<StoredDocKey> VBucketTestBase::generateKeys(int num, int start) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num + start; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

queued_item VBucketTestBase::makeQueuedItem(const char* key) {
    std::string val("x");
    uint32_t flags = 0;
    time_t expiry = 0;
    return queued_item(new Item(makeStoredDocKey(key),
                                flags,
                                expiry,
                                val.c_str(),
                                val.size()));
}

AddStatus VBucketTestBase::addOne(const StoredDocKey& k, int expiry) {
    Item i(k, 0, expiry, k.data(), k.size());
    return public_processAdd(i);
}

TempAddStatus VBucketTestBase::addOneTemp(const StoredDocKey& k) {
    auto hbl_sv = lockAndFind(k);
    return vbucket->addTempStoredValue(hbl_sv.first, k).status;
}

void VBucketTestBase::addMany(std::vector<StoredDocKey>& keys,
                              AddStatus expect) {
    for (const auto& k : keys) {
        EXPECT_EQ(expect, addOne(k));
    }
}

MutationStatus VBucketTestBase::setOne(const StoredDocKey& k, int expiry) {
    Item i(k, 0, expiry, k.data(), k.size());
    return public_processSet(i, i.getCas());
}

void VBucketTestBase::setMany(std::vector<StoredDocKey>& keys,
                              MutationStatus expect) {
    for (const auto& k : keys) {
        EXPECT_EQ(expect, setOne(k));
    }
}

void VBucketTestBase::softDeleteOne(const StoredDocKey& k,
                                    MutationStatus expect) {
    EXPECT_EQ(expect, public_processSoftDelete(k).first)
            << "Failed to soft delete key " << k.c_str();
}

void VBucketTestBase::softDeleteMany(std::vector<StoredDocKey>& keys,
                                     MutationStatus expect) {
    for (const auto& k : keys) {
        softDeleteOne(k, expect);
    }
}

StoredValue* VBucketTestBase::findValue(StoredDocKey& key) {
    return vbucket->ht.findForWrite(key).storedValue;
}

void VBucketTestBase::verifyValue(StoredDocKey& key,
                                  const char* value,
                                  TrackReference trackReference,
                                  WantsDeleted wantDeleted) {
    auto v = vbucket->ht.findForRead(key, trackReference, wantDeleted);
    EXPECT_NE(nullptr, v.storedValue);
    value_t val = v.storedValue->getValue();

    if (!value) {
        EXPECT_EQ(nullptr, val.get().get());
    } else {
        EXPECT_STREQ(value, val->to_s().c_str());
    }
}

std::pair<HashTable::HashBucketLock, StoredValue*> VBucketTestBase::lockAndFind(
        const StoredDocKey& key, const VBQueueItemCtx& ctx) {
    auto htRes = ctx.durability ? vbucket->ht.findForSyncWrite(key)
                                : vbucket->ht.findForWrite(key);
    return {std::move(htRes.lock), htRes.storedValue};
}

MutationStatus VBucketTestBase::public_processSet(Item& itm,
                                                  const uint64_t cas,
                                                  const VBQueueItemCtx& ctx) {
    // Need to take the collections read handle before the hbl
    auto cHandle = vbucket->lockCollections(itm.getKey());

    auto htRes = vbucket->ht.findForUpdate(itm.getKey());
    auto* v = htRes.selectSVToModify(itm);

    return vbucket
            ->processSet(
                    htRes, v, itm, cas, true, false, ctx, {/*no predicate*/})
            .first;
}

AddStatus VBucketTestBase::public_processAdd(Item& itm,
                                             const VBQueueItemCtx& ctx) {
    // Need to take the collections read handle before the hbl
    auto cHandle = vbucket->lockCollections(itm.getKey());
    auto htRes = vbucket->ht.findForUpdate(itm.getKey());

    StoredValue* v = htRes.selectSVToModify(itm);
    return vbucket
            ->processAdd(htRes,
                         v,
                         itm,
                         /*maybeKeyExists*/ true,
                         ctx,
                         cHandle)
            .first;
}

std::pair<MutationStatus, StoredValue*>
VBucketTestBase::public_processSoftDelete(const DocKey& key,
        VBQueueItemCtx ctx) {
    // Need to take the collections read handle before the hbl
    auto cHandle = vbucket->lockCollections(key);
    auto htRes = vbucket->ht.findForUpdate(key);
    auto* v = htRes.selectSVToModify(ctx.durability.has_value());
    if (!v) {
        return {MutationStatus::NotFound, nullptr};
    }
    if (v->isDeleted() && !v->isPending()) {
        return {MutationStatus::NotFound, nullptr};
    }
    return public_processSoftDelete(htRes, *v, ctx);
}

std::pair<MutationStatus, StoredValue*>
VBucketTestBase::public_processSoftDelete(HashTable::FindUpdateResult& htRes,
                                          StoredValue& v,
                                          VBQueueItemCtx ctx) {
    ItemMetaData metadata;
    metadata.revSeqno = v.getRevSeqno() + 1;
    MutationStatus status;
    StoredValue* deletedSV;
    std::tie(status, deletedSV, std::ignore) =
            vbucket->processSoftDelete(htRes,
                                       v,
                                       /*cas*/ 0,
                                       metadata,
                                       ctx,
                                       /*use_meta*/ false,
                                       v.getBySeqno(),
                                       DeleteSource::Explicit);
    return {status, deletedSV};
}

bool VBucketTestBase::public_deleteStoredValue(const DocKey& key) {
    auto hbl_sv = lockAndFind(StoredDocKey(key));
    if (!hbl_sv.second) {
        return false;
    }
    return vbucket->deleteStoredValue(hbl_sv.first, *hbl_sv.second);
}

std::pair<MutationStatus, GetValue> VBucketTestBase::public_getAndUpdateTtl(
        const DocKey& key, time_t exptime) {
    // Need to take the collections read handle before the hbl
    auto cHandle = vbucket->lockCollections(key);
    auto hbl = lockAndFind(StoredDocKey(key));
    return vbucket->processGetAndUpdateTtl(
            hbl.first, hbl.second, exptime, cHandle);
}

std::tuple<MutationStatus, StoredValue*, VBNotifyCtx>
VBucketTestBase::public_processExpiredItem(
        HashTable::FindUpdateResult& htRes,
        const Collections::VB::CachingReadHandle& cHandle,
        ExpireBy expirySource) {
    return vbucket->processExpiredItem(htRes, cHandle, expirySource);
}

bool operator==(const SWCompleteTrace& lhs, const SWCompleteTrace& rhs) {
    return lhs.count == rhs.count && lhs.cookie == rhs.cookie &&
           lhs.status == rhs.status;
}

class BlobTest : public Blob {
public:
    BlobTest() : Blob(nullptr,0) {}
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

std::string VBucketTest::PrintToStringParamName(
        const ::testing::TestParamInfo<ParamType>& info) {
    return to_string(std::get<0>(info.param)) + "_" +
           ::to_string(std::get<1>(info.param));
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
    if (getEvictionPolicy() != EvictionPolicy::Value) {
        return;
    }
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    addMany(keys, AddStatus::Success);

    StoredDocKey missingKey = makeStoredDocKey("aMissingKey");
    EXPECT_FALSE(this->vbucket->ht.findForRead(missingKey).storedValue);

    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.findForRead(key).storedValue);
    }

    addMany(keys, AddStatus::Exists);
    for (const auto& key : keys) {
        EXPECT_TRUE(this->vbucket->ht.findForRead(key).storedValue);
    }

    // Verify we can read after a soft deletion.
    EXPECT_EQ(MutationStatus::WasDirty,
              this->public_processSoftDelete(keys[0]).first);
    EXPECT_EQ(MutationStatus::NotFound,
              this->public_processSoftDelete(keys[0]).first);
    EXPECT_FALSE(this->vbucket->ht.findForRead(keys[0]).storedValue);

    Item i(keys[0], 0, 0, "newtest", 7);
    EXPECT_EQ(AddStatus::UnDel, this->public_processAdd(i));
    EXPECT_EQ(nkeys, this->vbucket->ht.getNumItems());
}

TEST_P(VBucketTest, AddExpiry) {
    // Need mock time functions to be able to time travel
    initialize_time_functions(get_mock_server_api()->core);

    if (getEvictionPolicy() != EvictionPolicy::Value) {
        return;
    }
    StoredDocKey k = makeStoredDocKey("aKey");

    ASSERT_EQ(AddStatus::Success, addOne(k, ep_real_time() + 5));
    EXPECT_EQ(AddStatus::Exists, addOne(k, ep_real_time() + 5));

    const auto* v = this->vbucket->ht.findForRead(k).storedValue;
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
    if (getEvictionPolicy() != EvictionPolicy::Value) {
        return;
    }

    // Setup - create a key and then delete it with a value.
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0, 0, "value", strlen("value"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->public_processSet(stored_item, stored_item.getCas()));

    auto* v(this->vbucket->ht.findForRead(key).storedValue);
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

    const auto* v = this->vbucket->ht.findForRead(key).storedValue;
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
    if (getEvictionPolicy() != EvictionPolicy::Value) {
        return;
    }

    // Setup - create a key and then delete it.
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0, 0, "value", strlen("value"));
    ASSERT_EQ(MutationStatus::WasClean,
              this->public_processSet(stored_item, stored_item.getCas()));

    auto deleteRes = this->public_processSoftDelete(key);
    EXPECT_EQ(MutationStatus::WasDirty, deleteRes.first);
    auto* v = deleteRes.second;
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

// Check that getItemsForCursor() can impose a limit on items fetched, but
// that it always fetches complete checkpoints.
TEST_P(VBucketTest, GetItemsForCursor_Limit) {
    // @todo: Expand to Ephemeral
    if (!persistent()) {
        return;
    }

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
    //
    // Note: Scope triggers the flush handle that removes the backup cursor
    {
        auto result = this->vbucket->getItemsToPersist(1);
        EXPECT_TRUE(result.moreAvailable);
        EXPECT_EQ(4, result.items.size());
        EXPECT_TRUE(result.items[0]->isCheckPointMetaItem());
        EXPECT_STREQ("1", result.items[1]->getKey().c_str());
        EXPECT_STREQ("2", result.items[2]->getKey().c_str());
        EXPECT_TRUE(result.items[3]->isCheckPointMetaItem());
        EXPECT_EQ(range.getStart(), result.ranges.front().getStart());
        EXPECT_EQ(range.getEnd() + 2, result.ranges.back().getEnd());
    }

    // Asking for 5 items should give us all items in second checkpoint and
    // third checkpoint - 7 total
    // (ckpt start, 2x mutation, ckpt_end, ckpt_start, 2x mutation)
    auto result = this->vbucket->getItemsToPersist(5);
    EXPECT_FALSE(result.moreAvailable);
    EXPECT_EQ(7, result.items.size());
    EXPECT_TRUE(result.items[0]->isCheckPointMetaItem());
    EXPECT_STREQ("3", result.items[1]->getKey().c_str());
    EXPECT_STREQ("4", result.items[2]->getKey().c_str());
    EXPECT_TRUE(result.items[3]->isCheckPointMetaItem());
    EXPECT_TRUE(result.items[4]->isCheckPointMetaItem());
    EXPECT_STREQ("5", result.items[5]->getKey().c_str());
    EXPECT_STREQ("6", result.items[6]->getKey().c_str());
    EXPECT_EQ(range.getEnd() + 3, result.ranges.front().getStart());
    EXPECT_EQ(range.getEnd() + 6, result.ranges.back().getEnd());
}

// Check that getItemsToPersist() can correctly impose a limit on items fetched.
// Note that from MB-37546 the CheckpointManager is the only source of items.
TEST_P(VBucketTest, DISABLED_GetItemsToPersist_Limit) {
    // Setup - Add items to multiple checkpoints in the CM.

    // Add 2 items into the first checkpoint (in addition to initial
    // checkpoint_start).
    auto keys = generateKeys(2 /*numItems*/, 1 /*start key*/);
    setMany(keys, MutationStatus::WasClean);
    auto& manager = *vbucket->checkpointManager;
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getNumItemsForPersistence());

    // Create second checkpoint
    const auto firstCkptId = manager.getOpenCheckpointId();
    const auto secondCkptId = manager.createNewCheckpoint();
    ASSERT_GT(secondCkptId, firstCkptId);

    // Add items to the second checkpoint
    keys = generateKeys(3 /*numItems*/, 3 /*start key*/);
    setMany(keys, MutationStatus::WasClean);
    EXPECT_EQ(3, manager.getNumOpenChkItems());
    EXPECT_EQ(5, manager.getNumItemsForPersistence());

    // Test - fetch items with (approxLimit < size_first_ckpt), we must retrieve
    // the complete first checkpoint (as it is not valid to read partial
    // checkpoint contents) but no item from the second checkpoint.
    auto result = this->vbucket->getItemsToPersist(1 /*approxLimit*/);
    EXPECT_TRUE(result.moreAvailable);
    ASSERT_EQ(3, result.items.size());
    EXPECT_STREQ("1", result.items[0]->getKey().c_str());
    EXPECT_STREQ("2", result.items[1]->getKey().c_str());
    EXPECT_EQ(queue_op::checkpoint_end, result.items[2]->getOperation());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(range.getStart(), result.ranges.front().getStart());
    EXPECT_EQ(range.getEnd() + 2, result.ranges.back().getEnd());

    // Again, get the complete second checkpoint, different items and range
    // expected
    result = this->vbucket->getItemsToPersist(2 /*approxLimit*/);
    EXPECT_FALSE(result.moreAvailable);
    EXPECT_EQ(4, result.items.size());
    EXPECT_EQ(queue_op::checkpoint_start, result.items[0]->getOperation());
    EXPECT_STREQ("3", result.items[1]->getKey().c_str());
    EXPECT_STREQ("4", result.items[2]->getKey().c_str());
    EXPECT_STREQ("5", result.items[3]->getKey().c_str());
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(range.getEnd() + 3, result.ranges.front().getStart());
    EXPECT_EQ(range.getEnd() + 5, result.ranges.back().getEnd());
}

TEST_P(VBucketTest, GetItemsToPersist_ZeroLimitThrows) {
    if (!persistent()) {
        return;
    }

    try {
        vbucket->getItemsToPersist(0);
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("limit=0"));
        return;
    }
    FAIL();
}

// Check that getItemsToPersist() correctly returns `moreAvailable` if we
// hit the CheckpointManager limit early.
TEST_P(VBucketTest, GetItemsToPersist_LimitCkptMoreAvailable) {
    if (!persistent()) {
        return;
    }

    // Setup - Add 2 items to checkpoint manager (in addition to initial
    // checkpoint_start).
    // The 2 items need to be in different checkpoints as the limit is an
    // approximate limit.
    auto& manager = *vbucket->checkpointManager;
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("1")));
    manager.createNewCheckpoint();
    ASSERT_EQ(2, manager.getOpenCheckpointId());
    ASSERT_EQ(MutationStatus::WasClean, setOne(makeStoredDocKey("2")));

    // Test - fetch items such that we have a limit for CheckpointManager of
    // 1. This should return moreAvailable=true.
    auto result = vbucket->getItemsToPersist(1);
    EXPECT_TRUE(result.moreAvailable);
    // start + mutation + end
    EXPECT_EQ(3, result.items.size());
}

class VBucketEvictionTest : public VBucketTest {};

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
              this->public_processSoftDelete(key).first);

    // Attempt to perform a set on a deleted key with a CAS.
    Item replacement(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(MutationStatus::NotFound,
              this->public_processSet(replacement,
                                               /*cas*/ 10))
            << "When trying to replace-with-CAS a deleted item";
}

TEST_P(VBucketEvictionTest, Durability_PendingNeverEjected) {
    // Necessary for enqueueing into the DurabilityMonitor (VBucket::set fails
    // otherwise)
    auto meta = nlohmann::json{
            {"topology", nlohmann::json::array({{"active", "replica"}})}};
    vbucket->setState(vbucket_state_active, &meta);

    ASSERT_EQ(0, vbucket->getNumItems());
    ASSERT_EQ(0, vbucket->getNumNonResidentItems());

    const auto item = makePendingItem(makeStoredDocKey("key"), "value");
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    ASSERT_EQ(MutationStatus::WasClean,
              public_processSet(*item, item->getCas(), ctx));

    auto& ht = vbucket->ht;

    // The Pending is resident
    EXPECT_EQ(1, ht.getNumItems());
    EXPECT_EQ(0, ht.getNumInMemoryNonResItems());

    auto storedItem = ht.findForWrite(item->getKey());
    ASSERT_TRUE(storedItem.storedValue);
    // Need to clear the dirty flag to ensure that we are testing the right
    // thing, i.e. that the item is not ejected because it is Pending (not
    // because it is dirty).
    storedItem.storedValue->markClean();
    ASSERT_FALSE(ht.unlocked_ejectItem(
            storedItem.lock, storedItem.storedValue, getEvictionPolicy()));

    // A Pending is never ejected (Key + Metadata + Value always resident)
    EXPECT_EQ(1, ht.getNumItems());
    EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
}

class VBucketFullEvictionTest : public VBucketTest {};

// This test aims to ensure the vBucket document count is correct in the
// scenario where the flusher picks up a deletion of a document then we
// recreate the document before the persistence callback is invoked.
TEST_P(VBucketFullEvictionTest, MB_30137) {
    auto k = makeStoredDocKey("key");
    queued_item qi(new Item(k, 0, 0, k.data(), k.size()));

    // (1) Store k
    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*qi, qi->getCas()));

    auto storedValue = vbucket->ht.findForRead(k).storedValue;
    ASSERT_TRUE(storedValue);
    EPPersistenceCallback cb1(global_stats, *vbucket);

    // (1.1) We need the persistence cursor to move through the current
    // checkpoint to ensure that the checkpoint manager correctly updates the
    // vBucket stats that throw assertions in development builds to identify
    // correctness issues. At this point we aim to persist everything fully
    // (1.2) before performing any other operations.
    auto cursorResult = vbucket->checkpointManager->registerCursorBySeqno(
            "persistence", 0, CheckpointCursor::Droppable::No);
    auto cursorPtr = cursorResult.cursor.lock();
    ASSERT_TRUE(cursorPtr);
    std::vector<queued_item> out;
    vbucket->checkpointManager->getItemsForCursor(*cursorPtr, out, 1);
    ASSERT_EQ(2, out.size());

    // (1.2) Mimic flusher by running the PCB for the store at (1)
    cb1(*out.back(),
        FlushStateMutation::Insert); // Using the create/update
                                              // callback

    EXPECT_EQ(1, vbucket->getNumItems());

    // (2) Delete k
    softDeleteOne(k, MutationStatus::WasClean);

    // (2.1) Now we need to mimic the persistence cursor moving through the
    // checkpoint again (picking up only the deletion (2)) to ensure that the
    // checkpoint manager sets the vBucket stats correctly (in particular the
    // dirtyQueueSize and the diskQueueSize).
    out.clear();
    vbucket->checkpointManager->getItemsForCursor(*cursorPtr, out, 1);
    ASSERT_EQ(1, out.size());

    // (3) Now set k again
    EXPECT_EQ(MutationStatus::WasDirty, public_processSet(*qi, qi->getCas()));

    // (3.1) Run the PCB for the delete/expiry (2)
    // Using the delete callback
    cb1(*out.back(), FlushStateDeletion::Delete);

    // In FE mode, getNumItems is tracking disk items, so we should have 0 disk
    // items until the 'flush' of the second store (3)
    EXPECT_EQ(0, vbucket->getNumItems());

    // (4) run the create/update PCB again, the store (3) should look like a
    // create because of the delete at (2)
    cb1(*out.back(),
        FlushStateMutation::Insert); // Using the create/update
                                              // callback

    EXPECT_EQ(1, vbucket->getNumItems());
}

// Test cases which run for persistent and ephemeral, and for each of their
// respective eviction policies (Value/Full for persistent, Auto-delete and
// fail new data for Ephemeral).
INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        VBucketTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

// Test cases related to eviction.
// They run for persistent and ephemeral, and for each of their
// respective eviction policies (Value/Full for persistent, Auto-delete and
// fail new data for Ephemeral).
INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        VBucketEvictionTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);

// Test cases which run in for Persistent VBucket, Full Eviction only
INSTANTIATE_TEST_SUITE_P(
        FullEviction,
        VBucketFullEvictionTest,
        ::testing::Values(std::make_tuple(VBucketTestBase::VBType::Persistent,
                                          EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);
