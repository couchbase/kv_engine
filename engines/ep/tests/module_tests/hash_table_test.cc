/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "hash_table_test.h"
#include "ep_time.h"
#include "hash_table_stat_visitor.h"
#include "item.h"
#include "item_freq_decayer_visitor.h"
#include "kv_bucket.h"
#include "programs/engine_testapp/mock_server.h"
#include "stats.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"
#include "threadtests.h"

#include <boost/dynamic_bitset.hpp>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/cb_malloc.h>

#include <signal.h>
#include <algorithm>
#include <limits>
#include <random>
#include <string>
#include <utility>

EPStats global_stats;

class Counter : public HashTableVisitor {
public:

    size_t count;
    size_t deleted;

    explicit Counter(bool v) : count(), deleted(), verify(v) {
    }

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override {
        if (v.isDeleted()) {
            ++deleted;
        } else {
            ++count;
            if (verify) {
                StoredDocKey key(v.getKey());
                value_t val = v.getValue();
                EXPECT_EQ(key.to_string(), val->to_s());
            }
        }
        return true;
    }

private:
    bool verify;
};

static int count(HashTable &h, bool verify=true) {
    Counter c(verify);
    h.visit(c);
    EXPECT_EQ(h.getNumItems(), c.count + c.deleted);
    return c.count;
}

static Item store(HashTable& h, const StoredDocKey& k) {
    auto value = k.to_string();
    Item i(k, 0, 0, value.data(), value.size());
    EXPECT_EQ(MutationStatus::WasClean, h.set(i));
    return i;
}

static void storeMany(HashTable& h, const std::vector<StoredDocKey>& keys) {
    for (const auto& key : keys) {
        store(h, key);
    }
}

template <typename T>
static const char* toString(AddStatus a) {
    switch(a) {
    case AddStatus::Success:
        return "AddStatus::Success";
    case AddStatus::NoMem:
        return "AddStatus::NoMem";
    case AddStatus::Exists:
        return "AddStatus::Exists";
    case AddStatus::UnDel:
        return "AddStatus::UnDel";
    case AddStatus::AddTmpAndBgFetch:
        return "AddStatus::AddTmpAndBgFetch";
    case AddStatus::BgFetch:
        return "AddStatus::BgFetch";
    }
    abort();
    return nullptr;
}

static std::vector<StoredDocKey> generateKeys(int num, int start=0) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

HashTableTest::HashTableTest() : defaultHtSize(Configuration().getHtSize()) {
    // Need mock time functions to be able to time travel
    initialize_time_functions(get_mock_server_api()->core);
}

bool HashTableTest::del(HashTable& ht, const DocKey& key) {
    auto htRes = ht.findForWrite(key);
    if (!htRes.storedValue) {
        return false;
    }
    ht.unlocked_del(htRes.lock, htRes.storedValue);
    return true;
}

std::unique_ptr<AbstractStoredValueFactory> HashTableTest::makeFactory(
        bool isOrdered) {
    if (isOrdered) {
        return std::make_unique<OrderedStoredValueFactory>(global_stats);
    }
    return std::make_unique<StoredValueFactory>(global_stats);
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

TEST_F(HashTableTest, Size) {
    HashTable h(global_stats,
                makeFactory(),
                defaultHtSize,
                /*locks*/ 1);
    ASSERT_EQ(0, count(h));

    store(h, makeStoredDocKey("testkey"));

    EXPECT_EQ(1, count(h));
}

TEST_F(HashTableTest, SizeTwo) {
    HashTable h(global_stats,
                makeFactory(),
                defaultHtSize,
                /*locks*/ 1);
    ASSERT_EQ(0, count(h));

    auto keys = generateKeys(5);
    storeMany(h, keys);
    EXPECT_EQ(5, count(h));

    h.clear();
    EXPECT_EQ(0, count(h));
}

TEST_F(HashTableTest, ReverseDeletions) {
    size_t initialSize = global_stats.getCurrentSize();
    HashTable h(global_stats, makeFactory(), 5, 1);
    ASSERT_EQ(0, count(h));
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);
    EXPECT_EQ(nkeys, count(h));

    std::reverse(keys.begin(), keys.end());

    for (const auto& key : keys) {
        del(h, key);
    }

    EXPECT_EQ(0, count(h));
    EXPECT_EQ(initialSize, global_stats.getCurrentSize());
}

TEST_F(HashTableTest, ForwardDeletions) {
    size_t initialSize = global_stats.getCurrentSize();
    HashTable h(global_stats, makeFactory(), 5, 1);
    ASSERT_EQ(5, h.getSize());
    ASSERT_EQ(1, h.getNumLocks());
    ASSERT_EQ(0, count(h));
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);
    EXPECT_EQ(nkeys, count(h));

    for (const auto& key : keys) {
        del(h, key);
    }

    EXPECT_EQ(0, count(h));
    EXPECT_EQ(initialSize, global_stats.getCurrentSize());
}

static void verifyFound(HashTable &h, const std::vector<StoredDocKey> &keys) {
    EXPECT_FALSE(h.findForRead(makeStoredDocKey("aMissingKey")).storedValue);

    for (const auto& key : keys) {
        EXPECT_TRUE(h.findForRead(key).storedValue);
    }
}

static void testFind(HashTable &h) {
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);

    verifyFound(h, keys);
}

TEST_F(HashTableTest, Find) {
    HashTable h(global_stats, makeFactory(), 5, 1);
    testFind(h);
}

TEST_F(HashTableTest, Resize) {
    HashTable h(global_stats, makeFactory(), 5, 3);

    auto keys = generateKeys(1000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize(6143);
    EXPECT_EQ(6143, h.getSize());

    verifyFound(h, keys);

    h.resize(769);
    EXPECT_EQ(769, h.getSize());

    verifyFound(h, keys);

    h.resize(static_cast<size_t>(std::numeric_limits<int>::max()) + 17);
    EXPECT_EQ(769, h.getSize());

    verifyFound(h, keys);
}

class AccessGenerator : public Generator<bool> {
public:
    AccessGenerator(std::vector<StoredDocKey> k, HashTable& h)
        : keys(std::move(k)), ht(h), size(10000) {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(keys.begin(), keys.end(), g);
    }

    bool operator()() override {
        for (const auto& key : keys) {
            if (rand() % 111 == 0) {
                resize();
            }
            HashTableTest::del(ht, key);
        }
        return true;
    }

private:

    void resize() {
        ht.resize(size);
        size = size == 1000 ? 3000 : 1000;
    }

    std::vector<StoredDocKey>  keys;
    HashTable                &ht;
    std::atomic<size_t>       size;
};

TEST_F(HashTableTest, ConcurrentAccessResize) {
    HashTable h(global_stats, makeFactory(), 5, 3);

    auto keys = generateKeys(2000);
    h.resize(keys.size());
    storeMany(h, keys);

    verifyFound(h, keys);

    srand(918475);
    AccessGenerator gen(keys, h);
    getCompletedThreads(4, &gen);
}

TEST_F(HashTableTest, AutoResize) {
    HashTable h(global_stats, makeFactory(), 5, 3);

    ASSERT_EQ(5, h.getSize());

    auto keys = generateKeys(1000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize();
    EXPECT_EQ(769, h.getSize());
    verifyFound(h, keys);
}

TEST_F(HashTableTest, DepthCounting) {
    HashTable h(global_stats, makeFactory(), 5, 1);
    const int nkeys = 5000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);

    HashTableDepthStatVisitor depthCounter;
    h.visitDepth(depthCounter);
    // std::cout << "Max depth:  " << depthCounter.maxDepth << std::endl;
    EXPECT_GT(depthCounter.max, 1000);
}

TEST_F(HashTableTest, PoisonKey) {
    HashTable h(global_stats, makeFactory(), 5, 1);

    store(h, makeStoredDocKey("A\\NROBs_oc)$zqJ1C.9?XU}Vn^(LW\"`+K/4lykF[ue0{ram;fvId6h=p&Zb3T~SQ]82'ixDP"));
    EXPECT_EQ(1, count(h));
}

// Test fixture for HashTable statistics tests.
class HashTableStatsTest
    : public HashTableTest,
      public ::testing::WithParamInterface<std::tuple<EvictionPolicy, bool>> {
protected:
    HashTableStatsTest()
        : ht(stats, makeFactory(), 5, 1),
          initialSize(0),
          key(makeStoredDocKey("somekey")),
          itemSize(16 * 1024),
          item(key, 0, 0, std::string(itemSize, 'x').data(), itemSize),
          evictionPolicy(std::get<0>(GetParam())) {
        // Assign a valid sequence number.
        item.setBySeqno(10);
        // Compress if test parameter specifies so
        if (std::get<1>(GetParam())) {
            EXPECT_TRUE(item.compressValue());
        }
    }

    void SetUp() override {
        global_stats.reset();
        ASSERT_EQ(0, ht.getItemMemory());
        ASSERT_EQ(0, ht.getCacheSize());
        ASSERT_EQ(0, ht.getMetadataMemory());
        ASSERT_EQ(0, ht.getUncompressedItemMemory());
        initialSize = stats.getCurrentSize();

        EXPECT_EQ(0, ht.getNumItems());
        EXPECT_EQ(0, ht.getNumInMemoryItems());
        EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
        EXPECT_EQ(0, ht.getNumTempItems());
        EXPECT_EQ(0, ht.getNumDeletedItems());
        EXPECT_EQ(0, ht.getNumSystemItems());
        EXPECT_EQ(0, ht.getNumPreparedSyncWrites());
        for (const auto& count : ht.getDatatypeCounts()) {
            EXPECT_EQ(0, count);
        }
    }

    void TearDown() override {
        EXPECT_EQ(0, ht.getItemMemory());
        EXPECT_EQ(0, ht.getUncompressedItemMemory());
        EXPECT_EQ(0, ht.getCacheSize());
        EXPECT_EQ(0, ht.getMetadataMemory());
        EXPECT_EQ(initialSize, stats.getCurrentSize());

        if (evictionPolicy == EvictionPolicy::Value) {
            // Only check is zero for ValueOnly; under full eviction getNumItems
            // return the total number of items (including those fully evicted).
            EXPECT_EQ(0, ht.getNumItems());
        }
        EXPECT_EQ(0, ht.getNumInMemoryItems());
        EXPECT_EQ(0, ht.getNumTempItems());
        EXPECT_EQ(0, ht.getNumDeletedItems());
        EXPECT_EQ(0, ht.getNumSystemItems());
        EXPECT_EQ(0, ht.getNumPreparedSyncWrites());
        for (const auto& datatypeCount : ht.getDatatypeCounts()) {
            EXPECT_EQ(0, datatypeCount);
        }
    }

    StoredValue* addAndEjectItem() {
        EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

        auto result(ht.findForWrite(key));
        EXPECT_TRUE(result.storedValue);
        result.storedValue->markClean();
        EXPECT_TRUE(ht.unlocked_ejectItem(
                result.lock, result.storedValue, evictionPolicy));
        return result.storedValue;
    }

    /// How should items be cleaned up at end of test:
    enum class CleanupMethod { ExplicitDelete, HashTableClear };

    void testSoftDelete(CleanupMethod method);
    void testPreparedSyncWrite(CleanupMethod method);
    void testPreparedSyncDelete(CleanupMethod method);

    EPStats stats;
    HashTable ht;
    size_t initialSize;
    const StoredDocKey key;
    const size_t itemSize;
    Item item;
    const EvictionPolicy evictionPolicy;
};

TEST_P(HashTableStatsTest, Size) {
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

    del(ht, key);
}

TEST_P(HashTableStatsTest, SizeFlush) {
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

    ht.clear();
}

TEST_P(HashTableStatsTest, SizeEject) {
    addAndEjectItem();

    del(ht, key);
}

// Check sizes when ejecting a value and then restoring it.
TEST_P(HashTableStatsTest, SizeEjectRestoreValue) {
    auto* v = addAndEjectItem();

    if (evictionPolicy == EvictionPolicy::Value) {
        // Value-only: expect to have metadata still present.
        EXPECT_EQ(1, ht.getNumItems());
        EXPECT_EQ(1, ht.getNumInMemoryNonResItems());
        EXPECT_EQ(v->metaDataSize(), ht.getUncompressedItemMemory())
                << "Expected uncompressed memory to be sizeof metadata after "
                   "evicting";
        EXPECT_EQ(1, ht.getDatatypeCounts()[item.getDataType()]);
    }

    if (evictionPolicy == EvictionPolicy::Full) {
        // ejectItem() will have removed both the value and meta.
        EXPECT_EQ(0, ht.getNumItems());
        EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
        EXPECT_EQ(0, ht.getUncompressedItemMemory());
        EXPECT_EQ(0, ht.getDatatypeCounts()[item.getDataType()]);

        // Need a new tempItem (metadata) to restore value into.
        Item temp(key,
                  0,
                  0,
                  nullptr,
                  0,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0,
                  StoredValue::state_temp_init);
        auto hbl = ht.getLockedBucket(key);
        v = ht.unlocked_addNewStoredValue(hbl, temp);
        EXPECT_EQ(1, ht.getNumTempItems());
    }

    {
        auto hbl = ht.getLockedBucket(key);
        EXPECT_TRUE(ht.unlocked_restoreValue(hbl, item, *v));
    }

    EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
    EXPECT_EQ(1, ht.getDatatypeCounts()[item.getDataType()]);

    del(ht, key);
}

// Check sizes when ejecting an item and then restoring metadata.
TEST_P(HashTableStatsTest, SizeEjectRestoreMeta) {
    if (evictionPolicy == EvictionPolicy::Value) {
        // restoreMeta only valid for Full eviction - metadata is
        // unevictable in value-only.
        return;
    }

    addAndEjectItem();

    // ejectItem() will have removed both the value and meta. Need
    // a new tempItem (metadata) to restore metdata into.
    Item temp(key,
              0,
              0,
              nullptr,
              0,
              PROTOCOL_BINARY_RAW_BYTES,
              0,
              StoredValue::state_temp_init);
    {
        auto hbl = ht.getLockedBucket(key);
        auto* v = ht.unlocked_addNewStoredValue(hbl, temp);
        EXPECT_EQ(1, ht.getNumTempItems());
        ht.unlocked_restoreMeta(hbl, item, *v);
        EXPECT_EQ(1, ht.getNumInMemoryNonResItems());
    }

    del(ht, key);
}

TEST_P(HashTableStatsTest, EjectFlush) {
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

    EXPECT_EQ(1, ht.getNumItems());
    EXPECT_EQ(1, ht.getNumInMemoryItems());
    EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
    EXPECT_EQ(0, ht.getNumTempItems());
    EXPECT_EQ(0, ht.getNumDeletedItems());

    {
        auto item(ht.findForWrite(key));
        EXPECT_TRUE(item.storedValue);
        item.storedValue->markClean();
        EXPECT_TRUE(ht.unlocked_ejectItem(
                item.lock, item.storedValue, evictionPolicy));
    }

    switch (evictionPolicy) {
    case EvictionPolicy::Value:
        EXPECT_EQ(1, ht.getNumItems());
        EXPECT_EQ(1, ht.getNumInMemoryItems());
        EXPECT_EQ(1, ht.getNumInMemoryNonResItems());
        break;
    case EvictionPolicy::Full:
        // In full-eviction, ejectItem() also ejects the metadata; hence will
        // have 0 items in HashTable.
        EXPECT_EQ(0, ht.getNumItems());
        EXPECT_EQ(0, ht.getNumInMemoryItems());
        EXPECT_EQ(0, ht.getNumInMemoryNonResItems());
        break;
    }
    EXPECT_EQ(0, ht.getNumTempItems());
    EXPECT_EQ(0, ht.getNumDeletedItems());

    ht.clear();
}

/*
 * Test that when unlocked_ejectItem returns false indicating that it failed
 * to eject an item, the stat numFailedEjects increases by one.
 */
TEST_P(HashTableStatsTest, numFailedEjects) {
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));
    EXPECT_EQ(0, stats.numFailedEjects);
    {
        auto item(ht.findForWrite(key));
        EXPECT_TRUE(item.storedValue);
        EXPECT_FALSE(ht.unlocked_ejectItem(
                item.lock, item.storedValue, evictionPolicy));
        EXPECT_EQ(1, stats.numFailedEjects);
    }
    ht.clear();
}

/**
 * MB-26126: Check that NumNonResident item counts are correct when restoring
 * a Deleted value to an metadata-only StoredValue.
 */
TEST_P(HashTableStatsTest, TempDeletedRestore) {
    // Add (deleted) item; then remove from HT.
    item.setDeleted();
    ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
    { // Locking scope.
        auto item = ht.findForWrite(key);
        ASSERT_NE(nullptr, item.storedValue);
        item.storedValue->markClean();
        ht.unlocked_del(item.lock, item.storedValue);
    }

    // Restore as temporary initial item (simulating bg_fetch).
    Item tempInitItem(key,
                      0,
                      0,
                      nullptr,
                      0,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0,
                      StoredValue::state_temp_init);
    {
        // Locking scope.
        auto hbl = ht.getLockedBucket(key);
        auto* sv = ht.unlocked_addNewStoredValue(hbl, tempInitItem);
        ASSERT_NE(nullptr, sv);

        // Restore the metadata for the (deleted) item.
        ht.unlocked_restoreMeta(hbl, item, *sv);

        // Check counts:
        EXPECT_EQ(0, ht.getNumItems())
                << "Deleted, meta shouldn't be counted in numItems";
        EXPECT_EQ(0, ht.getNumInMemoryItems())
                << "Deleted, meta shouldn't be counted as in-memory";
        EXPECT_EQ(0, ht.getNumInMemoryNonResItems())
                << "Deleted, meta shouldn't be counted as non-resident";
        EXPECT_EQ(1, ht.getNumTempItems())
                << "Deleted, meta should count as temp items";
        EXPECT_EQ(0, ht.getNumDeletedItems())
                << "Deleted, meta shouldn't count as deleted items";

        // Now restore the whole (deleted) value.
        EXPECT_TRUE(ht.unlocked_restoreValue(hbl, item, *sv));
    }

    // Check counts:
    EXPECT_EQ(1, ht.getNumItems())
            << "Deleted items should be counted in numItems";
    EXPECT_EQ(1, ht.getNumInMemoryItems())
            << "Deleted items should be counted as in-memory";
    EXPECT_EQ(0, ht.getNumInMemoryNonResItems())
            << "Deleted items shouldn't be counted as non-resident";
    EXPECT_EQ(0, ht.getNumTempItems())
            << "Deleted shouldn't count as temp items";
    EXPECT_EQ(1, ht.getNumDeletedItems())
            << "Deleted items should count as deleted items";

    // Cleanup, all counts should become zero.
    del(ht, key);
}

/// Check counts for a soft-deleted item.
TEST_P(HashTableStatsTest, SoftDelete) {
    testSoftDelete(CleanupMethod::ExplicitDelete);
}

TEST_P(HashTableStatsTest, SoftDeleteClear) {
    testSoftDelete(CleanupMethod::HashTableClear);
}

void HashTableStatsTest::testSoftDelete(CleanupMethod method) {
    ASSERT_EQ(MutationStatus::WasClean, ht.set(item));

    {
        // Mark the item as deleted. Locking scope.
        auto htRes = ht.findForWrite(key, WantsDeleted::No);
        ASSERT_NE(nullptr, htRes.storedValue);
        ht.unlocked_softDelete(htRes.lock,
                               *htRes.storedValue,
                               /*onlyMarkDeleted*/ true,
                               DeleteSource::Explicit);
    }

    // Check counts:
    EXPECT_EQ(1, ht.getNumItems())
            << "Deleted items should be counted in numItems";
    EXPECT_EQ(1, ht.getNumInMemoryItems())
            << "Deleted items should be counted as in-memory";
    EXPECT_EQ(0, ht.getNumInMemoryNonResItems())
            << "Deleted items shouldn't be counted as non-resident";
    EXPECT_EQ(0, ht.getNumTempItems())
            << "Deleted shouldn't count as temp items";
    EXPECT_EQ(1, ht.getNumDeletedItems())
            << "Deleted items should count as deleted items";

    // Cleanup, all counts should become zero.
    switch (method) {
    case CleanupMethod::ExplicitDelete:
        del(ht, key);
        break;
    case CleanupMethod::HashTableClear:
        ht.clear();
        break;
    }
}

/**
 * Test to track if the size of the uncompressed item memory
 * is updated correctly
 */
TEST_P(HashTableStatsTest, UncompressedMemorySizeTest) {
    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    auto item = makeCompressibleItem(Vbid(0),
                                     makeStoredDocKey("key0"),
                                     valueData,
                                     PROTOCOL_BINARY_DATATYPE_JSON,
                                     true);

    ASSERT_EQ(MutationStatus::WasClean, ht.set(*item));

    /**
     * Ensure that length of the value stored is the same as the length
     * of the compressed item
     */
    const auto* v = ht.findForRead(makeStoredDocKey("key0")).storedValue;
    EXPECT_NE(nullptr, v);
    EXPECT_EQ(item->getNBytes(), v->valuelen());

    EXPECT_EQ(1, ht.getNumItems());
    EXPECT_EQ(ht.getUncompressedItemMemory(),
              v->metaDataSize() + valueData.length());

    ASSERT_TRUE(del(ht, makeStoredDocKey("key0")));

    EXPECT_EQ(0, ht.getNumItems());
    EXPECT_EQ(0, ht.getUncompressedItemMemory());
}

TEST_P(HashTableStatsTest, SystemEventItem) {
    StoredDocKey key("key", CollectionID::System);
    store(ht, key);
    EXPECT_EQ(1, ht.getNumSystemItems());

    EXPECT_TRUE(del(ht, key));
    EXPECT_EQ(0, ht.getNumSystemItems());
}

// Test case for the collections purging path. We mark a system event as deleted
// so need to be able to deal with deleted system events in stats.
// In particular, removing a "deleted" system event item from the hash table
// should not cause us to update the numDeletedItems stat because we do not
// want to track system events in numDeletedItems
TEST_P(HashTableStatsTest, DeletedSystemEventItem) {
    StoredDocKey key("key", CollectionID::System);
    store(ht, key);
    ASSERT_EQ(1, ht.getNumSystemItems());
    ASSERT_EQ(0, ht.getNumDeletedItems());

    // Jump through a couple hoops to delete the thing, need the old StoredValue
    // and a new "deleted" system event item
    StoredValue* found = ht.findForWrite(key, WantsDeleted::No).storedValue;
    Item item(key, 0, 0, "", strlen(""));
    item.setDeleted(DeleteSource::Explicit);

    // Mark our item as deleted
    { // Scope for hbl so that we can del later
        auto hbl = ht.getLockedBucket(key);
        ht.unlocked_updateStoredValue(hbl, *found, item);
    }
    ASSERT_EQ(1, ht.getNumSystemItems());

    // We shouldn't throw anything (i.e. underflow stat counters) but we also
    // need to check the values in case we have turned assertions off.
    bool delRes = false;
    ASSERT_NO_THROW(delRes = del(ht, key));
    EXPECT_TRUE(delRes);
    EXPECT_EQ(0, ht.getNumSystemItems());
    EXPECT_EQ(0, ht.getNumDeletedItems());
}

void HashTableStatsTest::testPreparedSyncWrite(CleanupMethod method) {
    auto prepared = makePendingItem(key, "prepared");
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*prepared));

    // Test
    EXPECT_EQ(1, ht.getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht.getNumItems());
    for (const auto& count : ht.getDatatypeCounts()) {
        EXPECT_EQ(0, count);
    }

    // Cleanup
    switch (method) {
    case CleanupMethod::ExplicitDelete:
        EXPECT_TRUE(del(ht, key));
        break;
    case CleanupMethod::HashTableClear:
        ht.clear();
        break;
    }
}

TEST_P(HashTableStatsTest, PreparedSyncWrite) {
    testPreparedSyncWrite(CleanupMethod::ExplicitDelete);
}

TEST_P(HashTableStatsTest, PreparedSyncWriteClear) {
    testPreparedSyncWrite(CleanupMethod::HashTableClear);
}

void HashTableStatsTest::testPreparedSyncDelete(CleanupMethod method) {
    // Setup
    auto prepared = makePendingItem(key, "prepared");
    prepared->setDeleted(DeleteSource::Explicit);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(*prepared));

    // Test
    EXPECT_EQ(1, ht.getNumPreparedSyncWrites());
    EXPECT_EQ(1, ht.getNumItems());
    EXPECT_EQ(0, ht.getNumDeletedItems())
            << "NumDeletedItems should not include prepared SyncDeletes";
    for (const auto& count : ht.getDatatypeCounts()) {
        EXPECT_EQ(0, count);
    }

    // Cleanup
    switch (method) {
    case CleanupMethod::ExplicitDelete:
        EXPECT_TRUE(del(ht, key));
        break;
    case CleanupMethod::HashTableClear:
        ht.clear();
        break;
    }
}

TEST_P(HashTableStatsTest, PreparedSyncDelete) {
    testPreparedSyncDelete(CleanupMethod::ExplicitDelete);
}

TEST_P(HashTableStatsTest, PreparedSyncDeleteClear) {
    testPreparedSyncDelete(CleanupMethod::HashTableClear);
}

INSTANTIATE_TEST_SUITE_P(
        ValueAndFullEviction,
        HashTableStatsTest,
        ::testing::Combine(::testing::Values(EvictionPolicy::Value,
                                             EvictionPolicy::Full),
                           ::testing::Bool()));

TEST_F(HashTableTest, ItemAge) {
    // Setup
    HashTable ht(global_stats, makeFactory(), 5, 1);
    StoredDocKey key = makeStoredDocKey("key");
    Item item(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

    // Test
    StoredValue* v(ht.findForWrite(key).storedValue);
    EXPECT_EQ(0, v->getValue()->getAge());
    v->getValue()->incrementAge();
    EXPECT_EQ(1, v->getValue()->getAge());

    // Check saturation of age.
    for (int ii = 0; ii < 300; ii++) {
        v->getValue()->incrementAge();
    }
    EXPECT_EQ(0xff, v->getValue()->getAge());

    // Check reset of age after reallocation.
    v->reallocate();
    EXPECT_EQ(0, v->getValue()->getAge());

    // Check changing age when new value is used.
    Item item2(key, 0, 0, "value2", strlen("value2"));
    item2.getValue()->incrementAge();
    auto hbl = ht.getLockedBucket(key);
    auto updated = ht.unlocked_updateStoredValue(hbl, *v, item2);
    ASSERT_TRUE(updated.storedValue);
    EXPECT_EQ(1, updated.storedValue->getValue()->getAge());
}

/* Test release from HT (but not deletion) of an (HT) element */
TEST_F(HashTableTest, ReleaseItem) {
    /* Setup with 2 hash buckets and 1 lock */
    HashTable ht(global_stats, makeFactory(), 2, 1);

    /* Write 5 items (there are 2 hash buckets, we want to test removing a head
       element and a non-head element) */
    const int numItems = 5;
    std::string val("value");

    for (int i = 0; i < numItems; ++i) {
        StoredDocKey key =
                makeStoredDocKey(std::string("key" + std::to_string(i)));
        Item item(key, 0, 0, val.data(), val.length());
        EXPECT_EQ(MutationStatus::WasClean, ht.set(item));
    }
    EXPECT_EQ(numItems, ht.getNumItems());

    /* Remove the element added first. This is not (most likely because we
       might have a rare case wherein 4 items are hashed to one bucket and
       "removeKey1" would be a lone element in the other hash bucket) a head
       element of a hash bucket */
    StoredDocKey releaseKey1 =
            makeStoredDocKey(std::string("key" + std::to_string(0)));

    {
        // Locking scope.
        auto vToRelease1 = ht.findForWrite(releaseKey1, WantsDeleted::Yes);
        auto releasedSv1 =
                ht.unlocked_release(vToRelease1.lock, vToRelease1.storedValue);

        /* Validate the copied contents */
        EXPECT_EQ(*vToRelease1.storedValue, *(releasedSv1).get());

        /* Validate the HT element replace (hence released from HT) */
        EXPECT_EQ(vToRelease1.storedValue, releasedSv1.get().get());

        /* Validate the HT count */
        EXPECT_EQ(numItems - 1, ht.getNumItems());
    }

    /* Remove the element added last. This is certainly the head element of a
       hash bucket */
    StoredDocKey releaseKey2 =
            makeStoredDocKey(std::string("key" + std::to_string(numItems - 1)));

    auto vToRelease2 = ht.findForWrite(releaseKey2);
    auto releasedSv2 =
            ht.unlocked_release(vToRelease2.lock, vToRelease2.storedValue);

    /* Validate the copied contents */
    EXPECT_EQ(*vToRelease2.storedValue, *(releasedSv2).get());

    /* Validate the HT element replace (hence released from HT) */
    EXPECT_EQ(vToRelease2.storedValue, releasedSv2.get().get());

    /* Validate the HT count */
    EXPECT_EQ(numItems - 2, ht.getNumItems());
}

/* Test copying an element in HT */
TEST_F(HashTableTest, CopyItem) {
    /* Setup with 2 hash buckets and 1 lock. Note: Copying is allowed only on
       OrderedStoredValues and hence hash table must have
       OrderedStoredValueFactory */
    HashTable ht(global_stats, makeFactory(true), 2, 1);

    /* Write 3 items */
    const int numItems = 3;
    auto keys = generateKeys(numItems);
    storeMany(ht, keys);

    /* Replace the StoredValue in the HT by its copy */
    StoredDocKey copyKey = makeStoredDocKey(std::string(std::to_string(0)));
    auto replace = ht.findForWrite(copyKey);

    /* Record some stats before 'replace by copy' */
    auto metaDataMemBeforeCopy = ht.getMetadataMemory();
    auto datatypeCountsBeforeCopy = ht.getDatatypeCounts();
    auto cacheSizeBeforeCopy = ht.getCacheSize();
    auto memSizeBeforeCopy = ht.getItemMemory();
    auto statsCurrSizeBeforeCopy = global_stats.getCurrentSize();

    auto res = ht.unlocked_replaceByCopy(replace.lock, *replace.storedValue);

    /* Validate the copied contents */
    EXPECT_EQ(*replace.storedValue, *(res.first));

    /* Validate the HT element replace (hence released from HT) */
    EXPECT_EQ(replace.storedValue, res.second.get().get());

    /* Validate the HT count */
    EXPECT_EQ(numItems, ht.getNumItems());

    /* Stats should be equal to that before 'replaceByCopy' */
    EXPECT_EQ(metaDataMemBeforeCopy, ht.getMetadataMemory());
    EXPECT_EQ(datatypeCountsBeforeCopy, ht.getDatatypeCounts());
    EXPECT_EQ(cacheSizeBeforeCopy, ht.getCacheSize());
    EXPECT_EQ(memSizeBeforeCopy, ht.getItemMemory());
    EXPECT_EQ(statsCurrSizeBeforeCopy, global_stats.getCurrentSize());
}

/* Test copying a deleted element in HT */
TEST_F(HashTableTest, CopyDeletedItem) {
    /* Setup with 2 hash buckets and 1 lock. Note: Copying is allowed only on
       OrderedStoredValues and hence hash table must have
       OrderedStoredValueFactory */
    HashTable ht(global_stats, makeFactory(true), 2, 1);

    /* Write 3 items */
    const int numItems = 3;
    auto keys = generateKeys(numItems);
    storeMany(ht, keys);

    /* Delete a StoredValue */
    StoredDocKey copyKey = makeStoredDocKey(std::string(std::to_string(0)));
    auto replace = ht.findForWrite(copyKey);

    ht.unlocked_softDelete(replace.lock,
                           *replace.storedValue,
                           /* onlyMarkDeleted */ false,
                           DeleteSource::Explicit);
    EXPECT_EQ(numItems, ht.getNumItems());
    const int expNumDeletedItems = 1;
    EXPECT_EQ(expNumDeletedItems, ht.getNumDeletedItems());

    /* Record some stats before 'replace by copy' */
    auto metaDataMemBeforeCopy = ht.getMetadataMemory();
    auto datatypeCountsBeforeCopy = ht.getDatatypeCounts();
    auto cacheSizeBeforeCopy = ht.getCacheSize();
    auto memSizeBeforeCopy = ht.getItemMemory();
    auto statsCurrSizeBeforeCopy = global_stats.getCurrentSize();

    /* Replace the StoredValue in the HT by its copy */
    auto res = ht.unlocked_replaceByCopy(replace.lock, *replace.storedValue);

    /* Validate the copied contents */
    EXPECT_EQ(*replace.storedValue, *(res.first));

    /* Validate the HT element replace (hence released from HT) */
    EXPECT_EQ(replace.storedValue, res.second.get().get());

    /* Validate the HT count */
    EXPECT_EQ(numItems, ht.getNumItems());
    EXPECT_EQ(expNumDeletedItems, ht.getNumDeletedItems());

    /* Stats should be equal to that before 'replaceByCopy' */
    EXPECT_EQ(metaDataMemBeforeCopy, ht.getMetadataMemory());
    EXPECT_EQ(datatypeCountsBeforeCopy, ht.getDatatypeCounts());
    EXPECT_EQ(cacheSizeBeforeCopy, ht.getCacheSize());
    EXPECT_EQ(memSizeBeforeCopy, ht.getItemMemory());
    EXPECT_EQ(statsCurrSizeBeforeCopy, global_stats.getCurrentSize());
}

// Check that an OSV which was deleted and then made alive again has the
// lock expiry correctly reset (lock_expiry is stored in the same place as
// deleted time).
TEST_F(HashTableTest, LockAfterDelete) {
    /* Setup OSVFactory with 2 hash buckets and 1 lock. */
    HashTable ht(global_stats, makeFactory(true), 2, 1);

    // Delete a key, giving it a non-zero delete time.
    auto key = makeStoredDocKey("key");
    StoredValue* sv;
    store(ht, key);
    {
        auto toDelete = ht.findForWrite(key);
        sv = toDelete.storedValue;
        TimeTraveller toTheFuture(1985);
        ASSERT_EQ(DeletionStatus::Success,
                  ht.unlocked_softDelete(toDelete.lock,
                                         *sv,
                                         /* onlyMarkDeleted */ false,
                                         DeleteSource::Explicit)
                          .status);
    }
    ASSERT_EQ(1, ht.getNumItems());
    ASSERT_EQ(1, ht.getNumDeletedItems());

    // Sanity check - deleted time should be set.
    auto* osv = sv->toOrderedStoredValue();
    ASSERT_GE(osv->getCompletedOrDeletedTime(), ep_abs_time(1985));

    // Now re-create the same key (as alive).
    Item i(key, 0, 0, key.data(), key.size());
    EXPECT_EQ(MutationStatus::WasDirty, ht.set(i));
    EXPECT_FALSE(sv->isLocked(1985));
}

// Check that pauseResumeVisit calls with the correct Hash bucket.
TEST_F(HashTableTest, PauseResumeHashBucket) {
    // Two buckets, one lock.
    HashTable ht(global_stats, makeFactory(true), 2, 1);

    // Store keys to both hash buckets - need keys which hash to bucket 0 and 1.
    StoredDocKey key0("c", CollectionID::Default);
    ASSERT_EQ(0, ht.getLockedBucket(key0).getBucketNum());
    store(ht, key0);

    StoredDocKey key1("b", CollectionID::Default);
    ASSERT_EQ(1, ht.getLockedBucket(key1).getBucketNum());
    store(ht, key1);

    class MockHTVisitor : public HashTableVisitor {
    public:
        MOCK_METHOD2(visit,
                     bool(const HashTable::HashBucketLock& lh, StoredValue& v));
    } mockVisitor;

    // Visit the HashTable; should find two keys, one in each bucket and the
    // lockHolder should have the correct hashbucket.
    using namespace ::testing;
    EXPECT_CALL(mockVisitor,
                visit(Property(&HashTable::HashBucketLock::getBucketNum, 0),
                      Property(&StoredValue::getKey, Eq(key0))))
            .Times(1)
            .WillOnce(Return(true));
    EXPECT_CALL(mockVisitor,
                visit(Property(&HashTable::HashBucketLock::getBucketNum, 1),
                      Property(&StoredValue::getKey, Eq(key1))))
            .Times(1)
            .WillOnce(Return(true));

    HashTable::Position start;
    ht.pauseResumeVisit(mockVisitor, start);
}

// Test the itemFreqDecayerVisitor by adding 256 documents to the hash table.
// Then set the frequency count of each document in the range 0 to 255.  We
// then visit each document and decay it by 50%.  The test checks that the
// frequency count of each document has been decayed by 50%.
TEST_F(HashTableTest, ItemFreqDecayerVisitorTest) {
    HashTable ht(global_stats, makeFactory(true), 128, 1);
    auto keys = generateKeys(256);
    // Add 256 documents to the hash table
    storeMany(ht, keys);
    StoredValue* v;

    // Set the frequency count of each document in the range 0 to 255.
    for (int ii = 0; ii < 256; ii++) {
        auto key = makeStoredDocKey(std::to_string(ii));
        v = ht.findForWrite(key).storedValue;
        v->setFreqCounterValue(ii);
       }

       ItemFreqDecayerVisitor itemFreqDecayerVisitor(
               Configuration().getItemFreqDecayerPercent());
       // Decay the frequency count of each document by the configuration
       // default of 50%
       for (int ii = 0; ii < 256; ii++) {
           auto key = makeStoredDocKey(std::to_string(ii));
           auto item = ht.findForWrite(key);
           itemFreqDecayerVisitor.visit(item.lock, *item.storedValue);
       }

       // Confirm we visited all the documents
       EXPECT_EQ(256, itemFreqDecayerVisitor.getVisitedCount());

       // Check the frequency of the docs have been decayed by 50%.
       for (int ii = 0; ii < 256; ii++) {
           auto key = makeStoredDocKey(std::to_string(ii));
           v = ht.findForWrite(key).storedValue;
           uint16_t expectVal = ii * 0.5;
           EXPECT_EQ(expectVal, v->getFreqCounterValue());
       }
}

// Test the reallocateStoredValue method.
// Check it can reallocate and also ignores bogus input
TEST_F(HashTableTest, reallocateStoredValue) {
    // 3 hash-tables, 2 will be filled, the other left empty
    HashTable ht1(global_stats, makeFactory(true), 1, 1);
    HashTable ht2(global_stats, makeFactory(true), 2, 2);
    HashTable ht3(global_stats, makeFactory(true), 2, 2);

    // Fill 1 and 2
    auto keys = generateKeys(10);
    storeMany(ht1, keys);
    storeMany(ht2, keys);

    // Test we can reallocate every key
    for (int index = 0; index < 10; index++) {
        StoredValue* v =
                ht1.findForWrite(makeStoredDocKey(std::to_string(index)),
                                 WantsDeleted::No)
                        .storedValue;
        ASSERT_NE(nullptr, v);
        EXPECT_TRUE(ht1.reallocateStoredValue(std::forward<StoredValue>(*v)));
        EXPECT_EQ(10, ht1.getNumItems());
        EXPECT_NE(v,
                  ht1.findForWrite(makeStoredDocKey(std::to_string(index)),
                                   WantsDeleted::No)
                          .storedValue);
    }
    // Next request ht2 reallocate an sv stored in ht1, should return false
    StoredValue* v2 = ht1.findForWrite(makeStoredDocKey(std::to_string(3)),
                                       WantsDeleted::No)
                              .storedValue;
    EXPECT_FALSE(ht2.reallocateStoredValue(std::forward<StoredValue>(*v2)));

    // Check the empty hash-table returns false as well
    EXPECT_FALSE(ht3.reallocateStoredValue(std::forward<StoredValue>(*v2)));
}

// MB-33944: Test that calling HashTable::insertFromWarmup() doesn't fail
// of the given key is already resident (for example if a BG load already
// loaded it).
TEST_F(HashTableTest, InsertFromWarmupAlreadyResident) {
    // Setup - store a key into HashTable.
    HashTable ht(global_stats, makeFactory(), 5, 1);
    auto key = makeStoredDocKey("key");
    auto item = store(ht, key);
    {
        // Sanity check - key should be resident.
        auto res = ht.findForRead(key);
        ASSERT_TRUE(res.storedValue);
        ASSERT_TRUE(res.storedValue->isResident());
    }

    // Test - insertFromWarmup should succesfully skip (re)adding this item.
    EXPECT_EQ(MutationStatus::NotFound,
              ht.insertFromWarmup(item,
                                  /*eject*/ false,
                                  /*keyMetaOnly*/ false,
                                  EvictionPolicy::Full));
}

TEST_F(HashTableTest, ReplaceValueAndDatatype) {
    HashTable ht(global_stats, makeFactory(), 1, 1);

    // Store a deleted item
    StoredDocKey key = makeStoredDocKey("key");
    std::string val = "initialVal";
    Item item(key, 0, 0, val.c_str(), val.size());
    item.setDataType(PROTOCOL_BINARY_DATATYPE_JSON);
    ASSERT_EQ(MutationStatus::WasClean, ht.set(item));

    // Verify preconditions
    const auto findRes = ht.findForWrite(key);
    auto* sv = findRes.storedValue;
    ASSERT_EQ(val,
              std::string(sv->getValue()->getData(),
                          sv->getValue()->valueSize()));
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, sv->getDatatype());
    ASSERT_FALSE(sv->isDeleted());

    const auto initialSeqno = sv->getBySeqno();
    const auto initialCAS = sv->getCas();
    const auto initialRevSeqno = sv->getRevSeqno();
    const auto initialFreqCounter = sv->getFreqCounterValue();
    const auto initialAge = sv->getAge();
    const auto initialCommittedState = sv->getCommitted();

    // Replace value and datatype
    val = "newVal";
    std::unique_ptr<Blob> newVal(Blob::New(val.data(), val.size()));
    auto res = ht.unlocked_replaceValueAndDatatype(
            findRes.lock, *sv, std::move(newVal), PROTOCOL_BINARY_RAW_BYTES);
    EXPECT_EQ(MutationStatus::WasDirty, res.status);
    ASSERT_EQ(sv, res.storedValue);

    // Verify that value and datatype have changed, other properties unchanged
    EXPECT_EQ(val,
              std::string(sv->getValue()->getData(),
                          sv->getValue()->valueSize()));
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, sv->getDatatype());
    EXPECT_FALSE(sv->isDeleted());
    EXPECT_EQ(initialSeqno, sv->getBySeqno());
    EXPECT_EQ(initialCAS, sv->getCas());
    EXPECT_EQ(initialRevSeqno, sv->getRevSeqno());
    EXPECT_EQ(initialFreqCounter, sv->getFreqCounterValue());
    EXPECT_EQ(initialAge, sv->getAge());
    EXPECT_EQ(initialCommittedState, sv->getCommitted());
}

class GetRandomHashTable : public HashTable {
public:
    GetRandomHashTable(EPStats& st,
                       std::unique_ptr<AbstractStoredValueFactory> svFactory,
                       size_t initialSize,
                       size_t locks)
        : HashTable(st, std::move(svFactory), initialSize, locks) {
    }

    void testIntialiseVisitor(size_t size, int random) {
        HashTable::RandomKeyVisitor visitor{size, random};
        EXPECT_LT(visitor.getNextBucket(), size);
    }

    void testVisitor(size_t size, int random) {
        HashTable::RandomKeyVisitor visitor{size, random};
        EXPECT_FALSE(visitor.visitComplete());
        EXPECT_FALSE(visitor.maybeReset(size));

        boost::dynamic_bitset buckets(size);
        size_t itr = 0;
        const auto expectedIterations = size;
        while (!visitor.visitComplete()) {
            buckets.set(visitor.getNextBucket());
            ++itr;
        }
        EXPECT_EQ(expectedIterations, itr);
        EXPECT_TRUE(buckets.all());
    }

    // Simulate a resize by constructing with size=769 and then later calling
    // maybeReset with a smaller size. A second visitor covers a case where
    // the table grows
    void testVisitorResize() {
        HashTable::RandomKeyVisitor visitor1{769, 400};
        EXPECT_EQ(400, visitor1.getNextBucket());
        EXPECT_TRUE(visitor1.maybeReset(50));
        EXPECT_EQ(0, visitor1.getNextBucket());

        HashTable::RandomKeyVisitor visitor2{383, 50};
        EXPECT_EQ(50, visitor2.getNextBucket());
        EXPECT_TRUE(visitor2.maybeReset(769));
        EXPECT_EQ(50, visitor2.getNextBucket());
    }

    void testResizeMB_49454() {
        auto keys = generateKeys(1000);
        storeMany(*this, keys);
        resize();
        auto size = getSize();

        // Get rid of many keys so the resize will shrink
        keys.resize(950);
        for (const auto& key : keys) {
            HashTableTest::del(*this, key);
        }

        // Initialise the visitor so it computes a start point very close to the
        // end a place which definitely does not exist after the resize
        HashTable::RandomKeyVisitor visitor{getSize(), int(size - 10)};

        // Ensure if resize is called after the visitor is constructed
        // getRandomKey does not generate any faults
        resize();

        // Table must now be smaller
        EXPECT_GT(size, getSize());
        // And our bucket exceeds getSize (with some space for increments)
        EXPECT_EQ(visitor.getNextBucket(), size - 10);

        // Now call getRandomKey with our visitor that was configured with the
        // larger size
        EXPECT_NE(nullptr, getRandomKey(CollectionID::Default, visitor));
    }

    void testResizeLarger() {
        auto keys = generateKeys(100);
        storeMany(*this, keys);
        resize();
        auto size = getSize();

        // Initialise the visitor so it computes a start point very close to the
        // end a place which definitely does not exist after the resize
        HashTable::RandomKeyVisitor visitor{getSize(), int(size - 10)};

        // Increase the keys in the HT
        keys = generateKeys(500, 100 /*start key*/);
        storeMany(*this, keys);

        // Ensure if resize is called after the visitor is constructed
        // getRandomKey does not generate any faults
        resize();

        // Table must now be larger
        EXPECT_LT(size, getSize());

        // Now call getRandomKey with our visitor that was configured with the
        // smaller size
        EXPECT_NE(nullptr, getRandomKey(CollectionID::Default, visitor));
    }
};

class GetRandomHashTableTest : public HashTableTest {
public:
    GetRandomHashTable h{global_stats, makeFactory(), 1, 1};
};

TEST_F(GetRandomHashTableTest, TestInitRandomKeyVisitor) {
    h.testIntialiseVisitor(1, std::numeric_limits<int>::max());
    h.testIntialiseVisitor(1, std::numeric_limits<int>::max() / 2);
    h.testIntialiseVisitor(1, 0);
    h.testIntialiseVisitor(1, std::numeric_limits<int>::min());
}

TEST_F(GetRandomHashTableTest, TestRandomKeyVisitor) {
    // Zero size is not expected
    EXPECT_THROW(h.testVisitor(0, 0), std::invalid_argument);
    h.testVisitor(1, 0);
    h.testVisitor(769, 400);
    h.testVisitor(769, 769);
}

// This test just exercises methods of the RandomKeyVisitor class which is used
// by getRandomKey.
TEST_F(GetRandomHashTableTest, TestRandomKeyVisitorResizes) {
    h.testVisitorResize();
}

// This test covers MB-49454 - but driving getRandomKey via the protected
// parts of the code, this allows us to run any HashTable function in between
// reading size and running getRandomKey. In this case we resize the hash table
// making it smaller, which previously would of resulted in an invalid memory
// read.
TEST_F(GetRandomHashTableTest, MB_49454) {
    h.testResizeMB_49454();
}

// Similar to MB-49454, but make the table larger, which would of lead to other
// problems, but not a invalid memory access
TEST_F(GetRandomHashTableTest, TestResizeLarger) {
    h.testResizeLarger();
}
