/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include <item.h>
#include <kv_bucket.h>
#include <platform/cb_malloc.h>
#include <signal.h>
#include <stats.h>

#include <algorithm>
#include <limits>

#include "makestoreddockey.h"
#include "threadtests.h"

#include <gtest/gtest.h>

#ifdef _MSC_VER
#define alarm(a)
#endif

time_t time_offset;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL) + time_offset;
    }
}

EPStats global_stats;

class Counter : public HashTableVisitor {
public:

    size_t count;
    size_t deleted;

    Counter(bool v) : count(), deleted(), verify(v) {}

    void visit(StoredValue *v) {
        if (v->isDeleted()) {
            ++deleted;
        } else {
            ++count;
            if (verify) {
                StoredDocKey key(v->getKey());
                value_t val = v->getValue();
                EXPECT_STREQ(key.c_str(), val->to_s().c_str());
            }
        }
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

static void store(HashTable &h, const StoredDocKey& k) {
    Item i(k, 0, 0, k.data(), k.size());
    EXPECT_EQ(WAS_CLEAN, h.set(i));
}

static void storeMany(HashTable &h, std::vector<StoredDocKey> &keys) {
    for (const auto& key : keys) {
        store(h, key);
    }
}

static void addMany(HashTable &h, std::vector<StoredDocKey> &keys,
                    add_type_t expect) {
    item_eviction_policy_t policy = VALUE_ONLY;
    for (const auto& k : keys) {
        Item i(k, 0, 0, k.data(), k.size());
        add_type_t v = h.add(i, policy);
        EXPECT_EQ(expect, v);
    }
}

template <typename T>
static const char *toString(add_type_t a) {
    switch(a) {
    case ADD_SUCCESS: return "add_success";
    case ADD_NOMEM: return "add_nomem";
    case ADD_EXISTS: return "add_exists";
    case ADD_UNDEL: return "add_undel";
    case ADD_TMP_AND_BG_FETCH: return "add_tmp_and_bg_fetch";
    case ADD_BG_FETCH: return "add_bg_fetch";
    }
    abort();
    return NULL;
}

static void add(HashTable &h, const StoredDocKey& k, add_type_t expect,
                int expiry=0) {
    Item i(k, 0, expiry, k.data(), k.size());
    item_eviction_policy_t policy = VALUE_ONLY;
    add_type_t v = h.add(i, policy);
    EXPECT_EQ(expect, v);
}

static std::vector<StoredDocKey> generateKeys(int num, int start=0) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

// Test fixture for HashTable tests.
class HashTableTest : public ::testing::Test {
protected:
    HashTableTest() {
        // Default per-test timeout
        alarm(30);
    }
};

TEST_F(HashTableTest, Size) {
    HashTable h(global_stats, /*size*/0, /*locks*/1);
    ASSERT_EQ(0, count(h));

    store(h, makeStoredDocKey("testkey"));

    EXPECT_EQ(1, count(h));
}

TEST_F(HashTableTest, SizeTwo) {
    HashTable h(global_stats, /*size*/0, /*locks*/1);
    ASSERT_EQ(0, count(h));

    auto keys = generateKeys(5);
    storeMany(h, keys);
    EXPECT_EQ(5, count(h));

    h.clear();
    EXPECT_EQ(0, count(h));
}

TEST_F(HashTableTest, ReverseDeletions) {
    size_t initialSize = global_stats.currentSize.load();
    HashTable h(global_stats, 5, 1);
    ASSERT_EQ(0, count(h));
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);
    EXPECT_EQ(nkeys, count(h));

    std::reverse(keys.begin(), keys.end());

    for (const auto& key : keys) {
        h.del(key);
    }

    EXPECT_EQ(0, count(h));
    EXPECT_EQ(initialSize, global_stats.currentSize.load());
}

TEST_F(HashTableTest, ForwardDeletions) {
    size_t initialSize = global_stats.currentSize.load();
    HashTable h(global_stats, 5, 1);
    ASSERT_EQ(5, h.getSize());
    ASSERT_EQ(1, h.getNumLocks());
    ASSERT_EQ(0, count(h));
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);
    EXPECT_EQ(nkeys, count(h));

    for (const auto& key : keys) {
        h.del(key);
    }

    EXPECT_EQ(0, count(h));
    EXPECT_EQ(initialSize, global_stats.currentSize.load());
}

static void verifyFound(HashTable &h, const std::vector<StoredDocKey> &keys) {
    EXPECT_FALSE(h.find(makeStoredDocKey("aMissingKey")));

    for (const auto& key : keys) {
        EXPECT_TRUE(h.find(key));
    }
}

static void verifyValue(HashTable& ht, StoredDocKey& key, const char* value,
                        bool trackReference, bool wantDeleted) {
    StoredValue* v = ht.find(key, trackReference, wantDeleted);
    EXPECT_NE(nullptr, v);
    value_t val = v->getValue();
    if (!value) {
        EXPECT_EQ(nullptr, val.get());
    } else {
        EXPECT_STREQ(value, val->to_s().c_str());
    }
}

static void testFind(HashTable &h) {
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);

    verifyFound(h, keys);
}

TEST_F(HashTableTest, Find) {
    HashTable h(global_stats, 5, 1);
    testFind(h);
}

TEST_F(HashTableTest, AddExpiry) {
    HashTable h(global_stats, 5, 1);
    StoredDocKey k = makeStoredDocKey("aKey");

    add(h, k, ADD_SUCCESS, ep_real_time() + 5);
    add(h, k, ADD_EXISTS, ep_real_time() + 5);

    StoredValue *v = h.find(k);
    EXPECT_TRUE(v);
    EXPECT_FALSE(v->isExpired(ep_real_time()));
    EXPECT_TRUE(v->isExpired(ep_real_time() + 6));

    time_offset += 6;
    EXPECT_TRUE(v->isExpired(ep_real_time()));

    add(h, k, ADD_UNDEL, ep_real_time() + 5);
    EXPECT_TRUE(v);
    EXPECT_FALSE(v->isExpired(ep_real_time()));
    EXPECT_TRUE(v->isExpired(ep_real_time() + 6));
}

TEST_F(HashTableTest, Resize) {
    HashTable h(global_stats, 5, 3);

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

    AccessGenerator(const std::vector<StoredDocKey> &k,
                    HashTable &h) : keys(k), ht(h), size(10000) {
        std::random_shuffle(keys.begin(), keys.end());
    }

    bool operator()() {
        for (const auto& key : keys) {
            if (rand() % 111 == 0) {
                resize();
            }
            ht.del(key);
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
    HashTable h(global_stats, 5, 3);

    auto keys = generateKeys(2000);
    h.resize(keys.size());
    storeMany(h, keys);

    verifyFound(h, keys);

    srand(918475);
    AccessGenerator gen(keys, h);
    getCompletedThreads(4, &gen);
}

TEST_F(HashTableTest, AutoResize) {
    HashTable h(global_stats, 5, 3);

    ASSERT_EQ(5, h.getSize());

    auto keys = generateKeys(1000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize();
    EXPECT_EQ(769, h.getSize());
    verifyFound(h, keys);
}

TEST_F(HashTableTest, Add) {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 1000;

    auto keys = generateKeys(nkeys);
    addMany(h, keys, ADD_SUCCESS);

    StoredDocKey missingKey = makeStoredDocKey("aMissingKey");
    EXPECT_FALSE(h.find(missingKey));

    for (const auto& key : keys) {
        EXPECT_TRUE(h.find(key));
    }

    addMany(h, keys, ADD_EXISTS);
    for (const auto& key : keys) {
        EXPECT_TRUE(h.find(key));
    }

    // Verify we can read after a soft deletion.
    EXPECT_EQ(WAS_DIRTY, h.softDelete(keys[0], 0));
    EXPECT_EQ(NOT_FOUND, h.softDelete(keys[0], 0));
    EXPECT_FALSE(h.find(keys[0]));
    EXPECT_EQ(nkeys - 1, count(h));

    Item i(keys[0], 0, 0, "newtest", 7);
    item_eviction_policy_t policy = VALUE_ONLY;
    EXPECT_EQ(ADD_UNDEL, h.add(i, policy));
    EXPECT_EQ(nkeys, count(h, false));
}

TEST_F(HashTableTest, DepthCounting) {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 5000;

    auto keys = generateKeys(nkeys);
    storeMany(h, keys);

    HashTableDepthStatVisitor depthCounter;
    h.visitDepth(depthCounter);
    // std::cout << "Max depth:  " << depthCounter.maxDepth << std::endl;
    EXPECT_GT(depthCounter.max, 1000);
}

TEST_F(HashTableTest, PoisonKey) {
    HashTable h(global_stats, 5, 1);

    store(h, makeStoredDocKey("A\\NROBs_oc)$zqJ1C.9?XU}Vn^(LW\"`+K/4lykF[ue0{ram;fvId6h=p&Zb3T~SQ]82'ixDP"));
    EXPECT_EQ(1, count(h));
}

TEST_F(HashTableTest, SizeStats) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    ht.del(k);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, SizeStatsFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, SizeStatsSoftDel) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    EXPECT_EQ(WAS_DIRTY, ht.softDelete(k, 0));
    ht.del(k);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, SizeStatsSoftDelFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    StoredDocKey k = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    EXPECT_EQ(WAS_DIRTY, ht.softDelete(k, 0));
    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, SizeStatsEject) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    StoredDocKey key = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(key, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(key));
    EXPECT_TRUE(v);
    v->markClean();
    EXPECT_TRUE(ht.unlocked_ejectItem(v, policy));

    ht.del(key);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, SizeStatsEjectFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    StoredDocKey key = makeStoredDocKey("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(cb_calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(key, 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(key));
    EXPECT_TRUE(v);
    v->markClean();
    EXPECT_TRUE(ht.unlocked_ejectItem(v, policy));

    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    cb_free(someval);
}

TEST_F(HashTableTest, ItemAge) {
    // Setup
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");
    Item item(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(WAS_CLEAN, ht.set(item));

    // Test
    StoredValue* v(ht.find(key));
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
    v->setValue(item2, ht, false);
    EXPECT_EQ(1, v->getValue()->getAge());
}

// Check not specifying results in the INITIAL_NRU_VALUE.
TEST_F(HashTableTest, NRUDefault) {
    // Setup
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");

    Item item(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(WAS_CLEAN, ht.set(item));

    // trackReferenced=false so we don't modify the NRU while validating it.
    StoredValue* v(ht.find(key, /*trackReference*/false));
    EXPECT_NE(nullptr, v);
    EXPECT_EQ(INITIAL_NRU_VALUE, v->getNRUValue());

    // Check that find() by default /does/ update NRU.
    v = ht.find(key);
    EXPECT_NE(nullptr, v);
    EXPECT_EQ(INITIAL_NRU_VALUE - 1, v->getNRUValue());
}

// Check a specific NRU value (minimum)
TEST_F(HashTableTest, NRUMinimum) {
    // Setup
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");

    Item item(key, 0, 0, "value", strlen("value"));
    item.setNRUValue(MIN_NRU_VALUE);
    EXPECT_EQ(WAS_CLEAN, ht.set(item));

    // trackReferenced=false so we don't modify the NRU while validating it.
    StoredValue* v(ht.find(key,/*trackReference*/false));
    EXPECT_NE(nullptr, v);
    EXPECT_EQ(MIN_NRU_VALUE, v->getNRUValue());
}

/** Regression test for MB-21448 - if an attempt is made to perform a CAS
 *  operation on a logically deleted item we should return NOT_FOUND
 *  (aka KEY_ENOENT) and *not* INVALID_CAS (aka KEY_EEXISTS).
 */
TEST_F(HashTableTest, MB21448_UnlockedSetWithCASDeleted) {
    // Setup - create a key and then delete it.
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");
    Item item(key, 0, 0, "deleted", strlen("deleted"));
    ASSERT_EQ(WAS_CLEAN, ht.set(item));
    ASSERT_EQ(WAS_DIRTY, ht.softDelete(key, 0));

    // Attempt to perform a set on a deleted key with a CAS.
    Item replacement(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(NOT_FOUND,
              ht.set(replacement, /*cas*/10, /*allowExisting*/true,
                     /*hasMetaData*/false))
        << "When trying to replace-with-CAS a deleted item";
}

/** Test to check if an unlocked_softDelete performed on an
 *  existing item with a new value results in a success
 */
TEST_F(HashTableTest, unlockedSoftDeleteWithValue) {
    // Setup - create a key and then delete it with a value.
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0 , 0, "value", strlen("value"));
    ASSERT_EQ(WAS_CLEAN, ht.set(stored_item));

    StoredValue* v(ht.find(key, /*trackReference*/false));
    EXPECT_NE(nullptr, v);

    // Create an item and set its state to deleted
    Item deleted_item(key, 0 , 0, "deletedvalue",
                      strlen("deletedvalue"));
    deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(deleted_item, ht, true);

    ItemMetaData itm_meta;
    EXPECT_EQ(WAS_DIRTY,ht.unlocked_softDelete(v, 0, itm_meta, VALUE_ONLY));
    verifyValue(ht, key, "deletedvalue",/*trackReference*/true, /*wantsDeleted*/true);
}

/** Test to check if an unlocked_softDelete performed on a
 *  deleted item without a value and with a value
 */
TEST_F(HashTableTest, updateDeletedItem) {
    // Setup - create a key and then delete it.
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");
    Item stored_item(key, 0 , 0, "value", strlen("value"));
    ASSERT_EQ(WAS_CLEAN, ht.set(stored_item));

    StoredValue* v(ht.find(key, /*trackReference*/false));
    EXPECT_NE(nullptr, v);

    ItemMetaData itm_meta;
    EXPECT_EQ(WAS_DIRTY, ht.unlocked_softDelete(v, 0, itm_meta, VALUE_ONLY));
    verifyValue(ht, key, nullptr,/*trackReference*/true, /*wantsDeleted*/true);

    Item deleted_item(key, 0, 0, "deletedvalue", strlen("deletedvalue"));
    deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(deleted_item, ht, true);

    EXPECT_EQ(WAS_DIRTY,ht.unlocked_softDelete(v, 0, itm_meta, VALUE_ONLY));
    verifyValue(ht, key, "deletedvalue", /*trackReference*/true, /*wantDeleted*/true);

    Item update_deleted_item(key, 0, 0, "updatedeletedvalue",
                             strlen("updatedeletedvalue"));
    update_deleted_item.setDeleted();

    // Set a new deleted value
    v->setValue(update_deleted_item, ht, true);

    EXPECT_EQ(WAS_DIRTY,ht.unlocked_softDelete(v, 0, itm_meta, VALUE_ONLY));
    verifyValue(ht, key, "updatedeletedvalue",/*trackReference*/true,
                /*wantsDeleted*/true);
}

/* static storage for environment variable set by putenv().
 *
 * (This must be static as putenv() essentially 'takes ownership' of
 * the provided array, so it is unsafe to use an automatic variable.
 * However, if we use the result of cb_malloc() (i.e. the heap) then
 * memory leak checkers (e.g. Valgrind) will report the memory as
 * leaked as it's impossible to free it).
 */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    putenv(allow_no_stats_env);

    global_stats.setMaxDataSize(64*1024*1024);
    HashTable::setDefaultNumBuckets(3);
    return RUN_ALL_TESTS();
}
