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

#include "programs/engine_testapp/mock_server.h"

#include <gtest/gtest.h>

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
    EXPECT_EQ(MutationStatus::WasClean, h.set(i));
}

static void storeMany(HashTable &h, std::vector<StoredDocKey> &keys) {
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
    return NULL;
}

static std::vector<StoredDocKey> generateKeys(int num, int start=0) {
    std::vector<StoredDocKey> rv;

    for (int i = start; i < num; i++) {
        rv.push_back(makeStoredDocKey(std::to_string(i)));
    }

    return rv;
}

/**
 * Delete the item with the given key.
 *
 * @param key the storage key of the value to delete
 * @return true if the item existed before this call
 */
static bool del(HashTable& ht, const DocKey& key) {
    int bucket_num(0);
    std::unique_lock<std::mutex> lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = ht.unlocked_find(key,
                                      bucket_num,
                                      /*wantsDeleted*/ true,
                                      /*trackReference*/ false);
    if (!v) {
        return false;
    }
    ht.unlocked_del(lh, key, bucket_num);
    return true;
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

// Test fixture for HashTable tests.
class HashTableTest : public ::testing::Test {
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
        del(h, key);
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
        del(h, key);
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
            del(ht, key);
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

    EXPECT_EQ(MutationStatus::WasClean, ht.set(i));

    del(ht, k);

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

    EXPECT_EQ(MutationStatus::WasClean, ht.set(i));

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

    EXPECT_EQ(MutationStatus::WasClean, ht.set(i));

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(key));
    EXPECT_TRUE(v);
    v->markClean();
    EXPECT_TRUE(ht.unlocked_ejectItem(v, policy));

    del(ht, key);

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

    EXPECT_EQ(MutationStatus::WasClean, ht.set(i));

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
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

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
    v->setValue(item2, ht);
    EXPECT_EQ(1, v->getValue()->getAge());
}

// Check not specifying results in the INITIAL_NRU_VALUE.
TEST_F(HashTableTest, NRUDefault) {
    // Setup
    HashTable ht(global_stats, 5, 1);
    StoredDocKey key = makeStoredDocKey("key");

    Item item(key, 0, 0, "value", strlen("value"));
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

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
    EXPECT_EQ(MutationStatus::WasClean, ht.set(item));

    // trackReferenced=false so we don't modify the NRU while validating it.
    StoredValue* v(ht.find(key,/*trackReference*/false));
    EXPECT_NE(nullptr, v);
    EXPECT_EQ(MIN_NRU_VALUE, v->getNRUValue());
}
