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

#include <ep.h>
#include <item.h>
#include <signal.h>
#include <stats.h>

#include <algorithm>
#include <limits>

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
                std::string key = v->getKey();
                value_t val = v->getValue();
                EXPECT_EQ(0, key.compare(val->to_s()));
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

static void store(HashTable &h, const std::string &k) {
    Item i(k.data(), k.length(), 0, 0, k.c_str(), k.length());
    EXPECT_EQ(WAS_CLEAN, h.set(i));
}

static void storeMany(HashTable &h, std::vector<std::string> &keys) {
    for (const auto& key : keys) {
        store(h, key);
    }
}

static void addMany(HashTable &h, std::vector<std::string> &keys,
                    add_type_t expect) {
    item_eviction_policy_t policy = VALUE_ONLY;
    for (const auto& k : keys) {
        Item i(k.data(), k.length(), 0, 0, k.c_str(), k.length());
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

static void add(HashTable &h, const std::string &k, add_type_t expect,
                int expiry=0) {
    Item i(k.data(), k.length(), 0, expiry, k.c_str(), k.length());
    item_eviction_policy_t policy = VALUE_ONLY;
    add_type_t v = h.add(i, policy);
    EXPECT_EQ(expect, v);
}

static std::vector<std::string> generateKeys(int num, int start=0) {
    std::vector<std::string> rv;

    for (int i = start; i < num; i++) {
        rv.push_back(std::to_string(i));
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

    std::string k = "testkey";
    store(h, k);

    EXPECT_EQ(1, count(h));
}

TEST_F(HashTableTest, SizeTwo) {
    HashTable h(global_stats, /*size*/0, /*locks*/1);
    ASSERT_EQ(0, count(h));

    std::vector<std::string> keys = generateKeys(5);
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

    std::vector<std::string> keys = generateKeys(nkeys);
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
    alarm(20);
    size_t initialSize = global_stats.currentSize.load();
    HashTable h(global_stats, 5, 1);
    ASSERT_EQ(5, h.getSize());
    ASSERT_EQ(1, h.getNumLocks());
    ASSERT_EQ(0, count(h));
    const int nkeys = 1000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);
    EXPECT_EQ(nkeys, count(h));

    for (const auto& key : keys) {
        h.del(key);
    }

    EXPECT_EQ(0, count(h));
    EXPECT_EQ(initialSize, global_stats.currentSize.load());
}

static void verifyFound(HashTable &h, const std::vector<std::string> &keys) {
    std::string missingKey = "aMissingKey";
    EXPECT_FALSE(h.find(missingKey));

    for (const auto& key : keys) {
        EXPECT_TRUE(h.find(key));
    }
}

static void testFind(HashTable &h) {
    const int nkeys = 1000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);

    verifyFound(h, keys);
}

TEST_F(HashTableTest, Find) {
    HashTable h(global_stats, 5, 1);
    testFind(h);
}

TEST_F(HashTableTest, AddExpiry) {
    HashTable h(global_stats, 5, 1);
    std::string k("aKey");

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

    std::vector<std::string> keys = generateKeys(1000);
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

    AccessGenerator(const std::vector<std::string> &k,
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

    std::vector<std::string>  keys;
    HashTable                &ht;
    std::atomic<size_t>       size;
};

TEST_F(HashTableTest, ConcurrentAccessResize) {
    HashTable h(global_stats, 5, 3);

    std::vector<std::string> keys = generateKeys(2000);
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

    std::vector<std::string> keys = generateKeys(1000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize();
    EXPECT_EQ(769, h.getSize());
    verifyFound(h, keys);
}

TEST_F(HashTableTest, Add) {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 1000;

    std::vector<std::string> keys = generateKeys(nkeys);
    addMany(h, keys, ADD_SUCCESS);

    std::string missingKey = "aMissingKey";
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

    Item i(keys[0].data(), keys[0].length(), 0, 0, "newtest", 7);
    item_eviction_policy_t policy = VALUE_ONLY;
    EXPECT_EQ(ADD_UNDEL, h.add(i, policy));
    EXPECT_EQ(nkeys, count(h, false));
}

TEST_F(HashTableTest, DepthCounting) {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 5000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);

    HashTableDepthStatVisitor depthCounter;
    h.visitDepth(depthCounter);
    // std::cout << "Max depth:  " << depthCounter.maxDepth << std::endl;
    EXPECT_GT(depthCounter.max, 1000);
}

TEST_F(HashTableTest, PoisonKey) {
    std::string k("A\\NROBs_oc)$zqJ1C.9?XU}Vn^(LW\"`+K/4lykF[ue0{ram;fvId6h=p&Zb3T~SQ]82'ixDP");

    HashTable h(global_stats, 5, 1);

    store(h, k);
    EXPECT_EQ(1, count(h));
}

TEST_F(HashTableTest, SizeStats) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    ht.del(k);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, SizeStatsFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, SizeStatsSoftDel) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    EXPECT_EQ(WAS_DIRTY, ht.softDelete(k, 0));
    ht.del(k);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, SizeStatsSoftDelFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    EXPECT_EQ(WAS_DIRTY, ht.softDelete(k, 0));
    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, SizeStatsEject) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    std::string kstring(k);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(kstring));
    EXPECT_TRUE(v);
    v->markClean();
    EXPECT_TRUE(ht.unlocked_ejectItem(v, policy));

    ht.del(k);

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, SizeStatsEjectFlush) {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    ASSERT_EQ(0, ht.memSize.load());
    ASSERT_EQ(0, ht.cacheSize.load());
    size_t initialSize = global_stats.currentSize.load();

    const std::string k("somekey");
    std::string kstring(k);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    EXPECT_TRUE(someval);

    Item i(k.data(), k.length(), 0, 0, someval, itemSize);

    EXPECT_EQ(WAS_CLEAN, ht.set(i));

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(kstring));
    EXPECT_TRUE(v);
    v->markClean();
    EXPECT_TRUE(ht.unlocked_ejectItem(v, policy));

    ht.clear();

    EXPECT_EQ(0, ht.memSize.load());
    EXPECT_EQ(0, ht.cacheSize.load());
    EXPECT_EQ(initialSize, global_stats.currentSize.load());

    free(someval);
}

TEST_F(HashTableTest, ItemAge) {
    // Setup
    HashTable ht(global_stats, 5, 1);
    std::string key("key");
    Item item(key.data(), key.length(), 0, 0, "value", strlen("value"));
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
    Item item2(key.data(), key.length(), 0, 0, "value2", strlen("value2"));
    item2.getValue()->incrementAge();
    v->setValue(item2, ht, false);
    EXPECT_EQ(1, v->getValue()->getAge());
}

/* static storage for environment variable set by putenv().
 *
 * (This must be static as putenv() essentially 'takes ownership' of
 * the provided array, so it is unsafe to use an automatic variable.
 * However, if we use the result of malloc() (i.e. the heap) then
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
