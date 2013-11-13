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
#include <cassert>
#include <limits>

#include "threadtests.h"

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
                assert(key.compare(val->to_s()) == 0);
            }
        }
    }
private:
    bool verify;
};

static int count(HashTable &h, bool verify=true) {
    Counter c(verify);
    h.visit(c);
    assert(c.count + c.deleted == h.getNumItems());
    return c.count;
}

static void store(HashTable &h, std::string &k) {
    Item i(k, 0, 0, k.c_str(), k.length());
    assert(h.set(i) == WAS_CLEAN);
}

static void storeMany(HashTable &h, std::vector<std::string> &keys) {
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        store(h, key);
    }
}

static void addMany(HashTable &h, std::vector<std::string> &keys,
                    add_type_t expect) {
    std::vector<std::string>::iterator it;
    item_eviction_policy_t policy = VALUE_ONLY;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string k = *it;
        Item i(k, 0, 0, k.c_str(), k.length());
        add_type_t v = h.add(i, policy);
        assert(expect == v);
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

template <typename T>
void assertEquals(T a, T b) {
    if (a != b) {
        std::cerr << "Expected " << toString<T>(a)
                  << " got " << toString<T>(b) << std::endl;
        abort();
    }
}

static void add(HashTable &h, const std::string &k, add_type_t expect,
                int expiry=0) {
    Item i(k, 0, expiry, k.c_str(), k.length());
    item_eviction_policy_t policy = VALUE_ONLY;
    add_type_t v = h.add(i, policy);
    assertEquals(expect, v);
}

static std::vector<std::string> generateKeys(int num, int start=0) {
    std::vector<std::string> rv;

    for (int i = start; i < num; i++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "key%d", i);
        std::string key(buf);
        rv.push_back(key);
    }

    return rv;
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

static void testHashSize() {
    HashTable h(global_stats);
    assert(count(h) == 0);

    std::string k = "testkey";
    store(h, k);

    assert(count(h) == 1);
}

static void testHashSizeTwo() {
    HashTable h(global_stats);
    assert(count(h) == 0);

    std::vector<std::string> keys = generateKeys(5);
    storeMany(h, keys);
    assert(count(h) == 5);

    h.clear();
    assert(count(h) == 0);
}

static void testReverseDeletions() {
    alarm(10);
    size_t initialSize = global_stats.currentSize.get();
    HashTable h(global_stats, 5, 1);
    assert(count(h) == 0);
    const int nkeys = 10000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);
    assert(count(h) == nkeys);

    std::reverse(keys.begin(), keys.end());

    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        h.del(key);
    }

    assert(count(h) == 0);
    assert(global_stats.currentSize.get() == initialSize);
}

static void testForwardDeletions() {
    alarm(10);
    size_t initialSize = global_stats.currentSize.get();
    HashTable h(global_stats, 5, 1);
    assert(h.getSize() == 5);
    assert(h.getNumLocks() == 1);
    assert(count(h) == 0);
    const int nkeys = 10000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);
    assert(count(h) == nkeys);

    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        h.del(key);
    }

    assert(count(h) == 0);
    assert(global_stats.currentSize.get() == initialSize);
}

static void verifyFound(HashTable &h, const std::vector<std::string> &keys) {
    std::string missingKey = "aMissingKey";
    assert(h.find(missingKey) == NULL);

    std::vector<std::string>::const_iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        assert(h.find(key));
    }
}

static void testFind(HashTable &h) {
    const int nkeys = 5000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);

    verifyFound(h, keys);
}

static void testFind() {
    HashTable h(global_stats, 5, 1);
    testFind(h);
}

static void testAddExpiry() {
    HashTable h(global_stats, 5, 1);
    std::string k("aKey");

    add(h, k, ADD_SUCCESS, ep_real_time() + 5);
    add(h, k, ADD_EXISTS, ep_real_time() + 5);

    StoredValue *v = h.find(k);
    assert(v);
    assert(!v->isExpired(ep_real_time()));
    assert(v->isExpired(ep_real_time() + 6));

    time_offset += 6;
    assert(v->isExpired(ep_real_time()));

    add(h, k, ADD_UNDEL, ep_real_time() + 5);
    assert(v);
    assert(!v->isExpired(ep_real_time()));
    assert(v->isExpired(ep_real_time() + 6));
}

static void testResize() {
    HashTable h(global_stats, 5, 3);

    std::vector<std::string> keys = generateKeys(5000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize(6143);
    assert(h.getSize() == 6143);

    verifyFound(h, keys);

    h.resize(769);
    assert(h.getSize() == 769);

    verifyFound(h, keys);

    h.resize(static_cast<size_t>(std::numeric_limits<int>::max()) + 17);
    assert(h.getSize() == 769);

    verifyFound(h, keys);
}

class AccessGenerator : public Generator<bool> {
public:

    AccessGenerator(const std::vector<std::string> &k,
                    HashTable &h) : keys(k), ht(h), size(10000) {
        std::random_shuffle(keys.begin(), keys.end());
    }

    bool operator()() {
        std::vector<std::string>::iterator it;
        for (it = keys.begin(); it != keys.end(); ++it) {
            if (rand() % 111 == 0) {
                resize();
            }
            ht.del(*it);
        }
        return true;
    }

private:

    void resize() {
        ht.resize(size);
        size = size == 10000 ? 30000 : 10000;
    }

    std::vector<std::string>  keys;
    HashTable                &ht;
    size_t                    size;
};

static void testConcurrentAccessResize() {
    HashTable h(global_stats, 5, 3);

    std::vector<std::string> keys = generateKeys(20000);
    h.resize(keys.size());
    storeMany(h, keys);

    verifyFound(h, keys);

    srand(918475);
    AccessGenerator gen(keys, h);
    getCompletedThreads(16, &gen);
}

static void testAutoResize() {
    HashTable h(global_stats, 5, 3);

    std::vector<std::string> keys = generateKeys(5000);
    storeMany(h, keys);

    verifyFound(h, keys);

    h.resize();
    assert(h.getSize() == 6143);
    verifyFound(h, keys);
}

static void testAdd() {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 5000;

    std::vector<std::string> keys = generateKeys(nkeys);
    addMany(h, keys, ADD_SUCCESS);

    std::string missingKey = "aMissingKey";
    assert(h.find(missingKey) == NULL);

    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        assert(h.find(key));
    }

    addMany(h, keys, ADD_EXISTS);
    for (it = keys.begin(); it != keys.end(); ++it) {
        std::string key = *it;
        assert(h.find(key));
    }

    // Verify we can readd after a soft deletion.
    assert(h.softDelete(keys[0], 0) == WAS_DIRTY);
    assert(h.softDelete(keys[0], 0) == NOT_FOUND);
    assert(!h.find(keys[0]));
    assert(count(h) == nkeys - 1);

    Item i(keys[0], 0, 0, "newtest", 7);
    item_eviction_policy_t policy = VALUE_ONLY;
    assert(h.add(i, policy) == ADD_UNDEL);
    assert(count(h, false) == nkeys);
}

static void testDepthCounting() {
    HashTable h(global_stats, 5, 1);
    const int nkeys = 5000;

    std::vector<std::string> keys = generateKeys(nkeys);
    storeMany(h, keys);

    HashTableDepthStatVisitor depthCounter;
    h.visitDepth(depthCounter);
    // std::cout << "Max depth:  " << depthCounter.maxDepth << std::endl;
    assert(depthCounter.max > 1000);
}

static void testPoisonKey() {
    std::string k("A\\NROBs_oc)$zqJ1C.9?XU}Vn^(LW\"`+K/4lykF[ue0{ram;fvId6h=p&Zb3T~SQ]82'ixDP");

    HashTable h(global_stats, 5, 1);

    store(h, k);
    assert(count(h) == 1);
}

static void testSizeStats() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    ht.del(k);

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

static void testSizeStatsFlush() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    ht.clear();

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

static void testSizeStatsSoftDel() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    assert(ht.softDelete(k, 0) == WAS_DIRTY);
    ht.del(k);

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

static void testSizeStatsSoftDelFlush() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    assert(ht.softDelete(k, 0) == WAS_DIRTY);
    ht.clear();

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

static void testSizeStatsEject() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    std::string kstring(k);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(kstring));
    assert(v);
    v->markClean();
    assert(ht.unlocked_ejectItem(v, policy));

    ht.del(k);

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

static void testSizeStatsEjectFlush() {
    global_stats.reset();
    HashTable ht(global_stats, 5, 1);
    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    size_t initialSize = global_stats.currentSize.get();

    const char *k("somekey");
    std::string kstring(k);
    const size_t itemSize(16 * 1024);
    char *someval(static_cast<char*>(calloc(1, itemSize)));
    assert(someval);

    Item i(k, 0, 0, someval, itemSize);

    assert(ht.set(i) == WAS_CLEAN);

    item_eviction_policy_t policy = VALUE_ONLY;
    StoredValue *v(ht.find(kstring));
    assert(v);
    v->markClean();
    assert(ht.unlocked_ejectItem(v, policy));

    ht.clear();

    assert(ht.memSize.get() == 0);
    assert(ht.cacheSize.get() == 0);
    assert(initialSize == global_stats.currentSize.get());

    free(someval);
}

int main() {
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));
    global_stats.setMaxDataSize(64*1024*1024);
    HashTable::setDefaultNumBuckets(3);
    alarm(60);
    testHashSize();
    testHashSizeTwo();
    testReverseDeletions();
    testForwardDeletions();
    testFind();
    testAdd();
    testAddExpiry();
    testDepthCounting();
    testPoisonKey();
    testResize();
    testConcurrentAccessResize();
    testAutoResize();
    testSizeStats();
    testSizeStatsFlush();
    testSizeStatsSoftDel();
    testSizeStatsSoftDelFlush();
    testSizeStatsEject();
    testSizeStatsEjectFlush();
    exit(0);
}
