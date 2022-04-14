/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "engine_testsuite.h"

#include <daemon/enginemap.h>
#include <folly/portability/GTest.h>
#include <logger/logger.h>
#include <memcached/collections.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_engine.h>
#include <programs/engine_testapp/mock_server.h>
#include <vector>

using namespace std::string_view_literals;

class BasicEngineTestsuite : public EngineTestsuite {
protected:
    void SetUp() override {
        EngineTestsuite::SetUp();
        engine = createBucket(BucketType::Memcached, {});
        cookie = std::make_unique<MockCookie>();
    }

    void TearDown() override {
        EngineTestsuite::TearDown();
    }

    static cb::EngineErrorItemPair allocateItem(EngineIface& engine,
                                                const CookieIface& cookie,
                                                const DocKey& key,
                                                size_t nbytes,
                                                int flags,
                                                rel_time_t exptime,
                                                uint8_t datatype,
                                                Vbid vbucket) {
        try {
            auto pair = engine.allocateItem(cookie,
                                            key,
                                            nbytes,
                                            0, // No privileged bytes
                                            flags,
                                            exptime,
                                            datatype,
                                            vbucket);
            return {cb::engine_errc::success, std::move(pair.first)};
        } catch (const cb::engine_error& error) {
            return cb::makeEngineErrorItemPair(
                    cb::engine_errc(error.code().value()));
        }
    }

    /**
     * Perform a set operation of a document named "key" in the engine.
     * When the document is stored we'll try to overwrite it
     * by using a CAS operation (and make sure that the CAS value change)
     */
    void setDocument() {
        DocKey key("key", DocKeyEncodesCollectionId::No);
        auto ret = allocateItem(*engine,
                                *cookie,
                                key,
                                1,
                                1,
                                0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                Vbid(0));
        ASSERT_EQ(cb::engine_errc::success, ret.first);

        // Verify that we can store it with CAS = 0 (override)
        uint64_t cas = 0;
        ASSERT_EQ(cb::engine_errc::success,
                  engine->store(*cookie,
                                *ret.second.get(),
                                cas,
                                StoreSemantics::Set,
                                {},
                                DocumentState::Alive,
                                false));
        ASSERT_NE(0, cas);
        const auto prev_cas = cas;

        // Verify that CAS replace works
        ASSERT_EQ(cb::engine_errc::success,
                  engine->store(*cookie,
                                *ret.second.get(),
                                cas,
                                StoreSemantics::Set,
                                {},
                                DocumentState::Alive,
                                false));
        ASSERT_NE(prev_cas, cas);
    }

    std::unique_ptr<EngineIface> engine;
    std::unique_ptr<MockCookie> cookie;
};

/*
 * Make sure we can successfully allocate an item, allocate op returns success
 * and that item struct is populated
 */
TEST_F(BasicEngineTestsuite, Allocate) {
    DocKey key("akey", DocKeyEncodesCollectionId::No);
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_TRUE(ret.second);
}

/*
 * Verify set behavior
 */
TEST_F(BasicEngineTestsuite, Set) {
    setDocument();
}

/*
 * Verify add behavior
 */
TEST_F(BasicEngineTestsuite, Add) {
    DocKey key("key", DocKeyEncodesCollectionId::No);
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    for (int ii = 0; ii < 10; ++ii) {
        uint64_t cas;
        auto rv = engine->store(*cookie,
                                *ret.second.get(),
                                cas,
                                StoreSemantics::Add,
                                {},
                                DocumentState::Alive,
                                false);
        if (ii == 0) {
            ASSERT_EQ(cb::engine_errc::success, rv);
            ASSERT_NE(0, cas);
        } else {
            ASSERT_EQ(cb::engine_errc::not_stored, rv);
        }
    }
}

/*
 * Verify replace behavior
 */
TEST_F(BasicEngineTestsuite, Replace) {
    setDocument();

    DocKey key("key", DocKeyEncodesCollectionId::No);
    auto ret = allocateItem(*engine,
                            *cookie,
                            key,
                            sizeof(int),
                            1,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    item_info item_info;
    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), item_info));

    uint64_t prev_cas;
    uint64_t cas = 0;
    for (int ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        *(int*)(item_info.value[0].iov_base) = ii;
        ASSERT_EQ(cb::engine_errc::success,
                  engine->store(*cookie,
                                *ret.second.get(),
                                cas,
                                StoreSemantics::Replace,
                                {},
                                DocumentState::Alive,
                                false));
        ASSERT_NE(prev_cas, cas);
    }

    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), item_info));
    ASSERT_EQ(sizeof(int), item_info.value[0].iov_len);
    ASSERT_EQ(9, *reinterpret_cast<int*>(item_info.value[0].iov_base));
}

/*
 * Make sure when we can successfully store an item after it has been allocated
 * and that the cas for the stored item has been generated.
 */
TEST_F(BasicEngineTestsuite, Store) {
    DocKey key("bkey", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    ASSERT_NE(0, cas);
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine
 */
TEST_F(BasicEngineTestsuite, Get) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine and then deleted.
 */
TEST_F(BasicEngineTestsuite, GetDeleted) {
    DocKey key("get_removed_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::success, ret.first);

    // Asking for a dead document should not find it!
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Deleted);
    EXPECT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);

    // remove it
    mutation_descr_t mut_info;
    ASSERT_EQ(cb::engine_errc::success,
              engine->remove(*cookie, key, cas, Vbid(0), {}, mut_info));
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);

    // But we should be able to fetch it if we ask for deleted
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Deleted);
    EXPECT_EQ(cb::engine_errc::success, ret.first);
    EXPECT_TRUE(ret.second);
}

TEST_F(BasicEngineTestsuite, Expiry) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = allocateItem(*engine,
                            *cookie,
                            key,
                            1,
                            0,
                            10,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    mock_time_travel(11);
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::no_such_key, ret.first);
}

/*
 * Make sure that we can release an item. For the most part all this test does
 * is ensure that thinds dont go splat when we call release. It does nothing to
 * ensure that release did much of anything.
 */
TEST_F(BasicEngineTestsuite, Release) {
    DocKey key("release_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = allocateItem(
            *engine, *cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
}

/*
 * Make sure that we can remove an item and that after the item has been
 * removed it can not be retrieved.
 */
TEST_F(BasicEngineTestsuite, Remove) {
    DocKey key("remove_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    mutation_descr_t mut_info;

    auto ret = allocateItem(
            *engine, *cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    ASSERT_EQ(cb::engine_errc::success,
              engine->remove(*cookie, key, cas, Vbid(0), {}, mut_info));
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);
}

/*
 * Make sure we can successfully perform a flush operation and that any item
 * stored before the flush can not be retrieved
 */
TEST_F(BasicEngineTestsuite, Flush) {
    DocKey key("flush_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;

    mock_time_travel(3);

    auto ret = allocateItem(
            *engine, *cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    ASSERT_EQ(cb::engine_errc::success, engine->flush(*cookie));
    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);
}

/*
 * Make sure we can successfully retrieve the item info struct for an item and
 * that the contents of the item_info are as expected.
 */
TEST_F(BasicEngineTestsuite, GetItemInfo) {
    DocKey key("get_item_info_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const time_t exp = 1;
    item_info ii;

    auto ret = allocateItem(*engine,
                            *cookie,
                            key,
                            1,
                            0,
                            exp,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    /* Had this been actual code, there'd be a connection here */
    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), ii));
    ASSERT_EQ(cas, ii.cas);
    ASSERT_EQ(0u, ii.flags);
    ASSERT_STREQ(reinterpret_cast<const char*>(key.data()),
                 reinterpret_cast<const char*>(ii.key.data()));
    ASSERT_EQ(key.size(), ii.key.size());
    ASSERT_EQ(1u, ii.nbytes);
    // exptime is a rel_time_t; i.e. seconds since server started. Therefore can only
    // check that the returned value is at least as large as the value
    // we requested (i.e. not in the past).
    ASSERT_GE(ii.exptime, exp);
}

TEST_F(BasicEngineTestsuite, ItemSetCas) {
    DocKey key("item_set_cas_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const rel_time_t exp = 1;
    uint64_t newcas;
    item_info ii;

    auto ret = allocateItem(*engine,
                            *cookie,
                            key,
                            1,
                            0,
                            exp,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));
    newcas = cas + 1;
    engine->item_set_cas(*ret.second.get(), newcas);
    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), ii));
    ASSERT_EQ(ii.cas, newcas);
}

uint32_t evictions;
static void eviction_stats_handler(std::string_view key,
                                   std::string_view value,
                                   const void*) {
    if (key == "evictions"sv) {
        std::string v{value.data(), value.size()};
        evictions = atoi(v.c_str());
    }
}

TEST_F(BasicEngineTestsuite, LRU) {
    engine = createBucket(BucketType::Memcached, "cache_size=48");
    DocKey hot_key("hot_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    int ii;
    int jj;

    auto ret = allocateItem(*engine,
                            *cookie,
                            hot_key,
                            4096,
                            0,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));

    for (ii = 0; ii < 250; ++ii) {
        uint8_t key[1024];

        ret = engine->get(*cookie, hot_key, Vbid(0), DocStateFilter::Alive);
        ASSERT_EQ(cb::engine_errc::success, ret.first);
        DocKey allocate_key(key,
                            snprintf(reinterpret_cast<char*>(key),
                                     sizeof(key),
                                     "lru_test_key_%08d",
                                     ii),
                            DocKeyEncodesCollectionId::No);
        ret = allocateItem(*engine,
                           *cookie,
                           allocate_key,
                           4096,
                           0,
                           0,
                           PROTOCOL_BINARY_RAW_BYTES,
                           Vbid(0));
        ASSERT_EQ(cb::engine_errc::success, ret.first);
        ASSERT_EQ(cb::engine_errc::success,
                  engine->store(*cookie,
                                *ret.second.get(),
                                cas,
                                StoreSemantics::Set,
                                {},
                                DocumentState::Alive,
                                false));
        ASSERT_EQ(cb::engine_errc::success,
                  engine->get_stats(*cookie, {}, {}, eviction_stats_handler));
        if (evictions == 2) {
            break;
        }
    }

    ASSERT_LT(ii, 250);
    for (jj = 0; jj <= ii; ++jj) {
        uint8_t key[1024];
        DocKey get_key(key,
                       snprintf(reinterpret_cast<char*>(key),
                                sizeof(key),
                                "lru_test_key_%08d",
                                jj),
                       DocKeyEncodesCollectionId::No);
        if (jj == 0 || jj == 1) {
            ret = engine->get(*cookie, get_key, Vbid(0), DocStateFilter::Alive);
            ASSERT_EQ(cb::engine_errc::no_such_key, ret.first);
        } else {
            ret = engine->get(*cookie, get_key, Vbid(0), DocStateFilter::Alive);
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_TRUE(ret.second);
        }
    }

}

TEST_F(BasicEngineTestsuite, Datatype) {
    DocKey key("{foo:1}", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    item_info ii;

    auto ret = allocateItem(*engine, *cookie, key, 1, 0, 0, 1, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));

    ret = engine->get(*cookie, key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), ii));
    ASSERT_EQ(1, ii.datatype);
}

/*
 * Destroy many buckets - this test is really more interesting with valgrind
 *  destroy should invoke a background cleaner thread and at exit time there
 *  shall be no items left behind.
 */
TEST_F(BasicEngineTestsuite, test_n_bucket_destroy) {
    const int n_buckets = 20;
    const int n_keys = 256;
    std::vector<std::unique_ptr<EngineIface>> buckets;
    for (int ii = 0; ii < n_buckets; ii++) {
        buckets.emplace_back(createBucket(BucketType::Memcached, {}));
    }

    for (auto& bucket : buckets) {
        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, DocKeyEncodesCollectionId::No);
            auto ret = allocateItem(*bucket,
                                    *cookie,
                                    allocate_key,
                                    256,
                                    1,
                                    1,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    Vbid(0));
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_EQ(cb::engine_errc::success,
                      bucket->store(*cookie,
                                    *ret.second.get(),
                                    cas,
                                    StoreSemantics::Set,
                                    {},
                                    DocumentState::Alive,
                                    false));
        }
    }

    // Invoke all of the bucket deletions!
    buckets.clear();
}

/*
 * create and delete buckets, the idea being that the background deletion
 * is running whilst we're creating more buckets.
 */
TEST_F(BasicEngineTestsuite, test_bucket_destroy_interleaved) {
    const int n_keys = 20;
    const int buckets = 5;

    for (int b = 0; b < buckets; b++) {
        auto bucket = createBucket(BucketType::Memcached, {});

        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, DocKeyEncodesCollectionId::No);
            auto ret = allocateItem(*bucket,
                                    *cookie,
                                    allocate_key,
                                    111256,
                                    1,
                                    1,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    Vbid(0));
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_EQ(cb::engine_errc::success,
                      bucket->store(*cookie,
                                    *ret.second.get(),
                                    cas,
                                    StoreSemantics::Set,
                                    {},
                                    DocumentState::Alive,
                                    false));
        }
    }
}

// Only the default collection is supported on memcache buckets
class CollectionsTest : public BasicEngineTestsuite {
public:
    void SetUp() override {
        BasicEngineTestsuite::SetUp();
    }

    void storeTest(const DocKey& a, const DocKey& b);

    void removeTest(const DocKey& a, const DocKey& b);

protected:
    std::array<uint8_t, 3> k1 = {{'k', 'e', 'y'}};
    std::array<uint8_t, 4> k2 = {{0, 'k', 'e', 'y'}};
    std::array<uint8_t, 4> k3 = {{8, 'k', 'e', 'y'}};
    // key1 and key2 are logically the same, both 'key' in default collection
    DocKey key1{k1.data(), k1.size(), DocKeyEncodesCollectionId::No};
    DocKey key2{k2.data(), k2.size(), DocKeyEncodesCollectionId::Yes};
    // key 3 is in collection 8
    DocKey key3{k3.data(), k3.size(), DocKeyEncodesCollectionId::Yes};
};

// Test that allocate allows default (key1/key2) but errors on key3, this will
// mean most store paths will not work because we cannot allocate an item to
// store/replace
TEST_F(CollectionsTest, Allocate) {
    auto ret = allocateItem(*engine,
                            *cookie,
                            key1,
                            1,
                            1,
                            1,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, ret.first);
    EXPECT_TRUE(ret.second);

    ret = allocateItem(*engine,
                       *cookie,
                       key2,
                       1,
                       1,
                       1,
                       PROTOCOL_BINARY_RAW_BYTES,
                       Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, ret.first);
    EXPECT_TRUE(ret.second);

    ret = allocateItem(*engine,
                       *cookie,
                       key3,
                       1,
                       1,
                       1,
                       PROTOCOL_BINARY_RAW_BYTES,
                       Vbid(0));
    EXPECT_EQ(cb::engine_errc::unknown_collection, ret.first);
    EXPECT_FALSE(ret.second);
}

// Store using a and get with b
void CollectionsTest::storeTest(const DocKey& keyA, const DocKey& keyB) {
    // Test only makes sense if the keys are logically the same
    ASSERT_EQ(keyA.hash(), keyB.hash());

    auto ret = allocateItem(*engine,
                            *cookie,
                            keyA,
                            32,
                            0xcafef00d,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, ret.first);
    EXPECT_TRUE(ret.second);

    uint64_t cas = 0;
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *ret.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));

    ret = engine->get(*cookie, keyB, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    item_info item_info;
    ASSERT_TRUE(engine->get_item_info(*ret.second.get(), item_info));
    EXPECT_EQ(0xcafef00d, item_info.flags);
}

TEST_F(CollectionsTest, Store1) {
    // Expect store(key1) get(key2) to work
    storeTest(key1, key2);
}

TEST_F(CollectionsTest, Store2) {
    // Expect store(key2) get(key1) to work
    storeTest(key2, key1);
}

TEST_F(CollectionsTest, Store3) {
    // Expect store(key2) get(key1) to work
    storeTest(key2, key1);
    // and be told to go away if a get with key3 was attempted
    auto ret = engine->get(*cookie, key3, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::unknown_collection, ret.first);
}

// Store using a and remove with b
void CollectionsTest::removeTest(const DocKey& keyA, const DocKey& keyB) {
    // Test only makes sense if the keys are logically the same
    ASSERT_EQ(keyA.hash(), keyB.hash());

    auto allocRes = allocateItem(*engine,
                                 *cookie,
                                 keyA,
                                 32,
                                 0xcafef00d,
                                 0,
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 Vbid(0));
    EXPECT_EQ(cb::engine_errc::success, allocRes.first);
    EXPECT_TRUE(allocRes.second);

    uint64_t cas = 0;
    ASSERT_EQ(cb::engine_errc::success,
              engine->store(*cookie,
                            *allocRes.second.get(),
                            cas,
                            StoreSemantics::Set,
                            {},
                            DocumentState::Alive,
                            false));

    mutation_descr_t mut_info;
    cas = 0;
    auto ret = engine->remove(*cookie, keyB, cas, Vbid(0), {}, mut_info);
    ASSERT_EQ(cb::engine_errc::success, ret);

    // It's gone
    auto getResult = engine->get(*cookie, keyA, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::no_such_key, getResult.first);

    // It's still gone
    getResult = engine->get(*cookie, keyB, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::no_such_key, getResult.first);
}

TEST_F(CollectionsTest, RemoveSuccess1) {
    removeTest(key1, key2);
}

TEST_F(CollectionsTest, RemoveSuccess2) {
    removeTest(key2, key1);
}

TEST_F(CollectionsTest, RemoveError) {
    storeTest(key1, key2);

    // key3 denied
    uint64_t cas = 0;
    mutation_descr_t mut_info;
    auto ret = engine->remove(*cookie, key3, cas, Vbid(0), {}, mut_info);
    ASSERT_EQ(cb::engine_errc::unknown_collection, ret);

    // Still here
    auto getResult = engine->get(*cookie, key1, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, getResult.first);
}

TEST_F(CollectionsTest, GetFromUnknownCollection) {
    auto getVal = engine->get_if(*cookie, key3, Vbid(0), {});
    EXPECT_EQ(cb::engine_errc::unknown_collection, getVal.first);
    getVal = engine->get_and_touch(*cookie, key3, Vbid(0), 0, {});
    EXPECT_EQ(cb::engine_errc::unknown_collection, getVal.first);
    getVal = engine->get_locked(*cookie, key3, Vbid(0), 0);
    EXPECT_EQ(cb::engine_errc::unknown_collection, getVal.first);
    auto getMetaVal = engine->get_meta(*cookie, key3, Vbid(0));
    EXPECT_EQ(cb::engine_errc::unknown_collection, getMetaVal.first);
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->unlock(*cookie, key3, Vbid(0), 0));
}

// Can only ever get to the default collection
TEST_F(CollectionsTest, CollectionIDLookup) {
    auto rv = engine->get_collection_id(*cookie, ".");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(CollectionID::Default, rv.getCollectionId());

    rv = engine->get_collection_id(*cookie, "_default.");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(CollectionID::Default, rv.getCollectionId());

    rv = engine->get_collection_id(*cookie, "._default");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(CollectionID::Default, rv.getCollectionId());

    rv = engine->get_collection_id(*cookie, "_default._default");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(CollectionID::Default, rv.getCollectionId());

    rv = engine->get_collection_id(*cookie, "..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = engine->get_collection_id(*cookie, "_default.._default");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    rv = engine->get_collection_id(*cookie, "unknown.");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
}

// Can only ever get to the default scope
TEST_F(CollectionsTest, ScopeIDLookup) {
    auto rv = engine->get_scope_id(*cookie, "");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(ScopeID(ScopeID::Default), rv.getScopeId());

    rv = engine->get_scope_id(*cookie, ".");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(ScopeID(ScopeID::Default), rv.getScopeId());

    rv = engine->get_scope_id(*cookie, "_default");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(ScopeID(ScopeID::Default), rv.getScopeId());

    rv = engine->get_scope_id(*cookie, "blah");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    EXPECT_EQ(0, rv.getManifestId());

    rv = engine->get_scope_id(*cookie, "blah.");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    EXPECT_EQ(0, rv.getManifestId());

    rv = engine->get_scope_id(*cookie, "blah..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    rv = engine->get_scope_id(*cookie, "..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
}

// Can only ever get to the default scope (DocKey lookup)
TEST_F(CollectionsTest, ScopeIDLookup2) {
    auto rv = engine->get_scope_id(*cookie, key1.getCollectionID());
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(ScopeID(ScopeID::Default), rv.getScopeId());

    rv = engine->get_scope_id(*cookie, key2.getCollectionID());
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
    EXPECT_EQ(ScopeID(ScopeID::Default), rv.getScopeId());

    rv = engine->get_scope_id(*cookie, key3.getCollectionID());
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);
    EXPECT_EQ(0, rv.getManifestId());
}

// Cannot change collections
TEST_F(CollectionsTest, SetGetCollections) {
    EXPECT_EQ(cb::engine_errc::not_supported,
              engine->set_collection_manifest(*cookie, "{}"));

    std::string returnedData;
    cb::mcbp::Status returnedStatus;

    AddResponseFn addResponseFunc = [&returnedData, &returnedStatus](
                                            std::string_view key,
                                            std::string_view extras,
                                            std::string_view body,
                                            uint8_t datatype,
                                            cb::mcbp::Status status,
                                            uint64_t cas,
                                            const void* cookie) -> bool {
        std::copy(body.begin(), body.end(), std::back_inserter(returnedData));
        returnedStatus = status;
        return true;
    };

    std::string defaultManifest = R"(
        {
          "uid": "0",
          "scopes": [
            {
              "name": "_default",
              "uid": "0",
              "collections": [
                {
                  "name": "_default",
                  "uid": "0"
                }
              ]
            }
          ]
        })";

    EXPECT_EQ(cb::engine_errc::success,
              engine->get_collection_manifest(*cookie, addResponseFunc));
    auto defaultManifestJson = nlohmann::json::parse(defaultManifest);
    auto returnedJson = nlohmann::json::parse(returnedData);
    EXPECT_EQ(defaultManifestJson, returnedJson);
}
