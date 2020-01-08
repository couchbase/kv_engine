/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "engine_testsuite.h"

#include <daemon/enginemap.h>
#include <folly/portability/GTest.h>
#include <logger/logger.h>
#include <memcached/durability_spec.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_engine.h>
#include <programs/engine_testapp/mock_server.h>
#include <vector>

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

    /**
     * Perform a set operation of a document named "key" in the engine.
     * When the document is stored we'll try to overwrite it
     * by using a CAS operation (and make sure that the CAS value change)
     */
    void setDocument() {
        DocKey key("key", DocKeyEncodesCollectionId::No);
        auto ret = engine->allocate(
                cookie.get(), key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
        ASSERT_EQ(cb::engine_errc::success, ret.first);

        // Verify that we can store it with CAS = 0 (override)
        uint64_t cas = 0;
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->store(cookie.get(),
                                ret.second.get(),
                                cas,
                                OPERATION_SET,
                                {},
                                DocumentState::Alive));
        ASSERT_NE(0, cas);
        const auto prev_cas = cas;

        // Verify that CAS replace works
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->store(cookie.get(),
                                ret.second.get(),
                                cas,
                                OPERATION_SET,
                                {},
                                DocumentState::Alive));
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
    auto ret = engine->allocate(
            cookie.get(), key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
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
    auto ret = engine->allocate(
            cookie.get(), key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    for (int ii = 0; ii < 10; ++ii) {
        uint64_t cas;
        auto rv = engine->store(cookie.get(),
                                ret.second.get(),
                                cas,
                                OPERATION_ADD,
                                {},
                                DocumentState::Alive);
        if (ii == 0) {
            ASSERT_EQ(ENGINE_SUCCESS, rv);
            ASSERT_NE(0, cas);
        } else {
            ASSERT_EQ(ENGINE_NOT_STORED, rv);
        }
    }
}

/*
 * Verify replace behavior
 */
TEST_F(BasicEngineTestsuite, Replace) {
    setDocument();

    DocKey key("key", DocKeyEncodesCollectionId::No);
    auto ret = engine->allocate(cookie.get(),
                                key,
                                sizeof(int),
                                1,
                                0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    item_info item_info;
    ASSERT_TRUE(engine->get_item_info(ret.second.get(), &item_info));

    uint64_t prev_cas;
    uint64_t cas = 0;
    for (int ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        *(int*)(item_info.value[0].iov_base) = ii;
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->store(cookie.get(),
                                ret.second.get(),
                                cas,
                                OPERATION_REPLACE,
                                {},
                                DocumentState::Alive));
        ASSERT_NE(prev_cas, cas);
    }

    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_TRUE(engine->get_item_info(ret.second.get(), &item_info));
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
    auto ret = engine->allocate(
            cookie.get(), key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    ASSERT_NE(0, cas);
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine
 */
TEST_F(BasicEngineTestsuite, Get) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine and then deleted.
 */
TEST_F(BasicEngineTestsuite, GetDeleted) {
    DocKey key("get_removed_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::success, ret.first);

    // Asking for a dead document should not find it!
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Deleted);
    EXPECT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);

    // remove it
    mutation_descr_t mut_info;
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->remove(cookie.get(), key, cas, Vbid(0), {}, mut_info));
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::no_such_key, ret.first);
    EXPECT_FALSE(ret.second);

    // But we should be able to fetch it if we ask for deleted
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Deleted);
    EXPECT_EQ(cb::engine_errc::success, ret.first);
    EXPECT_TRUE(ret.second);
}

TEST_F(BasicEngineTestsuite, Expiry) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 10, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    mock_time_travel(11);
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
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
    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
}

/*
 * Make sure that we can remove an item and that after the item has been
 * removed it can not be retrieved.
 */
TEST_F(BasicEngineTestsuite, Remove) {
    DocKey key("remove_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    mutation_descr_t mut_info;

    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->remove(cookie.get(), key, cas, Vbid(0), {}, mut_info));
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
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

    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    ASSERT_EQ(ENGINE_SUCCESS, engine->flush(cookie.get()));
    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
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

    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    /* Had this been actual code, there'd be a connection here */
    ASSERT_TRUE(engine->get_item_info(ret.second.get(), &ii));
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

    auto ret = engine->allocate(
            cookie.get(), key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));
    newcas = cas + 1;
    engine->item_set_cas(ret.second.get(), newcas);
    ASSERT_TRUE(engine->get_item_info(ret.second.get(), &ii));
    ASSERT_EQ(ii.cas, newcas);
}

uint32_t evictions;
static void eviction_stats_handler(cb::const_char_buffer key,
                                   cb::const_char_buffer value,
                                   gsl::not_null<const void*>) {
    if (key == "evictions"_ccb) {
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

    auto ret = engine->allocate(cookie.get(),
                                hot_key,
                                4096,
                                0,
                                0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));

    for (ii = 0; ii < 250; ++ii) {
        uint8_t key[1024];

        ret = engine->get(
                cookie.get(), hot_key, Vbid(0), DocStateFilter::Alive);
        ASSERT_EQ(cb::engine_errc::success, ret.first);
        DocKey allocate_key(key,
                            snprintf(reinterpret_cast<char*>(key),
                                     sizeof(key),
                                     "lru_test_key_%08d",
                                     ii),
                            DocKeyEncodesCollectionId::No);
        ret = engine->allocate(cookie.get(),
                               allocate_key,
                               4096,
                               0,
                               0,
                               PROTOCOL_BINARY_RAW_BYTES,
                               Vbid(0));
        ASSERT_EQ(cb::engine_errc::success, ret.first);
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->store(cookie.get(),
                                ret.second.get(),
                                cas,
                                OPERATION_SET,
                                {},
                                DocumentState::Alive));
        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->get_stats(
                          cookie.get(), {}, {}, eviction_stats_handler));
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
            ret = engine->get(
                    cookie.get(), get_key, Vbid(0), DocStateFilter::Alive);
            ASSERT_EQ(cb::engine_errc::no_such_key, ret.first);
        } else {
            ret = engine->get(
                    cookie.get(), get_key, Vbid(0), DocStateFilter::Alive);
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_TRUE(ret.second);
        }
    }

}

TEST_F(BasicEngineTestsuite, Datatype) {
    DocKey key("{foo:1}", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    item_info ii;
    memset(&ii, 0, sizeof(ii));

    auto ret = engine->allocate(cookie.get(), key, 1, 0, 0, 1, Vbid(0));
    ASSERT_EQ(cb::engine_errc::success, ret.first);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->store(cookie.get(),
                            ret.second.get(),
                            cas,
                            OPERATION_SET,
                            {},
                            DocumentState::Alive));

    ret = engine->get(cookie.get(), key, Vbid(0), DocStateFilter::Alive);
    ASSERT_EQ(cb::engine_errc::success, ret.first);

    ASSERT_TRUE(engine->get_item_info(ret.second.get(), &ii));
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
            auto ret = bucket->allocate(cookie.get(),
                                        allocate_key,
                                        256,
                                        1,
                                        1,
                                        PROTOCOL_BINARY_RAW_BYTES,
                                        Vbid(0));
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_EQ(ENGINE_SUCCESS,
                      bucket->store(cookie.get(),
                                    ret.second.get(),
                                    cas,
                                    OPERATION_SET,
                                    {},
                                    DocumentState::Alive));
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
            auto ret = bucket->allocate(cookie.get(),
                                        allocate_key,
                                        111256,
                                        1,
                                        1,
                                        PROTOCOL_BINARY_RAW_BYTES,
                                        Vbid(0));
            ASSERT_EQ(cb::engine_errc::success, ret.first);
            ASSERT_EQ(ENGINE_SUCCESS,
                      bucket->store(cookie.get(),
                                    ret.second.get(),
                                    cas,
                                    OPERATION_SET,
                                    {},
                                    DocumentState::Alive));
        }
    }
}
