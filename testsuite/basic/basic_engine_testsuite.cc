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
#include <memcached/durability_spec.h>
#include <memcached/engine_testapp.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

struct test_harness* test_harness;

// Checks that a and b are equal; if not then assert.
#define assert_equal(a, b) assert_equal_impl((a), (b), #a, #b, __FILE__, __LINE__)

// Checkt that a >= b; if not then assert.
#define assert_ge(a, b) assert_ge_impl((a), (b), #a, #b, __FILE__, __LINE__)

template<typename T>
static void assert_equal_impl(const T& a_value, const T& b_value,
                              const char* a_name, const char* b_name,
                              const char* file, int line) {
    if (a_value != b_value) {
        std::stringstream ss;
        ss << "Check '" << a_name << " == " << b_name << "' failed - '"
           << a_value << " == " << b_value << "' at " << file << ":" << line;
        std::cerr << ss.str() << std::endl;
        abort();
    }
}

template<typename T>
static void assert_ge_impl(const T& a_value, const T& b_value,
                           const char* a_name, const char* b_name,
                           const char* file, int line) {
    if (a_value < b_value) {
        std::stringstream ss;
        ss << "Check '" << a_name << " >= " << b_name << "' failed - '"
           << a_value << " >= " << b_value << "' at " << file << ":" << line;
        std::cerr << ss.str() << std::endl;
        abort();
    }
}

/*
 * Make sure we can successfully allocate an item, allocate op returns success
 * and that item struct is populated
 */
static enum test_result allocate_test(EngineIface* h) {
    DocKey key("akey", DocKeyEncodesCollectionId::No);
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    test_harness->destroy_cookie(cookie);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(ret.second != nullptr);
    return SUCCESS;
}

/*
 * Verify set behavior
 */
static enum test_result set_test(EngineIface* h) {
    DocKey key("key", DocKeyEncodesCollectionId::No);
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);

    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        cb_assert(h->store(cookie,
                           ret.second.get(),
                           cas,
                           OPERATION_SET,
                           {},
                           DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Verify add behavior
 */
static enum test_result add_test(EngineIface* h) {
    DocKey key("key", DocKeyEncodesCollectionId::No);
    uint64_t cas;
    int ii;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);

    for (ii = 0; ii < 10; ++ii) {
        ENGINE_ERROR_CODE rv = h->store(cookie,
                                        ret.second.get(),
                                        cas,
                                        OPERATION_ADD,
                                        {},
                                        DocumentState::Alive);
        if (ii == 0) {
            cb_assert(rv == ENGINE_SUCCESS);
            cb_assert(cas != 0);
        } else {
            cb_assert(rv == ENGINE_NOT_STORED);
        }
    }
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Verify replace behavior
 */
static enum test_result replace_test(EngineIface* h) {
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    item_info item_info;

    cb_assert(set_test(h) == SUCCESS);
    DocKey key("key", DocKeyEncodesCollectionId::No);
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, sizeof(int), 1, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->get_item_info(ret.second.get(), &item_info) == true);

    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        *(int*)(item_info.value[0].iov_base) = ii;
        cb_assert(h->store(cookie,
                           ret.second.get(),
                           cas,
                           OPERATION_REPLACE,
                           {},
                           DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }

    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->get_item_info(ret.second.get(), &item_info) == true);
    cb_assert(item_info.value[0].iov_len == sizeof(int));
    cb_assert(*(int*)(item_info.value[0].iov_base) == 9);

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure when we can successfully store an item after it has been allocated
 * and that the cas for the stored item has been generated.
 */
static enum test_result store_test(EngineIface* h) {
    DocKey key("bkey", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(cas != 0);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine
 */
static enum test_result get_test(EngineIface* h) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine and then deleted.
 */
static enum test_result get_deleted_test(EngineIface* h) {
    DocKey key("get_removed_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);

    // Asking for a dead document should not find it!
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Deleted);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);

    // remove it
    mutation_descr_t mut_info;
    cb_assert(h->remove(cookie, key, cas, Vbid(0), {}, mut_info) ==
              ENGINE_SUCCESS);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);

    // But we should be able to fetch it if we ask for deleted
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Deleted);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(ret.second != nullptr);

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result expiry_test(EngineIface* h) {
    DocKey key("get_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 0, 10, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    test_harness->time_travel(11);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure that we can release an item. For the most part all this test does
 * is ensure that thinds dont go splat when we call release. It does nothing to
 * ensure that release did much of anything.
 */
static enum test_result release_test(EngineIface* h) {
    DocKey key("release_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure that we can remove an item and that after the item has been
 * removed it can not be retrieved.
 */
static enum test_result remove_test(EngineIface* h) {
    DocKey key("remove_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    mutation_descr_t mut_info;
    const auto* cookie = test_harness->create_cookie();

    auto ret = h->allocate(
            cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h->remove(cookie, key, cas, Vbid(0), {}, mut_info) ==
              ENGINE_SUCCESS);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure we can successfully perform a flush operation and that any item
 * stored before the flush can not be retrieved
 */
static enum test_result flush_test(EngineIface* h) {
    DocKey key("flush_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;

    test_harness->time_travel(3);
    const auto* cookie = test_harness->create_cookie();

    auto ret = h->allocate(
            cookie, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h->flush(cookie) == ENGINE_SUCCESS);
    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Make sure we can successfully retrieve the item info struct for an item and
 * that the contents of the item_info are as expected.
 */
static enum test_result get_item_info_test(EngineIface* h) {
    DocKey key("get_item_info_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const time_t exp = 1;
    item_info ii;
    const auto* cookie = test_harness->create_cookie();

    auto ret = h->allocate(
            cookie, key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    /* Had this been actual code, there'd be a connection here */
    cb_assert(h->get_item_info(ret.second.get(), &ii));
    assert_equal(cas, ii.cas);
    assert_equal(0u, ii.flags);
    cb_assert(strcmp(reinterpret_cast<const char*>(key.data()),
                     reinterpret_cast<const char*>(ii.key.data())) == 0);
    assert_equal(key.size(), ii.key.size());
    assert_equal(1u, ii.nbytes);
    // exptime is a rel_time_t; i.e. seconds since server started. Therefore can only
    // check that the returned value is at least as large as the value
    // we requested (i.e. not in the past).
    assert_ge(ii.exptime, exp);

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result item_set_cas_test(EngineIface* h) {
    DocKey key("item_set_cas_test_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    const rel_time_t exp = 1;
    uint64_t newcas;
    item_info ii;

    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);
    newcas = cas + 1;
    h->item_set_cas(ret.second.get(), newcas);
    cb_assert(h->get_item_info(ret.second.get(), &ii));
    cb_assert(ii.cas == newcas);
    test_harness->destroy_cookie(cookie);
    return SUCCESS;
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

static enum test_result lru_test(EngineIface* h) {
    DocKey hot_key("hot_key", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    int ii;
    int jj;

    const auto* cookie = test_harness->create_cookie();
    auto ret = h->allocate(
            cookie, hot_key, 4096, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);

    for (ii = 0; ii < 250; ++ii) {
        uint8_t key[1024];

        ret = h->get(cookie, hot_key, Vbid(0), DocStateFilter::Alive);
        cb_assert(ret.first == cb::engine_errc::success);
        DocKey allocate_key(key,
                            snprintf(reinterpret_cast<char*>(key),
                                     sizeof(key),
                                     "lru_test_key_%08d",
                                     ii),
                            DocKeyEncodesCollectionId::No);
        ret = h->allocate(cookie,
                          allocate_key,
                          4096,
                          0,
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          Vbid(0));
        cb_assert(ret.first == cb::engine_errc::success);
        cb_assert(h->store(cookie,
                           ret.second.get(),
                           cas,
                           OPERATION_SET,
                           {},
                           DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(h->get_stats(cookie, {}, {}, eviction_stats_handler) ==
                  ENGINE_SUCCESS);
        if (evictions == 2) {
            break;
        }
    }

    cb_assert(ii < 250);
    for (jj = 0; jj <= ii; ++jj) {
        uint8_t key[1024];
        DocKey get_key(key,
                       snprintf(reinterpret_cast<char*>(key),
                                sizeof(key),
                                "lru_test_key_%08d",
                                jj),
                       DocKeyEncodesCollectionId::No);
        if (jj == 0 || jj == 1) {
            ret = h->get(cookie, get_key, Vbid(0), DocStateFilter::Alive);
            cb_assert(ret.first == cb::engine_errc::no_such_key);
        } else {
            ret = h->get(cookie, get_key, Vbid(0), DocStateFilter::Alive);
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(ret.second != nullptr);
        }
    }

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result get_stats_test(EngineIface* h) {
    return PENDING;
}

static enum test_result reset_stats_test(EngineIface* h) {
    return PENDING;
}

static enum test_result get_stats_struct_test(EngineIface* h) {
    return PENDING;
}

static enum test_result aggregate_stats_test(EngineIface* h) {
    return PENDING;
}

static enum test_result test_datatype(EngineIface* h) {
    DocKey key("{foo:1}", DocKeyEncodesCollectionId::No);
    uint64_t cas = 0;
    item_info ii;
    memset(&ii, 0, sizeof(ii));
    const auto* cookie = test_harness->create_cookie();

    auto ret = h->allocate(cookie, key, 1, 0, 0, 1, Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h->store(cookie,
                       ret.second.get(),
                       cas,
                       OPERATION_SET,
                       {},
                       DocumentState::Alive) == ENGINE_SUCCESS);

    ret = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);

    cb_assert(h->get_item_info(ret.second.get(), &ii));
    cb_assert(ii.datatype == 1);

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Destroy many buckets - this test is really more interesting with valgrind
 *  destroy should invoke a background cleaner thread and at exit time there
 *  shall be no items left behind.
 */
static enum test_result test_n_bucket_destroy(engine_test_t *test) {
    const int n_buckets = 20;
    const int n_keys = 256;
    std::vector<std::pair<EngineIface*, EngineIface*> > buckets;
    for (int ii = 0; ii < n_buckets; ii++) {
        EngineIface* handle = test_harness->create_bucket(true, test->cfg);
        if (handle) {
            buckets.push_back(std::make_pair(
                    reinterpret_cast<EngineIface*>(handle), handle));
        } else {
            return FAIL;
        }
    }

    const auto* cookie = test_harness->create_cookie();
    for (auto bucket : buckets) {
        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, DocKeyEncodesCollectionId::No);
            auto ret = bucket.second->allocate(cookie,
                                               allocate_key,
                                               256,
                                               1,
                                               1,
                                               PROTOCOL_BINARY_RAW_BYTES,
                                               Vbid(0));
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(bucket.second->store(cookie,
                                           ret.second.get(),
                                           cas,
                                           OPERATION_SET,
                                           {},
                                           DocumentState::Alive) ==
                      ENGINE_SUCCESS);
        }
    }

    for (auto itr : buckets) {
        test_harness->destroy_bucket(itr.first, false);
    }

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * create and delete buckets, the idea being that the background deletion
 * is running whilst we're creating more buckets.
 */
static enum test_result test_bucket_destroy_interleaved(engine_test_t *test) {
    const int n_keys = 20;
    const int buckets = 5;

    const auto* cookie = test_harness->create_cookie();

    for (int b = 0; b < buckets; b++) {
        EngineIface* h1 = test_harness->create_bucket(true, test->cfg);
        auto* h = reinterpret_cast<EngineIface*>(h1);

        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, DocKeyEncodesCollectionId::No);
            auto ret = h->allocate(cookie,
                                   allocate_key,
                                   111256,
                                   1,
                                   1,
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   Vbid(0));
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(h->store(cookie,
                               ret.second.get(),
                               cas,
                               OPERATION_SET,
                               {},
                               DocumentState::Alive) == ENGINE_SUCCESS);
        }

        test_harness->destroy_bucket(h, false);
    }

    test_harness->destroy_cookie(cookie);
    return SUCCESS;
}

engine_test_t* get_tests() {
    static engine_test_t tests[]  = {
        TEST_CASE("allocate test", allocate_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("set test", set_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("add test", add_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("replace test", replace_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("store test", store_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get test", get_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get deleted test", get_deleted_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("expiry test", expiry_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("remove test", remove_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("release test", release_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("flush test", flush_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get item info test", get_item_info_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("set cas test", item_set_cas_test, NULL, NULL, NULL, NULL, NULL),
#ifndef VALGRIND
        // this test is disabled for VALGRIND because cache_size=48 and using malloc don't work.
        TEST_CASE("LRU test", lru_test, NULL, NULL, "cache_size=48", NULL, NULL),
#endif
        TEST_CASE("get stats test", get_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("reset stats test", reset_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get stats struct test", get_stats_struct_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("aggregate stats test", aggregate_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("Test datatype", test_datatype, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE_V2("Bucket destroy", test_n_bucket_destroy, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE_V2("Bucket destroy interleaved", test_bucket_destroy_interleaved, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    };
    return tests;
}

bool setup_suite(struct test_harness *th) {
    test_harness = th;
    return true;
}

bool teardown_suite() {
    return true;
}
