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

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include "basic_engine_testsuite.h"

#include <iostream>
#include <vector>
#include <sstream>

struct test_harness test_harness;


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
 * Make sure that get_info returns something and that repeated calls to it
 * return the same something.
 */
static enum test_result get_info_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    cb_assert(info != NULL);
    cb_assert(info == h1->get_info(h));
    return SUCCESS;
}

/*
 * Make sure that the structure returned by get_info has a non-null description.
 */
static enum test_result get_info_description_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    cb_assert(info->description != NULL);
    return SUCCESS;
}

/*
 * Make sure that the structure returned by get_info has a valid number of
 * features and that the size of the feautes array equals that value
 */
static enum test_result get_info_features_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    uint32_t nfeats = info->num_features;
    const feature_info *fi = info->features;
    cb_assert (nfeats > 0);
    while (nfeats-- > 0) {
        cb_assert(fi++ != NULL);
    }

    return SUCCESS;
}

/*
 * Make sure we can successfully allocate an item, allocate op returns success
 * and that item struct is populated
 */
static enum test_result allocate_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("akey", test_harness.doc_namespace);
    auto ret =
            h1->allocate(h, NULL, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(ret.second != nullptr);
    return SUCCESS;
}

/*
 * Verify set behavior
 */
static enum test_result set_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("key", test_harness.doc_namespace);
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    auto ret =
            h1->allocate(h, NULL, key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);

    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        cb_assert(h1->store(h,
                            NULL,
                            ret.second.get(),
                            &cas,
                            OPERATION_SET,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }
    return SUCCESS;
}

/*
 * Verify add behavior
 */
static enum test_result add_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("key", test_harness.doc_namespace);
    uint64_t cas;
    int ii;
    auto ret =
            h1->allocate(h, NULL, key, 1, 1, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);

    for (ii = 0; ii < 10; ++ii) {
        ENGINE_ERROR_CODE rv = h1->store(h,
                                         NULL,
                                         ret.second.get(),
                                         &cas,
                                         OPERATION_ADD,
                                         DocumentState::Alive);
        if (ii == 0) {
            cb_assert(rv == ENGINE_SUCCESS);
            cb_assert(cas != 0);
        } else {
            cb_assert(rv == ENGINE_NOT_STORED);
        }
    }
    return SUCCESS;
}

/*
 * Verify replace behavior
 */
static enum test_result replace_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    item_info item_info;

    cb_assert(set_test(h, h1) == SUCCESS);
    DocKey key("key", test_harness.doc_namespace);
    auto ret = h1->allocate(
            h, NULL, key, sizeof(int), 1, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->get_item_info(h, ret.second.get(), &item_info) == true);

    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        *(int*)(item_info.value[0].iov_base) = ii;
        cb_assert(h1->store(h,
                            NULL,
                            ret.second.get(),
                            &cas,
                            OPERATION_REPLACE,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }

    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->get_item_info(h, ret.second.get(), &item_info) == true);
    cb_assert(item_info.value[0].iov_len == sizeof(int));
    cb_assert(*(int*)(item_info.value[0].iov_base) == 9);

    return SUCCESS;
}

/*
 * Make sure when we can successfully store an item after it has been allocated
 * and that the cas for the stored item has been generated.
 */
static enum test_result store_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("bkey", test_harness.doc_namespace);
    uint64_t cas = 0;
    auto ret =
            h1->allocate(h, NULL, key, 1, 1, 1, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(cas != 0);
    return SUCCESS;
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine
 */
static enum test_result get_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("get_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    auto ret =
            h1->allocate(h, NULL, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);
    return SUCCESS;
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine and then deleted.
 */
static enum test_result get_deleted_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("get_removed_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    auto ret = h1->allocate(
            h, nullptr, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        nullptr,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    ret = h1->get(h, nullptr, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);

    // Asking for a dead document should not find it!
    ret = h1->get(h, nullptr, key, 0, DocStateFilter::Deleted);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);

    // remove it
    mutation_descr_t mut_info;
    cb_assert(h1->remove(h, nullptr, key, &cas, 0, &mut_info) == ENGINE_SUCCESS);
    ret = h1->get(h, nullptr, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);

    // But we should be able to fetch it if we ask for deleted
    ret = h1->get(h, nullptr, key, 0, DocStateFilter::Deleted);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(ret.second != nullptr);

    return SUCCESS;
}


static enum test_result expiry_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("get_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    auto ret =
            h1->allocate(h, NULL, key, 1, 0, 10, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    test_harness.time_travel(11);
    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    return SUCCESS;
}

/*
 * Make sure that we can release an item. For the most part all this test does
 * is ensure that thinds dont go splat when we call release. It does nothing to
 * ensure that release did much of anything.
 */
static enum test_result release_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("release_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    auto ret =
            h1->allocate(h, NULL, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    return SUCCESS;
}

/*
 * Make sure that we can remove an item and that after the item has been
 * removed it can not be retrieved.
 */
static enum test_result remove_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("remove_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    mutation_descr_t mut_info;

    auto ret =
            h1->allocate(h, NULL, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h1->remove(h, NULL, key, &cas, 0, &mut_info) == ENGINE_SUCCESS);
    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);
    return SUCCESS;
}

/*
 * Make sure we can successfully perform a flush operation and that any item
 * stored before the flush can not be retrieved
 */
static enum test_result flush_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("flush_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;

    test_harness.time_travel(3);
    auto ret =
            h1->allocate(h, NULL, key, 1, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    const auto* cookie = test_harness.create_cookie();
    cb_assert(h1->flush(h, cookie) == ENGINE_SUCCESS);
    test_harness.destroy_cookie(cookie);
    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::no_such_key);
    cb_assert(ret.second == nullptr);
    return SUCCESS;
}

/*
 * Make sure we can successfully retrieve the item info struct for an item and
 * that the contents of the item_info are as expected.
 */
static enum test_result get_item_info_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("get_item_info_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    const time_t exp = 1;
    item_info ii;

    auto ret =
            h1->allocate(h, NULL, key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    /* Had this been actual code, there'd be a connection here */
    cb_assert(h1->get_item_info(h, ret.second.get(), &ii) == true);
    assert_equal(cas, ii.cas);
    assert_equal(0u, ii.flags);
    cb_assert(strcmp(reinterpret_cast<const char*>(key.data()), static_cast<const char*>(ii.key)) == 0);
    assert_equal(uint16_t(key.size()), ii.nkey);
    assert_equal(1u, ii.nbytes);
    // exptime is a rel_time_t; i.e. seconds since server started. Therefore can only
    // check that the returned value is at least as large as the value
    // we requested (i.e. not in the past).
    assert_ge(ii.exptime, exp);
    return SUCCESS;
}

static enum test_result item_set_cas_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("item_set_cas_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    const rel_time_t exp = 1;
    uint64_t newcas;
    item_info ii;

    auto ret =
            h1->allocate(h, NULL, key, 1, 0, exp, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    newcas = cas + 1;
    h1->item_set_cas(h, ret.second.get(), newcas);
    cb_assert(h1->get_item_info(h, ret.second.get(), &ii) == true);
    cb_assert(ii.cas == newcas);
    return SUCCESS;
}

uint32_t evictions;
static void eviction_stats_handler(const char* key,
                                   const uint16_t klen,
                                   const char* val,
                                   const uint32_t vlen,
                                   gsl::not_null<const void*>) {
    if (strncmp(key, "evictions", klen) == 0) {
        char buffer[1024];
        memcpy(buffer, val, vlen);
        buffer[vlen] = '\0';
        evictions = atoi(buffer);
    }
}

static enum test_result lru_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey hot_key("hot_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    int ii;
    int jj;

    auto ret = h1->allocate(
            h, NULL, hot_key, 4096, 0, 0, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);

    for (ii = 0; ii < 250; ++ii) {
        uint8_t key[1024];

        ret = h1->get(h, NULL, hot_key, 0, DocStateFilter::Alive);
        cb_assert(ret.first == cb::engine_errc::success);
        DocKey allocate_key(key,
                            snprintf(reinterpret_cast<char*>(key), sizeof(key),
                                     "lru_test_key_%08d", ii),
                            test_harness.doc_namespace);
        ret = h1->allocate(h,
                           NULL,
                           allocate_key,
                           4096,
                           0,
                           0,
                           PROTOCOL_BINARY_RAW_BYTES,
                           0);
        cb_assert(ret.first == cb::engine_errc::success);
        cb_assert(h1->store(h,
                            NULL,
                            ret.second.get(),
                            &cas,
                            OPERATION_SET,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        const auto* cookie = test_harness.create_cookie();
        cb_assert(h1->get_stats(h, cookie, {}, eviction_stats_handler) ==
                  ENGINE_SUCCESS);
        test_harness.destroy_cookie(cookie);
        if (evictions == 2) {
            break;
        }
    }

    cb_assert(ii < 250);
    for (jj = 0; jj <= ii; ++jj) {
        uint8_t key[1024];
        DocKey get_key(key,
                       snprintf(reinterpret_cast<char*>(key), sizeof(key),
                                "lru_test_key_%08d", jj),
                       test_harness.doc_namespace);
        if (jj == 0 || jj == 1) {
            ret = h1->get(h, NULL, get_key, 0, DocStateFilter::Alive);
            cb_assert(ret.first == cb::engine_errc::no_such_key);
        } else {
            ret = h1->get(h, NULL, get_key, 0, DocStateFilter::Alive);
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(ret.second != nullptr);
        }
    }
    return SUCCESS;
}

static enum test_result get_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result reset_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result get_stats_struct_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result aggregate_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result test_datatype(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    DocKey key("{foo:1}", test_harness.doc_namespace);
    uint64_t cas = 0;
    item_info ii;
    memset(&ii, 0, sizeof(ii));

    auto ret = h1->allocate(h, NULL, key, 1, 0, 0, 1, 0 /*vb*/);
    cb_assert(ret.first == cb::engine_errc::success);
    cb_assert(h1->store(h,
                        NULL,
                        ret.second.get(),
                        &cas,
                        OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);

    ret = h1->get(h, NULL, key, 0, DocStateFilter::Alive);
    cb_assert(ret.first == cb::engine_errc::success);

    cb_assert(h1->get_item_info(h, ret.second.get(), &ii) == true);
    cb_assert(ii.datatype == 1);

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
    std::vector<std::pair<ENGINE_HANDLE*, ENGINE_HANDLE_V1*> > buckets;
    for (int ii = 0; ii < n_buckets; ii++) {
        ENGINE_HANDLE_V1* handle = test_harness.create_bucket(true, test->cfg);
        if (handle) {
            buckets.push_back(std::make_pair(reinterpret_cast<ENGINE_HANDLE*>(handle), handle));
        } else {
            return FAIL;
        }
    }

    for (auto bucket : buckets) {
        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, test_harness.doc_namespace);
            auto ret = bucket.second->allocate(bucket.first,
                                               NULL,
                                               allocate_key,
                                               256,
                                               1,
                                               1,
                                               PROTOCOL_BINARY_RAW_BYTES,
                                               0);
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(bucket.second->store(bucket.first,
                                           NULL,
                                           ret.second.get(),
                                           &cas,
                                           OPERATION_SET,
                                           DocumentState::Alive) ==
                      ENGINE_SUCCESS);
        }
    }

    for (auto itr : buckets) {
        test_harness.destroy_bucket(itr.first, itr.second, false);
    }

    return SUCCESS;
}

/*
 * create and delete buckets, the idea being that the background deletion
 * is running whilst we're creating more buckets.
 */
static enum test_result test_bucket_destroy_interleaved(engine_test_t *test) {
    const int n_keys = 20;
    const int buckets = 5;

    for (int b = 0; b < buckets; b++) {
        ENGINE_HANDLE_V1* h1 = test_harness.create_bucket(true, test->cfg);
        ENGINE_HANDLE* h = reinterpret_cast<ENGINE_HANDLE*>(h1);

        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            uint64_t cas = 0;
            DocKey allocate_key(ss, test_harness.doc_namespace);
            auto ret = h1->allocate(h,
                                    NULL,
                                    allocate_key,
                                    111256,
                                    1,
                                    1,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    0);
            cb_assert(ret.first == cb::engine_errc::success);
            cb_assert(h1->store(h,
                                NULL,
                                ret.second.get(),
                                &cas,
                                OPERATION_SET,
                                DocumentState::Alive) == ENGINE_SUCCESS);
        }

        test_harness.destroy_bucket(h, h1, false);
    }

    return SUCCESS;
}

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {
    static engine_test_t tests[]  = {
        TEST_CASE("get info test", get_info_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get info description test", get_info_description_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get info features test", get_info_features_test, NULL, NULL, NULL, NULL, NULL),
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

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    test_harness = *th;
    return true;
}
