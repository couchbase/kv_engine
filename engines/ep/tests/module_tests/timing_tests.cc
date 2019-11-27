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

#include <memcached/durability_spec.h>
#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/protocol_binary.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

bool abort_msg(const char *expr, const char *msg, int line);

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

cb::mcbp::Status last_status(cb::mcbp::Status::Success);
char *last_key = NULL;
char *last_body = NULL;
std::map<std::string, std::string> vals;

struct test_harness* testHarness;

bool abort_msg(const char *expr, const char *msg, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            __FILE__, line, msg, expr);
    abort();
    // UNREACHABLE
    return false;
}

static void rmdb(void) {
#ifdef WIN32
    _unlink("./test");
#else
    unlink("./test");
#endif
}

static bool teardown(EngineIface*) {
    atexit(rmdb);
    vals.clear();
    return true;
}

bool teardown_suite() {
    return true;
}

static inline void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    std::this_thread::sleep_for(std::chrono::microseconds(*sleepTime));
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

static ENGINE_ERROR_CODE storeCasVb11(EngineIface* h,
                                      const void* cookie,
                                      ENGINE_STORE_OPERATION op,
                                      const char* key,
                                      const char* value,
                                      size_t vlen,
                                      uint32_t flags,
                                      item** outitem,
                                      uint64_t casIn,
                                      Vbid vb) {
    uint64_t cas = 0;

    auto ret = h->allocate(cookie,
                           DocKey(key, DocKeyEncodesCollectionId::No),
                           vlen,
                           flags,
                           3600,
                           PROTOCOL_BINARY_RAW_BYTES,
                           vb);
    check(ret.first == cb::engine_errc::success, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(ret.second.get(), &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h->item_set_cas(ret.second.get(), casIn);

    auto rv = h->store(
            cookie, ret.second.get(), cas, op, {}, DocumentState::Alive);

    if (outitem) {
        *outitem = ret.second.release();
    }

    return rv;
}

extern "C" {
static void add_stats(cb::const_char_buffer key,
                      cb::const_char_buffer value,
                      gsl::not_null<const void*>) {
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());
    vals[k] = v;
    }
}

static int get_int_stat(EngineIface* h,
                        const char* statname,
                        const char* statkey = NULL) {
    vals.clear();
    const auto* cookie = testHarness->create_cookie();
    check(h->get_stats(cookie,
                       {statkey, statkey == NULL ? 0 : strlen(statkey)},
                       {},
                       add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    testHarness->destroy_cookie(cookie);
    std::string s = vals[statname];
    return atoi(s.c_str());
}

static void verify_curr_items(EngineIface* h,
                              int exp,
                              const char* msg) {
    int curr_items = get_int_stat(h, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

static void wait_for_flusher_to_settle(EngineIface* h) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, "ep_queue_size") > 0) {
        decayingSleep(&sleepTime);
    }
}

static size_t env_int(const char *k, size_t rv) {
    char *x = getenv(k);
    if (x) {
        rv = static_cast<size_t>(atoi(x));
    }
    return rv;
}

static test_result test_persistence(EngineIface* h) {
    size_t total = env_int("TEST_TOTAL_KEYS", 100000);
    size_t size = env_int("TEST_VAL_SIZE", 20);

    char key[24];
    char *data;
    data = static_cast<char *>(cb_malloc(sizeof(char) * size));
    cb_assert(data);

    for (size_t i = 0; i < (sizeof(char) * size); ++i) {
        data[i] = 0xff & rand();
    }

    for (size_t i = 0; i < total; ++i) {
        item *it = NULL;
        snprintf(key, sizeof(key), "k%d", static_cast<int>(i));

        check(storeCasVb11(h,
                           NULL,
                           OPERATION_SET,
                           key,
                           data,
                           size,
                           9713,
                           &it,
                           0,
                           Vbid(0)) == ENGINE_SUCCESS,
              "store failure");
    }
    cb_free(data);
    wait_for_flusher_to_settle(h);

    std::cout << total << " at " << size << " - "
              << get_int_stat(h, "ep_flush_duration_total")
              << ", " << get_int_stat(h, "ep_commit_time_total") << std::endl;

        verify_curr_items(h, total, "storing something");

    return SUCCESS;
}

bool setup_suite(struct test_harness *th) {
    testHarness = th;
    return true;
}

engine_test_t* get_tests(void) {

    static engine_test_t tests[]  = {
        TEST_CASE("test persistence", test_persistence, NULL, teardown, NULL,
         NULL, NULL),
        TEST_CASE(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    };
    return tests;
}
