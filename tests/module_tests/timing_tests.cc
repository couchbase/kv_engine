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

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <platform/cb_malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#ifdef linux
/* /usr/include/netinet/in.h defines macros from ntohs() to _bswap_nn to
 * optimize the conversion functions, but the prototypes generate warnings
 * from gcc. The conversion methods isn't the bottleneck for my app, so
 * just remove the warnings by undef'ing the optimization ..
 */
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

bool abort_msg(const char *expr, const char *msg, int line);

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

protocol_binary_response_status last_status(static_cast<protocol_binary_response_status>(0));
char *last_key = NULL;
char *last_body = NULL;
std::map<std::string, std::string> vals;

struct test_harness testHarness;

bool abort_msg(const char *expr, const char *msg, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            __FILE__, line, msg, expr);
    abort();
    // UNREACHABLE
    return false;
}

extern "C" {
    static void rmdb(void) {
#ifdef WIN32
        _unlink("./test");
#else
        unlink("./test");
#endif
    }

    static bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
        (void)h; (void)h1;
        atexit(rmdb);
        vals.clear();
        return true;
    }
}

static inline void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    usleep(*sleepTime);
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

static ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                      const void *cookie,
                                      ENGINE_STORE_OPERATION op,
                                      const char *key,
                                      const char *value, size_t vlen,
                                      uint32_t flags,
                                      item **outitem, uint64_t casIn,
                                      uint16_t vb) {
    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = h1->allocate(h, cookie, &it,
                                        DocKey(key, DocNamespace::DefaultCollection),
                                        vlen, flags, 3600,
                                        PROTOCOL_BINARY_RAW_BYTES, vb);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, DocumentState::Alive);

    if (outitem) {
        *outitem = it;
    }

    return rv;
}

extern "C" {
    static void add_stats(const char *key, const uint16_t klen,
                          const char *val, const uint32_t vlen,
                          const void *cookie) {
        (void)cookie;
        std::string k(key, klen);
        std::string v(val, vlen);
        vals[k] = v;
    }
}

static int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey = NULL) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string s = vals[statname];
    return atoi(s.c_str());
}

static void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int exp, const char *msg) {
    int curr_items = get_int_stat(h, h1, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

static void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_queue_size") > 0) {
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

extern "C" {
static test_result test_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
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

        check(storeCasVb11(h, h1, NULL, OPERATION_SET, key, data,
                           size, 9713, &it, 0, 0) == ENGINE_SUCCESS,
                  "store failure");
    }
    cb_free(data);
    wait_for_flusher_to_settle(h, h1);

    std::cout << total << " at " << size << " - "
              << get_int_stat(h, h1, "ep_flush_duration_total")
              << ", " << get_int_stat(h, h1, "ep_commit_time_total") << std::endl;

        verify_curr_items(h, h1, total, "storing something");

    return SUCCESS;
}
}

extern "C" MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}

extern "C" MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {

    static engine_test_t tests[]  = {
        TEST_CASE("test persistence", test_persistence, NULL, teardown, NULL,
         NULL, NULL),
        TEST_CASE(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    };
    return tests;
}
