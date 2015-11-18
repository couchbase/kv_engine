/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

// Usage: (to run just a single test case)
// make engine_tests EP_TEST_NUM=3

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "atomic.h"
#include "compress.h"
#include "ep-engine/command_ids.h"
#include "ep_test_apis.h"

#include "ep_testsuite_common.h"
#include "locks.h"
#include "mock/mock_dcp.h"
#include "mutex.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <JSON_checker.h>

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

// ptr_fun don't like the extern "C" thing for unlock cookie.. cast it
// away ;)
typedef void (*UNLOCK_COOKIE_T)(const void *cookie);

#define MULTI_DISPATCHER_CONFIG \
    "ht_size=129;ht_locks=3;chk_remover_stime=1;chk_period=60"

class ThreadData {
public:
    ThreadData(ENGINE_HANDLE *eh, ENGINE_HANDLE_V1 *ehv1,
               int e=0) : h(eh), h1(ehv1), extra(e) {}
    ENGINE_HANDLE    *h;
    ENGINE_HANDLE_V1 *h1;
    int               extra;
};


static const void* createTapConn(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *name) CB_MUST_USE_RESULT;

static const void* createTapConn(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *name) {
    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name,
                                             strlen(name),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");
    return cookie;
}

static void check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            const char* key, const char* val, size_t vlen,
                            uint16_t vbucket = 0) {
    item_info info;
    check(get_item_info(h, h1, &info, key, vbucket), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(vlen == info.value[0].iov_len, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, val, vlen) == 0, "Data mismatch");
}

static void check_observe_seqno(bool failover, uint8_t format_type, uint16_t vb_id,
                                uint64_t vb_uuid, uint64_t last_persisted_seqno,
                                uint64_t current_seqno, uint64_t failover_vbuuid = 0,
                                uint64_t failover_seqno = 0) {
    uint8_t  recv_format_type;
    uint16_t recv_vb_id;
    uint64_t recv_vb_uuid;
    uint64_t recv_last_persisted_seqno;
    uint64_t recv_current_seqno;
    uint64_t recv_failover_vbuuid;
    uint64_t recv_failover_seqno;

    memcpy(&recv_format_type, last_body.data(), sizeof(uint8_t));
    check(recv_format_type == format_type, "Wrong format type in result");
    memcpy(&recv_vb_id, last_body.data() + 1, sizeof(uint16_t));
    check(ntohs(recv_vb_id) == vb_id, "Wrong vbucket id in result");
    memcpy(&recv_vb_uuid, last_body.data() + 3, sizeof(uint64_t));
    check(ntohll(recv_vb_uuid) == vb_uuid, "Wrong vbucket uuid in result");
    memcpy(&recv_last_persisted_seqno, last_body.data() + 11, sizeof(uint64_t));
    check(ntohll(recv_last_persisted_seqno) == last_persisted_seqno,
          "Wrong persisted seqno in result");
    memcpy(&recv_current_seqno, last_body.data() + 19, sizeof(uint64_t));
    check(ntohll(recv_current_seqno) == current_seqno, "Wrong current seqno in result");

    if (failover) {
        memcpy(&recv_failover_vbuuid, last_body.data() + 27, sizeof(uint64_t));
        check(ntohll(recv_failover_vbuuid) == failover_vbuuid, "Wrong failover uuid in result");
        memcpy(&recv_failover_seqno, last_body.data() + 35, sizeof(uint64_t));
        check(ntohll(recv_failover_seqno) == failover_seqno, "Wrong failover seqno in result");
    }
}

static enum test_result test_getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "k1";
    uint16_t vbucketId = 0;
    uint32_t expiration = 25;

    getl(h, h1, key, vbucketId, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");
    if (!last_body.empty() && last_body != "NOT_FOUND") {
        fprintf(stderr, "Should have returned NOT_FOUND. Getl Failed");
        abort();
    }

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "{\"lock\":\"data\"}",
                &i, 0, vbucketId, 3600, PROTOCOL_BINARY_DATATYPE_JSON)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* retry getl, should succeed */
    getl(h, h1, key, vbucketId, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");
    check(last_body == "{\"lock\":\"data\"}", "Body was malformed.");
    check(last_datatype == PROTOCOL_BINARY_DATATYPE_JSON,
            "Expected datatype to be JSON");

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock's taken so this should fail */
    getl(h, h1, key, vbucketId, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (!last_body.empty() && last_body != "LOCK_ERROR") {
        fprintf(stderr, "Should have returned LOCK_ERROR. Getl Failed");
        abort();
    }

    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata2", &i, 0, vbucketId)
          != ENGINE_SUCCESS, "Should have failed to store an item.");
    h1->release(h, NULL, i);

    /* wait another 10 seconds */
    testHarness.time_travel(10);

    /* retry set, should succeed */
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* point to wrong vbucket, to test NOT_MY_VB response */
    getl(h, h1, key, 10, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Should have received not my vbucket response");

    /* acquire lock, should succeed */
    getl(h, h1, key, vbucketId, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Aquire lock should have succeeded");
    check(last_datatype == PROTOCOL_BINARY_RAW_BYTES,
            "Expected datatype to be RAW BYTES");

    /* try an incr operation followed by a delete, both of which should fail */
    uint64_t cas = 0;
    uint64_t result = 0;
    i = NULL;
    check(h1->arithmetic(h, NULL, key, 2, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_TMPFAIL, "Incr failed");
    h1->release(h, NULL, i);

    check(del(h, h1, key, 0, 0) == ENGINE_TMPFAIL, "Delete failed");


    /* bug MB 2699 append after getl should fail with ENGINE_TMPFAIL */

    testHarness.time_travel(26);

    char binaryData1[] = "abcdefg\0gfedcba";
    char binaryData2[] = "abzdefg\0gfedcba";

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, key,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    /* acquire lock, should succeed */
    getl(h, h1, key, vbucketId, expiration);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Aquire lock should have succeeded");
    auto locked_cas = last_cas.load();
    /* append should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key,
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_TMPFAIL,
          "Append should fail.");
    h1->release(h, NULL, i);

    // append should fail if we used invalid cas!
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key,
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i,
                       locked_cas-1, 0)
          == ENGINE_TMPFAIL,
          "Append should fail for locked key with invalid cas.");
    h1->release(h, NULL, i);

    // append should succeed if we used correct cas!
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key,
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i,
                       locked_cas, 0)
          == ENGINE_SUCCESS,
          "Append should Succeed for locked key with correct cas.");
    h1->release(h, NULL, i);

    /* bug MB 3252 & MB 3354.
     * 1. Set a key with an expiry value.
     * 2. Take a lock on the item before it expires
     * 3. Wait for the item to expire
     * 4. Perform a CAS operation, should fail
     * 5. Perform a set operation, should succeed
     */
    const char *ekey = "test_expiry";
    const char *edata = "some test data here.";

    item *it = NULL;

    check(h1->allocate(h, NULL, &it, ekey, strlen(ekey), strlen(edata), 0, 2,
          PROTOCOL_BINARY_RAW_BYTES) == ENGINE_SUCCESS, "Allocation Failed");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, edata, strlen(edata));

    check(h1->store(h, NULL, it, &cas, OPERATION_SET, 0) ==
        ENGINE_SUCCESS, "Failed to Store item");
    check_key_value(h, h1, ekey, edata, strlen(edata));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);
    cas = last_cas;

    /* cas should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_CAS, ekey,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, cas, 0)
          != ENGINE_SUCCESS,
          "CAS succeeded.");
    h1->release(h, NULL, i);

    /* but a simple store should succeed */
    check(store(h, h1, NULL, OPERATION_SET, ekey, edata, &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "k2";
    uint16_t vbucketId = 0;

    unl(h, h1, key, vbucketId);
    check(last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "expected the key to be missing...");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* getl, should succeed */
    getl(h, h1, key, vbucketId, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");

    /* save the returned cas value for later */
    uint64_t cas = last_cas;

    /* lock's taken unlocking with a random cas value should fail */
    unl(h, h1, key, vbucketId);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (!last_body.empty() && last_body != "UNLOCK_ERROR") {
        fprintf(stderr, "Should have returned UNLOCK_ERROR. Unl Failed");
        abort();
    }

    unl(h, h1, key, vbucketId, cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to succed unl with correct cas");

    /* acquire lock, should succeed */
    getl(h, h1, key, vbucketId, 0);

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock has expired, unl should fail */
    unl(h, h1, key, vbucketId, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail unl on lock timeout");

    return SUCCESS;
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // Add operation with cas != 0 doesn't make sense
        cas = 0;
    }
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not_my_vbucket");
    h1->release(h, NULL, i);
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_pending_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_pending), "Bucket state was not set to pending.");
    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // Add operation with cas != 0 doesn't make sense..
        cas = 0;
    }
    check(store(h, h1, cookie, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_EWOULDBLOCK,
        "Expected woodblock");
    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_replica), "Bucket state was not set to replica.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");

    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // performing add with a CAS != 0 doesn't make sense...
        cas = 0;
    }
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not my vbucket");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    h1->release(h, NULL, i);
    return SUCCESS;
}

//
// ----------------------------------------------------------------------
// The actual tests are below.
// ----------------------------------------------------------------------
//

static enum test_result test_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_key(h, h1, "k") == ENGINE_KEY_ENOENT, "Expected miss.");
    return SUCCESS;
}

static enum test_result test_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0, high_seqno = 0;
    const int num_sets = 5, num_keys = 4;

    std::string key_arr[num_keys] = { "dummy_key",
                                      "checkpoint_start",
                                      "checkpoint_end",
                                      "key" };


    for (int k = 0; k < num_keys; k++) {
        for (int j = 0; j < num_sets; j++) {
            memset(&info, 0, sizeof(info));
            vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
            high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno",
                                      "vbucket-seqno");

            std::string err_str_store("Error setting " + key_arr[k]);
            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_SET, key_arr[k].c_str(),
                          "somevalue", &i),
                    err_str_store.c_str());
            h1->release(h, NULL, i);

            std::string err_str_get_item_info("Error getting " + key_arr[k]);
            checkeq(true, get_item_info(h, h1, &info, key_arr[k].c_str()),
                  err_str_get_item_info.c_str());

            std::string err_str_vb_uuid("Expected valid vbucket uuid for " +
                                        key_arr[k]);
            checkeq(vb_uuid, info.vbucket_uuid, err_str_vb_uuid.c_str());

            std::string err_str_seqno("Expected valid sequence number for " +
                                        key_arr[k]);
            checkeq(high_seqno + 1, info.seqno, err_str_seqno.c_str());
        }
    }

    wait_for_flusher_to_settle(h, h1);

    return SUCCESS;
}

struct handle_pair {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
};

extern "C" {
    static void conc_del_set_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);
        item *it = NULL;

        for (int i = 0; i < 5000; ++i) {
            store(hp->h, hp->h1, NULL, OPERATION_ADD,
                  "key", "somevalue", &it);
            hp->h1->release(hp->h, NULL, it);
            usleep(10);
            checkeq(ENGINE_SUCCESS,
                    store(hp->h, hp->h1, NULL, OPERATION_SET,
                          "key", "somevalue", &it),
                    "Error setting.");
            hp->h1->release(hp->h, NULL, it);
            usleep(10);
            // Ignoring the result here -- we're racing.
            del(hp->h, hp->h1, "key", 0, 0);
            usleep(10);
        }
    }

    static void conc_incr_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);
        uint64_t result = 0;

        for (int i = 0; i < 10; i++) {
            item *it = NULL;
            check(hp->h1->arithmetic(hp->h, NULL, "key", 3, true, true, 1, 1, 0,
                                     &it, PROTOCOL_BINARY_RAW_BYTES, &result,
                                     0) == ENGINE_SUCCESS,
                                     "Failed arithmetic operation");
            hp->h1->release(hp->h, NULL, it);
        }
    }
}

static enum test_result test_conc_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const int n_threads = 8;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    wait_for_persisted_value(h, h1, "key", "value1");

    for (int i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], conc_del_set_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    wait_for_flusher_to_settle(h, h1);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    cb_assert(0 == get_int_stat(h, h1, "ep_warmup_dups"));

    return SUCCESS;
}

static enum test_result test_conc_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int n_threads = 10;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "0", &i) == ENGINE_SUCCESS,
          "store failure");
    h1->release(h, NULL, i);

    for (int i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], conc_incr_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    check_key_value(h, h1, "key", "100", 3);

    return SUCCESS;
}

static enum test_result test_conc_incr_new_itm (ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    const int n_threads = 10;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    for (int i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], conc_incr_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    check_key_value(h, h1, "key", "100", 3);

    return SUCCESS;
}

struct multi_set_args {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    std::string prefix;
    int count;
};

extern "C" {
    static void multi_set_thread(void *arg) {
        struct multi_set_args *msa = static_cast<multi_set_args *>(arg);

        for (int i = 0; i < msa->count; i++) {
            item *it = NULL;
            std::stringstream s;
            s << msa->prefix << i;
            std::string key(s.str());
            check(ENGINE_SUCCESS == store(msa->h, msa->h1, NULL, OPERATION_SET,
                          key.c_str(), "somevalue", &it), "Set failure!");
            msa->h1->release(msa->h, NULL, it);
        }
    }
}

static enum test_result test_multi_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    cb_thread_t thread1, thread2;
    struct multi_set_args msa1, msa2;
    msa1.h = h;
    msa1.h1 = h1;
    msa1.prefix = "ONE_";
    msa1.count = 50000;
    cb_assert(cb_create_thread(&thread1, multi_set_thread, &msa1, 0) == 0);

    msa2.h = h;
    msa2.h1 = h1;
    msa2.prefix = "TWO_";
    msa2.count = 50000;
    cb_assert(cb_create_thread(&thread2, multi_set_thread, &msa2, 0) == 0);

    cb_assert(cb_join_thread(thread1) == 0);
    cb_assert(cb_join_thread(thread2) == 0);

    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "curr_items") == 100000,
          "Mismatch in number of items inserted");
    check(get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno") == 100000,
          "Unexpected high sequence number");

    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(ENGINE_SUCCESS ==
          storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0),
          "Failed to set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
    return SUCCESS;
}

static enum test_result test_set_with_cas_non_existent(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_flush";
    item *i = NULL;

    check(h1->allocate(h, NULL, &i, key, strlen(key), 10, 0, 0,
          PROTOCOL_BINARY_RAW_BYTES) == ENGINE_SUCCESS, "Allocation failed.");

    Item *it = reinterpret_cast<Item*>(i);
    it->setCas(1234);

    uint64_t cas = 0;
    check(h1->store(h, NULL, i, &cas, OPERATION_SET, 0) == ENGINE_KEY_ENOENT,
          "Expected not found");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set.");
    h1->release(h, NULL, i);

    item_info info;
    uint32_t flags = 828258;
    check(get_item_info(h, h1, &info, "key"), "Failed to get value.");
    cb_assert(info.flags != flags);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "newvalue", strlen("newvalue"), flags, &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to set again.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to do initial set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    check(h1->get(h, NULL, &i, "key", 3, 0) == ENGINE_SUCCESS,
          "Failed to get value.");

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "Failed to get item info.");
    h1->release(h, NULL, i);

    check(store(h, h1, NULL, OPERATION_CAS, "key", "winCas", &i,
                info.cas) == ENGINE_SUCCESS,
          "Failed to store CAS");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "winCas", 6);

    uint64_t cval = 99999;
    check(store(h, h1, NULL, OPERATION_CAS, "non-existing", "winCas", &i,
                cval) == ENGINE_KEY_ENOENT,
          "CAS for non-existing key returned the wrong error code");
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint32_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error getting item info");
    check(vb_uuid == info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == info.seqno, "Expected valid sequence number");

    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");
    h1->release(h, NULL, i);

    // This aborts on failure.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    check(store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i) == ENGINE_SUCCESS,
          "Failed to add value again.");

    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_add_add_with_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD, "key",
                "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    info.nvalue = 1;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info) == true,
          "Should be able to get info");

    item *i2 = NULL;
    ENGINE_ERROR_CODE ret;
    check((ret = store(h, h1, NULL, OPERATION_ADD, "key",
                       "somevalue", &i2, info.cas)) == ENGINE_KEY_EEXISTS,
          "Should not be able to add the key two times");

    h1->release(h, NULL, i);
    h1->release(h, NULL, i2);
    return SUCCESS;
}

static enum test_result test_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint32_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) != ENGINE_SUCCESS,
          "Failed to fail to replace non-existing value.");

    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set value.");
    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to replace existing value.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error getting item info");

    check(vb_uuid == info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == info.seqno, "Expected valid sequence number");

    check_key_value(h, h1, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_replace_with_eviction(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set value.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key");
    int numBgFetched = get_int_stat(h, h1, "ep_bg_fetched");

    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue1", &i) == ENGINE_SUCCESS,
          "Failed to replace existing value.");

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "full_eviction") {
        numBgFetched++;
    }

    check(get_int_stat(h, h1, "ep_bg_fetched") == numBgFetched,
          "Bg fetched value didn't match");

    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue1", 10);
    return SUCCESS;
}

static enum test_result test_incr_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t result = 0;
    item *i = NULL;
    h1->arithmetic(h, NULL, "key", 3, true, false, 1, 0, 0,
                   &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                   0);
    h1->release(h, NULL, i);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected to not find key");
    return SUCCESS;
}

static enum test_result test_incr_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_datatype_support(cookie, false);

    uint64_t result = 0;
    item *i = NULL;
    check(h1->arithmetic(h, cookie, "key", 3, true, true, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed first arith");
    h1->release(h, cookie, i);
    check(result == 1, "Failed result verification.");

    // Check datatype of counter
    check(h1->get(h, cookie, &i, "key", 3, 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, i, &info);
    h1->release(h, cookie, i);
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Invalid datatype");

    check(h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed second arith.");
    h1->release(h, cookie, i);
    check(result == 2, "Failed second result verification.");

    check(h1->arithmetic(h, cookie, "key", 3, true, true, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed third arith.");
    h1->release(h, cookie, i);
    check(result == 3, "Failed third result verification.");

    check_key_value(h, h1, "key", "3", 1);

    // Check datatype of counter
    check(h1->get(h, cookie, &i, "key", 3, 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    info.nvalue = 1;
    h1->get_item_info(h, cookie, i, &info);
    h1->release(h, cookie, i);
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Invalid datatype");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint32_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    // MB-11332: append on non-existing key should return NOT_STORED
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_NOT_STORED,
          "MB-11332: Failed append.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error in getting item info");

    check(vb_uuid == info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == info.seqno, "Expected valid sequence number");

    check_key_value(h, h1, "key", "\r\nfoo\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected append failure.");
    h1->release(h, NULL, i);
    delete[] bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    std::string expected;
    expected.append(binaryData1, sizeof(binaryData1) - 1);
    expected.append(binaryData2, sizeof(binaryData2) - 1);

    check_key_value(h, h1, "key", expected.data(), expected.length());
    return SUCCESS;
}

static enum test_result test_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint32_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    // MB-11332: prepend on non-existing key should return NOT_STORED
    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_NOT_STORED,
          "MB-11332: Failed prepend.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed prepend.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error getting item info");
    check(vb_uuid == info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == info.seqno, "Expected valid sequence number");

    check_key_value(h, h1, "key", "foo\r\n\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected prepend failure.");
    h1->release(h, NULL, i);
    delete[] bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    std::string expected;
    expected.append(binaryData2, sizeof(binaryData2) - 1);
    expected.append(binaryData1, sizeof(binaryData1) - 1);

    check_key_value(h, h1, "key", expected.data(), expected.length());
    return SUCCESS;
}

static enum test_result test_append_compressed(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;

    snap_buf output1;
    doSnappyCompress("\r\n", 2, output1);
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key1",
                       (const char *)output1.buf.get(), output1.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key1",
                       "foo\n\r", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append uncompressed to compressed.");
    h1->release(h, NULL, i);

    snap_buf output2;
    doSnappyCompress("\r\nfoo\n\r", 7, output2);
    item_info info;
    check(get_item_info(h, h1, &info, "key1", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(output2.len == info.value[0].iov_len, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, output2.buf.get(), output2.len) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED, "Datatype mismatch");

    snap_buf output3;
    doSnappyCompress("bar", 3, output3);
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key1",
                       (const char*)output3.buf.get(), output3.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
            == ENGINE_SUCCESS,
            "Failed append compressed to compressed.");
    h1->release(h, NULL, i);

    snap_buf output4;
    doSnappyCompress("\r\nfoo\n\rbar", 10, output4);

    check(get_item_info(h, h1, &info, "key1", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(output4.len == info.value[0].iov_len, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, output4.buf.get(), output4.len) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED, "Datatype mismatch");

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key2",
                       "foo", 3, 82758, &i, 0, 0, 3600,
                       PROTOCOL_BINARY_RAW_BYTES)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    snap_buf output5;
    doSnappyCompress("bar", 3, output5);
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key2",
                       (const char*)output5.buf.get(), output5.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
            == ENGINE_SUCCESS,
            "Failed append compressed to uncompressed.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key2", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(info.value[0].iov_len == 6, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, "foobar", 6) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_RAW_BYTES, "Datatype mismatch");

    return SUCCESS;
}

static enum test_result test_prepend_compressed(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;

    snap_buf output1;
    doSnappyCompress("\r\n", 2, output1);
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key1",
                       (const char *)output1.buf.get(), output1.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key1",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed prepend uncompressed to compressed.");
    h1->release(h, NULL, i);

    snap_buf output2;
    doSnappyCompress("foo\r\n\r\n", 7, output2);

    item_info info;
    check(get_item_info(h, h1, &info, "key1", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(output2.len == info.value[0].iov_len, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, output2.buf.get(), output2.len) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED, "Datatype mismatch");

    snap_buf output3;
    doSnappyCompress("bar", 3, output3);
    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key1",
                       (const char*)output3.buf.get(), output3.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
            == ENGINE_SUCCESS,
            "Failed prepend compressed to compressed.");
    h1->release(h, NULL, i);

    snap_buf output4;
    doSnappyCompress("barfoo\r\n\r\n", 10, output4);

    check(get_item_info(h, h1, &info, "key1", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(output4.len == info.value[0].iov_len, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, output4.buf.get(), output4.len) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED, "Datatype mismatch");

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key2",
                       "foo", 3, 82758, &i, 0, 0, 3600,
                       PROTOCOL_BINARY_RAW_BYTES)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    snap_buf output5;
    doSnappyCompress("bar", 3, output5);
    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key2",
                       (const char*)output5.buf.get(), output5.len, 82758, &i, 0,
                       0, 3600, PROTOCOL_BINARY_DATATYPE_COMPRESSED)
            == ENGINE_SUCCESS,
            "Failed prepend compressed to uncompressed.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key2", 0), "checking key and value");
    check(info.nvalue == 1, "info.nvalue != 1");
    check(info.value[0].iov_len == 6, "Value length mismatch");
    check(memcmp(info.value[0].iov_base, "barfoo", 6) == 0, "Data mismatch");
    check(info.datatype == PROTOCOL_BINARY_RAW_BYTES, "Datatype mismatch");

    return SUCCESS;
}

static enum test_result test_append_prepend_to_json(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;

    const char* key1 = "foo1";
    const char* key2 = "foo2";
    const char* value1 = "{\"foo1\":\"bar1\"}";
    const char* value2 = "{\"foo2\":\"bar2\"}";

    // APPEND
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, key1,
                       value1, strlen(value1), 82758, &i, 0, 0,
                       3600, PROTOCOL_BINARY_DATATYPE_JSON)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    check(checkUTF8JSON((const unsigned char*)info.value[0].iov_base,
                        (int)info.value[0].iov_len) == 1, "Expected JSON");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Invalid datatype");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key1,
                       value2, strlen(value2), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    check(checkUTF8JSON((const unsigned char*)info.value[0].iov_base,
                        (int)info.value[0].iov_len) == 0, "Expected Binary");
    check(info.datatype == PROTOCOL_BINARY_RAW_BYTES,
                "Invalid datatype after append");
    h1->release(h, NULL, i);

    // PREPEND
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, key2,
                       value1, strlen(value1), 82758, &i, 0, 0,
                       3600, PROTOCOL_BINARY_DATATYPE_JSON)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(h1->get(h, NULL, &i, key2, strlen(key2), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    check(checkUTF8JSON((const unsigned char*)info.value[0].iov_base,
                        (int)info.value[0].iov_len) == 1, "Expected JSON");
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Invalid datatype");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, key2,
                       value2, strlen(value2), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed prepend.");
    h1->release(h, NULL, i);

    check(h1->get(h, NULL, &i, key2, strlen(key2), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    check(checkUTF8JSON((const unsigned char*)info.value[0].iov_base,
                        (int)info.value[0].iov_len) == 0, "Expected Binary");
    check(info.datatype == PROTOCOL_BINARY_RAW_BYTES,
                "Invalid datatype after prepend");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static enum test_result test_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_datatype_support(cookie, true);

    uint64_t result = 0;
    item *i = NULL;
    const char *key = "key";
    const char *val = "1";
    check(store(h, h1, NULL, OPERATION_ADD,key, val, &i,
                0, 0, 3600,
                checkUTF8JSON((const unsigned char *)val, 1))
            == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);

    check(h1->arithmetic(h, NULL, key, 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, key, "2", 1);

    // Check datatype of counter
    check(h1->get(h, cookie, &i, key, 3, 0) == ENGINE_SUCCESS,
            "Unable to get stored item");
    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, i, &info);
    h1->release(h, cookie, i);
    check(info.datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Invalid datatype");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_bug2799(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD, "key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "2", 1);

    testHarness.time_travel(3617);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    if (get_bool_stat(h, h1, "ep_flushall_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "flushall_enabled", "true"),
                "Set flushall_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_flushall_enabled"), "flushall wasn't enabled");

    // First try to delete something we know to not be there.
    check(del(h, h1, "key", 0, 0) == ENGINE_KEY_ENOENT, "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

/**
 * The following struct: flush_args and function run_flush(),
 * will be used by the test that follows: test_multiple_flush
 */
struct flush_args {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    ENGINE_ERROR_CODE expect;
    int when;
};

extern "C" {
    static void run_flush_all(void *arguments) {
        const void *cookie = testHarness.create_cookie();
        testHarness.set_ewouldblock_handling(cookie, true);
        struct flush_args *args = (struct flush_args *)arguments;

        check((args->h1)->flush(args->h, cookie, args->when) == args->expect,
                "Return code is not what is expected");

        testHarness.destroy_cookie(cookie);
    }
}

static enum test_result test_multiple_flush(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {

    if (get_bool_stat(h, h1, "ep_flushall_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "flushall_enabled", "true"),
                "Set flushall_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_flushall_enabled"),
          "flushall wasn't enabled");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 1,
          "Expected curr_items equals 1");

    set_degraded_mode(h, h1, NULL, true);
    cb_thread_t t1, t2;
    struct flush_args args1,args2;
    args1.h = h;
    args1.h1 = h1;
    args1.expect = ENGINE_SUCCESS;
    args1.when = 2;
    check(cb_create_thread(&t1, run_flush_all, &args1, 0) == 0,
            "cb_create_thread failed!");

    sleep(1);

    args2.h = h;
    args2.h1 = h1;
    args2.expect = ENGINE_TMPFAIL;
    args2.when = 0;
    check(cb_create_thread(&t2, run_flush_all, &args2, 0) == 0,
            "cb_create_thread failed!");

    cb_assert(cb_join_thread(t1) == 0);
    cb_assert(cb_join_thread(t2) == 0);

    set_degraded_mode(h, h1, NULL, false);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 0,
          "Expected curr_items equals 0");

    return SUCCESS;
}

static enum test_result test_flush_disabled(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // start an engine with disabled flush, the flush() should be noop and
    // we expect to see the key after flush()

    // store a key and check its existence
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP, "Flush should be disabled");
    //check the key
    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");

    // restart engine with flush enabled and redo the test, we expect flush to succeed
    std::string param = "flushall_enabled=false";
    std::string config = testHarness.get_current_testcase()->cfg;
    size_t found = config.find(param);
    if(found != config.npos) {
        config.replace(found, param.size(), "flushall_enabled=true");
    }
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);
    wait_for_warmup_complete(h, h1);


    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Flush should be enabled");
    set_degraded_mode(h, h1, NULL, false);

    //expect missing key
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_flush_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_key(h, h1, "key2"), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9);

    int overhead2 = get_int_stat(h, h1, "ep_overhead");
    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");

    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    wait_for_flusher_to_settle(h, h1);

    overhead2 = get_int_stat(h, h1, "ep_overhead");
    cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    cb_assert(overhead2 == overhead);
    cb_assert(nonResident2 == nonResident);
    cb_assert(cacheSize2 == cacheSize);

    return SUCCESS;
}

static enum test_result test_flush_multiv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");
    h1->release(h, NULL, i);

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_key(h, h1, "key2", 2), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9, 2);

    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_flush_all") != vals.end(), "Failed to get the status of flush_all");

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2", 2), "Expected missing key");

    return SUCCESS;
}

static int checkCurrItemsAfterShutdown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                       int numItems2Load, bool shutdownForce) {
    std::vector<std::string> keys;
    for (int index = 0; index < numItems2Load; ++index) {
        std::stringstream s;
        s << "keys_2_load-" << index;
        std::string key(s.str());
        keys.push_back(key);
    }

    check(get_int_stat(h, h1, "ep_total_persisted") == 0,
          "Expected ep_total_persisted equals 0");
    check(get_int_stat(h, h1, "curr_items") == 0,
          "Expected curr_items equals 0");

    // stop flusher before loading new items
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "CMD_STOP_PERSISTENCE failed!");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to stop persistence!");
    free(pkt);

    std::vector<std::string>::iterator itr;
    for (itr = keys.begin(); itr != keys.end(); ++itr) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, itr->c_str(), "oracle", &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    check(get_int_stat(h, h1, "ep_total_persisted") == 0,
          "Incorrect ep_total_persisted, expected 0");
    std::stringstream ss;
    ss << "Incorrect curr_items, expected " << numItems2Load;
    std::string errmsg(ss.str());
    check(get_int_stat(h, h1, "curr_items") == numItems2Load,
          errmsg.c_str());

    // resume flusher before shutdown + warmup
    pkt = createPacket(PROTOCOL_BINARY_CMD_START_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "CMD_START_PERSISTENCE failed!");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to start persistence!");
    free(pkt);

    // shutdown engine force and restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, shutdownForce);
    wait_for_warmup_complete(h, h1);
    return get_int_stat(h, h1, "curr_items");
}

static enum test_result test_flush_shutdown_force(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numItems2load = 3000;
    bool shutdownForce = true;
    int currItems = checkCurrItemsAfterShutdown(h, h1, numItems2load, shutdownForce);
    check (currItems <= numItems2load,
           "Number of curr items should be <= 3000, unless previous "
           "shutdown force had to wait for the flusher");
    return SUCCESS;
}

static enum test_result test_flush_shutdown_noforce(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numItems2load = 3000;
    bool shutdownForce = false;
    int currItems = checkCurrItemsAfterShutdown(h, h1, numItems2load, shutdownForce);
    check (currItems == numItems2load,
           "Number of curr items should be equal to 3000, unless previous "
           "shutdown did not wait for the flusher");
    return SUCCESS;
}

static enum test_result test_flush_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(del(h, h1, "key", 0, 0) == ENGINE_KEY_ENOENT, "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key2", "somevalue", 9);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush, post-restart set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush_multiv_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");
    h1->release(h, NULL, i);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(verify_vbucket_missing(h, h1, 2), "Bucket 2 came back.");
    return SUCCESS;
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(del(h, h1, "key", 0, 0) == ENGINE_KEY_ENOENT, "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    uint64_t orig_cas = it->getCas();
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    uint64_t cas = 0;
    uint64_t vb_uuid = 0;
    mutation_descr_t mut_info;
    uint32_t high_seqno = 0;

    memset(&mut_info, 0, sizeof(mut_info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    check(h1->remove(h, NULL, "key", 3, &cas, 0, &mut_info) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(orig_cas + 1 == cas, "Cas mismatch on delete");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(vb_uuid == mut_info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == mut_info.seqno, "Expected valid sequence number");

    // Can I time travel to an expired object and delete it?
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(3617);
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, "key", 0, 0),
            "Did not get ENOENT removing an expired object.");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_set_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    checkeq(ENGINE_SUCCESS, del(h, h1, "key", 0, 0),
            "Failed remove with value.");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    return SUCCESS;
}

static enum test_result test_set_delete_invalid_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key",
                "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info) == true,
          "Should be able to get info");
    h1->release(h, NULL, i);

    check(del(h, h1, "key", info.cas + 1, 0) == ENGINE_KEY_EEXISTS,
          "Didn't expect to be able to remove the item with wrong cas");
    return SUCCESS;
}

static enum test_result test_bug2509(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (int j = 0; j < 10000; ++j) {
        item *itm = NULL;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &itm),
                "Failed set.");
        h1->release(h, NULL, itm);
        usleep(10);
        checkeq(ENGINE_SUCCESS, del(h, h1, "key", 0, 0), "Failed remove with value.");
        usleep(10);
    }

    // Restart again, to verify we don't have any duplicates.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    return get_int_stat(h, h1, "ep_warmup_dups") == 0 ? SUCCESS : FAIL;
}

static enum test_result test_bug7023(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    // Make a vbucket mess.
    for (int j = 0; j < 10000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    std::vector<std::string>::iterator it;
    for (int j = 0; j < 5; ++j) {
        check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set set vbucket 0 dead.");
        vbucketDelete(h, h1, 0);
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "Expected vbucket deletion to work.");
        check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set set vbucket 0 active.");
        for (it = keys.begin(); it != keys.end(); ++it) {
            item *i;
            check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i)
                  == ENGINE_SUCCESS, "Failed to store a value");
            h1->release(h, NULL, i);

        }
    }
    wait_for_flusher_to_settle(h, h1);

    // Restart again, to verify no data loss.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    return get_int_stat(h, h1, "ep_warmup_value_count", "warmup") == 10000 ? SUCCESS : FAIL;
}

static enum test_result test_delete_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "key", "value1");

    check(del(h, h1, "key", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");

    wait_for_persisted_value(h, h1, "key", "value2");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check_key_value(h, h1, "key", "value2", 6);
    check(del(h, h1, "key", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");
    wait_for_flusher_to_settle(h, h1);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_get_delete_missing_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "key";
    wait_for_persisted_value(h, h1, key, "value2delete");

    // whack the db file and directory where the key is stored
    rmdb(dbname_env);

    item *i = NULL;
    int errorCode = h1->get(h, NULL, &i, key, strlen(key), 0);
    h1->release(h, NULL, i);

    // ep engine must be unaware of well-being of the db file as long as
    // the item is still in the memory
    check(errorCode == ENGINE_SUCCESS, "Expected success for get");

    i = NULL;
    evict_key(h, h1, key);
    errorCode = h1->get(h, NULL, &i, key, strlen(key), 0);
    h1->release(h, NULL, i);

    // ep engine must be now aware of the ill-fated db file where
    // the item is supposedly stored
    check(errorCode == ENGINE_TMPFAIL, "Expected tmp fail for get");

    return SUCCESS;
}


static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    ENGINE_ERROR_CODE ret;
    check((ret = store(h, h1, NULL, OPERATION_SET, "key", val, &i)) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check_key_value(h, h1, "key", val, strlen(val));
    return SUCCESS;
}

static enum test_result test_restart_session_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void* cookie = createTapConn(h, h1, "tap_client_thread");
    testHarness.destroy_cookie(cookie);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    cookie = createTapConn(h, h1, "tap_client_thread");

    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string val = vals["eq_tapq:tap_client_thread:backfill_completed"];
    check(strcmp(val.c_str(), "true") == 0, "Don't expect the backfill upon restart");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_specialKeys(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    ENGINE_ERROR_CODE ret;

    // Simplified Chinese "Couchbase"
    static const char key0[] = "沙发数据库";
    static const char val0[] = "some Chinese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key0, val0, &i)) == ENGINE_SUCCESS,
          "Failed set Chinese key");
    check_key_value(h, h1, key0, val0, strlen(val0));
    h1->release(h, NULL, i);
    // Traditional Chinese "Couchbase"
    static const char key1[] = "沙發數據庫";
    static const char val1[] = "some Traditional Chinese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key1, val1, &i)) == ENGINE_SUCCESS,
          "Failed set Traditional Chinese key");
    h1->release(h, NULL, i);
    // Korean "couch potato"
    static const char key2[] = "쇼파감자";
    static const char val2[] = "some Korean value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key2, val2, &i)) == ENGINE_SUCCESS,
          "Failed set Korean key");
    h1->release(h, NULL, i);
    // Russian "couch potato"
    static const char key3[] = "лодырь, лентяй";
    static const char val3[] = "some Russian value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key3, val3, &i)) == ENGINE_SUCCESS,
          "Failed set Russian key");
    h1->release(h, NULL, i);
    // Japanese "couch potato"
    static const char key4[] = "カウチポテト";
    static const char val4[] = "some Japanese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key4, val4, &i)) == ENGINE_SUCCESS,
          "Failed set Japanese key");
    h1->release(h, NULL, i);
    // Indian char key, and no idea what it is
    static const char key5[] = "हरियानवी";
    static const char val5[] = "some Indian value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key5, val5, &i)) == ENGINE_SUCCESS,
          "Failed set Indian key");
    h1->release(h, NULL, i);
    // Portuguese translation "couch potato"
    static const char key6[] = "sedentário";
    static const char val6[] = "some Portuguese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key6, val6, &i)) == ENGINE_SUCCESS,
          "Failed set Portuguese key");
    h1->release(h, NULL, i);
    // Arabic translation "couch potato"
    static const char key7[] = "الحافلةالبطاطة";
    static const char val7[] = "some Arabic value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key7, val7, &i)) == ENGINE_SUCCESS,
          "Failed set Arabic key");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check_key_value(h, h1, key0, val0, strlen(val0));
    check_key_value(h, h1, key1, val1, strlen(val1));
    check_key_value(h, h1, key2, val2, strlen(val2));
    check_key_value(h, h1, key3, val3, strlen(val3));
    check_key_value(h, h1, key4, val4, strlen(val4));
    check_key_value(h, h1, key5, val5, strlen(val5));
    check_key_value(h, h1, key6, val6, strlen(val6));
    check_key_value(h, h1, key7, val7, strlen(val7));
    return SUCCESS;
}

static enum test_result test_binKeys(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    ENGINE_ERROR_CODE ret;

    // binary key with char values beyond 0x7F
    static const char key0[] = "\xe0\xed\xf1\x6f\x7f\xf8\xfa";
    static const char val0[] = "some value val8";
    check((ret = store(h, h1, NULL, OPERATION_SET, key0, val0, &i)) == ENGINE_SUCCESS,
          "Failed set binary key0");
    check_key_value(h, h1, key0, val0, strlen(val0));
    h1->release(h, NULL, i);
    // binary keys with char values beyond 0x7F
    static const char key1[] = "\xf1\xfd\xfe\xff\xf0\xf8\xef";
    static const char val1[] = "some value val9";
    check((ret = store(h, h1, NULL, OPERATION_SET, key1, val1, &i)) == ENGINE_SUCCESS,
          "Failed set binary key1");
    check_key_value(h, h1, key1, val1, strlen(val1));
    h1->release(h, NULL, i);
    // binary keys with special utf-8 BOM (Byte Order Mark) values 0xBB 0xBF 0xEF
    static const char key2[] = "\xff\xfe\xbb\xbf\xef";
    static const char val2[] = "some utf-8 bom value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key2, val2, &i)) == ENGINE_SUCCESS,
          "Failed set binary utf-8 bom key");
    check_key_value(h, h1, key2, val2, strlen(val2));
    h1->release(h, NULL, i);
    // binary keys with special utf-16BE BOM values "U+FEFF"
    static const char key3[] = "U+\xfe\xff\xefU+\xff\xfe";
    static const char val3[] = "some utf-16 bom value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key3, val3, &i)) == ENGINE_SUCCESS,
          "Failed set binary utf-16 bom key");
    check_key_value(h, h1, key3, val3, strlen(val3));
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check_key_value(h, h1, key0, val0, strlen(val0));
    check_key_value(h, h1, key1, val1, strlen(val1));
    check_key_value(h, h1, key2, val2, strlen(val2));
    check_key_value(h, h1, key3, val3, strlen(val3));
    return SUCCESS;
}

static enum test_result test_restart_bin_val(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {



    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
    return SUCCESS;
}

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_key(h, h1, "key", 1),
          "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_get_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    item *i = NULL;
    check(ENGINE_EWOULDBLOCK == h1->get(h, cookie, &i, "key", strlen("key"), 1),
          "Expected woodblock.");
    h1->release(h, NULL, i);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_key(h, h1, "key", 1),
          "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t result;
    item *i = NULL;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    h1->release(h, NULL, i);
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_incr_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    uint64_t result;
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         1) == ENGINE_EWOULDBLOCK,
          "Expected wouldblock.");
    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_incr_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t result;
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my bucket.");
    h1->release(h, NULL, i);
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_wrong_vb_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_wrong_vb_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_wrong_vb_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_wrong_vb_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_wrong_vb_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_wrong_vb_del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == del(h, h1, "key", 0, 1), "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_expiry_pager_settings(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    cb_assert(!get_bool_stat(h, h1, "ep_exp_pager_enabled"));
    checkeq(3600, get_int_stat(h, h1, "ep_exp_pager_stime"),
            "Expiry pager sleep time not expected");
    set_param(h, h1, protocol_binary_engine_param_flush,
              "exp_pager_stime", "1");
    checkeq(1, get_int_stat(h, h1, "ep_exp_pager_stime"),
            "Expiry pager sleep time not updated");
    cb_assert(!get_bool_stat(h, h1, "ep_exp_pager_enabled"));
    sleep(1);
    checkeq(0, get_int_stat(h, h1, "ep_num_expiry_pager_runs"),
            "Expiry pager run count is not zero");

    set_param(h, h1, protocol_binary_engine_param_flush,
              "exp_pager_enabled", "true");
    checkeq(1, get_int_stat(h, h1, "ep_exp_pager_stime"),
            "Expiry pager sleep time not updated");
    wait_for_stat_to_be_gte(h, h1, "ep_num_expiry_pager_runs", 1);

    // Reload engine
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    cb_assert(!get_bool_stat(h, h1, "ep_exp_pager_enabled"));

    // Enable expiry pager again
    set_param(h, h1, protocol_binary_engine_param_flush,
              "exp_pager_enabled", "true");

    checkeq(get_int_stat(h, h1, "ep_exp_pager_initial_run_time"), -1,
            "Task time should be disable upon warmup");

    std::string err_msg;
    // Update exp_pager_initial_run_time and ensure the update is successful
    set_param(h, h1, protocol_binary_engine_param_flush,
              "exp_pager_initial_run_time", "3");
    std::string expected_time = "03:00";
    std::string str = get_str_stat(h, h1, "ep_expiry_pager_task_time");
    err_msg.assign("Updated time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update exp_pager_stime by 30 minutes and ensure that the update is successful
    int update_by = 30;
    time_t now = time(NULL);
    struct tm curr = *(gmtime(&now));
    curr.tm_min += update_by;
#ifdef _MSC_VER
    _mkgmtime(&curr);
#else
    timegm(&curr);
#endif
    char timeStr[20];
    strftime(timeStr, 20, "%Y-%m-%d %H:%M:%S", &curr);
    std::string targetTaskTime1(timeStr);

    set_param(h, h1, protocol_binary_engine_param_flush, "exp_pager_stime",
              std::to_string(update_by * 60).c_str());
    str = get_str_stat(h, h1, "ep_expiry_pager_task_time");

    now = time(NULL);
    curr = *(gmtime(&now));
    curr.tm_min += update_by;
#ifdef _MSC_VER
    _mkgmtime(&curr);
#else
    timegm(&curr);
#endif
    strftime(timeStr, 20, "%Y-%m-%d %H:%M:%S", &curr);
    std::string targetTaskTime2(timeStr);

    // ep_expiry_pager_task_time should fall within the range of
    // targetTaskTime1 and targetTaskTime2
    err_msg.assign("Unexpected task time range, expect: " +
                   targetTaskTime1 + " <= " + str + " <= " + targetTaskTime2);
    check(targetTaskTime1 <= str, err_msg.c_str());
    check(str <= targetTaskTime2, err_msg.c_str());

    return SUCCESS;
}

static enum test_result test_expiry(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(5);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    int expired_access = get_int_stat(h, h1, "ep_expired_access");
    int expired_pager = get_int_stat(h, h1, "ep_expired_pager");
    int active_expired = get_int_stat(h, h1, "vb_active_expired");
    check(expired_pager == 0, "Expected zero expired item by pager");
    check(expired_access == 1, "Expected an expired item on access");
    check(active_expired == 1, "Expected an expired active item");
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, key, data, &it),
            "Failed set.");
    h1->release(h, NULL, it);

    std::stringstream ss;
    ss << "curr_items stat should be still 1 after ";
    ss << "overwriting the key that was expired, but not purged yet";
    checkeq(1, get_int_stat(h, h1, "curr_items"), ss.str().c_str());

    return SUCCESS;
}

static enum test_result test_expiry_loader(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_loader";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    // Restart the engine to ensure the above expired item is not loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    cb_assert(0 == get_int_stat(h, h1, "ep_warmup_value_count", "warmup"));

    return SUCCESS;
}

static enum test_result test_expiration_on_compaction(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    if (get_bool_stat(h, h1, "ep_exp_pager_enabled")) {
        set_param(h, h1, protocol_binary_engine_param_flush,
                  "exp_pager_enabled", "false");
    }

    for (int i = 0; i < 50; i++) {
        item *itm = NULL;
        std::stringstream ss;
        ss << "key" << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "somevalue", &itm, 0, 0, 10,
                    PROTOCOL_BINARY_RAW_BYTES) == ENGINE_SUCCESS,
                "Set failed.");
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 50,
            "Unexpected number of items on database");

    testHarness.time_travel(15);

    // Compaction on VBucket
    compact_db(h, h1, 0, 0, 0, 0);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    check(get_int_stat(h, h1, "ep_expired_compactor") == 50,
            "Unexpected expirations by compactor");

    return SUCCESS;
}

static enum test_result test_expiration_on_warmup(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {

    set_param(h, h1, protocol_binary_engine_param_flush,
              "exp_pager_enabled", "false");
    int pager_runs = get_int_stat(h, h1, "ep_num_expiry_pager_runs");

    const char *key = "KEY";
    const char *data = "VALUE";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 3,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "curr_items") == 1, "Failed store item");
    testHarness.time_travel(5);

    check(get_int_stat(h, h1, "ep_num_expiry_pager_runs") == pager_runs,
          "Expiry pager shouldn't have run during this time");

    // Restart the engine to ensure the above item is expired
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check(get_bool_stat(h, h1, "ep_exp_pager_enabled"),
          "Expiry pager should be enabled on warmup");
    pager_runs = get_int_stat(h, h1, "ep_num_expiry_pager_runs");
    wait_for_stat_change(h, h1, "ep_num_expiry_pager_runs", pager_runs);
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 0,
            "The item should have been expired.");

    return SUCCESS;

}

static enum test_result test_bug3454(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_duplicate_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 5,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Advance the ep_engine time by 10 sec for the above item to be expired.
    testHarness.time_travel(10);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    cas = 0;
    // Add a new item with the same key.
    rv = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(rv == ENGINE_SUCCESS, "Add failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_SUCCESS,
          "Item shouldn't expire");
    h1->release(h, NULL, it);

    // Restart the engine to ensure the above unexpired new item is loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    cb_assert(1 == get_int_stat(h, h1, "ep_warmup_value_count", "warmup"));
    cb_assert(0 == get_int_stat(h, h1, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static enum test_result test_bug3522(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_no_items_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Add a new item with the same key and 2 sec of expiration.
    const char *new_data = "new data here.";
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(new_data), 0, 2,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, new_data, strlen(new_data));

    int pager_runs = get_int_stat(h, h1, "ep_num_expiry_pager_runs");
    cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, new_data, strlen(new_data));
    h1->release(h, NULL, it);
    testHarness.time_travel(3);
    wait_for_stat_change(h, h1, "ep_num_expiry_pager_runs", pager_runs);
    wait_for_flusher_to_settle(h, h1);

    // Restart the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    // TODO: modify this for a better test case
    cb_assert(0 == get_int_stat(h, h1, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static enum test_result test_get_replica_active_state(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_active);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");

    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica_pending_state(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;

    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    pkt = prepare_get_replica(h, h1, vbucket_state_pending);
    check(h1->unknown_command(h, cookie, pkt, add_response) ==
          ENGINE_EWOULDBLOCK, "Should have returned error for pending state");
    testHarness.destroy_cookie(cookie);
    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica_dead_state(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_dead);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");

    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
                              "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected PROTOCOL_BINARY_RESPONSE_SUCCESS response.");
    check(last_body == "replicadata",
          "Should have returned identical value");

    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica_non_resident(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "value", &i, 0, 0)
          == ENGINE_SUCCESS, "Store Failed");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);

    evict_key(h, h1, "key", 0, "Ejected.");
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket to replica");

    get_replica(h, h1, "key", 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    return SUCCESS;
}

static enum test_result test_get_replica_invalid_key(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    bool makeinvalidkey = true;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica, makeinvalidkey);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");
    free(pkt);
    return SUCCESS;
}

static enum test_result test_vb_del_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(ENGINE_EWOULDBLOCK == del(h, h1, "key", 0, 1, cookie),
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == del(h, h1, "key", 0, 1),
          "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    touch(h, h1, NULL, 0, (time(NULL) + 10));
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_TOUCH, 0, 0, NULL, 0, "akey", 4);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");
    free(request);

    // Try to touch an unknown item...
    touch(h, h1, "mykey", 0, (time(NULL) + 10));
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    touch(h, h1, "mykey", 5, (time(NULL) + 10));
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue"));

    touch(h, h1, "mykey", 0, (time(NULL) + 10));
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch mykey");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_touch_mb7342(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    const char *key = "MB-7342";
    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "v", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    touch(h, h1, key, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch key");

    check_key_value(h, h1, key, "v", 1);

    // Travel a loong time to see if the object is still there (the default
    // store sets an exp time of 3600
    testHarness.time_travel(3700);

    check_key_value(h, h1, key, "v", 1);

    return SUCCESS;
}

static enum test_result test_touch_mb10277(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    const char *key = "MB-10277";
    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "v", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    touch(h, h1, key, 0, 3600); // A new expiration time remains in the same.
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch key");

    return SUCCESS;
}

static enum test_result test_gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    gat(h, h1, NULL, 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_GAT, 0, 0, NULL, 0, "akey", 4);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");
    free(request);

    // Try to gat an unknown item...
    gat(h, h1, "mykey", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    gat(h, h1, "mykey", 5, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
            strlen("{\"some\":\"value\"}"));

    gat(h, h1, "mykey", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(last_datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Expected datatype to be JSON");
    check(last_body.compare(0, sizeof("{\"some\":\"value\"}"),
                            "{\"some\":\"value\"}") == 0,
          "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
                    strlen("{\"some\":\"value\"}"));

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gatq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    gat(h, h1, NULL, 0, 10, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_GATQ, 0, 0, NULL, 0, "akey", 4);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gatq");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");
    free(request);

    // Try to gatq an unknown item...
    last_status = static_cast<protocol_binary_response_status>(0xffff);
    gat(h, h1, "mykey", 0, 10, true);

    // We should not have sent any response!
    check(last_status == (protocol_binary_response_status)0xffff, "Testing unknown key");

    // illegal vbucket
    gat(h, h1, "mykey", 5, 10, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
                    strlen("{\"some\":\"value\"}"));

    gat(h, h1, "mykey", 0, 10, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(last_datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Expected datatype to be JSON");
    check(last_body.compare(0, sizeof("{\"some\":\"value\"}"),
                            "{\"some\":\"value\"}") == 0,
          "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
                    strlen("{\"some\":\"value\"}"));

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_mb5215(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "coolkey", "cooler", &itm)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "coolkey", "cooler", strlen("cooler"));

    // set new exptime to 111
    int expTime = time(NULL) + 111;

    touch(h, h1, "coolkey", 0, expTime);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch coolkey");

    //reload engine
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);

    //verify persisted expiration time
    const char *statkey = "key coolkey 0";
    int newExpTime;
    check(h1->get(h, NULL, &itm, "coolkey", 7, 0) == ENGINE_SUCCESS,
          "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    check(newExpTime == expTime, "Failed to persist new exptime");

    // evict key, touch expiration time, and verify
    evict_key(h, h1, "coolkey", 0, "Ejected.");

    expTime = time(NULL) + 222;
    touch(h, h1, "coolkey", 0, expTime);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch coolkey");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(h1->get(h, NULL, &itm, "coolkey", 7, 0) == ENGINE_SUCCESS,
          "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    check(newExpTime == expTime, "Failed to persist new exptime");

    return SUCCESS;
}

static enum test_result test_alloc_limit(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    ENGINE_ERROR_CODE rv;

    rv = h1->allocate(h, NULL, &it, "key", 3, 20 * 1024 * 1024, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_SUCCESS, "Allocated 20MB item");
    h1->release(h, NULL, it);

    rv = h1->allocate(h, NULL, &it, "key", 3, (20 * 1024 * 1024) + 1, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    check(rv == ENGINE_E2BIG, "Object too big");

    return SUCCESS;
}

static enum test_result test_whitespace_db(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals["ep_dbname"] != std::string(WHITESPACE_DB)) {
        std::cerr << "Expected dbname = ``" << WHITESPACE_DB << "''"
                  << ", got ``" << vals["ep_dbname"] << "''" << std::endl;
        return FAIL;
    }

    check(access(WHITESPACE_DB, F_OK) != -1, "I expected the whitespace db to exist");
    return SUCCESS;
}

static enum test_result test_memory_tracking(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    // Need memory tracker to be able to check our memory usage.
    std::string tracker = get_str_stat(h, h1, "ep_mem_tracker_enabled");
    if (tracker == "true") {
        return SUCCESS;
    } else {
        std::cerr << "Memory tracker not enabled ...";
        return SKIPPED;
    }
}

static enum test_result test_memory_limit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(10240000, get_int_stat(h, h1, "ep_max_size"), "Max size not at 10MB");
    set_param(h, h1, protocol_binary_engine_param_flush, "mutation_mem_threshold", "95");
    wait_for_stat_change(h, h1,"ep_db_data_size", 0);
    check(get_int_stat(h, h1, "ep_oom_errors") == 0 &&
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 0, "Expected no OOM errors.");

    size_t vlen = 4 * 1024 * 1024;
    char *data = new char[vlen + 1]; // +1 for terminating '\0' byte
    cb_assert(data);
    memset(data, 'x', vlen);
    data[vlen] = '\0';

    item *i = NULL;
    // So if we add an item,
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", data, &i),
            "store failure");
    check_key_value(h, h1, "key", data, vlen);
    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);

    // Set max_size equal to used memory, so that the next store operation
    // would throw an ENOMEM/ETMPFAIL.
    int new_max_size = get_int_stat(h, h1, "mem_used");
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              std::to_string(new_max_size).c_str());

    int num_pager_runs = get_int_stat(h, h1, "ep_num_pager_runs");
    int num_ejects = get_int_stat(h, h1, "ep_num_value_ejects");

    i = NULL;
    // There should be no room for another.
    ENGINE_ERROR_CODE second = store(h, h1, NULL, OPERATION_SET, "key2", data, &i);
    check(second == ENGINE_ENOMEM || second == ENGINE_TMPFAIL,
          "should have failed second set");
    if (i) {
        h1->release(h, NULL, i);
        i = NULL;
    }
    check(get_int_stat(h, h1, "ep_oom_errors") == 1 ||
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 1, "Expected an OOM error.");

    // Consider the number of ejects to estimate the outcome of the next
    // store operation, as the previous one that failed because of ENOMEM
    // would've woken up the item-pager.
    bool opToSucceed = false;
    if (get_int_stat(h, h1, "ep_num_pager_runs") > num_pager_runs &&
        get_int_stat(h, h1, "ep_num_value_ejects") > num_ejects) {
        opToSucceed = true;
    }
    ENGINE_ERROR_CODE overwrite = store(h, h1, NULL, OPERATION_SET, "key", data, &i);
    if (opToSucceed) {
        checkeq(ENGINE_SUCCESS,
                overwrite,
                "Item pager cleared up memory but this op still failed");
    } else {
        check(overwrite == ENGINE_ENOMEM || overwrite == ENGINE_TMPFAIL,
              "should have failed second override");
    }

    if (i) {
        h1->release(h, NULL, i);
        i = NULL;
    }

    if (overwrite != ENGINE_SUCCESS) {
        check(get_int_stat(h, h1, "ep_oom_errors") == 2 ||
              get_int_stat(h, h1, "ep_tmp_oom_errors") == 2,
              "Expected another OOM error.");
    }

    check_key_value(h, h1, "key", data, vlen);
    check(ENGINE_SUCCESS != verify_key(h, h1, "key2"), "Expected a failure in GET");
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Until we remove that item
    checkeq(ENGINE_SUCCESS, del(h, h1, "key", 0, 0), "Failed remove with value.");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    wait_for_flusher_to_settle(h, h1);

    checkeq(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue2", &i),
            ENGINE_SUCCESS,
            "should have succeded on the last set");
    check_key_value(h, h1, "key2", "somevalue2", 10);
    h1->release(h, NULL, i);
    delete []data;
    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_missing(h, h1, 1) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_state(h, h1, 0, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!verify_vbucket_missing(h, h1, 1)) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, h1, 1, vbucket_state_active)) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    return verify_vbucket_state(h, h1, 1, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_compact(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    const char *key = "Carss";
    const char *value = "pollute";
    if (!verify_vbucket_missing(h, h1, 0)) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, h1, 0, vbucket_state_active)) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    check(verify_vbucket_state(h, h1, 0, vbucket_state_active),
            "VBucket state not active");

    // Set two keys - one to be expired and other to remain...
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, value, &itm)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, key, value, strlen(value));

    // Set a non-expiring key...
    check(store(h, h1, NULL, OPERATION_SET, "trees", "cleanse", &itm)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "trees", "cleanse", strlen("cleanse"));

    touch(h, h1, key, 0, 11);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch Carss");

    testHarness.time_travel(12);
    wait_for_flusher_to_settle(h, h1);

    // Store a dummy item since we do not purge the item with highest seqno
    check(ENGINE_SUCCESS ==
            store(h, h1, NULL, OPERATION_SET, "dummykey", "dummyvalue", &itm,
                0, 0, 0), "Error setting.");
    h1->release(h, NULL, itm);

    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") == 0,
            "purge_seqno not found to be zero before compaction");

    // Compaction on VBucket
    compact_db(h, h1, 0, 2, 3, 1);

    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    // the key tree and its value should be intact...
    check(verify_key(h, h1, "trees") == ENGINE_SUCCESS,
          "key trees should be found.");
    // the key Carrs should have disappeared...
    int val = verify_key(h, h1, "Carss");
    check(val == ENGINE_KEY_ENOENT, "Key Carss has not expired.");

    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") == 4,
        "purge_seqno didn't match expected value");

    return SUCCESS;
}

static enum test_result test_compaction_config(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    check(get_int_stat(h, h1, "ep_compaction_write_queue_cap") == 10000,
            "Expected compaction queue cap to be 10000");
    set_param(h, h1, protocol_binary_engine_param_flush,
              "compaction_write_queue_cap", "100000");
    check(get_int_stat(h, h1, "ep_compaction_write_queue_cap") == 100000,
            "Expected compaction queue cap to be 100000");
    return SUCCESS;
}

struct comp_thread_ctx {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    uint16_t vbid;
};

extern "C" {
    static void compaction_thread(void *arg) {
        struct comp_thread_ctx *ctx = static_cast<comp_thread_ctx *>(arg);
        compact_db(ctx->h, ctx->h1, ctx->vbid, 0, 0, 0);
    }
}

static enum test_result test_multiple_vb_compactions(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    for (uint16_t i = 0; i < 4; ++i) {
        if (!set_vbucket_state(h, h1, i, vbucket_state_active)) {
            fprintf(stderr, "set state failed for vbucket %d.\n", i);
            return FAIL;
        }
        check(verify_vbucket_state(h, h1, i, vbucket_state_active),
              "VBucket state not active");
    }

    std::vector<std::string> keys;
    for (int j = 0; j < 20000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int count = 0;
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        uint16_t vbid = count % 4;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, vbid)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
        ++count;
    }

    // Compact multiple vbuckets.
    const int n_threads = 4;
    cb_thread_t threads[n_threads];
    struct comp_thread_ctx ctx[n_threads];

    for (int i = 0; i < n_threads; i++) {
        ctx[i].h = h;
        ctx[i].h1 = h1;
        ctx[i].vbid = static_cast<uint16_t>(i);
        int r = cb_create_thread(&threads[i], compaction_thread, &ctx[i], 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    return SUCCESS;
}

static enum test_result
test_multi_vb_compactions_with_workload(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    for (uint16_t i = 0; i < 4; ++i) {
        if (!set_vbucket_state(h, h1, i, vbucket_state_active)) {
            fprintf(stderr, "set state failed for vbucket %d.\n", i);
            return FAIL;
        }
        check(verify_vbucket_state(h, h1, i, vbucket_state_active),
              "VBucket state not active");
    }

    std::vector<std::string> keys;
    for (int j = 0; j < 10000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int count = 0;
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        uint16_t vbid = count % 4;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, vbid)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
        ++count;
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < 2; ++i) {
        count = 0;
        for (it = keys.begin(); it != keys.end(); ++it) {
            uint16_t vbid = count % 4;
            item *i = NULL;
            check(h1->get(h, NULL, &i, it->c_str(), strlen(it->c_str()), vbid) ==
                  ENGINE_SUCCESS, "Unable to get stored item");
            h1->release(h, NULL, i);
            ++count;
        }
    }
    wait_for_str_stat_to_be(h, h1, "ep_workload_pattern", "read_heavy", NULL);

    // Compact multiple vbuckets.
    const int n_threads = 4;
    cb_thread_t threads[n_threads];
    struct comp_thread_ctx ctx[n_threads];

    for (int i = 0; i < n_threads; i++) {
        ctx[i].h = h;
        ctx[i].h1 = h1;
        ctx[i].vbid = static_cast<uint16_t>(i);
        int r = cb_create_thread(&threads[i], compaction_thread, &ctx[i], 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    return SUCCESS;
}

static enum test_result vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                             const char* value = NULL) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    vbucketDelete(h, h1, 2, value);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected failure deleting non-existent bucket.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    vbucketDelete(h, h1, 1, value);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 0 was not missing after deleting it.");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_stats(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    int vbucketDel = get_int_stat(h, h1, "ep_vbucket_del");
    vbucketDelete(h, h1, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    wait_for_stat_change(h, h1, "ep_vbucket_del", vbucketDel);

    wait_for_stat_to_be(h, h1, "ep_total_cache_size", cacheSize);
    wait_for_stat_to_be(h, h1, "ep_overhead", overhead);
    wait_for_stat_to_be(h, h1, "ep_num_non_resident", nonResident);

    return SUCCESS;
}

static enum test_result vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                const char* value = NULL) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Store a value so the restart will try to resurrect it.
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 1)
          == ENGINE_SUCCESS, "Failed to set a value");
    check_key_value(h, h1, "key", "somevalue", 9, 1);
    h1->release(h, NULL, i);

    // Reload to get a flush forced.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(verify_vbucket_state(h, h1, 1, vbucket_state_active),
          "Bucket state was what it was initially, after restart.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");
    check_key_value(h, h1, "key", "somevalue", 9, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    vbucketDelete(h, h1, 1, value);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    if (verify_vbucket_state(h, h1, 1, vbucket_state_pending, true)) {
        std::cerr << "Bucket came up in pending state after delete." << std::endl;
        abort();
    }

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after restart.");

    return SUCCESS;
}

static enum test_result test_async_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy(h, h1);
}

static enum test_result test_sync_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy(h, h1, "async=0");
}

static enum test_result test_async_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy_restart(h, h1);
}

static enum test_result test_sync_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy_restart(h, h1, "async=0");
}

static enum test_result test_vb_set_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_add_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_vb_set_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_replace_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_replace_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_add_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_stats_seqno(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

    int num_keys = 100;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    check(get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno") == 100,
          "Invalid seqno");
    check(get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno") == 0,
          "Invalid seqno");
    check(get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno 1") == 0,
          "Invalid seqno");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");
    check(get_ull_stat(h, h1, "vb_1:uuid", "vbucket-seqno 1") == vb_uuid,
          "Invalid uuid");
    check(vals.size() == 4, "Expected four stats");

    // Check invalid vbucket
    check(h1->get_stats(h, NULL, "vbucket-seqno 2", 15, add_stats)
          == ENGINE_NOT_MY_VBUCKET, "Expected not my vbucket");

    // Check bad vbucket parameter (not numeric)
    check(h1->get_stats(h, NULL, "vbucket-seqno tt2", 17, add_stats)
          == ENGINE_EINVAL, "Expected invalid");

    // Check extra spaces at the end
    check(h1->get_stats(h, NULL, "vbucket-seqno    ", 17, add_stats)
          == ENGINE_EINVAL, "Expected invalid");

    return SUCCESS;
}

static enum test_result test_stats_diskinfo(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

    int num_keys = 100;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 1) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    size_t file_size = get_int_stat(h, h1, "ep_db_file_size", "diskinfo");
    size_t data_size = get_int_stat(h, h1, "ep_db_data_size", "diskinfo");
    check(file_size > 0, "DB file size should be greater than 0");
    check(data_size > 0, "DB data size should be greater than 0");
    check(file_size >= data_size, "DB file size should be >= DB data size");
    check(get_int_stat(h, h1, "vb_1:data_size", "diskinfo detail") > 0,
          "VB 1 data size should be greater than 0");

    check(h1->get_stats(h, NULL, "diskinfo ", 9, add_stats)
          == ENGINE_EINVAL, "Expected invalid");

    check(h1->get_stats(h, NULL, "diskinfo detai", 14, add_stats)
          == ENGINE_EINVAL, "Expected invalid");

    check(h1->get_stats(h, NULL, "diskinfo detaillll", 18, add_stats)
          == ENGINE_EINVAL, "Expected invalid");

    return SUCCESS;
}

static void notifier_request(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const void* cookie, uint32_t opaque,
                             uint16_t vbucket, uint64_t start,
                             bool shouldSucceed) {

    uint32_t flags = 0;
    uint64_t rollback = 0;
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = get_ull_stat(h, h1, "vb_0:0:seq", "failovers");
    uint64_t snap_end_seqno = snap_start_seqno;
    ENGINE_ERROR_CODE err = h1->dcp.stream_req(h, cookie, flags, opaque,
                                               vbucket, start, 0, vb_uuid,
                                               snap_start_seqno, snap_end_seqno,
                                               &rollback,
                                               mock_dcp_add_failover_log);
    check(err == ENGINE_SUCCESS, "Failed to initiate stream request");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("notifier") == 0, "Consumer not found");

    check((uint32_t)get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_flags", "dcp")
          == flags, "Flags didn't match");
    check((uint32_t)get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp")
          == opaque, "Opaque didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_start_seqno", "dcp")
          == start, "Start Seqno Didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_end_seqno", "dcp")
          == 0, "End Seqno didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_vb_uuid", "dcp")
          == vb_uuid, "VBucket UUID didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_snap_start_seqno", "dcp")
          == snap_start_seqno, "snap start seqno didn't match");
}

static enum test_result test_dcp_vbtakeover_no_stream(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    int est = get_int_stat(h, h1, "estimate", "dcp-vbtakeover 0");
    check(est == 10, "Invalid estimate for non-existent stream");

    check(h1->get_stats(h, NULL, "dcp-vbtakeover 1", strlen("dcp-vbtakeover 1"),
                        add_stats) == ENGINE_NOT_MY_VBUCKET,
                        "Expected not my vbucket");

    return SUCCESS;
}

static enum test_result test_dcp_notifier(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t flags = DCP_OPEN_NOTIFIER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp notifier open connection.");

    // Get notification for an old item
    notifier_request(h, h1, cookie, ++opaque, 0, 0, true);
    dcp_step(h, h1, cookie);
    check(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Expected stream end");

    // Get notification when we're slightly behind
    notifier_request(h, h1, cookie, ++opaque, 0, 9, true);
    dcp_step(h, h1, cookie);
    check(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Expected stream end");

    // Wait for notification of a future item
    notifier_request(h, h1, cookie, ++opaque, 0, 20, true);
    dcp_step(h, h1, cookie);

    for (int j = 0; j < 5; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    // Shouldn't get a stream end yet
    dcp_step(h, h1, cookie);
    check(dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Wasn't expecting a stream end");

    for (int j = 0; j < 6; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    // Should get a stream end
    dcp_step(h, h1, cookie);
    check(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Expected stream end");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_open(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    int created = get_int_stat(h, h1, "eq_dcpq:unittest:created", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");
    testHarness.destroy_cookie(cookie1);

    testHarness.time_travel(600);

    const void *cookie2 = testHarness.create_cookie();
    check(h1->dcp.open(h, cookie2, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");
    check(get_int_stat(h, h1, "eq_dcpq:unittest:created", "dcp") > created,
          "New dcp stream is not newer");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_none(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    std::string name("unittest");
    std::string stats_buffer("eq_dcpq:" + name + ":max_buffer_bytes");

    checkeq(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name.c_str(),
                         name.length()), ENGINE_SUCCESS,
            "Failed dcp consumer open connection.");

    checkeq((uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"), (uint32_t)0,
            "Flow Control Buffer Size not zero");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_static(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    const uint32_t flow_ctl_buf_def_size = 10485760;
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    std::string name("unittest");
    std::string stats_buffer("eq_dcpq:" + name + ":max_buffer_bytes");

    checkeq(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name.c_str(),
                         name.length()), ENGINE_SUCCESS,
            "Failed dcp consumer open connection.");

    checkeq((uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"),
            flow_ctl_buf_def_size,
            "Flow Control Buffer Size not equal to default value");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_dynamic(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    char stats_buffer[50];

    snprintf(stats_buffer, sizeof(stats_buffer),
             "eq_dcpq:%s:max_buffer_bytes", name);

    /* Check the min limit */
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "500000000");
    check(get_int_stat(h, h1, "ep_max_size") == 500000000,
          "Incorrect new size.");

    check(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp")
          == 10485760, "Flow Control Buffer Size not equal to min");
    testHarness.destroy_cookie(cookie1);

    /* Check the size as percentage of the bucket memory */
    const void *cookie2 = testHarness.create_cookie();
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "2000000000");
    check(get_int_stat(h, h1, "ep_max_size") == 2000000000,
          "Incorrect new size.");

    check(h1->dcp.open(h, cookie2, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp")
          == 20000000, "Flow Control Buffer Size not equal to 1% of mem size");
    testHarness.destroy_cookie(cookie2);

    /* Check the case when mem used by flow control bufs hit the threshold */
    /* Create around 10 more connections to use more than 10% of the total
       memory */
    for (int count = 0; count < 10; count++) {
        const void *cookie = testHarness.create_cookie();
        check(h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname)
              == ENGINE_SUCCESS,
              "Failed dcp consumer open connection.");
        testHarness.destroy_cookie(cookie);
    }
    /* By now mem used by flow control bufs would have crossed the threshold */
    const void *cookie3 = testHarness.create_cookie();
    check(h1->dcp.open(h, cookie3, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp") == 10485760,
          "Flow Control Buffer Size not equal to min after threshold is hit");
    testHarness.destroy_cookie(cookie3);

    /* Check the max limit */
    const void *cookie4 = testHarness.create_cookie();
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "7000000000");
    check(get_ull_stat(h, h1, "ep_max_size") == 7000000000,
          "Incorrect new size.");

    check(h1->dcp.open(h, cookie4, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp consumer open connection.");

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp")
          == 52428800, "Flow Control Buffer Size beyond max");
    testHarness.destroy_cookie(cookie4);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_aggressive(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const size_t ep_max_size = 1200000000;
    const uint32_t flow_ctl_buf_min = 10485760, flow_ctl_buf_max = 52428800;
    const double bucketMemQuotaFraction = 0.05;
    const int8_t max_conns = 6;
    const void *cookie[max_conns] = {NULL};

    uint32_t opaque = 0, seqno = 0, flags = 0;
    std::string name("unittest_");

    /* Create first connection */
    name += std::to_string(0);
    std::string stats_buffer("eq_dcpq:" + name + ":max_buffer_bytes");

    std::string ep_max_size_buf(std::to_string(ep_max_size));
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              ep_max_size_buf.c_str());
    checkeq(ep_max_size, (size_t)get_int_stat(h, h1, "ep_max_size"),
            "Incorrect new size.");

    /* Check the max limit */
    cookie[0] = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS, h1->dcp.open(h, cookie[0], opaque, seqno, flags,
                                         (void*)name.c_str(), name.length()),
            "Failed dcp consumer open connection.");
    checkeq(flow_ctl_buf_max,
            (uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to max");

    /* Create 4 more connections */
    for (int count = 1; count < max_conns-1; count++) {
        cookie[count] = testHarness.create_cookie();
        std::string name1("unittest_" + std::to_string(count));
        stats_buffer.clear();
        stats_buffer.append("eq_dcpq:" + name1 + ":max_buffer_bytes");
        checkeq(ENGINE_SUCCESS, h1->dcp.open(h, cookie[count], opaque, seqno,
                                             flags, (void*)name1.c_str(),
                                             name1.length()),
                "Failed dcp consumer open connection.");

        for (int i = 0; i <= count; i++) {
            /* Check if the buffer size of all connections has changed */
            std::string name2("unittest_" + std::to_string(i));
            stats_buffer.clear();
            stats_buffer.append("eq_dcpq:" + name2 + ":max_buffer_bytes");
            checkeq((uint32_t)((ep_max_size * bucketMemQuotaFraction)/
                               (count+1)),
                    (uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"),
                    "Flow Control Buffer Size not correct");
        }
    }

    /* Opening another connection should set the buffer size to min value */
    cookie[max_conns-1] = testHarness.create_cookie();
    name.clear();
    name.append("unittest_" +std::to_string((max_conns-1)));
    stats_buffer.clear();
    stats_buffer.append("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(ENGINE_SUCCESS, h1->dcp.open(h, cookie[max_conns-1], opaque, seqno,
                                         flags, (void*)name.c_str(),
                                         name.length()),
            "Failed dcp consumer open connection.");
    checkeq(flow_ctl_buf_min,
            (uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min");

    /* Disconnect connections and see if flow control buffer size of existing
       conns increase */
    for (int count = 0; count < max_conns/2; count++) {
        testHarness.destroy_cookie(cookie[count]);
    }
    /* Wait for disconnected connections to be deleted */
    wait_for_stat_to_be(h, h1, "ep_dcp_dead_conn_count", 0, "dcp");

    /* Check if the buffer size of all connections has increased */
    uint32_t exp_buf_size = (uint32_t)((ep_max_size * bucketMemQuotaFraction)/
                                       ((max_conns - max_conns/2)));
    /* Also check if we get control message indicating the flow control buffer
       size change from the consumer connections */
    struct dcp_message_producers* producers = get_dcp_producers(h, h1);

    for (int i = max_conns/2; i < max_conns; i++) {
        /* Check if the buffer size of all connections has changed */
        std::string name1("unittest_" + std::to_string(i));
        stats_buffer.clear();
        stats_buffer.append("eq_dcpq:" + name1 + ":max_buffer_bytes");
        checkeq(exp_buf_size,
                (uint32_t)get_int_stat(h, h1, stats_buffer.c_str(), "dcp"),
                "Flow Control Buffer Size not correct");
        checkeq(ENGINE_WANT_MORE, h1->dcp.step(h, cookie[i], producers),
                "Pending flow control buffer change not processed");
        checkeq((uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL, dcp_last_op,
                "Flow ctl buf size change control message not received");
        check(dcp_last_key.compare("connection_buffer_size") == 0,
              "Flow ctl buf size change control message key error");
        checkeq((int)exp_buf_size, atoi(dcp_last_value.c_str()),
                "Flow ctl buf size in control message not correct");
    }

    /* Disconnect remaining connections */
    for (int count = max_conns/2; count < max_conns; count++) {
        testHarness.destroy_cookie(cookie[count]);
    }
    return SUCCESS;
}

static enum test_result test_dcp_producer_open(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name  = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    int created = get_int_stat(h, h1, "eq_dcpq:unittest:created", "dcp");
    check(type.compare("producer") == 0, "Producer not found");
    testHarness.destroy_cookie(cookie1);

    testHarness.time_travel(600);

    const void *cookie2 = testHarness.create_cookie();
    check(h1->dcp.open(h, cookie2, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("producer") == 0, "Producer not found");
    check(get_int_stat(h, h1, "eq_dcpq:unittest:created", "dcp") > created,
          "New dcp stream is not newer");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_noop(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint32_t opaque = 1;

    check(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                       strlen(name)) == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "1024", 4) == ENGINE_SUCCESS,
          "Failed to establish connection buffer");

    check(h1->dcp.control(h, cookie, ++opaque, "enable_noop", 11, "true", 4)
                == ENGINE_SUCCESS,
          "Failed to enable no-ops");

    testHarness.time_travel(201);

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);
    bool done = false;
    while (!done) {
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_NOOP) {
                done = true;
                checkeq(1, get_int_stat(h, h1, "eq_dcpq:unittest:noop_wait", "dcp"),
                        "Didn't send noop");
                sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_NOOP,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                checkeq(0, get_int_stat(h, h1, "eq_dcpq:unittest:noop_wait", "dcp"),
                        "Didn't ack noop");
            } else if (dcp_last_op != 0) {
                abort();
            }

            dcp_last_op = 0;
        }
    }

    free(producers);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_noop_fail(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint32_t opaque = 1;

    check(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                       strlen(name)) == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "1024", 4) == ENGINE_SUCCESS,
          "Failed to establish connection buffer");

    check(h1->dcp.control(h, cookie, ++opaque, "enable_noop", 11, "true", 4)
                == ENGINE_SUCCESS,
          "Failed to enable no-ops");

    testHarness.time_travel(201);

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);
    bool done = false;
    bool disconnected = false;
    while (!done) {
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
            disconnected = true;
        } else {
            if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_NOOP) {
                checkeq(1, get_int_stat(h, h1, "eq_dcpq:unittest:noop_wait", "dcp"),
                        "Didn't send noop");
                testHarness.time_travel(201);
            } else if (dcp_last_op != 0) {
                abort();
            }

            dcp_last_op = 0;
        }
    }

    check(disconnected, "Connection should have been disconnected");

    free(producers);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static void dcp_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *name,
                       const void *cookie, uint16_t vbucket, uint32_t flags,
                       uint64_t start, uint64_t end, uint64_t vb_uuid,
                       uint64_t snap_start_seqno, uint64_t snap_end_seqno,
                       int exp_mutations, int exp_deletions, int exp_markers,
                       int extra_takeover_ops,
                       bool exp_disk_snapshot = false,
                       bool time_sync_enabled = false,
                       uint8_t exp_conflict_res = 0,
                       bool skipEstimateCheck = false,
                       uint64_t *total_bytes = NULL,
                       bool simulate_cursor_dropping = false) {
    uint32_t opaque = 1;
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                       nname) == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "1024", 4) == ENGINE_SUCCESS,
          "Failed to establish connection buffer");

    check(h1->dcp.control(h, cookie, ++opaque, "enable_ext_metadata", 19,
                          "true", 4) == ENGINE_SUCCESS,
          "Failed to enable xdcr extras");

    uint64_t rollback = 0;
    check(h1->dcp.stream_req(h, cookie, flags, opaque, vbucket, start, end,
                             vb_uuid, snap_start_seqno, snap_end_seqno, &rollback,
                             mock_dcp_add_failover_log)
                == ENGINE_SUCCESS,
          "Failed to initiate stream request");

    if (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        end  = -1;
    } else if (flags & DCP_ADD_STREAM_FLAG_LATEST ||
               flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        std::string high_seqno("vb_" + std::to_string(vbucket) + ":high_seqno");
        end = get_int_stat(h, h1, high_seqno.c_str(), "vbucket-seqno");
    }

    std::stringstream stats_flags;
    stats_flags << "eq_dcpq:" << name << ":stream_" << vbucket << "_flags";
    check((uint32_t)get_int_stat(h, h1, stats_flags.str().c_str(), "dcp")
          == flags, "Flags didn't match");

    std::stringstream stats_opaque;
    stats_opaque << "eq_dcpq:" << name << ":stream_" << vbucket << "_opaque";
    check((uint32_t)get_int_stat(h, h1, stats_opaque.str().c_str(), "dcp")
          == opaque, "Opaque didn't match");

    std::stringstream stats_start_seqno;
    stats_start_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_start_seqno";
    check((uint64_t)get_ull_stat(h, h1, stats_start_seqno.str().c_str(), "dcp")
          == start, "Start Seqno Didn't match");

    std::stringstream stats_end_seqno;
    stats_end_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_end_seqno";
    check((uint64_t)get_ull_stat(h, h1, stats_end_seqno.str().c_str(), "dcp")
          == end, "End Seqno didn't match");

    std::stringstream stats_vb_uuid;
    stats_vb_uuid << "eq_dcpq:" << name << ":stream_" << vbucket << "_vb_uuid";
    check((uint64_t)get_ull_stat(h, h1, stats_vb_uuid.str().c_str(), "dcp")
          == vb_uuid, "VBucket UUID didn't match");

    std::stringstream stats_snap_seqno;
    stats_snap_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_snap_start_seqno";
    check((uint64_t)get_ull_stat(h, h1, stats_snap_seqno.str().c_str(), "dcp")
          == snap_start_seqno, "snap start seqno didn't match");

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);

    if ((flags & DCP_ADD_STREAM_FLAG_TAKEOVER) == 0 &&
        (flags & DCP_ADD_STREAM_FLAG_DISKONLY) == 0 &&
        !skipEstimateCheck) {
        int est = end - start;
        std::stringstream stats_takeover;
        stats_takeover << "dcp-vbtakeover " << vbucket << " " << name;
        wait_for_stat_to_be(h, h1, "estimate", est, stats_takeover.str().c_str());
    }

    bool done = false;
    int num_mutations = 0;
    int num_deletions = 0;
    int num_snapshot_markers = 0;
    int num_set_vbucket_pending = 0;
    int num_set_vbucket_active = 0;

    ExtendedMetaData *emd = NULL;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    uint64_t last_by_seqno = 0;
    uint32_t bytes_read = 0;
    uint64_t all_bytes = 0;
    if (total_bytes) {
        all_bytes = *total_bytes;
    }

    bool delay_buffer_acking = false;
    if (simulate_cursor_dropping) {
        /**
         * Simulates cursor dropping by slowing down the initial buffer
         * acknowledgement from the consmer.
         *
         * Note that the cursor may not be dropped if the memory usage
         * is not over the cursor_dropping_upper_threshold or if the
         * checkpoint_remover sleep time is high.
         */
        delay_buffer_acking = true;
    }

    do {
        if (bytes_read > 512) {
            if (delay_buffer_acking) {
                sleep(2);
                delay_buffer_acking = false;
            }
            h1->dcp.buffer_acknowledgement(h, cookie, ++opaque, vbucket, bytes_read);
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    check(last_by_seqno < dcp_last_byseqno, "Expected bigger seqno");
                    last_by_seqno = dcp_last_byseqno;
                    num_mutations++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    if (time_sync_enabled) {
                        check(dcp_last_meta.size() == 16,
                                "Expected extended meta in mutation packet");
                    } else {
                        check(dcp_last_meta.size() == 5,
                                "Expected no extended metadata");
                    }

                    emd = new ExtendedMetaData(dcp_last_meta.c_str(),
                                               dcp_last_meta.size());
                    check(exp_conflict_res == emd->getConflictResMode(),
                              "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_DELETION:
                    check(last_by_seqno < dcp_last_byseqno, "Expected bigger seqno");
                    last_by_seqno = dcp_last_byseqno;
                    num_deletions++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    if (time_sync_enabled) {
                        check(dcp_last_meta.size() == 16,
                                "Expected adjusted time in mutation packet");
                    } else {
                        check(dcp_last_meta.size() == 5,
                                "Expected no extended metadata");
                    }

                    emd = new ExtendedMetaData(dcp_last_meta.c_str(),
                                               dcp_last_meta.size());
                    check(exp_conflict_res == emd->getConflictResMode(),
                              "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    if (exp_disk_snapshot && num_snapshot_markers == 0) {
                        check(dcp_last_flags == 1, "Expected disk snapshot");
                    }

                    if (dcp_last_flags & 8) {
                        pending_marker_ack = true;
                        marker_end = dcp_last_snap_end_seqno;
                    }

                    num_snapshot_markers++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
                    if (dcp_last_vbucket_state == vbucket_state_pending) {
                        num_set_vbucket_pending++;
                        for (int j = 0; j < extra_takeover_ops; ++j) {
                            item *i = NULL;
                            std::stringstream ss;
                            ss << "key" << j;
                            check(store(h, h1, NULL, OPERATION_SET,
                                        ss.str().c_str(), "data", &i,
                                        0, vbucket)
                                  == ENGINE_SUCCESS, "Failed to store a value");
                            h1->release(h, NULL, i);
                        }
                    } else if (dcp_last_vbucket_state == vbucket_state_active) {
                        num_set_vbucket_active++;
                    }
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    break;
                case 0:
                    /* No messages were ready on the last step call so we
                     * should just ignore this case. Note that we check for 0
                     * because we clear the dcp_last_op value below.
                     */
                     break;
                default:
                     // Aborting ...
                    std::stringstream ss;
                    ss << "Unknown DCP operation: " << dcp_last_op;
                    check(false, ss.str().c_str());
            }
            if (dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_END) {
                dcp_last_op = 0;
                dcp_last_nru = 0;
            }
        }
    } while (!done);

    if (total_bytes) {
        *total_bytes = all_bytes;
    }

    if (simulate_cursor_dropping) {
        if (num_snapshot_markers == 0) {
            cb_assert(num_mutations == 0 && num_deletions == 0);
        } else {
            check(num_mutations <= exp_mutations, "Invalid number of mutations");
            check(num_deletions <= exp_deletions, "Invalid number of deletes");
        }
    } else {
        check(num_mutations == exp_mutations, "Invalid number of mutations");
        check(num_deletions == exp_deletions, "Invalid number of deletes");
        check(num_snapshot_markers == exp_markers,
                "Didn't receive expected number of snapshot marker");
    }

    if (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        check(num_set_vbucket_pending == 1, "Didn't receive pending set state");
        check(num_set_vbucket_active == 1, "Didn't receive active set state");
    }

    /* Check if the readyQ size goes to zero after all items are streamed */
    std::stringstream stats_ready_queue_memory;
    stats_ready_queue_memory << "eq_dcpq:" << name << ":stream_" << vbucket
                             << "_ready_queue_memory";
    check((uint64_t)get_ull_stat(h, h1, stats_ready_queue_memory.str().c_str(), "dcp")
          == 0, "readyQ size did not go to zero");

    free(producers);
}

static void dcp_stream_req(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           uint32_t opaque, uint16_t vbucket, uint64_t start,
                           uint64_t end, uint64_t uuid,
                           uint64_t snap_start_seqno,
                           uint64_t snap_end_seqno,
                           uint64_t exp_rollback, ENGINE_ERROR_CODE err) {
    const void *cookie = testHarness.create_cookie();
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, strlen(name))
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    uint64_t rollback = 0;
    ENGINE_ERROR_CODE rv = h1->dcp.stream_req(h, cookie, 0, 1, 0, start, end,
                                              uuid, snap_start_seqno,
                                              snap_end_seqno,
                                              &rollback, mock_dcp_add_failover_log);
    check(rv == err, "Unexpected error code");
    if (err == ENGINE_ROLLBACK || err == ENGINE_KEY_ENOENT) {
        check(exp_rollback == rollback, "Rollback didn't match expected value");
    }
    testHarness.destroy_cookie(cookie);
}

static enum test_result test_dcp_producer_stream_req_partial(ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {
    int num_items = 200;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < (num_items / 2); ++j) {
        std::stringstream ss;
        ss << "key" << j;
        check(del(h, h1, ss.str().c_str(), 0, 0) == ENGINE_SUCCESS,
              "Expected delete to succeed");
    }

    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 95, 209, vb_uuid, 95, 95, 105,
               100, 2, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_partial_with_time_sync(
                                                             ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {

    set_drift_counter_state(h, h1, 1000, 0x01);

    int num_items = 200;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < (num_items / 2); ++j) {
        std::stringstream ss;
        ss << "key" << j;
        check(del(h, h1, ss.str().c_str(), 0, 0) == ENGINE_SUCCESS,
              "Expected delete to succeed");
    }

    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 95, 209, vb_uuid, 95, 95, 105,
               100, 2, 0, false, true, 1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}
static enum test_result test_dcp_producer_stream_req_full(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, end, vb_uuid, 0, 0,
               num_items, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_disk(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    int num_items = 400;
    for (int j = 0; j < num_items; ++j) {
        if (j == 200) {
            wait_for_flusher_to_settle(h, h1);
            wait_for_stat_to_be(h, h1, "ep_items_rm_from_checkpoints", 200);
            stop_persistence(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be_gte(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1,"unittest", cookie, 0, 0, 0, 200, vb_uuid, 0, 0, 200, 0, 1,
               0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_diskonly(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint32_t flags = DCP_ADD_STREAM_FLAG_DISKONLY;

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, flags, 0, -1, vb_uuid, 0, 0, 300,
               0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_mem(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 200, 300, vb_uuid, 200, 200,
               100, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_dgm(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    int i = 0;  // Item count
    while (true) {
        // Gathering stats on every store is expensive, just check every 100 iterations
        if ((i % 100) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") < 50) {
                break;
            }
        }

        item *itm = NULL;
        std::stringstream ss;
        ss << "key" << i;
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET,
                                      ss.str().c_str(), "somevalue", &itm);
        if (ret == ENGINE_SUCCESS) {
            i++;
        }
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, i, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= ((float)(50/100) * i));

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "5242880");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < 50);

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1,"unittest", cookie, 0, 0, 0, end, vb_uuid, 0, 0, i, 0, 1,
               0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_latest(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    uint32_t flags = DCP_ADD_STREAM_FLAG_LATEST;
    dcp_stream(h, h1, "unittest", cookie, 0, flags, 200, 205, vb_uuid, 200, 200,
               100, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_producer_stream_req_nmvb(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    uint32_t req_vbucket = 1;
    uint64_t rollback = 0;

    check(h1->dcp.stream_req(h, cookie1, 0, 0, req_vbucket, 0, 0, 0, 0,
                             0, &rollback, mock_dcp_add_failover_log)
                == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_agg_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie[5];

    uint64_t total_bytes = 0;
    for (int j = 0; j < 5; ++j) {
        char name[12];
        snprintf(name, sizeof(name), "unittest_%d", j);
        cookie[j] = testHarness.create_cookie();
        dcp_stream(h, h1, name, cookie[j], 0, 0, 200, 300, vb_uuid, 200, 200,
                   100, 0, 1, 0, false, false, 0, false, &total_bytes);
    }

    check(get_int_stat(h, h1, "unittest:producer_count", "dcpagg _") == 5,
          "producer count mismatch");
    check(get_int_stat(h, h1, "unittest:total_bytes", "dcpagg _") ==
          (int)total_bytes, "aggregate total bytes sent mismatch");
    check(get_int_stat(h, h1, "unittest:items_sent", "dcpagg _") == 500,
          "aggregate total items sent mismatch");
    check(get_int_stat(h, h1, "unittest:items_remaining", "dcpagg _") == 0,
          "aggregate total items remaining mismatch");

    for (int j = 0; j < 5; ++j) {
        testHarness.destroy_cookie(cookie[j]);
    }

    return SUCCESS;
}

static test_result test_dcp_cursor_dropping(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    int i = 0;  // Item count
    int maxSize = get_int_stat(h, h1, "ep_max_size", "memory");
    stop_persistence(h, h1);
    while(1) {
        // Load items into server until 90% of the mem quota
        // is used.
        int memUsed = get_int_stat(h, h1, "mem_used", "memory");
        if ((float)memUsed <= ((float)(maxSize) * 0.90)) {
            item *itm = NULL;
            std::stringstream ss;
            ss << "key" << i;
            ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET,
                    ss.str().c_str(), "somevalue", &itm);
            if (ret == ENGINE_SUCCESS) {
                i++;
            }
            h1->release(h, NULL, itm);
        } else {
            break;
        }
    }

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    const void *cookie = testHarness.create_cookie();

    start_persistence(h, h1);
    std::string name("unittest");

    dcp_stream(h, h1, name.c_str(), cookie, 0, 0, 0, end, vb_uuid, 0, 0, i,
               0, 1, 0, false, false, 0, true, NULL, true);

    check(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Last DCP op wasn't a stream END");

    if (get_int_stat(h, h1, "ep_cursors_dropped") > 0) {
        check(dcp_last_flags == 4, "Last DCP flag not END_STREAM_SLOW");
        // Also ensure the status of the active stream for the vbucket
        // shows as "temporarily_disconnected", in vbtakeover stats.
        std::string stats_takeover("dcp-vbtakeover 0 " + name);
        std::string status = get_str_stat(h, h1, "status",
                                          stats_takeover.c_str());
        checkeq(status.compare("temporarily_disconnected"), 0,
                "Unexpected status");
    } else {
        check(dcp_last_flags == 0, "Last DCP flag not END_STREAM_OK");
    }

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static test_result test_dcp_value_compression(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    check(get_float_stat(h, h1, "ep_dcp_min_compression_ratio") ==
          (float)0.85, "Unexpected dcp_min_compression_ratio");

    // Set dcp_min_compression_ratio to infinite, which means
    // a DCP producer would ship the doc no matter what the
    // achieved compressed length is.
    set_param(h, h1, protocol_binary_engine_param_flush,
              "dcp_min_compression_ratio",
              std::to_string(std::numeric_limits<float>::max()).c_str());

    item *i = NULL;
    std::string originalValue("{\"FOO\":\"BAR\"}");

    checkeq(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                         originalValue.c_str(), originalValue.length(),
                         0, &i, 0, 0, 3600,
                         PROTOCOL_BINARY_DATATYPE_JSON),
            ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t opaque = 1;

    checkeq(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER,
                         (void*)name, nname),
            ENGINE_SUCCESS,
            "Failed dcp producer open connection.");

    checkeq(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size",
                            strlen("connection_buffer_size"), "1024", 4),
            ENGINE_SUCCESS,
            "Failed to establish connection buffer");

    checkeq(h1->dcp.control(h, cookie, ++opaque, "enable_value_compression",
                            strlen("enable_value_compression"), "true", 4),
            ENGINE_SUCCESS,
            "Failed to enable value compression");

    uint64_t rollback = 0;
    checkeq(h1->dcp.stream_req(h, cookie, 0, opaque, 0, 0, end,
                               vb_uuid, 0, 0, &rollback,
                               mock_dcp_add_failover_log),
            ENGINE_SUCCESS,
            "Failed to initiate stream request");

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);

    bool done = false;
    uint32_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    std::string last_mutation_val;

    do {
        if (bytes_read > 512) {
            h1->dcp.buffer_acknowledgement(h, cookie, ++opaque, 0, bytes_read);
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    bytes_read += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    last_mutation_val.assign(dcp_last_value);
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    bytes_read += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    if (dcp_last_flags & 8) {
                        pending_marker_ack = true;
                        marker_end = dcp_last_snap_end_seqno;
                    }

                    bytes_read += dcp_last_packet_size;
                    break;
                default:
                     // Aborting ...
                    std::stringstream ss;
                    ss << "Unexpected DCP operation: " << dcp_last_op;
                    check(false, ss.str().c_str());
            }
        }
    } while (!done);

    cb_assert(!last_mutation_val.empty());

    snap_buf output;
    doSnappyUncompress(last_mutation_val.c_str(),
                       last_mutation_val.length(),
                       output);
    std::string received(output.buf.get(), output.len);

    checkeq(originalValue.compare(received), 0,
            "Value received is not what is expected");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_backfill_no_value(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    uint64_t num_items = 0, total_bytes = 0, est_bytes = 0;
    const int rr_thresh = 80;
    const std::string value("somevalue");
    /* We want a DGM case to test both in memory backfill and disk backfill */
    while (true) {
        /* Gathering stats on every store is expensive, just check every 100
         iterations */
        if ((num_items % 10) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") <
                rr_thresh) {
                break;
            }
        }

        item *itm = NULL;
        std::string key("key" + std::to_string(num_items));
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET, key.c_str(),
                                      value.c_str(), &itm);
        if (ret == ENGINE_SUCCESS) {
            num_items++;
            est_bytes += key.length();
        }
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= (((float)(100 - rr_thresh)/100) * num_items));

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "52428800");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < rr_thresh);

    const void *cookie = testHarness.create_cookie();

    /* Stream mutations with "no_value" flag set */
    dcp_stream(h, h1, "unittest", cookie, 0, DCP_ADD_STREAM_FLAG_NO_VALUE, 0,
               end, vb_uuid, 0, 0, num_items, 0, 1, 0, false, false, 0, false,
               &total_bytes);

    /* basebytes mutation + nmeta (when no ext_meta is expected) */
    const int packet_fixed_size = dcp_mutation_base_msg_bytes +
                                  dcp_meta_size_none;
    est_bytes += (num_items * packet_fixed_size);
    /* Add DCP_SNAPSHOT_MARKER bytes and DCP_STREAM_END bytes */
    est_bytes += (dcp_snapshot_marker_base_msg_bytes +
                  dcp_stream_end_resp_base_msg_bytes);
    checkeq(est_bytes, total_bytes, "Maybe values streamed in stream no_value");
    testHarness.destroy_cookie(cookie);

    /* Stream without NO_VALUE flag (default) and expect both key and value for
       all mutations */
    total_bytes = 0;
    const void *cookie1 = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, 0, end, vb_uuid, 0, 0,
               num_items, 0, 1, 0, false, false, 0, false, &total_bytes);

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * num_items);
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_mem_no_value(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    uint64_t num_items = 300, total_bytes = 0, est_bytes = 0;
    const uint64_t start = 200, end = 300;
    const std::string value("data");

    for (uint64_t j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::string key("key" + std::to_string(j));
        check(store(h, h1, NULL, OPERATION_SET, key.c_str(), value.c_str(), &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    /* Stream mutations with "no_value" flag set */
    dcp_stream(h, h1, "unittest", cookie, 0, DCP_ADD_STREAM_FLAG_NO_VALUE,
               start, end, vb_uuid, 200, 200, (end-start), 0, 1, 0, false,
               false, 0, false, &total_bytes);

    /* basebytes mutation + nmeta (when no ext_meta is expected) */
    const int packet_fixed_size = dcp_mutation_base_msg_bytes +
                                  dcp_meta_size_none;
    est_bytes += ((end-start) * packet_fixed_size);
    /* Add DCP_SNAPSHOT_MARKER bytes and DCP_STREAM_END bytes */
    est_bytes += (dcp_snapshot_marker_base_msg_bytes +
                  dcp_stream_end_resp_base_msg_bytes);
    /* Add key size (keys from "key201" till "key300") */
    est_bytes += (6 * 100);
    checkeq(est_bytes, total_bytes, "Maybe values streamed in stream no_value");
    testHarness.destroy_cookie(cookie);

    /* Stream without NO_VALUE flag (default) and expect both key and value for
     all mutations */
    total_bytes = 0;
    const void *cookie1 = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, start, end, vb_uuid, 200, 200,
               (end-start), 0, 1, 0, false, false, 0, false, &total_bytes);

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * (end-start));
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie, 0, 0, DCP_OPEN_PRODUCER, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    uint32_t flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie1 = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest", cookie1, 0, flags, 0, 1000, vb_uuid, 0, 0, 20,
               0, 2, 10);

    check(verify_vbucket_state(h, h1, 0, vbucket_state_dead), "Wrong vb state");

    testHarness.destroy_cookie(cookie);
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_takeover_no_items(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint32_t opaque = 1;

    check(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                       strlen(name)) == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    uint16_t vbucket = 0;
    uint32_t flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    uint64_t start_seqno = 10;
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = 10;
    uint64_t snap_end_seqno = 10;

    uint64_t rollback = 0;
    check(h1->dcp.stream_req(h, cookie, flags, ++opaque, vbucket, start_seqno,
                             end_seqno, vb_uuid, snap_start_seqno,
                             snap_end_seqno, &rollback,
                             mock_dcp_add_failover_log)
                == ENGINE_SUCCESS,
          "Failed to initiate stream request");

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);

    bool done = false;
    int num_snapshot_markers = 0;
    int num_set_vbucket_pending = 0;
    int num_set_vbucket_active = 0;

    do {
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    num_snapshot_markers++;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
                    if (dcp_last_vbucket_state == vbucket_state_pending) {
                        num_set_vbucket_pending++;
                    } else if (dcp_last_vbucket_state == vbucket_state_active) {
                        num_set_vbucket_active++;
                    }
                    sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    break;
                case 0:
                     break;
                default:
                    break;
                    abort();
            }
            dcp_last_op = 0;
        }
    } while (!done);

    check(num_snapshot_markers == 0, "Invalid number of snapshot marker");
    check(num_set_vbucket_pending == 1, "Didn't receive pending set state");
    check(num_set_vbucket_active == 1, "Didn't receive active set state");

    free(producers);
    check(verify_vbucket_state(h, h1, 0, vbucket_state_dead), "Wrong vb state");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static uint32_t add_stream_for_consumer(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const void* cookie, uint32_t opaque,
                                        uint16_t vbucket, uint32_t flags,
                                        protocol_binary_response_status response,
                                        uint64_t exp_snap_start = 0,
                                        uint64_t exp_snap_end = 0) {

    dcp_step(h, h1, cookie);
    uint32_t stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("connection_buffer_size") == 0);
    cb_assert(dcp_last_opaque != opaque);

    if (get_bool_stat(h, h1, "ep_dcp_enable_noop")) {
        // Check that the enable noop message is sent
        dcp_step(h, h1, cookie);
        stream_opaque = dcp_last_opaque;
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("enable_noop") == 0);
        cb_assert(dcp_last_opaque != opaque);

        // Check that the set noop interval message is sent
        dcp_step(h, h1, cookie);
        stream_opaque = dcp_last_opaque;
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("set_noop_interval") == 0);
        cb_assert(dcp_last_opaque != opaque);
    }

    dcp_step(h, h1, cookie);
    stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("set_priority") == 0);
    cb_assert(dcp_last_opaque != opaque);

    dcp_step(h, h1, cookie);
    stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("enable_ext_metadata") == 0);
    cb_assert(dcp_last_opaque != opaque);

    if (get_bool_stat(h, h1, "ep_dcp_value_compression_enabled")) {
        dcp_step(h, h1, cookie);
        stream_opaque = dcp_last_opaque;
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("enable_value_compression") == 0);
        cb_assert(dcp_last_opaque != opaque);
    }

    dcp_step(h, h1, cookie);
    stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op = PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("supports_cursor_dropping") == 0);
    cb_assert(dcp_last_opaque != opaque);

    check(h1->dcp.add_stream(h, cookie, opaque, vbucket, flags)
          == ENGINE_SUCCESS, "Add stream request failed");

    dcp_step(h, h1, cookie);
    stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    if (exp_snap_start != 0) {
        cb_assert(exp_snap_start == dcp_last_snap_start_seqno);
    }

    if (exp_snap_end != 0) {
        cb_assert(exp_snap_end == dcp_last_snap_end_seqno);
    }

    size_t bodylen = 0;
    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        bodylen = 16;
    } else if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        bodylen = 8;
    }

    size_t headerlen = sizeof(protocol_binary_response_header);
    size_t pkt_len = headerlen + bodylen;

    protocol_binary_response_header* pkt =
        (protocol_binary_response_header*)malloc(pkt_len * sizeof(uint8_t));
    memset(pkt->bytes, '\0', pkt_len);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(response);
    pkt->response.opaque = dcp_last_opaque;

    if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        bodylen = sizeof(uint64_t);
        uint64_t rollbackSeqno = 0;
        memcpy(pkt->bytes + headerlen, &rollbackSeqno, bodylen);
    }

    pkt->response.bodylen = htonl(bodylen);

    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));
    }

    check(h1->dcp.response_handler(h, cookie, pkt) == ENGINE_SUCCESS,
          "Expected success");
    dcp_step(h, h1, cookie);
    free (pkt);

    if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        return stream_opaque;
    }

    if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ) {
        cb_assert(dcp_last_opaque != opaque);
        verify_curr_items(h, h1, 0, "Wrong amount of items");

        protocol_binary_response_header* pkt =
            (protocol_binary_response_header*)malloc(pkt_len * sizeof(uint8_t));
        memset(pkt->bytes, '\0', 40);
        pkt->response.magic = PROTOCOL_BINARY_RES;
        pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
        pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
        pkt->response.opaque = dcp_last_opaque;
        pkt->response.bodylen = htonl(16);

        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

        check(h1->dcp.response_handler(h, cookie, pkt) == ENGINE_SUCCESS,
              "Expected success");
        dcp_step(h, h1, cookie);

        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
        cb_assert(dcp_last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS);
        cb_assert(dcp_last_stream_opaque == stream_opaque);
        free(pkt);
    } else {
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
        cb_assert(dcp_last_status == response);
        cb_assert(dcp_last_stream_opaque == stream_opaque);
    }

    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        uint64_t uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
        uint64_t seq = get_ull_stat(h, h1, "vb_0:0:seq", "failovers");
        cb_assert(uuid == 123456789);
        cb_assert(seq == 0);
    }

    return stream_opaque;
}

static enum test_result test_dcp_reconnect(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1,
                                           bool full, bool restart) {
    // Test reconnect when we were disconnected after receiving a full snapshot
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    int items = full ? 10 : 5;

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    check(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 0, 10, 2)
        == ENGINE_SUCCESS, "Failed to send snapshot marker");

    for (int i = 1; i <= items; i++) {
        std::stringstream ss;
        ss << "key" << i;
        check(h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                               ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                               0, 0, 0, "", 0, INITIAL_NRU_VALUE)
            == ENGINE_SUCCESS, "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", items);

    testHarness.destroy_cookie(cookie);

    if (restart) {
        testHarness.reload_engine(&h, &h1, testHarness.engine_path,
                                  testHarness.get_current_testcase()->cfg,
                                  true, true);
        wait_for_warmup_complete(h, h1);
    }

    cookie = testHarness.create_cookie();

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    uint64_t snap_start = full ? 10 : 0;
    uint64_t snap_end = 10;
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS, snap_start,
                            snap_end);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_reconnect_full(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after a dropped connection with a full snapshot
    return test_dcp_reconnect(h, h1, true, false);
}

static enum test_result test_dcp_reconnect_partial(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after a dropped connection with a partial snapshot
    return test_dcp_reconnect(h, h1, false, false);
}

static enum test_result test_dcp_crash_reconnect_full(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after we crash with a full snapshot
    return test_dcp_reconnect(h, h1, true, true);
}

static enum test_result test_dcp_crash_reconnect_partial(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after we crash with a partial snapshot
    return test_dcp_reconnect(h, h1, false, true);
}

static enum test_result test_dcp_consumer_takeover(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0,
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 1, 5, 10);
    for (int i = 1; i <= 5; i++) {
        std::stringstream ss;
        ss << "key" << i;
        check(h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                               ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                               0, 0, 0, "", 0, INITIAL_NRU_VALUE)
            == ENGINE_SUCCESS, "Failed to send dcp mutation");
    }

    h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 6, 10, 10);
    for (int i = 6; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;
        check(h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                               ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                               0, 0, 0, "", 0, INITIAL_NRU_VALUE)
            == ENGINE_SUCCESS, "Failed to send dcp mutation");
    }

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    cb_assert(dcp_last_status == ENGINE_SUCCESS);
    cb_assert(dcp_last_opaque != opaque);

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    cb_assert(dcp_last_status == ENGINE_SUCCESS);
    cb_assert(dcp_last_opaque != opaque);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_failover_scenario_with_dcp(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    int num_items = 50;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
        if (j % 10 == 0) {
            wait_for_flusher_to_settle(h, h1);
            createCheckpoint(h, h1);
        }
    }

    createCheckpoint(h, h1);
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0,
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    check(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 200, 300, 300)
            == ENGINE_SUCCESS, "Failed to send snapshot marker");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    check(h1->dcp.close_stream(h, cookie, stream_opaque, 0) == ENGINE_SUCCESS,
            "Expected success");

    // Simulating a failover scenario, where the replica vbucket will
    // be marked as active.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
            "Failed to set vbucket state.");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) ==
            ENGINE_SUCCESS, "Error in SET operation.");

    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "ep_diskqueue_items") == 0,
            "Unexpected diskqueue");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_consumer_backoff_stat(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, protocol_binary_engine_param_tap,
              "replication_throttle_queue_cap", "10");
    check(get_int_stat(h, h1, "ep_replication_throttle_queue_cap") == 10,
          "Incorrect tap_keepalive value.");

    stop_persistence(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    testHarness.time_travel(30);
    check(get_int_stat(h, h1, "eq_dcpq:unittest:total_backoffs", "dcp") == 0,
          "Expected backoffs to be 0");

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 0, 20, 1),
            ENGINE_SUCCESS, "Failed to send snapshot marker");

    for (int i = 1; i <= 20; i++) {
        std::stringstream ss;
        ss << "key" << i;
        check(h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                               ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                               0, 0, 0, "", 0, INITIAL_NRU_VALUE)
            == ENGINE_SUCCESS, "Failed to send dcp mutation");
    }

    wait_for_stat_change(h, h1, "eq_dcpq:unittest:total_backoffs", 0, "dcp");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_rollback_to_zero(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_ROLLBACK);

    wait_for_flusher_to_settle(h, h1);
    wait_for_rollback_to_finish(h, h1);

    check(get_int_stat(h, h1, "curr_items") == 0,
            "All items should be rolled back");
    checkeq(num_items, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_chk_manager_rollback(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    uint16_t vbid = 0;
    int num_items = 40;
    stop_persistence(h, h1);
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < num_items / 2; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << (j + num_items);
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, 60, "Wrong amount of items");
    set_vbucket_state(h, h1, vbid, vbucket_state_replica);

    // Create rollback stream
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    check(h1->dcp.add_stream(h, cookie, ++opaque, vbid, 0)
          == ENGINE_SUCCESS, "Add stream request failed");

    dcp_step(h, h1, cookie);
    uint32_t stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint64_t rollbackSeqno = htonll(40);
    protocol_binary_response_header* pkt =
        (protocol_binary_response_header*)malloc(32 * sizeof(uint8_t));
    memset(pkt->bytes, '\0', 32);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt->response.opaque = stream_opaque;
    pkt->response.bodylen = htonl(8);
    memcpy(pkt->bytes + 24, &rollbackSeqno, sizeof(uint64_t));

    check(h1->dcp.response_handler(h, cookie, pkt) == ENGINE_SUCCESS,
          "Expected success");

    do {
        dcp_step(h, h1, cookie);
        usleep(100);
    } while (dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);

    stream_opaque = dcp_last_opaque;
    free(pkt);

    // Send success

    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    pkt = (protocol_binary_response_header*)malloc(40 * sizeof(uint8_t));
    memset(pkt->bytes, '\0', 40);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt->response.opaque = stream_opaque;
    pkt->response.bodylen = htonl(16);
    memcpy(pkt->bytes + 24, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt->bytes + 22, &by_seqno, sizeof(uint64_t));

    check(h1->dcp.response_handler(h, cookie, pkt) == ENGINE_SUCCESS,
          "Expected success");
    dcp_step(h, h1, cookie);
    free(pkt);

    int items = get_int_stat(h, h1, "curr_items_tot");
    int seqno = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    int chk = get_int_stat(h, h1, "vb_0:num_checkpoint_items", "checkpoint");

    check(items == 40, "Got invalid amount of items");
    check(seqno == 40, "Seqno should be 40 after rollback");
    check(chk == 1, "There should only be one checkpoint item");
    checkeq(num_items/2, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items/2, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_fullrollback_for_consumer(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    const int num_items = 10;
    std::vector<std::string> keys;
    for (int i = 0; i < num_items; ++i) {
        std::stringstream ss;
        ss << "key" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                    &itm, 0, 0) == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == num_items,
            "Item count should've been 10");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    check(h1->dcp.add_stream(h, cookie, opaque, 0, 0)
            == ENGINE_SUCCESS, "Add stream request failed");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = htonll(5);
    protocol_binary_response_header *pkt1 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.magic = PROTOCOL_BINARY_RES;
    pkt1->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt1->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt1->response.bodylen = htonl(bodylen);
    pkt1->response.opaque = dcp_last_opaque;
    memcpy(pkt1->bytes + headerlen, &rollbackSeqno, bodylen);

    check(h1->dcp.response_handler(h, cookie, pkt1) == ENGINE_SUCCESS,
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, h1, "ep_rollback_count", 1);
    dcp_step(h, h1, cookie);

    opaque++;

    cb_assert(dcp_last_opaque != opaque);

    bodylen = 2 *sizeof(uint64_t);
    protocol_binary_response_header* pkt2 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.magic = PROTOCOL_BINARY_RES;
    pkt2->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt2->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt2->response.opaque = dcp_last_opaque;
    pkt2->response.bodylen = htonl(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    check(h1->dcp.response_handler(h, cookie, pkt2) == ENGINE_SUCCESS,
          "Expected success");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);

    free(pkt1);
    free(pkt2);

    //Verify that all items have been removed from consumer
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "vb_replica_curr_items") == 0,
            "Item count should've been 0");
    check(get_int_stat(h, h1, "ep_rollback_count") == 1,
            "Rollback count expected to be 1");
    checkeq(num_items, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_partialrollback_for_consumer(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {

    stop_persistence(h, h1);
    std::vector<std::string> keys;
    for (int i = 0; i < 100; ++i) {
        std::stringstream ss;
        ss << "key_" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                    &itm, 0, 0) == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 100,
            "Item count should've been 100");

    stop_persistence(h, h1);
    keys.clear();
    for (int i = 90; i < 110; ++i) {
        std::stringstream ss;
        ss << "key_" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                    &itm, 0, 0) == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 110,
            "Item count should've been 110");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    check(h1->dcp.add_stream(h, cookie, opaque, 0, 0)
            == ENGINE_SUCCESS, "Add stream request failed");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = htonll(100);
    protocol_binary_response_header *pkt1 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.magic = PROTOCOL_BINARY_RES;
    pkt1->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt1->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt1->response.bodylen = htonl(bodylen);
    pkt1->response.opaque = dcp_last_opaque;
    memcpy(pkt1->bytes + headerlen, &rollbackSeqno, bodylen);

    check(h1->dcp.response_handler(h, cookie, pkt1) == ENGINE_SUCCESS,
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, h1, "ep_rollback_count", 1);
    dcp_step(h, h1, cookie);
    opaque++;

    bodylen = 2 * sizeof(uint64_t);
    protocol_binary_response_header* pkt2 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.magic = PROTOCOL_BINARY_RES;
    pkt2->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt2->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt2->response.opaque = dcp_last_opaque;
    pkt2->response.bodylen = htonl(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    check(h1->dcp.response_handler(h, cookie, pkt2) == ENGINE_SUCCESS,
          "Expected success");
    dcp_step(h, h1, cookie);

    free(pkt1);
    free(pkt2);

    //?Verify that 10 items plus 10 updates have been removed from consumer
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "vb_replica_curr_items") == 100,
            "Item count should've been 100");
    check(get_int_stat(h, h1, "ep_rollback_count") == 1,
            "Rollback count expected to be 1");
    checkeq(20, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(20, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_buffer_log_size(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    char stats_buffer[50];
    char status_buffer[50];

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
            == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "0", 1) == ENGINE_SUCCESS,
                          "Failed to establish connection buffer");
    snprintf(status_buffer, sizeof(status_buffer),
             "eq_dcpq:%s:flow_control", name);
    std::string status = get_str_stat(h, h1, status_buffer, "dcp");
    check(status.compare("disabled") == 0, "Flow control enabled!");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "512", 4) == ENGINE_SUCCESS,
                          "Failed to establish connection buffer");

    snprintf(stats_buffer, sizeof(stats_buffer),
             "eq_dcpq:%s:max_buffer_bytes", name);

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp")
            == 512, "Buffer Size did not get set");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "1024", 4) == ENGINE_SUCCESS,
                          "Failed to establish connection buffer");

    check((uint32_t)get_int_stat(h, h1, stats_buffer, "dcp")
            == 1024, "Buffer Size did not get reset");

    /* Set flow control buffer size to zero which implies disable it */
    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                          "0", 1) == ENGINE_SUCCESS,
          "Failed to establish connection buffer");
    status = get_str_stat(h, h1, status_buffer, "dcp");
    check(status.compare("disabled") == 0, "Flow control enabled!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_get_failover_log(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
            == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    check(h1->dcp.get_failover_log(h, cookie, opaque, 0,
                                   mock_dcp_add_failover_log) ==
            ENGINE_SUCCESS, "Failed to retrieve failover log");

    testHarness.destroy_cookie(cookie);

    check(h1->get_stats(h, NULL, "failovers", 9, add_stats) == ENGINE_SUCCESS,
            "Failed to get stats.");

    size_t i = 0;
    for (i = 0; i < dcp_failover_log.size(); i++) {
        std::string itr;
        std::ostringstream ss;
        ss << i;
        itr = ss.str();
        std::string uuid = "vb_0:" + itr + ":id";
        std::string seqno = "vb_0:" + itr + ":seq";
        check(dcp_failover_log[i].first ==
                strtoull((vals[uuid]).c_str(), NULL, 10),
                "UUID mismatch in failover stats");
        check(dcp_failover_log[i].second ==
                strtoull((vals[seqno]).c_str(), NULL, 10),
                "SEQNO mismatch in failover stats");
    }

    vals.clear();
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_exists(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint16_t vbucket = 0;

    check(set_vbucket_state(h, h1, vbucket, vbucket_state_replica),
          "Failed to set vbucket state.");

    /* Open consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp consumer open connection.");

    /* Send add stream to consumer */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, ++opaque, vbucket, 0),
            "Add stream request failed");

    /* Send add stream to consumer twice and expect failure */
    checkeq(ENGINE_KEY_EEXISTS,
            h1->dcp.add_stream(h, cookie, ++opaque, 0, 0),
            "Stream exists for this vbucket");

    /* Try adding another stream for the vbucket in another consumer conn */
    /* Open another consumer connection */
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque1 = 0xFFFF0000;
    std::string name1("unittest1");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque1, 0, flags, (void*)name1.c_str(),
                         name1.length()),
            "Failed dcp consumer open connection.");

    /* Send add stream */
    checkeq(ENGINE_KEY_EEXISTS,
            h1->dcp.add_stream(h, cookie1, ++opaque1, vbucket, 0),
            "Stream exists for this vbucket");

    /* Just check that we can add passive stream for another vbucket in this
       conn*/
    checkeq(true, set_vbucket_state(h, h1, vbucket + 1, vbucket_state_replica),
            "Failed to set vbucket state.");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie1, ++opaque1, vbucket + 1, 0),
            "Add stream request failed in the second conn");
    testHarness.destroy_cookie(cookie);
    testHarness.destroy_cookie(cookie1);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_nmvb(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp consumer open connection.");

    // Send add stream to consumer for vbucket that doesn't exist
    opaque++;
    check(h1->dcp.add_stream(h, cookie, opaque, 1, 0)
          == ENGINE_NOT_MY_VBUCKET, "Add stream expected not my vbucket");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_exists(ENGINE_HANDLE*h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_nmvb(ENGINE_HANDLE*h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream_no_stream(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp producer open connection.");

    check(h1->dcp.close_stream(h, cookie, opaque + 1, 0) == ENGINE_KEY_ENOENT,
          "Expected stream doesn't exist");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
        get_str_stat(h, h1, "eq_dcpq:unittest:stream_0_state", "dcp");
    check(state.compare("reading") == 0, "Expected stream in reading state");

    check(h1->dcp.close_stream(h, cookie, stream_opaque, 0) == ENGINE_SUCCESS,
          "Expected success");

    state = get_str_stat(h, h1, "eq_dcpq:unittest:stream_0_state", "dcp");
    check(state.compare("dead") == 0, "Expected stream in dead state");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_end_stream(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    uint16_t vbucket = 0;
    uint32_t end_flag = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, vbucket, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
        get_str_stat(h, h1, "eq_dcpq:unittest:stream_0_state", "dcp");
    check(state.compare("reading") == 0, "Expected stream in reading state");

    check(h1->dcp.stream_end(h, cookie, stream_opaque, vbucket, end_flag)
          == ENGINE_SUCCESS, "Expected success");

    wait_for_str_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_state", "dead",
                            "dcp");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open an DCP connection
    check(h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t dataLen = 100;
    char *data = static_cast<char *>(malloc(dataLen));
    memset(data, 'x', dataLen);

    uint8_t cas = 0;
    uint16_t vbucket = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    check(h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1)
        == ENGINE_SUCCESS, "Failed to send snapshot marker");

    // Ensure that we don't accept invalid opaque values
    check(h1->dcp.mutation(h, cookie, opaque + 1, "key", 3, data, dataLen, cas,
                           vbucket, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, NULL, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to detect invalid DCP opaque value");

    // Send snapshot marker
    checkeq(h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 15, 300),
            ENGINE_SUCCESS,
            "Failed to send marker!");

    // Consume an DCP mutation
    check(h1->dcp.mutation(h, cookie, opaque, "key", 3, data, dataLen, cas,
                           vbucket, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, NULL, 0, 0) == ENGINE_SUCCESS,
          "Failed dcp mutate.");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");

    check_key_value(h, h1, "key", data, dataLen);

    testHarness.destroy_cookie(cookie);
    free(data);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_mutate_with_time_sync(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    set_drift_counter_state(h, h1, 1000, 0x01);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open an DCP connection
    check(h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t dataLen = 100;
    char *data = static_cast<char *>(malloc(dataLen));
    memset(data, 'x', dataLen);

    uint8_t cas = 0;
    uint16_t vbucket = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    check(h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1)
        == ENGINE_SUCCESS, "Failed to send snapshot marker");

    // Consume a DCP mutation with extended meta
    int64_t adjusted_time1 = gethrtime() * 2;
    ExtendedMetaData *emd = new ExtendedMetaData(adjusted_time1, false);
    cb_assert(emd && emd->getStatus() == ENGINE_SUCCESS);
    std::pair<const char*, uint16_t> meta = emd->getExtMeta();
    check(h1->dcp.mutation(h, cookie, opaque, "key", 3, data, dataLen, cas,
                           vbucket, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, meta.first, meta.second, 0)
            == ENGINE_SUCCESS,
            "Failed dcp mutate.");
    delete emd;

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    check(h1->dcp.close_stream(h, cookie, opaque, 0) == ENGINE_SUCCESS,
            "Expected success");

    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    check_key_value(h, h1, "key", data, dataLen);

    testHarness.destroy_cookie(cookie);
    free(data);

    protocol_binary_request_header *request;
    int64_t adjusted_time2;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    /**
     * Check that adjusted_time2 is marginally greater than
     * adjusted_time1.
     */
    check(adjusted_time2 >= adjusted_time1,
            "Adjusted time after mutation: Not what is expected");

    return SUCCESS;
}


static enum test_result test_dcp_consumer_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Store an item
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "value", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 1, "one item stored");

    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint8_t cas = 0;
    uint16_t vbucket = 0;
    uint32_t flags = 0;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t seqno = 0;

    // Open an DCP connection
    check(h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    check(h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1)
        == ENGINE_SUCCESS, "Failed to send snapshot marker");

    // verify that we don't accept invalid opaque id's
    check(h1->dcp.deletion(h, cookie, opaque + 1, "key", 3, cas, vbucket,
                           bySeqno, revSeqno, NULL, 0) == ENGINE_KEY_ENOENT,
          "Failed to detect invalid DCP opaque value.");

    // Consume an DCP deletion
    check(h1->dcp.deletion(h, cookie, opaque, "key", 3, cas, vbucket,
                           bySeqno, revSeqno, NULL, 0) == ENGINE_SUCCESS,
          "Failed dcp delete.");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    wait_for_stat_change(h, h1, "curr_items", 1);
    verify_curr_items(h, h1, 0, "one item deleted");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_delete_with_time_sync(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    set_drift_counter_state(h, h1, 1000, 0x01);

    // Store an item
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "value", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 1, "one item stored");

    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint8_t cas = 0;
    uint16_t vbucket = 0;
    uint32_t flags = 0;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t seqno = 0;

    // Open an DCP connection
    check(h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname)
          == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("consumer") == 0, "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    check(h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1)
        == ENGINE_SUCCESS, "Failed to send snapshot marker");

    // Consume an DCP deletion
    int64_t adjusted_time1 = gethrtime() * 2;
    ExtendedMetaData *emd = new ExtendedMetaData(adjusted_time1, false);
    cb_assert(emd && emd->getStatus() == ENGINE_SUCCESS);
    std::pair<const char*, uint16_t> meta = emd->getExtMeta();
    check(h1->dcp.deletion(h, cookie, opaque, "key", 3, cas, vbucket,
                           bySeqno, revSeqno, meta.first, meta.second)
            == ENGINE_SUCCESS,
            "Failed dcp delete.");
    delete emd;

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    wait_for_stat_change(h, h1, "curr_items", 1);
    verify_curr_items(h, h1, 0, "one item deleted");
    testHarness.destroy_cookie(cookie);

    protocol_binary_request_header *request;
    int64_t adjusted_time2;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    /**
     * Check that adjusted_time2 is marginally greater than
     * adjusted_time1.
     */
    check(adjusted_time2 >= adjusted_time1,
            "Adjusted time after deletion: Not what is expected");

    return SUCCESS;
}

static enum test_result test_dcp_consumer_noop(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    check(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname)
          == ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    struct dcp_message_producers* producers = get_dcp_producers(h, h1);
    testHarness.time_travel(201);

    // No-op not recieved for 201 seconds. Should be ok.
    check(h1->dcp.step(h, cookie, producers) == ENGINE_SUCCESS,
          "Expected engine success");

    testHarness.time_travel(200);

    // Message not recieved for over 400 seconds. Should disconnect.
    check(h1->dcp.step(h, cookie, producers) == ENGINE_DISCONNECT,
          "Expected engine disconnect");

    free(producers);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    for (size_t i = 0; i < 8192; ++i) {
        char *data = static_cast<char *>(malloc(i));
        memset(data, 'x', i);
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                             PROTOCOL_BINARY_RAW_BYTES,
                             data, i, 0) == ENGINE_SUCCESS,
              "Failed tap notify.");
        check_key_value(h, h1, "key", data, i);
        free(data);
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char data;
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    for (int i = 1; i < 10; ++i) {
        data = '0' + i;
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 0,
                             PROTOCOL_BINARY_RAW_BYTES,
                             &data, 1, 1) == ENGINE_SUCCESS,
              "Failed tap notify.");
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 0,
                             PROTOCOL_BINARY_RAW_BYTES,
                             &data, 1, 1) == ENGINE_SUCCESS,
              "Failed tap notify.");
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_set_vbstate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[8];
    vbucket_state_t vb_state = static_cast<vbucket_state_t>(htonl(vbucket_state_active));
    memcpy(eng_specific, &vb_state, sizeof(vb_state));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    // Get the vbucket UUID before vbucket takeover.
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(vb_state),
                         1, 0, TAP_VBUCKET_SET, 1, "", 0, 828, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         "", 0, 1) == ENGINE_SUCCESS,
          "Failed tap notify.");
    // Get the vbucket UUID after vbucket takeover.
    check(get_ull_stat(h, h1, "vb_1:0:id", "failovers") != vb_uuid,
          "A new vbucket uuid should be created after TAP-based vbucket takeover.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         "data", 4, 1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica_locked(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET,"key", "value", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    getl(h, h1, "key", 0, 10); // 10 secs of lock expiration
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "expected the key to be locked...");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         "data", 4, 0) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_DELETION, 0, "key", 3, 0, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         0, 0, 0) == ENGINE_SUCCESS,
          "Failed tap notify.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         NULL, 0, 1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         NULL, 0, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         NULL, 0, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica_locked(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET,"key", "value", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    getl(h, h1, "key", 0, 10); // 10 secs of lock expiration
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "expected the key to be locked...");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         NULL, 0, 0) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result verify_item(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    item *i,
                                    const char* key, size_t klen,
                                    const char* val, size_t vlen)
{
    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "get item info failed");
    check(info.nvalue == 1, "iovectors not supported");
    // We can pass in a NULL key to avoid the key check (for tap streams)
    if (key) {
        check(klen == info.nkey, "Incorrect key length");
        check(memcmp(info.key, key, klen) == 0, "Incorrect key value");
    }
    check(vlen == info.value[0].iov_len, "Incorrect value length");
    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");

    return SUCCESS;
}

static enum test_result test_uuid_stats(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1)
{
    vals.clear();
    check(h1->get_stats(h, NULL, "uuid", 4,
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    check(vals["uuid"] == "foobar", "Incorrect uuid");
    return SUCCESS;
}

static enum test_result test_tap_agg_stats(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    std::vector<const void*> cookies;

    cookies.push_back(createTapConn(h, h1, "replica_a"));
    cookies.push_back(createTapConn(h, h1, "replica_b"));
    cookies.push_back(createTapConn(h, h1, "replica_c"));
    cookies.push_back(createTapConn(h, h1, "rebalance_a"));
    cookies.push_back(createTapConn(h, h1, "userconnn"));

    check(get_int_stat(h, h1, "replica:count", "tapagg _") == 3,
          "Incorrect replica count on tap agg");
    check(get_int_stat(h, h1, "rebalance:count", "tapagg _") == 1,
          "Incorrect rebalance count on tap agg");
    check(get_int_stat(h, h1, "_total:count", "tapagg _") == 5,
          "Incorrect total count on tap agg");

    for (auto& cookie : cookies) {
        testHarness.unlock_cookie(cookie);
        testHarness.destroy_cookie(cookie);
    }

    return SUCCESS;
}

static enum test_result test_tap_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + num_keys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            testHarness.unlock_cookie(cookie);
            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_sends_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int ii = 0; ii < num_keys - 2; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS, del(h, h1, ss.str().c_str(), 0, 0), "Delete failed");
    }
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 2);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    int num_mutations = 0;
    int num_deletes = 0;

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
        case TAP_DISCONNECT:
            break;
        case TAP_MUTATION:
            h1->release(h, NULL, it);
            num_mutations++;
            break;
        case TAP_DELETION:
            h1->release(h, NULL, it);
            num_deletes++;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    check(num_mutations == 2, "Incorrect number of remaining mutations");
    check(num_deletes == (num_keys - 2), "Incorrect number of deletes");

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_sent_from_vb(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
        case TAP_DISCONNECT:
            break;
        case TAP_MUTATION:
        case TAP_DELETION:
            h1->release(h, NULL, it);
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:sent_from_vb_0",
                       "tap") == 5, "Incorrect number of items sent");


    std::map<uint16_t, uint64_t> vbmap;
    vbmap[0] = 0;
    changeVBFilter(h, h1, "tap_client_thread", vbmap);
    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:sent_from_vb_0",
                       "tap") == 0, "Incorrect number of items sent");

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_tap_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    int initializedKeys = 0;

    memset(keys, 0, sizeof(keys));

    for (; initializedKeys < num_keys / 2; ++initializedKeys) {
        keys[initializedKeys] = false;
        std::stringstream ss;
        ss << initializedKeys;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + initializedKeys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < initializedKeys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    uint16_t vbucketfilter[2];
    vbucketfilter[0] = ntohs(1);
    vbucketfilter[1] = ntohs(0);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS |
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;
    bool allows_more_mutations(true);

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (initializedKeys < num_keys) {
            keys[initializedKeys] = false;
            std::stringstream ss;
            ss << initializedKeys;
            check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                        "value", NULL, 0, 0) == ENGINE_SUCCESS,
                  "Failed to store an item.");
            ++initializedKeys;
        }

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            // This will be false if we've seen a vbucket set state.
            cb_assert(allows_more_mutations);

            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            break;
        case TAP_DISCONNECT:
            break;
        case TAP_VBUCKET_SET:
            cb_assert(nengine_specific == 4);
            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine_specific);
            state = static_cast<vbucket_state_t>(ntohl(state));
            if (state == vbucket_state_active) {
                allows_more_mutations = false;
            }
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_filter_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint16_t vbid;
    for (vbid = 0; vbid < 4; ++vbid) {
        check(set_vbucket_state(h, h1, vbid, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    const int num_keys = 40;
    bool keys[num_keys];
    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, ii % 4) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::map<uint16_t, uint64_t> filtermap;
    filtermap[0] = 0;
    filtermap[1] = 0;
    filtermap[2] = 0;

    uint16_t numOfVBs = htons(2); // Start with vbuckets 0 and 1
    char *userdata = static_cast<char*>(calloc(1, 28));
    char *ptr = userdata;
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids
        uint16_t vb = htons(i);
        memcpy(ptr, &vb, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
    }
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids and their checkpoint ids
        uint16_t vb = htons(i);
        memcpy(ptr, &vb, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        uint64_t chkid = htonll(filtermap[i]);
        memcpy(ptr, &chkid, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
    }

    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT,
                                             static_cast<void*>(userdata),
                                             28); // userdata length
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;

    tap_event_t event;
    int found = 0;

    uint16_t unlikely_vbucket_identifier = 17293;
    std::string key;
    bool done = false;
    bool filter_change_done = false;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            done = true;
            // Check if all the items except for vbucket 3 are received
            for (int ii = 0; ii < num_keys; ++ii) {
                if ((ii % 4) != 3 && !keys[ii]) {
                    done = false;
                    break;
                }
            }
            if (!done) {
                testHarness.waitfor_cookie(cookie);
            }
            break;
        case TAP_NOOP:
            break;
        case TAP_OPAQUE:
            if (nengine_specific == sizeof(uint32_t)) {
                uint32_t opaque_code;
                memcpy(&opaque_code, engine_specific, sizeof(opaque_code));
                opaque_code = ntohl(opaque_code);
                if (opaque_code == TAP_OPAQUE_COMPLETE_VB_FILTER_CHANGE) {
                    filter_change_done = true;
                }
            }
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
            h1->release(h, cookie, it);
            break;

        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out the key");
            vbid = atoi(key.c_str()) % 4;
            check(vbid == vbucket, "Incorrect vbucket id");
            check(vbid != 3,
                  "Received an item for a vbucket we don't subscribe to");
            keys[atoi(key.c_str())] = true;
            ++found;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            // We've got some of the elements.. Let's change the filter
            // and get the rest
            if (found == 10) {
                changeVBFilter(h, h1, name, filtermap);
                check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      "Expected success response from changing the TAP VB filter.");
            }
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    cb_assert(filter_change_done);
    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:qlen", "tap") == 0,
          "queue should be empty");
    free(userdata);

    return SUCCESS;
}

static enum test_result test_tap_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");
    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "0.05") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "2") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect grace period value");
    return SUCCESS;
}

static enum test_result test_tap_default_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");

    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "5") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "1000") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "300") == 0, "Incorrect grace period value");

    return SUCCESS;
}

static enum test_result test_tap_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    bool receivedKeys[nkeys];
    bool nackKeys[nkeys];

    for (int i = 0; i < nkeys; ++i) {
        nackKeys[i] = true;
        receivedKeys[i] = false;
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_item_info(h, h1, &info, ss.str().c_str()), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int index;
    int numRollbacks = 1000;

    do {
        if (numRollbacks > 0) {
            if (random() % 4 == 0) {
                iter = NULL;
            }
        }

        if (iter == NULL) {
            testHarness.unlock_cookie(cookie);
            iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                        name.length(),
                                        TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                        TAP_CONNECT_CHECKPOINT |
                                        TAP_CONNECT_SUPPORT_ACK |
                                        TAP_CONNECT_FLAG_DUMP,
                                        static_cast<void*>(vbucketfilter),
                                        4);
            check(iter != NULL, "Failed to create a tap iterator");
            testHarness.lock_cookie(cookie);
        }

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
            if (numRollbacks > 0) {
                if (random() % 4 == 0) {
                    iter = NULL;
                }
            }
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, NULL, 0,
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out key");
            index = atoi(key.c_str());
            check(index >= 0 && index <= nkeys, "Illegal key returned");

            testHarness.unlock_cookie(cookie);
            if (nackKeys[index]) {
                nackKeys[index] = false;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
            } else {
                receivedKeys[index] = true;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
            }
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);

            break;
        case TAP_CHECKPOINT_START:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_CHECKPOINT_END:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    for (int ii = 0; ii < nkeys; ++ii) {
        check(receivedKeys[ii], "Did not receive all of the keys");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_tap_implicit_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_item_info(h, h1, &info, ss.str().c_str()), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int mutations = 0;
    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            }
            if (seqno == static_cast<uint32_t>(4294967294UL)) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < static_cast<uint32_t>(4294967295UL));

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            }
            if (seqno == 1) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < 1);

    /* Now just get the rest */
    do {

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);
        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            } else if (event == TAP_DISCONNECT) {
                done = true;
            }
            if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (!done);
    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    check(mutations == 11, "Expected 11 mutations to be returned");
    return SUCCESS;
}

static enum test_result test_set_tap_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    set_param(h, h1, protocol_binary_engine_param_tap, "tap_keepalive", "600");
    check(get_int_stat(h, h1, "ep_tap_keepalive") == 600,
          "Incorrect tap_keepalive value.");
    set_param(h, h1, protocol_binary_engine_param_tap, "tap_keepalive", "5000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected an invalid value error due to exceeding a max value allowed");
    return SUCCESS;
}

static enum test_result test_tap_noop_config_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 200,
          "Expected tap_noop_interval == 200");
    return SUCCESS;
}

static enum test_result test_tap_noop_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 10,
          "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_notify(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    int ii = 0;
    char buffer[1024];
    ENGINE_ERROR_CODE r;

    const void *cookie = testHarness.create_cookie();
    memset(&buffer, 0, sizeof(buffer));
    do {
        std::stringstream ss;
        ss << "Key-"<< ++ii;
        std::string key = ss.str();

        char eng_specific[9];
        memset(eng_specific, 0, sizeof(eng_specific));
        r = h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific), 1, 0,
                           TAP_MUTATION, 0, key.c_str(), key.length(), 0, 0, 0,
                           0, buffer, 1024, 0);
    } while (r == ENGINE_SUCCESS);
    check(r == ENGINE_TMPFAIL, "non-acking streams should etmpfail");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_checkpoint_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    for (int i = 0; i < 5001; i++) {
        char key[8];
        sprintf(key, "key%d", i);
        check(store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0)
                    == ENGINE_SUCCESS, "Failed to store an item.");
        h1->release(h, NULL, itm);
    }
    check(get_int_stat(h, h1, "vb_0:open_checkpoint_id", "checkpoint") == 3,
          "New checkpoint wasn't create after 5001 item creates");
    check(get_int_stat(h, h1, "vb_0:num_open_checkpoint_items", "checkpoint") == 1,
          "New open checkpoint should has only one dirty item");
    return SUCCESS;
}

static enum test_result test_checkpoint_timeout(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    check(store(h, h1, NULL, OPERATION_SET, "key", "value", &itm, 0, 0)
                == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, itm);
    testHarness.time_travel(600);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 2, "checkpoint");
    return SUCCESS;
}

static enum test_result test_checkpoint_deduplication(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 4500; j++) {
            char key[8];
            sprintf(key, "key%d", j);
            check(store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0)
                        == ENGINE_SUCCESS, "Failed to store an item.");
            h1->release(h, NULL, itm);
        }
    }
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoint_items", 4501, "checkpoint");
    return SUCCESS;
}

static enum test_result test_collapse_checkpoints(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item *itm;
    stop_persistence(h, h1);
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 497; ++j) {
            char key[8];
            sprintf(key, "key%ld", j);
            check(store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0)
                        == ENGINE_SUCCESS, "Failed to store an item.");
            h1->release(h, NULL, itm);
        }
        /* Test with app keys with special strings */
        checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "dummy_key",
                                      "value", &itm, 0, 0),
                "Failed to store an item.");
        h1->release(h, NULL, itm);
        checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET,
                                      "checkpoint_start", "value", &itm, 0, 0),
                "Failed to store an item.");
        h1->release(h, NULL, itm);
        checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET,
                                      "checkpoint_end", "value", &itm, 0, 0),
                "Failed to store an item.");
        h1->release(h, NULL, itm);
    }
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica), "Failed to set vbucket state.");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    return SUCCESS;
}

static enum test_result test_item_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 0) ==
            ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalueX", &i, 0, 0) ==
            ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(store(h, h1, NULL, OPERATION_SET, "key1", "somevalueY", &i, 0, 0) ==
            ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    check_key_value(h, h1, "key", "somevalueX", 10);
    check_key_value(h, h1, "key1", "somevalueY", 10);

    check(del(h, h1, "key1", 0, 0) == ENGINE_SUCCESS,
            "Failed remove with value.");
    wait_for_flusher_to_settle(h, h1);

    check(store(h, h1, NULL, OPERATION_SET, "key1", "someothervalue", &i, 0, 0) ==
            ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    check_key_value(h, h1, "key1", "someothervalue", 14);

    check(get_int_stat(h, h1, "vb_active_ops_create") == 3,
            "Expected 3 creations");
    check(get_int_stat(h, h1, "vb_active_ops_update") == 1,
            "Expected 1 updation");
    check(get_int_stat(h, h1, "vb_active_ops_delete") == 1,
            "Expected 1 deletion");

    return SUCCESS;
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.size() > 10, "Kind of expected more stats than that.");
    check(vals.find("ep_version") != vals.end(), "Found no ep_version.");

    return SUCCESS;
}

static enum test_result test_mem_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char value[2048];
    memset(value, 'b', sizeof(value));
    strcpy(value + sizeof(value) - 4, "\r\n");
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    wait_for_persisted_value(h, h1, "key", value);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);
    int mem_used = get_int_stat(h, h1, "mem_used");
    int cache_size = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int value_size = get_int_stat(h, h1, "ep_value_size");
    check((mem_used - overhead) > cache_size,
          "ep_kv_size should be greater than the hashtable cache size due to the checkpoint overhead");
    evict_key(h, h1, "key", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_total_cache_size") <= cache_size,
          "Evict a value shouldn't increase the total cache size");
    check(get_int_stat(h, h1, "mem_used") < mem_used,
          "Expected mem_used to decrease when an item is evicted");

    check_key_value(h, h1, "key", value, strlen(value), 0); // Load an item from disk again.

    check(get_int_stat(h, h1, "mem_used") >= mem_used,
          "Expected mem_used to remain the same after an item is loaded from disk");
    check(get_int_stat(h, h1, "ep_value_size") == value_size,
          "Expected ep_value_size to remain the same after item is loaded from disk");

    return SUCCESS;
}

static enum test_result test_io_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);

    check(get_int_stat(h, h1, "rw_0:io_num_read", "kvstore") == 0,
          "Expected reset stats to set io_num_read to zero");
    check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") == 0,
          "Expected reset stats to set io_num_write to zero");
    check(get_int_stat(h, h1, "rw_0:io_read_bytes", "kvstore") == 0,
          "Expected reset stats to set io_read_bytes to zero");
    check(get_int_stat(h, h1, "rw_0:io_write_bytes", "kvstore") == 0,
          "Expected reset stats to set io_write_bytes to zero");
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    check(get_int_stat(h, h1, "rw_0:io_num_read", "kvstore") == 0 &&
          get_int_stat(h, h1, "rw_0:io_read_bytes", "kvstore") == 0,
          "Expected storing one value to not change the read counter");

    check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") == 1 &&
          get_int_stat(h, h1, "rw_0:io_write_bytes", "kvstore") == 23,
          "Expected storing the key to update the write counter");
    evict_key(h, h1, "a", 0, "Ejected.");

    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    check(get_int_stat(h, h1, "ro_0:io_num_read", "kvstore") == 1 &&
          get_int_stat(h, h1, "ro_0:io_read_bytes", "kvstore") == 4,
          "Expected reading the value back in to update the read counter");
    check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") == 1 &&
          get_int_stat(h, h1, "rw_0:io_write_bytes", "kvstore") == 23,
          "Expected reading the value back in to not update the write counter");

    return SUCCESS;
}

static enum test_result test_vb_file_stats(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_db_data_size", 0);

    int old_data_size = get_int_stat(h, h1, "ep_db_data_size");
    int old_file_size = get_int_stat(h, h1, "ep_db_file_size");
    check(old_file_size != 0, "Expected a non-zero value for ep_db_file_size");

    // Write a value and test ...
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    check(get_int_stat(h, h1, "ep_db_data_size") > old_data_size,
          "Expected the DB data size to increase");
    check(get_int_stat(h, h1, "ep_db_file_size") > old_file_size,
          "Expected the DB file size to increase");

    check(get_int_stat(h, h1, "vb_0:db_data_size", "vbucket-details 0") > 0,
          "Expected the vbucket DB data size to non-zero");
    check(get_int_stat(h, h1, "vb_0:db_file_size", "vbucket-details 0") > 0,
          "Expected the vbucket DB file size to non-zero");
    return SUCCESS;
}

static enum test_result test_vb_file_stats_after_warmup(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    item *it = NULL;
    for (int i = 0; i < 100; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it),
              "Error setting.");
        h1->release(h, NULL, it);
    }
    wait_for_flusher_to_settle(h, h1);

    int fileSize = get_int_stat(h, h1, "vb_0:db_file_size", "vbucket-details 0");
    int spaceUsed = get_int_stat(h, h1, "vb_0:db_data_size", "vbucket-details 0");

    // Restart the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    int newFileSize = get_int_stat(h, h1, "vb_0:db_file_size", "vbucket-details 0");
    int newSpaceUsed = get_int_stat(h, h1, "vb_0:db_data_size", "vbucket-details 0");

    check((float)newFileSize >= 0.9 * fileSize, "Unexpected fileSize for vbucket");
    check((float)newSpaceUsed >= 0.9 * spaceUsed, "Unexpected spaceUsed for vbucket");

    return SUCCESS;
}

static enum test_result test_bg_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    evict_key(h, h1, "a", 0, "Ejected.");
    testHarness.time_travel(43);
    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    checkeq(1, get_int_stat(h, h1, "ep_bg_num_samples"),
            "Expected one sample");

    check(vals.find("ep_bg_min_wait") != vals.end(), "Found no ep_bg_min_wait.");
    check(vals.find("ep_bg_max_wait") != vals.end(), "Found no ep_bg_max_wait.");
    check(vals.find("ep_bg_wait_avg") != vals.end(), "Found no ep_bg_wait_avg.");
    check(vals.find("ep_bg_min_load") != vals.end(), "Found no ep_bg_min_load.");
    check(vals.find("ep_bg_max_load") != vals.end(), "Found no ep_bg_max_load.");
    check(vals.find("ep_bg_load_avg") != vals.end(), "Found no ep_bg_load_avg.");

    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, 0);
    check(get_int_stat(h, h1, "ep_bg_num_samples") == 2,
          "Expected one sample");

    return SUCCESS;
}

static enum test_result test_bg_meta_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    h1->reset_stats(h, NULL);

    wait_for_persisted_value(h, h1, "k1", "v1");
    wait_for_persisted_value(h, h1, "k2", "v2");

    evict_key(h, h1, "k1", 0, "Ejected.");
    check(del(h, h1, "k2", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");
    wait_for_stat_to_be(h, h1, "curr_items", 1);

    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 0");

    check(get_meta(h, h1, "k2"), "Get meta failed");
    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(1, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 1");

    check(h1->get(h, NULL, &itm, "k1", 2, 0) == ENGINE_SUCCESS, "Missing key");
    checkeq(1, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 1");
    checkeq(1, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 1");
    h1->release(h, NULL, itm);

    // store new key with some random metadata
    const size_t keylen = strlen("k3");
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    add_with_meta(h, h1, "k3", keylen, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Set meta failed");

    check(get_meta(h, h1, "k2"), "Get meta failed");
    checkeq(1, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 1");
    checkeq(2, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 2");

    return SUCCESS;
}

static enum test_result test_key_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to store an item.");
    h1->release(h, NULL, i);
    // set (k2,v2) in vbucket 1
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to store an item.");
    h1->release(h, NULL, i);

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "key k2 1";
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vkey_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed set vbucket 2 state.");
    check(set_vbucket_state(h, h1, 3, vbucket_state_active), "Failed set vbucket 3 state.");
    check(set_vbucket_state(h, h1, 4, vbucket_state_active), "Failed set vbucket 4 state.");

    wait_for_persisted_value(h, h1, "k1", "v1");
    wait_for_persisted_value(h, h1, "k2", "v2", 1);
    wait_for_persisted_value(h, h1, "k3", "v3", 2);
    wait_for_persisted_value(h, h1, "k4", "v4", 3);
    wait_for_persisted_value(h, h1, "k5", "v5", 4);

    check(set_vbucket_state(h, h1, 2, vbucket_state_replica), "Failed to set VB2 state.");
    check(set_vbucket_state(h, h1, 3, vbucket_state_pending), "Failed to set VB3 state.");
    check(set_vbucket_state(h, h1, 4, vbucket_state_dead), "Failed to set VB4 state.");

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "vkey k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "vkey k2 1";
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k3" and vbucket "2"
    const char *statkey3 = "vkey k3 2";
    check(h1->get_stats(h, cookie, statkey3, strlen(statkey3), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k4" and vbucket "3"
    const char *statkey4 = "vkey k4 3";
    check(h1->get_stats(h, cookie, statkey4, strlen(statkey4), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k5" and vbucket "4"
    const char *statkey5 = "vkey k5 4";
    check(h1->get_stats(h, cookie, statkey5, strlen(statkey5), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_warmup_conf(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(get_int_stat(h, h1, "ep_warmup_min_items_threshold") == 100,
          "Incorrect initial warmup min items threshold.");
    check(get_int_stat(h, h1, "ep_warmup_min_memory_threshold") == 100,
          "Incorrect initial warmup min memory threshold.");

    check(!set_param(h, h1, protocol_binary_engine_param_flush, "warmup_min_items_threshold", "a"),
          "Set warmup_min_items_threshold should have failed");
    check(!set_param(h, h1, protocol_binary_engine_param_flush, "warmup_min_items_threshold", "a"),
          "Set warmup_min_memory_threshold should have failed");

    check(set_param(h, h1, protocol_binary_engine_param_flush, "warmup_min_items_threshold", "80"),
          "Set warmup_min_items_threshold should have worked");
    check(set_param(h, h1, protocol_binary_engine_param_flush, "warmup_min_memory_threshold", "80"),
          "Set warmup_min_memory_threshold should have worked");

    check(get_int_stat(h, h1, "ep_warmup_min_items_threshold") == 80,
          "Incorrect smaller warmup min items threshold.");
    check(get_int_stat(h, h1, "ep_warmup_min_memory_threshold") == 80,
          "Incorrect smaller warmup min memory threshold.");

    item *it = NULL;
    for (int i = 0; i < 100; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    std::string config(testHarness.get_current_testcase()->cfg);
    config = config + "warmup_min_memory_threshold=0";
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(vals.find("ep_warmup_key_count")->second == "100",
          "Expected 100 keys loaded after warmup");
    check(vals.find("ep_warmup_value_count")->second == "0",
          "Expected 0 values loaded after warmup");

    return SUCCESS;
}

static enum test_result test_bloomfilter_conf(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    if (get_bool_stat(h, h1, "ep_bfilter_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "bfilter_enabled", "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_bfilter_enabled"),
          "Bloom filter wasn't enabled");

    check(get_float_stat(h, h1, "ep_bfilter_residency_threshold") == (float)0.1,
          "Incorrect initial bfilter_residency_threshold.");

    check(set_param(h, h1, protocol_binary_engine_param_flush,
          "bfilter_enabled", "false"),
          "Set bloomfilter_enabled should have worked.");
    check(set_param(h, h1, protocol_binary_engine_param_flush,
          "bfilter_residency_threshold", "0.15"),
          "Set bfilter_residency_threshold should have worked.");

    check(get_bool_stat(h, h1, "ep_bfilter_enabled") == false,
          "Bloom filter should have been disabled.");
    check(get_float_stat(h, h1, "ep_bfilter_residency_threshold") == (float)0.15,
          "Incorrect bfilter_residency_threshold.");

    return SUCCESS;
}

static enum test_result test_bloomfilters(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {

    if (get_bool_stat(h, h1, "ep_bfilter_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "bfilter_enabled", "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_bfilter_enabled"),
            "Bloom filter wasn't enabled");

    // Key is only present if bgOperations is non-zero.
    int num_read_attempts = get_int_stat_or_default(h, h1, 0,
                                                    "ep_bg_num_samples");

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, h1, 0, 1, 1, 0);
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    int i;
    item *it = NULL;

    // Insert 10 items.
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(),
                    "somevalue", &it),
                    "Error setting.");
        h1->release(h, NULL, it);
    }
    wait_for_flusher_to_settle(h, h1);

    // Evict all 10 items.
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        evict_key(h, h1, key.str().c_str(), 0, "Ejected.");
    }
    wait_for_flusher_to_settle(h, h1);

    // Ensure 10 items are non-resident.
    cb_assert(10 == get_int_stat(h, h1, "ep_num_non_resident"));

    // Issue delete on first 5 items.
    for (i = 0; i < 5; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(del(h, h1, key.str().c_str(), 0, 0) == ENGINE_SUCCESS,
              "Failed remove with value.");
    }
    wait_for_flusher_to_settle(h, h1);

    // Ensure that there are 5 non-resident items
    cb_assert(5 == get_int_stat(h, h1, "ep_num_non_resident"));
    cb_assert(5 == get_int_stat(h, h1, "curr_items"));

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    if (eviction_policy == "value_only") {  // VALUE-ONLY EVICTION MODE

        check(get_int_stat(h, h1, "vb_0:bloom_filter_key_count", "vbucket-details 0")
                == 5, "Unexpected no. of keys in bloom filter");

        check(get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples") == num_read_attempts,
              "Expected bgFetch attempts to remain unchanged");

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, h1, key.str().c_str()), "Get meta failed");
        }

        // GetMeta would cause bgFetches as bloomfilter contains
        // the deleted items.
        check(get_int_stat(h, h1, "ep_bg_num_samples") == num_read_attempts + 5,
                "Expected bgFetch attempts to increase by five");

        // Run compaction, with drop_deletes
        compact_db(h, h1, 0, 15, 15, 1);
        while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, h1, key.str().c_str()), "Get meta failed");
        }
        check(get_int_stat(h, h1, "ep_bg_num_samples") == num_read_attempts + 5,
                "Expected bgFetch attempts to stay as before");

    } else {                                // FULL EVICTION MODE

        check(get_int_stat(h, h1, "vb_0:bloom_filter_key_count", "vbucket-details 0")
                == 10, "Unexpected no. of keys in bloom filter");


        // Because of issuing deletes on non-resident items
        check(get_int_stat(h, h1, "ep_bg_num_samples") == num_read_attempts + 5,
                "Expected bgFetch attempts to increase by five, after deletes");

        // Run compaction, with drop_deletes, to exclude deleted items
        // from bloomfilter.
        compact_db(h, h1, 0, 15, 15, 1);
        while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; i++) {
            std::stringstream key;
            key << "key-" << i;
            check(h1->get(h, NULL, &it, key.str().c_str(), key.str().length(), 0)
                  == ENGINE_KEY_ENOENT,
                  "Unable to get stored item");
        }
        // + 6 because last delete is not purged by the compactor
        check(get_int_stat(h, h1, "ep_bg_num_samples") == num_read_attempts + 6,
                "Expected bgFetch attempts to stay as before");
    }

    return SUCCESS;
}

static enum test_result test_bloomfilters_with_store_apis(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    if (get_bool_stat(h, h1, "ep_bfilter_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "bfilter_enabled", "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_bfilter_enabled"),
            "Bloom filter wasn't enabled");

    int num_read_attempts = get_int_stat_or_default(h, h1, 0,
                                                    "ep_bg_num_samples");

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, h1, 0, 1, 1, 0);
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    for (int i = 0; i < 1000; i++) {
        std::stringstream key;
        key << "key-" << i;
        check(get_meta(h, h1, key.str().c_str()) == false,
                "Get meta should fail.");
    }

    check(get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples") == num_read_attempts + 0,
            "Expected no bgFetch attempts");

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    if (eviction_policy == "full_eviction") {  // FULL EVICTION MODE
        // Set with Meta
        int j;
        for (j = 0; j < 10; j++) {
            uint64_t cas_for_set = last_cas;
            // init some random metadata
            ItemMetaData itm_meta;
            itm_meta.revSeqno = 10;
            itm_meta.cas = 0xdeadbeef;
            itm_meta.exptime = time(NULL) + 300;
            itm_meta.flags = 0xdeadbeef;

            std::stringstream key;
            key << "swm-" << j;
            set_with_meta(h, h1, key.str().c_str(), key.str().length(),
                          "somevalue", 9, 0, &itm_meta, cas_for_set);
        }

        check(get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples") == num_read_attempts + 0,
                "Expected no bgFetch attempts");

        item *itm = NULL;
        // Add
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "add-" << j;

            check(store(h, h1, NULL, OPERATION_ADD, key.str().c_str(),
                        "newvalue", &itm) == ENGINE_SUCCESS,
                    "Failed to add value again.");
            h1->release(h, NULL, itm);
        }

        check(get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples") == num_read_attempts + 0,
                "Expected no bgFetch attempts");

        // Delete
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "del-" << j;
            check(del(h, h1, key.str().c_str(), 0, 0) == ENGINE_KEY_ENOENT,
                    "Failed remove with value.");
        }

        check(get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples") == num_read_attempts + 0,
                "Expected no bgFetch attempts");

    }

    return SUCCESS;
}

static enum test_result test_bloomfilter_delete_plus_set_scenario(
                                       ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (get_bool_stat(h, h1, "ep_bfilter_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "bfilter_enabled", "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_bfilter_enabled"),
            "Bloom filter wasn't enabled");

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, h1, 0, 1, 1, 0);
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &itm) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, itm);

    wait_for_flusher_to_settle(h, h1);
    int num_writes = get_int_stat(h, h1, "rw_0:io_num_write", "kvstore");
    int num_persisted = get_int_stat(h, h1, "ep_total_persisted");
    cb_assert(num_writes == 1 && num_persisted == 1);

    check(del(h, h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");
    stop_persistence(h, h1);
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v2", &itm, 0, 0) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, itm);
    int key_count = get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                                 "vbucket-details 0");

    if (key_count == 0) {
        check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") <= 2,
                "Unexpected number of writes");
        start_persistence(h, h1);
        wait_for_flusher_to_settle(h, h1);
        check(get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                           "vbucket-details 0") == 0,
                "Unexpected number of keys in bloomfilter");
    } else {
        cb_assert(key_count == 1);
        check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") == 2,
                "Unexpected number of writes");
        start_persistence(h, h1);
        wait_for_flusher_to_settle(h, h1);
        check(get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                           "vbucket-details 0") == 1,
                "Unexpected number of keys in bloomfilter");
    }

    return SUCCESS;
}

static enum test_result test_datatype(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_datatype_support(cookie, true);

    item *itm = NULL;
    const std::string key("{\"foo\":\"bar\"}");
    const protocol_binary_datatypes datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    uint64_t cas = 0;
    std::string value("x");
    check(storeCasOut(h, h1, NULL, 0, key, value, datatype, itm, cas)
          == ENGINE_SUCCESS,
          "Expected set to succeed");

    check(h1->get(h, cookie, &itm, key.c_str(), key.size(), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");

    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, cookie, itm);
    check(info.datatype == 0x01, "Invalid datatype");

    const char* key1 = "foo";
    const char* val1 = "{\"foo1\":\"bar1\"}";
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = info.cas;
    itm_meta.exptime = info.exptime;
    itm_meta.flags = info.flags;
    set_with_meta(h, h1, key1, strlen(key1), val1, strlen(val1), 0, &itm_meta,
                  last_cas, false, info.datatype, false, 0, 0, cookie);

    check(h1->get(h, cookie, &itm, key1, strlen(key1), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");

    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, cookie, itm);
    check(info.datatype == 0x01, "Invalid datatype, when setWithMeta");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_datatype_with_unknown_command(ENGINE_HANDLE *h,
                                                           ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_datatype_support(cookie, true);
    item *itm = NULL;
    const char* key = "foo";
    const char* val = "{\"foo\":\"bar\"}";
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON;

    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0;
    itm_meta.exptime = 0;
    itm_meta.flags = 0;

    //SET_WITH_META
    set_with_meta(h, h1, key, strlen(key), val, strlen(val), 0, &itm_meta,
                  0, false, datatype, false, 0, 0, cookie);

    check(h1->get(h, cookie, &itm, key, strlen(key), 0) == ENGINE_SUCCESS,
            "Unable to get stored item");

    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, NULL, itm);
    check(info.datatype == 0x01, "Invalid datatype, when setWithMeta");

    //SET_RETURN_META
    set_ret_meta(h, h1, "foo1", 4, val, strlen(val), 0, 0, 0, 0, datatype,
                 cookie);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(last_datatype == 0x01, "Invalid datatype, when set_return_meta");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_session_cas_validation(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    //Testing PROTOCOL_BINARY_CMD_SET_VBUCKET..
    char ext[4];
    protocol_binary_request_header *pkt;
    vbucket_state_t state = vbucket_state_active;
    uint32_t val = static_cast<uint32_t>(state);
    val = htonl(val);
    memcpy(ext, (char*)&val, sizeof(val));

    uint64_t cas = 0x0101010101010101;
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_VBUCKET, 0, cas, ext, 4);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
            "SET_VBUCKET command failed");
    free(pkt);
    cb_assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    cas = 0x0102030405060708;
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_VBUCKET, 0, cas, ext, 4);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
            "SET_VBUCKET command failed");
    free(pkt);
    cb_assert(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_access_scanner_settings(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    std::string err_msg;
    // Check access scanner is enabled and alog_task_time is at default
    cb_assert(get_bool_stat(h, h1, "ep_access_scanner_enabled"));
    cb_assert(get_int_stat(h, h1, "ep_alog_task_time") == 2);

    // Ensure access_scanner_task_time is what its expected to be
    std::string str = get_str_stat(h, h1, "ep_access_scanner_task_time");
    std::string expected_time = "02:00";
    err_msg.assign("Initial time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update alog_task_time and ensure the update is successful
    set_param(h, h1, protocol_binary_engine_param_flush, "alog_task_time", "5");
    expected_time = "05:00";
    str = get_str_stat(h, h1, "ep_access_scanner_task_time");
    err_msg.assign("Updated time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update alog_sleep_time and ensure the update is successful
    int update_by = 10;
    time_t now = time(NULL);
    struct tm curr = *(gmtime(&now));
    curr.tm_min += update_by;
#ifdef _MSC_VER
    _mkgmtime(&curr);
#else
    timegm(&curr);
#endif
    char timeStr[20];
    strftime(timeStr, 20, "%Y-%m-%d %H:%M:%S", &curr);
    std::string targetTaskTime1(timeStr);

    set_param(h, h1, protocol_binary_engine_param_flush, "alog_sleep_time",
              std::to_string(update_by).c_str());
    str = get_str_stat(h, h1, "ep_access_scanner_task_time");

    now = time(NULL);
    curr = *(gmtime(&now));
    curr.tm_min += update_by;
#ifdef _MSC_VER
    _mkgmtime(&curr);
#else
    timegm(&curr);
#endif
    strftime(timeStr, 20, "%Y-%m-%d %H:%M:%S", &curr);
    std::string targetTaskTime2(timeStr);

    // ep_access_scanner_task_time should fall within the range of
    // targetTaskTime1 and targetTaskTime2
    err_msg.assign("Unexpected task time range, expect: " +
                   targetTaskTime1 + " <= " + str + " <= " + targetTaskTime2);
    check(targetTaskTime1 <= str, err_msg.c_str());
    check(str <= targetTaskTime2, err_msg.c_str());

    return SUCCESS;
}

static enum test_result test_access_scanner(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    const int num_shards = get_int_stat(h, h1, "ep_workload:num_shards",
                                        "workload");
    int access_scanner_skips = 0, alog_runs = 0;
    std::string name("/tmp/epaccess.log.0");
    std::string prev(name + ".old");

    /* Get the resident ratio down to below 90% */
    int num_items = 0;
    while (true) {
        // Gathering stats on every store is expensive, just check every 100 iterations
        if ((num_items % 100) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") < 94) {
                break;
            }
        }

        item *itm = NULL;
        std::string key("key" + std::to_string(num_items));
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET,
                                      key.c_str(), "somevalue", &itm);
        if (ret == ENGINE_SUCCESS) {
            num_items++;
        }
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= ((float)(6/100) * num_items));

    /* Run access scanner task twice and expect it to generate access log */
    for(int i = 0; i < 2; i++) {
        alog_runs = get_int_stat(h, h1, "ep_num_access_scanner_runs");
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                        "access_scanner_run", "true"),
              "Failed to trigger access scanner");
        wait_for_stat_to_be(h, h1, "ep_num_access_scanner_runs",
                            alog_runs + num_shards);
    }

    /* This time since resident ratio is < 90% access log should be generated */
    checkeq(0, access(name.c_str(), F_OK), "access log file should exist");

    /* Increase resident ratio by deleting items */
    vbucketDelete(h, h1, 0);
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set VB0 state.");

    /* Run access scanner task once */
    access_scanner_skips = get_int_stat(h, h1, "ep_num_access_scanner_skips");
    check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "access_scanner_run", "true"),
          "Failed to trigger access scanner");
    wait_for_stat_to_be(h, h1, "ep_num_access_scanner_skips",
                        access_scanner_skips + num_shards);

    /* Access log files should be removed because resident ratio > 95% */
    checkeq(-1, access(prev.c_str(), F_OK),
            ".old access log file should not exist");
    checkeq(-1, access(name.c_str(), F_OK), "access log file should not exist");

    return SUCCESS;
}

static enum test_result test_warmup_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed to set VB0 state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set VB1 state.");

    for (int i = 0; i < 5000; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    check(vals.find("ep_warmup_thread") != vals.end(), "Found no ep_warmup_thread");
    check(vals.find("ep_warmup_value_count") != vals.end(), "Found no ep_warmup_value_count");
    check(vals.find("ep_warmup_key_count") != vals.end(), "Found no ep_warmup_key_count");
    check(vals.find("ep_warmup_dups") != vals.end(), "Found no ep_warmup_dups");
    check(vals.find("ep_warmup_oom") != vals.end(), "Found no ep_warmup_oom");
    check(vals.find("ep_warmup_time") != vals.end(), "Found no ep_warmup_time");
    std::string warmup_time = vals["ep_warmup_time"];
    cb_assert(atoi(warmup_time.c_str()) > 0);

    vals.clear();
    check(h1->get_stats(h, NULL, "prev-vbucket", 12, add_stats) == ENGINE_SUCCESS,
          "Failed to get the previous state of vbuckets");
    check(vals.find("vb_0") != vals.end(), "Found no previous state for VB0");
    check(vals.find("vb_1") != vals.end(), "Found no previous state for VB1");
    std::string vb0_prev_state = vals["vb_0"];
    std::string vb1_prev_state = vals["vb_1"];
    cb_assert(strcmp(vb0_prev_state.c_str(), "active") == 0);
    cb_assert(strcmp(vb1_prev_state.c_str(), "replica") == 0);
    vals.clear();

    check(h1->get_stats(h, NULL, "vbucket-details", 15, add_stats) ==
          ENGINE_SUCCESS, "Failed to get the detailed stats of vbuckets");
    check(vals.find("vb_0:num_items")->second == "5000",
          "Expected 5000 items in VB 0");
    check(vals.find("vb_1:num_items")->second == "0",
          "Expected zero items in VB 1");

    return SUCCESS;
}

static enum test_result test_warmup_with_threshold(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set vbucket 1 state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 2 state.");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed set vbucket 3 state.");
    check(set_vbucket_state(h, h1, 3, vbucket_state_active), "Failed set vbucket 4 state.");

    for (int i = 0; i < 10000; ++i) {
        std::stringstream key;
        key << "key+" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it,
                    0, (i % 4)),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);

    check(get_int_stat(h, h1, "ep_warmup_min_item_threshold", "warmup") == 1,
            "Unable to set warmup_min_item_threshold to 1%");

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    std::string policy = vals.find("ep_item_eviction_policy")->second;
    if (policy == "full_eviction") {
        check(get_int_stat(h, h1, "ep_warmup_key_count", "warmup") ==
                get_int_stat(h, h1, "ep_warmup_value_count", "warmup"),
                "Warmed up key count didn't match warmed up value count");
    } else {
        check(get_int_stat(h, h1, "ep_warmup_key_count", "warmup") == 10000,
                "Warmup didn't warmup all keys");
    }
    check(get_int_stat(h, h1, "ep_warmup_value_count", "warmup") <= 110,
            "Warmed up value count found to be greater than 1%");
    std::string warmup_time = vals["ep_warmup_time"];
    cb_assert(atoi(warmup_time.c_str()) > 0);

    return SUCCESS;
}

#if 0
// Comment out the entire test since the hack gave warnings on win32
static enum test_result test_warmup_accesslog(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
#ifdef __APPLE__
    /* I'm getting a weird link error from clang.. disable the test until I
    ** understand why
    */
    return SKIPPED;
#else
    item *it = NULL;

    int n_items_to_store1 = 10;
    for (int i = 0; i < n_items_to_store1; ++i) {
        std::stringstream key;
        key << "key-" << i;
        const char* keystr = key.str().c_str();
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, keystr, "somevalue", &it, 0, 0),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    wait_for_flusher_to_settle(h, h1);

    int n_items_to_access = 10;
    for (int i = 0; i < n_items_to_access; ++i) {
        std::stringstream key;
        key << "key-" << i;
        const char* keystr = key.str().c_str();
        check(ENGINE_SUCCESS ==
              h1->get(h, NULL, &it, keystr, strlen(keystr), 0),
              "Error getting.");
        h1->release(h, NULL, it);
    }

    // sleep so that scanner task can have timew to generate access log
    sleep(61);

    // store additional items
    int n_items_to_store2 = 10;
    for (int i = 0; i < n_items_to_store2; ++i) {
        std::stringstream key;
        key << "key2-" << i;
        const char* keystr = key.str().c_str();
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, keystr, "somevalue", &it, 0, 0),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    // n_items_to_access items should be loaded from access log first
    // but we continue to load until we hit 75% item watermark

    int warmedup = get_int_stat(h, h1, "ep_warmup_value_count", "warmup");
    //    std::cout << "ep_warmup_value_count = " << warmedup << std::endl;
    int expected = (n_items_to_store1 + n_items_to_store2) * 0.75 + 1;

    check(warmedup == expected, "Expected 16 items to be resident");
    return SUCCESS;
#endif
}
#endif

static enum test_result test_cbd_225(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // get engine startup token
    time_t token1 = get_int_stat(h, h1, "ep_startup_time");
    check(token1 != 0, "Expected non-zero startup token");

    // store some random data
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    // check token again, which should be the same as before
    time_t token2 = get_int_stat(h, h1, "ep_startup_time");
    check(token2 == token1, "Expected the same startup token");

    // reload the engine
    testHarness.time_travel(10);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // check token, this time we should get a different one
    time_t token3 = get_int_stat(h, h1, "ep_startup_time");
    check(token3 != token1, "Expected a different startup token");

    return SUCCESS;
}

static enum test_result test_workload_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void* cookie = testHarness.create_cookie();
    check(h1->get_stats(h, cookie, "workload",
                        strlen("workload"), add_stats) == ENGINE_SUCCESS,
                        "Falied to get workload stats");
    testHarness.destroy_cookie(cookie);
    int num_read_threads = get_int_stat(h, h1, "ep_workload:num_readers",
                                               "workload");
    int num_write_threads = get_int_stat(h, h1, "ep_workload:num_writers",
                                                "workload");
    int num_auxio_threads = get_int_stat(h, h1, "ep_workload:num_auxio",
                                                "workload");
    int num_nonio_threads = get_int_stat(h, h1, "ep_workload:num_nonio",
                                                "workload");
    int max_read_threads = get_int_stat(h, h1, "ep_workload:max_readers",
                                               "workload");
    int max_write_threads = get_int_stat(h, h1, "ep_workload:max_writers",
                                                "workload");
    int max_auxio_threads = get_int_stat(h, h1, "ep_workload:max_auxio",
                                                "workload");
    int max_nonio_threads = get_int_stat(h, h1, "ep_workload:max_nonio",
                                                "workload");
    int num_shards = get_int_stat(h, h1, "ep_workload:num_shards", "workload");
    check(num_read_threads == 4, "Incorrect number of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    check(num_write_threads == 4, "Incorrect number of writers");
    check(num_auxio_threads == 1, "Incorrect number of auxio threads");
    check(num_nonio_threads == 1, "Incorrect number of nonio threads");
    check(max_read_threads == 4, "Incorrect limit of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    check(max_write_threads == 4, "Incorrect limit of writers");
    check(max_auxio_threads == 1, "Incorrect limit of auxio threads");
    check(max_nonio_threads == 1, "Incorrect limit of nonio threads");
    check(num_shards == 5, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_max_workload_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void* cookie = testHarness.create_cookie();
    check(h1->get_stats(h, cookie, "workload",
                        strlen("workload"), add_stats) == ENGINE_SUCCESS,
                        "Falied to get workload stats");
    testHarness.destroy_cookie(cookie);
    int num_read_threads = get_int_stat(h, h1, "ep_workload:num_readers",
                                               "workload");
    int num_write_threads = get_int_stat(h, h1, "ep_workload:num_writers",
                                                "workload");
    int num_auxio_threads = get_int_stat(h, h1, "ep_workload:num_auxio",
                                                "workload");
    int num_nonio_threads = get_int_stat(h, h1, "ep_workload:num_nonio",
                                                "workload");
    int max_read_threads = get_int_stat(h, h1, "ep_workload:max_readers",
                                               "workload");
    int max_write_threads = get_int_stat(h, h1, "ep_workload:max_writers",
                                                "workload");
    int max_auxio_threads = get_int_stat(h, h1, "ep_workload:max_auxio",
                                                "workload");
    int max_nonio_threads = get_int_stat(h, h1, "ep_workload:max_nonio",
                                                "workload");
    int num_shards = get_int_stat(h, h1, "ep_workload:num_shards", "workload");
    // if max limit on other groups missing use remaining for readers & writers
    check(num_read_threads == 5, "Incorrect number of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    check(num_write_threads == 4, "Incorrect number of writers");

    check(num_auxio_threads == 1, "Incorrect number of auxio threads");// config
    check(num_nonio_threads == 4, "Incorrect number of nonio threads");// config
    check(max_read_threads == 5, "Incorrect limit of readers");// derived
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    check(max_write_threads == 4, "Incorrect limit of writers");// max-capped
    check(max_auxio_threads == 1, "Incorrect limit of auxio threads");// config
    check(max_nonio_threads == 4, "Incorrect limit of nonio threads");// config
    check(num_shards == 5, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_worker_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "dispatcher",
                        strlen("dispatcher"), add_stats) == ENGINE_SUCCESS,
                        "Failed to get worker stats");

    std::set<std::string> tasklist;
    tasklist.insert("Running a flusher loop");
    tasklist.insert("Snapshotting vbucket states for the shard");
    tasklist.insert("Deleting VBucket");
    tasklist.insert("Updating stat snapshot on disk");
    tasklist.insert("Batching background fetch");
    tasklist.insert("Fetching item from disk for vkey stat");
    tasklist.insert("Fetching item from disk");
    tasklist.insert("Loading TAP backfill from disk");
    tasklist.insert("Tap connection notifier");
    tasklist.insert("Generating access log");
    tasklist.insert("Fetching item from disk for tap");
    tasklist.insert("Snapshotting vbucket states");
    tasklist.insert("Persisting a vbucket state for vbucket");
    tasklist.insert("Reaping tap or dcp connection");
    tasklist.insert("Warmup - initialize");
    tasklist.insert("Warmup - creating vbuckets");
    tasklist.insert("Warmup - estimate item count");
    tasklist.insert("Warmup - key dump");
    tasklist.insert("Warmup - check for access log");
    tasklist.insert("Warmup - loading access log");
    tasklist.insert("Warmup - loading KV Pairs");
    tasklist.insert("Warmup - loading data");
    tasklist.insert("Warmup - completion");
    tasklist.insert("Not currently running any task");

    std::set<std::string> statelist;
    statelist.insert("creating");
    statelist.insert("running");
    statelist.insert("waiting");
    statelist.insert("sleeping");
    statelist.insert("shutdown");
    statelist.insert("dead");

    std::string worker_0_task = vals["reader_worker_0:task"];
    unsigned pos = worker_0_task.find(":");
    worker_0_task = worker_0_task.substr(0, pos ? pos : worker_0_task.size());
    std::string worker_0_state = vals["reader_worker_0:state"];
    check(tasklist.find(worker_0_task)!=tasklist.end(),
          "worker_0's Current task incorrect");
    check(statelist.find(worker_0_state)!=statelist.end(),
          "worker_0's state incorrect");
    std::string worker_1_task = vals["reader_worker_1:task"];
    pos = worker_1_task.find(":");
    worker_1_task = worker_1_task.substr(0, pos ? pos : worker_1_task.size());
    std::string worker_1_state = vals["reader_worker_1:state"];
    check(tasklist.find(worker_1_task)!=tasklist.end(),
          "worker_1's Current task incorrect");
    check(statelist.find(worker_1_state)!=statelist.end(),
          "worker_1's state incorrect");

    check(get_int_stat(h, h1, "ep_num_workers") == 10, // cannot spawn less
          "Incorrect number of threads spawned");
    return SUCCESS;
}

static enum test_result test_cluster_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_active),
                    "VBucket state not active");
    uint64_t var = 1234;
    protocol_binary_request_header *pkt1 =
        createPacket(PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG, 1, 0, NULL, 0, NULL, 0, (char*)&var, 8);
    check(h1->unknown_command(h, NULL, pkt1, add_response) == ENGINE_SUCCESS,
            "Failed to set cluster configuration");
    free(pkt1);

    protocol_binary_request_header *pkt2 =
        createPacket(PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, 1, 0, NULL, 0, NULL, 0, NULL, 0);
    check(h1->unknown_command(h, NULL, pkt2, add_response) == ENGINE_SUCCESS,
            "Failed to get cluster configuration");
    free(pkt2);
    if (last_body.compare(0, sizeof(var), reinterpret_cast<char*>(&var),
                          sizeof(var)) != 0) {
        return FAIL;
    } else {
        return SUCCESS;
    }
}

static enum test_result test_not_my_vbucket_with_cluster_config(ENGINE_HANDLE *h,
                                                                ENGINE_HANDLE_V1 *h1) {
    uint64_t var = 4321;
    protocol_binary_request_header *pkt1 =
        createPacket(PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG, 1, 0, NULL, 0, NULL, 0, (char*)&var, 8);
    check(h1->unknown_command(h, NULL, pkt1, add_response) == ENGINE_SUCCESS,
            "Failed to set cluster configuration");
    free(pkt1);

    protocol_binary_request_header *pkt2 =
        createPacket(PROTOCOL_BINARY_CMD_GET_VBUCKET, 1, 0, NULL, 0, NULL, 0, NULL, 0);
    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, pkt2,
                                                add_response);
    check(ret == ENGINE_SUCCESS, "Should've received not_my_vbucket/cluster config");
    free(pkt2);
    if (last_body.compare(0, sizeof(var), reinterpret_cast<char*>(&var),
                          sizeof(var)) != 0) {
        return FAIL;
    } else {
        return SUCCESS;
    }
    check(verify_key(h, h1, "key", 2) == ENGINE_NOT_MY_VBUCKET, "Expected miss");
    check(h1->get_engine_vb_map(h, NULL, vb_map_response) == ENGINE_SUCCESS,
            "Failed to recover cluster configuration");
    if (last_body.compare(0, sizeof(var), reinterpret_cast<char*>(&var),
                          sizeof(var)) != 0) {
        return FAIL;
    } else {
        return SUCCESS;
    }
}

static enum test_result test_all_keys_api(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    const int start_key_idx = 10, del_key_idx = 12, num_keys = 5,
              total_keys = 100;

    for (uint32_t i = 0; i < total_keys; ++i) {
        std::string key("key_" + std::to_string(i));
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, it->c_str(),
                                      it->c_str(), &itm, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    std::string del_key("key_" + std::to_string(del_key_idx));
    checkeq(ENGINE_SUCCESS, del(h, h1, del_key.c_str(), 0, 0),
            "Failed to delete key");
    wait_for_flusher_to_settle(h, h1);
    checkeq(total_keys - 1, get_int_stat(h, h1, "curr_items"),
            "Item count mismatch");

    std::string start_key("key_" + std::to_string(start_key_idx));
    const uint16_t keylen = start_key.length();
    uint32_t count = htonl(num_keys);

    protocol_binary_request_header *pkt1 =
        createPacket(CMD_GET_KEYS, 0, 0, reinterpret_cast<char*>(&count),
                     sizeof(count), start_key.c_str(), keylen, NULL, 0, 0x00);

    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt1, add_response),
            "Failed to get all_keys, sort: ascending");
    free(pkt1);

    /* Check the keys. */
    size_t offset = 0;
    /* Since we have one deleted key, we must go till num_keys + 1 */
    for (size_t i = 0; i < num_keys + 1; ++i) {
        if (del_key_idx == start_key_idx + i) {
            continue;
        }
        uint16_t len;
        memcpy(&len, last_body.data() + offset, sizeof(uint16_t));
        len = ntohs(len);
        checkeq(keylen, len, "Key length mismatch in all_docs response");
        std::string key("key_" + std::to_string(start_key_idx + i));
        offset += sizeof(uint16_t);
        checkeq(0, last_body.compare(offset, keylen, key.c_str()),
                "Key mismatch in all_keys response");
        offset += keylen;
    }

    return SUCCESS;
}

static enum test_result test_all_keys_api_during_bucket_creation(
                                ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    uint32_t count = htonl(5);
    const char key[] = "key_10";

    protocol_binary_request_header *pkt1 =
        createPacket(CMD_GET_KEYS, 1, 0, reinterpret_cast<char*>(&count),
                     sizeof(count), key, strlen(key), NULL, 0, 0x00);

    stop_persistence(h, h1);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed set vbucket 1 state.");

    ENGINE_ERROR_CODE err = h1->unknown_command(h, NULL, pkt1,
                                                add_response);
    free(pkt1);
    start_persistence(h, h1);

    check(err == ENGINE_SUCCESS,
          "Unexpected return code from all_keys_api");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Unexpected response status");

    return SUCCESS;
}

static enum test_result test_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // Verify initial case.
    verify_curr_items(h, h1, 0, "init");

    // Verify set and add case
    check(store(h, h1, NULL, OPERATION_ADD,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 3, "three items stored");
    cb_assert(3 == get_int_stat(h, h1, "ep_total_enqueued"));

    wait_for_flusher_to_settle(h, h1);

    // Verify delete case.
    check(del(h, h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");

    wait_for_stat_change(h, h1, "curr_items", 3);
    verify_curr_items(h, h1, 2, "one item deleted - persisted");

    // Verify flush case (remove the two remaining from above)
    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);
    verify_curr_items(h, h1, 0, "flush");

    // Verify dead vbucket case.
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 0, "dead vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 with a dead vbucket");

    // Then resurrect.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 3, "resurrected vbucket");

    // Now completely delete it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");
    vbucketDelete(h, h1, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success deleting vbucket.");
    verify_curr_items(h, h1, 0, "del vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 after deleting a vbucket");

    return SUCCESS;
}

static enum test_result test_value_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    item *i = NULL;
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    check(get_int_stat(h, h1, "vb_active_num_non_resident") == 0,
          "Expected all active vbucket items to be resident");


    stop_persistence(h, h1);
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k1", 0, "Can't eject: Dirty object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k2", 1, "Can't eject: Dirty object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1, "k1", 0, "Ejected.");
    evict_key(h, h1, "k2", 1, "Ejected.");

    check(get_int_stat(h, h1, "vb_active_num_non_resident") == 2,
          "Expected two non-resident items for active vbuckets");

    evict_key(h, h1, "k1", 0, "Already ejected.");
    evict_key(h, h1, "k2", 1, "Already ejected.");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_EVICT_KEY, 0, 0,
                                                       NULL, 0, "missing-key", 11);
    pkt->request.vbucket = htons(0);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to evict key.");

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "value_only") {
        check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
              "expected the key to be missing...");
    } else {
        // Note that we simply return SUCCESS when EVICT_KEY is issued to
        // a non-resident or non-existent key with full eviction to avoid a disk lookup.
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "expected the success for evicting a non-existent key with full eviction");
    }
    free(pkt);

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");

    check_key_value(h, h1, "k1", "v1", 2);
    checkeq(1, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected only one active vbucket item to be non-resident");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica), "Failed to set vbucket state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    checkeq(0, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected no non-resident items");

    return SUCCESS;
}

static enum test_result test_mb5172(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key-1", "value-1", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key-2", "value-2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");

    // restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    return SUCCESS;
}


static enum test_result test_mb3169(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    uint64_t result(0);
    check(store(h, h1, NULL, OPERATION_SET, "set", "value", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "incr", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "delete", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "get", "getvalue", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 4);

    evict_key(h, h1, "set", 0, "Ejected.");
    evict_key(h, h1, "incr", 0, "Ejected.");
    evict_key(h, h1, "delete", 0, "Ejected.");
    evict_key(h, h1, "get", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 4,
          "Expected four items to be resident");

    check(store(h, h1, NULL, OPERATION_SET, "set", "value2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    checkeq(3, get_int_stat(h, h1, "ep_num_non_resident"),
          "Expected mutation to mark item resident");

    check(h1->arithmetic(h, NULL, "incr", 4, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0)  == ENGINE_SUCCESS, "Incr failed");
    h1->release(h, NULL, i);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 2,
          "Expected incr to mark item resident");

    check(del(h, h1, "delete", 0, 0) == ENGINE_SUCCESS, "Delete failed");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected delete to remove non-resident item");

    check_key_value(h, h1, "get", "getvalue", 8);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_duplicate_items_disk(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), "value", &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");
    int vb_del_num = get_int_stat(h, h1, "ep_vbucket_del");
    vbucketDelete(h, h1, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_vbucket_del", vb_del_num);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");
    // Make sure that a key/value item is persisted correctly
    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, h1, it->c_str(), 1, "Ejected.");
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 1);
    }
    check(get_int_stat(h, h1, "ep_warmup_dups") == 0,
          "Expected no duplicate items from disk");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_golden(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    cb_assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    cb_assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    int kv_size = get_int_stat(h, h1, "ep_kv_size");
    int mem_used = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    int kv_size2 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used2 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    cb_assert(kv_size2 < kv_size);
    cb_assert(mem_used2 < mem_used);

    // Reload the data.
    check_key_value(h, h1, "k1", "some value", 10);

    int kv_size3 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used3 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    cb_assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));
    // Should not have marked the thing dirty.
    cb_assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    cb_assert(kv_size == kv_size3);
    cb_assert(mem_used <= mem_used3);

    itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(del(h, h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_paged_rm(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    cb_assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    cb_assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    // Delete the value and make sure things return correctly.
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(del(h, h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    uint64_t result = 0;
    item *i = NULL;
    wait_for_persisted_value(h, h1, "k1", "13");

    evict_key(h, h1, "k1");

    check(h1->arithmetic(h, NULL, "k1", 2, true, false, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "k1", "14", 2);

    cb_assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_update_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "k1", "new value", &i) == ENGINE_SUCCESS,
          "Failed to update an item.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "k1", "new value", 9);

    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "bg fetched something");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_delete_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    check(del(h, h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed to delete.");

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    cb_assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

extern "C" {
    static void bg_set_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        item *i = NULL;
        check(store(td->h, td->h1, NULL, OPERATION_SET,
                    "k1", "new value", &i) == ENGINE_SUCCESS,
              "Failed to update an item.");
        td->h1->release(td->h, NULL, i);

        delete td;
    }

    static void bg_del_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        check(del(td->h, td->h1, "k1", 0, 0) == ENGINE_SUCCESS, "Failed to delete.");

        delete td;
    }

    static void bg_incr_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        uint64_t result = 0;
        item *i = NULL;
        check(td->h1->arithmetic(td->h, NULL, "k1", 2, true, false, 1, 1, 0,
                                 &i, PROTOCOL_BINARY_RAW_BYTES, &result,
                                 0) == ENGINE_SUCCESS,
              "Failed to incr value.");
        td->h1->release(td->h, NULL, i);

        delete td;
    }
}

static enum test_result test_disk_gt_ram_set_race(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_param(h, h1, protocol_binary_engine_param_flush, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    cb_thread_t tid;
    if (cb_create_thread(&tid, bg_set_thread, new ThreadData(h, h1), 0) != 0) {
        abort();
    }

    check_key_value(h, h1, "k1", "new value", 9);

    // Should have bg_fetched, but discarded the old value.
    cb_assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    cb_assert(cb_join_thread(tid) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr_race(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "13");
    cb_assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    set_param(h, h1, protocol_binary_engine_param_flush, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    cb_thread_t tid;
    if (cb_create_thread(&tid, bg_incr_thread, new ThreadData(h, h1), 0) != 0) {
        abort();
    }

    // Value is as it was before.
    check_key_value(h, h1, "k1", "13", 2);

    // Should have bg_fetched to retrieve it even with a concurrent
    // incr.  We *may* at this point have also completed the incr.
    // 1 == get only, 2 == get+incr.
    cb_assert(get_int_stat(h, h1, "ep_bg_fetched") >= 1);

    // Give incr time to finish (it's doing another background fetch)
    wait_for_stat_change(h, h1, "ep_bg_fetched", 1);
    wait_for_stat_change(h, h1, "ep_total_enqueued", 1);

    // The incr mutated the value.
    check_key_value(h, h1, "k1", "14", 2);

    cb_assert(cb_join_thread(tid) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_rm_race(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_param(h, h1, protocol_binary_engine_param_flush, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    cb_thread_t tid;
    if (cb_create_thread(&tid, bg_del_thread, new ThreadData(h, h1), 0) != 0) {
        abort();
    }

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    // Should have bg_fetched, but discarded the old value.
    cb_assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    cb_assert(cb_join_thread(tid) == 0);

    return SUCCESS;
}

static bool epsilon(int val, int target, int ep=5) {
    return abs(val - target) < ep;
}

static enum test_result test_max_size_and_water_marks_settings(
                                        ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(get_int_stat(h, h1, "ep_max_size") == 1000, "Incorrect initial size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 750),
          "Incorrect initial low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 850),
          "Incorrect initial high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.75),
          "Incorrect initial low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.85),
          "Incorrect initial high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "1000000");

    check(get_int_stat(h, h1, "ep_max_size") == 1000000,
          "Incorrect new size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 750000),
          "Incorrect larger low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 850000),
          "Incorrect larger high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.75),
          "Incorrect larger low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.85),
          "Incorrect larger high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "mem_low_wat", "700000");
    set_param(h, h1, protocol_binary_engine_param_flush, "mem_high_wat", "800000");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 700000,
          "Incorrect even larger low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 800000,
          "Incorrect even larger high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.7),
          "Incorrect even larger low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.8),
          "Incorrect even larger high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "100");

    check(get_int_stat(h, h1, "ep_max_size") == 100,
          "Incorrect smaller size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 70),
          "Incorrect smaller low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 80),
          "Incorrect smaller high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.7),
          "Incorrect smaller low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.8),
          "Incorrect smaller high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "mem_low_wat", "50");
    set_param(h, h1, protocol_binary_engine_param_flush, "mem_high_wat", "70");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 50,
          "Incorrect even smaller low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 70,
          "Incorrect even smaller high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.5),
          "Incorrect even smaller low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.7),
          "Incorrect even smaller high wat. percent");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    check(get_int_stat(h, h1, "ep_max_size") == 1000,
          "Incorrect initial size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 750),
          "Incorrect intial low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 850),
          "Incorrect initial high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.75),
          "Incorrect initial low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.85),
          "Incorrect initial high wat. percent");

    return SUCCESS;
}

static enum test_result test_validate_engine_handle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    (void)h;
    check(h1->get_stats_struct == NULL, "get_stats_struct member should be initialized to NULL");
    check(h1->aggregate_stats == NULL, "aggregate_stats member should be initialized to NULL");
    check(h1->unknown_command != NULL, "unknown_command member should be initialized to a non-NULL value");
    check(h1->tap_notify != NULL, "tap_notify member should be initialized to a non-NULL value");
    check(h1->get_tap_iterator != NULL, "get_tap_iterator member should be initialized to a non-NULL value");

    return SUCCESS;
}

static enum test_result test_kill9_bucket(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-0-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    // Last parameter indicates the force shutdown for the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    keys.clear();
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-1-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }

    return SUCCESS;
}

static enum test_result test_create_new_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Inserting more than 500 items will cause a new open checkpoint with id 2
    // to be created.

    for (int j = 0; j < 600; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), ss.str().c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    createCheckpoint(h, h1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success response from creating a new checkpoint");

    check(get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id", "checkpoint 0") == 3,
          "Last closed checkpoint Id for VB 0 should be 3");

    return SUCCESS;
}

extern "C" {
    static void checkpoint_persistence_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);

        // Issue a request with the unexpected large checkpoint id 100, which
        // will cause timeout.
        check(checkpointPersistence(hp->h, hp->h1, 100, 0) == ENGINE_TMPFAIL,
              "Expected temp failure for checkpoint persistence request");
        check(get_int_stat(hp->h, hp->h1, "ep_chk_persistence_timeout") > 10,
              "Expected CHECKPOINT_PERSISTENCE_TIMEOUT was adjusted to be greater"
              " than 10 secs");

        for (int j = 0; j < 1000; ++j) {
            std::stringstream ss;
            ss << "key" << j;
            item *i;
            check(store(hp->h, hp->h1, NULL, OPERATION_SET,
                  ss.str().c_str(), ss.str().c_str(), &i, 0, 0) == ENGINE_SUCCESS,
                  "Failed to store a value");
            hp->h1->release(hp->h, NULL, i);
        }

        createCheckpoint(hp->h, hp->h1);
    }

    static void seqno_persistence_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);

        for (int j = 0; j < 1000; ++j) {
            std::stringstream ss;
            ss << "key" << j;
            item *i;
            check(store(hp->h, hp->h1, NULL, OPERATION_SET,
                  ss.str().c_str(), ss.str().c_str(), &i, 0, 0) == ENGINE_SUCCESS,
                  "Failed to store a value");
            hp->h1->release(hp->h, NULL, i);
        }

        check(seqnoPersistence(hp->h, hp->h1, 0, 2003) == ENGINE_TMPFAIL,
              "Expected temp failure for seqno persistence request");
    }
}

static enum test_result test_checkpoint_persistence(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    const int  n_threads = 2;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_create_thread(&threads[i], checkpoint_persistence_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    // Last closed checkpoint id for vbucket 0.
    int closed_chk_id = get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id",
                                     "checkpoint 0");
    // Request to prioritize persisting vbucket 0.
    check(checkpointPersistence(h, h1, closed_chk_id, 0) == ENGINE_SUCCESS,
          "Failed to request checkpoint persistence");

    return SUCCESS;
}

static enum test_result test_dcp_persistence_seqno(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const int  n_threads = 2;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_create_thread(&threads[i], seqno_persistence_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    wait_for_flusher_to_settle(h, h1);

    check(seqnoPersistence(h, h1, 0, 2000) == ENGINE_SUCCESS,
          "Expected success for seqno persistence request");

    return SUCCESS;
}

static enum test_result test_dcp_last_items_purged(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    item_info info;
    mutation_descr_t mut_info;
    uint64_t vb_uuid = 0;
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    char key[][3] = {"k1", "k2", "k3"};

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    /* Set 3 items */
    for (int count = 0; count < num_items; count++){
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key[count], "somevalue", NULL,
                    0, 0, 0), "Error setting.");
    }

    memset(&mut_info, 0, sizeof(mut_info));

    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        check(h1->remove(h, NULL, key[count], strlen(key[count]), &cas, 0,
                         &mut_info) == ENGINE_SUCCESS,
              "Failed remove with value.");
        cas = 0;
    }

    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Run compaction */
    compact_db(h, h1, 0, 2, high_seqno, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") ==
            static_cast<int>(high_seqno - 1),
          "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    /* Create a DCP stream */
    const void *cookie = testHarness.create_cookie();
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, high_seqno, vb_uuid, 0, 0,
               1, 1, 1, 0, false, false, 0, true);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_rollback_after_purge(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    item_info info;
    mutation_descr_t mut_info;
    uint64_t vb_uuid = 0;
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    char key[][3] = {"k1", "k2", "k3"};

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    /* Set 3 items */
    for (int count = 0; count < num_items; count++){
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key[count], "somevalue", NULL,
                    0, 0, 0), "Error setting.");
    }
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Create a DCP stream to send 3 items to the replica */
    const void *cookie = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, high_seqno, vb_uuid, 0, 0,
               3, 0, 1, 0, false, false, 0, true);

    testHarness.destroy_cookie(cookie);

    memset(&mut_info, 0, sizeof(mut_info));
    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        check(h1->remove(h, NULL, key[count], strlen(key[count]), &cas, 0,
                         &mut_info) == ENGINE_SUCCESS,
              "Failed remove with value.");
        cas = 0;
    }
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Run compaction */
    compact_db(h, h1, 0, 2, high_seqno, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") ==
            static_cast<int>(high_seqno - 1),
          "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    /* DCP stream, expect a rollback to seq 0 */
    dcp_stream_req(h, h1, 1, 0, 3, high_seqno, vb_uuid,
                   3, high_seqno, 0, ENGINE_ROLLBACK);

    /* Do not expect rollback when you already have all items in the snapshot
       (that is, start == snap_end_seqno)*/
    dcp_stream_req(h, h1, 1, 0, high_seqno, high_seqno + 10, vb_uuid,
                   0, high_seqno, 0, ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_mutations(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t  stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 5, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    // Send a mutation and a deletion both out-of-sequence
    checkeq(h1->dcp.mutation(h, cookie, stream_opaque, "key", 3, "val", 3,
                             35, 0, 0, 0, 2, 0, 0, 0, "", 0,
                             INITIAL_NRU_VALUE),
            ENGINE_ERANGE,
            "Mutation should've returned ERANGE!");
    checkeq(h1->dcp.deletion(h, cookie, stream_opaque, "key5", 4, 40,
                             0, 3, 0, "", 0),
            ENGINE_ERANGE,
            "Deletion should've returned ERANGE!");

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");

    int buffered_items = get_int_stat(h, h1, bufferItemsStr.c_str(), "dcp");

    ENGINE_ERROR_CODE err = h1->dcp.mutation(h, cookie, stream_opaque,
                                             "key20", 5, "val", 3,
                                             45, 0, 0, 0, 20, 0, 0, 0, "",
                                             0, INITIAL_NRU_VALUE);

    if (buffered_items == 0) {
        checkeq(err, ENGINE_ERANGE, "Mutation shouldn't have been accepted!");
    } else {
        checkeq(err, ENGINE_SUCCESS, "Mutation should have been buffered!");
    }

    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    checkeq(get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0"),
            6, "The last mutation should've been dropped!");

    checkeq(h1->dcp.close_stream(h, cookie, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream!");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_marker(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("first_marker");

    checkeq(h1->dcp.open(h, cookie1, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie1, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie1, stream_opaque, 0, 1, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie1, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    checkeq(h1->dcp.close_stream(h, cookie1, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream1!");
    testHarness.destroy_cookie(cookie1);

    const void *cookie2 = testHarness.create_cookie();
    opaque = 0xFFFFF000;
    name.assign("second_marker");

    checkeq(h1->dcp.open(h, cookie2, opaque, 0 ,flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie2, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    opaqueStr.assign("eq_dcpq:" + name + ":stream_0_opaque");
    stream_opaque = get_int_stat(h , h1, opaqueStr.c_str(), "dcp");

    // Send a snapshot marker that would be rejected
    checkeq(h1->dcp.snapshot_marker(h, cookie2, stream_opaque, 0, 5, 10, 1),
            ENGINE_ERANGE,
            "Snapshot marker should have been dropped!");

    // Send a snapshot marker that would be accepted, but a few of
    // the mutations that are part of this snapshot will be dropped
    checkeq(h1->dcp.snapshot_marker(h, cookie2, stream_opaque, 0, 5, 15, 1),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 15; i++) {
        std::string key("key_" + std::to_string(i));
        ENGINE_ERROR_CODE err = h1->dcp.mutation(h, cookie2, stream_opaque,
                                                 key.c_str(), key.length(),
                                                 "val", 3, i * 3, 0, 0, 0, i,
                                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE);
        if (i <= 10) {
            checkeq(err, ENGINE_ERANGE, "Mutation should have been dropped!");
        } else {
            checkeq(err, ENGINE_SUCCESS, "Failed to send mutation!");
        }
    }

    checkeq(h1->dcp.close_stream(h, cookie2, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream2!");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_mutation_deletion(ENGINE_HANDLE* h,
                                                              ENGINE_HANDLE_V1* h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t  stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    // Mutation(s) or deletion(s) with seqno 0 are invalid!
    std::string key("key");
    std::string val("value");
    checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                             key.length(), val.c_str(), val.length(), 10, 0, 0,
                             0, /*seqno*/ 0, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
            ENGINE_EINVAL,
            "Mutation should have returned EINVAL!");

    checkeq(h1->dcp.deletion(h, cookie, stream_opaque, key.c_str(),
                             key.length(), 10, 0, /*seqno*/ 0, 0, "", 0),
            ENGINE_EINVAL,
            "Deletion should have returned EINVAL!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_snapshot_marker(ENGINE_HANDLE* h,
                                                         ENGINE_HANDLE_V1* h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 1, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    // Invalid snapshot marker with end <= start
    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 11, 8, 300),
            ENGINE_EINVAL,
            "Failed to send snapshot marker!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

extern "C" {
    static void wait_for_persistence_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);

        check(checkpointPersistence(hp->h, hp->h1, 100, 1) == ENGINE_TMPFAIL,
              "Expected temp failure for checkpoint persistence request");
    }
}

static enum test_result test_wait_for_persist_vb_del(ENGINE_HANDLE* h,
                                                     ENGINE_HANDLE_V1* h1) {

    cb_thread_t th;
    struct handle_pair hp = {h, h1};

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    int ret = cb_create_thread(&th, wait_for_persistence_thread, &hp, 0);
    cb_assert(ret == 0);

    wait_for_stat_to_be(h, h1, "ep_chk_persistence_remains", 1);

    vbucketDelete(h, h1, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    ret = cb_join_thread(th);
    cb_assert(ret == 0);

    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_max_items", "1000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_max_item param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_period", "100");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_period param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "max_checkpoints", "2");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set max_checkpoints param");

    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_max_items", "5");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_max_items param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_period", "0");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_period param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "max_checkpoints", "6");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for max_checkpoints param");

    return SUCCESS;
}

static enum test_result test_revid(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    ItemMetaData meta;
    for (uint64_t ii = 1; ii < 10; ++ii) {
        item *it;
        check(store(h, h1, NULL, OPERATION_SET, "test_revid", "foo", &it, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, it);

        check(get_meta(h, h1, "test_revid"), "Get meta failed");

        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
        checkeq(ii, last_meta.revSeqno, "Unexpected sequence number");
    }

    return SUCCESS;
}

static enum test_result test_regression_mb4314(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    ItemMetaData itm_meta;
    check(!get_meta(h, h1, "test_regression_mb4314"), "Expected to get meta");

    itm_meta.flags = 0xdeadbeef;
    itm_meta.exptime = 0;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    set_with_meta(h, h1, "test_regression_mb4314", 22, NULL, 0, 0, &itm_meta, last_cas);

    // Now try to read the item back:
    item *it = NULL;
    ENGINE_ERROR_CODE ret = h1->get(h, NULL, &it, "test_regression_mb4314", 22, 0);
    check(ret == ENGINE_SUCCESS, "Expected to get the item back!");
    h1->release(h, NULL, it);

    return SUCCESS;
}

static enum test_result test_mb3466(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    check(vals.find("mem_used") != vals.end(),
          "Expected \"mem_used\" to be returned");
    check(vals.find("bytes") != vals.end(),
          "Expected \"bytes\" to be returned");
    std::string memUsed = vals["mem_used"];
    std::string bytes = vals["bytes"];
    check(memUsed == bytes,
          "Expected mem_used and bytes to have the same value");

    return SUCCESS;
}

// ------------------- beginning of XDCR unit tests -----------------------//
static enum test_result test_get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "test_get_meta";
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == it->getRevSeqno(), "Expected seqno to match");
    check(last_meta.cas == it->getCas(), "Expected cas to match");
    check(last_meta.exptime == it->getExptime(), "Expected exptime to match");
    check(last_meta.flags == it->getFlags(), "Expected flags to match");
    check(last_conflict_resolution_mode == static_cast<uint8_t>(-1),
            "Expected to not receive the conflict resolution mode");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_extras(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1)
{
    const char *key1 = "test_getm_one";
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) ==
            ENGINE_SUCCESS,
          "Failed set.");
    Item *it1 = reinterpret_cast<Item*>(i);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key1, true), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == it1->getRevSeqno(), "Expected seqno to match");
    check(last_meta.cas == it1->getCas(), "Expected cas to match");
    check(last_meta.exptime == it1->getExptime(), "Expected exptime to match");
    check(last_meta.flags == it1->getFlags(), "Expected flags to match");
    check(last_conflict_resolution_mode == 0,
            "Expected to receive the conflict resolution mode revid_based");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");
    h1->release(h, NULL, i);

    // Enable time synchronization
    set_drift_counter_state(h, h1, 1000, 0x01);

    const char *key2 = "test_getm_two";
    check(store(h, h1, NULL, OPERATION_SET, key2, "somevalue", &i) ==
            ENGINE_SUCCESS,
          "Failed set.");
    Item *it2 = reinterpret_cast<Item*>(i);
    check(get_meta(h, h1, key2, true), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == it2->getRevSeqno(), "Expected seqno to match");
    check(last_meta.cas == it2->getCas(), "Expected cas to match");
    check(last_meta.exptime == it2->getExptime(), "Expected exptime to match");
    check(last_meta.flags == it2->getFlags(), "Expected flags to match");
    check(last_conflict_resolution_mode == 1,
            "Expected to receive the conflict resolution mode lww_based");
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect one getMeta op");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static enum test_result test_get_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";
    item *i = NULL;

    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    Item *it = reinterpret_cast<Item*>(i);
    wait_for_flusher_to_settle(h, h1);

    check(del(h, h1, key, it->getCas(), 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(last_meta.revSeqno == it->getRevSeqno() + 1, "Expected seqno to match");
    check(last_meta.cas == it->getCas() + 1, "Expected cas to match");
    check(last_meta.flags == it->getFlags(), "Expected flags to match");

    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_get_meta_with_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    // test get_meta followed by get for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_SUCCESS, "Expected get success");
    h1->release(h, NULL, i);
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by get for a deleted key. should fail.
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect more getMeta ops");

    // test get_meta followed by get for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(h1->get(h, NULL, &i, key2, strlen(key2), 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 3, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_get_meta_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    ItemMetaData itm_meta;

    // test get_meta followed by set for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 1);

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_get_meta") == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_get_meta") == 1, "Expect one getMeta op");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");
    h1->release(h, NULL, i);

    // check curr, temp item counts
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // test get_meta followed by set for a deleted key. should pass.
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_get_meta") == 2, "Expect more getMeta ops");
    h1->release(h, NULL, i);

    // test get_meta followed by set for a nonexistent key. should pass.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(store(h, h1, NULL, OPERATION_SET, key2, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    // check the stat again
    check(get_int_stat(h, h1, "ep_num_ops_get_meta") == 3,
          "Failed operation should also count");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;

    // test get_meta followed by delete for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by delete for a deleted key. should fail.
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(del(h, h1, key1, 0, 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect more getMeta op");

    // test get_meta followed by delete for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(del(h, h1, key2, 0, 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 3, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const char *key = "mykey";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    size_t temp = 0;

    // put some random metadata
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // store an item with meta data
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // store the item again, expect key exists
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected add to fail when the item exists already");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key1 = "delete_with_meta_key1";
    const char *key2 = "delete_with_meta_key2";
    const char *key3 = "delete_with_meta_key3";
    const size_t keylen = strlen(key1);
    ItemMetaData itemMeta;
    uint64_t vb_uuid;
    uint32_t high_seqno;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // put some random meta data
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key1,
                "somevalue", &i) == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(store(h, h1, NULL, OPERATION_SET, key2,
                "somevalue2", &i) == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    check(store(h, h1, NULL, OPERATION_SET, key3,
                "somevalue3", &i) == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    const void *cookie = testHarness.create_cookie();

    // delete an item with meta data
    del_with_meta(h, h1, key1, keylen, 0, &itemMeta, 0, false, false,
                  0, 0, cookie);

    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected valid sequence number");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect more setMeta ops");

    testHarness.set_mutation_extras_handling(cookie, false);

    // delete an item with meta data
    del_with_meta(h, h1, key2, keylen, 0, &itemMeta, 0, false, false,
                  0, 0, cookie);

    check(last_uuid == vb_uuid, "Expected same vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected same sequence number");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // delete an item with meta data
    del_with_meta(h, h1, key3, keylen, 0, &itemMeta);

    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 3, "Expected valid sequence number");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_deleted(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    item *i = NULL;

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 0, "Expect zero setMeta ops");

    // add a key
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    // delete the key
    check(del(h, h1, key, 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // get metadata of deleted key
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;
    // put some random metadata and delete the item with new meta data
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 0, "Faild ops does not count");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    wait_for_flusher_to_settle(h, h1);

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 1, "Expect some ops");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.revSeqno == last_meta.revSeqno, "Expected seqno to match");
    check(itm_meta.cas == last_meta.cas, "Expected cas to match");
    check(itm_meta.flags == last_meta.flags, "Expected flags to match");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itm_meta;

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 0, "Expect zero setMeta ops");

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);

    // get metadata of nonexistent key
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;

    // do delete with meta
    // put some random metadata and delete the item with new meta data
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 0, "Failed op does not count");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 1, "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.revSeqno == last_meta.revSeqno, "Expected seqno to match");
    check(itm_meta.cas == last_meta.cas, "Expected cas to match");
    check(itm_meta.flags == last_meta.flags, "Expected flags to match");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent_no_temp(ENGINE_HANDLE *h,
                                                                  ENGINE_HANDLE_V1 *h1) {
    const char *key1 = "delete_with_meta_no_temp_key1";
    const size_t keylen1 = strlen(key1);
    ItemMetaData itm_meta1;

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, h1, 0, 1, 1, 0);
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    // put some random metadata and delete the item with new meta data
    itm_meta1.revSeqno = 10;
    itm_meta1.cas = 0xdeadbeef;
    itm_meta1.exptime = 1735689600; // expires in 2025
    itm_meta1.flags = 0xdeadbeef;

    // do delete with meta with the correct cas value.
    // skipConflictResolution false
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta1, 0, false);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 1, "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // do delete with meta with the correct cas value.
    // skipConflictResolution true
    const char *key2 = "delete_with_meta_no_temp_key2";
    const size_t keylen2 = strlen(key2);
    ItemMetaData itm_meta2;

    // put some random metadata and delete the item with new meta data
    itm_meta2.revSeqno = 10;
    itm_meta2.cas = 0xdeadbeef;
    itm_meta2.exptime = 1735689600; // expires in 2025
    itm_meta2.flags = 0xdeadbeef;

    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta2, 0, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "ep_num_ops_del_meta") == 2, "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    const size_t keylen1 = strlen(key1);

    item *i = NULL;
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    uint16_t keylen1 = (uint16_t)strlen(key1);
    char const *key2 = "key2";
    uint16_t keylen2 = (uint16_t)strlen(key2);
    item *i = NULL;
    ItemMetaData itm_meta;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent delete
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete itself will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete
    check(del(h, h1, key1, 0, 0) == ENGINE_KEY_ENOENT, "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect some ops");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete itself will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent delete
    check(del(h, h1, key1, 0, 0) == ENGINE_KEY_ENOENT, "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    size_t newValLen = strlen(newVal);
    uint64_t vb_uuid;
    uint32_t high_seqno;

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Expect zero ops");
    check(get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta") == 0,
          "Expect zero ops");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expect zero items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expect zero temp items");

    // create a new key
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i) == ENGINE_SUCCESS,
          "Failed set.");

    // get metadata for the key
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expect one item");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expect zero temp item");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = time(NULL) + 300;
    itm_meta.flags = 0xdeadbeef;

    char *bigValue = new char[32*1024*1024];
    // do set with meta with the value size bigger than the max size allowed.
    set_with_meta(h, h1, key, keylen, bigValue, 32*1024*1024, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_E2BIG,
          "Expected the max value size exceeding error");
    delete []bigValue;

    // do set with meta with an incorrect cas value. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Failed op does not count");

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    const void *cookie = testHarness.create_cookie();

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set,
                  false, 0, false, 0, 0, cookie);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected valid sequence number");

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 1, "Expect some ops");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expect one item");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expect zero temp item");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");

    //disable getting vb uuid and seqno as extras
    testHarness.set_mutation_extras_handling(cookie, false);
    itm_meta.revSeqno++;
    cas_for_set = last_meta.cas;
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set,
                  false, 0, false, 0, 0, cookie);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_uuid == vb_uuid, "Expected same vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected same sequence number");

    itm_meta.revSeqno++;
    cas_for_set = last_meta.cas;
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 3, "Expected valid sequence number");

    // Make sure the item expiration was processed correctly
    testHarness.time_travel(301);
    check(h1->get(h, NULL, &i, key, keylen, 0) == ENGINE_KEY_ENOENT, "Failed to get value.");

    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_set_with_meta_by_force(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";

    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = time(NULL) + 300;
    itm_meta.flags = 0xdeadbeef;

    // Pass true to force SetWithMeta.
    set_with_meta(h, h1, key, keylen, val, strlen(val), 0, &itm_meta,
                  0, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);

    // get metadata again to verify that the warmup loads an item correctly.
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");

    check_key_value(h, h1, key, val, strlen(val));

    return SUCCESS;
}

static enum test_result test_set_with_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    uint16_t newValLen = (uint16_t)strlen(newVal);

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Expect zero ops");
    check(get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta") == 0,
                                                           "Expect zero ops");

    // create a new key
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // delete the key
    check(del(h, h1, key, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // get metadata for the key
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set_with_meta with an incorrect cas for a deleted item. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "Expected key_not_found error");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Failed op does not count");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 1, "Expect some ops");
    check(get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta") == 0,
          "Expect some ops");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.exptime == 1735689600, "Expected exptime to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    size_t valLen = strlen(val);

    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Expect zero ops");

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);
    // get metadata for the key
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected no temp_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set_with_meta with an incorrect cas for a non-existent item. should fail.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "Expected key_not_found error");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0, "Failed op does not count");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 1, "Expect some ops");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.exptime == 1735689600, "Expected exptime to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");
    check(get_int_stat(h, h1, "curr_items") == 1, "Expected single curr_items");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    item *i = NULL;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    // attempt set_with_meta. should fail since cas is no longer valid.
    last_meta.revSeqno += 2;
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    // attempt set_with_meta. should fail since cas is no longer valid.
    last_meta.revSeqno += 2;
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    size_t keylen2 = strlen(key2);
    item *i = NULL;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent delete that changes the cas
    check(del(h, h1, key1, 0, 0) == ENGINE_SUCCESS, "Delete failed");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete. should fail.
    check(del(h, h1, key1, 0, 0) == ENGINE_KEY_ENOENT, "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some op");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent delete. should fail.
    check(del(h, h1, key2, 0, 0) == ENGINE_KEY_ENOENT, "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key2, keylen2, NULL, 0, 0, &last_meta, last_cas, true);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_temp_item_deletion(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    // Do get_meta for an existing key
    char const *k1 = "k1";
    item *i = NULL;

    check(store(h, h1, NULL, OPERATION_SET, k1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    check(del(h, h1, k1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    check(get_meta(h, h1, k1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // Do get_meta for a non-existing key
    char const *k2 = "k2";
    check(!get_meta(h, h1, k2), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // Trigger the expiry pager and verify that two temp items are deleted
    testHarness.time_travel(30);
    wait_for_stat_to_be(h, h1, "ep_expired_pager", 2);
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Expected zero temp_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_add_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_bg_meta_fetched") == 1,
          "Expected one bg meta fetch");

    check(del(h, h1, "key", 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    itemMeta.revSeqno++;
    itemMeta.cas++;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_bg_meta_fetched") == 2,
          "Expected two be meta fetches");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 1,
          "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 2,
          "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 3,
          "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 3,
          "Expected set meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_set_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0,
          "Expect zero setMeta ops");

    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_bg_meta_fetched") == 1,
          "Expected one bg meta fetch");

    // Check all meta data is the same
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 1,
          "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 2,
          "Expected set meta conflict resolution failure");

    // Check has newer flags passes
    itemMeta.flags = 0xdeadbeff;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that newer exptime wins
    itemMeta.exptime = time(NULL) + 10;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 3,
          "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 4,
          "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 4,
          "Expected set meta conflict resolution failure");

    check(get_int_stat(h, h1, "ep_bg_meta_fetched") == 1,
          "Expect one bg meta fetch");

    return SUCCESS;
}

static enum test_result test_set_meta_lww_conflict_resolution(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    //Set initial drift and enable time synchronization
    set_drift_counter_state(h, h1, 0, 0x01);

    check(get_int_stat(h, h1, "ep_num_ops_set_meta") == 0,
          "Expect zero setMeta ops");

    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
                  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_bg_meta_fetched") == 1,
          "Expected one bg meta fetch");

    // Check all meta data is the same
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
                  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 1,
          "Expected set meta conflict resolution failure");

    // Check that an older cas fails
    itemMeta.cas = 0xdeadbeee;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
                  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 2,
          "Expected set meta conflict resolution failure");

    // Check that a higher cas passes
    itemMeta.cas = 0xdeadbeff;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
                  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that a higher cas, lower rev seqno and conflict resolution
    // with revision seqno will fail
    itemMeta.cas = 0xdeadbfff;
    itemMeta.revSeqno = 9;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
		  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail") == 3,
          "Expected set meta conflict resolution failure");

    // Check that a lower cas, higher rev seqno and conflict resolution
    // with revision seqno will pass
    itemMeta.revSeqno = 11;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0, false,
		  PROTOCOL_BINARY_RAW_BYTES, true, gethrtime(), 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    return SUCCESS;
}

static enum test_result test_del_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    h1->release(h, NULL, i);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 1,
          "Expected delete meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 2,
          "Expected delete meta conflict resolution failure");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 3,
          "Expected delete meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 4,
          "Expected delete meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 4,
          "Expected delete meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_del_meta_lww_conflict_resolution(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    item_info info;

    set_drift_counter_state(h, h1, 0, 0x01);

    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    wait_for_flusher_to_settle(h, h1);
    h1->release(h, NULL, i);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = info.cas + 1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 1,
          "Expected delete meta conflict resolution failure");

    // Check that higher rev seqno but lower cas fails
    itemMeta.cas = info.cas;
    itemMeta.revSeqno = 11;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 2,
          "Expected delete meta conflict resolution failure");

    // Check that a higher cas and lower rev seqno passes
    itemMeta.cas = info.cas + 2;
    itemMeta.revSeqno = 9;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected sucess");

    // Check that a higher rev seqno and lower cas and conflict resolution of
    // revision seqno passes
    itemMeta.revSeqno = 10;
    itemMeta.cas = info.cas + 1;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that a lower rev seqno and higher cas and conflict resolution of
    // revision seqno fails
    itemMeta.revSeqno = 9;
    itemMeta.cas = info.cas + 2;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, false, true, gethrtime(), 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 3,
          "Expected delete meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_adjusted_time_apis(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {

    int64_t adjusted_time1, adjusted_time2;
    protocol_binary_request_header *request;

    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
            "Expected Not Supported, as Time sync hasn't been enabled yet");

    set_drift_counter_state(h, h1, 1000, 0x01);

    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time1, last_body.data(), last_body.size());
    adjusted_time1 = ntohll(adjusted_time1);

    set_drift_counter_state(h, h1, 1000000, 0x01);

    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    // adjusted_time2 should be greater than adjusted_time1 marginally
    // by adjusted_time1 + (difference in the 2 driftCounts set previously)
    check(adjusted_time2 >= adjusted_time1 + 999000,
            "Adjusted_time2: now what expected");

    // Test sending adjustedTime with SetWithMeta
    ItemMetaData itm_meta;
    itm_meta.flags = 0xdeadbeef;
    itm_meta.exptime = 0;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    set_with_meta(h, h1, "key", 3, "value", 5, 0, &itm_meta, last_cas,
                  false, 0x00, true, adjusted_time2 * 2);
    wait_for_flusher_to_settle(h, h1);

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected a SUCCESS");
    check_key_value(h, h1, "key", "value", 5, 0);

    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
            NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time1, last_body.data(), last_body.size());
    adjusted_time1 = ntohll(adjusted_time1);

    // Check that adjusted_time1 should be marginally greater than
    // adjusted_time2 * 2
    check(adjusted_time1 >= adjusted_time2 * 2,
            "Adjusted_time1: not what is expected");

    // Test sending adjustedTime with DelWithMeta
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key2", "value2", &i) == ENGINE_SUCCESS,
            "Failed set.");
    h1->release(h, NULL, i);
    del_with_meta(h, h1, "key2", 4, 0, &itm_meta, last_cas, false,
                  true, adjusted_time1 * 2);
    wait_for_flusher_to_settle(h, h1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
            NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    // Check that adjusted_time2 should be marginally greater than
    // adjusted_time1 * 2
    check(adjusted_time2 >= adjusted_time1 * 2,
            "Adjusted_time2: not what is expected");

    return SUCCESS;
}

// ------------------------------ end of XDCR unit tests -----------------------//

static enum test_result test_observe_no_data(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::map<std::string, uint16_t> obskeys;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    return SUCCESS;
}

static enum test_result test_observe_seqno_basic_tests(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    // Check observe seqno for vbucket with id 1
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    //Check the output when there is no data in the vbucket
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");
    uint64_t high_seqno = get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno");
    observe_seqno(h, h1, 1, vb_uuid);

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    check_observe_seqno(false, 0, 1, vb_uuid, high_seqno, high_seqno);

    //Add some mutations and verify the output
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        // Set an item
        item *it = NULL;
        uint64_t cas1;
        std::string value('x', 100);
        check(storeCasOut(h, h1, NULL, 1, "key" + std::to_string(j), value,
                          PROTOCOL_BINARY_RAW_BYTES, it, cas1) == ENGINE_SUCCESS,
              "Expected set to succeed");
    }

    wait_for_flusher_to_settle(h, h1);

    int total_persisted = get_int_stat(h, h1, "ep_total_persisted");
    high_seqno = get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno");

    check(total_persisted == num_items,
          "Expected ep_total_persisted equals the number of items");

    observe_seqno(h, h1, 1, vb_uuid);

    check_observe_seqno(false, 0, 1, vb_uuid, total_persisted, high_seqno);
    //Stop persistence. Add more mutations and check observe result
    stop_persistence(h, h1);

    num_items = 20;
    for (int j = 10; j < num_items; ++j) {
        // Set an item
        item *it = NULL;
        uint64_t cas1;
        std::string value('x', 100);
        check(storeCasOut(h, h1, NULL, 1, "key" + std::to_string(j), value,
                          PROTOCOL_BINARY_RAW_BYTES, it, cas1) == ENGINE_SUCCESS,
              "Expected set to succeed");
    }

    high_seqno = get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno");
    observe_seqno(h, h1, 1, vb_uuid);

    check_observe_seqno(false, 0, 1, vb_uuid, total_persisted, high_seqno);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    total_persisted = get_int_stat(h, h1, "ep_total_persisted");

    observe_seqno(h, h1, 1, vb_uuid);

    check_observe_seqno(false, 0, 1, vb_uuid, total_persisted, high_seqno);
    return SUCCESS;
}

static enum test_result test_observe_seqno_failover(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        // Set an item
        item *it = NULL;
        uint64_t cas1;
        std::string value('x', 100);
        check(storeCasOut(h, h1, NULL, 0, "key" + std::to_string(j), value,
                          PROTOCOL_BINARY_RAW_BYTES, it, cas1) == ENGINE_SUCCESS,
              "Expected set to succeed");
    }

    wait_for_flusher_to_settle(h, h1);

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint64_t high_seqno = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    // restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    uint64_t new_vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    observe_seqno(h, h1, 0, vb_uuid);

    check_observe_seqno(true, 1, 0, new_vb_uuid, high_seqno, high_seqno,
                        vb_uuid, high_seqno);

    return SUCCESS;
}

static enum test_result test_observe_seqno_error(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {

    //not my vbucket test
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    observe_seqno(h, h1, 10, vb_uuid);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected not my vbucket");

    //invalid uuid for vbucket
    vb_uuid = 0xdeadbeef;
    std::stringstream invalid_data;
    invalid_data.write((char *) &vb_uuid, sizeof(uint64_t));

    protocol_binary_request_header *request;

    request = createPacket(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO, 0, 0, NULL, 0,
                           NULL, 0, invalid_data.str().data(),
                           invalid_data.str().length());
    h1->unknown_command(h, NULL, request, add_response);

    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "Expected vb uuid not found");

    return SUCCESS;
}

static enum test_result test_observe_single_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    stop_persistence(h, h1);

    // Set an item
    std::string value('x', 100);
    item *it = NULL;
    uint64_t cas1;
    check(storeCasOut(h, h1, NULL, /*vb*/0, "key", value,
                      PROTOCOL_BINARY_RAW_BYTES, it, cas1) == ENGINE_SUCCESS,
          "Set should work");

    // Do an observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key"] = 0;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that the key is not persisted
    uint16_t vb;
    uint16_t keylen;
    char key[3];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 3, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key", 3) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 7, sizeof(uint8_t));
    check(persisted == OBS_STATE_NOT_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 8, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_temp_item(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char const *k1 = "key";
    item *i = NULL;

    check(store(h, h1, NULL, OPERATION_SET, k1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    check(del(h, h1, k1, 0, 0) == ENGINE_SUCCESS, "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    check(get_meta(h, h1, k1), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(get_int_stat(h, h1, "curr_items") == 0, "Expected zero curr_items");

    // Make sure there is one temp_item
    check(get_int_stat(h, h1, "curr_temp_items") == 1, "Expected single temp_items");

    // Do an observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key"] = 0;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check that the key is not found
    uint16_t vb;
    uint16_t keylen;
    char key[3];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 3, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key", 3) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 7, sizeof(uint8_t));
    check(persisted == OBS_STATE_NOT_FOUND, "Expected NOT_FOUND in result");
    memcpy(&cas, last_body.data() + 8, sizeof(uint64_t));
    check(ntohll(cas) == 0, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_multi_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Create some vbuckets
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Set some keys to observe
    item *it = NULL;
    uint64_t cas1, cas2, cas3;
    std::string value('x', 100);
    check(storeCasOut(h, h1, NULL, 0, "key1", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas1) == ENGINE_SUCCESS,
          "Set should work");

    check(storeCasOut(h, h1, NULL, 1, "key2", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas2) == ENGINE_SUCCESS,
          "Set should work");

    check(storeCasOut(h, h1, NULL, 1, "key3", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas3) == ENGINE_SUCCESS,
          "Set should work");

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 3);

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    obskeys["key2"] = 1;
    obskeys["key3"] = 1;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 17, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 26, sizeof(uint64_t));
    check(ntohll(cas) == cas2, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 34, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    check(strncmp(key, "key3", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    check(ntohll(cas) == cas3, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_multiple_observes(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Holds the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    // Set some keys
    item *it = NULL;
    uint64_t cas1, cas2;
    std::string value('x', 100);
    check(storeCasOut(h, h1, NULL, 0, "key1", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas1) == ENGINE_SUCCESS,
          "Set should work");

    check(storeCasOut(h, h1, NULL, 0, "key2", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas2) == ENGINE_SUCCESS,
          "Set should work");

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 2);

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");
    check(last_body.size() == 17, "Incorrect body length");

    // Do another observe
    obskeys.clear();
    obskeys["key2"] = 0;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas2, "Wrong cas in result");
    check(last_body.size() == 17, "Incorrect body length");

    return SUCCESS;
}

static enum test_result test_observe_with_not_found(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Create some vbuckets
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Set some keys
    item *it = NULL;
    uint64_t cas1, cas3;
    std::string value('x', 100);
    check(storeCasOut(h, h1, NULL, 0, "key1", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas1) == ENGINE_SUCCESS,
          "Set should work");

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);
    stop_persistence(h, h1);

    check(storeCasOut(h, h1, NULL, 1, "key3", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas3) == ENGINE_SUCCESS,
          "Set should work");

    check(del(h, h1, "key3", 0, 1) == ENGINE_SUCCESS, "Failed to remove a key");

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    obskeys["key2"] = 0;
    obskeys["key3"] = 1;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    check(persisted == OBS_STATE_NOT_FOUND, "Expected key_not_found key status");

    memcpy(&vb, last_body.data() + 34, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    check(strncmp(key, "key3", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    check(persisted == OBS_STATE_LOGICAL_DEL, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    check(ntohll(cas) == cas3 + 1, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_errors(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::map<std::string, uint16_t> obskeys;

    // Check not my vbucket error
    obskeys["key"] = 1;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Expected not my vbucket");

    // Check invalid packets
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0, "0", 1);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Expected invalid");
    free(pkt);

    pkt = createPacket(PROTOCOL_BINARY_CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0, "0000", 4);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Expected invalid");
    free(pkt);

    return SUCCESS;
}

static enum test_result test_CBD_152(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // turn off flushall_enabled parameter
    set_param(h, h1, protocol_binary_engine_param_flush, "flushall_enabled", "false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set flushall_enabled param");

    // store a key and check its existence
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP, "Flush should be disabled");
    //check the key
    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");

    // turn on flushall_enabled parameter
    set_param(h, h1, protocol_binary_engine_param_flush, "flushall_enabled", "true");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set flushall_enabled param");
    // flush should succeed
    set_degraded_mode(h, h1, NULL, true);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Flush should be enabled");
    set_degraded_mode(h, h1, NULL, false);
    //expect missing key
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_control_data_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "value1", &itm) == ENGINE_SUCCESS,
          "Failed to set key");
    h1->release(h, NULL, itm);

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Faile to disable data traffic");
    free(pkt);

    check(store(h, h1, NULL, OPERATION_SET, "key", "value2", &itm) == ENGINE_TMPFAIL,
          "Expected to receive temporary failure");
    h1->release(h, NULL, itm);

    pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Faile to enable data traffic");
    free(pkt);

    check(store(h, h1, NULL, OPERATION_SET, "key", "value2", &itm) == ENGINE_SUCCESS,
          "Failed to set key");
    h1->release(h, NULL, itm);
    return SUCCESS;
}

static enum test_result test_item_pager(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    // 1. Create enough 1KB items to hit the high watermark (i.e. get TEMP_OOM).
    char data[1024];
    memset(&data, 'x', sizeof(data)-1);
    data[1023] = '\0';

    // Create documents, until we hit TempOOM. Due to accurate memory tracking
    // & overheads it's impossible to exactly predict how many we will need...
    int docs_stored = 0;
    for (int j = 0; ; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        std::string key(ss.str());

        item *i = NULL;
        ENGINE_ERROR_CODE err = store(h, h1, NULL, OPERATION_SET, key.c_str(),
                                      data, &i);
        h1->release(h, NULL, i);

        check(err == ENGINE_SUCCESS || err == ENGINE_TMPFAIL,
              "Failed to store a value");
        if (err == ENGINE_TMPFAIL) {
            break;
        }
        docs_stored++;
    }
    wait_for_flusher_to_settle(h, h1);

    // We should have stored at least a reasonable number of docs so we can
    // then have NRU act on 50% of them.
    check(docs_stored > 10,
          "Failed to store enough documents before hitting TempOOM\n");

    // Reference the first 50% of the stored documents making them have a
    // lower NRU and not candidates for ejection.
    for (int j = 0; j < docs_stored / 2; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        std::string key(ss.str());
        // Reference each stored item multiple times.
        for (int k = 0; k < 5; ++k) {
            item *i;
            check(h1->get(h, NULL, &i, key.c_str(), key.length(), 0) == ENGINE_SUCCESS,
                  "Failed to get value.");
            h1->release(h, NULL, i);
        }
    }

    // If the item pager hasn't run already, set mem_high_wat
    // to a value less than mem_used which would force the
    // item pager to run at least once.
    if (get_int_stat(h, h1, "ep_num_non_resident") == 0) {
        int mem_used = get_int_stat(h, h1, "mem_used");
        int new_low_wat = mem_used * 0.75;
        set_param(h, h1, protocol_binary_engine_param_flush,
                  "mem_low_wat", std::to_string(new_low_wat).c_str());
        int new_high_wat = mem_used * 0.85;
        set_param(h, h1, protocol_binary_engine_param_flush,
                  "mem_high_wat", std::to_string(new_high_wat).c_str());
    }

    testHarness.time_travel(5);

    wait_for_memory_usage_below(h, h1, get_int_stat(h, h1, "ep_mem_high_wat"));

#ifdef _MSC_VER
    // It seems like the scheduling of the tasks is different on windows
    // (at least on my virtual machines). Once we have all of the tests
    // passing for WIN32 we're going to start benchmarking it so we'll
    // figure out a better fix for this at a later time..
    // For now just spend some time waiting for it to bump the values
    int max = 0;
    while (get_int_stat(h, h1, "ep_num_non_resident") == 0) {
        sleep(1);
        if (++max == 30) {
            std::cerr << "Giving up waiting for item_pager to eject data.. "
                      << std::endl;
            return FAIL;
        }
    }
#endif

    int num_non_resident = get_int_stat(h, h1, "ep_num_non_resident");

    if (num_non_resident == 0) {
        wait_for_stat_change(h, h1, "ep_num_non_resident", 0);
    }

    // Check we can successfully fetch all of the documents (even ones not
    // resident).
    for (int j = 0; j < docs_stored; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        std::string key(ss.str());

        item *i;
        check(h1->get(h, NULL, &i, key.c_str(), key.length(), 0) == ENGINE_SUCCESS,
             "Failed to get value.");
        h1->release(h, NULL, i);
    }

    //Tmp ooms now trigger the item_pager task to eject some items,
    //thus there would be a few background fetches at least.
    check(get_int_stat(h, h1, "ep_bg_fetched") > 0,
          "Expected a few disk reads for referenced items");

    return SUCCESS;
}

static enum test_result test_set_vbucket_out_of_range(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    check(!set_vbucket_state(h, h1, 10000, vbucket_state_active),
          "Shouldn't have been able to set vbucket 10000");
    return SUCCESS;
}

static enum test_result test_exp_persisted_set_del(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    check(!get_meta(h, h1, "key3"), "Expected to get meta");

    ItemMetaData itm_meta;
    itm_meta.revSeqno = 1;
    itm_meta.cas = 1;
    itm_meta.exptime = 0;
    itm_meta.flags = 0;
    set_with_meta(h, h1, "key3", 4, "val0", 4, 0, &itm_meta, last_meta.cas);

    itm_meta.revSeqno = 2;
    itm_meta.cas = 2;
    set_with_meta(h, h1, "key3", 4, "val1", 4, 0, &itm_meta, last_meta.cas);
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);

    itm_meta.revSeqno = 3;
    itm_meta.cas = 3;
    itm_meta.exptime = 1735689600; // expires in 2025
    set_with_meta(h, h1, "key3", 4, "val1", 4, 0, &itm_meta, last_meta.cas);

    testHarness.time_travel(500000000);
    // Wait for the item to be expired, either by the pager,
    // or by access (as part of persistence callback from a
    // previous set - slow disk), or the compactor (unlikely).
    wait_for_expired_items_to_be(h, h1, 1);

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    check(get_meta(h, h1, "key3"), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == 4, "Expected seqno to match");
    check(last_meta.cas == 4, "Expected cas to match");
    check(last_meta.flags == 0, "Expected flags to match");

    return SUCCESS;
}

static enum test_result test_stats_vkey_valid_field(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();

    // Check vkey when a key doesn't exist
    const char* stats_key = "vkey key 0";
    check(h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                        add_stats) == ENGINE_KEY_ENOENT, "Expected not found.");


    stop_persistence(h, h1);

    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "value", &itm) == ENGINE_SUCCESS,
          "Failed to set key");
    h1->release(h, NULL, itm);

    // Check to make sure a non-persisted item is 'dirty'
    check(h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    check(vals.find("key_valid")->second.compare("dirty") == 0,
          "Expected 'dirty'");

    // Check that a key that is resident and persisted returns valid
    start_persistence(h, h1);
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);
    check(h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    check(vals.find("key_valid")->second.compare("valid") == 0,
          "Expected 'valid'");

    // Check that an evicted key still returns valid
    evict_key(h, h1, "key", 0, "Ejected.");
    check(h1->get_stats(h, cookie, "vkey key 0", 10, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_valid")->second.compare("valid") == 0, "Expected 'valid'");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_multiple_transactions(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");
    for (int j = 0; j < 1000; ++j) {
        std::stringstream s1;
        s1 << "key-0-" << j;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET,
              s1.str().c_str(), s1.str().c_str(), &i, 0, 0) == ENGINE_SUCCESS,
              "Failed to store a value");
        h1->release(h, NULL, i);
        std::stringstream s2;
        s2 << "key-1-" << j;
        check(store(h, h1, NULL, OPERATION_SET,
              s2.str().c_str(), s2.str().c_str(), &i, 0, 1) == ENGINE_SUCCESS,
              "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 2000);
    check(get_int_stat(h, h1, "ep_commit_num") > 1,
          "Expected 20 transaction completions at least");
    return SUCCESS;
}

static enum test_result test_gat_locked(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET,
                "key", "value", &itm) == ENGINE_SUCCESS, "Failed to set key");
    h1->release(h, NULL, itm);

    getl(h, h1, "key", 0, 15);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected getl to succeed on key");

    gat(h, h1, "key", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL, "Expected tmp fail");
    check(last_body == "Lock Error", "Wrong error message");

    testHarness.time_travel(16);
    gat(h, h1, "key", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    testHarness.time_travel(11);
    check(h1->get(h, NULL, &itm, "key", 3, 0) == ENGINE_KEY_ENOENT,
          "Expected value to be expired");

    return SUCCESS;
}

static enum test_result test_getl_delete_with_bad_cas(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET,
                "key", "value", &itm) == ENGINE_SUCCESS, "Failed to set key");
    h1->release(h, NULL, itm);

    uint64_t cas = last_cas;
    getl(h, h1, "key", 0, 15);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected getl to succeed on key");

    check(del(h, h1, "key", cas, 0) == ENGINE_TMPFAIL, "Expected TMPFAIL");

    return SUCCESS;
}

static enum test_result test_getl_delete_with_cas(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET,
                "key", "value", &itm) == ENGINE_SUCCESS, "Failed to set key");
    h1->release(h, NULL, itm);

    getl(h, h1, "key", 0, 15);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected getl to succeed on key");

    check(del(h, h1, "key", last_cas, 0) == ENGINE_SUCCESS, "Expected SUCCESS");

    return SUCCESS;
}

static enum test_result test_getl_set_del_with_meta(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    const char *key = "key";
    const char *val = "value";
    const char *newval = "newvalue";
    check(store(h, h1, NULL, OPERATION_SET,
                key, val, &itm) == ENGINE_SUCCESS, "Failed to set key");
    h1->release(h, NULL, itm);

    getl(h, h1, key, 0, 15);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected getl to succeed on key");

    check(get_meta(h, h1, key), "Expected to get meta");

    //init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = time(NULL) + 300;
    itm_meta.flags = 0xdeadbeef;

    //do a set with meta
    set_with_meta(h, h1, key, strlen(key), newval, strlen(newval), 0,
                  &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected item to be locked");

    //do a del with meta
    del_with_meta(h, h1, key, strlen(key), 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected item to be locked");
    return SUCCESS;
}

static enum test_result test_touch_locked(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET,
                "key", "value", &itm) == ENGINE_SUCCESS, "Failed to set key");
    h1->release(h, NULL, itm);

    getl(h, h1, "key", 0, 15);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected getl to succeed on key");

    touch(h, h1, "key", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL, "Expected tmp fail");
    check(last_body == "Lock Error", "Wrong error message");

    testHarness.time_travel(16);
    touch(h, h1, "key", 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    testHarness.time_travel(11);
    check(h1->get(h, NULL, &itm, "key", 3, 0) == ENGINE_KEY_ENOENT,
          "Expected value to be expired");

    return SUCCESS;
}

static enum test_result test_est_vb_move(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    check(estimateVBucketMove(h, h1, 0) == 0, "Empty VB estimate is wrong");

    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    check(estimateVBucketMove(h, h1, 0) == 11, "Invalid estimate");
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(1801);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");

    for (int ii = 0; ii < 2; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(del(h, h1, ss.str().c_str(), 0, 0) == ENGINE_SUCCESS,
              "Failed to remove a key");
    }

    check(estimateVBucketMove(h, h1, 0) == 8, "Invalid estimate");
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(1801);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 4, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:persisted_checkpoint_id", 3, "checkpoint");

    stop_persistence(h, h1);
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "longerkey" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    check(estimateVBucketMove(h, h1, 0) >= 15, "Invalid estimate");

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    size_t backfillAge = 0;
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_BACKFILL |
                                             TAP_CONNECT_CHECKPOINT,
                                             static_cast<void*>(&backfillAge),
                                             8);
    check(iter != NULL, "Failed to create a tap iterator");
    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    std::string key;
    tap_event_t event;
    bool backfillphase = true;
    bool done = false;
    uint32_t opaque;
    int total_sent = 0;
    int mutations = 0;
    int deletions = 0;

    size_t chk_items;
    size_t remaining;

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        int64_t byseq = -1;
        if (event == TAP_CHECKPOINT_START ||
            event == TAP_DELETION || event == TAP_MUTATION) {
            uint8_t *es = ((uint8_t*)engine_specific) + 8;
            memcpy(&byseq, (void*)es, 8);
            byseq = ntohll(byseq);
        }

        switch (event) {
        case TAP_PAUSE:
            if (total_sent == 10) {
                done = true;
            }
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_OPAQUE:
            opaque = ntohl(*(static_cast<int*> (engine_specific)));
            if (opaque == TAP_OPAQUE_CLOSE_BACKFILL) {
                check(mutations == 3, "Invalid number of backfill mutations");
                check(deletions == 2, "Invalid number of backfill deletions");
                backfillphase = false;
            }
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
            h1->release(h, NULL, it);
            break;
        case TAP_MUTATION:
        case TAP_DELETION:
            total_sent++;
            if (event == TAP_DELETION) {
                deletions++;
            } else {
                mutations++;
            }

            if (!backfillphase) {
                chk_items = estimateVBucketMove(h, h1, 0, name.c_str());
                remaining = 10 - total_sent;
                check(chk_items == remaining, "Invalid Estimate of chk items");
            }
            h1->release(h, NULL, it);
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:sent_from_vb_0",
                       "tap") == 10, "Incorrect number of items sent");
    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_set_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that set without cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 1,
                       "Expected 1 set rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    // Check that set with correct cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, last_meta.cas, 10, 1735689600);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 2,
                       "Expected 2 set rm ops");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 2, "Invalid result for seqno");

    // Check that updating an item with no cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 3,
                       "Expected 3 set rm ops");

    check(last_meta.flags == 5, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 3, "Invalid result for seqno");

    // Check that updating an item with the wrong cas fails
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, last_meta.cas + 1, 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected set returing meta to fail");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 3,
                       "Expected 3 set rm ops");

    return SUCCESS;
}

static enum test_result test_set_ret_meta_error(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check invalid packet constructions
    set_ret_meta(h, h1, "", 0, "value", 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected set returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0, "key", 3, "val", 3);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to store ret meta");
    free(pkt);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected set returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    set_ret_meta(h, h1, "key", 3, "value", 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected set returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected set returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected set returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to set vbucket state.");
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected set returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_add_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that add with cas fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 10, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_STORED,
          "Expected set returing meta to fail");

    // Check that add without cas succeeds.
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 1,
                       "Expected 1 set rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    // Check that re-adding a key fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_STORED,
          "Expected set returing meta to fail");

    // Check that adding a key with flags and exptime returns the correct values
    add_ret_meta(h, h1, "key2", 4, "value", 5, 0, 0, 10, 1735689600);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_set_ret_meta") == 2,
                       "Expected 2 set rm ops");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    return SUCCESS;
}

static enum test_result test_add_ret_meta_error(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check invalid packet constructions
    add_ret_meta(h, h1, "", 0, "value", 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected add returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0, "key", 3, "val", 3);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to add ret meta");
    free(pkt);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected add returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    add_ret_meta(h, h1, "key", 3, "value", 5, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected add returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to add vbucket state.");
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_del_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that deleting a non-existent key fails
    del_ret_meta(h, h1, "key", 3, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "Expected set returing meta to fail");

    // Check that deleting a non-existent key with a cas fails
    del_ret_meta(h, h1, "key", 3, 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "Expected set returing meta to fail");

    // Check that deleting a key with no cas succeeds
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_del_ret_meta") == 1,
                       "Expected 1 del rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 2, "Invalid result for seqno");

    // Check that deleting a key with a cas succeeds.
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 10, 1735689600);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 3, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, last_meta.cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");
    check(get_int_stat(h, h1, "ep_num_ops_del_ret_meta") == 2,
                       "Expected 2 del rm ops");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 4, "Invalid result for seqno");

    // Check that deleting a key with the wrong cas fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected set returing meta to succeed");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 5, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, last_meta.cas + 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected set returing meta to fail");
    check(get_int_stat(h, h1, "ep_num_ops_del_ret_meta") == 2,
                       "Expected 2 del rm ops");

    return SUCCESS;
}

static enum test_result test_del_ret_meta_error(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check invalid packet constructions
    del_ret_meta(h, h1, "", 0, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected add returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0, "key", 3);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to del ret meta");
    free(pkt);
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected add returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    del_ret_meta(h, h1, "key", 3, 0);
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected add returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    del_ret_meta(h, h1, "key", 3, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    del_ret_meta(h, h1, "key", 3, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to add vbucket state.");
    del_ret_meta(h, h1, "key", 3, 1);
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_set_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key", 0, "Ejected.");
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "key", "newvalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_setWithMeta_with_item_eviction(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    size_t newValLen = strlen(newVal);

    // create a new key
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 300;
    itm_meta.flags = 0xdeadbeef;

    // set with meta for a non-resident item should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    return SUCCESS;
}

struct multi_meta_args {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    int start;
    int end;
};

extern "C" {
    static void multi_set_with_meta(void *args) {
        struct multi_meta_args *mma = static_cast<multi_meta_args *>(args);

        for (int i = mma->start; i < mma->end; i++) {
            // init some random metadata
            ItemMetaData itm_meta;
            itm_meta.revSeqno = 10;
            itm_meta.cas = 0xdeadbeef;
            itm_meta.exptime = 0;
            itm_meta.flags = 0xdeadbeef;

            std::stringstream key;
            key << "key" << i;

            set_with_meta(mma->h, mma->h1, key.str().c_str(),
                          key.str().length(), "somevalueEdited", 15,
                          0, &itm_meta, last_cas);
        }
    }

    static void multi_del_with_meta(void *args) {
        struct multi_meta_args *mma = static_cast<multi_meta_args *>(args);

        for (int i = mma->start; i < mma->end; i++) {
            // init some random metadata
            ItemMetaData itm_meta;
            itm_meta.revSeqno = 10;
            itm_meta.cas = 0xdeadbeef;
            itm_meta.exptime = 0;
            itm_meta.flags = 0xdeadbeef;

            std::stringstream key;
            key << "key" << i;

            del_with_meta(mma->h, mma->h1, key.str().c_str(),
                          key.str().length(), 0, &itm_meta, last_cas);
        }
    }
}

static enum test_result test_multiple_set_delete_with_metas_full_eviction(
                                    ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
            "Failed to get stats");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    cb_assert(eviction_policy == "full_eviction");

    int i = 0;
    while(i < 1000) {
        uint64_t cas_for_set = last_cas;
        // init some random metadata
        ItemMetaData itm_meta;
        itm_meta.revSeqno = 10;
        itm_meta.cas = 0xdeadbeef;
        itm_meta.exptime = 0;
        itm_meta.flags = 0xdeadbeef;

        std::stringstream key;
        key << "key" << i;

        set_with_meta(h, h1, key.str().c_str(), key.str().length(),
                "somevalue", 9, 0, &itm_meta, cas_for_set);
        i++;
    }

    wait_for_flusher_to_settle(h, h1);

    int curr_vb_items = get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0");
    int num_ops_set_with_meta = get_int_stat(h, h1, "ep_num_ops_set_meta");
    cb_assert(curr_vb_items == num_ops_set_with_meta && curr_vb_items > 0);

    cb_thread_t thread1, thread2;
    struct multi_meta_args mma1, mma2;
    mma1.h = h;
    mma1.h1 = h1;
    mma1.start = 0;
    mma1.end = 100;
    cb_assert(cb_create_thread(&thread1, multi_set_with_meta, &mma1, 0) == 0);

    mma2.h = h;
    mma2.h1 = h1;
    mma2.start = curr_vb_items - 100;
    mma2.end = curr_vb_items;
    cb_assert(cb_create_thread(&thread2, multi_del_with_meta, &mma2, 0) == 0);

    cb_assert(cb_join_thread(thread1) == 0);
    cb_assert(cb_join_thread(thread2) == 0);

    wait_for_flusher_to_settle(h, h1);

    cb_assert(get_int_stat(h, h1, "ep_num_ops_set_meta") > num_ops_set_with_meta);
    cb_assert(get_int_stat(h ,h1, "ep_num_ops_del_meta") > 0);

    curr_vb_items = get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    check(get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0")
          == curr_vb_items, "Unexpected item count in vbucket");

    return SUCCESS;
}


static enum test_result test_add_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key", 0, "Ejected.");

    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");
    h1->release(h, NULL, i);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    check(store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i) == ENGINE_SUCCESS,
          "Failed to add value again.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_getMeta_with_item_eviction(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1)
{
    char const *key = "test_get_meta";
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    Item *it = reinterpret_cast<Item*>(i);

    check(get_meta(h, h1, key), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_meta.revSeqno == it->getRevSeqno(), "Expected seqno to match");
    check(last_meta.cas == it->getCas(), "Expected cas to match");
    check(last_meta.exptime == it->getExptime(), "Expected exptime to match");
    check(last_meta.flags == it->getFlags(), "Expected flags to match");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_gat_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "mykey", 0, "Ejected.");

    gat(h, h1, "mykey", 0, 10); // 10 sec as expiration time
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(last_body == "somevalue", "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_keyStats_with_item_eviction(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // set (k1,v1) in vbucket 0
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "k1", 0, "Ejected.");

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_delWithMeta_with_item_eviction(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {

    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    // put some random meta data
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key,
                "somevalue", &i) == ENGINE_SUCCESS, "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    // delete an item with meta data
    del_with_meta(h, h1, key, keylen, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_del_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key", 0, "Ejected.");

    Item *it = reinterpret_cast<Item*>(i);
    uint64_t orig_cas = it->getCas();
    h1->release(h, NULL, i);

    uint64_t cas = 0;
    uint64_t vb_uuid;
    mutation_descr_t mut_info;
    uint32_t high_seqno;

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    check(h1->remove(h, NULL, "key", 3, &cas, 0, &mut_info) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(orig_cas + 1 == cas, "Cas mismatch on delete");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(vb_uuid == mut_info.vbucket_uuid, "Expected valid vbucket uuid");
    check(high_seqno + 1 == mut_info.seqno, "Expected valid sequence number");

    return SUCCESS;
}

static enum test_result test_observe_with_item_eviction(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    // Create some vbuckets
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Set some keys to observe
    item *it = NULL;
    uint64_t cas1, cas2, cas3;

    std::string value('x', 100);
    check(storeCasOut(h, h1, NULL, 0, "key1", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas1) == ENGINE_SUCCESS,
          "Set should work.");
    check(storeCasOut(h, h1, NULL, 1, "key2", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas2) == ENGINE_SUCCESS,
          "Set should work.");
    check(storeCasOut(h, h1, NULL, 1, "key3", value, PROTOCOL_BINARY_RAW_BYTES,
                      it, cas3) == ENGINE_SUCCESS,
          "Set should work.");

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 3);

    evict_key(h, h1, "key1", 0, "Ejected.");
    evict_key(h, h1, "key2", 1, "Ejected.");

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    obskeys["key2"] = 1;
    obskeys["key3"] = 1;
    observe(h, h1, obskeys);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Check the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 17, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 26, sizeof(uint64_t));
    check(ntohll(cas) == cas2, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 34, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    check(strncmp(key, "key3", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    check(persisted == OBS_STATE_PERSISTED, "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    check(ntohll(cas) == cas3, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_expired_item_with_item_eviction(ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {
    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);
    gat(h, h1, "mykey", 0, 10); // 10 sec as expiration time
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(last_body == "somevalue", "Invalid data returned");

    // Store a dummy item since we do not purge the item with highest seqno
    check(ENGINE_SUCCESS ==
          store(h, h1, NULL, OPERATION_SET, "dummykey", "dummyvalue", NULL,
                0, 0, 0), "Error setting.");

    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "mykey", 0, "Ejected.");

    // time-travel 11 secs..
    testHarness.time_travel(11);

    // Compaction on VBucket 0
    compact_db(h, h1, 0, 10, 10, 0);

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    check(get_int_stat(h, h1, "vb_active_expired") == 1,
          "Expect the compactor to delete an expired item");

    // The item is already expired...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_non_existent_get_and_delete(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    check(h1->get(h, NULL, &i, "key1", 4, 0) == ENGINE_KEY_ENOENT,
            "Unexpected return status");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Unexpected temp item");
    check(del(h, h1, "key3", 0, 0) == ENGINE_KEY_ENOENT, "Unexpected return status");
    check(get_int_stat(h, h1, "curr_temp_items") == 0, "Unexpected temp item");
    return SUCCESS;
}

static enum test_result test_mb16421(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);

    // Evict Item!
    evict_key(h, h1, "mykey", 0, "Ejected.");

    // Issue Get Meta
    check(get_meta(h, h1, "mykey"), "Expected to get meta");

    // Issue Get
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_SUCCESS, "Item should be there");
    h1->release(h, NULL, itm);

    return SUCCESS;
}

static enum test_result test_get_random_key(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {

    const void *cookie = testHarness.create_cookie();

    // An empty database should return no key
    protocol_binary_request_header pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.request.opcode = PROTOCOL_BINARY_CMD_GET_RANDOM_KEY;

    check(h1->unknown_command(h, cookie, &pkt, add_response) ==
          ENGINE_KEY_ENOENT, "Database should be empty");

    // Store a key
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_SUCCESS, "Item should be there");
    h1->release(h, NULL, itm);

    // We should be able to get one if there is something in there
    check(h1->unknown_command(h, cookie, &pkt, add_response) ==
          ENGINE_SUCCESS, "get random should work");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_datatype == PROTOCOL_BINARY_DATATYPE_JSON, "Expected datatype to be JSON");

    // Since it is random we can't really check that we don't get the
    // same value twice...

    // Check for invalid packets
    pkt.request.extlen = 1;
    check(h1->unknown_command(h, cookie, &pkt, add_response) ==
          ENGINE_EINVAL, "extlen not allowed");

    pkt.request.extlen = 0;
    pkt.request.keylen = 1;
    check(h1->unknown_command(h, cookie, &pkt, add_response) ==
          ENGINE_EINVAL, "keylen not allowed");

    pkt.request.keylen = 0;
    pkt.request.bodylen = 1;
    check(h1->unknown_command(h, cookie, &pkt, add_response) ==
          ENGINE_EINVAL, "bodylen not allowed");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_failover_log_behavior(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    uint64_t num_entries, top_entry_id;
    // warm up
    wait_for_warmup_complete(h, h1);
    num_entries = get_int_stat(h, h1, "vb_0:num_entries", "failovers");

    check(num_entries == 1, "Failover log should have one entry for new vbucket");
    top_entry_id = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    // restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);
    num_entries = get_int_stat(h, h1, "vb_0:num_entries", "failovers");

    check(num_entries == 2, "Failover log should have grown");
    check(get_ull_stat(h, h1, "vb_0:0:id", "failovers") != top_entry_id,
            "Entry at current seq should be overwritten after restart");

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 10);

    // restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);
    num_entries = get_int_stat(h, h1, "vb_0:num_entries", "failovers");

    check(num_entries == 3, "Failover log should have grown");
    check(get_ull_stat(h, h1, "vb_0:0:seq", "failovers") == 10,
            "Latest failover log entry should have correct high sequence number");

    return SUCCESS;
}

/* The Defragmenter (and hence it's unit tests) depend on using jemalloc as the
 * memory allocator.
 */
#if defined(HAVE_JEMALLOC)
/* Waits for mapped memory value to drop below the specified value, or for
 * the maximum_sleep_time to be reached.
 * @returns True if the mapped memory value dropped, or false if the maximum
 * sleep time was reached.
 */
static bool wait_for_mapped_below(size_t mapped_threshold,
                                  useconds_t max_sleep_time) {
    useconds_t sleepTime = 128;
    useconds_t totalSleepTime = 0;
    while (testHarness.get_mapped_bytes() > mapped_threshold) {
        decayingSleep(&sleepTime);
        totalSleepTime += sleepTime;
        if (totalSleepTime > max_sleep_time) {
            return false;
        }
    }
    return true;
}

// Create a number of documents, spanning at least two or more pages, then
// delete most (but not all) of them - crucially ensure that one document from
// each page is still present. This will result in the rest of that page
// being 'wasted'. Then verify that after defragmentation the actual memory
// usage drops down to (close to) mem_used.
static enum test_result test_defragmenter(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();

    // Sanity check - need memory tracker to be able to check our memory usage.
    check(get_bool_stat(h, h1, "ep_mem_tracker_enabled"),
          "Memory tracker not enabled");

    // Enable vbucket 1 in addition to vbucket zero.
    const uint16_t num_vbuckets = 2;
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

    // Reduce our memory usage to a minimum before we start taking
    // measurments.
    testHarness.release_free_memory();

    // 0. Get baseline memory usage (before creating any objects).
    //    First ensure stats are up-to-date, by getting directly from the
    //    allocator (normally they are retrieved periodically by a worker
    //    thread).
    size_t mem_used_0 = get_ull_stat(h, h1, "mem_used", NULL);
    size_t mapped_0 = testHarness.get_mapped_bytes();

    // 1. Create a number of small documents. Doesn't really matter that
    //    they are small, main thing is we create enough to span multiple
    //    pages (so we can later leave 'holes' when they are deleted).
    const size_t size = 128;
    const size_t num_docs = 50000;
    std::string data(size, 'x');
    for (unsigned int i = 0; i < num_docs; i++ ) {
        // Deliberately using C-style int-to-string conversion (instead of
        // stringstream) to minimize heap pollution while filling ep_engine.
        char key[16];
        snprintf(key, sizeof(key), "%d", i);
        item *item = NULL;
        uint16_t vb = i % num_vbuckets;
        check(storeCasVb11(h, h1, cookie, OPERATION_ADD, key, data.c_str(),
                           data.length(), 0, &item, 0, vb, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, item);
    }
    wait_for_flusher_to_settle(h, h1);

    // Record memory usage after creation.
    size_t mem_used_1 = get_ull_stat(h, h1, "mem_used", NULL);
    size_t mapped_1 = testHarness.get_mapped_bytes();

    // Sanity check - mem_used should be at least size * count bytes larger than
    // initial.
    check(mem_used_1 >= mem_used_0 + (size * num_docs),
          "mem_used smaller than expected after creating documents");

    // 2. Determine how many documents are in each page, and then remove all but
    //    one from each page.
    size_t num_remaining = num_docs;
    const size_t LOG_PAGE_SIZE = 12; // 4K page
    {
        typedef std::unordered_map<uintptr_t, std::vector<int> > page_to_keys_t;
        page_to_keys_t page_to_keys;

        // Build a map of pages to keys
        for (unsigned int i = 0; i < num_docs; i++ ) {
            char key[16];
            snprintf(key, sizeof(key), "%d", i);
            uint16_t vb = i % num_vbuckets;

            item_info info;
            check(get_item_info(h, h1, &info, key, vb), "Unable to get item_info");
            check(info.nvalue == 1, "info.nvalue != 1");
            const uintptr_t page = uintptr_t(info.value[0].iov_base) >> LOG_PAGE_SIZE;
            page_to_keys[page].emplace_back(i);
        }

        // Now remove all but one document from each page.
        for (page_to_keys_t::iterator kv = page_to_keys.begin();
             kv != page_to_keys.end();
             kv++) {
            // Free all but one document on this page.
            while (kv->second.size() > 1) {
                auto doc_id = kv->second.back();
                char key[16];
                snprintf(key, sizeof(key), "%d", doc_id);
                uint16_t vb = doc_id % num_vbuckets;

                uint64_t cas = 0;
                mutation_descr_t mut_info;
                check(h1->remove(h, NULL, key, strlen(key), &cas, vb,
                                 &mut_info) == ENGINE_SUCCESS,
                      "Failed to remove key.");
                kv->second.pop_back();
                num_remaining--;
            }
        }
        wait_for_flusher_to_settle(h, h1);
    }

    // Release free memory back to OS to minimize our footprint after
    // removing the documents above.
    testHarness.release_free_memory();

    // Sanity check - mem_used should have reduced down by approximately how
    // many documents were removed.
    // Allow some extra, to handle any increase in data structure sizes used
    // to actually manage the objects.
    const double fuzz_factor = 1.2;
    const size_t all_docs_size = mem_used_1 - mem_used_0;
    const size_t remaining_size = (all_docs_size / num_docs) * num_remaining;
    const size_t expected_mem = (mem_used_0 + remaining_size) * fuzz_factor;
    wait_for_memory_usage_below(h, h1, expected_mem);

    size_t mapped_2 = testHarness.get_mapped_bytes();

    // Sanity check (2) - mapped memory should still be high - at least 90% of
    // the value after creation, before delete.
    const size_t current_mapped = mapped_2 - mapped_0;
    const size_t previous_mapped = mapped_1 - mapped_0;

    check(current_mapped >= 0.9 * double(previous_mapped),
          ("current_mapped memory (which is " + std::to_string(current_mapped) +
           ") is lower than 90% of previous mapped (which is " +
           std::to_string(previous_mapped) + "). ").c_str());

    // 3. Enable defragmenter and trigger defragmentation
    // (Enable defragmenter task if it was disabled)

    checkeq(get_bool_stat(h, h1, "ep_defragmenter_enabled"), false,
            "Expected defragmenter to be disabled");
    check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "defragmenter_enabled", "true"),
          "Set defragmenter_enabled should have worked");

    check(set_param(h, h1, protocol_binary_engine_param_flush, "defragmenter_run",
                    "true"),
          "Failed to trigger defragmenter");

    // Check that mapped memory has decreased after defragmentation - should be
    // less than 60% of the amount before defrag (this is pretty conservative,
    // but it's hard to accurately predict the whole-application size).
    // Give it 10 seconds to drop.
    const size_t expected_mapped = ((mapped_2 - mapped_0) * 0.6) + mapped_0;
    check(wait_for_mapped_below(expected_mapped,
                                10 * 1000 * 1000),
          "Mapped memory didn't reduce as expected after defragmentation");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}
#endif // defined(HAVE_JEMALLOC)

static enum test_result test_hlc_cas(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    const char *key = "key";
    item *i = NULL;
    item_info info;
    uint64_t curr_cas = 0, prev_cas = 0;

    memset(&info, 0, sizeof(info));

    //enabled time sync
    set_drift_counter_state(h, h1, 100000, true);
    check(store(h, h1, NULL, OPERATION_ADD, key, "data1", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //set a lesser drift and ensure that the CAS is monotonically
    //increasing
    set_drift_counter_state(h, h1, 100, true);

    check(store(h, h1, NULL, OPERATION_SET, key, "data2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //ensure that the adjusted time will be negative
    int64_t drift_counter = (-1) * (gethrtime() * 2);
    set_drift_counter_state(h, h1, drift_counter, true);

    protocol_binary_request_header *request;
    int64_t adjusted_time;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
            "Expected Success");
    check(last_body.size() == sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time, last_body.data(), last_body.size());
    adjusted_time = ntohll(adjusted_time);
    std::string err_msg("Adjusted time " + std::to_string(adjusted_time) +
                        " is supposed to have been negative!");
    check(adjusted_time < 0, err_msg.c_str());

    check(store(h, h1, NULL, OPERATION_REPLACE, key, "data3", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //disable time sync
    set_drift_counter_state(h, h1, 0, false);

    getl(h, h1, key, 0, 10);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");
    curr_cas = last_cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    uint64_t result = 0;
    check(h1->arithmetic(h, NULL, "key2", 4, true, true, 1, 1, 0,
                         &i, PROTOCOL_BINARY_RAW_BYTES, &result, 0)
                         == ENGINE_SUCCESS, "Failed arithmetic operation");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key2"), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");

    return SUCCESS;
}

static enum test_result test_failover_log_dcp(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const int num_items = 50, num_testcases = 12;
    uint64_t end_seqno = num_items + 1000;
    uint32_t high_seqno = 0;

    for (int j = 0; j < num_items; ++j) {
        std::string key("key" + std::to_string(j));
        check(store(h, h1, NULL, OPERATION_SET, key.c_str(), "data", NULL)
              == ENGINE_SUCCESS, "Failed to store a value");
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", num_items);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    wait_for_stat_to_be(h, h1, "curr_items", num_items);

    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t uuid = get_ull_stat(h, h1, "vb_0:1:id", "failovers");

    typedef struct dcp_params {
        uint64_t vb_uuid;
        uint64_t start_seqno;
        uint64_t snap_start_seqno;
        uint64_t snap_end_seqno;
        uint64_t exp_rollback;
        ENGINE_ERROR_CODE exp_err_code;
    } dcp_params_t;

    dcp_params_t params[num_testcases] =
    {   /* Do not expect rollback when start_seqno is 0 */
        {uuid, 0, 0, 0, 0, ENGINE_SUCCESS},
        /* Do not expect rollback when start_seqno is 0 and vb_uuid mismatch */
        {0xBAD, 0, 0, 0, 0, ENGINE_SUCCESS},
        /* Don't expect rollback when you already have all items in the snapshot
           (that is, start == snap_end) and upper >= snap_end */
        {uuid, high_seqno, 0, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, high_seqno - 1, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
        /* Do not expect rollback when you have no items in the snapshot
         (that is, start == snap_start) and upper >= snap_end */
        {uuid, high_seqno - 10, high_seqno - 10, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, high_seqno - 10, high_seqno - 10, high_seqno - 1, 0,
         ENGINE_SUCCESS},
        /* Do not expect rollback when you are in middle of a snapshot (that is,
           snap_start < start < snap_end) and upper >= snap_end */
        {uuid, 10, 0, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, 10, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
        /* Expect rollback when you are in middle of a snapshot (that is,
           snap_start < start < snap_end) and upper < snap_end. Rollback to
           snap_start if snap_start < upper */
        {uuid, 20, 10, high_seqno + 1, 10, ENGINE_ROLLBACK},
        /* Expect rollback when upper < snap_start_seqno. Rollback to upper */
        {uuid, high_seqno + 20, high_seqno + 10, high_seqno + 30, high_seqno,
         ENGINE_ROLLBACK},
        {uuid, high_seqno + 10, high_seqno + 10, high_seqno + 10, high_seqno,
         ENGINE_ROLLBACK},
        /* vb_uuid not found in failover table, rollback to zero */
        {0xBAD, 10, 0, high_seqno, 0, ENGINE_ROLLBACK},
        /* Add new test case here */
    };

    for (int i = 0; i < num_testcases; i++)
    {
        dcp_stream_req(h, h1, 1, 0, params[i].start_seqno, end_seqno,
                       params[i].vb_uuid, params[i].snap_start_seqno,
                       params[i].snap_end_seqno, params[i].exp_rollback,
                       params[i].exp_err_code);
    }
    return SUCCESS;
}

static enum test_result test_get_all_vb_seqnos(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();

    const int num_vbuckets = 10;

    /* Create vbuckets */
    for (int i = 0; i < num_vbuckets; i++) {
        if (i < num_vbuckets/2) {
            /* Active vbuckets */
            check(set_vbucket_state(h, h1, i, vbucket_state_active),
                  "Failed to set vbucket state.");
            for (int j= 0; j < i; j++) {
                std::string key("key" + std::to_string(i));
                check(store(h, h1, NULL, OPERATION_SET, key.c_str(),
                            "value", NULL, 0, i)
                      == ENGINE_SUCCESS, "Failed to store an item.");
            }
        } else {
            /* Replica vbuckets */
            check(set_vbucket_state(h, h1, i, vbucket_state_replica),
                  "Failed to set vbucket state.");
        }
    }

    /* Create request to get vb seqno of all vbuckets */
    get_all_vb_seqnos(h, h1, static_cast<vbucket_state_t>(0), cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, 0, num_vbuckets);

    /* Create request to get vb seqno of active vbuckets */
    get_all_vb_seqnos(h, h1, vbucket_state_active, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, 0, num_vbuckets/2);

    /* Create request to get vb seqno of replica vbuckets */
    get_all_vb_seqnos(h, h1, vbucket_state_replica, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, num_vbuckets/2, num_vbuckets);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

struct mb16357_ctx {
    mb16357_ctx(ENGINE_HANDLE *_h, ENGINE_HANDLE_V1 *_h1, int _items) :
        h(_h), h1(_h1), items(_items) { }

    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    int items;
};

extern "C" {
    static void dcp_thread_func(void *args) {
        struct mb16357_ctx *ctx = static_cast<mb16357_ctx *>(args);

        const void *cookie = testHarness.create_cookie();
        uint32_t opaque = 0xFFFF0000;
        uint32_t flags = 0;
        std::string name = "unittest";

        while (get_int_stat(ctx->h, ctx->h1, "ep_pending_compactions") == 0);

        // Switch to replica
        check(set_vbucket_state(ctx->h, ctx->h1, 0, vbucket_state_replica),
                "Failed to set vbucket state.");

        // Open consumer connection
        checkeq(ctx->h1->dcp.open(ctx->h, cookie, opaque, 0, flags,
                                  (void*)name.c_str(), name.length()),
                ENGINE_SUCCESS,
                "Failed dcp Consumer open connection.");

        add_stream_for_consumer(ctx->h, ctx->h1, cookie, opaque++, 0, 0,
                PROTOCOL_BINARY_RESPONSE_SUCCESS);


        uint32_t stream_opaque = get_int_stat(ctx->h, ctx->h1,
                                              "eq_dcpq:unittest:stream_0_opaque",
                                              "dcp");


        for (int i = 1; i <= ctx->items; i++) {
            std::stringstream ss;
            ss << "kamakeey-" << i;

            // send mutations in single mutation snapshots to race more with compaction
            ctx->h1->dcp.snapshot_marker(ctx->h, cookie,
                                         stream_opaque, 0/*vbid*/,
                                         ctx->items, ctx->items + i, 2);
            ctx->h1->dcp.mutation(ctx->h, cookie, stream_opaque,
                                  ss.str().c_str(), ss.str().length(),
                                  "value", 5, i * 3, 0, 0, 0,
                                  i + ctx->items, i + ctx->items,
                                  0, 0, "", 0, INITIAL_NRU_VALUE);
        }

        testHarness.destroy_cookie(cookie);
    }

    static void compact_thread_func(void *args) {
        struct mb16357_ctx *ctx = static_cast<mb16357_ctx *>(args);
        compact_db(ctx->h, ctx->h1, 0, 99, ctx->items, 1);
    }
}

static enum test_result test_mb16357(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {

    // Load up vb0 with n items, expire in 1 second
    int num_items = 1000;

    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key-" << j;
        check(store(h, h1, NULL, OPERATION_SET,
                    ss.str().c_str(), "data", &i, 0, 0, 1/*expire*/, 0)
                    == ENGINE_SUCCESS, "Failed to store a value"); //expire in 1 second

        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(3617); // force expiry pushing time forward.

    struct mb16357_ctx ctx(h, h1, num_items);
    cb_thread_t cp_thread, dcp_thread;

    cb_assert(cb_create_thread(&cp_thread,
                               compact_thread_func,
                               &ctx, 0) == 0);
    cb_assert(cb_create_thread(&dcp_thread,
                               dcp_thread_func,
                               &ctx, 0) == 0);

    cb_assert(cb_join_thread(cp_thread) == 0);
    cb_assert(cb_join_thread(dcp_thread) == 0);

    return SUCCESS;
}

/*
    Basic test demonstrating multi-bucket operations.
    Checks that writing the same key to many buckets works as it should.
*/
static enum test_result test_multi_bucket_set_get(engine_test_t* test) {
    const int n_buckets = 20;
    std::vector<BucketHolder> buckets;
    if (create_buckets(test->cfg, n_buckets, buckets) != n_buckets) {
        destroy_buckets(buckets);
        return FAIL;

    }

    for (auto bucket : buckets) {
        // re-use test_setup which will wait for bucket ready
        test_setup(bucket.h, bucket.h1);
    }

    int ii = 0;
    for (auto bucket : buckets) {
        item*i = NULL;
        std::stringstream val;
        val << "value_" << ii++;
        check(ENGINE_SUCCESS ==
              store(bucket.h, bucket.h1, NULL,
                    OPERATION_SET, "key", val.str().c_str(), &i),
                    "Error setting.");
        bucket.h1->release(bucket.h, NULL, i);
    }

    // Read back the values
    ii = 0;
    for (auto bucket : buckets) {
        std::stringstream val;
        val << "value_" << ii++;
        check_key_value(bucket.h, bucket.h1,
                        "key", val.str().c_str(), val.str().length());
    }

    destroy_buckets(buckets);

    return SUCCESS;
}

BaseTestCase testsuite_testcases[] = {
        TestCase("validate engine handle", test_validate_engine_handle,
                 NULL, teardown, NULL, prepare, cleanup),

        // basic tests
        TestCase("test alloc limit", test_alloc_limit, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test_memory_tracking", test_memory_tracking, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test total memory limit", test_memory_limit,
                 test_setup, teardown,
                 "max_size=10240000;ht_locks=1;ht_size=3;"
                 "chk_remover_stime=1;chk_period=60",
                 prepare, cleanup),
        TestCase("test max_size - water_mark changes",
                 test_max_size_and_water_marks_settings,
                 test_setup, teardown,
                 "max_size=1000;ht_locks=1;ht_size=3", prepare,
                 cleanup),
        TestCase("test whitespace dbname", test_whitespace_db,
                 test_setup, teardown,
                 "dbname=" WHITESPACE_DB ";ht_locks=1;ht_size=3",
                 prepare, cleanup),
        TestCase("get miss", test_get_miss, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("set", test_set, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("concurrent set", test_conc_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("concurrent incr", test_conc_incr, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test_conc_incr_new_itm", test_conc_incr_new_itm, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("multi set", test_multi_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set+get hit", test_set_get_hit, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test getl then del with cas", test_getl_delete_with_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test getl then del with bad cas",
                 test_getl_delete_with_bad_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test getl then set with meta",
                 test_getl_set_del_with_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("getl", test_getl, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("unl",  test_unl, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("set+get hit (bin)", test_set_get_hit_bin,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set with cas non-existent", test_set_with_cas_non_existent,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set+change flags", test_set_change_flags,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("add", test_add, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("add+add(same cas)", test_add_add_with_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("cas", test_cas, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("append", test_append, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("prepend", test_prepend, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("append (compressed)", test_append_compressed,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("prepend (compressed)", test_prepend_compressed,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("append/prepend to JSON", test_append_prepend_to_json,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("replace", test_replace, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("replace with eviction", test_replace_with_eviction,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("incr miss", test_incr_miss, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("incr", test_incr, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("incr with default", test_incr_default,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("incr expiry", test_bug2799, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test touch", test_touch, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test touch (MB-7342)", test_touch_mb7342, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test touch (MB-10277)", test_touch_mb10277, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test gat", test_gat, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test gatq", test_gatq, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test locked gat", test_gat_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test locked touch", test_touch_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test mb5215", test_mb5215, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("delete", test_delete, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("set/delete", test_set_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set/delete (invalid cas)", test_set_delete_invalid_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete/set/delete", test_delete_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("get/delete with missing db file", test_get_delete_missing_file,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("retain rowid over a soft delete", test_bug2509,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket deletion doesn't affect new data", test_bug7023,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("non-resident decrementers", test_mb3169,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("resident ratio after warmup", test_mb5172,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set vb 10000", test_set_vbucket_out_of_range,
                 test_setup, teardown, "max_vbuckets=1024", prepare, cleanup),
        TestCase("flush", test_flush, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("multiple flush requests", test_multiple_flush, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("flush with stats", test_flush_stats, test_setup, teardown,
                 "flushall_enabled=true;chk_remover_stime=1;chk_period=60",
                 prepare, cleanup),
        TestCase("flush multi vbuckets", test_flush_multiv,
                 test_setup, teardown,
                 "flushall_enabled=true;max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup),
        TestCase("flush_disabled", test_flush_disabled, test_setup, teardown,
                 "flushall_enabled=false;max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup),
        TestCase("flushall params", test_CBD_152, test_setup, teardown,
                 "flushall_enabled=true;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3", prepare, cleanup),
        TestCase("expiry pager settings", test_expiry_pager_settings,
                 test_setup, teardown, "exp_pager_enabled=false",
                 prepare, cleanup),
        TestCase("expiry", test_expiry, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("expiry_loader", test_expiry_loader, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("expiration on compaction", test_expiration_on_compaction,
                 test_setup, teardown, "exp_pager_enabled=false",
                 prepare, cleanup),
        TestCase("expiration on warmup", test_expiration_on_warmup,
                 test_setup, teardown, "exp_pager_stime=1", prepare, cleanup),
        TestCase("expiry_duplicate_warmup", test_bug3454, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("expiry_no_items_warmup", test_bug3522, test_setup,
                 teardown, "exp_pager_stime=3", prepare, cleanup),
        TestCase("replica read", test_get_replica, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("replica read: invalid state - active",
                 test_get_replica_active_state,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("replica read: invalid state - pending",
                 test_get_replica_pending_state,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("replica read: invalid state - dead",
                 test_get_replica_dead_state,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("replica read: invalid key", test_get_replica_invalid_key,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test getr with evicted key", test_get_replica_non_resident,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test observe no data", test_observe_no_data, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test observe single key", test_observe_single_key, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test observe on temp item", test_observe_temp_item, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test observe multi key", test_observe_multi_key, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test multiple observes", test_multiple_observes, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test observe with not found", test_observe_with_not_found, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test observe not my vbucket", test_observe_errors, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test observe seqno basic tests", test_observe_seqno_basic_tests,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test observe seqno failover", test_observe_seqno_failover,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test observe seqno error", test_observe_seqno_error,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test item pager", test_item_pager, test_setup,
                 teardown, "max_size=4096000", prepare, cleanup),
        TestCase("warmup conf", test_warmup_conf, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("bloomfilter conf", test_bloomfilter_conf, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test bloomfilters with value-only eviction",
                 test_bloomfilters, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test bloomfilters with full eviction",
                 test_bloomfilters, test_setup,
                 teardown, "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test bloomfilters with store apis - value_only eviction",
                 test_bloomfilters_with_store_apis, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test bloomfilters with store apis - full_eviction",
                 test_bloomfilters_with_store_apis, test_setup,
                 teardown, "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test bloomfilters's in a delete+set scenario",
                 test_bloomfilter_delete_plus_set_scenario, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test datatype", test_datatype, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test datatype with unknown command", test_datatype_with_unknown_command,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test session cas validation", test_session_cas_validation,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test access scanner settings", test_access_scanner_settings,
                 test_setup, teardown, "alog_path=/tmp/epaccess.log",
                 prepare, cleanup),
        TestCase("test access scanner", test_access_scanner, test_setup,
                 teardown, "alog_path=/tmp/epaccess.log;chk_remover_stime=1;"
                 "max_size=2621440", prepare, cleanup),

        // Stats tests
        TestCase("item stats", test_item_stats, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("stats", test_stats, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("io stats", test_io_stats, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("file stats", test_vb_file_stats, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("file stats post warmup", test_vb_file_stats_after_warmup,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("bg stats", test_bg_stats, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("bg meta stats", test_bg_meta_stats, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("mem stats", test_mem_stats, test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup),
        TestCase("stats key", test_key_stats, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("stats vkey", test_vkey_stats, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("stats vkey callback tests", test_stats_vkey_valid_field,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("warmup stats", test_warmup_stats, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("warmup with threshold", test_warmup_with_threshold,
                 test_setup, teardown,
                 "warmup_min_items_threshold=1", prepare, cleanup),
        TestCase("warmup with threshold and full eviction",
                 test_warmup_with_threshold,
                 test_setup, teardown,
                 "warmup_min_items_threshold=1;item_eviction_policy=full_eviction",
                 prepare, cleanup),
        TestCase("seqno stats", test_stats_seqno,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("diskinfo stats", test_stats_diskinfo,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("stats curr_items", test_curr_items, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("startup token stat", test_cbd_225, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("ep workload stats", test_workload_stats,
                 test_setup, teardown, "max_num_shards=5;max_threads=10", prepare, cleanup),
        TestCase("ep workload stats", test_max_workload_stats,
                 test_setup, teardown,
                 "max_num_shards=5;max_threads=14;max_num_auxio=1;max_num_nonio=4",
                 prepare, cleanup),
        TestCase("test set/get cluster config", test_cluster_config,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test NOT_MY_VBUCKET's clusterConfig response",
                 test_not_my_vbucket_with_cluster_config,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test ALL_KEYS api",
                 test_all_keys_api,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test ALL_KEYS api during bucket creation",
                 test_all_keys_api_during_bucket_creation,
                 test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("ep worker stats", test_worker_stats,
                 test_setup, teardown,
                 "max_num_workers=8;max_threads=8", prepare, cleanup),

        // eviction
        TestCase("value eviction", test_value_eviction, test_setup,
                 teardown, NULL, prepare, cleanup),
        // duplicate items on disk
        TestCase("duplicate items on disk", test_duplicate_items_disk,
                 test_setup, teardown, NULL, prepare, cleanup),
        // special non-Ascii keys
        TestCase("test special char keys", test_specialKeys, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test binary keys", test_binKeys, test_setup,
                 teardown, NULL, prepare, cleanup),
        // tap tests
        TestCase("set tap param", test_set_tap_param, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap_noop_interval default config",
                 test_tap_noop_config_default,
                 test_setup, teardown, NULL , prepare, cleanup),
        TestCase("tap_noop_interval config", test_tap_noop_config,
                 test_setup, teardown,
                 "tap_noop_interval=10", prepare, cleanup),
        TestCase("tap receiver mutation", test_tap_rcvr_mutate, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap receiver checkpoint start/end", test_tap_rcvr_checkpoint,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver vbucket state", test_tap_rcvr_set_vbstate,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (dead)", test_tap_rcvr_mutate_dead,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (pending)",
                 test_tap_rcvr_mutate_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (replica)",
                 test_tap_rcvr_mutate_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (replica) on a locked item",
                 test_tap_rcvr_mutate_replica_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete", test_tap_rcvr_delete,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (dead)", test_tap_rcvr_delete_dead,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (pending)", test_tap_rcvr_delete_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (replica)", test_tap_rcvr_delete_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (replica) on a locked item",
                 test_tap_rcvr_delete_replica_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap stream", test_tap_stream, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap stream send deletes", test_tap_sends_deleted, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test tap sent from vb", test_sent_from_vb, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap agg stats", test_tap_agg_stats, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap takeover (with concurrent mutations)", test_tap_takeover,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap filter stream", test_tap_filter_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3", prepare, cleanup),
        TestCase("tap default config", test_tap_default_config,
                 test_setup, teardown, NULL , prepare, cleanup),
        TestCase("tap config", test_tap_config, test_setup,
                 teardown,
                 "tap_backoff_period=0.05;tap_ack_interval=10;tap_ack_window_size=2;tap_ack_grace_period=10",
                 prepare, cleanup),
        TestCase("tap acks stream", test_tap_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;chk_max_items=500",
                 prepare, cleanup),
        TestCase("tap implicit acks stream", test_tap_implicit_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;tap_ack_initial_sequence_number=4294967290;chk_max_items=500",
                 prepare, cleanup),
        TestCase("tap notify", test_tap_notify, test_setup,
                 teardown, "max_size=1048576", prepare, cleanup),

        // restart tests
        TestCase("test restart", test_restart, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test restart with session stats", test_restart_session_stats, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set+get+restart+hit (bin)", test_restart_bin_val,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("flush+restart", test_flush_restart, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("flush multiv+restart", test_flush_multiv_restart,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test kill -9 bucket", test_kill9_bucket,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test shutdown with force", test_flush_shutdown_force,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test shutdown without force", test_flush_shutdown_noforce,
                 test_setup, teardown, NULL, prepare, cleanup),

        // it takes 61+ second to finish the following test.
        //TestCase("continue warmup after loading access log",
        //         test_warmup_accesslog,
        //         test_setup, teardown,
        //         "warmup_min_items_threshold=75;alog_path=/tmp/epaccess.log;"
        //         "alog_task_time=0;alog_sleep_time=1",
        //         prepare, cleanup),

        // disk>RAM tests
        TestCase("disk>RAM golden path", test_disk_gt_ram_golden,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup),
        TestCase("disk>RAM paged-out rm", test_disk_gt_ram_paged_rm,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup),
        TestCase("disk>RAM update paged-out", test_disk_gt_ram_update_paged_out,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("disk>RAM delete paged-out", test_disk_gt_ram_delete_paged_out,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("disk>RAM paged-out incr", test_disk_gt_ram_incr,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("disk>RAM set bgfetch race", test_disk_gt_ram_set_race,
                 test_setup, teardown, NULL, prepare, cleanup, true),
        TestCase("disk>RAM incr bgfetch race", test_disk_gt_ram_incr_race,
                 test_setup, teardown, NULL, prepare, cleanup, true),
        TestCase("disk>RAM delete bgfetch race", test_disk_gt_ram_rm_race,
                 test_setup, teardown, NULL, prepare, cleanup, true),
        // disk>RAM tests with WAL
        TestCase("disk>RAM golden path (wal)", test_disk_gt_ram_golden,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup),
        TestCase("disk>RAM paged-out rm (wal)", test_disk_gt_ram_paged_rm,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup),
        TestCase("disk>RAM update paged-out (wal)",
                 test_disk_gt_ram_update_paged_out, test_setup,
                 teardown, MULTI_DISPATCHER_CONFIG, prepare, cleanup),
        TestCase("disk>RAM delete paged-out (wal)",
                 test_disk_gt_ram_delete_paged_out, test_setup,
                 teardown, MULTI_DISPATCHER_CONFIG, prepare, cleanup),
        TestCase("disk>RAM paged-out incr (wal)", test_disk_gt_ram_incr,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup),
        TestCase("disk>RAM incr bgfetch race (wal)", test_disk_gt_ram_incr_race,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, true),
        // vbucket negative tests
        TestCase("vbucket incr (dead)", test_wrong_vb_incr,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket incr (pending)", test_vb_incr_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket incr (replica)", test_vb_incr_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket get (dead)", test_wrong_vb_get,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket get (pending)", test_vb_get_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket get (replica)", test_vb_get_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket set (dead)", test_wrong_vb_set,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket set (pending)", test_vb_set_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket set (replica)", test_vb_set_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket replace (dead)", test_wrong_vb_replace,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket replace (pending)", test_vb_replace_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket replace (replica)", test_vb_replace_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket add (dead)", test_wrong_vb_add,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket add (pending)", test_vb_add_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket add (replica)", test_vb_add_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket cas (dead)", test_wrong_vb_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket cas (pending)", test_vb_cas_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket cas (replica)", test_vb_cas_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket append (dead)", test_wrong_vb_append,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket append (pending)", test_vb_append_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket append (replica)", test_vb_append_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket prepend (dead)", test_wrong_vb_prepend,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket prepend (pending)", test_vb_prepend_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket prepend (replica)", test_vb_prepend_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket del (dead)", test_wrong_vb_del,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket del (pending)", test_vb_del_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket del (replica)", test_vb_del_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test vbucket get", test_vbucket_get, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test vbucket get missing", test_vbucket_get_miss,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test vbucket create", test_vbucket_create,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test vbucket compact", test_vbucket_compact,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test compaction config", test_compaction_config,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test multiple vb compactions", test_multiple_vb_compactions,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test multiple vb compactions with workload",
                 test_multi_vb_compactions_with_workload,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test async vbucket destroy", test_async_vbucket_destroy,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test sync vbucket destroy", test_sync_vbucket_destroy,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test async vbucket destroy (multitable)", test_async_vbucket_destroy,
                 test_setup, teardown,
                 "max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup),
        TestCase("test sync vbucket destroy (multitable)", test_sync_vbucket_destroy,
                 test_setup, teardown,
                 "max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup),
        TestCase("test vbucket destroy stats", test_vbucket_destroy_stats,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60",
                 prepare, cleanup),
        TestCase("test async vbucket destroy restart",
                 test_async_vbucket_destroy_restart, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test sync vbucket destroy restart",
                 test_sync_vbucket_destroy_restart, test_setup, teardown, NULL,
                 prepare, cleanup),

        // stats uuid
        TestCase("test stats uuid", test_uuid_stats, test_setup, teardown,
                 "uuid=foobar", prepare, cleanup),


        // checkpoint tests
        TestCase("checkpoint: create a new checkpoint",
                 test_create_new_checkpoint,
                 test_setup, teardown,
                 "chk_max_items=500;item_num_based_new_chk=true",
                 prepare, cleanup),
        TestCase("checkpoint: validate checkpoint config params",
                 test_validate_checkpoint_params,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test checkpoint create", test_checkpoint_create,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup),
        TestCase("test checkpoint timeout", test_checkpoint_timeout,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup),
        TestCase("test checkpoint deduplication", test_checkpoint_deduplication,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup),
        TestCase("checkpoint: collapse checkpoints",
                 test_collapse_checkpoints,
                 test_setup, teardown,
                 "chk_max_items=500;max_checkpoints=5;chk_remover_stime=1;"
                 "enable_chk_merge=true",
                 prepare, cleanup),
        TestCase("checkpoint: wait for persistence",
                 test_checkpoint_persistence,
                 test_setup, teardown,
                 "chk_max_items=500;max_checkpoints=5;item_num_based_new_chk=true",
                 prepare, cleanup),
        TestCase("test wait for persist vb del", test_wait_for_persist_vb_del,
                 test_setup, teardown, NULL, prepare, cleanup),

        // revision id's
        TestCase("revision sequence numbers", test_revid, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("mb-4314", test_regression_mb4314, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("mb-3466", test_mb3466, test_setup,
                 teardown, NULL, prepare, cleanup),

        // XDCR unit tests
        TestCase("get meta", test_get_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("get meta with extras", test_get_meta_with_extras,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta deleted", test_get_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta nonexistent", test_get_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by get", test_get_meta_with_get,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by set", test_get_meta_with_set,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by delete", test_get_meta_with_delete,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("add with meta", test_add_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete with meta", test_delete_with_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete with meta deleted", test_delete_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete with meta nonexistent",
                 test_delete_with_meta_nonexistent, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete with meta nonexistent no temp",
                 test_delete_with_meta_nonexistent_no_temp, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete_with_meta race with concurrent delete",
                 test_delete_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete_with_meta race with concurrent set",
                 test_delete_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set with meta", test_set_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set with meta by force", test_set_with_meta_by_force,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set with meta deleted", test_set_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set with meta nonexistent", test_set_with_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set_with_meta race with concurrent set",
                 test_set_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set_with_meta race with concurrent delete",
                 test_set_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test set_with_meta exp persisted", test_exp_persisted_set_del,
                 test_setup, teardown, "exp_pager_stime=3", prepare, cleanup),
        TestCase("test del meta conflict resolution",
                 test_del_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test add meta conflict resolution",
                 test_add_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test set meta conflict resolution",
                 test_set_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test del meta lww conflict resolution",
                 test_del_meta_lww_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test set meta lww conflict resolution",
                 test_set_meta_lww_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("temp item deletion", test_temp_item_deletion,
                 test_setup, teardown,
                 "exp_pager_stime=3", prepare, cleanup),
        TestCase("test estimate vb move", test_est_vb_move,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test getAdjustedTime, setDriftCounter apis",
                 test_adjusted_time_apis, test_setup, teardown, NULL,
                 prepare, cleanup),

        // Data traffic control tests
        TestCase("control data traffic", test_control_data_traffic,
                 test_setup, teardown, NULL, prepare, cleanup),

        // Transaction tests
        TestCase("multiple transactions", test_multiple_transactions,
                 test_setup, teardown, NULL, prepare, cleanup),

        // Returning meta tests
        TestCase("test set ret meta", test_set_ret_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test set ret meta error", test_set_ret_meta_error,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test add ret meta", test_add_ret_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test add ret meta error", test_add_ret_meta_error,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test del ret meta", test_del_ret_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test del ret meta error", test_del_ret_meta_error,
                 test_setup, teardown, NULL, prepare, cleanup),

        // DCP testcases
        TestCase("test dcp vbtakeover stat no stream",
                 test_dcp_vbtakeover_no_stream, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test dcp notifier", test_dcp_notifier, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test open consumer", test_dcp_consumer_open,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp consumer flow control none",
                 test_dcp_consumer_flow_control_none,
                 test_setup, teardown, "dcp_flow_control_policy=none",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control static",
                 test_dcp_consumer_flow_control_static,
                 test_setup, teardown, "dcp_flow_control_policy=static",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control dynamic",
                 test_dcp_consumer_flow_control_dynamic,
                 test_setup, teardown, "dcp_flow_control_policy=dynamic",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control aggressive",
                 test_dcp_consumer_flow_control_aggressive,
                 test_setup, teardown, "dcp_flow_control_policy=aggressive",
                 prepare, cleanup),
        TestCase("test open producer", test_dcp_producer_open,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp noop", test_dcp_noop, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test dcp noop failure", test_dcp_noop_fail, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test dcp consumer noop", test_dcp_consumer_noop, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test producer stream request (partial)",
                 test_dcp_producer_stream_req_partial, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request with time sync (partial)",
                 test_dcp_producer_stream_req_partial_with_time_sync,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (full)",
                 test_dcp_producer_stream_req_full, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk)",
                 test_dcp_producer_stream_req_disk, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk only)",
                 test_dcp_producer_stream_req_diskonly, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (memory only)",
                 test_dcp_producer_stream_req_mem, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (DGM)",
                 test_dcp_producer_stream_req_dgm, test_setup, teardown,
                 "chk_remover_stime=1;max_size=2621440", prepare, cleanup),
        TestCase("test producer stream request (latest flag)",
                 test_dcp_producer_stream_latest, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test producer stream request nmvb",
                 test_dcp_producer_stream_req_nmvb, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test dcp agg stats",
                 test_dcp_agg_stats, test_setup, teardown, "chk_max_items=100",
                 prepare, cleanup),
        TestCase("test dcp cursor dropping",
                 test_dcp_cursor_dropping, test_setup, teardown,
                 "cursor_dropping_lower_mark=60;cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;max_size=26214400", prepare, cleanup),
        TestCase("test dcp value compression",
                 test_dcp_value_compression, test_setup, teardown,
                 "dcp_value_compression_enabled=true",
                 prepare, cleanup),
        TestCase("test producer stream request backfill no value",
                 test_dcp_producer_stream_backfill_no_value, test_setup,
                 teardown, "chk_remover_stime=1;max_size=2621440", prepare,
                 cleanup),
        TestCase("test producer stream request mem no value",
                 test_dcp_producer_stream_mem_no_value, test_setup, teardown,
                 "chk_remover_stime=1;max_size=2621440", prepare, cleanup),
        TestCase("test dcp stream takeover", test_dcp_takeover, test_setup,
                teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp stream takeover no items", test_dcp_takeover_no_items,
                 test_setup, teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp consumer takeover", test_dcp_consumer_takeover,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test failover scenario with dcp",
                 test_failover_scenario_with_dcp, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test add stream", test_dcp_add_stream, test_setup, teardown,
                 "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test consumer backoff stat", test_consumer_backoff_stat,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp reconnect full snapshot", test_dcp_reconnect_full,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test reconnect partial snapshot", test_dcp_reconnect_partial,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test crash full snapshot", test_dcp_crash_reconnect_full,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test crash partial snapshot",
                 test_dcp_crash_reconnect_partial, test_setup, teardown,
                 "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test rollback to zero on consumer", test_rollback_to_zero,
                test_setup, teardown, "dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test chk manager rollback", test_chk_manager_rollback,
                test_setup, teardown,
                 "dcp_flow_control_policy=none;dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test full rollback on consumer", test_fullrollback_for_consumer,
                test_setup, teardown,
                "dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test partial rollback on consumer",
                test_partialrollback_for_consumer, test_setup, teardown,
                "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test change dcp buffer log size", test_dcp_buffer_log_size,
                test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test get failover log", test_dcp_get_failover_log,
                test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test add stream exists", test_dcp_add_stream_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream nmvb", test_dcp_add_stream_nmvb, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test add stream prod exists", test_dcp_add_stream_prod_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream prod nmvb", test_dcp_add_stream_prod_nmvb,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test close stream (no stream)",
                 test_dcp_close_stream_no_stream, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test close stream", test_dcp_close_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test dcp consumer end stream", test_dcp_consumer_end_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("dcp consumer mutate", test_dcp_consumer_mutate, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer mutate with time sync",
                 test_dcp_consumer_mutate_with_time_sync, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete", test_dcp_consumer_delete, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete with time sync",
                 test_dcp_consumer_delete_with_time_sync, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp failover log", test_failover_log_dcp, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp persistence seqno", test_dcp_persistence_seqno, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp last items purged", test_dcp_last_items_purged, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp rollback after purge", test_dcp_rollback_after_purge,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp erroneous mutations scenario", test_dcp_erroneous_mutations,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp erroneous snapshot marker scenario", test_dcp_erroneous_marker,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp invalid mutation(s)/deletion(s)",
                 test_dcp_invalid_mutation_deletion,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp invalid snapshot marker",
                 test_dcp_invalid_snapshot_marker,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase("test set with item_eviction",
                 test_set_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test set_with_meta with item_eviction",
                 test_setWithMeta_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test multiple set and del with meta with item_eviction",
                 test_multiple_set_delete_with_metas_full_eviction,
                 test_setup, teardown,
                 "item_eviction_policy=full_eviction",
                 prepare, cleanup),
        TestCase("test add with item_eviction",
                 test_add_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test replace with eviction", test_replace_with_eviction,
                 test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test get_meta with item_eviction",
                 test_getMeta_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test get_and_touch with item_eviction",
                 test_gat_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test key_stats with item_eviction",
                 test_keyStats_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test del with item_eviction",
                 test_del_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test del_with_meta with item_eviction",
                 test_delWithMeta_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test observe with item_eviction",
                 test_observe_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test expired item with item_eviction",
                 test_expired_item_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test stats curr_items with item_eviction",
                 test_curr_items, test_setup, teardown,
                 "item_eviction_policy=full_eviction;flushall_enabled=true", prepare, cleanup),
        TestCase("warmup stats", test_warmup_stats, test_setup,
                 teardown, "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test get & delete on non existent items",
                 test_non_existent_get_and_delete, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),
        TestCase("test MB-16421", test_mb16421,
                 test_setup, teardown, "item_eviction_policy=full_eviction",
                 prepare, cleanup),

        TestCase("test get random key", test_get_random_key,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase("test failover log behavior", test_failover_log_behavior,
                 test_setup, teardown, NULL, prepare, cleanup),

#if defined(HAVE_JEMALLOC)
        TestCase("test defragmenter", test_defragmenter,
                 test_setup, teardown,
                 "defragmenter_interval=9999"
                 ";defragmenter_age_threshold=0"
                 ";defragmenter_chunk_duration=99999"
                 ";defragmenter_enabled=false",
                 prepare, cleanup),
#endif

        TestCase("test hlc cas", test_hlc_cas, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test get all vb seqnos", test_get_all_vb_seqnos, test_setup,
                 teardown, NULL, prepare, cleanup),

        TestCase("test MB-16357", test_mb16357,
                 test_setup, teardown, "compaction_exp_mem_threshold=85",
                 prepare, cleanup),

        TestCaseV2("multi_bucket set/get ", test_multi_bucket_set_get, NULL,
                   teardown_v2, NULL, prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
