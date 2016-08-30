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
#include "ep_test_apis.h"

#include "ep_testsuite_common.h"
#include "locks.h"
#include <libcouchstore/couch_db.h>
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
    checkeq(format_type, recv_format_type, "Wrong format type in result");
    memcpy(&recv_vb_id, last_body.data() + 1, sizeof(uint16_t));
    checkeq(vb_id, ntohs(recv_vb_id), "Wrong vbucket id in result");
    memcpy(&recv_vb_uuid, last_body.data() + 3, sizeof(uint64_t));
    checkeq(vb_uuid, ntohll(recv_vb_uuid), "Wrong vbucket uuid in result");
    memcpy(&recv_last_persisted_seqno, last_body.data() + 11, sizeof(uint64_t));
    checkeq(last_persisted_seqno, ntohll(recv_last_persisted_seqno),
            "Wrong persisted seqno in result");
    memcpy(&recv_current_seqno, last_body.data() + 19, sizeof(uint64_t));
    checkeq(current_seqno, ntohll(recv_current_seqno), "Wrong current seqno in result");

    if (failover) {
        memcpy(&recv_failover_vbuuid, last_body.data() + 27, sizeof(uint64_t));
        checkeq(failover_vbuuid, ntohll(recv_failover_vbuuid),
                "Wrong failover uuid in result");
        memcpy(&recv_failover_seqno, last_body.data() + 35, sizeof(uint64_t));
        checkeq(failover_seqno, ntohll(recv_failover_seqno),
                "Wrong failover seqno in result");
    }
}

static enum test_result test_replace_with_eviction(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i),
            "Failed to set value.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key");
    int numBgFetched = get_int_stat(h, h1, "ep_bg_fetched");

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue1", &i),
            "Failed to replace existing value.");

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "full_eviction") {
        numBgFetched++;
    }

    checkeq(numBgFetched,
            get_int_stat(h, h1, "ep_bg_fetched"),
            "Bg fetched value didn't match");

    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue1", 10);
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
    checkeq(ENGINE_NOT_MY_VBUCKET,
            store(h, h1, NULL, op, "key", "somevalue", &i, cas, 1),
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending),
          "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_pending),
          "Bucket state was not set to pending.");
    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // Add operation with cas != 0 doesn't make sense..
        cas = 0;
    }
    checkeq(ENGINE_EWOULDBLOCK,
            store(h, h1, cookie, op, "key", "somevalue", &i, cas, 1),
            "Expected ewouldblock");
    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Bucket state was not set to replica.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");

    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // performing add with a CAS != 0 doesn't make sense...
        cas = 0;
    }
    checkeq(ENGINE_NOT_MY_VBUCKET,
            store(h, h1, NULL, op, "key", "somevalue", &i, cas, 1),
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

static int checkCurrItemsAfterShutdown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                       int numItems2Load, bool shutdownForce) {
    std::vector<std::string> keys;
    for (int index = 0; index < numItems2Load; ++index) {
        std::stringstream s;
        s << "keys_2_load-" << index;
        std::string key(s.str());
        keys.push_back(key);
    }

    checkeq(0, get_int_stat(h, h1, "ep_total_persisted"),
            "Expected ep_total_persisted equals 0");
    checkeq(0, get_int_stat(h, h1, "curr_items"),
            "Expected curr_items equals 0");

    // stop flusher before loading new items
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "CMD_STOP_PERSISTENCE failed!");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(),
            "Failed to stop persistence!");
    free(pkt);

    std::vector<std::string>::iterator itr;
    for (itr = keys.begin(); itr != keys.end(); ++itr) {
        item *i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, itr->c_str(), "oracle", &i, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    checkeq(0, get_int_stat(h, h1, "ep_total_persisted"),
            "Incorrect ep_total_persisted, expected 0");
    std::stringstream ss;
    ss << "Incorrect curr_items, expected " << numItems2Load;
    std::string errmsg(ss.str());
    checkeq(numItems2Load, get_int_stat(h, h1, "curr_items"),
            errmsg.c_str());

    // resume flusher before shutdown + warmup
    pkt = createPacket(PROTOCOL_BINARY_CMD_START_PERSISTENCE);
    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt, add_response),
            "CMD_START_PERSISTENCE failed!");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, "key", 0, 0),
            "Failed to fail initial delete.");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
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
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL, 0),
            "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i),
            "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key2", "somevalue", 9);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i),
            "Failed post-flush, post-restart set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"),
            "Expected missing key");
    return SUCCESS;
}

static enum test_result test_shutdown_snapshot_range(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const int num_items = 100;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    int end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    /* wait for a new open checkpoint with just chk start meta item */
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");

    /* change vb state to replica before restarting (as it happens in graceful
       failover)*/
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed set vbucket 0 to replica state.");

    /* trigger persist vb state task */
    check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "vb_state_persist_run", "0"),
          "Failed to trigger vb state persist");

    /* restart the engine */
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    /* Check if snapshot range is persisted correctly */
    checkeq(end, get_int_stat(h, h1, "vb_0:last_persisted_snap_start",
                              "vbucket-seqno"),
            "Wrong snapshot start persisted");
    checkeq(end, get_int_stat(h, h1, "vb_0:last_persisted_snap_end",
                                    "vbucket-seqno"),
            "Wrong snapshot end persisted");

    return SUCCESS;
}

static enum test_result test_flush_multiv_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active),
          "Failed to set vbucket state.");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i, 0, 2),
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
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL, 0),
            "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value again, should not be there.
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    check(verify_vbucket_missing(h, h1, 2), "Bucket 2 came back.");
    return SUCCESS;
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET, "key", val, &i);
    checkeq(ENGINE_SUCCESS, ret, "Failed set.");
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
    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    cookie = createTapConn(h, h1, "tap_client_thread");

    checkeq(ENGINE_SUCCESS, h1->get_stats(h, NULL, "tap", 3, add_stats),
            "Failed to get stats.");
    std::string val = vals["eq_tapq:tap_client_thread:backfill_completed"];
    checkeq(0, strcmp(val.c_str(), "true"), "Don't expect the backfill upon restart");
    testHarness.unlock_cookie(cookie);
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

static enum test_result test_restart_bin_val(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {



    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                         binaryData, sizeof(binaryData), 82758, &i, 0, 0),
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

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    checkeq(ENGINE_NOT_MY_VBUCKET, verify_key(h, h1, "key", 1),
            "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_get_pending(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending),
          "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    item *i = NULL;
    checkeq(ENGINE_EWOULDBLOCK,
            h1->get(h, cookie, &i, "key", strlen("key"), 1),
            "Expected woodblock.");
    h1->release(h, NULL, i);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    checkeq(ENGINE_NOT_MY_VBUCKET,
            verify_key(h, h1, "key", 1),
            "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t result;
    item *i = NULL;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                           &i, PROTOCOL_BINARY_RAW_BYTES, &result, 1),
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending),
          "Failed to set vbucket state.");
    checkeq(ENGINE_EWOULDBLOCK,
            h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                           &i, PROTOCOL_BINARY_RAW_BYTES, &result, 1),
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
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                           &i, PROTOCOL_BINARY_RAW_BYTES, &result, 1),
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
    checkeq(ENGINE_NOT_MY_VBUCKET, del(h, h1, "key", 0, 1),
            "Expected wrong bucket.");
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
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(5);
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &it, key, strlen(key), 0),
            "Item didn't expire");

    int expired_access = get_int_stat(h, h1, "ep_expired_access");
    int expired_pager = get_int_stat(h, h1, "ep_expired_pager");
    int active_expired = get_int_stat(h, h1, "vb_active_expired");
    checkeq(0, expired_pager, "Expected zero expired item by pager");
    checkeq(1, expired_access, "Expected an expired item on access");
    checkeq(1, active_expired, "Expected an expired active item");
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
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);

    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &it, key, strlen(key), 0),
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

    checkeq(0, get_int_stat(h, h1, "vb_0:persistence:num_visits",
                            "checkpoint"), "Cursor moved before item load");

    for (int i = 0; i < 50; i++) {
        item *itm = NULL;
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "somevalue", &itm, 0, 0, 10,
                      PROTOCOL_BINARY_RAW_BYTES),
                "Set failed.");
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    checkeq(50, get_int_stat(h, h1, "curr_items"),
            "Unexpected number of items on database");
    check(0 < get_int_stat(h, h1, "vb_0:persistence:num_visits", "checkpoint"),
          "Cursor not moved even after flusher runs");

    testHarness.time_travel(15);

    // Compaction on VBucket
    compact_db(h, h1, 0, 0, 0, 0, 0);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    checkeq(50, get_int_stat(h, h1, "ep_expired_compactor"),
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
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 10,
                      PROTOCOL_BINARY_RAW_BYTES);
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    checkeq(1, get_int_stat(h, h1, "curr_items"), "Failed store item");
    testHarness.time_travel(15);

    checkeq(pager_runs, get_int_stat(h, h1, "ep_num_expiry_pager_runs"),
            "Expiry pager shouldn't have run during this time");

    // Restart the engine to ensure the above item is expired
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    check(get_bool_stat(h, h1, "ep_exp_pager_enabled"),
          "Expiry pager should be enabled on warmup");

    // Wait for the expiry pager to run and expire our item.
    wait_for_stat_to_be_gte(h, h1, "ep_expired_pager", 1, nullptr, /*secs*/10);

    // Note: previously we checked that curr_items was zero here (immediately
    // after waiting for ep_expired_pager == 1), however we cannot assume that
    // - items are actually expired asynchronously.
    // See EPStore::deleteExpiredItem - for non-temporary, expired items we
    // call unlocked_softDelete (soft-marking the item as deleted in the
    // hashtable), and then call queueDirty to queue a deletion, and then
    // increment the expired stat. Only when that delete is actually persisted
    // and the deleted callback is invoked -
    // PeristenceCallback::callback(int&) - is curr_items finally decremented.
    // Therefore we need to wait for the flusher to settle (i.e. delete
    // callback to be called) for the curr_items stat to be accurate.
    wait_for_flusher_to_settle(h, h1);

    checkeq(0, get_int_stat(h, h1, "curr_items"),
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
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Advance the ep_engine time by 10 sec for the above item to be expired.
    testHarness.time_travel(10);
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &it, key, strlen(key), 0),
            "Item didn't expire");

    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    cas = 0;
    // Add a new item with the same key.
    rv = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    checkeq(ENGINE_SUCCESS, rv, "Add failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS,
            h1->get(h, NULL, &it, key, strlen(key), 0),
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
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Add a new item with the same key and 2 sec of expiration.
    const char *new_data = "new data here.";
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(new_data), 0, 2,
                      PROTOCOL_BINARY_RAW_BYTES);
    checkeq(ENGINE_SUCCESS, rv, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, new_data, strlen(new_data));

    int pager_runs = get_int_stat(h, h1, "ep_num_expiry_pager_runs");
    cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    checkeq(ENGINE_SUCCESS, rv, "Set failed.");
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
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Get Replica Failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
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
    checkeq(ENGINE_EWOULDBLOCK,
            h1->unknown_command(h, cookie, pkt, add_response),
            "Should have returned error for pending state");
    testHarness.destroy_cookie(cookie);
    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica_dead_state(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_dead);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Get Replica Failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
            "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");

    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Get Replica Failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected PROTOCOL_BINARY_RESPONSE_SUCCESS response.");
    checkeq(std::string("replicadata"), last_body,
            "Should have returned identical value");

    free(pkt);
    return SUCCESS;
}

static enum test_result test_get_replica_non_resident(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value", &i, 0, 0),
            "Store Failed");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);

    evict_key(h, h1, "key", 0, "Ejected.");
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket to replica");

    get_replica(h, h1, "key", 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");

    return SUCCESS;
}

static enum test_result test_get_replica_invalid_key(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    bool makeinvalidkey = true;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica, makeinvalidkey);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Get Replica Failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
            "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");
    free(pkt);
    return SUCCESS;
}

static enum test_result test_vb_del_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending),
          "Failed to set vbucket state.");
    checkeq(ENGINE_EWOULDBLOCK, del(h, h1, "key", 0, 1, cookie),
            "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    checkeq(ENGINE_NOT_MY_VBUCKET, del(h, h1, "key", 0, 1),
            "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
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

static enum test_result test_takeover_stats_race_with_vb_create(ENGINE_HANDLE *h,
                                                                ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state information");

    int del_items = get_int_stat(h, h1, "on_disk_deletes", "dcp-vbtakeover 1");

    checkeq(0, del_items, "Invalid number of on-disk deletes");

    del_items = get_int_stat(h, h1, "on_disk_deletes", "tap-vbtakeover 1");

    checkeq(0, del_items, "Invalid number of on-disk deletes");

    return SUCCESS;
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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, value, &itm),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, key, value, strlen(value));

    // Set a non-expiring key...
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "trees", "cleanse", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "trees", "cleanse", strlen("cleanse"));

    touch(h, h1, key, 0, 11);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "touch Carss");

    testHarness.time_travel(12);
    wait_for_flusher_to_settle(h, h1);

    // Store a dummy item since we do not purge the item with highest seqno
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "dummykey", "dummyvalue", &itm,
                  0, 0, 0),
            "Error setting.");
    h1->release(h, NULL, itm);

    wait_for_flusher_to_settle(h, h1);

    checkeq(0, get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno not found to be zero before compaction");

    // Compaction on VBucket
    compact_db(h, h1, 0, 0, 2, 3, 1);

    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);

    // the key tree and its value should be intact...
    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "trees"),
            "key trees should be found.");
    // the key Carrs should have disappeared...
    ENGINE_ERROR_CODE val = verify_key(h, h1, "Carss");
    checkeq(ENGINE_KEY_ENOENT, val, "Key Carss has not expired.");

    checkeq(4, get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    return SUCCESS;
}

static enum test_result test_compaction_config(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    checkeq(10000,
            get_int_stat(h, h1, "ep_compaction_write_queue_cap"),
            "Expected compaction queue cap to be 10000");
    set_param(h, h1, protocol_binary_engine_param_flush,
              "compaction_write_queue_cap", "100000");
    checkeq(100000, get_int_stat(h, h1, "ep_compaction_write_queue_cap"),
            "Expected compaction queue cap to be 100000");
    return SUCCESS;
}

struct comp_thread_ctx {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    uint16_t vbid;
    uint16_t db_file_id;
};

extern "C" {
    static void compaction_thread(void *arg) {
        struct comp_thread_ctx *ctx = static_cast<comp_thread_ctx *>(arg);
        compact_db(ctx->h, ctx->h1, ctx->vbid, ctx->db_file_id, 0, 0, 0);
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, vbid),
                "Failed to store a value");
        h1->release(h, NULL, i);
        ++count;
    }

    // Compact multiple vbuckets.
    const int n_threads = 4;
    cb_thread_t threads[n_threads];
    struct comp_thread_ctx ctx[n_threads];

    const int num_shards = get_int_stat(h, h1, "ep_workload:num_shards",
                                        "workload");

    for (int i = 0; i < n_threads; i++) {
        ctx[i].h = h;
        ctx[i].h1 = h1;
        ctx[i].vbid = static_cast<uint16_t>(i);
        ctx[i].db_file_id = ctx[i].vbid % num_shards;
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                      &i, 0, vbid),
                "Failed to store a value");
        h1->release(h, NULL, i);
        ++count;
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < 2; ++i) {
        count = 0;
        for (it = keys.begin(); it != keys.end(); ++it) {
            uint16_t vbid = count % 4;
            item *i = NULL;
            checkeq(ENGINE_SUCCESS,
                    h1->get(h, NULL, &i, it->c_str(), strlen(it->c_str()), vbid),
                    "Unable to get stored item");
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

    vbucketDelete(h, h1, 2, value);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
            last_status.load(),
            "Expected failure deleting non-existent bucket.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    vbucketDelete(h, h1, 1, value);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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

    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                      &i, 0, 1),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    int vbucketDel = get_int_stat(h, h1, "ep_vbucket_del");
    vbucketDelete(h, h1, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(),
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");

    // Store a value so the restart will try to resurrect it.
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 1),
            "Failed to set a value");
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state.");
    check_key_value(h, h1, "key", "somevalue", 9, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    vbucketDelete(h, h1, 1, value);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    checkeq(100, get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno"),
            "Invalid seqno");
    checkeq(100, get_int_stat(h, h1, "vb_0:last_persisted_seqno", "vbucket-seqno"),
            "Unexpected last_persisted_seqno");
    checkeq(0, get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno"),
            "Invalid seqno");
    checkeq(0, get_int_stat(h, h1, "vb_1:high_seqno", "vbucket-seqno 1"),
            "Invalid seqno");
    checkeq(0, get_int_stat(h, h1, "vb_1:last_persisted_seqno", "vbucket-seqno 1"),
            "Invalid last_persisted_seqno");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");

    auto seqno_stats = get_all_stats(h, h1, "vbucket-seqno 1");
    checkeq(vb_uuid, uint64_t(std::stoull(seqno_stats.at("vb_1:uuid"))),
            "Invalid uuid");

    checkeq(size_t(7), seqno_stats.size(), "Expected seven stats");

    // Check invalid vbucket
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->get_stats(h, NULL, "vbucket-seqno 2", 15, add_stats),
            "Expected not my vbucket");

    // Check bad vbucket parameter (not numeric)
    checkeq(ENGINE_EINVAL,
            h1->get_stats(h, NULL, "vbucket-seqno tt2", 17, add_stats),
            "Expected invalid");

    // Check extra spaces at the end
    checkeq(ENGINE_EINVAL,
            h1->get_stats(h, NULL, "vbucket-seqno    ", 17, add_stats),
            "Expected invalid");

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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 1),
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

    checkeq(ENGINE_EINVAL,
            h1->get_stats(h, NULL, "diskinfo ", 9, add_stats),
            "Expected invalid");

    checkeq(ENGINE_EINVAL,
            h1->get_stats(h, NULL, "diskinfo detai", 14, add_stats),
            "Expected invalid");

    checkeq(ENGINE_EINVAL,
            h1->get_stats(h, NULL, "diskinfo detaillll", 18, add_stats),
            "Expected invalid");

    return SUCCESS;
}

static enum test_result test_uuid_stats(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1)
{
    vals.clear();
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, "uuid", 4, add_stats),
            "Failed to get stats.");
    check(vals["uuid"] == "foobar", "Incorrect uuid");
    return SUCCESS;
}

static enum test_result test_item_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 0),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalueX", &i, 0, 0),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key1", "somevalueY", &i, 0, 0),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    check_key_value(h, h1, "key", "somevalueX", 10);
    check_key_value(h, h1, "key1", "somevalueY", 10);

    checkeq(ENGINE_SUCCESS, del(h, h1, "key1", 0, 0),
            "Failed remove with value.");
    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key1", "someothervalue", &i, 0, 0),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    check_key_value(h, h1, "key1", "someothervalue", 14);

    checkeq(3, get_int_stat(h, h1, "vb_active_ops_create"),
            "Expected 3 creations");
    checkeq(1, get_int_stat(h, h1, "vb_active_ops_update"),
            "Expected 1 updation");
    checkeq(1, get_int_stat(h, h1, "vb_active_ops_delete"),
            "Expected 1 deletion");

    return SUCCESS;
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
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
    int exp_read_bytes = 4, exp_write_bytes;
    std::string backend = get_str_stat(h, h1, "ep_backend");
    if (backend == "forestdb") {
        exp_write_bytes = 35; /* TBD: Do not hard code the value */
    } else if (backend == "couchdb") {
        exp_write_bytes = 23; /* TBD: Do not hard code the value */
    } else {
        return SKIPPED;
    }

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
          get_int_stat(h, h1, "rw_0:io_write_bytes", "kvstore") ==
                                                                exp_write_bytes,
          "Expected storing the key to update the write counter");
    evict_key(h, h1, "a", 0, "Ejected.");

    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    std::stringstream numReadStatStr;
    std::stringstream readBytesStatStr;

    if (backend == "couchdb") {
        numReadStatStr << "ro_" << 0 << ":io_num_read";
        readBytesStatStr << "ro_" << 0 << ":io_read_bytes";
    } else if (backend == "forestdb") {
        numReadStatStr << "rw_" << 0 << ":io_num_read";
        readBytesStatStr << "rw_" << 0 << ":io_read_bytes";
    } else {
        cb_assert(false);
    }

    check(get_int_stat(h, h1, numReadStatStr.str().c_str(), "kvstore") == 1 &&
              get_int_stat(h, h1, readBytesStatStr.str().c_str(), "kvstore") ==
                                                                 exp_read_bytes,
          "Expected reading the value back in to update the read counter");
    check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") == 1 &&
          get_int_stat(h, h1, "rw_0:io_write_bytes", "kvstore") ==
                                                                exp_write_bytes,
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
        checkeq(ENGINE_SUCCESS,
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

    auto stats = get_all_stats(h, h1);
    checkeq(1, std::stoi(stats.at("ep_bg_num_samples")),
               "Expected one sample");

    const char* bg_keys[] = { "ep_bg_min_wait",
                              "ep_bg_max_wait",
                              "ep_bg_wait_avg",
                              "ep_bg_min_load",
                              "ep_bg_max_load",
                              "ep_bg_load_avg"};
    for (const auto* key : bg_keys) {
        check(stats.find(key) != stats.end(),
              (std::string("Found no ") + key).c_str());
    }

    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, 0);
    check(get_int_stat(h, h1, "ep_bg_num_samples") == 2,
          "Expected one sample");

    h1->reset_stats(h, NULL);
    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"),
            "ep_bg_fetched is not reset to 0");
    return SUCCESS;
}

static enum test_result test_bg_meta_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    h1->reset_stats(h, NULL);

    wait_for_persisted_value(h, h1, "k1", "v1");
    wait_for_persisted_value(h, h1, "k2", "v2");

    evict_key(h, h1, "k1", 0, "Ejected.");
    checkeq(ENGINE_SUCCESS,
            del(h, h1, "k2", 0, 0), "Failed remove with value.");
    wait_for_stat_to_be(h, h1, "curr_items", 1);

    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 0");

    check(get_meta(h, h1, "k2"), "Get meta failed");
    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(1, get_int_stat(h, h1, "ep_bg_meta_fetched"), "Expected bg_meta_fetched to be 1");

    checkeq(ENGINE_SUCCESS, h1->get(h, NULL, &itm, "k1", 2, 0), "Missing key");
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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Set meta failed");

    check(get_meta(h, h1, "k2"), "Get meta failed");
    checkeq(1, get_int_stat(h, h1, "ep_bg_fetched"), "Expected bg_fetched to be 1");
    checkeq(1, get_int_stat(h, h1, "ep_bg_meta_fetched"),
            "Expected bg_meta_fetched to remain at 1");

    return SUCCESS;
}

static enum test_result test_key_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0),
            "Failed to store an item.");
    h1->release(h, NULL, i);
    // set (k2,v2) in vbucket 1
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1),
            "Failed to store an item.");
    h1->release(h, NULL, i);

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "key k2 1";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats),
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
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "vkey k2 1";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k3" and vbucket "2"
    const char *statkey3 = "vkey k3 2";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey3, strlen(statkey3), add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k4" and vbucket "3"
    const char *statkey4 = "vkey k4 3";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey4, strlen(statkey4), add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k5" and vbucket "4"
    const char *statkey5 = "vkey k5 4";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey5, strlen(statkey5), add_stats),
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
    checkeq(100, get_int_stat(h, h1, "ep_warmup_min_items_threshold"),
            "Incorrect initial warmup min items threshold.");
    checkeq(100, get_int_stat(h, h1, "ep_warmup_min_memory_threshold"),
            "Incorrect initial warmup min memory threshold.");

    check(!set_param(h, h1, protocol_binary_engine_param_flush,
                     "warmup_min_items_threshold", "a"),
          "Set warmup_min_items_threshold should have failed");
    check(!set_param(h, h1, protocol_binary_engine_param_flush,
                     "warmup_min_items_threshold", "a"),
          "Set warmup_min_memory_threshold should have failed");

    check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "warmup_min_items_threshold", "80"),
          "Set warmup_min_items_threshold should have worked");
    check(set_param(h, h1, protocol_binary_engine_param_flush,
                    "warmup_min_memory_threshold", "80"),
          "Set warmup_min_memory_threshold should have worked");

    checkeq(80, get_int_stat(h, h1, "ep_warmup_min_items_threshold"),
            "Incorrect smaller warmup min items threshold.");
    checkeq(80, get_int_stat(h, h1, "ep_warmup_min_memory_threshold"),
            "Incorrect smaller warmup min memory threshold.");

    item *it = NULL;
    for (int i = 0; i < 100; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(ENGINE_SUCCESS,
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

    const std::string eviction_policy = get_str_stat(h, h1, "ep_item_eviction_policy");
    if (eviction_policy == "value_only") {
        checkeq(100, get_int_stat(h, h1, "ep_warmup_key_count", "warmup"),
                "Expected 100 keys loaded after warmup");
    } else { // Full eviction mode
        checkeq(0, get_int_stat(h, h1, "ep_warmup_key_count", "warmup"),
                "Expected 0 keys loaded after warmup");
    }

    checkeq(0, get_int_stat(h, h1, "ep_warmup_value_count", "warmup"),
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

    // Ensure vbucket's bloom filter is enabled
    checkeq(std::string("ENABLED"),
            get_str_stat(h, h1, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    int i;
    item *it = NULL;

    // Insert 10 items.
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(ENGINE_SUCCESS,
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
        checkeq(ENGINE_SUCCESS,
                del(h, h1, key.str().c_str(), 0, 0),
                "Failed remove with value.");
    }
    wait_for_flusher_to_settle(h, h1);

    // Ensure that there are 5 non-resident items
    cb_assert(5 == get_int_stat(h, h1, "ep_num_non_resident"));
    cb_assert(5 == get_int_stat(h, h1, "curr_items"));

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    useconds_t sleepTime = 128;

    if (eviction_policy == "value_only") {  // VALUE-ONLY EVICTION MODE

        checkeq(5,
                get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                             "vbucket-details 0"),
                "Unexpected no. of keys in bloom filter");

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples"),
                "Expected bgFetch attempts to remain unchanged");

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, h1, key.str().c_str()), "Get meta failed");
        }

        // GetMeta would cause bgFetches as bloomfilter contains
        // the deleted items.
        checkeq(num_read_attempts + 5,
                get_int_stat(h, h1, "ep_bg_num_samples"),
                "Expected bgFetch attempts to increase by five");

        // Run compaction, with drop_deletes
        compact_db(h, h1, 0, 0, 15, 15, 1);
        while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, h1, key.str().c_str()), "Get meta failed");
        }
        checkeq(num_read_attempts + 5,
                get_int_stat(h, h1, "ep_bg_num_samples"),
                "Expected bgFetch attempts to stay as before");

    } else {                                // FULL EVICTION MODE

        checkeq(10,
                get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                             "vbucket-details 0"),
                "Unexpected no. of keys in bloom filter");


        // Because of issuing deletes on non-resident items
        checkeq(num_read_attempts + 5,
                get_int_stat(h, h1, "ep_bg_num_samples"),
                "Expected bgFetch attempts to increase by five, after deletes");

        // Run compaction, with drop_deletes, to exclude deleted items
        // from bloomfilter.
        compact_db(h, h1, 0, 0, 15, 15, 1);
        while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; i++) {
            std::stringstream key;
            key << "key-" << i;
            checkeq(ENGINE_KEY_ENOENT,
                    h1->get(h, NULL, &it, key.str().c_str(), key.str().length(), 0),
                    "Unable to get stored item");
        }
        // + 6 because last delete is not purged by the compactor
        checkeq(num_read_attempts + 6,
                get_int_stat(h, h1, "ep_bg_num_samples"),
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

    // Ensure vbucket's bloom filter is enabled
    checkeq(std::string("ENABLED"),
            get_str_stat(h, h1, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    for (int i = 0; i < 1000; i++) {
        std::stringstream key;
        key << "key-" << i;
        check(!get_meta(h, h1, key.str().c_str()),
                "Get meta should fail.");
    }

    checkeq(num_read_attempts,
            get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples"),
            "Expected no bgFetch attempts");

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
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

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples"),
                "Expected no bgFetch attempts");

        item *itm = NULL;
        // Add
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "add-" << j;

            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_ADD, key.str().c_str(),
                          "newvalue", &itm),
                    "Failed to add value again.");
            h1->release(h, NULL, itm);
        }

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples"),
                "Expected no bgFetch attempts");

        // Delete
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "del-" << j;
            checkeq(ENGINE_KEY_ENOENT,
                    del(h, h1, key.str().c_str(), 0, 0),
                    "Failed remove with value.");
        }

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, h1, 0, "ep_bg_num_samples"),
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

    // Ensure vbucket's bloom filter is enabled
    checkeq(std::string("ENABLED"),
            get_str_stat(h, h1, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &itm),
            "Failed to fail to store an item.");
    h1->release(h, NULL, itm);

    wait_for_flusher_to_settle(h, h1);
    int num_writes = get_int_stat(h, h1, "rw_0:io_num_write", "kvstore");
    int num_persisted = get_int_stat(h, h1, "ep_total_persisted");
    cb_assert(num_writes == 1 && num_persisted == 1);

    checkeq(ENGINE_SUCCESS,
            del(h, h1, "k1", 0, 0), "Failed remove with value.");
    stop_persistence(h, h1);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v2", &itm, 0, 0),
            "Failed to fail to store an item.");
    h1->release(h, NULL, itm);
    int key_count = get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                                 "vbucket-details 0");

    if (key_count == 0) {
        check(get_int_stat(h, h1, "rw_0:io_num_write", "kvstore") <= 2,
                "Unexpected number of writes");
        start_persistence(h, h1);
        wait_for_flusher_to_settle(h, h1);
        checkeq(0, get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                                "vbucket-details 0"),
                "Unexpected number of keys in bloomfilter");
    } else {
        cb_assert(key_count == 1);
        checkeq(2, get_int_stat(h, h1, "rw_0:io_num_write", "kvstore"),
                "Unexpected number of writes");
        start_persistence(h, h1);
        wait_for_flusher_to_settle(h, h1);
        checkeq(1, get_int_stat(h, h1, "vb_0:bloom_filter_key_count",
                                "vbucket-details 0"),
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
    checkeq(ENGINE_SUCCESS,
            storeCasOut(h, h1, NULL, 0, key, value, datatype, itm, cas),
            "Expected set to succeed");

    checkeq(ENGINE_SUCCESS,
            h1->get(h, cookie, &itm, key.c_str(), key.size(), 0),
            "Unable to get stored item");

    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, cookie, itm);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype, "Invalid datatype");

    const char* key1 = "foo";
    const char* val1 = "{\"foo1\":\"bar1\"}";
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = info.cas;
    itm_meta.exptime = info.exptime;
    itm_meta.flags = info.flags;
    set_with_meta(h, h1, key1, strlen(key1), val1, strlen(val1), 0, &itm_meta,
                  last_cas, false, info.datatype, false, 0, 0, cookie);

    checkeq(ENGINE_SUCCESS,
            h1->get(h, cookie, &itm, key1, strlen(key1), 0),
            "Unable to get stored item");

    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, cookie, itm);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype, "Invalid datatype, when setWithMeta");

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
    itm_meta.cas = 0x1;
    itm_meta.exptime = 0;
    itm_meta.flags = 0;

    //SET_WITH_META
    set_with_meta(h, h1, key, strlen(key), val, strlen(val), 0, &itm_meta,
                  0, false, datatype, false, 0, 0, cookie);

    checkeq(ENGINE_SUCCESS,
            h1->get(h, cookie, &itm, key, strlen(key), 0),
            "Unable to get stored item");

    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, cookie, itm, &info);
    h1->release(h, NULL, itm);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype, "Invalid datatype, when setWithMeta");

    //SET_RETURN_META
    set_ret_meta(h, h1, "foo1", 4, val, strlen(val), 0, 0, 0, 0, datatype,
                 cookie);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected set returing meta to succeed");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            last_datatype.load(), "Invalid datatype, when set_return_meta");

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
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "SET_VBUCKET command failed");
    free(pkt);
    cb_assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    cas = 0x0102030405060708;
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_VBUCKET, 0, cas, ext, 4);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "SET_VBUCKET command failed");
    free(pkt);
    cb_assert(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_access_scanner_settings(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string policy = vals.find("ep_item_eviction_policy")->second;
    std::string name = vals.find("ep_alog_path")->second;
    check(!name.empty(), "No access log path specified!");

    std::string oldparam(".log");
    std::string newparam(policy + oldparam);
    std::string config = testHarness.get_current_testcase()->cfg;
    std::string::size_type found = config.find(oldparam);
    if (found != config.npos) {
        config.replace(found, oldparam.size(), newparam);
    }

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);
    wait_for_warmup_complete(h, h1);

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

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string policy = vals.find("ep_item_eviction_policy")->second;
    std::string name = vals.find("ep_alog_path")->second;
    check(!name.empty(), "No access log path specified!");

    std::string oldparam(".log");
    std::string newparam(policy + oldparam);
    std::string config = testHarness.get_current_testcase()->cfg;
    std::string::size_type found = config.find(oldparam);
    if (found != config.npos) {
        config.replace(found, oldparam.size(), newparam);
    }

    /* We do not want the access scanner task to be running while we initiate it
       explicitly below. Hence set the alog_task_time to about 1 ~ 2 hours
       from now */
    time_t now = time(nullptr);
    struct tm* tm_now = gmtime(&now);

    set_param(h, h1, protocol_binary_engine_param_flush, "alog_task_time",
              (std::to_string(tm_now->tm_hour + 2)).c_str());
    wait_for_stat_to_be(h, h1, "ep_alog_task_time", tm_now->tm_hour + 2);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);
    wait_for_warmup_complete(h, h1);

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    name = vals.find("ep_alog_path")->second;

    /* Check access scanner is enabled */
    checkeq(true, get_bool_stat(h, h1, "ep_access_scanner_enabled"),
            "Access scanner task not enabled by default. Check test config");

    const int num_shards = get_int_stat(h, h1, "ep_workload:num_shards",
                                        "workload");
    int access_scanner_skips = 0, alog_runs = 0;
    name = name + ".0";
    std::string prev(name + ".old");

    /* Get the resident ratio down to below 90% */
    int num_items = 0;
    // Size chosen to create ~2000 items (i.e. 2x more than we sanity-check below)
    // with the given max_size for this test.
    const std::string value(2000, 'x');
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
                                      key.c_str(), value.c_str(), &itm);
        if (ret == ENGINE_SUCCESS) {
            num_items++;
        }
        h1->release(h, NULL, itm);
    }

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    if (num_items < 1000) {
        std::cerr << "Error: test_access_scanner: "
            "expected at least 1000 items after filling vbucket, "
            "but only have " << num_items << ". "
            "Check max_size setting for test." << std::endl;
        return FAIL;
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
        wait_for_stat_to_be_gte(h, h1, "ep_num_access_scanner_runs",
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

static enum test_result test_set_param_message(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, protocol_binary_engine_param_flush, "alog_task_time", "50");

    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
        "Expected an invalid value error for an out of bounds alog_task_time");
    check(std::string("Validation Error").compare(last_body), "Expected a "
            "validation error in the response body");
    return SUCCESS;
}

static enum test_result test_warmup_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed to set VB0 state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set VB1 state.");

    for (int i = 0; i < 5000; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.str().c_str(),
                      "somevalue", &it),
                "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);

    const auto warmup_stats = get_all_stats(h, h1, "warmup");

    // Check all expected warmup stats exists.
    const char* warmup_keys[] = { "ep_warmup_thread",
                                  "ep_warmup_value_count",
                                  "ep_warmup_key_count",
                                  "ep_warmup_dups",
                                  "ep_warmup_oom",
                                  "ep_warmup_time"};
    for (const auto* key : warmup_keys) {
        check(warmup_stats.find(key) != warmup_stats.end(),
              (std::string("Found no ") + key).c_str());
    }

    std::string warmup_time = warmup_stats.at("ep_warmup_time");
    cb_assert(std::stoi(warmup_time) > 0);

    const auto prev_vb_stats = get_all_stats(h, h1, "prev-vbucket");

    check(prev_vb_stats.find("vb_0") != prev_vb_stats.end(),
          "Found no previous state for VB0");
    check(prev_vb_stats.find("vb_1") != prev_vb_stats.end(),
          "Found no previous state for VB1");

    checkeq(std::string("active"), prev_vb_stats.at("vb_0"),
            "Unexpected stats for vb 0");
    checkeq(std::string("replica"), prev_vb_stats.at("vb_1"),
            "Unexpected stats for vb 1");

    const auto vb_details_stats = get_all_stats(h, h1, "vbucket-details");
    checkeq(5000, std::stoi(vb_details_stats.at("vb_0:num_items")),
            "Unexpected item count for vb 0");
    checkeq(0, std::stoi(vb_details_stats.at("vb_1:num_items")),
            "Unexpected item count for vb 1");

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
        checkeq(ENGINE_SUCCESS,
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

    checkeq(1,
            get_int_stat(h, h1, "ep_warmup_min_item_threshold", "warmup"),
            "Unable to set warmup_min_item_threshold to 1%");

    const std::string policy = get_str_stat(h, h1, "ep_item_eviction_policy");

    if (policy == "full_eviction") {
        checkeq(get_int_stat(h, h1, "ep_warmup_key_count", "warmup"),
                get_int_stat(h, h1, "ep_warmup_value_count", "warmup"),
                "Warmed up key count didn't match warmed up value count");
    } else {
        checkeq(10000, get_int_stat(h, h1, "ep_warmup_key_count", "warmup"),
                "Warmup didn't warmup all keys");
    }
    check(get_int_stat(h, h1, "ep_warmup_value_count", "warmup") <= 110,
            "Warmed up value count found to be greater than 1%");

    cb_assert(get_int_stat(h, h1, "ep_warmup_time", "warmup") > 0);

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
        checkeq(ENGINE_SUCCESS,
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
        checkeq(ENGINE_SUCCESS,
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
        checkeq(ENGINE_SUCCESS,
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

static enum test_result test_warmup_oom(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    write_items(h, h1, 20000, 0, "superlongnameofkey1234567890123456789012345678902");

    wait_for_flusher_to_settle(h, h1);

    std::string config(testHarness.get_current_testcase()->cfg);
    config = config + "max_size=2097152;item_eviction_policy=value_only";

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);

    wait_for_warmup_complete(h, h1);

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Failed to send data traffic command to the services");
    checkeq(PROTOCOL_BINARY_RESPONSE_ENOMEM, last_status.load(),
            "Data traffic command should have failed with enomem");
    free(pkt);

    return SUCCESS;
}

static enum test_result test_cbd_225(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // get engine startup token
    time_t token1 = get_int_stat(h, h1, "ep_startup_time");
    check(token1 != 0, "Expected non-zero startup token");

    // store some random data
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i),
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
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, "workload",
                          strlen("workload"), add_stats),
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
    checkeq(4, num_read_threads, "Incorrect number of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    checkeq(4, num_write_threads, "Incorrect number of writers");
    checkeq(1, num_auxio_threads, "Incorrect number of auxio threads");
    check(num_nonio_threads > 1 && num_nonio_threads <= 8,
          "Incorrect number of nonio threads");
    checkeq(4, max_read_threads, "Incorrect limit of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    checkeq(4, max_write_threads, "Incorrect limit of writers");
    checkeq(1, max_auxio_threads, "Incorrect limit of auxio threads");
    check(max_nonio_threads > 1 && max_nonio_threads <=8,
          "Incorrect limit of nonio threads");
    checkeq(5, num_shards, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_max_workload_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void* cookie = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, "workload",
                          strlen("workload"), add_stats),
            "Failed to get workload stats");
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
    checkeq(5, num_read_threads, "Incorrect number of readers");
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    checkeq(4, num_write_threads, "Incorrect number of writers");

    checkeq(1, num_auxio_threads, "Incorrect number of auxio threads");// config
    checkeq(4, num_nonio_threads, "Incorrect number of nonio threads");// config
    checkeq(5, max_read_threads, "Incorrect limit of readers");// derived
    // MB-12279: limiting max writers to 4 for DGM bgfetch performance
    checkeq(4, max_write_threads, "Incorrect limit of writers");// max-capped
    checkeq(1, max_auxio_threads, "Incorrect limit of auxio threads");// config
    checkeq(4, max_nonio_threads, "Incorrect limit of nonio threads");// config
    checkeq(5, num_shards, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_worker_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, "dispatcher",
                          strlen("dispatcher"), add_stats),
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

    checkeq(11, get_int_stat(h, h1, "ep_num_workers"), // cannot spawn less
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
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt1, add_response),
            "Failed to set cluster configuration");
    free(pkt1);

    protocol_binary_request_header *pkt2 =
        createPacket(PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, 1, 0, NULL, 0, NULL, 0, NULL, 0);
    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt2, add_response),
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
    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt1, add_response),
            "Failed to set cluster configuration");
    free(pkt1);

    protocol_binary_request_header *pkt2 =
        createPacket(PROTOCOL_BINARY_CMD_GET_VBUCKET, 1, 0, NULL, 0, NULL, 0, NULL, 0);
    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, pkt2,
                                                add_response);
    checkeq(ENGINE_SUCCESS, ret, "Should've received not_my_vbucket/cluster config");
    free(pkt2);
    if (last_body.compare(0, sizeof(var), reinterpret_cast<char*>(&var),
                          sizeof(var)) != 0) {
        return FAIL;
    } else {
        return SUCCESS;
    }
    check(verify_key(h, h1, "key", 2) == ENGINE_NOT_MY_VBUCKET, "Expected miss");
    checkeq(ENGINE_SUCCESS,
            h1->get_engine_vb_map(h, NULL, vb_map_response),
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
        createPacket(PROTOCOL_BINARY_CMD_GET_KEYS, 0, 0,
                     reinterpret_cast<char*>(&count),
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
        createPacket(PROTOCOL_BINARY_CMD_GET_KEYS, 1, 0,
                     reinterpret_cast<char*>(&count),
                     sizeof(count), key, strlen(key), NULL, 0, 0x00);

    stop_persistence(h, h1);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed set vbucket 1 state.");

    ENGINE_ERROR_CODE err = h1->unknown_command(h, NULL, pkt1,
                                                add_response);
    free(pkt1);
    start_persistence(h, h1);

    checkeq(ENGINE_SUCCESS, err,
            "Unexpected return code from all_keys_api");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Unexpected response status");

    return SUCCESS;
}

static enum test_result test_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // Verify initial case.
    verify_curr_items(h, h1, 0, "init");

    // Verify set and add case
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"k1", "v1", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 3, "three items stored");
    cb_assert(3 == get_int_stat(h, h1, "ep_total_enqueued"));

    wait_for_flusher_to_settle(h, h1);

    // Verify delete case.
    checkeq(ENGINE_SUCCESS, del(h, h1, "k1", 0, 0),
            "Failed remove with value.");

    wait_for_stat_change(h, h1, "curr_items", 3);
    verify_curr_items(h, h1, 2, "one item deleted - persisted");

    // Verify flush case (remove the two remaining from above)
    set_degraded_mode(h, h1, NULL, true);
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL, 0),
            "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);
    verify_curr_items(h, h1, 0, "flush");

    // Verify dead vbucket case.
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 0, "dead vbucket");
    checkeq(0, get_int_stat(h, h1, "curr_items_tot"),
            "Expected curr_items_tot to be 0 with a dead vbucket");

    // Then resurrect.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 3, "resurrected vbucket");

    // Now completely delete it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");
    vbucketDelete(h, h1, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success deleting vbucket.");
    verify_curr_items(h, h1, 0, "del vbucket");
    checkeq(0, get_int_stat(h, h1, "curr_items_tot"),
            "Expected curr_items_tot to be 0 after deleting a vbucket");

    return SUCCESS;
}

static enum test_result test_value_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    item *i = NULL;
    h1->reset_stats(h, NULL);
    checkeq(0, get_int_stat(h, h1, "ep_num_value_ejects"),
            "Expected reset stats to set ep_num_value_ejects to zero");
    checkeq(0, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected all items to be resident");
    checkeq(0, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected all active vbucket items to be resident");


    stop_persistence(h, h1);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k1", 0, "Can't eject: Dirty object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k2", 1, "Can't eject: Dirty object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1, "k1", 0, "Ejected.");
    evict_key(h, h1, "k2", 1, "Ejected.");

    checkeq(2, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected two non-resident items for active vbuckets");

    evict_key(h, h1, "k1", 0, "Already ejected.");
    evict_key(h, h1, "k2", 1, "Already ejected.");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_EVICT_KEY, 0, 0,
                                                       NULL, 0, "missing-key", 11);
    pkt->request.vbucket = htons(0);

    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Failed to evict key.");

    checkeq(ENGINE_SUCCESS, h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "value_only") {
        checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
                "expected the key to be missing...");
    } else {
        // Note that we simply return SUCCESS when EVICT_KEY is issued to
        // a non-resident or non-existent key with full eviction to avoid a disk lookup.
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "expected the success for evicting a non-existent key with full eviction");
    }
    free(pkt);

    h1->reset_stats(h, NULL);
    checkeq(0, get_int_stat(h, h1, "ep_num_value_ejects"),
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), "value", &i, 0, 1),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");
    int vb_del_num = get_int_stat(h, h1, "ep_vbucket_del");
    vbucketDelete(h, h1, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1),
                "Failed to store a value");
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
    checkeq(0, get_int_stat(h, h1, "ep_warmup_dups"),
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
    checkeq(ENGINE_SUCCESS,
            del(h, h1, "k1", 0, 0), "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    checkeq(overhead, get_int_stat(h, h1, "ep_overhead"),
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
    checkeq(ENGINE_SUCCESS,
            del(h, h1, "k1", 0, 0), "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    checkeq(overhead, get_int_stat(h, h1, "ep_overhead"),
            "Fell below initial overhead.");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    uint64_t result = 0;
    item *i = NULL;
    wait_for_persisted_value(h, h1, "k1", "13");

    evict_key(h, h1, "k1");

    checkeq(ENGINE_SUCCESS,
            h1->arithmetic(h, NULL, "k1", 2, true, false, 1, 1, 0,
                           &i, PROTOCOL_BINARY_RAW_BYTES, &result, 0),
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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "k1", "new value", &i),
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

    checkeq(ENGINE_SUCCESS,
            del(h, h1, "k1", 0, 0), "Failed to delete.");

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    cb_assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

extern "C" {
    static void bg_set_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        item *i = NULL;
        checkeq(ENGINE_SUCCESS,
                store(td->h, td->h1, NULL, OPERATION_SET,
                      "k1", "new value", &i),
                "Failed to update an item.");
        td->h1->release(td->h, NULL, i);

        delete td;
    }

    static void bg_del_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        checkeq(ENGINE_SUCCESS,
                del(td->h, td->h1, "k1", 0, 0), "Failed to delete.");

        delete td;
    }

    static void bg_incr_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        uint64_t result = 0;
        item *i = NULL;
        checkeq(ENGINE_SUCCESS,
                td->h1->arithmetic(td->h, NULL, "k1", 2, true, false, 1, 1, 0,
                                   &i, PROTOCOL_BINARY_RAW_BYTES, &result, 0),
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0),
                "Failed to store a value");
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }

    return SUCCESS;
}

static enum test_result test_revid(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    ItemMetaData meta;
    for (uint64_t ii = 1; ii < 10; ++ii) {
        item *it;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, "test_revid", "foo",
                      &it, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, it);

        check(get_meta(h, h1, "test_revid"), "Get meta failed");

        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");
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
    checkeq(ENGINE_SUCCESS, ret, "Expected to get the item back!");
    h1->release(h, NULL, it);

    return SUCCESS;
}

static enum test_result test_mb3466(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
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

static enum test_result test_observe_no_data(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::map<std::string, uint16_t> obskeys;
    observe(h, h1, obskeys);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
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

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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

    checkeq(total_persisted, num_items,
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
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
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
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, k1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS, del(h, h1, k1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    check(get_meta(h, h1, k1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");

    // Make sure there is one temp_item
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // Do an observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key"] = 0;
    observe(h, h1, obskeys);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    check(ntohll(cas) != cas3, "Expected cas to be different");

    return SUCCESS;
}

static enum test_result test_observe_errors(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::map<std::string, uint16_t> obskeys;

    // Check not my vbucket error
    obskeys["key"] = 1;
    observe(h, h1, obskeys);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(), "Expected not my vbucket");

    // Check invalid packets
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0, "0", 1);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected invalid");
    free(pkt);

    pkt = createPacket(PROTOCOL_BINARY_CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0, "0000", 4);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Observe failed.");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected invalid");
    free(pkt);

    return SUCCESS;
}

static enum test_result test_control_data_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value1", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Faile to disable data traffic");
    free(pkt);

    checkeq(ENGINE_TMPFAIL,
            store(h, h1, NULL, OPERATION_SET, "key", "value2", &itm),
            "Expected to receive temporary failure");
    h1->release(h, NULL, itm);

    pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Faile to enable data traffic");
    free(pkt);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value2", &itm),
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
            checkeq(ENGINE_SUCCESS,
                    h1->get(h, NULL, &i, key.c_str(), key.length(), 0),
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
        checkeq(ENGINE_SUCCESS,
                h1->get(h, NULL, &i, key.c_str(), key.length(), 0),
                "Failed to get value.");
        h1->release(h, NULL, i);
    }

    //Tmp ooms now trigger the item_pager task to eject some items,
    //thus there would be a few background fetches at least.
    check(get_int_stat(h, h1, "ep_bg_fetched") > 0,
          "Expected a few disk reads for referenced items");

    return SUCCESS;
}

static enum test_result test_stats_vkey_valid_field(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();

    // Check vkey when a key doesn't exist
    const char* stats_key = "vkey key 0";
    checkeq(ENGINE_KEY_ENOENT,
            h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                          add_stats),
            "Expected not found.");


    stop_persistence(h, h1);

    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    // Check to make sure a non-persisted item is 'dirty'
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                          add_stats),
            "Failed to get stats.");
    check(vals.find("key_valid")->second.compare("dirty") == 0,
          "Expected 'dirty'");

    // Check that a key that is resident and persisted returns valid
    start_persistence(h, h1);
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 1);
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, stats_key, strlen(stats_key),
                          add_stats),
            "Failed to get stats.");
    check(vals.find("key_valid")->second.compare("valid") == 0,
          "Expected 'valid'");

    // Check that an evicted key still returns valid
    evict_key(h, h1, "key", 0, "Ejected.");
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, "vkey key 0", 10, add_stats),
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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET,
                      s1.str().c_str(), s1.str().c_str(), &i, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, i);
        std::stringstream s2;
        s2 << "key-1-" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET,
                      s2.str().c_str(), s2.str().c_str(), &i, 0, 1),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_stat_to_be(h, h1, "ep_total_persisted", 2000);
    check(get_int_stat(h, h1, "ep_commit_num") > 1,
          "Expected 20 transaction completions at least");
    return SUCCESS;
}

static enum test_result test_set_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that set without cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
                       "Expected 1 set rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    // Check that set with correct cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, last_meta.cas, 10, 1735689600);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
                       "Expected 2 set rm ops");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 2, "Invalid result for seqno");

    // Check that updating an item with no cas succeeds
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 5, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
                       "Expected 3 set rm ops");

    check(last_meta.flags == 5, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 3, "Invalid result for seqno");

    // Check that updating an item with the wrong cas fails
    set_ret_meta(h, h1, "key", 3, "value", 5, 0, last_meta.cas + 1, 5, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected set returing meta to fail");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
                       "Expected 3 set rm ops");

    return SUCCESS;
}

static enum test_result test_set_ret_meta_error(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check invalid packet constructions
    set_ret_meta(h, h1, "", 0, "value", 5, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected set returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0,
                       "key", 3, "val", 3);
    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt, add_response),
            "Expected to be able to store ret meta");
    free(pkt);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected set returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    set_ret_meta(h, h1, "key", 3, "value", 5, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(),
          "Expected set returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected set returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected set returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to set vbucket state.");
    set_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected set returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_add_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that add with cas fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 10, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_STORED, last_status.load(),
          "Expected set returing meta to fail");

    // Check that add without cas succeeds.
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
                       "Expected 1 set rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    // Check that re-adding a key fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_STORED, last_status.load(),
          "Expected set returing meta to fail");

    // Check that adding a key with flags and exptime returns the correct values
    add_ret_meta(h, h1, "key2", 4, "value", 5, 0, 0, 10, 1735689600);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_set_ret_meta"),
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
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected add returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0,
                       "key", 3, "val", 3);
    checkeq(ENGINE_SUCCESS, h1->unknown_command(h, NULL, pkt, add_response),
          "Expected to be able to add ret meta");
    free(pkt);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected add returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    add_ret_meta(h, h1, "key", 3, "value", 5, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(),
          "Expected add returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to add vbucket state.");
    add_ret_meta(h, h1, "key", 3, "value", 5, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_del_ret_meta(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    // Check that deleting a non-existent key fails
    del_ret_meta(h, h1, "key", 3, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
          "Expected set returing meta to fail");

    // Check that deleting a non-existent key with a cas fails
    del_ret_meta(h, h1, "key", 3, 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
          "Expected set returing meta to fail");

    // Check that deleting a key with no cas succeeds
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 1, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_ret_meta"),
                       "Expected 1 del rm op");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 2, "Invalid result for seqno");

    // Check that deleting a key with a cas succeeds.
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 10, 1735689600);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 3, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, last_meta.cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_del_ret_meta"),
                       "Expected 2 del rm ops");

    check(last_meta.flags == 10, "Invalid result for flags");
    check(last_meta.exptime == 1735689600, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 4, "Invalid result for seqno");

    // Check that deleting a key with the wrong cas fails
    add_ret_meta(h, h1, "key", 3, "value", 5, 0, 0, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected set returing meta to succeed");

    check(last_meta.flags == 0, "Invalid result for flags");
    check(last_meta.exptime == 0, "Invalid result for expiration");
    check(last_meta.cas != 0, "Invalid result for cas");
    check(last_meta.revSeqno == 5, "Invalid result for seqno");

    del_ret_meta(h, h1, "key", 3, 0, last_meta.cas + 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected set returing meta to fail");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_del_ret_meta"),
                       "Expected 2 del rm ops");

    return SUCCESS;
}

static enum test_result test_del_ret_meta_error(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check invalid packet constructions
    del_ret_meta(h, h1, "", 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected add returing meta to succeed");

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, 0, 0, NULL, 0,
                       "key", 3);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response),
            "Expected to be able to del ret meta");
    free(pkt);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
          "Expected add returing meta to succeed");

    // Check tmp fail errors
    disable_traffic(h, h1);
    del_ret_meta(h, h1, "key", 3, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(),
          "Expected add returing meta to fail");
    enable_traffic(h, h1);

    // Check not my vbucket errors
    del_ret_meta(h, h1, "key", 3, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_replica),
          "Failed to set vbucket state.");
    del_ret_meta(h, h1, "key", 3, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Failed to add vbucket state.");
    del_ret_meta(h, h1, "key", 3, 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
          "Expected add returing meta to fail");
    vbucketDelete(h, h1, 1);

    return SUCCESS;
}

static enum test_result test_set_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, val, &i),
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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
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

    checkeq(curr_vb_items,
            get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected item count in vbucket");

    return SUCCESS;
}


static enum test_result test_add_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i),
            "Failed to add value.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key", 0, "Ejected.");

    checkeq(ENGINE_NOT_STORED,
            store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i),
            "Failed to fail to re-add value.");
    h1->release(h, NULL, i);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i),
            "Failed to add value again.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_gat_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "mykey", 0, "Ejected.");

    gat(h, h1, "mykey", 0, 10); // 10 sec as expiration time
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "gat mykey");
    check(last_body == "somevalue", "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &itm, "mykey", 5, 0),
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_keyStats_with_item_eviction(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // set (k1,v1) in vbucket 0
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0),
            "Failed to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "k1", 0, "Ejected.");

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats),
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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key,
                  "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    // delete an item with meta data
    del_with_meta(h, h1, key, keylen, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_del_with_item_eviction(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
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
    checkeq(ENGINE_SUCCESS,
            h1->remove(h, NULL, "key", 3, &cas, 0, &mut_info),
            "Failed remove with value.");
    check(orig_cas != cas, "Expected CAS to be different on delete");
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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "gat mykey");
    check(last_body == "somevalue", "Invalid data returned");

    // Store a dummy item since we do not purge the item with highest seqno
    checkeq(ENGINE_SUCCESS,
          store(h, h1, NULL, OPERATION_SET, "dummykey", "dummyvalue", NULL,
                0, 0, 0), "Error setting.");

    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "mykey", 0, "Ejected.");

    // time-travel 11 secs..
    testHarness.time_travel(11);

    // Compaction on VBucket 0
    compact_db(h, h1, 0, 0, 10, 10, 0);

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    checkeq(1, get_int_stat(h, h1, "vb_active_expired"),
          "Expect the compactor to delete an expired item");

    // The item is already expired...
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &itm, "mykey", 5, 0),
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_non_existent_get_and_delete(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &i, "key1", 4, 0),
            "Unexpected return status");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Unexpected temp item");
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, "key3", 0, 0), "Unexpected return status");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Unexpected temp item");
    return SUCCESS;
}

static enum test_result test_mb16421(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);

    // Evict Item!
    evict_key(h, h1, "mykey", 0, "Ejected.");

    // Issue Get Meta
    check(get_meta(h, h1, "mykey"), "Expected to get meta");

    // Issue Get
    checkeq(ENGINE_SUCCESS,
            h1->get(h, NULL, &itm, "mykey", 5, 0), "Item should be there");
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

    checkeq(ENGINE_KEY_ENOENT,
            h1->unknown_command(h, cookie, &pkt, add_response),
            "Database should be empty");

    // Store a key
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                  &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed set.");
    h1->release(h, NULL, itm);
    checkeq(ENGINE_SUCCESS,
            h1->get(h, NULL, &itm, "mykey", 5, 0),
            "Item should be there");
    h1->release(h, NULL, itm);

    // We should be able to get one if there is something in there
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, cookie, &pkt, add_response),
            "get random should work");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            last_datatype.load(),
            "Expected datatype to be JSON");

    // Since it is random we can't really check that we don't get the
    // same value twice...

    // Check for invalid packets
    pkt.request.extlen = 1;
    checkeq(ENGINE_EINVAL,
            h1->unknown_command(h, cookie, &pkt, add_response),
            "extlen not allowed");

    pkt.request.extlen = 0;
    pkt.request.keylen = 1;
    checkeq(ENGINE_EINVAL,
            h1->unknown_command(h, cookie, &pkt, add_response),
            "keylen not allowed");

    pkt.request.keylen = 0;
    pkt.request.bodylen = 1;
    checkeq(ENGINE_EINVAL,
            h1->unknown_command(h, cookie, &pkt, add_response),
            "bodylen not allowed");

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
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
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

static enum test_result test_hlc_cas(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    const char *key = "key";
    item *i = NULL;
    item_info info;
    uint64_t curr_cas = 0, prev_cas = 0;

    memset(&info, 0, sizeof(info));

    //Set a really large drift value
    set_drift_counter_state(h, h1, 100000);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD, key, "data1", &i, 0, 0),
            "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //set a lesser drift and ensure that the CAS is monotonically
    //increasing
    set_drift_counter_state(h, h1, 100);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "data2", &i, 0, 0),
            "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //ensure that the adjusted time will be negative
    int64_t drift_counter = (-1) * (gethrtime() * 2);
    set_drift_counter_state(h, h1, drift_counter);

    protocol_binary_request_header *request;
    int64_t adjusted_time;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected Success");
    checkeq(sizeof(int64_t), last_body.size(),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time, last_body.data(), last_body.size());
    adjusted_time = ntohll(adjusted_time);
    std::string err_msg("Adjusted time " + std::to_string(adjusted_time) +
                        " is supposed to have been negative!");
    check(adjusted_time < 0, err_msg.c_str());

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_REPLACE, key, "data3", &i, 0, 0),
            "Failed to store an item");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    //Set the drift value to 0
    set_drift_counter_state(h, h1, 0);

    getl(h, h1, key, 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected to be able to getl on first try");
    curr_cas = last_cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    uint64_t result = 0;
    checkeq(ENGINE_SUCCESS,
            h1->arithmetic(h, NULL, "key2", 4, true, true, 1, 1, 0,
                           &i, PROTOCOL_BINARY_RAW_BYTES, &result, 0),
            "Failed arithmetic operation");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key2"), "Error in getting item info");
    curr_cas = info.cas;
    check(curr_cas > prev_cas, "CAS is not monotonically increasing");

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
        checkeq(ENGINE_SUCCESS,
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

// Regression test for MB-17517 - ensure that if an item is locked when TAP
// attempts to stream it, it doesn't get a CAS of -1.
static enum test_result test_mb17517_tap_with_locked_key(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    const uint16_t vbid = 0;
    // Store an item and immediately lock it.
    item *it = NULL;
    std::string key("key");
    checkeq(store(h, h1, NULL, OPERATION_SET, key.c_str(), "value",
                  &it, 0, vbid, 3600, PROTOCOL_BINARY_RAW_BYTES),
            ENGINE_SUCCESS,
            "Failed to store an item.");
    h1->release(h, NULL, it);

    uint32_t lock_timeout = 10;
    getl(h, h1, key.c_str(), vbid, lock_timeout);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected to be able to getl on first try");

    wait_for_flusher_to_settle(h, h1);

    // Create the TAP connection and try to get the items.
    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name("test_mb17517_tap_with_locked_key");
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL, 0);
    check(iter != NULL, "Failed to create a tap iterator");

    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;

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
        case TAP_MUTATION: {
            testHarness.unlock_cookie(cookie);

            item_info info;
            info.nvalue = 1;
            if (!h1->get_item_info(h, NULL, it, &info)) {
                fprintf(stderr, "test_mb17517_tap_with_locked_key: "
                        "get_item_info failed\n");
                return FAIL;
            }

            // Check the CAS.
            if (info.cas == ~0ull) {
                fprintf(stderr, "test_mb17517_tap_with_locked_key: "
                        "Got CAS of -1 in TAP_MUTATION\n");
                return FAIL;
            }
            h1->release(h, NULL, it);
            testHarness.lock_cookie(cookie);
            break;
        }
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static void force_vbstate_to_25x(std::string dbname, int vbucket) {
    std::string filename = dbname +
                           DIRECTORY_SEPARATOR_CHARACTER +
                           std::to_string(vbucket) +
                           ".couch.1";
    Db* handle;
    couchstore_error_t err = couchstore_open_db(filename.c_str(),
                                                COUCHSTORE_OPEN_FLAG_CREATE,
                                                &handle);

    checkeq(COUCHSTORE_SUCCESS, err, "Failed to open new database");

    // Create 2.5 _local/vbstate
    std::string vbstate2_5_x ="{\"state\": \"active\","
                              " \"checkpoint_id\": \"1\","
                              " \"max_deleted_seqno\": \"0\"}";
    LocalDoc vbstate;
    vbstate.id.buf = (char *)"_local/vbstate";
    vbstate.id.size = sizeof("_local/vbstate") - 1;
    vbstate.json.buf = (char *)vbstate2_5_x.c_str();
    vbstate.json.size = vbstate2_5_x.size();
    vbstate.deleted = 0;

    err = couchstore_save_local_document(handle, &vbstate);
    checkeq(COUCHSTORE_SUCCESS, err, "Failed to write local document");
    couchstore_commit(handle);
    couchstore_close_file(handle);
    couchstore_free_db(handle);
}

// Regression test for MB-19635
// Check that warming up from a 2.x couchfile doesn't end up with a UUID of 0
// we warmup 2 vbuckets and ensure they get unique IDs.
static enum test_result test_mb19635_upgrade_from_25x(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {

    std::string backend = get_str_stat(h, h1, "ep_backend");
    if (backend == "forestdb") {
        return SKIPPED;
    }

    std::string dbname = get_dbname(testHarness.get_current_testcase()->cfg);

    force_vbstate_to_25x(dbname, 0);
    force_vbstate_to_25x(dbname, 1);

    // Now shutdown engine force and restart to warmup from the 2.5.x data.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);
    uint64_t vb_uuid0 = get_ull_stat(h, h1, "vb_0:uuid", "vbucket-details");
    uint64_t vb_uuid1 = get_ull_stat(h, h1, "vb_1:uuid", "vbucket-details");
    checkne(vb_uuid0, vb_uuid1, "UUID is not unique");
    return SUCCESS;
}

// Regression test the stats calls that they don't blow the snprintf
// buffers. All of the tests in this batch make sure that all of the stats
// exists (the stats call return a fixed set of stats)
static enum test_result test_mb19687_fixed(ENGINE_HANDLE* h,
                                           ENGINE_HANDLE_V1* h1) {

    std::vector<std::string> roKVStoreStats = {
                "ro_0:backend_type",
                "ro_0:close",
                "ro_0:failure_get",
                "ro_0:failure_open",
                "ro_0:io_compaction_read_bytes",
                "ro_0:io_compaction_write_bytes",
                "ro_0:io_num_read",
                "ro_0:io_num_write",
                "ro_0:io_read_bytes",
                "ro_0:io_total_read_bytes",
                "ro_0:io_total_write_bytes",
                "ro_0:io_write_bytes",
                "ro_0:numLoadedVb",
                "ro_0:open",
                "ro_1:backend_type",
                "ro_1:close",
                "ro_1:failure_get",
                "ro_1:failure_open",
                "ro_1:io_compaction_read_bytes",
                "ro_1:io_compaction_write_bytes",
                "ro_1:io_num_read",
                "ro_1:io_num_write",
                "ro_1:io_read_bytes",
                "ro_1:io_total_read_bytes",
                "ro_1:io_total_write_bytes",
                "ro_1:io_write_bytes",
                "ro_1:numLoadedVb",
                "ro_1:open",
                "ro_2:backend_type",
                "ro_2:close",
                "ro_2:failure_get",
                "ro_2:failure_open",
                "ro_2:io_compaction_read_bytes",
                "ro_2:io_compaction_write_bytes",
                "ro_2:io_num_read",
                "ro_2:io_num_write",
                "ro_2:io_read_bytes",
                "ro_2:io_total_read_bytes",
                "ro_2:io_total_write_bytes",
                "ro_2:io_write_bytes",
                "ro_2:numLoadedVb",
                "ro_2:open",
                "ro_3:backend_type",
                "ro_3:close",
                "ro_3:failure_get",
                "ro_3:failure_open",
                "ro_3:io_compaction_read_bytes",
                "ro_3:io_compaction_write_bytes",
                "ro_3:io_num_read",
                "ro_3:io_num_write",
                "ro_3:io_read_bytes",
                "ro_3:io_total_read_bytes",
                "ro_3:io_total_write_bytes",
                "ro_3:io_write_bytes",
                "ro_3:numLoadedVb",
                "ro_3:open"
    };

    std::vector<std::string> rwKVStoreStats = {
                "rw_0:backend_type",
                "rw_0:close",
                "rw_0:failure_del",
                "rw_0:failure_get",
                "rw_0:failure_open",
                "rw_0:failure_set",
                "rw_0:failure_vbset",
                "rw_0:io_compaction_read_bytes",
                "rw_0:io_compaction_write_bytes",
                "rw_0:io_num_read",
                "rw_0:io_num_write",
                "rw_0:io_read_bytes",
                "rw_0:io_total_read_bytes",
                "rw_0:io_total_write_bytes",
                "rw_0:io_write_bytes",
                "rw_0:lastCommDocs",
                "rw_0:numLoadedVb",
                "rw_0:open",
                "rw_1:backend_type",
                "rw_1:close",
                "rw_1:failure_del",
                "rw_1:failure_get",
                "rw_1:failure_open",
                "rw_1:failure_set",
                "rw_1:failure_vbset",
                "rw_1:io_compaction_read_bytes",
                "rw_1:io_compaction_write_bytes",
                "rw_1:io_num_read",
                "rw_1:io_num_write",
                "rw_1:io_read_bytes",
                "rw_1:io_total_read_bytes",
                "rw_1:io_total_write_bytes",
                "rw_1:io_write_bytes",
                "rw_1:lastCommDocs",
                "rw_1:numLoadedVb",
                "rw_1:open",
                "rw_2:backend_type",
                "rw_2:close",
                "rw_2:failure_del",
                "rw_2:failure_get",
                "rw_2:failure_open",
                "rw_2:failure_set",
                "rw_2:failure_vbset",
                "rw_2:io_compaction_read_bytes",
                "rw_2:io_compaction_write_bytes",
                "rw_2:io_num_read",
                "rw_2:io_num_write",
                "rw_2:io_read_bytes",
                "rw_2:io_total_read_bytes",
                "rw_2:io_total_write_bytes",
                "rw_2:io_write_bytes",
                "rw_2:lastCommDocs",
                "rw_2:numLoadedVb",
                "rw_2:open",
                "rw_3:backend_type",
                "rw_3:close",
                "rw_3:failure_del",
                "rw_3:failure_get",
                "rw_3:failure_open",
                "rw_3:failure_set",
                "rw_3:failure_vbset",
                "rw_3:io_compaction_read_bytes",
                "rw_3:io_compaction_write_bytes",
                "rw_3:io_num_read",
                "rw_3:io_num_write",
                "rw_3:io_read_bytes",
                "rw_3:io_total_read_bytes",
                "rw_3:io_total_write_bytes",
                "rw_3:io_write_bytes",
                "rw_3:lastCommDocs",
                "rw_3:numLoadedVb",
                "rw_3:open"
    };

    std::string backend = get_str_stat(h, h1, "ep_backend");
    std::vector<std::string> kvstats;

    /* initialize with all the read write stats */
    kvstats.insert(kvstats.begin(), rwKVStoreStats.begin(),
                   rwKVStoreStats.end());

    /* add the read-only stats in the case of couchstore */
    if (backend == "couchdb") {
        kvstats.insert(kvstats.end(), roKVStoreStats.begin(),
                       roKVStoreStats.end());
    }

    // all of these should be const, but g++ seems to have problems with that
    const std::map<std::string, std::vector<std::string> > statsKeys{
        {"tap-vbtakeover 0",
            {
                "name",
                "status",
                "estimate",
                "on_disk_deletes",
                "chk_items",
                "vb_items"
            }
        },
        {"dcp-vbtakeover 0",
            {
                "status",
                "on_disk_deletes",
                "vb_items",
                "chk_items",
                "estimate"
            }
        },
        {"tap",
            {
                "ep_replication_throttle_queue_cap",
                "ep_replication_throttle_threshold",
                "ep_replication_throttled",
                "ep_tap_ack_grace_period",
                "ep_tap_ack_interval",
                "ep_tap_ack_window_size",
                "ep_tap_backoff_period",
                "ep_tap_bg_fetch_requeued",
                "ep_tap_bg_fetched",
                "ep_tap_bg_max_pending",
                "ep_tap_count",
                "ep_tap_deletes",
                "ep_tap_fg_fetched",
                "ep_tap_noop_interval",
                "ep_tap_queue_backfillremaining",
                "ep_tap_queue_backoff",
                "ep_tap_queue_drain",
                "ep_tap_queue_fill",
                "ep_tap_queue_itemondisk",
                "ep_tap_total_backlog_size",
                "ep_tap_total_fetched",
                "ep_tap_total_queue"
            }
        },
        {"dcp",
            {
                "ep_dcp_count",
                "ep_dcp_dead_conn_count",
                "ep_dcp_items_remaining",
                "ep_dcp_items_sent",
                "ep_dcp_max_running_backfills",
                "ep_dcp_num_running_backfills",
                "ep_dcp_producer_count",
                "ep_dcp_queue_backfillremaining",
                "ep_dcp_queue_fill",
                "ep_dcp_total_bytes",
                "ep_dcp_total_queue"
            }
        },
        {"hash",
            {
                "vb_0:counted",
                "vb_0:locks",
                "vb_0:max_depth",
                "vb_0:mem_size",
                "vb_0:mem_size_counted",
                "vb_0:min_depth",
                "vb_0:reported",
                "vb_0:resized",
                "vb_0:size",
                "vb_0:state"
            }},
        {"vbucket",
            {
                "vb_0"
            }
        },
        {"vbucket-details 0",
            {
                "vb_0",
                "vb_0:bloom_filter",
                "vb_0:bloom_filter_key_count",
                "vb_0:bloom_filter_size",
                "vb_0:db_data_size",
                "vb_0:db_file_size",
                "vb_0:drift_counter",
                "vb_0:high_seqno",
                "vb_0:ht_cache_size",
                "vb_0:ht_item_memory",
                "vb_0:ht_memory",
                "vb_0:max_cas",
                "vb_0:num_ejects",
                "vb_0:num_items",
                "vb_0:num_non_resident",
                "vb_0:num_temp_items",
                "vb_0:ops_create",
                "vb_0:ops_delete",
                "vb_0:ops_reject",
                "vb_0:ops_update",
                "vb_0:pending_writes",
                "vb_0:purge_seqno",
                "vb_0:queue_age",
                "vb_0:queue_drain",
                "vb_0:queue_fill",
                "vb_0:queue_memory",
                "vb_0:queue_size",
                "vb_0:rollback_item_count",
                "vb_0:time_sync",
                "vb_0:uuid"
            }
        },
        {"vbucket-seqno",
            {
                "vb_0:abs_high_seqno",
                "vb_0:high_seqno",
                "vb_0:last_persisted_seqno",
                "vb_0:last_persisted_snap_end",
                "vb_0:last_persisted_snap_start",
                "vb_0:purge_seqno",
                "vb_0:uuid"
            }
        },
        {"vbucket-seqno 0",
            {
                "vb_0:abs_high_seqno",
                "vb_0:high_seqno",
                "vb_0:last_persisted_seqno",
                "vb_0:last_persisted_snap_end",
                "vb_0:last_persisted_snap_start",
                "vb_0:purge_seqno",
                "vb_0:uuid"
            }
        },
        {"prev-vbucket",
            {
                "vb_0"
            }
        },
        {"prev-vbucket",
            {
                "vb_0"
            }
        },
        {"checkpoint",
            {
                "vb_0:last_closed_checkpoint_id",
                "vb_0:mem_usage",
                "vb_0:num_checkpoint_items",
                "vb_0:num_checkpoints",
                "vb_0:num_conn_cursors",
                "vb_0:num_items_for_persistence",
                "vb_0:num_open_checkpoint_items",
                "vb_0:open_checkpoint_id",
                "vb_0:persisted_checkpoint_id",
                "vb_0:persistence:cursor_checkpoint_id",
                "vb_0:persistence:cursor_seqno",
                "vb_0:persistence:num_visits",
                "vb_0:state"
            }
        },
        {"checkpoint 0",
            {
                "vb_0:last_closed_checkpoint_id",
                "vb_0:mem_usage",
                "vb_0:num_checkpoint_items",
                "vb_0:num_checkpoints",
                "vb_0:num_conn_cursors",
                "vb_0:num_items_for_persistence",
                "vb_0:num_open_checkpoint_items",
                "vb_0:open_checkpoint_id",
                "vb_0:persisted_checkpoint_id",
                "vb_0:persistence:cursor_checkpoint_id",
                "vb_0:persistence:cursor_seqno",
                "vb_0:persistence:num_visits",
                "vb_0:state"
            }
        },
        {"uuid",
            {
                "uuid"
            }
        },
        {"kvstore",
              kvstats
        },
        {"info",
            {
                "info"
            }
        },
        {"allocator",
            {
                "detailed"
            }
        },
        {"config",
            {
                "ep_access_scanner_enabled",
                "ep_alog_block_size",
                "ep_alog_path",
                "ep_alog_resident_ratio_threshold",
                "ep_alog_sleep_time",
                "ep_alog_task_time",
                "ep_backend",
                "ep_backfill_mem_threshold",
                "ep_bfilter_enabled",
                "ep_bfilter_fp_prob",
                "ep_bfilter_key_count",
                "ep_bfilter_residency_threshold",
                "ep_bg_fetch_delay",
                "ep_chk_max_items",
                "ep_chk_period",
                "ep_chk_remover_stime",
                "ep_compaction_exp_mem_threshold",
                "ep_compaction_write_queue_cap",
                "ep_config_file",
                "ep_conflict_resolution_type",
                "ep_couch_bucket",
                "ep_cursor_dropping_lower_mark",
                "ep_cursor_dropping_upper_mark",
                "ep_data_traffic_enabled",
                "ep_dbname",
                "ep_dcp_backfill_byte_limit",
                "ep_dcp_conn_buffer_size",
                "ep_dcp_conn_buffer_size_aggr_mem_threshold",
                "ep_dcp_conn_buffer_size_aggressive_perc",
                "ep_dcp_conn_buffer_size_max",
                "ep_dcp_conn_buffer_size_perc",
                "ep_dcp_enable_noop",
                "ep_dcp_flow_control_policy",
                "ep_dcp_max_unacked_bytes",
                "ep_dcp_min_compression_ratio",
                "ep_dcp_idle_timeout",
                "ep_dcp_noop_tx_interval",
                "ep_dcp_producer_snapshot_marker_yield_limit",
                "ep_dcp_consumer_process_buffered_messages_yield_limit",
                "ep_dcp_consumer_process_buffered_messages_batch_size",
                "ep_dcp_scan_byte_limit",
                "ep_dcp_scan_item_limit",
                "ep_dcp_takeover_max_time",
                "ep_dcp_value_compression_enabled",
                "ep_defragmenter_age_threshold",
                "ep_defragmenter_chunk_duration",
                "ep_defragmenter_enabled",
                "ep_defragmenter_interval",
                "ep_enable_chk_merge",
                "ep_exp_pager_enabled",
                "ep_exp_pager_initial_run_time",
                "ep_exp_pager_stime",
                "ep_failpartialwarmup",
                "ep_flushall_enabled",
                "ep_getl_default_timeout",
                "ep_getl_max_timeout",
                "ep_ht_locks",
                "ep_ht_size",
                "ep_initfile",
                "ep_item_eviction_policy",
                "ep_item_num_based_new_chk",
                "ep_keep_closed_chks",
                "ep_max_checkpoints",
                "ep_max_failover_entries",
                "ep_max_item_size",
                "ep_max_num_auxio",
                "ep_max_num_nonio",
                "ep_max_num_readers",
                "ep_max_num_shards",
                "ep_max_num_workers",
                "ep_max_num_writers",
                "ep_max_size",
                "ep_max_threads",
                "ep_max_vbuckets",
                "ep_mem_high_wat",
                "ep_mem_low_wat",
                "ep_mutation_mem_threshold",
                "ep_pager_active_vb_pcnt",
                "ep_postInitfile",
                "ep_replication_throttle_cap_pcnt",
                "ep_replication_throttle_queue_cap",
                "ep_replication_throttle_threshold",
                "ep_tap_ack_grace_period",
                "ep_tap_ack_initial_sequence_number",
                "ep_tap_ack_interval",
                "ep_tap_ack_window_size",
                "ep_tap_backfill_resident",
                "ep_tap_backlog_limit",
                "ep_tap_backoff_period",
                "ep_tap_bg_max_pending",
                "ep_tap_keepalive",
                "ep_tap_noop_interval",
                "ep_tap_requeue_sleep_time",
                "ep_time_synchronization",
                "ep_uuid",
                "ep_vb0",
                "ep_waitforwarmup",
                "ep_warmup",
                "ep_warmup_batch_size",
                "ep_warmup_min_items_threshold",
                "ep_warmup_min_memory_threshold"
            }
        },
        {"workload",
            {
                "ep_workload:num_readers",
                "ep_workload:num_writers",
                "ep_workload:num_auxio",
                "ep_workload:num_nonio",
                "ep_workload:max_readers",
                "ep_workload:max_writers",
                "ep_workload:max_auxio",
                "ep_workload:max_nonio",
                "ep_workload:num_shards",
                "ep_workload:ready_tasks",
                "ep_workload:num_sleepers",
                "ep_workload:LowPrioQ_AuxIO:InQsize",
                "ep_workload:LowPrioQ_AuxIO:OutQsize",
                "ep_workload:LowPrioQ_NonIO:InQsize",
                "ep_workload:LowPrioQ_NonIO:OutQsize",
                "ep_workload:LowPrioQ_Reader:InQsize",
                "ep_workload:LowPrioQ_Reader:OutQsize",
                "ep_workload:LowPrioQ_Writer:InQsize",
                "ep_workload:LowPrioQ_Writer:OutQsize"
            }
        },
        {"failovers 0",
            {
                "vb_0:0:id",
                "vb_0:0:seq",
                "vb_0:num_entries"
            }
        },
        {"failovers",
            {
                "vb_0:0:id",
                "vb_0:0:seq",
                "vb_0:num_entries"
            }
        },
        {"diskinfo",
            {
                "ep_db_data_size",
                "ep_db_file_size"
            }
        },
        {"diskinfo detail",
            {
                "vb_0:data_size",
                "vb_0:file_size"
            }
        }
    };

    bool error = false;
    for (const auto& entry : statsKeys) {
        vals.clear();
        checkeq(ENGINE_SUCCESS,
                h1->get_stats(h, nullptr, entry.first.data(),
                              entry.first.size(), add_stats),
                (std::string("Failed to get stats: ") + entry.first).c_str());

        for (const auto& key : entry.second) {
            auto iter = vals.find(key);
            if (iter == vals.end()) {
                error = true;
                fprintf(stderr, "Missing stat:  %s from stat group %s\n",
                        key.c_str(),
                        entry.first.c_str());
            }
        }

        if (entry.second.size() != vals.size()) {
            fprintf(stderr,
                    "Incorrect number of stats returned for stat group %s: %lu != %lu\n",
                    entry.first.c_str(), (unsigned long)entry.second.size(),
                    (unsigned long)vals.size());
            error = true;
        }
    }

    if (error) {
        abort_msg("missing stats", "stats error", __FILE__, __LINE__);
    }

    return SUCCESS;
}

// Regression test the stats calls that they don't blow the snprintf
// buffers. All of the tests in this batch make sure that some of the stats
// exists (the server may return more)
static enum test_result test_mb19687_variable(ENGINE_HANDLE* h,
                                              ENGINE_HANDLE_V1* h1) {
    // all of these should be const, but g++ seems to have problems with that
    std::map<std::string, std::vector<std::string> > statsKeys{
        {"dispatcher", {}}, // Depends on how how long the dispatcher ran..

//        {"tapagg foo",      {}}, // tapagg takes a key and I need to have that tap stream
//        {"dcpagg bar",      {}}, // dcpagg takes a key and I need to have that dcp stream

        {"key mykey",
            {
                "key_cas",
                "key_exptime",
                "key_flags",
                "key_is_dirty",
                "key_vb_state"
            }
        },
        {"vkey mykey",
            {
                "key_cas",
                "key_exptime",
                "key_flags",
                "key_is_dirty",
                "key_valid",
                "key_vb_state"
            }
        },
        {"memory",
            {
                "bytes",
                "ep_blob_num",
                "ep_blob_overhead",
                "ep_item_num",
                "ep_kv_size",
                "ep_max_size",
                "ep_mem_high_wat",
                "ep_mem_high_wat_percent",
                "ep_mem_low_wat",
                "ep_mem_low_wat_percent",
                "ep_oom_errors",
                "ep_overhead",
                "ep_storedval_num",
                "ep_storedval_overhead",
                "ep_storedval_size",
                "ep_tmp_oom_errors",
                "ep_value_size",
                "mem_used",
            }
        },

        // These stat groups return histograms so we can't guess the
        // key names...
        {"timings",
            {}
        },
        {"scheduler",
            {}
        },
        {"runtimes",
            {}
        },
        {"kvtimings",
            {}
        },

        {"warmup",
            {
                "ep_warmup",
                "ep_warmup_state",
                "ep_warmup_thread",
                "ep_warmup_key_count",
                "ep_warmup_value_count",
                "ep_warmup_dups",
                "ep_warmup_oom",
                "ep_warmup_min_memory_threshold",
                "ep_warmup_min_item_threshold",
                "ep_warmup_estimated_key_count",
                "ep_warmup_estimated_value_count"
            }
        }
    };

    item_info info;
    memset(&info, 0, sizeof(info));

    item *i = nullptr;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD, "mykey", "data1", &i, 0, 0),
            "Failed to store an item");
    h1->release(h, NULL, i);

    bool error = false;
    for (const auto& entry : statsKeys) {
        vals.clear();
        checkeq(ENGINE_SUCCESS,
                h1->get_stats(h, nullptr, entry.first.data(),
                              entry.first.size(), add_stats),
                (std::string("Failed to get stats: ") + entry.first).c_str());

        // Verify that the stats we expected is there..
        for (const auto& key : entry.second) {
            auto iter = vals.find(key);
            if (iter == vals.end()) {
                error = true;
                fprintf(stderr, "Missing stat:  %s from stat group %s\n",
                        key.c_str(),
                        entry.first.c_str());
            }
        }
    }

    if (error) {
        abort_msg("missing stats", "stats error", __FILE__, __LINE__);
    }

    return SUCCESS;
}

static enum test_result test_mb20697(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");

    std::string dbname = vals["ep_dbname"];

    /* Nuke the database directory to simulate the commit failure */
    rmdb(dbname.c_str());

    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET,"key", "somevalue",
                                  NULL, 0, 0, 0), "store should have succeeded");

    /* Ensure that this results in commit failure and the stat gets incremented */
    wait_for_stat_change(h, h1, "ep_item_commit_failed", 0);

    return SUCCESS;
}

/* This test case checks the purge seqno validity when no items are actually
   purged in a compaction call */
static enum test_result test_vbucket_compact_no_purge(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const int num_items = 2;
    const char* key[num_items] = {"k1", "k2"};
    const char* value = "somevalue";

    /* Write 2 keys */
    for (int count = 0; count < num_items; count++){
        checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, key[count],
                                      value, NULL, 0, 0, 0),
                "Error setting.");
    }

    /* Delete one key */
    checkeq(ENGINE_SUCCESS, del(h, h1, key[0], 0, 0),
            "Failed remove with value.");

    /* Store a dummy item since we do not purge the item with highest seqno */
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "dummy_key", value, NULL,
                  0, 0, 0),
            "Error setting.");
    wait_for_flusher_to_settle(h, h1);

    /* Compact once */
    int exp_purge_seqno = get_int_stat(h, h1, "vb_0:high_seqno",
                                       "vbucket-seqno") - 1;
    compact_db(h, h1, 0, 2,
               get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno"), 1, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    checkeq(exp_purge_seqno,
            get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    /* Compact again, this time we don't expect to purge any items */
    compact_db(h, h1, 0, 2,
               get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno"), 1, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    checkeq(exp_purge_seqno,
            get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value after another compaction");

    /* Reload the engine */
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    /* Purge seqno should not change after reload */
    checkeq(exp_purge_seqno,
            get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value after reload");
    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite";

BaseTestCase testsuite_testcases[] = {
        TestCase("validate engine handle", test_validate_engine_handle,
                 NULL, teardown, NULL, prepare, cleanup),

        // ep-engine specific functionality
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
                 teardown, "max_size=6291456", prepare, cleanup),
        TestCase("warmup conf", test_warmup_conf, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("bloomfilter conf", test_bloomfilter_conf, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test bloomfilters",
                 test_bloomfilters, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test bloomfilters with store apis",
                 test_bloomfilters_with_store_apis, test_setup,
                 teardown, NULL, prepare, cleanup),
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
                 test_setup, teardown, "alog_path=./epaccess.log", prepare, cleanup),
        TestCase("test access scanner", test_access_scanner, test_setup,
                 teardown, "alog_path=./epaccess.log;chk_remover_stime=1;"
                 "max_size=6291456", prepare, cleanup),
        TestCase("test set_param message", test_set_param_message, test_setup,
                 teardown, "chk_remover_stime=1;max_size=6291456", prepare, cleanup),
        TestCase("test warmup oom value eviction", test_warmup_oom, test_setup,
                 teardown, "item_eviction_policy=full_eviction",
                 prepare, cleanup),

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
        TestCase("test shutdown snapshot range",
                 test_shutdown_snapshot_range, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),

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
        TestCase("test takeover stats race with vbucket create",
                 test_takeover_stats_race_with_vb_create, test_setup, teardown, NULL,
                 prepare, cleanup),

        // stats uuid
        TestCase("test stats uuid", test_uuid_stats, test_setup, teardown,
                 "uuid=foobar", prepare, cleanup),

        // revision id's
        TestCase("revision sequence numbers", test_revid, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("mb-4314", test_regression_mb4314, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("mb-3466", test_mb3466, test_setup,
                 teardown, NULL, prepare, cleanup),

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
                  NULL, prepare, cleanup),
        TestCase("test replace with eviction (full)", test_replace_with_eviction,
                 test_setup, teardown,
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
        TestCase("test hlc cas", test_hlc_cas, test_setup, teardown,
                 "time_synchronization=enabled_with_drift", prepare, cleanup),

        TestCaseV2("multi_bucket set/get ", test_multi_bucket_set_get, NULL,
                   teardown_v2, NULL, prepare, cleanup),

        TestCase("test_mb17517_tap_with_locked_key",
                 test_mb17517_tap_with_locked_key, test_setup, teardown, NULL,
                 prepare, cleanup),

        TestCase("test_mb19635_upgrade_from_25x",
                 test_mb19635_upgrade_from_25x, test_setup, teardown, NULL,
                 prepare, cleanup),

        TestCase("test_MB-19687_fixed", test_mb19687_fixed, test_setup, teardown, NULL,
                 prepare, cleanup),

        TestCase("test_MB-19687_variable", test_mb19687_variable, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test vbucket compact no purge", test_vbucket_compact_no_purge,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase("test_MB-20697", test_mb20697, test_setup, teardown, NULL, prepare,
                 cleanup),
        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
