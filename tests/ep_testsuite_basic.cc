/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Testsuite for 'basic' key-value functionality in ep-engine.
 */

#include "config.h"

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <JSON_checker.h>

#include <array>
#include <memcached/types.h>

#define WHITESPACE_DB "whitespace sucks.db"

// Types //////////////////////////////////////////////////////////////////////


// Helper functions ///////////////////////////////////////////////////////////

static bool epsilon(int val, int target, int ep=5) {
    return abs(val - target) < ep;
}


// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_alloc_limit(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    ENGINE_ERROR_CODE rv;

    rv = allocate(h, h1, NULL, &it, "key", 20 * 1024 * 1024, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES, 0);
    checkeq(ENGINE_SUCCESS, rv, "Allocated 20MB item");
    h1->release(h, NULL, it);

    rv = allocate(h, h1, NULL, &it, "key", (20 * 1024 * 1024) + 1, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES, 0);
    checkeq(ENGINE_E2BIG, rv, "Object too big");

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

// TODO: Ephemeral: Should refactor this into storage and memory tests,
// enable the memory checks for ephemeral.
static enum test_result test_memory_limit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(10240000, get_int_stat(h, h1, "ep_max_size"), "Max size not at 10MB");
    set_param(h, h1, protocol_binary_engine_param_flush, "mutation_mem_threshold", "95");
    if (isPersistentBucket(h, h1)) {
        wait_for_stat_change(h, h1,"ep_db_data_size", 0);
    }
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

static enum test_result test_max_size_and_water_marks_settings(
                                        ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(1000, get_int_stat(h, h1, "ep_max_size"), "Incorrect initial size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 750),
          "Incorrect initial low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 850),
          "Incorrect initial high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.75),
          "Incorrect initial low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.85),
          "Incorrect initial high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "1000000");

    checkeq(1000000, get_int_stat(h, h1, "ep_max_size"),
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

    checkeq(700000, get_int_stat(h, h1, "ep_mem_low_wat"),
            "Incorrect even larger low wat.");
    checkeq(800000, get_int_stat(h, h1, "ep_mem_high_wat"),
            "Incorrect even larger high wat.");
    check((get_float_stat(h, h1, "ep_mem_low_wat_percent") == (float)0.7),
          "Incorrect even larger low wat. percent");
    check((get_float_stat(h, h1, "ep_mem_high_wat_percent") == (float)0.8),
          "Incorrect even larger high wat. percent");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "100");

    checkeq(100, get_int_stat(h, h1, "ep_max_size"),
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

    checkeq(50, get_int_stat(h, h1, "ep_mem_low_wat"),
            "Incorrect even smaller low wat.");
    checkeq(70, get_int_stat(h, h1, "ep_mem_high_wat"),
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

    checkeq(1000, get_int_stat(h, h1, "ep_max_size"),
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

static enum test_result test_whitespace_db(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
           "Failed to get stats.");

    std::string dbname;
    std::string policy = vals.find("ep_item_eviction_policy")->second;
    dbname.assign(policy + std::string(WHITESPACE_DB));

    std::string oldparam("dbname=" + vals["ep_dbname"]);
    std::string newparam("dbname=" + dbname);
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

    vals.clear();
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
           "Failed to get stats.");

    if (vals["ep_dbname"] != dbname) {
        std::cerr << "Expected dbname = '" << dbname << "'"
                  << ", got '" << vals["ep_dbname"] << "'" << std::endl;
        return FAIL;
    }

    check(access(dbname.c_str(), F_OK) != -1, "I expected the whitespace db to exist");
    return SUCCESS;
}

static enum test_result test_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "k"), "Expected miss.");
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

    if (isPersistentBucket(h, h1)) {
        wait_for_flusher_to_settle(h, h1);

        std::stringstream error1, error2;
        error1 << "Expected ep_total_persisted >= num_keys (" << num_keys << ")";
        error2 << "Expected ep_total_persisted <= num_sets*num_keys ("
               << num_sets*num_keys << ")";

        // The flusher could of ran > 1 times. We can only assert
        // that we persisted between num_keys and upto num_keys*num_sets
        check(get_int_stat(h, h1, "ep_total_persisted") >= num_keys,
              error1.str().c_str());
        check(get_int_stat(h, h1, "ep_total_persisted") <= num_sets*num_keys,
              error2.str().c_str());
    }
    return SUCCESS;
}

extern "C" {
    static void conc_del_set_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);
        item *it = NULL;

        for (int i = 0; i < 5000; ++i) {
            store(hp->h, hp->h1, NULL, OPERATION_ADD,
                  "key", "somevalue", &it);
            hp->h1->release(hp->h, NULL, it);
            checkeq(ENGINE_SUCCESS,
                    store(hp->h, hp->h1, NULL, OPERATION_SET,
                          "key", "somevalue", &it),
                    "Error setting.");
            hp->h1->release(hp->h, NULL, it);
            // Ignoring the result here -- we're racing.
            del(hp->h, hp->h1, "key", 0, 0);
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

    if (isWarmupEnabled(h, h1)) {
        wait_for_flusher_to_settle(h, h1);

        testHarness.reload_engine(&h, &h1,
                                  testHarness.engine_path,
                                  testHarness.get_current_testcase()->cfg,
                                  true, false);
        wait_for_warmup_complete(h, h1);

        cb_assert(0 == get_int_stat(h, h1, "ep_warmup_dups"));
    }

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
            checkeq(ENGINE_SUCCESS,
                    store(msa->h, msa->h1, NULL, OPERATION_SET,
                          key.c_str(), "somevalue", &it),
                    "Set failure!");
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

    checkeq(100000, get_int_stat(h, h1, "curr_items"),
            "Mismatch in number of items inserted");
    checkeq(100000, get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno"),
            "Unexpected high sequence number");

    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_getl_delete_with_cas(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, "key", 0, 15),
            "Expected getl to succeed on key");
    item_info info;
    check(h1->get_item_info(h, nullptr, locked, &info),
          "Failed to get item info");

    checkeq(ENGINE_SUCCESS, del(h, h1, "key", info.cas, 0), "Expected SUCCESS");
    h1->release(h, nullptr, locked);

    return SUCCESS;
}

static enum test_result test_getl_delete_with_bad_cas(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,
                  "key", "value", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    uint64_t cas = last_cas;
    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, "key", 0, 15),
            "Expected getl to succeed on key");
    h1->release(h, nullptr, locked);

    checkeq(ENGINE_TMPFAIL, del(h, h1, "key", cas, 0), "Expected TMPFAIL");

    return SUCCESS;
}

static enum test_result test_getl_set_del_with_meta(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    const char *key = "key";
    const char *val = "value";
    const char *newval = "newvalue";
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, val, &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS, getl(h, h1, nullptr, &locked, key, 0, 15),
          "Expected getl to succeed on key");
    h1->release(h, nullptr, locked);

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
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected item to be locked");

    //do a del with meta
    del_with_meta(h, h1, key, strlen(key), 0, &itm_meta, last_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(),
          "Expected item to be locked");
    return SUCCESS;
}

static enum test_result test_getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "k1";
    uint16_t vbucketId = 0;
    uint32_t expiration = 25;

    item* locked;
    checkeq(ENGINE_KEY_ENOENT,
            getl(h, h1, nullptr, &locked, key, vbucketId, expiration),
          "expected the key to be missing...");

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "{\"lock\":\"data\"}",
                  &i, 0, vbucketId, 3600, PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed to store an item.");
    h1->release(h, NULL, i);

    /* retry getl, should succeed */
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, key, vbucketId, expiration),
            "Expected to be able to getl on first try");

    item_info info;
    check(h1->get_item_info(h, nullptr, locked, &info),
          "Failed to get item info");

    checkeq(std::string{"{\"lock\":\"data\"}"},
            std::string((const char*)info.value[0].iov_base,
                        info.value[0].iov_len),
            "Body was malformed.");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype,
            "Expected datatype to be JSON");
    h1->release(h, nullptr, locked);

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock's taken so this should fail */
    checkeq(ENGINE_TMPFAIL,
            getl(h, h1, nullptr, &locked, key, vbucketId, expiration),
            "Expected to fail getl on second try");

    checkne(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "lockdata2", &i, 0,
                  vbucketId),
            "Should have failed to store an item.");
    h1->release(h, NULL, i);

    /* wait another 10 seconds */
    testHarness.time_travel(10);

    /* retry set, should succeed */
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId),
            "Failed to store an item.");
    h1->release(h, NULL, i);

    /* point to wrong vbucket, to test NOT_MY_VB response */
    checkeq(ENGINE_NOT_MY_VBUCKET,
            getl(h, h1, nullptr, &locked, key, 10, expiration),
            "Should have received not my vbucket response");

    /* acquire lock, should succeed */
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, key, vbucketId, expiration),
            "Aquire lock should have succeeded");
    check(h1->get_item_info(h, nullptr, locked, &info),
          "Failed to get item info");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_RAW_BYTES), info.datatype,
            "Expected datatype to be RAW BYTES");
    h1->release(h, nullptr, locked);

    /* try an delete operation which should fail */
    uint64_t cas = 0;
    i = NULL;

    checkeq(ENGINE_TMPFAIL, del(h, h1, key, 0, 0), "Delete failed");


    /* bug MB 2699 append after getl should fail with ENGINE_TMPFAIL */

    testHarness.time_travel(26);

    char binaryData1[] = "abcdefg\0gfedcba";

    checkeq(ENGINE_SUCCESS,
            storeCasVb11(h, h1, NULL, OPERATION_SET, key,
                         binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0),
            "Failed set.");
    h1->release(h, NULL, i);

    /* acquire lock, should succeed */
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, key, vbucketId, expiration),
            "Aquire lock should have succeeded");
    h1->release(h, nullptr, locked);

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

    checkeq(ENGINE_SUCCESS,
            allocate(h, h1, NULL, &it, ekey, strlen(edata), 0, 2,
                     PROTOCOL_BINARY_RAW_BYTES, 0),
            "Allocation Failed");

    check(h1->get_item_info(h, NULL, it, &info),
          "Failed to get item info");

    memcpy(info.value[0].iov_base, edata, strlen(edata));

    checkeq(ENGINE_SUCCESS,
            h1->store(h, NULL, it, &cas, OPERATION_SET, DocumentState::Alive),
           "Failed to Store item");
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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, ekey, edata, &i, 0, vbucketId),
            "Failed to store an item.");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static enum test_result test_unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "k2";
    uint16_t vbucketId = 0;

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    if (eviction_policy == "full_eviction") {
        checkeq(ENGINE_TMPFAIL,
                unl(h, h1, nullptr, key, vbucketId),
                "expected a TMPFAIL");
    } else {
        checkeq(ENGINE_KEY_ENOENT,
                unl(h, h1, nullptr, key, vbucketId),
                "expected the key to be missing...");
    }

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId),
            "Failed to store an item.");
    h1->release(h, NULL, i);

    /* getl, should succeed */
    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, key, vbucketId, 0),
            "Expected to be able to getl on first try");
    item_info info;
    checkeq(true, h1->get_item_info(h, nullptr, locked, &info),
            "failed to get item info");
    uint64_t cas = info.cas;
    h1->release(h, nullptr, locked);

    /* lock's taken unlocking with a random cas value should fail */
    checkeq(ENGINE_TMPFAIL,
            unl(h, h1, nullptr, key, vbucketId),
            "Expected to fail getl on second try");

    checkeq(ENGINE_SUCCESS,
            unl(h, h1, nullptr, key, vbucketId, cas),
            "Expected to succed unl with correct cas");

    /* acquire lock, should succeed */
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, key, vbucketId, 0),
            "Lock should work after unlock");
    h1->release(h, nullptr, locked);

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock has expired, unl should fail */
    checkeq(ENGINE_TMPFAIL,
            unl(h, h1, nullptr, key, vbucketId, last_cas),
            "Expected to fail unl on lock timeout");

    return SUCCESS;
}

static enum test_result test_unl_nmvb(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "k2";
    uint16_t vbucketId = 10;

    checkeq(ENGINE_NOT_MY_VBUCKET,
            unl(h, h1, nullptr, key, vbucketId),
          "expected NOT_MY_VBUCKET to unlocking a key in a vbucket we don't own");

    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
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

    checkeq(ENGINE_SUCCESS,
            allocate(h, h1, NULL, &i, key, 10, 0, 0,
            PROTOCOL_BINARY_RAW_BYTES, 0),
            "Allocation failed.");

    Item *it = reinterpret_cast<Item*>(i);
    it->setCas(1234);

    uint64_t cas = 0;
    checkeq(ENGINE_KEY_ENOENT,
            h1->store(h, NULL, i, &cas, OPERATION_SET, DocumentState::Alive),
            "Expected not found");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed to set.");
    h1->release(h, NULL, i);

    item_info info;
    uint32_t flags = 828258;
    check(get_item_info(h, h1, &info, "key"), "Failed to get value.");
    cb_assert(info.flags != flags);

    checkeq(ENGINE_SUCCESS,
            storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                         "newvalue", strlen("newvalue"), flags, &i, 0, 0),
            "Failed to set again.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint64_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i),
            "Failed to add value.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error getting item info");
    checkeq(vb_uuid, info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, info.seqno, "Expected valid sequence number");

    checkeq(ENGINE_NOT_STORED,
            store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i),
            "Failed to fail to re-add value.");
    h1->release(h, NULL, i);

    // This aborts on failure.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i),
            "Failed to add value again.");

    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_add_add_with_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD, "key", "somevalue", &i),
            "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    check(h1->get_item_info(h, NULL, i, &info),
          "Should be able to get info");

    item *i2 = NULL;
    checkeq(ENGINE_KEY_EEXISTS,
            store(h, h1, NULL, OPERATION_ADD, "key",
                  "somevalue", &i2, info.cas),
            "Should not be able to add the key two times");

    h1->release(h, NULL, i);
    h1->release(h, NULL, i2);
    return SUCCESS;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed to do initial set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    checkeq(ENGINE_SUCCESS,
            get(h, h1, NULL, &i, "key", 0),
            "Failed to get value.");

    item_info info;
    check(h1->get_item_info(h, NULL, i, &info), "Failed to get item info.");
    h1->release(h, NULL, i);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_CAS, "key", "winCas", &i, info.cas),
            "Failed to store CAS");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "winCas", 6);

    uint64_t cval = 99999;
    checkeq(ENGINE_KEY_ENOENT,
            store(h, h1, NULL, OPERATION_CAS, "non-existing", "winCas",
                  &i, cval),
            "CAS for non-existing key returned the wrong error code");
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    item_info info;
    uint64_t vb_uuid = 0;
    uint64_t high_seqno = 0;

    memset(&info, 0, sizeof(info));

    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) != ENGINE_SUCCESS,
          "Failed to fail to replace non-existing value.");

    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i),
            "Failed to set value.");
    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i),
            "Failed to replace existing value.");
    h1->release(h, NULL, i);

    check(get_item_info(h, h1, &info, "key"), "Error getting item info");

    checkeq(vb_uuid, info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, info.seqno, "Expected valid sequence number");

    check_key_value(h, h1, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    touch(h, h1, NULL, 0, (time(NULL) + 10));
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL,
            last_status.load(), "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_TOUCH, 0, 0, NULL, 0, "akey", 4);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace),
            "Failed to call touch");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Testing invalid arguments");
    cb_free(request);

    // Try to touch an unknown item...
    touch(h, h1, "mykey", 0, (time(NULL) + 10));
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            "Testing unknown key");

    // illegal vbucket
    touch(h, h1, "mykey", 5, (time(NULL) + 10));
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
            "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue"));

    check(get_meta(h, h1, "mykey"), "Get meta failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Get meta failed");

    uint64_t curr_cas = last_meta.cas;
    time_t curr_exptime = last_meta.exptime;
    uint64_t curr_revseqno = last_meta.revSeqno;

    touch(h, h1, "mykey", 0, (time(NULL) + 10));
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "touch mykey");

    check(get_meta(h, h1, "mykey"), "Get meta failed");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Get meta failed");

    check(last_meta.cas != curr_cas, "touch should have updated the CAS");
    check(last_meta.exptime != curr_exptime, "touch should have updated the expiry time");
    check(last_meta.revSeqno == curr_revseqno + 1, "touch should have incremented rev seqno");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, NULL, &itm, "mykey", 0), "Item should be gone");
    return SUCCESS;
}

static enum test_result test_touch_mb7342(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    const char *key = "MB-7342";
    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "v", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);

    touch(h, h1, key, 0, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(), "touch key");

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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "v", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    touch(h, h1, key, 0, 3600); // A new expiration time remains in the same.
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(),
            "touch key");

    return SUCCESS;
}

static enum test_result test_gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    gat(h, h1, NULL, 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_GAT, 0, 0, NULL, 0, "akey", 4);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace),
            "Failed to call gat");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Testing invalid arguments");
    cb_free(request);

    // Try to gat an unknown item...
    gat(h, h1, "mykey", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            "Testing unknown key");

    // illegal vbucket
    gat(h, h1, "mykey", 5, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
            last_status.load(), "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                  &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
            strlen("{\"some\":\"value\"}"));

    gat(h, h1, "mykey", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(), "gat mykey");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            last_datatype.load(), "Expected datatype to be JSON");
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
    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, NULL, &itm, "mykey", 0), "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gatq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // key is a mandatory field!
    gat(h, h1, NULL, 0, 10, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL,
            last_status.load(), "Testing invalid arguments");

    // extlen is a mandatory field!
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_GATQ, 0, 0, NULL, 0, "akey", 4);
    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace),
            "Failed to call gatq");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL,
            last_status.load(), "Testing invalid arguments");
    cb_free(request);

    // Try to gatq an unknown item...
    last_status = static_cast<protocol_binary_response_status>(0xffff);
    gat(h, h1, "mykey", 0, 10, true);

    // We should not have sent any response!
    checkeq((protocol_binary_response_status)0xffff,
            last_status.load(), "Testing unknown key");

    // illegal vbucket
    gat(h, h1, "mykey", 5, 10, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
            last_status.load(),
            "Testing illegal vbucket");

    // Store the item!
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "mykey", "{\"some\":\"value\"}",
                  &itm, 0, 0, 3600, PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "mykey", "{\"some\":\"value\"}",
                    strlen("{\"some\":\"value\"}"));

    gat(h, h1, "mykey", 0, 10, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "gat mykey");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            last_datatype.load(), "Expected datatype to be JSON");
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
    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, NULL, &itm, "mykey", 0),
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gat_locked(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,
                  "key", "value", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, "key", 0, 15),
          "Expected getl to succeed on key");
    h1->release(h, nullptr, locked);

    gat(h, h1, "key", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(), "Expected tmp fail");
    check(last_body == "Lock Error", "Wrong error message");

    testHarness.time_travel(16);
    gat(h, h1, "key", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    testHarness.time_travel(11);
    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, NULL, &itm, "key", 0),
            "Expected value to be expired");

    return SUCCESS;
}

static enum test_result test_touch_locked(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value", &itm),
            "Failed to set key");
    h1->release(h, NULL, itm);

    item* locked = nullptr;
    checkeq(ENGINE_SUCCESS,
            getl(h, h1, nullptr, &locked, "key", 0, 15),
            "Expected getl to succeed on key");
    h1->release(h, nullptr, locked);

    touch(h, h1, "key", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, last_status.load(), "Expected tmp fail");
    check(last_body == "Lock Error", "Wrong error message");

    testHarness.time_travel(16);
    touch(h, h1, "key", 0, 10);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    testHarness.time_travel(11);
    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, NULL, &itm, "key", 0),
            "Expected value to be expired");

    return SUCCESS;
}

static enum test_result test_mb5215(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isWarmupEnabled(h, h1)) {
        return SKIPPED;
    }

    item *itm = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "coolkey", "cooler", &itm),
            "Failed set.");
    h1->release(h, NULL, itm);

    check_key_value(h, h1, "coolkey", "cooler", strlen("cooler"));

    // set new exptime to 111
    int expTime = time(NULL) + 111;

    touch(h, h1, "coolkey", 0, expTime);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(),
            "touch coolkey");

    //reload engine
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);

    //verify persisted expiration time
    const char *statkey = "key coolkey 0";
    int newExpTime;
    checkeq(ENGINE_SUCCESS,
            get(h, h1, NULL, &itm, "coolkey", 0),
            "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    checkeq(expTime, newExpTime, "Failed to persist new exptime");

    // evict key, touch expiration time, and verify
    evict_key(h, h1, "coolkey", 0, "Ejected.");

    expTime = time(NULL) + 222;
    touch(h, h1, "coolkey", 0, expTime);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(), "touch coolkey");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    checkeq(ENGINE_SUCCESS,
            get(h, h1, NULL, &itm, "coolkey", 0),
            "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    checkeq(expTime, newExpTime, "Failed to persist new exptime");

    return SUCCESS;
}

/* Testing functionality to store a value for a deleted item
 * and also retrieve the value of a deleted item
 */
static enum test_result test_delete_with_value(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    item *i = nullptr;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key1", "somevalue", &i),
            "Failed set");

    h1->release(h, nullptr, i);

    /* Store a deleted item first with CAS 0 */
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key1", "deletevalue", &i,
                  0, 0, 3600, 0x00, DocumentState::Deleted),
            "Failed delete with value");

    h1->release(h, nullptr, i);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key2", "somevalue", &i),
            "Failed set");

    item_info info;
    check(h1->get_item_info(h, nullptr, i, &info),
          "Getting item info failed");

    h1->release(h, nullptr, i);

    /* Store a deleted item with the existing CAS value */
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key2", "deletevaluewithcas",
                  &i, info.cas, 0, 3600, 0x00, DocumentState::Deleted),
            "Failed delete value with cas");

    h1->release(h, nullptr, i);

    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key2",
                  "newdeletevalue", &i, 0, 0, 3600, 0x00, DocumentState::Deleted),
            "Failed delete value with cas");

    wait_for_flusher_to_settle(h, h1);

    check(h1->get_item_info(h, nullptr, i, &info), "Getting item info failed");

    h1->release(h, nullptr, i);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key2",
                  "newdeletevaluewithcas", &i, info.cas, 0, 3600, 0x00,
                  DocumentState::Deleted), "Failed delete value with cas");

    h1->release(h, nullptr, i);

    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS,
            get(h, h1, nullptr, &i, "key2", 0 , DocumentState::Deleted),
                "Failed to get value");

    check(h1->get_item_info(h, nullptr, i, &info),
          "Getting item info failed");

    checkeq(static_cast<uint8_t>(DocumentState::Deleted),
            static_cast<uint8_t>(info.document_state),
            "document must be in deleted state");

    std::string buf(static_cast<char*>(info.value[0].iov_base),
                    info.value[0].iov_len);

    checkeq(0, buf.compare("newdeletevaluewithcas"), "Data mismatch");

    h1->release(h, nullptr, i);

    checkeq(ENGINE_KEY_ENOENT,
            get(h, h1, nullptr, &i, "key", 0, DocumentState::Alive),
                "Getting value should have failed");

    return SUCCESS;
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    checkeq(ENGINE_KEY_ENOENT,
            del(h, h1, "key", 0, 0), "Failed to fail initial delete.");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    uint64_t orig_cas = it->getCas();
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    uint64_t cas = 0;
    uint64_t vb_uuid = 0;
    mutation_descr_t mut_info;
    uint64_t high_seqno = 0;

    memset(&mut_info, 0, sizeof(mut_info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    checkeq(ENGINE_SUCCESS, del(h, h1, "key", &cas, 0, nullptr, &mut_info),
            "Failed remove with value.");
    check(orig_cas != cas, "Expected CAS to be updated on delete");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    checkeq(vb_uuid, mut_info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, mut_info.seqno, "Expected valid sequence number");

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
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    check(h1->get_item_info(h, NULL, i, &info),
          "Should be able to get info");
    h1->release(h, NULL, i);

    checkeq(ENGINE_KEY_EEXISTS, del(h, h1, "key", info.cas + 1, 0),
          "Didn't expect to be able to remove the item with wrong cas");

    checkeq(ENGINE_SUCCESS, del(h, h1, "key", info.cas, 0),
        "Subsequent delete with correct CAS did not succeed");

    return SUCCESS;
}

static enum test_result test_delete_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isWarmupEnabled(h, h1)) {
        return SKIPPED;
    }

    wait_for_persisted_value(h, h1, "key", "value1");

    checkeq(ENGINE_SUCCESS,
            del(h, h1, "key", 0, 0), "Failed remove with value.");

    wait_for_persisted_value(h, h1, "key", "value2");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check_key_value(h, h1, "key", "value2", 6);
    checkeq(ENGINE_SUCCESS,
            del(h, h1, "key", 0, 0), "Failed remove with value.");
    wait_for_flusher_to_settle(h, h1);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_get_delete_missing_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
           "Failed to get stats.");

    // TODO: This test needs to be skipped for forestdb as backend because
    // in that case we don't open and close on every operation. Thus, a get
    // after deleting the database file would still result in a SUCCESS.
    // In the future, regardless of the storage type, the resulting error
    // should be the same.
    std::string backend = vals["ep_backend"];
    if (backend == "forestdb") {
        return SKIPPED;
    }
    const char *key = "key";
    wait_for_persisted_value(h, h1, key, "value2delete");

    // whack the db file and directory where the key is stored
    std::string dbname = vals["ep_dbname"];
    rmdb(dbname.c_str());

    item *i = NULL;
    ENGINE_ERROR_CODE errorCode = get(h, h1, NULL, &i, key, 0);
    h1->release(h, NULL, i);

    // ep engine must be unaware of well-being of the db file as long as
    // the item is still in the memory
    checkeq(ENGINE_SUCCESS, errorCode, "Expected success for get");

    i = NULL;
    evict_key(h, h1, key);
    errorCode = get(h, h1, NULL, &i, key, 0);
    h1->release(h, NULL, i);

    // ep engine must be now aware of the ill-fated db file where
    // the item is supposedly stored
    checkeq(ENGINE_TMPFAIL, errorCode, "Expected tmp fail for get");

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

    if (isWarmupEnabled(h, h1)) {
        // Restart again, to verify we don't have any duplicates.
        testHarness.reload_engine(&h, &h1,
                                  testHarness.engine_path,
                                  testHarness.get_current_testcase()->cfg,
                                  true, false);
        wait_for_warmup_complete(h, h1);

        return get_int_stat(h, h1, "ep_warmup_dups") == 0 ? SUCCESS : FAIL;
    }
    return SUCCESS;
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
        check(set_vbucket_state(h, h1, 0, vbucket_state_dead),
              "Failed set set vbucket 0 dead.");
        vbucketDelete(h, h1, 0, "async=0");
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
                last_status.load(),
                "Expected vbucket deletion to work.");
        check(set_vbucket_state(h, h1, 0, vbucket_state_active),
              "Failed set set vbucket 0 active.");
        for (it = keys.begin(); it != keys.end(); ++it) {
            item *i;
            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i),
                    "Failed to store a value");
            h1->release(h, NULL, i);

        }
    }
    wait_for_flusher_to_settle(h, h1);

    if (isWarmupEnabled(h, h1)) {
        // Restart again, to verify no data loss.
        testHarness.reload_engine(&h, &h1,
                                  testHarness.engine_path,
                                  testHarness.get_current_testcase()->cfg,
                                  true, false);
        wait_for_warmup_complete(h, h1);
        return get_int_stat(h, h1, "ep_warmup_value_count", "warmup") == 10000 ? SUCCESS : FAIL;
    }
    return SUCCESS;
}

static enum test_result test_mb3169(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "set", "value", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "delete", "0", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "get", "getvalue", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_stat_to_be(h, h1, "ep_total_persisted", 3);

    evict_key(h, h1, "set", 0, "Ejected.");
    evict_key(h, h1, "delete", 0, "Ejected.");
    evict_key(h, h1, "get", 0, "Ejected.");

    checkeq(3, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected four items to be resident");

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "set", "value2", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    checkeq(2, get_int_stat(h, h1, "ep_num_non_resident"),
          "Expected mutation to mark item resident");

    checkeq(ENGINE_SUCCESS, del(h, h1, "delete", 0, 0),
            "Delete failed");

    checkeq(1, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected delete to remove non-resident item");

    check_key_value(h, h1, "get", "getvalue", 8);

    checkeq(0, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_mb5172(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isWarmupEnabled(h, h1)) {
        return SKIPPED;
    }

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key-1", "value-1", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key-2", "value-2", &i, 0, 0),
            "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);

    checkeq(0, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected all items to be resident");

    // restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    checkeq(0, get_int_stat(h, h1, "ep_num_non_resident"),
            "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_set_vbucket_out_of_range(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    check(!set_vbucket_state(h, h1, 10000, vbucket_state_active),
          "Shouldn't have been able to set vbucket 10000");
    return SUCCESS;
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    if (get_bool_stat(h, h1, "ep_flushall_enabled") == false) {
        check(set_param(h, h1, protocol_binary_engine_param_flush,
                        "flushall_enabled", "true"),
              "Set flushall_enabled should have worked");
    }
    check(get_bool_stat(h, h1, "ep_flushall_enabled"),
          "flushall wasn't enabled");

    // First try to delete something we know to not be there.
    checkeq(ENGINE_KEY_ENOENT,
            del(h, h1, "key", 0, 0),
            "Failed to fail initial delete.");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    set_degraded_mode(h, h1, NULL, true);
    checkeq(ENGINE_SUCCESS,
            h1->flush(h, NULL),
            "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_flush_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key"), "Expected key");
    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key2"), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9);

    set_degraded_mode(h, h1, NULL, true);
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL), "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key2"), "Expected missing key");

    wait_for_flusher_to_settle(h, h1);

    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    cb_assert(nonResident2 == nonResident);
    cb_assert(cacheSize2 == cacheSize);

    return SUCCESS;
}

static enum test_result test_flush_multiv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
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

    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key"), "Expected key");
    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key2", 2), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9, 2);

    set_degraded_mode(h, h1, NULL, true);
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL), "Failed to flush");
    set_degraded_mode(h, h1, NULL, false);

    vals.clear();
    checkeq(ENGINE_SUCCESS, h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    check(vals.find("ep_flush_all") != vals.end(),
          "Failed to get the status of flush_all");

    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"),
            "Expected missing key");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key2", 2),
            "Expected missing key");

    return SUCCESS;
}

static enum test_result test_flush_disabled(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // start an engine with disabled flush, the flush() should be noop and
    // we expect to see the key after flush()

    // store a key and check its existence
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    checkeq(ENGINE_ENOTSUP,
            h1->flush(h, NULL),
            "Flush should be disabled");
    //check the key
    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key"), "Expected key");

    // restart engine with flush enabled and redo the test, we expect flush to succeed
    if (isWarmupEnabled(h, h1)) {
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
        checkeq(ENGINE_SUCCESS,
                h1->flush(h, NULL), "Flush should be enabled");
        set_degraded_mode(h, h1, NULL, false);

        //expect missing key
        checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    }
    return SUCCESS;
}

static enum test_result test_CBD_152(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // turn off flushall_enabled parameter
    set_param(h, h1, protocol_binary_engine_param_flush, "flushall_enabled", "false");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Failed to set flushall_enabled param");

    // store a key and check its existence
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    checkeq(ENGINE_ENOTSUP, h1->flush(h, NULL), "Flush should be disabled");
    //check the key
    checkeq(ENGINE_SUCCESS, verify_key(h, h1, "key"), "Expected key");

    // turn on flushall_enabled parameter
    set_param(h, h1, protocol_binary_engine_param_flush, "flushall_enabled", "true");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Failed to set flushall_enabled param");
    // flush should succeed
    set_degraded_mode(h, h1, NULL, true);
    checkeq(ENGINE_SUCCESS, h1->flush(h, NULL), "Flush should be enabled");
    set_degraded_mode(h, h1, NULL, false);
    //expect missing key
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result set_max_cas_mb21190(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t max_cas = get_ull_stat(h, h1, "vb_0:max_cas", "vbucket-details 0");
    std::string max_cas_str = std::to_string(max_cas+1);
    set_param(h, h1, protocol_binary_engine_param_vbucket,
              "max_cas", max_cas_str.data(), 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failed to set_param max_cas");
    checkeq(max_cas + 1,
            get_ull_stat(h, h1, "vb_0:max_cas", "vbucket-details 0"),
            "max_cas didn't change");
    set_param(h, h1, protocol_binary_engine_param_vbucket,
              "max_cas", max_cas_str.data(), 1);
    checkeq(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, last_status.load(),
            "Expected not my vbucket for vb 1");
    set_param(h, h1, protocol_binary_engine_param_vbucket,
              "max_cas", "JUNK", 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Expected EINVAL");
    return SUCCESS;
}

static enum test_result warmup_mb21769(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isWarmupEnabled(h, h1)) {
        return SKIPPED;
    }

    // Validate some VB data post warmup
    // VB 0 will be empty
    // VB 1 will not be empty
    // VB 2 will not be empty and will have had set_state as the final ops

    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed to set vbucket state for vb1");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active),
          "Failed to set vbucket state for vb2");

    const int num_items = 10;
    write_items(h, h1, num_items, 0, "vb1", "value", 0/*expiry*/, 1/*vb*/);
    write_items(h, h1, num_items, 0, "vb2", "value", 0/*expiry*/, 2/*vb*/);
    wait_for_flusher_to_settle(h, h1);

    // flip replica to active to drive more _local writes
    check(set_vbucket_state(h, h1, 2, vbucket_state_replica),
          "Failed to set vbucket state (replica) for vb2");
    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 2, vbucket_state_active),
          "Failed to set vbucket state (replica) for vb2");
    wait_for_flusher_to_settle(h, h1);

    // Force a shutdown so the warmup will create failover entries
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);

    wait_for_warmup_complete(h, h1);

    // values of interested stats for each VB
    std::array<uint64_t, 3> high_seqnos = {{0, num_items, num_items}};
    std::array<uint64_t, 3> snap_starts = {{0, num_items, num_items}};
    std::array<uint64_t, 3> snap_ends = {{0, num_items, num_items}};
    // we will check the seqno of the 0th entry of each vbucket's failover table
    std::array<uint64_t, 3> failover_entry0 = {{0, num_items, num_items}};

    for (uint64_t vb = 0; vb <= 2; vb++) {
        std::string vb_prefix = "vb_" + std::to_string(vb) + ":";
        std::string high_seqno = vb_prefix + "high_seqno";
        std::string snap_start = vb_prefix + "last_persisted_snap_start";
        std::string snap_end = vb_prefix + "last_persisted_snap_end";
        std::string fail0 = vb_prefix + "0:seq";
        std::string vb_group_key = "vbucket-seqno " + std::to_string(vb);
        std::string failovers_key = "failovers " + std::to_string(vb);

        checkeq(high_seqnos[vb],
                get_ull_stat(h, h1, high_seqno.c_str(), vb_group_key.c_str()),
                std::string("high_seqno incorrect vb:" + std::to_string(vb)).c_str());
        checkeq(snap_starts[vb],
                get_ull_stat(h, h1, snap_start.c_str(), vb_group_key.c_str()),
                std::string("snap_start incorrect vb:" + std::to_string(vb)).c_str());
        checkeq(snap_ends[vb],
                get_ull_stat(h, h1, snap_end.c_str(), vb_group_key.c_str()),
                std::string("snap_end incorrect vb:" + std::to_string(vb)).c_str());
        checkeq(failover_entry0[vb],
                get_ull_stat(h, h1, fail0.c_str(), failovers_key.c_str()),
                std::string("failover table entry 0 is incorrect vb:" + std::to_string(vb)).c_str());
    }

    return SUCCESS;
}

/**
 * Callback from the document API being called after the CAS was assigned
 * to the object. We're allowed to modify the content, so let's just change
 * the string.
 *
 * @param info info about the document
 */
static void pre_link_doc_callback(item_info& info) {
    checkne(uint64_t(0), info.cas, "CAS value should be set");
    // mock the actual value so we can see it was changed
    memcpy(info.value[0].iov_base, "valuesome", 9);
}

/**
 * Verify that we've hooked into the checkpoint and that the pre-link
 * document api method is called.
 */
static test_result pre_link_document(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item_info info;
    item* it;

    PreLinkFunction function = pre_link_doc_callback;
    testHarness.set_pre_link_function(function);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key", "somevalue", &it),
            "Failed set.");
    h1->release(h, nullptr, it);
    testHarness.set_pre_link_function({});

    // Fetch the value and verify that the callback was called!
    checkeq(ENGINE_SUCCESS, get(h, h1, nullptr, &it, "key", 0), "get failed");
    check(h1->get_item_info(h, nullptr, it, &info), "Failed to get item info.");
    checkeq(0, memcmp(info.value[0].iov_base, "valuesome", 9),
           "Expected value to be modified");
    h1->release(h, nullptr, it);

    return SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////
// Test manifest //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_basic";

BaseTestCase testsuite_testcases[] = {
        TestCase("test alloc limit", test_alloc_limit, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test_memory_tracking", test_memory_tracking, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test total memory limit", test_memory_limit,
                 test_setup, teardown,
                 "max_size=10240000;ht_locks=1;ht_size=3;"
                 "chk_remover_stime=1;chk_period=60",
                 prepare_ep_bucket, cleanup),
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
        TestCase("unl not my vbucket", test_unl_nmvb,
                 test_setup, teardown, NULL, prepare, cleanup),
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
        TestCase("replace", test_replace, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test touch", test_touch, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test touch (MB-7342)", test_touch_mb7342, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test touch (MB-10277)", test_touch_mb10277, test_setup,
                 teardown, NULL, prepare_ep_bucket, cleanup),
        TestCase("test gat", test_gat, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test gatq", test_gatq, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test locked gat", test_gat_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test locked touch", test_touch_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test mb5215", test_mb5215, test_setup, teardown,
                 NULL, prepare_ep_bucket, cleanup),
        TestCase("delete", test_delete, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("delete with value", test_delete_with_value, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("set/delete", test_set_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set/delete (invalid cas)", test_set_delete_invalid_cas,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete/set/delete", test_delete_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("get/delete with missing db file", test_get_delete_missing_file,
                 test_setup, teardown, NULL, prepare_ep_bucket, cleanup),
        TestCase("retain rowid over a soft delete", test_bug2509,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("vbucket deletion doesn't affect new data", test_bug7023,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("non-resident decrementers", test_mb3169,
                 test_setup, teardown, NULL, prepare_ep_bucket, cleanup),
        TestCase("resident ratio after warmup", test_mb5172,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set vb 10000", test_set_vbucket_out_of_range,
                 test_setup, teardown, "max_vbuckets=1024", prepare, cleanup),
        TestCase("flush", test_flush, test_setup, teardown,
                 NULL,
                 /* TODO Ephemeral: FLUSH currently not working*/prepare_skip_broken_under_ephemeral,
                 cleanup),
        TestCase("flush with stats", test_flush_stats, test_setup, teardown,
                 "flushall_enabled=true;chk_remover_stime=1;chk_period=60",
                 /* TODO Ephemeral: FLUSH currently not working*/prepare_skip_broken_under_ephemeral,
                 cleanup),
        TestCase("flush multi vbuckets", test_flush_multiv,
                 test_setup, teardown,
                 "flushall_enabled=true;max_vbuckets=16;ht_size=7;ht_locks=3",
                 /* TODO Ephemeral: FLUSH currently not working*/prepare_skip_broken_under_ephemeral,
                 cleanup),
        TestCase("flush_disabled", test_flush_disabled, test_setup, teardown,
                 "flushall_enabled=false;max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup),
        TestCase("flushall params", test_CBD_152, test_setup, teardown,
                 "flushall_enabled=true;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3",
                 /* TODO Ephemeral: FLUSH currently not working*/prepare_skip_broken_under_ephemeral,
                 cleanup),
        TestCase("set max_cas MB21190", set_max_cas_mb21190, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("warmup_mb21769", warmup_mb21769, test_setup, teardown, nullptr,
                 prepare, cleanup),

        TestCase("pre_link_document", pre_link_document, test_setup, teardown, nullptr,
                 prepare, cleanup),

        // sentinel
        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
