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

#include <platform/cbassert.h>
#include <JSON_checker.h>


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

    rv = h1->allocate(h, NULL, &it, "key", 3, 20 * 1024 * 1024, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
    checkeq(ENGINE_SUCCESS, rv, "Allocated 20MB item");
    h1->release(h, NULL, it);

    rv = h1->allocate(h, NULL, &it, "key", 3, (20 * 1024 * 1024) + 1, 0, 0,
                      PROTOCOL_BINARY_RAW_BYTES);
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
    if (vals["ep_dbname"] != std::string(WHITESPACE_DB)) {
        std::cerr << "Expected dbname = ``" << WHITESPACE_DB << "''"
                  << ", got ``" << vals["ep_dbname"] << "''" << std::endl;
        return FAIL;
    }

    check(access(WHITESPACE_DB, F_OK) != -1, "I expected the whitespace db to exist");
    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

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

        // sentinel
        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
