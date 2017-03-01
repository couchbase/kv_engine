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
 * Testsuite for checkpoint functionality in ep-engine.
 */

#include "config.h"

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

// Helper functions ///////////////////////////////////////////////////////////

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_create_new_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Inserting more than 500 items will cause a new open checkpoint with id 2
    // to be created.

    for (int j = 0; j < 600; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        item *i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      ss.str().c_str(), &i, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    createCheckpoint(h, h1);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success response from creating a new checkpoint");

    checkeq(3, get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id",
                            "checkpoint 0"),
            "Last closed checkpoint Id for VB 0 should be 3");

    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_max_items", "1000");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failed to set checkpoint_max_item param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_period", "100");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failed to set checkpoint_period param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "max_checkpoints", "2");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failed to set max_checkpoints param");

    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_max_items", "5");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Expected to have an invalid value error for checkpoint_max_items param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "chk_period", "0");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Expected to have an invalid value error for checkpoint_period param");
    set_param(h, h1, protocol_binary_engine_param_checkpoint, "max_checkpoints", "6");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Expected to have an invalid value error for max_checkpoints param");

    return SUCCESS;
}

static enum test_result test_checkpoint_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    for (int i = 0; i < 5001; i++) {
        char key[8];
        sprintf(key, "key%d", i);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0),
                "Failed to store an item.");
        h1->release(h, NULL, itm);
    }
    checkeq(3, get_int_stat(h, h1, "vb_0:open_checkpoint_id", "checkpoint"),
            "New checkpoint wasn't create after 5001 item creates");
    checkeq(1, get_int_stat(h, h1, "vb_0:num_open_checkpoint_items", "checkpoint"),
            "New open checkpoint should has only one dirty item");
    return SUCCESS;
}

static enum test_result test_checkpoint_timeout(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "value", &itm, 0, 0),
            "Failed to store an item.");
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
            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0),
                    "Failed to store an item.");
            h1->release(h, NULL, itm);
        }
    }
    // 4500 keys + 1x checkpoint_start + 1x set_vbucket_state.
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoint_items", 4502, "checkpoint");
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
            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0),
                    "Failed to store an item.");
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

        for (int j = 0; j < 10; ++j) {
            std::stringstream ss;
            ss << "key" << j;
            item *i;
            checkeq(ENGINE_SUCCESS,
                    store(hp->h, hp->h1, NULL, OPERATION_SET,
                          ss.str().c_str(), ss.str().c_str(), &i, 0, 0),
                    "Failed to store a value");
            hp->h1->release(hp->h, NULL, i);
        }

        createCheckpoint(hp->h, hp->h1);
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
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    ret = cb_join_thread(th);
    cb_assert(ret == 0);

    return SUCCESS;
}


// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_checkpoint";

BaseTestCase testsuite_testcases[] = {
        TestCase("checkpoint: create a new checkpoint",
                 test_create_new_checkpoint,
                 test_setup,
                 teardown,
                 "chk_max_items=500;item_num_based_new_chk=true",
                 prepare,
                 cleanup),
        TestCase("checkpoint: validate checkpoint config params",
                 test_validate_checkpoint_params,
                 test_setup,
                 teardown,
                 NULL,
                 prepare,
                 cleanup),
        TestCase("test checkpoint create",
                 test_checkpoint_create,
                 test_setup,
                 teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare,
                 cleanup),
        TestCase("test checkpoint timeout",
                 test_checkpoint_timeout,
                 test_setup,
                 teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare,
                 cleanup),
        TestCase("test checkpoint deduplication",
                 test_checkpoint_deduplication,
                 test_setup,
                 teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare,
                 cleanup),
        TestCase("checkpoint: collapse checkpoints",
                 test_collapse_checkpoints,
                 test_setup,
                 teardown,
                 "chk_max_items=500;max_checkpoints=5;chk_remover_stime=1;"
                 "enable_chk_merge=true",
                 prepare_ep_bucket, // Relies on stopping persistence to setup
                 // test state.
                 cleanup),
        TestCase("checkpoint: wait for persistence",
                 test_checkpoint_persistence,
                 test_setup,
                 teardown,
                 "chk_max_items=500;max_checkpoints=5;item_num_based_new_chk="
                 "true",
                 prepare_ep_bucket, // Relies on being able to wait for
                 // persistence.
                 cleanup),
        TestCase("test wait for persist vb del",
                 test_wait_for_persist_vb_del,
                 test_setup,
                 teardown,
                 NULL,
                 prepare_ep_bucket,
                 cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)};
