/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Testsuite for checkpoint functionality in ep-engine.
 */
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

// Helper functions ///////////////////////////////////////////////////////////

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_create_new_checkpoint(EngineIface* h) {
    // Inserting more than 5 items (see testcase config) will cause a new open
    // checkpoint to be created.

    write_items(h, 5);
    wait_for_flusher_to_settle(h);

    checkeq(0,
            get_int_stat(h, "vb_0:last_closed_checkpoint_id", "checkpoint 0"),
            "Last closed checkpoint Id for VB 0 should still be 0 after "
            "storing 5 items");

    // Store 1 more - should push it over to the next checkpoint.
    write_items(h, 1, 5);
    wait_for_flusher_to_settle(h);

    checkeq(1,
            get_int_stat(h, "vb_0:last_closed_checkpoint_id", "checkpoint 0"),
            "Last closed checkpoint Id for VB 0 should increase to 1 after "
            "storing 6 items");

    createCheckpoint(h);
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected success response from creating a new checkpoint");

    checkeq(2,
            get_int_stat(h, "vb_0:last_closed_checkpoint_id", "checkpoint 0"),
            "Last closed checkpoint Id for VB 0 should be 2");

    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Checkpoint,
                      "chk_max_items",
                      "1000"),
            "Failed to set checkpoint_max_item param");
    checkeq(cb::engine_errc::success,
            set_param(h, EngineParamCategory::Checkpoint, "chk_period", "100"),
            "Failed to set checkpoint_period param");
    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Checkpoint, "max_checkpoints", "2"),
            "Failed to set max_checkpoints param");

    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Checkpoint, "chk_max_items", "5"),
            "Expected to have an invalid value error for checkpoint_max_items "
            "param");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Checkpoint, "chk_period", "0"),
            "Expected to have an invalid value error for checkpoint_period "
            "param");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(
                    h, EngineParamCategory::Checkpoint, "max_checkpoints", "1"),
            "Expected to have an invalid value error for max_checkpoints "
            "param");

    return SUCCESS;
}

static enum test_result test_checkpoint_create(EngineIface* h) {
    for (int i = 0; i < 5001; i++) {
        char key[8];
        sprintf(key, "key%d", i);
        checkeq(cb::engine_errc::success,
                store(h, nullptr, StoreSemantics::Set, key, "value"),
                "Failed to store an item.");
    }
    checkeq(2,
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint"),
            "New checkpoint wasn't create after 5001 item creates");
    checkeq(1,
            get_int_stat(h, "vb_0:num_open_checkpoint_items", "checkpoint"),
            "New open checkpoint should has only one dirty item");
    return SUCCESS;
}

static enum test_result test_checkpoint_timeout(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Failed to store an item.");
    testHarness->time_travel(600);
    wait_for_stat_to_be(h, "vb_0:open_checkpoint_id", 2, "checkpoint");
    return SUCCESS;
}

static enum test_result test_checkpoint_deduplication(EngineIface* h) {
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 4500; j++) {
            char key[8];
            sprintf(key, "key%d", j);
            checkeq(cb::engine_errc::success,
                    store(h, nullptr, StoreSemantics::Set, key, "value"),
                    "Failed to store an item.");
        }
    }
    // 4500 keys + 1x checkpoint_start + 1x set_vbucket_state.
    wait_for_stat_to_be(h, "vb_0:num_checkpoint_items", 4502, "checkpoint");
    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char* default_dbname = "./ep_testsuite_checkpoint.db";

BaseTestCase testsuite_testcases[] = {
        TestCase("checkpoint: create a new checkpoint",
                 test_create_new_checkpoint,
                 test_setup,
                 teardown,
                 "chk_max_items=5;item_num_based_new_chk=true;chk_period=600",
                 prepare,
                 cleanup),
        TestCase("checkpoint: validate checkpoint config params",
                 test_validate_checkpoint_params,
                 test_setup,
                 teardown,
                 nullptr,
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
                 "chk_max_items=5000;chk_period=600;checkpoint_memory_recovery_"
                 "upper_mark=0;checkpoint_memory_recovery_lower_mark=0",
                 prepare,
                 cleanup),
        TestCase("test checkpoint deduplication",
                 test_checkpoint_deduplication,
                 test_setup,
                 teardown,
                 "chk_max_items=5000;"
                 "chk_period=600;"
                 "chk_expel_enabled=false",
                 /* Checkpoint expelling needs to be disabled for this test
                  * because the test checks that 4500 items created 5 times are
                  * correctly de-duplicated (i.e. 4 * 4500 items are duplicated
                  * away). If expelling is enabled it is possible that some
                  * items will be expelled and hence will not get duplicated
                  * away.  Therefore the expected number of items in the
                  * checkpoint will not match.
                  */
                 prepare,
                 cleanup),

        TestCase(
                nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
