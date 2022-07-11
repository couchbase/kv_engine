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
    // Inserting more than 1 items (see testcase config) will cause a new open
    // checkpoint to be created.

    write_items(h, 1);
    wait_for_flusher_to_settle(h);

    checkeq(1,
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint 0"),
            "The open checkpoint id for VB 0 should still be 1 after "
            "storing 1 items");

    // Store 1 more - should push it over to the next checkpoint.
    write_items(h, 1, 1);
    wait_for_flusher_to_settle(h);

    checkeq(2,
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint 0"),
            "The open checkpoint id for VB 0 should increase to 2 after "
            "storing 6 items");

    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Checkpoint, "max_checkpoints", "2"),
            "Failed to set max_checkpoints param");

    checkeq(cb::engine_errc::invalid_arguments,
            set_param(
                    h, EngineParamCategory::Checkpoint, "max_checkpoints", "1"),
            "Expected to have an invalid value error for max_checkpoints "
            "param");

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
        TestCase("create a new checkpoint",
                 test_create_new_checkpoint,
                 test_setup,
                 teardown,
                 "checkpoint_max_size=1",
                 prepare,
                 cleanup),
        TestCase("validate checkpoint config params",
                 test_validate_checkpoint_params,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test checkpoint deduplication",
                 test_checkpoint_deduplication,
                 test_setup,
                 teardown,
                 // Notes on config:
                 // - Testing deduplication, so we need to ensure that all items
                 //   are queued into a single checkpoint. So we set
                 //   checkpoint_max_size reasonably high.
                 // - ItemExpel needs to be disabled for this test because the
                 //   test checks that 4500 items created 5 times are correctly
                 //   de-duplicated (i.e. 4 * 4500 items are duplicated away).
                 //   If expelling is enabled it is possible that some item will
                 //   be expelled and hence will not get duplicated away.
                 "checkpoint_max_size=10485760;chk_expel_enabled=false",
                 prepare,
                 cleanup),

        TestCase(
                nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
