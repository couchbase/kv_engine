/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
 * Unit tests for the EventuallyPersistentEngine class.
 */

#include "evp_engine_test.h"

#include "ep_engine.h"
#include "programs/engine_testapp/mock_server.h"
#include <platform/dirutils.h>

void EventuallyPersistentEngineTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    CouchbaseDirectoryUtilities::rmrf(test_dbname);

    // Setup an engine with a single active vBucket.
    EXPECT_EQ(ENGINE_SUCCESS,
              create_instance(1, get_mock_server_api, &handle))
        << "Failed to create ep engine instance";
    EXPECT_EQ(1, handle->interface) << "Unexpected engine handle version";
    engine_v1 = reinterpret_cast<ENGINE_HANDLE_V1*>(handle);

    engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);
    std::string config = "dbname=" + std::string(test_dbname);
    EXPECT_EQ(ENGINE_SUCCESS, engine->initialize(config.c_str()))
        << "Failed to initialize engine.";

    // Wait for warmup to complete.
    while (engine->getEpStore()->isWarmingUp()) {
        usleep(10);
    }

    // Once warmup is complete, set VB to active.
    engine->setVBucketState(vbid, vbucket_state_active, false);
}

void EventuallyPersistentEngineTest::TearDown() {
    // Need to force the destroy (i.e. pass true) because
    // NonIO threads may have been disabled (see DCPTest subclass).
    engine_v1->destroy(handle, true);
    destroy_mock_event_callbacks();
    destroy_engine();
    // Cleanup any files we created.
    CouchbaseDirectoryUtilities::rmrf(test_dbname);
}

void EventuallyPersistentEngineTest::store_item(uint16_t vbid,
                                                const std::string& key,
                                                const std::string& value) {
    Item item(key.c_str(), key.size(), /*flags*/0, /*exp*/0, value.c_str(),
              value.size());
    uint64_t cas;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->store(NULL, &item, &cas, OPERATION_SET, vbid));
}

const char EventuallyPersistentEngineTest::test_dbname[] = "ep_engine_ep_unit_tests_db";
