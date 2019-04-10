/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#pragma once

#include "vbucket_test.h"

#include "memory_tracker.h"
#include "objectregistry.h"

#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

class ItemCompressorTest
    : public ::testing::TestWithParam<item_eviction_policy_t>,
      public VBucketTestBase {
public:
    ItemCompressorTest() : VBucketTestBase(GetParam()) {
    }

    static void SetUpTestCase() {
        // Setup the MemoryTracker.
        MemoryTracker::getInstance(*get_mock_server_api()->alloc_hooks);
    }

    static void TearDownTestCase() {
        MemoryTracker::destroyInstance();
    }

protected:
    void SetUp() override {
        // Setup object registry. As we do not create a full ep-engine, we
        // use the "initial_tracking" for all memory tracking".
        ObjectRegistry::setStats(&mem_used);
    }

    void TearDown() override {
        ObjectRegistry::setStats(nullptr);
    }

    std::atomic<size_t> mem_used{0};
};
