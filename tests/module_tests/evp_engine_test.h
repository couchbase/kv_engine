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

#pragma once

#include "config.h"

#include <memcached/engine.h>

#include <gtest/gtest.h>

class EventuallyPersistentEngine;

class EventuallyPersistentEngineTest : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    /* Helper methods for tests */

    /* Stores an item into the given vbucket. */
    void store_item(uint16_t vbid, const std::string& key,
                    const std::string& value);

    std::string config_string;

    static const char test_dbname[];

    const uint16_t vbid = 0;

    ENGINE_HANDLE* handle;
    ENGINE_HANDLE_V1* engine_v1;
    EventuallyPersistentEngine* engine;
};
