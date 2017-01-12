/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
 * Unit tests for Warmup-related functions.
 *
 */

#include "evp_engine_test.h"

#include "ep_engine.h"

#include <platform/sized_buffer.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

class WarmupDisabledTest : public EventuallyPersistentEngineTest {

    void SetUp() override {
        config_string = "warmup=false";
        EventuallyPersistentEngineTest::SetUp();
    }
};

// Mock implementation of an ADD_STAT callback.
class MockAddStat {
public:
    MOCK_CONST_METHOD2(Callback, void(std::string key, std::string value));

    // Static method to act as trampoline to the GoogleMock object method.
    static void trampoline(const char *key, const uint16_t klen,
                           const char *val, const uint32_t vlen,
                           const void *cookie) {
        auto* object = reinterpret_cast<const MockAddStat*>(cookie);
        object->Callback({key, klen}, {val, vlen});
    }
};

// Check we get the expected stats (i.e. none) when Warmup is disabled.
TEST_F(WarmupDisabledTest, Stats) {

    std::string key{"warmup"};
    MockAddStat add_stat;
    EXPECT_CALL(add_stat, Callback("ep_warmup", ::testing::_)).Times(0);
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              engine->getStats(&add_stat,
                               key.data(),
                               key.size(),
                               MockAddStat::trampoline));
}
