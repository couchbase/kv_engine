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

#include "../mock/mock_add_stat_fn.h"
#include <folly/portability/GTest.h>
#include <memcached/tracer.h>

class WarmupDisabledTest : public EventuallyPersistentEngineTest {

    void SetUp() override {
        config_string = "warmup=false";
        EventuallyPersistentEngineTest::SetUp();
    }
};

// Check we get the expected stats (i.e. none) when Warmup is disabled.
TEST_F(WarmupDisabledTest, Stats) {
    MockAddStat add_stat;
    cb::tracing::Traceable cookie;
    using ::testing::_;
    EXPECT_CALL(add_stat, callback("ep_warmup", _, _)).Times(0);
    EXPECT_EQ(
            ENGINE_KEY_ENOENT,
            engine->getStats(&cookie, "warmup", {}, add_stat.asStdFunction()));
}
