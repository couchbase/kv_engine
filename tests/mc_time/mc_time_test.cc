/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "config.h"

#include <daemon/mc_time.h>
#include <daemon/memcached.h>
#include <gtest/gtest.h>

class McTimeTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        executorPool = std::make_unique<ExecutorPool>(0);
        mc_time_init_epoch();
    }

    void SetUp() override {
        // tick for each test
        mc_time_clock_tick();

        now = mc_time_get_current_time();
        epoch = mc_time_convert_to_abs_time(0);
    }

    time_t now = 0;
    time_t epoch = 0;
};

using namespace std::chrono;

// Basic expectations
TEST_F(McTimeTest, relative) {
    EXPECT_EQ(now + 100, mc_time_convert_to_real_time(100));
}

TEST_F(McTimeTest, absolute) {
    // Exceed 30 days from now as an absolute
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(1).count();
    EXPECT_EQ(
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count(),
            mc_time_convert_to_real_time(ts));
}

TEST_F(McTimeTest, absolute_less_than_epoch) {
    auto ts =
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count();

    // If this failed, has the test system got a bad clock?
    EXPECT_EQ(1, mc_time_convert_to_real_time(ts)) << "Check your system time";
}

TEST_F(McTimeTest, max) {
    EXPECT_EQ(std::numeric_limits<rel_time_t>::max() - epoch,
              mc_time_convert_to_real_time(std::numeric_limits<time_t>::max()));
}

TEST_F(McTimeTest, min) {
    EXPECT_EQ(std::numeric_limits<rel_time_t>::min(),
              mc_time_convert_to_real_time(std::numeric_limits<time_t>::min()));
}

TEST_F(McTimeTest, zero) {
    EXPECT_EQ(0, mc_time_convert_to_real_time(0));
}
