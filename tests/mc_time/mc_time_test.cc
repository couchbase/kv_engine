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

#include <daemon/executor.h>
#include <daemon/executorpool.h>
#include <daemon/mc_time.h>
#include <daemon/memcached.h>
#include <gtest/gtest.h>
#include <gsl/gsl>
#include <memory>

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
    EXPECT_EQ(now + 100, mc_time_convert_to_real_time(100, cb::NoExpiryLimit));
}

TEST_F(McTimeTest, absolute) {
    // Exceed 30 days from now as an absolute
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(1).count();
    EXPECT_EQ(
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count(),
            mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                         cb::NoExpiryLimit));
}

TEST_F(McTimeTest, absolute_less_than_epoch) {
    auto ts =
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count();

    // If this failed, has the test system got a bad clock?
    EXPECT_EQ(1,
              mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                           cb::NoExpiryLimit))
            << "Check your system time";
}

TEST_F(McTimeTest, max) {
    EXPECT_EQ(
            std::numeric_limits<rel_time_t>::max() - epoch,
            mc_time_convert_to_real_time(std::numeric_limits<rel_time_t>::max(),
                                         cb::NoExpiryLimit));
}

TEST_F(McTimeTest, min) {
    EXPECT_EQ(
            std::numeric_limits<rel_time_t>::min(),
            mc_time_convert_to_real_time(std::numeric_limits<rel_time_t>::min(),
                                         cb::NoExpiryLimit));
}

TEST_F(McTimeTest, zero) {
    EXPECT_EQ(0, mc_time_convert_to_real_time(0, cb::NoExpiryLimit));
}

TEST_F(McTimeTest, limited_relative) {
    EXPECT_EQ(now + 99, mc_time_convert_to_real_time(100, seconds(99)));
}

// Limiting to zero should work too
TEST_F(McTimeTest, limited_relative_zero) {
    EXPECT_EQ(now, mc_time_convert_to_real_time(100, seconds(0)));
}

TEST_F(McTimeTest, limited_absolute) {
    // Exceed 30 days from now as an absolute
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(1).count();
    EXPECT_EQ(now + 99,
              mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                           seconds(99)));
}

TEST_F(McTimeTest, limited_absolute_zero) {
    // Exceed 30 days from now as an absolute
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(2).count();

    auto rv = mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                           seconds(0));

    if (now == 0) {
        // mc_time will hit the case where the computed time is in the past so
        // returns 1
        EXPECT_EQ(1, rv);
    } else {
        // Returns now, i.e. expire now
        EXPECT_EQ(now, rv);
    }
}

TEST_F(McTimeTest, limited_less_than_epoch) {
    auto ts = epoch - 1;

    // If this failed, has the test system got a bad clock?
    EXPECT_EQ(1,
              mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                           seconds(99)))
            << "Check your system time";
}

TEST_F(McTimeTest, limited_overflow_absolute) {
    auto ts = epoch + 5;

    EXPECT_EQ(std::numeric_limits<rel_time_t>::max(),
              mc_time_convert_to_real_time(
                      gsl::narrow<rel_time_t>(ts),
                      seconds(std::numeric_limits<rel_time_t>::max())));
}

TEST_F(McTimeTest, limited_overflow_relative) {
    auto ts = 5;

    EXPECT_EQ(std::numeric_limits<rel_time_t>::max(),
              mc_time_convert_to_real_time(
                      ts, seconds(std::numeric_limits<rel_time_t>::max())));
}

// check if the limit and the expiry for relative expiry, the return val is the
// same, i.e. expire in 5 seconds, max is 5 thus return 5 seconds from now
TEST_F(McTimeTest, limited_relative_limit_and_expiry_equal) {
    auto ts = 5;

    EXPECT_EQ(5 + now, mc_time_convert_to_real_time(ts, seconds(ts)));
}

// check if the limit and the expiry for absolute. This is different for the
// absolute case because the expiry represents an absolute, but the limit
// represents a relative limit.
TEST_F(McTimeTest, limited_absolute_limit_and_expiry_equal) {
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(5).count();

    EXPECT_EQ(ts - epoch,
              mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts),
                                           seconds(ts)));
}
