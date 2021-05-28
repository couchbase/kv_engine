/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mc_time.h"
#include <folly/portability/GTest.h>
#include <gsl/gsl-lite.hpp>
#include <memory>

class McTimeTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        mc_time_init_epoch();
    }

    void SetUp() override {
        // tick for each test
        mc_time_clock_tick();

        seconds_since_start = mc_time_get_current_time();
        now = mc_time_convert_to_abs_time(seconds_since_start);
        epoch = mc_time_convert_to_abs_time(0);
    }

    rel_time_t seconds_since_start = 0;
    time_t now = 0;
    time_t epoch = 0;
};

using namespace std::chrono;

// Basic expectations
TEST_F(McTimeTest, relative) {
    EXPECT_EQ(seconds_since_start + 100, mc_time_convert_to_real_time(100));
}

TEST_F(McTimeTest, absolute) {
    // Exceed 30 days from seconds_since_start as an absolute
    auto ts = epoch + duration_cast<seconds>(hours(30 * 24)).count() +
              seconds(1).count();
    EXPECT_EQ(
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count(),
            mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts)));
}

TEST_F(McTimeTest, absolute_less_than_epoch) {
    auto ts =
            duration_cast<seconds>(hours(30 * 24)).count() + seconds(1).count();

    // If this failed, has the test system got a bad clock?
    EXPECT_EQ(1, mc_time_convert_to_real_time(gsl::narrow<rel_time_t>(ts)))
            << "Check your system time";
}

TEST_F(McTimeTest, max) {
    EXPECT_EQ(std::numeric_limits<rel_time_t>::max() - epoch,
              mc_time_convert_to_real_time(
                      std::numeric_limits<rel_time_t>::max()));
}

TEST_F(McTimeTest, min) {
    EXPECT_EQ(std::numeric_limits<rel_time_t>::min(),
              mc_time_convert_to_real_time(
                      std::numeric_limits<rel_time_t>::min()));
}

TEST_F(McTimeTest, zero) {
    EXPECT_EQ(0, mc_time_convert_to_real_time(0));
}

class McTimeLimitTest : public McTimeTest {};

using namespace std::chrono_literals;

// Basic expiry limiting test, use mc_time_limit_abstime to ensure it limits
// an absolute timestamp.
TEST_F(McTimeLimitTest, basic) {
    // The time 100 seconds from now must be limited to 99s from now.
    EXPECT_EQ(now + 99, mc_time_limit_abstime(now + 100, 99s));
}

// Limiting to zero works too, meaning the input time stamp becomes now.
TEST_F(McTimeLimitTest, limit_to_zero) {
    EXPECT_EQ(now, mc_time_limit_abstime(now + 100, 0s));
}

// A time stamp in the past needs no limiting
TEST_F(McTimeLimitTest, time_is_in_the_past) {
    EXPECT_EQ(now - 100, mc_time_limit_abstime(now - 100, 10s));
    EXPECT_EQ(1, mc_time_limit_abstime(1, 10s));
}

// A zero input (which means never expires) gets turned into an expiry time
TEST_F(McTimeLimitTest, zero_input) {
    EXPECT_EQ(now + 10, mc_time_limit_abstime(0, 10s));
}

// If the limit causes overflow, we return max time_t
TEST_F(McTimeLimitTest, would_overflow) {
    ASSERT_GT(now, 0);
    EXPECT_EQ(std::numeric_limits<time_t>::max(),
              mc_time_limit_abstime(0, 9223372036854775807s));
}
