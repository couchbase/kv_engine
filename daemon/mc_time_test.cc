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
#include "platform/platform_time.h"
#include "platform/random.h"
#include <folly/portability/GTest.h>
#include <gsl/gsl-lite.hpp>
#include <memory>
#include <random>

class McTimeTest : public ::testing::Test {
public:
    void SetUp() override {
        mc_time_init_epoch();
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

class McTimeDriftTest : public McTimeTest {
protected:
    static void add_uptime_offset(rel_time_t offset) {
        cb_set_uptime_offset(cb_get_uptime_offset() + offset);
    }

    static void add_timeofday_offset(time_t offset) {
        cb_set_timeofday_offset(cb_get_timeofday_offset() + offset);
    }

    static time_t timeofday() {
        struct timeval timeofday;
        cb_get_timeofday(&timeofday);
        return timeofday.tv_sec;
    }

    // Returns "now" according to the mc_time clock.
    static time_t abstime() {
        return mc_time_convert_to_abs_time(mc_time_get_current_time());
    }

    void SetUp() override {
        McTimeTest::SetUp();
        memcached_epoch_update_hook = [this](time_t oldEpoch, time_t newEpoch) {
            ++epochUpdates;
            lastEpochUpdateOffset = newEpoch - oldEpoch;
        };
    }

    void TearDown() override {
        memcached_epoch_update_hook = nullptr;
        valiateMcTime();
        cb_set_uptime_offset(0);
        cb_set_timeofday_offset(0);
        McTimeTest::TearDown();
    }

    /**
     * Moves the clock by another memcached_check_system_time interval (to allow
     * for any auto-correction to kick in). Then asserts that the mc_time clock
     * reads the same time as the system clock with +-1s tolerance.
     */
    void valiateMcTime() {
        for (int i = 0; i < memcached_check_system_time; i++) {
            add_uptime_offset(1);
            add_timeofday_offset(1);
            mc_time_clock_tick();
        }
        EXPECT_NEAR(timeofday(), abstime(), 1) << "Clock desync detected!";
    }

    int epochUpdates = 0;
    int lastEpochUpdateOffset = 0;
};

// Test what happens when the monotonic clock lags.
TEST_F(McTimeDriftTest, SlowMonotonicClockOneInterval) {
    add_uptime_offset(memcached_check_system_time);
    add_timeofday_offset(memcached_check_system_time * 2);

    // Should update the epoch.
    mc_time_clock_tick();
    EXPECT_EQ(1, epochUpdates) << "Expected epoch update!";

    EXPECT_NEAR(memcached_check_system_time, lastEpochUpdateOffset, 1);
    EXPECT_NEAR(now + memcached_check_system_time * 2, abstime(), 1);
}

TEST_F(McTimeDriftTest, SlowMonotonicClockHalfInterval) {
    add_uptime_offset(memcached_check_system_time * 1.5);
    add_timeofday_offset(memcached_check_system_time * 2);

    // Should update the epoch.
    mc_time_clock_tick();
    EXPECT_EQ(1, epochUpdates) << "Expected epoch update!";

    EXPECT_NEAR(memcached_check_system_time * 0.5, lastEpochUpdateOffset, 1);
    EXPECT_NEAR(now + memcached_check_system_time * 2, abstime(), 1);
}

// Test what happens when the system clock lags.
TEST_F(McTimeDriftTest, SlowSystemClockOneInterval) {
    add_uptime_offset(memcached_check_system_time * 3);
    add_timeofday_offset(memcached_check_system_time * 2);

    // Should update the epoch.
    mc_time_clock_tick();
    EXPECT_EQ(1, epochUpdates) << "Expected epoch update!";

    EXPECT_NEAR(-memcached_check_system_time, lastEpochUpdateOffset, 1);
    EXPECT_NEAR(now + memcached_check_system_time * 2, abstime(), 1);
}

TEST_F(McTimeDriftTest, SlowSystemClockHalfInterval) {
    add_uptime_offset(memcached_check_system_time * 2.5);
    add_timeofday_offset(memcached_check_system_time * 2);

    // Should update the epoch.
    mc_time_clock_tick();
    EXPECT_EQ(1, epochUpdates) << "Expected epoch update!";

    EXPECT_NEAR(-memcached_check_system_time * 0.5, lastEpochUpdateOffset, 1);
    EXPECT_NEAR(now + memcached_check_system_time * 2, abstime(), 1);
}

/**
 * We only update the mc_time epoch if:
 * - 60s have elapsed on the monotonic clock.
 * - *and* the system clock has moved by 60 (+/-) 1s.
 * That leave opportunity for the following to occur:
 * - for a 60s interval on the uptime clock, 59-61s have advanced on the system
 * clock. In that case, the two clocks become desynced by 1 second. This is
 * repeatable and the error accumulates.
 * There is no automatic correction, since we don't compare the system and
 * mc_time clocks directly, instead we just check >60s on the monotonic clock
 * and ~60s on the system clock have elapsed.
 */
TEST_F(McTimeDriftTest, ConsistentUptimeLag1s) {
    if (folly::kIsSanitize) {
        // Takes too long on sanitizers and that also makes the test fail.
        GTEST_SKIP();
    }

    int systemTimeChecks = 0;
    memcached_check_system_time_hook = [&](auto, auto) { ++systemTimeChecks; };
    for (int rep = 0; rep < 200; rep++) {
        // memcached_check_system_time worth of ticks - enough to do the system
        // time check (60s).
        for (int i = 0; i < memcached_check_system_time; i++) {
            // System time advances 61s.
            add_timeofday_offset(1 + (i == 0));
            add_uptime_offset(1);
            mc_time_clock_tick();
        }
    }
    memcached_check_system_time_hook = nullptr;
    EXPECT_NEAR(200, systemTimeChecks, 30);
    // Expect 1 epoch update every 2 system checks, since every 2 intervals we
    // accumulate 2s of error (threshold is 1s).
    EXPECT_NEAR(100, epochUpdates, 30);
}

TEST_F(McTimeDriftTest, ConsistentUptimeLagLargerThan1s) {
    if (folly::kIsSanitize) {
        // Takes too long on sanitizers and that also makes the test fail.
        GTEST_SKIP();
    }

    int systemTimeChecks = 0;
    memcached_check_system_time_hook = [&](auto, auto) { ++systemTimeChecks; };
    for (int rep = 0; rep < 200; rep++) {
        // memcached_check_system_time worth of ticks - enough to do the system
        // time check (60s).
        for (int i = 0; i < memcached_check_system_time; i++) {
            // System time advances 62s.
            add_timeofday_offset(1 + (i == 0) * 2);
            add_uptime_offset(1);
            mc_time_clock_tick();
        }
    }
    memcached_check_system_time_hook = nullptr;
    EXPECT_NEAR(200, systemTimeChecks, 30);
    EXPECT_NEAR(200, epochUpdates, 30);
}
