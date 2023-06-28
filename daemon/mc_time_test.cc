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
    void SetUp() override {
        // tick for each test
        cb::time::UptimeClock::instance().tick();

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

using namespace std::chrono;
using namespace std::chrono_literals;

// All tests in this fixture control the UptimeClock by manually adjusting the
// monotonicTicks and/or systemTicks and manually calling tick on the clock
// This allows for testing of expected behaviour and system clock jumps
class McTimeUptimeTest : public ::testing::Test {
public:
    // provide custom time keeping functions which the test code can manipulate
    McTimeUptimeTest()
        : uptimeClock(
                  [this]() {
                      auto now = time_point<steady_clock>();
                      return now + steadyTime;
                  },
                  [this]() {
                      auto now = time_point<system_clock>();
                      return now + systemTime;
                  }) {
    }

    void SetUp() override {
        ASSERT_EQ(seconds(0), uptimeClock.getUptime());
    }

    /**
     * Advance time and tick the clock.
     * The input parameters allow tests to drift the clocks
     * returns the monotonic time
     */
    seconds tick(int ticks = 1,
                 milliseconds steadyTick = 1000ms,
                 milliseconds systemTick = 1000ms) {
        for (int tick = 0; tick < ticks; tick++) {
            steadyTime += steadyTick;
            systemTime += systemTick;
            uptimeClock.tick();
        }
        return duration_cast<seconds>(steadyTime);
    }

    milliseconds steadyTime{0};

    // systemTime starts at a "realistic" point, which makes debugging and
    // reading any log warnings easier
    milliseconds systemTime{duration_cast<milliseconds>(1688054291s)};
    cb::time::UptimeClock uptimeClock;
};

TEST_F(McTimeUptimeTest, expectedFlow) {
    tick();
    ASSERT_EQ(0, uptimeClock.getSystemClockWarnings());
    ASSERT_EQ(1s, uptimeClock.getUptime());

    // tick, but without time moving
    uptimeClock.tick();
    ASSERT_EQ(1s, uptimeClock.getUptime());

    tick(2);
    ASSERT_EQ(3s, uptimeClock.getUptime());
}

TEST_F(McTimeUptimeTest, systemTimeCheckTriggers) {
    // Enable monitoring. Every 2 seconds check if the system clock has moved by
    // 2 (+/- 1) seconds.
    uptimeClock.configureSystemClockCheck(2s, 1s);

    // Test just demonstrates that ticking over the trigger sets off a warning
    // if the system clock drags, here systemTicks is 0, but monotonicTicks is 2
    tick(2, 1000ms, 0ms);
    EXPECT_EQ(1, uptimeClock.getSystemClockWarnings());
}

TEST_F(McTimeUptimeTest, systemTimeNoTriggerDragging) {
    // Enable monitoring. Every 2 seconds check if the system clock has moved by
    // 2 (+/- 1) seconds.
    uptimeClock.configureSystemClockCheck(2s, 1s);

    // Tick both equally - no warning. steady=2, system=2
    EXPECT_EQ(2s, tick(2));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());

    // Now "drag" the system clock. Every 1 second of steady time, 0.5s of
    // system will pass, however this is within tolerance and does not trigger
    // a warning.
    EXPECT_EQ(4s, tick(2, 1000ms, 500ms));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
}

TEST_F(McTimeUptimeTest, systemTimeNoTriggerRushing) {
    // Enable monitoring. Every 2 seconds check if the system clock has moved by
    // 2 (+/- 1) seconds.
    uptimeClock.configureSystemClockCheck(2s, 1s);

    // Tick both equally - no warning. steady=2, system=2
    EXPECT_EQ(2s, tick(2));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());

    // Now "rush" the system clock. Every 1 second of steady time, 1.5s of
    // system will pass, however this is within tolerance and does not trigger
    // a warning.
    EXPECT_EQ(4s, tick(2, 1000ms, 1500ms));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
}

TEST_F(McTimeUptimeTest, systemTimeTriggers) {
    uptimeClock.configureSystemClockCheck(2s, 1s);

    auto epoch = uptimeClock.getEpoch();

    // Now out of tolerance, every 1000ms only advance system by 100ms. When
    // the check triggers, it will look like system time went backwards.
    EXPECT_EQ(2s, tick(2, 1000ms, 100ms));
    EXPECT_EQ(1, uptimeClock.getSystemClockWarnings());
    // epoch has moved backwards to account for the apparent system clock jump
    // back. Expected change accounts for the 200ms advance of system time for
    // the 2000ms advance of steady time
    epoch -= 1800ms;
    EXPECT_EQ(epoch, uptimeClock.getEpoch());

    // Now out of tolerance (system clock is rushing)
    EXPECT_EQ(4s, tick(2, 1000ms, 2000ms));
    EXPECT_EQ(2, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(epoch + 2s, uptimeClock.getEpoch());
}
