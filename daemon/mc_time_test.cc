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
     * Advance clocks and invoke uptimeClock.tick
     *
     * The input parameters allow tests to drift the clocks
     *
     * @return the test fixture steadyTime
     */
    cb::time::Duration tick(int ticks = 1,
                            milliseconds steadyTick = 100ms,
                            milliseconds systemTick = 100ms) {
        for (int tick = 0; tick < ticks; tick++) {
            steadyTime += steadyTick;
            systemTime += systemTick;
            EXPECT_EQ(duration_cast<cb::time::Duration>(steadyTick),
                      uptimeClock.tick(
                              duration_cast<cb::time::Duration>(steadyTick)));
        }
        return duration_cast<cb::time::Duration>(steadyTime);
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
    ASSERT_EQ(100ms, uptimeClock.now().time_since_epoch());

    // tick, but without time moving
    uptimeClock.tick();
    ASSERT_EQ(100ms, uptimeClock.now().time_since_epoch());

    tick(2);
    ASSERT_EQ(300ms, uptimeClock.now().time_since_epoch());
}

TEST_F(McTimeUptimeTest, systemTimeCheckTriggers) {
    // Enable monitoring. Every 2 seconds check if the system clock has moved by
    // 2 (+/- 1) seconds.
    uptimeClock.configureSystemClockCheck(2s, 1s);

    // Test just demonstrates that ticking over the trigger sets off a warning
    // if the system clock drags, here systemTicks is 0, but monotonicTicks is 2
    tick(2, 1000ms, 0ms);
    EXPECT_EQ(1, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());
}

TEST_F(McTimeUptimeTest, systemTimeNoTriggerDragging) {
    // Enable monitoring. Every 200ms check if the system clock has moved by
    // 200 (+/- 100) milliseconds.
    uptimeClock.configureSystemClockCheck(200ms, 100ms);

    // Tick both equally - no warning. steady=2, system=2
    EXPECT_EQ(200ms, tick(2));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());

    // Now "drag" the system clock. Every 100ms of steady time, 50ms of
    // system will pass, however this is within tolerance and does not trigger
    // a warning.
    EXPECT_EQ(400ms, tick(2, 100ms, 50ms));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSystemClockChecks());
}

TEST_F(McTimeUptimeTest, systemTimeNoTriggerRushing) {
    // Enable monitoring. Every 200ms check if the system clock has moved by
    // 200 (+/- 100) milliseconds.
    uptimeClock.configureSystemClockCheck(200ms, 100ms);

    // Tick both equally - no warning. steady=2, system=2
    EXPECT_EQ(200ms, tick(2));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());

    // Now "rush" the system clock. Every 100ms of steady time, 150ms of
    // system will pass, however this is within tolerance and does not trigger
    // a warning.
    EXPECT_EQ(400ms, tick(2, 100ms, 150ms));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSystemClockChecks());
}

TEST_F(McTimeUptimeTest, systemTimeTriggers) {
    uptimeClock.configureSystemClockCheck(2s, 1s);

    auto epoch = uptimeClock.getEpoch();

    // Now out of tolerance, every 1000ms only advance system by 100ms. When
    // the check triggers, it will look like system time went backwards.
    EXPECT_EQ(2s, tick(2, 1000ms, 100ms));
    EXPECT_EQ(1, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());

    // epoch has moved backwards to account for the apparent system clock jump
    // back. Expected change accounts for the 200ms advance of system time for
    // the 2000ms advance of steady time
    epoch -= 1800ms;
    EXPECT_EQ(epoch.time_since_epoch().count(),
              uptimeClock.getEpoch().time_since_epoch().count());

    // Now out of tolerance (system clock is rushing)
    EXPECT_EQ(4s, tick(2, 1000ms, 2000ms));
    EXPECT_EQ(2, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSystemClockChecks());

    EXPECT_EQ((epoch + 2s).time_since_epoch().count(),
              uptimeClock.getEpoch().time_since_epoch().count());
}

// Test the return value of tick() that it returns the "real" elapsed time
// between ticks
TEST_F(McTimeUptimeTest, tickReturnValue) {
    EXPECT_EQ(0s, uptimeClock.tick());

    // The tick function will check each tick returns the steadyTick value
    EXPECT_EQ(4s, tick(2, 2000ms, 2000ms));
    EXPECT_EQ(20s, tick(2, 8000ms, 8000ms));
}

// Check that sub-second configuration works as expected.
TEST_F(McTimeUptimeTest, millisecondTicking) {
    uptimeClock.configureSystemClockCheck(10ms, 1ms);

    // The tick function will check each tick returns the steadyTick value
    EXPECT_EQ(20ms, tick(2, 10ms, 10ms));

    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSystemClockChecks());

    // within tolerance
    EXPECT_EQ(40ms, tick(2, 10ms, 11ms));
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(4, uptimeClock.getSystemClockChecks());

    auto epoch = uptimeClock.getEpochSeconds();

    // behind by 2ms (out of tolerance)
    EXPECT_EQ(50ms, tick(1, 10ms, 8ms));
    EXPECT_EQ(1, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(5, uptimeClock.getSystemClockChecks());

    EXPECT_EQ(60ms, tick(1, 10ms, 12ms));
    EXPECT_EQ(2, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(6, uptimeClock.getSystemClockChecks());

    // millisecond granularity doesn't yet make a visible change to epoch
    // seconds
    EXPECT_EQ(epoch, uptimeClock.getEpochSeconds());
    // still appears as if 0s elapsed
    EXPECT_EQ(0s, uptimeClock.getUptime());
    // ... but does using now() API
    EXPECT_EQ(60ms, uptimeClock.now().time_since_epoch());

    // Now tick onwards over 1s
    EXPECT_EQ(1060ms, tick(100, 10ms, 10ms));
    EXPECT_EQ(2, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(106, uptimeClock.getSystemClockChecks());
    EXPECT_EQ(1s, uptimeClock.getUptime());
    EXPECT_EQ(1060ms, uptimeClock.now().time_since_epoch());

    // Now a big jump
    EXPECT_EQ(1070ms, tick(1, 10ms, -8000ms));
    EXPECT_EQ(3, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(107, uptimeClock.getSystemClockChecks());
    EXPECT_EQ(1s, uptimeClock.getUptime());
    EXPECT_EQ(1070ms, uptimeClock.now().time_since_epoch());

    // uptime:1s, clock jump -8s
    EXPECT_EQ(epoch - 8s - 1s, uptimeClock.getEpochSeconds());
}

TEST_F(McTimeUptimeTest, tickDelay) {
    uptimeClock.configureSteadyClockCheck(100ms);
    // First advance time by 1s and call tick with a duration of 1s. This is
    // a perfect flow of time and triggers no warning, but 1 check occurs
    steadyTime += 1s;
    EXPECT_EQ(1s, uptimeClock.tick(1s));
    EXPECT_EQ(0, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSteadyClockChecks());

    // Now drag, but just within tolerance
    steadyTime += 900ms;
    EXPECT_EQ(900ms, uptimeClock.tick(1s));
    EXPECT_EQ(0, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSteadyClockChecks());

    // Now trigger a warning. Advance by 899ms. This is dragging and below
    // threshold -/+ 100ms. I.e. we tell tick 1000ms should of elapsed, but
    // 899ms elapsed.
    steadyTime += 899ms;
    EXPECT_EQ(899ms, uptimeClock.tick(1s));
    EXPECT_EQ(1, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(3, uptimeClock.getSteadyClockChecks());

    // Now the other way, advance time further than the tick
    steadyTime += 1101ms;
    EXPECT_EQ(1101ms, uptimeClock.tick(1s));
    EXPECT_EQ(2, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(4, uptimeClock.getSteadyClockChecks());

    // back in threshold
    steadyTime += 1100ms;
    EXPECT_EQ(1100ms, uptimeClock.tick(1s));
    EXPECT_EQ(2, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(5, uptimeClock.getSteadyClockChecks());

    // Finally check continued violations are picked up
    for (int i = 1; i <= 3; i++) {
        steadyTime += 1101ms;
        EXPECT_EQ(1101ms, uptimeClock.tick(1s));
        EXPECT_EQ(2 + i, uptimeClock.getSteadyClockWarnings());
        EXPECT_EQ(5 + i, uptimeClock.getSteadyClockChecks());
    }

    // system checks are disabled.
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
}

TEST_F(McTimeUptimeTest, MB11548) {
    uptimeClock.configureSystemClockCheck(600ms, 100ms);
    EXPECT_EQ(6s, tick(60));
    ASSERT_EQ(0, uptimeClock.getSystemClockWarnings());

    // partially test MB11548. MB11548 affected expiry when time changed to a
    // negative, large unsigned value and triggered expiry of everything. This
    // test doesn't check expiry, but checks how the time functions which are
    // used in expiry paths behave.

    // First mimic a store which sets expiry as 12s. KV always turns anything
    // under 30days into an absolute time. If expiry was over 30days, it is
    // used as is (assumed to be the correct absolute time of expiry).
    auto expiryInput = 12s;

    auto t1 = mc_time_convert_to_real_time(expiryInput.count(),
                                           uptimeClock.getEpoch(),
                                           uptimeClock.getUptime());
    auto absoluteExpiryTime =
            mc_time_convert_to_abs_time(t1, uptimeClock.getEpoch());

    // System clock now goes back, but 6 seconds must pass before we adjust
    systemTime -= 36s;
    EXPECT_EQ(12s, tick(60));
    ASSERT_EQ(1, uptimeClock.getSystemClockWarnings());

    // ep-engine checks for expiry using ep_abs_time(ep_current_time())
    // This is mc_time_convert_to_abs_time(uptimeClock.getUpTime, getEpoch)...

    auto expiryCheck = mc_time_convert_to_abs_time(
            uptimeClock.getUptime().count(), uptimeClock.getEpoch());

    // Check must fail to expir, only 6s of steady time passed, the document
    // lives for now.

    EXPECT_FALSE(absoluteExpiryTime < expiryCheck);
}

// MB-57249 identified that when a system clock warning occurs, a bogus warning
// can be seen to follow. With the fix no warnings occur because system time is
// compared to steady time
TEST_F(McTimeUptimeTest, MB_57249) {
    uptimeClock.configureSystemClockCheck(6s, 100ms);
    uptimeClock.configureSteadyClockCheck(100ms);

    // Code follows the description from the MB.
    // First trigger a check at 6 seconds where everything is good
    EXPECT_EQ(6s, tick(60));
    EXPECT_EQ(0, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());

    // Second trigger a check, but all clocks are moving correctly. It was the
    // call of tick that was not on time.
    // First 59 on time ticks
    EXPECT_EQ(11'900ms, tick(59));
    EXPECT_EQ(0, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(1, uptimeClock.getSystemClockChecks());

    // Now "delay" the next tick, which corresponds to a system check. Both
    // clocks have moved together and really there's no problem.
    steadyTime += 500ms;
    systemTime += 500ms;
    EXPECT_EQ(500ms, uptimeClock.tick(100ms));

    // Prior to fixing the MB, a warning was triggered here. Now no warning as
    // system time is checked against steady time - and they're both equal. A
    // steady clock warning will occur though because the callback was late.
    EXPECT_EQ(1, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(2, uptimeClock.getSystemClockChecks());

    // In the MB, a third check gets it wrong. The previous check was late and
    // this one is on time - all clocks have moved forwards equally by 5.6
    // seconds to the 18 seconds of uptime, but a warning occurs. The log
    // message was also confusingly a number suggesting system clock has gone
    // backwards. Prior to fixing a second warning would occur next.
    EXPECT_EQ(18'400ms, tick(60));
    EXPECT_EQ(1, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(3, uptimeClock.getSystemClockChecks());

    // Check again, all is good.
    EXPECT_EQ(24'400ms, tick(60));
    EXPECT_EQ(1, uptimeClock.getSteadyClockWarnings());
    EXPECT_EQ(0, uptimeClock.getSystemClockWarnings());
    EXPECT_EQ(4, uptimeClock.getSystemClockChecks());
}
