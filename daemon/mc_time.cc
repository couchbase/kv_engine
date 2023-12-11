/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Time keeping for memcached.
 *
 * Orginally part of memcached.c this module encapsulates some time
 * keeping functions that are exposed via the engine API.
 *
 * This module implements a prviate and non-data path "tick" function
 * which maintains a view of time.
 *
 *  - see mc_time_clock_tick
 *
 * This module provides public time methods that allow performant
 * data-path code access to:
 *
 *  1. A monotonic interval time (great for relative expiry and timed
 *     locks). - mc_time_get_current_time()
 *  2. approximation of system-clock. - mc_time_convert_to_abs_time()
 *    2.1 The following returns system clock time -
 *        mc_time_convert_to_abs_time(mc_time_get_current_time())
 *  3. A method for work with expiry timestamps as per the memcached
 *     protocol - mc_time_convert_to_real_time()
 *
 */
#include "mc_time.h"

#include "buckets.h"
#include "memcached.h"

#include <fmt/chrono.h>
#include <folly/io/async/EventBase.h>
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <platform/platform_time.h>
#include <atomic>
#include <chrono>

using namespace std::chrono;

/*
 * This constant defines the maximum relative time (30 days in seconds)
 * time values above this are interpretted as absolute.
 * note: c++20 will bring chrono::days
 */
const seconds memcached_maximum_relative_time(60 * 60 * 24 * 30);

std::chrono::steady_clock::time_point mc_time_uptime_now() {
    return cb::time::UptimeClock::instance().now();
}

/*
 * Return a monotonically increasing value.
 * The value returned represents seconds since memcached started.
 */
rel_time_t mc_time_get_current_time() {
    return cb::time::UptimeClock::instance().getUptime().count();
}

/// @return true if a + b would overflow rel_time_t
template <class A, class B>
static bool would_overflow(A a, B b) {
    return a > (std::numeric_limits<A>::max() - b);
}

// The above would_overflow(a. b) assumes rel_time_t is unsigned
static_assert(std::is_unsigned<rel_time_t>::value,
              "would_overflow assumes rel_time_t is unsigned");

rel_time_t mc_time_convert_to_real_time(rel_time_t t,
                                        system_clock::time_point currentEpoch,
                                        seconds currentUptime) {
    rel_time_t rv = 0;

    int64_t epoch{
            duration_cast<seconds>(currentEpoch.time_since_epoch()).count()};
    int64_t uptime{currentUptime.count()};

    if (t > memcached_maximum_relative_time.count()) { // t is absolute

        // Ensure overflow is predictable (we stay at max rel_time_t)
        if (would_overflow<int64_t, int64_t>(epoch, uptime)) {
            return std::numeric_limits<rel_time_t>::max();
        }

        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (t <= epoch) {
            rv = (rel_time_t)1;
        } else {
            rv = (rel_time_t)(t - epoch);
        }
    } else if (t != 0) { // t is relative
        // Ensure overflow is predictable (we stay at max rel_time_t)
        if (would_overflow<rel_time_t, int64_t>(t, uptime)) {
            rv = std::numeric_limits<rel_time_t>::max();
        } else {
            rv = (rel_time_t)(t + uptime);
        }
    }

    return rv;
}

rel_time_t mc_time_convert_to_real_time(rel_time_t t) {
    const auto& instance = cb::time::UptimeClock::instance();
    return mc_time_convert_to_real_time(
            t, instance.getEpoch(), instance.getUptime());
}

time_t mc_time_limit_abstime(time_t t, seconds limit, seconds uptime) {
    auto upperbound = mc_time_convert_to_abs_time(uptime.count());

    if (would_overflow<time_t, seconds::rep>(upperbound, limit.count())) {
        upperbound = std::numeric_limits<time_t>::max();
    } else {
        upperbound = upperbound + limit.count();
    }

    if (t == 0 || t > upperbound) {
        t = upperbound;
    }

    return t;
}

time_t mc_time_limit_abstime(time_t t, seconds limit) {
    return mc_time_limit_abstime(
            t, limit, cb::time::UptimeClock::instance().getUptime());
}

/*
 * Convert the relative time to an absolute time (relative to EPOCH ;) )
 */

time_t mc_time_convert_to_abs_time(rel_time_t rel_time,
                                   system_clock::time_point currentEpoch) {
    return rel_time +
           duration_cast<seconds>(currentEpoch.time_since_epoch()).count();
}

time_t mc_time_convert_to_abs_time(rel_time_t rel_time) {
    return mc_time_convert_to_abs_time(
            rel_time, cb::time::UptimeClock::instance().getEpoch());
}

static void mc_gather_timing_samples() {
    BucketManager::instance().forEach([](Bucket& bucket) {
        bucket.timings.sample();
        return true;
    });
}

namespace cb::time {

Regulator::Regulator(folly::EventBase& eventBase, Duration interval)
    : eventBase(eventBase),
      interval(interval),
      nextBucketManagerTick(UptimeClock::instance().now()) {
}

void Regulator::scheduleOneTick() {
    eventBase.schedule(
            [this]() {
                if (is_memcached_shutting_down()) {
                    stop_memcached_main_base();
                    return;
                }

                tick(interval);

                // And again.
                scheduleOneTick();
            },
            interval);
}

void Regulator::tickUptimeClockOnce() {
    // tick all clocks on the eventBase thread (wait) - as this call comes from
    // anywhere at anytime there is no defined period - hence nullopt input
    eventBase.runInEventBaseThreadAndWait(
            []() { Regulator::instance().tick(std::nullopt); });
}

void Regulator::tick(std::optional<Duration> expectedPeriod) {
    UptimeClock::instance().tick(expectedPeriod);
    const auto now = UptimeClock::instance().now();
    // Every 1s we should tick the BucketManager.
    if (now >= nextBucketManagerTick) {
        BucketManager::instance().tick();
        nextBucketManagerTick = now + 1s;
    }
    mc_gather_timing_samples();
}

static folly::Synchronized<std::unique_ptr<Regulator>, std::mutex>
        periodicTicker;

void Regulator::createAndRun(folly::EventBase& eventBase, Duration interval) {
    auto locked = periodicTicker.lock();
    if (!*locked) {
        *locked = std::make_unique<Regulator>(eventBase, interval);
    }

    // scheduleOneTick will schedule a periodic wakeup that will "tick" the
    // UptimeClock and some other modules that require a periodic call. The
    // function which is invoked during wakeup will (if not shutting down)
    // schedule the next tick
    locked->get()->scheduleOneTick();
}

Regulator& Regulator::instance() {
    auto locked = periodicTicker.lock();
    Expects(*locked);
    return *(locked->get());
}

UptimeClock::UptimeClock()
    : UptimeClock([]() { return steady_clock::now(); },
                  []() { return system_clock::now(); }) {
}

UptimeClock::UptimeClock(SteadyClock steadyClock, SystemClock systemClock)
    : steadyTimeNow(std::move(steadyClock)),
      systemTimeNow(std::move(systemClock)),
      start(steadyTimeNow()),
      lastKnownSystemTime(systemTimeNow()),
      epoch(lastKnownSystemTime),
      systemCheckLastKnownSteadyTime(start),
      lastKnownSteadyTime(start) {
}

std::chrono::steady_clock::time_point UptimeClock::now() const {
    return std::chrono::steady_clock::time_point(uptime.load());
}

seconds UptimeClock::getUptime() const {
    return duration_cast<seconds>(uptime.load());
}

system_clock::time_point UptimeClock::getEpoch() const {
    return epoch.load();
}

seconds UptimeClock::getEpochSeconds() const {
    return duration_cast<seconds>(getEpoch().time_since_epoch());
}

void UptimeClock::configureSystemClockCheck(Duration systemClockCheckInterval,
                                            Duration systemClockTolerance) {
    this->systemClockTolerance = systemClockTolerance;
    this->systemClockCheckInterval = systemClockCheckInterval;
    nextSystemTimeCheck = uptime.load() + systemClockCheckInterval;
}

void UptimeClock::configureSteadyClockCheck(Duration steadyClockTolerance) {
    this->steadyClockTolerance = steadyClockTolerance;
}

/*
 * Update a number of time keeping variables and account for system
 * clock changes.
 */

Duration UptimeClock::tick(std::optional<Duration> expectedPeriod) {
    /* calculate our monotonic uptime */
    auto now = steadyTimeNow();
    auto newUptime = duration_cast<Duration>(now - start);
    newUptime += seconds(cb_get_uptime_offset());

    // If this tick has an expected period, do a check for delays
    if (expectedPeriod && steadyClockTolerance) {
        doSteadyClockCheck(now, expectedPeriod.value(), newUptime);
    }

    /*
      every 'systemClockCheckInterval' keep an eye on the system clock.
      The expectation is that the two clocks should progress forwards "equally"
      and when the system clock does not - warn and adjust the epoch (MB-11548).
    */
    if (expectedPeriod && systemClockCheckInterval &&
        newUptime >= nextSystemTimeCheck) {
        doSystemClockCheck(now, newUptime);
    }

    lastKnownSteadyTime = now;

    // Now update and make this new uptime visible via the atomic variable
    auto previous = this->uptime.exchange(newUptime);
    return newUptime - previous;
}

void UptimeClock::doSystemClockCheck(steady_clock::time_point now,
                                     Duration newUptime) {
    ++systemClockChecks;

    auto systemTime = systemTimeNow();
    auto systemDuration = systemTime - lastKnownSystemTime;

    /* move our checksystem time marker to trigger the next check
       at the correct interval*/
    nextSystemTimeCheck += systemClockCheckInterval.value();

    // steady time since the last check is required for the check
    auto tickDuration = now - systemCheckLastKnownSteadyTime;

    // If the system clock has not progressed by the tickDuration
    // (accounting for the tolerance) consider this a warning and adjust
    // the epoch.
    if (systemDuration > (tickDuration + systemClockTolerance) ||
        systemDuration < (tickDuration - systemClockTolerance)) {
        ++systemClockCheckWarnings;
        auto newEpoch = systemTime - newUptime;
        if (cb::logger::get() != nullptr) {
            /* log all variables used in time calculations */
            LOG_WARNING(
                    "system clock changed? uptime:{} tickDuration:{} "
                    "differs from systemDuration:{} when accounting for "
                    "the tolerance:{}. previous:{:%FT%T%z}, "
                    "now:{:%FT%T%z}. Adjusting epoch from {:%FT%T%z} to "
                    "{:%FT%T%z}. Next check when uptime reaches:{}. "
                    "warnings:{}",
                    newUptime,
                    duration_cast<Duration>(tickDuration),
                    duration_cast<Duration>(systemDuration),
                    systemClockTolerance,
                    lastKnownSystemTime,
                    systemTime,
                    fmt::localtime(system_clock::to_time_t(
                            time_point<system_clock>(epoch.load()))),
                    fmt::localtime(system_clock::to_time_t(
                            time_point<system_clock>(newEpoch))),
                    duration_cast<duration<float>>(nextSystemTimeCheck),
                    systemClockCheckWarnings);
        }
        /* adjust memcached_epoch to ensure correct timeofday can
           be calculated by clients*/
        epoch.store(newEpoch);
    }

    systemCheckLastKnownSteadyTime = now;
    lastKnownSystemTime = systemTime;
}

void UptimeClock::doSteadyClockCheck(steady_clock::time_point now,
                                     Duration expectedPeriod,
                                     Duration newUptime) {
    ++steadyClockChecks;

    // get the duration since the last tick
    auto tickDuration = now - lastKnownSteadyTime;

    // ignore if within tolerance
    if (tickDuration >= expectedPeriod - steadyClockTolerance.value() &&
        tickDuration <= expectedPeriod + steadyClockTolerance.value()) {
        return;
    }

    ++steadyClockCheckWarnings;
    if (cb::logger::get() == nullptr) {
        return;
    }

    LOG_WARNING(
            "UptimeClock::tick is outside of tolerance ±{}. "
            "expected:{} but {} have elapsed. uptime:{} "
            "warnings:{}",
            steadyClockTolerance.value(),
            expectedPeriod,
            std::chrono::duration_cast<Duration>(tickDuration),
            duration_cast<duration<float>>(newUptime),
            steadyClockCheckWarnings);
}

UptimeClock& UptimeClock::instance() {
    static UptimeClock uptimeClock;
    return uptimeClock;
}

} // namespace cb::time
