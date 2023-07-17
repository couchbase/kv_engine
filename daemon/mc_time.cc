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
 * This constant defines the seconds between libevent clock callbacks.
 * This roughly equates to how frequency of gethrtime calls made.
 */
const seconds memcached_clock_tick_seconds(1);

/*
 * This constant defines the maximum relative time (30 days in seconds)
 * time values above this are interpretted as absolute.
 * note: c++20 will bring chrono::days
 */
const seconds memcached_maximum_relative_time(60 * 60 * 24 * 30);

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

rel_time_t mc_time_convert_to_real_time(rel_time_t t) {
    rel_time_t rv = 0;

    int64_t epoch{cb::time::UptimeClock::instance().getEpochSeconds().count()};
    int64_t uptime{cb::time::UptimeClock::instance().getUptime().count()};

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

time_t mc_time_limit_abstime(time_t t, seconds limit) {
    auto upperbound = mc_time_convert_to_abs_time(mc_time_get_current_time());

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

/*
 * Convert the relative time to an absolute time (relative to EPOCH ;) )
 */
time_t mc_time_convert_to_abs_time(const rel_time_t rel_time) {
    return cb::time::UptimeClock::instance().getEpochSeconds().count() +
           rel_time;
}

static void mc_gather_timing_samples() {
    BucketManager::instance().forEach([](Bucket& bucket) {
        // @todo: if the callback was slow this is not correct, should use the
        // realtime that elapsed.
        bucket.timings.sample(memcached_clock_tick_seconds);
        return true;
    });
}

namespace cb::time {

Regulator::Regulator(folly::EventBase& eventBase, Duration interval)
    : eventBase(eventBase), interval(interval) {
}

void Regulator::scheduleOneTick() {
    eventBase.schedule(
            [this]() {
                if (is_memcached_shutting_down()) {
                    stop_memcached_main_base();
                    return;
                }

                tick();

                // And again.
                scheduleOneTick();
            },
            interval);
}

void Regulator::tickUptimeClockOnce() {
    eventBase.runInEventBaseThreadAndWait(
            []() { Regulator::instance().tick(); });
}

void Regulator::tick() {
    UptimeClock::instance().tick();
    BucketManager::instance().tick();
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
      epoch(lastKnownSystemTime) {
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
    this->systemClockToleranceUpper =
            systemClockCheckInterval + systemClockTolerance;
    this->systemClockToleranceLower =
            systemClockCheckInterval - systemClockTolerance;

    this->systemClockCheckInterval = systemClockCheckInterval;
    nextSystemTimeCheck = uptime.load() + systemClockCheckInterval;
}

/*
 * Update a number of time keeping variables and account for system
 * clock changes.
 */
Duration UptimeClock::tick() {
    /* calculate our monotonic uptime */
    auto newUptime = duration_cast<Duration>(steadyTimeNow() - start);
    newUptime += seconds(cb_get_uptime_offset());

    /*
      every 'systemClockCheckInterval' keep an eye on the system clock.
    */
    if (systemClockCheckInterval && newUptime >= nextSystemTimeCheck) {
        doSystemClockCheck(newUptime);
    }

    // Now update and make this new uptime visible via the atomic variable
    auto previous = this->uptime.exchange(newUptime);
    return newUptime - previous;
}

void UptimeClock::doSystemClockCheck(Duration newUptime) {
    ++systemClockChecks;

    auto systemTime = systemTimeNow();
    auto checkDuration = systemTime - lastKnownSystemTime;

    /* move our checksystem time marker to trigger the next check
       at the correct interval*/
    nextSystemTimeCheck += systemClockCheckInterval.value();

    /* perform a fuzzy check on time. */
    if (((checkDuration > systemClockToleranceUpper) ||
         (checkDuration < systemClockToleranceLower))) {
        ++systemClockCheckWarnings;
        auto newEpoch = systemTime - newUptime;
        if (cb::logger::get() != nullptr) {
            /* log all variables used in time calculations */
            LOG_WARNING(
                    "system clock changed? uptime:{} system clock "
                    "difference of {} is outside of tolerance {}-{} "
                    "previous:{:%FT%T%z}, now:{:%FT%T%z}, "
                    "epoch:{:%FT%T%z}, new-epoch:{:%FT%T%z}, next check "
                    "when uptime is {}, warnings:{}",
                    newUptime,
                    duration_cast<Duration>(checkDuration),
                    systemClockToleranceLower,
                    systemClockToleranceUpper,
                    lastKnownSystemTime,
                    systemTime,
                    fmt::localtime(time_point<system_clock>(epoch.load())),
                    fmt::localtime(time_point<system_clock>(newEpoch)),
                    duration_cast<duration<float>>(nextSystemTimeCheck),
                    systemClockCheckWarnings);
        }
        /* adjust memcached_epoch to ensure correct timeofday can
           be calculated by clients*/
        epoch.store(newEpoch);
    }

    lastKnownSystemTime = systemTime;
}

UptimeClock& UptimeClock::instance() {
    static UptimeClock uptimeClock;
    return uptimeClock;
}

} // namespace cb::time
