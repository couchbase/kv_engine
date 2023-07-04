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
 */
#pragma once

#include "memcached/types.h"

#include <atomic>
#include <chrono>

namespace folly {
class EventBase;
}

/*
 * Return a monotonically increasing value.
 * The value returned represents seconds since memcached started.
 */
rel_time_t mc_time_get_current_time();

/*
 * Convert a relative time value to an absolute time.
 *
 * Note that the following usage approximates gettimeofday()
 * I.e. seconds since EPOCH without actually calling gettimeofday.
 *
 * time_convert_to_abs_time(time_get_current_time());
 */
time_t mc_time_convert_to_abs_time(rel_time_t rel_time);

/**
 * Convert a protocol encoded expiry time stamp to a relative time stamp
 * (relative to the epoch time of memcached)
 *
 * Example 1: A relative expiry time (where t is less than 30days in seconds) of
 * 1000s becomes epoch + 1000s
 *
 * @param t a protocol expiry time-stamp
 */
rel_time_t mc_time_convert_to_real_time(rel_time_t t);

/**
 * Apply a limit to an absolute timestamp (which represents an item's requested
 * expiry time)
 *
 * For example if t represents 23:00 and the time we invoke this method is 22:00
 * and the limit is 60s, then the returned value will be 22:01. The input of
 * 23:00 exceeds 22:00 + 60s, so it is limited to 22:00 + 60s.
 *
 * If t == 0, then the returned value is now + limit
 * If t < now, then the result is t, no limit needed.
 * If t == 0 and now + limit overflows time_t, time_t::max is returned.
 *
 * @param t The expiry time to be limited, 0 means no expiry, 1 to time_t::max
 *          are intepreted as the time absolute time of expiry
 * @param limit The limit in seconds
 * @return The expiry time after checking it against now + limit.
 */
time_t mc_time_limit_abstime(time_t t, std::chrono::seconds limit);

namespace cb::time {

/**
 * The Regulator keeps time "flowing" by continually scheduling a time based
 * callback.
 */
class Regulator {
public:
    /**
     * Construct the Regulator, it will invoke period ticks using the EventBase
     * parameter.
     *
     * @param eventBase EventBase::schedule is invoked against this object for
     *        the periodic tick.
     * @param interval the tick interval in seconds
     */
    Regulator(folly::EventBase& eventBase, std::chrono::seconds interval);

    /**
     * This method exists to allow the unit-test time adjustment command to
     * shift time and "force" a tick of the UptimeClock. This function will
     * trigger the time keeping thread (eventBase) to wake and call
     * UptimeClock::tick. This method will block until complete.
     */
    void tickUptimeClockOnce();

    /**
     * Create a static Regulator object and begin the periodic ticking. This
     * function must be called first from the main thread before "instance" can
     * be invoked.
     */
    static void createAndRun(folly::EventBase& eventBase,
                             std::chrono::seconds interval);

    /**
     * Retrieve the instance of the Regulator - this call expects that the
     * object has already been created, i.e. this must be called after
     * createAndRun
     */
    static Regulator& instance();

protected:
    /**
     * This method starts the flow of time by scheduling a single wakeup, which
     * when that wakeup runs, it reschedules another wakeup. This continues
     * until shutdown is detected.
     *
     * Each time the scheduled wakeup function executes - it is considered a
     * "tick" and in that function a number of other objects will have their
     * tick method invoked - in particular the UptimeClock
     */
    void scheduleOneTick();

    /**
     * One tick - scheduleOneTick will call this method. This method calls down
     * into various objects which need to know a tick has occurred, e.g.
     * UptimeClock::instance().tick()
     */
    void tick();

    folly::EventBase& eventBase;

    const std::chrono::seconds interval;
};

using SteadyClock = std::function<std::chrono::steady_clock::time_point()>;
using SystemClock = std::function<std::chrono::system_clock::time_point()>;

class UptimeClock {
public:
    /**
     * Construct an UptimeClock that uses std::chrono::steady_clock::now and
     * std::chrono:system_clock::now for tracking time
     */
    UptimeClock();

    /**
     * Construct an UptimeClock that uses the given callbacks for tracking time
     */
    UptimeClock(SteadyClock steadyClock, SystemClock systemClock);

    /**
     * tick will read the steady clock and maintain the uptime. The function
     * will also (if enabled) provide system clock monitoring - that is to check
     * for abnormal changes in the system clock, e.g. if the system clock was
     * changed. System clock changes also trigger adjustments of the process
     * "epoch" which is required for correct expiry processing.
     */
    void tick();

    /**
     * Note: This function only returns a changing value if tick() is called
     * @return number of seconds the process has been up
     */
    std::chrono::seconds getUptime() const;

    /**
     * The "epoch" is the process start time (or really it's the construction
     * time of this object, which occurs very early in memcached start-up). It
     * begins as a read of the systemTimeNow member function and provided that
     * systemTimeNow remains stable (no apparent shifts in system time) it will
     * remain as the real process start time. However if the system clock is
     * observed to abnormally shift, the epoch is adjusted to account for the
     * shift (see MB-11548).
     *
     * @return the "process" epoch as system_clock time_point
     */
    std::chrono::system_clock::time_point getEpoch() const;

    /// @return the epoch as seconds
    std::chrono::seconds getEpochSeconds() const;

    /**
     * UptimeClock provides a system clock check feature, where the system clock
     * is monitored and any abnormal changes trigger.
     *
     * 1) A log warning
     * 2) An adjustment of the epoch (see MB-11548)
     *
     * The enablement of system clock checking is done post construction
     * (permitting a future change to make this reconfigurable at runtime). This
     * method will enable system checking at the given interval and with the
     * provided tolerance.
     *
     * @param systemClockCheckInterval how many seconds between each
     *        check (seconds of steady time)
     * @param systemClockTolerance if the system clock has changed by more
     *        than this tolerance (+ or -), generate a warning and make
     *        epoch adjustments
     */
    void configureSystemClockCheck(
            std::chrono::seconds systemClockCheckInterval,
            std::chrono::seconds systemClockTolerance);

    /// @return count of how many times the system clock check triggered
    size_t getSystemClockWarnings() const {
        return systemClockCheckWarnings;
    }

    /// @return the instance of this to be used in memcached
    static UptimeClock& instance();

protected:
    /// function which returns a steady "monotonic" time
    SteadyClock steadyTimeNow;
    /// function which returns the system time
    SystemClock systemTimeNow;

    /// time when object constructed but used as the time the process started
    const std::chrono::steady_clock::time_point start;

    /**
     * uptime represents the number of seconds since the Monitor was created
     * and for non-test deployments should be considered the process upTime.
     * This requires that the tick() function is called to maintain this value
     *
     * atomic as written from time thread and read by many other threads.
     */
    std::atomic<std::chrono::seconds> uptime{std::chrono::seconds(0)};

    /**
     * A read of the system clock to be used in periodic checks of the system
     * clock. E.g. if uptime clock has progressed by n seconds, system time
     * ideally should also progress by n.
     */
    std::chrono::system_clock::time_point lastKnownSystemTime;

    /**
     * epoch is set to system_clock::now value when the object initialises.
     * It can be adjusted later if the system_clock shifts.
     * See configureSystemClockCheck
     */
    std::atomic<std::chrono::system_clock::time_point> epoch;

    /**
     * UptimeClock has configurable system clock checking. The idea is that for
     * every 'n' seconds of steadyTime (uptime) the system clock should also
     * flow equally (forwards). If not then a warning is produced and the
     * epoch variable is adjusted.
     *
     * The system clock is checked only after a defined uptime duration
     * (systemClockCheckInterval) and has a tolerance to the check, allowing
     * for the system clock to be ahead or behind by the
     * systemClockToleranceLower/systemClockToleranceUpper range.
     */
    std::chrono::seconds systemClockToleranceLower;
    std::chrono::seconds systemClockToleranceUpper;
    std::optional<std::chrono::seconds> systemClockCheckInterval;
    size_t systemClockCheckWarnings{0};

    /**
     *  The point on the uptime clock for a system clock check
     */
    std::chrono::seconds nextSystemTimeCheck;
};

} // namespace cb::time
