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

#include <event.h>
#include <logger/logger.h>
#include <platform/platform_time.h>
#include <atomic>

/*
 * This constant defines the seconds between libevent clock callbacks.
 * This roughly equates to how frequency of gethrtime calls made.
 */
const time_t memcached_clock_tick_seconds = 1;

/*
 * This constant defines the frequency of system clock checks.
 * This equates to an extra gettimeofday every 'n' seconds.
 */
const time_t memcached_check_system_time = 60;

/*
 * This constant defines the maximum relative time (30 days in seconds)
 * time values above this are interpretted as absolute.
 */
const time_t memcached_maximum_relative_time = 60*60*24*30;

static std::atomic<rel_time_t> memcached_uptime(0);
static std::atomic<time_t> memcached_epoch(0);
static volatile uint64_t memcached_monotonic_start = 0;
static struct event_base* main_ev_base = nullptr;

static void mc_time_clock_event_handler(evutil_socket_t fd, short which, void *arg);
static void mc_gather_timing_samples();

/*
 * Init internal state and start the timer event callback.
 */
void mc_time_init(struct event_base* ev_base) {

    main_ev_base = ev_base;

    mc_time_init_epoch();

    /* tick once to begin procedings */
    mc_time_clock_tick();

    /* Begin the time keeping by registering for a time based callback */
    mc_time_clock_event_handler(0, 0, nullptr);
}

/*
 * Initisalise our "EPOCH" variables
 * In order to provide a monotonic "uptime" and track system time, we record
 * some points in time.
 */
void mc_time_init_epoch() {
    struct timeval t;
    memcached_uptime = 0;
    memcached_monotonic_start = cb_get_monotonic_seconds();
    cb_get_timeofday(&t);
    memcached_epoch = t.tv_sec;
}

/*
 * Return a monotonically increasing value.
 * The value returned represents seconds since memcached started.
 */
rel_time_t mc_time_get_current_time() {
    return memcached_uptime;
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

    int64_t epoch{memcached_epoch.load()};
    int64_t uptime{memcached_uptime.load()};

    if (t > memcached_maximum_relative_time) { // t is absolute

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

time_t mc_time_limit_abstime(time_t t, std::chrono::seconds limit) {
    auto upperbound = mc_time_convert_to_abs_time(mc_time_get_current_time());

    if (would_overflow<time_t, std::chrono::seconds::rep>(upperbound,
                                                          limit.count())) {
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
    return memcached_epoch + rel_time;
}

/*
 * clock_handler - libevent call back.
 * This method is called (ticks) every 'memcached_clock_tick_seconds' and
 * primarily keeps time flowing.
 */
static void mc_time_clock_event_handler(evutil_socket_t fd, short which, void *arg) {
    static bool initialized = false;
    static struct event clockevent;
    struct timeval t;

    t.tv_sec = (long)memcached_clock_tick_seconds;
    t.tv_usec = 0;

    if (is_memcached_shutting_down()) {
        stop_memcached_main_base();
        return;
    }

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&clockevent, mc_time_clock_event_handler, 0);
    event_base_set(main_ev_base, &clockevent);
    evtimer_add(&clockevent, &t);

    mc_time_clock_tick();
}

/*
 * Update a number of time keeping variables and account for system
 * clock changes.
 */
void mc_time_clock_tick() {
    static uint64_t check_system_time = 0;
    static bool previous_time_valid = false;
    static struct timeval previous_time = {0, 0};

    /* calculate our monotonic uptime */
    memcached_uptime = (rel_time_t)(cb_get_monotonic_seconds() - memcached_monotonic_start + cb_get_uptime_offset());

    /* Collect samples */
    mc_gather_timing_samples();

    /*
      every 'memcached_check_system_time' seconds, keep an eye on the
      system clock.
    */
    if (memcached_uptime >= check_system_time) {
        struct timeval timeofday;
        cb_get_timeofday(&timeofday);
        time_t difference = labs(timeofday.tv_sec - previous_time.tv_sec);
        /* perform a fuzzy check on time, this allows 2 seconds each way. */
        if (previous_time_valid
            && ((difference > memcached_check_system_time + 1)
            || (difference < memcached_check_system_time - 1))) {
            if (cb::logger::get() != nullptr) {
                /* log all variables used in time calculations */
                LOG_WARNING(
                        "system clock changed? Expected delta of {}s ±1 since "
                        "last check, actual difference = {}s, "
                        "memcached_epoch = {}, "
                        "memcached_uptime = {}, new memcached_epoch = {}, "
                        "next check {}",
                        memcached_check_system_time,
                        difference,
                        memcached_epoch.load(),
                        memcached_uptime.load(),
                        (timeofday.tv_sec - memcached_uptime),
                        check_system_time + memcached_check_system_time);
            }
            /* adjust memcached_epoch to ensure correct timeofday can
               be calculated by clients*/
            memcached_epoch.store(timeofday.tv_sec - memcached_uptime);
        }

        /* move our checksystem time marker to trigger the next check
           at the correct interval*/
        check_system_time += memcached_check_system_time;

        previous_time_valid = true;
        previous_time = timeofday;
    }

    BucketManager::instance().tick();
}

static void mc_gather_timing_samples() {
    BucketManager::instance().forEach([](Bucket& bucket) {
        bucket.timings.sample(std::chrono::seconds(1));
        return true;
    });
}
