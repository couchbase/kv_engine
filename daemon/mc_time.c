/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Time keeping for memcached.
 *
 * Orginally part of memcached.c this module encapsulates some time keeping functions that are
 * exposed via the engine API.
 *
 * This module implements a prviate and non-data path "tick" function which maintains a view of time.
 *  - see mc_time_clock_tick
 *
 * This module provides public time methods that allow performant data-path code access to:
 *  1. A monotonic interval time (great for relative expiry and timed locks). - mc_time_get_current_time()
 *  2. approximation of system-clock. - mc_time_convert_to_abs_time()
 *    2.1 The following returns system clock time - mc_time_convert_to_abs_time(mc_time_get_current_time())
 *  3. A method for work with expiry timestamps as per the memcached protocol - mc_time_convert_to_real_time()
 *
 */


#include <signal.h>

#include "config.h"
#include "memcached.h"
#include "mc_time.h"

extern volatile sig_atomic_t memcached_shutdown;

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

static volatile rel_time_t memcached_uptime = 0;
static volatile time_t memcached_epoch = 0;
static volatile uint64_t memcached_monotonic_start = 0;
static struct event_base* main_ev_base = NULL;

static void mc_time_clock_event_handler(evutil_socket_t fd, short which, void *arg);
static void mc_time_clock_tick(void);
static void mc_time_init_epoch(void);

/*
 * Init internal state and start the timer event callback.
 */
void mc_time_init(struct event_base* ev_base) {

    main_ev_base = ev_base;

    mc_time_init_epoch();

    /* tick once to begin procedings */
    mc_time_clock_tick();

    /* Begin the time keeping by registering for a time based callback */
    mc_time_clock_event_handler(0, 0, 0);
}

/*
 * Initisalise our "EPOCH" variables
 * In order to provide a monotonic "uptime" and track system time, we record
 * some points in time.
 */
static void mc_time_init_epoch(void) {
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
rel_time_t mc_time_get_current_time(void) {
    return memcached_uptime;
}

/*
 * Given a timestamp (timestamp follows the rules of mc store protocol)
 * return the seconds from "now" it is expected to expire.
 */
rel_time_t mc_time_convert_to_real_time(const time_t t) {

    rel_time_t rv = 0;

    if (t > memcached_maximum_relative_time) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (t <= memcached_epoch) {
            rv = (rel_time_t)1;
        } else {
            rv = (rel_time_t)(t - memcached_epoch);
        }
    } else if (t != 0) {
        rv = (rel_time_t)(t + memcached_uptime);
    }
    return rv;
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

    t.tv_sec = memcached_clock_tick_seconds;
    t.tv_usec = 0;

    if (memcached_shutdown) {
        event_base_loopbreak(main_ev_base);
        return ;
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
 * Update a number of time keeping variables and account for system clock changes.
 */
static void mc_time_clock_tick(void) {
    static uint64_t check_system_time = 0;
    static bool previous_time_valid = false;
    static struct timeval previous_time = {0, 0};

    /* calculate our monotonic uptime */
    memcached_uptime = (cb_get_monotonic_seconds() - memcached_monotonic_start);

    /*
      every 'memcached_check_system_time' seconds, keep an eye on the system clock.
      This may occasionally trigger if we are just at the edge of memcached_check_system_time, but it's harmless.
    */
    if (memcached_uptime >= check_system_time) {
        struct timeval timeofday;
        cb_get_timeofday(&timeofday);
        if (previous_time_valid && abs((timeofday.tv_sec - previous_time.tv_sec)) != memcached_check_system_time) {
            if (settings.extensions.logger) {
                /* log all variables used in time calculations */
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "system clock changed? previous_time.tv_sec = %u, "
                    "timeofday.tv_sec = %u, memcached_epoch = %u, "
                    "memcached_uptime = %u, new memcached_epoch = %u, "
                    "next check %u\n",
                    previous_time.tv_sec, timeofday.tv_sec, memcached_epoch,
                    memcached_uptime, (timeofday.tv_sec - memcached_uptime),
                    check_system_time + memcached_check_system_time);
            }
            /* adjust memcached_epoch to ensure correct timeofday can be calculated by clients*/
            memcached_epoch = timeofday.tv_sec - memcached_uptime;
        }

        /* move our checksystem time marker to trigger the next check at the correct interval*/
        check_system_time += memcached_check_system_time;

        previous_time_valid = true;
        previous_time = timeofday;
    }
}
