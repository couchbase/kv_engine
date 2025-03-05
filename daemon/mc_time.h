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
#include "utilities/testing_hook.h"

/*
 * This constant defines the frequency of system clock checks.
 * This equates to an extra gettimeofday every 'n' seconds.
 */
constexpr time_t memcached_check_system_time = 60;

extern TestingHook<rel_time_t, rel_time_t> memcached_check_system_time_hook;
extern TestingHook<time_t, time_t> memcached_epoch_update_hook;

/*
 * Initialise this module.
 */
void mc_time_init(struct event_base* ev_base);

/*
 * Init the epoch time tracking variables
 */
void mc_time_init_epoch();

/*
 * Return a monotonically increasing value.
 * The value returned represents seconds since memcached started.
 */
rel_time_t mc_time_get_current_time();

/*
 * Update a number of time keeping variables and account for system
 * clock changes.
 */
void mc_time_clock_tick();

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
