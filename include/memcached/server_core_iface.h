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

#pragma once

#include "types.h"
#include <memcached/thread_pool_config.h>

#include <functional>

struct ServerCoreIface {
    virtual ~ServerCoreIface() = default;

    /**
     * The current time of the server uptime clock.
     * This is more powerful version of get_current_time() - it supports
     * sub-second precision, and returns a more strongly typed value.
     */
    virtual std::chrono::steady_clock::time_point get_uptime_now() = 0;

    /**
     * The current time.
     */
    virtual rel_time_t get_current_time() = 0;

    /**
     * Get the relative time for the given time_t value.
     *
     * @param exptime A time value expressed in 'protocol-format' (seconds).
     *        1 to 30 days will be as interpreted as relative from "now"
     *        > 30 days is interpreted as an absolute time.
     *        0 in, 0 out.
     * @param limit an optional limit to apply to the time calculations. If the
     *        limit was 60 days, then all calculations will ensure the returned
     *        time can never exceed limit days from now when used in conjunction
     *        with abstime.
     * @return The relative time since memcached's epoch.
     */
    virtual rel_time_t realtime(rel_time_t exptime) = 0;

    /**
     * Get the absolute time for the given rel_time_t value.
     */
    virtual time_t abstime(rel_time_t exptime) = 0;

    /**
     * Apply a limit to an absolute timestamp (which represents an item's
     * requested expiry time)
     *
     * For example if t represents 23:00 and the time we invoke this method is
     * 22:00 and the limit is 60s, then the returned value will be 22:01. The
     * input of 23:00 exceeds 22:00 + 60s, so it is limited to 22:00 + 60s.
     *
     * If t == 0, then the returned value is now + limit
     * If t < now, then the result is t, no limit needed.
     * If t == 0 and now + limit overflows time_t, time_t::max is returned.
     *
     * @param t The expiry time to be limited, 0 means no expiry, 1 to
     *        time_t::max are interpreted as the time absolute time of expiry
     * @param limit The limit in seconds
     * @return The expiry time after checking it against now + limit.
     */
    virtual time_t limit_abstime(time_t t, std::chrono::seconds limit) = 0;

    virtual size_t getMaxEngineFileDescriptors() = 0;

    /// The number of concurrent paging visitors to use for quota sharing.
    virtual size_t getQuotaSharingPagerConcurrency() = 0;

    /**
     * How long in milliseconds the ItemPager will sleep for when not being
     * requested to run.
     */
    virtual std::chrono::milliseconds getQuotaSharingPagerSleepTime() = 0;

    /**
     * How long to wait before disconnecting a DCP producer that appears to be
     * stuck.
     */
    virtual std::chrono::seconds getDcpDisconnectWhenStuckTimeout() = 0;

    /**
     * A regex (in base64) to match the name of the DCP producer to disconnect
     * when stuck.
     */
    virtual std::string getDcpDisconnectWhenStuckNameRegex() = 0;

    /**
     * Whether blind write optimisation is enabled.
     */
    virtual bool isMagmaBlindWriteOptimisationEnabled() = 0;
};
