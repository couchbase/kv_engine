/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include "types.h"
#include <memcached/thread_pool_config.h>

struct ServerCoreIface {
    virtual ~ServerCoreIface() = default;

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

    /**
     * parser config options
     */
    virtual int parse_config(const char* str,
                             struct config_item items[],
                             FILE* error) = 0;

    /**
     * Request the server to start a shutdown sequence.
     */
    virtual void shutdown() = 0;

    /**
     * Get the maximum size of an iovec the core supports receiving
     * through the item_info structure. The underlying engine may
     * support using more entries to hold its data internally, but
     * when making the data available for the core it must fit
     * within these limits.
     */
    virtual size_t get_max_item_iovec_size() = 0;

    /**
     * Trigger a tick of the clock
     */
    virtual void trigger_tick() = 0;

    /// Get the configured size for the reader and writer pool
    virtual ThreadPoolConfig getThreadPoolSizes() = 0;

    virtual bool isCollectionsEnabled() const = 0;
};
