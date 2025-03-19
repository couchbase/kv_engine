/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/cb_time.h>
#include <chrono>
#include <optional>

/**
 * Class provides a read method to a value that will only be refreshed by the
 * given callback if the expiry time has elapsed. This can be useful for
 * providing a cached view of some expensive to compute value.
 */
template <class Type, class Clock = cb::time::steady_clock>
class AutoRefreshedValue {
public:
    AutoRefreshedValue() : expiry(std::chrono::seconds(10)) {
    }
    AutoRefreshedValue(std::chrono::milliseconds expiry) : expiry(expiry) {
    }

    /**
     * Get the value and as a side effect update the value with the given
     * callback if the expiry time has elapsed since the last refresh.
     *
     * @param refresher callback to refresh the value if needed
     * @return The value
     */
    template <class RefreshCallback>
    Type getAndMaybeRefreshValue(RefreshCallback refresher) {
        const auto now = Clock::now();
        if (!value || (now - readTime > expiry)) {
            value = refresher();
            readTime = now;
        }

        return *value;
    }

private:
    // First read will initialise from null to value
    std::optional<Type> value;
    const std::chrono::milliseconds expiry;
    typename Clock::time_point readTime;
};
