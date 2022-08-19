/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <array>
#include <atomic>
#include <functional>

/**
 * The SloppyGauge is just a hack used in the prototype for
 * implementing throttling.
 *
 * To avoid having to read the time "every time" we want to get the
 * limits (reset the value for the next slot) the class provides
 * a "tick" method to move to the next slot.
 */
class SloppyGauge {
public:
    SloppyGauge() = default;

    /// Bump the number of units used for the current slot
    void increment(std::size_t used);

    /// Check to see if the current cu_count for the current slot
    /// is below the provided limits
    bool isBelow(std::size_t limit) const;

    /// move the clock forward, and carry everything above max forward
    /// into the next slot. The motivation is that we don't want someone
    /// to exceed the quota a lot right before the limit and then
    /// have a full quota at the next slot (given that we execute in an
    /// optimistic way by checking if there is _some room_ before performing
    /// an operation and do the proper accounting when we're done executing.
    void tick(size_t max);

    /// Reset all members to 0
    void reset();

    /// The following member is _ONLY_ to be used from unit tests!
    std::size_t getValue() const {
        return value;
    }

protected:
    std::atomic<std::size_t> value{0};
};
