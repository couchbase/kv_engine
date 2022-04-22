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
 * The SloppyComputeUnitGauge is just a hack used in the prototype for
 * implementing throttling.
 *
 * To avoid having to read the time "every time" we want to get the
 * limits (reset the value for the next slot) the class provides
 * a "tick" method to move to the next slot.
 *
 * We could have reduced the logic to have just the "current" and "next"
 * slot, but I picked 60 so that if you tick every second resolution you could
 * look back almost a minute (which would be nice in the prototoype to
 * see if it is working as expected ;) )
 */
class SloppyComputeUnitGauge {
public:
    SloppyComputeUnitGauge();

    /// Bump the number of compute units used for the current slot
    void increment(std::size_t used);

    /// Check to see if the current cu_count for the current slot
    /// is below the provided limits
    bool isBelow(std::size_t value) const;

    /// move the clock forward
    void tick();

    /// Iterate through the entries in the log (oldest to newest)
    void iterate(std::function<void(std::size_t)>) const;

protected:
    /// The index of the current slot to use
    std::atomic<unsigned int> current{0};
    /// An array containing the "history" we want to use
    std::array<std::atomic<std::size_t>, 60> slots;
};
