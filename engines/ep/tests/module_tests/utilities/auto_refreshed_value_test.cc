/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "auto_refreshed_value.h"

#include <folly/portability/GTest.h>

class Clock {
public:
    using duration = std::chrono::duration<int, std::nano>;
    using time_point = std::chrono::time_point<Clock>;

    static time_point now() {
        return time;
    }

    static void advance(duration value) {
        time = time + value;
    }
    static time_point time;
};

Clock::time_point Clock::time;

TEST(AutoRefreshedValue, test) {
    AutoRefreshedValue<int, Clock> value(std::chrono::milliseconds(100));

    EXPECT_EQ(1, value.getAndMaybeRefreshValue([]() { return 1; }));

    // Advance to limit (but not over)
    Clock::advance(std::chrono::milliseconds(100));
    EXPECT_EQ(1, value.getAndMaybeRefreshValue([]() {
        throw std::logic_error("Should not call");
        return 1;
    }));
    // Advance over refresh limit
    Clock::advance(std::chrono::milliseconds(1));
    EXPECT_EQ(2, value.getAndMaybeRefreshValue([]() { return 2; }));

    // And not returning new value yet
    Clock::advance(std::chrono::milliseconds(1));
    EXPECT_EQ(2, value.getAndMaybeRefreshValue([]() {
        throw std::logic_error("Should not call");
        return 1;
    }));
}