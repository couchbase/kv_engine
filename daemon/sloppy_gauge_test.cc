/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <thread>

#include "sloppy_gauge.h"

TEST(SloppyGaugeTest, increment) {
    SloppyGauge gauge;
    EXPECT_EQ(0, gauge.getValue());
    // Increment the count, and verify that we didn't move the slot
    gauge.increment(1);
    EXPECT_EQ(1, gauge.getValue());
}

TEST(SloppyGaugeTest, isBelow) {
    SloppyGauge gauge;
    ASSERT_TRUE(gauge.isBelow(1));
    gauge.increment(1);
    ASSERT_FALSE(gauge.isBelow(1));
    ASSERT_TRUE(gauge.isBelow(2));
}

TEST(SloppyGaugeTest, tickDataRollover) {
    SloppyGauge gauge;
    gauge.increment(1000);
    EXPECT_EQ(1000, gauge.getValue());
    for (int ii = 900; ii > 0; ii -= 100) {
        gauge.tick(100);
        EXPECT_EQ(ii, gauge.getValue());
    }
    gauge.tick(100);
    EXPECT_EQ(0, gauge.getValue());

    // Tick when the value is 0 should keep it at 0
    gauge.tick(100);
    EXPECT_EQ(0, gauge.getValue());
}

TEST(SloppyGaugeTest, tickNoDataRollover) {
    // Test that if we don't have any limits (when we're not running in
    // serverless configuration) that we don't roll over _everything_
    // into the next slot, but let each slot count whatever is used
    // within that second.
    SloppyGauge gauge;
    gauge.increment(1000);
    gauge.tick(0);
    EXPECT_EQ(0, gauge.getValue());
}

TEST(SloppyGaugeTest, Multithread) {
    SloppyGauge gauge;
    std::atomic_bool stop{false};

    std::thread other{[&gauge, &stop]() {
        while (!stop) {
            gauge.increment(1);
            std::this_thread::yield();
        }
    }};

    for (int ii = 0; ii < 100; ++ii) {
        // Just try to access the bits to let TSAN be able to test it
        if (gauge.getValue() > 1000) {
            gauge.tick(gauge.getValue());
        }
        std::this_thread::yield();
    }

    stop.store(true);
    other.join();
}
