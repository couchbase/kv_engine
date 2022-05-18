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

#include "sloppy_compute_unit_gauge.h"

class MockSloppyComputeUnitGauge : public SloppyComputeUnitGauge {
public:
    std::atomic<unsigned int>& getCurrent() {
        return current;
    }
    std::array<std::atomic<std::size_t>, 60>& getSlots() {
        return slots;
    }
};

TEST(SloppyComputeUnitGaugeTest, increment) {
    MockSloppyComputeUnitGauge gauge;
    auto& current = gauge.getCurrent();
    auto& slot = gauge.getSlots().at(0);

    // Verify that we're using slot 0 and that it is initialized
    ASSERT_EQ(0, current.load());
    ASSERT_EQ(0, slot);

    // Increment the count, and verify that we didn't move the slot
    gauge.increment(1);
    ASSERT_EQ(0, current.load());
    ASSERT_EQ(1, slot);
}

TEST(SloppyComputeUnitGaugeTest, isBelow) {
    SloppyComputeUnitGauge gauge;
    ASSERT_TRUE(gauge.isBelow(1));
    gauge.increment(1);
    ASSERT_FALSE(gauge.isBelow(1));
    ASSERT_TRUE(gauge.isBelow(2));
}

TEST(SloppyComputeUnitGaugeTest, tick) {
    MockSloppyComputeUnitGauge gauge;
    auto& slots = gauge.getSlots();

    for (std::size_t ii = 0; ii < slots.size() * 2; ++ii) {
        EXPECT_EQ(ii % slots.size(), gauge.getCurrent().load());
        gauge.increment(1000);
        auto& pre = slots.at(gauge.getCurrent().load());
        EXPECT_EQ(1000, pre);

        gauge.tick(100000);
        EXPECT_EQ((ii + 1) % slots.size(), gauge.getCurrent().load());
        auto& post = slots.at(gauge.getCurrent().load());
        EXPECT_EQ(0, post);
    }
}

TEST(SloppyComputeUnitGaugeTest, tickDataRollover) {
    MockSloppyComputeUnitGauge gauge;
    auto& slots = gauge.getSlots();

    gauge.increment(1000);
    for (std::size_t ii = 0; ii < slots.size() - 1; ++ii) {
        EXPECT_EQ(1000 - ii * 10, slots.at(gauge.getCurrent()));
        gauge.tick(10);
    }
}

TEST(SloppyComputeUnitGaugeTest, tickNoDataRollover) {
    // Test that if we don't have any limits (when we're not running in
    // serverless configuration) that we don't roll over _everything_
    // into the next slot, but let each slot count whatever is used
    // within that second.
    MockSloppyComputeUnitGauge gauge;
    gauge.increment(1000);
    gauge.tick(0);
    EXPECT_EQ(0, gauge.getSlots().at(gauge.getCurrent()));
}

TEST(SloppyComputeUnitGaugeTest, iterate) {
    SloppyComputeUnitGauge gauge;

    // Populate the gauge with data (and make sure that we don't
    // start at the beginning of the slots so that we can verify that
    // it wraps correctly
    for (std::size_t ii = 0; ii < 100; ++ii) {
        gauge.tick(10000);
        gauge.increment(ii);
    }

    // Iterate over the data. The first entry we should find should contain 40
    int ii = 40;
    gauge.iterate([&ii](auto count) {
        ASSERT_EQ(ii, count);
        ++ii;
    });
}

TEST(SloppyComputeUnitGaugeTest, Multithread) {
    MockSloppyComputeUnitGauge gauge;
    std::atomic_bool stop{false};

    std::thread other{[&gauge, &stop]() {
        while (!stop) {
            gauge.increment(1);
        }
    }};

    auto& slots = gauge.getSlots();
    for (std::size_t size = 0; size < slots.size(); ++size) {
        while (slots[gauge.getCurrent()] == 0) {
            std::this_thread::yield();
        }
        gauge.tick(slots[gauge.getCurrent()]);
    }

    stop.store(true);
    other.join();
}
