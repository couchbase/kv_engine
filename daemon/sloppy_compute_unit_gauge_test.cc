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

        gauge.tick();
        EXPECT_EQ((ii + 1) % slots.size(), gauge.getCurrent().load());
        auto& post = slots.at(gauge.getCurrent().load());
        EXPECT_EQ(0, post);
    }
}

TEST(SloppyComputeUnitGaugeTest, iterate) {
    SloppyComputeUnitGauge gauge;

    // Populate the gauge with data (and make sure that we don't
    // start at the beginning of the slots so that we can verify that
    // it wraps correctly
    for (std::size_t ii = 0; ii < 100; ++ii) {
        gauge.tick();
        gauge.increment(ii);
    }

    // Iterate over the data. The first entry we should find should contain 40
    int ii = 40;
    gauge.iterate([&ii](auto count) {
        ASSERT_EQ(ii, count);
        ++ii;
    });
}