/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include "statistical_counter.h"

#include <gtest/gtest.h>

/*
 * Unit tests for the StatisticalCounter class.
 */

// Test that we can construct a StatisticalCounter and when we first call
// generateCounterValue on a counter initialised to zero it will return one.
TEST(StatisticalCounterTest, initialInc) {
    StatisticalCounter<uint8_t> statisticalCounter;
    uint16_t counter{0};
    EXPECT_EQ(1, statisticalCounter.generateCounterValue(counter));
}

// Test the a u16int_t counter is considered saturated when it reaches the max
// of uint8_t.
TEST(StatisticalCounterTest, saturateCounter) {
    StatisticalCounter<uint8_t> statisticalCounter;
    uint16_t counter{0};
    while (counter != 255) {
        counter = statisticalCounter.generateCounterValue(counter);
    }
    EXPECT_TRUE(statisticalCounter.isSaturated(counter));
}
