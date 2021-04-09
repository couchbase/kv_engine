/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "probabilistic_counter.h"

#include <folly/portability/GTest.h>
#include <limits>

/**
 * Define the increment factor for the ProbabilisticCounter being used for
 * the tests. 0.012 allows an 8-bit ProbabilisticCounter to mimic a uint16
 * counter.
 */
static const double incFactor = 0.012;

/*
 * Unit tests for the ProbabilisticCounter class.
 */

// Test that we can construct a ProbabilisticCounter and when we first call
// generateCounterValue on a counter initialised to zero it will return one.
TEST(ProbabilisticCounterTest, initialInc) {
    ProbabilisticCounter<uint8_t> probabilisticCounter(incFactor);
    uint8_t counter{0};
    EXPECT_EQ(1, probabilisticCounter.generateValue(counter));
}

// Test the a u16int_t counter is considered saturated when it reaches the max
// of uint8_t.
TEST(ProbabilisticCounterrTest, saturateCounter) {
    ProbabilisticCounter<uint8_t> probabilisticCounter(incFactor);
    uint16_t counter{0};
    while (counter != std::numeric_limits<uint8_t>::max()) {
        counter = probabilisticCounter.generateValue(counter);
    }
    EXPECT_TRUE(probabilisticCounter.isSaturated(counter));
}
