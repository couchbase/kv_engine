/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "item_eviction.h"

#include <folly/portability/GTest.h>
#include <limits>

/*
 * Unit tests for the ItemEviction class.
 */

// Test that we can construct an instance of HashTable::Eviction, and the
// histogram can be accessed, with the value count being zero.
TEST(ItemEvictionClassTest, initialisation) {
    ItemEviction itemEv;
    EXPECT_EQ(0, itemEv.getFreqHistogramValueCount());
}

// Test the addValueToHistogram function
TEST(ItemEvictionClassTest, addValue) {
    ItemEviction itemEv;
    for (uint16_t ii = 0; ii < 256; ii++) {
        itemEv.addFreqAndAgeToHistograms(ii, ii);
    }
    EXPECT_EQ(256, itemEv.getFreqHistogramValueCount());
}

// Test the getFreqThreshold function
TEST(ItemEvictionClassTest, freqThreshold) {
    ItemEviction itemEv;
    for (uint16_t ii = 0; ii < 256; ii++) {
        itemEv.addFreqAndAgeToHistograms(ii, ii * 2);
    }
    ASSERT_EQ(256, itemEv.getFreqHistogramValueCount());
    auto result50 = itemEv.getThresholds(50.0, 50.0);
    EXPECT_EQ(127, result50.first);
    EXPECT_EQ(255, result50.second);
    auto result100 = itemEv.getThresholds(100.0, 100.0);
    EXPECT_EQ(255, result100.first);
    // NOTE: the maximum added value age value was 255*2 = 510, but the age
    // histogram only tracks one significant figure, so this value is not exact.
    EXPECT_EQ(511, result100.second);
}
