/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "item_eviction.h"

#include <gtest/gtest.h>
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
    EXPECT_EQ(254, result50.second);
    auto result100 = itemEv.getThresholds(100.0, 100.0);
    EXPECT_EQ(255, result100.first);
    EXPECT_EQ(510, result100.second);
}
