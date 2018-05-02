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

#include "hdrhistogram.h"

#include <gtest/gtest.h>
#include <cmath>
#include <memory>
#include <utility>

/*
 * Unit tests for the HdrHistogram
 */

// Test can add minimum value (0)
TEST(HdrHistogramTest, addMin) {
    HdrHistogram histogram{0, 255, 3};
    histogram.addValue(0);
    EXPECT_EQ(1, histogram.getValueCount());
    EXPECT_EQ(0, histogram.getValueAtPercentile(100.0));
}

// Test can add maximum value (255)
TEST(HdrHistogramTest, addMax) {
    HdrHistogram histogram{0, 255, 3};
    histogram.addValue(255);
    EXPECT_EQ(1, histogram.getValueCount());
    EXPECT_EQ(255, histogram.getValueAtPercentile(100.0));
}

// Test the bias of +1 used by the underlying hdr_histogram data structure
// does not affect the overall behaviour.
TEST(HdrHistogramTest, biasTest) {
    HdrHistogram histogram{0, 255, 3};

    for (int ii = 0; ii < 256; ii++) {
        histogram.addValue(ii);
    }

    EXPECT_EQ(0, histogram.getValueAtPercentile(0.1));
    EXPECT_EQ(2, histogram.getValueAtPercentile(1.0));
    EXPECT_EQ(127, histogram.getValueAtPercentile(50.0));
    EXPECT_EQ(255, histogram.getValueAtPercentile(100.0));
}

// Test the iterator
TEST(HdrHistogramTest, iteratorTest) {
    HdrHistogram histogram{0, 255, 3};

    for (int ii = 0; ii < 256; ii++) {
        histogram.addValue(ii);
    }

    // Need to create the iterator after we have added the data
    HdrHistogram::Iterator iter{
            histogram.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    uint64_t valueCount = 0;
    while (auto result = histogram.getNextValueAndCount(iter)) {
        EXPECT_TRUE(valueCount == result->first);
        ++valueCount;
    }
}

// Test the addValueAndCount method
TEST(HdrHistogramTest, addValueAndCountTest) {
    HdrHistogram histogram{0, 255, 3};

    histogram.addValueAndCount(0, 100);
    // Need to create the iterator after we have added the data
    HdrHistogram::Iterator iter{histogram.makeLinearIterator(1)};
    while (auto result = histogram.getNextValueAndCount(iter)) {
        EXPECT_EQ(0, result->first);
        EXPECT_EQ(100, result->second);
    }
}
