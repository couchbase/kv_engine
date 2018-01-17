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

#include "hdr_histogram.h"

#include <gtest/gtest.h>
#include <cmath>
#include <memory>

/*
 * Unit tests for the HdrHistogram_c library
 */

// Custom deleter for struct hdr_histogram.
struct Deleter {
    void operator()(struct hdr_histogram* val) {
        free(val);
    }
};

using hdrHistogramUniquePtr = std::unique_ptr<struct hdr_histogram, Deleter>;

TEST(HdrHistogramTest, functionTest) {
    struct hdr_histogram* hist;
    hdrHistogramUniquePtr histogram;

    // Initialise the histogram
    hdr_init(1, // Minimum value
             INT64_C(3600000000), // Maximum value
             3, // Number of significant figures
             &hist); // Pointer to initialise

    histogram.reset(std::move(hist));

    for (int ii = 10; ii <= 100; ii += 10) {
        hdr_record_values(histogram.get(), ii, 1000);
    }

    EXPECT_EQ(20, hdr_value_at_percentile(histogram.get(), 11.0));
    EXPECT_EQ(55, hdr_mean(histogram.get()));
    EXPECT_EQ(10, hdr_min(histogram.get()));
    EXPECT_EQ(100, hdr_max(histogram.get()));
    EXPECT_EQ(29, std::ceil(hdr_stddev(histogram.get())));
}
