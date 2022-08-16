/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "array_histogram.h"

#include "thread_gate.h"

#include <folly/portability/GTest.h>

#include <hdrhistogram/hdrhistogram.h>

TEST(ArrayHistogramTest, Add) {
    ArrayHistogram<uint64_t, std::numeric_limits<uint8_t>::max() + 1> hist;
    // nothing in the hist
    EXPECT_EQ(0, hist.getNumberOfSamples());

    hist.add(123, 1);
    EXPECT_EQ(1, hist.getNumberOfSamples());
    EXPECT_EQ(1, hist[123]);

    hist.add(123, 10);
    EXPECT_EQ(11, hist.getNumberOfSamples());
    EXPECT_EQ(11, hist[123]);

    hist.add(124, 5);
    EXPECT_EQ(16, hist.getNumberOfSamples());
    EXPECT_EQ(11, hist[123]);
    EXPECT_EQ(5, hist[124]);
}

TEST(ArrayHistogramTest, Remove) {
    ArrayHistogram<uint64_t, std::numeric_limits<uint8_t>::max() + 1> hist;
    // nothing in the hist
    EXPECT_EQ(0, hist.getNumberOfSamples());

    hist.add(123, 10);
    EXPECT_EQ(10, hist.getNumberOfSamples());
    EXPECT_EQ(10, hist[123]);

    hist.remove(123, 5);
    EXPECT_EQ(5, hist.getNumberOfSamples());
    EXPECT_EQ(5, hist[123]);

    hist.remove(123, 5);
    EXPECT_EQ(0, hist.getNumberOfSamples());
    EXPECT_EQ(0, hist[123]);
}

TEST(ArrayHistogramTest, Percentile) {
    // reference histogram impl. Values for ArrayHistogram should
    // match those of this hdr histogram
    HdrUint8Histogram hdr;

    ArrayHistogram<uint64_t, std::numeric_limits<uint8_t>::max() + 1> hist;
    // nothing in the hist
    EXPECT_EQ(0, hist.getNumberOfSamples());

    auto expect = [&](auto p0, auto p50, auto p100) {
        // assert reference values from HdrHistogram
        ASSERT_EQ(p0, hdr.getValueAtPercentile(0.0));
        ASSERT_EQ(p50, hdr.getValueAtPercentile(50.0));
        ASSERT_EQ(p100, hdr.getValueAtPercentile(100.0));

        EXPECT_EQ(p0, hist.getValueAtPercentile(0.0f));
        EXPECT_EQ(p50, hist.getValueAtPercentile(50.0f));
        EXPECT_EQ(p100, hist.getValueAtPercentile(100.0f));
    };

    // base case for when there are _no_ samples

    {
        SCOPED_TRACE("no samples");
        expect(0, 0, 0);
    }

    // 10 samples for value 100
    hdr.add(100 /* value */, 10 /* frequency */);
    hist.add(100 /* value */, 10 /* frequency */);

    // now that there are some samples in the histogram, the 0%ile should
    // always be the lowest recorded value, and the 100%ile should always be
    // the highest recorded value.
    // The 50%ile is more interesting, and is discussed for each case

    // only one value has been seen (though with a frequency of 10)
    // all percentiles fall in the same bucket.
    {
        SCOPED_TRACE("one bucket");
        expect(100, 100, 100);
    }

    // 10 samples for 100, 10 for 110, total 20 samples.
    // 10 samples are less than or equal to 100 so 50%ile is still in lower
    // bucket.
    hdr.add(110, 10);
    hist.add(110, 10);

    {
        SCOPED_TRACE("two buckets (even number of samples)");
        expect(100, 100, 110);
    }

    // tests rounding behaviour is consistent with HdrHistogram
    // 10 samples for 100, 11 for 110, total 21 samples.
    // 21*0.5 = 10.5, round to 11.
    // 11 samples are less than or equal to 110 so 50%ile is now in the bucket
    // for 110.
    hdr.add(110, 1);
    hist.add(110, 1);

    {
        SCOPED_TRACE("50%ile bumped to 110");
        expect(100, 110, 110);
    }

    // 10 samples for 100, 11 for 110, 20 for 120,  total 41 samples.
    // 41*0.5 = 20.5, round to 21.
    // 21 samples are less than or equal to 120 so 50%ile is now in the bucket
    // for 120.
    hdr.add(120, 30);
    hist.add(120, 30);

    {
        SCOPED_TRACE("50%ile and 100%ile bumped to 120");
        expect(100, 120, 120);
    }
}

TEST(ArrayHistogramTest, ThreadSafety) {
    // access histogram from multiple threads to confirm values can be
    // safely updated and read. Ideally would fail under TSAN if unsafe.
    // As locks are not used, computed percentile value are not expected to
    // be perfectly accurate, but should not lead to data races.
    ArrayHistogram<uint64_t, std::numeric_limits<uint8_t>::max() + 1> hist;

    // gate to coordinate for both worker threads and main thread
    ThreadGate tg(3);

    auto work = [&](auto value) {
        tg.threadUp();

        for (int i = 0; i < 10000; i++) {
            hist.add(value);
            hist.getValueAtPercentile(50.0f);
        }
    };

    auto thread1 = std::thread(work, 128);
    auto thread2 = std::thread(work, 255);

    tg.threadUp();

    thread1.join();
    thread2.join();
}