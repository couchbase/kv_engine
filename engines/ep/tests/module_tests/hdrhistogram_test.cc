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
#include "hdrhistogram.h"
#include "thread_gate.h"

#include <folly/portability/GTest.h>
#include <hdr_histogram.h>
#include <optional>

#include <cmath>
#include <iomanip>
#include <memory>
#include <random>
#include <thread>
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
    EXPECT_EQ(0, histogram.getMinValue());
}

// Test can add maximum value (255)
TEST(HdrHistogramTest, addMax) {
    HdrHistogram histogram{0, 255, 3};
    histogram.addValue(255);
    EXPECT_EQ(1, histogram.getValueCount());
    EXPECT_EQ(255, histogram.getValueAtPercentile(100.0));
    EXPECT_EQ(255, histogram.getMaxValue());
}

// Test the bias of +1 used by the underlying hdr_histogram data structure
// does not affect the overall behaviour.
TEST(HdrHistogramTest, biasTest) {
    HdrHistogram histogram{0, 255, 3};

    double sum = 0;
    for (int ii = 0; ii < 256; ii++) {
        histogram.addValue(ii);
        sum += ii;
    }

    EXPECT_EQ(0, histogram.getValueAtPercentile(0.1));
    EXPECT_EQ(2, histogram.getValueAtPercentile(1.0));
    EXPECT_EQ(127, histogram.getValueAtPercentile(50.0));
    EXPECT_EQ(229, histogram.getValueAtPercentile(90.0));
    EXPECT_EQ(242, histogram.getValueAtPercentile(95.0));
    EXPECT_EQ(255, histogram.getValueAtPercentile(100.0));

    EXPECT_EQ(sum / 256, histogram.getMean());
}

// Test the linear iterator
TEST(HdrHistogramTest, linearIteratorTest) {
    HdrHistogram histogram{0, 255, 3};

    for (int ii = 0; ii < 256; ii++) {
        histogram.addValue(ii);
    }

    // Need to create the iterator after we have added the data
    HdrHistogram::Iterator iter{
            histogram.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    uint64_t valueCount = 0;
    while (auto result = histogram.getNextValueAndCount(iter)) {
        EXPECT_EQ(valueCount, result->first);
        ++valueCount;
    }
}

// Test the linear iterator using base two
TEST(HdrHistogramTest, logIteratorBaseTwoTest) {
    const uint64_t initBucketWidth = 1;
    const uint64_t numOfValues = 256;
    const uint64_t minValue = 0;
    HdrHistogram histogram{minValue, numOfValues - 1, 3};

    for (uint64_t ii = minValue; ii < numOfValues; ii++) {
        histogram.addValue(ii);
    }

    // Need to create the iterator after we have added the data
    const double iteratorBase = 2.0;
    HdrHistogram::Iterator iter{
            histogram.makeLogIterator(initBucketWidth, iteratorBase)};

    uint64_t countSum = 0;
    uint64_t bucketIndex = 0;
    while (auto result = histogram.getNextValueAndCount(iter)) {
        // Check that the values of the buckets increase exponentially
        EXPECT_EQ(pow(iteratorBase, bucketIndex) - 1, result->first);
        // Check that the width of the bucket is the same number as the count
        // as we added values in a linear matter
        EXPECT_EQ((iter.value_iterated_to - iter.value_iterated_from),
                  result->second);
        bucketIndex++;
        countSum += result->second;
    }
    // check we count as many counts as we added
    EXPECT_EQ(numOfValues, countSum);
    // check the iterator has the same number of values we added
    EXPECT_EQ(numOfValues, iter.total_count);
}

// Test the linear iterator using base five
TEST(HdrHistogramTest, logIteratorBaseFiveTest) {
    const uint64_t initBucketWidth = 1;
    const uint64_t numOfValues = 625;
    const uint64_t minValue = 0;
    HdrHistogram histogram{minValue, numOfValues - 1, 3};

    for (uint64_t ii = minValue; ii < numOfValues; ii++) {
        histogram.addValue(ii);
    }

    // Need to create the iterator after we have added the data
    const double iteratorBase = 5.0;
    HdrHistogram::Iterator iter{
            histogram.makeLogIterator(initBucketWidth, iteratorBase)};

    uint64_t countSum = 0;
    uint64_t bucketIndex = 0;
    while (auto result = histogram.getNextValueAndCount(iter)) {
        // Check that the values of the buckets increase exponentially
        EXPECT_EQ(pow(iteratorBase, bucketIndex) - 1, result->first);
        // Check that the width of the bucket is the same number as the count
        // as we added values in a linear matter
        EXPECT_EQ((iter.value_iterated_to - iter.value_iterated_from),
                  result->second);
        bucketIndex++;
        countSum += result->second;
    }
    // check we count as many counts as we added
    EXPECT_EQ(numOfValues, countSum);
    // check the iterator has the same number of values we added
    EXPECT_EQ(numOfValues, iter.total_count);
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

#define LOG_NORMAL_MEAN 0
#define LOG_NORMAL_STD 2.0
#define LOG_NORMAL_SCALE_UP_MULT 35000
#define LOG_NORMAL_MIN 50000
static std::vector<uint64_t> valuesToAdd(10000);
static bool initialised = false;
// static function to return a log normal value scaled by
// LOG_NORMAL_SCALE_UP_MULT. It creates an array of 10000 static values that
// using std::lognormal_distribution and returners them in an incrementing
// linear fashion so that they can be used for the meanTest
static uint64_t GetNextLogNormalValue() {
    static unsigned int i = 0;

    if (!initialised) {
        // create a log normal distribution and random number generator
        // so we can add random values in a log normal distribution which is a
        // better representation of a production environment
        std::random_device randomDevice;
        std::mt19937 randomNumGen(randomDevice());
        std::lognormal_distribution<long double> distribution(LOG_NORMAL_MEAN,
                                                              LOG_NORMAL_STD);
        // We have denormalize the log normal distribution with a min
        // changing from 0 to 50000ns the max should remain at inf and set
        // the mean to about 84000ns.
        // Percentile values will vary as we use a random number generator to
        // seed a X value when getting values from the distribution. However,
        // the values below should give an idea of the distribution which
        // modelled around an "ADD" op from stats.log p50:~84000ns |
        // p90:~489000ns |p99:3424000ns |p99.9:20185000ns | p99.99:41418000ns
        for (auto& currVal : valuesToAdd) {
            auto valToAdd = static_cast<uint64_t>(
                    LOG_NORMAL_MIN + std::round(distribution(randomNumGen) *
                                                LOG_NORMAL_SCALE_UP_MULT));
            currVal = valToAdd;
        }
        initialised = true;
    }

    if (i >= valuesToAdd.size()) {
        i = 0;
    }

    return valuesToAdd[i++];
}

// Test the getMean method
TEST(HdrHistogramTest, meanTest) {
    HdrHistogram histogram{0, 60000000, 3};
    uint64_t sum = 0;
    uint64_t total_count = 0;

    for (uint64_t i = 0; i < 1000000; i++) {
        uint64_t count = GetNextLogNormalValue();
        uint64_t value = GetNextLogNormalValue();

        // only add random values inside the histograms range
        // otherwise we sill skew the mean
        if (value <= histogram.getMaxTrackableValue()) {
            histogram.addValueAndCount(value, count);
            // store values so we can calculate the real mean
            sum += value * count;
            total_count += count;
        }
    }

    // calculate the mean
    double_t avg = (sum / static_cast<double_t>(total_count));

    uint64_t meanDiff = std::abs(avg - histogram.getMean());
    double_t errorPer = (meanDiff / avg) * 100.0;

    // check that the error percentage is less than 0.05%
    EXPECT_GT(0.05, std::abs(errorPer));
}

void addValuesThread(HdrHistogram& histo,
                     ThreadGate& tg,
                     unsigned int iterations,
                     unsigned int size) {
    // wait for all threads to be ready to start
    tg.threadUp();
    for (unsigned int iteration = 0; iteration < iterations; iteration++) {
        for (unsigned int value = 0; value < size; value++) {
            histo.addValue(value);
        }
    }
}
// Test to check that no counts to HdrHistogram are dropped due to
// incorrect memory order when using parallel writing threads
TEST(HdrHistogramTest, addValueParallel) {
    // we want to perform a large amount of addValues so we increase the
    // probability of dropping a count
    unsigned int numOfAddIterations = 5000;
    unsigned int maxVal = 2;
    HdrHistogram histogram{0, maxVal, 3};

    // Create two threads and get them to add values to a small
    // histogram so there is a high contention on it's counts.
    std::vector<std::thread> threads(2);
    ThreadGate tg(threads.size());
    for (auto& t : threads) {
        t = std::thread(addValuesThread,
                        std::ref(histogram),
                        std::ref(tg),
                        numOfAddIterations,
                        maxVal);
    }
    // wait for all the threads to finish
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(numOfAddIterations * maxVal * threads.size(),
              histogram.getValueCount());
    ASSERT_EQ(maxVal - 1, histogram.getMaxValue());
    ASSERT_EQ(0, histogram.getMinValue());

    HdrHistogram::Iterator iter{
            histogram.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    uint64_t valueCount = 0;
    // Assert that the right number of values were added to the histogram
    while (auto result = histogram.getNextValueAndCount(iter)) {
        ASSERT_EQ(valueCount, result->first);
        ASSERT_EQ(threads.size() * numOfAddIterations, result->second);
        ++valueCount;
    }
}

// Test that when histogram is empty getValueAtPercentile returns 0.
TEST(HdrHistogramTest, percentileWhenEmptyTest) {
    HdrHistogram histogram{0, 255, 3};
    ASSERT_EQ(0, histogram.getValueCount());
    EXPECT_EQ(0, histogram.getValueAtPercentile(0.0));
    EXPECT_EQ(0, histogram.getValueAtPercentile(50.0));
    EXPECT_EQ(0, histogram.getValueAtPercentile(100.0));
}

// Test the aggregation operator method
TEST(HdrHistogramTest, aggregationTest) {
    HdrHistogram histogramOne{0, 15, 3};
    HdrHistogram histogramTwo{0, 15, 3};

    for (int i = 0; i < 15; i++) {
        histogramOne.addValue(i);
        histogramTwo.addValue(i);
    }
    // Do aggregation
    histogramOne += histogramTwo;

    HdrHistogram::Iterator iterOne{
            histogramOne.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    HdrHistogram::Iterator iterTwo{
            histogramTwo.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    uint64_t valueCount = 0;
    for (int i = 0; i < 15; i++) {
        auto resultOne = histogramOne.getNextValueAndCount(iterOne);
        auto resultTwo = histogramOne.getNextValueAndCount(iterTwo);
        // check values are the same for both histograms
        EXPECT_EQ(valueCount, resultTwo->first);
        EXPECT_EQ(valueCount, resultOne->first);
        // check that the counts for each value is twice as much as
        // in a bucket in histogram one as it is in histogram two
        EXPECT_EQ(resultOne->second, resultTwo->second * 2);
        ++valueCount;
    }

    // Check the totals of each histogram
    EXPECT_EQ(30, histogramOne.getValueCount());
    EXPECT_EQ(15, histogramTwo.getValueCount());
}

// Test the aggregation operator method
TEST(HdrHistogramTest, aggregationTestEmptyLhr) {
    HdrHistogram histogramOne{0, 15, 3};
    HdrHistogram histogramTwo{0, 200, 3};

    for (int i = 0; i < 200; i++) {
        histogramTwo.addValue(i);
    }
    // Do aggregation
    histogramOne += histogramTwo;

    HdrHistogram::Iterator iterOne{
            histogramOne.makeLinearIterator(/* valueUnitsPerBucket */ 1)};
    HdrHistogram::Iterator iterTwo{
            histogramTwo.makeLinearIterator(/* valueUnitsPerBucket */ 1)};

    // Max value of LHS should be updated too 200 thus counts should be the
    // same for every value in both histograms
    for (int i = 0; i < 200; i++) {
        auto resultOne = histogramOne.getNextValueAndCount(iterOne);
        auto resultTwo = histogramOne.getNextValueAndCount(iterTwo);
        // check values are the same for both histograms
        EXPECT_EQ(resultOne->first, resultTwo->first);
        // check that the counts for each value are the same
        EXPECT_EQ(resultOne->second, resultTwo->second);
    }

    // Check the totals of each histogram
    EXPECT_EQ(200, histogramOne.getValueCount());
    EXPECT_EQ(200, histogramTwo.getValueCount());
}

// Test the aggregation operator method
TEST(HdrHistogramTest, aggregationTestEmptyRhs) {
    HdrHistogram histogramOne{0, 1, 3};
    HdrHistogram histogramTwo{0, 1, 1};

    for (int i = 0; i < 200; i++) {
        histogramOne.addValue(i);
    }
    // Do aggregation
    histogramOne += histogramTwo;

    HdrHistogram::Iterator iter{
            histogramOne.makeLinearIterator(/* valueUnitsPerBucket */ 1)};

    uint64_t valueCount = 0;
    // make sure the histogram has expanded in size for all 200 values
    while (auto result = histogramOne.getNextValueAndCount(iter)) {
        EXPECT_EQ(valueCount, result->first);
        EXPECT_EQ(1, result->second);
        ++valueCount;
    }

    // Check the totals of each histogram
    EXPECT_EQ(200, histogramOne.getValueCount());
    EXPECT_EQ(0, histogramTwo.getValueCount());
}
