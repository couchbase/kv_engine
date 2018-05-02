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

#pragma once

#include "hdrhistogram.h"

#include <cstdlib> // Required due to the use of free
#include <limits>

/**
 * A container for data structures that are used in the algorithm for
 * selecting which documents to evict from the hash table.
 *
 * The algorithm is as follows:
 *
 * Each time a value is referenced in the hash table its frequency count
 * is incremented according to an 8 bit probabilistic counter.  The counter
 * behaviour is such that as the frequency increases (towards a maximum of
 * 255) it becomes increasingly harder to increment.  It is configured so
 * that a value must be referenced approximately 65K times before its
 * frequency count becomes saturated.
 *
 * During eviction we visit each item in the hash and build up a histogram of
 * frequency counts of the values.  So we have a sum of how many values have
 * a frequency count of 0,  how many have a frequency count of 1, ... how
 * many have a frequency of 255.
 *
 * We have a method getFreqThreshold(uint8_t percentage) which is applied to
 * the histogram and returns the frequency count that corresponds to the
 * given percentile.  For example, if called with 5%, it would return the
 * lowest frequency count that accounts for 5% of all the values added to the
 * histogram.
 *
 * Once we have obtained this threshold frequency count, we simply continue
 * iterating over the hash table and evict all those values that have a
 * frequency count at or below the threshold.
 *
 */
class ItemEviction {
    // Custom deleter for the hdr_histogram struct.
    struct HdrDeleter {
        void operator()(struct hdr_histogram* val) {
            free(val);
        }
    };

    using HdrHistogramUniquePtr =
            std::unique_ptr<struct hdr_histogram, HdrDeleter>;

public:
    ItemEviction();

    // Adds a value to the frequency histogram.
    void addValueToFreqHistogram(uint8_t v);

    // Returns the number of values added to the frequency histogram.
    uint64_t getFreqHistogramValueCount() const;

    // Clears the frequency histogram and sets the requiredToUpdateInterval
    // back to 1.
    void reset();

    // StatCounter: Returns the value held in the frequency histogram at the
    // percentile defined by the input parameter percentage.
    uint16_t getFreqThreshold(double percentage) const;

    // StatCounter: Return true if learning what the frequency counter
    // threshold should be for eviction, else return false.
    bool isLearning() const {
        return (getFreqHistogramValueCount() <= learningPopulation);
    }

    // StatCounter: Return true if it is necessary to update the frequency
    // threshold, else return false
    bool isRequiredToUpdate() const {
        return (getFreqHistogramValueCount() % requiredToUpdateInterval == 0);
    }

    // StatCounter: Update the requiredToUpdateInterval
    void setUpdateInterval(uint64_t interval) {
        requiredToUpdateInterval = interval;
    }

    // Map from the 8-bit probabilistic counter (256 states) to NRU (4 states).
    static uint8_t convertFreqCountToNRUValue(uint8_t statCounter);

    // The initial frequency count that items should be set to when first
    // added to the hash table.  It is not 0, as we want to ensure that we
    // do not immediately evict items that we have just added.
    static const uint8_t initialFreqCount = 4;

    // StatCounter: The number of frequencies that need to be added to the
    // frequency histogram before it is not necessary to recalculate the
    // threshold every time we visit an item in the hash table.
    static const uint64_t learningPopulation = 100;

private:
    //  The minimum value that can be added to the frequency histogram
    static const uint64_t minFreqValue = 0;

    // The maximum value that can be added to the frequency histogram
    static const uint64_t maxFreqValue = std::numeric_limits<uint8_t>::max();

    // The level of precision for the histogram.  The value must be between 1
    // and 5 (inclusive).
    static const int significantFigures = 3;

    // The value units per bucket that we use when creating the iterator
    // that traverses over the frequency histogram in the copyToHistogram
    // method.
    static const int valueUnitsPerBucket = 1;

    // The execution frequency histogram
    HdrHistogram freqHistogram{minFreqValue, maxFreqValue, significantFigures};

    // StatCounter: The number of frequencies that need to be added to the
    // frequency histogram before it is necessary to update the frequency
    // threshold.
    uint64_t requiredToUpdateInterval{1};
};
