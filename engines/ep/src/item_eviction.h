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

#pragma once

#include <hdrhistogram/hdrhistogram.h>

#include <cstdlib> // Required due to the use of free
#include <limits>
#include <utility>

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

public:
    ItemEviction();

    // Adds a frequency and age to the respective histograms.
    void addFreqAndAgeToHistograms(uint8_t freq, uint64_t age);

    // Returns the number of values added to the frequency histogram.
    uint64_t getFreqHistogramValueCount() const;

    // Clears the frequency histogram and sets the requiredToUpdateInterval
    // back to 1.
    void reset();

    // StatCounter: Returns the values held in the frequency and age
    // histograms at the percentiles defined by the input parameters.
    std::pair<uint16_t, uint64_t> getThresholds(double freqPercentage,
                                                double agePercentage) const;

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

    // StatCounter:: Copies the contents of the frequency histogram into
    // the histogram given as an input parameter
    // @param hist  the destination histogram for the copy
    void copyFreqHistogram(HdrHistogram& hist);

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

    static const uint64_t casBitsNotTime = 16;

private:
    // The maximum value that can be added to the age histogram
    static const uint64_t maxAgeValue = std::numeric_limits<uint64_t>::max() >> casBitsNotTime;

    // The level of precision for the age histogram.  The value must be
    // between 1 and 5 (inclusive).
    static const int ageSignificantFigures = 1;

    // The value units per bucket that we use when creating the iterator
    // that traverses over the frequency histogram in the copyToHistogram
    // method.
    static const int valueUnitsPerBucket = 1;

    // The execution frequency "histogram". As we have a fixed and
    // relatively low number of frequency counter values (sizeof(uint8_t)) we
    // track these in an array rather than a HdrHistogram as it saves space
    // (~2040 bytes vs 16600 bytes) and time as we do not need atomicity.
    std::array<size_t, 256> freqCounters{};

    // To find our value at a given percentile we also track the total number of
    // items we are tracking freq counter values for. With this we can
    // calculate how many items belong to that percentile, and then iterate the
    // freqCounters until we find a cumulative total exceeding that value to
    // map the percentile to a frequency counter value.
    size_t totalFreqCounterValues{0};

    // The age histogram.  Age is measured by taking the item's current cas
    // from the maxCas (which is the maximum cas value of the associated
    // vbucket).
    // The time in nanoseconds is stored in the top 48 bits of the cas
    // therefore we shift the age by casBitsNotTime.  This allows us
    // to have an age histogram with a reduced maximum value and
    // therefore reduces the memory requirements.
    HdrHistogram ageHistogram{
            1 /* minDiscernibleValue */, maxAgeValue, ageSignificantFigures};

    // StatCounter: The number of frequencies that need to be added to the
    // frequency histogram before it is necessary to update the frequency
    // threshold.
    uint64_t requiredToUpdateInterval{1};
};
