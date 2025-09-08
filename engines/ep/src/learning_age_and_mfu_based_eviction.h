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

#include "item_eviction_strategy.h"

#include "eviction_ratios.h"
#include "eviction_utils.h"

#include <hdrhistogram/hdrhistogram.h>

#include <cstdlib> // Required due to the use of free
#include <limits>
#include <utility>

class EPStats;

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
class LearningAgeAndMFUBasedEviction : public ItemEvictionStrategy {
public:
    LearningAgeAndMFUBasedEviction() = default;
    /**
     * Construct a "learning" eviction strategy, which can be used to decide
     * which values to attempt to evict, in order to remove a fraction (from
     * evictionRatios) of the coldest items from a vbucket.
     *
     * "Young" items (age determined from HLC cas) are protected from eviction,
     * based on @p agePercentage.
     *
     * This implementation learns an approximation of the MFU/freqCounter and
     * age distributions of items within a vb as it visits them. As a result,
     * the distributions used while visiting the first few items in a vbucket
     * may not be very precise. This is only significant with very small
     * datasets (e.g., synthetic sets in unit tests), where the decision to
     * evict a single item or not may noticably change the memory recovered.
     *
     * @param evictionRatios fraction of items to evict from
     *                       active+pending/replica vbuckets
     * @param agePercentage percentage of the "youngest" items to skip evicting
     * @param freqCounterAgeThreshold freq counter below which age should be
     *                                ignored. Ensures items will eventually be
     *                                evicted if repeatedly visited.
     * @param stats epstats in which per-vbucket visit stats will be recorded.
     *              Pointer, may be null to avoid tests which are not concerned
     *              with stats needing an EPStats instance anyway.
     */
    LearningAgeAndMFUBasedEviction(EvictionRatios evictionRatios,
                                   size_t agePercentage,
                                   uint16_t freqCounterAgeThreshold,
                                   EPStats* stats);

    // Clears the frequency histogram and sets the requiredToUpdateInterval
    // back to 1.
    void setupVBucketVisit(uint64_t numExpectedItems) override;

    void tearDownVBucketVisit(vbucket_state_t state) override;

    bool shouldTryEvict(uint8_t freq,
                        uint64_t age,
                        vbucket_state_t state) override;

    void eligibleItemSeen(uint8_t freq,
                          uint64_t age,
                          vbucket_state_t state) override;

    // Adds a frequency and age to the respective histograms.
    void addFreqAndAgeToHistograms(uint8_t freq, uint64_t age);

    // Returns the number of values added to the frequency histogram.
    uint64_t getFreqHistogramValueCount() const;

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

    // StatCounter:: Copies the contents of the frequency histogram into
    // the histogram given as an input parameter
    // @param hist  the destination histogram for the copy
    void copyFreqHistogram(HdrHistogram& hist);

    // StatCounter: The number of frequencies that need to be added to the
    // frequency histogram before it is not necessary to recalculate the
    // threshold every time we visit an item in the hash table.
    static const uint64_t learningPopulation = 100;

    /**
     * Directly set the freqCounterThreshold.
     *
     * Used only in tests, and will be removed once usages have been updated to
     * construct ItemEviction and inject it.
     */
    void setFreqCounterThreshold(uint16_t threshold) {
        freqCounterThreshold = threshold;
    }

private:
    // The level of precision for the age histogram.  The value must be
    // between 1 and 5 (inclusive).
    static const int ageSignificantFigures = 1;

    // The execution frequency "histogram". As we have a fixed and
    // relatively low number of frequency counter values (sizeof(uint8_t)) we
    // track these in an array rather than a HdrHistogram as it saves space
    // (~2040 bytes vs 16600 bytes) and time as we do not need atomicity.
    std::array<size_t, 256> freqCounters{};

    // ratio of active+pending and replica values to evict
    const EvictionRatios evictionRatios = {};

    // The age percent used to select the age threshold.  The value is
    // read by the ItemPager from a configuration parameter.
    const size_t agePercentage = 0;

    // The threshold for determining at what execution frequency should we
    // consider age when selecting items for eviction.  The value is
    // read by the ItemPager from a configuration parameter.
    const uint16_t freqCounterAgeThreshold = 0;

    // epstats ptr may be null for ease of testing; unit tests specifically
    // for learning behaviour don't necessarily need an EPStats instance.
    EPStats* const epstats = nullptr;

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
    HdrHistogram ageHistogram{1 /* minDiscernibleValue */,
                              cb::eviction::maxAgeValue,
                              ageSignificantFigures};

    // StatCounter: The number of frequencies that need to be added to the
    // frequency histogram before it is necessary to update the frequency
    // threshold.
    uint64_t requiredToUpdateInterval{1};

    // The frequency counter threshold that is used to determine whether we
    // should evict items from the hash table. Computed from freqCounters
    // periodically.
    uint16_t freqCounterThreshold = 0;

    // The age threshold that is used to determine whether we should evict
    // items from the hash table. Computed from ageHistogram periodically
    uint64_t ageThreshold = 0;
};
