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

#include "learning_age_and_mfu_based_eviction.h"
#include "item.h"

#include "stats.h"

#include <gsl/gsl-lite.hpp>

LearningAgeAndMFUBasedEviction::LearningAgeAndMFUBasedEviction(
        EvictionRatios evictionRatios,
        size_t agePercentage,
        uint16_t freqCounterAgeThreshold,
        EPStats* stats)
    : evictionRatios(evictionRatios),
      agePercentage(agePercentage),
      freqCounterAgeThreshold(freqCounterAgeThreshold),
      epstats(stats) {
}

bool LearningAgeAndMFUBasedEviction::shouldTryEvict(uint8_t freq,
                                                    uint64_t age,
                                                    vbucket_state_t state) {
    const double evictionRatio = evictionRatios.getForState(state);

    // A negative eviction ratio is invalid, and should never be encountered
    Expects(evictionRatio >= 0.0);

    // but an eviction ratio of exactly 0.0 is permissable (e.g., replica
    // eviction ratio set to a literal 0.0, but an active vb transitions to
    // replica after we build the vbucket filter but before we start visiting).
    if (evictionRatio == 0.0) {
        return false;
    }

    const bool belowMFUThreshold = freq <= freqCounterThreshold;
    // age exceeds threshold (from age histogram, set by config param
    // item_eviction_age_percentage)
    // OR
    // MFU is below threshold set by config param
    // item_eviction_freq_counter_age_threshold
    // Below this threshold the item is considered "cold" enough
    // to be evicted even if it is "young".
    const bool meetsAgeRequirements =
            age >= ageThreshold || freq < freqCounterAgeThreshold;

    // For replica vbuckets, young items are not protected from eviction.
    const bool isReplica = state == vbucket_state_replica;

    return belowMFUThreshold && (meetsAgeRequirements || isReplica);
}

void LearningAgeAndMFUBasedEviction::eligibleItemSeen(uint8_t freq,
                                                      uint64_t age,
                                                      vbucket_state_t state) {
    addFreqAndAgeToHistograms(freq, age);

    // Whilst we are learning it is worth always updating the
    // threshold. We also want to update the threshold at periodic
    // intervals.
    if (isLearning() || isRequiredToUpdate()) {
        std::tie(freqCounterThreshold, ageThreshold) = getThresholds(
                evictionRatios.getForState(state) * 100.0, agePercentage);
    }
}

void LearningAgeAndMFUBasedEviction::addFreqAndAgeToHistograms(uint8_t freq,
                                                               uint64_t age) {
    freqCounters[freq]++;
    totalFreqCounterValues++;
    ageHistogram.addValue(age);
}

uint64_t LearningAgeAndMFUBasedEviction::getFreqHistogramValueCount() const {
    return totalFreqCounterValues;
}

void LearningAgeAndMFUBasedEviction::setupVBucketVisit(
        uint64_t numExpectedItems) {
    freqCounters.fill(0);
    totalFreqCounterValues = 0;
    ageHistogram.reset();

    // Percent of items in the hash table to be visited
    // between updating the interval.
    const double percentOfItems = 0.1;
    // Calculate the number of items to visit before updating
    // the interval
    uint64_t noOfItems = std::ceil(numExpectedItems * (percentOfItems * 0.01));
    requiredToUpdateInterval =
            (noOfItems > learningPopulation) ? noOfItems : learningPopulation;
}

void LearningAgeAndMFUBasedEviction::tearDownVBucketVisit(
        vbucket_state_t state) {
    /**
     * Note: We are not taking a reader lock on the vbucket state.
     * Therefore it is possible that the stats could be slightly
     * out.  However given that its just for stats we don't want
     * to incur any performance cost associated with taking the
     * lock.
     */
    if (epstats) {
        const bool isActiveOrPending = (state == vbucket_state_active) ||
                                       (state == vbucket_state_pending);

        // select which histogram should be updated based on the state of the
        // vbucket just visited.
        auto& hist =
                isActiveOrPending
                        ? epstats->activeOrPendingFrequencyValuesSnapshotHisto
                        : epstats->replicaFrequencyValuesSnapshotHisto;

        // Take a snapshot of the latest frequency histogram
        hist.reset();
        copyFreqHistogram(hist);
    }
}

std::pair<uint16_t, uint64_t> LearningAgeAndMFUBasedEviction::getThresholds(
        double freqPercentage, double agePercentage) const {
    size_t runningTotal = 0;
    uint16_t freqThreshold;
    size_t percentileOfFreqCounts =
            (freqPercentage / 100.0) * totalFreqCounterValues;
    for (size_t i = 0; i < freqCounters.size(); i++) {
        runningTotal += freqCounters[i];
        if (runningTotal >= percentileOfFreqCounts) {
            freqThreshold = gsl::narrow<uint16_t>(i);
            break;
        }
    }

    uint64_t ageThreshold = ageHistogram.getValueAtPercentile(agePercentage);
    return std::make_pair(freqThreshold, ageThreshold);
}

void LearningAgeAndMFUBasedEviction::copyFreqHistogram(HdrHistogram& hist) {
    for (size_t i = 0; i < freqCounters.size(); i++) {
        hist.addValueAndCount(i, freqCounters[i]);
    }
}
