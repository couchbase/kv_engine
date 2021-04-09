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
#include "item.h"

#include <gsl/gsl>

ItemEviction::ItemEviction() {
}

void ItemEviction::addFreqAndAgeToHistograms(uint8_t freq, uint64_t age) {
    freqHistogram.addValue(freq);
    ageHistogram.addValue(age);
}

uint64_t ItemEviction::getFreqHistogramValueCount() const {
    return freqHistogram.getValueCount();
}

void ItemEviction::reset() {
    freqHistogram.reset();
    ageHistogram.reset();
    requiredToUpdateInterval = 1;
}

std::pair<uint16_t, uint64_t> ItemEviction::getThresholds(
        double freqPercentage, double agePercentage) const {
    auto freqThreshold = gsl::narrow<uint16_t>(
            freqHistogram.getValueAtPercentile(freqPercentage));
    uint64_t ageThreshold = ageHistogram.getValueAtPercentile(agePercentage);
    return std::make_pair(freqThreshold, ageThreshold);
}

uint8_t ItemEviction::convertFreqCountToNRUValue(uint8_t probCounter) {
    /*
     * The probabilistic counter has a range form 0 to 255, however the
     * increments are not linear - it gets more difficult to increment the
     * counter as its increases value.  Therefore incrementing from 0 to 1 is
     * much easier than incrementing from 254 to 255.
     *
     * Therefore when mapping to the 4 NRU values we do not simply want to
     * map 0-63 => 3, 64-127 => 2 etc.  Instead we want to reflect the bias
     * in the 4 NRU states.  Therefore we map as follows:
     * 0-3 => 3 (coldest), 4-31 => 2, 32->63 => 1, 64->255 => 0 (hottest),
     */
    if (probCounter >= 64) {
        return MIN_NRU_VALUE; /* 0 - the hottest */
    } else if (probCounter >= 32) {
        return 1;
    } else if (probCounter >= 4) {
        return INITIAL_NRU_VALUE; /* 2 */
    }
    return MAX_NRU_VALUE; /* 3 - the coldest */
}

void ItemEviction::copyFreqHistogram(HdrHistogram& hist) {
    hist = freqHistogram;
}
