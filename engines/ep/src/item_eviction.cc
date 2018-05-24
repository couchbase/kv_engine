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
    uint16_t freqThreshold = gsl::narrow<uint16_t>(
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
    HdrHistogram::Iterator iter{
            freqHistogram.makeLinearIterator(valueUnitsPerBucket)};
    while (auto result = freqHistogram.getNextValueAndCount(iter)) {
        hist.addValueAndCount(result->first, result->second);
    }
}
