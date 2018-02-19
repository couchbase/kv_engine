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

#include <limits>

#include <hdr_histogram.h>

ItemEviction::ItemEviction() : requiredToUpdateInterval(learningPopulation) {
    struct hdr_histogram* hist;
    hdr_init(minFreqValue,
             maxFreqValue,
             3, // Number of significant figures
             &hist); // Pointer to initialise
    freqHistogram.reset(hist);
}

void ItemEviction::addValueToFreqHistogram(uint8_t v) {
    //  A hdr_histogram cannot store 0.  Therefore we add one so the
    // range moves from 0 -> 255 to 1 -> 256.
    uint16_t vBiased = v + 1;
    hdr_record_value(freqHistogram.get(), vBiased);
}

uint64_t ItemEviction::getFreqHistogramValueCount() const {
    return freqHistogram->total_count;
}

void ItemEviction::reset() {
    hdr_reset(freqHistogram.get());
}

uint16_t ItemEviction::getFreqThreshold(uint8_t percentage) const {
    return hdr_value_at_percentile(freqHistogram.get(), percentage);
}
