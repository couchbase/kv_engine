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
#include <nlohmann/json.hpp>
#include <cstdlib> // Required due to the use of free
#include <iostream>
#include <sstream>

// Custom deleter for the hdr_histogram struct.
void HdrHistogram::HdrDeleter::operator()(struct hdr_histogram* val) {
    hdr_close(val);
}

HdrHistogram::HdrHistogram(uint64_t lowestTrackableValue,
                           uint64_t highestTrackableValue,
                           int significantFigures) {
    struct hdr_histogram* hist = nullptr;
    // We add a bias of +1 to the lowest and highest trackable value
    // because we add +1 to all values we store in the histogram (as this
    // allows us to record the value 0).
    hdr_init(lowestTrackableValue + 1,
             highestTrackableValue + 1, // Add one because all values
             significantFigures,
             &hist); // Pointer to initialise
    histogram.reset(hist);
}

HdrHistogram& HdrHistogram::operator+=(const HdrHistogram& other) {
    if (other.histogram != nullptr) {
        Iterator itr = other.makeLinearIterator(1);
        while (auto pair = other.getNextValueAndCount(itr)) {
            if (!pair.is_initialized())
                break;
            this->addValueAndCount(pair->first, pair->second);
        }
    }
    return *this;
}

void HdrHistogram::addValue(uint64_t v) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    hdr_record_value(histogram.get(), vBiased);
}

void HdrHistogram::addValueAndCount(uint64_t v, uint64_t count) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    hdr_record_values(histogram.get(), vBiased, count);
}

uint64_t HdrHistogram::getValueCount() const {
    return histogram->total_count;
}

void HdrHistogram::reset() {
    hdr_reset(histogram.get());
}

uint64_t HdrHistogram::getValueAtPercentile(double percentage) const {
    // We added the bias of +1 to the input value (see
    // addValueToFreqHistogram).  Therefore need to minus the bias
    // before returning the value from the histogram.
    // Note: If the histogram is empty we just want to return 0;
    uint64_t value = hdr_value_at_percentile(histogram.get(), percentage);
    uint64_t result = getValueCount() > 0 ? (value - 1) : 0;
    return result;
}

HdrHistogram::Iterator HdrHistogram::makeLinearIterator(
        int64_t valueUnitsPerBucket) const {
    HdrHistogram::Iterator iter;
    iter.type = Iterator::IterMode::Linear;
    hdr_iter_linear_init(&iter, histogram.get(), valueUnitsPerBucket);
    return iter;
}

HdrHistogram::Iterator HdrHistogram::makeLogIterator(int64_t firstBucketWidth,
                                                     double log_base) const {
    HdrHistogram::Iterator iter;
    iter.type = Iterator::IterMode::Log;
    hdr_iter_log_init(&iter, histogram.get(), firstBucketWidth, log_base);
    return iter;
}

boost::optional<std::pair<uint64_t, uint64_t>>
HdrHistogram::getNextValueAndCount(Iterator& iter) const {
    boost::optional<std::pair<uint64_t, uint64_t>> valueAndCount;
    if (hdr_iter_next(&iter)) {
        uint64_t value = iter.value;
        uint64_t count = 0;
        switch (iter.type) {
        case Iterator::IterMode::Log:
            count = iter.specifics.log.count_added_in_this_iteration_step;
            break;
        case Iterator::IterMode::Linear:
            count = iter.specifics.linear.count_added_in_this_iteration_step;
            break;
        default:
            count = iter.count;
            break;
        }

        // We added the bias of +1 to the input value (see
        // addValueToFreqHistogram).  Therefore need to minus the bias
        // before returning value.
        return valueAndCount = std::make_pair(value - 1, count);
    } else {
        return valueAndCount;
    }
}

void HdrHistogram::printPercentiles() {
    hdr_percentiles_print(histogram.get(), stdout, 100, 1.0, CLASSIC);
}

void HdrHistogram::dumpLinearValues(int64_t bucketWidth) {
    HdrHistogram::Iterator itr = makeLinearIterator(bucketWidth);
    std::cout << dumpValues(itr).str();
}

void HdrHistogram::dumpLogValues(int64_t firstBucketWidth, double log_base) {
    HdrHistogram::Iterator itr = makeLogIterator(firstBucketWidth, log_base);
    std::cout << dumpValues(itr).str();
}

std::stringstream HdrHistogram::dumpValues(Iterator& itr) {
    std::stringstream dump;
    while (auto pair = getNextValueAndCount(itr)) {
        dump << "Value[" << itr.value_iterated_from << "-"
             << itr.value_iterated_to << "]\tCount:\t" << pair->second
             << std::endl;
    }
    dump << "Total:\t" << itr.total_count << std::endl;

    return dump;
}

std::unique_ptr<nlohmann::json> HdrHistogram::to_json() {
    nlohmann::json rootObj;
    /* This is a first cut at a bucket width and a
     * better value will be need to be decided on after testing
     * (this experimentation is part of MB-22005)
     */
    uint64_t bucketWidth = 100;

    Iterator itr = makeLinearIterator(bucketWidth);

    if (itr.total_count > 0) {
        rootObj["total"] = itr.total_count;
        nlohmann::json dataArr;
        int i = 0;
        while (auto pair = getNextValueAndCount(itr)) {
            if (!pair.is_initialized())
                break;
            uint64_t count = pair->second;
            dataArr.push_back(
                    {itr.value_iterated_from, itr.value_iterated_to, count});
            i++;
        }
        rootObj["data"] = dataArr;
    }

    return std::make_unique<nlohmann::json>(rootObj);
}

std::string HdrHistogram::to_string() {
    return to_json()->dump();
}
