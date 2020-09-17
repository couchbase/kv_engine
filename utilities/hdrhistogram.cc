/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include <folly/lang/Assume.h>
#include <nlohmann/json.hpp>
#include <platform/cb_malloc.h>
#include <iostream>
#include <optional>
#include <sstream>

// Custom deleter for the hdr_histogram struct.
void HdrHistogram::HdrDeleter::operator()(struct hdr_histogram* val) {
    hdr_close_ex(val, cb_free);
}

HdrHistogram::HdrHistogram(uint64_t lowestTrackableValue,
                           uint64_t highestTrackableValue,
                           int significantFigures,
                           Iterator::IterMode iterMode) {
    if (lowestTrackableValue >= highestTrackableValue ||
        highestTrackableValue >=
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        throw std::logic_error(
                "HdrHistogram must have logical low/high tackable values min:" +
                std::to_string(lowestTrackableValue) +
                " max:" + std::to_string(highestTrackableValue));
    }

    struct hdr_histogram* hist = nullptr;
    // We add a bias of +1 to the lowest and highest trackable value
    // because we add +1 to all values we store in the histogram (as this
    // allows us to record the value 0).
    hdr_init_ex(lowestTrackableValue + 1,
                highestTrackableValue + 1, // Add one because all values
                significantFigures,
                &hist, // Pointer to initialise
                cb_calloc);
    histogram.wlock()->reset(hist);
    defaultIterationMode = iterMode;
}

HdrHistogram& HdrHistogram::operator+=(const HdrHistogram& other) {
    if (other.histogram.rlock()->get() != nullptr &&
        other.getValueCount() > 0) {
        // work out if we need to resize the receiving histogram
        uint64_t newMin = this->getMinTrackableValue();
        uint64_t newMax = this->getMaxTrackableValue();
        int32_t newSigfig = this->getSigFigAccuracy();
        bool resize = false;

        if (other.getMinTrackableValue() < this->getMinTrackableValue()) {
            newMin = other.getMinTrackableValue();
            resize = true;
        }
        if (other.getMaxTrackableValue() > this->getMaxTrackableValue()) {
            newMax = other.getMaxTrackableValue();
            resize = true;
        }

        if (other.getSigFigAccuracy() > this->getSigFigAccuracy()) {
            newSigfig = other.getSigFigAccuracy();
            resize = true;
        }
        if (resize) {
            this->resize(newMin, newMax, newSigfig);
        }
        hdr_add(this->histogram.rlock()->get(), other.histogram.rlock()->get());
    }
    return *this;
}

bool HdrHistogram::addValue(uint64_t v) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    return hdr_record_value(histogram.rlock()->get(), vBiased);
}

bool HdrHistogram::addValueAndCount(uint64_t v, uint64_t count) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    return hdr_record_values(histogram.rlock()->get(), vBiased, count);
}

uint64_t HdrHistogram::getValueCount() const {
    return static_cast<uint64_t>(histogram.rlock()->get()->total_count);
}

uint64_t HdrHistogram::getMinValue() const {
    // check there are values in the histogram otherwise hdr_min() will return
    // 0 which will cause use to return a UINT64_MAX.
    if (getValueCount()) {
        return static_cast<uint64_t>(hdr_min(histogram.rlock()->get()) - 1);
    }
    return 0;
}

uint64_t HdrHistogram::getMaxValue() const {
    // check there are values in the histogram otherwise hdr_max() will return
    // 0 which will cause use to return a UINT64_MAX.
    if (getValueCount()) {
        return static_cast<uint64_t>(hdr_max(histogram.rlock()->get()) - 1);
    }
    return 0;
}

void HdrHistogram::reset() {
    hdr_reset(histogram.wlock()->get());
}

uint64_t HdrHistogram::getValueAtPercentile(double percentage) const {
    // We added the bias of +1 to the input value (see
    // addValueToFreqHistogram).  Therefore need to minus the bias
    // before returning the value from the histogram.
    // Note: If the histogram is empty we just want to return 0;
    uint64_t value =
            hdr_value_at_percentile(histogram.rlock()->get(), percentage);
    uint64_t result = getValueCount() > 0 ? (value - 1) : 0;
    return result;
}

HdrHistogram::Iterator HdrHistogram::makeLinearIterator(
        int64_t valueUnitsPerBucket) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Linear);
    hdr_iter_linear_init(&itr, itr.histoRLockPtr->get(), valueUnitsPerBucket);
    return itr;
}

HdrHistogram::Iterator HdrHistogram::makeLogIterator(int64_t firstBucketWidth,
                                                     double log_base) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Log);
    hdr_iter_log_init(
            &itr, itr.histoRLockPtr->get(), firstBucketWidth, log_base);
    return itr;
}

HdrHistogram::Iterator HdrHistogram::makePercentileIterator(
        uint32_t ticksPerHalfDist) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Percentiles);
    hdr_iter_percentile_init(&itr, itr.histoRLockPtr->get(), ticksPerHalfDist);
    return itr;
}

HdrHistogram::Iterator HdrHistogram::makeRecordedIterator() const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Recorded);
    hdr_iter_recorded_init(&itr, itr.histoRLockPtr->get());
    return itr;
}

HdrHistogram::Iterator HdrHistogram::makeIterator(
        Iterator::IterMode mode) const {
    switch (mode) {
    case Iterator::IterMode::Percentiles:
        return makePercentileIterator(5);
    case Iterator::IterMode::Log:
        return makeLogIterator(1, 2);
    case Iterator::IterMode::Linear: {
        int64_t bucketWidth =
                (getMaxTrackableValue() - getMinTrackableValue()) / 20;
        if (bucketWidth < 0) {
            bucketWidth = 1;
        }
        return makeLinearIterator(bucketWidth);
    }
    case Iterator::IterMode::Recorded:
        return makeRecordedIterator();
    }
    folly::assume_unreachable();
}

HdrHistogram::Iterator HdrHistogram::getHistogramsIterator() const {
    return makeIterator(defaultIterationMode);
}

std::optional<std::pair<uint64_t, uint64_t>> HdrHistogram::getNextValueAndCount(
        Iterator& iter) const {
    std::optional<std::pair<uint64_t, uint64_t>> valueAndCount;
    if (hdr_iter_next(&iter)) {
        auto value = static_cast<uint64_t>(iter.value);
        uint64_t count = 0;
        switch (iter.type) {
        case Iterator::IterMode::Log:
            count = iter.specifics.log.count_added_in_this_iteration_step;
            break;
        case Iterator::IterMode::Linear:
            count = iter.specifics.linear.count_added_in_this_iteration_step;
            break;
        case Iterator::IterMode::Percentiles:
            value = iter.highest_equivalent_value;
            count = iter.cumulative_count - iter.lastCumulativeCount;
            iter.lastCumulativeCount = iter.cumulative_count;
            break;
        case Iterator::IterMode::Recorded:
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

std::optional<std::tuple<uint64_t, uint64_t, uint64_t>>
HdrHistogram::getNextBucketLowHighAndCount(Iterator& iter) const {
    std::optional<std::tuple<uint64_t, uint64_t, uint64_t>> bucketHighLowCount;
    auto valueAndCount = getNextValueAndCount(iter);
    if (valueAndCount.has_value()) {
        bucketHighLowCount = std::make_tuple(
                iter.lastVal, valueAndCount->first, valueAndCount->second);
        iter.lastVal = valueAndCount->first;
    }
    return bucketHighLowCount;
}

std::optional<std::pair<uint64_t, double>>
HdrHistogram::getNextValueAndPercentile(Iterator& iter) const {
    std::optional<std::pair<uint64_t, double>> valueAndPer;
    if (iter.type == Iterator::IterMode::Percentiles) {
        if (hdr_iter_next(&iter)) {
            // We subtract one from the lowest value as we have added a one
            // offset as the underlying hdr_histogram cannot store 0 and
            // therefore the value must be greater than or equal to 1.
            valueAndPer = std::make_pair(iter.highest_equivalent_value - 1,
                                         iter.specifics.percentiles.percentile);
        }
    }
    return valueAndPer;
}

void HdrHistogram::printPercentiles() const {
    hdr_percentiles_print(histogram.rlock()->get(), stdout, 5, 1.0, CLASSIC);
}

void HdrHistogram::dumpLinearValues(int64_t bucketWidth) {
    auto itr = makeLinearIterator(bucketWidth);
    std::cout << dumpValues(itr).str();
}

void HdrHistogram::dumpLogValues(int64_t firstBucketWidth, double log_base) {
    auto itr = makeLogIterator(firstBucketWidth, log_base);
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

nlohmann::json HdrHistogram::to_json(Iterator::IterMode itrType) {
    nlohmann::json rootObj;

    // Five is the number of iteration steps per half-distance to 100%.
    auto itr = makePercentileIterator(5);

    rootObj["total"] = itr.total_count;
    // bucketsLow represents the starting value of the first bucket
    // e.g. if the first bucket was from [0 - 10]ms then it would be 0
    rootObj["bucketsLow"] = itr.value_iterated_from;
    nlohmann::json dataArr;

    int64_t lastval = 0;
    while (auto pair = getNextValueAndPercentile(itr)) {
        if (!pair.has_value())
            break;

        dataArr.push_back(
                {pair->first, itr.cumulative_count - lastval, pair->second});
        lastval = itr.cumulative_count;
    }
    rootObj["data"] = dataArr;

    return rootObj;
}

std::string HdrHistogram::to_string() {
    return to_json().dump();
}

size_t HdrHistogram::getMemFootPrint() const {
    return hdr_get_memory_size(histogram.rlock()->get()) + sizeof(HdrHistogram);
}

double HdrHistogram::getMean() const {
    return hdr_mean(histogram.rlock()->get()) - 1;
}

void HdrHistogram::resize(uint64_t lowestTrackableValue,
                          uint64_t highestTrackableValue,
                          int significantFigures) {
    if (lowestTrackableValue >= highestTrackableValue ||
        highestTrackableValue >=
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        throw std::logic_error(
                "HdrHistogram must have logical low/high tackable values min:" +
                std::to_string(lowestTrackableValue) +
                " max:" + std::to_string(highestTrackableValue));
    }

    struct hdr_histogram* hist = nullptr;
    // We add a bias of +1 to the lowest and highest value that can be tracked
    // because we add +1 to all values we store in the histogram (as this
    // allows us to record the value 0).
    hdr_init_ex(lowestTrackableValue + 1,
                highestTrackableValue + 1, // Add one because all values
                significantFigures,
                &hist, // Pointer to initialise
                cb_calloc);

    hdr_add(hist, histogram.rlock()->get());
    histogram.wlock()->reset(hist);
}
