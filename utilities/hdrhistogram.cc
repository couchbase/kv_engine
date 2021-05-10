/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "hdrhistogram.h"

#include <folly/lang/Assume.h>
#include <nlohmann/json.hpp>
#include <platform/cb_malloc.h>
#include <spdlog/fmt/fmt.h>
#include <iostream>
#include <optional>
#include <type_traits>

// Custom deleter for the hdr_histogram struct.
void HdrHistogram::HdrDeleter::operator()(struct hdr_histogram* val) {
    hdr_close_ex(val, cb_free);
}

HdrHistogram::HdrHistogram(uint64_t lowestDiscernibleValue,
                           int64_t highestTrackableValue,
                           int significantFigures,
                           Iterator::IterMode iterMode)
    : defaultIterationMode(iterMode) {
    auto handle = histogram.wlock();
    resize(handle,
           lowestDiscernibleValue,
           highestTrackableValue,
           significantFigures);
}

HdrHistogram& HdrHistogram::operator+=(const HdrHistogram& other) {
    /*
     * Note: folly::acquireLockedPair() takes an exclusive lock on
     * this->histogram as we pass it a non-const ref. Where as it takes a shared
     * lock on other.histogram as we pass it a const ref. We can do this as we
     * only need to write to the ptr of this->histogram. (see
     * folly::acquireLocked() source code comments for more information regard
     * how it takes locks)
     */
    auto lockPair = folly::acquireLockedPair(this->histogram, other.histogram);
    // Don't use structured binding as this messes up clang-analyzer, analyzing
    // the destruction of folly's locks.
    WHistoLockedPtr& thisLock = lockPair.first;
    ConstRHistoLockedPtr& otherLock = lockPair.second;
    if (otherLock->get()->total_count > 0) {
        // work out if we need to resize the receiving histogram
        auto newDiscernibleMin = getMinDiscernibleValue(thisLock);
        auto newMax = getMaxTrackableValue(thisLock);
        auto newSigfig = getSigFigAccuracy(thisLock);
        bool resize = false;

        if (getMinDiscernibleValue(otherLock) < newDiscernibleMin) {
            newDiscernibleMin = getMinDiscernibleValue(otherLock);
            resize = true;
        }
        if (getMaxTrackableValue(otherLock) > newMax) {
            newMax = getMaxTrackableValue(otherLock);
            resize = true;
        }

        if (getSigFigAccuracy(otherLock) > newSigfig) {
            newSigfig = getSigFigAccuracy(otherLock);
            resize = true;
        }
        if (resize) {
            this->resize(thisLock, newDiscernibleMin, newMax, newSigfig);
        }
        hdr_add(thisLock->get(), otherLock->get());
    }
    return *this;
}

HdrHistogram& HdrHistogram::operator=(const HdrHistogram& other) {
    if (this != &other) {
        // reset this object to make sure we are in a state to copy too
        this->reset();
        // take advantage of the code already written in for the addition
        // assigment operator
        *this += other;
    }
    return *this;
}

bool HdrHistogram::addValue(uint64_t v) {
    return hdr_record_value(histogram.rlock()->get(), v);
}

bool HdrHistogram::addValueAndCount(uint64_t v, uint64_t count) {
    return hdr_record_values(histogram.rlock()->get(), v, count);
}

uint64_t HdrHistogram::getValueCount() const {
    return static_cast<uint64_t>(histogram.rlock()->get()->total_count);
}

uint64_t HdrHistogram::getMinValue() const {
    return static_cast<uint64_t>(hdr_min(histogram.rlock()->get()));
}

uint64_t HdrHistogram::getMaxValue() const {
    return static_cast<uint64_t>(hdr_max(histogram.rlock()->get()));
}

void HdrHistogram::reset() {
    hdr_reset(histogram.wlock()->get());
}

uint64_t HdrHistogram::getValueAtPercentile(double percentage) const {
    return hdr_value_at_percentile(histogram.rlock()->get(), percentage);
}

HdrHistogram::Iterator HdrHistogram::makeLinearIterator(
        int64_t valueUnitsPerBucket) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Linear);
    hdr_iter_linear_init(&itr, itr.histoRLockPtr->get(), valueUnitsPerBucket);
    // hdr_histogram requires hdr_iter_next to be called before reading any
    // values, but C++ input iterators should start pointing to the first value
    // (or end() if empty). Advance the underlying iterator to valid data
    // immediately.
    return std::move(++itr);
}

HdrHistogram::Iterator HdrHistogram::makeLogIterator(int64_t firstBucketWidth,
                                                     double log_base) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Log);
    hdr_iter_log_init(
            &itr, itr.histoRLockPtr->get(), firstBucketWidth, log_base);

    return std::move(++itr);
}

HdrHistogram::Iterator HdrHistogram::makePercentileIterator(
        uint32_t ticksPerHalfDist) const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Percentiles);
    hdr_iter_percentile_init(&itr, itr.histoRLockPtr->get(), ticksPerHalfDist);

    return std::move(++itr);
}

HdrHistogram::Iterator HdrHistogram::makeRecordedIterator() const {
    HdrHistogram::Iterator itr(histogram, Iterator::IterMode::Recorded);
    hdr_iter_recorded_init(&itr, itr.histoRLockPtr->get());

    return std::move(++itr);
}

HdrHistogram::Iterator HdrHistogram::makeIterator(
        Iterator::IterMode mode) const {
    switch (mode) {
    case Iterator::IterMode::Percentiles:
        return makePercentileIterator(5);
    case Iterator::IterMode::Log:
        return makeLogIterator(1, 2);
    case Iterator::IterMode::Linear: {
        int64_t bucketWidth = getMaxTrackableValue() / 20;
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

HdrHistogram::Iterator HdrHistogram::begin() const {
    return getHistogramsIterator();
}

std::string HdrHistogram::Iterator::dumpValues() {
    fmt::memory_buffer dump;
    while (*this != EndSentinel{}) {
        fmt::format_to(dump,
                       "Value[{}-{}]\tCount:{}\t\n",
                       value_iterated_from,
                       value_iterated_to,
                       bucket.count);
        ++(*this);
    }
    fmt::format_to(dump, "Total:\t{}\n", total_count);
    return {dump.data(), dump.size()};
}

void HdrHistogram::printPercentiles() const {
    hdr_percentiles_print(histogram.rlock()->get(), stdout, 5, 1.0, CLASSIC);
}

void HdrHistogram::dumpLinearValues(int64_t bucketWidth) const {
    HdrHistogram::Iterator itr = makeLinearIterator(bucketWidth);
    fmt::print(stdout, itr.dumpValues());
}

void HdrHistogram::dumpLogValues(int64_t firstBucketWidth,
                                 double log_base) const {
    HdrHistogram::Iterator itr = makeLogIterator(firstBucketWidth, log_base);
    fmt::print(stdout, itr.dumpValues());
}

bool HdrHistogram::Iterator::incrementUnderlyingIterator() {
    if (state != IterState::ReadingRecordedValues) {
        // the underlying C iterator has already been exhausted
        return false;
    }

    if (!hdr_iter_next(this)) {
        // underlying iterator is now at end, iterator needs to "manually"
        // compute any remaining buckets up to the max representable.
        state = IterState::ComputingRemainingBuckets;
        return false;
    }

    // advance the bucket boundaries, the (exclusive) lower bound
    // is advance to the old (inclusive) upper bound
    bucket.lower_bound = bucket.upper_bound;
    bucket.upper_bound = static_cast<uint64_t>(this->value);

    switch (type) {
    case Iterator::IterMode::Log:
        bucket.count = specifics.log.count_added_in_this_iteration_step;
        break;
    case Iterator::IterMode::Linear:
        bucket.count = specifics.linear.count_added_in_this_iteration_step;
        break;
    case Iterator::IterMode::Percentiles:
        bucket.upper_bound = highest_equivalent_value;
        bucket.count = cumulative_count - lastCumulativeCount;
        lastCumulativeCount = cumulative_count;
        bucket.percentile = specifics.percentiles.percentile;
        break;
    case Iterator::IterMode::Recorded:
        bucket.count = this->count;
        break;
    }

    return true;
}

// helper for static_assert in constexpr if to get a dependent false value.
// static_assert(false,...) even in a constexpr branch makes the program
// ill-formed. By making the value dependent (even though it is always false)
// then:
//    if constexpr (foo_v) {
//        static_assert(false_v<bar>);
//    }
// works as desired, failing compilation iff foo_v

template <typename...>
inline constexpr bool false_v = false;

template <class Specifics>
void HdrHistogram::Iterator::advanceToNextBucket(Specifics& specifics) {
    if (bucket.upper_bound >=
        uint64_t(hdr_lowest_equivalent_value(
                h, HdrHistogram::getMaxTrackableValue(histoRLockPtr)))) {
        state = IterState::Finished;
        return;
    }
    // this is a recreation of a section of _{log,linear}_iter_next from
    // hdr_histogram.c - this is rather hacky but to avoid this would
    // require a hacky change to the underlying C library.
    // advance the buckets in the same manner the underlying iterator would.
    // It might seem that the bounds could be directly calculated
    // e.g., newFoo = foo*log_base
    // but this does not reach the same values hdr_histogram would.
    // It's possible this could be made correct by considering equivalent value
    // ranges and other quirks, but for simplicity and reliability the values
    // are calculated here just as in the C iterator, by incrementing
    // counts_index.

    // computed buckets are always empty, they are past the highest recorded
    // value.
    bucket.count = 0;

    // previous inclusive upper bound is the new exclusive lower bound
    bucket.lower_bound = bucket.upper_bound;

    // if the histogram contains nothing, counts_index may be -1.
    counts_index = std::max(0, counts_index);

    while (counts_index < h->counts_len &&
           bucket.upper_bound <
                   uint64_t(
                           specifics
                                   .next_value_reporting_level_lowest_equivalent)) {
        bucket.upper_bound = hdr_value_at_index(h, counts_index);
        ++counts_index;
    }

    if constexpr (std::is_same_v<Specifics, hdr_iter_log>) {
        specifics.next_value_reporting_level *= specifics.log_base;
    } else if constexpr (std::is_same_v<Specifics, hdr_iter_linear>) {
        specifics.next_value_reporting_level +=
                specifics.value_units_per_bucket;
    } else {
        static_assert(
                false_v<Specifics>,
                "Only log or linear iterators can compute buckets beyond the "
                "recorded values");
    }

    specifics.next_value_reporting_level_lowest_equivalent =
            hdr_lowest_equivalent_value(h,
                                        specifics.next_value_reporting_level);
}

HdrHistogram::Iterator& HdrHistogram::Iterator::operator++() {
    if (state == IterState::Finished) {
        return *this;
    }
    if (!incrementUnderlyingIterator()) {
        // the underlying C iterator has reached the end of the recorded values
        // and has stopped, but the caller may wish to iterate over all buckets
        // up to the max representable value.
        // Log and Linear iterators can compute the remaining buckets.
        switch (type) {
        case Iterator::IterMode::Percentiles:
        case Iterator::IterMode::Recorded:
            // nothing to do for these iteration modes, it's not meaningful
            // to go past the highest representable value.
            state = IterState::Finished;
            break;
        case Iterator::IterMode::Log:
            advanceToNextBucket(specifics.log);
            break;
        case Iterator::IterMode::Linear:
            advanceToNextBucket(specifics.linear);
            break;
        }
    }
    return *this;
}

bool HdrHistogram::Iterator::operator==(const Iterator& other) const {
    // ptr comparison of histograms, value comparison of current bucket
    return *histoRLockPtr == *other.histoRLockPtr && bucket == other.bucket;
}

bool HdrHistogram::Iterator::operator==(const EndSentinel& sentinel) const {
    // check if underlying iterator has reached the end.
    if (sentinel == EndSentinel::HighestRecorded) {
        // if the iterator is no longer reading buckets from the underlying
        // C iterator, then the highest recorded value has been seen.
        return state != IterState::ReadingRecordedValues;
    }
    return state == IterState::Finished;
}

nlohmann::json HdrHistogram::to_json() const {
    nlohmann::json rootObj;

    // Five is the number of iteration steps per half-distance to 100%.
    auto itr = makePercentileIterator(5);

    rootObj["total"] = itr.total_count;
    // bucketsLow represents the starting value of the first bucket
    // e.g. if the first bucket was from [0 - 10]ms then it would be 0
    rootObj["bucketsLow"] = itr.value_iterated_from;
    nlohmann::json dataArr;

    while (itr != end()) {
        dataArr.push_back({itr->upper_bound, itr->count, *itr->percentile});
        ++itr;
    }
    rootObj["data"] = dataArr;

    return rootObj;
}

std::string HdrHistogram::to_string() const {
    return to_json().dump();
}

size_t HdrHistogram::getMemFootPrint() const {
    return hdr_get_memory_size(histogram.rlock()->get()) + sizeof(HdrHistogram);
}

double HdrHistogram::getMean() const {
    return hdr_mean(histogram.rlock()->get());
}

void HdrHistogram::resize(WHistoLockedPtr& histoLockPtr,
                          uint64_t lowestDiscernibleValue,
                          int64_t highestTrackableValue,
                          int significantFigures) {
    // hdr_init_ex will also check but will just return EINVAL. Check here
    // first so we can generate amore useful exception message, as this could be
    // a common mistake when adding a new histogram
    if (lowestDiscernibleValue == 0) {
        // lowestDiscernibleValue set to zero is not meaningful
        throw std::invalid_argument(fmt::format(
                "HdrHistogram lowestDiscernibleValue:{} must be greater than 0",
                lowestDiscernibleValue));
    }

    struct hdr_histogram* hist = nullptr;

    auto status = hdr_init_ex(lowestDiscernibleValue,
                              highestTrackableValue,
                              significantFigures,
                              &hist, // Pointer to initialise
                              cb_calloc);

    if (status != 0) {
        throw std::system_error(
                status,
                std::generic_category(),
                fmt::format("HdrHistogram init failed, "
                            "params lowestDiscernibleValue:{} "
                            "highestTrackableValue:{} significantFigures:{}",
                            lowestDiscernibleValue,
                            highestTrackableValue,
                            significantFigures));
    }

    if (*histoLockPtr) {
        hdr_add(hist, histoLockPtr->get());
    }

    histoLockPtr->reset(hist);
}

std::ostream& operator<<(std::ostream& os,
                         const HdrHistogram::Iterator::IterMode& mode) {
    switch (mode) {
    case HdrHistogram::Iterator::IterMode::Log:
        return os << "Log";
    case HdrHistogram::Iterator::IterMode::Linear:
        return os << "Linear";
    case HdrHistogram::Iterator::IterMode::Recorded:
        return os << "Recorded";
    case HdrHistogram::Iterator::IterMode::Percentiles:
        return os << "Percentiles";
    }
    folly::assume_unreachable();
}