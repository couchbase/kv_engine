/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "array_histogram.h"

#include <numeric>

namespace detail {
// helper to identify atomic counts, to use compare_exchange_weak when
// updating to avoid over/underflow.
template <class T>
constexpr bool is_atomic = false;

template <class U>
constexpr bool is_atomic<cb::RelaxedAtomic<U>> = true;

template <class U>
constexpr bool is_atomic<std::atomic<U>> = true;
} // namespace detail

template <class CountType, size_t ArraySize, class StoredType>
void ArrayHistogram<CountType, ArraySize, StoredType>::add(size_t value,
                                                           CountType count) {
    // Note: Count type is static asserted to be unsigned, this should not
    // be able to decrease the frequency counter outside of overflow.
    auto& frequency = valueArray.at(value);

    if constexpr (detail::is_atomic<StoredType>) {
        auto current = frequency.load();
        CountType desired;
        do {
            if (std::numeric_limits<CountType>::max() - current < count) {
                // this addition would overflow, saturate instead.
                desired = std::numeric_limits<CountType>::max();
            } else {
                desired = current + count;
            }
        } while (!frequency.compare_exchange_weak(current, desired));
    } else {
        if (std::numeric_limits<CountType>::max() - frequency < count) {
            // this addition would overflow, saturate instead.
            frequency = std::numeric_limits<CountType>::max();
        } else {
            frequency += count;
        }
    }
}

template <class CountType, size_t ArraySize, class StoredType>
void ArrayHistogram<CountType, ArraySize, StoredType>::remove(size_t value,
                                                              CountType count) {
    // Note: Count type is static asserted to be unsigned, this should not
    // be able to increase the frequency counter outside of underflow.
    auto& frequency = valueArray.at(value);

    if constexpr (detail::is_atomic<StoredType>) {
        auto current = frequency.load();
        CountType desired;
        do {
            if (current < count) {
                // this addition would underflow, clamp to zero instead.
                desired = 0;
            } else {
                desired = current - count;
            }
        } while (!frequency.compare_exchange_weak(current, desired));
    } else {
        if (frequency < count) {
            // this addition would overflow, saturate instead.
            frequency = 0;
        } else {
            frequency -= count;
        }
    }
}

template <class CountType, size_t ArraySize, class StoredType>
size_t ArrayHistogram<CountType, ArraySize, StoredType>::getValueAtPercentile(
        float percentile) const {
    auto totalCount = getNumberOfSamples();
    if (!totalCount) {
        // histogram is empty
        return 0;
    }

    // this threshold logic is equivalent to that of HdrHistogram
    // see hdr_value_at_percentile, and is tested for consistency with that
    // histogram impl in array_histogram_test.cc.

    // Find the number of samples which should be less than or equal to
    // the value at the requested percentile.
    // 0.5 bias accounts for cases where the number of samples is fractional
    // and should be rounded
    // e.g., without the bias
    //   3 samples, 50%ile
    //                      (50.0       / 100.0) * 3
    //  would give 1.5 samples, and would be truncated to 1.
    //  using the first sample as the 50th percentile could give too low
    // of a value -
    //  samples = {10, 20, 30}
    //  would find the 50th percentile to be 10.
    // With the bias:
    //                      (50.0       / 100.0) * 3 + 0.5
    //  would give 2.0 samples, and would find the 50th percentile to be 20.
    auto threshold = CountType(((percentile / 100.0f) * totalCount) + 0.5f);

    if (!threshold) {
        // the histogram is not empty, but the requested percentile
        // worked out a threshold of 0. This would lead to the result
        // being 0, as the 0th bucket (even if empty) would reach threshold.
        // Instead, for consistency with other histograms (HdrHistogram)
        // the value should be expected to be the lowest recorded value.
        // To achieve this, bump the threshold to 1. Now, the first bucket
        // containing any samples will meet the threshold, and the value for
        // that bucket will be the result.
        threshold = 1;
    }

    CountType totalSeen = 0;
    for (auto i = CountType(0); i < valueArray.size(); i++) {
        totalSeen += valueArray[i];
        if (totalSeen >= threshold) {
            return i;
        }
    }

    return valueArray.size();
}

template <class CountType, size_t ArraySize, class StoredType>
CountType ArrayHistogram<CountType, ArraySize, StoredType>::getNumberOfSamples()
        const {
    return std::accumulate(valueArray.begin(), valueArray.end(), 0);
}

template <class CountType, size_t ArraySize, class StoredType>
bool ArrayHistogram<CountType, ArraySize, StoredType>::empty() const {
    return getNumberOfSamples() == 0;
}

template <class CountType, size_t ArraySize, class StoredType>
CountType ArrayHistogram<CountType, ArraySize, StoredType>::operator[](
        size_t idx) const {
    return valueArray.at(idx);
}

template <class CountType, size_t ArraySize, class StoredType>
ArrayHistogram<CountType, ArraySize, StoredType>&
ArrayHistogram<CountType, ArraySize, StoredType>::operator+=(
        const ArrayHistogram& other) {
    // Note `other` is specified using the injected class name, so must be
    // the exact same type. This is intentional, and also means there's no
    // need to handle differently sized array histograms being summed.
    for (size_t i = 0; i < other.valueArray.size(); i++) {
        valueArray[i] += other[i];
    }
    return *this;
}

// explicit instantiation of the most common variant
template class ArrayHistogram<uint64_t,
                              std::numeric_limits<uint8_t>::max() + 1>;