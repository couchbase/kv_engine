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
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "relaxed_atomic.h"

/**
 * A simplistic implementation of a histogram backed by an array.
 *
 * This template is only suitable for "small" @p ArraySize values
 * as it requires an array of `CountType` sized to have an element for every
 * value [0, ArraySize).
 *
 * E.g.,
 *     ArrayHistogram<uint64_t, 256>
 *
 * will also instantiate an array of uint64_t, with size 256.
 *
 * This would not be viable with significantly larger ArraySize.
 *
 * By default the tracked values are cb::RelaxedAtomic<CountType>, and all
 * methods can be safely used from multiple threads.
 *
 * No locks are used; percentiles computed while the histogram is concurrently
 * modified will be approximate.
 *
 * If external synchronisation is provided or access is single threaded,
 * StoredType can be specified as a non-atomic type.
 *
 * @tparam CountType frequency type, type of the elements of the contained array
 * @tparam ArraySize number of bins the histogram should contain.
 * @tparam StoredType type used internally to store the frequency
 */
template <class CountType,
          size_t ArraySize,
          class StoredType = cb::RelaxedAtomic<CountType>>
class ArrayHistogram {
public:
    static_assert(
            ArraySize <= std::numeric_limits<uint16_t>::max() + 1,
            "ArrayHistogram allocates an array of size ArraySize."
            "Consider HdrHistogram if wider ranges of values are needed.");
    static_assert(!std::numeric_limits<CountType>::is_signed,
                  "ArrayHistogram does not expect negative frequency counts, "
                  "use an unsigned CountType");

    /**
     * Update the histogram, increasing the bin for @p value by
     * @p count.
     * @param value value to record in the histogram
     * @param count frequency count
     */
    void add(size_t value, CountType count = 1);

    /**
     * Update the histogram, decreasing the bin for @p value by
     * @p count.
     *
     * No bounds checking is applied; the user should ensure the value does
     * not underflow.
     *
     * @param value value to record in the histogram
     * @param count frequency count
     */
    void remove(size_t value, CountType count = 1);

    /**
     * Find the value at the requested percentile.
     *
     * Finds the lowest value for which @percentile samples are less than or
     * equal to that value.
     *
     * E.g.,
     *
     *  valueAtPercentile(50.0);
     *
     * Will return a value X such that 50% (or more) of the samples in the
     * histogram are less than or equal to X.
     *
     * No interpolation is applied.
     *
     * @param percentile desired percentile expressed in range [0.0,100.0]
     * @return the value at the desired percentile
     */
    size_t getValueAtPercentile(float percentile) const;

    /**
     * Get the total number of samples recorded in the histogram.
     */
    CountType getNumberOfSamples() const;

    /**
     * Check if the histogram is empty i.e., has no recorded samples.
     */
    bool empty() const;

    /**
     * Read how many samples have been recorded for a given value.
     */
    CountType operator[](size_t idx) const;

    ArrayHistogram& operator+=(const ArrayHistogram& other);

private:
    // array for storing the counters for each "bin".
    // sized to store a count for every value in [0, ArrayValue)
    std::array<StoredType, ArraySize> valueArray{{0}};
};

// extern instantiation of the most common variant
extern template class ArrayHistogram<uint64_t,
                                     std::numeric_limits<uint8_t>::max() + 1>;

template <class CountType, size_t ArraySize, class StoredType>
ArrayHistogram<CountType, ArraySize, StoredType> operator+(
        ArrayHistogram<CountType, ArraySize, StoredType> a,
        const ArrayHistogram<CountType, ArraySize, StoredType>& b) {
    a += b;
    return a;
}