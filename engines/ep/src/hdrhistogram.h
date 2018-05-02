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

#pragma once

#include <boost/optional/optional.hpp>
#include <memory>
#include <utility>

#include <hdr_histogram.h>

/**
 * A container for the c hdr_histogram data structure.
 *
 */
class HdrHistogram {
    // Custom deleter for the hdr_histogram struct.
    struct HdrDeleter {
        void operator()(struct hdr_histogram* val);
    };

    using UniquePtr = std::unique_ptr<struct hdr_histogram, HdrDeleter>;

public:
#ifdef WIN32
    /**
     * The following typedef is required because of an issue msvc2015 where
     * use of HdrHistogram::Iterator in statwriter.h causes the following
     * error:
     * C2079: 'iter' uses undefined struct 'HdrHistogram::hdr_iter'
     */
    typedef struct hdr_iter HdrIter;
    using Iterator = HdrIter;
#else
    using Iterator = struct hdr_iter;
#endif

    /**
     * Constructor for the histogram.
     * @param lowestTrackableValue  smallest value that can be held in the
     *        histogram.  Note the underlying hdr_histogram cannot store 0 and
     *        therefore the value must be greater than or equal to 1.
     * @param highestTrackableValue  the largest values that can be held in
     *        the histogram
     * @param sigificantFigures  the level of precision for the histogram.
     *        Note the underlying hdr_histogram requires the value to be
     *        between 1 and 5 (inclusive).
     */
    HdrHistogram(uint64_t lowestTrackableValue,
                 uint64_t highestTrackableValue,
                 int significantFigures);

    /**
     * Adds a value to the histogram.
     */
    void addValue(uint64_t v);

    /**
     * Adds a value and associated count to the histogram.
     */
    void addValueAndCount(uint64_t v, uint64_t count);

    /**
     * Returns the number of values added to the histogram.
     */
    uint64_t getValueCount() const;

    /**
     * Clears the histogram.
     */
    void reset();

    /**
     * Returns the value held in the histogram at the percentile defined by
     * the input parameter percentage.
     */
    uint64_t getValueAtPercentile(double percentage) const;

    /**
     * Returns a linear iterator for the histogram
     * @param valueUnitsPerBucket  the number of values to be grouped into
     *        a single bucket
     */
    Iterator makeLinearIterator(int64_t valueUnitsPerBucket) const;

    /**
     * Gets the next value and corresponding count from the histogram
     * Returns an optional pair, comprising of:
     * 1) value
     * 2) count associated with the value
     * The pair is optional because iterating past the last value in the
     * histogram will return no result.
     */
    boost::optional<std::pair<uint64_t, uint64_t>> getNextValueAndCount(
            Iterator& iter) const;

private:
    /**
     * unique_ptr to a hdr_histogram structure
     */
    UniquePtr histogram;
};
