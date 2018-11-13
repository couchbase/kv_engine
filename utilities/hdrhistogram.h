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
#include <nlohmann/json_fwd.hpp>
#include <memory>
#include <utility>

// hdr_histogram.h emits a warning on windows
#ifdef WIN32
#pragma warning(push, 0)
#endif

#include <hdr_histogram.h>

#ifdef WIN32
#pragma warning(pop)
#endif

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
    struct Iterator : public hdr_iter {
        /**
         * Enum to state the method of iteration the iterator is using
         */
        enum class IterMode {
            /**
             * Log enum is a type of iterator that will move
             * though values in logarithmically increasing steps
             */
            Log,
            /**
             * Linear enum is a type of iterator that will move
             * though values in consistent step widths
             */
            Linear,
            /**
             * Recorded enum is a type of iterator that will move
             * though all values by the finest granularity
             * supported by the underlying data structure
             */
            Recorded,
            /**
             * Percentiles enum is a type of iterator that will move
             * though values by percentile levels step widths
             */
            Percentiles
        };
        IterMode type;
    };

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
     * Addition assigment operator for aggregation of histograms
     * across buckets
     * @param other histogram to add to this one
     * @return returns this histogram with the addition of the values from
     * the other histogram
     */
    HdrHistogram& operator+=(const HdrHistogram& other);

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
     * Returns a log iterator for the histogram
     * @param firstBucketWidth  the number of values to be grouped into
     *        the first bucket bucket
     * @param log_base base of the logarithm of iterator
     */
    Iterator makeLogIterator(int64_t firstBucketWidth, double log_base) const;

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

    /**
     * prints the histogram counts by percentiles to stdout
     */
    void printPercentiles();

    /**
     * dumps the histogram to stdout using a logarithmic iterator
     * @param firstBucketWidth range of the first bucket
     * @param log_base base of the logarithmic iterator
     */
    void dumpLogValues(int64_t firstBucketWidth, double log_base);

    /**
     * dumps the histogram to stdout using a linear iterator
     * @param bucketWidth size of each bucket in terms of range
     */
    void dumpLinearValues(int64_t bucketWidth);

    /**
     * Dumps the histograms count data to a string stream
     * using the iterator its called with
     * @param itr iterator to be used to access the histogram data
     * @return a string stream containing the histogram dump
     */
    std::stringstream dumpValues(Iterator& itr);

    /**
     * Method to get the histogram as a json object
     * @return a nlohmann::json containing the histograms data iterated linearly
     */
    std::unique_ptr<nlohmann::json> to_json();

    /**
     * Dumps the histogram data to json in a string form
     * @return a string of histogram json data
     */
    std::string to_string();

private:
    /**
     * unique_ptr to a hdr_histogram structure
     */
    UniquePtr histogram;
};
