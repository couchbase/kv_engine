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

#include <boost/optional/optional_fwd.hpp>
#include <nlohmann/json_fwd.hpp>
#include <chrono>
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

    HdrHistogram() : HdrHistogram(0, 1, 1){};

    /**
     * Copy constructor to define how to do a deep copy of a HdrHistogram
     * @param other other HdrHistogram to copy
     */
    HdrHistogram(const HdrHistogram& other)
        : HdrHistogram(other.getMinTrackableValue(),
                       other.getMaxTrackableValue(),
                       other.getSigFigAccuracy()) {
        // take advantage of the code already written in for the addition
        // assigment operator
        *this += other;
    };

    /**
     * Assignment operator to perform a deep copy of one histogram to another
     * @param other HdrHistogram to copy
     * @return returns this HdrHistogram, which is now a copy of the other
     * HdrHistogram
     */
    HdrHistogram& operator=(const HdrHistogram& other) {
        // reset this object to make sure we are in a state to copy too
        this->reset();
        // take advantage of the code already written in for the addition
        // assigment operator
        *this += other;
        return *this;
    };

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
     * @param v value to be added to the histogram and account for by 1 count
     * @return true if it successfully added that value to the histogram
     */
    bool addValue(uint64_t v);

    /**
     * Adds a value and associated count to the histogram.
     * @param v value to be added to the histogram
     * @param count number of counts that should be added to the histogram
     * for this value v.
     * @return true if it successfully added that value to the histogram
     */
    bool addValueAndCount(uint64_t v, uint64_t count);

    /**
     * Returns the number of values added to the histogram.
     */
    uint64_t getValueCount() const;

    /**
     * Returns the min value stored to the histogram
     */
    uint64_t getMinValue() const;

    /**
     * Returns the max value stored to the histogram
     */
    uint64_t getMaxValue() const;

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
     * Returns a percentile iterator for the histogram
     * @param ticksPerHalfDist The number iteration steps per
     * half-distance to 100%.
     * @return iterator that moves over the histogram as percentiles
     */
    Iterator makePercentileIterator(uint32_t ticksPerHalfDist) const;

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
     * Gets the next value and corresponding percentile from the histogram
     * Returns an optional pair, comprising of:
     * 1) highest equivalent value
     * 2) next percentile that the iterator moves to
     * The pair is optional because iterating past the last value in the
     * histogram will return no result.
     */
    boost::optional<std::pair<uint64_t, double>> getNextValueAndPercentile(
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
     * itrType method which to iterate over the data
     * @return a nlohmann::json containing the histograms data iterated
     * over by itrType. Which by default is Percentiles
     */
    nlohmann::json to_json(
            Iterator::IterMode itrType = Iterator::IterMode::Percentiles);

    /**
     * Dumps the histogram data to json in a string form
     * @return a string of histogram json data
     */
    std::string to_string();

    /**
     * Method to get the total amount of memory being used by this histogram
     * @return number of bytes being used by this histogram
     */
    size_t getMemFootPrint() const;

    /**
     * Method to get the minimum trackable value of this histogram
     * @return minimum trackable value
     */
    uint64_t getMinTrackableValue() const {
        // We subtract one from the lowest value as we have added a one offset
        // as the underlying hdr_histogram cannot store 0 and
        // therefore the value must be greater than or equal to 1.
        return static_cast<uint64_t>(histogram->lowest_trackable_value) - 1;
    }

    /**
     * Method to get the maximum trackable value of this histogram
     * @return maximum trackable value
     */
    uint64_t getMaxTrackableValue() const {
        // We subtract one from the lowest value as we have added a one offset
        // as the underlying hdr_histogram cannot store 0 and
        // therefore the value must be greater than or equal to 1.
        return static_cast<uint64_t>(histogram->highest_trackable_value) - 1;
    }

    /**
     * Method to get the number of significant figures being used to value
     * resolution and resolution.
     * @return an int between 0 and 5 of the number of significant
     * figures bing used
     */
    int getSigFigAccuracy() const {
        return histogram->significant_figures;
    }

private:
    void resize(uint64_t lowestTrackableValue,
                uint64_t highestTrackableValue,
                int significantFigures);

    /**
     * unique_ptr to a hdr_histogram structure
     */
    std::unique_ptr<struct hdr_histogram, HdrDeleter> histogram;
};

/** Histogram to store counts for microsecond intervals
 *  Can hold a range of 0us to 60000000us (60 seconds) with a
 *  precision of 2 significant figures
 */
class HdrMicroSecHistogram : public HdrHistogram {
public:
    HdrMicroSecHistogram() : HdrHistogram(0, 60000000, 2){};
    bool add(std::chrono::microseconds v) {
        return addValue(static_cast<uint64_t>(v.count()));
    }
};