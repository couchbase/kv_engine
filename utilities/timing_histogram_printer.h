/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json.hpp>

/**
 * Timing histogram printer is a utility class to dump the histogram
 * data (in JSON) provided by the server
 */
class TimingHistogramPrinter {
public:
    explicit TimingHistogramPrinter(const nlohmann::json& json);
    uint64_t getTotal() const;
    void dumpHistogram(std::string_view name, FILE* out = stdout);
    static void printLegend(FILE* out = stdout);

protected:
    void dump(FILE* out,
              std::string_view timeunit,
              long double low,
              long double high,
              int64_t count,
              double percentile);

    // Calculation for padding around the count in each histogram bucket
    int countFieldWidth() const;

    // Calculation for histogram size rendering - how wide should the
    // ASCII bar be for the count of samples.
    int barChartWidth(int64_t count) const;

    /**
     * The highest value of all the samples (used to figure out the width
     * used for each sample in the printout)
     */
    uint64_t maxCount = 0;

    /**
     * Json object to store the data returned by memcached
     */
    nlohmann::json data;

    /**
     * The starting point of the lowest buckets width.
     * E.g. if buckets were [10 - 20][20 - 30] it would be 10.
     * Used to help reduce the amount the amount of json sent to
     * mctimings
     */
    uint64_t bucketsLow = 0;

    /**
     * Total number of counts recorded in the histogram buckets.
     */
    uint64_t total = 0;

    /**
     * Number of samples which overflowed the histograms' buckets.
     * (Added in 7.2.0).
     */
    uint64_t overflowed = 0;

    /**
     * Maximum value the histogram can track. Any values which are greater
     * than this are counted in `overflowed`.
     * (Added in 7.2.0).
     */
    uint64_t maxTrackableValue = 0;
};
