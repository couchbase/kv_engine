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
    enum class HistogramType {
        TimeMicroseconds,
        TimeSeconds,
        Size,
        Count,
        Ratio,
    };

    explicit TimingHistogramPrinter(const nlohmann::json& json);
    uint64_t getTotal() const;
    void dumpHistogram(std::string_view name, FILE* out = stdout);
    static void printLegend(FILE* out = stdout);

    /**
     * Deduce the histogram type from the stat key/name.
     *
     * @param name The stat key name
     * @return The deduced HistogramType
     */
    static HistogramType getHistogramType(std::string_view name);

    /**
     * Format an average value into a human-readable string according to
     * histogram type.
     *
     * @param avg The average value
     * @param type The histogram type
     * @return The formatted average string
     */
    static std::string formatAvg(long double avg, HistogramType type);

    /**
     * Set whether to use UTF-8 sparkline fractional block characters (default:
     * true)
     */
    void setUseUtf8(bool enable);

    /**
     * Set a custom maximum bar width (<= 0 to auto-detect from terminal size)
     */
    void setBarWidth(int width);

protected:
    void dump(FILE* out,
              std::string_view unit,
              long double low,
              long double high,
              int64_t count,
              double percentile,
              int availableBarWidth);

    /**
     * Render a bar representing the count.
     *
     * @param count The bucket count
     * @param availableBarWidth Maximum bar width in characters
     * @return Rendered bar string (UTF-8 sparkline or ASCII '#')
     */
    std::string renderBar(int64_t count, int availableBarWidth) const;

    // Calculation for padding around the count in each histogram bucket
    size_t countFieldWidth() const;

    bool useUtf8 = true;
    int customBarWidth = 0;

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
