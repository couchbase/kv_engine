/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "timing_histogram_printer.h"
#include "json_utilities.h"
#include <fmt/format.h>

const static std::string_view legend = R"(Histogram Legend:
[1. - 2.]3. (4.)    5.|
    1. All values in this bucket were recorded for a higher value than this.
    2. The maximum value inclusive that could have been recorded in this bucket.
    3. The unit for the values of that (1.) and (2.) are in microseconds, milliseconds or seconds.
    4. Percentile of recorded values to the histogram that has values <= the value at (2.).
    5. The number of recorded values that were in the range (1.) to (2.) inclusive.

)";

TimingHistogramPrinter::TimingHistogramPrinter(const nlohmann::json& json)
    : data(cb::jsonGet<nlohmann::json>(json, "data")),
      bucketsLow(cb::jsonGet<uint64_t>(json, "bucketsLow")),
      total(cb::jsonGet<uint64_t>(json, "total")),
      overflowed(cb::getOptionalJsonObject(json, "overflowed").value_or(0)),
      maxTrackableValue(
              cb::getOptionalJsonObject(json, "max_trackable").value_or(0)) {
}

uint64_t TimingHistogramPrinter::getTotal() const {
    return total + overflowed;
}

void TimingHistogramPrinter::dumpHistogram(std::string_view name, FILE* out) {
    if (data.is_null()) {
        return;
    }

    fmt::print(out, "The following data is collected for \"{}\"\n", name);

    auto dataArray = data.get<std::vector<std::vector<nlohmann::json>>>();
    for (auto item : dataArray) {
        auto count = item[1].get<uint64_t>();
        if (count > maxCount) {
            maxCount = count;
        }
    }
    maxCount = std::max(maxCount, overflowed);

    // If no buckets have no recorded values do not try to render buckets
    if (maxCount > 0) {
        // create double versions of sec, ms, us so we can print them to 2dp
        using namespace std::chrono;
        using doubleMicroseconds = duration<long double, std::micro>;
        using doubleMilliseconds = duration<long double, std::milli>;
        using doubleSeconds = duration<long double>;

        // loop though all the buckets in the json object and print them
        // to std out
        uint64_t lastBuckLow = bucketsLow;
        for (auto bucket : dataArray) {
            // Get the current bucket's highest value it would track counts
            // for
            auto buckHigh = bucket[0].get<int64_t>();
            // Get the counts for this bucket
            auto count = bucket[1].get<int64_t>();
            // Get the percentile of counts that are <= buckHigh
            auto percentile = bucket[2].get<double>();

            // Cast the high bucket width to us, ms and seconds so we
            // can check which units we should be using for this bucket
            auto buckHighUs = doubleMicroseconds(buckHigh);
            auto buckHighMs = duration_cast<doubleMilliseconds>(buckHighUs);
            auto buckHighS = duration_cast<doubleSeconds>(buckHighUs);

            if (buckHighS.count() > 1) {
                auto low =
                        duration_cast<doubleSeconds>(microseconds(lastBuckLow));
                dump(out,
                     "s",
                     low.count(),
                     buckHighS.count(),
                     count,
                     percentile);
            } else if (buckHighMs.count() > 1) {
                auto low = duration_cast<doubleMilliseconds>(
                        doubleMicroseconds(lastBuckLow));
                dump(out,
                     "ms",
                     low.count(),
                     buckHighMs.count(),
                     count,
                     percentile);
            } else {
                dump(out, "us", lastBuckLow, buckHigh, count, percentile);
            }

            // Set the low bucket value to this buckets high width value.
            lastBuckLow = buckHigh;
        }

        // Emit a pseudo-bucket for any overflowed samples which could not
        // be represented, if present.
        if (overflowed) {
            const auto barWidth = barChartWidth(overflowed);
            const auto countWidth = countFieldWidth();
            const doubleSeconds maxTrackableS =
                    doubleMicroseconds(maxTrackableValue);
            fmt::print(out,
                       "[{:6.2f} - {:6.2f}]s (overflowed)\t{}| {}\n",
                       maxTrackableS.count(),
                       std::numeric_limits<double>::infinity(),
                       fmt::format("{0:>{1}}", overflowed, countWidth),
                       std::string(barWidth, '#'));
        }
    }

    fmt::print(out, "Total: {} operations\n", getTotal());
}

void TimingHistogramPrinter::printLegend(FILE* out) {
    fmt::print(out, "{}", legend);
    fflush(out);
}

void TimingHistogramPrinter::dump(FILE* out,
                                  std::string_view timeunit,
                                  long double low,
                                  long double high,
                                  int64_t count,
                                  double percentile) {
    int num = barChartWidth(count);
    int numberOfSpaces = countFieldWidth();

    fmt::print(out,
               "[{:6.2f} - {:6.2f}]{} ({:6.4f}%)\t{}| {}\n",
               low,
               high,
               timeunit,
               percentile,
               fmt::format("{0:>{1}}", count, numberOfSpaces),
               std::string(num, '#'));
}

// Calculation for padding around the count in each histogram bucket
int TimingHistogramPrinter::countFieldWidth() const {
    return fmt::formatted_size("{}", maxCount) + 1;
}

// Calculation for histogram size rendering - how wide should the
// ASCII bar be for the count of samples.
int TimingHistogramPrinter::barChartWidth(int64_t count) const {
    double factionOfHashes =
            maxCount > 0 ? (count / static_cast<double>(maxCount)) : 0.0;
    int num = static_cast<int>(44.0 * factionOfHashes);
    return num;
}
