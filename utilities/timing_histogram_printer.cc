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
#include <platform/byte_literals.h>
#include <platform/terminal_size.h>
#include <array>
#include <cmath>
#include <limits>

static constexpr std::string_view legend = R"(Histogram Legend:
[1. - 2.]3. (4.)    5.|
    1. All values in this bucket were recorded for a higher value than this.
    2. The maximum value inclusive that could have been recorded in this bucket.
    3. The unit for the values of that (1.) and (2.) are in microseconds,
       milliseconds, seconds, bytes, or none for counts/ratios.
    4. Percentile of recorded values to the histogram that has values <= the
       value at (2.).
    5. The number of recorded values that were in the range (1.) to (2.)
       inclusive.

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

void TimingHistogramPrinter::setUseUtf8(bool enable) {
    useUtf8 = enable;
}

void TimingHistogramPrinter::setBarWidth(int width) {
    customBarWidth = width;
}

TimingHistogramPrinter::HistogramType TimingHistogramPrinter::getHistogramType(
        std::string_view name) {
    const auto colon = name.rfind(':');
    const auto baseName =
            (colon != std::string_view::npos) ? name.substr(colon + 1) : name;

    if (baseName.ends_with("Size") || baseName.ends_with("Seek") ||
        baseName == "item_alloc_sizes") {
        return HistogramType::Size;
    }
    if (baseName.ends_with("Count") || baseName == "bg_batch_size" ||
        baseName == "ep_active_or_pending_eviction_values_evicted" ||
        baseName == "ep_replica_eviction_values_evicted" ||
        baseName == "ep_active_or_pending_eviction_values_snapshot" ||
        baseName == "ep_replica_eviction_values_snapshot") {
        return HistogramType::Count;
    }
    if (baseName.ends_with("Ratio")) {
        return HistogramType::Ratio;
    }
    if (baseName == "paged_out_time") {
        return HistogramType::TimeSeconds;
    }
    return HistogramType::TimeMicroseconds;
}

std::string TimingHistogramPrinter::formatAvg(long double avg,
                                              HistogramType type) {
    switch (type) {
    case HistogramType::Count:
        return fmt::format("{:.2f}", avg);
    case HistogramType::Ratio:
        return fmt::format("{:.2f}", avg / 10.0L);
    case HistogramType::Size: {
        constexpr auto KiB = 1_KiB;
        constexpr auto MiB = 1_MiB;
        constexpr auto GiB = 1_GiB;
        constexpr auto TiB = 1_TiB;

        if (avg >= static_cast<long double>(TiB)) {
            return fmt::format("{:.2f}TiB",
                               avg / static_cast<long double>(TiB));
        }
        if (avg >= static_cast<long double>(GiB)) {
            return fmt::format("{:.2f}GiB",
                               avg / static_cast<long double>(GiB));
        }
        if (avg >= static_cast<long double>(MiB)) {
            return fmt::format("{:.2f}MiB",
                               avg / static_cast<long double>(MiB));
        }
        if (avg >= static_cast<long double>(KiB)) {
            return fmt::format("{:.2f}KiB",
                               avg / static_cast<long double>(KiB));
        }
        return fmt::format("{:.2f}B", avg);
    }
    case HistogramType::TimeSeconds: {
        if (avg >= 60.0L) {
            auto mins = static_cast<int64_t>(avg) / 60;
            auto secs = static_cast<int64_t>(avg) % 60;
            return fmt::format("{}m:{:02d}s", mins, secs);
        }
        if (avg >= 1.0L) {
            return fmt::format("{:.2f}s", avg);
        }
        if (avg >= 0.001L) {
            return fmt::format("{:.2f}ms", avg * 1000.0L);
        }
        return fmt::format("{:.2f}us", avg * 1'000'000.0L);
    }
    case HistogramType::TimeMicroseconds: {
        const long double sec = avg / 1'000'000.0L;
        if (sec >= 60.0L) {
            auto mins = static_cast<int64_t>(sec) / 60;
            auto secs = static_cast<int64_t>(sec) % 60;
            return fmt::format("{}m:{:02d}s", mins, secs);
        }
        if (avg >= 1'000'000.0L) {
            return fmt::format("{:.2f}s", sec);
        }
        if (avg >= 1'000.0L) {
            return fmt::format("{:.2f}ms", avg / 1000.0L);
        }
        return fmt::format("{:.2f}us", avg);
    }
    }
    return fmt::format("{:.2f}", avg);
}

std::string TimingHistogramPrinter::renderBar(int64_t count,
                                              int availableBarWidth) const {
    if (maxCount == 0 || count <= 0 || availableBarWidth <= 0) {
        return {};
    }

    const double fraction =
            static_cast<double>(count) / static_cast<double>(maxCount);
    const double barLen = fraction * static_cast<double>(availableBarWidth);

    if (!useUtf8) {
        // A braced init list is ambiguous here between the (count, char)
        // and initializer_list<char> constructors, and narrows to the
        // latter.
        // NOLINTNEXTLINE(modernize-return-braced-init-list)
        return std::string(static_cast<size_t>(barLen), '#');
    }

    static constexpr std::array<std::string_view, 8> sparkChars = {
            "", "▏", "▎", "▍", "▌", "▋", "▊", "▉"};

    const auto wholeBlocks = static_cast<size_t>(barLen);
    const auto eighths = std::min<size_t>(
            static_cast<size_t>((barLen - wholeBlocks) * 8.0), 7);

    std::string bar;
    bar.reserve((wholeBlocks * 3) + 4);
    for (size_t i = 0; i < wholeBlocks; ++i) {
        bar += "█";
    }
    if (eighths > 0) {
        bar += sparkChars[eighths];
    }
    return bar;
}

void TimingHistogramPrinter::dumpHistogram(std::string_view name, FILE* out) {
    if (data.is_null()) {
        return;
    }

    fmt::print(out, "The following data is collected for \"{}\"\n", name);

    auto dataArray = data.get<std::vector<std::vector<nlohmann::json>>>();
    for (const auto& item : dataArray) {
        auto count = item[1].get<uint64_t>();
        if (count > maxCount) {
            maxCount = count;
        }
    }
    maxCount = std::max(maxCount, overflowed);

    // If no buckets have no recorded values do not try to render buckets
    if (maxCount > 0) {
        const auto type = getHistogramType(name);

        int availableBarWidth = customBarWidth;
        if (availableBarWidth <= 0) {
            availableBarWidth = 44;
            try {
                const auto [termWidth, _] = cb::terminal::getTerminalSize();
                // getTerminalSize() returns SIZE_MAX when stdout isn't a
                // tty (e.g. piped or redirected output) - keep the default
                // width in that case rather than treating it as an
                // (effectively infinite) terminal width.
                if (termWidth != std::numeric_limits<size_t>::max()) {
                    const size_t prefixWidth = 34 + countFieldWidth() + 2;
                    if (termWidth > prefixWidth + 10) {
                        availableBarWidth =
                                static_cast<int>(termWidth - prefixWidth);
                    }
                }
            } catch (...) {
            }
        }

        using namespace std::chrono;
        using doubleMicroseconds = duration<long double, std::micro>;
        using doubleMilliseconds = duration<long double, std::milli>;
        using doubleSeconds = duration<long double>;

        uint64_t lastBuckLow = bucketsLow;
        uint64_t totalSeen = 0;
        bool nonZeroSeen = false;
        long double weightedSum = 0.0L;

        for (const auto& bucket : dataArray) {
            auto buckHigh = bucket[0].get<int64_t>();
            auto count = bucket[1].get<int64_t>();
            auto percentile = bucket[2].get<double>();

            // Skip leading zero buckets
            if (count == 0 && !nonZeroSeen) {
                lastBuckLow = buckHigh;
                continue;
            }

            // Skip trailing zero buckets (when all counts have already been
            // processed)
            if (totalSeen == total && count == 0) {
                lastBuckLow = buckHigh;
                continue;
            }

            nonZeroSeen = true;
            totalSeen += count;

            const auto midpoint = (static_cast<long double>(lastBuckLow) +
                                   static_cast<long double>(buckHigh)) /
                                  2.0L;
            weightedSum += midpoint * static_cast<long double>(count);

            switch (type) {
            case HistogramType::Count:
                dump(out,
                     "",
                     static_cast<long double>(lastBuckLow),
                     static_cast<long double>(buckHigh),
                     count,
                     percentile,
                     availableBarWidth);
                break;
            case HistogramType::Ratio:
                dump(out,
                     "",
                     static_cast<long double>(lastBuckLow) / 10.0L,
                     static_cast<long double>(buckHigh) / 10.0L,
                     count,
                     percentile,
                     availableBarWidth);
                break;
            case HistogramType::Size: {
                constexpr auto KiB = static_cast<int64_t>(1_KiB);
                constexpr auto MiB = static_cast<int64_t>(1_MiB);
                constexpr auto GiB = static_cast<int64_t>(1_GiB);
                constexpr auto TiB = static_cast<int64_t>(1_TiB);

                if (buckHigh >= TiB) {
                    constexpr auto div = static_cast<long double>(TiB);
                    dump(out,
                         "TiB",
                         lastBuckLow / div,
                         buckHigh / div,
                         count,
                         percentile,
                         availableBarWidth);
                } else if (buckHigh >= GiB) {
                    constexpr auto div = static_cast<long double>(GiB);
                    dump(out,
                         "GiB",
                         lastBuckLow / div,
                         buckHigh / div,
                         count,
                         percentile,
                         availableBarWidth);
                } else if (buckHigh >= MiB) {
                    constexpr auto div = static_cast<long double>(MiB);
                    dump(out,
                         "MiB",
                         lastBuckLow / div,
                         buckHigh / div,
                         count,
                         percentile,
                         availableBarWidth);
                } else if (buckHigh >= KiB) {
                    constexpr auto div = static_cast<long double>(KiB);
                    dump(out,
                         "KiB",
                         lastBuckLow / div,
                         buckHigh / div,
                         count,
                         percentile,
                         availableBarWidth);
                } else {
                    dump(out,
                         "B",
                         lastBuckLow,
                         buckHigh,
                         count,
                         percentile,
                         availableBarWidth);
                }
                break;
            }
            case HistogramType::TimeSeconds: {
                auto buckHighUs = doubleMicroseconds(buckHigh * 1'000'000.0L);
                auto buckHighMs = duration_cast<doubleMilliseconds>(buckHighUs);
                auto buckHighS = duration_cast<doubleSeconds>(buckHighUs);

                if (buckHighS.count() > 1) {
                    dump(out,
                         "s",
                         static_cast<long double>(lastBuckLow),
                         static_cast<long double>(buckHigh),
                         count,
                         percentile,
                         availableBarWidth);
                } else if (buckHighMs.count() > 1) {
                    auto low = duration_cast<doubleMilliseconds>(
                            doubleSeconds(lastBuckLow));
                    dump(out,
                         "ms",
                         low.count(),
                         buckHighMs.count(),
                         count,
                         percentile,
                         availableBarWidth);
                } else {
                    auto low = duration_cast<doubleMicroseconds>(
                            doubleSeconds(lastBuckLow));
                    dump(out,
                         "us",
                         low.count(),
                         buckHighUs.count(),
                         count,
                         percentile,
                         availableBarWidth);
                }
                break;
            }
            case HistogramType::TimeMicroseconds: {
                auto buckHighUs = doubleMicroseconds(buckHigh);
                auto buckHighMs = duration_cast<doubleMilliseconds>(buckHighUs);
                auto buckHighS = duration_cast<doubleSeconds>(buckHighUs);

                if (buckHighS.count() > 1) {
                    auto low = duration_cast<doubleSeconds>(
                            microseconds(lastBuckLow));
                    dump(out,
                         "s",
                         low.count(),
                         buckHighS.count(),
                         count,
                         percentile,
                         availableBarWidth);
                } else if (buckHighMs.count() > 1) {
                    auto low = duration_cast<doubleMilliseconds>(
                            doubleMicroseconds(lastBuckLow));
                    dump(out,
                         "ms",
                         low.count(),
                         buckHighMs.count(),
                         count,
                         percentile,
                         availableBarWidth);
                } else {
                    dump(out,
                         "us",
                         lastBuckLow,
                         buckHigh,
                         count,
                         percentile,
                         availableBarWidth);
                }
                break;
            }
            }

            lastBuckLow = buckHigh;
        }

        // Emit a pseudo-bucket for any overflowed samples which could not
        // be represented, if present.
        if (overflowed) {
            const auto bar = renderBar(overflowed, availableBarWidth);
            const auto countWidth = countFieldWidth();

            switch (type) {
            case HistogramType::Count:
                fmt::print(out,
                           "[{:6.2f} - {:6.2f}] (overflowed)\t{}| {}\n",
                           static_cast<long double>(maxTrackableValue),
                           std::numeric_limits<double>::infinity(),
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           bar);
                break;
            case HistogramType::Ratio:
                fmt::print(out,
                           "[{:6.2f} - {:6.2f}] (overflowed)\t{}| {}\n",
                           static_cast<long double>(maxTrackableValue) / 10.0L,
                           std::numeric_limits<double>::infinity(),
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           bar);
                break;
            case HistogramType::Size: {
                constexpr auto KiB = 1_KiB;
                constexpr auto MiB = 1_MiB;
                constexpr auto GiB = 1_GiB;
                constexpr auto TiB = 1_TiB;

                std::string_view unit = "B";
                long double val = maxTrackableValue;
                if (maxTrackableValue >= TiB) {
                    unit = "TiB";
                    val /= static_cast<long double>(TiB);
                } else if (maxTrackableValue >= GiB) {
                    unit = "GiB";
                    val /= static_cast<long double>(GiB);
                } else if (maxTrackableValue >= MiB) {
                    unit = "MiB";
                    val /= static_cast<long double>(MiB);
                } else if (maxTrackableValue >= KiB) {
                    unit = "KiB";
                    val /= static_cast<long double>(KiB);
                }
                fmt::print(out,
                           "[{:6.2f} - {:6.2f}]{} (overflowed)\t{}| {}\n",
                           val,
                           std::numeric_limits<double>::infinity(),
                           unit,
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           bar);
                break;
            }
            case HistogramType::TimeSeconds:
                fmt::print(out,
                           "[{:6.2f} - {:6.2f}]s (overflowed)\t{}| {}\n",
                           static_cast<long double>(maxTrackableValue),
                           std::numeric_limits<double>::infinity(),
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           bar);
                break;
            case HistogramType::TimeMicroseconds: {
                const doubleSeconds maxTrackableS =
                        doubleMicroseconds(maxTrackableValue);
                fmt::print(out,
                           "[{:6.2f} - {:6.2f}]s (overflowed)\t{}| {}\n",
                           maxTrackableS.count(),
                           std::numeric_limits<double>::infinity(),
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           bar);
                break;
            }
            }
        }

        if (total > 0) {
            const auto avgVal = weightedSum / static_cast<long double>(total);
            fmt::print(out, "Avg: {}\n", formatAvg(avgVal, type));
        }
    }

    fmt::print(out, "Total: {} operations\n", getTotal());
}

void TimingHistogramPrinter::printLegend(FILE* out) {
    fmt::print(out, "{}", legend);
    fflush(out);
}

void TimingHistogramPrinter::dump(FILE* out,
                                  std::string_view unit,
                                  long double low,
                                  long double high,
                                  int64_t count,
                                  double percentile,
                                  int availableBarWidth) {
    const auto bar = renderBar(count, availableBarWidth);
    const auto numberOfSpaces = countFieldWidth();

    fmt::print(out,
               "[{:6.2f} - {:6.2f}]{} ({:6.4f}%)\t{}| {}\n",
               low,
               high,
               unit,
               percentile,
               fmt::format("{0:>{1}}", count, numberOfSpaces),
               bar);
}

// Calculation for padding around the count in each histogram bucket
size_t TimingHistogramPrinter::countFieldWidth() const {
    return fmt::formatted_size("{}", maxCount) + 1;
}
