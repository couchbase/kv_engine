/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stat_formatters.h"
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <nlohmann/json.hpp>
#include <platform/split_string.h>
#include <programs/natsort.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <iostream>
#include <map>

std::string time_label(long double s) {
    constexpr long double bigValue = 1152921504606846976.0L; // 2^60
    if (s > bigValue) {
        return "inf";
    }
    if (s < -bigValue) {
        return "-inf";
    }
    const bool negative = s < 0;
    s = std::fabs(s);
    if (s <= 1) {
        return fmt::format("{:4d}us", static_cast<int64_t>(s));
    }

    struct Entry {
        std::string_view label;
        long double threshold;
    };
    static constexpr std::array<Entry, 4> sizeMap = {{{"m", 600000000.0L},
                                                      {"s", 10000000.0L},
                                                      {"ms", 10000.0L},
                                                      {"us", 1.0L}}};

    std::string_view lbl = "us";
    long double factor = 1.0L;
    for (const auto& e : sizeMap) {
        if (e.threshold >= s) {
            continue;
        }
        lbl = e.label;
        factor = e.threshold;
        break;
    }

    std::string result;
    if (lbl == "m") {
        factor /= 10.0L;
        auto mins = static_cast<int64_t>(s / factor);
        auto secs =
                static_cast<int64_t>(std::fmod(s, factor) / (factor / 60.0L));
        result = fmt::format("{}m:{:02d}s", mins, secs);
    } else if (lbl == "us") {
        result = fmt::format("{:4d}us", static_cast<int64_t>(s));
    } else {
        factor /= 10.0L;
        result = fmt::format("{:4d}{}", static_cast<int64_t>(s / factor), lbl);
    }
    return (negative ? "-" : "") + result;
}

void formatStatsDict(std::vector<std::pair<std::string, std::string>> stats,
                     std::string_view prefix,
                     std::ostream& out) {
    if (stats.empty()) {
        return;
    }
    std::ranges::sort(stats, [](const auto& lhs, const auto& rhs) {
        return cb::natsort::less{}(lhs.first, rhs.first);
    });
    size_t longest = 0;
    for (const auto& [k, v] : stats) {
        longest = std::max(longest, k.size() + 2);
    }
    for (const auto& [k, v] : stats) {
        fmt::print(
                out, "{}{:<{}}{}\n", prefix, fmt::format("{}:", k), longest, v);
    }
}

std::vector<std::pair<std::string, std::string>> computeHashStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withDetail) {
    std::vector<uint64_t> mins;
    std::vector<uint64_t> maxes;
    std::vector<uint64_t> counts;
    std::map<std::string, uint64_t> summaryHisto;
    long double weightedSum = 0.0L;
    uint64_t weightedCount = 0;
    std::map<std::string, std::string> statsMap;

    for (const auto& [key, value] : rawStats) {
        statsMap[key] = value;
        if (key.find("max_dep") != std::string::npos) {
            try {
                maxes.push_back(std::stoull(value));
            } catch (...) {
            }
        }
        if (key.find("min_dep") != std::string::npos) {
            try {
                mins.push_back(std::stoull(value));
            } catch (...) {
            }
        }
        if (key.find(":counted") != std::string::npos) {
            try {
                counts.push_back(std::stoull(value));
            } catch (...) {
            }
        }
        if (key.ends_with(":histo")) {
            // The per-vbucket histogram arrives as a single JSON object
            // (see TimingHistogramPrinter), not as flat per-bucket stats -
            // decode it and fold its buckets into a cross-vbucket summary,
            // keyed the same way as each bucket's [lowerBound, upperBound).
            try {
                const auto json = nlohmann::json::parse(value);
                uint64_t lastLow = json.at("bucketsLow").get<uint64_t>();
                for (const auto& bucket : json.at("data")) {
                    const auto high = bucket.at(0).get<uint64_t>();
                    const auto count = bucket.at(1).get<uint64_t>();
                    summaryHisto[fmt::format(
                            "summary:histo_{},{}", lastLow, high)] += count;
                    const auto midpoint = (static_cast<long double>(lastLow) +
                                           static_cast<long double>(high)) /
                                          2.0L;
                    weightedSum += midpoint * static_cast<long double>(count);
                    weightedCount += count;
                    lastLow = high;
                }
            } catch (const std::exception&) {
            }
        }
    }

    auto calcAvg = [](const std::vector<uint64_t>& vec) -> std::string {
        if (vec.empty()) {
            return "0";
        }
        long double sum = 0.0L;
        for (auto val : vec) {
            sum += static_cast<long double>(val);
        }
        long double avgVal = sum / static_cast<long double>(vec.size());
        return fmt::format("{}", static_cast<double>(avgVal));
    };

    if (!mins.empty()) {
        statsMap["avg_min"] = calcAvg(mins);
        statsMap["largest_min"] =
                std::to_string(*std::ranges::max_element(mins));
    }
    if (!maxes.empty()) {
        statsMap["avg_max"] = calcAvg(maxes);
        statsMap["largest_max"] =
                std::to_string(*std::ranges::max_element(maxes));
    }
    if (!counts.empty()) {
        statsMap["avg_count"] = calcAvg(counts);
        statsMap["min_count"] =
                std::to_string(*std::ranges::min_element(counts));
        statsMap["max_count"] =
                std::to_string(*std::ranges::max_element(counts));
        uint64_t total = 0;
        for (auto count : counts) {
            total += count;
        }
        statsMap["total_counts"] = std::to_string(total);
    }
    for (const auto& [key, value] : summaryHisto) {
        statsMap[key] = std::to_string(value);
    }
    if (weightedCount > 0) {
        statsMap["summary:histo_mean"] = fmt::format(
                "{}",
                static_cast<double>(weightedSum /
                                    static_cast<long double>(weightedCount)));
    }

    std::vector<std::pair<std::string, std::string>> toDisplay;
    for (const auto& [key, value] : statsMap) {
        if (withDetail || key.find("vb_") == std::string::npos) {
            toDisplay.emplace_back(key, value);
        }
    }

    std::ranges::sort(toDisplay, [](const auto& lhs, const auto& rhs) {
        return cb::natsort::less{}(lhs.first, rhs.first);
    });

    return toDisplay;
}

void printHashStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withDetail,
        std::ostream& out) {
    auto stats = computeHashStats(rawStats, withDetail);
    formatStatsDict(std::move(stats), " ", out);
}

void printDispatcherStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withLogs,
        std::ostream& out) {
    std::map<std::string, std::vector<std::pair<std::string, std::string>>>
            dispatchers;
    std::map<std::string,
             std::map<int, std::vector<std::pair<std::string, std::string>>>>
            recentLogs;
    std::map<std::string,
             std::map<int, std::vector<std::pair<std::string, std::string>>>>
            slowLogs;

    for (const auto& [key, val] : rawStats) {
        auto parts = cb::string::split(key, ':');
        if (parts.empty()) {
            continue;
        }
        std::string dispatcher = std::string(parts[0]);
        std::string value = val;
        if (parts.back() == "runtime") {
            try {
                value = time_label(std::stold(val));
            } catch (...) {
            }
        }

        if (parts.size() >= 4 && (parts[1] == "log" || parts[1] == "slow")) {
            int offset = 0;
            try {
                offset = std::stoi(std::string(parts[2]));
            } catch (...) {
            }
            std::string field = std::string(parts[3]);
            if (parts[1] == "log") {
                recentLogs[dispatcher][offset].emplace_back(field, value);
            } else {
                slowLogs[dispatcher][offset].emplace_back(field, value);
            }
        } else if (parts.size() >= 2) {
            std::string field = std::string(parts[1]);
            dispatchers[dispatcher].emplace_back(field, value);
        }
    }

    for (const auto& [dispatcher, fields] : dispatchers) {
        out << " " << dispatcher << "\n";
        formatStatsDict(fields, "     ", out);
        if (withLogs) {
            auto itSlow = slowLogs.find(dispatcher);
            if (itSlow != slowLogs.end() && !itSlow->second.empty()) {
                out << "     Slow jobs:\n";
                for (const auto& [offset, logFields] : itSlow->second) {
                    formatStatsDict(logFields, "        ", out);
                    out << "        ---------\n";
                }
            }
            auto itRecent = recentLogs.find(dispatcher);
            if (itRecent != recentLogs.end() && !itRecent->second.empty()) {
                out << "     Recent jobs:\n";
                for (const auto& [offset, logFields] : itRecent->second) {
                    formatStatsDict(logFields, "        ", out);
                    out << "        ---------\n";
                }
            }
        }
    }
}

std::vector<std::pair<std::string, std::string>> computeResponsesStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        const nlohmann::json& errorMap,
        bool showAll) {
    std::vector<std::pair<std::string, std::string>> entries;
    for (const auto& [key, val] : rawStats) {
        if (key == "responses" || key.find("responses") != std::string::npos) {
            try {
                auto parsed = nlohmann::json::parse(val);
                if (parsed.is_object()) {
                    for (const auto& [hexKey, countVal] : parsed.items()) {
                        uint64_t count = countVal.is_number()
                                                 ? countVal.get<uint64_t>()
                                                 : 0;
                        if (count > 0 || showAll) {
                            auto errorIt = errorMap.find(hexKey);
                            std::string name =
                                    errorIt != errorMap.end()
                                            ? errorIt->value("name", hexKey)
                                            : hexKey;
                            entries.emplace_back(std::move(name),
                                                 std::to_string(count));
                        }
                    }
                }
            } catch (const std::exception&) {
            }
        }
    }
    std::ranges::sort(entries, [](const auto& lhs, const auto& rhs) {
        return cb::natsort::less{}(lhs.first, rhs.first);
    });
    return entries;
}

void printResponsesStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        const nlohmann::json& errorMap,
        bool showAll,
        std::ostream& out) {
    auto stats = computeResponsesStats(rawStats, errorMap, showAll);
    formatStatsDict(std::move(stats), " ", out);
}
