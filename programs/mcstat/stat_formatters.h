/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json.hpp>
#include <iosfwd>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/**
 * Port of cbstats.py's time_label(): render a microsecond value the way
 * cbstats.py does (e.g. "4us", "838ms", "10m:05s").
 */
std::string time_label(long double s);

/**
 * Format a key-value dictionary the way cbstats.py's stats_formatter() does:
 * sorted by natural sort order, with the key column padded to fit the longest
 * key (key + ":").
 *
 * @param stats Key-value pairs to format
 * @param prefix Prefix string for each line (e.g. " ", "     ", "        ")
 * @param out Output stream
 */
void formatStatsDict(std::vector<std::pair<std::string, std::string>> stats,
                     std::string_view prefix,
                     std::ostream& out);

/**
 * Compute aggregate hash stats (min, max, avg depth, counts, histograms)
 * from raw 'hash' stat key-values.
 *
 * Each vb_<id>:histo entry is a JSON-encoded histogram (see
 * TimingHistogramPrinter); its buckets are decoded and summed across all
 * vbuckets to produce summary:histo_<low>,<high> counts and an overall
 * summary:histo_mean, mirroring the "summary:*" lines cbstats.py used to
 * derive from the (now vbucket-only) per-bucket stats it received.
 *
 * @param rawStats Key-value pairs returned from 'hash'
 * @param withDetail If true, retains per-vbucket vb_* entries
 * @return Aggregated key-value pairs sorted in natural order
 */
std::vector<std::pair<std::string, std::string>> computeHashStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withDetail);

/**
 * Pretty-print hash table statistics.
 *
 * @param rawStats Key-value pairs returned from 'hash'
 * @param withDetail If true, includes per-vbucket details; if false, only
 * aggregates
 * @param out Output stream
 */
void printHashStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withDetail,
        std::ostream& out);

/**
 * Pretty-print dispatcher statistics.
 *
 * @param rawStats Key-value pairs returned from 'dispatcher'
 * @param withLogs If true, includes recent and slow job logs
 * @param out Output stream
 */
void printDispatcherStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        bool withLogs,
        std::ostream& out);

/**
 * Resolve response status codes to human-readable names using errorMap.
 *
 * @param rawStats Key-value pairs returned from 'responses'
 * @param errorMap The "errors" object from GetErrorMap
 * @param showAll If true, shows error codes with 0 counts
 * @return Resolved key-value pairs (e.g. {"SUCCESS", "7515"}, {"KEY_ENOENT",
 * "6"})
 */
std::vector<std::pair<std::string, std::string>> computeResponsesStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        const nlohmann::json& errorMap,
        bool showAll);

/**
 * Pretty-print response status code counts.
 *
 * @param rawStats Key-value pairs returned from 'responses'
 * @param errorMap The "errors" object from the server's error map (as
 * returned by the GetErrorMap command), used to resolve status codes to
 * their human-readable names. Keyed by lowercase hex status code (no "0x"
 * prefix), each entry an object containing a "name" field.
 * @param showAll If true, shows error codes with 0 counts as well
 * @param out Output stream
 */
void printResponsesStats(
        const std::vector<std::pair<std::string, std::string>>& rawStats,
        const nlohmann::json& errorMap,
        bool showAll,
        std::ostream& out);
