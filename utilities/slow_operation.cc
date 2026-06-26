/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "slow_operation.h"

#include <fmt/format.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/split_string.h>

#include <algorithm>

namespace cb {

TraceEntry::TraceEntry(std::string_view text) {
    auto idx = text.find('=');
    if (idx == std::string_view::npos) {
        throw std::invalid_argument(
                fmt::format("TraceEntry: missing '=' in: {}", text));
    }
    name = std::string(text.substr(0, idx));
    text.remove_prefix(idx + 1);

    idx = text.find(':');
    if (idx == std::string_view::npos) {
        throw std::invalid_argument(
                fmt::format("TraceEntry: missing ':' in: {}", text));
    }
    if (!safe_strtoull(text.substr(0, idx), timestamp)) {
        throw std::invalid_argument(
                fmt::format("TraceEntry: invalid timestamp in: {}", text));
    }
    text.remove_prefix(idx + 1);
    if (!safe_strtoull(text, duration)) {
        throw std::invalid_argument(
                fmt::format("TraceEntry: invalid duration in: {}", text));
    }
}

std::pair<std::string, std::vector<TraceEntry>> parse_slow_operation(
        std::string_view line) {
    constexpr std::string_view slow_op = "Slow operation";
    const auto pos = line.find(slow_op);
    if (pos == std::string_view::npos) {
        return {};
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(line.substr(pos + slow_op.size() + 1));
    } catch (const std::exception&) {
        return {};
    }

    std::string command = json["command"];
    std::string text = json["trace"];
    std::vector<TraceEntry> spans;
    for (const auto& entry : cb::string::split(text)) {
        spans.emplace_back(entry);
    }
    std::ranges::sort(spans, [](const auto& a, const auto& b) {
        return a.timestamp < b.timestamp;
    });

    return {std::move(command), std::move(spans)};
}

} // namespace cb
