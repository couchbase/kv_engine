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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace cb {

/** One span entry from a slow-operation trace log. */
struct TraceEntry {
    /**
     * Construct a TraceEntry by parsing a single span token.
     *
     * @param text A span token in the format "name=timestamp:duration",
     *             where timestamp is nanoseconds and duration is
     *             microseconds.
     * @throws std::invalid_argument if the token is missing '=' or ':'.
     * @throws std::invalid_argument if timestamp or duration are not
     *         valid integers.
     */
    explicit TraceEntry(std::string_view text);
    std::string name;
    /** Start time of the span in nanoseconds since steady_clock epoch. */
    uint64_t timestamp;
    /** Duration of the span in microseconds. */
    uint64_t duration;
};

/**
 * Parse a single log line that contains a "Slow operation" entry.
 *
 * Extracts the command name and the list of trace spans from the embedded
 * JSON, sorts the spans by timestamp, and returns them as a pair.
 *
 * @param line The log line to parse.
 * @return A pair of command name and sorted trace spans, or an empty pair
 *         if the line does not contain "Slow operation" or the JSON cannot
 *         be parsed.
 */
std::pair<std::string, std::vector<TraceEntry>> parse_slow_operation(
        std::string_view line);

} // namespace cb
