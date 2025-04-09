/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <fmt/format.h>

namespace mclogfmt {

enum class LogFormat {
    /// Deduce the log format and convert between (Json <-> JsonLog).
    Auto,
    /// Format: <json>
    Json,
    /// Format: <TS> <LEVEL> message <json>
    JsonLog,
};

/**
 * Construct an output line with the given format.
 * @param timestamp the timestamp
 * @param severity the severity
 * @param message the message
 * @param context the JSON context
 * @param output the output format
 */
void formatLine(fmt::memory_buffer& buffer,
                std::string_view timestamp,
                std::string_view severity,
                std::string_view message,
                std::string_view context,
                LogFormat output);

struct LineFilter {
    /// Only timestamps ordered after this one will be allowed.
    std::string after;
    /// Only timestamps ordered before this one will be allowed.
    std::string before;

    /// Returns true if the input matches the filter.
    bool operator()(std::string_view ts) const;
};

/**
 * Changes the formatting of the line from the input one, to the one specified.
 * @param buffer the output memory buffer
 * @param line the input line
 * @param output the output format
 */
void convertLine(fmt::memory_buffer& buffer,
                 std::string_view line,
                 LogFormat output,
                 const LineFilter& filter);

void processFile(std::istream& s, LogFormat output, const LineFilter& filter);

} // namespace mclogfmt
