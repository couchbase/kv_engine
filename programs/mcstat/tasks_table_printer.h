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
 * Format a time duration in nanoseconds to a ps-style timestamp string.
 *
 * @param nanoseconds Time duration in nanoseconds
 * @return String formatted as [-]M:SS.cs or [-]H:MM:SS.cs
 */
std::string formatPsTime(int64_t nanoseconds);

/**
 * Pretty-print tasks stat results as a top-style summary table.
 *
 * @param stats Stat key-value pairs returned by the server (e.g.
 * ep_tasks:cur_time, ep_tasks:tasks)
 * @param sortBy Optional column name or index to sort by
 * @param out Output stream to print to
 */
void printTasksTable(
        const std::vector<std::pair<std::string, std::string>>& stats,
        std::string_view sortBy,
        std::ostream& out);
