/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mclogfmt.h"

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/command_line_options_parser.h>
#include <platform/dirutils.h>
#include <platform/terminal_color.h>
#include <iostream>
#include <istream>
#include <vector>

namespace mclogfmt {

// Ingore the following lines.
// Those match the output appended by cbcollect_info.
static const std::vector<std::string_view> IGNORED_LINES = {
        R"(===============================================================================\n)",
        "Memcached logs\n",
};

// Ignore lines on which the first token matches.
// Those match the output appended by cbcollect_info.
static const std::vector<std::string_view> IGNORED_WORDS = {
        "sh",
        "cd",
};

void formatLine(fmt::memory_buffer& buffer,
                std::string_view timestamp,
                std::string_view severity,
                std::string_view message,
                std::string_view context,
                LogFormat output) {
    switch (output) {
    case LogFormat::Json:
        if (context.empty()) {
            fmt::format_to(std::back_inserter(buffer),
                           R"({{"ts":"{}","lvl":"{}","msg":{}}})",
                           timestamp,
                           severity,
                           nlohmann::json(message).dump());
        } else {
            fmt::format_to(std::back_inserter(buffer),
                           R"({{"ts":"{}","lvl":"{}","msg":{},"ctx":{}}})",
                           timestamp,
                           severity,
                           nlohmann::json(message).dump(),
                           context);
        }
        break;
    case LogFormat::JsonLog:
        if (context.empty()) {
            fmt::format_to(std::back_inserter(buffer),
                           "{} {} {}",
                           timestamp,
                           severity,
                           message);
        } else {
            fmt::format_to(std::back_inserter(buffer),
                           "{} {} {} {}",
                           timestamp,
                           severity,
                           message,
                           context);
        }
        break;
    case LogFormat::Auto:
        throw std::runtime_error(
                "processLine(): Unexpected output=LogFormat::Auto.");
    }
}

void convertLine(fmt::memory_buffer& buffer,
                 std::string_view line,
                 LogFormat output) {
    if (std::ranges::find(IGNORED_LINES, line) != IGNORED_LINES.end()) {
        return;
    }

    std::string_view lineView = line;
    if (lineView.at(0) == '{') {
        auto parsedLog = nlohmann::ordered_json::parse(lineView);
        auto contextString = parsedLog["ctx"].dump();

        if (output == LogFormat::Auto) {
            output = LogFormat::JsonLog;
        }
        formatLine(buffer,
                   parsedLog.at("ts").template get_ref<std::string&>(),
                   parsedLog.at("lvl").template get_ref<std::string&>(),
                   parsedLog.at("msg").template get_ref<std::string&>(),
                   contextString,
                   output);
        return;
    }

    if (output == LogFormat::Auto) {
        output = LogFormat::Json;
    }

    auto timestampEnd = line.find(' ');
    if (timestampEnd == std::string::npos) {
        return;
    }
    auto timestamp{lineView.substr(0, timestampEnd)};
    if (std::ranges::find(IGNORED_WORDS, timestamp) != IGNORED_WORDS.end()) {
        return;
    }

    auto severityEnd = line.find(' ', timestampEnd + 1);
    if (severityEnd == std::string::npos) {
        return;
    }
    auto severity{
            lineView.substr(timestampEnd + 1, severityEnd - timestampEnd - 1)};

    auto contextBegin = line.find(" {\"");

    try {
        if (contextBegin != std::string::npos && line.back() == '}') {
            auto context = lineView.substr(contextBegin + 1);
            auto message = lineView.substr(severityEnd + 1,
                                           contextBegin - severityEnd - 1);

            { auto validJson = nlohmann::json::parse(context); }
            formatLine(buffer, timestamp, severity, message, context, output);
            return;
        }
    } catch (const nlohmann::json::exception& e) {
        // Proceed without context.
    }

    auto message = lineView.substr(severityEnd + 1);
    formatLine(buffer, timestamp, severity, message, {}, output);
}

void processFile(std::istream& s, LogFormat output) {
    for (std::string line; std::getline(s, line);) {
        fmt::memory_buffer buffer;
        convertLine(buffer, line, output);
        if (buffer.size()) {
            fmt::println("{}", std::string_view{buffer.data(), buffer.size()});
        }
    }
}

} // namespace mclogfmt
