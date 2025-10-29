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
#include <programs/natsort.h>
#include <iostream>
#include <istream>
#include <vector>

namespace mclogfmt {

bool LineFilter::operator()(std::string_view ts) const {
    cb::natsort::compare_three_way comparator;

    if (!after.empty() && comparator(ts, after) == std::strong_ordering::less) {
        return false;
    }

    if (!before.empty() &&
        comparator(before, ts) != std::strong_ordering::greater) {
        return false;
    }

    return true;
}

void formatAsJson(fmt::memory_buffer& buffer,
                  std::string_view timestamp,
                  std::string_view severity,
                  std::string_view message,
                  std::string_view context) {
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
}

void formatAsJsonLog(fmt::memory_buffer& buffer,
                     std::string_view timestamp,
                     std::string_view severity,
                     std::string_view message,
                     std::string_view context) {
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
}

void formatLine(fmt::memory_buffer& buffer,
                std::string_view timestamp,
                std::string_view severity,
                std::string_view message,
                std::string_view context,
                LogFormat output) {
    switch (output) {
    case LogFormat::Json:
        formatAsJson(buffer, timestamp, severity, message, context);
        break;
    case LogFormat::JsonLog:
        formatAsJsonLog(buffer, timestamp, severity, message, context);
        break;
    case LogFormat::Auto:
        throw std::runtime_error(
                "processLine(): Unexpected output=LogFormat::Auto.");
    }
}

static void convertJsonLine(fmt::memory_buffer& buffer,
                            std::string_view line,
                            LogFormat output,
                            const LineFilter& filter) {
    auto parsedLog = nlohmann::ordered_json::parse(line);
    auto& ts = parsedLog.at("ts").template get_ref<std::string&>();

    if (!filter(ts)) {
        return;
    }

    auto contextString = parsedLog["ctx"].dump();
    formatLine(buffer,
               ts,
               parsedLog.at("lvl").template get_ref<std::string&>(),
               parsedLog.at("msg").template get_ref<std::string&>(),
               contextString,
               output);
}

static void convertPlainTextLine(fmt::memory_buffer& buffer,
                                 std::string_view line,
                                 LogFormat output,
                                 const LineFilter& filter) {
    auto timestampEnd = line.find(' ');
    if (timestampEnd == std::string::npos) {
        return;
    }
    auto timestamp{line.substr(0, timestampEnd)};

    if (!filter(timestamp)) {
        return;
    }

    auto severityEnd = line.find(' ', timestampEnd + 1);
    if (severityEnd == std::string::npos) {
        return;
    }
    auto severity{
            line.substr(timestampEnd + 1, severityEnd - timestampEnd - 1)};

    auto contextBegin = line.find(" {\"");

    try {
        if (contextBegin != std::string::npos && line.back() == '}') {
            auto context = line.substr(contextBegin + 1);
            auto message = line.substr(severityEnd + 1,
                                       contextBegin - severityEnd - 1);

            { auto validJson = nlohmann::json::parse(context); }
            formatLine(buffer, timestamp, severity, message, context, output);
            return;
        }
    } catch (const nlohmann::json::exception&) {
        // Proceed without context.
    }

    auto message = line.substr(severityEnd + 1);
    formatLine(buffer, timestamp, severity, message, {}, output);
}

void convertLine(fmt::memory_buffer& buffer,
                 std::string_view line,
                 LogFormat output,
                 const LineFilter& filter) {
    if (line.empty()) {
        return;
    }

    if (line.at(0) == '{') {
        if (output == LogFormat::Auto) {
            output = LogFormat::JsonLog;
        }
        convertJsonLine(buffer, line, output, filter);
        return;
    }

    if (output == LogFormat::Auto) {
        output = LogFormat::Json;
    }

    if (!isdigit(line.at(0))) {
        // Ignore lines that don't start with a timestamp. These are the results
        // of cbcollect_info.
        return;
    }

    convertPlainTextLine(buffer, line, output, filter);
}

void processFile(std::istream& s, LogFormat output, const LineFilter& filter) {
    for (std::string line; std::getline(s, line);) {
        fmt::memory_buffer buffer;
        convertLine(buffer, line, output, filter);
        if (buffer.size()) {
            fmt::println("{}", std::string_view{buffer.data(), buffer.size()});
        }
    }
}

} // namespace mclogfmt
