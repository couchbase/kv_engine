/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/command_line_options_parser.h>
#include <platform/dirutils.h>
#include <platform/terminal_color.h>
#include <utilities/terminate_handler.h>
#include <fstream>
#include <iostream>
#include <istream>
#include <vector>

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

enum class LogFormat {
    /// Deduce the log format and convert between (Json <-> JsonLog).
    Auto,
    /// Format: <json>
    Json,
    /// Format: <TS> <LEVEL> message <json>
    JsonLog,
};

LogFormat parseLogFormat(std::string_view logFormat);

void processFile(std::istream& s, LogFormat output);

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();

    using cb::getopt::Argument;
    cb::getopt::CommandLineOptionsParser parser;

    std::optional<std::string> input;
    LogFormat outputMode{LogFormat::Auto};
    parser.addOption({
            [&input](auto value) { input = std::string{value}; },
            'i',
            "input",
            Argument::Required,
            "filename",
            "Specify an input filename. Reads from stdin if not specified.",
    });
    parser.addOption(
            {[&outputMode](auto value) { outputMode = parseLogFormat(value); },
             'o',
             "output",
             Argument::Required,
             "mode",
             "Specify an output mode. Options: auto (default), json, json-log "
             "(>=8.0)"});
    parser.addOption({[&parser](auto) {
                          std::cerr << "mclogfmt [options]" << std::endl;
                          parser.usage(std::cerr);
                          std::exit(EXIT_SUCCESS);
                      },
                      "help",
                      "Print this help"});

    const auto arguments = parser.parse(argc, argv, [&parser]() {
        std::cerr << std::endl;
        parser.usage(std::cerr);
        std::exit(EXIT_FAILURE);
    });

    if (!input.has_value()) {
        processFile(std::cin, outputMode);
    } else {
        std::ifstream s(*input);
        processFile(s, outputMode);
    }

    return EXIT_SUCCESS;
}

LogFormat parseLogFormat(std::string_view output) {
    if (output == "json") {
        return LogFormat::Json;
    }
    if (output == "json-log") {
        return LogFormat::JsonLog;
    }
    if (output == "auto") {
        return LogFormat::Auto;
    }
    throw std::invalid_argument(
            fmt::format("Unexpected log format: '{}'", output));
}

void processLine(std::string_view timestamp,
                 std::string_view severity,
                 std::string_view message,
                 std::string_view context,
                 LogFormat output) {
    switch (output) {
    case LogFormat::Json:
        if (context.empty()) {
            fmt::println(R"({{"ts":"{}","lvl":"{}","msg":{}}})",
                         timestamp,
                         severity,
                         nlohmann::json(message).dump());
        } else {
            fmt::println(R"({{"ts":"{}","lvl":"{}","msg":{},"ctx":{}}})",
                         timestamp,
                         severity,
                         nlohmann::json(message).dump(),
                         context);
        }
        break;
    case LogFormat::JsonLog:
        if (context.empty()) {
            fmt::println("{} {} {}", timestamp, severity, message);
        } else {
            fmt::println("{} {} {} {}", timestamp, severity, message, context);
        }
        break;
    case LogFormat::Auto:
        throw std::runtime_error(
                "processLine(): Unexpected output=LogFormat::Auto.");
    }
}

void processFile(std::istream& s, LogFormat output) {
    for (std::string line; std::getline(s, line);) {
        if (std::ranges::find(IGNORED_LINES, line) != IGNORED_LINES.end()) {
            continue;
        }

        std::string_view lineView = line;
        if (lineView.at(0) == '{') {
            auto parsedLog = nlohmann::ordered_json::parse(lineView);
            auto contextString = parsedLog["ctx"].dump();

            if (output == LogFormat::Auto) {
                output = LogFormat::JsonLog;
            }
            processLine(parsedLog.at("ts").template get_ref<std::string&>(),
                        parsedLog.at("lvl").template get_ref<std::string&>(),
                        parsedLog.at("msg").template get_ref<std::string&>(),
                        contextString,
                        output);
            continue;
        }

        if (output == LogFormat::Auto) {
            output = LogFormat::Json;
        }

        auto timestampEnd = line.find(' ');
        if (timestampEnd == std::string::npos) {
            continue;
        }
        auto timestamp{lineView.substr(0, timestampEnd)};
        if (std::ranges::find(IGNORED_WORDS, timestamp) !=
            IGNORED_WORDS.end()) {
            continue;
        }

        auto severityEnd = line.find(' ', timestampEnd + 1);
        if (severityEnd == std::string::npos) {
            continue;
        }
        auto severity{lineView.substr(timestampEnd + 1,
                                      severityEnd - timestampEnd - 1)};

        auto contextBegin = line.find(" {\"");

        try {
            if (contextBegin != std::string::npos && line.back() == '}') {
                auto context = lineView.substr(contextBegin + 1);
                auto message = lineView.substr(severityEnd + 1,
                                               contextBegin - severityEnd - 1);

                { auto validJson = nlohmann::json::parse(context); }
                processLine(timestamp, severity, message, context, output);
                continue;
            }
        } catch (const nlohmann::json::exception& e) {
            // Proceed without context.
        }

        auto message = lineView.substr(severityEnd + 1);
        processLine(timestamp, severity, message, {}, output);
    }
}
