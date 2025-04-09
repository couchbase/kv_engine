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
#include <utilities/terminate_handler.h>
#include <fstream>
#include <iostream>
#include <optional>

mclogfmt::LogFormat parseLogFormat(std::string_view output) {
    using mclogfmt::LogFormat;
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

int main(int argc, char** argv) {
    using namespace mclogfmt;

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
