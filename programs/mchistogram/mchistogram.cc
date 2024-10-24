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
#include <folly/FileUtil.h>
#include <nlohmann/json.hpp>
#include <platform/command_line_options_parser.h>
#include <platform/dirutils.h>
#include <utilities/timing_histogram_printer.h>
#include <iostream>

using cb::getopt::CommandLineOptionsParser;

static void usage(CommandLineOptionsParser& getopt, int exitcode) {
    std::cerr << R"(Usage mchistogram [options] filename(s)

mchistogram reads a file (or stdin) containing the JSON representation of one
of the histograms produced by Couchbase and prints it as a histogram.

Example:
   $ mcstat [options] --json timings > timings.json
   $ mchistogram timings.json

Options:

)" << getopt;

    exit(exitcode);
}

static bool isHistogram(const nlohmann::json& json) {
    return json.contains("bucketsLow") && json.contains("data");
}

static void printHistograms(std::string_view content,
                            std::string_view filename) {
    try {
        auto json = nlohmann::json::parse(content);
        if (json.is_object()) {
            if (isHistogram(json)) {
                TimingHistogramPrinter printer(json);
                printer.dumpHistogram("unknown");
            } else {
                bool found = false;
                for (const auto& [k, v] : json.items()) {
                    if (isHistogram(v)) {
                        TimingHistogramPrinter printer(v);
                        printer.dumpHistogram(k);
                        found = true;
                    }
                }
                if (!found) {
                    fmt::println(
                            stderr,
                            R"(It doesn't look like "{}" contains a histogram)",
                            filename);
                    std::exit(EXIT_FAILURE);
                }
            }
        } else {
            fmt::println(stderr,
                         R"(It doesn't look like "{}" contains a histogram)",
                         filename);
            std::exit(EXIT_FAILURE);
        }
    } catch (const std::exception& e) {
        fmt::println(stderr,
                     R"(Exception thrown from dumping "{}": {})",
                     filename,
                     e.what());
        std::exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    CommandLineOptionsParser parser;
    parser.addOption({[&parser](auto) { usage(parser, EXIT_SUCCESS); },
                      "help",
                      "This help text"});
    parser.addOption({[](auto) {
                          fmt::println("Couchbase Server {}", PRODUCT_VERSION);
                          std::exit(EXIT_SUCCESS);
                      },
                      "version",
                      "Print program version and exit"});

    auto arguments = parser.parse(
            argc, argv, [&parser]() { usage(parser, EXIT_FAILURE); });
    if (arguments.empty()) {
        std::string content;
        if (folly::readFile<std::string>(STDIN_FILENO, content)) {
            printHistograms(content, "stdin");
            std::exit(EXIT_SUCCESS);
        }

        std::system_error error(
                errno, std::system_category(), "Failed to read standard input");
        fmt::println(stderr, "{}", error.what());
        std::exit(EXIT_FAILURE);
    }

    for (const auto& file : arguments) {
        printHistograms(cb::io::loadFile(file), file);
    }

    return EXIT_SUCCESS;
}
