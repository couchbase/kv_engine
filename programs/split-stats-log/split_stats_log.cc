/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/// This is a small utility to extract sections out of the larger stats.log
/// file. The stats.log file contains a lot of information, and it can be
/// overwhelming to parse through it manually. This utility allows us to extract
/// specific sections of interest, such as the connections information, and
/// write it to a separate files for easier analysis.
///
/// The utility currently only extracts the "mcstat connections" section, but it
/// can be easily extended to extract other sections as well by adding more
/// handlers to the segment_handlers map. The extracted connections information
/// is written to a JSON file for easier consumption by other tools or scripts.
/// This utility can be run in the same directory as the stats.log file, and it
/// will recursively search for all stats.log files and extract the relevant
/// sections from them. The output files will be named "connections.json" and
/// will be placed in the same directory as the corresponding stats.log file.
/// This allows for easy organization and access to the extracted information,
/// especially when dealing with multiple stats.log files from different runs or
/// environments.
///

#include <nlohmann/json.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <unordered_map>
#include <vector>

/// Parse a file content into sections, each section consisting of a
/// description line and its content. The file format is:
///   separator line
///   description line
///   command line (which we ignore)
///   separator line
///   content
///   separator line
///   description line
///   ...
///
/// @param content The content of the file
/// @return A vector of pairs where the first element is the description and
///         the second element is the content of that section
static std::vector<std::pair<std::string_view, std::string_view>> parse_file(
        std::string_view content) {
    constexpr std::string_view separator =
            "=================================================================="
            "============";

    std::vector<std::pair<std::string_view, std::string_view>> sections;

    while (!content.empty()) {
        // Find the separator line
        auto idx = content.find(separator);
        if (idx == std::string_view::npos) {
            break;
        }
        content.remove_prefix(idx + separator.size());

        // Skip the newline after the separator
        if (!content.empty() && content.front() == '\n') {
            content.remove_prefix(1);
        }

        // Extract the description line
        idx = content.find('\n');
        if (idx == std::string_view::npos) {
            break;
        }
        auto description = content.substr(0, idx);
        content.remove_prefix(idx + 1);

        // Skip the separator line after the description
        idx = content.find(separator);
        if (idx == std::string_view::npos) {
            break;
        }
        content.remove_prefix(idx + separator.size());

        // Skip the newline after the separator
        if (!content.empty() && content.front() == '\n') {
            content.remove_prefix(1);
        }

        // Find the next separator which marks the end of the content
        idx = content.find(separator);
        std::string_view section_content;
        if (idx == std::string_view::npos) {
            section_content = content;
            content = {};
        } else {
            section_content = content.substr(0, idx);
            // Don't remove prefix here - the next iteration will find this
            // separator
        }

        sections.emplace_back(description, section_content);
    }

    return sections;
}

static std::string load_file(const std::filesystem::path& path) {
    const auto size = std::filesystem::file_size(path);
    std::string content;
    content.reserve(size);

    std::ifstream file(path);
    std::string line;
    while (std::getline(file, line)) {
        content.append(line);
        content.push_back('\n');
    }
    return content;
}

void iterate_lines(std::string_view content,
                   const std::function<void(std::string_view)>& callback) {
    while (!content.empty()) {
        const auto newline_pos = content.find('\n');
        std::string_view line;
        if (newline_pos == std::string_view::npos) {
            line = content;
            content = {};
        } else {
            line = content.substr(0, newline_pos);
            content.remove_prefix(newline_pos + 1);
        }

        // Skip empty lines
        if (!line.empty()) {
            callback(line);
        }
    }
}

static void parse_connections(const std::filesystem::path& file,
                              const std::string_view,
                              const std::string_view value) {
    nlohmann::json json = nlohmann::json::array();
    iterate_lines(value, [&json](std::string_view line) {
        // The line look like: "  0 { ... }", so we need to trim the leading
        // digits and spaces and the rest should be JSON
        while (!line.empty() &&
               (std::isdigit(line.front()) || std::isspace(line.front()))) {
            line.remove_prefix(1);
        }

        // Skip empty lines after trimming (this would be a bug)
        if (line.empty()) {
            return;
        }
        try {
            json.emplace_back(nlohmann::json::parse(line));
        } catch (std::exception& e) {
            std::cerr << "Garbled entry found. Ignoring: " << e.what()
                      << std::endl;
        }
    });
    const auto output_path = file.parent_path() / "connections.json";
    std::cout << "     -> " << output_path.generic_string() << std::endl;
    std::ofstream output(output_path);
    output << json.dump(2) << std::endl;
    output.close();
}

static void analyze_file(const std::filesystem::path& path) {
    using HandlerFunction = std::function<void(
            const std::filesystem::path&, std::string_view, std::string_view)>;
    const std::unordered_map<std::string, HandlerFunction> segment_handlers = {
            {"memcached mcstat connections", parse_connections}};

    std::cout << "Analyzing: " << path.generic_string() << std::endl;
    const auto content = load_file(path);
    const auto segments = parse_file(content);
    for (const auto& [segment, value] : segments) {
        std::cout << "Segment: [" << segment << "]" << std::endl;
        auto handler = segment_handlers.find(std::string{segment});
        if (handler != segment_handlers.end()) {
            handler->second(path, segment, value);
        }
    }
}

int main() {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(
                 std::filesystem::current_path())) {
        if (entry.is_regular_file() && entry.path().filename() == "stats.log") {
            analyze_file(entry.path());
        }
    }

    return EXIT_SUCCESS;
}
