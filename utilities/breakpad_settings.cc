/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "breakpad_settings.h"
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <filesystem>
namespace cb::breakpad {

void Settings::validate() const {
    // validate that this represents a valid configuration:
    if (enabled) {
        // minidump_dir must be specified
        if (minidump_dir.empty()) {
            throw std::invalid_argument(
                    R"("breakpad:minidump_dir" must be specified)");
        }
        //  And it must be a directory
        const auto path = std::filesystem::path(minidump_dir);
        if (!std::filesystem::is_directory(path)) {
            throw std::system_error(
                    std::make_error_code(std::errc::no_such_file_or_directory),
                    fmt::format(R"("breakpad:minidump_dir":"{}")",
                                minidump_dir));
        }
    }
}

void to_json(nlohmann::json& json, const Settings& settings) {
    json = {{"content", "default"},
            {"enabled", settings.enabled},
            {"minidump_dir", settings.minidump_dir}};
}

void from_json(const nlohmann::json& json, Settings& settings) {
    settings.enabled = json.value("enabled", false);
    settings.minidump_dir = json.value("minidump_dir", "");
    const auto content = json.value("content", "default");
    if (content != "default") {
        throw std::invalid_argument(
                R"("breakpad:content" settings must be set to "default")");
    }
}

std::ostream& operator<<(std::ostream& os, const Content& content) {
    switch (content) {
    case cb::breakpad::Content::Default:
        os << "default";
        return os;
    }
    throw std::invalid_argument("cb::breakpad::Content: Invalid value: " +
                                std::to_string(int(content)));
}

} // namespace cb::breakpad
