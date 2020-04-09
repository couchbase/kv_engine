/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "breakpad_settings.h"
#include "json_utilities.h"

#include <platform/dirutils.h>

#include <nlohmann/json.hpp>

namespace cb::breakpad {

Settings::Settings(const nlohmann::json& json) {
    enabled = cb::jsonGet<bool>(json, "enabled");

    if (enabled) {
        minidump_dir = cb::jsonGet<std::string>(json, "minidump_dir");
        if (!cb::io::isDirectory(minidump_dir)) {
            throw std::system_error(
                    std::make_error_code(std::errc::no_such_file_or_directory),
                    R"("breakpad:minidump_dir":')" + minidump_dir + "'");
        }
    }

    auto content = json.value("content", "default");
    if (content != "default") {
        throw std::invalid_argument(
                R"("breakpad:content" settings must set to "default")");
    }
}
} // namespace cb::breakpad

std::string to_string(cb::breakpad::Content content) {
    switch (content) {
    case cb::breakpad::Content::Default:
        return "default";
    }
    throw std::invalid_argument(
            "to_string(cb::breakpad::Content): Invalid value: " +
            std::to_string(int(content)));
}
