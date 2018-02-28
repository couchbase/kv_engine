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

#include <platform/dirutils.h>
#include <cstring>
#include <system_error>

namespace cb {
namespace breakpad {

Settings::Settings(gsl::not_null<const cJSON*> json) {
    auto* root = const_cast<cJSON*>(json.get());
    auto* obj = cJSON_GetObjectItem(root, "enabled");
    if (obj == nullptr) {
        throw std::invalid_argument(
                R"("breakpad" settings MUST contain "enabled" attribute)");
    }
    if (obj->type == cJSON_True) {
        enabled = true;
    } else if (obj->type == cJSON_False) {
        enabled = false;
    } else {
        throw std::invalid_argument(
                R"("breakpad:enabled" settings must be a boolean value)");
    }

    obj = cJSON_GetObjectItem(root, "minidump_dir");
    if (obj == nullptr) {
        if (enabled) {
            throw std::invalid_argument(
                    R"("breakpad" settings MUST contain "minidump_dir" attribute when enabled)");
        }
    } else if (obj->type != cJSON_String) {
        throw std::invalid_argument(
                R"("breakpad:minidump_dir" settings must be a string)");
    } else {
        minidump_dir.assign(obj->valuestring);
        if (enabled) {
            if (!cb::io::isDirectory(minidump_dir)) {
                throw std::system_error(
                        std::make_error_code(
                                std::errc::no_such_file_or_directory),
                        R"("breakpad:minidump_dir":')" + minidump_dir + "'");
            }
        }
    }

    obj = cJSON_GetObjectItem(root, "content");
    if (obj != nullptr) {
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                    R"("breakpad:content" settings must be a string)");
        }
        if (strcmp(obj->valuestring, "default") != 0) {
            throw std::invalid_argument(
                    R"("breakpad:content" settings must set to "default")");
        }
        content = Content::Default;
    }
}

} // namespace breakpad
} // namespace cb

std::string to_string(cb::breakpad::Content content) {
    switch (content) {
    case cb::breakpad::Content::Default:
        return "default";
    }
    throw std::invalid_argument(
            "to_string(cb::breakpad::Content): Invalid value: " +
            std::to_string(int(content)));
}
