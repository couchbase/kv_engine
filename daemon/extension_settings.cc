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
#include "extension_settings.h"

ExtensionSettings::ExtensionSettings(gsl::not_null<const cJSON*> json) {
    auto* root = const_cast<cJSON*>(json.get());
    auto* obj = cJSON_GetObjectItem(root, "module");
    if (obj == nullptr) {
        throw std::invalid_argument(
                "ExtensionSettings: mandatory element module not found");
    }
    if (obj->type != cJSON_String) {
        throw std::invalid_argument(
                R"(ExtensionSettings: "module" must be a string)");
    }
    soname.assign(obj->valuestring);

    obj = cJSON_GetObjectItem(root, "config");
    if (obj != nullptr) {
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                    R"(ExtensionSettings: "config" must be a string)");
        }
        config.assign(obj->valuestring);
    }
}
