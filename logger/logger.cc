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

#include "logger.h"

namespace cb {
namespace logger {

Config::Config(const cJSON& json) {
    auto* root = const_cast<cJSON*>(&json);
    auto* obj = cJSON_GetObjectItem(root, "filename");
    if (obj != nullptr) {
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                    R"(cb::logger::Config: "filename" must be a string)");
        }
        filename.assign(obj->valuestring);
    }

    obj = cJSON_GetObjectItem(root, "buffersize");
    if (obj != nullptr) {
        if (obj->type != cJSON_Number) {
            throw std::invalid_argument(
                    R"(cb::logger::Config: "buffersize" must be an unsigned int)");
        }
        buffersize = static_cast<unsigned int>(obj->valueint);
    }

    obj = cJSON_GetObjectItem(root, "cyclesize");
    if (obj != nullptr) {
        if (obj->type != cJSON_Number) {
            throw std::invalid_argument(
                    R"(cb::logger::Config: "cyclesize" must be an unsigned int)");
        }
        cyclesize = static_cast<unsigned int>(obj->valueint);
    }

    obj = cJSON_GetObjectItem(root, "unit_test");
    unit_test = false;
    if (obj != nullptr) {
        if (obj->type == cJSON_True) {
            unit_test = true;
        } else if (obj->type != cJSON_False) {
            throw std::invalid_argument(
                    R"(Config: "unit_test" must be a bool)");
        }
    }

    obj = cJSON_GetObjectItem(root, "console");
    if (obj != nullptr) {
        if (obj->type == cJSON_False) {
            console = false;
        } else if (obj->type != cJSON_True) {
            throw std::invalid_argument(
                    R"(Config: "console" must be a bool)");
        }
    }
}

bool Config::operator==(const Config& other) const {
    return (this->filename == other.filename) &&
           (this->buffersize == other.buffersize) &&
           (this->cyclesize == other.cyclesize) &&
           (this->unit_test == other.unit_test) &&
           (this->console == other.console);
}

bool Config::operator!=(const Config& other) const {
    return !(*this == other);
}

} // namespace logger
} // namespace cb
