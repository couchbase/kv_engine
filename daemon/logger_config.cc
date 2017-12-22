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

#include "logger_config.h"

LoggerConfig::LoggerConfig(gsl::not_null<const cJSON*> json) {
    auto* root = const_cast<cJSON*>(json.get());
    auto* obj = cJSON_GetObjectItem(root, "filename");
    if (obj != nullptr) {
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                    "LoggerConfig: \"filename\" must be a string");
        }
        filename.assign(obj->valuestring);
    }

    obj = cJSON_GetObjectItem(root, "buffersize");
    if (obj != nullptr) {
        if (obj->type != cJSON_Number) {
            throw std::invalid_argument(
                    "LoggerConfig: \"buffersize\" must be an unsigned int");
        }
        buffersize = static_cast<unsigned int>(obj->valueint);
    }

    obj = cJSON_GetObjectItem(root, "cyclesize");
    if (obj != nullptr) {
        if (obj->type != cJSON_Number) {
            throw std::invalid_argument(
                    "LoggerConfig: \"cyclesize\" must be an unsigned int");
        }
        cyclesize = static_cast<unsigned int>(obj->valueint);
    }

    obj = cJSON_GetObjectItem(root, "sleeptime");
    if (obj != nullptr) {
        if (obj->type != cJSON_Number) {
            throw std::invalid_argument(
                    "LoggerConfig: \"sleeptime\" must be an unsigned int");
        }
        sleeptime = static_cast<unsigned int>(obj->valueint);
    }

    obj = cJSON_GetObjectItem(root, "unit_test");
    unit_test = false;
    if (obj != nullptr) {
        if (obj->type == cJSON_True) {
            unit_test = true;
        } else if (obj->type != cJSON_False) {
            throw std::invalid_argument(
                    "LoggerConfig: \"unit_test\" must be a bool");
        }
    }
}

bool LoggerConfig::operator==(const LoggerConfig& other) const {
    return (this->filename == other.filename) &&
           (this->buffersize == other.buffersize) &&
           (this->sleeptime == other.sleeptime) &&
           (this->cyclesize == other.cyclesize) &&
           (this->unit_test == other.unit_test);
}

bool LoggerConfig::operator!=(const LoggerConfig& other) const {
    return !(*this == other);
}
