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

#include <nlohmann/json.hpp>

namespace cb::logger {

Config::Config(const nlohmann::json& json) {
    filename = json.value("filename", filename);
    buffersize = json.value("buffersize", buffersize);
    cyclesize = json.value("cyclesize", cyclesize);
    unit_test = json.value("unit_test", unit_test);
    console = json.value("console", console);
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

} // namespace cb::logger
