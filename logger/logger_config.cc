/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
