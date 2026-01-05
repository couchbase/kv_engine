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

void to_json(nlohmann::json& json, const Config& config) {
    json = {{"filename", config.filename},
            {"buffersize", config.buffersize},
            {"cyclesize", config.cyclesize},
            {"max_aggregated_size", config.max_aggregated_size},
            {"unit_test", config.unit_test},
            {"console", config.console}};
}

void from_json(const nlohmann::json& json, Config& config) {
    config.filename = json.value("filename", config.filename);
    config.buffersize = json.value("buffersize", config.buffersize);
    config.cyclesize = json.value("cyclesize", config.cyclesize);
    config.unit_test = json.value("unit_test", config.unit_test);
    config.console = json.value("console", config.console);
    config.max_aggregated_size =
            json.value("max_aggregated_size", config.max_aggregated_size);
}

} // namespace cb::logger
