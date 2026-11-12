/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "nightmare_configuration.h"

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <stdexcept>

namespace cb::config {

NightmareParameters loadNightmareConfiguration(std::string_view path) {
    // ordered_json keeps file order: some parameters must be set before others
    // (see the checkpoint recovery marks in nightmare_configuration.json).
    const auto json =
            nlohmann::ordered_json::parse(cb::io::loadFile(std::string{path}),
                                          /* callback */ nullptr,
                                          /* allow exceptions */ true,
                                          /* ignore_comments */ true);
    if (!json.is_object()) {
        throw std::invalid_argument(
                "cb::config::loadNightmareConfiguration: expected a JSON "
                "object mapping parameter name to value");
    }

    NightmareParameters parameters;
    for (const auto& [key, value] : json.items()) {
        std::string valueStr;
        if (value.is_string()) {
            valueStr = value.get<std::string>();
        } else if (value.is_number() || value.is_boolean()) {
            valueStr = value.dump();
        } else {
            throw std::invalid_argument(fmt::format(
                    "cb::config::loadNightmareConfiguration: value for \"{}\" "
                    "must be a string, number or boolean",
                    key));
        }

        // A value holding the separator would silently split in two.
        if (valueStr.find(';') != std::string::npos) {
            throw std::invalid_argument(fmt::format(
                    "cb::config::loadNightmareConfiguration: value for \"{}\" "
                    "must not contain ';'",
                    key));
        }

        parameters.emplace_back(key, std::move(valueStr));
    }

    return parameters;
}

std::string toConfigString(const NightmareParameters& parameters) {
    std::string config;
    for (const auto& [key, value] : parameters) {
        config.append(key).append("=").append(value).append(";");
    }
    return config;
}

} // namespace cb::config
