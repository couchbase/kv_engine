/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/configuration_iface.h>

#include <folly/lang/Assume.h>
#include <utility>

std::string_view format_as(ParameterErrorType type) {
    switch (type) {
    case ParameterErrorType::Unsupported:
        return "unsupported";
    case ParameterErrorType::InvalidValue:
        return "invalid_arguments";
    }
    folly::assume_unreachable();
}

void to_json(nlohmann::json& json, const ParameterErrorType& type) {
    json = format_as(type);
}

void to_json(nlohmann::json& json, const ParameterInfo& info) {
    json["value"] = info.value;
    json["requiresRestart"] = info.requiresRestart;
}

void to_json(nlohmann::json& json, const ParameterError& error) {
    json["error"] = error.type;
    json["message"] = error.message;
}

void to_json(nlohmann::json& json, const ParameterValidationResult& result) {
    std::visit([&json](auto&& arg) { json = arg; }, result);
}

bool hasErrors(const ParameterValidationMap& validation) {
    for (const auto& [key, result] : validation) {
        if (std::holds_alternative<ParameterError>(result)) {
            return true;
        }
    }
    return false;
}

bool requiresRestart(const ParameterValidationMap& validation) {
    for (const auto& [key, result] : validation) {
        if (std::holds_alternative<ParameterInfo>(result) &&
            std::get<ParameterInfo>(result).requiresRestart) {
            return true;
        }
    }
    return false;
}

ParameterError ParameterError::unsupported() {
    return {ParameterErrorType::Unsupported,
            "Parameter not supported by this bucket"};
}

ParameterError ParameterError::invalidValue(std::string message) {
    return {ParameterErrorType::InvalidValue, std::move(message)};
}

ParameterError::ParameterError(ParameterErrorType type, std::string message)
    : type(type), message(std::move(message)) {
}

ParameterInfo::ParameterInfo(nlohmann::json value, bool requiresRestart)
    : value(std::move(value)), requiresRestart(requiresRestart) {
}
