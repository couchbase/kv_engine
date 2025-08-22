/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <variant>

/**
 * Encodes the type of error, which we return to ns_server.
 */
enum class ParameterErrorType {
    /// The parameter is not supported.
    Unsupported,
    /// The parameter value is invalid.
    InvalidValue,
};

std::string_view format_as(ParameterErrorType type);

void to_json(nlohmann::json& j, const ParameterErrorType& type);

/**
 * Encodes the error details, which we return to ns_server.
 */
struct ParameterError {
    ParameterError() = delete;
    static ParameterError unsupported();

    static ParameterError invalidValue(std::string message);

    ParameterErrorType type{ParameterErrorType::Unsupported};
    std::string message;

private:
    ParameterError(ParameterErrorType type, std::string message);
};

void to_json(nlohmann::json& j, const ParameterError& error);

enum class ParameterVisibility {
    /// The parameter is user-accessible.
    Public,
    /// The parameter is for internal use only.
    Internal,
};

void to_json(nlohmann::json& j, const ParameterVisibility& visibility);

/**
 * Encodes the validation information for a parameter.
 */
struct ParameterInfo {
    ParameterInfo(nlohmann::json value,
                  bool requiresRestart,
                  ParameterVisibility visibility);

    /// The value to set for the parameter.
    nlohmann::json value;
    /// Whether the parameter requires a restart to take effect.
    bool requiresRestart{false};
    /// The visibility of the parameter.
    ParameterVisibility visibility{ParameterVisibility::Internal};
};

void to_json(nlohmann::json& j, const ParameterInfo& info);

/**
 * A map of parameters to set.
 * The key is the parameter name, and the value is the parameter value.
 */
using ParameterMap = std::unordered_map<std::string, std::string>;

/**
 * The result of validating a parameter.
 */
using ParameterValidationResult = std::variant<ParameterInfo, ParameterError>;

void to_json(nlohmann::json& j, const ParameterValidationResult& result);

/**
 * A map of parameters to validation results.
 */
using ParameterValidationMap =
        std::unordered_map<std::string, ParameterValidationResult>;
/**
 * Check if the validation map has any errors.
 *
 * @param validation The validation map to check.
 * @return True if the validation map has any errors, false otherwise.
 */
bool hasErrors(const ParameterValidationMap& validation);

/**
 * Check if the validation map requires a restart.
 *
 * @param validation The validation map to check.
 * @return True if the validation map requires a restart, false otherwise.
 */
bool requiresRestart(const ParameterValidationMap& validation);

/**
 * The interface for configuring a component.
 */
class ConfigurationIface {
public:
    virtual ~ConfigurationIface() = default;

    /**
     * Validate the parameters.
     *
     * @param parameters The parameters to validate.
     * @return A map of parameter names to validation results.
     */
    virtual ParameterValidationMap validateParameters(
            const ParameterMap& parameters) const = 0;
};

/**
 * Create a configuration which accepts all parameters.
 *
 * This is used for components which do not support more complicated validation
 * logic.
 */
std::unique_ptr<ConfigurationIface> createDummyConfiguration();
