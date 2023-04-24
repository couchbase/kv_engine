/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <limits>
#include <optional>

namespace cb::throttle {

/// Convert a throttle limit to json (either "unlimited" or a number)
nlohmann::json limit_to_json(std::size_t limit);

/// Get the limit stored under the provided key (if the key isn't set
/// the limit is set to undefined_value)
std::size_t get_limit(
        const nlohmann::json& json,
        const char* key,
        std::size_t undefined_value = std::numeric_limits<std::size_t>::max());

/// The payload used for the SetThrottleLimit command
struct SetThrottleLimitPayload {
    SetThrottleLimitPayload() = default;
    SetThrottleLimitPayload(std::size_t reserved, std::size_t hard)
        : reserved(reserved), hard_limit(hard) {
    }
    std::size_t reserved = std::numeric_limits<std::size_t>::max();
    std::size_t hard_limit = std::numeric_limits<std::size_t>::max();
};

/// Convert the SetThrottleLimitPayload to a JSON document
void to_json(nlohmann::json& json, const SetThrottleLimitPayload& object);

/// Initialize the SetThrottleLimitPayload object from JSON
void from_json(const nlohmann::json& json, SetThrottleLimitPayload& object);

/// The payload in the SetNodeThrottleLimit command
struct SetNodeThrottleLimitPayload {
    std::optional<std::size_t> capacity;
    std::optional<std::size_t> default_throttle_reserved_units;
    std::optional<std::size_t> default_throttle_hard_limit;
};

/// Convert the SetNodeThrottleLimitPayload to a JSON document
void to_json(nlohmann::json& json, const SetNodeThrottleLimitPayload& object);

/// Initialize the SetNodeThrottleLimitPayload object from JSON
void from_json(const nlohmann::json& json, SetNodeThrottleLimitPayload& object);

} // namespace cb::throttle