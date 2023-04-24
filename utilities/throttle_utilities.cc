/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "throttle_utilities.h"
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string_view>

namespace cb::throttle {
using namespace std::string_view_literals;
constexpr std::string_view unlimited = "unlimited"sv;

nlohmann::json limit_to_json(std::size_t limit) {
    if (limit == std::numeric_limits<std::size_t>::max()) {
        return unlimited;
    }
    return limit;
}

std::size_t get_limit(const nlohmann::json& json,
                      const char* key,
                      std::size_t undefined_value) {
    if (!json.contains(key)) {
        return undefined_value;
    }
    const auto& element = json.at(key);
    if (element.is_number()) {
        return element.get<std::size_t>();
    }
    if (element.is_string()) {
        auto value = element.get<std::string>();
        if (value == unlimited) {
            return std::numeric_limits<std::size_t>::max();
        }
        throw std::runtime_error(
                fmt::format("from_json(Set[Node]ThrottleLimitPayload): {} must "
                            "be set to \"{}\" if passed as a string",
                            key,
                            unlimited));
    }
    throw std::runtime_error(fmt::format(
            "from_json(SetThrottleLimitPayload): Invalid format for {}", key));
}

void to_json(nlohmann::json& json, const SetThrottleLimitPayload& object) {
    json["reserved"] = limit_to_json(object.reserved);
    json["hard_limit"] = limit_to_json(object.hard_limit);
}

void from_json(const nlohmann::json& json, SetThrottleLimitPayload& object) {
    object.reserved = get_limit(json, "reserved");
    object.hard_limit = get_limit(json, "hard_limit");
}

void to_json(nlohmann::json& json, const SetNodeThrottleLimitPayload& object) {
    if (object.capacity) {
        json["capacity"] = limit_to_json(object.capacity.value());
    }
    if (object.default_throttle_reserved_units) {
        json["default_throttle_reserved_units"] =
                limit_to_json(object.default_throttle_reserved_units.value());
    }
    if (object.default_throttle_hard_limit) {
        json["default_throttle_hard_limit"] =
                limit_to_json(object.default_throttle_hard_limit.value());
    }
}

void from_json(const nlohmann::json& json,
               SetNodeThrottleLimitPayload& object) {
    if (json.contains("capacity")) {
        object.capacity = get_limit(json, "capacity");
    }

    if (json.contains("default_throttle_reserved_units")) {
        object.default_throttle_reserved_units =
                get_limit(json, "default_throttle_reserved_units");
    }
    if (json.contains("default_throttle_hard_limit")) {
        object.default_throttle_hard_limit =
                get_limit(json, "default_throttle_hard_limit");
    }

    if (object.default_throttle_hard_limit ||
        object.default_throttle_reserved_units) {
        if (!(object.default_throttle_hard_limit &&
              object.default_throttle_reserved_units)) {
            // both must be present
            throw std::invalid_argument(
                    "both hard and reserved must be provided");
        }

        // hard must be >= reserved
        if (object.default_throttle_hard_limit <
            object.default_throttle_reserved_units) {
            throw std::invalid_argument("hard must be >= reserved");
        }
    }
}

} // namespace cb::throttle