/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "disk_format_constraint.h"
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace cb::snapshot {

void to_json(nlohmann::json& json, const DiskFormatConstraint& constraint) {
    json = {{"backend", constraint.backend}};
    if (constraint.maxVersion != 0) {
        json["max_version"] = constraint.maxVersion;
    }
}

void from_json(const nlohmann::json& json, DiskFormatConstraint& constraint) {
    if (!json.contains("backend") || !json["backend"].is_string()) {
        throw std::invalid_argument(
                "from_json: backend must be present as string");
    }
    constraint.backend = json["backend"].get<std::string>();
    if (json.contains("max_version")) {
        if (!json["max_version"].is_number_unsigned()) {
            throw std::invalid_argument(
                    "from_json: max_version must be an unsigned number");
        }
        constraint.maxVersion = json["max_version"].get<uint32_t>();
    } else {
        constraint.maxVersion = 0;
    }
}

bool DiskFormatConstraint::validate(std::string_view snapBackend,
                                    uint32_t snapVersion,
                                    nlohmann::json& failure) const {
    if (!snapBackend.empty() && !backend.empty() && snapBackend != backend) {
        failure = {{"reason", "backend mismatch"},
                   {"snapshot", snapBackend},
                   {"constraint", backend}};
        return false;
    }
    if (snapVersion != 0 && maxVersion != 0 && snapVersion > maxVersion) {
        failure = {{"reason", "version mismatch"},
                   {"snapshot", snapVersion},
                   {"constraint", maxVersion}};
        return false;
    }
    return true;
}

} // namespace cb::snapshot
