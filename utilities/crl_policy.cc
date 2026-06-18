/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "crl_policy.h"

#include <nlohmann/json.hpp>
#include <stdexcept>

std::string_view format_as(CrlPolicy policy) {
    switch (policy) {
    case CrlPolicy::Disabled:
        return "Disabled";
    case CrlPolicy::Permissive:
        return "Permissive";
    case CrlPolicy::Strict:
        return "Strict";
    case CrlPolicy::Require:
        return "Require";
    }
    return "unknown";
}

void to_json(nlohmann::json& json, const CrlPolicy& policy) {
    json = format_as(policy);
}

void from_json(const nlohmann::json& json, CrlPolicy& policy) {
    if (!json.is_string()) {
        throw std::invalid_argument("CRL policy must be a string");
    }
    if (json.get<std::string>() == "Disabled") {
        policy = CrlPolicy::Disabled;
    } else if (json.get<std::string>() == "Permissive") {
        policy = CrlPolicy::Permissive;
    } else if (json.get<std::string>() == "Strict") {
        policy = CrlPolicy::Strict;
    } else if (json.get<std::string>() == "Require") {
        policy = CrlPolicy::Require;
    } else {
        throw std::invalid_argument(
                R"(CRL policy must be one of "Disabled", "Permissive", "Strict" or "Require")");
    }
}

void to_json(nlohmann::json& json, const CrlPolicyPerScope& scope) {
    json = {{"node_to_node", scope.nodeToNode},
            {"client_auth", scope.clientAuth}};
}

void from_json(const nlohmann::json& json, CrlPolicyPerScope& scope) {
    scope.nodeToNode = json.value("node_to_node", CrlPolicy::Disabled);
    scope.clientAuth = json.value("client_auth", CrlPolicy::Disabled);
}
