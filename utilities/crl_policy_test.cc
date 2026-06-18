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

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

// =====================================================================
// format_as / to_json / from_json for CrlPolicy
// =====================================================================

TEST(CrlPolicy, FormatAs) {
    EXPECT_EQ("Disabled", format_as(CrlPolicy::Disabled));
    EXPECT_EQ("Permissive", format_as(CrlPolicy::Permissive));
    EXPECT_EQ("Strict", format_as(CrlPolicy::Strict));
    EXPECT_EQ("Require", format_as(CrlPolicy::Require));
}

TEST(CrlPolicy, ToJson) {
    EXPECT_EQ("Disabled",
              nlohmann::json(CrlPolicy::Disabled).get<std::string>());
    EXPECT_EQ("Permissive",
              nlohmann::json(CrlPolicy::Permissive).get<std::string>());
    EXPECT_EQ("Strict", nlohmann::json(CrlPolicy::Strict).get<std::string>());
    EXPECT_EQ("Require", nlohmann::json(CrlPolicy::Require).get<std::string>());
}

TEST(CrlPolicy, FromJsonRoundtrip) {
    for (auto policy : {CrlPolicy::Disabled,
                        CrlPolicy::Permissive,
                        CrlPolicy::Strict,
                        CrlPolicy::Require}) {
        nlohmann::json j = policy;
        EXPECT_EQ(policy, j.get<CrlPolicy>());
    }
}

TEST(CrlPolicy, FromJsonInvalidType) {
    nlohmann::json j = 42;
    EXPECT_THROW(j.get<CrlPolicy>(), std::invalid_argument);
}

TEST(CrlPolicy, FromJsonUnknownString) {
    nlohmann::json j = "Unknown";
    EXPECT_THROW(j.get<CrlPolicy>(), std::invalid_argument);
}

// =====================================================================
// to_json / from_json for CrlPolicyPerScope
// =====================================================================

TEST(CrlPolicyPerScope, ToJson) {
    CrlPolicyPerScope scope{CrlPolicy::Strict, CrlPolicy::Require};
    nlohmann::json j = scope;
    EXPECT_EQ("Strict", j["node_to_node"].get<std::string>());
    EXPECT_EQ("Require", j["client_auth"].get<std::string>());
}

TEST(CrlPolicyPerScope, FromJsonRoundtrip) {
    CrlPolicyPerScope original{CrlPolicy::Permissive, CrlPolicy::Strict};
    nlohmann::json j = original;
    auto parsed = j.get<CrlPolicyPerScope>();
    EXPECT_EQ(original.nodeToNode, parsed.nodeToNode);
    EXPECT_EQ(original.clientAuth, parsed.clientAuth);
}

TEST(CrlPolicyPerScope, FromJsonDefaults) {
    nlohmann::json j = nlohmann::json::object();
    auto scope = j.get<CrlPolicyPerScope>();
    EXPECT_EQ(CrlPolicy::Disabled, scope.nodeToNode);
    EXPECT_EQ(CrlPolicy::Disabled, scope.clientAuth);
}

TEST(CrlPolicyPerScope, FromJsonPartialKeys) {
    nlohmann::json j = {{"node_to_node", "Require"}};
    auto scope = j.get<CrlPolicyPerScope>();
    EXPECT_EQ(CrlPolicy::Require, scope.nodeToNode);
    EXPECT_EQ(CrlPolicy::Disabled, scope.clientAuth);
}

TEST(CrlPolicyPerScope, FromJsonPartialKeys_ClientAuthOnly) {
    nlohmann::json j = {{"client_auth", "Strict"}};
    auto scope = j.get<CrlPolicyPerScope>();
    EXPECT_EQ(CrlPolicy::Disabled, scope.nodeToNode);
    EXPECT_EQ(CrlPolicy::Strict, scope.clientAuth);
}
