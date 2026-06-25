/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <string>
#include <string_view>
#include <vector>

enum class CrlPolicy {
    /// Ignore missing or expired
    Disabled,
    /// Check the CRL. Allow and warn for missing and expired CRL
    /// Reject revoked certificates
    Permissive,
    /// Allow (and warn) for a missing CRL. Reject for an expired CRL, reject
    /// revoked certificates
    Strict,
    /// Reject when missing or expired CRL, reject revoked certificates
    Require
};

std::string_view format_as(CrlPolicy policy);
void to_json(nlohmann::json& json, const CrlPolicy& policy);
void from_json(const nlohmann::json& json, CrlPolicy& policy);

/// Identifies the TLS connection scope to which a CRL policy applies.
///
/// Each scope can carry an independent CrlPolicy so that, for example,
/// strict revocation enforcement is applied to client-certificate
/// authentication while node-to-node connections use a more permissive
/// policy during a phased rollout.
enum class CrlPolicyScope {
    /// Intra-cluster TLS: the certificates presented by peer nodes during
    /// node-to-node connections and used by outbound connections created
    /// by memcached
    NodeToNode,

    /// Client-certificate authentication: the certificates presented by
    /// external clients (SDKs, applications) when mTLS / X.509
    /// authentication is enabled (ClientCertMode::Enabled or Mandatory).
    /// This is the primary P0 enforcement scope.
    ClientAuth,
};

struct CrlPolicyPerScope {
    CrlPolicy nodeToNode{CrlPolicy::Disabled};
    CrlPolicy clientAuth{CrlPolicy::Disabled};
    bool operator==(const CrlPolicyPerScope&) const = default;
};

void to_json(nlohmann::json& json, const CrlPolicyPerScope& scope);
void from_json(const nlohmann::json& json, CrlPolicyPerScope& scope);

/// A holder structure for all CRL-related settings
struct CrlConfiguration {
    CrlPolicyPerScope policies;
    std::vector<std::string> files;
    bool check_intermediate{false};
    bool operator==(const CrlConfiguration&) const = default;
};
