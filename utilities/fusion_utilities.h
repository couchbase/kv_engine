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

#include <platform/split_string.h>
#include <string>

inline const std::string magma_fusion_namespace_prefix = "kv";
inline constexpr size_t magma_fusion_namespace_depth = 2;

inline std::string generateFusionNamespace(const std::string& uuid) {
    return magma_fusion_namespace_prefix + "/" + uuid;
}

inline bool isValidFusionNamespace(std::string_view namespaceStr) {
    const auto parts = cb::string::split(namespaceStr, '/');
    return parts.size() == magma_fusion_namespace_depth &&
           parts[0] == magma_fusion_namespace_prefix;
}