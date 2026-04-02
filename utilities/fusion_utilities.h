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

inline constexpr std::string_view fusion_json_key_bucket_uuid = "bucket_uuid";
inline constexpr std::string_view fusion_json_key_logstore_uri = "logstore_uri";
inline constexpr std::string_view fusion_json_key_metadatastore_auth_token =
        "metadatastore_auth_token";
inline constexpr std::string_view fusion_json_key_metadatastore_uri =
        "metadatastore_uri";
inline constexpr std::string_view fusion_json_key_mount_paths = "mountPaths";
inline constexpr std::string_view fusion_json_key_namespace = "namespace";
inline constexpr std::string_view fusion_json_key_snapshot_uuid =
        "snapshot_uuid";
inline constexpr std::string_view fusion_json_key_snapshot_uuid_camel =
        "snapshotUUID";
inline constexpr std::string_view fusion_json_key_term = "term";
inline constexpr std::string_view fusion_json_key_token = "token";
inline constexpr std::string_view fusion_json_key_valid_till = "valid_till";
inline constexpr std::string_view fusion_json_key_vbucket_list = "vbucket_list";

inline std::string generateFusionNamespace(const std::string& uuid) {
    return magma_fusion_namespace_prefix + "/" + uuid;
}

inline bool isValidFusionNamespace(std::string_view namespaceStr) {
    const auto parts = cb::string::split(namespaceStr, '/');
    return parts.size() == magma_fusion_namespace_depth &&
           parts[0] == magma_fusion_namespace_prefix;
}