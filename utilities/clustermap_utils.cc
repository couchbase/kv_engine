/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "clustermap_utils.h"

#include <nlohmann/json.hpp>

namespace cb {

std::optional<VBucketCounts> getFutureVbucketCounts(
        std::string_view configJson) {
    try {
        // Parse the JSON configuration
        auto config_json = nlohmann::json::parse(configJson);

        // Find this node's index in the nodesExt array
        if (!config_json.contains("nodesExt") ||
            !config_json["nodesExt"].is_array()) {
            return std::nullopt;
        }

        int nodeIndex = -1;
        int index = 0;
        for (const auto& node : config_json["nodesExt"]) {
            if (node.contains("thisNode") && node["thisNode"].is_boolean() &&
                node["thisNode"].get<bool>()) {
                nodeIndex = index;
                break;
            }
            index++;
        }

        // If we didn't find thisNode, return nullopt
        if (nodeIndex == -1) {
            return std::nullopt;
        }

        // Check if vBucketServerMap and vBucketMapForward exist
        if (!config_json.contains("vBucketServerMap") ||
            !config_json["vBucketServerMap"].is_object()) {
            return std::nullopt;
        }

        const auto& vBucketMap = config_json["vBucketServerMap"];
        if (!vBucketMap.contains("vBucketMapForward") ||
            !vBucketMap["vBucketMapForward"].is_array()) {
            return std::nullopt;
        }

        // Count vbuckets where this node is active (index 0) or replica (index
        // > 0)
        VBucketCounts counts;
        counts.signature =
                std::hash<nlohmann::json>{}(vBucketMap["vBucketMapForward"]);
        for (const auto& vbucket : vBucketMap["vBucketMapForward"]) {
            if (!vbucket.is_array() || vbucket.empty()) {
                continue;
            }

            // Check each position in the vbucket array
            for (size_t i = 0; i < vbucket.size(); i++) {
                if (!vbucket[i].is_number_integer()) {
                    continue;
                }

                if (vbucket[i].get<int>() == nodeIndex) {
                    if (i == 0) {
                        counts.active++;
                    } else {
                        counts.replica++;
                    }
                    break; // Node found for this vbucket, move to next
                }
            }
        }

        return counts;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

} // namespace cb