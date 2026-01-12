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

#include <cstddef>
#include <optional>
#include <string_view>

namespace cb {
/**
 * Counts of active and replica vbuckets for a node and the signature of the
 * fast forward map to detect if the map changes (rev/epoch can change but map
 * contents doesn't).
 */
struct VBucketCounts {
    size_t active{0};
    size_t replica{0};
    size_t signature{0};
};

/**
 * Parse cluster configuration JSON and count active and replica vbuckets in
 * fast forward map for this node.
 *
 * @param configJson The cluster configuration JSON string
 * @return std::optional containing the counts of active and replica vbuckets,
 *         or std::nullopt if parsing fails or no fast forward map exists
 */
std::optional<VBucketCounts> getFutureVbucketCounts(
        std::string_view configJson);

} // namespace cb