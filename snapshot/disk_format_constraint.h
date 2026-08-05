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
#include <cstdint>
#include <string>
#include <string_view>

namespace cb::snapshot {

/**
 * The disk format supported by a node requesting a snapshot. Sent as the
 * "storage" field of a PrepareSnapshot request value and used by the node
 * preparing the snapshot to refuse creating a snapshot the requester can't
 * use.
 */
struct DiskFormatConstraint {
    /// The storage backend of the requesting node ("couchstore" or "magma")
    std::string backend;
    /// The maximum disk format version supported by the requesting node.
    /// 0 means unknown; no version restriction is applied.
    uint32_t maxVersion = 0;

    bool operator==(const DiskFormatConstraint&) const = default;

    /**
     * Check if a snapshot manifest may be used by a node with the given disk
     * format constraint. The check is skipped for fields which are unknown
     * (empty backend / version 0) on either side.
     *
     * @param snapBackend the actual snapshot backend
     * @param snapVersion the actual snapshot version
     * @param failure assigned an object describing the validation failure if
     *        the constraint was not met; left untouched otherwise
     * @return whether the constraint was met
     */
    [[nodiscard]] bool validate(std::string_view snapBackend,
                                uint32_t snapVersion,
                                nlohmann::json& failure) const;
};

void to_json(nlohmann::json& json, const DiskFormatConstraint& constraint);
void from_json(const nlohmann::json& json, DiskFormatConstraint& constraint);

} // namespace cb::snapshot
