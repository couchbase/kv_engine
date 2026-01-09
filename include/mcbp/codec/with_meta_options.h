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

#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>

namespace cb::mcbp {
struct WithMetaOptions {
    WithMetaOptions() = default;
    /// Intialize the options from the bitfield used in the
    /// Add/Set/DeleteWithMeta and SetWithMetaEx commands
    WithMetaOptions(uint32_t options);
    bool operator==(const WithMetaOptions&) const = default;

    /// Encode the options back into a bitfield
    uint32_t encode();

    CheckConflicts check_conflicts = CheckConflicts::Yes;
    GenerateCas generate_cas = GenerateCas::No;
    DeleteSource delete_source = DeleteSource::Explicit;
    ForceAcceptWithMetaOperation force_accept =
            ForceAcceptWithMetaOperation::No;
};

void to_json(nlohmann::json& j, const WithMetaOptions& opt);
void from_json(const nlohmann::json& j, WithMetaOptions& opt);

} // namespace cb::mcbp
