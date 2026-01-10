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

#include "with_meta_options.h"

#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>
#include <vector>

namespace cb::mcbp::request {

enum class MutateWithMetaCommand {
    Add,
    Set,
    Delete,
};

void to_json(nlohmann::json& json, const MutateWithMetaCommand& cmd);
void from_json(const nlohmann::json& json, MutateWithMetaCommand& cmd);

struct MutateWithMetaPayload {
    MutateWithMetaCommand command;
    std::optional<uint64_t> cas;
    std::optional<uint64_t> rev_seqno;
    uint32_t flags;
    uint32_t exptime;

    WithMetaOptions options;
    std::vector<std::size_t> cas_offsets;
    std::vector<std::size_t> seqno_offsets;

    bool operator==(const MutateWithMetaPayload&) const = default;
};

void to_json(nlohmann::json& json, const MutateWithMetaPayload& payload);
void from_json(const nlohmann::json& json, MutateWithMetaPayload& payload);

} // namespace cb::mcbp::request
