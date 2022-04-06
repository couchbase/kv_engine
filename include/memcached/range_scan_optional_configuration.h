/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace cb::rangescan {

// Optional parameters that define a snapshot requirements for a RangeScan
struct SnapshotRequirements {
    // The vbucket on the frontend request must match this uuid
    // The snapshot we must also match this uuid
    uint64_t vbUuid{0};
    // This seqno must of been persisted to snapshot
    uint64_t seqno{0};
    /**
     * This is the timeout to use when the seqno is not yet persisted.
     * When std::nullopt, no timeout applies. unit tests can use this to set
     * a timeout of 0, which allows for instant "expiry", yet the external
     * mcbp protocol will reserve 0 to mean no timeout (and std::nullopt will
     * be used for that).
     */
    std::optional<std::chrono::milliseconds> timeout;
    // true: The seqno must still exist in snapshot
    bool seqnoMustBeInSnapshot{false};
};

// Optional configuration for a sampling scan
struct SamplingConfiguration {
    std::size_t samples{0};
    uint32_t seed{0};
};

} // namespace cb::rangescan