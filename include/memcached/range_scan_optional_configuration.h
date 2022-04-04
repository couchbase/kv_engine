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

#include <cstddef>
#include <cstdint>

namespace cb::rangescan {

// Optional parameters that define a snapshot requirements for a RangeScan
struct SnapshotRequirements {
    // The vbucket on the frontend request must match this uuid
    // The snapshot we must also match this uuid
    uint64_t vbUuid{0};
    // This seqno must of been persisted to snapshot
    uint64_t seqno{0};
    // true: The seqno must still exist in snapshot
    bool seqnoMustBeInSnapshot{false};
};

} // namespace cb::rangescan