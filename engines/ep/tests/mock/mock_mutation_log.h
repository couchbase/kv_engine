/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "mutation_log.h"

class MockMutationLog : public MutationLog {
public:
    using MutationLog::MutationLog;

    /* NOLINTNEXTLINE(modernize-avoid-c-arrays) */
    std::unique_ptr<uint8_t[]>& public_getBlockBuffer() {
        return blockBuffer;
    }

    size_t public_getBlockPos() {
        return blockPos;
    }
};
