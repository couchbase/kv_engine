/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_types.h"
#include "item.h"

#include <cstdint>

/**
 * Captures the result of a rollback request.
 * Contains if the rollback was successful, highSeqno of the vBucket after
 * rollback, and the last snaspshot range in the vb after rollback. Also
 * contains the high completed and high prepared seqnos.
 */
class RollbackResult {
public:
    /**
     * Constructor only to be used for unsuccessful rollback. Takes a single
     * bool to indicate success. Must be false or an assertion will fire.
     */
    explicit RollbackResult(bool success);

    RollbackResult(bool success,
                   uint64_t highSeqno,
                   uint64_t snapStartSeqno,
                   uint64_t snapEndSeqno);

    bool operator==(const RollbackResult& other);
    bool operator!=(const RollbackResult& other) {
        return !operator==(other);
    }

    bool success;
    uint64_t highSeqno;
    uint64_t snapStartSeqno;
    uint64_t snapEndSeqno;
};

std::ostream& operator<<(std::ostream& os, const RollbackResult& result);
