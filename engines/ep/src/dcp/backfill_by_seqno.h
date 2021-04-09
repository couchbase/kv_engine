/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "backfill.h"

/**
 * This class provides common data required by concrete classes providing a
 * backfill over a seqno range. The name is influenced by the original
 * disk backfill which backfilled using the couchstore by-seqno index.
 * However this class is now consumed by in-memory and disk backfill classes
 * where a start and end is required.
 */
class DCPBackfillBySeqno : public virtual DCPBackfill {
public:
    DCPBackfillBySeqno(std::shared_ptr<ActiveStream> s,
                       uint64_t start,
                       uint64_t end)
        : DCPBackfill(s), startSeqno(start), endSeqno(end) {
    }

protected:
    /**
     * Start seqno of the backfill
     */
    const uint64_t startSeqno{0};

    /**
     * End seqno of the backfill
     */
    uint64_t endSeqno{0};
};