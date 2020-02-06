/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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