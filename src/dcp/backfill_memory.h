/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include "callbacks.h"
#include "dcp/backfill.h"

class EphemeralVBucket;

/**
 * Concrete class that does backfill from in-memory ordered data strucuture and
 * informs the DCP stream of the backfill progress.
 *
 * This class calls one synchronous vBucket API to read items in the sequential
 * order from the in-memory ordered data structure and calls the DCP stream
 * for disk snapshot, backfill items and backfill completion.
 */
class DCPBackfillMemory : public DCPBackfill {
public:
    DCPBackfillMemory(SingleThreadedRCPtr<EphemeralVBucket> evb,
                      const active_stream_t& s,
                      uint64_t startSeqno,
                      uint64_t endSeqno);

    backfill_status_t run() override;

    uint16_t getVBucketId() override;

    bool isStreamDead() override {
        return !stream->isActive();
    }

    void cancel() override {
    }

private:
    /**
     * Ref counted ptr to EphemeralVBucket
     */
    SingleThreadedRCPtr<EphemeralVBucket> evb;
};
