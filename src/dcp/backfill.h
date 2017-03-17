/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "dcp/stream.h"

class EventuallyPersistentEngine;
class ScanContext;

/**
 * Indicates the status of the backfill that is run
 */
enum backfill_status_t {
    backfill_success,
    backfill_finished,
    backfill_snooze
};

class DCPBackfill {
public:
    DCPBackfill(const active_stream_t& s,
                uint64_t startSeqno,
                uint64_t endSeqno)
        : stream(s), startSeqno(startSeqno), endSeqno(endSeqno) {
    }

    virtual ~DCPBackfill() {
    }

    /**
     * Run the DCP backfill and return the status of the run
     *
     * @return status of the current run
     */
    virtual backfill_status_t run() = 0;

    /**
     * Get the id of the vbucket for which this object is created
     *
     * @return vbid
     */
    virtual uint16_t getVBucketId() = 0;

    /**
     * Indicates if the DCP stream associated with the backfill is dead
     *
     * @return true if stream is in dead state; else false
     */
    virtual bool isStreamDead() = 0;

    /**
     * Cancels the backfill
     */
    virtual void cancel() = 0;

protected:
    /**
     * Ptr to the associated Active DCP stream. Backfill can be run for only
     * an active DCP stream
     */
    active_stream_t stream;

    /**
     * Start seqno of the backfill
     */
    uint64_t startSeqno;

    /**
     * End seqno of the backfill
     */
    uint64_t endSeqno;
};

using UniqueDCPBackfillPtr = std::unique_ptr<DCPBackfill>;
