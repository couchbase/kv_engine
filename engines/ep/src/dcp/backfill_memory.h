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

#include "dcp/backfill_by_seqno.h"
#include "ephemeral_vb.h"

/**
 * Concrete class that does backfill from in-memory ordered data structure and
 * informs the DCP stream of the backfill progress.
 *
 * This class creates a range iterator on the in-memory seqList, then
 * during scan() reads items one by one, passing to the given ActiveStream
 * for disk snapshot, backfill items and backfill completion.
 */
class DCPBackfillMemoryBuffered : public DCPBackfillBySeqno {
public:
    DCPBackfillMemoryBuffered(EphemeralVBucketPtr evb,
                              std::shared_ptr<ActiveStream> s,
                              uint64_t startSeqno,
                              uint64_t endSeqno);

    ~DCPBackfillMemoryBuffered() override;

    backfill_status_t run() override;

    void cancel() override;

private:
    /* The possible states of the DCPBackfillMemoryBuffered */
    enum class BackfillState : uint8_t { Init, Scanning, Done };

    static std::string backfillStateToString(BackfillState state);

    /**
     * Creates a range iterator on Ephemeral VBucket to read items as a snapshot
     * in sequential order. Backfill snapshot range is decided here.
     */
    backfill_status_t create();

    /**
     * Reads the items in the snapshot (iterator) one by one. In case of high
     * memory usage postpones the reading of items, and reading can be resumed
     * later on from that point.
     */
    backfill_status_t scan();

    /**
     * Indicates the completion to the stream.
     *
     * @param cancelled indicates if the backfill finished fully or was
     *                  cancelled in between; for debug
     */
    void complete(bool cancelled);

    /**
     * Makes valid transitions on the backfill state machine
     */
    void transitionState(BackfillState newState);

    /**
     * shared pointer to EphemeralVBucket. Needs to be shared as we cannot
     * delete the underlying VBucket while we have an iterator active on it.
     */
    std::shared_ptr<EphemeralVBucket> evb;

    BackfillState state;

    /**
     * Range iterator (on the vbucket) created for the backfill
     */
    SequenceList::RangeIterator rangeItr;

    // VBucket ID, only used for debug / tracing.
    const Vbid vbid;
};
