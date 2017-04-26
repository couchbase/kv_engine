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

#include "config.h"

#include "dcp/backfill_memory.h"
#include "dcp/stream.h"
#include "ep_engine.h"
#include "ephemeral_vb.h"
#include "seqlist.h"

DCPBackfillMemory::DCPBackfillMemory(EphemeralVBucketPtr evb,
                                     const active_stream_t& s,
                                     uint64_t startSeqno,
                                     uint64_t endSeqno)
    : DCPBackfill(s, startSeqno, endSeqno), weakVb(evb) {
}

backfill_status_t DCPBackfillMemory::run() {
    auto evb = weakVb.lock();
    if (!evb) {
        /* We don't have to close the stream here. Task doing vbucket state
           change should handle stream closure */
        LOG(EXTENSION_LOG_WARNING,
            "DCPBackfillMemory::run(): "
            "(vb:%d) running backfill ended prematurely as weakVb can't be "
            "locked; start seqno:%" PRIi64 ", end seqno:%" PRIi64,
            getVBucketId(),
            startSeqno,
            endSeqno);
        return backfill_finished;
    }

    /* Get vb state lock */
    ReaderLockHolder rlh(evb->getStateLock());
    if (evb->getState() == vbucket_state_dead) {
        /* We don't have to close the stream here. Task doing vbucket state
           change should handle stream closure */
        LOG(EXTENSION_LOG_WARNING,
            "DCPBackfillMemory::run(): "
            "(vb:%d) running backfill ended prematurely with vb in dead state; "
            "start seqno:%" PRIi64 ", end seqno:%" PRIi64,
            getVBucketId(),
            startSeqno,
            endSeqno);
        return backfill_finished;
    }

    /* Get sequence of items (backfill) from memory */
    std::vector<UniqueItemPtr> items;
    ENGINE_ERROR_CODE status;
    std::tie(status, items) = evb->inMemoryBackfill(startSeqno, endSeqno);

    /* Handle any failures */
    if (status != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "DCPBackfillMemory::run(): "
            "(vb:%d) running backfill failed with error %d ; "
            "start seqno:%" PRIi64 ", end seqno:%" PRIi64
            ". "
            "Hence closing the stream",
            getVBucketId(),
            status,
            startSeqno,
            endSeqno);
        /* Close the stream, DCP clients can retry */
        stream->setDead(END_STREAM_BACKFILL_FAIL);
        return backfill_finished;
    }

    /* Put items onto readyQ of the DCP stream */
    stream->incrBackfillRemaining(items.size());

    /* Mark disk snapshot */
    stream->markDiskSnapshot(items.front()->getBySeqno(),
                             items.back()->getBySeqno());

    /* Move every item to the stream */
    for (auto& item : items) {
        stream->backfillReceived(
                std::move(item), BACKFILL_FROM_MEMORY, /*force*/ true);
    }

    /* Indicate completion to the stream */
    stream->completeBackfill();

    return backfill_finished;
}

uint16_t DCPBackfillMemory::getVBucketId() {
    return stream->getVBucket();
}
