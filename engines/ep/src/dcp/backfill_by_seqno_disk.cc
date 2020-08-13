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

#include "dcp/backfill_by_seqno_disk.h"
#include "dcp/active_stream_impl.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore.h"

// Here we must force call the baseclass (DCPBackfill(s) )because of the use of
// multiple inheritance (and virtual inheritance), otherwise stream will be null
// as DCPBackfill() would be used.
DCPBackfillBySeqnoDisk::DCPBackfillBySeqnoDisk(KVBucket& bucket,
                                               std::shared_ptr<ActiveStream> s,
                                               uint64_t startSeqno,
                                               uint64_t endSeqno)
    : DCPBackfill(s),
      DCPBackfillDisk(bucket),
      DCPBackfillBySeqno(s, startSeqno, endSeqno) {
}

backfill_status_t DCPBackfillBySeqnoDisk::create() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillBySeqnoDisk::create(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        transitionState(backfill_state_done);
        return backfill_finished;
    }
    Vbid vbid = stream->getVBucket();

    uint64_t lastPersistedSeqno = bucket.getLastPersistedSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        stream->log(spdlog::level::level_enum::info,
                    "({}) Rescheduling backfill "
                    "because backfill up to seqno {}"
                    " is needed but only up to "
                    "{} is persisted",
                    vbid,
                    endSeqno,
                    lastPersistedSeqno);
        return backfill_snooze;
    }

    KVStore* kvstore = bucket.getROUnderlying(vbid);
    ValueFilter valFilter = ValueFilter::VALUES_DECOMPRESSED;
    if (stream->isKeyOnly()) {
        valFilter = ValueFilter::KEYS_ONLY;
    } else {
        if (stream->isCompressionEnabled()) {
            valFilter = ValueFilter::VALUES_COMPRESSED;
        }
    }

    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::make_unique<DiskCallback>(stream),
            std::make_unique<CacheCallback>(bucket, stream),
            vbid,
            startSeqno,
            DocumentFilter::ALL_ITEMS,
            valFilter,
            stream->isPointInTimeEnabled() == PointInTimeEnabled::Yes
                    ? SnapshotSource::Historical
                    : SnapshotSource::Head);

    // Check startSeqno against the purge-seqno of the opened datafile.
    // 1) A normal stream request would of checked inside streamRequest, but
    //    compaction may have changed the purgeSeqno
    // 2) Cursor dropping can also schedule backfills and they must not re-start
    //    behind the current purge-seqno
    // If the startSeqno != 1 (a client 0 to n request becomes 1 to n) then
    // start-seqno must be above purge-seqno
    if (!scanCtx || (startSeqno != 1 && (startSeqno <= scanCtx->purgeSeqno))) {
        auto vb = bucket.getVBucket(vbid);
        std::stringstream log;
        log << "DCPBackfillBySeqnoDisk::create(): (" << getVBucketId()
            << ") cannot be scanned. Associated stream is set to dead state.";
        end_stream_status_t status = END_STREAM_BACKFILL_FAIL;
        if (scanCtx) {
            log << " startSeqno:" << startSeqno
                << " < purgeSeqno:" << scanCtx->purgeSeqno;
            status = END_STREAM_ROLLBACK;
        } else {
            log << " failed to create scan";
        }
        log << ". The vbucket state:";
        if (vb) {
            log << VBucket::toString(vb->getState());
        } else {
            log << "vb not found!!";
        }

        stream->log(spdlog::level::level_enum::warn, "{}", log.str());
        stream->setDead(status);
        transitionState(backfill_state_done);
    } else {
        bool markerSent =
                stream->markDiskSnapshot(startSeqno,
                                         scanCtx->maxSeqno,
                                         scanCtx->persistedCompletedSeqno,
                                         scanCtx->maxVisibleSeqno,
                                         scanCtx->timestamp);

        if (markerSent) {
            // This value may be an overestimate - it includes prepares/aborts
            // which will not be sent if the stream is not sync write aware
            stream->setBackfillRemaining(scanCtx->documentCount);
            transitionState(backfill_state_scanning);
        } else {
            transitionState(backfill_state_completing);
        }
    }

    this->scanCtx = std::move(scanCtx);

    return backfill_success;
}

backfill_status_t DCPBackfillBySeqnoDisk::scan() {
    auto stream = streamPtr.lock();
    if (!stream) {
        complete(true);
        return backfill_finished;
    }

    Vbid vbid = stream->getVBucket();

    if (!(stream->isActive())) {
        complete(true);
        return backfill_finished;
    }

    KVStore* kvstore = bucket.getROUnderlying(vbid);
    scan_error_t error =
            kvstore->scan(static_cast<BySeqnoScanContext&>(*scanCtx));

    if (error == scan_again) {
        return backfill_success;
    }

    stream->setBackfillScanLastRead(scanCtx->lastReadSeqno);

    transitionState(backfill_state_completing);

    return backfill_success;
}

void DCPBackfillBySeqnoDisk::complete(bool cancelled) {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillBySeqnoDisk::complete(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn; {}",
                getVBucketId(),
                cancelled ? "cancelled" : "finished");
        transitionState(backfill_state_done);
        return;
    }

    stream->completeBackfill();

    auto severity = cancelled ? spdlog::level::level_enum::info
                              : spdlog::level::level_enum::debug;
    stream->log(severity,
                "({}) Backfill task ({} to {}) {}",
                vbid,
                startSeqno,
                endSeqno,
                cancelled ? "cancelled" : "finished");

    transitionState(backfill_state_done);
}
