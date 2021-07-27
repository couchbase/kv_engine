/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill_by_seqno_disk.h"
#include "dcp/active_stream_impl.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"

#include <mcbp/protocol/dcp_stream_end_status.h>

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

    const KVStore* kvstore = bucket.getROUnderlying(vbid);
    if (!kvstore) {
        stream->log(spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::create(): couldn't get KVStore "
                    "for vbucket {}",
                    vbid);
        return backfill_finished;
    }
    auto valFilter = getValueFilter(*stream);

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

    if (!scanCtx) {
        stream->log(spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::create() failed to create scan "
                    "for {}, startSeqno:{}, PointInTimeEnabled:{}",
                    getVBucketId(),
                    startSeqno,
                    stream->isPointInTimeEnabled() == PointInTimeEnabled::Yes);
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        transitionState(backfill_state_done);
        return backfill_finished;
    }

    auto [collHighSuccess, collHigh] =
            getHighSeqnoOfCollections(*scanCtx, *kvstore, stream->getFilter());
    if (!collHighSuccess) {
        stream->log(spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(): "
                    "failed to access collections stats on disk for {}.",
                    getVBucketId());
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        transitionState(backfill_state_done);
        return backfill_finished;
    }

    bool allowNonRollBackCollectionStream = false;
    if (collHigh.has_value()) {
        allowNonRollBackCollectionStream =
                stream->getStartSeqno() < scanCtx->purgeSeqno &&
                stream->getStartSeqno() >= collHigh.value() &&
                collHigh.value() <= scanCtx->purgeSeqno;
    }

    // Check startSeqno against the purge-seqno of the opened datafile.
    // 1) A normal stream request would of checked inside streamRequest, but
    //    compaction may have changed the purgeSeqno
    // 2) Cursor dropping can also schedule backfills and they must not re-start
    //    behind the current purge-seqno
    //
    // If allowNonRollBackCollectionStream is false then the purge seqno,
    // collections high seqno must have moved or the stream isn't a
    // collection stream, making it not valid to prevent the stream from
    // rolling back.
    //
    // If the startSeqno != 1 (a client 0 to n request becomes 1 to n) then
    // start-seqno must be above purge-seqno
    if (startSeqno != 1 && (startSeqno <= scanCtx->purgeSeqno) &&
        !allowNonRollBackCollectionStream) {
        auto vb = bucket.getVBucket(vbid);
        stream->log(spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::create(): ({}) cannot be "
                    "scanned. Associated stream is set to dead state. "
                    "startSeqno:{} < purgeSeqno:{}. The vbucket state:{}, "
                    "collHigh-valid:{}, collHigh:{}",
                    getVBucketId(),
                    startSeqno,
                    scanCtx->purgeSeqno,
                    vb ? VBucket::toString(vb->getState()) : "vb not found!!",
                    collHigh.has_value(),
                    collHigh.value_or(-1));

        stream->setDead(cb::mcbp::DcpStreamEndStatus::Rollback);
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

    const KVStore* kvstore = bucket.getROUnderlying(vbid);
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

std::pair<bool, std::optional<uint64_t>>
DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(
        const BySeqnoScanContext& seqnoScanCtx,
        const KVStore& kvStore,
        const Collections::VB::Filter& filter) const {
    if (!seqnoScanCtx.handle) {
        return {false, std::nullopt};
    }

    if (filter.isPassThroughFilter()) {
        return {true, std::nullopt};
    }

    std::optional<uint64_t> collHigh;

    const auto& handle = *seqnoScanCtx.handle.get();
    for (auto cid : filter) {
        auto [success, collStats] =
                kvStore.getCollectionStats(handle, cid.first);
        if (!success) {
            EP_LOG_WARN(
                    "DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(): "
                    "getCollectionStats() failed for {} cid:{}",
                    seqnoScanCtx.vbid,
                    cid.first);
            return {false, std::nullopt};
        }
        collHigh = std::max(collHigh.value_or(0), collStats.highSeqno.load());
    }

    return {true, collHigh};
}
