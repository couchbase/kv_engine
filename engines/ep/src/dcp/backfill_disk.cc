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

#include "dcp/backfill_disk.h"
#include "active_stream.h"
#include "bucket_logger.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"
#include <fmt/ostream.h>

CacheCallback::CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s)
    : bucket(bucket), streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
}

// Do a get and restrict the collections lock scope to just these checks.
GetValue CacheCallback::get(VBucket& vb,
                            CacheLookup& lookup,
                            ActiveStream& stream) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, get a handle on the collection manifest
    auto cHandle = vb.lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        return {};
    }
    return vb.getInternal(nullptr,
                          bucket.getEPEngine(),
                          /*options*/ NONE,
                          stream.isKeyOnly() ? VBucket::GetKeyOnly::Yes
                                             : VBucket::GetKeyOnly::No,
                          cHandle);
}

void CacheCallback::callback(CacheLookup& lookup) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // If diskKey is in Prepared namespace then the in-memory StoredValue isn't
    // sufficient - as it doesn't contain the durability requirements (level).
    // Must get from disk.
    if (lookup.getKey().isPrepared()) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // Check if the stream will allow the key, this is here to avoid reading
    // the value when dropping keys
    if (!stream_->collectionAllowed(lookup.getKey().getDocKey())) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto gv = get(*vb, lookup, *stream_);
    if (gv.getStatus() == cb::engine_errc::success) {
        if (gv.item->getBySeqno() == lookup.getBySeqno()) {
            if (stream_->backfillReceived(std::move(gv.item),
                                          BACKFILL_FROM_MEMORY)) {
                setStatus(cb::engine_errc::key_already_exists);
                return;
            }
            setStatus(cb::engine_errc::no_memory); // Pause the backfill
            return;
        }
    }
    setStatus(cb::engine_errc::success);
}

DiskCallback::DiskCallback(std::shared_ptr<ActiveStream> s) : streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
}

void DiskCallback::callback(GetValue& val) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    if (skipItem(*val.item)) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // MB-26705: Make the backfilled item cold so ideally the consumer would
    // evict this before any cached item if they get into memory pressure.
    val.item->setFreqCounterValue(0);

    if (!stream_->backfillReceived(std::move(val.item), BACKFILL_FROM_DISK)) {
        setStatus(cb::engine_errc::no_memory); // Pause the backfill
    } else {
        setStatus(cb::engine_errc::success);
    }
}

bool BySeqnoDiskCallback::skipItem(const Item& item) const {
    switch (item.getCommitted()) {
    case CommittedState::CommittedViaMutation:
    case CommittedState::CommittedViaPrepare:
    case CommittedState::PrepareAborted:
        break;
    case CommittedState::Pending:
    case CommittedState::PreparedMaybeVisible:
    case CommittedState::PrepareCommitted:
        if (item.getBySeqno() <= int64_t(persistedCompletedSeqno)) {
            return true;
        }
    }
    return false;
}

DCPBackfillDisk::DCPBackfillDisk(KVBucket& bucket) : bucket(bucket) {
}

DCPBackfillDisk::~DCPBackfillDisk() = default;

backfill_status_t DCPBackfillDisk::run() {
    std::lock_guard<std::mutex> lh(lock);
    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });
    switch (state) {
    case backfill_state_init:
        return create();
    case backfill_state_scanning:
        return scan();
    case backfill_state_scanning_history_snapshot:
        return scanHistory();
    case backfill_state_completing:
        complete(false);
        return backfill_finished;
    case backfill_state_done:
        return backfill_finished;
    }

    throw std::logic_error("DCPBackfillDisk::run: Invalid backfill state " +
                           std::to_string(state));
}

void DCPBackfillDisk::cancel() {
    std::lock_guard<std::mutex> lh(lock);
    if (state != backfill_state_done) {
        complete(true);
    }
}

static std::string backfillStateToString(backfill_state_t state) {
    switch (state) {
    case backfill_state_init:
        return "initalizing";
    case backfill_state_scanning:
        return "scanning";
    case backfill_state_scanning_history_snapshot:
        return "scanning_history_snapshot";
    case backfill_state_completing:
        return "completing";
    case backfill_state_done:
        return "done";
    }
    return "<invalid>:" + std::to_string(state);
}

void DCPBackfillDisk::transitionState(backfill_state_t newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
    case backfill_state_init:
        // Not valid to transition back to 'init'
        break;
    case backfill_state_scanning:
        if (state == backfill_state_init) {
            validTransition = true;
        }
        break;
    case backfill_state_scanning_history_snapshot: {
        if (state == backfill_state_scanning || state == backfill_state_init) {
            validTransition = true;
        }
        break;
    }
    case backfill_state_completing:
        if (state == backfill_state_init || state == backfill_state_scanning ||
            state == backfill_state_scanning_history_snapshot) {
            validTransition = true;
        }
        break;
    case backfill_state_done:
        if (state == backfill_state_init || state == backfill_state_scanning ||
            state == backfill_state_scanning_history_snapshot ||
            state == backfill_state_completing) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "DCPBackfillDisk::transitionState:"
                " newState (which is " +
                backfillStateToString(newState) +
                ") is not valid for current state (which is " +
                backfillStateToString(state) + ")");
    }

    state = newState;
}

bool DCPBackfillDisk::setupForHistoryScan(ActiveStream& stream,
                                          ScanContext& scanCtx,
                                          uint64_t startSeqno) {
    Expects(!historyScan);
    if (!stream.areChangeStreamsEnabled()) {
        return false;
    }

    if (scanCtx.historyStartSeqno == 0) {
        // If historyStartSeqno is zero, no history is retained.
        return false;
    }

    // The scan will be proceding into the history range.
    // Either  the scan is all within the history range, or the scan will cross
    // from the non-history to history range. The next blocks create a
    // HistoryScanCtx, which stores with it data required for the history
    // snapshot.
    snapshot_range_t completeRange{startSeqno, scanCtx.maxSeqno};
    if (startSeqno >= scanCtx.historyStartSeqno) {
        // The scan will be completely in the history range.
        // Set the start then as the requested startSeqno
        historyScan = std::make_unique<HistoryScanCtx>(
                snapshot_info_t{startSeqno, completeRange},
                SnapshotType::History);
        return true;
    } else {
        // The scan will be in both ranges, scan crosses from the non-history
        // into the history window.
        // Set the start then as the historyStartSeqno
        historyScan = std::make_unique<HistoryScanCtx>(
                snapshot_info_t{scanCtx.historyStartSeqno, completeRange},
                SnapshotType::HistoryFollowingNoHistory);
        // Adjust the current scan so that it doesn't enter into the history
        // range, it will include, then stop after the last seqno before history
        // begins.
        scanCtx.maxSeqno = scanCtx.historyStartSeqno - 1;
    }

    return false;
}

bool DCPBackfillDisk::createHistoryScanContext() {
    Expects(historyScan);
    Expects(!historyScan->scanCtx);

    auto& historyScanCtx = *historyScan;

    auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    return historyScanCtx.createScanContext(*kvstore, *scanCtx);
}

bool DCPBackfillDisk::HistoryScanCtx::createScanContext(const KVStoreIface& kvs,
                                                        ScanContext& ctx) {
    // Create a new BySeqnoScanContext but most importantly move the previous
    // ScanContext::handle (so the same snapshot is used).
    scanCtx = kvs.initBySeqnoScanContext(std::move(ctx.callback),
                                         std::move(ctx.lookup),
                                         ctx.vbid,
                                         snapshotInfo.start,
                                         ctx.docFilter,
                                         ctx.valFilter,
                                         SnapshotSource::HeadAllVersions,
                                         std::move(ctx.handle));
    if (!scanCtx) {
        // initBySeqnoScanContext logs for failure
        return false;
    }
    return true;
}

DCPBackfillDisk::HistoryScanCtx::HistoryScanCtx(snapshot_info_t snapshotInfo,
                                                SnapshotType snapshotType)
    : snapshotInfo(snapshotInfo), snapshotType(snapshotType) {
    Expects(snapshotType == SnapshotType::History ||
            snapshotType == SnapshotType::HistoryFollowingNoHistory);
}

DCPBackfillDisk::HistoryScanCtx::~HistoryScanCtx() = default;

SnapshotType DCPBackfillDisk::HistoryScanCtx::getSnapshotType() const {
    return snapshotType;
}

// Creation "step"
bool DCPBackfillDisk::scanHistoryCreate(
        const std::shared_ptr<ActiveStream>& streamPtr) {
    Expects(historyScan);

    auto& historyScanCtx = *historyScan;
    Expects(!historyScanCtx.scanCtx);

    // try to create historyScanCtx.scanCtx
    if (!createHistoryScanContext()) {
        EP_LOG_WARN(
                "DCPBackfillDisk::scanHistoryCreate(): ({}) failure "
                "creating history ScanContext",
                getVBucketId());
        transitionState(backfill_state_done);
        return false;
    }

    // snapshot marker (only once per call to scanHistory)
    const auto& ctx =
            dynamic_cast<const BySeqnoScanContext&>(*historyScanCtx.scanCtx);
    if (!streamPtr->markDiskSnapshot(
                historyScanCtx.snapshotInfo.range.getStart(),
                historyScanCtx.snapshotInfo.range.getEnd(),
                ctx.persistedCompletedSeqno,
                ctx.maxVisibleSeqno,
                ctx.timestamp,
                historyScanCtx.getSnapshotType())) {
        transitionState(backfill_state_completing);
        return false;
    }
    return true;
}

backfill_status_t DCPBackfillDisk::scanHistory() {
    Expects(historyScan);

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillDisk::scanHistory(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        transitionState(backfill_state_done);
        return backfill_finished;
    }

    EP_LOG_DEBUG("DCPBackfillDisk::scanHistory ({}) running", getVBucketId());

    // If there is no historyScanCtx.scanCtx create it now (one-off creation).
    // The existing ScanContext cannot be re-used because it could of been
    // created by DCPBackfillBySeqnoDisk or DCPBackfillByIdDisk. In the ByID
    // case it is completely unusable in this scan phase. The simplest approach
    // is to create a new BySeqnoScanContext (moving the KVFileHandle).
    auto& historyScanCtx = *historyScan;
    if (!historyScanCtx.scanCtx && !scanHistoryCreate(stream)) {
        // Failed to create the new ScanContext, inform the stream
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }

    auto& bySeqnoCtx =
            dynamic_cast<BySeqnoScanContext&>(*historyScanCtx.scanCtx);

    auto* const kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);
    switch (kvstore->scanAllVersions(bySeqnoCtx)) {
    case scan_success:
        // Call complete and transition straight to done (via complete). This
        // avoids the sub-class (by_id or by_seq) complete function being called
        stream->completeBackfill(
                bySeqnoCtx.lastReadSeqno, runtime, bySeqnoCtx.diskBytesRead);
        transitionState(backfill_state_completing);
        transitionState(backfill_state_done);
        return backfill_success;
    case scan_again:
        // Scan should run again (e.g. was paused by callback)
        return backfill_success;
    case scan_failed:
        // Scan did not complete successfully. Backfill is missing data,
        // propagate error to stream and (unsuccessfully) finish scan.
        stream->log(spdlog::level::err,
                    "DCPBackfillDisk::scanHistory(): ({}, startSeqno:{}, "
                    "maxSeqno:{}) Scan failed at lastReadSeqno:{}. Setting "
                    "stream to dead state.",
                    getVBucketId(),
                    bySeqnoCtx.startSeqno,
                    bySeqnoCtx.maxSeqno,
                    bySeqnoCtx.lastReadSeqno);
        scanCtx.reset();
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }
    folly::assume_unreachable();
}
