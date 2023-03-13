/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill_disk_to_stream.h"

#include "bucket_logger.h"
#include "dcp/active_stream.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"

#include <spdlog/fmt/fmt.h>

DCPBackfill::State DCPBackfillDiskToStream::getNextScanState(
        DCPBackfill::State current) {
    // Disk scan transition is conditional depending on the state of the history
    switch (current) {
    case DCPBackfill::State::Create:
        // Create->Scan or Create->ScanHistory
        if (historyScan && historyScan->startSeqnoIsInsideHistoryWindow()) {
            return DCPBackfill::State::ScanHistory;
        }
        return DCPBackfill::State::Scan;
    case DCPBackfill::State::Scan:
        // Scan->ScanHistory or Scan->Done
        if (historyScan) {
            return DCPBackfill::State::ScanHistory;
        }
        return DCPBackfill::State::Done;

    case DCPBackfill::State::ScanHistory:
        // Done always follows ScanHistory
        return DCPBackfill::State::Done;
    case DCPBackfill::State::Done:
        break;
    }
    throw std::invalid_argument(fmt::format(
            "DCPBackfillDiskToStream::getNextScanState invalid input {}",
            current));
}

bool DCPBackfillDiskToStream::setupForHistoryScan(ActiveStream& stream,
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

    // The scan will be proceeding into the history range.
    // Either  the scan is all within the history range, or the scan will cross
    // from the non-history to history range. The next blocks create a
    // HistoryScanCtx, which stores with it data required for the history
    // snapshot.
    snapshot_range_t completeRange{startSeqno, scanCtx.maxSeqno};
    if (startSeqno >= scanCtx.historyStartSeqno) {
        // The scan will be completely in the history range.
        // Set the start then as the requested startSeqno
        historyScan = std::make_unique<HistoryScanCtx>(
                scanCtx.historyStartSeqno,
                snapshot_info_t{startSeqno, completeRange});
        return true;
    } else {
        // The scan will be in both ranges, scan crosses from the non-history
        // into the history window.
        // Set the start then as the historyStartSeqno
        historyScan = std::make_unique<HistoryScanCtx>(
                scanCtx.historyStartSeqno,
                snapshot_info_t{scanCtx.historyStartSeqno, completeRange});
        // Adjust the current scan so that it doesn't enter into the history
        // range, it will include, then stop after the last seqno before history
        // begins.
        scanCtx.maxSeqno = scanCtx.historyStartSeqno - 1;
    }

    return false;
}

bool DCPBackfillDiskToStream::createHistoryScanContext(KVBucket& bucket,
                                                       ScanContext& scanCtx) {
    Expects(historyScan);
    Expects(!historyScan->scanCtx);

    auto& historyScanCtx = *historyScan;

    auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    return historyScanCtx.createScanContext(*kvstore, scanCtx);
}

bool DCPBackfillDiskToStream::HistoryScanCtx::createScanContext(
        const KVStoreIface& kvs, ScanContext& ctx) {
    // Create a new BySeqno scan, but move the callback and most importantly
    // the KVFileHandle - so this scan uses the original snapshot
    scanCtx = kvs.initBySeqnoScanContext(std::move(ctx.callback),
                                         std::move(ctx.lookup),
                                         ctx.vbid,
                                         snapshotInfo.start,
                                         ctx.docFilter,
                                         ctx.valFilter,
                                         SnapshotSource::Head,
                                         std::move(ctx.handle));
    if (!scanCtx) {
        // initBySeqnoScanContext logs for failure
        return false;
    }
    return true;
}

bool DCPBackfillDiskToStream::HistoryScanCtx::startSeqnoIsInsideHistoryWindow()
        const {
    return snapshotInfo.range.getStart() >= historyStartSeqno;
}

DCPBackfillDiskToStream::HistoryScanCtx::HistoryScanCtx(
        uint64_t historyStartSeqno, snapshot_info_t snapshotInfo)
    : historyStartSeqno(historyStartSeqno), snapshotInfo(snapshotInfo) {
}
DCPBackfillDiskToStream::HistoryScanCtx::~HistoryScanCtx() = default;

// Creation "step"
bool DCPBackfillDiskToStream::scanHistoryCreate(KVBucket& bucket,
                                                ScanContext& scanCtx,
                                                ActiveStream& stream) {
    Expects(historyScan);

    auto& historyScanCtx = *historyScan;
    Expects(!historyScanCtx.scanCtx);

    // try to create historyScanCtx.scanCtx
    if (!createHistoryScanContext(bucket, scanCtx)) {
        EP_LOG_WARN(
                "DCPBackfillDiskToStream::scanHistoryCreate(): ({}) failure "
                "creating history ScanContext",
                getVBucketId());
        return false;
    }

    // snapshot marker (only once per call to scanHistory)
    const auto& ctx =
            dynamic_cast<const BySeqnoScanContext&>(*historyScanCtx.scanCtx);
    if (!stream.markDiskSnapshot(historyScanCtx.snapshotInfo.range.getStart(),
                                 historyScanCtx.snapshotInfo.range.getEnd(),
                                 ctx.persistedCompletedSeqno,
                                 ctx.maxVisibleSeqno,
                                 ctx.timestamp,
                                 ActiveStream::SnapshotSource::History)) {
        // Stream rejected the marker
        return false;
    }
    return true;
}

backfill_status_t DCPBackfillDiskToStream::doHistoryScan(KVBucket& bucket,
                                                         ScanContext& scanCtx) {
    Expects(historyScan);

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillDiskToStream::doHistoryScan(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        return backfill_finished;
    }

    EP_LOG_DEBUG("DCPBackfillDiskToStream::doHistoryScan ({}) running",
                 getVBucketId());

    // If there is no historyScanCtx.scanCtx create it now (one-off creation).
    // The existing ScanContext cannot be re-used because it could of been
    // created by DCPBackfillBySeqnoDisk or DCPBackfillByIdDisk. In the ByID
    // case it is completely unusable in this scan phase. The simplest approach
    // is to create a new BySeqnoScanContext (moving the KVFileHandle).
    auto& historyScanCtx = *historyScan;
    if (!historyScanCtx.scanCtx &&
        !scanHistoryCreate(bucket, scanCtx, *stream)) {
        // Failed to create the new ScanContext, inform the stream
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }

    auto& bySeqnoCtx =
            dynamic_cast<BySeqnoScanContext&>(*historyScanCtx.scanCtx);

    auto* const kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);
    switch (kvstore->scanAllVersions(bySeqnoCtx)) {
    case ScanStatus::Success:
        stream->setBackfillScanLastRead(bySeqnoCtx.lastReadSeqno);
        historyScanComplete(*stream);
        return backfill_finished;
    case ScanStatus::Cancelled:
        // Cancelled as vbucket/stream have gone away, normal behaviour
        historyScanComplete(*stream);
        return backfill_finished;
    case ScanStatus::Yield:
        // Scan should run again (e.g. was paused by callback)
        stream->incrementNumBackfillPauses();
        return backfill_success;
    case ScanStatus::Failed:
        // Scan did not complete successfully. Backfill is missing data,
        // propagate error to stream and (unsuccessfully) finish scan.
        stream->log(
                spdlog::level::err,
                "DCPBackfillDiskToStream::doHistoryScan(): ({}, startSeqno:{}, "
                "maxSeqno:{}) Scan failed at lastReadSeqno:{}. Setting "
                "stream to dead state.",
                getVBucketId(),
                bySeqnoCtx.startSeqno,
                bySeqnoCtx.maxSeqno,
                bySeqnoCtx.lastReadSeqno);
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }
    folly::assume_unreachable();
}

void DCPBackfillDiskToStream::historyScanComplete(ActiveStream& stream) {
    Expects(historyScan);
    auto& historyScanCtx = *historyScan;
    seqnoScanComplete(stream,
                      historyScanCtx.scanCtx->diskBytesRead,
                      historyScanCtx.snapshotInfo.range.getStart(),
                      historyScanCtx.snapshotInfo.range.getEnd());
}

void DCPBackfillDiskToStream::seqnoScanComplete(ActiveStream& stream,
                                                size_t bytesRead,
                                                uint64_t startSeqno,
                                                uint64_t endSeqno) {
    runtime += (std::chrono::steady_clock::now() - runStart);
    stream.completeBackfill(runtime, bytesRead);
    stream.log(spdlog::level::level_enum::debug,
               "({}) Backfill task ({} to {}) complete",
               vbid,
               startSeqno,
               endSeqno);
}
