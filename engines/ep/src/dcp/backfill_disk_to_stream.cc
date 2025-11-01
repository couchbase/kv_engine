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

#include "backfill_disk.h"
#include "bucket_logger.h"
#include "dcp/active_stream.h"
#include "ep_bucket.h"
#include "kvstore/kvstore.h"

#include <platform/json_log_conversions.h>
#include <spdlog/fmt/fmt.h>

DCPBackfillDiskToStream::DCPBackfillDiskToStream(
        KVBucket& bucket, std::shared_ptr<ActiveStream> s)
    : DCPBackfillToStream(std::move(s)),
      bucket(bucket),
      maxNoProgressDuration(
              getBackfillIdleLimitSeconds(bucket.getConfiguration())),
      bytesToFree(std::chrono::seconds{10}) {
}

DCPBackfillDiskToStream::~DCPBackfillDiskToStream() = default;

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
                snapshot_info_t{startSeqno, completeRange},
                SnapshotType::History);
        return true;
    }
    // The scan will be in both ranges, scan crosses from the non-history
    // into the history window.
    // Set the start then as the historyStartSeqno
    historyScan = std::make_unique<HistoryScanCtx>(
            scanCtx.historyStartSeqno,
            snapshot_info_t{scanCtx.historyStartSeqno, completeRange},
            SnapshotType::HistoryFollowingNoHistory);
    // Adjust the current scan so that it doesn't enter into the history
    // range, it will include, then stop after the last seqno before history
    // begins.
    scanCtx.maxSeqno = scanCtx.historyStartSeqno - 1;

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
    // Create a new BySeqnoScanContext but most importantly move the previous
    // ScanContext::handle (so the same snapshot is used). We can also move the
    // original CacheLookup callback but must create a new disk callback. This
    // because when scanning the history, we want a "vanilla" DiskCallback which
    // does not skip any items (see skipItem member function).
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

bool DCPBackfillDiskToStream::HistoryScanCtx::startSeqnoIsInsideHistoryWindow()
        const {
    return snapshotInfo.range.getStart() >= historyStartSeqno;
}

DCPBackfillDiskToStream::HistoryScanCtx::HistoryScanCtx(
        uint64_t historyStartSeqno,
        snapshot_info_t snapshotInfo,
        SnapshotType snapshotType)
    : historyStartSeqno(historyStartSeqno),
      snapshotInfo(snapshotInfo),
      snapshotType(snapshotType) {
    Expects(snapshotType == SnapshotType::History ||
            snapshotType == SnapshotType::HistoryFollowingNoHistory);
}

DCPBackfillDiskToStream::HistoryScanCtx::~HistoryScanCtx() = default;

SnapshotType DCPBackfillDiskToStream::HistoryScanCtx::getSnapshotType() const {
    return snapshotType;
}

// Creation "step"
bool DCPBackfillDiskToStream::scanHistoryCreate(
        KVBucket& bucket,
        ScanContext& scanCtx,
        const std::shared_ptr<ActiveStream>& streamPtr) {
    Expects(historyScan);

    auto& historyScanCtx = *historyScan;
    Expects(!historyScanCtx.scanCtx);

    // try to create historyScanCtx.scanCtx
    if (!createHistoryScanContext(bucket, scanCtx)) {
        EP_LOG_WARN_CTX(
                "DCPBackfillDiskToStream::scanHistoryCreate(): failure "
                "creating history ScanContext",
                {"vb", getVBucketId()});
        return false;
    }

    // snapshot marker (only once per call to scanHistory)
    const auto& ctx =
            dynamic_cast<const BySeqnoScanContext&>(*historyScanCtx.scanCtx);
    if (!streamPtr->markDiskSnapshot(
                historyScanCtx.snapshotInfo.range.getStart(),
                historyScanCtx.snapshotInfo.range.getEnd(),
                ctx.persistedCompletedSeqno,
                ctx.highPreparedSeqno,
                ctx.persistedPreparedSeqno,
                ctx.maxVisibleSeqno,
                ctx.purgeSeqno,
                historyScanCtx.getSnapshotType())) {
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
        EP_LOG_WARN_CTX(
                "DCPBackfillDiskToStream::doHistoryScan(): backfill create "
                "ended prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }

    EP_LOG_DEBUG_CTX("DCPBackfillDiskToStream::doHistoryScan running",
                     {"vb", getVBucketId()});

    // If there is no historyScanCtx.scanCtx create it now (one-off creation).
    // The existing ScanContext cannot be re-used because it could of been
    // created by DCPBackfillBySeqnoDisk or DCPBackfillByIdDisk. In the ByID
    // case it is completely unusable in this scan phase. The simplest approach
    // is to create a new BySeqnoScanContext (moving the KVFileHandle).
    auto& historyScanCtx = *historyScan;
    if (!historyScanCtx.scanCtx &&
        !scanHistoryCreate(bucket, scanCtx, stream)) {
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
        OBJ_LOG_ERROR_CTX(*stream,
                          "DCPBackfillDiskToStream::doHistoryScan(): Scan "
                          "failed. Setting "
                          "stream to dead state.",
                          {"start_seqno", bySeqnoCtx.startSeqno},
                          {"max_seqno", bySeqnoCtx.maxSeqno},
                          {"last_read_seqno", bySeqnoCtx.lastReadSeqno});
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
                      historyScanCtx.scanCtx->keysScanned,
                      historyScanCtx.snapshotInfo.range.getStart(),
                      historyScanCtx.snapshotInfo.range.getEnd(),
                      historyScanCtx.scanCtx->lastReadSeqno);
    historyScan.reset();
    trackedPosition = std::nullopt;
}

void DCPBackfillDiskToStream::seqnoScanComplete(ActiveStream& stream,
                                                size_t bytesRead,
                                                size_t keysScanned,
                                                uint64_t startSeqno,
                                                uint64_t endSeqno,
                                                uint64_t maxSeqno) {
    runtime += (cb::time::steady_clock::now() - runStart);
    stream.completeBackfill(maxSeqno, runtime, bytesRead, keysScanned);
    OBJ_LOG_DEBUG_CTX(stream,
                      "Backfill task complete",
                      {"start_seqno", startSeqno},
                      {"end_seqno", endSeqno});
}

bool DCPBackfillDiskToStream::isSlow(const ActiveStream& stream) {
    // Takeover streams are immune to this. Ns_server does not handle the stream
    // close gracefully in some cases. The optional also controls if this
    // feature is enabled
    if (!maxNoProgressDuration || stream.isTakeoverStream()) {
        return false;
    }

    // If history scan, care only about the progress of that, as
    if (historyScan && historyScan->scanCtx &&
        isProgressStalled(historyScan->scanCtx->getPosition()) &&
        shouldEndStreamToReclaimDisk(*historyScan->scanCtx, stream)) {
        OBJ_LOG_WARN_CTX(
                stream,
                "Backfill task cancelled as no progress has been made on the "
                "history-scan for more than the no progress duration and disk "
                "space should be reclaimed",
                {"max_no_progress_duration", *maxNoProgressDuration},
                {"position", historyScan->scanCtx->getPosition().getValue()},
                {"trackedPosition", trackedPosition->getValue()});
        return true;
    }
    if (scanCtx && isProgressStalled(scanCtx->getPosition()) &&
        shouldEndStreamToReclaimDisk(*scanCtx, stream)) {
        OBJ_LOG_WARN_CTX(
                stream,
                "Backfill task cancelled as no progress has been made on the "
                "scan for more than the no progress duration and disk "
                "space should be reclaimed",
                {"max_no_progress_duration", *maxNoProgressDuration},
                {"position", scanCtx->getPosition().getValue()},
                {"trackedPosition", trackedPosition->getValue()});
        return true;
    }
    return false;
}

static std::optional<float> diskUsagePercent(
        const std::filesystem::space_info& si) noexcept {
    // space_info is -1 when filesystem::space could not determine a value.
    // if capacity is 0 also chicken out
    if (constexpr std::uintmax_t X(std::numeric_limits<uint64_t>::max());
        si.capacity == 0 || si.capacity == X || si.free == X ||
        si.available == X) {
        return {};
    }
    const auto used{si.capacity - si.free};
    return 100.0 * used / si.capacity;
}

bool DCPBackfillDiskToStream::isProgressStalled(
        const ScanContext::Position& position) {
    if (!trackedPosition || *trackedPosition != position) {
        // Begin tracking or save the new position. Both cases record current
        // time for the next check.
        lastPositionChangedTime = cb::time::steady_clock::now();
        trackedPosition = position;
        return false;
    }

    // *trackedPosition == position, are we within the time limit?
    const auto now = cb::time::steady_clock::now();
    if ((now - lastPositionChangedTime) < *maxNoProgressDuration) {
        return false;
    }

    // The backfill has been idle for longer than maxNoProgressDuration
    return true;
}

bool DCPBackfillDiskToStream::shouldEndStreamToReclaimDisk(
        const ScanContext& scan, const ActiveStream& stream) {
    if (!scan.handle) {
        // no handle available to check.
        return false;
    }

    auto bytesToFree = this->bytesToFree.getAndMaybeRefreshValue(
            [&scan]() { return scan.handle->getHowManyBytesCouldBeFreed(); });
    if (bytesToFree == 0) {
        // Bytes to free is zero, ending the stream and closing the scan will
        // have no effect on free-space so don't bother with disrupting the
        // client.
        return false;
    }

    const auto& config = bucket.getConfiguration();
    const auto threshold = config.getDcpBackfillIdleDiskThreshold();
    auto& epBucket = dynamic_cast<EPBucket&>(bucket);
    auto si = epBucket.getCachedDiskSpaceInfo();
    auto usage = diskUsagePercent(si);
    if (!usage) {
        // space_info not usable, log it
        OBJ_LOG_WARN_CTX(
                stream,
                "DCPBackfillDiskToStream::shouldEndStreamToReclaimDisk cannot "
                "get disk usage",
                {"directory", config.getDbname()},
                {"capacity", si.capacity},
                {"free", si.free},
                {"available", si.available});
        return false;
    }

    // Only end if usage exceeds the configured threshold
    if (*usage < threshold) {
        return false;
    }
    OBJ_LOG_WARN_CTX(stream,
                     "DCPBackfillDiskToStream::"
                     "shouldEndStreamToReclaimDisk and usage >= threshold",
                     {"directory", config.getDbname()},
                     {"capacity", si.capacity},
                     {"free", si.free},
                     {"available", si.available},
                     {"percent_used", *usage},
                     {"threshold", threshold},
                     {"bytes_to_free", bytesToFree});
    return true;
}
