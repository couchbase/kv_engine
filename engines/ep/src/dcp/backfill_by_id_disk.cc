/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill_by_id_disk.h"
#include "bucket_logger.h"
#include "dcp/active_stream.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"

#include <mcbp/protocol/unsigned_leb128.h>

DCPBackfillByIdDisk::DCPBackfillByIdDisk(KVBucket& bucket,
                                         std::shared_ptr<ActiveStream> s)
    : DCPBackfillDiskToStream(bucket, std::move(s)) {
}

static ByIdRange createCollectionSystemNamespaceRange(CollectionID cid) {
    // The system event start/end we can make from SystemEventFactory
    auto sysRange =
            SystemEventFactory::makeCollectionEventKeyPairForRangeScan(cid);
    return ByIdRange{sysRange.first, sysRange.second};
}

static ByIdRange createCollectionRange(CollectionID cid) {
    // Create the start and end keys for the collection itself
    cb::mcbp::unsigned_leb128<CollectionIDType> start(uint32_t{cid});

    // The end key is the "start key" + "\xff", so we clone the start key into
    // an array that is 1 byte larger than the largest possible leb128 prefix
    // and set the byte after the leb128 prefix to be 0xff.
    std::array<uint8_t,
               cb::mcbp::unsigned_leb128<CollectionIDType>::getMaxSize() + 1>
            end;
    std::ranges::copy(start, end.begin());
    end[start.size()] = std::numeric_limits<uint8_t>::max();
    return ByIdRange{DiskDocKey{{start.data(),
                                 start.size(),
                                 DocKeyEncodesCollectionId::Yes}},
                     DiskDocKey{{end.data(),
                                 start.size() + 1,
                                 DocKeyEncodesCollectionId::Yes}}};
}

backfill_status_t DCPBackfillByIdDisk::create() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillByIdDisk::create(): backfill create ended "
                "prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    auto valFilter = stream->getValueFilter();

    // Iterate through the collections of the ActiveStream::filter. Note the
    // assumption is that if we are executing here, the ActiveStream's filter
    // is suitable for the ByID scan.
    std::vector<ByIdRange> ranges;
    for (auto [cid, sid] : stream->getFilter()) {
        (void)sid; // No use for the mapped scope in building the scan
        // Create the ByIdRange objects required to scan the given collection.
        // Two ranges required for a collection scan, first is the system
        // namespace but looking for events relating to the collection.
        // Second is the collection itself.
        ranges.emplace_back(createCollectionSystemNamespaceRange(cid));
        ranges.emplace_back(createCollectionRange(cid));
    }

    scanCtx = kvstore->initByIdScanContext(
            std::make_unique<DiskCallback>(stream),
            std::make_unique<CacheCallback>(
                    bucket,
                    stream,
                    static_cast<std::chrono::milliseconds>(
                            bucket.getConfiguration()
                                    .getDcpBackfillRunDurationLimit())),
            getVBucketId(),
            ranges,
            DocumentFilter::ALL_ITEMS,
            valFilter);
    backfill_status_t status = backfill_finished;
    if (!scanCtx) {
        auto vb = bucket.getVBucket(getVBucketId());
        std::stringstream log;
        log << getVBucketId() << " initByIdScanContext failed";
        if (vb) {
            log << VBucket::toString(vb->getState());
        } else {
            log << "vb not found!!";
        }

        stream->logWithContext(spdlog::level::level_enum::warn,
                               "DCPBackfillByIdDisk::create()",
                               {{"error", log.str()}});
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }
    // Will check if a history scan is required.
    setupForHistoryScan(*stream, *scanCtx, 0);

    bool markerSent = stream->markOSODiskSnapshot(scanCtx->maxSeqno);
    if (markerSent) {
        status = backfill_success;
    } else {
        complete(*stream);
    }

    return status;
}

backfill_status_t DCPBackfillByIdDisk::scan() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillByIdDisk::scan(): backfill scan ended prematurely "
                "as the associated stream is deleted by the producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }
    if (!stream->isActive()) {
        stream->logWithContext(
                spdlog::level::warn,
                "DCPBackfillByIdDisk::scan(): ended prematurely as "
                "stream is not active");
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    auto& cacheCallback =
            static_cast<CacheCallback&>(scanCtx->getCacheCallback());
    cacheCallback.setBackfillStartTime();
    switch (kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx))) {
    case ScanStatus::Success:
    case ScanStatus::Cancelled:
        complete(*stream);
        return backfill_finished;
    case ScanStatus::Yield:
        // Scan should run again (e.g. was paused by callback)
        stream->incrementNumBackfillPauses();
        return backfill_success;
    case ScanStatus::Failed:
        // Scan did not complete successfully. Propagate error to stream.
        stream->logWithContext(
                spdlog::level::err,
                "DCPBackfillByIdDisk::scan(): Scan failed Setting "
                "stream to dead state.");
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }

    folly::assume_unreachable();
}

void DCPBackfillByIdDisk::complete(ActiveStream& stream) {
    trackedPosition = std::nullopt;
    runtime += (std::chrono::steady_clock::now() - runStart);
    stream.completeOSOBackfill(scanCtx->maxSeqno,
                               runtime,
                               scanCtx->diskBytesRead,
                               scanCtx->keysScanned);
    stream.logWithContext(spdlog::level::level_enum::debug,
                          "DCPBackfillByIdDisk task complete");
}

backfill_status_t DCPBackfillByIdDisk::scanHistory() {
    return doHistoryScan(bucket, *scanCtx);
}