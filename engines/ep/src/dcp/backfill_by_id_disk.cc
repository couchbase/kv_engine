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
#include "dcp/active_stream_impl.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"

#include <mcbp/protocol/unsigned_leb128.h>

DCPBackfillByIdDisk::DCPBackfillByIdDisk(KVBucket& bucket,
                                         std::shared_ptr<ActiveStream> s,
                                         CollectionID cid)
    : DCPBackfill(s), DCPBackfillDisk(bucket), cid(cid) {
}

backfill_status_t DCPBackfillByIdDisk::create() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillByIdDisk::create(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    auto valFilter = stream->getValueFilter();

    // Create two ranges of keys to have loaded from the scan.
    // 1) system/collection/cid - for the 'metadata', i.e the create/drop marker
    // 2) the range for the collection itself
    // The range for each of the above is the prefix we want and then the suffix
    // of "\xff". E.g. for collection 8
    // start="\8", end="\8\xFF"

    // The system event start/end we can make from SystemEventFactory
    auto sysRange =
            SystemEventFactory::makeCollectionEventKeyPairForRangeScan(cid);

    // Create the start and end keys for the collection itself
    cb::mcbp::unsigned_leb128<CollectionIDType> start(uint32_t{cid});

    // The end key is the "start key" + "\xff", so we clone the start key into
    // an array that is 1 byte larger than the largest possible leb128 prefixe
    // and set the byte after the leb128 prefix to be 0xff.
    std::array<uint8_t,
               cb::mcbp::unsigned_leb128<CollectionIDType>::getMaxSize() + 1>
            end;
    std::copy(start.begin(), start.end(), end.begin());
    end[start.size()] = std::numeric_limits<uint8_t>::max();

    std::vector<ByIdRange> ranges;
    ranges.emplace_back(ByIdRange{sysRange.first, sysRange.second});
    ranges.emplace_back(
            ByIdRange{DiskDocKey{{start.data(),
                                  start.size(),
                                  DocKeyEncodesCollectionId::Yes}},
                      DiskDocKey{{end.data(),
                                  start.size() + 1,
                                  DocKeyEncodesCollectionId::Yes}}});

    scanCtx = kvstore->initByIdScanContext(
            std::make_unique<DiskCallback>(stream),
            std::make_unique<CacheCallback>(bucket, stream),
            getVBucketId(),
            ranges,
            DocumentFilter::ALL_ITEMS,
            valFilter);
    if (!scanCtx) {
        auto vb = bucket.getVBucket(getVBucketId());
        std::stringstream log;
        log << "DCPBackfillByIdDisk::create(): (" << getVBucketId()
            << ") cannot be scanned. Associated stream is set to dead state."
            << " failed to create scan ";
        if (vb) {
            log << VBucket::toString(vb->getState());
        } else {
            log << "vb not found!!";
        }

        stream->log(spdlog::level::level_enum::warn, "{}", log.str());
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    } else {
        auto& dc = dynamic_cast<DiskCallback&>(scanCtx->getValueCallback());
        auto& idScanCtx = dynamic_cast<ByIdScanContext&>(*scanCtx);
        // Set the persistedCompletedSeqno of DiskCallback taken from the
        // persistedCompletedSeqno of the scan context so it's consistent with
        // the file handle
        dc.persistedCompletedSeqno = idScanCtx.persistedCompletedSeqno;

        bool markerSent = stream->markOSODiskSnapshot(scanCtx->maxSeqno);
        if (markerSent) {
            transitionState(State::Scan);
        } else {
            complete();
            return backfill_finished;
        }
    }

    return backfill_success;
}

backfill_status_t DCPBackfillByIdDisk::scan() {
    auto stream = streamPtr.lock();
    if (!stream) {
        complete();
        return backfill_finished;
    }

    if (!(stream->isActive())) {
        complete();
        return backfill_finished;
    }

    const auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    switch (kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx))) {
    case ScanStatus::Success:
        complete();
        return backfill_success;
    case ScanStatus::Yield:
        // Scan should run again (e.g. was paused by callback)
        return backfill_success;
    case ScanStatus::Cancelled:
        // Aborted as vbucket/stream have gone away, normal behaviour
        complete();
        return backfill_finished;
    case ScanStatus::Failed:
        // Scan did not complete successfully. Propagate error to stream.
        stream->log(spdlog::level::err,
                    "DCPBackfillByIdDisk::scan(): ({}, {}) Scan failed Setting "
                    "stream to dead state.",
                    getVBucketId(),
                    cid.to_string());
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }

    folly::assume_unreachable();
}

void DCPBackfillByIdDisk::complete() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_INFO(
                "DCPBackfillByIdDisk::complete(): "
                "({}) backfill ended prematurely as the associated "
                "stream is deleted by the producer",
                getVBucketId());
        transitionState(State::Done);
        return;
    }

    stream->completeOSOBackfill(
            scanCtx->maxSeqno, runtime, scanCtx->diskBytesRead);

    stream->log(spdlog::level::level_enum::debug,
                "({}) Backfill task cid:{} complete",
                vbid,
                cid.to_string());

    transitionState(State::Done);
}
