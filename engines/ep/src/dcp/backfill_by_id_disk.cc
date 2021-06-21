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
#include "kvstore.h"

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
        transitionState(backfill_state_done);
        return backfill_finished;
    }
    Vbid vbid = stream->getVBucket();

    const KVStore* kvstore = bucket.getROUnderlying(vbid);
    auto valFilter = getValueFilter(*stream);

    // Create two ranges of keys to have loaded from the scan.
    // 1) system/collection/cid - for the 'metadata', i.e the create/drop marker
    // 2) the range for the collection itself

    // The system event start/end we can make from SystemEventFactory
    auto sysStart = SystemEventFactory::makeCollectionEventKey(cid);
    auto sysEnd = SystemEventFactory::makeCollectionEventKey(uint32_t{cid} + 1);

    // Create the start and end keys for the collection itself
    cb::mcbp::unsigned_leb128<CollectionIDType> start(uint32_t{cid});

    // The end key is the start key + 1, so we clone the start key and increment
    // the last (stop) byte by 1
    std::array<uint8_t,
               cb::mcbp::unsigned_leb128<CollectionIDType>::getMaxSize()>
            end;
    std::copy(start.begin(), start.end(), end.begin());
    end[start.size() - 1]++;

    std::vector<ByIdRange> ranges;
    ranges.emplace_back(ByIdRange{DiskDocKey{sysStart}, DiskDocKey{sysEnd}});
    ranges.emplace_back(ByIdRange{
            DiskDocKey{{start.data(),
                        start.size(),
                        DocKeyEncodesCollectionId::Yes}},
            DiskDocKey{
                    {end.data(), end.size(), DocKeyEncodesCollectionId::Yes}}});

    scanCtx = kvstore->initByIdScanContext(
            std::make_unique<DiskCallback>(stream),
            std::make_unique<CacheCallback>(bucket, stream),
            vbid,
            ranges,
            DocumentFilter::ALL_ITEMS,
            valFilter);
    if (!scanCtx) {
        auto vb = bucket.getVBucket(vbid);
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
        transitionState(backfill_state_done);
    } else {
        bool markerSent = stream->markOSODiskSnapshot(scanCtx->maxSeqno);

        if (markerSent) {
            transitionState(backfill_state_scanning);
        } else {
            transitionState(backfill_state_completing);
        }
    }

    return backfill_success;
}

backfill_status_t DCPBackfillByIdDisk::scan() {
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

    scan_error_t error = kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx));

    if (error == scan_again) {
        return backfill_success;
    }

    transitionState(backfill_state_completing);

    return backfill_success;
}

void DCPBackfillByIdDisk::complete(bool cancelled) {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillByIdDisk::complete(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn; {}",
                getVBucketId(),
                cancelled ? "cancelled" : "finished");
        transitionState(backfill_state_done);
        return;
    }

    stream->completeOSOBackfill();

    auto severity = cancelled ? spdlog::level::level_enum::info
                              : spdlog::level::level_enum::debug;
    stream->log(severity,
                "({}) Backfill task cid:{} {}",
                vbid,
                cid.to_string(),
                cancelled ? "cancelled" : "finished");

    transitionState(backfill_state_done);
}
