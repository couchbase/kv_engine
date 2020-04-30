/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "dcp/backfill_by_id_disk.h"
#include "dcp/active_stream_impl.h"
#include "kv_bucket.h"
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
        transitionState(backfill_state_done);
        return backfill_finished;
    }
    Vbid vbid = stream->getVBucket();

    KVStore* kvstore = bucket.getROUnderlying(vbid);
    ValueFilter valFilter = ValueFilter::VALUES_DECOMPRESSED;
    if (stream->isKeyOnly()) {
        valFilter = ValueFilter::KEYS_ONLY;
    } else {
        if (stream->isCompressionEnabled()) {
            valFilter = ValueFilter::VALUES_COMPRESSED;
        }
    }

    // Create two ranges of keys to have loaded from the scan.
    // 1) system/collection/cid - for the 'metadata', i.e the create/drop marker
    // 2) the range for the collection itself

    // The system event start/end we can make from SystemEventFactory
    auto sysStart = SystemEventFactory::makeCollectionEventKey(cid);
    auto sysEnd = SystemEventFactory::makeCollectionEventKey(cid + 1);

    // Create the start and end keys for the collection itself
    cb::mcbp::unsigned_leb128<CollectionIDType> start(cid);
    cb::mcbp::unsigned_leb128<CollectionIDType> end(cid + 1);

    std::vector<ByIdRange> ranges;
    ranges.push_back({DiskDocKey{sysStart}, DiskDocKey{sysEnd}});
    ranges.push_back({DiskDocKey{{start.data(),
                                  start.size(),
                                  DocKeyEncodesCollectionId::Yes}},
                      DiskDocKey{{end.data(),
                                  end.size(),
                                  DocKeyEncodesCollectionId::Yes}}});

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
        stream->setDead(END_STREAM_BACKFILL_FAIL);
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
        return complete(true);
    }

    Vbid vbid = stream->getVBucket();

    if (!(stream->isActive())) {
        return complete(true);
    }

    KVStore* kvstore = bucket.getROUnderlying(vbid);

    scan_error_t error = kvstore->scan(static_cast<ByIdScanContext&>(*scanCtx));

    if (error == scan_again) {
        return backfill_success;
    }

    transitionState(backfill_state_completing);

    return backfill_success;
}

backfill_status_t DCPBackfillByIdDisk::complete(bool cancelled) {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillByIdDisk::complete(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn; {}",
                getVBucketId(),
                cancelled ? "cancelled" : "finished");
        transitionState(backfill_state_done);
        return backfill_finished;
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

    return backfill_success;
}
