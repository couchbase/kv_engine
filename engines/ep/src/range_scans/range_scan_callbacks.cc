/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_callbacks.h"

#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "item.h"
#include "objectregistry.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_types.h"
#include "vbucket.h"

#include <mcbp/codec/range_scan_continue_codec.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/cookie_iface.h>
#include <statistics/cbstat_collector.h>

RangeScanDataHandler::RangeScanDataHandler(EventuallyPersistentEngine& engine,
                                           bool keyOnly)
    : sendTriggerThreshold(
              engine.getConfiguration().getRangeScanReadBufferSendSize()),
      keyOnly(keyOnly) {
}

RangeScanDataHandler::Status RangeScanDataHandler::getScanStatus(
        size_t bufferedSize) {
    if (bufferedSize >= sendTriggerThreshold) {
        return RangeScanDataHandler::Status::ExceededBufferLimit;
    }
    return RangeScanDataHandler::Status::OK;
}

std::unique_ptr<RangeScanContinueResult>
RangeScanDataHandler::continuePartialOnFrontendThread() {
    // lock and move the current buffered data.
    return scannedData.withLock([this](auto& ls) {
        return std::make_unique<RangeScanContinueResultPartial>(
                std::move(ls.responseBuffer), keyOnly);
    });
}

std::unique_ptr<RangeScanContinueResult>
RangeScanDataHandler::continueMoreOnFrontendThread() {
    // lock and move the current buffered data and readBytes
    return scannedData.withLock([this](auto& ls) {
        auto readBytes = ls.pendingReadBytes;
        ls.pendingReadBytes = 0;
        return std::make_unique<RangeScanContinueResultMore>(
                std::move(ls.responseBuffer), readBytes, keyOnly);
    });
}

std::unique_ptr<RangeScanContinueResult>
RangeScanDataHandler::completeOnFrontendThread() {
    // lock and move the current buffered data and readBytes
    return scannedData.withLock([this](auto& ls) {
        auto readBytes = ls.pendingReadBytes;
        ls.pendingReadBytes = 0;
        return std::make_unique<RangeScanContinueResultComplete>(
                std::move(ls.responseBuffer), readBytes, keyOnly);
    });
}

std::unique_ptr<RangeScanContinueResult>
RangeScanDataHandler::cancelOnFrontendThread() {
    // lock and move the current buffered data and readBytes
    return scannedData.withLock([this](auto& ls) {
        auto readBytes = ls.pendingReadBytes;
        ls.pendingReadBytes = 0;
        return std::make_unique<RangeScanContinueResultCancelled>(
                std::move(ls.responseBuffer), readBytes, keyOnly);
    });
}

RangeScanDataHandler::Status RangeScanDataHandler::handleKey(DocKey key) {
    auto locked = scannedData.lock();
    locked->pendingReadBytes += key.size();

    // ensure the buffer is sized for the configured buffer size, this scan will
    // read keys upto this size and we can avoid alloc/memcpy as we push back
    locked->responseBuffer.reserve(sendTriggerThreshold);

    cb::mcbp::response::RangeScanContinueKeyPayload::encode(
            locked->responseBuffer, key);
    return getScanStatus(locked->responseBuffer.size());
}

RangeScanDataHandler::Status RangeScanDataHandler::handleItem(
        std::unique_ptr<Item> item) {
    auto locked = scannedData.lock();
    locked->pendingReadBytes += item->getKey().size() + item->getNBytes();

    // ensure the buffer is sized for the configured buffer size, this scan will
    // read keys upto this size and we can avoid alloc/memcpy as we push back
    locked->responseBuffer.reserve(sendTriggerThreshold);

    cb::mcbp::response::RangeScanContinueValuePayload::encode(
            locked->responseBuffer, item->toItemInfo(0, false));
    return getScanStatus(locked->responseBuffer.size());
}

void RangeScanDataHandler::addStats(std::string_view prefix,
                                    const StatCollector& collector) {
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        fmt::format_to(std::back_inserter(key), "{}:{}", prefix, statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };

    addStat("send_threshold", sendTriggerThreshold);
}

RangeScanCacheCallback::RangeScanCacheCallback(RangeScan& scan,
                                               EPBucket& bucket)
    : scan(scan), bucket(bucket) {
}

// Do a get and restrict the collections lock scope to just these checks.
GetValue RangeScanCacheCallback::get(
        VBucketStateLockRef vbStateLock,
        VBucket& vb,
        Collections::VB::CachingReadHandle& cHandle,
        CacheLookup& lookup) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, so requires a handle on the collection
    // manifest
    return vb.getInternal(vbStateLock,
                          nullptr,
                          bucket.getEPEngine(),
                          /*options*/ NONE,
                          scan.isKeyOnly() ? VBucket::GetKeyOnly::Yes
                                           : VBucket::GetKeyOnly::No,
                          cHandle);
}

void RangeScanCacheCallback::callback(CacheLookup& lookup) {
    // cancelled can be set by the client, timeout or bucket shutdown
    if (scan.isCancelled()) {
        setScanErrorStatus(cb::engine_errc::range_scan_cancelled);
        return;
    }

    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setScanErrorStatus(cb::engine_errc::not_my_vbucket);
        return;
    }
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!scan.isVbucketScannable(*vb)) {
        setScanErrorStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    // For key or value scan, collection lock can be obtained and checked
    auto cHandle = vb->lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        // This scan is done - collection was dropped.
        setUnknownCollection(cHandle.getManifestUid());
        return;
    }

    if (scan.skipItem()) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    // Key only scan ends here
    if (scan.isKeyOnly()) {
        scan.handleKey(lookup.getKey().getDocKey());

        if (scan.shouldScanYield()) {
            yield();
        } else {
            // call setStatus so the scan doesn't try the value lookup. This
            // status is not visible to the client
            setStatus(cb::engine_errc::key_already_exists);
        }
        return;
    }

    auto gv = get(rlh, *vb, cHandle, lookup);
    if (gv.getStatus() == cb::engine_errc::success &&
        gv.item->getBySeqno() == lookup.getBySeqno()) {
        // RangeScans do not transmit xattrs
        gv.item->removeXattrs();
        scan.handleItem(std::move(gv.item), RangeScan::Source::Memory);

        if (scan.shouldScanYield()) {
            yield();
        } else {
            // call setStatus so the scan doesn't try the value lookup. This
            // status is not visible to the client
            setStatus(cb::engine_errc::key_already_exists);
        }
    } else {
        // Didn't find a matching value in-memory, continue to disk read
        setStatus(cb::engine_errc::success);
    }
}

void RangeScanCacheCallback::setScanErrorStatus(cb::engine_errc status) {
    Expects(status != cb::engine_errc::success);
    StatusCallback<CacheLookup>::setStatus(status);
    scan.cancelOnIOThread(status);
}

void RangeScanCacheCallback::setUnknownCollection(uint64_t manifestUid) {
    StatusCallback<CacheLookup>::setStatus(cb::engine_errc::unknown_collection);
    scan.setUnknownCollectionManifestUid(manifestUid);
    scan.cancelOnIOThread(cb::engine_errc::unknown_collection);
}

RangeScanDiskCallback::RangeScanDiskCallback(RangeScan& scan) : scan(scan) {
}

void RangeScanDiskCallback::callback(GetValue& val) {
    if (scan.isCancelled()) {
        setScanErrorStatus(cb::engine_errc::range_scan_cancelled);
        return;
    }

    // RangeScans do not transmit xattrs
    val.item->removeXattrs();
    scan.handleItem(std::move(val.item), RangeScan::Source::Disk);

    if (scan.shouldScanYield()) {
        yield();
    } else {
        setStatus(cb::engine_errc::success);
    }
}

void RangeScanDiskCallback::setScanErrorStatus(cb::engine_errc status) {
    Expects(status != cb::engine_errc::success);
    StatusCallback<GetValue>::setStatus(status);
    scan.cancelOnIOThread(status);
}
