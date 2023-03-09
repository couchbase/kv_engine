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
#include "vbucket.h"

#include <mcbp/codec/range_scan_continue_codec.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/cookie_iface.h>
#include <memcached/range_scan_status.h>
#include <statistics/cbstat_collector.h>

RangeScanDataHandler::RangeScanDataHandler(EventuallyPersistentEngine& engine,
                                           bool keyOnly)
    : engine(engine),
      sendTriggerThreshold(
              engine.getConfiguration().getRangeScanReadBufferSendSize()),
      keyOnly(keyOnly) {
}

RangeScanDataHandler::Status RangeScanDataHandler::checkAndSend(
        CookieIface& cookie) {
    RangeScanDataHandler::Status status{RangeScanDataHandler::Status::OK};
    {
        NonBucketAllocationGuard guard;
        if (cookie.checkThrottle(pendingReadBytes, 0)) {
            status = RangeScanDataHandler::Status::Throttle;
        }
    }

    if (responseBuffer.size() >= sendTriggerThreshold) {
        auto sendStatus = send(cookie);
        // if send is !OK then we no longer care about the throttle status
        if (sendStatus != RangeScanDataHandler::Status::OK) {
            status = sendStatus;
        }
    }

    return status;
}

RangeScanDataHandler::Status RangeScanDataHandler::send(
        CookieIface& cookie, cb::engine_errc status) {
    bool sendSuccess = false;
    {
        cb::mcbp::response::RangeScanContinueResponseExtras extras(keyOnly);
        NonBucketAllocationGuard guard;
        sendSuccess = cookie.sendResponse(
                status,
                extras.getBuffer(),
                {reinterpret_cast<const char*>(responseBuffer.data()),
                 responseBuffer.size()});
    }
    responseBuffer.clear();
    if (!sendSuccess) {
        return RangeScanDataHandler::Status::Disconnected;
    }
    return RangeScanDataHandler::Status::OK;
}

RangeScanDataHandler::Status RangeScanDataHandler::handleKey(
        CookieIface& cookie, DocKey key) {
    pendingReadBytes += key.size();
    cb::mcbp::response::RangeScanContinueKeyPayload::encode(responseBuffer,
                                                            key);
    return checkAndSend(cookie);
}

RangeScanDataHandler::Status RangeScanDataHandler::handleItem(
        CookieIface& cookie, std::unique_ptr<Item> item) {
    pendingReadBytes += item->getKey().size() + item->getNBytes();
    cb::mcbp::response::RangeScanContinueValuePayload::encode(
            responseBuffer, item->toItemInfo(0, false));
    return checkAndSend(cookie);
}

RangeScanDataHandler::Status RangeScanDataHandler::handleStatus(
        CookieIface& cookie, cb::engine_errc status) {
    // The final status includes (when enabled) the read cost
    cookie.addDocumentReadBytes(pendingReadBytes);
    pendingReadBytes = 0;

    RangeScanDataHandler::Status rv{RangeScanDataHandler::Status::OK};

    if (handleStatusCanRespond(status)) {
        // Send a response that will include any buffered data as the value
        rv = send(cookie, status);
    } else {
        // handleStatus cannot respond to this. These are error conditions
        // and particularly not-my-vbucket scanned data cannot be included.
        // Clear the buffer now as this data is no longer required.
        responseBuffer.clear();
    }

    NonBucketAllocationGuard guard;
    // Wake-up front-end to complete the command
    engine.notifyIOComplete(&cookie, status);
    return rv;
}

RangeScanDataHandler::Status RangeScanDataHandler::handleUnknownCollection(
        CookieIface& cookie, uint64_t manifestUid) {
    // For unknown collection, the error context must be generated
    engine.setUnknownCollectionErrorContext(cookie, manifestUid);
    return handleStatus(cookie, cb::engine_errc::unknown_collection);
}

void RangeScanDataHandler::addStats(std::string_view prefix,
                                    const StatCollector& collector) {
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        format_to(std::back_inserter(key), "{}:{}", prefix, statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };

    addStat("send_threshold", sendTriggerThreshold);
}

bool RangeScanDataHandler::handleStatusCanRespond(cb::engine_errc status) {
    switch (cb::rangescan::getContinueHandlingStatus(status)) {
    case cb::rangescan::HandlingStatus::TaskSends:
        return true;
    case cb::rangescan::HandlingStatus::ExecutorSends:
        return false;
    }
    folly::assume_unreachable();
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
        if (!scan.handleKey(lookup.getKey().getDocKey())) {
            // if disconnected "mid" continue for now let the policy be to
            // cancel the scan - any further continue risks being inconsistent
            // as we have no idea as to the point at which the range was lost.
            setScanErrorStatus(cb::engine_errc::range_scan_cancelled);
            return;
        }

        if (scan.areLimitsExceeded()) {
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
        if (!scan.handleItem(std::move(gv.item), RangeScan::Source::Memory)) {
            setScanErrorStatus(cb::engine_errc::range_scan_cancelled);
            return;
        }
        if (scan.areLimitsExceeded()) {
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
    // Calling handleStatus will make the status visible to the client
    if (!scan.handleStatus(status)) {
        StatusCallback<CacheLookup>::setStatus(
                cb::engine_errc::range_scan_cancelled);
    }
}

void RangeScanCacheCallback::setUnknownCollection(uint64_t manifestUid) {
    StatusCallback<CacheLookup>::setStatus(cb::engine_errc::unknown_collection);
    scan.handleUnknownCollection(manifestUid);
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
    if (!scan.handleItem(std::move(val.item), RangeScan::Source::Disk)) {
        setScanErrorStatus(cb::engine_errc::range_scan_cancelled);
        return;
    }
    if (scan.areLimitsExceeded()) {
        yield();
    } else {
        setStatus(cb::engine_errc::success);
    }
}

void RangeScanDiskCallback::setScanErrorStatus(cb::engine_errc status) {
    Expects(status != cb::engine_errc::success);
    StatusCallback<GetValue>::setStatus(status);
    // Calling handleStatus will make the status visible to the client
    if (!scan.handleStatus(status)) {
        StatusCallback<GetValue>::setStatus(
                cb::engine_errc::range_scan_cancelled);
    }
}
