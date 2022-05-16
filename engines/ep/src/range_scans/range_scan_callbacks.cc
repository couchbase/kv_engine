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
#include "objectregistry.h"
#include "range_scans/range_scan.h"
#include "vbucket.h"

#include <mcbp/codec/range_scan_continue_codec.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/server_cookie_iface.h>

void RangeScanDataHandler::checkAndSend(const CookieIface& cookie) {
    // @todo: set the "size" from configuration and also test various sizes
    // For now a "page-size" multiple is probably fine. This value controls
    // roughly how much a scan can read into memory, but we can be over this
    // e.g. if we were to load a 20Mib value...
    if (responseBuffer.size() >= 8192) {
        send(cookie);
    }
}

void RangeScanDataHandler::send(const CookieIface& cookie,
                                cb::engine_errc status) {
    {
        NonBucketAllocationGuard guard;
        engine.getServerApi()->cookie->send_response(
                cookie,
                status,
                {reinterpret_cast<const char*>(responseBuffer.data()), responseBuffer.size()});
    }
    responseBuffer.clear();
}

void RangeScanDataHandler::handleKey(const CookieIface& cookie, DocKey key) {
    cb::mcbp::response::RangeScanContinueKeyPayload::encode(responseBuffer, key);
    checkAndSend(cookie);
}

void RangeScanDataHandler::handleItem(const CookieIface& cookie,
                                      std::unique_ptr<Item> item) {
    cb::mcbp::response::RangeScanContinueValuePayload::encode(
            responseBuffer, item->toItemInfo(0, false));
    checkAndSend(cookie);
}

void RangeScanDataHandler::handleStatus(const CookieIface& cookie,
                                        cb::engine_errc status) {
    // send the 'final' status along with any outstanding data
    // The status must be range_scan_{more/complete} or an error
    Expects(status != cb::engine_errc::success);
    send(cookie, status);

    NonBucketAllocationGuard guard;
    // And execution is complete
    engine.getServerApi()->cookie->execution_complete(cookie);
}

RangeScanCacheCallback::RangeScanCacheCallback(RangeScan& scan,
                                               EPBucket& bucket)
    : scan(scan), bucket(bucket) {
}

// Do a get and restrict the collections lock scope to just these checks.
GetValue RangeScanCacheCallback::get(VBucket& vb, CacheLookup& lookup) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, get a handle on the collection manifest
    auto cHandle = vb.lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        return GetValue{nullptr, cb::engine_errc::unknown_collection};
    }
    return vb.getInternal(nullptr,
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

    if (scan.skipItem()) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    } else if (scan.isTotalLimitReached()) {
        // end the scan. Using setScanErrorStatus ensures this status is
        // attached to any output from the scan.
        setScanErrorStatus(cb::engine_errc::range_scan_complete);
        return;
    }

    // Key only scan ends here
    if (scan.isKeyOnly()) {
        scan.handleKey(lookup.getKey().getDocKey());
        if (scan.areLimitsExceeded()) {
            yield();
        } else {
            // call setStatus so the scan doesn't try the value lookup. This
            // status is not visible to the client
            setStatus(cb::engine_errc::key_already_exists);
        }
        return;
    }

    auto gv = get(*vb, lookup);
    if (gv.getStatus() == cb::engine_errc::success &&
        gv.item->getBySeqno() == lookup.getBySeqno()) {
        // RangeScans do not transmit xattrs
        gv.item->removeXattrs();
        scan.handleItem(std::move(gv.item), RangeScan::Source::Memory);
        if (scan.areLimitsExceeded()) {
            yield();
        } else {
            // call setStatus so the scan doesn't try the value lookup. This
            // status is not visible to the client
            setStatus(cb::engine_errc::key_already_exists);
        }
    } else if (gv.getStatus() == cb::engine_errc::unknown_collection) {
        setScanErrorStatus(cb::engine_errc::unknown_collection);
    } else {
        // Didn't find a matching value in-memory, continue to disk read
        setStatus(cb::engine_errc::success);
    }
}

void RangeScanCacheCallback::setScanErrorStatus(cb::engine_errc status) {
    Expects(status != cb::engine_errc::success);
    StatusCallback<CacheLookup>::setStatus(status);
    // Calling handleStatus will make the status visible to the client
    scan.handleStatus(status);
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
    scan.handleStatus(status);
}
