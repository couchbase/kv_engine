/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "range_scans/range_scan.h"
#include "vbucket.h"

RangeScanCacheCallback::RangeScanCacheCallback(
        const RangeScan& scan,
        EPBucket& bucket,
        RangeScanDataHandlerIFace& handler)
    : scan(scan), bucket(bucket), handler(handler) {
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
        setStatus(cb::engine_errc::failed);
        return;
    }

    // Check vbucket is valid and active, vb only needed for value scan, but
    // may as well check it for key only so we can cancel if state changes
    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    // Key only scan ends here
    if (scan.isKeyOnly()) {
        handler.handleKey(lookup.getKey().getDocKey());
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto gv = get(*vb, lookup);
    if (gv.getStatus() == cb::engine_errc::success &&
        gv.item->getBySeqno() == lookup.getBySeqno()) {
        handler.handleItem(std::move(gv.item));
        setStatus(cb::engine_errc::key_already_exists);
    } else if (gv.getStatus() == cb::engine_errc::unknown_collection) {
        setStatus(cb::engine_errc::unknown_collection);
    } else {
        // Didn't find a matching value in-memory, continue to disk read
        setStatus(cb::engine_errc::success);
    }
}

RangeScanDiskCallback::RangeScanDiskCallback(const RangeScan& scan,
                                             RangeScanDataHandlerIFace& handler)
    : scan(scan), handler(handler) {
}

void RangeScanDiskCallback::callback(GetValue& val) {
    if (scan.isCancelled()) {
        setStatus(cb::engine_errc::failed);
        return;
    }

    handler.handleItem(std::move(val.item));
    setStatus(cb::engine_errc::success);
}
