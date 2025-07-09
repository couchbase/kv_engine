/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_create_task.h"

#include "bucket_logger.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "range_scans/range_scan_callbacks.h"
#include "range_scans/range_scan_types.h"

#include <phosphor/phosphor.h>

RangeScanCreateTask::RangeScanCreateTask(
        EPBucket& bucket,
        CookieIface& cookie,
        std::unique_ptr<RangeScanDataHandlerIFace> handler,
        const cb::rangescan::CreateParameters& params,
        std::unique_ptr<RangeScanCreateToken> scanData)
    : EpTask(bucket.getEPEngine(), TaskId::RangeScanCreateTask),
      bucket(bucket),
      vbid(params.vbid),
      start(makeStartStoredDocKey(params.cid, params.start)),
      end(makeEndStoredDocKey(params.cid, params.end)),
      handler(std::move(handler)),
      cookie(cookie),
      keyOnly(params.keyOnly),
      snapshotReqs(params.snapshotReqs),
      samplingConfig(params.samplingConfig),
      scanData(std::move(scanData)),
      name(params.name) {
    // They must be the same collection
    Expects(this->start.getCollectionID() == this->end.getCollectionID());
}

bool RangeScanCreateTask::run() {
    TRACE_EVENT1("ep-engine/task", "RangeScanCreateTask", "vbid", vbid.get());

    auto status = cb::engine_errc::success;
    try {
        std::tie(status, scanData->uuid) = create();
    } catch (const cb::engine_error& e) {
        // Failure induced by KV will have logged, e.g. KVStore open failures.
        // Failure induced by the user (e.g. empty range) has no need to log
        engine->setErrorContext(cookie, e.what());
        status = cb::engine_errc(e.code().value());
        // create failure, clear out cookie (this object will free the data
        // which was "there")
        engine->clearEngineSpecific(cookie);
    }

    // On success, release the scanData. The frontend thread will retrieve and
    // handle destruction and free
    if (status == cb::engine_errc::success) {
        scanData.release();
    }

    engine->notifyIOComplete(&cookie, status);
    return false; // done, no reschedule required
}

std::pair<cb::engine_errc, cb::rangescan::Id> RangeScanCreateTask::create() {
    auto vb = bucket.getVBucket(vbid);
    if (!vb) {
        return {cb::engine_errc::not_my_vbucket, {}};
    }
    // RangeScan constructor will throw if the snapshot cannot be opened or is
    // not usable for the scan (does not meet requirements)
    auto scan = std::make_shared<RangeScan>(bucket,
                                            *vb,
                                            DiskDocKey{start},
                                            DiskDocKey{end},
                                            std::move(handler),
                                            cookie,
                                            keyOnly,
                                            snapshotReqs,
                                            samplingConfig,
                                            std::move(name));
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    return {epVb.addNewRangeScan(scan), scan->getUuid()};
}

StoredDocKey RangeScanCreateTask::makeStartStoredDocKey(
        CollectionID cid, cb::rangescan::KeyView key) {
    auto sKey = StoredDocKey{key.getKeyView(), cid};
    if (!key.isInclusive()) {
        sKey.append(0);
    }
    return sKey;
}

StoredDocKey RangeScanCreateTask::makeEndStoredDocKey(
        CollectionID cid, cb::rangescan::KeyView key) {
    auto sKey = StoredDocKey{key.getKeyView(), cid};
    if (!key.isInclusive()) {
        // If back 'byte' is > 0
        if (sKey.back()) {
            // subtract 1 and extend the key with 255. sKey now covers up-to but
            // not including the input key
            --sKey.back();
            while (sKey.size() < MaxCollectionsKeyLen) {
                sKey.append(std::numeric_limits<uint8_t>::max());
            }
        } else {
            // else pop the 0
            sKey.pop_back();
        }
    }
    return sKey;
}
