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
#include "range_scans/range_scan_types.h"

#include <phosphor/phosphor.h>

RangeScanCreateTask::RangeScanCreateTask(
        EPBucket& bucket,
        Vbid vbid,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        RangeScanDataHandlerIFace& handler,
        const CookieIface& cookie,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::unique_ptr<RangeScanCreateData> scanData)
    : GlobalTask(&bucket.getEPEngine(), TaskId::RangeScanCreateTask, 0, false),
      bucket(bucket),
      vbid(vbid),
      start(makeStoredDocKey(cid, start)),
      end(makeStoredDocKey(cid, end)),
      handler(handler),
      cookie(cookie),
      keyOnly(keyOnly),
      snapshotReqs(snapshotReqs),
      scanData(std::move(scanData)) {
    // They must be the same collection
    Expects(this->start.getCollectionID() == this->end.getCollectionID());
}

bool RangeScanCreateTask::run() {
    TRACE_EVENT1("ep-engine/task", "RangeScanCreateTask", "vbid", vbid.get());

    auto status = cb::engine_errc::success;
    try {
        std::tie(status, scanData->uuid) = create();
    } catch (const cb::engine_error& e) {
        EP_LOG_WARN(
                "RangeScanCreateTask::run() failed to create RangeScan "
                "exception:{}",
                e.what());
        status = cb::engine_errc(e.code().value());
    }

    // On success, release the scanData. The frontend thread will retrieve and
    // handle destruction and free
    if (status == cb::engine_errc::success) {
        scanData.release();
    }

    engine->notifyIOComplete(&cookie, status);
    return false; // done, no reschedule required
}

std::pair<cb::engine_errc, cb::rangescan::Id> RangeScanCreateTask::create()
        const {
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
                                            handler,
                                            cookie,
                                            keyOnly,
                                            snapshotReqs);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    return {epVb.addNewRangeScan(scan), scan->getUuid()};
}

StoredDocKey RangeScanCreateTask::makeStoredDocKey(CollectionID cid,
                                                   cb::rangescan::KeyView key) {
    return StoredDocKey{key.getKeyView(), cid};
}
