/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_continue_task.h"

#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>

RangeScanContinueTask::RangeScanContinueTask(EPBucket& bucket)
    : GlobalTask(bucket.getEPEngine(), TaskId::RangeScanContinueTask, 0, false),
      bucket(bucket) {
}

bool RangeScanContinueTask::run() {
    if (auto scan = bucket.takeNextRangeScan(getId())) {
        TRACE_EVENT1("ep-engine/task",
                     "RangeScanContinueTask",
                     "vbid",
                     scan->getVBucketId().get());
        continueScan(*scan);
        // Task must reschedule
        return true;
    }
    // Task can now expire
    return false;
}

void RangeScanContinueTask::continueScan(RangeScan& scan) {
    auto status = scan.prepareToContinueOnIOThread();
    if (status == cb::engine_errc::range_scan_more) {
        status = scan.continueOnIOThread(
                *bucket.getRWUnderlying(scan.getVBucketId()));
    }

    if (status == cb::engine_errc::success) {
        auto vb = bucket.getVBucket(scan.getVBucketId());
        if (vb) {
            dynamic_cast<EPVBucket&>(*vb).completeRangeScan(scan.getUuid());
        }
    } else if (status != cb::engine_errc::range_scan_more) {
        auto vb = bucket.getVBucket(scan.getVBucketId());
        if (vb) {
            // Note this may return a failure, which we are ignoring
            // 1) Scan was explicitly cancelled, so already removed from the set
            // of known scans.
            // 2) vbucket could of been removed and re-added whilst scan was
            // busy so, vbucket has no knowledge
            vb->cancelRangeScan(scan.getUuid(),
                                nullptr /* no cookie */,
                                false /* no schedule*/);
        }
    }
    // else scan can be continued
}