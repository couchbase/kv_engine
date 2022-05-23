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
#include "vbucket.h"

#include <phosphor/phosphor.h>

RangeScanContinueTask::RangeScanContinueTask(EPBucket& bucket)
    : GlobalTask(
              &bucket.getEPEngine(), TaskId::RangeScanContinueTask, 0, false),
      bucket(bucket) {
}

bool RangeScanContinueTask::run() {
    auto scan = bucket.takeNextRangeScan();

    // With more than one continue task scheduled, it's possible to run and find
    // the scans have all been taken/handled by other tasks
    if (scan) {
        TRACE_EVENT1("ep-engine/task",
                     "RangeScanContinueTask",
                     "vbid",
                     scan->getVBucketId().get());
        // Attempt to continue the scan
        continueScan(*scan);
    }

    // @todo: consider a reschedule if more work exists, similar to compaction
    return false;
}

void RangeScanContinueTask::continueScan(RangeScan& scan) {
    auto status =
            scan.continueScan(*bucket.getRWUnderlying(scan.getVBucketId()));

    if (status == cb::engine_errc::success) {
        // success is used in all cases where the scan is now complete (even if
        // it was  an error). The scan must now be cancelled
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
        return;
    }

    Expects(status == cb::engine_errc::too_busy);
}