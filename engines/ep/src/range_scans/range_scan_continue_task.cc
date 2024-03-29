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
    : EpTask(bucket.getEPEngine(), TaskId::RangeScanContinueTask),
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
    auto result = scan.prepareToRunOnContinueTask();
    if (result.status == cb::engine_errc::range_scan_more) {
        result.status = scan.continueOnIOThread(
                *bucket.getRWUnderlying(scan.getVBucketId()));
    }

    if (result.cookie) {
        bucket.getEPEngine().notifyIOComplete(*result.cookie, result.status);
    }
}
