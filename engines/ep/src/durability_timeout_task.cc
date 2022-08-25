/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "durability_timeout_task.h"
#include "vbucket.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

VBucketSyncWriteTimeoutTask::VBucketSyncWriteTimeoutTask(Taskable& taskable,
                                                         VBucket& vBucket)
    : GlobalTask(taskable, TaskId::DurabilityTimeoutTask, INT_MAX, false),
      vBucket(vBucket),
      vbid(vBucket.getId()) {
}

std::string VBucketSyncWriteTimeoutTask::getDescription() const {
    return fmt::format("Expired SyncWrite callback for {}", vbid);
}

std::chrono::microseconds VBucketSyncWriteTimeoutTask::maxExpectedDuration()
        const {
    // Calibrated to observed p99.9 duration in system tests.
    return std::chrono::milliseconds{10};
}

bool VBucketSyncWriteTimeoutTask::run() {
    // Inform the vBucket that it should process (and abort) any pending
    // SyncWrites which have timed out as of now.
    vBucket.processDurabilityTimeout(std::chrono::steady_clock::now());

    // Note that while run() returns 'true' here (to re-schedule based on
    // ::waketime), there's no explicit snooze() in this method. This is
    // because processDurabilityTimeout() above will update the snooze time
    // (via SyncWriteScheduledExpiry::{update,cancel}NextExpiryTime) during
    // processing.
    return true;
}
