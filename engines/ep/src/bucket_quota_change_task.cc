/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_quota_change_task.h"
#include "ep_engine.h"
#include <executor/executorpool.h>

bool BucketQuotaChangeTask::run() {
    // Snooze forever - if we need to run again we will wake the task, or if a
    // new quota change comes in while we are running then it will wake the task
    snooze(INT_MAX);

    checkForNewQuotaChange();

    if (desiredQuota != getCurrentBucketQuota()) {
        engine->getConfiguration().setMaxSize(desiredQuota);
    }

    finishProcessingQuotaChange();

    // Always run again, the task should live forever as it handles attempted
    // concurrent quota changes
    return true;
}

size_t BucketQuotaChangeTask::getCurrentBucketQuota() const {
    return engine->getConfiguration().getMaxSize();
}

void BucketQuotaChangeTask::notifyNewQuotaChange(size_t desired) {
    // New quota changes enter here. They're only allowed to set the
    // latestDesiredQuota member (and wake the task) as the task will set
    // its own state when it runs to avoid us having to worry about consistency
    // of states.
    latestDesiredQuota = desired;
    ExecutorPool::get()->wake(getId());
}

void BucketQuotaChangeTask::checkForNewQuotaChange() {
    auto newQuotaChangeValue = latestDesiredQuota.load();
    if (newQuotaChangeValue != 0) {
        // Loop compare exchange to ensure that we grab the latest value when
        // we are resetting the atomic which also acts as our notification
        // method
        while (!latestDesiredQuota.compare_exchange_strong(newQuotaChangeValue,
                                                           0)) {
        }

        desiredQuota = newQuotaChangeValue;
    }
}

void BucketQuotaChangeTask::finishProcessingQuotaChange() {
    latestDesiredQuota = 0;
    desiredQuota = 0;
}
