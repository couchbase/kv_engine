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

#include "bucket_logger.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"

#include <executor/executorpool.h>

bool BucketQuotaChangeTask::run() {
    // Snooze forever - if we need to run again we will wake the task, or if a
    // new quota change comes in while we are running then it will wake the task
    snooze(INT_MAX);

    checkForNewQuotaChange();

    const auto desiredQuota = engine->getEpStats().desiredMaxDataSize.load();
    if (desiredQuota >= getCurrentBucketQuota()) {
        // Increases in quota (or no change at all) do not run the risk of
        // TMP_OOMs if we blow the quota so they're easy to deal with here.
        // We still want to deal with them within the task as they should
        // halt any currently processing quota reduction and so we need to tidy
        // up that state here.
        if (desiredQuota > getCurrentBucketQuota()) {
            // setMaxDataSize sets all the appropriate values to change the
            // quota.
            engine->setMaxDataSize(desiredQuota);
        }
        finishProcessingQuotaChange();
        return true;
    }

    // We are reducing the quota, and we may have either started doing that
    // already, or be about to.
    switch (state) {
    case ChangeState::ApplyingWatermarkChanges:
        EP_LOG_DEBUG("Applying watermark changes for desired quota:{}",
                     desiredQuota);

        // Read our current low and high watermarks so that we can reset their
        // values if we abort this quota change. Read from EPStats rather than
        // configuration as configuration will genereally default to 0 for
        // auto-configuration and we need the actual values here as we don't
        // store the auto-configuration multiples anywhere.
        previousLowWatermark = engine->getEpStats().mem_low_wat;
        previousHighWatermark = engine->getEpStats().mem_high_wat;

        // To reduce memory usage we need to adjust watermarks to start
        // recovering memory
        prepareToReduceMemoryUsage(desiredQuota);

        if (!setNewQuotaIfMemoryUsageAcceptable(desiredQuota)) {
            EP_LOG_INFO(
                    "Quota change to {} could not be applied immediately "
                    "as a reduction in memory usage from {} to {} is required",
                    desiredQuota,
                    engine->getEpStats().getEstimatedTotalMemoryUsed(),
                    engine->getEpStats().mem_high_wat);

            state = ChangeState::WaitingForMemoryReclamation;
            // Check again soon
            snooze(engine->getConfiguration()
                           .getBucketQuotaChangeTaskPollInterval());
        }
        break;
    case ChangeState::WaitingForMemoryReclamation:
        if (!setNewQuotaIfMemoryUsageAcceptable(desiredQuota)) {
            EP_LOG_INFO(
                    "Quota change to {} in progress. Current memory usage"
                    " {} must reach {} before quota change can progress",
                    desiredQuota,
                    engine->getEpStats().getEstimatedTotalMemoryUsed(),
                    engine->getEpStats().mem_high_wat);

            // Check again soon
            snooze(engine->getConfiguration()
                           .getBucketQuotaChangeTaskPollInterval());
        }
        break;
    case ChangeState::Done:
        // Nothing to do
        break;
    }

    // Always reschedule, task should live forever as it is used to deal with
    // concurrent quota change requests
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

bool BucketQuotaChangeTask::setNewQuotaIfMemoryUsageAcceptable(
        size_t desiredQuota) {
    if (checkMemoryState()) {
        setDesiredQuota(desiredQuota);
        finishProcessingQuotaChange();
        return true;
    }

    return false;
}

void BucketQuotaChangeTask::checkForNewQuotaChange() {
    size_t newQuotaChangeValue = latestDesiredQuota;
    if (newQuotaChangeValue != 0) {
        const auto desiredQuota =
                engine->getEpStats().desiredMaxDataSize.load();
        bool quotaChangeWasInProgress = desiredQuota != 0;
        if (quotaChangeWasInProgress) {
            EP_LOG_INFO(
                    "Aborting quota change to {} to start quota change "
                    "to {}",
                    desiredQuota,
                    newQuotaChangeValue);

            // We have some new quota change to process, stop what we're doing
            // and start making the new change...
            engine->getConfiguration().setMemLowWat(previousLowWatermark);
            engine->getConfiguration().setMemHighWat(previousHighWatermark);
        } else {
            EP_LOG_INFO("Starting quota change from {} to {}",
                        getCurrentBucketQuota(),
                        newQuotaChangeValue);
        }

        // Loop compare exchange to ensure that we grab the latest value when
        // we are resetting the atomic which also acts as our notification
        // method
        while (!latestDesiredQuota.compare_exchange_strong(newQuotaChangeValue,
                                                           0)) {
        }

        state = ChangeState::ApplyingWatermarkChanges;
        engine->getEpStats().desiredMaxDataSize = newQuotaChangeValue;
        previousLowWatermark = 0;
        previousHighWatermark = 0;
    }
}

void BucketQuotaChangeTask::finishProcessingQuotaChange() {
    EP_LOG_INFO("Completed quota change to: {}",
                engine->getEpStats().desiredMaxDataSize);
    state = ChangeState::Done;
    engine->getEpStats().desiredMaxDataSize = 0;
    previousLowWatermark = 0;
    previousHighWatermark = 0;
}

void BucketQuotaChangeTask::prepareToReduceMemoryUsage(size_t desiredQuota) {
    // Step 1
    engine->configureStorageMemoryForQuota(desiredQuota);

    // Step 2
    auto checkpointLowWatRatio =
            engine->getKVBucket()->getCheckpointMemoryRecoveryLowerMark();
    auto checkpointUpperWatRatio =
            engine->getKVBucket()->getCheckpointMemoryRecoveryUpperMark();

    auto currentQuota = getCurrentBucketQuota();
    double changeRatio = static_cast<double>(currentQuota) /
                         static_cast<double>(desiredQuota);

    auto desiredCheckpointLowWat = checkpointLowWatRatio * changeRatio;
    auto desiredCheckpointUpperWat = checkpointUpperWatRatio * changeRatio;

    engine->getKVBucket()->setCheckpointMemoryRecoveryLowerMark(
            desiredCheckpointLowWat);
    engine->getKVBucket()->setCheckpointMemoryRecoveryUpperMark(
            desiredCheckpointUpperWat);
    engine->getKVBucket()->verifyCheckpointMemoryState();

    // Step 3
    engine->getDcpConnMap().updateMaxRunningBackfills(desiredQuota);

    // Step 4
    engine->configureMemWatermarksForQuota(desiredQuota);

    // Step 5 (also triggers expiry pager if applicable for ephemeral
    // fail_new_data)
    engine->getKVBucket()->checkAndMaybeFreeMemory();
}

void BucketQuotaChangeTask::setDesiredQuota(size_t desiredQuota) {
    engine->getConfiguration().setMaxSize(desiredQuota);
    engine->getEpStats().setMaxDataSize(desiredQuota);

    engine->getEpStats().setLowWaterMark(engine->getEpStats().mem_low_wat);
    engine->getEpStats().setHighWaterMark(engine->getEpStats().mem_high_wat);

    engine->getKVBucket()->autoConfigCheckpointMaxSize();

    engine->updateArenaAllocThresholdForQuota(desiredQuota);
}

bool BucketQuotaChangeTask::checkMemoryState() {
    return engine->getEpStats().getPreciseTotalMemoryUsed() <
           engine->getEpStats().mem_high_wat;
}
