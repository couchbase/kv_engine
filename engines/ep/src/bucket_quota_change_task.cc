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
        finishProcessingQuotaChange(cb::engine_errc::success);
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
        if (const auto ret = prepareToReduceMemoryUsage(desiredQuota);
            ret != cb::engine_errc::success) {
            EP_LOG_ERR(
                    "Aborting quota change to {} - Couldn't prepare to reduce "
                    "memory usage - Configuration reverted to initial values",
                    desiredQuota);
            finishProcessingQuotaChange(ret);
            break;
        }

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
        finishProcessingQuotaChange(cb::engine_errc::success);
        return true;
    }

    return false;
}

void BucketQuotaChangeTask::checkForNewQuotaChange() {
    size_t newQuotaChangeValue = latestDesiredQuota;

    if (newQuotaChangeValue == 0) {
        return;
    }

    const auto desiredQuota = engine->getEpStats().desiredMaxDataSize.load();
    bool quotaChangeWasInProgress = desiredQuota != 0;
    if (quotaChangeWasInProgress) {
        EP_LOG_INFO(
                "Aborting quota change to {} to start quota change "
                "to {}",
                desiredQuota,
                newQuotaChangeValue);

        // We have some new quota change to process, stop what we're doing
        // and start making the new change...
        auto& stats = engine->getEpStats();
        stats.setLowWaterMark(previousLowWatermark);
        stats.setHighWaterMark(previousHighWatermark);
        engine->updateLegacyMemWatermarksConfiguration();
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

void BucketQuotaChangeTask::finishProcessingQuotaChange(cb::engine_errc code) {
    EP_LOG_INFO("Quota change completed: desiredQuota:{} status:{}",
                engine->getEpStats().desiredMaxDataSize,
                to_string(code));
    state = ChangeState::Done;
    engine->getEpStats().desiredMaxDataSize = 0;
    previousLowWatermark = 0;
    previousHighWatermark = 0;
}

cb::engine_errc BucketQuotaChangeTask::prepareToReduceMemoryUsage(
        size_t desiredQuota) {
    Expects(desiredQuota > 0);

    const auto initQuota = getCurrentBucketQuota();
    auto& bucket = *engine->getKVBucket();
    const auto initCkptLower = bucket.getCheckpointMemoryRecoveryLowerMark();
    const auto initCkptUpper = bucket.getCheckpointMemoryRecoveryUpperMark();

    const auto cleanupHandler = [&]() -> void {
        engine->configureStorageMemoryForQuota(initQuota);
        bucket.setCheckpointMemoryRecoveryLowerMark(initCkptLower);
        bucket.setCheckpointMemoryRecoveryUpperMark(initCkptUpper);
        bucket.getKVStoreScanTracker().updateMaxRunningScans(
                initQuota,
                engine->getConfiguration().getRangeScanKvStoreScanRatio(),
                engine->getConfiguration()
                        .getDcpBackfillInProgressPerConnectionLimit());
        engine->configureMemWatermarksForQuota(initQuota);
    };

    // Step 1
    engine->configureStorageMemoryForQuota(desiredQuota);

    // Step 2
    if (const auto ret = prepareToReduceCheckpointMemoryUsage(desiredQuota);
        ret != cb::engine_errc::success) {
        cleanupHandler();
        return ret;
    }

    // Step 3
    bucket.getKVStoreScanTracker().updateMaxRunningScans(
            desiredQuota,
            engine->getConfiguration().getRangeScanKvStoreScanRatio(),
            engine->getConfiguration()
                    .getDcpBackfillInProgressPerConnectionLimit());

    // Step 4
    engine->configureMemWatermarksForQuota(desiredQuota);

    // Step 5 (also triggers expiry pager if applicable for ephemeral
    // fail_new_data)
    bucket.checkAndMaybeFreeMemory();

    return cb::engine_errc::success;
}

cb::engine_errc BucketQuotaChangeTask::prepareToReduceCheckpointMemoryUsage(
        size_t desiredQuota) {
    // We need to possibly trigger checkpoint mem-recovery as if CMQuota was
    // reduced immediately but before actually reducing it.
    // Given that we can't touch the CMQuota yet, then we reduce the checkpoint
    // upper/lower marks for making mem-recovery behave as it would by reducing
    // the CMQuota.

    const auto initQuota = getCurrentBucketQuota();
    // First, save initial values for reapplying them later at quota-reduction
    // completion.
    auto& bucket = *engine->getKVBucket();
    previousCkptLowerMark = bucket.getCheckpointMemoryRecoveryLowerMark();
    previousCkptUpperMark = bucket.getCheckpointMemoryRecoveryUpperMark();

    if (desiredQuota >= initQuota) {
        EP_LOG_ERR(
                "BucketQuotaChangeTask::prepareToReduceCheckpointMemoryUsage: "
                "desiredQuota ({}) >= initQuota ({})",
                desiredQuota,
                initQuota);
        return cb::engine_errc::invalid_arguments;
    }
    Expects(initQuota > 0);
    // Note: desiredQuota < currentQuota implies changeRatio in [0.0, 1.0)
    const float changeRatio = static_cast<float>(desiredQuota) / initQuota;
    // Which implies tempMarks in [0.0, originalMarks)
    const auto tempCheckpointLowerMark = previousCkptLowerMark * changeRatio;
    const auto tempCheckpointUpperMark = previousCkptUpperMark * changeRatio;

    if (const auto ret = bucket.setCheckpointMemoryRecoveryLowerMark(
                tempCheckpointLowerMark);
        ret != cb::engine_errc::success) {
        return ret;
    }
    if (const auto ret = bucket.setCheckpointMemoryRecoveryUpperMark(
                tempCheckpointUpperMark);
        ret != cb::engine_errc::success) {
        return ret;
    }
    // Trigger checkpoint mem recovery if necessary
    bucket.verifyCheckpointMemoryState();

    return cb::engine_errc::success;
}

void BucketQuotaChangeTask::setDesiredQuota(size_t desiredQuota) {
    auto& config = engine->getConfiguration();
    config.setMaxSize(desiredQuota);

    auto& stats = engine->getEpStats();
    stats.setMaxDataSize(desiredQuota);

    engine->configureMemWatermarksForQuota(desiredQuota);

    auto& bucket = *engine->getKVBucket();
    bucket.autoConfigCheckpointMaxSize();
    bucket.setCheckpointMemoryRecoveryUpperMark(previousCkptUpperMark);
    bucket.setCheckpointMemoryRecoveryLowerMark(previousCkptLowerMark);

    engine->updateArenaAllocThresholdForQuota(desiredQuota);

    engine->setDcpConsumerBufferRatio(config.getDcpConsumerBufferRatio());
}

bool BucketQuotaChangeTask::checkMemoryState() {
    return engine->getEpStats().getPreciseTotalMemoryUsed() <
           engine->getEpStats().mem_high_wat;
}
