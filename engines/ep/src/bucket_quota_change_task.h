/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <executor/globaltask.h>

#include <climits>

/**
 * The purpose of the BucketQuotaChangeTask is to change the Bucket quota
 * gracefully such that we avoid QoS issues due to TMP_FAIL of new mutations
 * when we decrease the Bucket quota. This task deals with Bucket quota
 * increases too to avoid attempted concurrent changes in Bucket quota in either
 * direction.
 *
 * To deal with Bucket quota increases, the tasks needs to only increase the
 * Bucket quota and all associated values. We do not need to wait for memory
 * recovery. To deal with Bucket quota reductions the task operates in 2 phases:
 *
 * Phase 1:
 *
 * Set the new watermark values to reduce memory usage without causing out of
 * memory errors.
 *
 * 1) Set the storage quota to the new value (to kick off any background memory
 *    reclamation)
 * 2) Set the checkpoint manager watermarks (kicks in memory recovery if
 *    necessary but does not reduce the quota so new mutations are not any more
 *    likely to be blocked due ot the quota).
 * 3) Decrease our memory determined backfill limit
 * 4) Set the low and high watermark values
 * 5) Wake the ItemPager/ExpiryPager to recover memory from the HashTable
 *
 * We perform steps 1-4 in this particular order to avoid over-reclaiming memory
 * from the HashTables if possible. After completing Phase 1, the task moves
 * immediately to Phase 2 (i.e. if memory is already low enough to support the
 * new quota then the new quota is applied immediately).
 *
 * Phase 2:
 *
 * Wait for memory usage to be within acceptable limits for the new quota,
 * then enforece the new quota.
 *
 * 6) Wait until memory is below the high watermark
 * 7) Enforce the new quota by changing the config and stats values
 *
 */
class BucketQuotaChangeTask : public GlobalTask {
public:
    explicit BucketQuotaChangeTask(EventuallyPersistentEngine& e)
        : GlobalTask(&e,
                     TaskId::CheckpointMemRecoveryTask,
                     /*initialSleepTime*/ INT_MAX,
                     /*completeBeforeShutdown*/ false) {
    }

    bool run() override;

    std::string getDescription() const override {
        return "Changing bucket quota";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::milliseconds(1);
    }

    /**
     * Notify the task that there is a new quota change to process. Wakes the
     * task to run again.
     *
     * @param desired The desired new quota
     */
    void notifyNewQuotaChange(size_t desired);

private:
    size_t getCurrentBucketQuota() const;

    /**
     * Checks latestDesiredQuota for a new value, and updates quota change state
     * accordingly.
     */
    void checkForNewQuotaChange();

    /**
     * Phase 1:
     * Steps 1-5:
     * Set watermark values for the desiredQuota to start memory reclamation.
     *
     * @param desiredQuota new quota
     */
    void prepareToReduceMemoryUsage(size_t desiredQuota);

    /**
     * Phase 2:
     * Step 6-7:
     * Check if memory usage is acceptable and set the new quota if possible
     *
     * @param desiredQuota new quota
     * @return true if the new quota was set
     */
    bool setNewQuotaIfMemoryUsageAcceptable(size_t desiredQuota);

    /**
     * Phase 2:
     * Step 7:
     * Set the actual quota value to the desired value.
     *
     * @param desiredQuota
     */
    void setDesiredQuota(size_t desiredQuota);

    /**
     * Check if memory usage is such that we can set the new quota
     *
     * @return true if below the high watermark
     */
    bool checkMemoryState();

    /**
     * Cleans up any state set while processing the quota change.
     */
    void finishProcessingQuotaChange();

    // State of the current quota change
    enum class ChangeState {
        ApplyingWatermarkChanges,
        WaitingForMemoryReclamation,
        Done,
    };

    // latestDesiredQuota is set when scheduling this task. It is used to
    // determine if a new quota change has come in whilst processing the
    // state change. It is used from multiple threads and so must be atomic.
    // A value of 0 means that either no change is in progress, or the
    // BucketQuotaChangeTask is already operating on the latest value.
    std::atomic<size_t> latestDesiredQuota{0};

    /**
     * All the state required to change the quota. Below state is not atomic
     * is it will only be operated on by the thread running the task.
     */
    ChangeState state{ChangeState::Done};

    // Watermarks values for the old quota. They are stored
    size_t previousLowWatermark{0};
    size_t previousHighWatermark{0};
};
