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
 * @TODO MB-52264 Update this comment with details on how we go about this.
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
        // @TODO MB-52264 evaluate this
        return std::chrono::seconds(1);
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
     * Cleans up any state set while processing the quota change.
     */
    void finishProcessingQuotaChange();

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

    // The quota change we are currently processing. A value of 0 means that no
    // quota change is currently in progress.
    size_t desiredQuota{0};
};
