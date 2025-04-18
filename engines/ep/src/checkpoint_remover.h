/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "checkpoint.h"
#include "checkpoint_types.h"
#include "ep_task.h"
#include "utilities/testing_hook.h"
#include <executor/notifiable_task.h>
#include <folly/Synchronized.h>
#include <mutex>

class EPStats;
class EventuallyPersistentEngine;

/**
 * Task which destroys and frees checkpoints.
 */
class CheckpointDestroyerTask : public EpTask {
public:
    /**
     * Construct a CheckpointDestroyerTask.
     * @param e the engine instance this task is associated with
     */
    CheckpointDestroyerTask(EventuallyPersistentEngine& e);

    std::chrono::microseconds maxExpectedDuration() const override {
        // this duration inherited from the replaced checkpoint visitor.
        return std::chrono::milliseconds(50);
    }

    std::string getDescription() const override {
        return "Destroying closed unreferenced checkpoints";
    }

    bool run() override;

    void queueForDestruction(CheckpointList&& list);

    size_t getMemoryUsage() const;

    size_t getNumCheckpoints() const;

    // Test hook executed at ::run() epilogue.
    TestingHook<> runHook;

private:
    folly::Synchronized<CheckpointList, std::mutex> toDestroy;

    cb::AtomicNonNegativeCounter<size_t> pendingDestructionMemoryUsage;
    // flag that this task has already been notified to avoid repeated
    // executorpool wake calls (not necessarily cheap)
    std::atomic<bool> notified{false};
};

/**
 * Dispatcher job responsible for ItemExpel and CursorDrop/CheckpointRemoval
 */
class CheckpointMemRecoveryTask : public EpSignalTask {
public:
    /**
     * @param e the engine
     * @param st the stats
     * @param removerId of this task's instance, defined in [0, num_removers -1]
     */
    CheckpointMemRecoveryTask(EventuallyPersistentEngine& e,
                              EPStats& st,
                              size_t removerId);

    bool runInner(bool manuallyNotified) override;

    std::string getDescription() const override {
        return "CheckpointMemRecoveryTask:" + std::to_string(removerId);
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Empirical evidence from perf runs suggests this task runs
        // under 250ms 99.99999% of the time.
        return std::chrono::milliseconds(250);
    }

    /**
     * @return a vector of vbid/mem pair in descending order by checkpoint
     * memory usage. Note that the task is "sharded", so only the vbuckets that
     * belong to this task's shard are returned. See the removerId member for
     * details on sharding.
     */
    std::vector<std::pair<Vbid, size_t>> getVbucketsSortedByChkMem() const;

protected:
    friend class CheckpointRemoverTest;

    enum class ReductionRequired : uint8_t { No, Yes };
    enum class ReductionTarget { BucketLWM, CheckpointLowerMark };

    /**
     * Utilises target flag to determine amount of checkpoint memory to free:
     *  - Free checkpoint memory to reach chk_lower_mark
     *  - Free as much checkpoint memory as possible so overall
     *    memory reaches LWM.
     *
     * @param target whether to recovery until bucket LWM or chk lower mark
     * @return Return amount of checkpoint memory to free
     */
    size_t getBytesToFree(ReductionTarget target) const;

    /**
     * Attempts to release memory by creating a new checkpoint. That might make
     * the previous/open checkpoint closed/unref and thus eligible for in-place
     * removal. The procedure operates on all vbuckets in decreasing
     * checkpoint-mem-usage order.
     *
     * @param target whether to recovery until bucket LWM or chk lower mark
     * @return Whether further memory reduction is required
     */
    ReductionRequired attemptNewCheckpointCreation(ReductionTarget target);

    /**
     * Attempts to free memory by using item expelling from checkpoints from all
     * vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @param target whether to recovery until bucket LWM or chk lower mark
     * @return Whether further memory reduction is required
     */
    ReductionRequired attemptItemExpelling(ReductionTarget target);

    /**
     * Attempts to make checkpoints eligible for removal by dropping cursors
     * from all vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @param target whether to recovery until bucket LWM or chk lower mark
     * @return Whether further memory reduction is required
     */
    ReductionRequired attemptCursorDropping(ReductionTarget target);

    EPStats& stats;

    // This task is "sharded" by (vbid % numRemovers == removerId), ie each task
    // instance determines what vbuckets to process by picking only vbuckets
    // that verify that equation. Note that removerId is in {0, numRemovers - 1}
    const size_t removerId;
};
