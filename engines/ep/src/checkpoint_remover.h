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
#include <executor/globaltask.h>
#include <executor/notifiable_task.h>
#include <folly/Synchronized.h>
#include <mutex>

class EPStats;
class EventuallyPersistentEngine;

/**
 * Task which destroys and frees checkpoints.
 */
class CheckpointDestroyerTask : public GlobalTask {
public:
    /**
     * Construct a CheckpointDestroyerTask.
     * @param e the engine instance this task is associated with
     */
    CheckpointDestroyerTask(EventuallyPersistentEngine* e);

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
class CheckpointMemRecoveryTask : public NotifiableTask {
public:
    /**
     * @param e the engine
     * @param st the stats
     * @param interval
     * @param removerId of this task's instance, defined in [0, num_removers -1]
     */
    CheckpointMemRecoveryTask(EventuallyPersistentEngine* e,
                              EPStats& st,
                              size_t interval,
                              size_t removerId);

    bool runInner() override;

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
    size_t getSleepTime() const override {
        return sleepTime;
    }

    enum class ReductionRequired : uint8_t { No, Yes };

    /**
     * Attempts to release memory by removing closed/unref checkpoints from all
     * vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @return Whether further memory reduction is required and bytes released
     */
    std::pair<ReductionRequired, size_t> attemptCheckpointRemoval();

    /**
     * Attempts to free memory by using item expelling from checkpoints from all
     * vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @return Whether further memory reduction is required
     */
    ReductionRequired attemptItemExpelling();

    /**
     * Attempts to make checkpoints eligible for removal by dropping cursors
     * from all vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @return Whether further memory reduction is required
     */
    ReductionRequired attemptCursorDropping();

    EventuallyPersistentEngine *engine;
    EPStats                   &stats;
    size_t                     sleepTime;

    // This task is "sharded" by (vbid % numRemovers == removerId), ie each task
    // instance determines what vbuckets to process by picking only vbuckets
    // that verify that equation. Note that removerId is in {0, numRemovers - 1}
    const size_t removerId;
};
