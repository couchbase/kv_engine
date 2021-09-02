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
#include <folly/Synchronized.h>
#include <mutex>

class EPStats;
class EventuallyPersistentEngine;

/**
 * Task which destroys and frees checkpoints.
 *
 * This task is not responsible for identifying the checkpoints to destroy,
 * instead the ClosedUnrefCheckpointRemoverTask splices out checkpoints,
 * handing them to this task.
 *
 * In the future, "eager" checkpoint removal may be implemented, directly
 * handing unreferenced checkpoints to this task at the time they become
 * unreferenced.
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

    cb::NonNegativeCounter<size_t> pendingDestructionMemoryUsage;
    // flag that this task has already been notified to avoid repeated
    // executorpool wake calls (not necessarily cheap)
    std::atomic<bool> notified{false};
};

/**
 * Dispatcher job responsible for removing closed unreferenced checkpoints
 * from memory.
 */
class ClosedUnrefCheckpointRemoverTask : public GlobalTask {
public:
    /**
     * Construct ClosedUnrefCheckpointRemover.
     * @param s the store
     * @param st the stats
     */
    ClosedUnrefCheckpointRemoverTask(EventuallyPersistentEngine *e,
                                     EPStats &st, size_t interval) :
        GlobalTask(e, TaskId::ClosedUnrefCheckpointRemoverTask, interval, false),
        engine(e), stats(st), sleepTime(interval), available(true) {}

    /**
     * Attempts to release memory by removing closed/unref checkpoints from all
     * vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @param memToRelease The amount of memory (in bytes) that needs to be
     *   released.
     * @return the amount of memory (in bytes) that was released.
     */
    size_t attemptCheckpointRemoval(size_t memToRelease);

    /**
     * Attempts to free memory by using item expelling from checkpoints from all
     * vbuckets in decreasing checkpoint-mem-usage order.
     *
     * @param memToClear The amount of memory (in bytes) that needs to be
     *   recovered.
     * @return the amount of memory (in bytes) that was recovered.
     */
    size_t attemptItemExpelling(size_t memToClear);

    bool run() override;

    std::string getDescription() const override {
        return "Removing closed unreferenced checkpoints from memory";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Empirical evidence from perf runs suggests this task runs
        // under 250ms 99.99999% of the time.
        return std::chrono::milliseconds(250);
    }

private:
    EventuallyPersistentEngine *engine;
    EPStats                   &stats;
    size_t                     sleepTime;
    std::atomic<bool>          available;
};
