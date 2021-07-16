/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "vb_ready_queue.h"
#include "vbucket_fwd.h"
#include <executor/globaltask.h>
#include <memcached/vbucket.h>

#include <chrono>

/*
 * Abstract base for tasks which run against a given vbucket once notified.
 *
 * Users/subtypes may call:
 *  task.notify(vbid);
 *
 * And expect that the task will call:
 *  visitVBucket(vb);
 *
 * "soon".
 *
 * Multiple vbuckets may be visited in a single run, if multiple have been
 * notified as ready.
 *
 * Repeated calls to notify() _before_ the task runs will not lead to the
 * vbucket being visited repeatedly.
 * Repeated calls to notify() _while_ the task runs _may_ lead to the vbucket
 * being visited repeatedly; subtypes should be prepared to visit a vbucket
 * and not necessarily find work to be done.
 */
class VBNotifiableTask : public GlobalTask {
public:
    bool run() override;

    /**
     * Called from a background task to perform work on a vbucket which
     * has previously been notify()-ed.
     */
    virtual void visitVBucket(VBucket& vb) = 0;

    /**
     * Subtypes should provide a description for the running task.
     *
     * Note: overriden in this class for informative/documentation
     * purposes, this is part of the GlobalTask interface.
     */
    std::string getDescription() const override = 0;

    /**
     * Notifies the task that the given vBucket has work to be done.
     * If the given vBucket isn't already pending, then will wake up the task
     * for it to run.
     */
    void notify(Vbid vbid);

    std::chrono::microseconds maxExpectedDuration() const override {
        // Task shouldn't run much longer than maxChunkDuration; given we yield
        // after that duration - however _could_ exceed a bit given we check
        // the duration on each vBucket. As such add a 2x margin of error.
        return std::chrono::duration_cast<std::chrono::microseconds>(
                2 * maxChunkDuration);
    }

protected:
    /**
     * Construct a VBNotifiableTask.
     *
     * @param engine the engine this task is associated with
     * @param taskId task ID as defined in tasks.def.h
     * @param maxChunkDuration maximum time the task should run for before
     * yielding
     */
    VBNotifiableTask(EventuallyPersistentEngine& engine,
                     TaskId taskId,
                     std::chrono::steady_clock::duration maxChunkDuration);

    /**
     * Called once per task run, immediately before visiting vBuckets.
     *
     * This is not included in maxChunkDuration, so must not take significant
     * amount of time.
     */
    virtual void visitPrologue(){};

    /**
     * Called once per task run, after all notified vbuckets have been visited.
     *
     * This is not included in maxChunkDuration, so must not take significant
     * amount of time.
     */
    virtual void visitEpilogue(){};

private:
    /**
     * Queue of unique vBuckets to process.
     */
    VBReadyQueue queue;

    /**
     * Flag which is used to check if a wakeup has already been scheduled for
     * this task.
     */
    std::atomic<bool> wakeUpScheduled{false};

    /// Maximum duration this task should execute for before yielding back to
    /// the ExecutorPool (to allow other tasks to run).
    const std::chrono::steady_clock::duration maxChunkDuration;
};
