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

#include "vb_notifiable_task.h"

#include "ep_engine.h"
#include "vbucket.h"
#include <executor/executorpool.h>

#include <climits>

using namespace std::chrono_literals;

VBNotifiableTask::VBNotifiableTask(
        EventuallyPersistentEngine& engine,
        TaskId taskId,
        std::chrono::steady_clock::duration maxChunkDuration)
    : GlobalTask(&engine, taskId),
      queue(engine.getConfiguration().getMaxVbuckets()),
      maxChunkDuration(maxChunkDuration) {
}

bool VBNotifiableTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Start by putting ourselves back to sleep once run() completes.
    // If a new VB is notified (or a VB is re-notified after it is processed in
    // the loop below) then that will cause the task to be re-awoken.
    snooze(INT_MAX);
    // Clear the wakeUpScheduled flag - that allows callers
    // to wake up (re-schedule) this task if new vBuckets have work which
    // need completing.
    wakeUpScheduled.store(false);

    visitPrologue();

    const auto startTime = std::chrono::steady_clock::now();

    Vbid pendingVb;
    while (queue.popFront(pendingVb)) {
        auto vb = engine->getVBucket(Vbid(pendingVb));
        if (vb) {
            visitVBucket(*vb);
        }

        // Yield back to scheduler if we have exceeded the maximum runtime
        // for a single execution.
        auto runtime = std::chrono::steady_clock::now() - startTime;
        if (runtime > maxChunkDuration) {
            wakeUp();
            break;
        }
    }

    visitEpilogue();

    return true;
}

void VBNotifiableTask::notify(Vbid vbid) {
    if (!queue.pushUnique(vbid)) {
        // Return if already in queue, no need to notify the task
        return;
    }

    bool expected = false;

    // Performance: Only wake up the task once (and don't repeatedly try to
    // wake if it's already scheduled to wake) - ExecutorPool::wake() isn't
    // super cheap so avoid it if already pending.
    if (wakeUpScheduled.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(getId());
    }
}
