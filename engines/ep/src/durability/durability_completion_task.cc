/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "durability_completion_task.h"

#include "ep_engine.h"
#include "executorpool.h"
#include "vbucket.h"

#include <climits>

using namespace std::chrono_literals;

DurabilityCompletionTask::DurabilityCompletionTask(
        EventuallyPersistentEngine& engine)
    : GlobalTask(&engine, TaskId::DurabilityCompletionTask),
      queue(engine.getConfiguration().getMaxVbuckets()) {
}

bool DurabilityCompletionTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Start by putting ourselves back to sleep once run() completes.
    // If a new VB is notified (or a VB is re-notified after it is processed in
    // the loop below) then that will cause the task to be re-awoken.
    snooze(INT_MAX);
    // Clear the wakeUpScheduled flag - that allows notifySyncWritesToComplete()
    // to wake up (re-schedule) this task if new vBuckets have SyncWrites which
    // need completing.
    wakeUpScheduled.store(false);

    const auto startTime = std::chrono::steady_clock::now();

    Vbid pendingVb;
    while (queue.popFront(pendingVb)) {
        auto vb = engine->getVBucket(Vbid(pendingVb));
        if (vb) {
            vb->processResolvedSyncWrites();
        }

        // Yield back to scheduler if we have exceeded the maximum runtime
        // for a single execution.
        auto runtime = std::chrono::steady_clock::now() - startTime;
        if (runtime > maxChunkDuration) {
            wakeUp();
            break;
        }
    }

    return true;
}

void DurabilityCompletionTask::notifySyncWritesToComplete(Vbid vbid) {
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

const std::chrono::steady_clock::duration
        DurabilityCompletionTask::maxChunkDuration = 25ms;
