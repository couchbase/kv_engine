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

#include "checkpoint_remover.h"

#include "bucket_logger.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include <executor/executorpool.h>

#include <folly/ScopeGuard.h>
#include <phosphor/phosphor.h>
#include <limits>
#include <memory>

CheckpointDestroyerTask::CheckpointDestroyerTask(EventuallyPersistentEngine* e)
    : GlobalTask(e,
                 TaskId::CheckpointDestroyerTask,
                 std::numeric_limits<int>::max() /* sleepTime */) {
}

bool CheckpointDestroyerTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    // sleep forever once done, until notified again
    snooze(std::numeric_limits<int>::max());
    notified.store(false);
    // to hold the lock for as short of a time as possible, swap toDestroy
    // with a temporary list, and destroy the temporary list outside of the lock
    CheckpointList temporary;
    {
        auto handle = toDestroy.lock();
        handle->swap(temporary);
    }
    return true;
}

void CheckpointDestroyerTask::queueForDestruction(CheckpointList&& list) {
    // iterating the list is not ideal, but it should generally be
    // small (in many cases containing a single item), and correctly tracking
    // memory usage is useful.
    for (auto& checkpoint : list) {
        checkpoint->setMemoryTracker(&pendingDestructionMemoryUsage);
    }
    {
        auto handle = toDestroy.lock();
        handle->splice(handle->end(), list);
    }
    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(getId());
    }
}

size_t CheckpointDestroyerTask::getMemoryUsage() const {
    return pendingDestructionMemoryUsage.load();
}

ClosedUnrefCheckpointRemoverTask::ClosedUnrefCheckpointRemoverTask(
        EventuallyPersistentEngine* e, EPStats& st, size_t interval)
    : GlobalTask(e, TaskId::ClosedUnrefCheckpointRemoverTask, interval, false),
      engine(e),
      stats(st),
      sleepTime(interval),
      shouldScanForUnreferencedCheckpoints(
              !e->getCheckpointConfig().isEagerCheckpointRemoval()) {
}

size_t ClosedUnrefCheckpointRemoverTask::attemptCheckpointRemoval(
        size_t memToClear) {
    size_t memoryCleared = 0;
    auto& bucket = *engine->getKVBucket();
    const auto vbuckets = bucket.getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= memToClear) {
            break;
        }

        auto vb = bucket.getVBucket(it.first);
        if (!vb) {
            continue;
        }

        memoryCleared +=
                vb->checkpointManager->removeClosedUnrefCheckpoints().memory;
    }
    return memoryCleared;
}

size_t ClosedUnrefCheckpointRemoverTask::attemptItemExpelling(
        size_t memToClear) {
    size_t memoryCleared = 0;
    auto& kvBucket = *engine->getKVBucket();
    const auto vbuckets = kvBucket.getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= memToClear) {
            break;
        }
        const auto vbid = it.first;
        VBucketPtr vb = kvBucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        const auto expelResult =
                vb->checkpointManager->expelUnreferencedCheckpointItems();
        EP_LOG_DEBUG(
                "Expelled {} unreferenced checkpoint items "
                "from {} "
                "and estimated to have recovered {} bytes.",
                expelResult.count,
                vb->getId(),
                expelResult.memory);
        memoryCleared += expelResult.memory;
    }
    return memoryCleared;
}

size_t ClosedUnrefCheckpointRemoverTask::attemptCursorDropping(
        size_t memToClear) {
    size_t memReleased = 0;
    auto& kvBucket = *engine->getKVBucket();
    const auto vbuckets = kvBucket.getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        const auto vbid = it.first;
        VBucketPtr vb = kvBucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Get a list of cursors that can be dropped from the vbucket's CM and
        // do CursorDrop/CheckpointRemoval until the released target is hit.
        auto& manager = *vb->checkpointManager;
        const auto cursors = manager.getListOfCursorsToDrop();
        for (const auto& cursor : cursors) {
            if (!kvBucket.getEPEngine().getDcpConnMap().handleSlowStream(
                        vb->getId(), cursor.lock().get())) {
                continue;
            }
            ++stats.cursorsDropped;

            // Note: The call remove checkpoints from the CheckpointList and
            // moves them to the Destroyer for deallocation. Thus, here
            // 'memReleased' is an estimation of what is being released, not
            // what has been already released.
            const auto res = manager.removeClosedUnrefCheckpoints();
            memReleased += res.memory;
            EP_LOG_DEBUG(
                    "{} Dropping cursor made {} bytes eligible for "
                    "deallocation",
                    vb->getId(),
                    res.memory);
            if (memReleased >= memToClear) {
                // We hit our release target, all done.
                break;
            }
        }
    }
    return memReleased;
}

bool ClosedUnrefCheckpointRemoverTask::run() {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");

    auto& bucket = *engine->getKVBucket();
    const auto wasAboveBackfillThreshold =
            bucket.isMemUsageAboveBackfillThreshold();

    const auto onReturn = folly::makeGuard([&]() {
        // Wake up any sleeping backfill tasks if the memory usage is lowered
        // below the backfill threshold by checkpoint memory recovery.
        if (wasAboveBackfillThreshold &&
            !bucket.isMemUsageAboveBackfillThreshold()) {
            engine->getDcpConnMap().notifyBackfillManagerTasks();
        }

        snooze(sleepTime);
    });

    const auto memToClear = bucket.getRequiredCheckpointMemoryReduction();
    if (memToClear == 0) {
        return true;
    }

    size_t memRecovered{0};

    // Try full CheckpointRemoval first, across all vbuckets
    if (shouldScanForUnreferencedCheckpoints) {
        memRecovered += attemptCheckpointRemoval(memToClear);
        if (memRecovered >= memToClear) {
            // Recovered enough by CheckpointRemoval, done
            return true;
        }
    }
#if CB_DEVELOPMENT_ASSERTS
    else {
        // if eager checkpoint removal has been configured, calling
        // attemptCheckpointRemoval here should never, ever, find any
        // checkpoints to remove; they should always be removed as soon
        // as they are made eligible, before the lock is released.
        // This is not cheap to verify, as it requires scanning every
        // vbucket, so is only checked if dev asserts are on.
        Expects(attemptCheckpointRemoval(memToClear) == 0);
    }
#endif

    // Try expelling, if enabled.
    // Note: The next call tries to expel from all vbuckets before returning.
    // The reason behind trying expel here is to avoid dropping cursors if
    // possible, as that kicks the stream back to backfilling.
    if (engine->getConfiguration().isChkExpelEnabled()) {
        memRecovered += attemptItemExpelling(memToClear - memRecovered);
        if (memRecovered >= memToClear) {
            // Recovered enough by ItemExpel, done
            return true;
        }
    }

    // More memory to recover, try CursorDrop + CheckpointRemoval
    // Note: Checkpoints made eligible for removal are queued into the Destroyer
    // task for deallocation
    attemptCursorDropping(memToClear - memRecovered);

    return true;
}
