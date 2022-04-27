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
#include "vbucket.h"
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

    for (auto checkpoint = temporary.begin(); checkpoint != temporary.end();) {
        pendingDestructionMemoryUsage.fetch_sub((*checkpoint)->getMemUsage());
        checkpoint = temporary.erase(checkpoint);
    }
    return true;
}

void CheckpointDestroyerTask::queueForDestruction(CheckpointList&& list) {
    // iterating the list is not ideal, but it should generally be
    // small (in many cases containing a single item), and correctly tracking
    // memory usage is useful.
    for (auto& checkpoint : list) {
        pendingDestructionMemoryUsage.fetch_add(checkpoint->getMemUsage());
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

CheckpointMemRecoveryTask::CheckpointMemRecoveryTask(
        EventuallyPersistentEngine* e,
        EPStats& st,
        size_t interval,
        size_t removerId)
    : NotifiableTask(e, TaskId::CheckpointMemRecoveryTask, interval),
      engine(e),
      stats(st),
      sleepTime(interval),
      removerId(removerId) {
}

std::pair<CheckpointMemRecoveryTask::ReductionRequired, size_t>
CheckpointMemRecoveryTask::attemptCheckpointRemoval() {
    size_t memReleased = 0;
    auto& bucket = *engine->getKVBucket();
    const auto vbuckets = getVbucketsSortedByChkMem();
    for (const auto& it : vbuckets) {
        const auto vbid = it.first;
        auto vb = bucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Note: The call removes checkpoints from the CheckpointList and the
        // disposer provided here allows to deallocate them in-place.
        memReleased +=
                vb->checkpointManager->removeClosedUnrefCheckpoints().memory;

        if (bucket.getRequiredCheckpointMemoryReduction() == 0) {
            // All done
            return {ReductionRequired::No, memReleased};
        }
    }

    return {ReductionRequired::Yes, memReleased};
}

CheckpointMemRecoveryTask::ReductionRequired
CheckpointMemRecoveryTask::attemptItemExpelling() {
    auto& bucket = *engine->getKVBucket();
    const auto vbuckets = getVbucketsSortedByChkMem();
    for (const auto& it : vbuckets) {
        const auto vbid = it.first;
        auto vb = bucket.getVBucket(vbid);
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
                vbid,
                expelResult.memory);

        if (bucket.getRequiredCheckpointMemoryReduction() == 0) {
            // All done
            return ReductionRequired::No;
        }
    }
    return ReductionRequired::Yes;
}

CheckpointMemRecoveryTask::ReductionRequired
CheckpointMemRecoveryTask::attemptCursorDropping() {
    auto& bucket = *engine->getKVBucket();
    const auto vbuckets = getVbucketsSortedByChkMem();
    for (const auto& it : vbuckets) {
        const auto vbid = it.first;
        auto vb = bucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Get a list of cursors that can be dropped from the vbucket's CM and
        // do CursorDrop/CheckpointRemoval until the released target is hit.
        auto& manager = *vb->checkpointManager;
        const auto cursors = manager.getListOfCursorsToDrop();
        for (const auto& cursor : cursors) {
            if (!engine->getDcpConnMap().handleSlowStream(
                        vbid, cursor.lock().get())) {
                continue;
            }
            ++stats.cursorsDropped;

            // @todo CheckpointRemoval::Eager
            // Note: In eager-mode, the previous call has dropped a cursor and
            // it has also already removed from CM and moved to the Destroyer
            // any checkpoint made unreferenced by the drop.
            // So the next call to CM::removeClosedUnrefCheckpoints() is just
            // expected to be a NOP in eager-mode.
#if CB_DEVELOPMENT_ASSERTS
            Expects(manager.removeClosedUnrefCheckpoints().count == 0);
#else
            // MB-51408: We need to make the call for keeping executing the
            // inner checkpoint creation logic - minimal fix for Neo.
            manager.removeClosedUnrefCheckpoints();
#endif

            if (bucket.getRequiredCheckpointMemoryReduction() == 0) {
                // All done
                return ReductionRequired::No;
            }
        }
    }
    return ReductionRequired::Yes;
}

bool CheckpointMemRecoveryTask::runInner() {
    TRACE_EVENT0("ep-engine/task", "CheckpointMemRecoveryTask");

    auto& bucket = *engine->getKVBucket();

    if (!bucket.isCheckpointMemoryReductionRequired()) {
        return true;
    }

    const auto wasAboveBackfillThreshold =
            bucket.isMemUsageAboveBackfillThreshold();

    const auto onReturn = folly::makeGuard([&]() {
        // Wake up any sleeping backfill tasks if the memory usage is lowered
        // below the backfill threshold by checkpoint memory recovery.
        if (wasAboveBackfillThreshold &&
            !bucket.isMemUsageAboveBackfillThreshold()) {
            engine->getDcpConnMap().notifyBackfillManagerTasks();
        }
    });

    const auto bytesToFree = bucket.getRequiredCheckpointMemoryReduction();

    EP_LOG_DEBUG(
            "{} Triggering checkpoint memory recovery - attempting to free {} "
            "MB",
            getDescription(),
            bytesToFree / (1024 * 1024));

    // @todo CheckpointRemoval::Eager
#if CB_DEVELOPMENT_ASSERTS
    // if eager checkpoint removal has been configured, calling
    // attemptCheckpointRemoval here should never, ever, find any
    // checkpoints to remove; they should always be removed as soon
    // as they are made eligible, before the lock is released.
    // This is not cheap to verify, as it requires scanning every
    // vbucket, so is only checked if dev asserts are on.
    Expects(attemptCheckpointRemoval().second == 0);
#else
    // MB-51408: We need to make the call for keeping executing the inner
    // checkpoint creation logic - minimal fix for Neo.
    attemptCheckpointRemoval();
#endif

    // Try expelling, if enabled.
    // Note: The next call tries to expel from all vbuckets before returning.
    // The reason behind trying expel here is to avoid dropping cursors if
    // possible, as that kicks the stream back to backfilling.
    if (engine->getConfiguration().isChkExpelEnabled()) {
        if (attemptItemExpelling() == ReductionRequired::No) {
            // Recovered enough by ItemExpel, done
            return true;
        }
    }

    // More memory to recover, try CursorDrop + CheckpointRemoval
    attemptCursorDropping();

    return true;
}

std::vector<std::pair<Vbid, size_t>>
CheckpointMemRecoveryTask::getVbucketsSortedByChkMem() const {
    std::vector<std::pair<Vbid, size_t>> res;
    const auto& bucket = *engine->getKVBucket();
    const auto numRemovers = bucket.getCheckpointRemoverTaskCount();
    const auto& vbMap = bucket.getVBuckets();
    for (size_t vbid = 0; vbid < vbMap.getSize(); ++vbid) {
        // Skip if not in shard
        if (vbid % numRemovers != removerId) {
            continue;
        }

        auto vb = vbMap.getBucket(Vbid(vbid));
        if (vb) {
            res.emplace_back(vb->getId(), vb->getChkMgrMemUsage());
        }
    }

    std::sort(res.begin(),
              res.end(),
              [](std::pair<Vbid, size_t> a, std::pair<Vbid, size_t> b) {
                  return a.second > b.second;
              });
    return res;
}
