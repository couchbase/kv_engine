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

CheckpointDestroyerTask::CheckpointDestroyerTask(EventuallyPersistentEngine& e)
    : EpTask(e,
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

    runHook();

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

size_t CheckpointDestroyerTask::getNumCheckpoints() const {
    return toDestroy.lock()->size();
}

CheckpointMemRecoveryTask::CheckpointMemRecoveryTask(
        EventuallyPersistentEngine& e, EPStats& st, size_t removerId)
    : EpSignalTask(e, TaskId::CheckpointMemRecoveryTask, INT_MAX),
      stats(st),
      removerId(removerId) {
}

CheckpointMemRecoveryTask::ReductionRequired
CheckpointMemRecoveryTask::attemptNewCheckpointCreation(
        ReductionTarget target) {
    auto& bucket = *engine->getKVBucket();
    const auto vbuckets = getVbucketsSortedByChkMem();
    for (const auto& it : vbuckets) {
        const auto vbid = it.first;
        auto vb = bucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        // The call might possibly create a new checkpoint and make the old one
        // closed/unref. If that happens, the old checkpoint is detached from
        // the CM and given to the Destroyer for deallocation.
        vb->checkpointManager->maybeCreateNewCheckpoint();

        // We might have created a new checkpoint and moved cursors into it at
        // checkpoint begin (the empty item).
        // We need to notify the related streams of that, DCP items_remaining
        // stats wouldn't drop to zero otherwise (as in that state surely a
        // cursor has at least the checkpoint_start item to process).
        //
        // @todo MB-53778: Currently we potentially notify unnecessary streams.
        //   The side effect isn't expected to cause any major issue. An idle
        //   DCP Producer might be unnecessarily woken up, but immediately put
        //   again to sleep at the first step(). Less of a problem for busy DCP
        //   Producers: they are in their step() loop anyway, so any attempt of
        //   notification is actually a NOP.
        //
        // Note: Predicating this call on whether a new checkpoint was created
        //   doesn't prevent unnecessary notification. That's because checkpoint
        //   creation doesn't imply that some cursor has jumped into the new
        //   open checkpoint.
        bucket.notifyReplication(vbid, queue_op::empty);

        if (getBytesToFree(target) == 0) {
            // All done
            return ReductionRequired::No;
        }
    }

    return ReductionRequired::Yes;
}

CheckpointMemRecoveryTask::ReductionRequired
CheckpointMemRecoveryTask::attemptItemExpelling(ReductionTarget target) {
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

        if (getBytesToFree(target) == 0) {
            // All done
            return ReductionRequired::No;
        }
    }
    return ReductionRequired::Yes;
}

CheckpointMemRecoveryTask::ReductionRequired
CheckpointMemRecoveryTask::attemptCursorDropping(ReductionTarget target) {
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
            // The call drops the cursor and also removes from CM any checkpoint
            // made unreferenced by the drop. Removed checkpoints are passed to
            // the Destroyer for deallocation.
            if (!engine->getDcpConnMap().handleSlowStream(
                        vbid, cursor.lock().get())) {
                continue;
            }
            ++stats.cursorsDropped;

            if (getBytesToFree(target) == 0) {
                // All done
                return ReductionRequired::No;
            }
        }
    }
    return ReductionRequired::Yes;
}

size_t CheckpointMemRecoveryTask::getBytesToFree(ReductionTarget target) const {
    auto& bucket = *engine->getKVBucket();

    size_t memToBucketLWM = 0;
    auto currMemUsage = stats.getEstimatedTotalMemoryUsed();
    auto memLWM = stats.mem_low_wat.load();
    if (target == ReductionTarget::BucketLWM && currMemUsage > memLWM) {
        memToBucketLWM = currMemUsage - memLWM;
    }

    // Return max of (chk_mem_usage above lower_mark) and (mem_usage above LWM)
    return std::max(bucket.getRequiredCMMemoryReduction(), memToBucketLWM);
}

bool CheckpointMemRecoveryTask::runInner(bool) {
    TRACE_EVENT0("ep-engine/task", "CheckpointMemRecoveryTask");

    auto& bucket = *engine->getKVBucket();

    ReductionTarget target = ReductionTarget::CheckpointLowerMark;
    // EphemeralMemRecoveryTask can trigger this task when HWM is exceeded.
    // In that case we want to try free until bucket LWM instead of lower_mark.
    if (engine->getConfiguration().isEphemeralMemRecoveryEnabled() &&
        stats.getEstimatedTotalMemoryUsed() >= stats.mem_high_wat) {
        target = ReductionTarget::BucketLWM;
    }

    const auto bytesToFree = getBytesToFree(target);
    if (bytesToFree == 0) {
        return true;
    }

    EP_LOG_DEBUG("{} Triggering CM memory recovery - attempting to free {} MB",
                 getDescription(),
                 bytesToFree / (1_MiB));

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

    if (attemptNewCheckpointCreation(target) == ReductionRequired::No) {
        // Recovered enough, done
        return true;
    }

    // Try expelling, if enabled.
    // Note: The next call tries to expel from all vbuckets before returning.
    // The reason behind trying expel here is to avoid dropping cursors if
    // possible, as that kicks the stream back to backfilling.
    if (engine->getConfiguration().isChkExpelEnabled()) {
        if (attemptItemExpelling(target) == ReductionRequired::No) {
            // Recovered enough by ItemExpel, done
            return true;
        }
    }

    // More memory to recover, try CursorDrop + CheckpointRemoval
    if (attemptCursorDropping(target) == ReductionRequired::No) {
        return true;
    }

    // Run again if we were not able to free to checkpoint lower mark.
    if (target == ReductionTarget::CheckpointLowerMark) {
        snooze(0.1);
    }

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

    std::ranges::sort(res,
                      [](std::pair<Vbid, size_t> a, std::pair<Vbid, size_t> b) {
                          return a.second > b.second;
                      });
    return res;
}
