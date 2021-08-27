/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "checkpoint_visitor.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"

#include <tuple>

CheckpointVisitor::CheckpointVisitor(KVBucketIface* store,
                                     EPStats& stats,
                                     std::atomic<bool>& stateFinalizer,
                                     size_t memToRelease)
    : store(store),
      stats(stats),
      taskStart(std::chrono::steady_clock::now()),
      wasAboveBackfillThreshold(store->isMemUsageAboveBackfillThreshold()),
      stateFinalizer(stateFinalizer),
      memToRelease(memToRelease) {
    Expects(memToRelease > 0);
}

void CheckpointVisitor::visitBucket(const VBucketPtr& vb) {
    Expects(memToRelease > 0);

    // Get a list of cursors that can be dropped from the vbucket's CM, so
    // as to unreference an estimated number of checkpoints.
    const auto cursors = vb->checkpointManager->getListOfCursorsToDrop();
    for (const auto& cursor : cursors) {
        if (!store->getEPEngine().getDcpConnMap().handleSlowStream(
                    vb->getId(), cursor.lock().get())) {
            continue;
        }
        ++stats.cursorsDropped;

        // @todo MB-48038: The computation here is untouched from the original
        // code. This is all wrong though. 'releasableByRemoval' is the
        // point-in-time value of how much is releasable by removing unref
        // checkpoints, that is not how much has been just made releasable by
        // the latest cursor dropped. As such, 'memToRelease' shouldn't be
        // updated as it is. This is being all fixed in a dedicated follow-up.
        const auto releasableByRemoval =
                vb->getChkMgrMemUsageOfUnrefCheckpoints();
        if (releasableByRemoval >= memToRelease) {
            // Stop dropping cursors if we have made enough checkpoints eligible
            // for removal. This will stop the execution of this visitor.
            memToRelease = 0;
            break;
        } else {
            memToRelease -= releasableByRemoval;
        }
    }

    const auto numItemsRemoved =
            vb->checkpointManager->removeClosedUnrefCheckpoints(*vb);
    if (numItemsRemoved > 0) {
        EP_LOG_DEBUG("Removed {} items from unreferenced checkpoints from {}",
                     numItemsRemoved,
                     vb->getId());
    }
}

void CheckpointVisitor::complete() {
    bool inverse = false;
    stateFinalizer.compare_exchange_strong(inverse, true);

    stats.checkpointRemoverHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - taskStart));

    // Wake up any sleeping backfill tasks if the memory usage is lowered
    // below the backfill threshold as a result of checkpoint removal.
    if (wasAboveBackfillThreshold &&
        !store->isMemUsageAboveBackfillThreshold()) {
        store->getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
    }
}

InterruptableVBucketVisitor::ExecutionState
CheckpointVisitor::shouldInterrupt() {
    // First check if it's time to stop the execution
    if (memToRelease == 0) {
        return ExecutionState::Stop;
    }

    // Rely on the default behaviour otherwise
    return CappedDurationVBucketVisitor::shouldInterrupt();
}

std::function<bool(const Vbid&, const Vbid&)>
CheckpointVisitor::getVBucketComparator() const {
    // Some ep_testsuite failures highlight that accessing vbucket in the VBMap
    // within the comparator may cause issues as the VBMap may change while
    // the comparator is being called. Thus, we build-up a vb-ckpt-mem-usage
    // vector from the current state of VBMap and then we pass it and use it
    // in the comparator.

    const auto& vbMap = store->getVBuckets();
    std::vector<size_t> ckptMemUsage(vbMap.getSize());
    const auto vbuckets = vbMap.getBuckets();
    for (const auto vbid : vbuckets) {
        const auto vb = store->getVBucket(vbid);
        ckptMemUsage[vbid.get()] = vb ? vb->getChkMgrMemUsage() : 0;
    }

    return [ckptMemUsage = std::move(ckptMemUsage)](const Vbid& vbid1,
                                                    const Vbid& vbid2) -> bool {
        return ckptMemUsage.at(vbid1.get()) < ckptMemUsage.at(vbid2.get());
    };
}