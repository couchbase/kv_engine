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

CheckpointVisitor::CheckpointVisitor(KVBucketIface* s,
                                     EPStats& st,
                                     std::atomic<bool>& sfin)
    : store(s),
      stats(st),
      removed(0),
      taskStart(std::chrono::steady_clock::now()),
      wasAboveBackfillThreshold(s->isMemUsageAboveBackfillThreshold()),
      stateFinalizer(sfin) {
}

void CheckpointVisitor::visitBucket(const VBucketPtr& vb) {
    removed = vb->checkpointManager->removeClosedUnrefCheckpoints(*vb);
    if (removed > 0) {
        EP_LOG_DEBUG("Removed {} closed unreferenced checkpoints from {}",
                     removed,
                     vb->getId());
    }
    removed = 0;
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
