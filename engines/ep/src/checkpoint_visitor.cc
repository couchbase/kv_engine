/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "checkpoint_visitor.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"

CheckpointVisitor::CheckpointVisitor(KVBucketIface* s,
                                     EPStats& st,
                                     std::atomic<bool>& sfin,
                                     ExpelItems expelItems)
    : store(s),
      stats(st),
      removed(0),
      taskStart(std::chrono::steady_clock::now()),
      wasHighMemoryUsage(s->isMemoryUsageTooHigh()),
      stateFinalizer(sfin),
      expelItems(expelItems) {
}

void CheckpointVisitor::visitBucket(const VBucketPtr& vb) {
    bool newCheckpointCreated = false;
    removed = vb->checkpointManager->removeClosedUnrefCheckpoints(
            *vb, newCheckpointCreated);
    // If the new checkpoint is created, notify this event to the
    // corresponding paused DCP connections.
    if (newCheckpointCreated) {
        store->getEPEngine().getDcpConnMap().notifyVBConnections(
                vb->getId(), vb->checkpointManager->getHighSeqno());
    }

    stats.itemsRemovedFromCheckpoints.fetch_add(removed);
    if (removed > 0) {
        EP_LOG_DEBUG("Removed {} closed unreferenced checkpoints from {}",
                     removed,
                     vb->getId());
    }
    removed = 0;

    if (expelItems == ExpelItems::Yes) {
        auto expelled =
                vb->checkpointManager->expelUnreferencedCheckpointItems();
        EP_LOG_DEBUG("Expelled {} unreferenced checkpoint items from {}",
                     expelled,
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
    // below the high watermark as a result of checkpoint removal.
    if (wasHighMemoryUsage && !store->isMemoryUsageTooHigh()) {
        store->getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
    }
}
