/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "config.h"

#include "checkpoint_remover.h"
#include "ep.h"
#include "ep_engine.h"
#include "vbucket.h"
#include "tapconnmap.h"

/**
 * Remove all the closed unreferenced checkpoints for each vbucket.
 */
class CheckpointVisitor : public VBucketVisitor {
public:

    /**
     * Construct a CheckpointVisitor.
     */
    CheckpointVisitor(EventuallyPersistentStore *s, EPStats &st, bool *sfin)
        : store(s), stats(st), removed(0),
          stateFinalizer(sfin) {}

    bool visitBucket(RCPtr<VBucket> &vb) {
        currentBucket = vb;
        bool newCheckpointCreated = false;
        removed = vb->checkpointManager.removeClosedUnrefCheckpoints(vb,
                                                         newCheckpointCreated);
        // If the new checkpoint is created, notify this event to the
        // corresponding paused TAP & DCP connections.
        if (newCheckpointCreated) {
            store->getEPEngine().getTapConnMap().notifyVBConnections(
                                                                  vb->getId());
            store->getEPEngine().getDcpConnMap().notifyVBConnections(
                                        vb->getId(),
                                        vb->checkpointManager.getHighSeqno());
        }
        update();
        return false;
    }

    void update() {
        stats.itemsRemovedFromCheckpoints.fetch_add(removed);
        if (removed > 0) {
            LOG(EXTENSION_LOG_INFO,
                "Removed %ld closed unreferenced checkpoints from VBucket %d",
                removed, currentBucket->getId());
        }
        removed = 0;
    }

    void complete() {
        if (stateFinalizer) {
            *stateFinalizer = true;
        }
    }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     removed;
    bool                      *stateFinalizer;
};

bool ClosedUnrefCheckpointRemoverTask::run(void) {
    if (available) {
        available = false;
        EventuallyPersistentStore *store = engine->getEpStore();
        shared_ptr<CheckpointVisitor> pv(new CheckpointVisitor(store, stats,
                    &available));
        store->visit(pv, "Checkpoint Remover", NONIO_TASK_IDX,
                     Priority::CheckpointRemoverPriority);
    }
    snooze(sleepTime);
    return true;
}
