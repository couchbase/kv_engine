/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucket.hh"
#include "ep_engine.h"
#include "ep.hh"
#include "checkpoint_remover.hh"

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
        removed = vb->checkpointManager.removeClosedUnrefCheckpoints(vb, newCheckpointCreated);
        // If the new checkpoint is created, notify this event to the tap notify IO thread
        // so that it can then signal all paused TAP connections.
        if (newCheckpointCreated) {
            store->getEPEngine().notifyNotificationThread();
        }
        update();
        return false;
    }

    void update() {
        stats.itemsRemovedFromCheckpoints.incr(removed);
        if (removed > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Removed %ld closed unreferenced checkpoints from VBucket %d.\n",
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

bool ClosedUnrefCheckpointRemover::callback(Dispatcher &d, TaskId t) {
    if (available) {
        ++stats.checkpointRemoverRuns;

        available = false;
        shared_ptr<CheckpointVisitor> pv(new CheckpointVisitor(store, stats, &available));
        store->visit(pv, "Checkpoint Remover", &d, Priority::CheckpointRemoverPriority);
    }
    d.snooze(t, sleepTime);
    return true;
}
