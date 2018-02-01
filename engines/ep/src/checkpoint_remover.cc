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

#include "checkpoint.h"
#include "checkpoint_remover.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>
#include <platform/make_unique.h>

/**
 * Remove all the closed unreferenced checkpoints for each vbucket.
 */
class CheckpointVisitor : public VBucketVisitor {
public:

    /**
     * Construct a CheckpointVisitor.
     */
    CheckpointVisitor(KVBucketIface* s, EPStats& st, std::atomic<bool>& sfin)
        : store(s),
          stats(st),
          removed(0),
          taskStart(ProcessClock::now()),
          wasHighMemoryUsage(s->isMemoryUsageTooHigh()),
          stateFinalizer(sfin) {
    }

    void visitBucket(VBucketPtr &vb) override {
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
            LOG(EXTENSION_LOG_INFO,
                "Removed %ld closed unreferenced checkpoints from VBucket %d",
                removed, vb->getId());
        }
        removed = 0;
    }

    void complete() override {
        bool inverse = false;
        stateFinalizer.compare_exchange_strong(inverse, true);

        stats.checkpointRemoverHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        ProcessClock::now() - taskStart));

        // Wake up any sleeping backfill tasks if the memory usage is lowered
        // below the high watermark as a result of checkpoint removal.
        if (wasHighMemoryUsage && !store->isMemoryUsageTooHigh()) {
            store->getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
        }
    }

private:
    KVBucketIface* store;
    EPStats                   &stats;
    size_t                     removed;
    ProcessClock::time_point   taskStart;
    bool                       wasHighMemoryUsage;
    std::atomic<bool>         &stateFinalizer;
};

void ClosedUnrefCheckpointRemoverTask::cursorDroppingIfNeeded(void) {
    /**
     * Cursor dropping will commence only if the total memory used is
     * greater than the upper threshold which is a percentage of the
     * quota, specified by cursor_dropping_upper_mark. Once cursor
     * dropping starts, it will continue until memory usage is projected
     * to go under the lower threshold which is a percentage of the quota,
     * specified by cursor_dropping_lower_mark.
     */
    if (stats.getEstimatedTotalMemoryUsed() >
        stats.cursorDroppingUThreshold.load()) {
        size_t amountOfMemoryToClear = stats.getEstimatedTotalMemoryUsed() -
                                       stats.cursorDroppingLThreshold.load();
        size_t memoryCleared = 0;
        KVBucketIface* kvBucket = engine->getKVBucket();
        // Get a list of active vbuckets sorted by memory usage
        // of their respective checkpoint managers.
        auto vbuckets =
                   kvBucket->getVBuckets().getActiveVBucketsSortedByChkMgrMem();
        for (const auto& it: vbuckets) {
            if (memoryCleared < amountOfMemoryToClear) {
                uint16_t vbid = it.first;
                VBucketPtr vb = kvBucket->getVBucket(vbid);
                if (vb) {
                    // Get a list of cursors that can be dropped from the
                    // vbucket's checkpoint manager, so as to unreference
                    // an estimated number of checkpoints.
                    std::vector<std::string> cursors =
                            vb->checkpointManager->getListOfCursorsToDrop();
                    std::vector<std::string>::iterator itr = cursors.begin();
                    for (; itr != cursors.end(); ++itr) {
                        if (memoryCleared < amountOfMemoryToClear) {
                            if (engine->getDcpConnMap().handleSlowStream(vbid,
                                                                        *itr))
                            {
                                ++stats.cursorsDropped;
                                memoryCleared +=
                                      vb->getChkMgrMemUsageOfUnrefCheckpoints();
                            }
                        } else {
                            break;
                        }
                    }
                }
            } else {    // memoryCleared >= amountOfMemoryToClear
                break;
            }
        }
    }
}

bool ClosedUnrefCheckpointRemoverTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");
    bool inverse = true;
    if (available.compare_exchange_strong(inverse, false)) {
        cursorDroppingIfNeeded();
        KVBucketIface* kvBucket = engine->getKVBucket();
        auto pv =
                std::make_unique<CheckpointVisitor>(kvBucket, stats, available);
        // p99.999 is 15ms
        auto maxExpectedDuration = std::chrono::milliseconds(15);
        kvBucket->visit(std::move(pv),
                        "Checkpoint Remover",
                        TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                        /*sleepTime*/ 0,
                        maxExpectedDuration);
    }
    snooze(sleepTime);
    return true;
}
