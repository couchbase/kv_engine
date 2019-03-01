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

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_visitor.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>
#include <memory>

void ClosedUnrefCheckpointRemoverTask::cursorDroppingIfNeeded(void) {
    /**
     * Cursor dropping will commence if one of the following conditions is met:
     * 1. if the total memory used is greater than the upper threshold which is
     * a percentage of the quota, specified by cursor_dropping_upper_mark
     * 2. if the overall checkpoint memory usage goes above a certain % of the
     * bucket quota, specified by cursor_dropping_checkpoint_mem_upper_mark
     *
     * Once cursor dropping starts, it will continue until memory usage is
     * projected to go under the lower threshold, either
     * cursor_dropping_lower_mark or cursor_dropping_checkpoint_mem_lower_mark
     * based on the trigger condition.
     */
    const auto bucketQuota = engine->getConfiguration().getMaxSize();

    const bool aboveLowWatermark =
            stats.getEstimatedTotalMemoryUsed() >= stats.mem_low_wat.load();
    const bool hitCheckpointMemoryThreshold =
            engine->getKVBucket()
                    ->getVBuckets()
                    .getActiveVBucketsTotalCheckpointMemoryUsage() >=
            bucketQuota * (engine->getConfiguration()
                                   .getCursorDroppingCheckpointMemUpperMark() /
                           100);

    if (stats.getEstimatedTotalMemoryUsed() >
                stats.cursorDroppingUThreshold.load() ||
        (aboveLowWatermark && hitCheckpointMemoryThreshold)) {
        size_t amountOfMemoryToClear;
        if (aboveLowWatermark && hitCheckpointMemoryThreshold) {
            // If we were triggered by the fact we hit the low watermark and we
            // are over the threshold of allowed checkpoint memory usage, then
            // try to clear memory down to the lower limit of the allowable
            // memory usage threshold.
            amountOfMemoryToClear =
                    stats.getEstimatedTotalMemoryUsed() -
                    (bucketQuota *
                     (engine->getConfiguration()
                              .getCursorDroppingCheckpointMemLowerMark() /
                      100));
        } else {
            amountOfMemoryToClear = stats.getEstimatedTotalMemoryUsed() -
                                    stats.cursorDroppingLThreshold.load();
        }
        size_t memoryCleared = 0;
        KVBucketIface* kvBucket = engine->getKVBucket();
        // Get a list of active vbuckets sorted by memory usage
        // of their respective checkpoint managers.
        auto vbuckets =
                   kvBucket->getVBuckets().getActiveVBucketsSortedByChkMgrMem();
        for (const auto& it: vbuckets) {
            if (memoryCleared < amountOfMemoryToClear) {
                Vbid vbid = it.first;
                VBucketPtr vb = kvBucket->getVBucket(vbid);
                if (vb) {
                    // Get a list of cursors that can be dropped from the
                    // vbucket's checkpoint manager, so as to unreference
                    // an estimated number of checkpoints.
                    auto cursors =
                            vb->checkpointManager->getListOfCursorsToDrop();
                    for (const auto& cursor : cursors) {
                        if (memoryCleared < amountOfMemoryToClear) {
                            if (engine->getDcpConnMap().handleSlowStream(
                                        vbid, cursor.lock().get())) {
                                auto memoryFreed =
                                        vb->getChkMgrMemUsageOfUnrefCheckpoints();
                                ++stats.cursorsDropped;
                                stats.cursorMemoryFreed += memoryFreed;
                                memoryCleared += memoryFreed;
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

        kvBucket->visitAsync(std::move(pv),
                             "Checkpoint Remover",
                             TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                             /*sleepTime*/ 0,
                             maxExpectedDuration());
    }
    snooze(sleepTime);
    return true;
}
