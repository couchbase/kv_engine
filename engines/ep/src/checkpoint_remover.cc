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

// isReductionInCheckpointMemoryNeeded() wants to determine if checkpoint
// expelling and/or cursor dropping should be invoked, if true a calculated
// memory reduction 'target' is also returned.
//
// The following diagram depicts the bucket memory where # is the max_size and
// the labelled vertical lines each show a threshold that is used in deciding if
// memory reduction is needed. Also depicted are two of the 'live' statistics
// that are also used in deciding if memory reduction is needed.
//
// 0                                                                      #
// ├───────────────────┬───────────────┬───┬───────────┬──┬─┬────┬────┬───┤
// │                   │               │   │           │  │ │    │    │   │
// │                   │               │   │           │  │ │    │    │   │
// └───────────────────▼───────────────▼───▼───────────▼──▼─▼────▼────▼───┘
//                     A               B   X           L  Y C    H    D
//
// The thresholds used with the current defaults:
//
//   A = cursor_dropping_checkpoint_mem_lower_mark (30%)
//   B = cursor_dropping_checkpoint_mem_upper_mark (50%)
//   C = cursor_dropping_lower_mark (80%)
//   D = cursor_dropping_upper_mark (95%)
//   L = mem_low_watermark (75%)
//   H = mem_high_watermark (85%)
//
// The two live statistics:
//
//   X = checkpoint memory used, the value returned from
//       VBucketMap::getVBucketsTotalCheckpointMemoryUsage()
//   Y = 'mem_used' i.e. the value of stats.getEstimatedTotalMemoryUsed()
//
// Memory reduction will commence if any of the following conditions are met:
// 1) If checkpoint memory usage (X) is greater than
//    cursor_dropping_checkpoint_mem_upper_mark and 'mem_used' (Y) is greater
//    than mem_low_watermark (L).
// 2) If 'mem_used' (Y) is greater than cursor_dropping_upper_mark (D)
//
// If case 1 is the trigger this function will return X - A as the target amount
// to free.
//
// If case 2 is the trigger this function will return Y - C as the target amount
// to free.
//
// When memory reduction is required two different techniques are applied:
// 1) First checkpoint expelling. If that technique does not 'free' the required
//    target, then a second technique is applied.
// 2) The second technique is cursor dropping.
//
// At the end of the memory reduction if the target is still not reached no
// further action occurs. The next invocation of the
// ClosedUnrefCheckpointRemoverTask will start the process again.
//
std::pair<bool, size_t>
ClosedUnrefCheckpointRemoverTask::isReductionInCheckpointMemoryNeeded() const {
    const auto& config = engine->getConfiguration();
    const auto bucketQuota = config.getMaxSize();
    const auto memUsed = stats.getEstimatedTotalMemoryUsed();

    const auto vBucketChkptMemSize =
            engine->getKVBucket()
                    ->getVBuckets()
                    .getVBucketsTotalCheckpointMemoryUsage();

    const auto chkptMemLimit =
            (bucketQuota * config.getCursorDroppingCheckpointMemUpperMark()) /
            100;

    const bool hitCheckpointMemoryThreshold =
            vBucketChkptMemSize >= chkptMemLimit;

    const bool aboveLowWatermark = memUsed >= stats.mem_low_wat.load();

    const bool ckptMemExceedsCheckpointMemoryThreshold =
            aboveLowWatermark && hitCheckpointMemoryThreshold;

    const bool memUsedExceedsCursorDroppingUpperMark =
            memUsed > stats.cursorDroppingUThreshold.load();

    auto toMB = [](size_t bytes) { return bytes / (1024 * 1024); };
    if (memUsedExceedsCursorDroppingUpperMark ||
        ckptMemExceedsCheckpointMemoryThreshold) {
        size_t amountOfMemoryToClear;

        if (ckptMemExceedsCheckpointMemoryThreshold) {
            // Calculate the lower percentage of quota and subtract that from
            // the current checkpoint memory size to obtain the 'target'.
            amountOfMemoryToClear =
                    vBucketChkptMemSize -
                    ((bucketQuota *
                      config.getCursorDroppingCheckpointMemLowerMark()) /
                     100);
            EP_LOG_INFO(
                    "Triggering memory recovery as checkpoint_memory ({} MB) "
                    "exceeds cursor_dropping_checkpoint_mem_upper_mark ({}%, "
                    "{} MB). Attempting to free {} MB of memory.",
                    toMB(vBucketChkptMemSize),
                    config.getCursorDroppingCheckpointMemUpperMark(),
                    toMB(chkptMemLimit),
                    toMB(amountOfMemoryToClear));

        } else {
            amountOfMemoryToClear =
                    memUsed - stats.cursorDroppingLThreshold.load();
            EP_LOG_INFO(
                    "Triggering memory recovery as mem_used ({} MB) "
                    "exceeds cursor_dropping_upper_mark ({}%, {} MB). "
                    "current checkpoint consumption is {} MB "
                    "Attempting to free {} MB of memory.",
                    toMB(memUsed),
                    config.getCursorDroppingUpperMark(),
                    toMB(stats.cursorDroppingUThreshold.load()),
                    toMB(vBucketChkptMemSize),
                    toMB(amountOfMemoryToClear));
        }
        // Memory recovery is required.
        return std::make_pair(true, amountOfMemoryToClear);
    }
    // Memory recovery is not required.
    return std::make_pair(false, 0);
}

size_t ClosedUnrefCheckpointRemoverTask::attemptMemoryRecovery(
        MemoryRecoveryMechanism mechanism, size_t amountOfMemoryToClear) {
    size_t memoryCleared = 0;
    KVBucketIface* kvBucket = engine->getKVBucket();
    // Get a list of vbuckets sorted by memory usage
    // of their respective checkpoint managers.
    auto vbuckets = kvBucket->getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= amountOfMemoryToClear) {
            break;
        }
        Vbid vbid = it.first;
        VBucketPtr vb = kvBucket->getVBucket(vbid);
        if (!vb) {
            continue;
        }
        switch (mechanism) {
        case MemoryRecoveryMechanism::checkpointExpel: {
            auto expelResult =
                    vb->checkpointManager->expelUnreferencedCheckpointItems();
            EP_LOG_DEBUG(
                    "Expelled {} unreferenced checkpoint items "
                    "from {} "
                    "and estimated to have recovered {} bytes.",
                    expelResult.expelCount,
                    vb->getId(),
                    expelResult.estimateOfFreeMemory);
            memoryCleared += expelResult.estimateOfFreeMemory;
            break;
        }
        case MemoryRecoveryMechanism::cursorDrop: {
            // Get a list of cursors that can be dropped from the
            // vbucket's checkpoint manager, so as to unreference
            // an estimated number of checkpoints.
            auto cursors = vb->checkpointManager->getListOfCursorsToDrop();
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
                } else { // memoryCleared >= amountOfMemoryToClear
                    break;
                }
            }
        } // case cursorDrop
        } // switch (mechanism)
    }
    return memoryCleared;
}

bool ClosedUnrefCheckpointRemoverTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");
    bool inverse = true;
    if (available.compare_exchange_strong(inverse, false)) {
        bool shouldReduceMemory{false};
        size_t amountOfMemoryToClear{0};
        size_t amountOfMemoryRecovered{0};

        std::tie(shouldReduceMemory, amountOfMemoryToClear) =
                isReductionInCheckpointMemoryNeeded();
        if (shouldReduceMemory) {
            // Try expelling first, if enabled
            if (engine->getConfiguration().isChkExpelEnabled()) {
                amountOfMemoryRecovered = attemptMemoryRecovery(
                        MemoryRecoveryMechanism::checkpointExpel,
                        amountOfMemoryToClear);
            }
            // If still need to recover more memory, drop cursors
            if (amountOfMemoryToClear > amountOfMemoryRecovered) {
                attemptMemoryRecovery(
                        MemoryRecoveryMechanism::cursorDrop,
                        amountOfMemoryToClear - amountOfMemoryRecovered);
            }
        }
        KVBucketIface* kvBucket = engine->getKVBucket();

        auto pv =
                std::make_unique<CheckpointVisitor>(kvBucket, stats, available);

        // Empirical evidence from perf runs shows that 99.9% of "Checkpoint
        // Remover" task should complete under 50ms
        const auto maxExpectedDurationForVisitorTask =
                std::chrono::milliseconds(50);

        kvBucket->visitAsync(std::move(pv),
                             "Checkpoint Remover",
                             TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                             maxExpectedDurationForVisitorTask);
    }
    snooze(sleepTime);
    return true;
}
