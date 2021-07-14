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

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_visitor.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"

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
// ├───────────────────┬───────────────┬───┬───────────┬──┬──────┬────────┤
// │                   │               │   │           │  │      │        │
// │                   │               │   │           │  │      │        │
// └───────────────────▼───────────────▼───▼───────────▼──▼──────▼────────┘
//                     A               B   X           L  Y      H
//
// The thresholds used with the current defaults:
//
//   A = cursor_dropping_checkpoint_mem_lower_mark (30%)
//   B = cursor_dropping_checkpoint_mem_upper_mark (50%)
//   L = mem_low_watermark (75%)
//   H = mem_high_watermark (85%)
//
// The two live statistics:
//
//   X = checkpoint memory used, the value returned by
//       stats.getEstimatedCheckpointMemUsage()
//   Y = 'mem_used' i.e. the value of stats.getEstimatedTotalMemoryUsed()
//
// Memory reduction will commence if checkpoint memory usage (X) is greater than
// cursor_dropping_checkpoint_mem_upper_mark OR 'mem_used' (Y) is greater than
// mem_low_watermark (L).
//
// If memory reduction triggers then this function will return X - A as the
// target amount to free.
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

    const auto checkpointMemoryUsage = stats.getEstimatedCheckpointMemUsage();
    const auto checkpointMemoryLimit =
            (bucketQuota * config.getCursorDroppingCheckpointMemUpperMark()) /
            100;
    const bool hitCheckpointMemoryThreshold =
            checkpointMemoryUsage >= checkpointMemoryLimit;

    // @todo MB-46827: Remove the condition on LWM - checkpoint memory recovery
    // must trigger if checkpoint mem-usage is high, as defined by checkpoint
    // threshold and regardless the LWM
    const bool aboveLowWatermark = memUsed >= stats.mem_low_wat.load();

    const bool ckptMemExceedsCheckpointMemoryThreshold =
            aboveLowWatermark && hitCheckpointMemoryThreshold;

    auto toMB = [](size_t bytes) { return bytes / (1024 * 1024); };
    if (ckptMemExceedsCheckpointMemoryThreshold) {
        size_t amountOfMemoryToClear;

        // Calculate the lower percentage of quota and subtract that from
        // the current checkpoint memory size to obtain the 'target'.
        amountOfMemoryToClear =
                checkpointMemoryUsage -
                ((bucketQuota *
                  config.getCursorDroppingCheckpointMemLowerMark()) /
                 100);
        EP_LOG_INFO(
                "Triggering memory recovery as checkpoint_memory ({} MB) "
                "exceeds cursor_dropping_checkpoint_mem_upper_mark ({}%, "
                "{} MB). Attempting to free {} MB of memory.",
                toMB(checkpointMemoryUsage),
                config.getCursorDroppingCheckpointMemUpperMark(),
                toMB(checkpointMemoryLimit),
                toMB(amountOfMemoryToClear));

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

bool ClosedUnrefCheckpointRemoverTask::run() {
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
