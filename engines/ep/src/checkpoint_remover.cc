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
// The following diagram depicts the bucket memory where Q is the bucket quota
// and the labelled vertical lines each show thresholds/stats used in deciding
// if memory reduction is needed.
//
// 0                                                                      Q
// ├──────────────┬────────────┬───┬───────┬──────────────────────────────┤
// │              │            │   │       │                              │
// │              │            │   │       │                              │
// └──────────────▼────────────▼───▼───────▼──────────────────────────────┘
//                A            B   X       C
//
//   A = checkpoint_memory_recovery_lower_mark
//   B = checkpoint_memory_recovery_upper_mark
//   C = checkpoints quota (as defined by checkpoint_memory_ratio)
//   X = current checkpoint memory used
//   Q = bucket quota
//
// Memory reduction will commence if checkpoint memory usage (X) is greater than
// checkpoint_memory_recovery_upper_mark (B).
//
// If memory reduction triggers then this function will return (X - A) as the
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
    const auto& bucket = *engine->getKVBucket();
    const auto checkpointMemoryRatio = bucket.getCheckpointMemoryRatio();
    const auto checkpointQuota = stats.getMaxDataSize() * checkpointMemoryRatio;
    const auto recoveryThreshold =
            checkpointQuota * bucket.getCheckpointMemoryRecoveryUpperMark();
    const auto usage = stats.getEstimatedCheckpointMemUsage();

    if (usage < recoveryThreshold) {
        return std::make_pair(false, 0);
    }

    const auto lowerRatio = bucket.getCheckpointMemoryRecoveryLowerMark();
    const auto lowerMark = checkpointQuota * lowerRatio;
    Expects(usage > lowerMark);
    const size_t amountOfMemoryToClear = usage - lowerMark;

    const auto toMB = [](size_t bytes) { return bytes / (1024 * 1024); };
    const auto upperRatio = bucket.getCheckpointMemoryRecoveryUpperMark();
    EP_LOG_INFO(
            "Triggering memory recovery as checkpoint memory usage ({} MB) "
            "exceeds the upper_mark ({}, "
            "{} MB) - total checkpoint quota {}, {} MB . Attempting to free {} "
            "MB of memory.",
            toMB(usage),
            upperRatio,
            toMB(checkpointQuota * upperRatio),
            checkpointMemoryRatio,
            toMB(checkpointQuota),
            toMB(amountOfMemoryToClear));

    return std::make_pair(true, amountOfMemoryToClear);
}

size_t ClosedUnrefCheckpointRemoverTask::attemptItemExpelling(
        size_t memToClear) {
    size_t memoryCleared = 0;
    auto& kvBucket = *engine->getKVBucket();
    const auto vbuckets = kvBucket.getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= memToClear) {
            break;
        }
        const auto vbid = it.first;
        VBucketPtr vb = kvBucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        const auto expelResult =
                vb->checkpointManager->expelUnreferencedCheckpointItems();
        EP_LOG_DEBUG(
                "Expelled {} unreferenced checkpoint items "
                "from {} "
                "and estimated to have recovered {} bytes.",
                expelResult.expelCount,
                vb->getId(),
                expelResult.estimateOfFreeMemory);
        memoryCleared += expelResult.estimateOfFreeMemory;
    }
    return memoryCleared;
}

size_t ClosedUnrefCheckpointRemoverTask::attemptCursorDropping(
        size_t memToClear) {
    size_t memoryCleared = 0;
    auto& kvBucket = *engine->getKVBucket();
    const auto vbuckets = kvBucket.getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= memToClear) {
            break;
        }
        const auto vbid = it.first;
        VBucketPtr vb = kvBucket.getVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Get a list of cursors that can be dropped from the vbucket's CM, so
        // as to unreference an estimated number of checkpoints.
        const auto cursors = vb->checkpointManager->getListOfCursorsToDrop();
        for (const auto& cursor : cursors) {
            if (memoryCleared >= memToClear) {
                break;
            }

            if (engine->getDcpConnMap().handleSlowStream(vbid,
                                                         cursor.lock().get())) {
                const auto memoryFreed =
                        vb->getChkMgrMemUsageOfUnrefCheckpoints();
                ++stats.cursorsDropped;
                stats.cursorMemoryFreed += memoryFreed;
                memoryCleared += memoryFreed;
            }
        }
    }
    return memoryCleared;
}

bool ClosedUnrefCheckpointRemoverTask::run() {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");

    bool inverse = true;
    if (!available.compare_exchange_strong(inverse, false)) {
        snooze(sleepTime);
        return true;
    }

    bool shouldReduceMemory{false};
    size_t memToClear{0};
    size_t memRecovered{0};

    std::tie(shouldReduceMemory, memToClear) =
            isReductionInCheckpointMemoryNeeded();
    if (shouldReduceMemory) {
        // Try expelling first, if enabled
        if (engine->getConfiguration().isChkExpelEnabled()) {
            memRecovered = attemptItemExpelling(memToClear);
        }
        // If still need to recover more memory, drop cursors
        if (memToClear > memRecovered) {
            attemptCursorDropping(memToClear - memRecovered);
        }
    }

    KVBucketIface* kvBucket = engine->getKVBucket();
    auto pv = std::make_unique<CheckpointVisitor>(kvBucket, stats, available);

    // Note: Empirical evidence from perf runs shows that 99.9% of "Checkpoint
    // Remover" task should complete under 50ms
    kvBucket->visitAsync(std::move(pv),
                         "Checkpoint Remover",
                         TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                         std::chrono::milliseconds(50) /*maxExpectedDuration*/);

    snooze(sleepTime);
    return true;
}
