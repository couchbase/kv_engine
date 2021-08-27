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

bool ClosedUnrefCheckpointRemoverTask::run() {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");

    bool inverse = true;
    if (!available.compare_exchange_strong(inverse, false)) {
        snooze(sleepTime);
        return true;
    }

    auto* kvBucket = engine->getKVBucket();
    const auto memToClear = kvBucket->getRequiredCheckpointMemoryReduction();

    if (memToClear == 0) {
        available = true;
        snooze(sleepTime);
        return true;
    }

    // Try expelling first, if enabled.
    // Note: The next call tries to expel from all vbuckets before returning.
    // The reason behind trying expel first is to avoid dropping cursors if
    // possible, as that kicks the stream back to backfilling.
    size_t memRecovered{0};
    if (engine->getConfiguration().isChkExpelEnabled()) {
        memRecovered = attemptItemExpelling(memToClear);
    }

    if (memRecovered >= memToClear) {
        // Recovered enough by ItemExpel, done
        available = true;
        snooze(sleepTime);
        return true;
    }

    // More memory to recover, try CursorDrop + CheckpointRemoval
    const auto leftToClear = memToClear - memRecovered;
    auto visitor = std::make_unique<CheckpointVisitor>(
            kvBucket, stats, available, leftToClear);

    // Note: Empirical evidence from perf runs shows that 99.9% of "Checkpoint
    // Remover" task should complete under 50ms.
    //
    // @todo: With changes for MB-48038 we are doing more work in the
    //  CheckpointVisitor, so the expected duration will probably need to be
    //  adjusted.
    kvBucket->visitAsync(std::move(visitor),
                         "Checkpoint Remover",
                         TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                         std::chrono::milliseconds(50) /*maxExpectedDuration*/);

    snooze(sleepTime);
    return true;
}
