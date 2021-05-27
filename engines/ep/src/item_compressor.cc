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

#include "item_compressor.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "item_compressor_visitor.h"
#include "kv_bucket.h"
#include "stored-value.h"
#include <phosphor/phosphor.h>

ItemCompressorTask::ItemCompressorTask(EventuallyPersistentEngine* e,
                                       EPStats& stats_)
    : GlobalTask(e, TaskId::ItemCompressorTask, 0, false),
      stats(stats_),
      epstore_position(engine->getKVBucket()->startPosition()) {
}

bool ItemCompressorTask::run() {
    TRACE_EVENT0("ep-engine/task", "ItemCompressorTask");
    if (engine->getCompressionMode() == BucketCompressionMode::Active) {
        // Get our pause/resume visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor
        // starting from the beginning.
        if (!prAdapter) {
            prAdapter = std::make_unique<PauseResumeVBAdapter>(
                    std::make_unique<ItemCompressorVisitor>());
            epstore_position = engine->getKVBucket()->startPosition();
        }

        // Print start status.
        if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
            std::stringstream ss;
            ss << getDescription() << " for bucket '" << engine->getName()
               << "'";
            if (epstore_position == engine->getKVBucket()->startPosition()) {
                ss << " starting. ";
            } else {
                ss << " resuming from " << epstore_position << ", ";
                ss << prAdapter->getHashtablePosition() << ".";
            }
            ss << " Using chunk_duration=" << getChunkDuration().count()
               << " ms."
               << " mem_used=" << stats.getEstimatedTotalMemoryUsed();
            EP_LOG_DEBUG("{}", ss.str());
        }

        // Prepare the underlying visitor.
        auto& visitor = getItemCompressorVisitor();
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + getChunkDuration();
        visitor.setDeadline(deadline);
        visitor.clearStats();
        visitor.setCompressionMode(engine->getCompressionMode());
        visitor.setMinCompressionRatio(engine->getMinCompressionRatio());

        // Do it - set off the visitor.
        epstore_position = engine->getKVBucket()->pauseResumeVisit(
                *prAdapter, epstore_position);
        const auto end = std::chrono::steady_clock::now();

        // Update stats
        stats.compressorNumCompressed.fetch_add(visitor.getCompressedCount());
        stats.compressorNumVisited.fetch_add(visitor.getVisitedCount());

        // Check if the visitor completed a full pass.
        bool completed =
                (epstore_position == engine->getKVBucket()->endPosition());

        // Print status.
        if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
            std::stringstream ss;
            ss << getDescription() << " for bucket '" << engine->getName()
               << "'";
            if (completed) {
                ss << " finished.";
            } else {
                ss << " paused at position " << epstore_position << ".";
            }
            std::chrono::microseconds duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            end - start);
            ss << " Took " << duration.count() << " us."
               << " compressed " << visitor.getCompressedCount() << "/"
               << visitor.getVisitedCount() << " visited documents."
               << " mem_used=" << stats.getEstimatedTotalMemoryUsed()
               << ".Sleeping for " << getSleepTime() << " seconds.";
            EP_LOG_DEBUG("{}", ss.str());
        }

        // Delete(reset) visitor if it finished.
        if (completed) {
            prAdapter.reset();
        }
    }

    snooze(getSleepTime());
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    return true;
}

void ItemCompressorTask::stop() {
    if (uid) {
        ExecutorPool::get()->cancel(uid);
    }
}

std::string ItemCompressorTask::getDescription() const {
    return "Item Compressor";
}

std::chrono::microseconds ItemCompressorTask::maxExpectedDuration() const {
    // The item compressor processes items in chunks, with each chunk
    // constrained by a ChunkDuration runtime, so we expect to only take
    // that long. However, the ProgressTracker used estimates the time
    // remaining, so apply some headroom to that figure so we don't get
    // inundated with spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

double ItemCompressorTask::getSleepTime() const {
    return (engine->getConfiguration().getItemCompressorInterval() * 0.001);
}

std::chrono::milliseconds ItemCompressorTask::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getItemCompressorChunkDuration());
}

ItemCompressorVisitor& ItemCompressorTask::getItemCompressorVisitor() {
    return dynamic_cast<ItemCompressorVisitor&>(prAdapter->getHTVisitor());
}
