/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "defragmenter.h"

#include <phosphor/phosphor.h>

#include "defragmenter_visitor.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "kv_bucket.h"
#include "stored-value.h"

DefragmenterTask::DefragmenterTask(EventuallyPersistentEngine* e,
                                   EPStats& stats_)
    : GlobalTask(e, TaskId::DefragmenterTask, 0, false),
      stats(stats_),
      epstore_position(engine->getKVBucket()->startPosition()) {
}

bool DefragmenterTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "DefragmenterTask");
    if (engine->getConfiguration().isDefragmenterEnabled()) {
        ALLOCATOR_HOOKS_API* alloc_hooks = engine->getServerApi()->alloc_hooks;
        // Get our pause/resume visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor
        // starting from the beginning.
        if (!prAdapter) {
            prAdapter = std::make_unique<PauseResumeVBAdapter>(
                    std::make_unique<DefragmentVisitor>(
                            getAgeThreshold(), getMaxValueSize(alloc_hooks)));
            epstore_position = engine->getKVBucket()->startPosition();
        }

        // Print start status.
        std::stringstream ss;
        ss << to_string(getDescription()) << " for bucket '"
           << engine->getName() << "'";
        if (epstore_position == engine->getKVBucket()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << prAdapter->getHashtablePosition() << ".";
        }
        ss << " Using chunk_duration=" << getChunkDuration().count() << " ms."
           << " mem_used=" << stats.getEstimatedTotalMemoryUsed()
           << ", mapped_bytes=" << getMappedBytes();
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        bool old_tcache = alloc_hooks->enable_thread_cache(false);

        // Prepare the underlying visitor.
        auto& visitor = getDefragVisitor();
        const auto start = ProcessClock::now();
        const auto deadline = start + getChunkDuration();
        visitor.setDeadline(deadline);
        visitor.clearStats();

        // Do it - set off the visitor.
        epstore_position = engine->getKVBucket()->pauseResumeVisit(
                *prAdapter, epstore_position);
        const auto end = ProcessClock::now();

        // Defrag complete. Restore thread caching.
        alloc_hooks->enable_thread_cache(old_tcache);

        // Update stats
        stats.defragNumMoved.fetch_add(visitor.getDefragCount());
        stats.defragNumVisited.fetch_add(visitor.getVisitedCount());

        // Release any free memory we now have in the allocator back to the OS.
        // TODO: Benchmark this - is it necessary? How much of a slowdown does it
        // add? How much memory does it return?
        alloc_hooks->release_free_memory();

        // Check if the visitor completed a full pass.
        bool completed = (epstore_position ==
                                    engine->getKVBucket()->endPosition());

        // Print status.
        ss.str("");
        ss << to_string(getDescription()) << " for bucket '"
           << engine->getName() << "'";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << epstore_position << ".";
        }
        std::chrono::microseconds duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
        ss << " Took " << duration.count() << " us."
           << " moved " << visitor.getDefragCount() << "/"
           << visitor.getVisitedCount() << " visited documents."
           << " mem_used=" << stats.getEstimatedTotalMemoryUsed()
           << ", mapped_bytes=" << getMappedBytes() << ". Sleeping for "
           << getSleepTime() << " seconds.";
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

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

void DefragmenterTask::stop(void) {
    if (uid) {
        ExecutorPool::get()->cancel(uid);
    }
}

cb::const_char_buffer DefragmenterTask::getDescription() {
    return "Memory defragmenter";
}

std::chrono::microseconds DefragmenterTask::maxExpectedDuration() {
    // Defragmenter processes items in chunks, with each chunk constrained
    // by a ChunkDuration runtime, so we expect to only take that long.
    // However, the ProgressTracker used estimates the time remaining, so
    // apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

size_t DefragmenterTask::getSleepTime() const {
    return engine->getConfiguration().getDefragmenterInterval();
}

size_t DefragmenterTask::getAgeThreshold() const {
    return engine->getConfiguration().getDefragmenterAgeThreshold();
}

size_t DefragmenterTask::getMaxValueSize(ALLOCATOR_HOOKS_API* alloc_hooks) {
    size_t nbins{0};
    alloc_hooks->get_allocator_property("arenas.nbins", &nbins);

    char buff[20];
    snprintf(buff,
             sizeof(buff),
             "arenas.bin.%" PRIu64 ".size",
             static_cast<uint64_t>(nbins) - 1);

    size_t largest_bin_size;
    alloc_hooks->get_allocator_property(buff, &largest_bin_size);

    return largest_bin_size;
}

std::chrono::milliseconds DefragmenterTask::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getDefragmenterChunkDuration());
}

size_t DefragmenterTask::getMappedBytes() {
    ALLOCATOR_HOOKS_API* alloc_hooks = engine->getServerApi()->alloc_hooks;

    allocator_stats stats = {0};
    stats.ext_stats.resize(alloc_hooks->get_extra_stats_size());
    alloc_hooks->get_allocator_stats(&stats);

    size_t mapped_bytes = stats.fragmentation_size + stats.allocated_size;
    return mapped_bytes;
}

DefragmentVisitor& DefragmenterTask::getDefragVisitor() {
    return dynamic_cast<DefragmentVisitor&>(prAdapter->getHTVisitor());
}
