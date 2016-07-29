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
#include "stored-value.h"

DefragmenterTask::DefragmenterTask(EventuallyPersistentEngine* e,
                                   EPStats& stats_)
  : GlobalTask(e, TaskId::DefragmenterTask, false),
    stats(stats_),
    epstore_position(engine->getEpStore()->startPosition()),
    visitor(NULL) {
}

DefragmenterTask::~DefragmenterTask() {
    delete visitor;
}

bool DefragmenterTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "DefragmenterTask");
    if (engine->getConfiguration().isDefragmenterEnabled()) {
        // Get our visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor and
        // reset the position.
        if (visitor == NULL) {
            visitor = new DefragmentVisitor(getAgeThreshold());
            epstore_position = engine->getEpStore()->startPosition();
        }

        // Print start status.
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (epstore_position == engine->getEpStore()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << visitor->getHashtablePosition() << ".";
        }
        ss << " Using chunk_duration=" << getChunkDurationMS() << " ms."
           << " mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << getMappedBytes();
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        ALLOCATOR_HOOKS_API* alloc_hooks = engine->getServerApi()->alloc_hooks;
        bool old_tcache = alloc_hooks->enable_thread_cache(false);

        // Prepare the visitor.
        hrtime_t start = gethrtime();
        hrtime_t deadline = start + (getChunkDurationMS() * 1000 * 1000);
        visitor->setDeadline(deadline);
        visitor->clearStats();

        // Do it - set off the visitor.
        epstore_position = engine->getEpStore()->pauseResumeVisit
                (*visitor, epstore_position);
        hrtime_t end = gethrtime();

        // Defrag complete. Restore thread caching.
        alloc_hooks->enable_thread_cache(old_tcache);

        // Update stats
        stats.defragNumMoved.fetch_add(visitor->getDefragCount());
        stats.defragNumVisited.fetch_add(visitor->getVisitedCount());

        // Release any free memory we now have in the allocator back to the OS.
        // TODO: Benchmark this - is it necessary? How much of a slowdown does it
        // add? How much memory does it return?
        alloc_hooks->release_free_memory();

        // Check if the visitor completed a full pass.
        bool completed = (epstore_position == engine->getEpStore()->endPosition());

        // Print status.
        ss.str("");
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << epstore_position << ".";
        }
        ss << " Took " << (end - start) / 1024 << " us."
           << " moved " << visitor->getDefragCount() << "/"
           << visitor->getVisitedCount() << " visited documents."
           << " mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << getMappedBytes()
           << ". Sleeping for " << getSleepTime() << " seconds.";
        LOG(EXTENSION_LOG_INFO, "%s", ss.str().c_str());

        // Delete visitor if it finished.
        if (completed) {
            delete visitor;
            visitor = NULL;
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

std::string DefragmenterTask::getDescription(void) {
    return std::string("Memory defragmenter");
}

size_t DefragmenterTask::getSleepTime() const {
    return engine->getConfiguration().getDefragmenterInterval();
}

size_t DefragmenterTask::getAgeThreshold() const {
    return engine->getConfiguration().getDefragmenterAgeThreshold();
}

size_t DefragmenterTask::getChunkDurationMS() const {
    return engine->getConfiguration().getDefragmenterChunkDuration();
}

size_t DefragmenterTask::getMappedBytes() {
    ALLOCATOR_HOOKS_API* alloc_hooks = engine->getServerApi()->alloc_hooks;

    allocator_stats stats = {0};
    stats.ext_stats_size = alloc_hooks->get_extra_stats_size();
    stats.ext_stats = new allocator_ext_stat[stats.ext_stats_size];
    alloc_hooks->get_allocator_stats(&stats);

    size_t mapped_bytes = stats.heap_size - stats.free_mapped_size -
                          stats.free_unmapped_size;
    delete[] stats.ext_stats;
    return mapped_bytes;
}
