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

#include "ep_engine.h"
#include "stored-value.h"

/** Defragmentation visitor - visit all objects and defragment
 *
 */
class DefragmentVisitor : public VBucketVisitor {
public:
    DefragmentVisitor(size_t age_threshold_);

    virtual void visit(StoredValue* v);

    // Returns the number of documents that have been defragmented.
    size_t get_defrag_count() const;

    // Returns the number of documents that have been visited.
    size_t get_visited_count() const;

private:
    /* Configuration parameters */

    // Size of the largest size class from the allocator.
    const size_t max_size_class;

    // How old a blob must be to consider it for defragmentation.
    const uint8_t age_threshold;

    /* Statistics */
    // Count of how many documents have been defrag'd.
    size_t defrag_count;
    // How many documents have been visited.
    size_t visited_count;
};

DefragmenterTask::DefragmenterTask(EventuallyPersistentEngine* e,
                                   EPStats& stats_, size_t sleep_time_,
                                   size_t age_threshold_)
  : GlobalTask(e, Priority::DefragmenterTaskPriority, sleep_time_, false),
    stats(stats_),
    sleep_time(sleep_time_),
    age_threshold(age_threshold_) {
}

bool DefragmenterTask::run(void) {
    if (engine->getConfiguration().isDefragmenterEnabled()) {
        // Print start status.
        std::stringstream ss;
        ss << getDescription() << " starting for bucket '" << engine->getName()
           << "'. mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << get_mapped_bytes();
        LOG(EXTENSION_LOG_INFO, ss.str().c_str());

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        bool old_tcache = engine->getServerApi()->alloc_hooks->enable_thread_cache(false);

        // Do the defrag.
        DefragmentVisitor dv(age_threshold);
        hrtime_t start = gethrtime();
        engine->getEpStore()->visit(dv);
        hrtime_t end = gethrtime();

        // Defrag complete. Restore thread caching.
        engine->getServerApi()->alloc_hooks->enable_thread_cache(old_tcache);

        // Update stats
        stats.defragNumMoved.fetch_add(dv.get_defrag_count());

        // Release any free memory we now have in the allocator back to the OS.
        engine->getServerApi()->alloc_hooks->release_free_memory();

        // Print status.
        ss.str("");
        ss << getDescription() << " finished for bucket '" << engine->getName()
           << "', took " << (end - start) / 1024 << " us."
           << " moved " << dv.get_defrag_count() << "/"
           << dv.get_visited_count() << " documents."
           << " mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << get_mapped_bytes()
           << ". Sleeping for " << sleep_time << " seconds.";
        LOG(EXTENSION_LOG_INFO, ss.str().c_str());
    }

    snooze(sleep_time);
    if (engine->getEpStats().isShutdown) {
            return false;
    }
    return true;
}

void DefragmenterTask::stop(void) {
    if (taskId) {
        ExecutorPool::get()->cancel(taskId);
    }
}

std::string DefragmenterTask::getDescription(void) {
    return std::string("Memory defragmenter");
}

size_t DefragmenterTask::get_mapped_bytes() {
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

DefragmentVisitor::DefragmentVisitor(size_t age_threshold_)
  : max_size_class(3584),  // TODO: Derive from allocator hooks.
    age_threshold(age_threshold_),
    defrag_count(0),
    visited_count(0) {
}

void DefragmentVisitor::visit(StoredValue *v) {
    const size_t value_len = v->valuelen();

    // value must be at least non-zero (also covers Items with null Blobs)
    // and no larger than the biggest size class the allocator
    // supports, so it can be successfully reallocated to a run with other
    // objects of the same size.
    if (value_len > 0 && value_len <= max_size_class) {
        // If sufficiently old reallocate, otherwise increment it's age.
        if (v->getValue()->getAge() >= age_threshold) {
            v->reallocate();
            defrag_count++;
        } else {
            v->getValue()->incrementAge();
        }
    }
    visited_count++;
}

size_t DefragmentVisitor::get_defrag_count() const {
    return defrag_count;
}

size_t DefragmentVisitor::get_visited_count() const {
    return visited_count;
}
