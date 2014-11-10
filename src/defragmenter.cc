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
class DefragmentVisitor : public PauseResumeEPStoreVisitor,
                                 PauseResumeHashTableVisitor {
public:
    DefragmentVisitor(size_t age_threshold_);

    // Set the deadline at which point the visitor will pause visiting.
    void set_deadline(hrtime_t deadline_);

    // Implementation of PauseResumeEPStoreVisitor interface:
    virtual bool visit(uint16_t vbucket_id, HashTable& ht);

    // Implementation of PauseResumeHashTableVisitor interface:
    virtual bool visit(StoredValue& v);

    // Returns the current hashtable position.
    HashTable::Position get_hashtable_position() const;

    // Resets any held stats to zero.
    void clear_stats();

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

    /* Runtime state */

    // Until what point can the visitor run? Visiting will stop when
    // this time is exceeded.
    hrtime_t deadline;

    // When resuming, which vbucket should we start from?
    uint16_t resume_vbucket_id;

    // When pausing / resuming, hashtable position to use.
    HashTable::Position hashtable_position;

    /* Statistics */
    // Count of how many documents have been defrag'd.
    size_t defrag_count;
    // How many documents have been visited.
    size_t visited_count;
};

DefragmenterTask::DefragmenterTask(EventuallyPersistentEngine* e,
                                   EPStats& stats_, size_t sleep_time_,
                                   size_t age_threshold_,
                                   size_t chunk_duration_ms_)
  : GlobalTask(e, Priority::DefragmenterTaskPriority, sleep_time_, false),
    stats(stats_),
    sleep_time(sleep_time_),
    age_threshold(age_threshold_),
    chunk_duration_ms(chunk_duration_ms_),
    epstore_position(engine->getEpStore()->startPosition()),
    visitor(NULL) {
}

bool DefragmenterTask::run(void) {
    if (engine->getConfiguration().isDefragmenterEnabled()) {
        // Get our visitor. If we didn't finish the previous pass,
        // then resume from where we last were, otherwise create a new visitor and
        // reset the position.
        if (visitor == NULL) {
            visitor = new DefragmentVisitor(age_threshold);
            epstore_position = engine->getEpStore()->startPosition();
        }

        // Print start status.
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (epstore_position == engine->getEpStore()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << visitor->get_hashtable_position() << ".";
        }
        ss << " Using chunk_duration=" << chunk_duration_ms << " ms."
           << " mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << get_mapped_bytes();
        LOG(EXTENSION_LOG_INFO, ss.str().c_str());

        // Disable thread-caching (as we are about to defragment, and hence don't
        // want any of the new Blobs in tcache).
        ALLOCATOR_HOOKS_API* alloc_hooks = engine->getServerApi()->alloc_hooks;
        bool old_tcache = alloc_hooks->enable_thread_cache(false);

        // Prepare the visitor.
        hrtime_t start = gethrtime();
        hrtime_t deadline = start + (chunk_duration_ms * 1000 * 1000);
        visitor->set_deadline(deadline);
        visitor->clear_stats();

        // Do it - set off the visitor.
        epstore_position = engine->getEpStore()->pauseResumeVisit
                (*visitor, epstore_position);
        hrtime_t end = gethrtime();

        // Defrag complete. Restore thread caching.
        alloc_hooks->enable_thread_cache(old_tcache);

        // Update stats
        stats.defragNumMoved.fetch_add(visitor->get_defrag_count());
        stats.defragNumVisited.fetch_add(visitor->get_visited_count());

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
           << " moved " << visitor->get_defrag_count() << "/"
           << visitor->get_visited_count() << " visited documents."
           << " mem_used=" << stats.getTotalMemoryUsed()
           << ", mapped_bytes=" << get_mapped_bytes()
           << ". Sleeping for " << sleep_time << " seconds.";
        LOG(EXTENSION_LOG_INFO, ss.str().c_str());

        // Delete visitor if it finished.
        if (completed) {
            delete visitor;
            visitor = NULL;
        }
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

// DegragmentVisitor implementation ///////////////////////////////////////////

DefragmentVisitor::DefragmentVisitor(size_t age_threshold_)
  : max_size_class(3584),  // TODO: Derive from allocator hooks.
    age_threshold(age_threshold_),
    deadline(0),
    resume_vbucket_id(0),
    hashtable_position(),
    defrag_count(0),
    visited_count(0) {
}

void DefragmentVisitor::set_deadline(hrtime_t deadline_) {
    deadline = deadline_;
}

bool DefragmentVisitor::visit(uint16_t vbucket_id, HashTable& ht) {

    // Check if this vbucket_id matches the position we should resume
    // from. If so then call the visitor using our stored HashTable::Position.
    HashTable::Position ht_start;
    if (resume_vbucket_id == vbucket_id) {
        ht_start = hashtable_position;
    }

    hashtable_position = ht.pauseResumeVisit(*this, ht_start);

    if (hashtable_position != ht.endPosition()) {
        // We didn't get to the end of this hashtable. Record the vbucket_id
        // we got to and return false.
        resume_vbucket_id = vbucket_id;
        return false;
    } else {
        return true;
    }
}

bool DefragmentVisitor::visit(StoredValue& v) {
    const size_t value_len = v.valuelen();

    // value must be at least non-zero (also covers Items with null Blobs)
    // and no larger than the biggest size class the allocator
    // supports, so it can be successfully reallocated to a run with other
    // objects of the same size.
    if (value_len > 0 && value_len <= max_size_class) {
        // If sufficiently old reallocate, otherwise increment it's age.
        if (v.getValue()->getAge() >= age_threshold) {
            v.reallocate();
            defrag_count++;
        } else {
            v.getValue()->incrementAge();
        }
    }
    visited_count++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    hrtime_t now = gethrtime();
    return (now < deadline);
}

HashTable::Position DefragmentVisitor::get_hashtable_position() const {
    return hashtable_position;
}

void DefragmentVisitor::clear_stats() {
    defrag_count = 0;
    visited_count = 0;
}

size_t DefragmentVisitor::get_defrag_count() const {
    return defrag_count;
}

size_t DefragmentVisitor::get_visited_count() const {
    return visited_count;
}
