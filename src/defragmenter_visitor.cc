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

#include "defragmenter_visitor.h"

class ProgressTracker
{
public:
    ProgressTracker(DefragmentVisitor& visitor);

    void setDeadline(hrtime_t new_deadline);

    bool shouldContinueVisiting();

private:
    // After how many visited items should the time be first checked?
    static const size_t INITIAL_VISIT_COUNT_CHECK = 100;

    // When we can only visit less than this number of items, pause.
    static const size_t MINIMUM_VISIT_COUNT_BEFORE_PAUSE = 10;

    DefragmentVisitor& visitor;

    // Do we need to capture an initial time for measuring progress?
    bool need_initial_time;

    size_t next_visit_count_check;
    hrtime_t deadline;
    hrtime_t previous_time;
    size_t previous_visited;
};

// DegragmentVisitor implementation ///////////////////////////////////////////

DefragmentVisitor::DefragmentVisitor(uint8_t age_threshold_)
  : max_size_class(3584),  // TODO: Derive from allocator hooks.
    age_threshold(age_threshold_),
    progressTracker(NULL),
    resume_vbucket_id(0),
    hashtable_position(),
    defrag_count(0),
    visited_count(0) {
    progressTracker = new ProgressTracker(*this);
}

DefragmentVisitor::~DefragmentVisitor() {
    delete(progressTracker);
}

void DefragmentVisitor::setDeadline(hrtime_t deadline) {
    progressTracker->setDeadline(deadline);
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
    return progressTracker->shouldContinueVisiting();
}

HashTable::Position DefragmentVisitor::getHashtablePosition() const {
    return hashtable_position;
}

void DefragmentVisitor::clearStats() {
    defrag_count = 0;
    visited_count = 0;
}

size_t DefragmentVisitor::getDefragCount() const {
    return defrag_count;
}

size_t DefragmentVisitor::getVisitedCount() const {
    return visited_count;
}

/* ProgressTracker implementation ********************************************/

ProgressTracker::ProgressTracker(DefragmentVisitor& visitor_)
  : visitor(visitor_),
    need_initial_time(true),
    next_visit_count_check(INITIAL_VISIT_COUNT_CHECK),
    deadline(std::numeric_limits<hrtime_t>::max()),
    previous_time(0),
    previous_visited(0) {
}

void ProgressTracker::setDeadline(hrtime_t new_deadline) {
    need_initial_time = true;
    deadline = new_deadline;
}

/* There is a time-based deadline on how many items should be visited before we
 * pause, however reading time can be an expensive operation (especially on
 * virtualised platforms). Therefore instead of reading the time on every item
 * we use the rate of visiting (items/sec) to estimate when we expect to complete,
 * only calling gethrtime() periodically to check our rate.
 */
bool ProgressTracker::shouldContinueVisiting() {
    const size_t visited_items = visitor.getVisitedCount();

    // Grab time if we haven't already got it.
    if (need_initial_time) {
        next_visit_count_check = visited_items + INITIAL_VISIT_COUNT_CHECK;
        previous_time = gethrtime();
        previous_visited = visited_items;
        need_initial_time = false;
    }

    bool should_continue = true;

    if (visited_items < next_visit_count_check
        || visited_items == previous_visited) {
        // Not yet reached enough items to check time; ok to continue.
        return true;
    } else {
        // First check if the deadline has been exceeded; if so need to pause.
        const hrtime_t now = gethrtime();
        if (now >= deadline) {
            should_continue = false;
        } else {
            // Not yet exceeded. Estimate how many more items we can visit
            // before it is exceeded.

            // Calculate time delta since last check. In the worst case,
            // visiting items *may* take less time than a single period of
            // our "high" resolution clock (e.g. some platforms only have
            // microsecond-level precision for gethrtime()).
            // Therefore to prevent successive time measurements being
            // identical (and hence time_delta being zero, ultimately
            // triggering a div-by-zero error), add the period of the clock to
            // the delta.
            const hrtime_t time_delta = (now - previous_time) + gethrtime_period();

            const size_t visited_delta = visited_items - previous_visited;
            // Calculate time for one item. Similar to above, ensure this is
            // always at least a nonzero value (by adding hrtime_period) to
            // prevent div-by-zero.
            const hrtime_t time_per_item = (time_delta / visited_delta) + gethrtime_period();

            const hrtime_t time_remaining = (deadline - now);
            const size_t est_items_to_deadline = time_remaining / time_per_item;

            // If there isn't sufficient time to visit our minimum, pause now.
            if (est_items_to_deadline < MINIMUM_VISIT_COUNT_BEFORE_PAUSE) {
                should_continue = false;
            } else {
                // Update the previous counts
                previous_time = now;
                previous_visited = visited_items;

                // Schedule next check after 50% of the estimated number of items
                // to deadline.
                next_visit_count_check =
                        visited_items + (est_items_to_deadline / 2);
            }
        }
    }

    return should_continue;
}
