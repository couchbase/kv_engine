/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "progress_tracker.h"

ProgressTracker::ProgressTracker()
    : need_initial_time(true),
      next_visit_count_check(INITIAL_VISIT_COUNT_CHECK),
      deadline(std::chrono::steady_clock::time_point::max()),
      previous_time(std::chrono::steady_clock::time_point::min()),
      previous_visited(0) {
}

void ProgressTracker::setDeadline(
        std::chrono::steady_clock::time_point new_deadline) {
    need_initial_time = true;
    deadline = new_deadline;
}

/* There is a time-based deadline on how many items should be visited before we
 * pause, however reading time can be an expensive operation (especially on
 * virtualised platforms). Therefore instead of reading the time on every item
 * we use the rate of visiting (items/sec) to estimate when we expect to
 * complete, only calling std::chrono::steady_clock::now() periodically to check
 * our rate.
 */
bool ProgressTracker::shouldContinueVisiting(size_t visited_items) {
    // Grab time if we haven't already got it.
    if (need_initial_time) {
        next_visit_count_check = visited_items + INITIAL_VISIT_COUNT_CHECK;
        previous_time = std::chrono::steady_clock::now();
        previous_visited = visited_items;
        need_initial_time = false;
    }

    if (visited_items < next_visit_count_check
        || visited_items == previous_visited) {
        // Not yet reached enough items to check time; ok to continue.
        return true;
    }

    // First check if the deadline has been exceeded; if so need to pause.
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
        return false;
    }

    // Not yet exceeded. Estimate how many more items we can visit
    // before it is exceeded.

    // Calculate time delta since last check. In the worst case,
    // visiting items *may* take less time than a single period of
    // our "high" resolution clock (e.g. some platforms only have
    // microsecond-level precision).
    // Therefore to prevent successive time measurements being
    // identical (and hence time_delta being zero, ultimately
    // triggering a div-by-zero error), add the minimum duration of the
    // clock to the delta.
    const auto time_delta =
            (now - previous_time) + std::chrono::steady_clock::duration(1);

    const size_t visited_delta = visited_items - previous_visited;
    // Calculate time for one item. Similar to above, ensure this is
    // always at least a nonzero value (by adding the clock min
    // duration) to prevent div-by-zero.
    const auto time_per_item = (time_delta / visited_delta) +
                               std::chrono::steady_clock::duration(1);

    const auto time_remaining = (deadline - now);
    const size_t est_items_to_deadline = time_remaining / time_per_item;

    // If there isn't sufficient time to visit our minimum, pause now.
    if (est_items_to_deadline < MINIMUM_VISIT_COUNT_BEFORE_PAUSE) {
        return false;
    }

    // Update the previous counts
    previous_time = now;
    previous_visited = visited_items;

    // Schedule next check after 50% of the estimated number of items
    // to deadline.
    next_visit_count_check = visited_items + (est_items_to_deadline / 2);
    return true;
}
