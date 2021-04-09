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

#pragma once

#include <chrono>

/**
 * Helper class used by objects which want to track their progress (how long
 * they have been running for) in an efficient way; without having to check
 * clock::now() on every iteration.
 *
 * Example usage:
 *
 *     // Create a ProcessTracker with a deadline of 10ms from now.
 *     ProgressTracker tracker;
 *     tracker.setDeadline(std::chrono::steady_clock::now +
 * std::chrono::milliseconds(10));
 *
 *     // Loop doing some work; where you want to stop after some time limit.
 *     for (size_t ii = 0; ii < ...; ii++) {
 *         doSomeWork();
 *         if (tracker.shouldContinueVisiting(ii)) {
 *             break;
 *         }
 *     }
 */
class ProgressTracker
{
public:
    ProgressTracker();

    void setDeadline(std::chrono::steady_clock::time_point new_deadline);

    /**
     * Inform the progress tracker that work has been done, and if visiting
     * should continue.
     *
     * @param visited_items Number of items visited by the visitor so far in
     *        this run (i.e. running total).
     * @return true if visiting should continue, or false if it should be
     *         paused.
     */
    bool shouldContinueVisiting(size_t visited_items);

private:
    // After how many visited items should the time be first checked?
    static const size_t INITIAL_VISIT_COUNT_CHECK = 100;

    // When we can only visit less than this number of items, pause.
    static const size_t MINIMUM_VISIT_COUNT_BEFORE_PAUSE = 10;

    // Do we need to capture an initial time for measuring progress?
    bool need_initial_time;

    size_t next_visit_count_check;
    std::chrono::steady_clock::time_point deadline;
    std::chrono::steady_clock::time_point previous_time;
    size_t previous_visited;
};
