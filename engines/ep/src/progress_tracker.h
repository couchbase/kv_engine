/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
