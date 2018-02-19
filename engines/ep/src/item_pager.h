/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#ifndef SRC_ITEM_PAGER_H_
#define SRC_ITEM_PAGER_H_ 1

#include "config.h"

#include "globaltask.h"

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EPStats;
class EventuallyPersistentEngine;

/**
 * The item pager phase
 */
enum item_pager_phase {
    PAGING_UNREFERENCED,
    PAGING_RANDOM
};

/**
 * Dispatcher job responsible for periodically pushing data out of
 * memory.
 */
class ItemPager : public GlobalTask {
public:
    /**
     * Construct an ItemPager.
     *
     * @param e the store (where we'll visit)
     * @param st the stats
     */
    ItemPager(EventuallyPersistentEngine& e, EPStats& st);

    bool run(void);

    item_pager_phase getPhase() const {
        return phase;
    }

    void setPhase(item_pager_phase item_phase) {
        phase = item_phase;
    }

    cb::const_char_buffer getDescription() {
        return "Paging out items.";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Typically runs in single-digit milliseconds. Set max expected to
        // 25ms - a "fair" timeslice for a task to take.
        return std::chrono::milliseconds(25);
    }

    /**
     * Request that this ItemPager is scheduled to run 'now'
     */
    void scheduleNow();

private:
    EventuallyPersistentEngine& engine;
    EPStats& stats;
    std::shared_ptr<std::atomic<bool>> available;

    // Current pager phase. Atomic as may be accessed by multiple PagingVisitor
    // objects running on different threads.
    std::atomic<item_pager_phase> phase;
    bool doEvict;

    /**
     * How long this task sleeps for if not requested to run. Initialised from
     * the configuration parameter - pager_sleep_time_ms
     * stored as a double seconds value ready for use in snooze calls.
     */
    std::chrono::duration<double> sleepTime;

    /// atomic bool used in the task's run trigger
    std::atomic<bool> notified;
};

/**
 * Dispatcher job responsible for purging expired items from
 * memory and disk.
 */
class ExpiredItemPager : public GlobalTask {
public:

    /**
     * Construct an ExpiredItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     * @param stime number of seconds to wait between runs
     */
    ExpiredItemPager(EventuallyPersistentEngine *e, EPStats &st,
                     size_t stime, ssize_t taskTime = -1);

    bool run(void);

    cb::const_char_buffer getDescription() {
        return "Paging expired items.";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Typically runs in single-digit milliseconds. Set max expected to
        // 25ms - a "fair" timeslice for a task to take.
        return std::chrono::milliseconds(25);
    }

private:
    /**
     *  This function is to update the next expiry pager
     *  task time, based on the current snooze time.
     */
    void updateExpPagerTime(double sleepSecs);

    EventuallyPersistentEngine     *engine;
    EPStats                        &stats;
    double                          sleepTime;
    std::shared_ptr<std::atomic<bool>>   available;
};

#endif  // SRC_ITEM_PAGER_H_
