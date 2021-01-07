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
#pragma once

#include "globaltask.h"

#include <memcached/types.h> // for ssize_t

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EPStats;
class EventuallyPersistentEngine;
class VBucketFilter;

/**
 * Tracks the desired eviction ratios for different vbucket states.
 *
 * PagingVisitor attempts to evict the specified fraction of the items from
 * vbuckets in the given state.
 *
 * Note, deleted vbuckets are not evicted from, and pending vbuckets currently
 * share a ratio with active vbuckets, so only two ratios are tracked.
 */
class EvictionRatios {
public:
    EvictionRatios() = default;

    EvictionRatios(double activeAndPending, double replica)
        : activeAndPending(activeAndPending), replica(replica) {
    }
    /**
     * Get the fraction of items to be evicted from vbuckets in the given state.
     */
    double getForState(vbucket_state_t state);
    /**
     * Set the fraction of items to be evicted from vbuckets in the given state.
     *
     * Active and pending vbuckets share a ratio, setting either will overwrite
     * the existing value for both states.
     *
     * Dead vbuckets are not evicted from, so setting a ratio for dead
     * will be ignored.
     */
    void setForState(vbucket_state_t state, double value);

private:
    double activeAndPending = 0.0;
    double replica = 0.0;
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

    bool run() noexcept override;

    std::string getDescription() override {
        return "Paging out items.";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Typically runs in single-digit milliseconds. Set max expected to
        // 25ms - a "fair" timeslice for a task to take.
        return std::chrono::milliseconds(25);
    }

    /**
     * Request that this ItemPager is scheduled to run 'now'
     */
    void scheduleNow();

private:
    /**
     * Get how many bytes could theoretically be reclaimed from
     * vbuckets matching the given filter, if all resident items were evicted.
     */
    size_t getEvictableBytes(const VBucketFilter& filter) const;

    /**
     * Reset the phase to the default determined by the bucket type
     */
    void resetPhase();

    EventuallyPersistentEngine& engine;
    EPStats& stats;
    std::shared_ptr<std::atomic<bool>> available;

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

    bool run() noexcept override;

    std::string getDescription() override {
        return "Paging expired items.";
    }

    std::chrono::microseconds maxExpectedDuration() override {
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
