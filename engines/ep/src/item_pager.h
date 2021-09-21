/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>

#include <folly/Synchronized.h>
#include <memcached/types.h> // for ssize_t
#include <chrono>

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EPStats;
class EventuallyPersistentEngine;
class VBucketFilter;

namespace cb {
class Semaphore;
}

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
     * @param numConcurrentPagers how many paging visitors should be created
     *        per run ItemPager run.
     */
    ItemPager(EventuallyPersistentEngine& e,
              EPStats& st,
              size_t numConcurrentPagers);

    bool run() override;

    std::string getDescription() const override {
        return "Paging out items.";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
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

    const size_t numConcurrentPagers;

    EventuallyPersistentEngine& engine;
    EPStats& stats;
    // used to avoid creating more paging visitors while any are still running
    std::shared_ptr<cb::Semaphore> pagerSemaphore;

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
    ExpiredItemPager(EventuallyPersistentEngine* e,
                     EPStats& st,
                     size_t stime,
                     ssize_t taskTime,
                     int numConcurrentExpiryPagers);

    /**
     * Update the periodic sleep interval of the task.
     */
    void updateSleepTime(std::chrono::seconds sleepTime);

    /**
     * Update the initial runtime of the task.
     *
     * Runtime provided as a hour of the day (GMT) 0-23, or -1.
     * If -1, the task will not wait for a specific time of day before running
     * for the first time, it will just follow the periodic sleep interval.
     */
    void updateInitialRunTime(ssize_t initialRunTime);

    std::chrono::seconds getSleepTime() const;

    /**
     * Lock the config for writing, and return a handle.
     *
     * Used when other operations need synchronising with config changes
     * (e.g., scheduling or cancelling the task)
     */
    auto wlockConfig() {
        return config.wlock();
    }

    bool isEnabled() const;

    bool run() override;

    std::string getDescription() const override {
        return "Paging expired items.";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Typically runs in single-digit milliseconds. Set max expected to
        // 25ms - a "fair" timeslice for a task to take.
        return std::chrono::milliseconds(25);
    }

private:
    struct Config {
        std::chrono::seconds sleepTime = std::chrono::seconds(0);
        ssize_t initialRunTime = -1;
        bool enabled = false;
    };

    void updateWakeTimeFromCfg(const Config& cfg);

    folly::Synchronized<Config> config;
    /**
     *  This function is to update the next expiry pager
     *  task time, based on the current snooze time.
     */
    void updateExpPagerTime(double sleepSecs);

    EventuallyPersistentEngine     *engine;
    EPStats                        &stats;
    // used to avoid creating more paging visitors while any are still running
    std::shared_ptr<cb::Semaphore> pagerSemaphore;
};
