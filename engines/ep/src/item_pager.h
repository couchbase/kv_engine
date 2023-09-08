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

#include <executor/notifiable_task.h>
#include <folly/Synchronized.h>
#include <memcached/types.h> // for ssize_t
#include <chrono>
#include <memory>
#include <optional>
#include <utility>

#include "eviction_ratios.h"
#include "permitted_vb_states.h"

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EPStats;
class EventuallyPersistentEngine;
class ItemEvictionStrategy;
class VBucketFilter;
class KVBucket;

namespace cb {
class Semaphore;
}

/**
 * An abstract base class for a task which runs periodically and evicts items
 * from memory.
 */
class ItemPager : public NotifiableTask {
public:
    // ctor when ItemPager is not associated with a specific bucket.
    ItemPager(Taskable& t,
              size_t numConcurrentPagers,
              std::chrono::milliseconds sleepTime);

    // ctor when Item pager is associated with a specific bucket.
    ItemPager(EventuallyPersistentEngine& engine,
              size_t numConcurrentPagers,
              std::chrono::milliseconds sleepTime);

    bool runInner(bool manuallyNotified) override;

    std::chrono::microseconds getSleepTime() const override {
        return sleepTime;
    }

protected:
    struct PageableMemInfo {
        /** Current pageable memory usage */
        size_t current = 0;
        /** Pageable memory high watermark */
        size_t upper = 0;
        /** Pageable memory low watermark */
        size_t lower = 0;
    };

    /**
     * Gets the memory quotas and usage to consider for paging.
     */
    virtual PageableMemInfo getPageableMemInfo() const = 0;

    /**
     * Schedules paging visitor tasks to run immediately.
     */
    virtual void schedulePagingVisitors(std::size_t bytesToEvict) = 0;

    /**
     * Creates a VBucketFilter object which only accepts VBuckets in one of
     * the specified states. Returns an empty optional if there are no VBuckets
     * in any of the specified states.
     */
    static std::optional<VBucketFilter> createVBucketFilter(
            KVBucket& kvBucket, PermittedVBStates acceptedStates);

    /**
     * Returns the list of state we need to consider for eviction according
     * to the eviction ratios.
     */
    PermittedVBStates getStatesForEviction(EvictionRatios states) const;

    /**
     * Get how many bytes could theoretically be reclaimed from
     * vbuckets in the specified states, if all resident items were evicted.
     */
    size_t getEvictableBytes(KVBucket& kvBucket,
                             PermittedVBStates states) const;

    /**
     * Computes the eviction ratios we need to be able to evict @p bytesToEvict
     * bytes from the given buckets.
     *
     * Some implementations of this method may only allow 1 KVBucket to be
     * specified.
     */
    virtual EvictionRatios getEvictionRatios(
            const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
            std::size_t bytesToEvict) const = 0;

    size_t numConcurrentPagers;
    // used to avoid creating more paging visitors while any are still running
    const std::shared_ptr<cb::Semaphore> pagerSemaphore;
    // Should eviction continue until the low watermark is reached?
    bool doEvict;

    /**
     * How long this task sleeps for if not requested to run. Initialised from
     * the configuration parameter - pager_sleep_time_ms
     */
    std::chrono::milliseconds sleepTime;
};

/**
 * Dispatcher job responsible for periodically pushing data out of
 * memory, by taking into account just the bucket's own memory usage and
 * watermarks.
 */
class StrictQuotaItemPager : public ItemPager {
public:
    /**
     * Construct an ItemPager.
     *
     * @param e the store (where we'll visit)
     * @param st the stats
     * @param numConcurrentPagers how many paging visitors should be created
     *        per run ItemPager run.
     */
    StrictQuotaItemPager(EventuallyPersistentEngine& e,
                         EPStats& st,
                         size_t numConcurrentPagers);

    std::string getDescription() const override {
        return "Paging out items.";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Typically runs in single-digit milliseconds. Set max expected to
        // 25ms - a "fair" timeslice for a task to take.
        return std::chrono::milliseconds(25);
    }

    PageableMemInfo getPageableMemInfo() const override;

    EvictionRatios getEvictionRatios(
            const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
            std::size_t bytesToEvict) const override;

    void schedulePagingVisitors(std::size_t bytesToEvict) override;

private:
    /**
     * Get a factory which can create multiple matching eviction strategy
     * instances based on the engine configuration.
     *
     * The constructed strategies will be consistent with each other, and
     * expensive but shareable work will only be done once.
     */
    std::function<std::unique_ptr<ItemEvictionStrategy>()>
    getEvictionStrategyFactory(EvictionRatios evictionRatios);

    /**
     * Reset the phase to the default determined by the bucket type
     */
    void resetPhase();

    EPStats& stats;
};

/**
 * Dispatcher job responsible for expiring items currently stored in memory. The
 * act of expiring (deleting) the item generally results in the deletion of the
 * items value which saves memory and disk space.
 */
class ExpiredItemPager : public GlobalTask,
                         public std::enable_shared_from_this<ExpiredItemPager> {
public:

    /**
     * Construct an ExpiredItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     * @param stime number of seconds to wait between runs
     */
    ExpiredItemPager(EventuallyPersistentEngine& e,
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
     * Enable and schedule the expiry pager task.
     *
     * @return true if the task was scheduled, false if it was already
     *              enabled
     */
    bool enable();

    /**
     * Disable and cancel the expiry pager task.
     *
     * @return true if the task was cancelled, false if it was already
     *              disabled
     */
    bool disable();

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
    std::chrono::seconds calculateWakeTimeFromCfg(const Config& cfg);

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
