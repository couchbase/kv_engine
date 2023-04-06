/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "item_pager.h"

#include <executor/notifiable_task.h>
#include <memcached/server_bucket_iface.h>
#include <functional>
#include <mutex>
#include <vector>

class EPEngineGroup;
class ItemEvictionStrategy;
class EventuallyPersistentEngine;
class CrossBucketVisitorAdapter;

/**
 * Dispatcher job responsible for periodically pushing data out of
 * memory for buckets sharing HT quota.
 */
class QuotaSharingItemPager : public ItemPager {
public:
    /**
     * Creates a new instance of the quota sharing item pager.
     *
     * @param bucketApi The bucket interface to use to interact with buckets.
     * @param group The group of engines to consider during paging.
     * @param t The taskable which this task is associated with.
     * @param getNumConcurrentPagers Used to determine the number of concurrent
     * pagers to create.
     * @param getSleepTime Used to determine how long the task will sleep for
     * when not requested to run.
     */
    QuotaSharingItemPager(
            ServerBucketIface& bucketApi,
            EPEngineGroup& group,
            Taskable& t,
            std::function<size_t()> getNumConcurrentPagers,
            std::function<std::chrono::milliseconds()> getSleepTime);

    std::chrono::microseconds getSleepTime() const override;

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

    PageableMemInfo getPageableMemInfo() const override;

    void schedulePagingVisitors(std::size_t bytesToEvict) override;

    EvictionRatios getEvictionRatios(
            const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
            std::size_t bytesToEvict) const override;

private:
    /**
     * Calculates the number of evictable bytes from buckets in the specified
     * states.
     */
    size_t getEvictableMemForState(
            const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
            PermittedVBStates states) const;

    /**
     * The ServerBucketIface to use for accessing buckets.
     */
    ServerBucketIface& bucketApi;

    /**
     * The group of engines to consider during paging.
     */
    EPEngineGroup& group;

    /**
     * Used to determine the number of concurrent pagers to create.
     */
    std::function<size_t()> getNumConcurrentPagers;

    /**
     * Used to determine how long the task will sleep for when not requested to
     * run.
     */
    std::function<std::chrono::milliseconds()> getSleepTimeCb;
};
