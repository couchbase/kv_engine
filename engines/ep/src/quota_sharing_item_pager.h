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
#include <folly/Synchronized.h>
#include <memcached/server_bucket_iface.h>
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
     * @param numConcurrentPagers The number of concurrent pagers to create.
     * @param sleepTime The initial sleep time of the task.
     */
    QuotaSharingItemPager(ServerBucketIface& bucketApi,
                          EPEngineGroup& group,
                          Taskable& t,
                          size_t numConcurrentPagers,
                          std::chrono::milliseconds sleepTime);

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

    PageableMemInfo getPageableMemInfo() const override;

    void schedulePagingVisitors(std::size_t bytesToEvict) override;

    EvictionRatios getEvictionRatios(
            const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
            std::size_t bytesToEvict) const override;

private:
    using RetainedAdapterList = folly::Synchronized<
            std::vector<std::shared_ptr<CrossBucketVisitorAdapter>>,
            std::mutex>;

    /**
     * Removes any references to completed adapters.
     *
     * We might keep up to numConcurrentPagers of these alive at any time after
     * they have completed.
     */
    void releaseCompletedAdapters(
            typename RetainedAdapterList::DataType& adapters);

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
     * Adapter instances which should be retained (not destroyed) until
     * completed. The adapters are not tasks themselves, but they own tasks
     * and have callbacks from these tasks, so if we don't retain the
     * shared_ptrs somewhere, we will get a use-after-free when a task calls
     * back into a destroyed adapter. We might keep up to numConcurrentPagers of
     * these alive at any time after they have completed.
     *
     * We remove completed instances by calling releaseCompletedAdapters when
     * appropriate (we have no callback mechanism to know exactly when an
     * adapter has completed).
     */
    RetainedAdapterList retainedAdapters;
};