/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "quota_sharing_item_pager.h"

#include "bucket_logger.h"
#include "cross_bucket_visitor_adapter.h"
#include "ep_engine.h"
#include "ep_engine_group.h"
#include "item.h"
#include "kv_bucket.h"
#include "mfu_only_item_eviction.h"
#include "paging_visitor.h"
#include "vb_visitors.h"
#include "vbucket.h"
#include <folly/portability/Unistd.h>
#include <platform/semaphore.h>
#include <numeric>

/**
 * The visitor used by the QuotaSharingItemPager. Considers sum(low_wat) when
 * deciding whether to stop visiting vBuckets.
 */
class QuotaSharingItemPagingVisitor : public ItemPagingVisitor {
public:
    QuotaSharingItemPagingVisitor(
            QuotaSharingItemPager& pager,
            KVBucket& s,
            EPStats& st,
            std::unique_ptr<ItemEvictionStrategy> strategy,
            bool pause,
            const VBucketFilter& vbFilter)
        : ItemPagingVisitor(
                  s, st, std::move(strategy), nullptr, pause, vbFilter),
          pager(pager) {
    }

    bool shouldStopPaging() const override {
        // Override the stop condition so we stop when sum(mem_used) <= sum(lwm)
        auto memInfo = pager.getPageableMemInfo();
        return memInfo.current <= memInfo.lower;
    }

    ExecutionState shouldInterrupt() override {
        if (shouldStopPaging()) {
            return ExecutionState::Stop;
        }
        return ItemPagingVisitor::shouldInterrupt();
    }

private:
    QuotaSharingItemPager& pager;
};

QuotaSharingItemPager::QuotaSharingItemPager(
        ServerBucketIface& bucketApi,
        EPEngineGroup& group,
        Taskable& t,
        std::function<size_t()> getNumConcurrentPagers,
        std::function<std::chrono::milliseconds()> getSleepTime)
    : ItemPager(t, getNumConcurrentPagers(), getSleepTime()),
      bucketApi(bucketApi),
      group(group),
      getNumConcurrentPagers(std::move(getNumConcurrentPagers)),
      getSleepTimeCb(std::move(getSleepTime)) {
}

std::chrono::microseconds QuotaSharingItemPager::getSleepTime() const {
    return getSleepTimeCb();
}

std::string QuotaSharingItemPager::getDescription() const {
    return "Paging out items (quota sharing).";
}

std::chrono::microseconds QuotaSharingItemPager::maxExpectedDuration() const {
    // 25ms seems like a fair timeslice for the pager to create and schedule
    // paging visitors for all buckets.
    return std::chrono::milliseconds(25);
}

ItemPager::PageableMemInfo QuotaSharingItemPager::getPageableMemInfo() const {
    ItemPager::PageableMemInfo memInfo;
    for (const auto& handle : group.getActive()) {
        auto& ep = dynamic_cast<EventuallyPersistentEngine&>(*handle.get());
        memInfo.current += ep.getKVBucket()->getPageableMemCurrent();
        memInfo.lower += ep.getKVBucket()->getPageableMemLowWatermark();
        memInfo.upper += ep.getKVBucket()->getPageableMemHighWatermark();
    }
    return memInfo;
}

size_t QuotaSharingItemPager::getEvictableMemForState(
        const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
        PermittedVBStates states) const {
    return std::accumulate(kvBuckets.begin(),
                           kvBuckets.end(),
                           0,
                           [this, &states](auto acc, KVBucket& kvBucket) {
                               return acc + getEvictableBytes(kvBucket, states);
                           });
}

EvictionRatios QuotaSharingItemPager::getEvictionRatios(
        const std::vector<std::reference_wrapper<KVBucket>>& kvBuckets,
        const std::size_t bytesToEvict) const {
    Expects(!kvBuckets.empty());

    size_t replicaEvictableMem =
            getEvictableMemForState(kvBuckets, {vbucket_state_replica});

    auto remainingBytesToEvict = gsl::narrow<ssize_t>(bytesToEvict);
    double replicaEvictionRatio = 0.0;
    if (replicaEvictableMem) {
        // try evict from replicas first if we can
        replicaEvictionRatio = std::min(
                1.0, double(remainingBytesToEvict) / replicaEvictableMem);
        remainingBytesToEvict -= replicaEvictableMem;
    }

    double activeAndPendingEvictionRatio = 0.0;
    if (remainingBytesToEvict > 0) {
        // replicas are not sufficient (or are not eligible for eviction if
        // ephemeral). Not enough memory can be reclaimed from them to
        // reach the low watermark.
        // Consider active and pending vbuckets too.
        // active and pending share an eviction ratio, it need only be
        // set once
        size_t activePendingEvictableMem = getEvictableMemForState(
                kvBuckets, {vbucket_state_active, vbucket_state_pending});
        activeAndPendingEvictionRatio = std::min(
                1.0, double(remainingBytesToEvict) / activePendingEvictableMem);
    }

    return {activeAndPendingEvictionRatio, replicaEvictionRatio};
}

static MFUOnlyItemEviction::Thresholds createUpfrontEvictionThresholds(
        const std::vector<std::reference_wrapper<KVBucket>> kvBuckets,
        EvictionRatios evictionRatios) {
    MFUHistogram activePendingMFUHist;
    MFUHistogram replicaMFUHist;

    for (KVBucket& kvBucket : kvBuckets) {
        for (auto& vbucket : kvBucket.getVBuckets()) {
            switch (vbucket.getState()) {
            case vbucket_state_active:
            case vbucket_state_pending:
                activePendingMFUHist += vbucket.getEvictableMFUHistogram();
                break;
            case vbucket_state_replica:
                replicaMFUHist += vbucket.getEvictableMFUHistogram();
                break;
            case vbucket_state_dead:
                break;
            }
        }
    }
    MFUOnlyItemEviction::Thresholds thresholds;

    if (evictionRatios.activeAndPending) {
        // if the ratio is _exactly_ zero, we don't want to evict anything,
        // so don't set the (optional) threshold. Note that this is
        // distinct from setting the threshold to 0, which would still evict
        // items with MFU == 0.
        thresholds.activePending = activePendingMFUHist.getValueAtPercentile(
                evictionRatios.activeAndPending * 100);
    }

    if (evictionRatios.replica) {
        thresholds.replica = replicaMFUHist.getValueAtPercentile(
                evictionRatios.replica * 100);
    }

    return thresholds;
}

void QuotaSharingItemPager::schedulePagingVisitors(std::size_t bytesToEvict) {
    auto handles = group.getActive();

    // Create a vector of KVBuckets which is easier to work with
    std::vector<std::reference_wrapper<KVBucket>> kvBuckets;
    kvBuckets.reserve(handles.size());
    for (const auto& engine : handles) {
        kvBuckets.emplace_back(*engine->getKVBucket());
    }

    // If we have value eviction buckets, it is less likely that we
    // reach the low_wat the first time we run.
    if (std::any_of(kvBuckets.begin(), kvBuckets.end(), [](KVBucket& store) {
            return store.getItemEvictionPolicy() == EvictionPolicy::Value;
        })) {
        // Set doEvict=true so this task gets rescheduled until low_wat is
        // reached.
        doEvict = true;
    }

    for (KVBucket& kvBucket : kvBuckets) {
        kvBucket.getEPEngine().getEpStats().pagerRuns++;
    }

    auto evictionRatios = getEvictionRatios(kvBuckets, bytesToEvict);

    std::size_t estimatedTotalMemUsed = std::accumulate(
            kvBuckets.begin(),
            kvBuckets.end(),
            0,
            [](size_t acc, KVBucket& kvBucket) {
                return acc + kvBucket.getEPEngine()
                                     .getEpStats()
                                     .getEstimatedTotalMemoryUsed();
            });
    EP_LOG_DEBUG(
            "Using {} bytes of memory, paging out {}% of active and "
            "pending items, {}% of replica items, from {} buckets.",
            estimatedTotalMemUsed,
            (evictionRatios.activeAndPending * 100.0),
            (evictionRatios.replica * 100.0),
            kvBuckets.size());

    PermittedVBStates statesToEvictFrom = getStatesForEviction(evictionRatios);

    // The maximum expected duration for the visitor task's run() to run.
    // We only visit a single vBucket per run, so set it to something low.
    // 25ms is the maxChunkDuration we use for the CappedDurationVisitor used
    // which is applied for the StrictQuotaItemPager visits, so it is likely
    // a reasonable value, if perhaps an overestimate.
    const auto maxExpectedDurationForVisitorTask =
            std::chrono::milliseconds(25);

    auto thresholds =
            createUpfrontEvictionThresholds(kvBuckets, evictionRatios);

    // Detect changes in the number of concurrent pagers
    auto newNumConcurrentPagers = getNumConcurrentPagers();
    if (numConcurrentPagers != newNumConcurrentPagers) {
        EP_LOG_INFO(
                "Changing the number of concurrent pagers for "
                "QuotaSharingItemPager from {} to {}",
                numConcurrentPagers,
                newNumConcurrentPagers);
        numConcurrentPagers = newNumConcurrentPagers;
        pagerSemaphore->setCapacity(newNumConcurrentPagers);
    }

    for (size_t i = 0; i < numConcurrentPagers; i++) {
        CrossBucketVisitorAdapter::VisitorMap visitors;
        for (const auto& engine : handles) {
            // Create another handle to the same bucket to insert into the map
            auto duplicateHandle = bucketApi.tryAssociateBucket(engine.get());
            if (!duplicateHandle) {
                // While the original handle prevents the bucket from being
                // destroyed, it does not prevent it from going through
                // a state transition (Pausing, Destroying states). In that
                // case, associating will fail -- just ignore the bucket.
                continue;
            }

            auto& kvBucket = *engine->getKVBucket();
            auto filter = createVBucketFilter(kvBucket, statesToEvictFrom);
            // Distribute the buckets so we have the required number of
            // visitors per bucket, each visitor scheduled as part of a
            // different CrossBucketVisitorAdapter.
            if (filter->size() >= numConcurrentPagers) {
                filter = filter->slice(i, /* stride */ numConcurrentPagers);
            } else if (i < filter->size()) {
                // There is no way to slice the filter if it matches less than
                // numConcurrentPagers vBuckets. Give each pager one vBucket.
                auto vbidIt = filter->getVBSet().begin();
                std::advance(vbidIt, i);
                filter = VBucketFilter(std::set{*vbidIt});
            } else {
                continue;
            }

            auto pv = std::make_unique<QuotaSharingItemPagingVisitor>(
                    *this,
                    kvBucket,
                    engine->getEpStats(),
                    std::make_unique<MFUOnlyItemEviction>(thresholds),
                    true, /* allow pausing between vbuckets */
                    *filter);

            visitors.emplace_back(
                    std::make_pair(std::move(*duplicateHandle), std::move(pv)));
        }

        auto visitorAdapter = std::make_shared<CrossBucketVisitorAdapter>(
                bucketApi,
                CrossBucketVisitorAdapter::ScheduleOrder::RoundRobin,
                TaskId::ItemPagerVisitor,
                "Item pager (quota sharing)",
                maxExpectedDurationForVisitorTask,
                pagerSemaphore);
        visitorAdapter->scheduleNow(std::move(visitors));
    }
}
