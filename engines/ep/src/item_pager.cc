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

#include "item_pager.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "executorpool.h"
#include "item.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "kv_bucket_iface.h"
#include "paging_visitor.h"

#include <folly/lang/Assume.h>
#include <platform/platform_time.h>

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <string>
#include <utility>

#include <phosphor/phosphor.h>
#include <memory>

double EvictionRatios::getForState(vbucket_state_t state) {
    switch (state) {
    case vbucket_state_replica:
        return replica;
    case vbucket_state_active:
    case vbucket_state_pending:
        return activeAndPending;
    case vbucket_state_dead:
        return 0;
    }
    folly::assume_unreachable();
}

void EvictionRatios::setForState(vbucket_state_t state, double value) {
    switch (state) {
    case vbucket_state_replica:
        replica = value;
        return;
    case vbucket_state_active:
    case vbucket_state_pending:
        activeAndPending = value;
        return;
    case vbucket_state_dead:
        // no-op
        return;
    }
    folly::assume_unreachable();
}

ItemPager::ItemPager(EventuallyPersistentEngine& e, EPStats& st)
    : GlobalTask(&e, TaskId::ItemPager, 10, false),
      engine(e),
      stats(st),
      available(new std::atomic<bool>(true)),
      doEvict(false),
      sleepTime(std::chrono::milliseconds(
              e.getConfiguration().getPagerSleepTimeMs())),
      notified(false) {
}

bool ItemPager::run() {
    TRACE_EVENT0("ep-engine/task", "ItemPager");

    // Setup so that we will sleep before clearing notified.
    snooze(sleepTime.count());

    // Save the value of notified to be used in the "do we page check", it could
    // be that we've gone over HWM have been notified to run, then came back
    // down (e.g. 1 byte under HWM), we should still page in this scenario.
    // Notified would be false if we were woken by the periodic scheduler
    const bool wasNotified = notified;

    // Clear the notification flag before starting the task's actions
    notified.store(false);

    KVBucket* kvBucket = engine.getKVBucket();
    auto current = engine.getKVBucket()->getPageableMemCurrent();
    auto upper = engine.getKVBucket()->getPageableMemHighWatermark();
    auto lower = engine.getKVBucket()->getPageableMemLowWatermark();

    if (current <= lower) {
        // doEvict may have been set to ensure eviction would continue until the
        // low watermark was reached - it now has, so clear the flag.
        doEvict = false;
        // If a PagingVisitor were to be created now, it would visit vbuckets
        // but not try to evict anything. Stop now instead.
        return true;
    }

    if ((current > upper) || doEvict || wasNotified) {
        bool inverse = true;
        if (!(*available).compare_exchange_strong(inverse, false)) {
            // available != true, another PagingVisitor exists and is still
            // running. Don't create another.
            return true;
        }
        // Note: available is reset to true by PagingVisitor::complete()

        if (kvBucket->getItemEvictionPolicy() == EvictionPolicy::Value) {
            doEvict = true;
        }

        ++stats.pagerRuns;

        VBucketFilter replicaFilter;
        VBucketFilter activePendingFilter;

        for (auto vbid : kvBucket->getVBucketsInState(vbucket_state_replica)) {
            replicaFilter.addVBucket(vbid);
        }

        for (auto vbid : kvBucket->getVBucketsInState(vbucket_state_active)) {
            activePendingFilter.addVBucket(vbid);
        }
        for (auto vbid : kvBucket->getVBucketsInState(vbucket_state_pending)) {
            activePendingFilter.addVBucket(vbid);
        }

        ssize_t bytesToEvict = current - lower;

        const double replicaEvictableMem = getEvictableBytes(replicaFilter);
        const double activePendingEvictableMem =
                getEvictableBytes(activePendingFilter);

        double replicaEvictionRatio = 0.0;
        double activeAndPendingEvictionRatio = 0.0;

        if (kvBucket->canEvictFromReplicas()) {
            // try evict from replicas first if we can
            replicaEvictionRatio =
                    std::min(1.0, bytesToEvict / replicaEvictableMem);

            bytesToEvict -= replicaEvictableMem;
        }

        if (bytesToEvict > 0) {
            // replicas are not sufficient (or are not eligible for eviction if
            // ephemeral). Not enough memory can be reclaimed from them to
            // reach the low watermark.
            // Consider active and pending vbuckets too.
            // active and pending share an eviction ratio, it need only be
            // set once
            activeAndPendingEvictionRatio =
                    std::min(1.0, bytesToEvict / activePendingEvictableMem);
        }

        EP_LOG_DEBUG(
                "Using {} bytes of memory, paging out {}% of active and "
                "pending items, {}% of replica items.",
                stats.getEstimatedTotalMemoryUsed(),
                (activeAndPendingEvictionRatio * 100.0),
                (replicaEvictionRatio * 100.0));

        VBucketFilter filter;

        if (replicaEvictionRatio > 0.0) {
            filter = filter.filter_union(replicaFilter);
        }

        if (activeAndPendingEvictionRatio > 0.0) {
            filter = filter.filter_union(activePendingFilter);
        }

        // compute active vbuckets evicition bias factor
        const Configuration& cfg = engine.getConfiguration();

        auto pv = std::make_unique<PagingVisitor>(
                *kvBucket,
                stats,
                EvictionRatios{activeAndPendingEvictionRatio,
                               replicaEvictionRatio},
                available,
                ITEM_PAGER,
                false,
                filter,
                cfg.getItemEvictionAgePercentage(),
                cfg.getItemEvictionFreqCounterAgeThreshold());

        // p99.99 is ~200ms
        const auto maxExpectedDurationForVisitorTask =
                std::chrono::milliseconds(200);

        kvBucket->visitAsync(std::move(pv),
                             "Item pager",
                             TaskId::ItemPagerVisitor,
                             maxExpectedDurationForVisitorTask);
    }

    return true;
}

void ItemPager::scheduleNow() {
    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(getId());
    }
}

/**
 * Visitor used to aggregate how much memory could potentially be reclaimed
 * by evicting every eligible item from specified vbuckets
 */
class VBucketEvictableMemVisitor : public VBucketVisitor {
public:
    explicit VBucketEvictableMemVisitor(const VBucketFilter& filter)
        : filter(filter) {
    }

    void visitBucket(const VBucketPtr& vb) override {
        if (!filter.empty() && filter(vb->getId())) {
            totalEvictableMemory += vb->getPageableMemUsage();
        }
    }

    size_t getTotalEvictableMemory() const {
        return totalEvictableMemory;
    }

private:
    const VBucketFilter& filter;
    size_t totalEvictableMemory = 0;
};

size_t ItemPager::getEvictableBytes(const VBucketFilter& filter) const {
    KVBucket* kvBucket = engine.getKVBucket();

    VBucketEvictableMemVisitor visitor(filter);
    kvBucket->visit(visitor);

    return visitor.getTotalEvictableMemory();
}

ExpiredItemPager::ExpiredItemPager(EventuallyPersistentEngine *e,
                                   EPStats &st, size_t stime,
                                   ssize_t taskTime) :
    GlobalTask(e, TaskId::ExpiredItemPager,
               static_cast<double>(stime), false),
    engine(e),
    stats(st),
    sleepTime(static_cast<double>(stime)),
    available(new std::atomic<bool>(true)) {

    double initialSleep = sleepTime;
    if (taskTime != -1) {
        /*
         * Ensure task start time will always be within a range of (0, 23).
         * A validator is already in place in the configuration file.
         */
        size_t startTime = taskTime % 24;

        /*
         * The following logic calculates the amount of time this task
         * needs to sleep for initially so that it would wake up at the
         * designated task time, note that this logic kicks in only when
         * taskTime is set to value other than -1.
         * Otherwise this task will wake up periodically in a time
         * specified by sleeptime.
         */
        time_t now = ep_abs_time(ep_current_time());
        struct tm timeNow, timeTarget;
        cb_gmtime_r(&now, &timeNow);
        timeTarget = timeNow;
        if (timeNow.tm_hour >= (int)startTime) {
            timeTarget.tm_mday += 1;
        }
        timeTarget.tm_hour = startTime;
        timeTarget.tm_min = 0;
        timeTarget.tm_sec = 0;

        initialSleep = difftime(mktime(&timeTarget), mktime(&timeNow));
        snooze(initialSleep);
    }

    updateExpPagerTime(initialSleep);
}

bool ExpiredItemPager::run() {
    TRACE_EVENT0("ep-engine/task", "ExpiredItemPager");
    KVBucket* kvBucket = engine->getKVBucket();
    bool inverse = true;
    if ((*available).compare_exchange_strong(inverse, false)) {
        ++stats.expiryPagerRuns;

        VBucketFilter filter;
        Configuration& cfg = engine->getConfiguration();
        auto pv = std::make_unique<PagingVisitor>(
                *kvBucket,
                stats,
                EvictionRatios{0.0 /* active&pending */,
                               0.0 /* replica */}, // evict nothing
                available,
                EXPIRY_PAGER,
                true,
                filter,
                cfg.getItemEvictionAgePercentage(),
                cfg.getItemEvictionFreqCounterAgeThreshold());

        // p99.99 is ~50ms (same as ItemPager).
        const auto maxExpectedDurationForVisitorTask =
                std::chrono::milliseconds(50);

        // track spawned tasks for shutdown..
        kvBucket->visitAsync(std::move(pv),
                             "Expired item remover",
                             TaskId::ExpiredItemPagerVisitor,
                             maxExpectedDurationForVisitorTask);
    }
    snooze(sleepTime);
    updateExpPagerTime(sleepTime);

    return true;
}

void ExpiredItemPager::updateExpPagerTime(double sleepSecs) {
    struct timeval _waketime;
    gettimeofday(&_waketime, nullptr);
    _waketime.tv_sec += sleepSecs;
    stats.expPagerTime.store(_waketime.tv_sec);
}
