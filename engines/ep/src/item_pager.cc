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

ItemPager::ItemPager(EventuallyPersistentEngine& e, EPStats& st)
    : GlobalTask(&e, TaskId::ItemPager, 10, false),
      engine(e),
      stats(st),
      available(new std::atomic<bool>(true)),
      phase(REPLICA_ONLY),
      doEvict(false),
      sleepTime(std::chrono::milliseconds(
              e.getConfiguration().getPagerSleepTimeMs())),
      notified(false) {
    // For the hifi_mfu algorithm if a couchbase/persistent bucket we
    // want to start visiting the replica vbucket first.  However for
    // ephemeral we do not evict from replica vbuckets and therefore
    // we start with active and pending vbuckets.
    phase = (engine.getConfiguration().getBucketType() == "persistent")
                    ? REPLICA_ONLY
                    : ACTIVE_AND_PENDING_ONLY;
}

bool ItemPager::run(void) {
    TRACE_EVENT0("ep-engine/task", "ItemPager");

    // Setup so that we will sleep before clearing notified.
    snooze(sleepTime.count());

    // Save the value of notified to be used in the "do we page check", it could
    // be that we've gone over HWM have been notified to run, then came back
    // down (e.g. 1 byte under HWM), we should still page in this scenario.
    // Notified would be false if we were woken by the periodic scheduler
    bool wasNotified = notified;

    // Clear the notification flag before starting the task's actions
    notified.store(false);

    KVBucket* kvBucket = engine.getKVBucket();
    double current = static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);

    if (current <= lower) {
        doEvict = false;
    }

    bool inverse = true;
    if (((current > upper) || doEvict || wasNotified) &&
        (*available).compare_exchange_strong(inverse, false)) {
        if (kvBucket->getItemEvictionPolicy() == EvictionPolicy::Value) {
            doEvict = true;
        }

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        EP_LOG_DEBUG("Using {} bytes of memory, paging out {} of items.",
                     stats.getEstimatedTotalMemoryUsed(),
                     (toKill * 100.0));

        // compute active vbuckets evicition bias factor
        Configuration& cfg = engine.getConfiguration();
        size_t activeEvictPerc = cfg.getPagerActiveVbPcnt();
        double bias = static_cast<double>(activeEvictPerc) / 50;

        VBucketFilter filter;
        // For the hifi_mfu algorithm use the phase to filter which vbuckets
        // we want to visit (either replica or active/pending vbuckets).
        vbucket_state_t state;
        if (phase == REPLICA_ONLY) {
            state = vbucket_state_replica;
        } else if (phase == ACTIVE_AND_PENDING_ONLY) {
            state = vbucket_state_active;
            auto acceptableVBs = kvBucket->getVBucketsInState(state);
            for (auto vb : acceptableVBs) {
                filter.addVBucket(vb);
            }
            state = vbucket_state_pending;
        } else {
            throw std::invalid_argument(
                    "ItemPager::run - "
                    "phase is invalid for hifi_mfu eviction algorithm");
        }
        auto acceptableVBs = kvBucket->getVBucketsInState(state);
        for (auto vb : acceptableVBs) {
            filter.addVBucket(vb);
        }

        bool isEphemeral = (cfg.getBucketType() == "ephemeral");

        auto pv = std::make_unique<PagingVisitor>(
                *kvBucket,
                stats,
                toKill,
                available,
                ITEM_PAGER,
                false,
                bias,
                filter,
                &phase,
                isEphemeral,
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

bool ExpiredItemPager::run(void) {
    TRACE_EVENT0("ep-engine/task", "ExpiredItemPager");
    KVBucket* kvBucket = engine->getKVBucket();
    bool inverse = true;
    if ((*available).compare_exchange_strong(inverse, false)) {
        ++stats.expiryPagerRuns;

        VBucketFilter filter;
        Configuration& cfg = engine->getConfiguration();
        bool isEphemeral =
                (engine->getConfiguration().getBucketType() == "ephemeral");
        auto pv = std::make_unique<PagingVisitor>(
                *kvBucket,
                stats,
                -1,
                available,
                EXPIRY_PAGER,
                true,
                1,
                filter,
                /* pager_phase */ nullptr,
                isEphemeral,
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
