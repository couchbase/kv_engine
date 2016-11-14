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

#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "kvbucket.h"
#include "ep_engine.h"
#include "tapconnmap.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <string>
#include <utility>

#include <phosphor/phosphor.h>


static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

enum pager_type_t {
    ITEM_PAGER,
    EXPIRY_PAGER
};

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public VBucketVisitor,
                      public HashTableVisitor {
public:

    /**
     * Construct a PagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param pcnt percentage of objects to attempt to evict (0-1)
     * @param sfin pointer to a bool to be set to true after run completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket
     *              visits
     * @param bias active vbuckets eviction probability bias multiplier (0-1)
     * @param phase pointer to an item_pager_phase to be set
     */
    PagingVisitor(KVBucket& s, EPStats &st, double pcnt,
                  std::shared_ptr<std::atomic<bool>> &sfin, pager_type_t caller,
                  bool pause, double bias,
                  std::atomic<item_pager_phase>* phase) :
        store(s), stats(st), percent(pcnt),
        activeBias(bias), ejected(0),
        startTime(ep_real_time()), stateFinalizer(sfin), owner(caller),
        canPause(pause), completePhase(true),
        wasHighMemoryUsage(s.isMemoryUsageTooHigh()),
        taskStart(gethrtime()), pager_phase(phase) {}

    void visit(StoredValue *v) override {
        // Delete expired items for an active vbucket.
        bool isExpired = (currentBucket->getState() == vbucket_state_active) &&
            v->isExpired(startTime) && !v->isDeleted();
        if (isExpired || v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            expired.push_back(std::make_pair(currentBucket->getId(),
                                             v->getKey()));
            return;
        }

        // return if not ItemPager, which uses valid eviction percentage
        if (percent <= 0 || !pager_phase) {
            return;
        }

        // always evict unreferenced items, or randomly evict referenced item
        double r = *pager_phase == PAGING_UNREFERENCED ?
            1 :
            static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);

        if (*pager_phase == PAGING_UNREFERENCED &&
            v->getNRUValue() == MAX_NRU_VALUE) {
            doEviction(v);
        } else if (*pager_phase == PAGING_RANDOM &&
                   v->incrNRUValue() == MAX_NRU_VALUE &&
                   r <= percent) {
            doEviction(v);
        }
    }

    void visitBucket(RCPtr<VBucket> &vb) override {
        update();

        bool newCheckpointCreated = false;
        size_t removed = vb->checkpointManager.removeClosedUnrefCheckpoints(vb,
                                                         newCheckpointCreated);
        stats.itemsRemovedFromCheckpoints.fetch_add(removed);
        // If the new checkpoint is created, notify this event to the
        // corresponding paused TAP & DCP connections.
        if (newCheckpointCreated) {
            store.getEPEngine().getTapConnMap().notifyVBConnections(
                                                                   vb->getId());
            store.getEPEngine().getDcpConnMap().notifyVBConnections(
                                        vb->getId(),
                                        vb->checkpointManager.getHighSeqno());
        }

        // fast path for expiry item pager
        if (percent <= 0 || !pager_phase) {
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                vb->ht.visit(*this);
            }
            return;
        }

        // skip active vbuckets if active resident ratio is lower than replica
        double current = static_cast<double>(stats.getTotalMemoryUsed());
        double lower = static_cast<double>(stats.mem_low_wat);
        double high = static_cast<double>(stats.mem_high_wat);
        if (vb->getState() == vbucket_state_active && current < high &&
            store.getActiveResidentRatio() <
            store.getReplicaResidentRatio())
        {
            return;
        }

        if (current > lower) {
            double p = (current - static_cast<double>(lower)) / current;
            adjustPercent(p, vb->getState());
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                vb->ht.visit(*this);
            }

        } else { // stop eviction whenever memory usage is below low watermark
            completePhase = false;
        }
    }

    void update() {
        store.deleteExpiredItems(expired, EXP_BY_PAGER);

        if (numEjected() > 0) {
            LOG(EXTENSION_LOG_INFO, "Paged out %ld values", numEjected());
        }

        size_t num_expired = expired.size();
        if (num_expired > 0) {
            LOG(EXTENSION_LOG_INFO, "Purged %ld expired items", num_expired);
        }

        ejected = 0;
        expired.clear();
    }

    bool pauseVisitor() override {
        size_t queueSize = stats.diskQueueSize.load();
        return canPause && queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
    }

    void complete() override {
        update();

        hrtime_t elapsed_time = (gethrtime() - taskStart) / 1000;
        if (owner == ITEM_PAGER) {
            stats.itemPagerHisto.add(elapsed_time);
        } else if (owner == EXPIRY_PAGER) {
            stats.expiryPagerHisto.add(elapsed_time);
        }

        bool inverse = false;
        (*stateFinalizer).compare_exchange_strong(inverse, true);

        if (pager_phase && completePhase) {
            if (*pager_phase == PAGING_UNREFERENCED) {
                *pager_phase = PAGING_RANDOM;
            } else {
                *pager_phase = PAGING_UNREFERENCED;
            }
        }

        // Wake up any sleeping backfill tasks if the memory usage is lowered
        // below the high watermark as a result of checkpoint removal.
        if (wasHighMemoryUsage && !store.isMemoryUsageTooHigh()) {
            store.getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
        }
    }

    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() { return ejected; }

private:
    void adjustPercent(double prob, vbucket_state_t state) {
        if (state == vbucket_state_replica ||
            state == vbucket_state_dead)
        {
            // replica items should have higher eviction probability
            double p = prob*(2 - activeBias);
            percent = p < 0.9 ? p : 0.9;
        } else {
            // active items have lower eviction probability
            percent = prob*activeBias;
        }
    }

    void doEviction(StoredValue *v) {
        item_eviction_policy_t policy = store.getItemEvictionPolicy();
        std::string key = v->getKey();

        if (currentBucket->ht.unlocked_ejectItem(v, policy)) {
            ++ejected;

            /**
             * For FULL EVICTION MODE, add all items that are being
             * evicted to the corresponding bloomfilter.
             */
            if (policy == FULL_EVICTION) {
                currentBucket->addToFilter(key);
            }
        }
    }

    std::list<std::pair<uint16_t, std::string> > expired;

    KVBucket& store;
    EPStats &stats;
    double percent;
    double activeBias;
    size_t ejected;
    time_t startTime;
    std::shared_ptr<std::atomic<bool>> stateFinalizer;
    pager_type_t owner;
    bool canPause;
    bool completePhase;
    bool wasHighMemoryUsage;
    hrtime_t taskStart;
    std::atomic<item_pager_phase>* pager_phase;
    RCPtr<VBucket> currentBucket;
};

ItemPager::ItemPager(EventuallyPersistentEngine *e, EPStats &st) :
    GlobalTask(e, TaskId::ItemPager, 10, false),
    engine(e),
    stats(st),
    available(new std::atomic<bool>(true)),
    phase(PAGING_UNREFERENCED),
    doEvict(false) { }

bool ItemPager::run(void) {
    TRACE_EVENT0("ep-engine/task", "ItemPager");
    KVBucket* kvBucket = engine->getKVBucket();
    double current = static_cast<double>(stats.getTotalMemoryUsed());
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);
    double sleepTime = 5;

    if (current <= lower) {
        doEvict = false;
    }

    bool inverse = true;
    if (((current > upper) || doEvict) &&
        (*available).compare_exchange_strong(inverse, false)) {
        if (kvBucket->getItemEvictionPolicy() == VALUE_ONLY) {
            doEvict = true;
        }

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        std::stringstream ss;
        ss << "Using " << stats.getTotalMemoryUsed()
           << " bytes of memory, paging out %0f%% of items." << std::endl;
        LOG(EXTENSION_LOG_INFO, ss.str().c_str(), (toKill*100.0));

        // compute active vbuckets evicition bias factor
        Configuration &cfg = engine->getConfiguration();
        size_t activeEvictPerc = cfg.getPagerActiveVbPcnt();
        double bias = static_cast<double>(activeEvictPerc) / 50;

        std::shared_ptr<PagingVisitor> pv(new PagingVisitor(*kvBucket, stats,
                                                            toKill, available,
                                                            ITEM_PAGER, false,
                                                            bias, &phase));
        kvBucket->visit(pv, "Item pager", NONIO_TASK_IDX,
                        TaskId::ItemPagerVisitor);
    }

    snooze(sleepTime);
    return true;
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

        std::shared_ptr<PagingVisitor> pv(new PagingVisitor(*kvBucket, stats,
                                                            -1, available,
                                                            EXPIRY_PAGER, true,
                                                            1, NULL));
        // track spawned tasks for shutdown..
        kvBucket->visit(pv, "Expired item remover", NONIO_TASK_IDX,
                        TaskId::ExpiredItemPagerVisitor, 10);
    }
    snooze(sleepTime);
    updateExpPagerTime(sleepTime);

    return true;
}

void ExpiredItemPager::updateExpPagerTime(double sleepSecs) {
    struct timeval _waketime;
    gettimeofday(&_waketime, NULL);
    _waketime.tv_sec += sleepSecs;
    stats.expPagerTime.store(_waketime.tv_sec);
}
