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

#include "checkpoint.h"
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

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <string>
#include <utility>

#include <phosphor/phosphor.h>
#include <platform/make_unique.h>


static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

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

    throw std::invalid_argument("EvictionRatios::getForState: state " +
                                std::to_string(static_cast<int>(state)));
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

    throw std::invalid_argument("EvictionRatios::setForState: state " +
                                std::to_string(static_cast<int>(state)));
}

PagingVisitor::PagingVisitor(KVBucket& s,
                             EPStats& st,
                             double pcnt,
                             std::shared_ptr<std::atomic<bool>>& sfin,
                             pager_type_t caller,
                             bool pause,
                             double bias,
                             const VBucketFilter& vbFilter,
                             std::atomic<item_pager_phase>* phase,
                             bool _isEphemeral,
                             size_t agePercentage,
                             size_t freqCounterAgeThreshold,
                             EvictionPolicy evictionPolicy,
                             EvictionRatios evictionRatios)
    : VBucketVisitor(vbFilter),
      ejected(0),
      freqCounterThreshold(0),
      ageThreshold(0),
      store(s),
      stats(st),
      percent(pcnt),
      activeBias(bias),
      startTime(ep_real_time()),
      stateFinalizer(sfin),
      owner(caller),
      canPause(pause),
      completePhase(true),
      wasHighMemoryUsage(s.isMemoryUsageTooHigh()),
      taskStart(ProcessClock::now()),
      pager_phase(phase),
      isEphemeral(_isEphemeral),
      agePercentage(agePercentage),
      freqCounterAgeThreshold(freqCounterAgeThreshold),
      maxCas(0),
      evictionPolicy(evictionPolicy),
      evictionRatios(evictionRatios) {
}

    bool PagingVisitor::visit(const HashTable::HashBucketLock& lh,
                              StoredValue& v) {
        // Delete expired items for an active vbucket.
        bool isExpired = (currentBucket->getState() == vbucket_state_active) &&
                         v.isExpired(startTime) && !v.isDeleted();
        if (isExpired || v.isTempNonExistentItem() || v.isTempDeletedItem()) {
            std::unique_ptr<Item> it = v.toItem(false, currentBucket->getId());
            expired.push_back(*it.get());
            return true;
        }

        // return if not ItemPager, which uses valid eviction percentage
        if (owner == EXPIRY_PAGER) {
            return true;
        }

        switch (evictionPolicy) {
        case EvictionPolicy::lru2Bit: {
            // always evict unreferenced items, or randomly evict referenced
            // item
            double r =
                    *pager_phase == PAGING_UNREFERENCED ?
                            1 :
                            static_cast<double>(std::rand()) /
                            static_cast<double>(RAND_MAX);

            if (*pager_phase == PAGING_UNREFERENCED && v.getNRUValue()
                    == MAX_NRU_VALUE) {
                doEviction(lh, &v);
            } else if (*pager_phase == PAGING_RANDOM
                    && v.incrNRUValue() == MAX_NRU_VALUE && r <= percent) {
                doEviction(lh, &v);
            }
            return true;
        }
        case EvictionPolicy::hifi_mfu: {
            // return if not ItemPager which uses valid eviction percentage
            const double evictionRatio =
                    evictionRatios.getForState(currentBucket->getState());

            if (evictionRatio <= 0.0) {
                return true;
            }

            /*
             * We take a copy of the freqCounterValue because calling
             * doEviction can modify the value, and when we want to
             * add it to the histogram we want to use the original value.
             */
            auto storedValueFreqCounter = v.getFreqCounterValue();
            bool evicted = true;

            /*
             * Calculate the age when the item was last stored / modified.
             * We do this by taking the item's current cas from the maxCas
             * (which is the maximum cas value of the current vbucket just
             * before we begin visiting all the items in the hash table).
             *
             * The time is actually stored in the top 48 bits of the cas
             * therefore we shift the age by casBitsNotTime.
             *
             * Note: If the item was written before we switched over to the
             * hybrid logical clock (HLC) (i.e. the item was written when the
             * bucket was 4.0/3.x etc...) then the cas value will be low and
             * so the item will appear very old.  However, this does not
             * matter as it just means that is likely to be evicted.
             */
            uint64_t age = (maxCas > v.getCas()) ? (maxCas - v.getCas()) : 0;
            age = age >> ItemEviction::casBitsNotTime;

            const bool belowMFUThreshold =
                    storedValueFreqCounter <= freqCounterThreshold;
            // age exceeds threshold (from age histogram, set by config param
            // item_eviction_age_percentage
            // OR
            // MFU is below threshold set by config param
            // item_eviction_freq_counter_age_threshold
            // Below this threshold the item is considered "cold" enough
            // to be evicted even if it is "young".
            const bool meetsAgeRequirements =
                    age >= ageThreshold ||
                    storedValueFreqCounter < freqCounterAgeThreshold;

            // For replica vbuckets, young items are not protected from
            // eviction.
            const bool isReplica =
                    currentBucket->getState() == vbucket_state_replica;

            if (belowMFUThreshold && (meetsAgeRequirements || isReplica)) {
                /*
                 * If the storedValue is eligible for eviction then add its
                 * frequency counter value to the histogram, otherwise add the
                 * maximum (255) to indicate that the storedValue cannot be
                 * evicted.
                 *
                 * By adding the maximum value for each storedValue that cannot
                 * be evicted we ensure that the histogram is biased correctly
                 * so that we get a frequency threshold that will remove the
                 * correct number of storedValue items.
                 */
                if (!doEviction(lh, &v)) {
                    evicted = false;
                    storedValueFreqCounter = std::numeric_limits<uint8_t>::max();
                }
            } else {
                evicted = false;
                // If the storedValue is NOT eligible for eviction then
                // we want to add the maximum value (255).
                if (!currentBucket->eligibleToPageOut(lh, v)) {
                    storedValueFreqCounter = std::numeric_limits<uint8_t>::max();
                } else {
                    /*
                     * MB-29333 - For items that we have visited and did not
                     * evict just because their frequency counter was too high,
                     * the frequency counter must be decayed by 1 to
                     * ensure that they will get evicted if repeatedly
                     * visited (and assuming their frequency counter is not
                     * incremented in between visits of the item pager).
                     */
                    if (storedValueFreqCounter > 0) {
                        v.setFreqCounterValue(storedValueFreqCounter - 1);
                    }
                }
            }
            itemEviction.addFreqAndAgeToHistograms(storedValueFreqCounter, age);

            if (evicted) {
                /**
                 * Note: We are not taking a reader lock on the vbucket state.
                 * Therefore it is possible that the stats could be slightly
                 * out.  However given that its just for stats we don't want
                 * to incur any performance cost associated with taking the
                 * lock.
                 */
                auto& frequencyValuesEvictedHisto =
                        ((currentBucket->getState() == vbucket_state_active) ||
                         (currentBucket->getState() == vbucket_state_pending))
                                ? stats.activeOrPendingFrequencyValuesEvictedHisto
                                : stats.replicaFrequencyValuesEvictedHisto;
                frequencyValuesEvictedHisto.addValue(storedValueFreqCounter);
            }

            // Whilst we are learning it is worth always updating the
            // threshold. We also want to update the threshold at periodic
            // intervals.
            if (itemEviction.isLearning() ||
                itemEviction.isRequiredToUpdate()) {
                const auto thresholds = itemEviction.getThresholds(
                        evictionRatio * 100.0, agePercentage);
                freqCounterThreshold = thresholds.first;
                ageThreshold = thresholds.second;
            }

            return true;
        }
        }
        throw std::invalid_argument("PagingVisitor::visit - "
                "EvictionPolicy is invalid");
    }

    void PagingVisitor::visitBucket(VBucketPtr& vb) {
        update();
        removeClosedUnrefCheckpoints(vb);

        // fast path for expiry item pager
        if (owner == EXPIRY_PAGER) {
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                // EvictionPolicy is not required when running expiry item
                // pager
                vb->ht.visit(*this);
            }
            return;
        }

        double current = store.getPageableMemCurrent();
        double lower = store.getPageableMemLowWatermark();

        // Non-ephemeral: skip active vbuckets if active resident ratio is
        // lower than replica.
        // (Ephemeral can _only_ evict from active so don't want to skip them!)
        if (!isEphemeral && vb->getState() == vbucket_state_active) {
            double high = store.getPageableMemHighWatermark();
            if (current < high && store.getActiveResidentRatio() <
                                          store.getReplicaResidentRatio()) {
                return;
            }
        }

        if (current > lower) {
            double p = 0;
            if (evictionPolicy == EvictionPolicy::lru2Bit) {
                p = (current - static_cast<double>(lower)) / current;
                adjustPercent(p, vb->getState());
            }

            LOG(EXTENSION_LOG_DEBUG,
                "PagingVisitor::visitBucket() vb:%d current:%f lower:%f p:%f "
                "percent:%f",
                vb->getId(),
                current,
                lower,
                p,
                percent);

            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                maxCas = currentBucket->getMaxCas();
                itemEviction.reset();
                freqCounterThreshold = 0;

                if (evictionPolicy == EvictionPolicy::hifi_mfu) {
                    // Percent of items in the hash table to be visited
                    // between updating the interval.
                    const double percentOfItems = 0.1;
                    // Calculate the number of items to visit before updating
                    // the interval
                    uint64_t noOfItems = std::ceil(vb->getNumItems() *
                                                   (percentOfItems * 0.01));
                    uint64_t interval =
                            (noOfItems > ItemEviction::learningPopulation)
                                    ? noOfItems
                                    : ItemEviction::learningPopulation;
                    itemEviction.setUpdateInterval(interval);
                }
                vb->ht.visit(*this);
                /**
                 * Note: We are not taking a reader lock on the vbucket state.
                 * Therefore it is possible that the stats could be slightly
                 * out.  However given that its just for stats we don't want
                 * to incur any performance cost associated with taking the
                 * lock.
                 */
                const bool isActiveOrPending =
                        ((currentBucket->getState() == vbucket_state_active) ||
                         (currentBucket->getState() == vbucket_state_pending));

                // Take a snapshot of the latest frequency histogram
                if (isActiveOrPending) {
                    stats.activeOrPendingFrequencyValuesSnapshotHisto.reset();
                    itemEviction.copyFreqHistogram(
                            stats.activeOrPendingFrequencyValuesSnapshotHisto);
                } else {
                    stats.replicaFrequencyValuesSnapshotHisto.reset();
                    itemEviction.copyFreqHistogram(
                            stats.replicaFrequencyValuesSnapshotHisto);
                }

                // We have just evicted all eligible items from the hash table
                // so we now want to reclaim the memory being used to hold
                // closed and unreferenced checkpoints in the vbucket, before
                // potentially moving to the next vbucket.
                removeClosedUnrefCheckpoints(vb);
            }

        } else { // stop eviction whenever memory usage is below low watermark
            completePhase = false;
        }
    }

    std::function<bool(const VBucket::id_type&, const VBucket::id_type&)>
    PagingVisitor::getVBucketComparator() const {
        // Get the pageable mem used and state of each vb _once_ and cache it.
        // Fetching these values repeatedly in the comparator could cause issues
        // as the values can change _during_ a given sort call.

        std::map<VBucket::id_type, std::pair<bool, size_t>>
                stateAndPageableMemUsed;

        for (const auto& vbid : store.getVBuckets().getBuckets()) {
            auto vb = store.getVBucket(vbid);
            if (vb) {
                stateAndPageableMemUsed[vbid] = {
                        vb->getState() == vbucket_state_replica,
                        vb->getPageableMemUsage()};
            }
        }

        // Capture initializers are C++14
        return [stateAndPageableMemUsed](const VBucket::id_type& a,
                                         const VBucket::id_type& b) mutable {
            // sort replicas before all other vbucket states, then sort by
            // pageableMemUsed
            return stateAndPageableMemUsed[a] > stateAndPageableMemUsed[b];
        };
    }

    void PagingVisitor::update() {
        store.deleteExpiredItems(expired, ExpireBy::Pager);

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

    bool PagingVisitor::pauseVisitor() {
        size_t queueSize = stats.diskQueueSize.load();
        return canPause && queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
    }

    void PagingVisitor::complete() {
        update();

        auto elapsed_time =
                std::chrono::duration_cast<std::chrono::microseconds>(
                        ProcessClock::now() - taskStart);
        if (owner == ITEM_PAGER) {
            stats.itemPagerHisto.add(elapsed_time);
        } else if (owner == EXPIRY_PAGER) {
            stats.expiryPagerHisto.add(elapsed_time);
        }

        bool inverse = false;
        (*stateFinalizer).compare_exchange_strong(inverse, true);

        if (evictionPolicy == EvictionPolicy::lru2Bit) {
            if (pager_phase && completePhase) {
                if (*pager_phase == PAGING_UNREFERENCED) {
                    *pager_phase = PAGING_RANDOM;
                } else if (*pager_phase == PAGING_RANDOM) {
                    *pager_phase = PAGING_UNREFERENCED;
                } else if (*pager_phase == REPLICA_ONLY) {
                    *pager_phase = ACTIVE_AND_PENDING_ONLY;
                } else if (*pager_phase == ACTIVE_AND_PENDING_ONLY &&
                           !isEphemeral) {
                    *pager_phase = REPLICA_ONLY;
                }
            }
        }

        // Wake up any sleeping backfill tasks if the memory usage is lowered
        // below the high watermark as a result of checkpoint removal.
        if (wasHighMemoryUsage && !store.isMemoryUsageTooHigh()) {
            store.getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
        }

        if (ITEM_PAGER == owner) {
            // Re-check memory which may wake up the ItemPager and schedule
            // a new PagingVisitor with the next phase/memory target etc...
            // This is done after we've signalled 'completion' by clearing
            // the stateFinalizer, which ensures the ItemPager doesn't just
            // ignore a request.
            store.checkAndMaybeFreeMemory();
        }
    }


    // Removes checkpoints that are both closed and unreferenced, thereby
    // freeing the associated memory.
    // @param vb  The vbucket whose eligible checkpoints are removed from.
    void PagingVisitor::removeClosedUnrefCheckpoints(VBucketPtr& vb) {
        bool newCheckpointCreated = false;
        size_t removed = vb->checkpointManager->removeClosedUnrefCheckpoints(
                *vb, newCheckpointCreated);
        stats.itemsRemovedFromCheckpoints.fetch_add(removed);
        // If the new checkpoint is created, notify this event to the
        // corresponding paused DCP connections.
        if (newCheckpointCreated) {
            store.getEPEngine().getDcpConnMap().notifyVBConnections(
                    vb->getId(), vb->checkpointManager->getHighSeqno());
        }
    }

    void PagingVisitor::adjustPercent(double prob, vbucket_state_t state) {
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

    bool PagingVisitor::doEviction(const HashTable::HashBucketLock& lh,
                                   StoredValue* v) {
        item_eviction_policy_t policy = store.getItemEvictionPolicy();
        StoredDocKey key(v->getKey());

        if (currentBucket->pageOut(lh, v)) {
            ++ejected;

            /**
             * For FULL EVICTION MODE, add all items that are being
             * evicted to the corresponding bloomfilter.
             */
            if (policy == FULL_EVICTION) {
                currentBucket->addToFilter(key);
            }
            // performed eviction so return true
            return true;
        }
        // did not perform eviction so return false
        return false;
    }


ItemPager::ItemPager(EventuallyPersistentEngine& e, EPStats& st)
    : GlobalTask(&e, TaskId::ItemPager, 10, false),
      engine(e),
      stats(st),
      available(new std::atomic<bool>(true)),
      phase(PAGING_UNREFERENCED),
      doEvict(false),
      sleepTime(std::chrono::milliseconds(
              e.getConfiguration().getPagerSleepTimeMs())),
      notified(false) {
}

bool ItemPager::run(void) {
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
    double current = engine.getKVBucket()->getPageableMemCurrent();
    double upper = engine.getKVBucket()->getPageableMemHighWatermark();
    double lower = engine.getKVBucket()->getPageableMemLowWatermark();

    // If we dynamically switch from the hifi_mfu policy to the 2-bit_lru
    // policy or vice-versa, we will not be in a valid phase. Therefore
    // reinitialise to the correct phase for the eviction policy.
    if (engine.getConfiguration().getHtEvictionPolicy() == "2-bit_lru") {
        if (phase != PAGING_UNREFERENCED && phase != PAGING_RANDOM) {
            phase = PAGING_UNREFERENCED;
        }
    }

    if (current <= lower) {
        doEvict = false;
    }

    bool inverse = true;
    if (((current > upper) || doEvict || wasNotified) &&
        (*available).compare_exchange_strong(inverse, false)) {
        if (kvBucket->getItemEvictionPolicy() == VALUE_ONLY) {
            doEvict = true;
        }

        ++stats.pagerRuns;

        // MB-40531: I'm backporting the change from a world (mad-hatter) where
        // only the hifi_mfu eviction policy exists. I'm making it simple: just
        // preserve the existing code for 2-bit_lru and rewrite the hifi_mfu
        // case as per MB-40531.

        Configuration& cfg = engine.getConfiguration();
        VBucketFilter filter;
        std::unique_ptr<PagingVisitor> pv;

        // Note: Percent to free and active bias factor used only in lru2Bit
        double percentToFree = 0;
        double activeBias = 0;

        // Note: Ratios used only in hifi_mru
        double replicaEvictionRatio = 0.0;
        double activeAndPendingEvictionRatio = 0.0;

        if (engine.getConfiguration().getHtEvictionPolicy() == "2-bit_lru") {
            percentToFree = (current - static_cast<double>(lower)) / current;
            activeBias = static_cast<double>(cfg.getPagerActiveVbPcnt()) / 50;

            std::stringstream ss;
            ss << "Using " << stats.getEstimatedTotalMemoryUsed()
               << " bytes of memory, paging out %0f%% of items." << std::endl;
            LOG(EXTENSION_LOG_INFO, ss.str().c_str(), (percentToFree * 100.0));

        } else { // hifi_mfu
            if (current <= lower) {
                // early exit - no need to run a paging visitor
                return true;
            }

            VBucketFilter replicaFilter;
            VBucketFilter activePendingFilter;

            for (auto vbid :
                 kvBucket->getVBucketsInState(vbucket_state_replica)) {
                replicaFilter.addVBucket(vbid);
            }

            for (auto vbid :
                 kvBucket->getVBucketsInState(vbucket_state_active)) {
                activePendingFilter.addVBucket(vbid);
            }
            for (auto vbid :
                 kvBucket->getVBucketsInState(vbucket_state_pending)) {
                activePendingFilter.addVBucket(vbid);
            }

            ssize_t bytesToEvict = current - lower;

            const double replicaEvictableMem = getEvictableBytes(replicaFilter);
            const double activePendingEvictableMem =
                    getEvictableBytes(activePendingFilter);

            if (kvBucket->canEvictFromReplicas()) {
                // try evict from replicas first if we can
                replicaEvictionRatio =
                        std::min(1.0, bytesToEvict / replicaEvictableMem);

                bytesToEvict -= replicaEvictableMem;
            }

            if (bytesToEvict > 0) {
                // replicas are not sufficient (or are not eligible for eviction
                // if ephemeral). Not enough memory can be reclaimed from them
                // to reach the low watermark. Consider active and pending
                // vbuckets too. active and pending share an eviction ratio, it
                // need only be set once
                activeAndPendingEvictionRatio =
                        std::min(1.0, bytesToEvict / activePendingEvictableMem);
            }

            LOG(EXTENSION_LOG_DEBUG,
                "Using %lu bytes of memory, paging out %f%% of active and "
                "pending items, %f%% of replica items.",
                stats.getEstimatedTotalMemoryUsed(),
                (activeAndPendingEvictionRatio * 100.0),
                (replicaEvictionRatio * 100.0));

            if (replicaEvictionRatio > 0.0) {
                filter = filter.filter_union(replicaFilter);
            }

            if (activeAndPendingEvictionRatio > 0.0) {
                filter = filter.filter_union(activePendingFilter);
            }
        }

        pv = std::make_unique<PagingVisitor>(
                *kvBucket,
                stats,
                percentToFree,
                available,
                ITEM_PAGER,
                false,
                activeBias,
                filter,
                &phase,
                cfg.getBucketType() == "ephemeral",
                cfg.getItemEvictionAgePercentage(),
                cfg.getItemEvictionFreqCounterAgeThreshold(),
                cfg.getHtEvictionPolicy() == "2-bit_lru"
                        ? PagingVisitor::EvictionPolicy::lru2Bit
                        : PagingVisitor::EvictionPolicy::hifi_mfu,
                EvictionRatios{activeAndPendingEvictionRatio,
                               replicaEvictionRatio});

        // p99.99 is ~200ms
        const auto maxExpectedDuration = std::chrono::milliseconds(200);
        kvBucket->visit(std::move(pv),
                        "Item pager",
                        TaskId::ItemPagerVisitor,
                        0, // sleepTime
                        maxExpectedDuration);
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

    void visitBucket(VBucketPtr& vb) override {
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
        PagingVisitor::EvictionPolicy evictionPolicy =
                (cfg.getHtEvictionPolicy() == "2-bit_lru")
                        ? PagingVisitor::EvictionPolicy::lru2Bit
                        : PagingVisitor::EvictionPolicy::hifi_mfu;
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
                cfg.getItemEvictionFreqCounterAgeThreshold(),
                evictionPolicy,
                EvictionRatios{0.0 /* active&pending */,
                               0.0 /* replica */} /*evict nothing*/);

        // p99.99 is ~50ms (same as ItemPager).
        const auto maxExpectedDuration = std::chrono::milliseconds(50);

        // track spawned tasks for shutdown..
        kvBucket->visit(std::move(pv),
                        "Expired item remover",
                        TaskId::ExpiredItemPagerVisitor,
                        10,
                        maxExpectedDuration);
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
