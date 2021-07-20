/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "paging_visitor.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "item.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "kv_bucket_iface.h"
#include <executor/executorpool.h>

#include <platform/semaphore.h>

#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <utility>

static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

PagingVisitor::PagingVisitor(KVBucket& s,
                             EPStats& st,
                             EvictionRatios evictionRatios,
                             std::shared_ptr<cb::Semaphore> pagerSemaphore,
                             pager_type_t caller,
                             bool pause,
                             const VBucketFilter& vbFilter,
                             size_t agePercentage,
                             size_t freqCounterAgeThreshold)
    : ejected(0),
      freqCounterThreshold(0),
      ageThreshold(0),
      store(s),
      stats(st),
      evictionRatios(evictionRatios),
      startTime(ep_real_time()),
      pagerSemaphore(std::move(pagerSemaphore)),
      owner(caller),
      canPause(pause),
      isBelowLowWaterMark(false),
      wasAboveBackfillThreshold(s.isMemUsageAboveBackfillThreshold()),
      taskStart(std::chrono::steady_clock::now()),
      agePercentage(agePercentage),
      freqCounterAgeThreshold(freqCounterAgeThreshold),
      maxCas(0) {
    setVBucketFilter(vbFilter);
}

bool PagingVisitor::visit(const HashTable::HashBucketLock& lh, StoredValue& v) {
    // The ItemPager should never touch a prepare. Prepares will be eventually
    // purged, but should not expire, whether completed or pending.
    if (v.isPending() || v.isPrepareCompleted()) {
        return true;
    }

    // Delete expired items for an active vbucket.
    bool isExpired = (currentBucket->getState() == vbucket_state_active) &&
                     v.isExpired(startTime) && !v.isDeleted();
    if (isExpired || v.isTempNonExistentItem() || v.isTempDeletedItem()) {
        std::unique_ptr<Item> it = v.toItem(currentBucket->getId());
        expired.push_back(*it.get());
        return true;
    }

    // We don't skip temp initial items (state_temp_init) here. This means that
    // we could evict one before a BG fetch completes. This is fine as it may be
    // desirable to do so under extremely high memory pressure and this ensures
    // that they are cleaned up should a BG fetch fail or get stuck for whatever
    // reason. Should a BG fetch complete after eviction of a temp initial item
    // it will return SUCCESS and notify the client to run the op again which
    // will rerun the BG fetch.
    const double evictionRatio =
            evictionRatios.getForState(currentBucket->getState());

    // return if not ItemPager which uses valid eviction percentage
    if (evictionRatio <= 0.0) {
        return true;
    }

    /*
     * We take a copy of the freqCounterValue because calling
     * doEviction can modify the value, and when we want to
     * add it to the histogram we want to use the original value.
     */
    auto storedValueFreqCounter = v.getFreqCounterValue();

    auto age = casToAge(v.getCas());

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

    // For replica vbuckets, young items are not protected from eviction.
    const bool isReplica = currentBucket->getState() == vbucket_state_replica;

    bool evicted = false;
    bool eligibleForPaging = false;

    if (belowMFUThreshold && (meetsAgeRequirements || isReplica)) {
        // try to evict, may fail if sv is not eligible due to being
        // pending/non-resident/dirty.
        evicted = eligibleForPaging = doEviction(lh, &v);
    } else {
        // just check eligibility without trying to evict
        eligibleForPaging = currentBucket->eligibleToPageOut(lh, v);
        if (eligibleForPaging) {
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

    if (eligibleForPaging) {
        itemEviction.addFreqAndAgeToHistograms(storedValueFreqCounter, age);

        // Whilst we are learning it is worth always updating the
        // threshold. We also want to update the threshold at periodic
        // intervals.
        if (itemEviction.isLearning() || itemEviction.isRequiredToUpdate()) {
            std::tie(freqCounterThreshold, ageThreshold) =
                    itemEviction.getThresholds(evictionRatio * 100.0,
                                               agePercentage);
        }
    }

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

    return true;
}

void PagingVisitor::visitBucket(const VBucketPtr& vb) {
    update();

    vb->checkpointManager->removeClosedUnrefCheckpoints(*vb);

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

    // skip active vbuckets if active resident ratio is lower than replica
    auto current = static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    auto lower = static_cast<double>(stats.mem_low_wat);

    if (current <= lower) {
        // stop eviction whenever memory usage is below low watermark
        isBelowLowWaterMark = true;
        return;
    }

    if (!vBucketFilter(vb->getId())) {
        return;
    }

    currentBucket = vb;
    maxCas = currentBucket->getMaxCas();
    itemEviction.reset();
    freqCounterThreshold = 0;

    // Percent of items in the hash table to be visited
    // between updating the interval.
    const double percentOfItems = 0.1;
    // Calculate the number of items to visit before updating
    // the interval
    uint64_t noOfItems = std::ceil(vb->getNumItems() * (percentOfItems * 0.01));
    uint64_t interval = (noOfItems > ItemEviction::learningPopulation)
                                ? noOfItems
                                : ItemEviction::learningPopulation;
    itemEviction.setUpdateInterval(interval);

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
    vb->checkpointManager->removeClosedUnrefCheckpoints(*vb);
}

void PagingVisitor::update() {
    store.deleteExpiredItems(expired, ExpireBy::Pager);

    if (numEjected() > 0) {
        EP_LOG_DEBUG("Paged out {} values", numEjected());
    }

    size_t num_expired = expired.size();
    if (num_expired > 0) {
        EP_LOG_DEBUG("Purged {} expired items", num_expired);
    }

    ejected = 0;
    expired.clear();
}

bool PagingVisitor::pauseVisitor() {
    if (!canPause) {
        return false;
    }
    bool shouldPause = CappedDurationVBucketVisitor::pauseVisitor();
    if (owner == EXPIRY_PAGER) {
        size_t queueSize = stats.diskQueueSize.load();
        shouldPause |= queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
    }
    return shouldPause;
}

void PagingVisitor::complete() {
    update();

    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - taskStart);
    if (owner == ITEM_PAGER) {
        stats.itemPagerHisto.add(elapsed_time);
    } else if (owner == EXPIRY_PAGER) {
        stats.expiryPagerHisto.add(elapsed_time);
    }

    // visitor done, return token so parent is aware when all visitors
    // have finished.
    pagerSemaphore->release();

    // Wake up any sleeping backfill tasks if the memory usage is lowered
    // below the backfill threshold as a result of item ejection.
    if (wasAboveBackfillThreshold &&
        !store.isMemUsageAboveBackfillThreshold()) {
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

std::function<bool(const Vbid&, const Vbid&)>
PagingVisitor::getVBucketComparator() const {
    // Get the pageable mem used and state of each vb _once_ and cache it.
    // Fetching these values repeatedly in the comparator could cause issues
    // as the values can change _during_ a given sort call.

    auto numVbs = store.getVBuckets().getSize();

    std::vector<bool> isReplica(numVbs);
    std::vector<size_t> memUsed(numVbs);

    for (const auto& vbid : store.getVBuckets().getBuckets()) {
        auto vb = store.getVBucket(vbid);
        if (vb) {
            isReplica[vbid.get()] = vb->getState() == vbucket_state_replica;
            memUsed[vbid.get()] = vb->getPageableMemUsage();
        }
    }

    return [isReplica = std::move(isReplica), memUsed = std::move(memUsed)](
                   const Vbid& a, const Vbid& b) mutable {
        // sort replicas before all other vbucket states, then sort by
        // pageableMemUsed
        return std::make_pair(isReplica[a.get()], memUsed[a.get()]) >
               std::make_pair(isReplica[b.get()], memUsed[b.get()]);
    };
}

bool PagingVisitor::doEviction(const HashTable::HashBucketLock& lh,
                               StoredValue* v) {
    auto policy = store.getItemEvictionPolicy();
    StoredDocKey key(v->getKey());

    if (currentBucket->pageOut(readHandle, lh, v)) {
        ++ejected;

        /**
         * For FULL EVICTION MODE, add all items that are being
         * evicted to the corresponding bloomfilter.
         */
        if (policy == ::EvictionPolicy::Full) {
            currentBucket->addToFilter(key);
        }
        // performed eviction so return true
        return true;
    }
    // did not perform eviction so return false
    return false;
}

uint64_t PagingVisitor::casToAge(uint64_t cas) const {
    uint64_t age = (maxCas > cas) ? (maxCas - cas) : 0;
    age = age >> ItemEviction::casBitsNotTime;
    return age;
}

void PagingVisitor::setUpHashBucketVisit() {
    // Grab a locked ReadHandle
    readHandle = currentBucket->lockCollections();
}

void PagingVisitor::tearDownHashBucketVisit() {
    // Unlock the readHandle. It can now never be locked again, and should
    // not be used until overwriting with a locked ReadHandle.
    readHandle.unlock();
}
