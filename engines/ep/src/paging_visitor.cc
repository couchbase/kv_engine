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
#include "kv_bucket.h"
#include "kv_bucket_iface.h"
#include "learning_age_and_mfu_based_eviction.h"
#include "vbucket.h"
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
                             std::shared_ptr<cb::Semaphore> pagerSemaphore,
                             bool pause,
                             const VBucketFilter& vbFilter)
    : store(s),
      stats(st),
      startTime(ep_real_time()),
      pagerSemaphore(std::move(pagerSemaphore)),
      isPausingAllowed(pause),
      pagingVisitorPauseCheckCount(s.getEPEngine()
                                           .getConfiguration()
                                           .getPagingVisitorPauseCheckCount()),
      wasAboveBackfillThreshold(s.isMemUsageAboveBackfillThreshold()),
      taskStart(std::chrono::steady_clock::now()) {
    setVBucketFilter(vbFilter);
}

bool PagingVisitor::shouldVisit(const HashTable::HashBucketLock& lh,
                                StoredValue& v) {
    // We should never touch a prepare. Prepares will be eventually
    // purged, but should not expire, whether completed or pending.
    if (v.isPending() || v.isPrepareCompleted()) {
        return false;
    }

    if (!currentBucket->canEvict()) {
        // current vbucket is not permitted to evict (e.g., an ephemeral
        // replica vbucket).
        // This may depend on vbucket state, but the state lock is held for
        // each hash bucket visit (see setUpHashBucketVisit) so no additional
        // locking is required here.
        return false;
    }

    // MB-57049: Clean up temp items when visited by the expiry pager.
    // We hit this code path before the expiration/eviction code paths, so
    // by returning here, we avoid updating the expiration/ejection counts.
    // Only the temp item count will be updated as a result of this clean up.
    if (currentBucket->ht.cleanupIfTemporaryItem(lh, v)) {
        return false;
    }

    return true;
}

bool PagingVisitor::maybeExpire(StoredValue& v) {
    auto vbState = currentBucket->getState();

    // Delete expired items for an active vbucket.
    bool isExpired = (vbState == vbucket_state_active) &&
                     v.isExpired(startTime) && !v.isDeleted();
    if (isExpired) {
        std::unique_ptr<Item> it = v.toItem(currentBucket->getId());
        expired.push_back(*it.get());
        return true;
    }
    return false;
}

void PagingVisitor::update() {
    // Process expirations
    if (!expired.empty()) {
        const auto startTime = ep_real_time();
        for (auto& item : expired) {
            store.processExpiredItem(item, startTime, ExpireBy::Pager);
        }
        EP_LOG_DEBUG("Purged {} expired items", expired.size());
        expired.clear();
    }
}

void PagingVisitor::complete() {
    update();

    if (pagerSemaphore) {
        // visitor done, return token so parent is aware when all visitors
        // have finished.
        pagerSemaphore->release();
    }

    // Wake up any sleeping backfill tasks if the memory usage is lowered
    // below the backfill threshold as a result of item ejection.
    if (wasAboveBackfillThreshold &&
        !store.isMemUsageAboveBackfillThreshold()) {
        store.getEPEngine().getDcpConnMap().notifyBackfillManagerTasks();
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

std::chrono::microseconds PagingVisitor::getElapsedTime() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - taskStart);
}

void PagingVisitor::setUpHashBucketVisit() {
    // We need to lock the VBucket state here, because if the vb changes state
    // during paging, this could lead to data loss.
    vbStateLock =
            std::shared_lock<folly::SharedMutex>(currentBucket->getStateLock());
    // Grab a locked ReadHandle
    readHandle = currentBucket->lockCollections();
}

void PagingVisitor::tearDownHashBucketVisit() {
    // Unlock the locks. They can now never be locked again, and should
    // be overwritten with new ones. The unlocking order matters - vbStateLock
    // must outlive the collections lock to avoid lock inversion.
    readHandle.unlock();
    vbStateLock.unlock();
}

bool PagingVisitor::shouldContinueHashTableVisit(
        const HashTable::HashBucketLock&) {
    if (pagingVisitorPauseCheckCount <= 1) {
        return true;
    }
    const auto threshold =
            currentBucket->ht.getSize() / pagingVisitorPauseCheckCount;
    if (++htVisitsSincePauseCheck >= threshold) {
        htVisitsSincePauseCheck = 0;
        return shouldInterrupt() != ExecutionState::Pause;
    }
    return true;
}

ExpiredPagingVisitor::ExpiredPagingVisitor(
        KVBucket& s,
        EPStats& st,
        std::shared_ptr<cb::Semaphore> pagerSemaphore,
        bool pause,
        const VBucketFilter& vbFilter)
    : PagingVisitor(s, st, pagerSemaphore, pause, vbFilter) {
}

InterruptableVBucketVisitor::ExecutionState
ExpiredPagingVisitor::shouldInterrupt() {
    if (!canPause()) {
        return ExecutionState::Continue;
    }

    if (stats.getDiskQueueSize() >= MAX_PERSISTENCE_QUEUE_SIZE) {
        return ExecutionState::Pause;
    }

    return CappedDurationVBucketVisitor::shouldInterrupt();
}

bool ExpiredPagingVisitor::visit(const HashTable::HashBucketLock& lh,
                                 StoredValue& v) {
    if (!shouldVisit(lh, v)) {
        return true;
    }
    maybeExpire(v);
    return shouldContinueHashTableVisit(lh);
}

void ExpiredPagingVisitor::visitBucket(VBucket& vb) {
    update();

    if (vBucketFilter(vb.getId())) {
        currentBucket = &vb;
        // EvictionPolicy is not required when running expiry item
        // pager
        hashTablePosition = vb.ht.pauseResumeVisit(*this, hashTablePosition);
        if (hashTablePosition == vb.ht.endPosition()) {
            hashTablePosition = {};
        }
        currentBucket = nullptr;
    }
}

void ExpiredPagingVisitor::complete() {
    PagingVisitor::complete();
    stats.expiryPagerHisto.add(getElapsedTime());
}

ItemPagingVisitor::ItemPagingVisitor(
        KVBucket& s,
        EPStats& st,
        std::unique_ptr<ItemEvictionStrategy> strategy,
        std::shared_ptr<cb::Semaphore> pagerSemaphore,
        bool pause,
        const VBucketFilter& vbFilter)
    : PagingVisitor(s, st, pagerSemaphore, pause, vbFilter),
      evictionStrategy(std::move(strategy)),
      ejected(0),
      maxCas(0) {
}

InterruptableVBucketVisitor::ExecutionState
ItemPagingVisitor::shouldInterrupt() {
    if (!canPause()) {
        return ExecutionState::Continue;
    }

    return CappedDurationVBucketVisitor::shouldInterrupt();
}

bool ItemPagingVisitor::visit(const HashTable::HashBucketLock& lh,
                              StoredValue& v) {
    if (!shouldVisit(lh, v)) {
        return true;
    }
    if (maybeExpire(v)) {
        return shouldContinueHashTableVisit(lh);
    }

    auto vbState = currentBucket->getState();

    // Any dropped documents can be discarded
    if (readHandle.isLogicallyDeleted(v.getKey(), v.getBySeqno())) {
        // There is no reason to compute anything further for this item, it can
        // and will definitely be dropped, regardless of the desired eviction
        // ratio.
        doEviction(lh, &v, true /*isDropped*/);
        return shouldContinueHashTableVisit(lh);
    }

    /*
     * We take a copy of the freqCounterValue because calling
     * doEviction can modify the value, and when we want to
     * add it to the histogram we want to use the original value.
     */
    auto storedValueFreqCounter = v.getFreqCounterValue();

    auto age = casToAge(v.getCas());

    bool eligibleForPaging = false;

    if (evictionStrategy->shouldTryEvict(
                storedValueFreqCounter, age, vbState)) {
        // try to evict, may fail if sv is not eligible due to being
        // pending/non-resident/dirty.
        eligibleForPaging = doEviction(lh, &v, false /*isDropped*/);
    } else {
        // just check eligibility without trying to evict
        eligibleForPaging = currentBucket->isEligibleForEviction(lh, v);
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
                currentBucket->ht.setSVFreqCounter(
                        lh, v, storedValueFreqCounter - 1);
            }
        }
    }

    if (eligibleForPaging) {
        evictionStrategy->eligibleItemSeen(
                storedValueFreqCounter, age, vbState);
    }

    return shouldContinueHashTableVisit(lh);
}

void ItemPagingVisitor::visitBucket(VBucket& vb) {
    update();

    if (shouldStopPaging()) {
        hashTablePosition = {};
        return;
    }

    if (!vBucketFilter(vb.getId())) {
        hashTablePosition = {};
        return;
    }

    maxCas = vb.getMaxCas();
    evictionStrategy->setupVBucketVisit(vb.getNumItems());

    currentBucket = &vb;
    hashTablePosition = vb.ht.pauseResumeVisit(*this, hashTablePosition);
    if (hashTablePosition == vb.ht.endPosition()) {
        hashTablePosition = {};
    }
    currentBucket = nullptr;

    evictionStrategy->tearDownVBucketVisit(vb.getState());
}

bool ItemPagingVisitor::doEviction(const HashTable::HashBucketLock& lh,
                                   StoredValue* v,
                                   bool isDropped) {
    auto policy = store.getItemEvictionPolicy();
    StoredDocKey key(v->getKey());

    // We take a copy of the freqCounterValue because pageOut may modify the
    // stored value.
    auto storedValueFreqCounter = v->getFreqCounterValue();

    if (currentBucket->pageOut(vbStateLock, readHandle, lh, v, isDropped)) {
        ++ejected;

        /**
         * For FULL EVICTION MODE, add all items that are being
         * evicted (but not dropped) to the corresponding bloomfilter. Ignoring
         * dropped items as they cannot be read back.
         */
        if (!isDropped && policy == ::EvictionPolicy::Full) {
            currentBucket->addToFilter(key);
        }

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
        // performed eviction so return true
        return true;
    }
    // did not perform eviction so return false
    return false;
}

void ItemPagingVisitor::complete() {
    PagingVisitor::complete();
    stats.itemPagerHisto.add(getElapsedTime());

    // Re-check memory which may wake up the ItemPager and schedule
    // a new PagingVisitor with the next phase/memory target etc...
    // This is done after we've signalled 'completion' by clearing
    // the stateFinalizer, which ensures the ItemPager doesn't just
    // ignore a request.
    store.checkAndMaybeFreeMemory();
}

void ItemPagingVisitor::update() {
    PagingVisitor::update();
    if (ejected > 0) {
        EP_LOG_DEBUG("Paged out {} values", ejected);
        ejected = 0;
    }
}

bool ItemPagingVisitor::shouldStopPaging() const {
    auto current = static_cast<double>(store.getPageableMemCurrent());
    auto lower = static_cast<double>(store.getPageableMemLowWatermark());

    // stop eviction whenever pageable memory usage is below the pageable low
    // watermark
    return current <= lower;
}

uint64_t ItemPagingVisitor::casToAge(uint64_t cas) const {
    uint64_t age = (maxCas > cas) ? (maxCas - cas) : 0;
    age = age >> cb::eviction::casBitsNotTime;
    return age;
}
