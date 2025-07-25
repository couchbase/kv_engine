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

#pragma once

#include "collections/vbucket_manifest_handles.h"
#include "hash_table.h"
#include "learning_age_and_mfu_based_eviction.h"
#include "progress_tracker.h"
#include "vb_visitors.h"

#include <atomic>
#include <list>

class EPStats;
class Item;
class EventuallyPersistentEngine;
class KVBucket;
class StoredValue;

namespace cb {
class Semaphore;
}

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public CappedDurationVBucketVisitor,
                      public HashTableVisitor {
public:
    /**
     * Construct a PagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param pause flag indicating if PagingVisitor can pause between vbucket
     *              visits
     * @param vbFilter the filter used to select which vbuckets to visit
     */
    PagingVisitor(KVBucket& s,
                  EPStats& st,
                  std::shared_ptr<cb::Semaphore> pagerSemaphore,
                  bool pause,
                  const VBucketFilter& vbFilter);

    virtual void update(bool expireAllItems = true);

    void complete() override;

    std::function<bool(const Vbid&, const Vbid&)> getVBucketComparator()
            const override;

    /**
     * Override the setUpHashBucketVisit method so that we can acquire a
     * Collections::VB::ReadHandle. Required if we evict any items in
     * Ephemeral buckets (i.e. delete them) and need to update the collection
     * high seqno.
     */
    bool setUpHashBucketVisit() override;

    /**
     * Override the tearDownHasBucketVisit method to release the
     * Collections::VB::ReadHandle that we previously acquired.
     */
    void tearDownHashBucketVisit() override;

    /**
     * Overrides InterruptableVBucketVisitor::needsToRevisitLast to indicate
     * whether the vBucket which was just visited needs to be visited again.
     * When the HashTable visitor indicates a pause, we yield the vBucket visit,
     * and need to revisit.
     */
    NeedsRevisit needsToRevisitLast() override {
        // Even if we need to pause on the very first HT bucket, we will
        // continue from the next. When the HT visit completes, we reset
        // the position. Hence, we use the position to determine whether
        // there was a pause.
        if (hashTablePosition != HashTable::Position{}) {
            return NeedsRevisit::YesNow;
        }
        return NeedsRevisit::No;
    }

protected:
    /**
     * Derived visitors should not touch the item.
     *
     * This may be because the item is a prepare or because this is an
     * replica ephemeral bucket for example.
     */
    bool shouldVisit(const HashTable::HashBucketLock& lh, StoredValue& v);

    /**
     * Expire the item if its TTL was up when the visitor was created.
     * @return true if expired
     */
    bool maybeExpire(StoredValue& v);

    /**
     * Returns the time elapsed since the creation of the visitor instance.
     */
    std::chrono::microseconds getElapsedTime() const;

    /**
     * Is the visitor allowed to pause?
     */
    bool canPause() const {
        return isPausingAllowed;
    }

    /**
     * @param hbl HashBucketLock for the item currently visiting
     * @return false if enough time has elapsed
     */
    bool shouldContinueHashTableVisit(const HashTable::HashBucketLock& hbl);

    /**
     * @return true if the list of expired items is empty
     */
    bool isExpiredListEmpty() const;

    // getter for expired items
    const std::list<Item>& getExpiredItems() const {
        return expired;
    }

    // The current vbucket that the eviction algorithm is operating on.
    // Only valid while inside visitBucket().
    VBucket* currentBucket{nullptr};

    // The VBucket state lock handle that we use around HashBucket visits.
    std::shared_lock<folly::SharedMutex> vbStateLock;

    // The VB::Manifest read handle that we use to lock around HashBucket
    // visits. Will contain a nullptr if we aren't currently locking anything.
    Collections::VB::ReadHandle readHandle;

    // The position in the HashTable to continue visiting from
    HashTable::Position hashTablePosition;

    /**
     * Flag used to indicate if we should only expire items accumulated in the
     * Expired list and not visit the HashTable to find new items to expire.
     */
    bool processExpiredItemsOnly = false;

    KVBucket& store;
    EPStats& stats;

private:
    std::list<Item> expired;
    time_t startTime;
    std::shared_ptr<cb::Semaphore> pagerSemaphore;
    const bool isPausingAllowed;

    // Expected number of times the PagingVisitor will check the pause
    // condition per vBucket. Used to determine how many HashTable visits to
    // make before checking.
    const size_t pagingVisitorPauseCheckCount;

    // Number of HashTable visits since the last pause check.
    // Used to determine when to check for pausing.
    size_t htVisitsSincePauseCheck = 0;

    // The time limit in milliseconds for processing items from the expired
    // items list at the start of an expiry pager run. If the expired items list
    // is not empty when the pager starts, it will only process items from this
    // list for up to this duration before yielding.
    const std::chrono::milliseconds expiryVisitorItemsOnlyDuration;
    // The time limit in milliseconds for processing expired items after
    // visiting the HashTable. After finding expired items during hash table
    // traversal, the expiry pager will process them until this duration is
    // reached before yielding.
    const std::chrono::milliseconds expiryVisitorExpireAfterVisitDuration;

    /**
     * Flag used to identify if memory usage was above the backfill threshold
     * when the PagingVisitor started. Used to determine if we have to wake up
     * snoozed backfills at PagingVisitor completion.
     */
    bool wasAboveBackfillThreshold;

    cb::time::steady_clock::time_point taskStart;

    ProgressTracker progressTracker;
};

class ExpiredPagingVisitor : public PagingVisitor {
public:
    /**
     * Construct an ExpiredPagingVisitor that will expire eligible items.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param pause flag indicating if PagingVisitor can pause between vbucket
     *              visits
     * @param vbFilter the filter used to select which vbuckets to visit
     */
    ExpiredPagingVisitor(KVBucket& s,
                         EPStats& st,
                         std::shared_ptr<cb::Semaphore> pagerSemaphore,
                         bool pause,
                         const VBucketFilter& vbFilter);

    /**
     * @return ExecutionState::Pause if this visitor execution duration-quantum
     *  has been consumed. "Paused" is also returned based on the DWQ size.
     *  ExecutionState::Continue otherwise.
     */
    ExecutionState shouldInterrupt() override;

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    void visitBucket(VBucket& vb) override;

    void complete() override;

    /**
     * If we have more items in the expired list, we need to revisit so the
     * expiries can be processed and the list can be emptied.
     * Otherwise check if the hash table needs a revisit.
     */
    NeedsRevisit needsToRevisitLast() override {
        if (!isExpiredListEmpty()) {
            return NeedsRevisit::YesNow;
        }

        return PagingVisitor::needsToRevisitLast();
    }
};

class ItemPagingVisitor : public PagingVisitor {
public:
    /**
     * Construct an ItemPagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param strategy the eviction strategy to use
     * @param pagerSemaphore an optional semaphore which will be released
     *                       once this visitor completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket
     *              visits
     * @param vbFilter the filter used to select which vbuckets to visit
     */
    ItemPagingVisitor(KVBucket& s,
                      EPStats& st,
                      std::unique_ptr<ItemEvictionStrategy> strategy,
                      std::shared_ptr<cb::Semaphore> pagerSemaphore,
                      bool pause,
                      const VBucketFilter& vbFilter);

    /**
     * @return ExecutionState::Pause if this visitor execution duration-quantum
     *  has been consumed. ExecutionState::Continue otherwise.
     */
    ExecutionState shouldInterrupt() override;

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    void visitBucket(VBucket& vb) override;

    void complete() override;

    void update(bool expireAllItems = true) override;

protected:
    /**
     * Have we freed enough memory to stop paging?
     */
    virtual bool shouldStopPaging() const;

    // Protected for testing purposes
    // Holds the data structures used during the selection of documents to
    // evict from the hash table.
    std::unique_ptr<ItemEvictionStrategy> evictionStrategy;

    // The number of documents that were evicted.
    size_t ejected;

private:
    /*
     * Calculate the age when the item was last stored / modified.
     *
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
    uint64_t casToAge(uint64_t cas) const;

    bool doEviction(const HashTable::HashBucketLock& lh,
                    StoredValue* v,
                    bool isDropped);

    // Holds the current vbucket's maxCas value at the point just before we
    // visit all items in the vbucket.
    uint64_t maxCas;
};
