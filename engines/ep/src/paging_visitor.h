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
#include "item_eviction.h"
#include "item_pager.h"
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

enum pager_type_t { ITEM_PAGER, EXPIRY_PAGER };

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public CappedDurationVBucketVisitor,
                      public HashTableVisitor {
public:
    enum class EvictionPolicy : uint8_t {
        lru2Bit, // The original 2-bit LRU policy
        hifi_mfu // The new hifi_mfu policy
    };

    /**
     * Construct a PagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param evictionRatio fractions (0-1) of objects to attempt to evict
     *                      from replica vbs, and from active/pending vbs
     * @param sfin pointer to a bool to be set to true after run completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket
     *              visits
     * @param bias active vbuckets eviction probability bias multiplier (0-1)
     * @param vbFilter the filter used to select which vbuckets to visit
     * @param _agePercentage age percentile used to find age threshold items
     *        must exceed to be considered for eviction if their MFU value is
     *        above _freqCounterAgeThreshold
     * @param _freqCounterAgeThreshold MFU frequency threshold beyond which the
     *        item age is considered
     */
    PagingVisitor(KVBucket& s,
                  EPStats& st,
                  EvictionRatios evictionRatios,
                  std::shared_ptr<cb::Semaphore> pagerSemaphore,
                  pager_type_t caller,
                  bool pause,
                  const VBucketFilter& vbFilter,
                  size_t _agePercentage,
                  size_t _freqCounterAgeThreshold);

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    void visitBucket(const VBucketPtr& vb) override;

    void update();

    /**
     * @return ExecutionState::Pause if this visitor execution duration-quantum
     *  has been consumed. Only for the ExpiryPager "paused" is also returned
     *  based on the DWQ size. ExecutionState::Continue otherwise.
     */
    ExecutionState shouldInterrupt() override;

    void complete() override;

    std::function<bool(const Vbid&, const Vbid&)> getVBucketComparator()
            const override;

    /**
     * Override the setUpHashBucketVisit method so that we can acquire a
     * Collections::VB::ReadHandle. Required if we evict any items in
     * Ephemeral buckets (i.e. delete them) and need to update the collection
     * high seqno.
     */
    void setUpHashBucketVisit() override;

    /**
     * Override the tearDownHasBucketVisit method to release the
     * Collections::VB::ReadHandle that we previously acquired.
     */
    void tearDownHashBucketVisit() override;

    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() {
        return ejected;
    }

protected:
    // Protected for testing purposes
    // Holds the data structures used during the selection of documents to
    // evict from the hash table.
    ItemEviction itemEviction;

    // The number of documents that were evicted.
    size_t ejected;

    // The current vbucket that the eviction algorithm is operating on.
    VBucketPtr currentBucket;

    // The frequency counter threshold that is used to determine whether we
    // should evict items from the hash table.
    uint16_t freqCounterThreshold;

    // The age threshold that is used to determine whether we should evict
    // items from the hash table.
    uint64_t ageThreshold;

private:
    bool doEviction(const HashTable::HashBucketLock& lh, StoredValue* v);

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

    std::list<Item> expired;

    KVBucket& store;
    EPStats& stats;
    EvictionRatios evictionRatios;
    time_t startTime;
    std::shared_ptr<cb::Semaphore> pagerSemaphore;
    pager_type_t owner;
    bool canPause;

    /// Flag used to identify if memory usage is below the low watermark.
    bool isBelowLowWaterMark;

    /**
     * Flag used to identify if memory usage was above the backfill threshold
     * when the PagingVisitor started. Used to determine if we have to wake up
     * snoozed backfills at PagingVisitor completion.
     */
    bool wasAboveBackfillThreshold;

    std::chrono::steady_clock::time_point taskStart;

    // The age percent used to select the age threshold.  The value is
    // read by the ItemPager from a configuration parameter.
    size_t agePercentage;

    // The threshold for determining at what execution frequency should we
    // consider age when selecting items for eviction.  The value is
    // read by the ItemPager from a configuration parameter.
    uint16_t freqCounterAgeThreshold;

    // Holds the current vbucket's maxCas value at the point just before we
    // visit all items in the vbucket.
    uint64_t maxCas;

    // The VB::Manifest read handle that we use to lock around HashBucket
    // visits. Will contain a nullptr if we aren't currently locking anything.
    Collections::VB::ReadHandle readHandle;
};
