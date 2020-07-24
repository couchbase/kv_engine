/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#pragma once

#include "collections/vbucket_manifest.h"
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

enum pager_type_t { ITEM_PAGER, EXPIRY_PAGER };

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public PausableVBucketVisitor, public HashTableVisitor {
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
                  std::shared_ptr<std::atomic<bool>>& sfin,
                  pager_type_t caller,
                  bool pause,
                  const VBucketFilter& vbFilter,
                  size_t _agePercentage,
                  size_t _freqCounterAgeThreshold);

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    void visitBucket(const VBucketPtr& vb) override;

    void update();

    bool pauseVisitor() override;

    void complete() override;

    /**
     * Override the setUpHashBucketVisit method so that we can acquire a
     * Collections::VB::Manifest::ReadHandle. Required if we evict any items in
     * Ephemeral buckets (i.e. delete them) and need to update the collection
     * high seqno.
     */
    void setUpHashBucketVisit() override;

    /**
     * Override the tearDownHasBucketVisit method to release the
     * Collections::VB::Manifest::ReadHandle that we previously acquired.
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
    // Removes checkpoints that are both closed and unreferenced, thereby
    // freeing the associated memory.
    // @param vb  The vbucket whose eligible checkpoints are removed from.
    void removeClosedUnrefCheckpoints(VBucket& vb);

    bool doEviction(const HashTable::HashBucketLock& lh, StoredValue* v);

    std::list<Item> expired;

    KVBucket& store;
    EPStats& stats;
    EvictionRatios evictionRatios;
    time_t startTime;
    std::shared_ptr<std::atomic<bool>> stateFinalizer;
    pager_type_t owner;
    bool canPause;
    /// Flag used to identify if memory usage is below the low watermark.
    bool isBelowLowWaterMark;
    bool wasHighMemoryUsage;
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
    Collections::VB::Manifest::ReadHandle readHandle;
};
