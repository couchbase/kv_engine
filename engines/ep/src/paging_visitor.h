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

#include "hash_table.h"
#include "item_eviction.h"
#include "item_pager.h"
#include "vb_visitors.h"

#include <atomic>
#include <list>

class EPStats;
class EventuallyPersistentEngine;
class KVBucket;
class StoredValue;

enum pager_type_t { ITEM_PAGER, EXPIRY_PAGER };

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public VBucketVisitor, public HashTableVisitor {
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
     * @param vbFilter the filter used to select which vbuckets to visit
     * @param isEphemeral  boolean indicating if operating on ephemeral bucket
     * @param phase pointer to an item_pager_phase to be set
     */
    PagingVisitor(KVBucket& s,
                  EPStats& st,
                  double pcnt,
                  std::shared_ptr<std::atomic<bool>>& sfin,
                  pager_type_t caller,
                  bool pause,
                  double bias,
                  const VBucketFilter& vbFilter,
                  std::atomic<item_pager_phase>* phase,
                  bool _isEphemeral,
                  size_t _agePercentage,
                  size_t _freqCounterAgeThreshold);

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    void visitBucket(VBucketPtr& vb) override;

    void update();

    bool pauseVisitor() override;

    void complete() override;

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
    void removeClosedUnrefCheckpoints(VBucketPtr& vb);

    void adjustPercent(double prob, vbucket_state_t state);

    bool doEviction(const HashTable::HashBucketLock& lh, StoredValue* v);

    std::list<Item> expired;

    KVBucket& store;
    EPStats& stats;
    double percent;
    double activeBias;
    time_t startTime;
    std::shared_ptr<std::atomic<bool>> stateFinalizer;
    pager_type_t owner;
    bool canPause;
    bool completePhase;
    bool wasHighMemoryUsage;
    ProcessClock::time_point taskStart;
    std::atomic<item_pager_phase>* pager_phase;

    // Indicates whether the vbucket we are visiting is from an ephemeral
    // bucket.
    bool isEphemeral;

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
};
