/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstdint>

#include "memcached/vbucket.h"

class HdrHistogram;

/**
 * Interface used to determine if a given item should be evicted when visiting
 * items in the HashTable, based on one or more of:
 * * MFU (freq counter)
 * * Age
 * * vbucket state
 *
 * Implementations may choose to use some or all of these factors.
 *
 * As items are visited, if they _could_ be evicted safely, whether or not the
 * strategy decided to _try_, eligibleItemSeen will be called. Impls may
 * choose to monitor properties of eligible items if useful (e.g., to learn
 * MFU distributions).
 *
 * The impl does not need to be concerned whether an item actually _can_ be
 * evicted safely; that is managed by the KVBucket and PagingVisitor.
 *
 * `shouldTryEvict` specifically avoids providing access to the entire item to
 * avoid adding this responsibility accidentally migrating here (and also to
 * minimise coupling and simplify tests/mocks).
 *
 * The visitor will notify the strategy before and after visiting a vbucket,
 * but it is not required that it do anything in these calls if not necessary.
 *

 */
class ItemEvictionStrategy {
public:
    virtual ~ItemEvictionStrategy();
    /**
     * Called immediately before visiting a vbucket.
     *
     * Informs the implementation that this vbucket is expected to contain
     * numExpectedItems items.
     *
     * @param numExpectedItems the approximate number of items in the vbucket
     */
    virtual void setupVBucketVisit(uint64_t numExpectedItems) = 0;

    /**
     * Called immediately after visiting a vbucket.
     *
     * If the impl has per-vbucket state which needs clearing, or per-vb stats
     * to record, it should be done in this method.
     */
    virtual void tearDownVBucketVisit(vbucket_state_t state) = 0;

    /**
     * Test whether an item with MFU @p freq and age @p age, in a vbucket in
     * state @p state, should be considered for eviction.
     *
     * This does not _guarantee_ that eviction will succeed (e.g., does not
     * check if the item is clean), but is to be used to select which items to
     * _try_ evict - e.g., an impl may return true for the X coldest items in
     * the vb.
     *
     * The exact requirements that must be met are implementation defined.
     *
     * @param freq value's probabilistic most frequently used counter
     * @param age value's age, see HLCT for unit info
     * @param state the currently visited vbucket's state
     * @return true if the caller should attempt to evict the item
     */
    virtual bool shouldTryEvict(uint8_t freq,
                                uint64_t age,
                                vbucket_state_t state) = 0;

    /**
     * Inform the impl that a value has been visited which was in a state such
     * that it _could_ have been evicted safely (e.g., not dirty, not already
     * evicted) - regardless of whether it _was_ evicted or not.
     *
     * This may be used to monitor the distribution of MFU/age of "evictable
     * values", if the implementation cares.
     *
     * @param freq value's probabilistic most frequently used counter
     * @param age value's age, see HLCT for unit info
     * @param state the currently visited vbucket's state
     */
    virtual void eligibleItemSeen(uint8_t freq,
                                  uint64_t age,
                                  vbucket_state_t state) = 0;

    /**
     * Construct an ItemEvictionStrategy which will not attempt
     * to evict anything.
     *
     * Useable as a default or for expiry-only paging visitors.
     */
    static std::unique_ptr<ItemEvictionStrategy> evict_nothing();

    /**
     * Construct an ItemEvictionStrategy which will attempt
     * to evict everything, regardless of age of MFU.
     */
    static std::unique_ptr<ItemEvictionStrategy> evict_everything();
};
