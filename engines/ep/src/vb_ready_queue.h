/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "testing_hook.h"

#include <boost/dynamic_bitset.hpp>
#include <folly/MPMCQueue.h>
#include <memcached/engine_common.h>
#include <memcached/vbucket.h>

#include <mutex>
#include <queue>
#include <unordered_set>

class CookieIface;

/**
 * VBReadyQueue is a queue of vbuckets that are ready for some task to process.
 * The queue does not allow duplicates and the push_unique method enforces this.
 */
class VBReadyQueue {
public:
    /// Construct a VBReadyQueue with the specified maximum number of vBuckets.
    VBReadyQueue(size_t maxVBuckets);

    bool exists(Vbid vbucket);

    /**
     * Return true and set the ref-param 'frontValue' if the queue is not
     * empty. frontValue is set to the front of the queue.
     */
    bool popFront(Vbid& frontValue);

    /**
     * Pop the front item.
     * Safe to call on an empty list
     */
    void pop();

    /**
     * Push the vbucket only if it's not already in the queue.
     * @return true if the queue was previously empty (i.e. we have
     * transitioned from zero -> one elements in the queue).
     */
    bool pushUnique(Vbid vbucket);

    /**
     * Size of the queue.
     */
    size_t size() const;

    /**
     * @return true if empty
     */
    bool empty();

    void addStats(const std::string& prefix,
                  const AddStatFn& add_stat,
                  const CookieIface* c) const;

    /**
     * Testing hooks for testing things... Lock free is HARD so sanity checks
     * for this stuff is good. Don't want to bloat the class in production cases
     * so only included in debug builds.
     */
#ifndef NDEBUG
    TestingHook<> popFrontAfterSizeLoad;
    TestingHook<> popFrontAfterQueueRead;
    TestingHook<> popFrontAfterSizeSub;
    TestingHook<> popFrontAfterQueuedValueSet;

    TestingHook<> pushUniqueQueuedValuesUpdatedPreQueueWrite;
    TestingHook<> pushUniqueQueuedValuesUpdatedPostQueueWrite;
#endif
private:
    /// A queue of Vbid for vBuckets needing work
    folly::MPMCQueue<Vbid> readyQueue;

    /**
     * Queue size is tracked separately from the queue as finding the size of it
     * is expensive.
     */
    std::atomic<size_t> queueSize{};

    /**
     * maintain a set of values that are in the readyQueue.
     * find() is performed by front-end threads so we want it to be
     * efficient so just an array lookup is required.
     */
    std::vector<std::atomic_bool> queuedValues{};
};
