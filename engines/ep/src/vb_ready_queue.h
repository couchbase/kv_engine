/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include <memcached/engine_common.h>
#include <memcached/vbucket.h>

#include <mutex>
#include <queue>
#include <unordered_set>

/**
 * VBReadyQueue is a std::queue wrapper for managing a queue of vbuckets that
 * are ready for some task to process. The queue does not allow duplicates and
 * the push_unique method enforces this.
 *
 * Internally a std::queue and std::set track the contents and the std::set
 * enables a fast exists method which is used by front-end threads.
 */
class VBReadyQueue {
public:
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

    /**
     * Clears the queue
     */
    void clear();

    void addStats(const std::string& prefix,
                  const AddStatFn& add_stat,
                  const void* c) const;

private:
    // Mutable so that we can lock in addStats (const) to copy the queue/set
    mutable std::mutex lock;

    /* a queue of vbuckets that are ready for producing */
    std::queue<Vbid> readyQueue;

    /**
     * maintain a std::unordered_set of values that are in the readyQueue.
     * find() is performed by front-end threads so we want it to be
     * efficient so just a set lookup is required.
     */
    std::unordered_set<Vbid> queuedValues;
};
