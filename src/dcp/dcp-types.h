/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Forward declarations of various DCP types.
 * To utilise the types the correct header must also be included in
 * the users compilation unit.
 */

#pragma once

#include "atomic.h"

#include <queue>
#include <unordered_set>

// Implementation defined in dcp/consumer.h
class DcpConsumer;
typedef SingleThreadedRCPtr<DcpConsumer> dcp_consumer_t;

// Implementation defined in dcp/producer.h
class DcpProducer;
typedef SingleThreadedRCPtr<DcpProducer> dcp_producer_t;

// Implementation defined in dcp/stream.h
class Stream;
typedef SingleThreadedRCPtr<Stream> stream_t;

// Implementation defined in dcp/stream.h
class PassiveStream;
typedef RCPtr<PassiveStream> passive_stream_t;

// Implementation defined in dcp/backfill-manager.h
class BackfillManager;
typedef RCPtr<BackfillManager> backfill_manager_t;

/**
 * DcpReadyQueue is a std::queue wrapper for managing a
 * queue of vbuckets that are ready for a DCP producer/consumer to process.
 * The queue does not allow duplicates and the push_unique method enforces
 * this. The interface is generally customised for the needs of:
 * - getNextItem and is thread safe as the frontend operations and
 *   DCPProducer threads are accessing this data.
 * - processBufferedItems by the processer task of the consumer
 *
 * Internally a std::queue and std::set track the contents and the std::set
 * enables a fast exists method which is used by front-end threads.
 */
class DcpReadyQueue {
public:
    bool exists(uint16_t vbucket) {
        LockHolder lh(lock);
        return (queuedValues.count(vbucket) != 0);
    }

    /**
     * Return true and set the ref-param 'frontValue' if the queue is not
     * empty. frontValue is set to the front of the queue.
     */
    bool popFront(uint16_t &frontValue) {
        LockHolder lh(lock);
        if (!readyQueue.empty()) {
            frontValue = readyQueue.front();
            readyQueue.pop();
            queuedValues.erase(frontValue);
            return true;
        }
        return false;
    }

    /**
     * Pop the front item.
     * Safe to call on an empty list
     */
    void pop() {
        LockHolder lh(lock);
        if (!readyQueue.empty()) {
            queuedValues.erase(readyQueue.front());
            readyQueue.pop();
        }
    }

    /**
     * Push the vbucket only if it's not already in the queue
     * Return true if the vbucket was added to the queue.
     */
    bool pushUnique(uint16_t vbucket) {
        LockHolder lh(lock);
        if (queuedValues.count(vbucket) == 0) {
            readyQueue.push(vbucket);
            queuedValues.insert(vbucket);
            return true;
        }
        return false;
    }

    /**
     * Size of the queue.
     */
    size_t size() {
        LockHolder lh(lock);
        return readyQueue.size();
    }

private:
    Mutex lock;

    /* a queue of vbuckets that are ready for producing */
    std::queue<uint16_t> readyQueue;

    /**
     * maintain a std::unordered_set of values that are in the readyQueue.
     * find() is performed by front-end threads so we want it to be
     * efficient so just a set lookup is required.
     */
    std::unordered_set<uint16_t> queuedValues;
};
