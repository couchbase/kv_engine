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

#include "vb_ready_queue.h"

#include "locks.h"

#include <statistics/cbstat_collector.h>

VBReadyQueue::VBReadyQueue(size_t maxVBuckets) : queuedValues(maxVBuckets) {
}

bool VBReadyQueue::exists(Vbid vbucket) {
    LockHolder lh(lock);
    return queuedValues.test(vbucket.get());
}

bool VBReadyQueue::popFront(Vbid& frontValue) {
    LockHolder lh(lock);
    if (!readyQueue.empty()) {
        frontValue = readyQueue.front();
        readyQueue.pop();
        queuedValues.reset(frontValue.get());
        return true;
    }
    return false;
}

void VBReadyQueue::pop() {
    LockHolder lh(lock);
    if (!readyQueue.empty()) {
        queuedValues.reset(readyQueue.front().get());
        readyQueue.pop();
    }
}

bool VBReadyQueue::pushUnique(Vbid vbucket) {
    bool wasEmpty;
    {
        LockHolder lh(lock);
        wasEmpty = readyQueue.empty();
        const bool wasSet = queuedValues.test_set(vbucket.get());
        if (!wasSet) {
            readyQueue.push(vbucket);
        }
    }
    return wasEmpty;
}

size_t VBReadyQueue::size() const {
    LockHolder lh(lock);
    return readyQueue.size();
}

bool VBReadyQueue::empty() {
    LockHolder lh(lock);
    return readyQueue.empty();
}

void VBReadyQueue::clear() {
    LockHolder lh(lock);
    while (!readyQueue.empty()) {
        readyQueue.pop();
    }
    queuedValues.reset();
}

std::queue<Vbid> VBReadyQueue::swap() {
    LockHolder lh(lock);
    std::queue<Vbid> result;
    readyQueue.swap(result);
    queuedValues.reset();

    return result;
}

void VBReadyQueue::addStats(const std::string& prefix,
                            const AddStatFn& add_stat,
                            const void* c) const {
    // Take a copy of the queue data under lock; then format it to stats.
    std::queue<Vbid> qCopy;
    boost::dynamic_bitset<> qMapCopy;
    {
        LockHolder lh(lock);
        qCopy = readyQueue;
        qMapCopy = queuedValues;
    }

    add_casted_stat((prefix + "size").c_str(), qCopy.size(), add_stat, c);
    add_casted_stat(
            (prefix + "map_size").c_str(), qMapCopy.count(), add_stat, c);

    // Form a comma-separated string of the queue's contents.
    std::string contents;
    while (!qCopy.empty()) {
        contents += std::to_string(qCopy.front().get()) + ",";
        qCopy.pop();
    }
    if (!contents.empty()) {
        contents.pop_back();
    }
    add_casted_stat(
            (prefix + "contents").c_str(), contents.c_str(), add_stat, c);

    // Form a comma-separated string of the queue map's contents.
    std::string qMapContents;
    for (auto vbid = qMapCopy.find_first(); vbid != qMapCopy.npos;
         vbid = qMapCopy.find_next(vbid)) {
        qMapContents += std::to_string(vbid) + ",";
    }
    if (!qMapContents.empty()) {
        qMapContents.pop_back();
    }
    add_casted_stat((prefix + "map_contents").c_str(),
                    qMapContents.c_str(),
                    add_stat,
                    c);
}
