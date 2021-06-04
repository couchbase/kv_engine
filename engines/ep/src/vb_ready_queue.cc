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

#include <statistics/cbstat_collector.h>

VBReadyQueue::VBReadyQueue(size_t maxVBuckets)
    : readyQueue(maxVBuckets), queuedValues(maxVBuckets) {
}

bool VBReadyQueue::exists(Vbid vbucket) {
    std::lock_guard<std::mutex> lh(lock);
    return queuedValues.test(vbucket.get());
}

bool VBReadyQueue::popFront(Vbid& frontValue) {
    std::lock_guard<std::mutex> lh(lock);
    if (readyQueue.read(frontValue)) {
        queuedValues.reset(frontValue.get());
        return true;
    }
    return false;
}

void VBReadyQueue::pop() {
    Vbid dummy;
    popFront(dummy);
}

bool VBReadyQueue::pushUnique(Vbid vbucket) {
    bool wasEmpty;
    {
        std::lock_guard<std::mutex> lh(lock);
        wasEmpty = readyQueue.size() == 0;
        const bool wasSet = queuedValues.test_set(vbucket.get());
        if (!wasSet) {
            auto result = readyQueue.write(vbucket);
            Expects(result);
        }
    }
    return wasEmpty;
}

size_t VBReadyQueue::size() const {
    std::lock_guard<std::mutex> lh(lock);
    return readyQueue.size();
}

bool VBReadyQueue::empty() {
    std::lock_guard<std::mutex> lh(lock);
    return readyQueue.isEmpty();
}

void VBReadyQueue::clear() {
    std::lock_guard<std::mutex> lh(lock);
    Vbid vbid;
    while (!readyQueue.isEmpty()) {
        readyQueue.read(vbid);
    }
    queuedValues.reset();
}

std::queue<Vbid> VBReadyQueue::swap() {
    std::lock_guard<std::mutex> lh(lock);
    std::queue<Vbid> result;
    // TODO: Fix this - either remove swap() or reconsider MPMCQueue; given
    // the below implementation is not O(1) anymore.
    Vbid element;
    while (readyQueue.read(element)) {
        result.push(element);
    }
    queuedValues.reset();

    return result;
}

void VBReadyQueue::addStats(const std::string& prefix,
                            const AddStatFn& add_stat,
                            const void* c) const {
    // Take a copy of the queue data under lock; then format it to stats. We
    // can't copy the readyQueue as it wouldn't be thread safe so folly removed
    // the copy ctor so we'll just grab the size.
    boost::dynamic_bitset<> qMapCopy;
    auto size = 0;
    {
        std::lock_guard<std::mutex> lh(lock);
        qMapCopy = queuedValues;
        size = readyQueue.size();
    }

    add_casted_stat((prefix + "size").c_str(), size, add_stat, c);
    add_casted_stat(
            (prefix + "map_size").c_str(), qMapCopy.count(), add_stat, c);

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
