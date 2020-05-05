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

#include "vb_ready_queue.h"

#include "locks.h"
#include "statistics/collector.h"

bool VBReadyQueue::exists(Vbid vbucket) {
    LockHolder lh(lock);
    return (queuedValues.count(vbucket) != 0);
}

bool VBReadyQueue::popFront(Vbid& frontValue) {
    LockHolder lh(lock);
    if (!readyQueue.empty()) {
        frontValue = readyQueue.front();
        readyQueue.pop();
        queuedValues.erase(frontValue);
        return true;
    }
    return false;
}

void VBReadyQueue::pop() {
    LockHolder lh(lock);
    if (!readyQueue.empty()) {
        queuedValues.erase(readyQueue.front());
        readyQueue.pop();
    }
}

bool VBReadyQueue::pushUnique(Vbid vbucket) {
    bool wasEmpty;
    {
        LockHolder lh(lock);
        wasEmpty = queuedValues.empty();
        const bool inserted = queuedValues.emplace(vbucket).second;
        if (inserted) {
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
    queuedValues.clear();
}

void VBReadyQueue::addStats(const std::string& prefix,
                            const AddStatFn& add_stat,
                            const void* c) const {
    // Take a copy of the queue data under lock; then format it to stats.
    std::queue<Vbid> qCopy;
    std::unordered_set<Vbid> qMapCopy;
    {
        LockHolder lh(lock);
        qCopy = readyQueue;
        qMapCopy = queuedValues;
    }

    add_casted_stat((prefix + "size").c_str(), qCopy.size(), add_stat, c);
    add_casted_stat(
            (prefix + "map_size").c_str(), qMapCopy.size(), add_stat, c);

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
    for (auto& vbid : qMapCopy) {
        qMapContents += std::to_string(vbid.get()) + ",";
    }
    if (!qMapContents.empty()) {
        qMapContents.pop_back();
    }
    add_casted_stat((prefix + "map_contents").c_str(),
                    qMapContents.c_str(),
                    add_stat,
                    c);
}
