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
    return queuedValues[vbucket.get()].load();
}

bool VBReadyQueue::popFront(Vbid& frontValue) {
    // Read the size before we attempt to pop anything. Folly's MPMC queue has a
    // non-blocking read function but the memory ordering is release/acquire.
    // This is fine if we were just using the queue, but we require a slightly
    // greater level of synchronization between the readyQueue and the queue
    // size so we must do a sequentially consistent read (of the queueSize) to
    // avoid any memory ordering issues which would otherwise cause us to see
    // the queue as empty when (if it was sequentially consistent) it would not
    // be.
    auto size = queueSize.load();
#ifndef NDEBUG
    popFrontAfterSizeLoad();
#endif
    if (size) {
        // Blocking read - definitely should be something in the queue.
        readyQueue.blockingRead(frontValue);

#ifndef NDEBUG
        popFrontAfterQueueRead();
#endif

        // If one looks closely at the pushUnique function, one might assume
        // that we could lose a notification if the pushUnique function is
        // executed entirely between the line before and the line after this
        // comment. Why? Because the queuedValues[vbid] is set to true so the
        // vBucket will not be enqueued. This is correct, but doesn't take into
        // account that this function (after setting queuedValues to false) will
        // return the vBucket that we did not enqueue to the caller. The caller
        // will not miss the update for this vBucket, it will just be processed
        // immediately.
        bool expected = true;
        queuedValues[frontValue.get()].compare_exchange_strong(expected, false);

#ifndef NDEBUG
        popFrontAfterQueuedValueSet();
#endif

        queueSize.fetch_sub(1);

#ifndef NDEBUG
        popFrontAfterSizeSub();
#endif
        return true;
    }
    return false;
}

void VBReadyQueue::pop() {
    Vbid dummy;
    popFront(dummy);
}

bool VBReadyQueue::pushUnique(Vbid vbucket) {
    bool expected = false;
    if (queuedValues[vbucket.get()].compare_exchange_strong(expected, true)) {
#ifndef NDEBUG
        pushUniqueQueuedValuesUpdatedPreQueueWrite();
#endif
        auto queued = readyQueue.write(vbucket);
        Expects(queued);

#ifndef NDEBUG
        pushUniqueQueuedValuesUpdatedPostQueueWrite();
#endif
        return queueSize.fetch_add(1) == 0;
    } else {
        return false;
    }
}

size_t VBReadyQueue::size() const {
    return queueSize.load();
}

bool VBReadyQueue::empty() {
    return queueSize.load() == 0;
}

void VBReadyQueue::addStats(const std::string& prefix,
                            const AddStatFn& add_stat,
                            const void* c) const {
    // We can't access the queue easily in a thread safe way so just report the
    // size
    add_casted_stat((prefix + "size").c_str(), readyQueue.size(), add_stat, c);
}
