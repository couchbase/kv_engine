/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "item_freq_decayer_visitor.h"

#include "vbucket.h"

// AgeVisitor implementation ///////////////////////////////////////////

ItemFreqDecayerVisitor::ItemFreqDecayerVisitor(uint16_t percentage_)
    : percentage(percentage_), visitedCount(0) {
}

void ItemFreqDecayerVisitor::setDeadline(
        cb::time::steady_clock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

bool ItemFreqDecayerVisitor::visit(const HashTable::HashBucketLock& lh,
                                   StoredValue& v) {
    // age the value's frequency counter by the given percentage
    ht->setSVFreqCounter(lh, v, v.getFreqCounterValue() * (percentage * 0.01));
    visitedCount++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(visitedCount);
}

void ItemFreqDecayerVisitor::setCurrentVBucket(VBucket& vb) {
    setCurrentHT(vb.ht);
}

void ItemFreqDecayerVisitor::setCurrentHT(HashTable& ht) {
    this->ht = &ht;
}

void ItemFreqDecayerVisitor::clearStats() {
    visitedCount = 0;
}

size_t ItemFreqDecayerVisitor::getVisitedCount() const {
    return visitedCount;
}
