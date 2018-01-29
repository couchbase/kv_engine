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

#include "item_freq_decayer_visitor.h"

// AgeVisitor implementation ///////////////////////////////////////////

ItemFreqDecayerVisitor::ItemFreqDecayerVisitor(uint16_t percentage_)
    : percentage(percentage_), visitedCount(0) {
}

void ItemFreqDecayerVisitor::setDeadline(ProcessClock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

bool ItemFreqDecayerVisitor::visit(const HashTable::HashBucketLock& lh,
                                   StoredValue& v) {
    // age the value's frequency counter by the given percentage
    v.setFreqCounterValue(v.getFreqCounterValue() * (percentage * 0.01));
    visitedCount++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(visitedCount);
}

void ItemFreqDecayerVisitor::clearStats() {
    visitedCount = 0;
}

size_t ItemFreqDecayerVisitor::getVisitedCount() const {
    return visitedCount;
}
