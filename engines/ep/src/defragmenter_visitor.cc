/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "defragmenter_visitor.h"
#include "vbucket.h"

// DegragmentVisitor implementation ///////////////////////////////////////////

DefragmentVisitor::DefragmentVisitor(size_t max_size_class)
    : max_size_class(max_size_class),
      defrag_count(0),
      visited_count(0),
      currentVb(nullptr) {
}

DefragmentVisitor::~DefragmentVisitor() {
}

void DefragmentVisitor::setDeadline(
        std::chrono::steady_clock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

void DefragmentVisitor::setBlobAgeThreshold(uint8_t age) {
    age_threshold = age;
}

void DefragmentVisitor::setStoredValueAgeThreshold(uint8_t age) {
    sv_age_threshold = age;
}

bool DefragmentVisitor::visit(const HashTable::HashBucketLock& lh,
                              StoredValue& v) {
    const size_t value_len = v.valuelen();

    // value must be at least non-zero (also covers Items with null Blobs)
    // and no larger than the biggest size class the allocator
    // supports, so it can be successfully reallocated to a run with other
    // objects of the same size.
    if (value_len > 0 && value_len <= max_size_class) {
        // If sufficiently old and if it looks like nothing else holds a
        // reference to the blob reallocate, otherwise increment it's age.
        // It may be possible to add a reference to the blob without holding
        // any locks, therefore the check is somewhat of an estimate which
        // should be good enough.
        if (v.getValue()->getAge() >= age_threshold &&
            v.getValue().refCount() < 2) {
            v.reallocate();
            defrag_count++;
        } else {
            v.getValue()->incrementAge();
        }
    }

    if (sv_age_threshold) {
        if (v.getAge() >= sv_age_threshold.value()) {
            defragmentStoredValue(v);
        } else {
            v.incrementAge();
        }
    }

    visited_count++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(visited_count);
}

void DefragmentVisitor::clearStats() {
    defrag_count = 0;
    visited_count = 0;
    sv_defrag_count = 0;
}

size_t DefragmentVisitor::getDefragCount() const {
    return defrag_count;
}

size_t DefragmentVisitor::getVisitedCount() const {
    return visited_count;
}

size_t DefragmentVisitor::getStoredValueDefragCount() const {
    return sv_defrag_count;
}

void DefragmentVisitor::setCurrentVBucket(VBucket& vb) {
    currentVb = &vb;
}

void DefragmentVisitor::defragmentStoredValue(StoredValue& v) const {
    if (currentVb->ht.reallocateStoredValue(std::forward<StoredValue>(v))) {
        sv_defrag_count++;
    }
}
