/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "defragmenter_visitor.h"

// DegragmentVisitor implementation ///////////////////////////////////////////

DefragmentVisitor::DefragmentVisitor(uint8_t age_threshold_,
                                     size_t max_size_class)
    : max_size_class(max_size_class),
      age_threshold(age_threshold_),
      defrag_count(0),
      visited_count(0),
      currentVb(nullptr) {
}

DefragmentVisitor::~DefragmentVisitor() {
}

void DefragmentVisitor::setDeadline(ProcessClock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

bool DefragmentVisitor::visit(const HashTable::HashBucketLock& lh,
                              StoredValue& v) {
    const size_t value_len = v.valuelen();
    bool valueCompressed = false;

    // Check if the item can be compressed
    if (!mcbp::datatype::is_snappy(v.getDatatype()) &&
        compressMode == BucketCompressionMode::Active && value_len > 0) {
        currentVb->ht.compressValue(v);
        valueCompressed = true;
    }

    // value must be at least non-zero (also covers Items with null Blobs)
    // and no larger than the biggest size class the allocator
    // supports, so it can be successfully reallocated to a run with other
    // objects of the same size.
    if (!valueCompressed && value_len > 0 && value_len <= max_size_class) {
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
    visited_count++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(visited_count);
}

void DefragmentVisitor::clearStats() {
    defrag_count = 0;
    visited_count = 0;
}

size_t DefragmentVisitor::getDefragCount() const {
    return defrag_count;
}

size_t DefragmentVisitor::getVisitedCount() const {
    return visited_count;
}

void DefragmentVisitor::setCompressionMode(
        const BucketCompressionMode compressionMode) {
    compressMode = compressionMode;
}

void DefragmentVisitor::setCurrentVBucket(VBucket& vb) {
    currentVb = &vb;
}
