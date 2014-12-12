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

DefragmentVisitor::DefragmentVisitor(uint8_t age_threshold_)
  : max_size_class(3584),  // TODO: Derive from allocator hooks.
    age_threshold(age_threshold_),
    deadline(~0),
    resume_vbucket_id(0),
    hashtable_position(),
    defrag_count(0),
    visited_count(0) {
}

void DefragmentVisitor::set_deadline(hrtime_t deadline_) {
    deadline = deadline_;
}

bool DefragmentVisitor::visit(uint16_t vbucket_id, HashTable& ht) {

    // Check if this vbucket_id matches the position we should resume
    // from. If so then call the visitor using our stored HashTable::Position.
    HashTable::Position ht_start;
    if (resume_vbucket_id == vbucket_id) {
        ht_start = hashtable_position;
    }

    hashtable_position = ht.pauseResumeVisit(*this, ht_start);

    if (hashtable_position != ht.endPosition()) {
        // We didn't get to the end of this hashtable. Record the vbucket_id
        // we got to and return false.
        resume_vbucket_id = vbucket_id;
        return false;
    } else {
        return true;
    }
}

bool DefragmentVisitor::visit(StoredValue& v) {
    const size_t value_len = v.valuelen();

    // value must be at least non-zero (also covers Items with null Blobs)
    // and no larger than the biggest size class the allocator
    // supports, so it can be successfully reallocated to a run with other
    // objects of the same size.
    if (value_len > 0 && value_len <= max_size_class) {
        // If sufficiently old reallocate, otherwise increment it's age.
        if (v.getValue()->getAge() >= age_threshold) {
            v.reallocate();
            defrag_count++;
        } else {
            v.getValue()->incrementAge();
        }
    }
    visited_count++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    hrtime_t now = gethrtime();
    return (now < deadline);
}

HashTable::Position DefragmentVisitor::get_hashtable_position() const {
    return hashtable_position;
}

void DefragmentVisitor::clear_stats() {
    defrag_count = 0;
    visited_count = 0;
}

size_t DefragmentVisitor::get_defrag_count() const {
    return defrag_count;
}

size_t DefragmentVisitor::get_visited_count() const {
    return visited_count;
}
