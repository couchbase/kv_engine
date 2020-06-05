/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "vb_visitors.h"

#include "vbucket.h"

VBucketVisitor::VBucketVisitor() = default;

VBucketVisitor::VBucketVisitor(const VBucketFilter& filter)
    : vBucketFilter(filter) {
}

VBucketVisitor::~VBucketVisitor() = default;

void CappedDurationVBucketVisitor::begin() {
    // Record when this chunk started so we know when to pause.
    chunkStart = Clock::now();
}

bool CappedDurationVBucketVisitor::pauseVisitor() {
    return Clock::now() > (chunkStart + maxChunkDuration);
}

PauseResumeVBAdapter::PauseResumeVBAdapter(
        std::unique_ptr<VBucketAwareHTVisitor> htVisitor)
    : htVisitor(std::move(htVisitor)) {
}

bool PauseResumeVBAdapter::visit(VBucket& vb) {
    // Check if this vbucket_id matches the position we should resume
    // from. If so then call the visitor using our stored HashTable::Position.
    HashTable::Position ht_start;
    if (resume_vbucket_id == vb.getId()) {
        ht_start = hashtable_position;
    }

    htVisitor->setCurrentVBucket(vb);
    hashtable_position = vb.ht.pauseResumeVisit(*htVisitor, ht_start);

    if (hashtable_position != vb.ht.endPosition()) {
        // We didn't get to the end of this VBucket. Record the vbucket_id
        // we got to and return false.
        resume_vbucket_id = vb.getId();
        return false;
    } else {
        return true;
    }
}
