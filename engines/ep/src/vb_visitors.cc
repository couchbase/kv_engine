/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vb_visitors.h"

#include "vbucket.h"
#include <utility>

VBucketVisitor::VBucketVisitor() = default;

VBucketVisitor::VBucketVisitor(VBucketFilter filter)
    : vBucketFilter(std::move(filter)) {
}

VBucketVisitor::~VBucketVisitor() = default;

void CappedDurationVBucketVisitor::begin() {
    // Record when this chunk started so we know when to pause.
    chunkStart = Clock::now();
}

InterruptableVBucketVisitor::ExecutionState
CappedDurationVBucketVisitor::shouldInterrupt() {
    return Clock::now() > (chunkStart + maxChunkDuration)
                   ? ExecutionState::Pause
                   : ExecutionState::Continue;
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
