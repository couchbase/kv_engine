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
 *   distributed undemor the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "ephemeral_vb_count_visitor.h"

#include "ephemeral_vb.h"
#include "seqlist.h"
#include "vb_count_visitor.h"

void EphemeralVBucket::CountVisitor::visitBucket(const VBucketPtr& vb) {
    // Handle base class counts
    VBucketCountVisitor::visitBucket(vb);

    // Then append Ephemeral-specific:
    if (desired_state != vbucket_state_dead) {
        auto& ephVB = dynamic_cast<EphemeralVBucket&>(*vb);
        autoDeleteCount += ephVB.autoDeleteCount;
        htDeletedPurgeCount += ephVB.htDeletedPurgeCount;
        seqlistCount += ephVB.seqList->getNumItems();
        seqlistDeletedCount += ephVB.seqList->getNumDeletedItems();
        seqListPurgeCount += ephVB.seqListPurgeCount;

        uint64_t rrBegin, rrEnd;
        std::tie(rrBegin, rrEnd) = ephVB.seqList->getRangeRead();

        seqlistReadRangeCount += rrEnd - rrBegin;
        seqlistStaleCount += ephVB.seqList->getNumStaleItems();
        seqlistStaleValueBytes += ephVB.seqList->getStaleValueBytes();
        seqlistStaleMetadataBytes += ephVB.seqList->getStaleMetadataBytes();
    }
}
