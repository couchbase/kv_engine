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

#include "ephemeral_vb_count_visitor.h"

#include "ephemeral_vb.h"
#include "seqlist.h"
#include "vb_count_visitor.h"

void EphemeralVBucket::CountVisitor::visitBucket(VBucket& vb) {
    // Handle base class counts
    VBucketCountVisitor::visitBucket(vb);

    // Then append Ephemeral-specific:
    if (desired_state != vbucket_state_dead) {
        auto& ephVB = dynamic_cast<EphemeralVBucket&>(vb);
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
