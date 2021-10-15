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

#pragma once

#include "ephemeral_vb.h"
#include "vb_count_visitor.h"

/**
 * vBucket visitor that counts statistics related to Ephemeral Buckets (both
 * applicable to the base class and specific to Ephemeral).
 */
class EphemeralVBucket::CountVisitor : public VBucketCountVisitor {
public:
    explicit CountVisitor(vbucket_state_t state) : VBucketCountVisitor(state) {
    }

    void visitBucket(VBucket& vb) override;

    size_t autoDeleteCount = 0;
    size_t htDeletedPurgeCount = 0;
    uint64_t seqlistCount = 0;
    uint64_t seqlistDeletedCount = 0;
    size_t seqListPurgeCount = 0;
    uint64_t seqlistReadRangeCount = 0;
    uint64_t seqlistStaleCount = 0;
    size_t seqlistStaleValueBytes = 0;
    size_t seqlistStaleMetadataBytes = 0;
};
