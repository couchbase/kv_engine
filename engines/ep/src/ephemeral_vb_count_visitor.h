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

    void visitBucket(const VBucketPtr& vb) override;

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
