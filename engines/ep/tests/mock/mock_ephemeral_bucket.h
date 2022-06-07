/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ephemeral_bucket.h"

/*
 * Mock of the EphemeralBucket class.
 */
class MockEphemeralBucket : public EphemeralBucket {
public:
    explicit MockEphemeralBucket(EventuallyPersistentEngine& theEngine);

    VBucketPtr makeVBucket(Vbid id,
                           vbucket_state_t state,
                           KVShard* shard,
                           std::unique_ptr<FailoverTable> table,
                           NewSeqnoCallback newSeqnoCb,
                           std::unique_ptr<Collections::VB::Manifest> manifest,
                           vbucket_state_t initState,
                           int64_t lastSeqno,
                           uint64_t lastSnapStart,
                           uint64_t lastSnapEnd,
                           uint64_t purgeSeqno,
                           uint64_t maxCas,
                           int64_t hlcEpochSeqno,
                           bool mightContainXattrs,
                           const nlohmann::json* replicationTopology,
                           uint64_t maxVisibleSeqno,
                           uint64_t maxPrepareSeqno) override;

    void setDurabilityCompletionTask(
            std::shared_ptr<DurabilityCompletionTask> task);
};
