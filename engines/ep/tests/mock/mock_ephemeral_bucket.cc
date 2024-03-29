/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_ephemeral_bucket.h"
#include "collections/vbucket_manifest.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "mock_checkpoint_manager.h"
#include "vbucket.h"
#include <executor/executorpool.h>

MockEphemeralBucket::MockEphemeralBucket(EventuallyPersistentEngine& theEngine)
    : EphemeralBucket(theEngine) {
}

VBucketPtr MockEphemeralBucket::makeVBucket(
        Vbid id,
        vbucket_state_t state,
        KVShard* shard,
        std::unique_ptr<FailoverTable> table,
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
        uint64_t maxPrepareSeqno) {
    auto vptr = EphemeralBucket::makeVBucket(id,
                                             state,
                                             shard,
                                             std::move(table),
                                             std::move(manifest),
                                             initState,
                                             lastSeqno,
                                             lastSnapStart,
                                             lastSnapEnd,
                                             purgeSeqno,
                                             maxCas,
                                             hlcEpochSeqno,
                                             mightContainXattrs,
                                             replicationTopology,
                                             maxVisibleSeqno,
                                             maxPrepareSeqno);
    vptr->checkpointManager = std::make_unique<MockCheckpointManager>(
            stats,
            *vptr,
            engine.getCheckpointConfig(),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            maxVisibleSeqno,
            maxPrepareSeqno,
            /*flusher callback*/ nullptr);
    return vptr;
}

void MockEphemeralBucket::setDurabilityCompletionTask(
        std::shared_ptr<DurabilityCompletionTask> task) {
    durabilityCompletionTask = task;
}
