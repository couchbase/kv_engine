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
#include "mock_replicationthrottle.h"
#include "vbucket.h"
#include <executor/executorpool.h>

MockEphemeralBucket::MockEphemeralBucket(EventuallyPersistentEngine& theEngine)
    : EphemeralBucket(theEngine) {
    // Replace replicationThrottle with Mock to allow controlling when it
    // pauses during tests.
    replicationThrottle =
            std::make_unique<::testing::NiceMock<MockReplicationThrottle>>(
                    replicationThrottle.release());
}

VBucketPtr MockEphemeralBucket::makeVBucket(
        Vbid id,
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
        uint64_t maxVisibleSeqno) {
    auto vptr = EphemeralBucket::makeVBucket(id,
                                             state,
                                             shard,
                                             std::move(table),
                                             std::move(newSeqnoCb),
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
                                             maxVisibleSeqno);

    // need to preserve the overhead callback after replacing the checkpoint
    // manager
    auto overheadChangedCB =
            vptr->checkpointManager->getOverheadChangedCallback();
    vptr->checkpointManager = std::make_unique<MockCheckpointManager>(
            stats,
            *vptr,
            engine.getCheckpointConfig(),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            maxVisibleSeqno,
            /*flusher callback*/ nullptr,
            makeCheckpointDisposer());

    vptr->checkpointManager->setOverheadChangedCallback(overheadChangedCB);

    return vptr;
}

void MockEphemeralBucket::setDurabilityCompletionTask(
        std::shared_ptr<DurabilityCompletionTask> task) {
    durabilityCompletionTask = task;
}
