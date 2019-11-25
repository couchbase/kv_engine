/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#pragma once

#include "ep_bucket.h"

class MockItemFreqDecayerTask;

/*
 * Mock of the EPBucket class.
 */
class MockEPBucket : public EPBucket {
public:
    MockEPBucket(EventuallyPersistentEngine& theEngine) : EPBucket(theEngine) {
    }

    void createItemFreqDecayerTask();

    void disableItemFreqDecayerTask();

    MockItemFreqDecayerTask* getMockItemFreqDecayerTask();

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
                           const nlohmann::json& replicationTopology,
                           uint64_t maxVisibleSeqno) override;

    void setDurabilityCompletionTask(
            std::shared_ptr<DurabilityCompletionTask> task);

    /// @returns a non-const pointer to Flusher object.
    Flusher* getFlusherNonConst(Vbid vbid);
};
