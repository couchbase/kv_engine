/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "ephemeral_bucket.h"

#include "ep_engine.h"
#include "ephemeral_vb.h"

EphemeralBucket::EphemeralBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine) {
    /* We always have VALUE_ONLY eviction policy because a key not
       present in HashTable implies key not present at all.
       Note: This should not be confused with the eviction algorithm
             that we are going to use like NRU, FIFO etc. */
    eviction_policy = VALUE_ONLY;
}

RCPtr<VBucket> EphemeralBucket::makeVBucket(
        VBucket::id_type id,
        vbucket_state_t state,
        KVShard* shard,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        vbucket_state_t initState,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        uint64_t purgeSeqno,
        uint64_t maxCas) {
    return RCPtr<VBucket>(new EphemeralVBucket(id,
                                               state,
                                               stats,
                                               engine.getCheckpointConfig(),
                                               shard,
                                               lastSeqno,
                                               lastSnapStart,
                                               lastSnapEnd,
                                               std::move(table),
                                               std::move(newSeqnoCb),
                                               engine.getConfiguration(),
                                               eviction_policy,
                                               initState,
                                               purgeSeqno,
                                               maxCas));
}

void EphemeralBucket::completeStatsVKey(const void* cookie,
                                        const DocKey& key,
                                        uint16_t vbid,
                                        uint64_t bySeqNum) {
    throw std::logic_error(
            "EphemeralBucket::completeStatsVKey() "
            "is not a valid call. Called on vb " +
            std::to_string(vbid) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}
