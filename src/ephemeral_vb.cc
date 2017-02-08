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
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "ephemeral_vb.h"

EphemeralVBucket::EphemeralVBucket(
        id_type i,
        vbucket_state_t newState,
        EPStats& st,
        CheckpointConfig& chkConfig,
        KVShard* kvshard,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        Configuration& config,
        item_eviction_policy_t evictionPolicy,
        vbucket_state_t initState,
        uint64_t purgeSeqno,
        uint64_t maxCas)
    : VBucket(i,
              newState,
              st,
              chkConfig,
              kvshard,
              lastSeqno,
              lastSnapStart,
              lastSnapEnd,
              std::move(table),
              /*flusherCb*/nullptr,
              std::move(newSeqnoCb),
              config,
              evictionPolicy,
              initState,
              purgeSeqno,
              maxCas) {}

std::pair<MutationStatus, VBNotifyCtx> EphemeralVBucket::updateStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    /* Update the StoredValue in HT + EphemeralVBucket data structure.
       From the EphemeralVBucket data structure we will know whether to update
       the current StoredValue or mark the current StoredValue as stale and
       add a new StoredValue.
       (Coming soon)
     */
    MutationStatus status = ht.unlocked_updateStoredValue(htLock, v, itm);

    if (queueItmCtx) {
        return {status, queueDirty(v, *queueItmCtx)};
    }

    /* PlaceHolder for post seqno generation operation in EphemeralVBucket
     * data structure
     */
    return {status, VBNotifyCtx()};
}

std::pair<StoredValue*, VBNotifyCtx> EphemeralVBucket::addNewStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    StoredValue* v = ht.unlocked_addNewStoredValue(htLock, itm);

    std::lock_guard<std::mutex> lh(sequenceLock);

    /* Add the newly added stored value to Ephemeral VBucket data structure.
       (Coming soon)
     */
    if (queueItmCtx) {
        return {v, queueDirty(*v, *queueItmCtx)};
    }

    /* PlaceHolder for post seqno generation operation in EphemeralVBucket
     * data structure
     */
    return {v, VBNotifyCtx()};
}

VBNotifyCtx EphemeralVBucket::softDeleteStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    /* Soft delete the StoredValue in HT + EphemeralVBucket data structure.
       From the EphemeralVBucket data structure we will know whether to update
       (soft delete) the current StoredValue or mark the current StoredValue
       as stale and add a new softDeleted StoredValue.
       (Coming soon)
     */
    ht.unlocked_softDelete(htLock, v, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        v.setBySeqno(bySeqno);
    }

    return queueDirty(v, queueItmCtx);

    /* PlaceHolder for post seqno generation operation in EphemeralVBucket
     * data structure
     */
}
