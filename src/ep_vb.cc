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

#include "ep_vb.h"

EPVBucket::EPVBucket(id_type i,
                     vbucket_state_t newState,
                     EPStats& st,
                     CheckpointConfig& chkConfig,
                     KVShard* kvshard,
                     int64_t lastSeqno,
                     uint64_t lastSnapStart,
                     uint64_t lastSnapEnd,
                     std::unique_ptr<FailoverTable> table,
                     std::shared_ptr<Callback<id_type> > flusherCb,
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
              flusherCb,
              std::move(newSeqnoCb),
              config,
              evictionPolicy,
              initState,
              purgeSeqno,
              maxCas) {
}

std::pair<MutationStatus, VBNotifyCtx> EPVBucket::updateStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    MutationStatus status = ht.unlocked_updateStoredValue(htLock, v, itm);

    if (queueItmCtx) {
        return {status, queueDirty(v, *queueItmCtx)};
    }
    return {status, VBNotifyCtx()};
}

std::pair<StoredValue*, VBNotifyCtx> EPVBucket::addNewStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx,
        int bucketNum) {
    StoredValue* v = ht.unlocked_addNewStoredValue(htLock, itm, bucketNum);

    if (queueItmCtx) {
        return {v, queueDirty(*v, *queueItmCtx)};
    }

    return {v, VBNotifyCtx()};
}

VBNotifyCtx EPVBucket::softDeleteStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    ht.unlocked_softDelete(htLock, v, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        v.setBySeqno(bySeqno);
    }

    return queueDirty(v, queueItmCtx);
}
