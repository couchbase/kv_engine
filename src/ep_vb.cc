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
#include "executorpool.h"

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

size_t EPVBucket::getNumItems() const {
    if (eviction == VALUE_ONLY) {
        return ht.getNumInMemoryItems();
    } else {
        return ht.getNumItems();
    }
}

ENGINE_ERROR_CODE EPVBucket::statsVKey(const DocKey& key,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       const int bgFetchDelay) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl, key, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new VKeyStatBGFetchTask(&engine,
                                              key,
                                              getId(),
                                              v->getBySeqno(),
                                              cookie,
                                              bgFetchDelay,
                                              false);
        iom->schedule(task, READER_TASK_IDX);
        return ENGINE_EWOULDBLOCK;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            AddStatus rv = addTempStoredValue(hbl, key);
            switch (rv) {
            case AddStatus::NoMem:
                return ENGINE_ENOMEM;
            case AddStatus::Exists:
            case AddStatus::UnDel:
            case AddStatus::Success:
            case AddStatus::AddTmpAndBgFetch:
                // Since the hashtable bucket is locked, we shouldn't get here
                throw std::logic_error(
                        "VBucket::statsVKey: "
                        "Invalid result from unlocked_addTempItem (" +
                        std::to_string(static_cast<uint16_t>(rv)) + ")");

            case AddStatus::BgFetch: {
                ++stats.numRemainingBgJobs;
                ExecutorPool* iom = ExecutorPool::get();
                ExTask task = new VKeyStatBGFetchTask(
                        &engine, key, getId(), -1, cookie, bgFetchDelay, false);
                iom->schedule(task, READER_TASK_IDX);
            }
            }
            return ENGINE_EWOULDBLOCK;
        }
    }
}

void EPVBucket::completeStatsVKey(const DocKey& key,
                                  const RememberingCallback<GetValue>& gcb) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl, key, eviction, true);

    if (v && v->isTempInitialItem()) {
        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            ht.unlocked_restoreValue(
                    hbl.getHTLock(), *(gcb.val.getValue()), *v);
            if (!v->isResident()) {
                throw std::logic_error(
                        "VBucket::completeStatsVKey: "
                        "storedvalue (which has seqno:" +
                        std::to_string(v->getBySeqno()) +
                        ") should be resident after calling restoreValue()");
            }
        } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
            v->setNonExistent();
        } else {
            // underlying kvstore couldn't fetch requested data
            // log returned error and notify TMPFAIL to client
            LOG(EXTENSION_LOG_WARNING,
                "VBucket::completeStatsVKey: "
                "Failed background fetch for vb:%" PRIu16 ", seqno:%" PRIu64,
                getId(),
                v->getBySeqno());
        }
    }
}

protocol_binary_response_status EPVBucket::evictKey(const DocKey& key,
                                                    const char** msg) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     /*wantDeleted*/ false,
                                     /*trackReference*/ false);

    if (!v) {
        if (eviction == VALUE_ONLY) {
            *msg = "Not found.";
            return PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
        *msg = "Already ejected.";
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }

    if (v->isResident()) {
        if (ht.unlocked_ejectItem(v, eviction)) {
            *msg = "Ejected.";

            // Add key to bloom filter in case of full eviction mode
            if (eviction == FULL_EVICTION) {
                addToFilter(key);
            }
            return PROTOCOL_BINARY_RESPONSE_SUCCESS;
        }
        *msg = "Can't eject: Dirty object.";
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    *msg = "Already ejected.";
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool EPVBucket::htUnlockedEjectItem(StoredValue*& v) {
    return ht.unlocked_ejectItem(v, eviction);
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
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

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
