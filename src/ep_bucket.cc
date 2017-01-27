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

#include "ep_bucket.h"

#include "ep_engine.h"
#include "flusher.h"

EPBucket::EPBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine) {
}

bool EPBucket::initialize() {
    KVBucket::initialize();

    startFlusher();

    return true;
}

void EPBucket::deinitialize() {
    stopFlusher();

    KVBucket::deinitialize();
}

protocol_binary_response_status EPBucket::evictKey(const DocKey& key,
                                                   uint16_t vbucket,
                                                   const char** msg,
                                                   size_t* msg_size) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() != vbucket_state_active)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    auto lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = vb->fetchValidValue(lh,
                                         key,
                                         bucket_num,
                                         /*wantDeleted*/ false,
                                         /*trackReference*/ false);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    *msg_size = 0;
    if (v) {
        if (v->isResident()) {
            if (vb->ht.unlocked_ejectItem(v, eviction_policy)) {
                *msg = "Ejected.";

                // Add key to bloom filter incase of full eviction mode
                if (getItemEvictionPolicy() == FULL_EVICTION) {
                    vb->addToFilter(key);
                }
            } else {
                *msg = "Can't eject: Dirty object.";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
            }
        } else {
            *msg = "Already ejected.";
        }
    } else {
        if (eviction_policy == VALUE_ONLY) {
            *msg = "Not found.";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            *msg = "Already ejected.";
        }
    }

    return rv;
}

void EPBucket::reset() {
    KVBucket::reset();

    // Need to additionally update disk state
    bool inverse = true;
    flushAllTaskCtx.delayFlushAll.compare_exchange_strong(inverse, false);
    // Waking up (notifying) one flusher is good enough for diskFlushAll
    vbMap.getShard(EP_PRIMARY_SHARD)->getFlusher()->notifyFlushEvent();
}

void EPBucket::startFlusher() {
    for (const auto& shard : vbMap.shards) {
        shard->getFlusher()->start();
    }
}

void EPBucket::stopFlusher() {
    for (uint16_t i = 0; i < vbMap.shards.size(); i++) {
        Flusher* flusher = vbMap.shards[i]->getFlusher();
        LOG(EXTENSION_LOG_NOTICE,
            "Attempting to stop the flusher for "
            "shard:%" PRIu16,
            i);
        bool rv = flusher->stop(stats.forceShutdown);
        if (rv && !stats.forceShutdown) {
            flusher->wait();
        }
    }
}

bool EPBucket::pauseFlusher() {
    bool rv = true;
    for (uint16_t i = 0; i < vbMap.shards.size(); i++) {
        Flusher* flusher = vbMap.shards[i]->getFlusher();
        if (!flusher->pause()) {
            LOG(EXTENSION_LOG_WARNING,
                "Attempted to pause flusher in state "
                "[%s], shard = %d",
                flusher->stateName(),
                i);
            rv = false;
        }
    }
    return rv;
}

bool EPBucket::resumeFlusher() {
    bool rv = true;
    for (uint16_t i = 0; i < vbMap.shards.size(); i++) {
        Flusher* flusher = vbMap.shards[i]->getFlusher();
        if (!flusher->resume()) {
            LOG(EXTENSION_LOG_WARNING,
                "Attempted to resume flusher in state [%s], "
                "shard = %d",
                flusher->stateName(),
                i);
            rv = false;
        }
    }
    return rv;
}

void EPBucket::wakeUpFlusher() {
    if (stats.diskQueueSize.load() == 0) {
        for (uint16_t i = 0; i < vbMap.shards.size(); i++) {
            Flusher *flusher = vbMap.shards[i]->getFlusher();
            flusher->wake();
        }
    }
}

ENGINE_ERROR_CODE EPBucket::getFileStats(const void* cookie,
                                         ADD_STAT add_stat) {
    const auto numShards = vbMap.getNumShards();
    DBFileInfo totalInfo;

    for (uint16_t shardId = 0; shardId < numShards; shardId++) {
        const auto dbInfo =
                getRWUnderlyingByShard(shardId)->getAggrDbFileInfo();
        totalInfo.spaceUsed += dbInfo.spaceUsed;
        totalInfo.fileSize += dbInfo.fileSize;
    }

    add_casted_stat("ep_db_data_size", totalInfo.spaceUsed, add_stat, cookie);
    add_casted_stat("ep_db_file_size", totalInfo.fileSize, add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EPBucket::getPerVBucketDiskStats(const void* cookie,
                                                   ADD_STAT add_stat) {
    class DiskStatVisitor : public VBucketVisitor {
    public:
        DiskStatVisitor(const void* c, ADD_STAT a) : cookie(c), add_stat(a) {
        }

        void visitBucket(RCPtr<VBucket>& vb) override {
            char buf[32];
            uint16_t vbid = vb->getId();
            DBFileInfo dbInfo =
                    vb->getShard()->getRWUnderlying()->getDbFileInfo(vbid);

            try {
                checked_snprintf(buf, sizeof(buf), "vb_%d:data_size", vbid);
                add_casted_stat(buf, dbInfo.spaceUsed, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:file_size", vbid);
                add_casted_stat(buf, dbInfo.fileSize, add_stat, cookie);
            } catch (std::exception& error) {
                LOG(EXTENSION_LOG_WARNING,
                    "DiskStatVisitor::visitBucket: Failed to build stat: %s",
                    error.what());
            }
        }

    private:
        const void* cookie;
        ADD_STAT add_stat;
    };

    DiskStatVisitor dsv(cookie, add_stat);
    visit(dsv);
    return ENGINE_SUCCESS;
}

RCPtr<VBucket> EPBucket::makeVBucket(
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
    auto flusherCb = std::make_shared<NotifyFlusherCB>(shard);
    return RCPtr<VBucket>(new VBucket(id,
                                      state,
                                      stats,
                                      engine.getCheckpointConfig(),
                                      shard,
                                      lastSeqno,
                                      lastSnapStart,
                                      lastSnapEnd,
                                      std::move(table),
                                      flusherCb,
                                      std::move(newSeqnoCb),
                                      engine.getConfiguration(),
                                      eviction_policy,
                                      initState,
                                      purgeSeqno,
                                      maxCas));
}
