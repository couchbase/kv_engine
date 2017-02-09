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

#include "bgfetcher.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "flusher.h"

EPBucket::EPBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine) {
    const std::string& policy =
            engine.getConfiguration().getItemEvictionPolicy();
    if (policy.compare("value_only") == 0) {
        eviction_policy = VALUE_ONLY;
    } else {
        eviction_policy = FULL_EVICTION;
    }
}

bool EPBucket::initialize() {
    KVBucket::initialize();

    if (!startBgFetcher()) {
        LOG(EXTENSION_LOG_FATAL,
           "EPBucket::initialize: Failed to create and start bgFetchers");
        return false;
    }
    startFlusher();

    return true;
}

void EPBucket::deinitialize() {
    stopFlusher();
    stopBgFetcher();

    KVBucket::deinitialize();
}

void EPBucket::reset() {
    KVBucket::reset();

    // Need to additionally update disk state
    bool inverse = true;
    deleteAllTaskCtx.delay.compare_exchange_strong(inverse, false);
    // Waking up (notifying) one flusher is good enough for diskDeleteAll
    vbMap.getShard(EP_PRIMARY_SHARD)->getFlusher()->notifyFlushEvent();
}

void EPBucket::startFlusher() {
    for (const auto& shard : vbMap.shards) {
        shard->getFlusher()->start();
    }
}

void EPBucket::stopFlusher() {
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        LOG(EXTENSION_LOG_NOTICE,
            "Attempting to stop the flusher for "
            "shard:%" PRIu16,
            shard->getId());
        bool rv = flusher->stop(stats.forceShutdown);
        if (rv && !stats.forceShutdown) {
            flusher->wait();
        }
    }
}

bool EPBucket::pauseFlusher() {
    bool rv = true;
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        if (!flusher->pause()) {
            LOG(EXTENSION_LOG_WARNING,
                "Attempted to pause flusher in state "
                "[%s], shard = %d",
                flusher->stateName(),
                shard->getId());
            rv = false;
        }
    }
    return rv;
}

bool EPBucket::resumeFlusher() {
    bool rv = true;
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        if (!flusher->resume()) {
            LOG(EXTENSION_LOG_WARNING,
                "Attempted to resume flusher in state [%s], "
                "shard = %" PRIu16,
                flusher->stateName(),
                shard->getId());
            rv = false;
        }
    }
    return rv;
}

void EPBucket::wakeUpFlusher() {
    if (stats.diskQueueSize.load() == 0) {
        for (const auto& shard : vbMap.shards) {
            shard->getFlusher()->wake();
        }
    }
}

bool EPBucket::startBgFetcher() {
    for (const auto& shard : vbMap.shards) {
        BgFetcher* bgfetcher = shard->getBgFetcher();
        if (bgfetcher == NULL) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to start bg fetcher for shard %" PRIu16,
                shard->getId());
            return false;
        }
        bgfetcher->start();
    }
    return true;
}

void EPBucket::stopBgFetcher() {
    for (const auto& shard : vbMap.shards) {
        BgFetcher* bgfetcher = shard->getBgFetcher();
        if (multiBGFetchEnabled() && bgfetcher->pendingJob()) {
            LOG(EXTENSION_LOG_WARNING,
                "Shutting down engine while there are still pending data "
                "read for shard %" PRIu16 " from database storage",
                shard->getId());
        }
        LOG(EXTENSION_LOG_NOTICE,
            "Stopping bg fetcher for shard:%" PRIu16,
            shard->getId());
        bgfetcher->stop();
    }
}

std::pair<uint64_t, bool> EPBucket::getLastPersistedCheckpointId(uint16_t vb) {
    auto vbucket = vbMap.getBucket(vb);
    if (vbucket) {
        return {vbucket->getPersistenceCheckpointId(), true};
    } else {
        return {0, true};
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
    return RCPtr<VBucket>(new EPVBucket(id,
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

ENGINE_ERROR_CODE EPBucket::statsVKey(const DocKey& key,
                                      uint16_t vbucket,
                                      const void* cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->statsVKey(key, cookie, engine, bgFetchDelay);
}

void EPBucket::completeStatsVKey(const void* cookie,
                                 const DocKey& key,
                                 uint16_t vbid,
                                 uint64_t bySeqNum) {
    RememberingCallback<GetValue> gcb;

    getROUnderlying(vbid)->get(key, vbid, gcb);
    gcb.waitForValue();

    if (eviction_policy == FULL_EVICTION) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (vb) {
            vb->completeStatsVKey(key, gcb);
        }
    }

    if (gcb.val.getStatus() == ENGINE_SUCCESS) {
        engine.addLookupResult(cookie, gcb.val.getValue());
    } else {
        engine.addLookupResult(cookie, NULL);
    }

    --stats.numRemainingBgJobs;
    engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
}
