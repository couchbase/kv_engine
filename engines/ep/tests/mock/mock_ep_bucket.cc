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

#include "mock_ep_bucket.h"

#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "mock_checkpoint_manager.h"
#include "mock_item_freq_decayer.h"
#include "mock_replicationthrottle.h"
#include "vbucket.h"
#include <executor/executorpool.h>

MockEPBucket::MockEPBucket(EventuallyPersistentEngine& theEngine)
    : EPBucket(theEngine) {
    // Replace replicationThrottle with Mock to allow controlling when it
    // pauses during tests.
    replicationThrottle =
            std::make_unique<::testing::NiceMock<MockReplicationThrottle>>(
                    replicationThrottle.release());
    ON_CALL(*this, dropKey)
            .WillByDefault([this](VBucket& vb,
                                  const DiskDocKey& key,
                                  int64_t seqno,
                                  bool isAbort,
                                  int64_t pcs) {
                EPBucket::dropKey(vb, key, seqno, isAbort, pcs);
            });
}

void MockEPBucket::initializeMockBucket() {
    initializeShards();
}

void MockEPBucket::removeMakeCompactionContextCallback() {
    vbMap.forEachShard([this](KVShard& shard) {
        shard.getRWUnderlying()->setMakeCompactionContextCallback(nullptr);
    });
}

void MockEPBucket::createItemFreqDecayerTask() {
    Configuration& config = engine.getConfiguration();
    itemFreqDecayerTask = std::make_shared<MockItemFreqDecayerTask>(
            engine, config.getItemFreqDecayerPercent());
}

void MockEPBucket::disableItemFreqDecayerTask() {
    ExecutorPool::get()->cancel(itemFreqDecayerTask->getId());
}

ItemFreqDecayerTask* MockEPBucket::getItemFreqDecayerTask() {
    return dynamic_cast<ItemFreqDecayerTask*>(itemFreqDecayerTask.get());
}

MockItemFreqDecayerTask* MockEPBucket::getMockItemFreqDecayerTask() {
    return dynamic_cast<MockItemFreqDecayerTask*>(itemFreqDecayerTask.get());
}

VBucketPtr MockEPBucket::makeVBucket(
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
        uint64_t maxVisibleSeqno,
        uint64_t maxPrepareSeqno) {
    auto vptr = EPBucket::makeVBucket(id,
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
                                      maxVisibleSeqno,
                                      maxPrepareSeqno);
    // Create a MockCheckpointManager.
    vptr->checkpointManager = std::make_unique<MockCheckpointManager>(
            stats,
            *vptr,
            engine.getCheckpointConfig(),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            maxVisibleSeqno,
            maxPrepareSeqno,
            std::make_shared<NotifyFlusherCB>(shard));
    return vptr;
}

void MockEPBucket::setDurabilityCompletionTask(
        std::shared_ptr<DurabilityCompletionTask> task) {
    durabilityCompletionTask = task;
}

Flusher* MockEPBucket::getFlusherNonConst(Vbid vbid) {
    return getFlusher(vbid);
}

void MockEPBucket::completeBGFetchMulti(
        Vbid vbId,
        std::vector<bgfetched_item_t>& fetchedItems,
        std::chrono::steady_clock::time_point start) {
    completeBGFetchMultiHook(vbId);
    EPBucket::completeBGFetchMulti(vbId, fetchedItems, start);
}

std::shared_ptr<CompactTask> MockEPBucket::getCompactionTask(Vbid vbid) const {
    auto handle = compactionTasks.rlock();
    auto itr = handle->find(vbid);
    if (itr == handle->end()) {
        return {};
    }
    return itr->second;
}

std::shared_ptr<CompactionContext> MockEPBucket::makeCompactionContext(
        Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) {
    auto context = EPBucket::makeCompactionContext(vbid, config, purgeSeqno);
    return mockMakeCompactionContext(context);
}

void MockEPBucket::publicCompactionCompletionCallback(CompactionContext& ctx) {
    compactionCompletionCallback(ctx);
}

cb::AwaitableSemaphore& MockEPBucket::public_getCompactionSemaphore() {
    return *compactionSemaphore;
}

void MockEPBucket::public_updateCompactionConcurrency() {
    updateCompactionConcurrency();
}
