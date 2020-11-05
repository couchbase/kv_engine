/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "mock_ep_bucket.h"

#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "failover-table.h"
#include "kvstore.h"
#include "mock_checkpoint_manager.h"
#include "mock_item_freq_decayer.h"
#include "mock_replicationthrottle.h"

MockEPBucket::MockEPBucket(EventuallyPersistentEngine& theEngine)
    : EPBucket(theEngine) {
    // Replace replicationThrottle with Mock to allow controlling when it
    // pauses during tests.
    replicationThrottle =
            std::make_unique<::testing::NiceMock<MockReplicationThrottle>>(
                    replicationThrottle.release());
    ON_CALL(*this, dropKey)
            .WillByDefault([this](Vbid vbid,
                                  const DiskDocKey& key,
                                  int64_t seqno,
                                  bool isAbort,
                                  int64_t pcs) {
                EPBucket::dropKey(vbid, key, seqno, isAbort, pcs);
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
            &engine, config.getItemFreqDecayerPercent());
}

void MockEPBucket::disableItemFreqDecayerTask() {
    ExecutorPool::get()->cancel(itemFreqDecayerTask->getId());
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
        uint64_t maxVisibleSeqno) {
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
                                      maxVisibleSeqno);
    // Create a MockCheckpointManager.
    vptr->checkpointManager = std::make_unique<MockCheckpointManager>(
            stats,
            id,
            engine.getCheckpointConfig(),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            maxVisibleSeqno,
            /*flusher callback*/ nullptr);
    return vptr;
}

void MockEPBucket::setDurabilityCompletionTask(
        std::shared_ptr<DurabilityCompletionTask> task) {
    durabilityCompletionTask = task;
}

Flusher* MockEPBucket::getFlusherNonConst(Vbid vbid) {
    return vbMap.getShardByVbId(vbid)->getFlusher();
}

void MockEPBucket::setCollectionsManagerPreSetStateAtWarmupHook(
        std::function<void()> hook) {
    Expects(collectionsManager.get());
    collectionsManager->preSetStateAtWarmupHook = hook;
}

void MockEPBucket::completeBGFetchMulti(
        Vbid vbId,
        std::vector<bgfetched_item_t>& fetchedItems,
        std::chrono::steady_clock::time_point start) {
    if (completeBGFetchMultiHook) {
        completeBGFetchMultiHook(vbId);
    }
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
