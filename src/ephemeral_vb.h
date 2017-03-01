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

#pragma once

#include "config.h"

#include "vbucket.h"

class EphemeralVBucket : public VBucket {
public:
    EphemeralVBucket(id_type i,
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
                     vbucket_state_t initState = vbucket_state_dead,
                     uint64_t purgeSeqno = 0,
                     uint64_t maxCas = 0);

    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const ProcessClock::time_point startTime) override;

    vb_bgfetch_queue_t getBGFetchItems() override;

    bool hasPendingBGFetchItems() override;

    ENGINE_ERROR_CODE addHighPriorityVBEntry(uint64_t id,
                                             const void* cookie,
                                             bool isBySeqno) override;

    void notifyOnPersistence(EventuallyPersistentEngine& e,
                             uint64_t id,
                             bool isBySeqno) override;

    void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) override;

    size_t getHighPriorityChkSize() override;

    size_t getNumItems() const override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                const void* cookie,
                                EventuallyPersistentEngine& engine,
                                int bgFetchDelay) override {
        return ENGINE_ENOTSUP;
    }

    void completeStatsVKey(const DocKey& key,
                           const RememberingCallback<GetValue>& gcb) override;

    bool maybeKeyExistsInFilter(const DocKey& key) override {
        /* There is no disk to indicate that a key may exist */
        return false;
    }

    protocol_binary_response_status evictKey(const DocKey& key,
                                             const char** msg) override {
        /* There is nothing (no disk) to evictKey to. Later on if we decide to
           use this as a deletion, then we can handle it differently */
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    bool htUnlockedEjectItem(StoredValue*& v) override {
        /* There is nothing (no disk) to evictKey to. Later on if we decide to
           use this as a deletion, then we can handle it differently */
        return false;
    }

    void addStats(bool details, ADD_STAT add_stat, const void* c) override;

    KVShard* getShard() override {
        return nullptr;
    }

private:
    std::pair<MutationStatus, VBNotifyCtx> updateStoredValue(
            const std::unique_lock<std::mutex>& htLock,
            StoredValue& v,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) override;

    std::pair<StoredValue*, VBNotifyCtx> addNewStoredValue(
            const HashTable::HashBucketLock& hbl,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) override;

    VBNotifyCtx softDeleteStoredValue(
            const std::unique_lock<std::mutex>& htLock,
            StoredValue& v,
            bool onlyMarkDeleted,
            const VBQueueItemCtx& queueItmCtx,
            uint64_t bySeqno) override;

    void bgFetch(const DocKey& key,
                 const void* cookie,
                 EventuallyPersistentEngine& engine,
                 int bgFetchDelay,
                 bool isMeta = false) override;

    ENGINE_ERROR_CODE
    addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                          const DocKey& key,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          int bgFetchDelay,
                          bool metadataOnly,
                          bool isReplication = false) override;

    GetValue getInternalNonResident(const DocKey& key,
                                    const void* cookie,
                                    EventuallyPersistentEngine& engine,
                                    int bgFetchDelay,
                                    get_options_t options,
                                    const StoredValue& v) override;

    /**
     * Lock to synchronize order of bucket elements.
     * The sequence number is not generated in EphemeralVBucket for now. It is
     * generated in the CheckpointManager and is synchronized on "queueLock" in
     * CheckpointManager. This, though undesirable, is needed because the
     * CheckpointManager relies on seqno for its meta(dummy) items and also self
     * generates them.
     *
     * All operations/data structures that rely on ordered sequence of items
     * must hold i) sequenceLock in 'EphemeralVBucket' and then
     * ii) queueLock in 'CheckpointManager'.
     */
    mutable std::mutex sequenceLock;
};
