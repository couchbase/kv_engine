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

/* Forward declarations */
class SequenceList;

class EphemeralVBucket : public VBucket {
public:
    class CountVisitor;

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
                     uint64_t maxCas = 0,
                     const std::string& collectionsManifest = "");

    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const ProcessClock::time_point startTime) override;

    void resetStats() override;

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

    bool pageOut(const HashTable::HashBucketLock& lh, StoredValue*& v) override;

    void addStats(bool details, ADD_STAT add_stat, const void* c) override;

    KVShard* getShard() override {
        return nullptr;
    }

    std::unique_ptr<DCPBackfill> createDCPBackfill(
            EventuallyPersistentEngine& e,
            const active_stream_t& stream,
            uint64_t startSeqno,
            uint64_t endSeqno) override;

    /**
     * Reads backfill items from in memory ordered data structure
     *
     * @param startSeqno requested start sequence number of the backfill
     * @param endSeqno requested end sequence number of the backfill
     *
     * @return ENGINE_SUCCESS and items in the snapshot
     *         ENGINE_ENOMEM on no memory to copy items
     *         ENGINE_ERANGE on incorrect start and end
     */
    std::pair<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>> inMemoryBackfill(
            uint64_t start, uint64_t end);

    void dump() const override;

    uint64_t getPersistenceSeqno() const override {
        /* We do not have persistence in an ephemeral vb. hence we return
           the last seen seqno (highSeqno) as the persisted seqno.
           This is needed because higher layers like ns_server have long
           considered persisted seqno as last seen seqno for certain operations
           like vb takeover */
        return static_cast<uint64_t>(getHighSeqno());
    }

private:
    std::tuple<StoredValue*, MutationStatus, VBNotifyCtx> updateStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) override;

    std::pair<StoredValue*, VBNotifyCtx> addNewStoredValue(
            const HashTable::HashBucketLock& hbl,
            const Item& itm,
            const VBQueueItemCtx* queueItmCtx) override;

    std::tuple<StoredValue*, VBNotifyCtx> softDeleteStoredValue(
            const HashTable::HashBucketLock& hbl,
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

    /* Data structure for in-memory sequential storage */
    std::unique_ptr<SequenceList> seqList;

    /**
     * Count of how many items have been deleted via the 'auto_delete' policy
     */
    EPStats::Counter autoDeleteCount;
};
