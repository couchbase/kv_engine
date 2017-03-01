/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

struct HighPriorityVBEntry {
    HighPriorityVBEntry()
        : cookie(NULL), id(0), start(gethrtime()), isBySeqno_(false) {
    }
    HighPriorityVBEntry(const void* c, uint64_t idNum, bool isBySeqno)
        : cookie(c), id(idNum), start(gethrtime()), isBySeqno_(isBySeqno) {
    }

    const void* cookie;
    uint64_t id;
    hrtime_t start;
    bool isBySeqno_;
};

/**
 * Eventually Peristent VBucket (EPVBucket) is a child class of VBucket.
 * It implements the logic of VBucket that is related only to persistence.
 */
class EPVBucket : public VBucket {
public:
    EPVBucket(id_type i,
              vbucket_state_t newState,
              EPStats& st,
              CheckpointConfig& chkConfig,
              KVShard* kvshard,
              int64_t lastSeqno,
              uint64_t lastSnapStart,
              uint64_t lastSnapEnd,
              std::unique_ptr<FailoverTable> table,
              std::shared_ptr<Callback<id_type>> flusherCb,
              NewSeqnoCallback newSeqnoCb,
              Configuration& config,
              item_eviction_policy_t evictionPolicy,
              vbucket_state_t initState = vbucket_state_dead,
              uint64_t purgeSeqno = 0,
              uint64_t maxCas = 0);

    ~EPVBucket();

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
                                int bgFetchDelay) override;

    void completeStatsVKey(const DocKey& key,
                           const RememberingCallback<GetValue>& gcb) override;

    protocol_binary_response_status evictKey(const DocKey& key,
                                             const char** msg) override;

    bool htUnlockedEjectItem(StoredValue*& v) override;

    void addStats(bool details, ADD_STAT add_stat, const void* c) override;

    KVShard* getShard() override {
        return shard;
    }

    static size_t getCheckpointFlushTimeout();

protected:
    /**
     * queue a background fetch of the specified item.
     * Returns the number of pending background fetches after
     * adding the specified item.
     */
    size_t queueBGFetchItem(const DocKey& key,
                            std::unique_ptr<VBucketBGFetchItem> fetch,
                            BgFetcher* bgFetcher);

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

    /**
     * Helper function to update stats after completion of a background fetch
     * for either the value of metadata of a key.
     *
     * @param init the time of epstore's initialization
     * @param start the time when the background fetch was started
     * @param stop the time when the background fetch completed
     */
    void updateBGStats(const ProcessClock::time_point init,
                       const ProcessClock::time_point start,
                       const ProcessClock::time_point stop);

    void adjustCheckpointFlushTimeout(size_t wall_time);

    GetValue getInternalNonResident(const DocKey& key,
                                    const void* cookie,
                                    EventuallyPersistentEngine& engine,
                                    int bgFetchDelay,
                                    get_options_t options,
                                    const StoredValue& v) override;

    /* Indicates if multiple bg fetches are handled in a single bg fetch task */
    const bool multiBGFetchEnabled;

    std::mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    /* Members to handle high priority VB (persistence related) tasks */
    std::mutex hpChksMutex;
    std::list<HighPriorityVBEntry> hpChks;
    std::atomic<size_t> numHpChks; // size of list hpChks (to avoid MB-9434)

    /* Pointer to the shard to which this VBucket belongs to */
    KVShard* shard;

    static std::atomic<size_t> chkFlushTimeout;
};
