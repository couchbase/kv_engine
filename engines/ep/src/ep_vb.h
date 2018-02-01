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
#include "dcp/backfill_disk.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"

class BgFetcher;

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
              uint64_t maxCas = 0,
              int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
              bool mightContainXattrs = false,
              const std::string& collectionsManifest = "");

    ~EPVBucket();

    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const ProcessClock::time_point startTime) override;

    vb_bgfetch_queue_t getBGFetchItems() override;

    bool hasPendingBGFetchItems() override;

    HighPriorityVBReqStatus checkAddHighPriorityVBEntry(
            uint64_t seqnoOrChkId,
            const void* cookie,
            HighPriorityVBNotify reqType) override;

    void notifyHighPriorityRequests(EventuallyPersistentEngine& engine,
                                    uint64_t id,
                                    HighPriorityVBNotify notifyType) override;

    void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) override;

    size_t getNumItems() const override;

    void setNumTotalItems(size_t items) override;

    void incrNumTotalItems() override;

    void decrNumTotalItems() override;

    size_t getNumNonResidentItems() const override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                const void* cookie,
                                EventuallyPersistentEngine& engine,
                                int bgFetchDelay) override;

    void completeStatsVKey(const DocKey& key, const GetValue& gcb) override;

    protocol_binary_response_status evictKey(const DocKey& key,
                                             const char** msg) override;

    bool pageOut(const HashTable::HashBucketLock& lh, StoredValue*& v) override;

    bool areDeletedItemsAlwaysResident() const override;

    void addStats(bool details, ADD_STAT add_stat, const void* c) override;

    KVShard* getShard() override {
        return shard;
    }

    UniqueDCPBackfillPtr createDCPBackfill(EventuallyPersistentEngine& e,
                                           std::shared_ptr<ActiveStream> stream,
                                           uint64_t startSeqno,
                                           uint64_t endSeqno) override {
        /* create a disk backfill object */
        return std::make_unique<DCPBackfillDisk>(
                e, stream, startSeqno, endSeqno);
    }

    uint64_t getPersistenceSeqno() const override {
        return persistenceSeqno.load();
    }

    uint64_t getPublicPersistenceSeqno() const override {
        // For EPVBuckets this is the same as the PersistenceSeqno.
        return getPersistenceSeqno();
    }

    void queueBackfillItem(queued_item& qi,
                           const GenerateBySeqno generateBySeqno) override;

    /**
     * Setup deferred deletion, this is where deletion of the vbucket is
     * deferred and completed by an AUXIO task as it will hit disk for the data
     * file unlink.
     *
     * @param cookie A cookie to notify when the deletion task completes.
     */
    void setupDeferredDeletion(const void* cookie) override;

    /**
     * @return the file revision to be unlinked by the deferred deletion task
     */
    uint64_t getDeferredDeletionFileRevision() const {
        return deferredDeletionFileRevision;
    }

    /**
     * Schedule a VBucketMemoryAndDiskDeletionTask to delete this object.
     * @param engine owning engine (required for task construction)
     */
    void scheduleDeferredDeletion(EventuallyPersistentEngine& engine) override;

    /**
     * Insert an item into the VBucket during warmup. If we're trying to insert
     * a partial item we mark it as nonResident
     *
     * @param itm Item to insert. itm is not modified. But cannot be passed as
     *            const because it is passed to functions that can generally
     *            modify the itm but do not modify it due to the flags passed.
     * @param eject true if we should eject the value immediately
     * @param keyMetaDataOnly is this just the key and meta-data or a complete
     *                        item
     *
     * @return the result of the operation
     */
    MutationStatus insertFromWarmup(Item& itm,
                                    bool eject,
                                    bool keyMetaDataOnly);

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
    std::tuple<StoredValue*, MutationStatus, VBNotifyCtx> updateStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            const Item& itm,
            const VBQueueItemCtx& queueItmCtx,
            bool justTouch = false) override;

    std::pair<StoredValue*, VBNotifyCtx> addNewStoredValue(
            const HashTable::HashBucketLock& hbl,
            const Item& itm,
            const VBQueueItemCtx& queueItmCtx,
            GenerateRevSeqno genRevSeqno) override;

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

    GetValue getInternalNonResident(const DocKey& key,
                                    const void* cookie,
                                    EventuallyPersistentEngine& engine,
                                    int bgFetchDelay,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateNewMemoryUsage(EPStats& st, const Item& item) override {
        return st.getEstimatedTotalMemoryUsed() +
               StoredValue::getRequiredStorage(item);
    }

    /**
     * Total number of alive (non-deleted) items on-disk in this vBucket.
     * Initially populated during warmup as the number of items on disk;
     * then incremented / decremented by persistence callbacks as new
     * items are created & old items deleted.
     */
    cb::NonNegativeCounter<size_t> onDiskTotalItems;

    /* Indicates if multiple bg fetches are handled in a single bg fetch task */
    const bool multiBGFetchEnabled;

    std::mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    /* Pointer to the shard to which this VBucket belongs to */
    KVShard* shard;

    /**
     * When deferred deletion is enabled for this object we store the database
     * file revision we will unlink from disk.
     */
    std::atomic<uint64_t> deferredDeletionFileRevision;

    friend class EPVBucketTest;
};
