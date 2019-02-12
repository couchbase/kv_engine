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
    EPVBucket(Vbid i,
              vbucket_state_t newState,
              EPStats& st,
              CheckpointConfig& chkConfig,
              KVShard* kvshard,
              int64_t lastSeqno,
              uint64_t lastSnapStart,
              uint64_t lastSnapEnd,
              std::unique_ptr<FailoverTable> table,
              std::shared_ptr<Callback<Vbid>> flusherCb,
              NewSeqnoCallback newSeqnoCb,
              SyncWriteCompleteCallback syncWriteCb,
              Configuration& config,
              item_eviction_policy_t evictionPolicy,
              std::unique_ptr<Collections::VB::Manifest> manifest,
              vbucket_state_t initState = vbucket_state_dead,
              uint64_t purgeSeqno = 0,
              uint64_t maxCas = 0,
              int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
              bool mightContainXattrs = false);

    ~EPVBucket();

    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const std::chrono::steady_clock::time_point startTime) override;

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

    size_t getNumTotalItems() const override;

    void incrNumTotalItems() override;

    void decrNumTotalItems() override;

    size_t getNumNonResidentItems() const override;

    size_t getNumSystemItems() const override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                const void* cookie,
                                EventuallyPersistentEngine& engine) override;

    void completeStatsVKey(const DocKey& key, const GetValue& gcb) override;

    cb::mcbp::Status evictKey(
            const DocKey& key,
            const char** msg,
            const Collections::VB::Manifest::CachingReadHandle& cHandle)
            override;

    bool pageOut(const Collections::VB::Manifest::ReadHandle& readHandle,
                 const HashTable::HashBucketLock& lh,
                 StoredValue*& v) override;

    bool eligibleToPageOut(const HashTable::HashBucketLock& lh,
                           const StoredValue& v) const override;

    bool areDeletedItemsAlwaysResident() const override;

    void addStats(bool details,
                  const AddStatFn& add_stat,
                  const void* c) override;

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

    size_t getNumPersistedDeletes() const override;

    /**
     * If the key@bySeqno is found, drop it from the hash table
     *
     * @param key The key to look for
     * @param bySeqno The seqno of the key to remove
     * @param cHandle Collections readhandle (caching mode) for this key
     */
    void dropKey(
            const DocKey& key,
            int64_t bySeqno,
            Collections::VB::Manifest::CachingReadHandle& cHandle) override;

    /**
     * Add a system event Item to the vbucket and return its seqno.
     *
     * In persistent buckets the item goes straight to the checkpoint and never
     * in the hash-table.
     *
     * @param item an Item object to queue, can be any kind of item and will be
     *        given a CAS and seqno by this function.
     * @param seqno An optional sequence number, if not specified checkpoint
     *        queueing will assign a seqno to the Item.
     * @param cid The collection ID that this system event is concerned with.
     *        Optional as this may be a scope system event.
     * @param wHandle Collections write handle under which this operation is
     *        locked.
     */
    int64_t addSystemEventItem(
            Item* item,
            OptionalSeqno seqno,
            boost::optional<CollectionID> cid,
            const Collections::VB::Manifest::WriteHandle& wHandle) override;

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

    std::tuple<StoredValue*, DeletionStatus, VBNotifyCtx> softDeleteStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            bool onlyMarkDeleted,
            const VBQueueItemCtx& queueItmCtx,
            uint64_t bySeqno,
            DeleteSource deleteSource = DeleteSource::Explicit) override;

    VBNotifyCtx commitStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            const VBQueueItemCtx& queueItmCtx,
            boost::optional<int64_t> commitSeqno) override;

    void bgFetch(const DocKey& key,
                 const void* cookie,
                 EventuallyPersistentEngine& engine,
                 bool isMeta = false) override;

    ENGINE_ERROR_CODE
    addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                          const DocKey& key,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          bool metadataOnly) override;

    /**
     * Helper function to update stats after completion of a background fetch
     * for either the value of metadata of a key.
     *
     * @param init the time of epstore's initialization
     * @param start the time when the background fetch was started
     * @param stop the time when the background fetch completed
     */
    void updateBGStats(const std::chrono::steady_clock::time_point init,
                       const std::chrono::steady_clock::time_point start,
                       const std::chrono::steady_clock::time_point stop);

    GetValue getInternalNonResident(const DocKey& key,
                                    const void* cookie,
                                    EventuallyPersistentEngine& engine,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateNewMemoryUsage(EPStats& st, const Item& item) override;

    /**
     * Total number of alive (non-deleted) items on-disk in this vBucket.
     * Initially populated during warmup as the number of items on disk;
     * then incremented / decremented by persistence callbacks as new
     * items are created & old items deleted.
     */
    cb::NonNegativeCounter<size_t> onDiskTotalItems;

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
