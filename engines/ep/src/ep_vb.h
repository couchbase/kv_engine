/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "dcp/backfill_by_seqno_disk.h"
#include "range_scans/range_scan_owner.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"

class BgFetcher;
class EPBucket;
class KVStoreIface;
class KVStoreRevision;

struct vbucket_state;

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
              SyncWriteResolvedCallback syncWriteResolvedCb,
              SyncWriteCompleteCallback syncWriteCb,
              SyncWriteTimeoutHandlerFactory syncWriteTimeoutFactory,
              SeqnoAckCallback seqnoAckCb,
              Configuration& config,
              EvictionPolicy evictionPolicy,
              std::unique_ptr<Collections::VB::Manifest> manifest,
              KVBucket* bucket = nullptr,
              vbucket_state_t initState = vbucket_state_dead,
              uint64_t purgeSeqno = 0,
              uint64_t maxCas = 0,
              int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
              bool mightContainXattrs = false,
              const nlohmann::json* replicationTopology = {},
              uint64_t maxVisibleSeqno = 0);

    ~EPVBucket() override;

    cb::engine_errc completeBGFetchForSingleItem(
            const DiskDocKey& key,
            const FrontEndBGFetchItem& fetched_item,
            const std::chrono::steady_clock::time_point startTime) override;

    /**
     * Expire an item found during compaction that required a BGFetch
     *
     * @param key - key of the item
     * @param fetchedItem - The BGFetched item and the original
     */
    void completeCompactionExpiryBgFetch(
            const DiskDocKey& key, const CompactionBGFetchItem& fetchedItem);

    vb_bgfetch_queue_t getBGFetchItems() override;

    bool hasPendingBGFetchItems() override;

    HighPriorityVBReqStatus checkAddHighPriorityVBEntry(
            uint64_t seqno,
            const CookieIface* cookie,
            std::chrono::milliseconds timeout) override;

    void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) override;

    size_t getNumItems() const override;

    void setNumTotalItems(size_t items) override;

    size_t getNumTotalItems() const override;

    void incrNumTotalItems(size_t numItemsAdded) override;

    void decrNumTotalItems(size_t numItemsRemoved) override;

    size_t getNumNonResidentItems() const override;

    size_t getNumSystemItems() const override;

    cb::engine_errc statsVKey(const DocKey& key,
                              const CookieIface* cookie,
                              EventuallyPersistentEngine& engine) override;

    void completeStatsVKey(const DocKey& key, const GetValue& gcb) override;

    cb::mcbp::Status evictKey(
            const char** msg,
            const Collections::VB::CachingReadHandle& cHandle) override;

    bool pageOut(const Collections::VB::ReadHandle& readHandle,
                 const HashTable::HashBucketLock& lh,
                 StoredValue*& v,
                 bool isDropped) override;

    bool eligibleToPageOut(const HashTable::HashBucketLock& lh,
                           const StoredValue& v) const override;

    size_t getPageableMemUsage() override;

    bool areDeletedItemsAlwaysResident() const override;

    void addStats(VBucketStatsDetailLevel detail,
                  const AddStatFn& add_stat,
                  const CookieIface* c) override;

    KVShard* getShard() override {
        return shard;
    }

    BgFetcher& getBgFetcher();
    Flusher* getFlusher() override;

    UniqueDCPBackfillPtr createDCPBackfill(EventuallyPersistentEngine& e,
                                           std::shared_ptr<ActiveStream> stream,
                                           uint64_t startSeqno,
                                           uint64_t endSeqno) override;

    UniqueDCPBackfillPtr createDCPBackfill(EventuallyPersistentEngine& e,
                                           std::shared_ptr<ActiveStream> stream,
                                           CollectionID cid) override;

    uint64_t getPersistenceSeqno() const override {
        return persistenceSeqno.load();
    }

    uint64_t getPublicPersistenceSeqno() const override {
        // For EPVBuckets this is the same as the PersistenceSeqno.
        return getPersistenceSeqno();
    }

    /**
     * Setup deferred deletion, this is where deletion of the vbucket is
     * deferred and completed by an AUXIO task as it will hit disk for the data
     * file unlink.
     *
     * @param cookie A cookie to notify when the deletion task completes.
     */
    void setupDeferredDeletion(const CookieIface* cookie) override;

    /**
     * Should only be called by the deletion task which ensures that all other
     * references to this object are out of scope.
     *
     * @return the file revision to be unlinked by the deferred deletion task
     */
    std::unique_ptr<KVStoreRevision> takeDeferredDeletionFileRevision();

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
     * @param checkMemUsed true if the insert should check if there's memory
     *        for the item.
     *
     * @return the result of the operation
     */
    MutationStatus insertFromWarmup(Item& itm,
                                    bool eject,
                                    bool keyMetaDataOnly,
                                    bool checkMemUsed);

    /**
     * Restores the state of outstanding Prepared SyncWrites during warmup.
     * Populates the HashTable and the DurabilityMonitor with the given
     * set of queued_items.
     *
     * @param vbs The vbucket_state read during warmup
     * @param outstandingPrepares Sequence of prepared_sync_writes, sorted by
     *        seqno in ascending order.
     */
    void loadOutstandingPrepares(
            const vbucket_state& vbs,
            std::vector<queued_item>&& outstandingPrepares);

    size_t getNumPersistedDeletes() const override;

    /**
     * If the key@bySeqno is found, drop it from the hash table
     *
     * @paran key key to drop
     * @param bySeqno The seqno of the key to drop
     */
    void dropKey(const DocKey& key, int64_t bySeqno) override;

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
     * @param assignedSeqnoCallback passed to CheckpointManager::queueDirty
     */
    uint64_t addSystemEventItem(
            std::unique_ptr<Item> item,
            OptionalSeqno seqno,
            std::optional<CollectionID> cid,
            const Collections::VB::WriteHandle& wHandle,
            std::function<void(uint64_t)> assignedSeqnoCallback) override;

    /**
     * @return std::function that will invoke WriteHandle::saveDroppedCollection
     */
    std::function<void(int64_t)> getSaveDroppedCollectionCallback(
            CollectionID cid,
            Collections::VB::WriteHandle& writeHandle,
            const Collections::VB::ManifestEntry& droppedEntry) const override;

    /**
     * Update failovers, checkpoint mgr and other vBucket members after
     * rollback.
     *
     * @param rollbackResult contains high seqno of the vBucket after rollback,
     *                       snapshot start seqno of the last snapshot in the
     *                       vBucket after the rollback,
     *                       snapshot end seqno of the last snapshot in the
     *                       vBucket after the rollback
     * @param prevHighSeqno high seqno before the rollback
     * @param bucket The KVBucket which logically owns the VBucket
     */
    void postProcessRollback(const RollbackResult& rollbackResult,
                             uint64_t prevHighSeqno,
                             KVBucket& bucket);

    /**
     * Use the KVStore to calculate and set the 'onDiskItemCount'
     */
    void setNumTotalItems(KVStoreIface& kvstore);

    void notifyFlusher() override;

    cb::engine_errc createRangeScan(CollectionID cid,
                                    cb::rangescan::KeyView start,
                                    cb::rangescan::KeyView end,
                                    RangeScanDataHandlerIFace& handler,
                                    const CookieIface& cookie,
                                    cb::rangescan::KeyOnly keyOnly) override;
    cb::engine_errc continueRangeScan(
            cb::rangescan::Id id,
            const CookieIface& cookie,
            size_t itemLimit,
            std::chrono::milliseconds timeLimit) override;
    cb::engine_errc cancelRangeScan(cb::rangescan::Id id,
                                    bool schedule) override;

    /**
     * Function to be called after a RangeScanCreateTask notifies success.
     * This will perform the final steps of creation (completing the command)
     */
    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScanComplete(
            const CookieIface& cookie);

    /**
     * Get the scan associated with the given id
     *
     * @return if found a scan, else a nullptr
     */
    std::shared_ptr<RangeScan> getRangeScan(cb::rangescan::Id id) const;

    /**
     * Add a new range scan
     *
     * @param scan add this scan to the internal set of available scans
     * @return success if the scan was successfully added
     */
    cb::engine_errc addNewRangeScan(std::shared_ptr<RangeScan> scan);

protected:
    /**
     * queue a background fetch of the specified item.
     * Returns the number of pending background fetches after
     * adding the specified item.
     */
    size_t queueBGFetchItem(const DocKey& key,
                            std::unique_ptr<BGFetchItem> fetch,
                            BgFetcher& bgFetcher);

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

    VBNotifyCtx commitStoredValue(HashTable::FindUpdateResult& values,
                                  uint64_t prepareSeqno,
                                  const VBQueueItemCtx& queueItmCtx,
                                  std::optional<int64_t> commitSeqno) override;

    VBNotifyCtx abortStoredValue(const HashTable::HashBucketLock& hbl,
                                 StoredValue& v,
                                 int64_t prepareSeqno,
                                 std::optional<int64_t> abortSeqno) override;

    VBNotifyCtx addNewAbort(const HashTable::HashBucketLock& hbl,
                            const DocKey& key,
                            int64_t prepareSeqno,
                            int64_t abortSeqno) override;

    void bgFetch(HashTable::HashBucketLock&& hbl,
                 const DocKey& key,
                 const StoredValue& v,
                 const CookieIface* cookie,
                 EventuallyPersistentEngine& engine,
                 bool isMeta = false) override;

    cb::engine_errc addTempItemAndBGFetch(HashTable::HashBucketLock&& hbl,
                                          const DocKey& key,
                                          const CookieIface* cookie,
                                          EventuallyPersistentEngine& engine,
                                          bool metadataOnly) override;

    cb::engine_errc bgFetchForCompactionExpiry(HashTable::HashBucketLock& hbl,
                                               const DocKey& key,
                                               const Item& item) override;

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

    GetValue getInternalNonResident(HashTable::HashBucketLock&& hbl,
                                    const DocKey& key,
                                    const CookieIface* cookie,
                                    EventuallyPersistentEngine& engine,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateNewMemoryUsage(EPStats& st, const Item& item) override;

    bool isValidDurabilityLevel(cb::durability::Level level) override;

    void processImplicitlyCompletedPrepare(
            HashTable::StoredValueProxy& v) override;

    /**
     * Update collections following a rollback
     *
     * @param bucket The KVBucket which logically owns the VBucket
     */
    void collectionsRolledBack(KVBucket& bucket);

    /**
     * Clears all checkpoints and high-seqno in CM and resets the DWQ counters.
     *
     * @param seqno The high-seqno to set for the cleared CM
     */
    void clearCMAndResetDiskQueueStats(uint64_t seqno);

    /**
     * Remove the given stored value from the hash-table
     */
    void dropStoredValue(const HashTable::HashBucketLock& hbl,
                         StoredValue& value);

    /**
     * Total number of alive (non-deleted), Committed items on-disk in this
     * vBucket (excludes Prepares).
     * Initially populated during warmup as the number of items on disk;
     * then incremented / decremented by persistence callbacks as new
     * items are created & old items deleted.
     */
    cb::AtomicNonNegativeCounter<size_t> onDiskTotalItems;

    std::mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    /* Pointer to the shard to which this VBucket belongs to */
    KVShard* shard;

    /**
     * When deferred deletion is enabled for this object we store the database
     * file revision we will unlink from disk.
     */
    std::unique_ptr<KVStoreRevision> deferredDeletionFileRevision;

    /**
     * All of this VBucket's RangeScan objects are owned by this member
     */
    VB::RangeScanOwner rangeScans;

    friend class EPVBucketTest;
};
