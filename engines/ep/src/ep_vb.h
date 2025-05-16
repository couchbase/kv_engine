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
class EventDrivenTimeoutTask;
class KVStoreIface;
class KVStoreRevision;
struct RangeScanCreateToken;
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
              uint64_t maxVisibleSeqno = 0,
              uint64_t maxPrepareSeqno = 0);

    ~EPVBucket() override;

    cb::engine_errc completeBGFetchForSingleItem(
            const DiskDocKey& key,
            const FrontEndBGFetchItem& fetched_item,
            const cb::time::steady_clock::time_point startTime) override;

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
            std::unique_ptr<SeqnoPersistenceRequest> request) override;

    void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) override;

    size_t getNumItems() const override;

    void setNumTotalItems(size_t items) override;

    size_t getNumTotalItems() const override;

    void incrNumTotalItems(size_t numItemsAdded) override;

    void decrNumTotalItems(size_t numItemsRemoved) override;

    size_t getNumNonResidentItems() const override;

    size_t getNumSystemItems() const override;

    uint64_t getHistoryDiskSize() override;

    cb::engine_errc statsVKey(const DocKeyView& key,
                              CookieIface& cookie,
                              EventuallyPersistentEngine& engine) override;

    void completeStatsVKey(const DocKeyView& key, const GetValue& gcb) override;

    cb::engine_errc evictKey(
            const char** msg,
            VBucketStateLockRef vbStateLock,
            const Collections::VB::CachingReadHandle& cHandle) override;

    bool pageOut(VBucketStateLockRef vbStateLock,
                 const Collections::VB::ReadHandle& readHandle,
                 const HashTable::HashBucketLock& lh,
                 StoredValue*& v,
                 bool isDropped) override;

    bool canEvict() const override;

    bool isEligibleForEviction(const HashTable::HashBucketLock& lh,
                               const StoredValue& v) const override;

    size_t getPageableMemUsage() override;

    bool areDeletedItemsAlwaysResident() const override;

    void addStats(VBucketStatsDetailLevel detail,
                  const AddStatFn& add_stat,
                  CookieIface& c) override;

    KVShard* getShard() override {
        return shard;
    }

    /**
     * Gets the BgFetcher for the corresponding vBucket. If there are multiple
     * BgFetchers assigned to this vBucket, the distributionKey is used to
     * select a BgFetcher from that set.
     */
    BgFetcher& getBgFetcher(uint32_t distributionKey);
    Flusher* getFlusher() override;

    UniqueDCPBackfillPtr createDCPBackfill(EventuallyPersistentEngine& e,
                                           std::shared_ptr<ActiveStream> stream,
                                           uint64_t startSeqno,
                                           uint64_t endSeqno) override;

    UniqueDCPBackfillPtr createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream) override;

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
    void setupDeferredDeletion(CookieIface* cookie) override;

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
     * Upsert an item into the VBucket's hash-table, no flush to disk occurs.
     * If we're trying to insert a partial item we mark it as not-resident
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
    MutationStatus upsertToHashTable(Item& itm,
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
    void dropKey(const DocKeyView& key, int64_t bySeqno) override;

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
     * @param vbStateLock a lock on the VBucket state
     * @param rollbackResult contains high seqno of the vBucket after rollback,
     *                       snapshot start seqno of the last snapshot in the
     *                       vBucket after the rollback,
     *                       snapshot end seqno of the last snapshot in the
     *                       vBucket after the rollback
     * @param prevHighSeqno high seqno before the rollback
     * @param bucket The KVBucket which logically owns the VBucket
     */
    void postProcessRollback(VBucketStateLockRef vbStateLock,
                             const RollbackResult& rollbackResult,
                             uint64_t prevHighSeqno,
                             KVBucket& bucket);

    /**
     * Use the KVStore to calculate and set the 'onDiskItemCount'
     */
    void setNumTotalItems(KVStoreIface& kvstore);

    void notifyFlusher() override;

    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            CookieIface& cookie,
            std::unique_ptr<RangeScanDataHandlerIFace> handler,
            const cb::rangescan::CreateParameters& params) override;
    cb::engine_errc continueRangeScan(
            CookieIface& cookie,
            const cb::rangescan::ContinueParameters& params) override;
    cb::engine_errc cancelRangeScan(cb::rangescan::Id id,
                                    CookieIface* cookie) override;
    cb::engine_errc doRangeScanStats(const StatCollector& collector) override;

    /**
     * Function to be called after a RangeScanCreateTask notifies success.
     * This will perform the final steps of creation (completing the command)
     */
    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScanComplete(
            std::unique_ptr<RangeScanCreateToken> rangeScanCreateData,
            CookieIface& cookie);

    /**
     * Setup a SeqnoPersistenceRequest using the RangeScanSnapshotRequirements
     *
     * @return the status of checkAddHighPriorityVBEntry as a task may not of
     *         been scheduled
     */
    HighPriorityVBReqStatus createRangeScanWait(
            const cb::rangescan::SnapshotRequirements& requirements,
            CookieIface& cookie);

    /**
     * A range-scan-create may need to be cancelled after the I/O task has
     * completed.
     *
     * @return status (currently only success)
     */
    cb::engine_errc checkAndCancelRangeScanCreate(CookieIface& cookie);

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

    /**
     * Locate all scans and cancel any with a lifetime > duration
     * @param duration scans with a "lifetime" above this duration are cancelled
     * @return seconds until the next scan would need cancelling or nothing
     */
    std::optional<std::chrono::seconds> cancelRangeScansExceedingDuration(
            std::chrono::seconds duration);

    /**
     * Locate all scans and cancel them
     */
    void cancelRangeScans();

    VB::RangeScanOwner& getRangeScans() {
        return rangeScans;
    }

    void incrementHistoricalItemsFlushed() {
        historicalItemsFlushed++;
    }

    auto getHistoricalItemsFlushed() const {
        return historicalItemsFlushed.load();
    }

    /**
     * BloomFilter operations for vbucket
     */
    void createFilter(size_t key_count, double probability) override;
    void initTempFilter(size_t key_count, double probability);
    void addToFilter(const DocKeyView& key) override;
    bool maybeKeyExistsInFilter(const DocKeyView& key) override;
    bool isTempFilterAvailable();
    void addToTempFilter(const DocKeyView& key);
    void swapFilter();
    void clearFilter() override;
    void setFilterStatus(bfilter_status_t to) override;
    std::string getFilterStatusString() override;
    size_t getFilterSize() override;
    size_t getNumOfKeysInFilter() override;
    void addBloomFilterStats(const AddStatFn& add_stat,
                             CookieIface& c) override;
    void addBloomFilterStats_UNLOCKED(const AddStatFn& add_stat,
                                      CookieIface& c,
                                      const BloomFilter& filter);

    /**
     * @returns The memory usage in bytes of the main bloom filter and
     * temporary bloom filter if it exists.
     */

    size_t getFilterMemoryFootprint() override;

    /// @return true if we should log a flush failure
    bool shouldWarnForFlushFailure();

    /**
     * Persistent buckets process failover from data that the flusher
     * maintains
     */
    failover_entry_t processFailover() override;

    /**
     * Create a new failover entry in the vbucket and generate a
     * set_vbucket_state item to persist the state.
     *
     * @param seqno Sequence number to create failover entry for
     */
    void createFailoverEntry(uint64_t seqno) override;

protected:
    /**
     * queue a background fetch of the specified item.
     * Returns the number of pending background fetches after
     * adding the specified item.
     */
    size_t queueBGFetchItem(const DocKeyView& key,
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

    VBNotifyCtx abortStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            int64_t prepareSeqno,
            std::optional<int64_t> abortSeqno,
            const Collections::VB::CachingReadHandle& cHandle) override;

    VBNotifyCtx addNewAbort(
            const HashTable::HashBucketLock& hbl,
            const DocKeyView& key,
            int64_t prepareSeqno,
            int64_t abortSeqno,
            const Collections::VB::CachingReadHandle& cHandle) override;

    cb::engine_errc bgFetch(HashTable::HashBucketLock&& hbl,
                            const DocKeyView& key,
                            const StoredValue& v,
                            CookieIface* cookie,
                            EventuallyPersistentEngine& engine,
                            bool isMeta = false) override;

    cb::engine_errc addTempItemAndBGFetch(HashTable::HashBucketLock&& hbl,
                                          const DocKeyView& key,
                                          CookieIface* cookie,
                                          EventuallyPersistentEngine& engine,
                                          bool metadataOnly) override;

    [[nodiscard]] std::unique_ptr<CompactionBGFetchItem>
    createBgFetchForCompactionExpiry(const HashTable::HashBucketLock& hbl,
                                     const DocKeyView& key,
                                     const Item& item) override;

    void bgFetchForCompactionExpiry(HashTable::HashBucketLock& hbl,
                                    const DocKeyView& key,
                                    const Item& item) override;

    /**
     * Helper function to update stats after completion of a background fetch
     * for either the value of metadata of a key.
     *
     * @param init the time of epstore's initialization
     * @param start the time when the background fetch was started
     * @param stop the time when the background fetch completed
     */
    void updateBGStats(const cb::time::steady_clock::time_point init,
                       const cb::time::steady_clock::time_point start,
                       const cb::time::steady_clock::time_point stop);

    GetValue getInternalNonResident(HashTable::HashBucketLock&& hbl,
                                    const DocKeyView& key,
                                    CookieIface* cookie,
                                    EventuallyPersistentEngine& engine,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateRequiredMemory(const Item& item) override;

    bool isValidDurabilityLevel(cb::durability::Level level) override;

    void processImplicitlyCompletedPrepare(
            HashTable::StoredValueProxy& v) override;

    /**
     * Update collections following a rollback
     *
     * @param vbStateLock A lock on the VBucket state
     * @param bucket The KVBucket which logically owns the VBucket
     */
    void collectionsRolledBack(VBucketStateLockRef vbStateLock,
                               KVBucket& bucket);

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

    bool isBfilterEnabled();

    /**
     * Sets the collection info on the cookie for the given scan.
     * @return no_such_key if the scan does not exist, unknown_collection if the
     *         scan's collection no longer exists or success.
     */
    cb::engine_errc setupCookieForRangeScan(cb::rangescan::Id id,
                                            CookieIface& cookie);

    /**
     * Total number of alive (non-deleted), Committed items on-disk in this
     * vBucket (excludes Prepares).
     * Initially populated during warmup as the number of items on disk;
     * then incremented / decremented by persistence callbacks as new
     * items are created & old items deleted.
     */
    cb::AtomicNonNegativeCounter<size_t> onDiskTotalItems;

    folly::Synchronized<vb_bgfetch_queue_t, std::mutex> pendingBGFetches;

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

    /// counter of items tagged with CanDeduplicate::No flushed
    cb::RelaxedAtomic<size_t> historicalItemsFlushed;

    struct BfilterData {
        // Bloom Filter structures
        std::unique_ptr<BloomFilter> bFilter;
        std::unique_ptr<BloomFilter> tempFilter; // Used during compaction.
        /**
         * Cache the bfilter_enabled config set on the Engine configuration.
         * Gets updated via setFilterStatus().
         */
        bool kvStoreBfilterEnabled{false};
    };

    folly::Synchronized<BfilterData, std::mutex> bFilterData;

    cb::time::steady_clock::time_point flushFailedLogTime;

    friend class EPVBucketTest;
};

using EPVBucketPtr = std::shared_ptr<EPVBucket>;