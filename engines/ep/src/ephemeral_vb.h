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

#include "seqlist.h"
#include "stats.h"
#include "vbucket.h"

#include <optional>

class EphemeralVBucket : public VBucket {
public:
    class CountVisitor;
    class HTTombstonePurger;
    class StaleItemDeleter;

    EphemeralVBucket(
            Vbid i,
            vbucket_state_t newState,
            EPStats& st,
            CheckpointConfig& chkConfig,
            KVShard* kvshard,
            int64_t lastSeqno,
            uint64_t lastSnapStart,
            uint64_t lastSnapEnd,
            std::unique_ptr<FailoverTable> table,
            NewSeqnoCallback newSeqnoCb,
            SyncWriteResolvedCallback syncWriteResolvedCb,
            SyncWriteCompleteCallback syncWriteCb,
            SyncWriteTimeoutHandlerFactory syncWriteNextExpiryChangedFact,
            SeqnoAckCallback seqnoAckCb,
            Configuration& config,
            EvictionPolicy evictionPolicy,
            std::unique_ptr<Collections::VB::Manifest> manifest,
            KVBucket* bucket = nullptr,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            bool mightContainXattrs = false,
            const nlohmann::json* replicationTopology = {});

    cb::engine_errc completeBGFetchForSingleItem(
            const DiskDocKey& key,
            const FrontEndBGFetchItem& fetched_item,
            const std::chrono::steady_clock::time_point startTime) override;

    void resetStats() override;

    vb_bgfetch_queue_t getBGFetchItems() override;

    bool hasPendingBGFetchItems() override;

    HighPriorityVBReqStatus checkAddHighPriorityVBEntry(
            uint64_t seqno,
            const CookieIface* cookie,
            std::chrono::milliseconds timeout) override;

    void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) override;

    size_t getNumItems() const override;

    size_t getNumNonResidentItems() const override {
        return 0;
    }

    size_t getNumSystemItems() const override;

    cb::engine_errc statsVKey(const DocKey& key,
                              const CookieIface* cookie,
                              EventuallyPersistentEngine& engine) override {
        return cb::engine_errc::not_supported;
    }

    void completeStatsVKey(const DocKey& key, const GetValue& gcb) override;

    bool maybeKeyExistsInFilter(const DocKey& key) override {
        /* There is no disk to indicate that a key may exist */
        return false;
    }

    cb::mcbp::Status evictKey(
            const char** msg,
            const Collections::VB::CachingReadHandle& readHandle) override {
        /* There is nothing (no disk) to evictKey to. Later on if we decide to
           use this as a deletion, then we can handle it differently */
        return cb::mcbp::Status::NotSupported;
    }

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
        return nullptr;
    }

    Flusher* getFlusher() override {
        return nullptr;
    }

    std::unique_ptr<DCPBackfillIface> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            uint64_t startSeqno,
            uint64_t endSeqno) override;

    std::unique_ptr<DCPBackfillIface> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            CollectionID cid) override;

    /**
     * Creates a range iterator for the underlying SequenceList 'optionally'.
     * Under scenarios like where we want to limit the number of range iterators
     * the SequenceList, new range iterator will not be allowed
     *
     * @param isBackfill indicates if the iterator is for backfill (for debug)
     *
     * @return range iterator object when possible
     *         null when not possible
     */
    std::optional<SequenceList::RangeIterator> makeRangeIterator(
            bool isBackfill);

    void dump(std::ostream& ostream) const override;

    uint64_t getPersistenceSeqno() const override {
        /* Technically we do not have persistence in an ephemeral vb, however
         * logically the "persistence" seqno is used internally as the
         * value to use for replication / takeover. Hence we return
           the last seen seqno (highSeqno) as the persisted seqno. */
        return static_cast<uint64_t>(getHighSeqno());
    }

    uint64_t getPublicPersistenceSeqno() const override {
        return 0;
    }

    void updateStatsForStateChange(vbucket_state_t from,
                                   vbucket_state_t to) override;

    void incrNumTotalItems(size_t itemsDelta) override {
        throw std::logic_error(
                "EphemeralVBucket::incrNumTotalItems not supported");
    }

    void decrNumTotalItems(size_t itemsDelta) override {
        throw std::logic_error(
                "EphemeralVBucket::decrNumTotalItems not supported");
    }

    void setNumTotalItems(size_t items) override {
        throw std::logic_error(
                "EphemeralVBucket::setNumTotalItems not supported");
    }

    size_t getNumTotalItems() const override {
        throw std::logic_error(
                "EphemeralVBucket::getNumTotalItems not supported");
    }

    /** Purge any stale items in this VBucket's sequenceList.
     *
     * @param shouldPause Callback function that indicates if tombstone purging
     *                    should pause. This is called for every element in the
     *                    sequence list when we iterate over the list during the
     *                    purge. The caller should decide if the purge should
     *                    continue or if it should be paused (in case it is
     *                    running for a long time). By default, we assume that
     *                    the tombstone purging need not be paused at all
     *
     * @return Number of items purged.
     */
    size_t purgeStaleItems(
            std::function<bool()> shouldPauseCbk = []() { return false; });

    void setupDeferredDeletion(const CookieIface* cookie) override;

    /**
     * Schedule a VBucketMemoryDeletionTask to delete this object.
     * @param engine owning engine (required for task construction)
     */
    void scheduleDeferredDeletion(EventuallyPersistentEngine& engine) override;

    /**
     * in ephemeral buckets the equivalent meaning is the number of deletes seen
     * by the vbucket.
     * Note: This stat is needed by ns-server during vb-takeover
     * @return the number of deletes seen by this vbucket
     */
    size_t getNumPersistedDeletes() const override;

    void dropKey(const DocKey& key, int64_t bySeqno) override;

    /**
     * Add a system event Item to the vbucket and return its seqno.
     *
     * In ephemeral buckets the item is stored in the hashtable (and the seqno
     * linked-list)
     *
     * @param item an Item object to queue, can be any kind of item and will be
     *        given a CAS and seqno by this function.
     * @param seqno An optional sequence number, if not specified checkpoint
     *        queueing will assign a seqno to the Item.
     * @param cid The collection ID that this system event is concerned with.
     *        Optional as this may be a scope system event.
     * @param wHandle Collections write handle under which this operation is
     *        locked.
     * @param assignedSeqnoCallback not used by ephemeral vbucket
     */
    uint64_t addSystemEventItem(
            std::unique_ptr<Item> item,
            OptionalSeqno seqno,
            std::optional<CollectionID> cid,
            const Collections::VB::WriteHandle& wHandle,
            std::function<void(uint64_t)> assignedSeqnoCallback) override;

    /**
     * Check with the collections manifest if this key belongs to a dropped
     * collection (or is in a flushed range)
     * @param key The key to test
     * @param bySeqno The seqno of the key
     * @return true if the key is logically deleted
     */
    bool isKeyLogicallyDeleted(const DocKey& key, int64_t bySeqno) const;

    uint64_t getMaxVisibleSeqno() const;

    /**
     * @return a std::function with no target - ephemeral buckets don't need
     * this functionality
     */
    std::function<void(int64_t)> getSaveDroppedCollectionCallback(
            CollectionID cid,
            Collections::VB::WriteHandle& writeHandle,
            const Collections::VB::ManifestEntry& droppedEntry) const override {
        return {};
    }

    size_t getSeqListNumItems() const {
        return seqList->getNumItems();
    }

    size_t getSeqListNumDeletedItems() const {
        return seqList->getNumDeletedItems();
    }

    size_t getSeqListNumStaleItems() const {
        return seqList->getNumStaleItems();
    }

    cb::engine_errc createRangeScan(CollectionID,
                                    cb::rangescan::KeyView,
                                    cb::rangescan::KeyView,
                                    RangeScanDataHandlerIFace&,
                                    const CookieIface&,
                                    cb::rangescan::KeyOnly) override;
    cb::engine_errc continueRangeScan(cb::rangescan::Id,
                                      const CookieIface&,
                                      size_t) override;
    cb::engine_errc cancelRangeScan(cb::rangescan::Id, bool) override;

protected:
    /* Data structure for in-memory sequential storage */
    std::unique_ptr<SequenceList> seqList;

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
    /**
     * Update the tracked numDeletedItems and highSeqno after an prepare has
     * been aborted, and sets the prepareSeqno in the aborted stored value.
     *
     * @param seqLock The sequence lock the caller is expected to hold
     * @param listWriteLg Write lock of the sequenceList from getListWriteLock()
     * @param oldOsv the prepared stored value which has been aborted (or
     * nullptr)
     * @param newOsv the new aborted stored value which has been added to the
     * hashtable
     * @param prepareSeqno the seqno of the prepare which has been aborted
     */
    void updateSeqListPostAbort(std::lock_guard<std::mutex>& seqLock,
                                std::lock_guard<std::mutex>& listWriteLg,
                                const OrderedStoredValue* oldOsv,
                                OrderedStoredValue& newOsv,
                                int64_t prepareSeqno);

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

    GetValue getInternalNonResident(HashTable::HashBucketLock&& hbl,
                                    const DocKey& key,
                                    const CookieIface* cookie,
                                    EventuallyPersistentEngine& engine,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateNewMemoryUsage(EPStats& st, const Item& item) override;

    bool isValidDurabilityLevel(cb::durability::Level level) override;

    void processImplicitlyCompletedPrepare(
            HashTable::StoredValueProxy& htRes) override;

    /**
     * (i) Repositions an already non-temp element in the sequence list (OR)
     * (ii) For a temp item that is being updated (that is, being made non-temp
     *      by an update), appends it to the sequence list.
     *
     * @param seqLock A sequence lock the calling module is expected to hold.
     * @param writeLock Write lock of the sequenceList from getListWriteLock()
     * @param v Ref to orderedStoredValue which will placed into the linked list
     *
     * @return UpdateStatus::Success list element has been updated and moved to
     *                               end.
     *         UpdateStatus::Append list element is *not* updated. Caller must
     *                              handle the append.
     */
    SequenceList::UpdateStatus modifySeqList(
            std::lock_guard<std::mutex>& seqLock,
            std::lock_guard<std::mutex>& writeLock,
            OrderedStoredValue& osv);

    /**
     * Perform the post-queue collections stat counting using a read handle and
     * a given CollectionID.
     *
     * @param readHandle read handle for the entire collection manifest that
     *        allows us to lookup a collection then set the high seqno for it
     * @param collection the collection we need to update
     * @param notifyCtx holds info needed for stat counting
     */
    void doCollectionsStats(const Collections::VB::ReadHandle& readHandle,
                            CollectionID collection,
                            const VBNotifyCtx& notifyCtx);

    /**
     * Remove the given stored value from the hash-table
     */
    void dropStoredValue(const HashTable::HashBucketLock& hbl,
                         StoredValue& value);

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

    /**
     * Count of how many items have been deleted via the 'auto_delete' policy
     */
    EPStats::Counter autoDeleteCount;

    /**
     * Count of how many deleted items have been purged from the HashTable
     * (marked as stale and transferred from HT to sequence list).
     */
    EPStats::Counter htDeletedPurgeCount;

    /** Count of how many items have been purged from the sequence list
     *  (removed from seqList and deleted).
     */
    EPStats::Counter seqListPurgeCount;
};

using EphemeralVBucketPtr = std::shared_ptr<EphemeralVBucket>;
