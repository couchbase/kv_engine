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
#include "seqlist.h"
#include "stats.h"
#include "vbucket.h"

#include <boost/optional/optional.hpp>

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
            SyncWriteCompleteCallback syncWriteCb,
            Configuration& config,
            item_eviction_policy_t evictionPolicy,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            bool mightContainXattrs = false,
            const Collections::VB::PersistedManifest& collectionsManifest = {});

    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const std::chrono::steady_clock::time_point startTime) override;

    void resetStats() override;

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

    size_t getNumNonResidentItems() const override {
        return 0;
    }

    size_t getNumSystemItems() const override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                const void* cookie,
                                EventuallyPersistentEngine& engine) override {
        return ENGINE_ENOTSUP;
    }

    void completeStatsVKey(const DocKey& key, const GetValue& gcb) override;

    bool maybeKeyExistsInFilter(const DocKey& key) override {
        /* There is no disk to indicate that a key may exist */
        return false;
    }

    cb::mcbp::Status evictKey(
            const DocKey& key,
            const char** msg,
            const Collections::VB::Manifest::CachingReadHandle&) override {
        /* There is nothing (no disk) to evictKey to. Later on if we decide to
           use this as a deletion, then we can handle it differently */
        return cb::mcbp::Status::NotSupported;
    }

    bool pageOut(const HashTable::HashBucketLock& lh, StoredValue*& v) override;

    bool eligibleToPageOut(const HashTable::HashBucketLock& lh,
                           const StoredValue& v) const override;

    bool areDeletedItemsAlwaysResident() const override;

    void addStats(bool details, ADD_STAT add_stat, const void* c) override;

    KVShard* getShard() override {
        return nullptr;
    }

    std::unique_ptr<DCPBackfill> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            uint64_t startSeqno,
            uint64_t endSeqno) override;

    /**
     * Reads backfill items from in memory ordered data structure.
     *
     * Because the backfill may have to be extended to ensure consistency (e.g.,
     * an item in the range has been updated and the new version is
     * outside of the original range would result in a missing item), the
     * end of the range may be at a higher seqno than was requested; this new
     * end value is returned.
     *
     * @param startSeqno requested start sequence number of the backfill
     * @param endSeqno requested end sequence number of the backfill
     *
     * @return ENGINE_SUCCESS, items in the snapshot, adjusted endSeqno
     *         ENGINE_ENOMEM on no memory to copy items
     *         ENGINE_ERANGE on incorrect start and end
     */
    std::tuple<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>, seqno_t>
    inMemoryBackfill(uint64_t start, uint64_t end);

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
    boost::optional<SequenceList::RangeIterator> makeRangeIterator(
            bool isBackfill);

    void dump() const override;

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

    void incrNumTotalItems() override {
        throw std::logic_error(
                "EphemeralVBucket::incrNumTotalItems not supported");
    }

    void decrNumTotalItems() override {
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

    void queueBackfillItem(queued_item& qi,
                           const GenerateBySeqno generateBySeqno) override;

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
    size_t purgeStaleItems(std::function<bool()> shouldPauseCbk = []() {
        return false;
    });

    void setupDeferredDeletion(const void* cookie) override;

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

    /**
     * Notify that the collection has been fully deleted and it's metadata can
     * now be fully purged. This results in the in-memory manifest
     * being updated to remove all knowledge of the collection.
     *
     * @param identifier ID of the collection that has completed deleting
     * @param eraserContext The context used by the eraser processing
     */
    void completeDeletion(
            CollectionID identifier,
            Collections::VB::EraserContext& eraserContext) override;

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
     */
    int64_t addSystemEventItem(Item* item, OptionalSeqno seqno) override;

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

    std::tuple<StoredValue*, VBNotifyCtx> softDeleteStoredValue(
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

    GetValue getInternalNonResident(const DocKey& key,
                                    const void* cookie,
                                    EventuallyPersistentEngine& engine,
                                    QueueBgFetch queueBgFetch,
                                    const StoredValue& v) override;

    size_t estimateNewMemoryUsage(EPStats& st, const Item& item) override {
        return st.getEstimatedTotalMemoryUsed() +
               OrderedStoredValue::getRequiredStorage(item.getKey());
    }

    /**
     * (i) Updates an already non-temp element in the sequence list (OR)
     * (ii) For a temp item that is being updated (that is, being made non-temp
     *      by an update), appends it to the sequence list
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

    /**
     * Enum indicating if the backfill is memory managed or not
     */
    enum class BackfillType : uint8_t { None, Buffered };
    BackfillType backfillType;
};

using EphemeralVBucketPtr = std::shared_ptr<EphemeralVBucket>;
