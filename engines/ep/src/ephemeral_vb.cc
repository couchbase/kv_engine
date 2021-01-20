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

#include "ephemeral_vb.h"

#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "configuration.h"
#include "dcp/backfill_memory.h"
#include "durability/passive_durability_monitor.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_tombstone_purger.h"
#include "executorpool.h"
#include "failover-table.h"
#include "item.h"
#include "linked_list.h"
#include "stored_value_factories.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucketdeletiontask.h"
#include <folly/lang/Assume.h>

EphemeralVBucket::EphemeralVBucket(
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
        SeqnoAckCallback seqnoAckCb,
        Configuration& config,
        EvictionPolicy evictionPolicy,
        std::unique_ptr<Collections::VB::Manifest> manifest,
        vbucket_state_t initState,
        uint64_t purgeSeqno,
        uint64_t maxCas,
        bool mightContainXattrs,
        const nlohmann::json* replicationTopology)
    : VBucket(i,
              newState,
              st,
              chkConfig,
              lastSeqno,
              lastSnapStart,
              lastSnapEnd,
              std::move(table),
              /*flusherCb*/ nullptr,
              std::make_unique<OrderedStoredValueFactory>(st),
              std::move(newSeqnoCb),
              syncWriteResolvedCb,
              syncWriteCb,
              seqnoAckCb,
              config,
              evictionPolicy,
              std::move(manifest),
              initState,
              purgeSeqno,
              maxCas,
              0, // Every item in ephemeral has a HLC cas
              mightContainXattrs,
              replicationTopology),
      seqList(std::make_unique<BasicLinkedList>(i, st)) {
}

size_t EphemeralVBucket::getNumItems() const {
    return ht.getNumInMemoryItems() -
           (ht.getNumDeletedItems() + ht.getNumSystemItems() +
            ht.getNumPreparedSyncWrites());
}

size_t EphemeralVBucket::getNumSystemItems() const {
    return ht.getNumSystemItems();
}

void EphemeralVBucket::completeStatsVKey(const DocKey& key,
                                         const GetValue& gcb) {
    throw std::logic_error(
            "EphemeralVBucket::completeStatsVKey() is not valid call. "
            "Called on " +
            getId().to_string() + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

bool EphemeralVBucket::pageOut(const Collections::VB::ReadHandle& readHandle,
                               const HashTable::HashBucketLock& lh,
                               StoredValue*& v) {
    auto cid = v->getKey().getCollectionID();
    if (!eligibleToPageOut(lh, *v) || !readHandle.exists(cid)) {
        return false;
    }
    VBQueueItemCtx queueCtx;
    v->setRevSeqno(v->getRevSeqno() + 1);
    DeletionStatus status;
    VBNotifyCtx notifyCtx;
    // After soft delete it is no longer safe to manipulate v the original
    // StoredValue v.
    // If v was made stale (because a range lock covers it) it may be
    // deleted at any time. Only the new SV ptr which has been returned should
    // be handled now.
    std::tie(v, status, notifyCtx) = softDeleteStoredValue(
            lh, *v, /*onlyMarkDeleted*/ false, queueCtx, 0);

    switch (status) {
    case DeletionStatus::Success:
        ht.updateMaxDeletedRevSeqno(v->getRevSeqno());
        notifyNewSeqno(notifyCtx);
        doCollectionsStats(readHandle, cid, notifyCtx);
        autoDeleteCount++;
        return true;

    case DeletionStatus::IsPendingSyncWrite:
        return false;
    }
    folly::assume_unreachable();
}

bool EphemeralVBucket::eligibleToPageOut(const HashTable::HashBucketLock& lh,
                                         const StoredValue& v) const {
    // We only delete from active vBuckets to ensure that replicas stay in
    // sync with the active (the delete from active is sent via DCP to the
    // the replicas as an explicit delete).
    if (getState() != vbucket_state_active) {
        return false;
    }
    if (v.isDeleted() && !v.getValue()) {
        // If the item has already been deleted (and doesn't have a value
        // associated with it) then there's no further deletion possible,
        // until the deletion marker (tombstone) is later purged at the
        // metadata purge internal.
        return false;
    }

    if (v.getKey().isInSystemCollection()) {
        // The system event documents must not be paged-out
        return false;
    }
    return true;
}

size_t EphemeralVBucket::getPageableMemUsage() {
    if (getState() == vbucket_state_replica) {
        // Ephemeral buckets are not backed by disk - nothing can be evicted
        // from a replica as deleting from replicas would cause inconsistency
        // with the active. When the active vb evicts items deletions will be
        // streamed to the replica.
        return 0;
    }
    return ht.getItemMemory();
}

bool EphemeralVBucket::areDeletedItemsAlwaysResident() const {
    // Ephemeral buckets do keep all deleted items resident in memory.
    // (We have nowhere else to store them, given there is no disk).
    return true;
}

void EphemeralVBucket::addStats(VBucketStatsDetailLevel detail,
                                const AddStatFn& add_stat,
                                const void* c) {
    // Include base class statistics:
    _addStats(detail, add_stat, c);

    if (detail == VBucketStatsDetailLevel::Full) {
        // Ephemeral-specific details
        addStat("auto_delete_count", autoDeleteCount.load(), add_stat, c);
        addStat("ht_tombstone_purged_count",
                htDeletedPurgeCount.load(),
                add_stat,
                c);
        addStat("seqlist_count", seqList->getNumItems(), add_stat, c);
        addStat("seqlist_deleted_count",
                seqList->getNumDeletedItems(),
                add_stat,
                c);
        addStat("seqlist_high_seqno", seqList->getHighSeqno(), add_stat, c);
        addStat("seqlist_highest_deduped_seqno",
                seqList->getHighestDedupedSeqno(),
                add_stat,
                c);
        addStat("seqlist_purged_count", seqListPurgeCount.load(), add_stat, c);

        uint64_t rr_begin, rr_end;
        std::tie(rr_begin, rr_end) = seqList->getRangeRead();

        addStat("seqlist_range_read_begin", rr_begin, add_stat, c);
        addStat("seqlist_range_read_end", rr_end, add_stat, c);
        addStat("seqlist_range_read_count", rr_end - rr_begin, add_stat, c);
        addStat("seqlist_stale_count",
                seqList->getNumStaleItems(),
                add_stat,
                c);
        addStat("seqlist_stale_value_bytes",
                seqList->getStaleValueBytes(),
                add_stat,
                c);
        addStat("seqlist_stale_metadata_bytes",
                seqList->getStaleValueBytes(),
                add_stat,
                c);
    }
}

void EphemeralVBucket::dump() const {
    std::cerr << "EphemeralVBucket[" << this
              << "] with state:" << toString(getState())
              << " numItems:" << getNumItems()
              << std::endl
              << "  ";
    seqList->dump();
    std::cerr << "  " << ht << std::endl;
}

ENGINE_ERROR_CODE EphemeralVBucket::completeBGFetchForSingleItem(
        const DiskDocKey& key,
        const FrontEndBGFetchItem& fetched_item,
        const std::chrono::steady_clock::time_point startTime) {
    /* [EPHE TODO]: Just return error code and make all the callers handle it */
    throw std::logic_error(
            "EphemeralVBucket::completeBGFetchForSingleItem() "
            "is not valid. Called on " +
            getId().to_string() + "for key: " + key.to_string());
}

void EphemeralVBucket::resetStats() {
    autoDeleteCount.reset();
}

vb_bgfetch_queue_t EphemeralVBucket::getBGFetchItems() {
    throw std::logic_error(
            "EphemeralVBucket::getBGFetchItems() is not valid. "
            "Called on " +
            getId().to_string());
}

bool EphemeralVBucket::hasPendingBGFetchItems() {
    throw std::logic_error(
            "EphemeralVBucket::hasPendingBGFetchItems() is not valid. "
            "Called on " +
            getId().to_string());
}

HighPriorityVBReqStatus EphemeralVBucket::checkAddHighPriorityVBEntry(
        uint64_t seqnoOrChkId,
        const void* cookie,
        HighPriorityVBNotify reqType) {
    if (reqType == HighPriorityVBNotify::ChkPersistence) {
        return HighPriorityVBReqStatus::NotSupported;
    }

    {
        /* Serialize the request with sequence lock */
        std::lock_guard<std::mutex> lh(sequenceLock);

        if (seqnoOrChkId <= getPersistenceSeqno()) {
            /* Need not notify asynchronously as the vb already has the
               requested seqno */
            return HighPriorityVBReqStatus::RequestNotScheduled;
        }

        addHighPriorityVBEntry(seqnoOrChkId, cookie, reqType);
    }

    return HighPriorityVBReqStatus::RequestScheduled;
}

void EphemeralVBucket::notifyHighPriorityRequests(
        EventuallyPersistentEngine& engine,
        uint64_t idNum,
        HighPriorityVBNotify notifyType) {
    throw std::logic_error(
            "EphemeralVBucket::notifyHighPriorityRequests() is not valid. "
            "Called on " +
            getId().to_string());
}

void EphemeralVBucket::notifyAllPendingConnsFailed(
        EventuallyPersistentEngine& e) {
    auto toNotify = tmpFailAndGetAllHpNotifies(e);

    for (auto& notify : toNotify) {
        e.notifyIOComplete(notify.first, notify.second);
    }

    fireAllOps(e);
}

std::unique_ptr<DCPBackfillIface> EphemeralVBucket::createDCPBackfill(
        EventuallyPersistentEngine& e,
        std::shared_ptr<ActiveStream> stream,
        uint64_t startSeqno,
        uint64_t endSeqno) {
    /* create a memory backfill object */
    EphemeralVBucketPtr evb =
            std::static_pointer_cast<EphemeralVBucket>(shared_from_this());
    return std::make_unique<DCPBackfillMemoryBuffered>(
            evb, stream, startSeqno, endSeqno);
}

std::unique_ptr<DCPBackfillIface> EphemeralVBucket::createDCPBackfill(
        EventuallyPersistentEngine& e,
        std::shared_ptr<ActiveStream> stream,
        CollectionID cid) {
    (void)cid;
    throw std::runtime_error(
            "Unexpected call to EphemeralVBucket::createDCPBackfill (id "
            "variant)");
}

std::tuple<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>, seqno_t>
EphemeralVBucket::inMemoryBackfill(uint64_t start, uint64_t end) {
    return seqList->rangeRead(start, end);
}

std::optional<SequenceList::RangeIterator> EphemeralVBucket::makeRangeIterator(
        bool isBackfill) {
    return seqList->makeRangeIterator(isBackfill);
}

bool EphemeralVBucket::isKeyLogicallyDeleted(const DocKey& key,
                                             int64_t bySeqno) {
    if (key.isInSystemCollection()) {
        return false;
    }
    auto cHandle = lockCollections(key);
    if (!cHandle.valid() || cHandle.isLogicallyDeleted(bySeqno)) {
        dropKey(bySeqno, cHandle);
        return true;
    }
    return false;
}

uint64_t EphemeralVBucket::getMaxVisibleSeqno() const {
    return seqList->getMaxVisibleSeqno();
}

void EphemeralVBucket::updateStatsForStateChange(vbucket_state_t from,
                                                 vbucket_state_t to) {
    checkpointManager->updateStatsForStateChange(from, to);
    if (from == vbucket_state_replica && to != vbucket_state_replica) {
        // vbucket is changing state away from replica, it's memory usage
        // should no longer be accounted for as a replica.
        stats.replicaHTMemory -= ht.getCacheSize();
    } else if (from != vbucket_state_replica && to == vbucket_state_replica) {
        // vbucket is changing state to _become_ a replica, it's memory usage
        // _should_ be accounted for as a replica.
        stats.replicaHTMemory += ht.getCacheSize();
    }
}

size_t EphemeralVBucket::purgeStaleItems(std::function<bool()> shouldPauseCbk) {
    // Iterate over the sequence list and delete any stale items. But we do
    // not want to delete the last element in the vbucket, hence we pass
    // 'seqList->getHighSeqno() - 1'.
    // Note: Even though highSeqno is present in the list we pass it as a param
    //       because we want the list purgeTombstones() to be generic, that is,
    //       should be aware of the constraint that last element should not be
    //       deleted
    if (seqList->getHighSeqno() < 2) {
        /* not enough items to purge */
        return 0;
    }

    // Callback applied to non-stale items at SeqList::purgeTombstones, pending
    // Prepares included.
    auto droppedCollectionCallback = [this](const DocKey& key,
                                            int64_t bySeqno) {
        return isKeyLogicallyDeleted(key, bySeqno);
    };

    auto seqListPurged = seqList->purgeTombstones(
            static_cast<seqno_t>(seqList->getHighSeqno()) - 1,
            droppedCollectionCallback,
            shouldPauseCbk);

    // Update stats and return.
    seqListPurgeCount += seqListPurged;
    setPurgeSeqno(seqList->getHighestPurgedDeletedSeqno());

    return seqListPurged;
}

std::tuple<StoredValue*, MutationStatus, VBNotifyCtx>
EphemeralVBucket::updateStoredValue(const HashTable::HashBucketLock& hbl,
                                    StoredValue& v,
                                    const Item& itm,
                                    const VBQueueItemCtx& queueItmCtx,
                                    bool justTouch) {
    VBNotifyCtx notifyCtx;
    StoredValue* newSv = nullptr;
    MutationStatus status(MutationStatus::WasClean);

    std::lock_guard<std::mutex> lh(sequenceLock);

    const bool wasTemp = v.isTempItem();
    const bool oldValueDeleted = v.isDeleted();
    const bool recreatingDeletedItem = v.isDeleted() && !itm.isDeleted();

    StoredValue::UniquePtr ownedSv;

    {
        // Once we update the seqList, there is a short period where the
        // highSeqno and highestDedupedSeqno are both incorrect. We have to hold
        // this lock to prevent a new rangeRead starting, and covering an
        // inconsistent range.
        std::lock_guard<std::mutex> listWriteLg(seqList->getListWriteLock());

        /* Update in the Ordered data structure (seqList) first and then update
           in the hash table */
        SequenceList::UpdateStatus res =
                modifySeqList(lh, listWriteLg, *(v.toOrderedStoredValue()));

        switch (res) {
        case SequenceList::UpdateStatus::Success: {
            /* OrderedStoredValue moved to end of the list, just update its
               value */
            auto result = ht.unlocked_updateStoredValue(hbl, v, itm);
            status = result.status;
            newSv = result.storedValue;

            seqList->updateNumDeletedItems(oldValueDeleted, itm.isDeleted());

        } break;

        case SequenceList::UpdateStatus::Append: {
            /* OrderedStoredValue cannot be moved to end of the list,
               due to a range read. Hence, release the storedvalue from the
               hash table, indicate the list to mark the OrderedStoredValue
               stale (old duplicate) and add a new StoredValue for the itm.

               Note: It is important to remove item from hash table before
                     marking stale because once marked stale list assumes the
                     ownership of the item and may delete it anytime. */
            /* Release current storedValue from hash table */
            ownedSv = ht.unlocked_release(hbl, &v);

            /* Add a new storedvalue for the item */
            newSv = ht.unlocked_addNewStoredValue(hbl, itm);

            seqList->appendToList(
                    lh, listWriteLg, *(newSv->toOrderedStoredValue()));

            // We couldn't overwrite any existing OVS due to a range-read, we
            // just update with the same logic of an "add new stored value"
            seqList->updateNumDeletedItems(false, itm.isDeleted());

        } break;
        }

        /* Put on checkpoint mgr */
        notifyCtx = queueDirty(hbl, *newSv, queueItmCtx);

        /* Update the high seqno in the sequential storage */
        auto& osv = *(newSv->toOrderedStoredValue());
        seqList->updateHighSeqno(listWriteLg, osv);

        seqList->maybeUpdateMaxVisibleSeqno(lh, listWriteLg, osv);

        /* Temp items are never added to the seqList, hence updating a temp
           item should not update the deduped seqno */
        if (!wasTemp) {
            seqList->updateHighestDedupedSeqno(listWriteLg, osv);
        }

        if (res == SequenceList::UpdateStatus::Append) {
            /* Mark the un-updated storedValue as stale. This must be done after
               the new storedvalue for the item is visible for range read in the
               list. This is because we do not want the seqlist to delete the
               stale item before its latest copy is added to the list.
               (item becomes visible for range read only after updating the list
                with the seqno of the item) */
            seqList->markItemStale(listWriteLg, std::move(ownedSv), newSv);
        }
    }

    if (itm.isCommitted()) {
        if (recreatingDeletedItem) {
            ++opsCreate;
            notifyCtx.itemCountDifference = 1;
        } else if (!oldValueDeleted && itm.isDeleted()) {
            ++opsDelete;
            notifyCtx.itemCountDifference = -1;
        } else {
            ++opsUpdate;
        }
    }

    return std::make_tuple(newSv, status, notifyCtx);
}

std::pair<StoredValue*, VBNotifyCtx> EphemeralVBucket::addNewStoredValue(
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx& queueItmCtx,
        GenerateRevSeqno genRevSeqno) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    if (genRevSeqno == GenerateRevSeqno::Yes) {
        /* This item could potentially be recreated */
        updateRevSeqNoOfNewStoredValue(*v);
    }

    std::lock_guard<std::mutex> lh(sequenceLock);

    OrderedStoredValue* osv;
    try {
        osv = v->toOrderedStoredValue();
    } catch (const std::bad_cast& e) {
        throw std::logic_error(
                "EphemeralVBucket::addNewStoredValue(): Error " +
                std::string(e.what()) + " for " + getId().to_string() +
                " for key: " +
                std::string(reinterpret_cast<const char*>(v->getKey().data()),
                            v->getKey().size()));
    }

    VBNotifyCtx notifyCtx;
    {
        std::lock_guard<std::mutex> listWriteLg(seqList->getListWriteLock());

        /* Add to the sequential storage */
        seqList->appendToList(lh, listWriteLg, *osv);

        /* Put on checkpoint mgr */
        notifyCtx = queueDirty(hbl, *v, queueItmCtx);

        /* Update the high seqno in the sequential storage */
        seqList->updateHighSeqno(listWriteLg, *osv);

        seqList->maybeUpdateMaxVisibleSeqno(lh, listWriteLg, *osv);
    }
    if (itm.isCommitted()) {
        if (!itm.isDeleted()) {
            ++opsCreate;
            notifyCtx.itemCountDifference = 1;
        }
    }

    seqList->updateNumDeletedItems(false, itm.isDeleted());

    return {v, notifyCtx};
}

std::tuple<StoredValue*, DeletionStatus, VBNotifyCtx>
EphemeralVBucket::softDeleteStoredValue(const HashTable::HashBucketLock& hbl,
                                        StoredValue& v,
                                        bool onlyMarkDeleted,
                                        const VBQueueItemCtx& queueItmCtx,
                                        uint64_t bySeqno,
                                        DeleteSource deleteSource) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    StoredValue* newSv = &v;
    StoredValue::UniquePtr ownedSv;

    const bool wasCommittedNonTemp = v.isCommitted() && !v.isTempItem();
    const bool oldValueDeleted = v.isDeleted();

    VBNotifyCtx notifyCtx;
    {
        // Once we update the seqList, there is a short period where the
        // highSeqno and highestDedupedSeqno are both incorrect. We have to hold
        // this lock to prevent a new rangeRead starting, and covering an
        // inconsistent range.
        std::lock_guard<std::mutex> listWriteLg(seqList->getListWriteLock());

        /* Update the in the Ordered data structure (seqList) first and then
           update in the hash table */
        SequenceList::UpdateStatus res =
                modifySeqList(lh, listWriteLg, *(v.toOrderedStoredValue()));

        switch (res) {
        case SequenceList::UpdateStatus::Success:
            /* OrderedStoredValue is moved to end of the list, do nothing */
            break;

        case SequenceList::UpdateStatus::Append: {
            /* OrderedStoredValue cannot be moved to end of the list,
               due to a range read. Hence, replace the storedvalue in the
               hash table with its copy and indicate the list to mark the
               OrderedStoredValue stale (old duplicate).

               Note: It is important to remove item from hash table before
                     marking stale because once marked stale list assumes the
                     ownership of the item and may delete it anytime. */

            /* Replace the current storedValue in the hash table with its
               copy */
            std::tie(newSv, ownedSv) = ht.unlocked_replaceByCopy(hbl, v);

            seqList->appendToList(
                    lh, listWriteLg, *(newSv->toOrderedStoredValue()));
        } break;
        }

        /* Delete the storedvalue */
        ht.unlocked_softDelete(hbl, *newSv, onlyMarkDeleted, deleteSource);

        if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
            newSv->setBySeqno(bySeqno);
        }

        // Replica/DelWithMeta can dictate the tombstone time, check for it.
        if (queueItmCtx.generateDeleteTime == GenerateDeleteTime::No &&
            newSv->isDeleted() && newSv->getExptime()) {
            // Both the deleted time and the replicated tombstone time are
            // absolute (unix epoch) - just copy across.
            newSv->toOrderedStoredValue()->setCompletedOrDeletedTime(
                    newSv->getExptime());
        }

        notifyCtx = queueDirty(hbl, *newSv, queueItmCtx);
        if (wasCommittedNonTemp && !oldValueDeleted) {
            ++opsDelete;
            notifyCtx.itemCountDifference = -1;
        }

        /* Update the high seqno in the sequential storage */
        auto& osv = *(newSv->toOrderedStoredValue());
        seqList->updateHighSeqno(listWriteLg, osv);

        seqList->maybeUpdateMaxVisibleSeqno(lh, listWriteLg, osv);

        /* Temp items are never added to the seqList, hence updating a temp
           item should not update the deduped seqno */
        if (wasCommittedNonTemp) {
            seqList->updateHighestDedupedSeqno(listWriteLg, osv);
        }

        if (res == SequenceList::UpdateStatus::Append) {
            /* Mark the un-updated storedValue as stale. This must be done after
               the new storedvalue for the item is visible for range read in the
               list. This is because we do not want the seqlist to delete the
               stale item before its latest copy is added to the list.
               (item becomes visible for range read only after updating the list
               with the seqno of the item) */
            seqList->markItemStale(listWriteLg, std::move(ownedSv), newSv);
        }
    }

    seqList->updateNumDeletedItems(oldValueDeleted, true);

    return std::make_tuple(newSv, DeletionStatus::Success, notifyCtx);
}

VBNotifyCtx EphemeralVBucket::commitStoredValue(
        HashTable::FindUpdateResult& values,
        uint64_t prepareSeqno,
        const VBQueueItemCtx& queueItmCtx,
        std::optional<int64_t> commitSeqno) {
    if (!values.pending) {
        throw std::invalid_argument(
                "EphemeralVBucket::commitStoredValue: Cannot call on a "
                "non-Pending StoredValue");
    }

    // Committing an item in an Ephemeral VBucket consists of:
    // 1a. If an existing Committed exists, update that with the data from
    //     the Prepare.
    // 1b. If no existing Committed OSV, then create a new Committed and
    //     populate
    //     with data from the Prepare.
    // 2. Mark the prepare as completed

    // Look for an existing Committed OSV under this key.
    StoredValue* newCommitted;
    VBNotifyCtx notifyCtx;
    // Need an Item to hold the state to apply to the existing / new Committed
    // OSV, so create one from the preparedSV.
    // PERF: Would be faster if we didn't create a temporary Item just to
    //       to the Committed OSV. Possible alternatives:
    //       a) use the existing Prepared SV, or
    //       b) pass down the Item which DurabilityMonitor already has - but
    //          this complicates the VBucket API just for the Ephemeral case.

    // While Item construction requires valid durability requirements for a
    // pending_sync_write, the next line below we change the Item to Committed
    // so just provide a dummy requirements.
    auto dummyReqs =
            cb::durability::Requirements(cb::durability::Level::Majority, {});
    auto item = values.pending->toItem(getId(),
                                       StoredValue::HideLockedCas::No,
                                       StoredValue::IncludeValue::Yes,
                                       dummyReqs);
    item->setCommittedviaPrepareSyncWrite();
    if (commitSeqno) {
        item->setBySeqno(*commitSeqno);
    }

    if (values.committed) {
        std::tie(newCommitted, std::ignore, notifyCtx) = updateStoredValue(
                values.pending.getHBL(), *values.committed, *item, queueItmCtx);
        Expects(newCommitted);
    } else {
        std::tie(newCommitted, notifyCtx) =
                addNewStoredValue(values.pending.getHBL(),
                                  *item,
                                  queueItmCtx,
                                  GenerateRevSeqno::No);
    }

    // Manually set the prepareSeqno on the OSV so that it is stored in the
    // seqList. This is required to provide the correct prepare seqno when
    // backfilling from the seqList. The consumer always expects a non-zero
    // prepare seqno to perform some additional error checking.
    auto& osv = *(newCommitted->toOrderedStoredValue());
    osv.setPrepareSeqno(prepareSeqno);

    values.pending.setCommitted(CommittedState::PrepareCommitted);
    return notifyCtx;
}

VBNotifyCtx EphemeralVBucket::abortStoredValue(
        const HashTable::HashBucketLock& hbl,
        StoredValue& prepared,
        int64_t prepareSeqno,
        std::optional<int64_t> abortSeqno) {
    // From MB-36650 this function is enabled to accept also Completed Prepares
    // (Committed/Aborted) for converting/updating the SV in input to
    // PrepareAborted.
    if (!(prepared.isPending() || prepared.isCompleted())) {
        throw std::invalid_argument(
                "EphemeralVBucket::abortStoredValue: Cannot call on a "
                "non-Pending and non-Completed StoredValue");
    }

    // Aborting an item in an Ephemeral VBucket consists of:
    // 1. Updating the OSV in the seqList
    // 2. Mark the prepare as aborted

    // This function is similar to softDeleteStoredValue, but implemented
    // separately as we would have to special case a few things for prepares
    // that would make the code less readable.

    VBQueueItemCtx queueItmCtx;
    VBNotifyCtx notifyCtx;

    // Need the sequenceLock as we may be generating a new seqno
    std::lock_guard<std::mutex> lh(sequenceLock);

    StoredValue* newSv = &prepared;
    StoredValue::UniquePtr oldSv;
    {
        // 1) Typical ephemeral seqList manipulation due to range reads. See
        // softDeleteStoredValue for more information.
        std::lock_guard<std::mutex> listWriteLg(seqList->getListWriteLock());
        SequenceList::UpdateStatus res = modifySeqList(
                lh, listWriteLg, *(prepared.toOrderedStoredValue()));
        switch (res) {
        case SequenceList::UpdateStatus::Success:
            break;
        case SequenceList::UpdateStatus::Append:
            std::tie(newSv, oldSv) = ht.unlocked_replaceByCopy(hbl, prepared);
            seqList->appendToList(
                    lh, listWriteLg, *(newSv->toOrderedStoredValue()));
            break;
        }

        // Set the specified seqno and queue using queueAbort. A normal deletion
        // would use queueDirty.
        if (abortSeqno) {
            queueItmCtx.genBySeqno = GenerateBySeqno::No;
            newSv->setBySeqno(*abortSeqno);
        }
        notifyCtx = queueAbort(hbl, *newSv, prepareSeqno, queueItmCtx);

        // A normal deletion does not need to manually set the bySeqno of the
        // abort item because queueDirty does this for you. As we are using
        // queueAbort, update the bySeqno manually.
        if (!abortSeqno) {
            newSv->setBySeqno(notifyCtx.bySeqno);
        }

        // 2) We need to modify the SV to mark it as an abort (not a delete)
        ht.unlocked_abortPrepare(hbl, *newSv);

        /* Update the high seqno in the sequential storage */
        auto& osv = *(newSv->toOrderedStoredValue());

        updateSeqListPostAbort(
                listWriteLg, oldSv ? oldSv->toOrderedStoredValue() : nullptr, osv, prepareSeqno);

        // If we did an append we still need to mark the un-updated StoredValue
        // as stale.
        if (res == SequenceList::UpdateStatus::Append) {
            seqList->markItemStale(listWriteLg, std::move(oldSv), newSv);
        }

        // We de-duped a prepare so we need to update the highest deduped seqno
        // to prevent a backfill range read from ending without reaching this
        // abort
        seqList->updateHighestDedupedSeqno(listWriteLg, osv);
    }

    return notifyCtx;
}

VBNotifyCtx EphemeralVBucket::addNewAbort(const HashTable::HashBucketLock& hbl,
                                          const DocKey& key,
                                          int64_t prepareSeqno,
                                          int64_t abortSeqno) {
    // Reached this method because an abort was received without previously
    // receiving a prepare.

    VBQueueItemCtx queueItmCtx;
    queueItmCtx.genBySeqno = GenerateBySeqno::No;

    // Need the sequenceLock as we may be generating a new seqno
    std::lock_guard<std::mutex> lh(sequenceLock);

    queued_item item = createNewAbortedItem(key, prepareSeqno, abortSeqno);
    StoredValue* newSv = ht.unlocked_addNewStoredValue(hbl, *item);

    auto& osv = *newSv->toOrderedStoredValue();

    VBNotifyCtx notifyCtx;
    {
        std::lock_guard<std::mutex> listWriteLg(seqList->getListWriteLock());

        seqList->appendToList(lh, listWriteLg, osv);

        notifyCtx = queueAbortForUnseenPrepare(item, queueItmCtx);

        updateSeqListPostAbort(listWriteLg, nullptr, osv, prepareSeqno);
    }

    return notifyCtx;
}

void EphemeralVBucket::updateSeqListPostAbort(
        std::lock_guard<std::mutex>& listWriteLg,
        const OrderedStoredValue* oldOsv,
        OrderedStoredValue& newOsv,
        int64_t prepareSeqno) {
    seqList->updateNumDeletedItems(oldOsv ? oldOsv->isDeleted() : false,
                                   newOsv.isDeleted());

    /* Update the high seqno in the sequential storage */
    seqList->updateHighSeqno(listWriteLg, newOsv);

    // Manually set the prepareSeqno on the OSV so that it is stored in the
    // seqList
    newOsv.setPrepareSeqno(prepareSeqno);
}

void EphemeralVBucket::bgFetch(const DocKey& key,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const bool isMeta) {
    throw std::logic_error(
            "EphemeralVBucket::bgFetch() is not valid. Called on " +
            getId().to_string() + " for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

ENGINE_ERROR_CODE
EphemeralVBucket::addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                                        const DocKey& key,
                                        const void* cookie,
                                        EventuallyPersistentEngine& engine,
                                        bool metadataOnly) {
    /* [EPHE TODO]: Just return error code and make all the callers handle it */
    throw std::logic_error(
            "EphemeralVBucket::addTempItemAndBGFetch() is not valid. "
            "Called on " +
            getId().to_string() + " for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

void EphemeralVBucket::bgFetchForCompactionExpiry(const DocKey& key,
                                                  const Item& item) {
    throw std::logic_error(
            "EphemeralVBucket::bgFetchForCompactionExpiry() is not valid. "
            "Called on " +
            getId().to_string() + " for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

GetValue EphemeralVBucket::getInternalNonResident(
        const DocKey& key,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        QueueBgFetch queueBgFetch,
        const StoredValue& v) {
    /* We reach here only if the v is deleted and does not have any value */
    return GetValue();
}

size_t EphemeralVBucket::estimateNewMemoryUsage(EPStats& st, const Item& item) {
    return st.getEstimatedTotalMemoryUsed() +
           OrderedStoredValue::getRequiredStorage(item.getKey());
}

void EphemeralVBucket::setupDeferredDeletion(const void* cookie) {
    setDeferredDeletionCookie(cookie);
    setDeferredDeletion(true);
}

void EphemeralVBucket::scheduleDeferredDeletion(
        EventuallyPersistentEngine& engine) {
    ExTask task = std::make_shared<VBucketMemoryDeletionTask>(engine, this);
    ExecutorPool::get()->schedule(task);
}

SequenceList::UpdateStatus EphemeralVBucket::modifySeqList(
        std::lock_guard<std::mutex>& seqLock,
        std::lock_guard<std::mutex>& writeLock,
        OrderedStoredValue& osv) {
    if (osv.isTempItem()) {
        /* If the StoredValue is temp, then it has not been added to the
           Ordered data structure (seqList) yet. Hence just append to the list.
           Also we are making the StoredValue 'non-temp' here, within the
           listWriteLg, by generating a sequence number */
        seqList->appendToList(seqLock, writeLock, osv);
        return SequenceList::UpdateStatus::Success;
    } else {
        /* Update the OrderedStoredValue in the Ordered data structure (list) */
        return seqList->updateListElem(seqLock, writeLock, osv);
    }
}

size_t EphemeralVBucket::getNumPersistedDeletes() const {
    /* the name is getNumPersistedDeletes, in ephemeral buckets the equivalent
       meaning is the number of deletes seen by the vbucket.
       This is needed by ns-server during vb-takeover */

    return getNumInMemoryDeletes();
}

void EphemeralVBucket::dropKey(int64_t bySeqno,
                               Collections::VB::CachingReadHandle& cHandle) {
    const auto& key = cHandle.getKey();

    // The system event doesn't get dropped here (tombstone purger will deal)
    if (key.isInSystemCollection()) {
        return;
    }

    auto res = ht.findForUpdate(key);
    auto releaseAndMarkStale = [this](const HashTable::HashBucketLock& hbl,
                                      StoredValue* sv) {
        auto ownedSV = ht.unlocked_release(hbl, sv);
        {
            std::lock_guard<std::mutex> listWriteLg(
                    seqList->getListWriteLock());
            // Mark the item stale, with no replacement item
            seqList->markItemStale(listWriteLg, std::move(ownedSV), nullptr);
        }
    };

    if (res.pending) {
        // We don't need to drop a complete prepare or an abort from the DM so
        // only call for in-flight prepares
        if (res.pending->isPending()) {
            dropPendingKey(key, res.pending->getBySeqno());
        }

        releaseAndMarkStale(res.getHBL(), res.pending.release());
    }
    if (res.committed) {
        releaseAndMarkStale(res.getHBL(), res.committed);
    }
    return;
}

uint64_t EphemeralVBucket::addSystemEventItem(
        std::unique_ptr<Item> item,
        OptionalSeqno seqno,
        std::optional<CollectionID> cid,
        const Collections::VB::WriteHandle& wHandle,
        std::function<void(uint64_t)> assignedSeqnoCallback) {
    if (assignedSeqnoCallback) {
        // MB-40216 assignedSeqnoCallback only needed by persistent buckets so
        // flag that this has been set but not used.
        throw std::logic_error(
                "EphemeralVBucket::addSystemEventItem: assignedSeqnoCallback "
                "is not valid for this vbucket type");
    }

    item->setVBucketId(getId());

    // ephemeral keeps system events in-memory, so keep it compressed.
    item->compressValue();

    auto htRes = ht.findForWrite(item->getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    VBQueueItemCtx queueItmCtx;
    queueItmCtx.genBySeqno = getGenerateBySeqno(seqno);
    if (v) {
        std::tie(v, std::ignore, std::ignore) =
                updateStoredValue(hbl, *v, *item, queueItmCtx);
    } else {
        std::tie(v, std::ignore) = addNewStoredValue(
                hbl, *item, queueItmCtx, GenerateRevSeqno::Yes);
    }

    // Set the system events delete time if needed for tombstoning
    if (v->isDeleted()) {
        // We can never purge the drop of the default collection because it has
        // an implied creation event. If we did allow the default collection
        // tombstone to be purged a client would wrongly assume it exists.
        // Set the delete time so that purging never occurs
        if (cid && cid.value().isDefaultCollection()) {
            v->setCompletedOrDeletedTime(-1);
        } else {
            v->setCompletedOrDeletedTime(ep_real_time());
        }
    }

    VBNotifyCtx notifyCtx;

    // If the seqno is initialized, skip replication notification
    notifyCtx.notifyReplication = !seqno.has_value();
    notifyCtx.bySeqno = v->getBySeqno();
    notifyNewSeqno(notifyCtx);

    // We don't record anything interesting for scopes
    if (cid) {
        VBucket::doCollectionsStats(wHandle, *cid, notifyCtx);
        if (item->isDeleted()) {
            stats.dropCollectionStats(*cid);

            // Inform the PDM about the dropped collection so that it knows
            // that it can skip any outstanding prepares until they are cleaned
            // up
            if (getState() != vbucket_state_active) {
                getPassiveDM().notifyDroppedCollection(*cid, notifyCtx.bySeqno);
            }
        } else {
            stats.trackCollectionStats(*cid);
        }
    }
    Expects(v->getBySeqno() >= 0);
    return uint64_t(v->getBySeqno());
}

bool EphemeralVBucket::isValidDurabilityLevel(cb::durability::Level level) {
    switch (level) {
    case cb::durability::Level::Majority:
        return true;
    case cb::durability::Level::None:
    case cb::durability::Level::MajorityAndPersistOnMaster:
    case cb::durability::Level::PersistToMajority:
        // No persistence on Ephemeral
        return false;
    }

    folly::assume_unreachable();
}

void EphemeralVBucket::processImplicitlyCompletedPrepare(
        HashTable::StoredValueProxy& v) {
    v.setCommitted(CommittedState::PrepareCommitted);
}

void EphemeralVBucket::doCollectionsStats(
        const Collections::VB::ReadHandle& readHandle,
        CollectionID collection,
        const VBNotifyCtx& notifyCtx) {
    readHandle.setHighSeqno(collection, notifyCtx.bySeqno);

    if (notifyCtx.itemCountDifference == 1) {
        readHandle.incrementItemCount(collection);
    } else if (notifyCtx.itemCountDifference == -1) {
        readHandle.decrementItemCount(collection);
    }
}
