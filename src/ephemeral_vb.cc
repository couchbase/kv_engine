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

#include "dcp/backfill_memory.h"
#include "ephemeral_tombstone_purger.h"
#include "failover-table.h"
#include "linked_list.h"
#include "stored_value_factories.h"

EphemeralVBucket::EphemeralVBucket(id_type i,
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
                                   vbucket_state_t initState,
                                   uint64_t purgeSeqno,
                                   uint64_t maxCas,
                                   const std::string& collectionsManifest)
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
              config,
              evictionPolicy,
              initState,
              purgeSeqno,
              maxCas,
              collectionsManifest),
      seqList(std::make_unique<BasicLinkedList>(i, st)) {
}

size_t EphemeralVBucket::getNumItems() const {
    return ht.getNumInMemoryItems() - ht.getNumDeletedItems();
}

void EphemeralVBucket::completeStatsVKey(
        const DocKey& key, const RememberingCallback<GetValue>& gcb) {
    throw std::logic_error(
            "EphemeralVBucket::completeStatsVKey() is not valid call. "
            "Called on vb " +
            std::to_string(getId()) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

bool EphemeralVBucket::pageOut(const HashTable::HashBucketLock& lh,
                               StoredValue*& v) {
    // We only delete from active vBuckets to ensure that replicas stay in
    // sync with the active (the delete from active is sent via DCP to the
    // the replicas as an explicit delete).
    if (getState() != vbucket_state_active) {
        return false;
    }
    if (v->isDeleted() && !v->getValue()) {
        // If the item has already been deleted (and doesn't have a value
        // associated with it) then there's no further deletion possible,
        // until the deletion marker (tombstone) is later purged at the
        // metadata purge internal.
        return false;
    }
    VBQueueItemCtx queueCtx(GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            TrackCasDrift::No,
                            /*isBackfill*/ false,
                            nullptr);
    v->setRevSeqno(v->getRevSeqno() + 1);
    StoredValue* newSv;
    VBNotifyCtx notifyCtx;
    std::tie(newSv, notifyCtx) = softDeleteStoredValue(
            lh, *v, /*onlyMarkDeleted*/ false, queueCtx, 0);
    ht.updateMaxDeletedRevSeqno(newSv->getRevSeqno());
    notifyNewSeqno(notifyCtx);

    autoDeleteCount++;

    return true;
}

void EphemeralVBucket::addStats(bool details,
                                ADD_STAT add_stat,
                                const void* c) {
    // Include base class statistics:
    _addStats(details, add_stat, c);

    if (details) {
        // Ephemeral-specific details
        addStat("auto_delete_count", autoDeleteCount.load(), add_stat, c);
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
        const auto rr_begin = seqList->getRangeReadBegin();
        const auto rr_end = seqList->getRangeReadEnd();
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
              << "] with state: " << toString(getState())
              << " numItems:" << getNumItems()
              << std::endl;
    seqList->dump();
}

ENGINE_ERROR_CODE EphemeralVBucket::completeBGFetchForSingleItem(
        const DocKey& key,
        const VBucketBGFetchItem& fetched_item,
        const ProcessClock::time_point startTime) {
    /* [EPHE TODO]: Just return error code and make all the callers handle it */
    throw std::logic_error(
            "EphemeralVBucket::completeBGFetchForSingleItem() "
            "is not valid. Called on vb " +
            std::to_string(getId()) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

void EphemeralVBucket::resetStats() {
    autoDeleteCount.reset();
}

vb_bgfetch_queue_t EphemeralVBucket::getBGFetchItems() {
    throw std::logic_error(
            "EphemeralVBucket::getBGFetchItems() is not valid. "
            "Called on vb " +
            std::to_string(getId()));
}

bool EphemeralVBucket::hasPendingBGFetchItems() {
    throw std::logic_error(
            "EphemeralVBucket::hasPendingBGFetchItems() is not valid. "
            "Called on vb " +
            std::to_string(getId()));
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
    auto toNotify = getHighPriorityNotifies(engine, idNum, notifyType);

    for (auto& notify : toNotify) {
        engine.notifyIOComplete(notify.first, notify.second);
    }
}

void EphemeralVBucket::notifyAllPendingConnsFailed(
        EventuallyPersistentEngine& e) {
    auto toNotify = tmpFailAndGetAllHpNotifies(e);

    for (auto& notify : toNotify) {
        e.notifyIOComplete(notify.first, notify.second);
    }

    fireAllOps(e);
}

std::unique_ptr<DCPBackfill> EphemeralVBucket::createDCPBackfill(
        EventuallyPersistentEngine& e,
        const active_stream_t& stream,
        uint64_t startSeqno,
        uint64_t endSeqno) {
    /* create a memory backfill object */
    return std::make_unique<DCPBackfillMemory>(
            this, stream, startSeqno, endSeqno);
}

std::pair<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>>
EphemeralVBucket::inMemoryBackfill(uint64_t start, uint64_t end) {
    return seqList->rangeRead(start, end);
}

/* Vb level backfill queue is for items in a huge snapshot (disk backfill
   snapshots from DCP are typically huge) that could not be fit on a
   checkpoint. They update all stats, checkpoint seqno, but are not put
   on checkpoint and are directly persisted from the queue.

   In ephemeral buckets we must not add backfill items from DCP (on
   replica vbuckets), to the vb backfill queue because we have put them on
   linkedlist already. Also we do not have the flusher task to drain the
   items from that queue.
   (Unlike checkpoints, the items in this queue is not cleaned up
    in a background cleanup task).

   But we must be careful to update certain stats and checkpoint seqno
   like in a regular couchbase bucket. */
void EphemeralVBucket::queueBackfillItem(
        queued_item& qi, const GenerateBySeqno generateBySeqno) {
    if (GenerateBySeqno::Yes == generateBySeqno) {
        qi->setBySeqno(checkpointManager.nextBySeqno());
    } else {
        checkpointManager.setBySeqno(qi->getBySeqno());
    }
    ++stats.totalEnqueued;
    stats.memOverhead->fetch_add(sizeof(queued_item));
}

size_t EphemeralVBucket::purgeTombstones(rel_time_t purgeAge) {
    // First mark all deleted items in the HashTable which can be purged as
    // Stale - this removes them from the HashTable, transferring ownership to
    // SequenceList.
    HTTombstonePurger purger(*this, purgeAge);
    ht.visit(purger);

    // Secondly iterate over the sequence list and delete any stale items
    auto seqListPurged = seqList->purgeTombstones();

    // Update stats and return.
    htDeletedPurgeCount += purger.getNumPurged();
    seqListPurgeCount += seqListPurged;
    setPurgeSeqno(seqList->getHighestPurgedDeletedSeqno());

    return seqListPurged;
}

std::tuple<StoredValue*, MutationStatus, VBNotifyCtx>
EphemeralVBucket::updateStoredValue(const HashTable::HashBucketLock& hbl,
                                    StoredValue& v,
                                    const Item& itm,
                                    const VBQueueItemCtx* queueItmCtx) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    const bool recreatingDeletedItem = v.isDeleted();

    /* Update the OrderedStoredValue in hash table + Ordered data structure
       (list) */
    auto res = seqList->updateListElem(lh, *(v.toOrderedStoredValue()));

    StoredValue* newSv = &v;
    StoredValue::UniquePtr ownedSv;
    MutationStatus status(MutationStatus::WasClean);

    switch (res) {
    case SequenceList::UpdateStatus::Success:
        /* OrderedStoredValue moved to end of the list, just update its
           value */
        status = ht.unlocked_updateStoredValue(hbl.getHTLock(), v, itm);
        break;

    case SequenceList::UpdateStatus::Append: {
        /* OrderedStoredValue cannot be moved to end of the list,
           due to a range read. Hence, release the storedvalue from the
           hash table, indicate the list to mark the OrderedStoredValue
           stale (old duplicate) and add a new StoredValue for the itm.

           Note: It is important to remove item from hash table before
                 marking stale because once marked stale list assumes the
                 ownership of the item and may delete it anytime. */
        /* Release current storedValue from hash table */
        /* [EPHE TODO]: Write a HT func to release the StoredValue directly
                        than taking key as a param and deleting
                        (MB-23184) */
        ownedSv = ht.unlocked_release(hbl, v.getKey());

        /* Add a new storedvalue for the item */
        newSv = ht.unlocked_addNewStoredValue(hbl, itm);

        seqList->appendToList(lh, *(newSv->toOrderedStoredValue()));
    } break;
    }

    VBNotifyCtx notifyCtx;
    if (queueItmCtx) {
        /* Put on checkpoint mgr */
        notifyCtx = queueDirty(*newSv, *queueItmCtx);
    }

    /* Update the high seqno in the sequential storage */
    seqList->updateHighSeqno(*(newSv->toOrderedStoredValue()));
    if (recreatingDeletedItem) {
        ++opsCreate;
    } else {
        ++opsUpdate;
    }

    if (res == SequenceList::UpdateStatus::Append) {
        /* Mark the un-updated storedValue as stale. This must be done after
           the new storedvalue for the item is visible for range read in the
           list. This is because we do not want the seqlist to delete the stale
           item before its latest copy is added to the list.
           (item becomes visible for range read only after updating the list
            with the seqno of the item) */
        seqList->markItemStale(std::move(ownedSv));
    }
    return std::make_tuple(newSv, status, notifyCtx);
}

std::pair<StoredValue*, VBNotifyCtx> EphemeralVBucket::addNewStoredValue(
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    std::lock_guard<std::mutex> lh(sequenceLock);

    /* Add to the sequential storage */
    try {
        seqList->appendToList(lh, *(v->toOrderedStoredValue()));
    } catch (const std::bad_cast& e) {
        throw std::logic_error(
                "EphemeralVBucket::addNewStoredValue(): Error " +
                std::string(e.what()) + " for vbucket: " +
                std::to_string(getId()) + " for key: " +
                std::string(reinterpret_cast<const char*>(v->getKey().data()),
                            v->getKey().size()));
    }

    VBNotifyCtx notifyCtx;
    if (queueItmCtx) {
        /* Put on checkpoint mgr */
        notifyCtx = queueDirty(*v, *queueItmCtx);
    }

    /* Update the high seqno in the sequential storage */
    seqList->updateHighSeqno(*(v->toOrderedStoredValue()));
    ++opsCreate;

    return {v, notifyCtx};
}

std::tuple<StoredValue*, VBNotifyCtx> EphemeralVBucket::softDeleteStoredValue(
        const HashTable::HashBucketLock& hbl,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    StoredValue* newSv = &v;
    StoredValue::UniquePtr ownedSv;

    /* Update the OrderedStoredValue in hash table + Ordered data structure
       (list) */
    auto res = seqList->updateListElem(lh, *(v.toOrderedStoredValue()));

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

        /* Release current storedValue from hash table */
        /* [EPHE TODO]: Write a HT func to replace the StoredValue directly
                        than taking key as a param and deleting (MB-23184) */
        std::tie(newSv, ownedSv) = ht.unlocked_replaceByCopy(hbl, v);

        seqList->appendToList(lh, *(newSv->toOrderedStoredValue()));
    } break;
    }

    /* Delete the storedvalue */
    ht.unlocked_softDelete(hbl.getHTLock(), *newSv, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        newSv->setBySeqno(bySeqno);
    }

    VBNotifyCtx notifyCtx = queueDirty(*newSv, queueItmCtx);

    /* Update the high seqno in the sequential storage */
    seqList->updateHighSeqno(*(newSv->toOrderedStoredValue()));
    ++opsDelete;

    if (res == SequenceList::UpdateStatus::Append) {
        /* Mark the un-updated storedValue as stale. This must be done after
           the new storedvalue for the item is visible for range read in the
           list. This is because we do not want the seqlist to delete the stale
           item before its latest copy is added to the list.
           (item becomes visible for range read only after updating the list
           with the seqno of the item) */
        seqList->markItemStale(std::move(ownedSv));
    }
    return std::make_tuple(newSv, notifyCtx);
}

void EphemeralVBucket::bgFetch(const DocKey& key,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const int bgFetchDelay,
                               const bool isMeta) {
    throw std::logic_error(
            "EphemeralVBucket::bgFetch() is not valid. Called on vb " +
            std::to_string(getId()) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

ENGINE_ERROR_CODE
EphemeralVBucket::addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                                        const DocKey& key,
                                        const void* cookie,
                                        EventuallyPersistentEngine& engine,
                                        int bgFetchDelay,
                                        bool metadataOnly,
                                        bool isReplication) {
    /* [EPHE TODO]: Just return error code and make all the callers handle it */
    throw std::logic_error(
            "EphemeralVBucket::addTempItemAndBGFetch() is not valid. "
            "Called on vb " +
            std::to_string(getId()) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

GetValue EphemeralVBucket::getInternalNonResident(
        const DocKey& key,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        int bgFetchDelay,
        get_options_t options,
        const StoredValue& v) {
    /* We reach here only if the v is deleted and does not have any value */
    return GetValue();
}
