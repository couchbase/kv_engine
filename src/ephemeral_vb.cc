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
      seqList(std::make_unique<BasicLinkedList>(i)) {
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

void EphemeralVBucket::addStats(bool details,
                                ADD_STAT add_stat,
                                const void* c) {
    _addStats(details, add_stat, c);
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

ENGINE_ERROR_CODE EphemeralVBucket::addHighPriorityVBEntry(uint64_t id,
                                                           const void* cookie,
                                                           bool isBySeqno) {
    return ENGINE_ENOTSUP;
}

void EphemeralVBucket::notifyOnPersistence(EventuallyPersistentEngine& e,
                                           uint64_t idNum,
                                           bool isBySeqno) {
    throw std::logic_error(
            "EphemeralVBucket::notifyOnPersistence() is not valid. "
            "Called on vb " +
            std::to_string(getId()));
}

void EphemeralVBucket::notifyAllPendingConnsFailed(
        EventuallyPersistentEngine& e) {
    fireAllOps(e);
}

size_t EphemeralVBucket::getHighPriorityChkSize() {
    return 0;
}

std::tuple<StoredValue*, MutationStatus, VBNotifyCtx>
EphemeralVBucket::updateStoredValue(const HashTable::HashBucketLock& hbl,
                                    StoredValue& v,
                                    const Item& itm,
                                    const VBQueueItemCtx* queueItmCtx) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    OrderedStoredValue* osv = v.toOrderedStoredValue();

    /* Update the OrderedStoredValue in hash table + Ordered data structure
       (list) */
    auto res = seqList->updateListElem(lh, *osv);

    StoredValue* newSv = &v;
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
                 ownership of the item and delete it anytime. */
        /* Release current storedValue from hash table */
        /* [EPHE TODO]: Write a HT func to release the StoredValue directly
                        than taking key as a param and deleting
                        (MB-23184) */
        ht.unlocked_release(hbl, v.getKey()).release();

        /* Add a new storedvalue for the item */
        newSv = ht.unlocked_addNewStoredValue(hbl, itm);
        status = MutationStatus::WasClean;

        OrderedStoredValue* newOsv = newSv->toOrderedStoredValue();
        seqList->appendToList(lh, *newOsv);
    } break;
    }

    VBNotifyCtx notifyCtx;
    if (queueItmCtx) {
        /* Put on checkpoint mgr */
        notifyCtx = queueDirty(*newSv, *queueItmCtx);
    }

    /* Update the high seqno in the sequential storage */
    seqList->updateHighSeqno(newSv->getBySeqno());

    if (res == SequenceList::UpdateStatus::Append) {
        /* Mark the un-updated storedValue as stale. This must be done after
           the new storedvalue for the item is visible for range read in the
           list. This is because we do not want the seqlist to delete the stale
           item before its latest copy is added to the list.
           (item becomes visible for range read only after updating the list
            with the seqno of the item) */
        seqList->markItemStale(*osv);
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
    seqList->updateHighSeqno(v->getBySeqno());

    return {v, notifyCtx};
}

VBNotifyCtx EphemeralVBucket::softDeleteStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    std::lock_guard<std::mutex> lh(sequenceLock);

    /* Soft delete the StoredValue in HT + EphemeralVBucket data structure.
     From the EphemeralVBucket data structure we will know whether to update
     (soft delete) the current StoredValue or mark the current StoredValue
     as stale and add a new softDeleted StoredValue.
     (Coming soon)
     */
    ht.unlocked_softDelete(htLock, v, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        v.setBySeqno(bySeqno);
    }

    return queueDirty(v, queueItmCtx);

    /* PlaceHolder for post seqno generation operation in EphemeralVBucket
     * data structure
     */
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
