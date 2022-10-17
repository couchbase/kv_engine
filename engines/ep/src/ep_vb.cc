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

#include "ep_vb.h"

#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "executorpool.h"
#include "failover-table.h"
#include "item.h"
#include "kvshard.h"
#include "stored_value_factories.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_state.h"
#include "vbucketdeletiontask.h"
#include <folly/lang/Assume.h>

EPVBucket::EPVBucket(Vbid i,
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
                     SeqnoAckCallback seqnoAckCb,
                     Configuration& config,
                     EvictionPolicy evictionPolicy,
                     std::unique_ptr<Collections::VB::Manifest> manifest,
                     vbucket_state_t initState,
                     uint64_t purgeSeqno,
                     uint64_t maxCas,
                     int64_t hlcEpochSeqno,
                     bool mightContainXattrs,
                     const nlohmann::json& replicationTopology,
                     uint64_t maxVisibleSeqno)
    : VBucket(i,
              newState,
              st,
              chkConfig,
              lastSeqno,
              lastSnapStart,
              lastSnapEnd,
              std::move(table),
              flusherCb,
              std::make_unique<StoredValueFactory>(st),
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
              hlcEpochSeqno,
              mightContainXattrs,
              replicationTopology,
              maxVisibleSeqno),
      shard(kvshard) {
}

EPVBucket::~EPVBucket() {
    if (!pendingBGFetches.empty()) {
        EP_LOG_WARN("Have {} pending BG fetches while destroying vbucket",
                    pendingBGFetches.size());
    }
}

ENGINE_ERROR_CODE EPVBucket::completeBGFetchForSingleItem(
        const DiskDocKey& key,
        const VBucketBGFetchItem& fetched_item,
        const std::chrono::steady_clock::time_point startTime) {
    ENGINE_ERROR_CODE status = fetched_item.value->getStatus();
    Item* fetchedValue = fetched_item.value->item.get();
    { // locking scope
        auto docKey = key.getDocKey();
        folly::SharedMutex::ReadHolder rlh(getStateLock());
        auto cHandle = lockCollections(docKey);
        auto res = fetchValidValue(
                WantsDeleted::Yes,
                TrackReference::Yes,
                cHandle.valid() ? QueueExpired::Yes : QueueExpired::No,
                cHandle,
                getState() == vbucket_state_replica ? ForGetReplicaOp::Yes
                                                    : ForGetReplicaOp::No);
        auto* v = res.storedValue;
        if (fetched_item.metaDataOnly) {
            if (status == ENGINE_SUCCESS) {
                if (v && v->isTempInitialItem()) {
                    ht.unlocked_restoreMeta(
                            res.lock.getHTLock(), *fetchedValue, *v);
                }
            } else if (status == ENGINE_KEY_ENOENT) {
                if (v && v->isTempInitialItem()) {
                    v->setNonExistent();
                }
                /* If ENGINE_KEY_ENOENT is the status from storage and the temp
                 key is removed from hash table by the time bgfetch returns
                 (in case multiple bgfetch is scheduled for a key), we still
                 need to return ENGINE_SUCCESS to the memcached worker thread,
                 so that the worker thread can visit the ep-engine and figure
                 out the correct flow */
                status = ENGINE_SUCCESS;
            } else {
                if (v && !v->isTempInitialItem()) {
                    status = ENGINE_SUCCESS;
                }
            }
        } else {
            bool restore = false;
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            } else {
                switch (eviction) {
                case EvictionPolicy::Value:
                    if (v && !v->isResident()) {
                        restore = true;
                    }
                    break;
                case EvictionPolicy::Full:
                    if (v) {
                        if (v->isTempInitialItem() || !v->isResident()) {
                            restore = true;
                        }
                    }
                    break;
                default:
                    throw std::logic_error("Unknown eviction policy");
                }
            }

            if (restore) {
                if (status == ENGINE_SUCCESS) {
                    ht.unlocked_restoreValue(
                            res.lock.getHTLock(), *fetchedValue, *v);
                    if (!v->isResident()) {
                        throw std::logic_error(
                                "VBucket::completeBGFetchForSingleItem: "
                                "storedvalue (which has seqno " +
                                std::to_string(v->getBySeqno()) +
                                ") should be resident after calling "
                                "restoreValue()");
                    }
                } else if (status == ENGINE_KEY_ENOENT) {
                    v->setNonExistent();
                    if (eviction == EvictionPolicy::Full) {
                        // For the full eviction, we should notify
                        // ENGINE_SUCCESS to the memcached worker thread,
                        // so that the worker thread can visit the
                        // ep-engine and figure out the correct error
                        // code.
                        status = ENGINE_SUCCESS;
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    EP_LOG_WARN("Failed background fetch for {}, seqno:{}",
                                getId(),
                                v->getBySeqno());
                    status = ENGINE_TMPFAIL;
                }
            }
        }
    } // locked scope ends

    if (fetched_item.metaDataOnly) {
        ++stats.bg_meta_fetched;
    } else {
        ++stats.bg_fetched;
    }

    const auto fetchEnd = std::chrono::steady_clock::now();
    updateBGStats(fetched_item.initTime, startTime, fetchEnd);

    // Close the BackgroundWait span; and add a BackgroundLoad span
    auto* traceable = cookie2traceable(fetched_item.cookie);
    if (traceable && traceable->isTracingEnabled()) {
        NonBucketAllocationGuard guard;
        auto& tracer = traceable->getTracer();
        tracer.end(fetched_item.traceSpanId, startTime);
        auto spanId =
                tracer.begin(cb::tracing::Code::BackgroundLoad, startTime);
        tracer.end(spanId, fetchEnd);
    }

    return status;
}

vb_bgfetch_queue_t EPVBucket::getBGFetchItems() {
    vb_bgfetch_queue_t fetches;
    LockHolder lh(pendingBGFetchesLock);
    fetches.swap(pendingBGFetches);
    return fetches;
}

bool EPVBucket::hasPendingBGFetchItems() {
    LockHolder lh(pendingBGFetchesLock);
    return !pendingBGFetches.empty();
}

HighPriorityVBReqStatus EPVBucket::checkAddHighPriorityVBEntry(
        uint64_t seqnoOrChkId,
        const void* cookie,
        HighPriorityVBNotify reqType) {
    if (shard) {
        ++shard->highPriorityCount;
    }
    addHighPriorityVBEntry(seqnoOrChkId, cookie, reqType);
    return HighPriorityVBReqStatus::RequestScheduled;
}

void EPVBucket::notifyHighPriorityRequests(EventuallyPersistentEngine& engine,
                                           uint64_t idNum,
                                           HighPriorityVBNotify notifyType) {
    auto toNotify = getHighPriorityNotifications(engine, idNum, notifyType);

    if (shard) {
        shard->highPriorityCount.fetch_sub(toNotify.size());
    }

    for (auto& notify : toNotify) {
        engine.notifyIOComplete(notify.first, notify.second);
    }
}

void EPVBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) {
    auto toNotify = tmpFailAndGetAllHpNotifies(e);

    if (shard) {
        shard->highPriorityCount.fetch_sub(toNotify.size());
    }

    // Add all the pendingBGFetches to the toNotify map
    {
        LockHolder lh(pendingBGFetchesLock);
        size_t num_of_deleted_pending_fetches = 0;
        for (auto& bgf : pendingBGFetches) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = bgf.second;
            for (auto& bgitem : bg_itm_ctx.bgfetched_list) {
                toNotify[bgitem->cookie] = ENGINE_NOT_MY_VBUCKET;
                e.storeEngineSpecific(bgitem->cookie, nullptr);
                ++num_of_deleted_pending_fetches;
            }
        }
        stats.numRemainingBgItems.fetch_sub(num_of_deleted_pending_fetches);
        pendingBGFetches.clear();
    }

    for (auto& notify : toNotify) {
        e.notifyIOComplete(notify.first, notify.second);
    }

    fireAllOps(e);
}

size_t EPVBucket::getNumItems() const {
    if (eviction == EvictionPolicy::Value) {
        return ht.getNumInMemoryItems() -
               (ht.getNumDeletedItems() + ht.getNumSystemItems() +
                ht.getNumPreparedSyncWrites());
    } else {
        // onDiskTotalItems includes everything not deleted. It does not include
        // prepared SyncWrites so just return it.
        return onDiskTotalItems;
    }
}

size_t EPVBucket::getNumTotalItems() const {
    return onDiskTotalItems;
}

void EPVBucket::setNumTotalItems(size_t totalItems) {
    onDiskTotalItems = totalItems;
}

void EPVBucket::incrNumTotalItems() {
    ++onDiskTotalItems;
}

void EPVBucket::decrNumTotalItems() {
    --onDiskTotalItems;
}

size_t EPVBucket::getNumNonResidentItems() const {
    if (eviction == EvictionPolicy::Value) {
        return ht.getNumInMemoryNonResItems();
    } else {
        size_t num_items = onDiskTotalItems;
        size_t num_res_items =
                ht.getNumInMemoryItems() - ht.getNumInMemoryNonResItems();
        return num_items > num_res_items ? (num_items - num_res_items) : 0;
    }
}

size_t EPVBucket::getNumSystemItems() const {
    // @todo: MB-26334 need to track system counts for persistent buckets
    return 0;
}

ENGINE_ERROR_CODE EPVBucket::statsVKey(const DocKey& key,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine) {
    auto readHandle = lockCollections(key);
    if (!readHandle.valid()) {
        return ENGINE_UNKNOWN_COLLECTION;
    }

    auto res = fetchValidValue(WantsDeleted::Yes,
                               TrackReference::Yes,
                               QueueExpired::Yes,
                               readHandle);

    auto* v = res.storedValue;
    if (v) {
        if (VBucket::isLogicallyNonExistent(*v, readHandle)) {
            ht.cleanupIfTemporaryItem(res.lock, *v);
            return ENGINE_KEY_ENOENT;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = std::make_shared<VKeyStatBGFetchTask>(&engine,
                                                            key,
                                                            getId(),
                                                            v->getBySeqno(),
                                                            cookie,
                                                            false);
        iom->schedule(task);
        return ENGINE_EWOULDBLOCK;
    } else {
        if (eviction == EvictionPolicy::Value) {
            return ENGINE_KEY_ENOENT;
        } else {
            auto rv = addTempStoredValue(res.lock, key);
            switch (rv.status) {
            case TempAddStatus::NoMem:
                return ENGINE_ENOMEM;
            case TempAddStatus::BgFetch: {
                ++stats.numRemainingBgJobs;
                ExecutorPool* iom = ExecutorPool::get();
                ExTask task = std::make_shared<VKeyStatBGFetchTask>(
                        &engine, key, getId(), -1, cookie, false);
                iom->schedule(task);
                return ENGINE_EWOULDBLOCK;
            }
            }
            folly::assume_unreachable();
        }
    }
}

void EPVBucket::completeStatsVKey(const DocKey& key, const GetValue& gcb) {
    auto cHandle = lockCollections(key);
    auto res = fetchValidValue(
            WantsDeleted::Yes,
            TrackReference::Yes,
            cHandle.valid() ? QueueExpired::Yes : QueueExpired::No,
            cHandle);

    auto* v = res.storedValue;
    if (v && v->isTempInitialItem()) {
        if (gcb.getStatus() == ENGINE_SUCCESS) {
            ht.unlocked_restoreValue(res.lock.getHTLock(), *gcb.item, *v);
            if (!v->isResident()) {
                throw std::logic_error(
                        "VBucket::completeStatsVKey: "
                        "storedvalue (which has seqno:" +
                        std::to_string(v->getBySeqno()) +
                        ") should be resident after calling restoreValue()");
            }
        } else if (gcb.getStatus() == ENGINE_KEY_ENOENT) {
            v->setNonExistent();
        } else {
            // underlying kvstore couldn't fetch requested data
            // log returned error and notify TMPFAIL to client
            EP_LOG_WARN(
                    "VBucket::completeStatsVKey: "
                    "Failed background fetch for {}, seqno:{}",
                    getId(),
                    v->getBySeqno());
        }
    }
}

bool EPVBucket::areDeletedItemsAlwaysResident() const {
    // Persistent buckets do not keep all deleted items resident in memory.
    // (They may be *temporarily* resident while a request is in flight asking
    // for a deleted item).
    return false;
}

void EPVBucket::addStats(VBucketStatsDetailLevel detail,
                         const AddStatFn& add_stat,
                         const void* c) {
    _addStats(detail, add_stat, c);

    if (detail == VBucketStatsDetailLevel::Full) {
        DBFileInfo fileInfo;

        // Only try to read disk if we believe the file has been created
        if (!isBucketCreation()) {
            try {
                fileInfo = shard->getRWUnderlying()->getDbFileInfo(getId());
            } catch (std::runtime_error& e) {
                EP_LOG_WARN(
                        "VBucket::addStats: Exception caught during "
                        "getDbFileInfo "
                        "for {} - what(): {}",
                        getId(),
                        e.what());
            }
        }
        addStat("db_data_size", fileInfo.getEstimatedLiveData(), add_stat, c);
        addStat("db_file_size", fileInfo.fileSize, add_stat, c);
        addStat("db_prepare_size", fileInfo.prepareBytes, add_stat, c);
    }
}

cb::mcbp::Status EPVBucket::evictKey(
        const char** msg,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto res = fetchValidValue(
            WantsDeleted::No, TrackReference::No, QueueExpired::Yes, cHandle);
    auto* v = res.storedValue;
    if (!v) {
        if (eviction == EvictionPolicy::Value) {
            *msg = "Not found.";
            return cb::mcbp::Status::KeyEnoent;
        }
        *msg = "Already ejected.";
        return cb::mcbp::Status::Success;
    }

    if (v->isResident()) {
        if (ht.unlocked_ejectItem(res.lock, v, eviction)) {
            *msg = "Ejected.";

            // Add key to bloom filter in case of full eviction mode
            if (eviction == EvictionPolicy::Full) {
                addToFilter(cHandle.getKey());
            }
            return cb::mcbp::Status::Success;
        }
        *msg = "Can't eject: Dirty object.";
        return cb::mcbp::Status::KeyEexists;
    }

    *msg = "Already ejected.";
    return cb::mcbp::Status::Success;
}

bool EPVBucket::pageOut(const Collections::VB::Manifest::ReadHandle& readHandle,
                        const HashTable::HashBucketLock& lh,
                        StoredValue*& v) {
    return ht.unlocked_ejectItem(lh, v, eviction);
}

bool EPVBucket::eligibleToPageOut(const HashTable::HashBucketLock& lh,
                                  const StoredValue& v) const {
    return v.eligibleForEviction(eviction);
}

size_t EPVBucket::getPageableMemUsage() {
    if (eviction == EvictionPolicy::Full) {
        return ht.getItemMemory();
    } else {
        return ht.getItemMemory() - ht.getMetadataMemory();
    }
}

size_t EPVBucket::queueBGFetchItem(const DocKey& key,
                                   std::unique_ptr<VBucketBGFetchItem> fetch,
                                   BgFetcher* bgFetcher) {
    // While a DiskDocKey supports both the committed and prepared namespaces,
    // ep-engine doesn't support evicting prepared SyncWrites and as such
    // we don't allow bgfetching from Prepared namespace - so just construct
    // DiskDocKey with pending unconditionally false.
    DiskDocKey diskKey{key, /*pending*/ false};
    LockHolder lh(pendingBGFetchesLock);
    vb_bgfetch_item_ctx_t& bgfetch_itm_ctx = pendingBGFetches[diskKey];

    if (bgfetch_itm_ctx.bgfetched_list.empty()) {
        bgfetch_itm_ctx.isMetaOnly = GetMetaOnly::Yes;
    }

    if (!fetch->metaDataOnly) {
        bgfetch_itm_ctx.isMetaOnly = GetMetaOnly::No;
    }

    fetch->value = &bgfetch_itm_ctx.value;
    bgfetch_itm_ctx.bgfetched_list.push_back(std::move(fetch));

    bgFetcher->addPendingVB(getId());
    return pendingBGFetches.size();
}

std::tuple<StoredValue*, MutationStatus, VBNotifyCtx>
EPVBucket::updateStoredValue(const HashTable::HashBucketLock& hbl,
                             StoredValue& v,
                             const Item& itm,
                             const VBQueueItemCtx& queueItmCtx,
                             bool justTouch) {
    HashTable::UpdateResult result;
    if (justTouch) {
        result.status = MutationStatus::WasDirty;
        result.storedValue = &v;
    } else {
        result = ht.unlocked_updateStoredValue(hbl, v, itm);
        switch (result.status) {
        case MutationStatus::WasClean:
        case MutationStatus::WasDirty:
            break;
        case MutationStatus::IsPendingSyncWrite:
            // Fail; skip queueDirty and return early.
            return std::make_tuple(
                    result.storedValue, result.status, VBNotifyCtx{});
        default:
            throw std::logic_error(
                    "EPVBucket::updateStoredValue: Unexpected status from "
                    "HT::updateStoredValue:" +
                    to_string(result.status));
        }
    }

    return std::make_tuple(result.storedValue,
                           result.status,
                           queueDirty(hbl, *result.storedValue, queueItmCtx));
}

std::pair<StoredValue*, VBNotifyCtx> EPVBucket::addNewStoredValue(
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx& queueItmCtx,
        GenerateRevSeqno genRevSeqno) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    if (genRevSeqno == GenerateRevSeqno::Yes) {
        /* This item could potentially be recreated */
        updateRevSeqNoOfNewStoredValue(*v);
    }

    return {v, queueDirty(hbl, *v, queueItmCtx)};
}

std::tuple<StoredValue*, DeletionStatus, VBNotifyCtx>
EPVBucket::softDeleteStoredValue(const HashTable::HashBucketLock& hbl,
                                 StoredValue& v,
                                 bool onlyMarkDeleted,
                                 const VBQueueItemCtx& queueItmCtx,
                                 uint64_t bySeqno,
                                 DeleteSource deleteSource) {
    auto result = ht.unlocked_softDelete(hbl, v, onlyMarkDeleted, deleteSource);
    switch (result.status) {
    case DeletionStatus::Success:
        // Proceed to queue the deletion into the CheckpointManager.
        if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
            result.deletedValue->setBySeqno(bySeqno);
        }
        return std::make_tuple(
                result.deletedValue,
                result.status,
                queueDirty(hbl, *result.deletedValue, queueItmCtx));

    case DeletionStatus::IsPendingSyncWrite:
        return std::make_tuple(
                result.deletedValue, result.status, VBNotifyCtx{});
    };
    folly::assume_unreachable();
}

VBNotifyCtx EPVBucket::commitStoredValue(HashTable::FindUpdateResult& values,
                                         uint64_t prepareSeqno,
                                         const VBQueueItemCtx& queueItmCtx,
                                         boost::optional<int64_t> commitSeqno) {
    // Remove a previously committed SV if one exists
    if (values.committed) {
        // Only delete the existing committed item
        ht.unlocked_del(values.pending.getHBL(), values.committed);
    }

    ht.unlocked_setCommitted(values.pending.getHBL(),
                             *values.pending.getSV(),
                             CommittedState::CommittedViaPrepare);

    if (commitSeqno) {
        Expects(queueItmCtx.genBySeqno == GenerateBySeqno::No);
        values.pending.setBySeqno(*commitSeqno);
    }

    return queueDirty(
            values.pending.getHBL(), *values.pending.getSV(), queueItmCtx);
}

VBNotifyCtx EPVBucket::abortStoredValue(const HashTable::HashBucketLock& hbl,
                                        StoredValue& v,
                                        int64_t prepareSeqno,
                                        boost::optional<int64_t> abortSeqno) {
    // Note: We have to enqueue the item into the CM /before/ removing it from
    //     the HT as the removal is synchronous and deallocates the StoredValue
    VBQueueItemCtx queueItmCtx;
    if (abortSeqno) {
        queueItmCtx.genBySeqno = GenerateBySeqno::No;
        v.setBySeqno(*abortSeqno);
    }
    auto notify = queueAbort(hbl, v, prepareSeqno, queueItmCtx);

    ht.unlocked_del(hbl, &v);

    return notify;
}

VBNotifyCtx EPVBucket::addNewAbort(const HashTable::HashBucketLock& hbl,
                                   const DocKey& key,
                                   int64_t prepareSeqno,
                                   int64_t abortSeqno) {
    VBQueueItemCtx queueItmCtx;
    queueItmCtx.genBySeqno = GenerateBySeqno::No;
    queued_item item = createNewAbortedItem(key, prepareSeqno, abortSeqno);
    return queueAbortForUnseenPrepare(item, queueItmCtx);
}

void EPVBucket::bgFetch(const DocKey& key,
                        const void* cookie,
                        EventuallyPersistentEngine& engine,
                        const bool isMeta) {
    // schedule to the current batch of background fetch of the given
    // vbucket
    size_t bgfetch_size = queueBGFetchItem(
            key,
            std::make_unique<VBucketBGFetchItem>(cookie, isMeta),
            getShard()->getBgFetcher());
    if (getShard()) {
        getShard()->getBgFetcher()->notifyBGEvent();
    }
    EP_LOG_DEBUG("Queued a background fetch, now at {}",
                 uint64_t(bgfetch_size));
}

/* [TBD]: Get rid of std::unique_lock<std::mutex> lock */
ENGINE_ERROR_CODE
EPVBucket::addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                                 const DocKey& key,
                                 const void* cookie,
                                 EventuallyPersistentEngine& engine,
                                 bool metadataOnly) {
    auto rv = addTempStoredValue(hbl, key);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return ENGINE_ENOMEM;
    case TempAddStatus::BgFetch:
        hbl.getHTLock().unlock();
        bgFetch(key, cookie, engine, metadataOnly);
        return ENGINE_EWOULDBLOCK;
    }
    folly::assume_unreachable();
}

void EPVBucket::updateBGStats(
        const std::chrono::steady_clock::time_point init,
        const std::chrono::steady_clock::time_point start,
        const std::chrono::steady_clock::time_point stop) {
    ++stats.bgNumOperations;
    auto waitNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(start - init);
    auto w = static_cast<hrtime_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(waitNs)
                    .count());
    BlockTimer::log(waitNs, "bgwait", stats.timingLog);
    stats.bgWaitHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(waitNs));
    stats.bgWait.fetch_add(w);
    atomic_setIfLess(stats.bgMinWait, w);
    atomic_setIfBigger(stats.bgMaxWait, w);

    auto lNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    auto l = static_cast<hrtime_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(lNs).count());
    BlockTimer::log(lNs, "bgload", stats.timingLog);
    stats.bgLoadHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(lNs));
    stats.bgLoad.fetch_add(l);
    atomic_setIfLess(stats.bgMinLoad, l);
    atomic_setIfBigger(stats.bgMaxLoad, l);
}

GetValue EPVBucket::getInternalNonResident(const DocKey& key,
                                           const void* cookie,
                                           EventuallyPersistentEngine& engine,
                                           QueueBgFetch queueBgFetch,
                                           const StoredValue& v) {
    if (queueBgFetch == QueueBgFetch::Yes) {
        bgFetch(key, cookie, engine);
    }
    return GetValue(
            nullptr, ENGINE_EWOULDBLOCK, v.getBySeqno(), true, v.getNRUValue());
}

void EPVBucket::setupDeferredDeletion(const void* cookie) {
    setDeferredDeletionCookie(cookie);
    auto revision = getShard()->getRWUnderlying()->prepareToDelete(getId());
    EP_LOG_INFO("EPVBucket::setupDeferredDeletion({}) {}, revision:{}",
                cookie,
                getId(),
                revision);
    deferredDeletionFileRevision.store(revision);
    setDeferredDeletion(true);
}

void EPVBucket::scheduleDeferredDeletion(EventuallyPersistentEngine& engine) {
    ExTask task = std::make_shared<VBucketMemoryAndDiskDeletionTask>(
            engine, *shard, this);
    ExecutorPool::get()->schedule(task);
}

MutationStatus EPVBucket::insertFromWarmup(Item& itm,
                                           bool eject,
                                           bool keyMetaDataOnly,
                                           bool checkMemUsed) {
    if (checkMemUsed &&
        !hasMemoryForStoredValue(stats, itm, UseActiveVBMemThreshold::Yes)) {
        return MutationStatus::NoMem;
    }

    return ht.insertFromWarmup(itm, eject, keyMetaDataOnly, eviction);
}

void EPVBucket::loadOutstandingPrepares(
        const folly::SharedMutex::WriteHolder& vbStateLock,
        const vbucket_state& vbs,
        std::vector<queued_item>&& outstandingPrepares) {
    // First insert all prepares into the HashTable, updating their type
    // to PreparedMaybeVisible to ensure that the document cannot be read until
    // the Prepare is re-committed.
    for (auto& prepare : outstandingPrepares) {
        prepare->setPreparedMaybeVisible();

        // This function is being used for warmup and rollback. In both cases
        // we are not checking the mem_used and allowing the inserts. Note: that
        // by default the threshold which is being checked is 93% of quota.
        // In either case of warmup or rollback, we cannot tolerate having less
        // than 100% of the prepares loaded for correct functionality, even in
        // full-eviction mode.
        auto res = insertFromWarmup(*prepare,
                                    /*shouldEject*/ false,
                                    /*metadataOnly*/ false,
                                    /*checkMemUsed*/ false);
        Expects(res == MutationStatus::NotFound);
    }

    EP_LOG_INFO(
            "EPVBucket::loadOutstandingPrepares: ({}) created DM with PCS:{}, "
            "PPS:{}, HPS:{}, number of prepares loaded:{}",
            getId(),
            vbs.persistedCompletedSeqno,
            vbs.persistedPreparedSeqno,
            vbs.highPreparedSeqno,
            outstandingPrepares.size());

    // Second restore them into the appropriate DurabilityMonitor.
    switch (getState()) {
    case vbucket_state_active: {
        durabilityMonitor = std::make_unique<ActiveDurabilityMonitor>(
                stats, *this, vbs, std::move(outstandingPrepares));

        // Some of the prepares may now be viable for commit
        getActiveDM().checkForCommit();
        return;
    }
    case vbucket_state_replica:
    case vbucket_state_pending:
    case vbucket_state_dead:
        durabilityMonitor = std::make_unique<PassiveDurabilityMonitor>(
                *this,
                vbs.highPreparedSeqno,
                vbs.persistedCompletedSeqno,
                std::move(outstandingPrepares));
        return;
    }
}

size_t EPVBucket::estimateNewMemoryUsage(EPStats& st, const Item& item) {
    return st.getEstimatedTotalMemoryUsed() +
           StoredValue::getRequiredStorage(item.getKey());
}

size_t EPVBucket::getNumPersistedDeletes() const {
    if (isBucketCreation()) {
        // If creation is true then no disk file exists
        return 0;
    }
    return shard->getRWUnderlying()->getNumPersistedDeletes(getId());
}

void EPVBucket::dropKey(int64_t bySeqno,
                        Collections::VB::Manifest::CachingReadHandle& cHandle) {
    // dropKey must not generate expired items as it's used for erasing a
    // collection.
    auto res = fetchValidValue(
            WantsDeleted::No, TrackReference::No, QueueExpired::No, cHandle);
    if (res.storedValue && res.storedValue->getBySeqno() == bySeqno) {
        ht.unlocked_del(res.lock, res.storedValue);
    }
}

/*
 * Queue the item to the checkpoint and return the seqno the item was
 * allocated.
 */
int64_t EPVBucket::addSystemEventItem(
        Item* item,
        OptionalSeqno seqno,
        boost::optional<CollectionID> cid,
        const Collections::VB::Manifest::WriteHandle& wHandle) {
    item->setVBucketId(getId());
    queued_item qi(item);

    // Set the system events delete time if needed for tombstoning
    if (qi->isDeleted() && qi->getDeleteTime() == 0) {
        qi->setExpTime(ep_real_time());
    }

        checkpointManager->queueDirty(
                *this,
                qi,
                getGenerateBySeqno(seqno),
                GenerateCas::Yes,
                nullptr /* No pre link step as this is for system events */);
    VBNotifyCtx notifyCtx;
    // If the seqno is initialized, skip replication notification
    notifyCtx.notifyReplication = !seqno.is_initialized();
    notifyCtx.notifyFlusher = true;
    notifyCtx.bySeqno = qi->getBySeqno();
    notifyNewSeqno(notifyCtx);

    // We don't record anything interesting for scopes
    if (cid) {
        doCollectionsStats(wHandle, *cid, notifyCtx);
    }
    return qi->getBySeqno();
}

bool EPVBucket::isValidDurabilityLevel(cb::durability::Level level) {
    switch (level) {
    case cb::durability::Level::None:
        return false;
    case cb::durability::Level::Majority:
    case cb::durability::Level::MajorityAndPersistOnMaster:
    case cb::durability::Level::PersistToMajority:
        return true;
    }

    folly::assume_unreachable();
}

void EPVBucket::processImplicitlyCompletedPrepare(
        HashTable::StoredValueProxy& v) {
    // As we have passed a StoredValueProxy to this function (the callers need
    // a HashTable::FindUpdateResult) we need to be careful about our stats
    // updates. The StoredValueProxy attempts to do a
    // HashTable::Statistics::epilogue stats update when we destruct it. This is
    // generally fine, but if we want to use any other HashTable function with
    // a StoredValueProxy we need a way to skip the StoredValueProxy's stats
    // update as the other HashTable function will do it's own. In this case,
    // we can call StoredValueProxy::release to release the ownership of the
    // pointer in the StoredValueProxy and skip any stats update. This consumes
    // the StoredValue* and invalidates the StoredValueProxy so it should not be
    // used after.
    ht.unlocked_del(v.getHBL(), v.release());
}
