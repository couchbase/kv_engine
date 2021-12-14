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

#include "ep_vb.h"

#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill_by_id_disk.h"
#include "dcp/backfill_by_seqno_disk.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "failover-table.h"
#include "flusher.h"
#include "item.h"
#include "kvshard.h"
#include "stored_value_factories.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_state.h"
#include "vbucketdeletiontask.h"
#include <executor/executorpool.h>
#include <folly/lang/Assume.h>
#include <gsl/gsl-lite.hpp>
#include <platform/histogram.h>

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
                     SyncWriteTimeoutHandlerFactory syncWriteTimeoutFactory,
                     SeqnoAckCallback seqnoAckCb,
                     Configuration& config,
                     EvictionPolicy evictionPolicy,
                     std::unique_ptr<Collections::VB::Manifest> manifest,
                     KVBucket* bucket,
                     vbucket_state_t initState,
                     uint64_t purgeSeqno,
                     uint64_t maxCas,
                     int64_t hlcEpochSeqno,
                     bool mightContainXattrs,
                     const nlohmann::json* replicationTopology,
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
              syncWriteTimeoutFactory,
              seqnoAckCb,
              config,
              evictionPolicy,
              std::move(manifest),
              bucket,
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

cb::engine_errc EPVBucket::completeBGFetchForSingleItem(
        const DiskDocKey& key,
        const FrontEndBGFetchItem& fetched_item,
        const std::chrono::steady_clock::time_point startTime) {
    cb::engine_errc status = fetched_item.value->getStatus();
    Item* fetchedValue = fetched_item.value->item.get();

    // when this scope ends, update stats and tracing
    auto traceEndGuard = gsl::finally(
            [this, &fetched_item = std::as_const(fetched_item), startTime] {
                const auto fetchEnd = std::chrono::steady_clock::now();
                updateBGStats(fetched_item.initTime, startTime, fetchEnd);

                // Close the BackgroundWait span; and add a BackgroundLoad span
                auto* traceable = cookie2traceable(fetched_item.cookie);
                if (traceable && traceable->isTracingEnabled()) {
                    NonBucketAllocationGuard guard;
                    auto& tracer = traceable->getTracer();
                    tracer.end(fetched_item.traceSpanId, startTime);
                    auto spanId = tracer.begin(
                            cb::tracing::Code::BackgroundLoad, startTime);
                    tracer.end(spanId, fetchEnd);
                }
            });

    switch (fetched_item.filter) {
    case ValueFilter::KEYS_ONLY:
        ++stats.bg_meta_fetched;
        break;
    case ValueFilter::VALUES_DECOMPRESSED:
    case ValueFilter::VALUES_COMPRESSED:
        ++stats.bg_fetched;
        break;
    }

    { // locking scope
        auto docKey = key.getDocKey();
        folly::SharedMutex::ReadHolder rlh(getStateLock());
        auto cHandle = lockCollections(docKey);
        auto res = fetchValidValue(
                WantsDeleted::Yes,
                TrackReference::Yes,
                cHandle,
                getState() == vbucket_state_replica ? ForGetReplicaOp::Yes
                                                    : ForGetReplicaOp::No);
        auto* v = res.storedValue;

        if (!v) {
            /* MB-14859:
             * If cb::engine_errc::no_such_key is the status from storage and
             * the temp key is removed from hash table by the time bgfetch
             * returns (in case multiple bgfetch is scheduled for a key), we
             * still need to return cb::engine_errc::success to the memcached
             * worker thread, so that the worker thread can visit the ep-engine
             * and figure out the correct flow */
            if (fetched_item.filter == ValueFilter::KEYS_ONLY &&
                status == cb::engine_errc::no_such_key) {
                return cb::engine_errc::success;
            }
            return status;
        }

        if (v->isResident()) {
            // Nothing to do - item already has meta and a resident value
            return cb::engine_errc::success;
        }

        switch (fetched_item.filter) {
        case ValueFilter::KEYS_ONLY:
            if (status == cb::engine_errc::success) {
                if (v->isTempInitialItem() &&
                    v->getCas() == fetched_item.token) {
                    ht.unlocked_restoreMeta(
                            res.lock.getHTLock(), *fetchedValue, *v);
                }
            } else if (status == cb::engine_errc::no_such_key) {
                if (v->isTempInitialItem() &&
                    v->getCas() == fetched_item.token) {
                    v->setNonExistent();
                }

                status = cb::engine_errc::success;
            } else {
                if (!v->isTempInitialItem()) {
                    status = cb::engine_errc::success;
                }
            }
            break;
        case ValueFilter::VALUES_DECOMPRESSED:
        case ValueFilter::VALUES_COMPRESSED: {
            // We can only restore the value if:
            // 1) The stored value exists (checked above)
            // 2) It is temp-initial or non-resident
            //    (non-residency checked above)
            // 3) The cas of the stored value is equal to that of the
            //    fetch token
            // This ensures that we only "complete" bg fetches if this
            // is still the most recent version of the key. If we did
            // not, we'd potentially fetch old values back into the
            // HashTable.
            if (v->getCas() == fetched_item.token) {
                if (status == cb::engine_errc::success) {
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
                } else if (status == cb::engine_errc::no_such_key) {
                    if (!v->isTempItem()) {
                        throw std::logic_error(fmt::format(
                                "({}) VBucket::completeBGFetchForSingleItem: "
                                "non-temp non-resident StoredValue should "
                                "always exist on disk, but doesn't. Will not"
                                "change non-temp item to temp non-existent. "
                                "seqno:{} isDeleted:{} ",
                                getId(),
                                v->getBySeqno(),
                                v->isDeleted()));
                    }
                    v->setNonExistent();
                    if (eviction == EvictionPolicy::Full) {
                        // For the full eviction, we should notify
                        // cb::engine_errc::success to the memcached worker
                        // thread, so that the worker thread can visit the
                        // ep-engine and figure out the correct error
                        // code.
                        status = cb::engine_errc::success;
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    EP_LOG_WARN("Failed background fetch for {}, seqno:{}",
                                getId(),
                                v->getBySeqno());
                    status = cb::engine_errc::temporary_failure;
                }
            }
            break;
        }
        }
    } // locked scope ends

    return status;
}

void EPVBucket::completeCompactionExpiryBgFetch(
        const DiskDocKey& key, const CompactionBGFetchItem& fetchedItem) {
    cb::engine_errc status = fetchedItem.value->getStatus();

    // Status might be non-success if either:
    //     a) BGFetch failed for some reason
    //     b) Item does not exist on disk (KEY_ENOENT)
    //
    // In the case of a) we don't care about doing anything here, the next
    // compaction will try to expire the item on disk anyway.
    // In the case of b) we can simply skip trying to expire this item as it
    // has been superseded (by a deletion).
    if (status != cb::engine_errc::success) {
        return;
    }

    Item* fetchedValue = fetchedItem.value->item.get();
    { // locking scope
        auto docKey = key.getDocKey();
        folly::SharedMutex::ReadHolder rlh(getStateLock());
        if (getState() != vbucket_state_active) {
            return;
        }

        auto cHandle = lockCollections(docKey);
        if (!cHandle.valid()) {
            return;
        }

        auto htRes = ht.findForUpdate(docKey);

        // We can only "complete" the fetch if the item that drove it (the
        // temp initial StoredValue) is still in the HT in the same state.
        if (!htRes.committed) {
            return;
        }

        if (!htRes.committed->isTempInitialItem()) {
            return;
        }

        if (htRes.committed->getCas() != fetchedItem.token) {
            return;
        }

        // If we find a StoredValue then the item that we are trying to expire
        // has been superseded by a new one (as we wouldn't have tried to
        // BGFetch the item if it was there originally). In this case, we don't
        // have to expire anything.
        if (htRes.committed && !htRes.committed->isTempItem() &&
            htRes.committed->getCas() != fetchedItem.compactionItem.getCas()) {
            return;
        }

        // Check the cas of our BGFetched item against the cas of the item we
        // originally saw during our compaction (stashed in the
        // CompactionBGFetchItem object). If they are different then the item
        // has been superseded by a new one and we don't need to do anything.
        // Otherwise, expire the item.
        if (fetchedValue->getCas() != fetchedItem.compactionItem.getCas()) {
            return;
        }

        // Only add a new StoredValue if there is not an already existing
        // Temp item. Otherwise, we should just re-use the existing one to
        // prevent us from having multiple values for the same key.
        auto* sVToUse = htRes.committed;
        if (!htRes.committed) {
            auto addTemp = addTempStoredValue(htRes.getHBL(), key.getDocKey());
            if (addTemp.status == TempAddStatus::NoMem) {
                return;
            }
            sVToUse = addTemp.storedValue;
            sVToUse->setTempDeleted();
            sVToUse->setRevSeqno(fetchedItem.compactionItem.getRevSeqno());
            htRes.committed = sVToUse;
        }

        // @TODO perf: Investigate if it is necessary to add this to the
        //  HashTable
        ht.unlocked_updateStoredValue(
                htRes.getHBL(), *sVToUse, fetchedItem.compactionItem);
        VBNotifyCtx notifyCtx;
        std::tie(std::ignore, std::ignore, notifyCtx) =
                processExpiredItem(htRes, cHandle, ExpireBy::Compactor);
        // we unlock ht lock here because we want to avoid potential
        // lock inversions arising from notifyNewSeqno() call
        htRes.getHBL().getHTLock().unlock();
        notifyNewSeqno(notifyCtx);
        doCollectionsStats(cHandle, notifyCtx);
    }
}

vb_bgfetch_queue_t EPVBucket::getBGFetchItems() {
    vb_bgfetch_queue_t fetches;
    std::lock_guard<std::mutex> lh(pendingBGFetchesLock);
    fetches.swap(pendingBGFetches);
    return fetches;
}

bool EPVBucket::hasPendingBGFetchItems() {
    std::lock_guard<std::mutex> lh(pendingBGFetchesLock);
    return !pendingBGFetches.empty();
}

HighPriorityVBReqStatus EPVBucket::checkAddHighPriorityVBEntry(
        uint64_t seqno, const CookieIface* cookie) {
    addHighPriorityVBEntry(seqno, cookie);
    return HighPriorityVBReqStatus::RequestScheduled;
}

void EPVBucket::notifyHighPriorityRequests(EventuallyPersistentEngine& engine,
                                           uint64_t seqno) {
    auto toNotify = getHighPriorityNotifications(engine, seqno);

    for (auto& notify : toNotify) {
        engine.notifyIOComplete(notify.first, notify.second);
    }
}

void EPVBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) {
    auto toNotify = tmpFailAndGetAllHpNotifies(e);

    // Add all the pendingBGFetches to the toNotify map
    {
        std::lock_guard<std::mutex> lh(pendingBGFetchesLock);
        size_t num_of_deleted_pending_fetches = 0;
        for (auto& bgf : pendingBGFetches) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = bgf.second;
            for (auto& bgitem : bg_itm_ctx.getRequests()) {
                bgitem->abort(e, cb::engine_errc::not_my_vbucket, toNotify);
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

void EPVBucket::incrNumTotalItems(size_t numItemsAdded) {
    onDiskTotalItems += numItemsAdded;
}

void EPVBucket::decrNumTotalItems(size_t numItemsRemoved) {
    onDiskTotalItems -= numItemsRemoved;
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

cb::engine_errc EPVBucket::statsVKey(const DocKey& key,
                                     const CookieIface* cookie,
                                     EventuallyPersistentEngine& engine) {
    auto readHandle = lockCollections(key);
    if (!readHandle.valid()) {
        return cb::engine_errc::unknown_collection;
    }

    auto res = fetchValidValue(WantsDeleted::Yes,
                               TrackReference::Yes,
                               readHandle);

    auto* v = res.storedValue;
    if (v) {
        if (VBucket::isLogicallyNonExistent(*v, readHandle)) {
            ht.cleanupIfTemporaryItem(res.lock, *v);
            return cb::engine_errc::no_such_key;
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
        return cb::engine_errc::would_block;
    } else {
        if (eviction == EvictionPolicy::Value) {
            return cb::engine_errc::no_such_key;
        } else {
            auto rv = addTempStoredValue(res.lock, key);
            switch (rv.status) {
            case TempAddStatus::NoMem:
                return cb::engine_errc::no_memory;
            case TempAddStatus::BgFetch: {
                ++stats.numRemainingBgJobs;
                ExecutorPool* iom = ExecutorPool::get();
                ExTask task = std::make_shared<VKeyStatBGFetchTask>(
                        &engine, key, getId(), -1, cookie, false);
                iom->schedule(task);
                return cb::engine_errc::would_block;
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
            cHandle);

    auto* v = res.storedValue;
    if (v && v->isTempInitialItem()) {
        if (gcb.getStatus() == cb::engine_errc::success) {
            ht.unlocked_restoreValue(res.lock.getHTLock(), *gcb.item, *v);
            if (!v->isResident()) {
                throw std::logic_error(
                        "VBucket::completeStatsVKey: "
                        "storedvalue (which has seqno:" +
                        std::to_string(v->getBySeqno()) +
                        ") should be resident after calling restoreValue()");
            }
        } else if (gcb.getStatus() == cb::engine_errc::no_such_key) {
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
                         const CookieIface* c) {
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

UniqueDCPBackfillPtr EPVBucket::createDCPBackfill(
        EventuallyPersistentEngine& e,
        std::shared_ptr<ActiveStream> stream,
        uint64_t startSeqno,
        uint64_t endSeqno) {
    /* create a DCPBackfillBySeqnoDisk object */
    return std::make_unique<DCPBackfillBySeqnoDisk>(
            *e.getKVBucket(), stream, startSeqno, endSeqno);
}

UniqueDCPBackfillPtr EPVBucket::createDCPBackfill(
        EventuallyPersistentEngine& e,
        std::shared_ptr<ActiveStream> stream,
        CollectionID cid) {
    /* create a DCPBackfillByIdDisk object */
    return std::make_unique<DCPBackfillByIdDisk>(*e.getKVBucket(), stream, cid);
}

cb::mcbp::Status EPVBucket::evictKey(
        const char** msg, const Collections::VB::CachingReadHandle& cHandle) {
    auto res = fetchValidValue(WantsDeleted::No, TrackReference::No, cHandle);
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

bool EPVBucket::pageOut(const Collections::VB::ReadHandle& readHandle,
                        const HashTable::HashBucketLock& lh,
                        StoredValue*& v,
                        bool isDropped) {
    if (isDropped) {
        Expects(v);
        dropStoredValue(lh, *v);
        return true;
    }
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
                                   std::unique_ptr<BGFetchItem> fetch,
                                   BgFetcher& bgFetcher) {
    // While a DiskDocKey supports both the committed and prepared namespaces,
    // ep-engine doesn't support evicting prepared SyncWrites and as such
    // we don't allow bgfetching from Prepared namespace - so just construct
    // DiskDocKey with pending unconditionally false.
    DiskDocKey diskKey{key, /*pending*/ false};
    std::lock_guard<std::mutex> lh(pendingBGFetchesLock);
    vb_bgfetch_item_ctx_t& bgfetch_itm_ctx = pendingBGFetches[diskKey];
    bgfetch_itm_ctx.addBgFetch(std::move(fetch));

    bgFetcher.addPendingVB(getId());
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
                                         std::optional<int64_t> commitSeqno) {
    // Remove a previously committed SV if one exists
    if (values.committed) {
        // Only delete the existing committed item
        ht.unlocked_del(values.pending.getHBL(), values.committed);
    }

    values.pending.setCommitted(CommittedState::CommittedViaPrepare);

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
                                        std::optional<int64_t> abortSeqno) {
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

void EPVBucket::bgFetch(HashTable::HashBucketLock&& hbl,
                        const DocKey& key,
                        const StoredValue& v,
                        const CookieIface* cookie,
                        EventuallyPersistentEngine& engine,
                        const bool isMeta) {
    auto token = v.getCas();
    // We unlock the hbl here as queueBGFetchItem will take a vBucket wide lock
    // and we don't want need this lock anymore.
    hbl.getHTLock().unlock();

    // @TODO could the BgFetcher ever not be there? It should probably be a
    // reference if that's the case
    // schedule to the current batch of background fetch of the given
    // vbucket
    const auto filter =
            isMeta ? ValueFilter::KEYS_ONLY
                   : dynamic_cast<EPBucket&>(*bucket)
                             .getValueFilterForCompressionMode(cookie);
    size_t bgfetch_size = queueBGFetchItem(
            key,
            std::make_unique<FrontEndBGFetchItem>(cookie, filter, token),
            getBgFetcher());
    EP_LOG_DEBUG("Queued a background fetch, now at {}",
                 uint64_t(bgfetch_size));
}

/* [TBD]: Get rid of std::unique_lock<std::mutex> lock */
cb::engine_errc EPVBucket::addTempItemAndBGFetch(
        HashTable::HashBucketLock&& hbl,
        const DocKey& key,
        const CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        bool metadataOnly) {
    auto rv = addTempStoredValue(hbl, key);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return cb::engine_errc::no_memory;
    case TempAddStatus::BgFetch:
        bgFetch(std::move(hbl),
                key,
                *rv.storedValue,
                cookie,
                engine,
                metadataOnly);
        return cb::engine_errc::would_block;
    }
    folly::assume_unreachable();
}

cb::engine_errc EPVBucket::bgFetchForCompactionExpiry(
        HashTable::HashBucketLock& hbl, const DocKey& key, const Item& item) {
    auto rv = addTempStoredValue(hbl, key);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return cb::engine_errc::no_memory;
    case TempAddStatus::BgFetch:
        // schedule to the current batch of background fetch of the given
        // vbucket
        auto token = rv.storedValue->getCas();
        hbl.getHTLock().unlock();
        auto& bgFetcher = getBgFetcher();
        auto bgFetchSize = queueBGFetchItem(
                key,
                std::make_unique<CompactionBGFetchItem>(item, token),
                bgFetcher);
        EP_LOG_DEBUG(
                "Queue a background fetch for compaction expiry, now at {}",
                bgFetchSize);
        return cb::engine_errc::would_block;
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
    BlockTimer::log(waitNs, "bgwait", stats.timingLog.get());
    stats.bgWaitHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(waitNs));
    stats.bgWait.fetch_add(w);

    auto lNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    auto l = static_cast<hrtime_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(lNs).count());
    BlockTimer::log(lNs, "bgload", stats.timingLog.get());
    stats.bgLoadHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(lNs));
    stats.bgLoad.fetch_add(l);
}

GetValue EPVBucket::getInternalNonResident(HashTable::HashBucketLock&& hbl,
                                           const DocKey& key,
                                           const CookieIface* cookie,
                                           EventuallyPersistentEngine& engine,
                                           QueueBgFetch queueBgFetch,
                                           const StoredValue& v) {
    if (queueBgFetch == QueueBgFetch::Yes) {
        bgFetch(std::move(hbl), key, v, cookie, engine);
    }
    return GetValue(
            nullptr, cb::engine_errc::would_block, v.getBySeqno(), true);
}

void EPVBucket::setupDeferredDeletion(const CookieIface* cookie) {
    setDeferredDeletionCookie(cookie);
    auto revision = getShard()->getRWUnderlying()->prepareToDelete(getId());
    EP_LOG_INFO("EPVBucket::setupDeferredDeletion({}) {}, revision:{}",
                static_cast<const void*>(cookie),
                getId(),
                revision->getRevision());
    deferredDeletionFileRevision = std::move(revision);
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
            "PPS:{}, HPS:{}, number of prepares loaded:{}, outstandingPrepares "
            "seqnoRange:[{} -> {}]",
            getId(),
            vbs.persistedCompletedSeqno,
            vbs.persistedPreparedSeqno,
            vbs.highPreparedSeqno,
            outstandingPrepares.size(),
            !outstandingPrepares.empty()
                    ? outstandingPrepares.front()->getBySeqno()
                    : 0,
            !outstandingPrepares.empty()
                    ? outstandingPrepares.back()->getBySeqno()
                    : 0);

    // Second restore them into the appropriate DurabilityMonitor.
    switch (getState()) {
    case vbucket_state_active: {
        durabilityMonitor = std::make_unique<ActiveDurabilityMonitor>(
                stats,
                *this,
                vbs,
                syncWriteTimeoutFactory(*this),
                std::move(outstandingPrepares));

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

void EPVBucket::dropKey(const DocKey& key, int64_t bySeqno) {
    // dropKey must not generate expired items as it's used for erasing a
    // collection.

    auto res = ht.findForUpdate(key);
    if (res.committed && res.committed->getBySeqno() == bySeqno) {
        dropStoredValue(res.getHBL(), *res.committed);
    }
    if (res.pending && res.pending->getBySeqno() == bySeqno) {
        dropStoredValue(res.getHBL(), *res.pending.release());
    }
}

void EPVBucket::dropStoredValue(const HashTable::HashBucketLock& hbl,
                                StoredValue& value) {
    ht.unlocked_del(hbl, &value);
}

/*
 * Queue the item to the checkpoint and return the seqno the item was
 * allocated.
 */
uint64_t EPVBucket::addSystemEventItem(
        std::unique_ptr<Item> item,
        OptionalSeqno seqno,
        std::optional<CollectionID> cid,
        const Collections::VB::WriteHandle& wHandle,
        std::function<void(uint64_t)> assignedSeqnoCallback) {
    item->setVBucketId(getId());
    queued_item qi(item.release());

    // Set the system events delete time if needed for tombstoning
    if (qi->isDeleted() && qi->getDeleteTime() == 0) {
        // We can never purge the drop of the default collection because it has
        // an
        // implied creation event. If we did allow the default collection
        // tombstone to be purged a client would wrongly assume it exists.
        if (cid && cid.value().isDefaultCollection()) {
            qi->setExpTime(~0);
        } else {
            qi->setExpTime(ep_real_time());
        }
    }

    qi->setQueuedTime();

    checkpointManager->queueDirty(
            qi,
            getGenerateBySeqno(seqno),
            GenerateCas::Yes,
            nullptr /* No pre link step as this is for system events */,
            assignedSeqnoCallback);

    VBNotifyCtx notifyCtx;
    // If the seqno is initialized, skip replication notification
    notifyCtx.notifyReplication = !seqno.has_value();
    notifyCtx.notifyFlusher = true;
    notifyCtx.bySeqno = qi->getBySeqno();
    notifyNewSeqno(notifyCtx);

    // We don't record anything interesting for scopes
    if (cid) {
        doCollectionsStats(wHandle, *cid, notifyCtx);
        if (qi->isDeleted()) {
            stats.dropCollectionStats(*cid);

            // Inform the PDM about the dropped collection so that it knows
            // that it can skip any outstanding prepares until they are cleaned
            // up
            auto state = getState();
            if (state == vbucket_state_replica ||
                state == vbucket_state_pending) {
                getPassiveDM().notifyDroppedCollection(*cid, notifyCtx.bySeqno);
            }
        } else {
            stats.trackCollectionStats(*cid);
        }
    }
    Expects(qi->getBySeqno() >= 0);
    return uint64_t(qi->getBySeqno());
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

BgFetcher& EPVBucket::getBgFetcher() {
    return dynamic_cast<EPBucket&>(*bucket).getBgFetcher(getId());
}

std::function<void(int64_t)> EPVBucket::getSaveDroppedCollectionCallback(
        CollectionID cid,
        Collections::VB::WriteHandle& writeHandle,
        const Collections::VB::ManifestEntry& droppedEntry) const {
    // Return a function which will call back into the manifest to save the
    // collection and the seqno assigned to it
    return [&writeHandle, cid, &droppedEntry](uint64_t droppedSeqno) {
        writeHandle.saveDroppedCollection(cid, droppedEntry, droppedSeqno);
    };
}

void EPVBucket::postProcessRollback(const RollbackResult& rollbackResult,
                                    uint64_t prevHighSeqno,
                                    KVBucket& bucket) {
    const auto seqno = rollbackResult.highSeqno;
    failovers->pruneEntries(seqno);
    clearCMAndResetDiskQueueStats(seqno);
    setPersistedSnapshot(
            {rollbackResult.snapStartSeqno, rollbackResult.snapEndSeqno});
    incrRollbackItemCount(prevHighSeqno - seqno);
    setReceivingInitialDiskSnapshot(false);

    auto& kvstore = *bucket.getRWUnderlying(getId());
    setPersistenceSeqno(kvstore.getLastPersistedSeqno(getId()));

    // And update collections post rollback
    collectionsRolledBack(bucket);

    setNumTotalItems(kvstore);
}

void EPVBucket::collectionsRolledBack(KVBucket& bucket) {
    auto& kvstore = *bucket.getRWUnderlying(getId());
    auto [getManifestStatus, persistedManifest] =
            kvstore.getCollectionsManifest(getId());

    if (!getManifestStatus) {
        // Something bad happened that the KVStore ought to have logged.
        throw std::runtime_error(
                "EPVBucket::collectionsRolledBack: " + getId().to_string() +
                " failed to read the manifest from disk");
    }

    manifest = std::make_unique<Collections::VB::Manifest>(
            bucket.getSharedCollectionsManager(), persistedManifest);
    auto wh = manifest->wlock();
    // For each collection in the VB, reload the stats to the point before
    // the rollback seqno
    for (auto& collection : wh) {
        auto [status, stats] =
                kvstore.getCollectionStats(getId(), collection.first);
        if (status == KVStore::GetCollectionStatsStatus::Failed) {
            EP_LOG_WARN(
                    "EPVBucket::collectionsRolledBack(): getCollectionStats() "
                    "failed for {}",
                    getId());
        }
        collection.second.setItemCount(stats.itemCount);
        collection.second.setDiskSize(stats.diskSize);
        collection.second.resetPersistedHighSeqno(stats.highSeqno);
        collection.second.resetHighSeqno(
                collection.second.getPersistedHighSeqno());
    }
}

void EPVBucket::setNumTotalItems(KVStoreIface& kvstore) {
    const size_t itemCount = kvstore.getItemCount(getId());
    const auto* vbState = kvstore.getCachedVBucketState(getId());
    Expects(vbState);
    // We don't want to include the number of prepares on disk in the number
    // of items in the vBucket/Bucket that is displayed to the user so
    // subtract the number of prepares from the number of on disk items.
    const auto vbItemCount1 = itemCount - vbState->onDiskPrepares;
    const auto systemEvents = lockCollections().getSystemEventItemCount();
    const auto vbItemCount2 = vbItemCount1 - systemEvents;
    try {
        setNumTotalItems(vbItemCount2);
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL(
                "EPVBucket::setNumTotalItems {} caught exception itemCount:{} - "
                "onDiskPrepares:{} "
                " - systemEvents:{} = {} e.what:{}",
                getId(),
                itemCount,
                vbState->onDiskPrepares,
                systemEvents,
                vbItemCount2,
                e.what());
        throw e;
    }
}

void EPVBucket::notifyFlusher() {
    auto shard = getShard();
    if (shard) {
        auto ptr = shard->getBucket(getId());
        getFlusher()->notifyFlushEvent(ptr);
    }
}

Flusher* EPVBucket::getFlusher() {
    return dynamic_cast<EPBucket&>(*bucket).getFlusher(getId());
}

void EPVBucket::clearCMAndResetDiskQueueStats(uint64_t seqno) {
    checkpointManager->clear(seqno);

    const auto size = dirtyQueueSize.load();
    dirtyQueueSize.fetch_sub(size);
    stats.diskQueueSize.fetch_sub(size);
    dirtyQueueAge.store(0);
}

std::unique_ptr<KVStoreRevision> EPVBucket::takeDeferredDeletionFileRevision() {
    return std::move(deferredDeletionFileRevision);
}

cb::engine_errc EPVBucket::createRangeScan(CollectionID cid,
                                           cb::rangescan::KeyView start,
                                           cb::rangescan::KeyView end,
                                           RangeScanDataHandlerIFace& handler,
                                           const CookieIface& cookie,
                                           cb::rangescan::KeyOnly keyOnly) {
    return cb::engine_errc::not_supported; // @todo :)
}

cb::engine_errc EPVBucket::addNewRangeScan(std::shared_ptr<RangeScan> scan) {
    return rangeScans.addNewScan(std::move(scan));
}

cb::engine_errc EPVBucket::continueRangeScan(cb::rangescan::Id id) {
    return rangeScans.continueScan(id);
}
cb::engine_errc EPVBucket::cancelRangeScan(cb::rangescan::Id id) {
    return rangeScans.cancelScan(id);
}
