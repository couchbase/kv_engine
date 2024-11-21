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
#include "range_scans/range_scan_callbacks.h"
#include "range_scans/range_scan_continue_task.h"
#include "range_scans/range_scan_create_task.h"
#include "range_scans/range_scan_types.h"
#include "stored_value_factories.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_state.h"
#include "vbucketdeletiontask.h"
#include <boost/uuid/uuid_io.hpp>
#include <executor/executorpool.h>
#include <folly/lang/Assume.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/cookie_iface.h>
#include <memcached/range_scan_optional_configuration.h>
#include <platform/histogram.h>
#include <statistics/collector.h>
#include <utilities/logtags.h>

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
                     uint64_t maxVisibleSeqno,
                     uint64_t maxPrepareSeqno)
    : VBucket(i,
              newState,
              st,
              chkConfig,
              lastSeqno,
              lastSnapStart,
              lastSnapEnd,
              std::move(table),
              flusherCb,
              std::make_unique<StoredValueFactory>(),
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
              maxVisibleSeqno,
              maxPrepareSeqno),
      shard(kvshard),
      rangeScans(static_cast<EPBucket*>(bucket), *this) {
    const std::string backend = config.getBackendString();
    if (config.isBfilterEnabled()) {
        if (backend == "couchdb" ||
            (backend == "nexus" &&
             config.getNexusPrimaryBackendString() == "couchdb")) {
            // Initialize bloom filters upon vbucket creation during
            // bucket creation and rebalance
            createFilter(config.getBfilterKeyCount(),
                         config.getBfilterFpProb());
        }
        setKvStoreBfilterEnabled();
    }
}

EPVBucket::~EPVBucket() {
    // Clear out the bloomfilter(s)
    // clearFilter();
    auto size = pendingBGFetches.lock()->size();
    if (size > 0) {
        EP_LOG_WARN("Have {} pending BG fetches while destroying vbucket",
                    size);
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
                auto* traceable = fetched_item.cookie;
                if (traceable) {
                    NonBucketAllocationGuard guard;
                    auto& tracer = traceable->getTracer();
                    tracer.record(cb::tracing::Code::BackgroundWait,
                                  fetched_item.initTime,
                                  startTime);
                    tracer.record(cb::tracing::Code::BackgroundLoad,
                                  startTime,
                                  fetchEnd);
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
        std::shared_lock rlh(getStateLock());
        auto cHandle = lockCollections(docKey);
        auto res = fetchValidValue(rlh,
                                   WantsDeleted::Yes,
                                   TrackReference::Yes,
                                   cHandle,
                                   getState() == vbucket_state_replica
                                           ? ForGetReplicaOp::Yes
                                           : ForGetReplicaOp::No);
        auto* v = res.storedValue;

        if (!v) {
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
                    ht.unlocked_restoreMeta(res.lock, *fetchedValue, *v);
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
                    ht.unlocked_restoreValue(res.lock, *fetchedValue, *v);
                    if (!v->isResident()) {
                        std::stringstream ss;
                        ss << *v;
                        throw std::logic_error(fmt::format(
                                "VBucket::completeBGFetchForSingleItem: "
                                "storedvalue (which has seqno {}) should be"
                                "resident after calling restoreValue(). v:{}",
                                std::to_string(v->getBySeqno()),
                                cb::UserDataView(ss.str())));
                    }
                } else if (status == cb::engine_errc::no_such_key) {
                    if (!v->isTempItem()) {
                        std::stringstream ss;
                        ss << *v;
                        throw std::logic_error(fmt::format(
                                "({}) VBucket::completeBGFetchForSingleItem: "
                                "non-temp non-resident StoredValue should "
                                "always exist on disk, but doesn't. Will not"
                                "change non-temp item to temp non-existent. "
                                "v:{}",
                                getId(),
                                cb::UserDataView(ss.str())));
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
                    EP_LOG_WARN_CTX("Failed background fetch",
                                    {"vb", getId()},
                                    {"seqno", v->getBySeqno()},
                                    {"status", status});
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
    stats.bg_fetched_compaction++;
    if (status != cb::engine_errc::success) {
        return;
    }

    Item* fetchedValue = fetchedItem.value->item.get();
    { // locking scope
        auto docKey = key.getDocKey();
        std::shared_lock rlh(getStateLock());
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

        // Only continue if this item is a temp in the initial state.
        if (!htRes.committed->isTempInitialItem()) {
            return;
        }

        // Not our temp-initial item
        if (htRes.committed->getCas() != fetchedItem.token) {
            return;
        }

        // Check the cas of our BGFetched item against the cas of the item
        // we originally saw during our compaction (stashed in the
        // CompactionBGFetchItem object). If they are different then the
        // item has been superseded by a new one and we don't need to do
        // anything. Otherwise, expire the item.
        if (fetchedValue->getCas() != fetchedItem.compactionItem.getCas()) {
            ht.unlocked_del(htRes.getHBL(), *htRes.committed);
            return;
        }

        // @TODO perf: Investigate if it is necessary to add this to the
        //  HashTable
        ht.unlocked_updateStoredValue(
                htRes.getHBL(), *htRes.committed, fetchedItem.compactionItem);
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
    pendingBGFetches.lock()->swap(fetches);
    return fetches;
}

bool EPVBucket::hasPendingBGFetchItems() {
    return !pendingBGFetches.lock()->empty();
}

HighPriorityVBReqStatus EPVBucket::checkAddHighPriorityVBEntry(
        std::unique_ptr<SeqnoPersistenceRequest> request) {
    Expects(request);
    if (request->seqno <= getPersistenceSeqno()) {
        return HighPriorityVBReqStatus::RequestNotScheduled;
    }

    bucket->addVbucketWithSeqnoPersistenceRequest(
            getId(), addHighPriorityVBEntry(std::move(request)));

    return HighPriorityVBReqStatus::RequestScheduled;
}

void EPVBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) {
    auto toNotify = tmpFailAndGetAllHpNotifies(e);

    // Add all the pendingBGFetches to the toNotify map
    {
        auto lockedQueue = pendingBGFetches.lock();
        size_t num_of_deleted_pending_fetches = 0;
        for (auto& [_, ctx] : *lockedQueue) {
            for (auto& bgitem : ctx.getRequests()) {
                bgitem->abort(e, cb::engine_errc::not_my_vbucket, toNotify);
                ++num_of_deleted_pending_fetches;
            }
        }
        stats.numRemainingBgItems.fetch_sub(num_of_deleted_pending_fetches);
        lockedQueue->clear();
    }

    for (auto& [cookie, status] : toNotify) {
        e.notifyIOComplete(cookie, status);
    }

    fireAllOps(e);
}

size_t EPVBucket::getNumItems() const {
    if (eviction == EvictionPolicy::Value) {
        return ht.getNumInMemoryItems() -
               (ht.getNumDeletedItems() + ht.getNumSystemItems() +
                ht.getNumPreparedSyncWrites());
    }
    // onDiskTotalItems includes everything not deleted. It does not include
    // prepared SyncWrites so just return it.
    return onDiskTotalItems;
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
    }
    size_t num_items = onDiskTotalItems;
    size_t num_res_items =
            ht.getNumInMemoryItems() - ht.getNumInMemoryNonResItems();
    return num_items > num_res_items ? (num_items - num_res_items) : 0;
}

size_t EPVBucket::getNumSystemItems() const {
    // @todo: MB-26334 need to track system counts for persistent buckets
    return 0;
}

uint64_t EPVBucket::getHistoryDiskSize() {
    if (isBucketCreation()) {
        // If creation is true then no disk file exists
        return 0;
    }
    try {
        auto& kvstore = *shard->getRWUnderlying();
        // Avoid calling getDbFileInfo when backend does not support history.
        // Especially important for Couchstore, as the call goes to disk.
        if (kvstore.getStorageProperties().canRetainHistory()) {
            return kvstore.getDbFileInfo(getId()).historyDiskSize;
        }
        return 0;
    } catch (std::runtime_error& error) {
        EP_LOG_WARN(
                "EPVBucket::getHistoryDiskSize: Exception caught during "
                "getDbFileInfo "
                "for {} - what(): {}",
                getId(),
                error.what());
        return 0;
    }
}

cb::engine_errc EPVBucket::statsVKey(const DocKeyView& key,
                                     CookieIface& cookie,
                                     EventuallyPersistentEngine& engine) {
    std::shared_lock rlh(getStateLock());
    auto readHandle = lockCollections(key);
    if (!readHandle.valid()) {
        return cb::engine_errc::unknown_collection;
    }

    auto res = fetchValidValue(
            rlh, WantsDeleted::Yes, TrackReference::Yes, readHandle);

    auto* v = res.storedValue;
    if (v) {
        if (VBucket::isLogicallyNonExistent(*v, readHandle)) {
            ht.cleanupIfTemporaryItem(res.lock, *v);
            return cb::engine_errc::no_such_key;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = std::make_shared<VKeyStatBGFetchTask>(
                engine, key, getId(), v->getBySeqno(), cookie, false);
        iom->schedule(task);
        return cb::engine_errc::would_block;
    }
    if (eviction == EvictionPolicy::Value) {
        return cb::engine_errc::no_such_key;
    }
    auto rv = addTempStoredValue(res.lock, key, EnforceMemCheck::Yes);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return cb::engine_errc::no_memory;
    case TempAddStatus::BgFetch: {
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = std::make_shared<VKeyStatBGFetchTask>(
                engine, key, getId(), -1, cookie, false);
        iom->schedule(task);
        return cb::engine_errc::would_block;
    }
    }
    folly::assume_unreachable();
}

void EPVBucket::completeStatsVKey(const DocKeyView& key, const GetValue& gcb) {
    std::shared_lock rlh(getStateLock());

    auto cHandle = lockCollections(key);
    auto res = fetchValidValue(
            rlh, WantsDeleted::Yes, TrackReference::Yes, cHandle);

    auto* v = res.storedValue;
    if (v && v->isTempInitialItem()) {
        if (gcb.getStatus() == cb::engine_errc::success) {
            ht.unlocked_restoreValue(res.lock, *gcb.item, *v);
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
                         CookieIface& c) {
    _addStats(detail, add_stat, c);

    if (detail == VBucketStatsDetailLevel::Full) {
        DBFileInfo fileInfo;
        std::optional<uint64_t> historyStartSeqno;

        // Only try to read disk if we believe the file has been created
        if (!isBucketCreation()) {
            try {
                fileInfo = shard->getRWUnderlying()->getDbFileInfo(getId());
                historyStartSeqno =
                        shard->getRWUnderlying()->getHistoryStartSeqno(getId());
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

        if (historyStartSeqno) {
            addStat("history_start_seqno",
                    historyStartSeqno.value(),
                    add_stat,
                    c);
        }
        addStat("history_items_flushed",
                historicalItemsFlushed.load(),
                add_stat,
                c);
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
        EventuallyPersistentEngine& e, std::shared_ptr<ActiveStream> stream) {
    /* create a DCPBackfillByIdDisk object */
    return std::make_unique<DCPBackfillByIdDisk>(*e.getKVBucket(),
                                                 std::move(stream));
}

cb::engine_errc EPVBucket::evictKey(
        const char** msg,
        VBucketStateLockRef vbStateLock,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto res = fetchValidValue(
            vbStateLock, WantsDeleted::No, TrackReference::No, cHandle);
    auto* v = res.storedValue;
    if (!v) {
        if (eviction == EvictionPolicy::Value) {
            *msg = "Not found.";
            return cb::engine_errc::no_such_key;
        }
        *msg = "Already ejected.";
        return cb::engine_errc::success;
    }

    if (v->isResident()) {
        if (ht.unlocked_ejectItem(res.lock, v, eviction)) {
            *msg = "Ejected.";

            // Add key to bloom filter in case of full eviction mode
            if (eviction == EvictionPolicy::Full) {
                addToFilter(cHandle.getKey());
            }
            return cb::engine_errc::success;
        }
        *msg = "Can't eject: Dirty object.";
        return cb::engine_errc::key_already_exists;
    }

    *msg = "Already ejected.";
    return cb::engine_errc::success;
}

bool EPVBucket::pageOut(VBucketStateLockRef,
                        const Collections::VB::ReadHandle& readHandle,
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

bool EPVBucket::canEvict() const {
    // epvbs have no vbucket state-dependent restrictions on eviction.
    // Eligibility for eviction can be determined purely from the properties
    // of the stored value.
    return true;
}

bool EPVBucket::isEligibleForEviction(const HashTable::HashBucketLock& lh,
                                      const StoredValue& v) const {
    // TODO: move the below eligibility check out of StoredValue.
    //       the result is only accurate for SVs in an EPVBucket
    return v.eligibleForEviction(eviction);
}

size_t EPVBucket::getPageableMemUsage() {
    if (eviction == EvictionPolicy::Full) {
        return ht.getItemMemory();
    }
    return ht.getItemMemory() - ht.getMetadataMemory();
}

size_t EPVBucket::queueBGFetchItem(const DocKeyView& key,
                                   std::unique_ptr<BGFetchItem> fetch,
                                   BgFetcher& bgFetcher) {
    // While a DiskDocKey supports both the committed and prepared namespaces,
    // ep-engine doesn't support evicting prepared SyncWrites and as such
    // we don't allow bgfetching from Prepared namespace - so just construct
    // DiskDocKey with pending unconditionally false.
    DiskDocKey diskKey{key, /*pending*/ false};
    auto lockedQueue = pendingBGFetches.lock();
    (*lockedQueue)[diskKey].addBgFetch(std::move(fetch));
    bgFetcher.addPendingVB(getId());
    return lockedQueue->size();
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
    }
    folly::assume_unreachable();
}

VBNotifyCtx EPVBucket::commitStoredValue(HashTable::FindUpdateResult& values,
                                         uint64_t prepareSeqno,
                                         const VBQueueItemCtx& queueItmCtx,
                                         std::optional<int64_t> commitSeqno) {
    // Remove a previously committed SV if one exists
    if (values.committed) {
        // Only delete the existing committed item
        ht.unlocked_del(values.pending.getHBL(), *values.committed);
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

VBNotifyCtx EPVBucket::abortStoredValue(
        const HashTable::HashBucketLock& hbl,
        StoredValue& v,
        int64_t prepareSeqno,
        std::optional<int64_t> abortSeqno,
        const Collections::VB::CachingReadHandle& cHandle) {
    // Note: We have to enqueue the item into the CM /before/ removing it from
    //     the HT as the removal is synchronous and deallocates the StoredValue
    VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
    if (abortSeqno) {
        queueItmCtx.genBySeqno = GenerateBySeqno::No;
        v.setBySeqno(*abortSeqno);
    }
    auto notify = queueAbort(hbl, v, prepareSeqno, queueItmCtx);

    ht.unlocked_del(hbl, v);

    return notify;
}

VBNotifyCtx EPVBucket::addNewAbort(
        const HashTable::HashBucketLock& hbl,
        const DocKeyView& key,
        int64_t prepareSeqno,
        int64_t abortSeqno,
        const Collections::VB::CachingReadHandle& cHandle) {
    VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
    queueItmCtx.genBySeqno = GenerateBySeqno::No;
    queued_item item = createNewAbortedItem(key, prepareSeqno, abortSeqno);
    return queueAbortForUnseenPrepare(item, queueItmCtx);
}

cb::engine_errc EPVBucket::bgFetch(HashTable::HashBucketLock&& hbl,
                                   const DocKeyView& key,
                                   const StoredValue& v,
                                   CookieIface* cookie,
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

    size_t requiredMemory =
            StoredValue::getRequiredStorage(key) + sizeof(FrontEndBGFetchItem);
    auto status = engine.checkMemoryForBGFetch(requiredMemory);
    if (status != cb::engine_errc::success) {
        return status;
    }

    const auto filter =
            isMeta ? ValueFilter::KEYS_ONLY
                   : dynamic_cast<EPBucket&>(*bucket)
                             .getValueFilterForCompressionMode(cookie);
    size_t bgfetch_size = queueBGFetchItem(
            key,
            std::make_unique<FrontEndBGFetchItem>(cookie, filter, token),
            getBgFetcher(token));
    EP_LOG_DEBUG("Queued a background fetch, now at {}",
                 uint64_t(bgfetch_size));
    return cb::engine_errc::would_block;
}

/* [TBD]: Get rid of std::unique_lock<std::mutex> lock */
cb::engine_errc EPVBucket::addTempItemAndBGFetch(
        HashTable::HashBucketLock&& hbl,
        const DocKeyView& key,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        bool metadataOnly) {
    auto rv = addTempStoredValue(hbl, key, EnforceMemCheck::Yes);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return cb::engine_errc::no_memory;
    case TempAddStatus::BgFetch:
        return bgFetch(std::move(hbl),
                       key,
                       *rv.storedValue,
                       cookie,
                       engine,
                       metadataOnly);
    }
    folly::assume_unreachable();
}

std::unique_ptr<CompactionBGFetchItem>
EPVBucket::createBgFetchForCompactionExpiry(
        const HashTable::HashBucketLock& hbl,
        const DocKeyView& key,
        const Item& item) {
    auto rv = addTempStoredValue(hbl, key, EnforceMemCheck::Yes);
    switch (rv.status) {
    case TempAddStatus::NoMem:
        return nullptr;
    case TempAddStatus::BgFetch:
        auto token = rv.storedValue->getCas();
        return std::make_unique<CompactionBGFetchItem>(item, token);
    }
    folly::assume_unreachable();
}

void EPVBucket::bgFetchForCompactionExpiry(HashTable::HashBucketLock& hbl,
                                           const DocKeyView& key,
                                           const Item& item) {
    auto bgFetchItem = createBgFetchForCompactionExpiry(hbl, key, item);
    if (!bgFetchItem) {
        return;
    }

    hbl.getHTLock().unlock();
    // add to the current batch of background fetch of the given vbucket
    auto& bgFetcher = getBgFetcher(bgFetchItem->token);
    auto bgFetchSize = queueBGFetchItem(key, std::move(bgFetchItem), bgFetcher);
    EP_LOG_DEBUG("Queue a background fetch for compaction expiry, now at {}",
                 bgFetchSize);
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
                                           const DocKeyView& key,
                                           CookieIface* cookie,
                                           EventuallyPersistentEngine& engine,
                                           QueueBgFetch queueBgFetch,
                                           const StoredValue& v) {
    if (queueBgFetch == QueueBgFetch::Yes) {
        cb::engine_errc ec = bgFetch(std::move(hbl), key, v, cookie, engine);
        return GetValue(nullptr, ec, v.getBySeqno(), true);
    }
    return GetValue(
            nullptr, cb::engine_errc::would_block, v.getBySeqno(), true);
}

void EPVBucket::setupDeferredDeletion(CookieIface* cookie) {
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
    if (checkMemUsed && !hasMemoryForStoredValue(itm)) {
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

size_t EPVBucket::estimateRequiredMemory(const Item& item) {
    return StoredValue::getRequiredStorage(item.getKey());
}

size_t EPVBucket::getNumPersistedDeletes() const {
    if (isBucketCreation()) {
        // If creation is true then no disk file exists
        return 0;
    }
    return shard->getRWUnderlying()->getNumPersistedDeletes(getId());
}

void EPVBucket::dropKey(const DocKeyView& key, int64_t bySeqno) {
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
    ht.unlocked_del(hbl, value);
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

    // System events must also respect history, e.g. sequences of maxTTL changes
    // should be preserved.
    if (bucket && bucket->isHistoryRetentionEnabled() && cid) {
        qi->setCanDeduplicate(wHandle.getCanDeduplicate(cid.value()));
    }

    checkpointManager->queueDirty(
            qi,
            getGenerateBySeqno(seqno),
            GenerateCas::Yes,
            nullptr /* No pre link step as this is for system events */,
            assignedSeqnoCallback);

    // Note: If the seqno is already initialized, skip replication notification
    VBNotifyCtx notifyCtx(
            qi->getBySeqno(), !seqno.has_value(), true, qi->getOperation());
    notifyNewSeqno(notifyCtx);

    // We don't record anything interesting for scopes
    if (cid) {
        wHandle.setHighSeqno(cid.value(),
                             qi->getBySeqno(),
                             Collections::VB::HighSeqnoType::SystemEvent);
        if (qi->isDeleted()) {
            stats.dropCollectionStats(*cid);

            // Inform the PDM about the dropped collection so that it knows
            // that it can skip any outstanding prepares until they are cleaned
            // up
            auto state = getState();
            if (state == vbucket_state_replica ||
                state == vbucket_state_pending) {
                getPassiveDM().notifyDroppedCollection(*cid,
                                                       notifyCtx.getSeqno());
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
    ht.unlocked_del(v.getHBL(), *v.release());
}

BgFetcher& EPVBucket::getBgFetcher(uint32_t distributionKey) {
    return dynamic_cast<EPBucket&>(*bucket).getBgFetcher(getId(),
                                                         distributionKey);
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

void EPVBucket::postProcessRollback(VBucketStateLockRef vbStateLock,
                                    const RollbackResult& rollbackResult,
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
    collectionsRolledBack(vbStateLock, bucket);

    setNumTotalItems(kvstore);
}

void EPVBucket::collectionsRolledBack(VBucketStateLockRef vbStateLock,
                                      KVBucket& bucket) {
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
    auto wh = manifest->wlock(vbStateLock);
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

static size_t countSystemEvents(Vbid vbid, KVStoreIface& kvstore) {
    auto start = StoredDocKey{{}, CollectionID::SystemEvent};
    auto end = StoredDocKey{"\xff", CollectionID::SystemEvent};
    size_t count{0};
    kvstore.getRange(vbid,
                     DiskDocKey{start},
                     DiskDocKey{end},
                     ValueFilter::KEYS_ONLY,
                     [&count](GetValue&&) { ++count; });
    return count;
}

void EPVBucket::setNumTotalItems(KVStoreIface& kvstore) {
    const size_t systemEvents = countSystemEvents(getId(), kvstore);
    const size_t totalItemCount = kvstore.getItemCount(getId());
    const auto* vbState = kvstore.getCachedVBucketState(getId());
    Expects(vbState);
    // We don't want to include the number of prepares on disk in the number
    // of items in the vBucket/Bucket that is displayed to the user so
    // subtract the number of prepares from the number of on disk items.
    // The same applies to SysEvents.
    const auto itemCount =
            totalItemCount - vbState->onDiskPrepares - systemEvents;
    try {
        setNumTotalItems(itemCount);
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL_CTX("EPVBucket::setNumTotalItems: ",
                            {"vb", getId().get()},
                            {"total_item_count", totalItemCount},
                            {"on_disk_prepares", vbState->onDiskPrepares},
                            {"system_events", systemEvents},
                            {"itemCount", itemCount},
                            {"error:", e.what()});
        throw;
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
    stats.getCoreLocalDiskQueueSize().fetch_sub(size);
    dirtyQueueAge.store(0);
}

std::unique_ptr<KVStoreRevision> EPVBucket::takeDeferredDeletionFileRevision() {
    return std::move(deferredDeletionFileRevision);
}

std::pair<cb::engine_errc, cb::rangescan::Id> EPVBucket::createRangeScan(
        CookieIface& cookie,
        std::unique_ptr<RangeScanDataHandlerIFace> handler,
        const cb::rangescan::CreateParameters& params) {
    // Obtain the engine specific, which will be null (new create) or a pointer
    // to RangeScanCreateToken (I/O complete path of create)
    std::unique_ptr<RangeScanCreateToken> rangeScanCreateToken(
            bucket->getEPEngine()
                    .getEngineSpecific<RangeScanCreateToken*>(cookie)
                    .value_or(nullptr));

    if (rangeScanCreateToken) {
        // When the data exists, two paths are possible.
        // 1) I/O complete from RangeScanCreateTask
        // 2) I/O complete from SeqnoPersistenceRequest
        // The state variable determines what todo next.
        if (rangeScanCreateToken->state == RangeScanCreateState::Creating) {
            // create state - command is now completed
            return createRangeScanComplete(std::move(rangeScanCreateToken),
                                           cookie);
        }
    } else if (isBucketCreation()) {
        // Scan create is racing with vbucket creation
        return {cb::engine_errc::temporary_failure, {}};
    } else {

        // Create our RangeScanCreateToken, the state will now be Pending
        rangeScanCreateToken = std::make_unique<RangeScanCreateToken>();
        // Place pointer in the cookie so we can get this object back on success
        bucket->getEPEngine().storeEngineSpecific(cookie,
                                                  rangeScanCreateToken.get());
    }

    // Check for seqno persistence and the state. If the create is pending
    // and the seqno is not persisted, wait if there's a timeout, else fail
    if (params.snapshotReqs &&
        getPersistenceSeqno() < params.snapshotReqs->seqno &&
        rangeScanCreateToken->state == RangeScanCreateState::Pending) {
        if (params.snapshotReqs->timeout) {
            auto status = createRangeScanWait(*params.snapshotReqs, cookie);
            Expects(status != HighPriorityVBReqStatus::NotSupported);
            if (status == HighPriorityVBReqStatus::RequestScheduled) {
                rangeScanCreateToken->state =
                        RangeScanCreateState::WaitForPersistence;
                // release the data, it's now 'owned' by the cookie
                rangeScanCreateToken.release();
                // waiting for persistence...
                return {cb::engine_errc::would_block, {}};
            } // else the seqno is now persisted, we can continue to create
        } else {
            bucket->getEPEngine().clearEngineSpecific(cookie);
            // No timeout, fail command here. This is the same return code as
            // an expired SeqnoPersistenceRequest
            return {cb::engine_errc::temporary_failure, {}};
        }
    }

    // Set status to creation
    rangeScanCreateToken->state = RangeScanCreateState::Creating;

    // If no handler has been given, create one.
    if (!handler) {
        handler = std::make_unique<RangeScanDataHandler>(
                bucket->getEPEngine(),
                params.keyOnly == cb::rangescan::KeyOnly::Yes,
                params.includeXattrs == cb::rangescan::IncludeXattrs::Yes);
    }

    // At this point the scan can proceed to create, but we need to check and
    // reserve capacity for this scan.
    if (!bucket->getKVStoreScanTracker().canCreateAndReserveRangeScan()) {
        return {cb::engine_errc::too_busy, {}};
    }

    // Create a task and give it the RangeScanCreateToken, on failure the task
    // will destruct the data
    ExecutorPool::get()->schedule(std::make_shared<RangeScanCreateTask>(
            dynamic_cast<EPBucket&>(*bucket),
            cookie,
            std::move(handler),
            params,
            std::move(rangeScanCreateToken)));
    return {cb::engine_errc::would_block, {}};
}

std::pair<cb::engine_errc, cb::rangescan::Id>
EPVBucket::createRangeScanComplete(
        std::unique_ptr<RangeScanCreateToken> rangeScanCreateData,
        CookieIface& cookie) {
    Expects(rangeScanCreateData);
    bucket->getEPEngine().clearEngineSpecific(cookie);
    return {cb::engine_errc::success, rangeScanCreateData->uuid};
}

HighPriorityVBReqStatus EPVBucket::createRangeScanWait(
        const cb::rangescan::SnapshotRequirements& requirements,
        CookieIface& cookie) {
    struct RangeScanWaitForPersistenceRequest : public SeqnoPersistenceRequest {
        RangeScanWaitForPersistenceRequest(EventuallyPersistentEngine& engine,
                                           CookieIface* cookie,
                                           uint64_t seqno,
                                           std::chrono::milliseconds timeout)
            : SeqnoPersistenceRequest(cookie, seqno, timeout), engine(engine) {
        }

        // override with a function that cleans up
        void expired() const override {
            // Capture the unique_ptr and allow it to go out of scope
            std::unique_ptr<RangeScanCreateToken> rangeScanCreateToken(
                    engine.takeEngineSpecific<RangeScanCreateToken*>(*cookie)
                            .value_or(nullptr));
        }

        EventuallyPersistentEngine& engine;
    };

    return checkAddHighPriorityVBEntry(
            std::make_unique<RangeScanWaitForPersistenceRequest>(
                    bucket->getEPEngine(),
                    &cookie,
                    requirements.seqno,
                    requirements.timeout.value()));
}

cb::engine_errc EPVBucket::checkAndCancelRangeScanCreate(CookieIface& cookie) {
    // Obtain the data (so it now frees if not null)
    std::unique_ptr<RangeScanCreateToken> rangeScanCreateToken(
            bucket->getEPEngine()
                    .takeEngineSpecific<RangeScanCreateToken*>(cookie)
                    .value_or(nullptr));
    // Nothing to cancel
    if (!rangeScanCreateToken) {
        return cb::engine_errc::success;
    }

    if (rangeScanCreateToken->state == RangeScanCreateState::Creating) {
        return cancelRangeScan(rangeScanCreateToken->uuid, nullptr);
    }
    return cb::engine_errc::success;
}

std::shared_ptr<RangeScan> EPVBucket::getRangeScan(cb::rangescan::Id id) const {
    return rangeScans.getScan(id);
}

cb::engine_errc EPVBucket::addNewRangeScan(std::shared_ptr<RangeScan> scan) {
    return rangeScans.addNewScan(std::move(scan), *this, bucket->getEPEngine());
}

cb::engine_errc EPVBucket::setupCookieForRangeScan(cb::rangescan::Id id,
                                                   CookieIface& cookie) {
    CollectionID cid;
    {
        auto scan = rangeScans.getScan(id);
        if (!scan) {
            return cb::engine_errc::no_such_key;
        }
        cid = scan->getCollectionID();
    }

    auto [uid, entry] = bucket->getCollectionEntry(cid);
    if (!entry) {
        bucket->getEPEngine().setUnknownCollectionErrorContext(cookie, uid);
        return cb::engine_errc::unknown_collection;
    }
    cookie.setCurrentCollectionInfo(
            entry->sid,
            cid,
            uid,
            entry->metered == Collections::Metered::Yes,
            Collections::isSystemCollection(entry->name, cid));
    return cb::engine_errc::success;
}

cb::engine_errc EPVBucket::continueRangeScan(
        CookieIface& cookie, const cb::rangescan::ContinueParameters& params) {
    // Take the token (if any), engine specific is now null and this function
    // owns the token via a unique_ptr.
    auto continueToken =
            bucket->getEPEngine().takeEngineSpecific<RangeScanContinueToken>(
                    cookie);

    if (continueToken.has_value()) {
        // This is an I/O complete phase of the continue request. Validate that
        // the stashed UUID matches the input.
        Expects(continueToken->uuid == params.uuid);
    }

    auto status = setupCookieForRangeScan(params.uuid, cookie);
    if (status == cb::engine_errc::success) {
        status = rangeScans.checkPrivileges(
                params.uuid, cookie, bucket->getEPEngine());
    }

    if (status != cb::engine_errc::success) {
        // continue of a dropped collection detected, cancel the scan now as
        // this may sooner free resources than waiting for another path to
        // detect
        if (status == cb::engine_errc::unknown_collection) {
            EP_LOG_INFO(
                    "EPVBucket::continueRangeScan {} {} cancelling for a "
                    "dropped collection",
                    getId(),
                    params.uuid);
            // cancel the scan which requests an I/O task to close any resources
            rangeScans.cancelScan(dynamic_cast<EPBucket&>(*bucket),
                                  params.uuid);
        } else if (status == cb::engine_errc::no_such_key &&
                   continueToken.has_value()) {
            // hasPrivilege could not find the scan, yet this is an I/O complete
            // phase. This means the scan did exist and was cancelled, so return
            // range_scan_cancelled as the status
            status = cb::engine_errc::range_scan_cancelled;
        }

        return status;
    }

    auto continueState =
            rangeScans.continueScan(dynamic_cast<EPBucket&>(*bucket),
                                    cookie,
                                    continueToken.has_value(),
                                    params);

    if (continueState.first == cb::engine_errc::would_block) {
        // Stash a token and save the current UUID
        bucket->getEPEngine().storeEngineSpecific(
                cookie, RangeScanContinueToken{params.uuid});
    }

    if (continueState.second) {
        continueState.second->complete(cookie);
    }

    return continueState.first;
}

cb::engine_errc EPVBucket::cancelRangeScan(cb::rangescan::Id id,
                                           CookieIface* cookie) {
    cb::engine_errc status{cb::engine_errc::success};
    if (cookie) {
        status = rangeScans.checkPrivileges(id, *cookie, bucket->getEPEngine());
        if (status == cb::engine_errc::no_access ||
            status == cb::engine_errc::no_such_key) {
            return status;
        }
    }
    // hasPrivilege returns success, no_such_key, no_access, or
    // unknown_collection. no_access/no_such_key have been dealt with, so now
    // process success or unknown_collection
    Expects(status == cb::engine_errc::success ||
            status == cb::engine_errc::unknown_collection);

    // Cancel even for a dropped collection. Note cancelScan returns either
    // success or no_such_key. The latter can happen if the scan self cancels
    // by some other means (completes/timesout on another thread).
    const auto cancelStatus =
            rangeScans.cancelScan(dynamic_cast<EPBucket&>(*bucket), id);
    Expects(cancelStatus == cb::engine_errc::success ||
            cancelStatus == cb::engine_errc::no_such_key);

    if (cancelStatus == cb::engine_errc::success) {
        EP_LOG_DEBUG("{} RangeScan {} cancelled by request", getId(), id);
    }

    // The client doesn't wait for the task to run/complete
    return cancelStatus;
}

cb::engine_errc EPVBucket::doRangeScanStats(const StatCollector& collector) {
    collector.addStat(
            "num_running",
            bucket->getKVStoreScanTracker().getNumRunningRangeScans());
    collector.addStat(
            "max_running",
            bucket->getKVStoreScanTracker().getMaxRunningRangeScans());
    return rangeScans.doStats(collector);
}

std::optional<std::chrono::seconds>
EPVBucket::cancelRangeScansExceedingDuration(std::chrono::seconds duration) {
    return rangeScans.cancelAllExceedingDuration(
            dynamic_cast<EPBucket&>(*bucket), duration);
}

void EPVBucket::cancelRangeScans() {
    rangeScans.cancelAllScans(dynamic_cast<EPBucket&>(*bucket));
}

void EPVBucket::createFilter(size_t key_count, double probability) {
    // Create the actual bloom filter upon vbucket creation during
    // scenarios:
    //      - Bucket creation
    //      - Rebalance
    auto bFilterDataLocked = bFilterData.lock();

    if (bFilterDataLocked->bFilter == nullptr &&
        bFilterDataLocked->tempFilter == nullptr) {
        bFilterDataLocked->bFilter = std::make_unique<BloomFilter>(
                key_count, probability, BFILTER_ENABLED);
    } else {
        EP_LOG_WARN("({}) Bloom filter / Temp filter already exist!", id);
    }
}

void EPVBucket::setKvStoreBfilterEnabled() {
    bFilterData.lock()->kvStoreBfilterEnabled = true;
}

void EPVBucket::initTempFilter(size_t key_count, double probability) {
    // Create a temp bloom filter with status as COMPACTING,
    // if the main filter is found to exist, set its state to
    // COMPACTING as well.

    auto bFilterDataLocked = bFilterData.lock();
    bFilterDataLocked->tempFilter = std::make_unique<BloomFilter>(
            key_count, probability, BFILTER_COMPACTING);
    if (bFilterDataLocked->bFilter) {
        bFilterDataLocked->bFilter->setStatus(BFILTER_COMPACTING);
    }
}

void EPVBucket::addToFilter(const DocKeyView& key) {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        bFilterDataLocked->bFilter->addKey(key);
    }

    // If the temp bloom filter is not found to be nullptr,
    // it means that compaction is running on the particular
    // vbucket. Therefore add the key to the temp filter as
    // well, as once compaction completes the temp filter
    // will replace the main bloom filter.
    if (bFilterDataLocked->tempFilter) {
        bFilterDataLocked->tempFilter->addKey(key);
    }
}

bool EPVBucket::maybeKeyExistsInFilter(const DocKeyView& key) {
    enum class Status { KeyMayExist, KeyDoesNotExist, TryInKvStore };

    auto status = bFilterData.withLock([&key](const auto& locked) {
        if (locked.bFilter) {
            // kv-engine maintains a bloom-filter only for couchstore, check if
            // the key exists in that bloom-filter.
            return locked.bFilter->maybeKeyExists(key)
                           ? Status::KeyMayExist
                           : Status::KeyDoesNotExist;
        }
        if (locked.kvStoreBfilterEnabled) {
            return Status::TryInKvStore;
        }
        //  Force callers of this function to perform a bg-fetch task to
        //  check if the key exists in corresponding kvstore. Execution
        //  should never reach here for couchstore.
        return Status::KeyMayExist;
    });

    switch (status) {
    case Status::KeyMayExist:
        return true;
    case Status::KeyDoesNotExist:
        return false;
    case Status::TryInKvStore:
        //  - magma maintains its own bloom-filter, check in the key exists in
        //  that bloom-filter.
        //  - couchstore always returns true.
        return bucket->getROUnderlying(id)->keyMayExist(id, key);
    }
    folly::assume_unreachable();
}

bool EPVBucket::isTempFilterAvailable() {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->tempFilter &&
        (bFilterDataLocked->tempFilter->getStatus() == BFILTER_COMPACTING ||
         bFilterDataLocked->tempFilter->getStatus() == BFILTER_ENABLED)) {
        return true;
    }
    return false;
}

void EPVBucket::addToTempFilter(const DocKeyView& key) {
    // Keys will be added to only the temp filter during
    // compaction.
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->tempFilter) {
        bFilterDataLocked->tempFilter->addKey(key);
    }
}

void EPVBucket::swapFilter() {
    // Delete the main bloom filter and replace it with
    // the temp filter that was populated during compaction,
    // only if the temp filter's state is found to be either at
    // COMPACTING or ENABLED (if in the case the user enables
    // bloomfilters for some reason while compaction was running).
    // Otherwise, it indicates that the filter's state was
    // possibly disabled during compaction, therefore clear out
    // the temp filter. If it gets enabled at some point, a new
    // bloom filter will be made available after the next
    // compaction.

    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->tempFilter) {
        bFilterDataLocked->bFilter.reset();

        if (bFilterDataLocked->tempFilter->getStatus() == BFILTER_COMPACTING ||
            bFilterDataLocked->tempFilter->getStatus() == BFILTER_ENABLED) {
            bFilterDataLocked->bFilter = std::move(bFilterDataLocked->tempFilter);
            bFilterDataLocked->bFilter->setStatus(BFILTER_ENABLED);
        }
        bFilterDataLocked->tempFilter.reset();
    }
}

void EPVBucket::clearFilter() {
    auto bFilterDataLocked = bFilterData.lock();
    bFilterDataLocked->bFilter.reset();
    bFilterDataLocked->tempFilter.reset();
}

void EPVBucket::setFilterStatus(bfilter_status_t to) {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        bFilterDataLocked->bFilter->setStatus(to);
    }
    if (bFilterDataLocked->tempFilter) {
        bFilterDataLocked->tempFilter->setStatus(to);
    }
    bFilterDataLocked->kvStoreBfilterEnabled = (to == BFILTER_ENABLED);
}

std::string EPVBucket::getFilterStatusString() {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        return bFilterDataLocked->bFilter->getStatusString();
    }
    if (bFilterDataLocked->tempFilter) {
        return bFilterDataLocked->tempFilter->getStatusString();
    }
    return "DOESN'T EXIST";
}

size_t EPVBucket::getFilterSize() {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        return bFilterDataLocked->bFilter->getFilterSize();
    }
    return 0;
}

size_t EPVBucket::getNumOfKeysInFilter() {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        return bFilterDataLocked->bFilter->getNumOfKeysInFilter();
    }
    return 0;
}

size_t EPVBucket::getFilterMemoryFootprint() {
    auto bFilterDataLocked = bFilterData.lock();
    size_t memFootprint{0};
    if (bFilterDataLocked->bFilter) {
        memFootprint += bFilterDataLocked->bFilter->getMemoryFootprint();
    }
    if (bFilterDataLocked->tempFilter) {
        memFootprint += bFilterDataLocked->tempFilter->getMemoryFootprint();
    }
    return memFootprint;
}

void EPVBucket::addBloomFilterStats(const AddStatFn& add_stat, CookieIface& c) {
    auto bFilterDataLocked = bFilterData.lock();
    if (bFilterDataLocked->bFilter) {
        addBloomFilterStats_UNLOCKED(
                add_stat, c, *(bFilterDataLocked->bFilter));
    } else if (bFilterDataLocked->tempFilter) {
        addBloomFilterStats_UNLOCKED(
                add_stat, c, *(bFilterDataLocked->tempFilter));
    } else {
        addStat("bloom_filter",
                bFilterDataLocked->kvStoreBfilterEnabled
                        ? std::string("ENABLED")
                        : std::string("DISABLED"),
                add_stat,
                c);
        addStat("bloom_filter_size", static_cast<size_t>(0), add_stat, c);
        addStat("bloom_filter_key_count", static_cast<size_t>(0), add_stat, c);
        addStat("bloom_filter_memory", static_cast<size_t>(0), add_stat, c);
    }
}

void EPVBucket::addBloomFilterStats_UNLOCKED(const AddStatFn& add_stat,
                                             CookieIface& c,
                                             const BloomFilter& filter) {
    addStat("bloom_filter", filter.getStatusString(), add_stat, c);
    addStat("bloom_filter_size", filter.getFilterSize(), add_stat, c);
    addStat("bloom_filter_key_count",
            filter.getNumOfKeysInFilter(),
            add_stat,
            c);
    addStat("bloom_filter_memory", filter.getMemoryFootprint(), add_stat, c);
}