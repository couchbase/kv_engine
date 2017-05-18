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
#include "ep_engine.h"
#include "executorpool.h"
#include "failover-table.h"
#include "kvshard.h"
#include "stored_value_factories.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include "vbucketdeletiontask.h"

EPVBucket::EPVBucket(id_type i,
                     vbucket_state_t newState,
                     EPStats& st,
                     CheckpointConfig& chkConfig,
                     KVShard* kvshard,
                     int64_t lastSeqno,
                     uint64_t lastSnapStart,
                     uint64_t lastSnapEnd,
                     std::unique_ptr<FailoverTable> table,
                     std::shared_ptr<Callback<id_type> > flusherCb,
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
              flusherCb,
              std::make_unique<StoredValueFactory>(st),
              std::move(newSeqnoCb),
              config,
              evictionPolicy,
              initState,
              purgeSeqno,
              maxCas,
              collectionsManifest),
      multiBGFetchEnabled(kvshard
                                  ? kvshard->getROUnderlying()
                                            ->getStorageProperties()
                                            .hasEfficientGet()
                                  : false),
      shard(kvshard) {
}

EPVBucket::~EPVBucket() {
    if (!pendingBGFetches.empty()) {
        LOG(EXTENSION_LOG_WARNING,
            "Have %ld pending BG fetches while destroying vbucket\n",
            pendingBGFetches.size());
    }
}

ENGINE_ERROR_CODE EPVBucket::completeBGFetchForSingleItem(
        const DocKey& key,
        const VBucketBGFetchItem& fetched_item,
        const ProcessClock::time_point startTime) {
    ENGINE_ERROR_CODE status = fetched_item.value->getStatus();
    Item* fetchedValue = fetched_item.value->item.get();
    { // locking scope
        ReaderLockHolder rlh(getStateLock());
        auto hbl = ht.getLockedBucket(key);
        StoredValue* v = fetchValidValue(hbl,
                                         key,
                                         WantsDeleted::Yes,
                                         TrackReference::Yes,
                                         QueueExpired::Yes);

        if (fetched_item.metaDataOnly) {
            if (status == ENGINE_SUCCESS) {
                if (v && v->isTempInitialItem()) {
                    ht.unlocked_restoreMeta(hbl.getHTLock(), *fetchedValue, *v);
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
                case VALUE_ONLY:
                    if (v && !v->isResident()) {
                        restore = true;
                    }
                    break;
                case FULL_EVICTION:
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
                            hbl.getHTLock(), *fetchedValue, *v);
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
                    if (eviction == FULL_EVICTION) {
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
                    LOG(EXTENSION_LOG_WARNING,
                        "Failed background fetch for vb:%" PRIu16
                        ", seqno:%" PRIu64,
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

    updateBGStats(fetched_item.initTime, startTime, ProcessClock::now());
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
    if (eviction == VALUE_ONLY) {
        return ht.getNumInMemoryItems();
    } else {
        return ht.getNumItems();
    }
}

ENGINE_ERROR_CODE EPVBucket::statsVKey(const DocKey& key,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       const int bgFetchDelay) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = std::make_shared<VKeyStatBGFetchTask>(&engine,
                                                            key,
                                                            getId(),
                                                            v->getBySeqno(),
                                                            cookie,
                                                            bgFetchDelay,
                                                            false);
        iom->schedule(task);
        return ENGINE_EWOULDBLOCK;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            AddStatus rv = addTempStoredValue(hbl, key);
            switch (rv) {
            case AddStatus::NoMem:
                return ENGINE_ENOMEM;
            case AddStatus::Exists:
            case AddStatus::UnDel:
            case AddStatus::Success:
            case AddStatus::AddTmpAndBgFetch:
                // Since the hashtable bucket is locked, we shouldn't get here
                throw std::logic_error(
                        "VBucket::statsVKey: "
                        "Invalid result from unlocked_addTempItem (" +
                        std::to_string(static_cast<uint16_t>(rv)) + ")");

            case AddStatus::BgFetch: {
                ++stats.numRemainingBgJobs;
                ExecutorPool* iom = ExecutorPool::get();
                ExTask task = std::make_shared<VKeyStatBGFetchTask>(
                        &engine, key, getId(), -1, cookie, bgFetchDelay, false);
                iom->schedule(task);
            }
            }
            return ENGINE_EWOULDBLOCK;
        }
    }
}

void EPVBucket::completeStatsVKey(const DocKey& key, const GetValue& gcb) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes);

    if (v && v->isTempInitialItem()) {
        if (gcb.getStatus() == ENGINE_SUCCESS) {
            ht.unlocked_restoreValue(hbl.getHTLock(), *gcb.item, *v);
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
            LOG(EXTENSION_LOG_WARNING,
                "VBucket::completeStatsVKey: "
                "Failed background fetch for vb:%" PRIu16 ", seqno:%" PRIu64,
                getId(),
                v->getBySeqno());
        }
    }
}

void EPVBucket::addStats(bool details, ADD_STAT add_stat, const void* c) {
    _addStats(details, add_stat, c);

    if (details) {
        try {
            DBFileInfo fileInfo =
                    shard->getRWUnderlying()->getDbFileInfo(getId());
            addStat("db_data_size", fileInfo.spaceUsed, add_stat, c);
            addStat("db_file_size", fileInfo.fileSize, add_stat, c);
        } catch (std::runtime_error& e) {
            LOG(EXTENSION_LOG_WARNING,
                "VBucket::addStats: Exception caught during getDbFileInfo "
                "for vb:%" PRIu16 " - what(): %s",
                getId(),
                e.what());
        }
    }
}

protocol_binary_response_status EPVBucket::evictKey(const DocKey& key,
                                                    const char** msg) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(
            hbl, key, WantsDeleted::No, TrackReference::No, QueueExpired::Yes);

    if (!v) {
        if (eviction == VALUE_ONLY) {
            *msg = "Not found.";
            return PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
        *msg = "Already ejected.";
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }

    if (v->isResident()) {
        if (ht.unlocked_ejectItem(v, eviction)) {
            *msg = "Ejected.";

            // Add key to bloom filter in case of full eviction mode
            if (eviction == FULL_EVICTION) {
                addToFilter(key);
            }
            return PROTOCOL_BINARY_RESPONSE_SUCCESS;
        }
        *msg = "Can't eject: Dirty object.";
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    *msg = "Already ejected.";
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool EPVBucket::pageOut(const HashTable::HashBucketLock& lh, StoredValue*& v) {
    return ht.unlocked_ejectItem(v, eviction);
}

void EPVBucket::queueBackfillItem(queued_item& qi,
                                  const GenerateBySeqno generateBySeqno) {
    LockHolder lh(backfill.mutex);
    if (GenerateBySeqno::Yes == generateBySeqno) {
        qi->setBySeqno(checkpointManager.nextBySeqno());
    } else {
        checkpointManager.setBySeqno(qi->getBySeqno());
    }
    backfill.items.push(qi);
    ++stats.diskQueueSize;
    ++stats.vbBackfillQueueSize;
    ++stats.totalEnqueued;
    doStatsForQueueing(*qi, qi->size());
    stats.memOverhead->fetch_add(sizeof(queued_item));
}

size_t EPVBucket::queueBGFetchItem(const DocKey& key,
                                   std::unique_ptr<VBucketBGFetchItem> fetch,
                                   BgFetcher* bgFetcher) {
    LockHolder lh(pendingBGFetchesLock);
    vb_bgfetch_item_ctx_t& bgfetch_itm_ctx = pendingBGFetches[key];

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
    MutationStatus status;
    if (justTouch) {
        status = MutationStatus::WasDirty;
    } else {
        status = ht.unlocked_updateStoredValue(hbl.getHTLock(), v, itm);
    }

    return std::make_tuple(&v, status, queueDirty(v, queueItmCtx));
}

std::pair<StoredValue*, VBNotifyCtx> EPVBucket::addNewStoredValue(
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx& queueItmCtx) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);
    return {v, queueDirty(*v, queueItmCtx)};
}

std::tuple<StoredValue*, VBNotifyCtx> EPVBucket::softDeleteStoredValue(
        const HashTable::HashBucketLock& hbl,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    ht.unlocked_softDelete(hbl.getHTLock(), v, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        v.setBySeqno(bySeqno);
    }

    return std::make_tuple(&v, queueDirty(v, queueItmCtx));
}

void EPVBucket::bgFetch(const DocKey& key,
                        const void* cookie,
                        EventuallyPersistentEngine& engine,
                        const int bgFetchDelay,
                        const bool isMeta) {
    if (multiBGFetchEnabled) {
        // schedule to the current batch of background fetch of the given
        // vbucket
        size_t bgfetch_size = queueBGFetchItem(
                key,
                std::make_unique<VBucketBGFetchItem>(cookie, isMeta),
                getShard()->getBgFetcher());
        if (getShard()) {
            getShard()->getBgFetcher()->notifyBGEvent();
        }
        LOG(EXTENSION_LOG_DEBUG,
            "Queued a background fetch, now at %" PRIu64,
            uint64_t(bgfetch_size));
    } else {
        ++stats.numRemainingBgJobs;
        stats.maxRemainingBgJobs.store(
                std::max(stats.maxRemainingBgJobs.load(),
                         stats.numRemainingBgJobs.load()));
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = std::make_shared<SingleBGFetcherTask>(
                &engine, key, getId(), cookie, isMeta, bgFetchDelay, false);
        iom->schedule(task);
        LOG(EXTENSION_LOG_DEBUG,
            "Queued a background fetch, now at %" PRIu64,
            uint64_t(stats.numRemainingBgJobs.load()));
    }
}

/* [TBD]: Get rid of std::unique_lock<std::mutex> lock */
ENGINE_ERROR_CODE
EPVBucket::addTempItemAndBGFetch(HashTable::HashBucketLock& hbl,
                                 const DocKey& key,
                                 const void* cookie,
                                 EventuallyPersistentEngine& engine,
                                 int bgFetchDelay,
                                 bool metadataOnly,
                                 bool isReplication) {
    AddStatus rv = addTempStoredValue(hbl, key, isReplication);
    switch (rv) {
    case AddStatus::NoMem:
        return ENGINE_ENOMEM;

    case AddStatus::Exists:
    case AddStatus::UnDel:
    case AddStatus::Success:
    case AddStatus::AddTmpAndBgFetch:
        // Since the hashtable bucket is locked, we shouldn't get here
        throw std::logic_error(
                "VBucket::addTempItemAndBGFetch: "
                "Invalid result from addTempItem: " +
                std::to_string(static_cast<uint16_t>(rv)));

    case AddStatus::BgFetch:
        hbl.getHTLock().unlock();
        bgFetch(key, cookie, engine, bgFetchDelay, metadataOnly);
    }
    return ENGINE_EWOULDBLOCK;
}

void EPVBucket::updateBGStats(const ProcessClock::time_point init,
                              const ProcessClock::time_point start,
                              const ProcessClock::time_point stop) {
    ++stats.bgNumOperations;
    hrtime_t waitNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(start - init)
                    .count();
    hrtime_t w = waitNs / 1000;
    BlockTimer::log(waitNs, "bgwait", stats.timingLog);
    stats.bgWaitHisto.add(w);
    stats.bgWait.fetch_add(w);
    atomic_setIfLess(stats.bgMinWait, w);
    atomic_setIfBigger(stats.bgMaxWait, w);

    hrtime_t lNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start)
                    .count();
    hrtime_t l = lNs / 1000;
    BlockTimer::log(lNs, "bgload", stats.timingLog);
    stats.bgLoadHisto.add(l);
    stats.bgLoad.fetch_add(l);
    atomic_setIfLess(stats.bgMinLoad, l);
    atomic_setIfBigger(stats.bgMaxLoad, l);
}

GetValue EPVBucket::getInternalNonResident(const DocKey& key,
                                           const void* cookie,
                                           EventuallyPersistentEngine& engine,
                                           int bgFetchDelay,
                                           get_options_t options,
                                           const StoredValue& v) {
    if (options & QUEUE_BG_FETCH) {
        bgFetch(key, cookie, engine, bgFetchDelay);
    } else if (options & get_options_t::ALLOW_META_ONLY) {
        // You can't both ask for a background fetch and just the meta...
        return GetValue(v.toItem(false, 0),
                        ENGINE_SUCCESS,
                        v.getBySeqno(),
                        true,
                        v.getNRUValue());
    }

    return GetValue(
            nullptr, ENGINE_EWOULDBLOCK, v.getBySeqno(), true, v.getNRUValue());
}

void EPVBucket::setupDeferredDeletion(const void* cookie) {
    setDeferredDeletionCookie(cookie);
    deferredDeletionFileRevision.store(
            getShard()->getRWUnderlying()->prepareToDelete(getId()));
    setDeferredDeletion(true);
}

void EPVBucket::scheduleDeferredDeletion(EventuallyPersistentEngine& engine) {
    ExTask task = std::make_shared<VBucketMemoryAndDiskDeletionTask>(
            engine, *shard, this);
    ExecutorPool::get()->schedule(task);
}

MutationStatus EPVBucket::insertFromWarmup(Item& itm,
                                         bool eject,
                                         bool keyMetaDataOnly) {
    if (!StoredValue::hasAvailableSpace(stats, itm)) {
        return MutationStatus::NoMem;
    }

    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);

    if (v == NULL) {
        v = ht.unlocked_addNewStoredValue(hbl, itm);
        if (keyMetaDataOnly) {
            v->markNotResident();
            ++(ht.numNonResidentItems);
        }
        /* We need to decrNumTotalItems because ht.numTotalItems is already
         set by warmup when it estimated the item count from disk */
        ht.decrNumTotalItems();
        v->setNewCacheItem(false);
    } else {
        if (keyMetaDataOnly) {
            // We don't have a better error code ;)
            return MutationStatus::InvalidCas;
        }

        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->setCas(itm.getCas());
                v->setFlags(itm.getFlags());
                v->setExptime(itm.getExptime());
                v->setRevSeqno(itm.getRevSeqno());
            } else {
                return MutationStatus::InvalidCas;
            }
        }
        ht.unlocked_updateStoredValue(hbl.getHTLock(), *v, itm);
    }

    v->markClean();

    if (eject && !keyMetaDataOnly) {
        ht.unlocked_ejectItem(v, eviction);
    }

    return MutationStatus::NotFound;
}
