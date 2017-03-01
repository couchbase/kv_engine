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

/* Macros */
const size_t MIN_CHK_FLUSH_TIMEOUT = 10; // 10 sec.
const size_t MAX_CHK_FLUSH_TIMEOUT = 30; // 30 sec.

/* Statics definitions */
std::atomic<size_t> EPVBucket::chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT);

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
                     uint64_t maxCas)
    : VBucket(i,
              newState,
              st,
              chkConfig,
              lastSeqno,
              lastSnapStart,
              lastSnapEnd,
              std::move(table),
              flusherCb,
              std::move(newSeqnoCb),
              config,
              evictionPolicy,
              initState,
              purgeSeqno,
              maxCas),
      multiBGFetchEnabled(kvshard
                                  ? kvshard->getROUnderlying()
                                            ->getStorageProperties()
                                            .hasEfficientGet()
                                  : false),
      numHpChks(0),
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
    ENGINE_ERROR_CODE status = fetched_item.value.getStatus();
    Item* fetchedValue = fetched_item.value.getValue();
    { // locking scope
        ReaderLockHolder rlh(getStateLock());
        auto hbl = ht.getLockedBucket(key);
        StoredValue* v = fetchValidValue(hbl, key, true);

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

ENGINE_ERROR_CODE EPVBucket::addHighPriorityVBEntry(uint64_t id,
                                                    const void* cookie,
                                                    bool isBySeqno) {
    LockHolder lh(hpChksMutex);
    if (shard) {
        ++shard->highPriorityCount;
    }
    hpChks.push_back(HighPriorityVBEntry(cookie, id, isBySeqno));
    numHpChks.store(hpChks.size());
    return ENGINE_SUCCESS;
}

void EPVBucket::notifyOnPersistence(EventuallyPersistentEngine& e,
                                    uint64_t idNum,
                                    bool isBySeqno) {
    std::unique_lock<std::mutex> lh(hpChksMutex);
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;
    std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();

    std::string logStr(isBySeqno ? "seqno persistence"
                                 : "checkpoint persistence");

    while (entry != hpChks.end()) {
        if (isBySeqno != entry->isBySeqno_) {
            ++entry;
            continue;
        }

        std::string logStr(isBySeqno ? "seqno persistence"
                                     : "checkpoint persistence");

        hrtime_t wall_time(gethrtime() - entry->start);
        size_t spent = wall_time / 1000000000;
        if (entry->id <= idNum) {
            toNotify[entry->cookie] = ENGINE_SUCCESS;
            stats.chkPersistenceHisto.add(wall_time / 1000);
            adjustCheckpointFlushTimeout(wall_time / 1000000000);
            LOG(EXTENSION_LOG_NOTICE,
                "Notified the completion of %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64
                ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(),
                getId(),
                entry->id,
                idNum,
                entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else if (spent > getCheckpointFlushTimeout()) {
            adjustCheckpointFlushTimeout(spent);
            e.storeEngineSpecific(entry->cookie, NULL);
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            LOG(EXTENSION_LOG_WARNING,
                "Notified the timeout on %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64
                ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(),
                getId(),
                entry->id,
                idNum,
                entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else {
            ++entry;
        }
    }
    numHpChks.store(hpChks.size());
    lh.unlock();

    std::map<const void*, ENGINE_ERROR_CODE>::iterator itr = toNotify.begin();
    for (; itr != toNotify.end(); ++itr) {
        e.notifyIOComplete(itr->first, itr->second);
    }
}

void EPVBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) {
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;
    {
        LockHolder lh(hpChksMutex);
        std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();
        while (entry != hpChks.end()) {
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            e.storeEngineSpecific(entry->cookie, NULL);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        }
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

    std::map<const void*, ENGINE_ERROR_CODE>::iterator itr = toNotify.begin();
    for (; itr != toNotify.end(); ++itr) {
        e.notifyIOComplete(itr->first, itr->second);
    }

    fireAllOps(e);
}

size_t EPVBucket::getHighPriorityChkSize() {
    return numHpChks.load();
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
    StoredValue* v = fetchValidValue(hbl, key, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new VKeyStatBGFetchTask(&engine,
                                              key,
                                              getId(),
                                              v->getBySeqno(),
                                              cookie,
                                              bgFetchDelay,
                                              false);
        iom->schedule(task, READER_TASK_IDX);
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
                ExTask task = new VKeyStatBGFetchTask(
                        &engine, key, getId(), -1, cookie, bgFetchDelay, false);
                iom->schedule(task, READER_TASK_IDX);
            }
            }
            return ENGINE_EWOULDBLOCK;
        }
    }
}

void EPVBucket::completeStatsVKey(const DocKey& key,
                                  const RememberingCallback<GetValue>& gcb) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl, key, true);

    if (v && v->isTempInitialItem()) {
        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            ht.unlocked_restoreValue(
                    hbl.getHTLock(), *(gcb.val.getValue()), *v);
            if (!v->isResident()) {
                throw std::logic_error(
                        "VBucket::completeStatsVKey: "
                        "storedvalue (which has seqno:" +
                        std::to_string(v->getBySeqno()) +
                        ") should be resident after calling restoreValue()");
            }
        } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
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
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     /*wantDeleted*/ false,
                                     /*trackReference*/ false);

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

bool EPVBucket::htUnlockedEjectItem(StoredValue*& v) {
    return ht.unlocked_ejectItem(v, eviction);
}

size_t EPVBucket::queueBGFetchItem(const DocKey& key,
                                   std::unique_ptr<VBucketBGFetchItem> fetch,
                                   BgFetcher* bgFetcher) {
    LockHolder lh(pendingBGFetchesLock);
    vb_bgfetch_item_ctx_t& bgfetch_itm_ctx = pendingBGFetches[key];

    if (bgfetch_itm_ctx.bgfetched_list.empty()) {
        bgfetch_itm_ctx.isMetaOnly = true;
    }

    if (!fetch->metaDataOnly) {
        bgfetch_itm_ctx.isMetaOnly = false;
    }

    bgfetch_itm_ctx.bgfetched_list.push_back(std::move(fetch));

    bgFetcher->addPendingVB(getId());
    return pendingBGFetches.size();
}

std::pair<MutationStatus, VBNotifyCtx> EPVBucket::updateStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    MutationStatus status = ht.unlocked_updateStoredValue(htLock, v, itm);

    if (queueItmCtx) {
        return {status, queueDirty(v, *queueItmCtx)};
    }
    return {status, VBNotifyCtx()};
}

std::pair<StoredValue*, VBNotifyCtx> EPVBucket::addNewStoredValue(
        const HashTable::HashBucketLock& hbl,
        const Item& itm,
        const VBQueueItemCtx* queueItmCtx) {
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    if (queueItmCtx) {
        return {v, queueDirty(*v, *queueItmCtx)};
    }

    return {v, VBNotifyCtx()};
}

VBNotifyCtx EPVBucket::softDeleteStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        bool onlyMarkDeleted,
        const VBQueueItemCtx& queueItmCtx,
        uint64_t bySeqno) {
    ht.unlocked_softDelete(htLock, v, onlyMarkDeleted);

    if (queueItmCtx.genBySeqno == GenerateBySeqno::No) {
        v.setBySeqno(bySeqno);
    }

    return queueDirty(v, queueItmCtx);
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
        ExTask task = new SingleBGFetcherTask(
                &engine, key, getId(), cookie, isMeta, bgFetchDelay, false);
        iom->schedule(task, READER_TASK_IDX);
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

void EPVBucket::adjustCheckpointFlushTimeout(size_t wall_time) {
    size_t middle = (MIN_CHK_FLUSH_TIMEOUT + MAX_CHK_FLUSH_TIMEOUT) / 2;

    if (wall_time <= MIN_CHK_FLUSH_TIMEOUT) {
        chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;
    } else if (wall_time <= middle) {
        chkFlushTimeout = middle;
    } else {
        chkFlushTimeout = MAX_CHK_FLUSH_TIMEOUT;
    }
}

size_t EPVBucket::getCheckpointFlushTimeout() {
    return chkFlushTimeout;
}

GetValue EPVBucket::getInternalNonResident(const DocKey& key,
                                           const void* cookie,
                                           EventuallyPersistentEngine& engine,
                                           int bgFetchDelay,
                                           get_options_t options,
                                           const StoredValue& v) {
    if (options & QUEUE_BG_FETCH) {
        bgFetch(key, cookie, engine, bgFetchDelay);
    }
    return GetValue(
            NULL, ENGINE_EWOULDBLOCK, v.getBySeqno(), true, v.getNRUValue());
}
