/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include <string.h>
#include <time.h>

#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "access_scanner.h"
#include "checkpoint_remover.h"
#include "ep.h"
#include "ep_engine.h"
#include "flusher.h"
#include "htresizer.h"
#include "kvshard.h"
#include "kvstore.h"
#include "locks.h"
#include "warmup.h"
#include "tapconnmap.h"

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats &st) : stats(st) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("max_size") == 0) {
            stats.setMaxDataSize(value);
            size_t low_wat = static_cast<size_t>
                             (static_cast<double>(value) * 0.6);
            size_t high_wat = static_cast<size_t>(
                              static_cast<double>(value) * 0.75);
            stats.mem_low_wat.store(low_wat);
            stats.mem_high_wat.store(high_wat);
        } else if (key.compare("mem_low_wat") == 0) {
            stats.mem_low_wat.store(value);
        } else if (key.compare("mem_high_wat") == 0) {
            stats.mem_high_wat.store(value);
        } else if (key.compare("tap_throttle_threshold") == 0) {
            stats.tapThrottleThreshold.store(
                                          static_cast<double>(value) / 100.0);
        } else if (key.compare("warmup_min_memory_threshold") == 0) {
            stats.warmupMemUsedCap.store(static_cast<double>(value) / 100.0);
        } else if (key.compare("warmup_min_items_threshold") == 0) {
            stats.warmupNumReadCap.store(static_cast<double>(value) / 100.0);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to change value for unknown variable, %s\n",
                key.c_str());
        }
    }

private:
    EPStats &stats;
};

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
class EPStoreValueChangeListener : public ValueChangedListener {
public:
    EPStoreValueChangeListener(EventuallyPersistentStore &st) : store(st) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("bg_fetch_delay") == 0) {
            store.setBGFetchDelay(static_cast<uint32_t>(value));
        } else if (key.compare("expiry_window") == 0) {
            store.setItemExpiryWindow(value);
        } else if (key.compare("max_txn_size") == 0) {
            store.setTransactionSize(value);
        } else if (key.compare("exp_pager_stime") == 0) {
            store.setExpiryPagerSleeptime(value);
        } else if (key.compare("alog_sleep_time") == 0) {
            store.setAccessScannerSleeptime(value);
        } else if (key.compare("alog_task_time") == 0) {
            store.resetAccessScannerStartTime();
        } else if (key.compare("mutation_mem_threshold") == 0) {
            double mem_threshold = static_cast<double>(value) / 100;
            StoredValue::setMutationMemoryThreshold(mem_threshold);
        } else if (key.compare("tap_throttle_queue_cap") == 0) {
            store.getEPEngine().getTapThrottle().setQueueCap(value);
        } else if (key.compare("tap_throttle_cap_pcnt") == 0) {
            store.getEPEngine().getTapThrottle().setCapPercent(value);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to change value for unknown variable, %s\n",
                key.c_str());
        }
    }

private:
    EventuallyPersistentStore &store;
};

class VBucketMemoryDeletionTask : public GlobalTask {
public:
    VBucketMemoryDeletionTask(EventuallyPersistentEngine &eng,
                              RCPtr<VBucket> &vb, double delay) :
                              GlobalTask(&eng,
                              Priority::VBMemoryDeletionPriority, delay,false),
                              e(eng), vbucket(vb), vbid(vb->getId()) { }

    std::string getDescription() {
        std::stringstream ss;
        ss << "Removing (dead) vbucket " << vbid << " from memory";
        return ss.str();
    }

    bool run(void) {
        vbucket->notifyAllPendingConnsFailed(e);
        vbucket->ht.clear();
        vbucket.reset();
        return false;
    }

private:
    EventuallyPersistentEngine &e;
    RCPtr<VBucket> vbucket;
    uint16_t vbid;
};

class PendingOpsNotification : public GlobalTask {
public:
    PendingOpsNotification(EventuallyPersistentEngine &e, RCPtr<VBucket> &vb) :
        GlobalTask(&e, Priority::VBMemoryDeletionPriority, 0, false),
        engine(e), vbucket(vb) { }

    std::string getDescription() {
        std::stringstream ss;
        ss << "Notify pending operations for vbucket " << vbucket->getId();
        return ss.str();
    }

    bool run(void) {
        vbucket->fireAllOps(engine);
        return false;
    }

private:
    EventuallyPersistentEngine &engine;
    RCPtr<VBucket> vbucket;
};

EventuallyPersistentStore::EventuallyPersistentStore(
    EventuallyPersistentEngine &theEngine) :
    engine(theEngine), stats(engine.getEpStats()),
    vbMap(theEngine.getConfiguration(), *this),
    bgFetchQueue(0),
    diskFlushAll(false), bgFetchDelay(0), statsSnapshotTaskId(0),
    lastTransTimePerItem(0),snapshotVBState(false)
{
    cachedResidentRatio.activeRatio.store(0);
    cachedResidentRatio.replicaRatio.store(0);

    Configuration &config = engine.getConfiguration();
    MutationLog *shardlog;
    for (uint16_t i = 0; i < config.getMaxNumShards(); i++) {
        std::stringstream s;
        s << i;
        shardlog = new MutationLog(engine.getConfiguration().getAlogPath() +
                                 "." + s.str(),
                                 engine.getConfiguration().getAlogBlockSize());
        accessLog.push_back(shardlog);
    }

    storageProperties = new StorageProperties(true, true, true, true);

    ExecutorPool::get()->registerBucket(ObjectRegistry::getCurrentEngine());

    auxUnderlying = KVStoreFactory::create(stats, config, true);
    assert(auxUnderlying);

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    if (config.getConflictResolutionType().compare("seqno") == 0) {
        conflictResolver = new SeqBasedResolution();
    }

    setItemExpiryWindow(config.getExpiryWindow());
    config.addValueChangedListener("expiry_window",
                                   new EPStoreValueChangeListener(*this));

    setTransactionSize(config.getMaxTxnSize());
    config.addValueChangedListener("max_txn_size",
                                   new EPStoreValueChangeListener(*this));

    stats.setMaxDataSize(config.getMaxSize());
    config.addValueChangedListener("max_size",
                                   new StatsValueChangeListener(stats));

    stats.mem_low_wat.store(config.getMemLowWat());
    config.addValueChangedListener("mem_low_wat",
                                   new StatsValueChangeListener(stats));

    stats.mem_high_wat.store(config.getMemHighWat());
    config.addValueChangedListener("mem_high_wat",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleThreshold.store(static_cast<double>
                                    (config.getTapThrottleThreshold())
                                     / 100.0);
    config.addValueChangedListener("tap_throttle_threshold",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleWriteQueueCap.store(config.getTapThrottleQueueCap());
    config.addValueChangedListener("tap_throttle_queue_cap",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("tap_throttle_cap_pcnt",
                                   new EPStoreValueChangeListener(*this));

    setBGFetchDelay(config.getBgFetchDelay());
    config.addValueChangedListener("bg_fetch_delay",
                                   new EPStoreValueChangeListener(*this));

    stats.warmupMemUsedCap.store(static_cast<double>
                               (config.getWarmupMinMemoryThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_memory_threshold",
                                   new StatsValueChangeListener(stats));
    stats.warmupNumReadCap.store(static_cast<double>
                                (config.getWarmupMinItemsThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_items_threshold",
                                   new StatsValueChangeListener(stats));

    double mem_threshold = static_cast<double>
                                      (config.getMutationMemThreshold()) / 100;
    StoredValue::setMutationMemoryThreshold(mem_threshold);
    config.addValueChangedListener("mutation_mem_threshold",
                                   new EPStoreValueChangeListener(*this));

    const std::string &policy = config.getItemEvictionPolicy();
    if (policy.compare("value_only") == 0) {
        eviction_policy = VALUE_ONLY;
    } else {
        eviction_policy = FULL_EVICTION;
    }

    // @todo - Ideally we should run the warmup thread in it's own
    //         thread so that it won't block the flusher (in the write
    //         thread), but we can't put it in the RO dispatcher either,
    //         because that would block the background fetches..
    warmupTask = new Warmup(this);
}

class WarmupWaitListener : public WarmupStateListener {
public:
    WarmupWaitListener(Warmup &f, bool wfw) :
        warmup(f), waitForWarmup(wfw) { }

    virtual void stateChanged(const int, const int to) {
        if (waitForWarmup) {
            if (to == WarmupState::Done) {
                LockHolder lh(syncobject);
                syncobject.notify();
            }
        } else if (to != WarmupState::Initialize) {
            LockHolder lh(syncobject);
            syncobject.notify();
        }
    }

    void wait() {
        LockHolder lh(syncobject);
        // Verify that we're not already reached the state...
        int currstate = warmup.getState().getState();

        if (waitForWarmup) {
            if (currstate == WarmupState::Done) {
                return;
            }
        } else if (currstate != WarmupState::Initialize) {
            return ;
        }

        syncobject.wait();
    }

private:
    Warmup &warmup;
    bool waitForWarmup;
    SyncObject syncobject;
};

bool EventuallyPersistentStore::initialize() {
    // We should nuke everything unless we want warmup
    Configuration &config = engine.getConfiguration();
    if (!config.isWarmup()) {
        reset();
    }

    if (!startFlusher()) {
        LOG(EXTENSION_LOG_WARNING,
            "FATAL: Failed to create and start flushers");
        return false;
    }
    if (!startBgFetcher()) {
        LOG(EXTENSION_LOG_WARNING,
           "FATAL: Failed to create and start bgfetchers");
        return false;
    }

    WarmupWaitListener warmupListener(*warmupTask, config.isWaitforwarmup());
    warmupTask->addWarmupStateListener(&warmupListener);
    warmupTask->start();
    warmupListener.wait();
    warmupTask->removeWarmupStateListener(&warmupListener);

    if (config.isVb0() && !vbMap.getBucket(0)) {
        setVBucketState(0, vbucket_state_active, false);
    }

    if (config.isFailpartialwarmup() && stats.warmOOM > 0) {
        LOG(EXTENSION_LOG_WARNING,
            "Warmup failed to load %d records due to OOM, exiting.\n",
            static_cast<unsigned int>(stats.warmOOM));
        return false;
    }

    ExTask itmpTask = new ItemPager(&engine, stats);
    ExecutorPool::get()->schedule(itmpTask, NONIO_TASK_IDX);

    size_t expiryPagerSleeptime = config.getExpPagerStime();
    setExpiryPagerSleeptime(expiryPagerSleeptime);
    config.addValueChangedListener("exp_pager_stime",
                                   new EPStoreValueChangeListener(*this));

    ExTask htrTask = new HashtableResizerTask(this, 10);
    ExecutorPool::get()->schedule(htrTask, NONIO_TASK_IDX);

    size_t checkpointRemoverInterval = config.getChkRemoverStime();
    ExTask chkTask = new ClosedUnrefCheckpointRemoverTask(&engine, stats,
                                                    checkpointRemoverInterval);
    ExecutorPool::get()->schedule(chkTask, NONIO_TASK_IDX);
    return true;
}

EventuallyPersistentStore::~EventuallyPersistentStore() {
    stopWarmup();
    stopBgFetcher();
    ExecutorPool::get()->stopTaskGroup(&engine, NONIO_TASK_IDX);

    ExecutorPool::get()->cancel(statsSnapshotTaskId);
    ExecutorPool::get()->cancel(accessScanner.task);

    stopFlusher();
    ExecutorPool::get()->unregisterBucket(ObjectRegistry::getCurrentEngine());

    engine.getUprConnMap().closeAllStreams();

    delete conflictResolver;
    delete warmupTask;
    delete auxUnderlying;
    delete storageProperties;

    std::vector<MutationLog*>::iterator it;
    for (it = accessLog.begin(); it != accessLog.end(); it++) {
        delete *it;
    }
}

const Flusher* EventuallyPersistentStore::getFlusher(uint16_t shardId) {
    return vbMap.getShard(shardId)->getFlusher();
}

Warmup* EventuallyPersistentStore::getWarmup(void) const {
    return warmupTask;
}

bool EventuallyPersistentStore::startFlusher() {
    for (uint16_t i = 0; i < vbMap.numShards; ++i) {
        Flusher *flusher = vbMap.shards[i]->getFlusher();
        flusher->start();
    }
    return true;
}

void EventuallyPersistentStore::stopFlusher() {
    for (uint16_t i = 0; i < vbMap.numShards; i++) {
        Flusher *flusher = vbMap.shards[i]->getFlusher();
        bool rv = flusher->stop(stats.forceShutdown);
        if (rv && !stats.forceShutdown) {
            flusher->wait();
        }
    }
}

bool EventuallyPersistentStore::pauseFlusher() {
    bool rv = true;
    for (uint16_t i = 0; i < vbMap.numShards; i++) {
        Flusher *flusher = vbMap.shards[i]->getFlusher();
        if (!flusher->pause()) {
            LOG(EXTENSION_LOG_WARNING, "Attempted to pause flusher in state "
                "[%s], shard = %d", flusher->stateName(), i);
            rv = false;
        }
    }
    return rv;
}

bool EventuallyPersistentStore::resumeFlusher() {
    bool rv = true;
    for (uint16_t i = 0; i < vbMap.numShards; i++) {
        Flusher *flusher = vbMap.shards[i]->getFlusher();
        if (!flusher->resume()) {
            LOG(EXTENSION_LOG_WARNING,
                    "Warning: attempted to resume flusher in state [%s], "
                    "shard = %d", flusher->stateName(), i);
            rv = false;
        }
    }
    return rv;
}

void EventuallyPersistentStore::wakeUpFlusher() {
    if (stats.diskQueueSize.load() == 0) {
        for (uint16_t i = 0; i < vbMap.numShards; i++) {
            Flusher *flusher = vbMap.shards[i]->getFlusher();
            flusher->wake();
        }
    }
}

bool EventuallyPersistentStore::startBgFetcher() {
    for (uint16_t i = 0; i < vbMap.numShards; i++) {
        BgFetcher *bgfetcher = vbMap.shards[i]->getBgFetcher();
        if (bgfetcher == NULL) {
            LOG(EXTENSION_LOG_WARNING,
                "Falied to start bg fetcher for shard %d", i);
            return false;
        }
        bgfetcher->start();
    }
    return true;
}

void EventuallyPersistentStore::stopBgFetcher() {
    for (uint16_t i = 0; i < vbMap.numShards; i++) {
        BgFetcher *bgfetcher = vbMap.shards[i]->getBgFetcher();
        if (multiBGFetchEnabled() && bgfetcher->pendingJob()) {
            LOG(EXTENSION_LOG_WARNING,
                "Shutting down engine while there are still pending data "
                "read for shard %d from database storage", i);
        }
        LOG(EXTENSION_LOG_INFO, "Stopping bg fetcher for underlying storage");
        bgfetcher->stop();
    }
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbid,
                                                vbucket_state_t wanted_state) {
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    vbucket_state_t found_state(vb ? vb->getState() : vbucket_state_dead);
    if (found_state == wanted_state) {
        return vb;
    } else {
        RCPtr<VBucket> rv;
        return rv;
    }
}

void
EventuallyPersistentStore::deleteExpiredItem(uint16_t vbid, std::string &key,
                                             time_t startTime,
                                             uint64_t revSeqno) {
    RCPtr<VBucket> vb = getVBucket(vbid);
    if (vb) {
        int bucket_num(0);
        incExpirationStat(vb);
        LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, false);
        if (v) {
            if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                // This is a temporary item whose background fetch for metadata
                // has completed.
                bool deleted = vb->ht.unlocked_del(key, bucket_num);
                assert(deleted);
            } else if (v->isExpired(startTime) && !v->isDeleted()) {
                vb->ht.unlocked_softDelete(v, 0, getItemEvictionPolicy());
                queueDirty(vb, v, false);
                lh.unlock();
            }
        } else {
            if (eviction_policy == FULL_EVICTION) {
                // Create a temp item and delete and push it
                // into the checkpoint queue.
                add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                            eviction_policy);
                if (rv == ADD_NOMEM) {
                    return;
                }
                v = vb->ht.unlocked_find(key, bucket_num, true, false);
                v->setStoredValueState(StoredValue::state_deleted_key);
                v->setRevSeqno(revSeqno);
                vb->ht.unlocked_softDelete(v, 0, eviction_policy);
                queueDirty(vb, v, false);
                lh.unlock();
            }
        }
    }
}

void
EventuallyPersistentStore::deleteExpiredItems(std::list<std::pair<uint16_t,
                                                        std::string> > &keys) {
    std::list<std::pair<uint16_t, std::string> >::iterator it;
    time_t startTime = ep_real_time();
    for (it = keys.begin(); it != keys.end(); it++) {
        deleteExpiredItem(it->first, it->second, startTime, 0);
    }
}

StoredValue *EventuallyPersistentStore::fetchValidValue(RCPtr<VBucket> &vb,
                                                        const std::string &key,
                                                        int bucket_num,
                                                        bool wantDeleted,
                                                        bool trackReference,
                                                        bool queueExpired) {
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, wantDeleted,
                                          trackReference);
    if (v && !v->isDeleted()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (vb->getState() != vbucket_state_active) {
                return wantDeleted ? v : NULL;
            }
            if (queueExpired) {
                incExpirationStat(vb, false);
                vb->ht.unlocked_softDelete(v, 0, eviction_policy);
                queueDirty(vb, v, false, false);
            }
            return wantDeleted ? v : NULL;
        }
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(
                                                        const std::string &key,
                                                        uint16_t vbucket,
                                                        const char **msg,
                                                        size_t *msg_size,
                                                        bool force) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() != vbucket_state_active && !force)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, force, false);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    *msg_size = 0;
    if (v) {
        if (force)  {
            v->markClean();
        }
        if (v->isResident()) {
            if (vb->ht.unlocked_ejectItem(v, eviction_policy)) {
                *msg = "Ejected.";
            } else {
                *msg = "Can't eject: Dirty object.";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
            }
        } else {
            *msg = "Already ejected.";
        }
    } else {
        if (eviction_policy == VALUE_ONLY) {
            *msg = "Not found.";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            *msg = "Already ejected.";
        }
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTempItemForBgFetch(
                                                        LockHolder &lock,
                                                        int bucket_num,
                                                        const std::string &key,
                                                        RCPtr<VBucket> &vb,
                                                        const void *cookie,
                                                        bool metadataOnly) {

    add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                eviction_policy);
    switch(rv) {
        case ADD_NOMEM:
            return ENGINE_ENOMEM;
        case ADD_EXISTS:
        case ADD_UNDEL:
        case ADD_SUCCESS:
        case ADD_TMP_AND_BG_FETCH:
            // Since the hashtable bucket is locked, we shouldn't get here
            abort();
        case ADD_BG_FETCH:
            lock.unlock();
            bgFetch(key, vb->getId(), -1, cookie, metadataOnly);
    }
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::set(const Item &itm,
                                                 const void *cookie,
                                                 bool force,
                                                 uint8_t nru) {

    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (itm.getCas() != 0);
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);
    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, itm.getCas(),
                                                true, false,
                                                eviction_policy, nru);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case NOT_FOUND:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
        // FALLTHROUGH
    case WAS_DIRTY:
        // Even if the item was dirty, push it into the vbucket's open
        // checkpoint.
    case WAS_CLEAN:
        queueDirty(vb, v);
        break;
    case NEED_BG_FETCH:
        { // CAS operation with non-resident item + full eviction.
            if (v) {
                // temp item is already created. Simply schedule a bg fetch job
                lh.unlock();
                bgFetch(itm.getKey(), vb->getId(), -1, cookie, true);
                return ENGINE_EWOULDBLOCK;
            }
            ret = addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                        cookie, true);
            break;
        }
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::add(const Item &itm,
                                                 const void *cookie)
{
    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    if (itm.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return ENGINE_NOT_STORED;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);
    add_type_t atype = vb->ht.unlocked_add(bucket_num, v, itm,
                                           eviction_policy);

    switch (atype) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_TMP_AND_BG_FETCH:
        return addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                     cookie, true);
    case ADD_BG_FETCH:
        lh.unlock();
        bgFetch(itm.getKey(), vb->getId(), -1, cookie, true);
        return ENGINE_EWOULDBLOCK;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        queueDirty(vb, v);
        break;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTAPBackfillItem(const Item &itm,
                                                                uint8_t nru) {

    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb ||
        vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);


    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, 0, true, true,
                                                eviction_policy, nru);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case WAS_DIRTY:
        // If a given backfill item is already dirty, don't queue it again.
        break;
    case NOT_FOUND:
        // FALLTHROUGH
    case WAS_CLEAN:
        queueDirty(vb, v, true, true, true);
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case NEED_BG_FETCH:
        // SET on a non-active vbucket should not require a bg_metadata_fetch.
        abort();
    }

    return ret;
}

class KVStatsCallback : public Callback<kvstats_ctx> {
    public:
        KVStatsCallback(EventuallyPersistentStore *store)
            : epstore(store) { }

        void callback(kvstats_ctx &ctx) {
            RCPtr<VBucket> vb = epstore->getVBucket(ctx.vbucket);
            if (vb) {
                vb->fileSpaceUsed = ctx.fileSpaceUsed;
                vb->fileSize = ctx.fileSize;
            }
        }

    private:
        EventuallyPersistentStore *epstore;
};

void EventuallyPersistentStore::snapshotVBuckets(const Priority &priority,
                                                 uint16_t shardId) {

    class VBucketStateVisitor : public VBucketVisitor {
    public:
        VBucketStateVisitor(VBucketMap &vb_map, uint16_t sid)
            : vbuckets(vb_map), shardId(sid) { }
        bool visitBucket(RCPtr<VBucket> &vb) {
            if (vbuckets.getShard(vb->getId())->getId() == shardId) {
                vbucket_state vb_state;
                vb_state.state = vb->getState();
                vb_state.checkpointId = vbuckets.getPersistenceCheckpointId(
                                        vb->getId());
                vb_state.maxDeletedSeqno = 0;
                vb_state.failovers = vb->failovers->toJSON();
                states[vb->getId()] = vb_state;
            }
            return false;
        }

        void visit(StoredValue*) {
            assert(false); // this does not happen
        }

        std::map<uint16_t, vbucket_state> states;

    private:
        VBucketMap &vbuckets;
        uint16_t shardId;
    };

    KVShard *shard = vbMap.shards[shardId];
    if (priority == Priority::VBucketPersistLowPriority) {
        shard->setLowPriorityVbSnapshotFlag(false);
    } else {
        shard->setHighPriorityVbSnapshotFlag(false);
    }

    KVStatsCallback kvcb(this);
    VBucketStateVisitor v(vbMap, shard->getId());
    visit(v);
    hrtime_t start = gethrtime();
    LockHolder lh(shard->getWriteLock());
    KVStore *rwUnderlying = shard->getRWUnderlying();
    if (!rwUnderlying->snapshotVBuckets(v.states, &kvcb)) {
        LOG(EXTENSION_LOG_WARNING,
            "VBucket snapshot task failed!!! Rescheduling");
        scheduleVBSnapshot(priority, shard->getId());
    } else {
        stats.snapshotVbucketHisto.add((gethrtime() - start) / 1000);
    }

    if (priority == Priority::VBucketPersistHighPriority) {
        std::map<uint16_t, vbucket_state>::iterator it = v.states.begin();
        for (; it != v.states.end(); ++it) {
            vbMap.setBucketCreation(it->first, false);
        }
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                           vbucket_state_t to,
                                                           bool transfer,
                                                           bool notify_upr) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb && to == vb->getState()) {
        return ENGINE_SUCCESS;
    }

    uint16_t shardId = vbMap.getShard(vbid)->getId();
    if (vb) {
        vbucket_state_t oldstate = vb->getState();
        if (oldstate != to && notify_upr) {
            engine.getUprConnMap().vbucketStateChanged(vbid, to);
        }

        vb->setState(to, engine.getServerApi());
        if (to == vbucket_state_active && !transfer) {
            vb->failovers->createEntry(vb->getHighSeqno());
        }
        lh.unlock();
        if (oldstate == vbucket_state_pending &&
            to == vbucket_state_active) {
            ExTask notifyTask = new PendingOpsNotification(engine, vb);
            ExecutorPool::get()->schedule(notifyTask, NONIO_TASK_IDX);
        }
        scheduleVBSnapshot(Priority::VBucketPersistLowPriority, shardId);
    } else {
        FailoverTable* ft = new FailoverTable(engine.getMaxFailoverEntries());
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats,
                                         engine.getCheckpointConfig(),
                                         vbMap.getShard(vbid), 0, ft));
        // The first checkpoint for active vbucket should start with id 2.
        uint64_t start_chk_id = (to == vbucket_state_active) ? 2 : 0;
        newvb->checkpointManager.setOpenCheckpointId(start_chk_id);
        if (vbMap.addBucket(newvb) == ENGINE_ERANGE) {
            lh.unlock();
            return ENGINE_ERANGE;
        }
        vbMap.setPersistenceCheckpointId(vbid, 0);
        vbMap.setPersistenceSeqno(vbid, 0);
        vbMap.setBucketCreation(vbid, true);
        lh.unlock();
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority, shardId);
    }
    return ENGINE_SUCCESS;
}

void EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p) {
    snapshotVBState = false;
    KVShard *shard = NULL;
    if (p == Priority::VBucketPersistHighPriority) {
        for (size_t i = 0; i < vbMap.numShards; ++i) {
            shard = vbMap.shards[i];
            if (shard->setHighPriorityVbSnapshotFlag(true)) {
                ExTask task = new VBSnapshotTask(&engine, p, i, false);
                ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            }
        }
    } else {
        for (size_t i = 0; i < vbMap.numShards; ++i) {
            shard = vbMap.shards[i];
            if (shard->setLowPriorityVbSnapshotFlag(true)) {
                ExTask task = new VBSnapshotTask(&engine, p, i, false);
                ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            }
        }
    }
}

void EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p,
                                                   uint16_t shardId) {
    snapshotVBState = false;
    KVShard *shard = vbMap.shards[shardId];
    if (p == Priority::VBucketPersistHighPriority) {
        if (shard->setHighPriorityVbSnapshotFlag(true)) {
            ExTask task = new VBSnapshotTask(&engine, p, shardId, false);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
        }
    } else {
        if (shard->setLowPriorityVbSnapshotFlag(true)) {
            ExTask task = new VBSnapshotTask(&engine, p, shardId, false);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
        }
    }
}

bool EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid,
                                                        const void* cookie,
                                                        bool recreate) {
    LockHolder lh(vbsetMutex);

    hrtime_t start_time(gethrtime());
    bool success = true;
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead ||
         vbMap.isBucketDeletion(vbid)) {
        lh.unlock();
        uint16_t sid = vbMap.getShard(vbid)->getId();
        KVShard *shard = vbMap.shards[sid];
        LockHolder ls(shard->getWriteLock());
        KVStore *rwUnderlying = getRWUnderlying(vbid);
        if (rwUnderlying->delVBucket(vbid, recreate)) {
            vbMap.setBucketDeletion(vbid, false);
            ++stats.vbucketDeletions;
        } else {
            ++stats.vbucketDeletionFail;
            success = false;
        }
    }

    if (success) {
        hrtime_t spent(gethrtime() - start_time);
        hrtime_t wall_time = spent / 1000;
        BlockTimer::log(spent, "disk_vb_del", stats.timingLog);
        stats.diskVBDelHisto.add(wall_time);
        atomic_setIfBigger(stats.vbucketDelMaxWalltime, wall_time);
        stats.vbucketDelTotWalltime.fetch_add(wall_time);
        if (cookie) {
            engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        }
        return true;
    }

    return false;
}

void EventuallyPersistentStore::scheduleVBDeletion(RCPtr<VBucket> &vb,
                                                   const void* cookie,
                                                   double delay,
                                                   bool recreate) {
    ExTask delTask = new VBucketMemoryDeletionTask(engine, vb, delay);
    ExecutorPool::get()->schedule(delTask, NONIO_TASK_IDX);

    uint16_t vbid = vb->getId();
    if (vbMap.setBucketDeletion(vbid, true)) {
        ExTask task = new VBDeleteTask(&engine, vbid, cookie,
                                       Priority::VBucketDeletionPriority,
                                       vbMap.getShard(vbid)->getId(),
                                       recreate, delay);
        ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::deleteVBucket(uint16_t vbid,
                                                           const void* c) {
    // Lock to prevent a race condition between a failed update and add
    // (and delete).
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    engine.getUprConnMap().vbucketStateChanged(vbid, vbucket_state_dead);
    vbMap.removeBucket(vbid);
    lh.unlock();
    scheduleVBDeletion(vb, c);
    scheduleVBSnapshot(Priority::VBucketPersistHighPriority,
                       vbMap.getShard(vbid)->getId());
    if (c) {
        return ENGINE_EWOULDBLOCK;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::compactDB(uint16_t vbid,
                                                       compaction_ctx c,
                                                       const void *cookie) {
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ExTask task = new CompactVBucketTask(&engine, Priority::CompactorPriority,
                                         vbid, c, cookie);

    ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);

    LOG(EXTENSION_LOG_DEBUG, "Scheduled compaction task %d on vbucket %d,"
        "purge_before_ts = %lld, purge_before_seq = %lld, dropdeletes = %d",
        task->getId(), vbid, c.purge_before_ts,
        c.purge_before_seq, c.drop_deletes);

   return ENGINE_EWOULDBLOCK;
}

class ExpiredItemsCallback : public Callback<compaction_ctx> {
    public:
        ExpiredItemsCallback(EventuallyPersistentStore *store, uint16_t vbid)
            : epstore(store), vbucket(vbid) { }

        void callback(compaction_ctx &ctx) {
            std::list<expiredItemCtx>::iterator it;
            for (it  = ctx.expiredItems.begin();
                 it != ctx.expiredItems.end(); it++) {
                epstore->deleteExpiredItem(vbucket, it->keyStr,
                                           ctx.curr_time,
                                           it->revSeqno);
            }
        }

    private:
        EventuallyPersistentStore *epstore;
        uint16_t vbucket;
};

bool EventuallyPersistentStore::compactVBucket(const uint16_t vbid,
                                               compaction_ctx *ctx,
                                               const void *cookie) {
    KVShard *shard = vbMap.getShard(vbid);
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
    LockHolder lh(shard->getWriteLock());
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        if (vb->getState() == vbucket_state_active) {
            // Set the current time ONLY for active vbuckets.
            ctx->curr_time = ep_real_time();
        } else {
            ctx->curr_time = 0;
        }
        KVStore *rwUnderlying = shard->getRWUnderlying();
        ExpiredItemsCallback cb(this, vbid);
        KVStatsCallback kvcb(this);
        if (!rwUnderlying->compactVBucket(vbid, ctx, cb, kvcb)) {
            LOG(EXTENSION_LOG_WARNING,
                    "VBucket compaction failed failed!!!");
            err = ENGINE_TMPFAIL;
            engine.storeEngineSpecific(cookie, NULL);
        }
    } else {
        err = ENGINE_NOT_MY_VBUCKET;
        engine.storeEngineSpecific(cookie, NULL);
    }

    if (cookie) {
        engine.notifyIOComplete(cookie, err);
    }
    --stats.pendingCompactions;
    return false;
}

bool EventuallyPersistentStore::resetVBucket(uint16_t vbid) {
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        vbucket_state_t vbstate = vb->getState();
        if (vb->getNumItems(eviction_policy) == 0) { // Already reset?
            // The first checkpoint for active vbucket should start with id 2.
            uint64_t start_chk_id = (vbstate == vbucket_state_active) ? 2 : 0;
            vb->checkpointManager.setOpenCheckpointId(start_chk_id);
            vbMap.setPersistenceCheckpointId(vbid, 0);
            vbMap.setPersistenceSeqno(vbid, 0);
            return true;
        }

        vbMap.removeBucket(vbid);
        lh.unlock();

        std::list<std::string> tap_cursors = vb->checkpointManager.
                                             getTAPCursorNames();
        // Delete the vbucket database file and recreate the empty file
        scheduleVBDeletion(vb, NULL, 0, true);
        setVBucketState(vbid, vbstate, false);

        // Copy the all cursors from the old vbucket into the new vbucket
        RCPtr<VBucket> newvb = vbMap.getBucket(vbid);
        newvb->checkpointManager.resetTAPCursors(tap_cursors);

        rv = true;
    }
    return rv;
}

extern "C" {

    typedef struct {
        EventuallyPersistentEngine* engine;
        std::map<std::string, std::string> smap;
    } snapshot_stats_t;

    static void add_stat(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie) {
        assert(cookie);
        void *ptr = const_cast<void *>(cookie);
        snapshot_stats_t* snap = static_cast<snapshot_stats_t*>(ptr);
        ObjectRegistry::onSwitchThread(snap->engine);

        std::string k(key, klen);
        std::string v(val, vlen);
        snap->smap.insert(std::pair<std::string, std::string>(k, v));
    }
}

void EventuallyPersistentStore::snapshotStats() {
    snapshot_stats_t snap;
    snap.engine = &engine;
    std::map<std::string, std::string>  smap;
    bool rv = engine.getStats(&snap, NULL, 0, add_stat) == ENGINE_SUCCESS &&
              engine.getStats(&snap, "tap", 3, add_stat) == ENGINE_SUCCESS &&
              engine.getStats(&snap, "upr", 3, add_stat) == ENGINE_SUCCESS;
    if (rv && stats.isShutdown) {
        snap.smap["ep_force_shutdown"] = stats.forceShutdown ?
                                                              "true" : "false";
        std::stringstream ss;
        ss << ep_real_time();
        snap.smap["ep_shutdown_time"] = ss.str();
    }
    getOneRWUnderlying()->snapshotStats(snap.smap);
}

void EventuallyPersistentStore::updateBGStats(const hrtime_t init,
                                              const hrtime_t start,
                                              const hrtime_t stop) {
    if (stop > start && start > init) {
        // skip the measurement if the counter wrapped...
        ++stats.bgNumOperations;
        hrtime_t w = (start - init) / 1000;
        BlockTimer::log(start - init, "bgwait", stats.timingLog);
        stats.bgWaitHisto.add(w);
        stats.bgWait.fetch_add(w);
        atomic_setIfLess(stats.bgMinWait, w);
        atomic_setIfBigger(stats.bgMaxWait, w);

        hrtime_t l = (stop - start) / 1000;
        BlockTimer::log(stop - start, "bgload", stats.timingLog);
        stats.bgLoadHisto.add(l);
        stats.bgLoad.fetch_add(l);
        atomic_setIfLess(stats.bgMinLoad, l);
        atomic_setIfBigger(stats.bgMaxLoad, l);
    }
}

void EventuallyPersistentStore::completeBGFetch(const std::string &key,
                                                uint16_t vbucket,
                                                uint64_t rowid,
                                                const void *cookie,
                                                hrtime_t init,
                                                bool isMeta) {
    hrtime_t start(gethrtime());
    // Go find the data
    RememberingCallback<GetValue> gcb;
    if (isMeta) {
        gcb.val.setPartial();
        ++stats.bg_meta_fetched;
    } else {
        ++stats.bg_fetched;
    }
    getROUnderlying(vbucket)->get(key, rowid, vbucket, gcb);
    gcb.waitForValue();
    assert(gcb.fired);
    ENGINE_ERROR_CODE status = gcb.val.getStatus();

    // Lock to prevent a race condition between a fetch for restore and delete
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (vb) {
        int bucket_num(0);
        LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = fetchValidValue(vb, key, bucket_num, true);
        if (isMeta) {
            if (v && v->unlocked_restoreMeta(gcb.val.getValue(),
                                             gcb.val.getStatus(), vb->ht)) {
                status = ENGINE_SUCCESS;
            }
        } else {
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            }

            bool restore = false;
            if (eviction_policy == VALUE_ONLY &&
                v && !v->isResident() && !v->isDeleted()) {
                restore = true;
            } else if (eviction_policy == FULL_EVICTION &&
                       v && v->isTempInitialItem()) {
                restore = true;
            }

            if (restore) {
                if (gcb.val.getStatus() == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(gcb.val.getValue(), vb->ht);
                    assert(v->isResident());
                    if (vb->getState() == vbucket_state_active &&
                        v->getExptime() != gcb.val.getValue()->getExptime() &&
                        v->getCas() == gcb.val.getValue()->getCas()) {
                        // MB-9306: It is possible that by the time bgfetcher
                        // returns, the item may have been updated and queued
                        // Hence test the CAS value to be the same first.
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, v);
                        hlh.unlock();
                    }
                } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
                    v->setStoredValueState(
                                          StoredValue::state_non_existent_key);
                    if (eviction_policy == FULL_EVICTION) {
                        // For the full eviction, we should notify
                        // ENGINE_SUCCESS to the memcached worker thread, so
                        // that the worker thread can visit the ep-engine and
                        // figure out the correct error code.
                        status = ENGINE_SUCCESS;
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: failed background fetch for vb=%d seq=%d "
                        "key=%s", vbucket, v->getBySeqno(), key.c_str());
                    status = ENGINE_TMPFAIL;
                }
            }
        }
    } else {
        LOG(EXTENSION_LOG_INFO, "VBucket %d's file was deleted in the middle of"
            " a bg fetch for key %s\n", vbucket, key.c_str());
        status = ENGINE_NOT_MY_VBUCKET;
    }

    lh.unlock();

    hrtime_t stop = gethrtime();
    updateBGStats(init, start, stop);
    bgFetchQueue--;

    delete gcb.val.getValue();
    engine.notifyIOComplete(cookie, status);
}

void EventuallyPersistentStore::completeBGFetchMulti(uint16_t vbId,
                                 std::vector<bgfetched_item_t> &fetchedItems,
                                 hrtime_t startTime)
{
    RCPtr<VBucket> vb = getVBucket(vbId);
    if (!vb) {
        std::vector<bgfetched_item_t>::iterator itemItr = fetchedItems.begin();
        for (; itemItr != fetchedItems.end(); ++itemItr) {
            engine.notifyIOComplete((*itemItr).second->cookie,
                                    ENGINE_NOT_MY_VBUCKET);
        }
        LOG(EXTENSION_LOG_WARNING,
            "EP Store completes %d of batched background fetch for "
            "for vBucket = %d that is already deleted\n",
            (int)fetchedItems.size(), vbId);
        return;
    }

    std::vector<bgfetched_item_t>::iterator itemItr = fetchedItems.begin();
    for (; itemItr != fetchedItems.end(); ++itemItr) {
        VBucketBGFetchItem *bgitem = (*itemItr).second;
        ENGINE_ERROR_CODE status = bgitem->value.getStatus();
        Item *fetchedValue = bgitem->value.getValue();
        const std::string &key = (*itemItr).first;

        int bucket = 0;
        LockHolder blh = vb->ht.getLockedBucket(key, &bucket);
        StoredValue *v = fetchValidValue(vb, key, bucket, true);
        if (bgitem->metaDataOnly) {
            if (v && v->unlocked_restoreMeta(fetchedValue, status, vb->ht)) {
                status = ENGINE_SUCCESS;
            }
        } else {
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            }

            bool restore = false;
            if (eviction_policy == VALUE_ONLY &&
                v && !v->isResident() && !v->isDeleted()) {
                restore = true;
            } else if (eviction_policy == FULL_EVICTION &&
                       v && v->isTempInitialItem()) {
                restore = true;
            }

            if (restore) {
                if (status == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(fetchedValue, vb->ht);
                    assert(v->isResident());
                    if (vb->getState() == vbucket_state_active &&
                        v->getExptime() != fetchedValue->getExptime() &&
                        v->getCas() == fetchedValue->getCas()) {
                        // MB-9306: It is possible that by the time
                        // bgfetcher returns, the item may have been
                        // updated and queued
                        // Hence test the CAS value to be the same first.
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, v);
                        blh.unlock();
                    }
                } else if (status == ENGINE_KEY_ENOENT) {
                    v->setStoredValueState(StoredValue::state_non_existent_key);
                    if (eviction_policy == FULL_EVICTION) {
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
                        "Warning: failed background fetch for vb=%d "
                        "key=%s", vbId, key.c_str());
                    status = ENGINE_TMPFAIL;
                }
            }
        }
        blh.unlock();

        if (bgitem->metaDataOnly) {
            ++stats.bg_meta_fetched;
        } else {
            ++stats.bg_fetched;
        }

        hrtime_t endTime = gethrtime();
        updateBGStats(bgitem->initTime, startTime, endTime);
        engine.notifyIOComplete(bgitem->cookie, status);
    }

    LOG(EXTENSION_LOG_DEBUG,
        "EP Store completes %d of batched background fetch "
        "for vBucket = %d endTime = %lld\n",
        fetchedItems.size(), vbId, gethrtime()/1000000);
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        uint64_t rowid,
                                        const void *cookie,
                                        bool isMeta) {
    std::stringstream ss;

    if (multiBGFetchEnabled()) {
        RCPtr<VBucket> vb = getVBucket(vbucket);
        assert(vb);
        KVShard *myShard = vbMap.getShard(vbucket);

        // schedule to the current batch of background fetch of the given
        // vbucket
        VBucketBGFetchItem * fetchThis = new VBucketBGFetchItem(cookie,
                                                                isMeta);
        vb->queueBGFetchItem(key, fetchThis, myShard->getBgFetcher());
        myShard->getBgFetcher()->notifyBGEvent();
        ss << "Queued a background fetch, now at "
           << vb->numPendingBGFetchItems() << std::endl;
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
    } else {
        bgFetchQueue++;
        stats.maxRemainingBgJobs = std::max(stats.maxRemainingBgJobs,
                                            bgFetchQueue.load());
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new BGFetchTask(&engine, key, vbucket, rowid, cookie,
                                      isMeta,
                                      Priority::BgFetcherGetMetaPriority,
                                      bgFetchDelay, false);
        iom->schedule(task, READER_TASK_IDX);
        ss << "Queued a background fetch, now at " << bgFetchQueue.load()
           << std::endl;
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
    }
}

GetValue EventuallyPersistentStore::getInternal(const std::string &key,
                                                uint16_t vbucket,
                                                const void *cookie,
                                                bool queueBG,
                                                bool honorStates,
                                                vbucket_state_t allowedState,
                                                bool trackReference) {

    vbucket_state_t disallowedState = (allowedState == vbucket_state_active) ?
        vbucket_state_replica : vbucket_state_active;
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == disallowedState) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true,
                                     trackReference);
    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            GetValue rv;
            return rv;
        }
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, v->getBySeqno(), cookie);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getBySeqno(),
                            true, v->getNRUValue());
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getBySeqno(), false, v->getNRUValue());
        return rv;
    } else {
        if (eviction_policy == VALUE_ONLY || diskFlushAll) {
            GetValue rv;
            return rv;
        }
        ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
        if (queueBG) { // Full eviction and need a bg fetch.
            ec = addTempItemForBgFetch(lh, bucket_num, key, vb,
                                       cookie, false);
        }
        return GetValue(NULL, ec, -1, true);
    }
}

GetValue EventuallyPersistentStore::getRandomKey() {
    long max = vbMap.getSize();

    long start = random() % max;
    long curr = start;
    Item *itm = NULL;

    while (itm == NULL) {
        RCPtr<VBucket> vb = getVBucket(curr++);
        while (!vb || vb->getState() != vbucket_state_active) {
            if (curr == start) {
                return GetValue(NULL, ENGINE_KEY_ENOENT);
            }
            if (curr == max) {
                curr = 0;
            }

            vb = getVBucket(curr++);
        }

        if ((itm = vb->ht.getRandomKey(random())) != NULL) {
            GetValue rv(itm, ENGINE_SUCCESS);
            return rv;
        }

        if (curr == max) {
            curr = 0;
        }

        if (curr == start) {
            return GetValue(NULL, ENGINE_KEY_ENOENT);
        }
        // Search next vbucket
    }

    return GetValue(NULL, ENGINE_KEY_ENOENT);
}


ENGINE_ERROR_CODE EventuallyPersistentStore::getMetaData(
                                                        const std::string &key,
                                                        uint16_t vbucket,
                                                        const void *cookie,
                                                        ItemMetaData &metadata,
                                                        uint32_t &deleted,
                                                        bool trackReferenced)
{
    (void) cookie;
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    deleted = 0;
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true,
                                          trackReferenced);

    if (v) {
        stats.numOpsGetMeta++;

        if (v->isTempInitialItem()) { // Need bg meta fetch.
            bgFetch(key, vbucket, -1, cookie, true);
            return ENGINE_EWOULDBLOCK;
        } else if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return ENGINE_KEY_ENOENT;
        } else {
            if (v->isTempDeletedItem() || v->isDeleted() ||
                v->isExpired(ep_real_time())) {
                deleted |= GET_META_ITEM_DELETED_FLAG;
            }
            metadata.cas = v->getCas();
            metadata.flags = v->getFlags();
            metadata.exptime = v->getExptime();
            metadata.revSeqno = v->getRevSeqno();
            return ENGINE_SUCCESS;
        }
    } else {
        // The key wasn't found. However, this may be because it was previously
        // deleted or evicted with the full eviction strategy.
        // So, add a temporary item corresponding to the key to the hash table
        // and schedule a background fetch for its metadata from the persistent
        // store. The item's state will be updated after the fetch completes.
        return addTempItemForBgFetch(lh, bucket_num, key, vb, cookie, true);
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setWithMeta(const Item &itm,
                                                         uint64_t cas,
                                                         const void *cookie,
                                                         bool force,
                                                         bool allowExisting,
                                                         uint8_t nru,
                                                         bool genBySeqno)
{
    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);

    if (!force) {
        if (v)  {
            if (v->isTempInitialItem()) {
                bgFetch(itm.getKey(), itm.getVBucketId(), -1, cookie, true);
                return ENGINE_EWOULDBLOCK;
            }
            if (!conflictResolver->resolve(v, itm.getMetaData(), false)) {
                ++stats.numOpsSetMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            return addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                         cookie, true);
        }
    }

    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, cas, allowExisting,
                                                true, eviction_policy, nru);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case WAS_DIRTY:
    case WAS_CLEAN:
        queueDirty(vb, v, false, true, genBySeqno);
        break;
    case NOT_FOUND:
        ret = ENGINE_KEY_ENOENT;
        break;
    case NEED_BG_FETCH:
        {            // CAS operation with non-resident item + full eviction.
            if (v) { // temp item is already created. Simply schedule a
                lh.unlock(); // bg fetch job.
                bgFetch(itm.getKey(), vb->getId(), -1, cookie, true);
                return ENGINE_EWOULDBLOCK;
            }
            ret = addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                        cookie, true);
        }
    }

    return ret;
}

GetValue EventuallyPersistentStore::getAndUpdateTtl(const std::string &key,
                                                    uint16_t vbucket,
                                                    const void *cookie,
                                                    time_t exptime)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            GetValue rv;
            return rv;
        }

        if (!v->isResident()) {
            bgFetch(key, vbucket, v->getBySeqno(), cookie);
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getBySeqno());
        }
        if (v->isLocked(ep_current_time())) {
            GetValue rv(NULL, ENGINE_KEY_EEXISTS, 0);
            return rv;
        }

        bool exptime_mutated = exptime != v->getExptime() ? true : false;
        if (exptime_mutated) {
           v->markDirty();
           v->setExptime(exptime);
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getBySeqno());

        if (exptime_mutated) {
            // persist the itme in the underlying storage for
            // mutated exptime
            queueDirty(vb, v);
            lh.unlock();
        }
        return rv;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            GetValue rv;
            return rv;
        } else {
            ENGINE_ERROR_CODE ec = addTempItemForBgFetch(lh, bucket_num, key,
                                                         vb, cookie, false);
            return GetValue(NULL, ec, -1, true);
        }
    }
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::statsVKey(const std::string &key,
                                     uint16_t vbucket,
                                     const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }
        bgFetchQueue++;
        assert(bgFetchQueue > 0);
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new VKeyStatBGFetchTask(&engine, key, vbucket,
                                           v->getBySeqno(), cookie,
                                           Priority::VKeyStatBgFetcherPriority,
                                           bgFetchDelay, false);
        iom->schedule(task, READER_TASK_IDX);
        return ENGINE_EWOULDBLOCK;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                        eviction_policy);
            switch(rv) {
            case ADD_NOMEM:
                return ENGINE_ENOMEM;
            case ADD_EXISTS:
            case ADD_UNDEL:
            case ADD_SUCCESS:
            case ADD_TMP_AND_BG_FETCH:
                // Since the hashtable bucket is locked, we shouldn't get here
                abort();
            case ADD_BG_FETCH:
                {
                    ++bgFetchQueue;
                    assert(bgFetchQueue > 0);
                    ExecutorPool* iom = ExecutorPool::get();
                    ExTask task = new VKeyStatBGFetchTask(&engine, key,
                                                          vbucket, -1, cookie,
                                           Priority::VKeyStatBgFetcherPriority,
                                                          bgFetchDelay, false);
                    iom->schedule(task, READER_TASK_IDX);
                }
            }
            return ENGINE_EWOULDBLOCK;
        }
    }
}

void EventuallyPersistentStore::completeStatsVKey(const void* cookie,
                                                  std::string &key,
                                                  uint16_t vbid,
                                                  uint64_t bySeqNum) {
    RememberingCallback<GetValue> gcb;

    getROUnderlying(vbid)->get(key, bySeqNum, vbid, gcb);
    gcb.waitForValue();
    assert(gcb.fired);

    if (eviction_policy == FULL_EVICTION) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (vb) {
            int bucket_num(0);
            LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
            StoredValue *v = fetchValidValue(vb, key, bucket_num, true);
            if (v && v->isTempInitialItem()) {
                if (gcb.val.getStatus() == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(gcb.val.getValue(), vb->ht);
                    assert(v->isResident());
                } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
                    v->setStoredValueState(
                                          StoredValue::state_non_existent_key);
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: failed background fetch for vb=%d seq=%d "
                        "key=%s", vbid, v->getBySeqno(), key.c_str());
                }
            }
        }
    }

    if (gcb.val.getStatus() == ENGINE_SUCCESS) {
        engine.addLookupResult(cookie, gcb.val.getValue());
    } else {
        engine.addLookupResult(cookie, NULL);
    }

    bgFetchQueue--;
    engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
}

bool EventuallyPersistentStore::getLocked(const std::string &key,
                                          uint16_t vbucket,
                                          Callback<GetValue> &cb,
                                          rel_time_t currentTime,
                                          uint32_t lockTimeout,
                                          const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        GetValue rv(NULL, ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return false;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempNonExistentItem() ||
            v->isTempDeletedItem()) {
            GetValue rv;
            cb.callback(rv);
            return true;
        }

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            GetValue rv;
            cb.callback(rv);
            return false;
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (cookie) {
                bgFetch(key, vbucket, v->getBySeqno(), cookie);
            }
            GetValue rv(NULL, ENGINE_EWOULDBLOCK, -1, true);
            cb.callback(rv);
            return false;
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout);

        Item *it = v->toItem(false, vbucket);
        it->setCas();
        v->setCas(it->getCas());

        GetValue rv(it);
        cb.callback(rv);
        return true;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            GetValue rv;
            cb.callback(rv);
            return true;
        } else {
            ENGINE_ERROR_CODE ec = addTempItemForBgFetch(lh, bucket_num, key,
                                                         vb, cookie, false);
            GetValue rv(NULL, ec, -1, true);
            cb.callback(rv);
            return false;
        }
    }
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::unlockKey(const std::string &key,
                                     uint16_t vbucket,
                                     uint64_t cas,
                                     rel_time_t currentTime)
{

    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempNonExistentItem() ||
            v->isTempDeletedItem()) {
            return ENGINE_KEY_ENOENT;
        }
        if (v->isLocked(currentTime)) {
            if (v->getCas() == cas) {
                v->unlock();
                return ENGINE_SUCCESS;
            }
        }
        return ENGINE_TMPFAIL;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            // With the full eviction, an item's lock is automatically
            // released when the item is evicted from memory. Therefore,
            // we simply return ENGINE_TMPFAIL when we receive unlockKey
            // for an item that is not in memocy cache. Note that we don't
            // spawn any bg fetch job to figure out if an item actually
            // exists in disk or not.
            return ENGINE_TMPFAIL;
        }
    }
}


ENGINE_ERROR_CODE EventuallyPersistentStore::getKeyStats(
                                            const std::string &key,
                                            uint16_t vbucket,
                                            const void *cookie,
                                            struct key_stats &kstats,
                                            bool bgfetch,
                                            bool wantsDeleted)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if ((v->isDeleted() && !wantsDeleted) ||
            v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            return ENGINE_KEY_ENOENT;
        }
        if (eviction_policy == FULL_EVICTION &&
            v->isTempInitialItem() && bgfetch) {
            lh.unlock();
            bgFetch(key, vbucket, -1, cookie, true);
            return ENGINE_EWOULDBLOCK;
        }
        kstats.logically_deleted = v->isDeleted();
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        kstats.vb_state = vb->getState();
        return ENGINE_SUCCESS;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            if (bgfetch) {
                return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                             cookie, true);
            } else {
                return ENGINE_KEY_ENOENT;
            }
        }
    }
}

std::string EventuallyPersistentStore::validateKey(const std::string &key,
                                                   uint16_t vbucket,
                                                   Item &diskItem) {
    int bucket_num(0);
    RCPtr<VBucket> vb = getVBucket(vbucket);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true,
                                     false, true);

    if (v) {
        if (v->isDeleted() || v->isTempNonExistentItem() ||
            v->isTempDeletedItem()) {
            return "item_deleted";
        }

        if (diskItem.getFlags() != v->getFlags()) {
            return "flags_mismatch";
        } else if (v->isResident() && memcmp(diskItem.getData(),
                                             v->getValue()->getData(),
                                             diskItem.getNBytes())) {
            return "data_mismatch";
        } else {
            return "valid";
        }
    } else {
        return "item_deleted";
    }

}

ENGINE_ERROR_CODE EventuallyPersistentStore::deleteItem(const std::string &key,
                                                        uint64_t* cas,
                                                        uint16_t vbucket,
                                                        const void *cookie,
                                                        bool force,
                                                        ItemMetaData *itemMeta,
                                                        bool tapBackfill)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() == vbucket_state_dead && !force)) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, false);
    if (!v || v->isDeleted() || v->isTempItem()) {
        if (eviction_policy == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else { // Full eviction.
            if (!force) {
                if (!v) { // Item might be evicted from cache.
                    return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                                 cookie, true);
                } else if (v->isTempInitialItem()) {
                    lh.unlock();
                    bgFetch(key, vbucket, -1, cookie, true);
                    return ENGINE_EWOULDBLOCK;
                } else { // Non-existent or deleted key.
                    return ENGINE_KEY_ENOENT;
                }
            } else {
                if (!v) { // Item might be evicted from cache.
                    // Create a temp item and delete it below as it is a
                    // force deletion.
                    add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num,
                                                              key,
                                                              eviction_policy);
                    if (rv == ADD_NOMEM) {
                        return ENGINE_ENOMEM;
                    }
                    v = vb->ht.unlocked_find(key, bucket_num, true, false);
                    v->setStoredValueState(StoredValue::state_deleted_key);
                } else if (v->isTempInitialItem()) {
                    v->setStoredValueState(StoredValue::state_deleted_key);
                } else { // Non-existent or deleted key.
                    return ENGINE_KEY_ENOENT;
                }
            }
        }
    }

    mutation_type_t delrv;
    delrv = vb->ht.unlocked_softDelete(v, *cas, eviction_policy);

    if (itemMeta && v) {
        itemMeta->revSeqno = v->getRevSeqno();
        itemMeta->cas = v->getCas();
        itemMeta->flags = v->getFlags();
        itemMeta->exptime = v->getExptime();
    }
    *cas = v ? v->getCas() : 0;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (delrv) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case INVALID_CAS:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case IS_LOCKED:
        ret = ENGINE_TMPFAIL;
        break;
    case NOT_FOUND:
        ret = ENGINE_KEY_ENOENT;
        break;
    case WAS_DIRTY:
    case WAS_CLEAN:
        queueDirty(vb, v, tapBackfill);
        break;
    case NEED_BG_FETCH:
        // We already figured out if a bg fetch is requred for a full-evicted
        // item above.
        abort();
    }
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::deleteWithMeta(
                                                        const std::string &key,
                                                        uint64_t* cas,
                                                        uint16_t vbucket,
                                                        const void *cookie,
                                                        bool force,
                                                        ItemMetaData *itemMeta,
                                                        bool tapBackfill,
                                                        bool genBySeqno,
                                                        uint64_t bySeqno)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() == vbucket_state_dead && !force)) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, false);
    if (!force) { // Need conflict resolution.
        if (v)  {
            if (v->isTempInitialItem()) {
                bgFetch(key, vbucket, -1, cookie, true);
                return ENGINE_EWOULDBLOCK;
            }
            if (!conflictResolver->resolve(v, *itemMeta, true)) {
                ++stats.numOpsDelMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                         cookie, true);
        }
    } else {
        if (!v) {
            // Create a temp item and delete it below as it is a force deletion
            add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                        eviction_policy);
            if (rv == ADD_NOMEM) {
                return ENGINE_ENOMEM;
            }
            v = vb->ht.unlocked_find(key, bucket_num, true, false);
            v->setStoredValueState(StoredValue::state_deleted_key);
        } else if (v->isTempInitialItem()) {
            v->setStoredValueState(StoredValue::state_deleted_key);
        }
    }

    mutation_type_t delrv;
    delrv = vb->ht.unlocked_softDelete(v, *cas, *itemMeta,
                                       eviction_policy, true);
    *cas = v ? v->getCas() : 0;

    if (!genBySeqno) {
        v->setBySeqno(bySeqno);
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (delrv) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case INVALID_CAS:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case IS_LOCKED:
        ret = ENGINE_TMPFAIL;
        break;
    case NOT_FOUND:
        ret = ENGINE_KEY_ENOENT;
        break;
    case WAS_DIRTY:
    case WAS_CLEAN:
            queueDirty(vb, v, tapBackfill, true, genBySeqno);
        break;
    case NEED_BG_FETCH:
        lh.unlock();
        bgFetch(key, vbucket, -1, cookie, true);
        ret = ENGINE_EWOULDBLOCK;
    }

    return ret;
}

void EventuallyPersistentStore::reset() {
    std::vector<int> buckets = vbMap.getBuckets();
    std::vector<int>::iterator it;
    for (it = buckets.begin(); it != buckets.end(); ++it) {
        RCPtr<VBucket> vb = getVBucket(*it);
        if (vb) {
            vb->ht.clear();
            vb->checkpointManager.clear(vb->getState());
            vb->resetStats();
        }
    }

    bool inverse = false;
    if (diskFlushAll.compare_exchange_strong(inverse, true)) {
        ++stats.diskQueueSize;
        // wake up (notify) one flusher is good enough for diskFlushAll
        vbMap.shards[EP_PRIMARY_SHARD]->getFlusher()->notifyFlushEvent();
    }
}

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * EventuallyPersistentStore::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback : public Callback<mutation_result>,
                            public Callback<int> {
public:

    PersistenceCallback(const queued_item &qi, RCPtr<VBucket> &vb,
                        EventuallyPersistentStore *st, EPStats *s, uint64_t c)
        : queuedItem(qi), vbucket(vb), store(st), stats(s), cas(c) {
        assert(vb);
        assert(s);
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(),
                                                        &bucket_num);
            StoredValue *v = store->fetchValidValue(vbucket,
                                                    queuedItem->getKey(),
                                                    bucket_num, true, false);
            if (v) {
                if (v->getCas() == cas) {
                    // mark this item clean only if current and stored cas
                    // value match
                    v->markClean();
                }
                if (v->isNewCacheItem()) {
                    if (value.second) {
                        // Insert in value-only or full eviction mode.
                        ++vbucket->opsCreate;
                    } else { // Update in full eviction mode.
                        --vbucket->ht.numTotalItems;
                        ++vbucket->opsUpdate;
                    }
                    v->setNewCacheItem(false);
                } else { // Update in value-only or full eviction mode.
                    ++vbucket->opsUpdate;
                }
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats->decrDiskQueueSize(1);
            stats->totalPersisted++;
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                int bucket_num(0);
                LockHolder lh = vbucket->ht.getLockedBucket(
                                           queuedItem->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vbucket,
                                                        queuedItem->getKey(),
                                                        bucket_num, true,
                                                        false);
                if (v) {
                    std::stringstream ss;
                    ss << "Persisting ``" << queuedItem->getKey() << "'' on vb"
                       << queuedItem->getVBucketId() << " (rowid="
                       << v->getBySeqno() << ") returned 0 updates\n";
                    LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Error persisting now missing ``%s'' from vb%d",
                        queuedItem->getKey().c_str(),
                        queuedItem->getVBucketId());
                }

                vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
                stats->decrDiskQueueSize(1);
            } else {
                std::stringstream ss;
                ss <<
                "Fatal error in persisting SET ``" <<
                queuedItem->getKey() << "'' on vb "
                   << queuedItem->getVBucketId() << "!!! Requeue it...\n";
                LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
                redirty();
            }
        }
    }

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void callback(int &value) {
        // > 1 would be bad.  We were only trying to delete one row.
        assert(value < 2);
        // -1 means fail
        // 1 means we deleted one row
        // 0 means we did not delete a row, but did not fail (did not exist)
        if (value >= 0) {
            // We have succesfully removed an item from the disk, we
            // may now remove it from the hash table.
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(),
                                                        &bucket_num);
            StoredValue *v = store->fetchValidValue(vbucket,
                                                    queuedItem->getKey(),
                                                    bucket_num, true, false);
            if (v && v->isDeleted()) {
                bool newCacheItem = v->isNewCacheItem();
                bool deleted = vbucket->ht.unlocked_del(queuedItem->getKey(),
                                                        bucket_num);
                assert(deleted);
                if (newCacheItem && value > 0) {
                    // Need to decrement the item counter again for an item that
                    // exists on DB file, but not in memory (i.e., full eviction),
                    // because we created the temp item in memory and incremented
                    // the item counter when a deletion is pushed in the queue.
                    --vbucket->ht.numTotalItems;
                }
            }

            if (value > 0) {
                ++stats->totalPersisted;
                ++vbucket->opsDelete;
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats->decrDiskQueueSize(1);
        } else {
            std::stringstream ss;
            ss << "Fatal error in persisting DELETE ``" <<
            queuedItem->getKey() << "'' on vb "
               << queuedItem->getVBucketId() << "!!! Requeue it...\n";
            LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
            redirty();
        }
    }

private:

    void redirty() {
        if (store->vbMap.isBucketDeletion(vbucket->getId())) {
            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats->decrDiskQueueSize(1);
            return;
        }
        ++stats->flushFailed;
        store->invokeOnLockedStoredValue(queuedItem->getKey(),
                                         queuedItem->getVBucketId(),
                                         &StoredValue::reDirty);
        vbucket->rejectQueue.push(queuedItem);
    }

    const queued_item queuedItem;
    RCPtr<VBucket> &vbucket;
    EventuallyPersistentStore *store;
    EPStats *stats;
    uint64_t cas;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

void EventuallyPersistentStore::flushOneDeleteAll() {
    for (size_t i = 0; i < vbMap.numShards; ++i) {
        KVShard* shard = vbMap.shards[i];
        LockHolder lh(shard->getWriteLock());
        shard->getRWUnderlying()->reset(i == 0);
    }

    bool inverse = true;
    diskFlushAll.compare_exchange_strong(inverse, false);
    stats.decrDiskQueueSize(1);
}

int EventuallyPersistentStore::flushVBucket(uint16_t vbid) {
    KVShard *shard = vbMap.getShard(vbid);
    if (diskFlushAll) {
        if (shard->getId() == EP_PRIMARY_SHARD) {
            flushOneDeleteAll();
        } else {
            // disk flush is pending just return
            return 0;
        }
    }

    if (vbMap.isBucketCreation(vbid)) {
        return RETRY_FLUSH_VBUCKET;
    }

    int items_flushed = 0;
    bool schedule_vb_snapshot = false;
    rel_time_t flush_start = ep_current_time();

    LockHolder lh(shard->getWriteLock());
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        KVStatsCallback cb(this);
        std::vector<queued_item> items;
        KVStore *rwUnderlying = getRWUnderlying(vbid);

        while (!vb->rejectQueue.empty()) {
            items.push_back(vb->rejectQueue.front());
            vb->rejectQueue.pop();
        }

        vb->getBackfillItems(items);
        vb->checkpointManager.getAllItemsForPersistence(items);

        if (!items.empty()) {
            while (!rwUnderlying->begin()) {
                ++stats.beginFailed;
                LOG(EXTENSION_LOG_WARNING, "Failed to start a transaction!!! "
                    "Retry in 1 sec ...");
                sleep(1);
            }
            rwUnderlying->optimizeWrites(items);

            Item *prev = NULL;
            std::list<PersistenceCallback*> pcbs;
            std::vector<queued_item>::iterator it = items.begin();
            for(; it != items.end(); ++it) {
                if ((*it)->getOperation() != queue_op_set &&
                    (*it)->getOperation() != queue_op_del) {
                    continue;
                } else if (!prev || prev->getKey() != (*it)->getKey()) {
                    prev = (*it).get();
                    ++items_flushed;
                    PersistenceCallback *cb = flushOneDelOrSet(*it, vb);
                    if (cb) {
                        pcbs.push_back(cb);
                    }
                    ++stats.flusher_todo;
                } else {
                    stats.decrDiskQueueSize(1);
                    vb->doStatsForFlushing(*(*it), (*it)->size());
                }
            }

            BlockTimer timer(&stats.diskCommitHisto, "disk_commit",
                             stats.timingLog);
            hrtime_t start = gethrtime();

            while (!rwUnderlying->commit(&cb)) {
                ++stats.commitFailed;
                LOG(EXTENSION_LOG_WARNING, "Flusher commit failed!!! Retry in "
                    "1 sec...\n");
                sleep(1);
            }

            while (!pcbs.empty()) {
                delete pcbs.front();
                pcbs.pop_front();
            }

            ++stats.flusherCommits;
            hrtime_t end = gethrtime();
            uint64_t commit_time = (end - start) / 1000000;
            uint64_t trans_time = (end - flush_start) / 1000000;

            lastTransTimePerItem = (items_flushed == 0) ? 0 :
                static_cast<double>(trans_time) /
                static_cast<double>(items_flushed);
            stats.commit_time.store(commit_time);
            stats.cumulativeCommitTime.fetch_add(commit_time);
            stats.cumulativeFlushTime.fetch_add(ep_current_time()
                                                - flush_start);
            stats.flusher_todo.store(0);

        }

        if (vb->rejectQueue.empty()) {
            vb->checkpointManager.itemsPersisted();
        }

        uint64_t chkid = vb->checkpointManager.getPersistenceCursorPreChkId();
        if (vb->rejectQueue.empty()) {
            vb->notifyCheckpointPersisted(engine, chkid, false);
        }

        if (chkid > 0 && chkid != vbMap.getPersistenceCheckpointId(vbid)) {
            vbMap.setPersistenceCheckpointId(vbid, chkid);
            schedule_vb_snapshot = true;
        }

        uint64_t seqno = vb->checkpointManager.getPersistenceCursorSeqno();
        if (vb->rejectQueue.empty()) {
            vb->notifyCheckpointPersisted(engine, seqno, true);
        }

        if (seqno > 0 && seqno != vbMap.getPersistenceSeqno(vbid)) {
            vbMap.setPersistenceSeqno(vbid, seqno);
        }
    }

    if (schedule_vb_snapshot || snapshotVBState) {
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority,
                           shard->getId());
    }

    return items_flushed;
}

PersistenceCallback*
EventuallyPersistentStore::flushOneDelOrSet(const queued_item &qi,
                                            RCPtr<VBucket> &vb) {

    if (!vb) {
        stats.decrDiskQueueSize(1);
        return NULL;
    }

    size_t itemBytes = qi->size();
    int64_t bySeqno = qi->getBySeqno();
    bool deleted = qi->isDeleted();
    rel_time_t queued(qi->getQueuedTime());

    int dirtyAge = ep_current_time() - queued;
    stats.dirtyAgeHisto.add(dirtyAge * 1000000);
    stats.dirtyAge.store(dirtyAge);
    stats.dirtyAgeHighWat.store(std::max(stats.dirtyAge.load(),
                                         stats.dirtyAgeHighWat.load()));

    if (vbMap.isBucketDeletion(qi->getVBucketId())) {
        stats.decrDiskQueueSize(1);
        vb->doStatsForFlushing(*qi, itemBytes);
        return NULL;
    }

    // Wait until the vbucket database is created by the vbucket state
    // snapshot task.
    if (vbMap.isBucketCreation(qi->getVBucketId())) {
        vb->rejectQueue.push(qi);
        ++vb->opsReject;
        return NULL;
    }

    KVStore *rwUnderlying = getRWUnderlying(qi->getVBucketId());
    if (!deleted) {
        // TODO: Need to separate disk_insert from disk_update because
        // bySeqno doesn't give us that information.
        BlockTimer timer(bySeqno == -1 ?
                         &stats.diskInsertHisto : &stats.diskUpdateHisto,
                         bySeqno == -1 ? "disk_insert" : "disk_update",
                         stats.timingLog);
        PersistenceCallback *cb =
            new PersistenceCallback(qi, vb, this, &stats, qi->getCas());
        rwUnderlying->set(*qi, *cb);
        return cb;
    } else {
        BlockTimer timer(&stats.diskDelHisto, "disk_delete",
                         stats.timingLog);
        PersistenceCallback *cb =
            new PersistenceCallback(qi, vb, this, &stats, 0);
        rwUnderlying->del(*qi, *cb);
        return cb;
    }
}

void EventuallyPersistentStore::queueDirty(RCPtr<VBucket> &vb,
                                           StoredValue* v,
                                           bool tapBackfill,
                                           bool notifyReplicator,
                                           bool genBySeqno) {
    if (vb) {
        queued_item qi(v->toItem(false, vb->getId()));
        bool rv = tapBackfill ? vb->queueBackfillItem(qi, genBySeqno) :
                                vb->checkpointManager.queueDirty(vb, qi,
                                                                 genBySeqno);
        v->setBySeqno(qi->getBySeqno());

        if (rv) {
            KVShard* shard = vbMap.getShard(vb->getId());
            shard->getFlusher()->notifyFlushEvent();

        }
        if (!tapBackfill && notifyReplicator) {
            engine.getTapConnMap().notifyVBConnections(vb->getId());
            engine.getUprConnMap().notifyVBConnections(vb->getId());
        }
    }
}

std::map<uint16_t, vbucket_state> EventuallyPersistentStore::loadVBucketState()
{
    return getOneROUnderlying()->listPersistedVbuckets();
}

void EventuallyPersistentStore::warmupCompleted() {
    // Run the vbucket state snapshot job once after the warmup
    scheduleVBSnapshot(Priority::VBucketPersistHighPriority);

    if (engine.getConfiguration().getAlogPath().length() > 0) {
        size_t smin = engine.getConfiguration().getAlogSleepTime();
        setAccessScannerSleeptime(smin);
        Configuration &config = engine.getConfiguration();
        config.addValueChangedListener("alog_sleep_time",
                                       new EPStoreValueChangeListener(*this));
        config.addValueChangedListener("alog_task_time",
                                       new EPStoreValueChangeListener(*this));
    }

    // "0" sleep_time means that the first snapshot task will be executed
    // right after warmup. Subsequent snapshot tasks will be scheduled every
    // 60 sec by default.
    ExecutorPool *iom = ExecutorPool::get();
    ExTask task = new StatSnap(&engine, Priority::StatSnapPriority, 0, false);
    statsSnapshotTaskId = iom->schedule(task, WRITER_TASK_IDX);
}

bool EventuallyPersistentStore::maybeEnableTraffic()
{
    // @todo rename.. skal vaere isTrafficDisabled elns
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());

    if (memoryUsed  >= stats.mem_low_wat) {
        LOG(EXTENSION_LOG_WARNING,
            "Total memory use reached to the low water mark, stop warmup");
        return true;
    } else if (memoryUsed > (maxSize * stats.warmupMemUsedCap)) {
        LOG(EXTENSION_LOG_WARNING,
                "Enough MB of data loaded to enable traffic");
        return true;
    } else if (eviction_policy == VALUE_ONLY &&
               stats.warmedUpValues >=
                               (stats.warmedUpKeys * stats.warmupNumReadCap)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        LOG(EXTENSION_LOG_WARNING,
            "Enough number of items loaded to enable traffic");
        return true;
    }
    return false;
}

bool EventuallyPersistentStore::isWarmingUp() {
    return !warmupTask->isComplete();
}

void EventuallyPersistentStore::stopWarmup(void)
{
    // forcefully stop current warmup task
    if (isWarmingUp()) {
        LOG(EXTENSION_LOG_WARNING, "Stopping warmup while engine is loading "
            "data from underlying storage, shutdown = %s\n",
            stats.isShutdown ? "yes" : "no");
        warmupTask->stop();
    }
}

void EventuallyPersistentStore::setExpiryPagerSleeptime(size_t val) {
    LockHolder lh(expiryPager.mutex);

    if (expiryPager.sleeptime != 0) {
        ExecutorPool::get()->cancel(expiryPager.task);
    }

    expiryPager.sleeptime = val;
    if (val != 0) {
        ExTask expTask = new ExpiredItemPager(&engine, stats,
                                                expiryPager.sleeptime);
        expiryPager.task = ExecutorPool::get()->schedule(expTask,
                                                        NONIO_TASK_IDX);
    }
}

void EventuallyPersistentStore::setAccessScannerSleeptime(size_t val) {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.sleeptime != 0) {
        ExecutorPool::get()->cancel(accessScanner.task);
    }

    // store sleeptime in seconds
    accessScanner.sleeptime = val * 60;
    if (accessScanner.sleeptime != 0) {
        ExTask task = new AccessScanner(*this, stats,
                                        Priority::AccessScannerPriority,
                                        accessScanner.sleeptime);
        accessScanner.task = ExecutorPool::get()->schedule(task,
                                                           AUXIO_TASK_IDX);

        struct timeval tv;
        gettimeofday(&tv, NULL);
        advance_tv(tv, accessScanner.sleeptime);
        stats.alogTime.store(tv.tv_sec);
    }
}

void EventuallyPersistentStore::resetAccessScannerStartTime() {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.sleeptime != 0) {
        ExecutorPool::get()->cancel(accessScanner.task);
        // re-schedule task according to the new task start hour
        ExTask task = new AccessScanner(*this, stats,
                                        Priority::AccessScannerPriority,
                                        accessScanner.sleeptime);
        accessScanner.task = ExecutorPool::get()->schedule(task,
                                                           AUXIO_TASK_IDX);

        struct timeval tv;
        gettimeofday(&tv, NULL);
        advance_tv(tv, accessScanner.sleeptime);
        stats.alogTime.store(tv.tv_sec);
    }
}

void EventuallyPersistentStore::visit(VBucketVisitor &visitor)
{
    size_t maxSize = vbMap.getSize();
    assert(maxSize <= std::numeric_limits<uint16_t>::max());
    for (size_t i = 0; i < maxSize; ++i) {
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = vbMap.getBucket(vbid);
        if (vb) {
            bool wantData = visitor.visitBucket(vb);
            // We could've lost this along the way.
            if (wantData) {
                vb->ht.visit(visitor);
            }
        }
    }
    visitor.complete();
}

VBCBAdaptor::VBCBAdaptor(EventuallyPersistentStore *s,
                         shared_ptr<VBucketVisitor> v,
                         const char *l, const Priority &p, double sleep) :
    GlobalTask(&s->getEPEngine(), p, 0, false), store(s),
    visitor(v), label(l), sleepTime(sleep), currentvb(0)
{
    const VBucketFilter &vbFilter = visitor->getVBucketFilter();
    size_t maxSize = store->vbMap.getSize();
    assert(maxSize <= std::numeric_limits<uint16_t>::max());
    for (size_t i = 0; i < maxSize; ++i) {
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = store->vbMap.getBucket(vbid);
        if (vb && vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBCBAdaptor::run(void) {
    if (!vbList.empty()) {
        currentvb = vbList.front();
        RCPtr<VBucket> vb = store->vbMap.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                snooze(sleepTime, false);
                return true;
            }
            if (visitor->visitBucket(vb)) {
                vb->ht.visit(*visitor);
            }
        }
        vbList.pop();
    }

    bool isdone = vbList.empty();
    if (isdone) {
        visitor->complete();
    }
    return !isdone;
}

VBucketVisitorTask::VBucketVisitorTask(EventuallyPersistentStore *s,
                                       shared_ptr<VBucketVisitor> v,
                                       uint16_t sh, const char *l,
                                       double sleep, bool shutdown):
    GlobalTask(&(s->getEPEngine()), Priority::AccessScannerPriority,
               0, shutdown),
    store(s), visitor(v), label(l), sleepTime(sleep), currentvb(0),
    shardID(sh)
{
    const VBucketFilter &vbFilter = visitor->getVBucketFilter();
    std::vector<int> vbs = store->vbMap.getShard(shardID)->getVBuckets();
    assert(vbs.size() <= std::numeric_limits<uint16_t>::max());
    std::vector<int>::iterator it;
    for (it = vbs.begin(); it != vbs.end(); ++it) {
        uint16_t vbid = static_cast<uint16_t>(*it);
        RCPtr<VBucket> vb = store->vbMap.getBucket(vbid);
        if (vb && vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBucketVisitorTask::run() {
    if (!vbList.empty()) {
        currentvb = vbList.front();
        RCPtr<VBucket> vb = store->vbMap.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                snooze(sleepTime, false);
                return true;
            }
            if (visitor->visitBucket(vb)) {
                vb->ht.visit(*visitor);
            }
        }
        vbList.pop();
    }

    bool isDone = vbList.empty();
    if (isDone) {
        visitor->complete();
    }
    return !isDone;
}

void EventuallyPersistentStore::resetUnderlyingStats(void)
{
    for (size_t i = 0; i < vbMap.numShards; i++) {
        KVShard *shard = vbMap.shards[i];
        shard->getRWUnderlying()->resetStats();
        shard->getROUnderlying()->resetStats();
    }
    auxUnderlying->resetStats();
}

void EventuallyPersistentStore::addKVStoreStats(ADD_STAT add_stat,
                                                const void* cookie) {
    for (size_t i = 0; i < vbMap.numShards; i++) {
        std::stringstream rwPrefix;
        std::stringstream roPrefix;
        rwPrefix << "rw_" << i;
        roPrefix << "ro_" << i;
        vbMap.shards[i]->getRWUnderlying()->addStats(rwPrefix.str(), add_stat,
                                                     cookie);
        vbMap.shards[i]->getROUnderlying()->addStats(roPrefix.str(), add_stat,
                                                     cookie);
    }
}

void EventuallyPersistentStore::addKVStoreTimingStats(ADD_STAT add_stat,
                                                      const void* cookie) {
    for (size_t i = 0; i < vbMap.numShards; i++) {
        std::stringstream rwPrefix;
        std::stringstream roPrefix;
        rwPrefix << "rw_" << i;
        roPrefix << "ro_" << i;
        vbMap.shards[i]->getRWUnderlying()->addTimingStats(rwPrefix.str(),
                                                           add_stat,
                                                           cookie);
        vbMap.shards[i]->getROUnderlying()->addTimingStats(roPrefix.str(),
                                                           add_stat,
                                                           cookie);
    }
}

KVStore *EventuallyPersistentStore::getOneROUnderlying(void) {
    return vbMap.getShard(EP_PRIMARY_SHARD)->getROUnderlying();
}

KVStore *EventuallyPersistentStore::getOneRWUnderlying(void) {
    return vbMap.getShard(EP_PRIMARY_SHARD)->getRWUnderlying();
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::rollback(uint16_t vbid,
                                    uint64_t rollbackSeqno,
                                    shared_ptr<RollbackCB> cb) {
    rollback_error_code err;
    err = vbMap.shards[vbid]->getROUnderlying()->
                              rollback(vbid, rollbackSeqno, cb);

    if (err.first != ENGINE_FAILED) {
        RCPtr<VBucket> vb = vbMap.getBucket(vbid);
        vb->failovers->pruneEntries(err.second);
    }
    return err.first;
}
