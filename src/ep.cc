/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include <vector>
#include <time.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include <functional>

#include "iomanager/iomanager.h"
#include "ep.hh"
#include "flusher.hh"
#include "warmup.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "kvstore.hh"
#include "ep_engine.h"
#include "htresizer.hh"
#include "checkpoint_remover.hh"
#include "access_scanner.hh"
#include "kvshard.hh"

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats &st) : stats(st) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("max_size") == 0) {
            stats.setMaxDataSize(value);
            size_t low_wat = static_cast<size_t>(static_cast<double>(value) * 0.6);
            size_t high_wat = static_cast<size_t>(static_cast<double>(value) * 0.75);
            stats.mem_low_wat.set(low_wat);
            stats.mem_high_wat.set(high_wat);
        } else if (key.compare("mem_low_wat") == 0) {
            stats.mem_low_wat.set(value);
        } else if (key.compare("mem_high_wat") == 0) {
            stats.mem_high_wat.set(value);
        } else if (key.compare("tap_throttle_threshold") == 0) {
            stats.tapThrottleThreshold.set(static_cast<double>(value) / 100.0);
        } else if (key.compare("warmup_min_memory_threshold") == 0) {
            stats.warmupMemUsedCap.set(static_cast<double>(value) / 100.0);
        } else if (key.compare("warmup_min_items_threshold") == 0) {
            stats.warmupNumReadCap.set(static_cast<double>(value) / 100.0);
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
        } else if (key.compare("klog_max_log_size") == 0) {
            store.getMutationLogCompactorConfig().setMaxLogSize(value);
        } else if (key.compare("klog_max_entry_ratio") == 0) {
            store.getMutationLogCompactorConfig().setMaxEntryRatio(value);
        } else if (key.compare("klog_compactor_queue_cap") == 0) {
            store.getMutationLogCompactorConfig().setMaxEntryRatio(value);
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

class VBucketMemoryDeletionCallback : public DispatcherCallback {
public:
    VBucketMemoryDeletionCallback(EventuallyPersistentStore *e, RCPtr<VBucket> &vb) :
    ep(e), vbucket(vb) {}

    bool callback(Dispatcher &, TaskId &) {
        vbucket->ht.clear();
        vbucket.reset();
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Removing (dead) vbucket " << vbucket->getId() << " from memory";
        return ss.str();
    }

private:
    EventuallyPersistentStore *ep;
    RCPtr<VBucket> vbucket;
};

EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine) :
    engine(theEngine), stats(engine.getEpStats()),
    vbMap(theEngine.getConfiguration(), *this),
    mutationLog(theEngine.getConfiguration().getKlogPath(),
                theEngine.getConfiguration().getKlogBlockSize()),
    accessLog(engine.getConfiguration().getAlogPath(),
              engine.getConfiguration().getAlogBlockSize()),
    diskFlushAll(false), bgFetchDelay(0), statsSnapshotTaskId(0), mLogCompactorTaskId(0),
    lastTransTimePerItem(0),snapshotVBState(false)
{
    Configuration &config = engine.getConfiguration();
    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;

    storageProperties = new StorageProperties(true, true, true, true);

    IOManager::get()->registerBucket(ObjectRegistry::getCurrentEngine());

    auxUnderlying = KVStoreFactory::create(stats, config, true);
    assert(auxUnderlying);
    auxIODispatcher = new Dispatcher(theEngine, "AUXIO_Dispatcher");
    nonIODispatcher = new Dispatcher(theEngine, "NONIO_Dispatcher");

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

    stats.mem_low_wat.set(config.getMemLowWat());
    config.addValueChangedListener("mem_low_wat",
                                   new StatsValueChangeListener(stats));

    stats.mem_high_wat.set(config.getMemHighWat());
    config.addValueChangedListener("mem_high_wat",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleThreshold.set(static_cast<double>(config.getTapThrottleThreshold())
                                   / 100.0);
    config.addValueChangedListener("tap_throttle_threshold",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleWriteQueueCap.set(config.getTapThrottleQueueCap());
    config.addValueChangedListener("tap_throttle_queue_cap",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("tap_throttle_cap_pcnt",
                                   new EPStoreValueChangeListener(*this));

    setBGFetchDelay(config.getBgFetchDelay());
    config.addValueChangedListener("bg_fetch_delay",
                                   new EPStoreValueChangeListener(*this));

    stats.warmupMemUsedCap.set(static_cast<double>(config.getWarmupMinMemoryThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_memory_threshold",
                                   new StatsValueChangeListener(stats));
    stats.warmupNumReadCap.set(static_cast<double>(config.getWarmupMinItemsThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_items_threshold",
                                   new StatsValueChangeListener(stats));

    double mem_threshold = static_cast<double>(config.getMutationMemThreshold()) / 100;
    StoredValue::setMutationMemoryThreshold(mem_threshold);
    config.addValueChangedListener("mutation_mem_threshold",
                                   new EPStoreValueChangeListener(*this));

    if (config.isVb0()) {
        RCPtr<VBucket> vb(new VBucket(0, vbucket_state_active, stats,
                                      engine.getCheckpointConfig(), vbMap.getShard(0)));
        vbMap.addBucket(vb);
    }

    try {
        mutationLog.open();
        assert(theEngine.getConfiguration().getKlogPath() == ""
               || mutationLog.isEnabled());
    } catch(MutationLog::ReadException e) {
        LOG(EXTENSION_LOG_WARNING,
            "Error opening mutation log:  %s (disabling)", e.what());
        mutationLog.disable();
    }

    bool syncset(mutationLog.setSyncConfig(theEngine.getConfiguration().getKlogSync()));
    assert(syncset);

    mlogCompactorConfig.setMaxLogSize(config.getKlogMaxLogSize());
    config.addValueChangedListener("klog_max_log_size",
                                   new EPStoreValueChangeListener(*this));
    mlogCompactorConfig.setMaxEntryRatio(config.getKlogMaxEntryRatio());
    config.addValueChangedListener("klog_max_entry_ratio",
                                   new EPStoreValueChangeListener(*this));
    mlogCompactorConfig.setQueueCap(config.getKlogCompactorQueueCap());
    config.addValueChangedListener("klog_compactor_queue_cap",
                                   new EPStoreValueChangeListener(*this));
    mlogCompactorConfig.setSleepTime(config.getKlogCompactorStime());

    // @todo - Ideally we should run the warmup thread in it's own
    //         thread so that it won't block the flusher (in the write
    //         thread), but we can't put it in the RO dispatcher either,
    //         because that would block the background fetches..
    warmupTask = new Warmup(this, auxIODispatcher);
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

    startDispatcher();
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
    startNonIODispatcher();

    WarmupWaitListener warmupListener(*warmupTask, config.isWaitforwarmup());
    warmupTask->addWarmupStateListener(&warmupListener);
    warmupTask->start();
    warmupListener.wait();
    warmupTask->removeWarmupStateListener(&warmupListener);

    if (config.isFailpartialwarmup() && stats.warmOOM > 0) {
        LOG(EXTENSION_LOG_WARNING,
            "Warmup failed to load %d records due to OOM, exiting.\n",
            static_cast<unsigned int>(stats.warmOOM));
        return false;
    }

    size_t expiryPagerSleeptime = config.getExpPagerStime();

    shared_ptr<DispatcherCallback> cb(new ItemPager(this, stats));
    nonIODispatcher->schedule(cb, NULL, Priority::ItemPagerPriority, 10);

    setExpiryPagerSleeptime(expiryPagerSleeptime);
    config.addValueChangedListener("exp_pager_stime",
                                    new EPStoreValueChangeListener(*this));

    shared_ptr<DispatcherCallback> htr(new HashtableResizer(this));
    nonIODispatcher->schedule(htr, NULL, Priority::HTResizePriority, 10);

    size_t checkpointRemoverInterval = config.getChkRemoverStime();
    shared_ptr<DispatcherCallback> chk_cb(new ClosedUnrefCheckpointRemover(this,
                                                                           stats,
                                                                           checkpointRemoverInterval));
    nonIODispatcher->schedule(chk_cb, NULL,
                              Priority::CheckpointRemoverPriority,
                              checkpointRemoverInterval);

    if (mutationLog.isEnabled()) {
        IOManager* iom = IOManager::get();
        mLogCompactorTaskId =
            iom->scheduleMLogCompactor(&engine, Priority::MutationLogCompactorPriority,
                                       static_cast<int>(mlogCompactorConfig.getSleepTime()));
    }
    return true;
}

EventuallyPersistentStore::~EventuallyPersistentStore() {
    stopWarmup();
    stopFlusher();
    stopBgFetcher();

    IOManager::get()->cancel(statsSnapshotTaskId);
    IOManager::get()->cancel(mLogCompactorTaskId);
    IOManager::get()->unregisterBucket(ObjectRegistry::getCurrentEngine());

    auxIODispatcher->stop(stats.forceShutdown);
    nonIODispatcher->stop(stats.forceShutdown);

    delete conflictResolver;
    delete warmupTask;
    delete auxIODispatcher;
    delete nonIODispatcher;
    delete auxUnderlying;
    delete storageProperties;
}

void EventuallyPersistentStore::startDispatcher() {
    auxIODispatcher->start();
}

void EventuallyPersistentStore::startNonIODispatcher() {
    nonIODispatcher->start();
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
                "Warning: attempted to resume flusher in state [%s], shard = %d",
                flusher->stateName(), i);
            rv = false;
        }
    }
    return rv;
}

void EventuallyPersistentStore::wakeUpFlusher() {
    if (stats.diskQueueSize.get() == 0) {
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

void EventuallyPersistentStore::firePendingVBucketOps() {
    uint16_t i;
    for (i = 0; i < vbMap.getSize(); i++) {
        RCPtr<VBucket> vb = getVBucket(i, vbucket_state_active);
        if (vb) {
            vb->fireAllOps(engine);
        }
    }
}

/// @cond DETAILS
/**
 * Inner loop of deleteExpiredItems.
 */
class Deleter {
public:
    Deleter(EventuallyPersistentStore *ep) : e(ep), startTime(ep_real_time()) {}
    void operator() (std::pair<uint16_t, std::string> vk) {
        RCPtr<VBucket> vb = e->getVBucket(vk.first);
        if (vb) {
            int bucket_num(0);
            e->incExpirationStat(vb);
            LockHolder lh = vb->ht.getLockedBucket(vk.second, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(vk.second, bucket_num, true, false);
            if (v && v->isTempItem()) {
                // This is a temporary item whose background fetch for metadata
                // has completed.
                bool deleted = vb->ht.unlocked_del(vk.second, bucket_num);
                assert(deleted);
            } else if (v && v->isExpired(startTime) && !v->isDeleted()) {
                vb->ht.unlocked_softDelete(v, 0);
                e->queueDirty(vb, vk.second, vb->getId(), queue_op_del,
                              v->getSeqno(), false);
            }
        }
    }

private:
    EventuallyPersistentStore *e;
    time_t                     startTime;
};
/// @endcond

void
EventuallyPersistentStore::deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &keys) {
    // This can be made a lot more efficient, but I'd rather see it
    // show up in a profiling report first.
    std::for_each(keys.begin(), keys.end(), Deleter(this));
}

StoredValue *EventuallyPersistentStore::fetchValidValue(RCPtr<VBucket> &vb,
                                                        const std::string &key,
                                                        int bucket_num,
                                                        bool wantDeleted,
                                                        bool trackReference,
                                                        bool queueExpired) {
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, wantDeleted, trackReference);
    if (v && !v->isDeleted()) { // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            incExpirationStat(vb, false);
            vb->ht.unlocked_softDelete(v, 0);
            if (queueExpired) {
                queueDirty(vb, key, vb->getId(), queue_op_del, v->getSeqno());
            }
            if (wantDeleted) {
                return v;
            }
            return NULL;
        }
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
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
            if (v->ejectValue(stats, vb->ht)) {
                *msg = "Ejected.";
            } else {
                *msg = "Can't eject: Dirty or a small object.";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
            }
        } else {
            *msg = "Already ejected.";
        }
    } else {
        *msg = "Not found.";
        rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    }

    return rv;
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

    mutation_type_t mtype = vb->ht.set(itm, nru);
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
        // Even if the item was dirty, push it into the vbucket's open checkpoint.
    case WAS_CLEAN:
        queueDirty(vb, itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno());
        break;
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
    if (!vb || vb->getState() == vbucket_state_dead || vb->getState() == vbucket_state_replica) {
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

    switch (vb->ht.add(itm)) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        queueDirty(vb, itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno());
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTAPBackfillItem(const Item &itm, bool meta,
                                                                uint8_t nru) {

    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb ||
        vb->getState() == vbucket_state_dead ||
        (vb->getState() == vbucket_state_active &&
         !engine.getCheckpointConfig().isInconsistentSlaveCheckpoint())) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    mutation_type_t mtype;

    if (meta) {
        mtype = vb->ht.set(itm, 0, true, true, nru);
    } else {
        mtype = vb->ht.set(itm, nru);
    }
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
        // If a given backfill item is already dirty, don't queue the same item again.
        break;
    case NOT_FOUND:
        // FALLTHROUGH
    case WAS_CLEAN:
        queueDirty(vb, itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno(), true);
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}


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
                vb_state.checkpointId = vbuckets.getPersistenceCheckpointId(vb->getId());
                vb_state.maxDeletedSeqno = 0;
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

    VBucketStateVisitor v(vbMap, shard->getId());
    visit(v);
    hrtime_t start = gethrtime();
    KVStore *rwUnderlying = shard->getRWUnderlying();
    if (!rwUnderlying->snapshotVBuckets(v.states)) {
        LOG(EXTENSION_LOG_WARNING,
            "VBucket snapshot task failed!!! Rescheduling");
        scheduleVBSnapshot(priority, shard->getId());
    } else {
        stats.snapshotVbucketHisto.add((gethrtime() - start) / 1000);
    }

    if (priority == Priority::VBucketPersistHighPriority) {
        std::vector<int> vbIds = shard->getVBuckets();
        for (size_t i = 0; i < vbIds.size(); ++i) {
            uint16_t id = static_cast<uint16_t>(vbIds[i]);
            vbMap.setBucketCreation(id, false);
        }
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                             vbucket_state_t to) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb && to == vb->getState()) {
        return ENGINE_SUCCESS;
    }

    uint16_t shardId = vbMap.getShard(vbid)->getId();
    if (vb) {
        vb->setState(to, engine.getServerApi());
        lh.unlock();
        if (vb->getState() == vbucket_state_pending && to == vbucket_state_active) {
            engine.notifyNotificationThread();
        }
        scheduleVBSnapshot(Priority::VBucketPersistLowPriority, shardId);
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats, engine.getCheckpointConfig(),
                                         vbMap.getShard(vbid)));
        // The first checkpoint for active vbucket should start with id 2.
        uint64_t start_chk_id = (to == vbucket_state_active) ? 2 : 0;
        newvb->checkpointManager.setOpenCheckpointId(start_chk_id);
        if (vbMap.addBucket(newvb) == ENGINE_ERANGE) {
            lh.unlock();
            return ENGINE_ERANGE;
        }
        vbMap.setPersistenceCheckpointId(vbid, 0);
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
                IOManager::get()->scheduleVBSnapshot(&engine, p, i);
            }
        }
    } else {
        for (size_t i = 0; i < vbMap.numShards; ++i) {
            shard = vbMap.shards[i];
            if (shard->setLowPriorityVbSnapshotFlag(true)) {
                IOManager::get()->scheduleVBSnapshot(&engine, p, i);
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
            IOManager::get()->scheduleVBSnapshot(&engine, p, shardId);
        }
    } else {
        if (shard->setLowPriorityVbSnapshotFlag(true)) {
            IOManager::get()->scheduleVBSnapshot(&engine, p, shardId);
        }
    }
}

bool EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid,
                                                        const void* cookie,
                                                        bool recreate) {
    LockHolder lh(vbsetMutex);

    hrtime_t start_time(gethrtime());
    vbucket_del_result result = vbucket_del_invalid;
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead || vbMap.isBucketDeletion(vbid)) {
        lh.unlock();
        KVStore *rwUnderlying = getRWUnderlying(vbid);
        if (rwUnderlying->delVBucket(vbid, recreate)) {
            vbMap.setBucketDeletion(vbid, false);
            mutationLog.deleteAll(vbid);
            // This is happening in an independent transaction, so
            // we're going go ahead and commit it out.
            mutationLog.commit1();
            mutationLog.commit2();
            ++stats.vbucketDeletions;
            result = vbucket_del_success;
        } else {
            ++stats.vbucketDeletionFail;
            result =  vbucket_del_fail;
        }
    }

    if (result == vbucket_del_success || result == vbucket_del_invalid) {
        hrtime_t spent(gethrtime() - start_time);
        hrtime_t wall_time = spent / 1000;
        BlockTimer::log(spent, "disk_vb_del", stats.timingLog);
        stats.diskVBDelHisto.add(wall_time);
        stats.vbucketDelMaxWalltime.setIfBigger(wall_time);
        stats.vbucketDelTotWalltime.incr(wall_time);
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
    shared_ptr<DispatcherCallback> mem_cb(new VBucketMemoryDeletionCallback(this, vb));
    nonIODispatcher->schedule(mem_cb, NULL, Priority::VBMemoryDeletionPriority, delay, false);

    uint16_t vbid = vb->getId();
    if (vbMap.setBucketDeletion(vbid, true)) {
        IOManager::get()->scheduleVBDelete(&engine, cookie, vbid,
                                           Priority::VBucketDeletionPriority,
                                           vbMap.getShard(vbid)->getId(),
                                           recreate, delay);
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::deleteVBucket(uint16_t vbid, const void* c) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

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

bool EventuallyPersistentStore::resetVBucket(uint16_t vbid) {
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        if (vb->ht.getNumItems() == 0) { // Already reset?
            return true;
        }

        vbMap.removeBucket(vbid);
        lh.unlock();

        vbucket_state_t vbstate = vb->getState();
        std::list<std::string> tap_cursors = vb->checkpointManager.getTAPCursorNames();
        // Delete the vbucket database file and recreate the empty file
        scheduleVBDeletion(vb, NULL, 0, true);
        setVBucketState(vbid, vbstate);

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
              engine.getStats(&snap, "tap", 3, add_stat) == ENGINE_SUCCESS;
    if (rv && stats.shutdown.isShutdown) {
        snap.smap["ep_force_shutdown"] = stats.forceShutdown ? "true" : "false";
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
        stats.bgWait += w;
        stats.bgMinWait.setIfLess(w);
        stats.bgMaxWait.setIfBigger(w);

        hrtime_t l = (stop - start) / 1000;
        BlockTimer::log(stop - start, "bgload", stats.timingLog);
        stats.bgLoadHisto.add(l);
        stats.bgLoad += l;
        stats.bgMinLoad.setIfLess(l);
        stats.bgMaxLoad.setIfBigger(l);
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
    if (vb && vb->getState() == vbucket_state_active) {
        int bucket_num(0);
        LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = fetchValidValue(vb, key, bucket_num, true);
        if (isMeta) {
            if (v && !v->isResident()) {
                if (v->unlocked_restoreMeta(gcb.val.getValue(),
                                            gcb.val.getStatus())) {
                    status = ENGINE_SUCCESS;
                }
            }
        } else {
            if (v && !v->isResident()) {
                if (gcb.val.getStatus() == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(gcb.val.getValue(), stats, vb->ht);
                    assert(v->isResident());
                    if (v->getExptime() != gcb.val.getValue()->getExptime()) {
                        assert(v->isDirty());
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, key, vbucket, queue_op_set,
                                v->getSeqno());
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: failed background fetch for vb=%d seq=%d "
                        "key=%s", vbucket, v->getId(), key.c_str());
                    status = ENGINE_TMPFAIL;
                }
            }
        }
    }

    lh.unlock();

    hrtime_t stop = gethrtime();
    updateBGStats(init, start, stop);
    bgFetchQueue--;

    delete gcb.val.getValue();
    engine.notifyIOComplete(cookie, status);
}

void EventuallyPersistentStore::completeBGFetchMulti(uint16_t vbId,
                                 std::vector<VBucketBGFetchItem *> &fetchedItems,
                                 hrtime_t startTime)
{
    stats.bg_fetched += fetchedItems.size();
    RCPtr<VBucket> vb = getVBucket(vbId);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING,
            "EP Store completes %d of batched background fetch for "
            "for vBucket = %d that is already deleted\n",
            (int)fetchedItems.size(), vbId);
        return;
    }

    std::vector<VBucketBGFetchItem *>::iterator itemItr = fetchedItems.begin();
    for (; itemItr != fetchedItems.end(); itemItr++) {
        GetValue &value = (*itemItr)->value;
        ENGINE_ERROR_CODE status = value.getStatus();
        Item *fetchedValue = value.getValue();
        const std::string &key = (*itemItr)->key;

        if (vb->getState() == vbucket_state_active ||
            vb->getState() == vbucket_state_replica) {
            int bucket = 0;
            LockHolder blh = vb->ht.getLockedBucket(key, &bucket);
            StoredValue *v = fetchValidValue(vb, key, bucket, true);
            if (v && !v->isResident()) {
                if (status == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(fetchedValue, stats, vb->ht);
                    assert(v->isResident());
                    if (v->getExptime() != fetchedValue->getExptime()) {
                        assert(v->isDirty());
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, key, vbId, queue_op_set, v->getSeqno());
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: failed background fetch for vb=%d seq=%d "
                        "key=%s", vbId, v->getId(), key.c_str());
                    status = ENGINE_TMPFAIL;
                }
            }
        }

        hrtime_t endTime = gethrtime();
        updateBGStats((*itemItr)->initTime, startTime, endTime);
        engine.notifyIOComplete((*itemItr)->cookie, status);
        std::stringstream ss;
        ss << "Completed a background fetch, now at "
           << vb->numPendingBGFetchItems() << std::endl;
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
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

    // NOTE: mutil-fetch feature will be disabled for metadata
    // read until MB-5808 is fixed
    if (multiBGFetchEnabled() && !isMeta) {
        RCPtr<VBucket> vb = getVBucket(vbucket);
        assert(vb);
        KVShard *myShard = vbMap.getShard(vbucket);

        // schedule to the current batch of background fetch of the given vbucket
        VBucketBGFetchItem * fetchThis = new VBucketBGFetchItem(key, rowid, cookie);
        vb->queueBGFetchItem(fetchThis, myShard->getBgFetcher());
        ss << "Queued a background fetch, now at "
           << vb->numPendingBGFetchItems() << std::endl;
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
    } else {
        bgFetchQueue++;
        IOManager* iom = IOManager::get();
        iom->scheduleBGFetch(&engine, key, vbucket, rowid, cookie, isMeta,
                             Priority::BgFetcherGetMetaPriority,
                             vbMap.getShard(vbucket)->getId(), 0,
                             bgFetchDelay);
        ss << "Queued a background fetch, now at " << bgFetchQueue.get()
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
    StoredValue *v = fetchValidValue(vb, key, bucket_num, false, trackReference);

    if (v) {
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, v->getId(), cookie);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId(), true,
                            v->getNRUValue());
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getId(), false, v->getNRUValue());
        return rv;
    } else {
        GetValue rv;
        return rv;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::getMetaData(const std::string &key,
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
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, trackReferenced);

    if (v) {
        stats.numOpsGetMeta++;

        if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return ENGINE_KEY_ENOENT;
        } else {
            if (v->isDeleted() || v->isExpired(ep_real_time())) {
                deleted |= GET_META_ITEM_DELETED_FLAG;
            }
            metadata.cas = v->getCas();
            metadata.flags = v->getFlags();
            metadata.exptime = v->getExptime();
            metadata.seqno = v->getSeqno();
            return ENGINE_SUCCESS;
        }
    } else {
        // The key wasn't found. However, this may be because it was previously
        // deleted. So, add a temporary item corresponding to the key to the
        // hash table and schedule a background fetch for its metadata from the
        // persistent store. The item's state will be updated after the fetch
        // completes and the item will automatically expire after a pre-
        // determined amount of time.
        add_type_t rv = vb->ht.unlocked_addTempDeletedItem(bucket_num, key);
        switch(rv) {
        case ADD_NOMEM:
            return ENGINE_ENOMEM;
        case ADD_EXISTS:
        case ADD_UNDEL:
            // Since the hashtable bucket is locked, we should never get here
            abort();
        case ADD_SUCCESS:
            bgFetch(key, vbucket, -1, cookie, true);
        }
        return ENGINE_EWOULDBLOCK;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setWithMeta(const Item &itm,
                                                         uint64_t cas,
                                                         const void *cookie,
                                                         bool force,
                                                         bool allowExisting,
                                                         uint8_t nru)
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
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true, false);

    if (!force) {
        if (v)  {
            if (!conflictResolver->resolve(v, itm.getMetaData())) {
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            add_type_t rv = vb->ht.unlocked_addTempDeletedItem(bucket_num,
                                                               itm.getKey());
            switch(rv) {
            case ADD_NOMEM:
                return ENGINE_ENOMEM;
            case ADD_EXISTS:
            case ADD_UNDEL:
                // Since the hashtable bucket is locked, we shouldn't get here
                abort();
            case ADD_SUCCESS:
                bgFetch(itm.getKey(), itm.getVBucketId(), -1, cookie, true);
            }
            return ENGINE_EWOULDBLOCK;
        }
    }

    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, cas, allowExisting,
                                                true, nru);
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
        queueDirty(vb, itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno());
        break;
    case NOT_FOUND:
        ret = ENGINE_KEY_ENOENT;
        break;
    }

    return ret;
}

GetValue EventuallyPersistentStore::getAndUpdateTtl(const std::string &key,
                                                    uint16_t vbucket,
                                                    const void *cookie,
                                                    bool queueBG,
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
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        if (v->isLocked(ep_current_time())) {
            GetValue rv(NULL, ENGINE_KEY_EEXISTS, 0);
            return rv;
        }
        bool exptime_mutated = exptime != v->getExptime() ? true : false;
        if (exptime_mutated) {
           v->markDirty();
        }
        v->setExptime(exptime);

        if (v->isResident()) {
            if (exptime_mutated) {
                // persist the itme in the underlying storage for
                // mutated exptime
                queueDirty(vb, key, vbucket, queue_op_set, v->getSeqno());
            }
        } else {
            if (queueBG || exptime_mutated) {
                // in case exptime_mutated, first do bgFetch then
                // persist mutated exptime in the underlying storage
                bgFetch(key, vbucket, v->getId(), cookie);
                return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId());
            } else {
                // You didn't want the item anyway...
                return GetValue(NULL, ENGINE_SUCCESS, v->getId());
            }
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getId());
        return rv;
    } else {
        GetValue rv;
        return rv;
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
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        bgFetchQueue++;
        assert(bgFetchQueue > 0);
        IOManager* iom = IOManager::get();
        iom->scheduleVKeyFetch(&engine, key, vbucket, v->getId(), cookie,
                               Priority::VKeyStatBgFetcherPriority,
                               vbMap.getShard(vbucket)->getId(), 0,
                               bgFetchDelay);
        return ENGINE_EWOULDBLOCK;
    } else {
        return ENGINE_KEY_ENOENT;
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
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            GetValue rv;
            cb.callback(rv);
            return false;
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {

            if (cookie) {
                bgFetch(key, vbucket, v->getId(), cookie);
            }
            GetValue rv(NULL, ENGINE_EWOULDBLOCK, v->getId());
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

    } else {
        GetValue rv;
        cb.callback(rv);
    }
    return true;
}

StoredValue* EventuallyPersistentStore::getStoredValue(const std::string &key,
                                                       uint16_t vbucket,
                                                       bool honorStates) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(honorStates && vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return NULL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    return fetchValidValue(vb, key, bucket_num);
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
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        if (v->isLocked(currentTime)) {
            if (v->getCas() == cas) {
                v->unlock();
                return ENGINE_SUCCESS;
            }
        }
        return ENGINE_TMPFAIL;
    }

    return ENGINE_KEY_ENOENT;
}


ENGINE_ERROR_CODE EventuallyPersistentStore::getKeyStats(const std::string &key,
                                            uint16_t vbucket,
                                            struct key_stats &kstats,
                                            bool wantsDeleted)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, wantsDeleted);

    if (v) {
        kstats.logically_deleted = v->isDeleted();
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        kstats.vb_state = vb->getState();
        return ENGINE_SUCCESS;
    }
    return ENGINE_KEY_ENOENT;
}

std::string EventuallyPersistentStore::validateKey(const std::string &key,
                                                   uint16_t vbucket,
                                                   Item &diskItem) {
    int bucket_num(0);
    RCPtr<VBucket> vb = getVBucket(vbucket);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, false,
                                     false, true);

    if (v) {
        if (diskItem.getNBytes() != v->valLength()) {
            return "length_mismatch";
        } else if (diskItem.getFlags() != v->getFlags()) {
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
                                                        bool use_meta,
                                                        bool update_meta,
                                                        ItemMetaData *itemMeta,
                                                        bool tapBackfill)
{
    uint64_t newSeqno = itemMeta->seqno;
    uint64_t newCas   = itemMeta->cas;
    uint32_t newFlags = itemMeta->flags;
    time_t newExptime = itemMeta->exptime;

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
    // If use_meta is true (delete_with_meta), we'd like to look for the key
    // with the wantsDeleted flag set to true in case a prior get_meta has
    // created a temporary item for the key.
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, use_meta, false);
    if (use_meta && !force) {
        if (v)  {
            if (!conflictResolver->resolve(v, *itemMeta)) {
                return ENGINE_KEY_EEXISTS;
            }
        } else{
            add_type_t rv = vb->ht.unlocked_addTempDeletedItem(bucket_num, key);
            switch(rv) {
            case ADD_NOMEM:
                return ENGINE_ENOMEM;
            case ADD_EXISTS:
            case ADD_UNDEL:
                // Since the hashtable bucket is locked, we shouldn't get here
                abort();
            case ADD_SUCCESS:
                bgFetch(key, vbucket, -1, cookie, true);
            }
            return ENGINE_EWOULDBLOCK;
        }
    } else if (!v) {
        if (vb->getState() != vbucket_state_active && force) {
            queueDirty(vb, key, vbucket, queue_op_del, newSeqno, tapBackfill);
        }
        return ENGINE_KEY_ENOENT;
    }

    mutation_type_t delrv;
    if (use_meta) {
        delrv = vb->ht.unlocked_softDelete(v, *cas, newSeqno, use_meta, newCas,
                                           newFlags, newExptime);
    } else {
        delrv = vb->ht.unlocked_softDelete(v, *cas);
    }

    if (update_meta) {
        itemMeta->seqno = v->getSeqno();
        itemMeta->cas = v->getCas();
        itemMeta->flags = v->getFlags();
        itemMeta->exptime = v->getExptime();
    }

    *cas = v->getCas();

    ENGINE_ERROR_CODE rv;
    if (delrv == NOT_FOUND || delrv == INVALID_CAS) {
        rv = (delrv == INVALID_CAS) ? ENGINE_KEY_EEXISTS : ENGINE_KEY_ENOENT;
    } else if (delrv == IS_LOCKED) {
        rv = ENGINE_TMPFAIL;
    } else { // WAS_CLEAN or WAS_DIRTY
        rv = ENGINE_SUCCESS;
    }

    if (delrv == WAS_CLEAN || delrv == WAS_DIRTY || delrv == NOT_FOUND) {
        uint64_t seqnum = v ? v->getSeqno() : 1;
        lh.unlock();
        queueDirty(vb, key, vbucket, queue_op_del, seqnum, tapBackfill);
    }
    return rv;
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
    if (diskFlushAll.cas(false, true)) {
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
                        EventuallyPersistentStore *st, MutationLog *ml,
                        EPStats *s, uint64_t c) :
        queuedItem(qi), vbucket(vb), store(st), mutationLog(ml),
        stats(s), cas(c) {
        assert(vb);
        assert(s);
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
            StoredValue *v = store->fetchValidValue(vbucket, queuedItem->getKey(),
                                                    bucket_num, true, false);
            if (v && value.second > 0) {
                if (v->isPendingId()) {
                    mutationLog->newItem(queuedItem->getVBucketId(), queuedItem->getKey(),
                                         value.second);
                    ++stats->newItems;
                }
                v->setId(value.second);
            }
            if (v && v->getCas() == cas) {
                // mark this item clean only if current and stored cas
                // value match
                v->markClean();
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            --stats->diskQueueSize;
            assert(stats->diskQueueSize < GIGANTOR);
            stats->totalPersisted++;
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                int bucket_num(0);
                LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vbucket, queuedItem->getKey(),
                                                        bucket_num, true, false);
                if (v) {
                    std::stringstream ss;
                    ss << "Persisting ``" << queuedItem->getKey() << "'' on vb"
                       << queuedItem->getVBucketId() << " (rowid=" << v->getId()
                       << ") returned 0 updates\n";
                    LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Error persisting now missing ``%s'' from vb%d",
                        queuedItem->getKey().c_str(), queuedItem->getVBucketId());
                }

                vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
                --stats->diskQueueSize;
                assert(stats->diskQueueSize < GIGANTOR);
            } else {
                std::stringstream ss;
                ss << "Fatal error in persisting SET ``" << queuedItem->getKey() << "'' on vb "
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
            mutationLog->delItem(queuedItem->getVBucketId(), queuedItem->getKey());
            // We have succesfully removed an item from the disk, we
            // may now remove it from the hash table.
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
            StoredValue *v = store->fetchValidValue(vbucket, queuedItem->getKey(),
                                                    bucket_num, true, false);
            if (v && v->isDeleted()) {
                bool deleted = vbucket->ht.unlocked_del(queuedItem->getKey(),
                                                        bucket_num);
                assert(deleted);
            } else if (v) {
                v->clearId();
            }

            if (value > 0) {
                ++stats->totalPersisted;
                ++stats->delItems;
                ++vbucket->opsDelete;
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            --stats->diskQueueSize;
            assert(stats->diskQueueSize < GIGANTOR);
        } else {
            std::stringstream ss;
            ss << "Fatal error in persisting DELETE ``" << queuedItem->getKey() << "'' on vb "
               << queuedItem->getVBucketId() << "!!! Requeue it...\n";
            LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
            redirty();
        }
    }

private:

    void redirty() {
        if (store->vbMap.isBucketDeletion(vbucket->getId())) {
            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            --stats->diskQueueSize;
            assert(stats->diskQueueSize < GIGANTOR);
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
    MutationLog *mutationLog;
    EPStats *stats;
    uint64_t cas;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

void EventuallyPersistentStore::flushOneDeleteAll() {
    // just pick one underlying is enough to
    // reset entire underlying database store
    vbMap.shards[EP_PRIMARY_SHARD]->getRWUnderlying()->reset();

    // Log a flush of every known vbucket.
    std::vector<int> vbs(vbMap.getBuckets());
    for (std::vector<int>::iterator it(vbs.begin()); it != vbs.end(); ++it) {
        mutationLog.deleteAll(static_cast<uint16_t>(*it));
    }
    // This is happening in an independent transaction, so we're going
    // go ahead and commit it out.
    mutationLog.commit1();
    mutationLog.commit2();
    diskFlushAll.cas(true, false);
    --stats.diskQueueSize;
    assert(stats.diskQueueSize < GIGANTOR);
}

int EventuallyPersistentStore::flushVBucket(uint16_t vbid) {
    if (diskFlushAll) {
        if (vbMap.getShard(vbid)->getId() == EP_PRIMARY_SHARD) {
            flushOneDeleteAll();
        } else {
            // disk flush is pending just return
            return 0;
        }
    }

    int items_flushed = 0;
    bool schedule_vb_snapshot = false;
    rel_time_t flush_start = ep_current_time();
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb && !vbMap.isBucketCreation(vbid)) {
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

            QueuedItem *prev = NULL;
            std::list<PersistenceCallback*> pcbs;
            std::vector<queued_item>::iterator it = items.begin();
            for(; it != items.end(); ++it) {
                if ((*it)->getOperation() != queue_op_set &&
                    (*it)->getOperation() != queue_op_del &&
                    (*it)->getOperation() != queue_op_empty) {
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
                    --stats.diskQueueSize;
                    vb->doStatsForFlushing(*(*it), (*it)->size());
                    assert(stats.diskQueueSize < GIGANTOR);
                }
            }

            BlockTimer timer(&stats.diskCommitHisto, "disk_commit",
                             stats.timingLog);
            hrtime_t start = gethrtime();

            mutationLog.commit1();
            while (!rwUnderlying->commit()) {
                ++stats.commitFailed;
                LOG(EXTENSION_LOG_WARNING, "Flusher commit failed!!! Retry in "
                    "1 sec...\n");
                sleep(1);
            }

            while (!pcbs.empty()) {
                delete pcbs.front();
                pcbs.pop_front();
            }

            mutationLog.commit2();
            ++stats.flusherCommits;
            hrtime_t end = gethrtime();
            uint64_t commit_time = (end - start) / 1000000;
            uint64_t trans_time = (end - flush_start) / 1000000;

            lastTransTimePerItem = (items_flushed == 0) ? 0 :
                static_cast<double>(trans_time) /
                static_cast<double>(items_flushed);
            stats.commit_time.set(commit_time);
            stats.cumulativeCommitTime.incr(commit_time);
            stats.cumulativeFlushTime.incr(ep_current_time() - flush_start);
            stats.flusher_todo.set(0);
        }

        uint64_t chkid = vb->checkpointManager.getPersistenceCursorPreChkId();
        if (vb->rejectQueue.empty()) {
            vb->notifyCheckpointPersisted(engine, chkid);
        }

        if (chkid > 0 && chkid != vbMap.getPersistenceCheckpointId(vbid)) {
            vbMap.setPersistenceCheckpointId(vbid, chkid);
            schedule_vb_snapshot = true;
        }
    }

    if (schedule_vb_snapshot || snapshotVBState) {
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority,
                           vbMap.getShard(vbid)->getId());
    }

    return items_flushed;
}

// While I actually know whether a delete or set was intended, I'm
// still a bit better off running the older code that figures it out
// based on what's in memory.
PersistenceCallback*
EventuallyPersistentStore::flushOneDelOrSet(const queued_item &qi,
                                            RCPtr<VBucket> &vb) {

    if (!vb) {
        --stats.diskQueueSize;
        assert(stats.diskQueueSize < GIGANTOR);
        return NULL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(qi->getKey(), &bucket_num);
    StoredValue *v = fetchValidValue(vb, qi->getKey(), bucket_num, true, false, false);

    size_t itemBytes = qi->size();

    bool found = v != NULL;
    int64_t rowid = found ? v->getId() : -1;
    bool deleted = found && v->isDeleted();
    bool isDirty = found && v->isDirty();
    rel_time_t queued(qi->getQueuedTime());

    Item itm(qi->getKey(),
             found ? v->getFlags() : 0,
             found ? v->getExptime() : 0,
             found ? v->getValue() : value_t(NULL),
             found ? v->getCas() : Item::nextCas(),
             rowid,
             qi->getVBucketId(),
             found ? v->getSeqno() : qi->getSeqno());

    if (!deleted && isDirty && v->isExpired(ep_real_time() + itemExpiryWindow)) {
        ++stats.flushExpired;
        --stats.diskQueueSize;
        assert(stats.diskQueueSize < GIGANTOR);
        vb->doStatsForFlushing(*qi, itemBytes);
        v->markClean();
        v->clearId();
        return NULL;
    }

    if (isDirty) {
        if (!v->isPendingId()) {
            int dirtyAge = ep_current_time() - queued;
            stats.dirtyAgeHisto.add(dirtyAge * 1000000);
            stats.dirtyAge.set(dirtyAge);
            stats.dirtyAgeHighWat.set(std::max(stats.dirtyAge.get(),
                                               stats.dirtyAgeHighWat.get()));
        } else {
            isDirty = false;
            v->reDirty();
            vb->rejectQueue.push(qi);
            ++vb->opsReject;
            return NULL;
        }
    }

    KVStore *rwUnderlying = getRWUnderlying(qi->getVBucketId());
    if (isDirty && !deleted) {
        if (vbMap.isBucketDeletion(qi->getVBucketId())) {
            --stats.diskQueueSize;
            assert(stats.diskQueueSize < GIGANTOR);
            vb->doStatsForFlushing(*qi, itemBytes);
            return NULL;
        }
        // Wait until the vbucket database is created by the vbucket state
        // snapshot task.
        if (vbMap.isBucketCreation(qi->getVBucketId())) {
            v->clearPendingId();
            lh.unlock();
            vb->rejectQueue.push(qi);
            ++vb->opsReject;
        } else {
            assert(rowid == v->getId());
            if (rowid == -1) {
                v->setPendingId();
            }

            lh.unlock();
            BlockTimer timer(rowid == -1 ?
                             &stats.diskInsertHisto : &stats.diskUpdateHisto,
                             rowid == -1 ? "disk_insert" : "disk_update",
                             stats.timingLog);
            PersistenceCallback *cb;
            cb = new PersistenceCallback(qi, vb, this,
                                         &mutationLog, &stats, itm.getCas());
            rwUnderlying->set(itm, *cb);
            if (rowid == -1)  {
                ++vb->opsCreate;
            } else {
                ++vb->opsUpdate;
            }
            return cb;
        }
    } else if (deleted || !found) {
        if (vbMap.isBucketDeletion(qi->getVBucketId())) {
            --stats.diskQueueSize;
            assert(stats.diskQueueSize < GIGANTOR);
            vb->doStatsForFlushing(*qi, itemBytes);
            return NULL;
        }

        if (vbMap.isBucketCreation(qi->getVBucketId())) {
            if (found) {
                v->clearPendingId();
            }
            lh.unlock();
            vb->rejectQueue.push(qi);
            ++vb->opsReject;
        } else {
            lh.unlock();
            BlockTimer timer(&stats.diskDelHisto, "disk_delete", stats.timingLog);
            PersistenceCallback *cb;
            cb = new PersistenceCallback(qi, vb, this, &mutationLog, &stats, 0);
            rwUnderlying->del(itm, rowid, *cb);
            return cb;
        }
    } else {
        --stats.diskQueueSize;
        assert(stats.diskQueueSize < GIGANTOR);
        vb->doStatsForFlushing(*qi, itemBytes);
    }

    return NULL;
}

void EventuallyPersistentStore::queueDirty(RCPtr<VBucket> &vb,
                                           const std::string &key,
                                           uint16_t vbid,
                                           enum queue_operation op,
                                           uint64_t seqno,
                                           bool tapBackfill) {
    if (doPersistence) {
        if (vb) {
            ++stats.diskQueueSize;
            queued_item itm(new QueuedItem(key, vbid, op, seqno));
            vb->doStatsForQueueing(*itm, itm->size());
            bool rv = tapBackfill ? vb->queueBackfillItem(itm) :
                                    vb->checkpointManager.queueDirty(itm, vb);
            if (rv) {
                KVShard* shard = vbMap.getShard(vbid);
                shard->getFlusher()->notifyFlushEvent();
                ++stats.totalEnqueued;
            } else {
                --stats.diskQueueSize;
                vb->doStatsForFlushing(*itm, itm->size());
            }
        }
    }
}

std::map<uint16_t, vbucket_state> EventuallyPersistentStore::loadVBucketState() {
    return getOneROUnderlying()->listPersistedVbuckets();
}

void EventuallyPersistentStore::loadSessionStats() {
    std::map<std::string, std::string> session_stats;
    getOneROUnderlying()->getPersistedStats(session_stats);
    engine.getTapConnMap().loadPrevSessionStats(session_stats);
}

void EventuallyPersistentStore::warmupCompleted() {
    stats.warmupComplete.set(true);

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

    // "0" sleep_time means that the first snapshot task will be executed right after
    // warmup. Subsequent snapshot tasks will be scheduled every 60 sec by default.
    IOManager *iom = IOManager::get();
    statsSnapshotTaskId =
        iom->scheduleStatsSnapshot(&engine, Priority::StatSnapPriority, 0,
                                   false, 0);
}

static void warmupLogCallback(void *arg, uint16_t vb,
                              const std::string &key, uint64_t rowid) {
    shared_ptr<Callback<GetValue> > *cb = reinterpret_cast<shared_ptr<Callback<GetValue> >*>(arg);
    Item *itm = new Item(key.data(), key.size(),
                         0, // flags
                         0, // exp
                         NULL, 0, // data
                         0, // CAS
                         rowid,
                         vb);

    GetValue gv(itm, ENGINE_SUCCESS, rowid, true /* partial */);

    (*cb)->callback(gv);
}

bool EventuallyPersistentStore::warmupFromLog(const std::map<uint16_t, vbucket_state> &state,
                                              shared_ptr<Callback<GetValue> > cb) {

    if (!mutationLog.exists()) {
        return false;
    }

    bool rv(true);

    MutationLogHarvester harvester(mutationLog, &getEPEngine());
    for (std::map<uint16_t, vbucket_state>::const_iterator it = state.begin();
         it != state.end(); ++it) {

        harvester.setVBucket(it->first);
    }

    hrtime_t start(gethrtime());
    rv = harvester.load();
    hrtime_t end1(gethrtime());

    if (!rv) {
        LOG(EXTENSION_LOG_WARNING, "Failed to read mutation log: %s",
            mutationLog.getLogFile().c_str());
        return false;
    }

    if (harvester.total() == 0) {
        // We didn't read a single item from the log..
        // @todo. the harvester should be extened to either
        // "throw" a FileNotFound exception, or a method we may
        // look at in order to check if it existed.
        return false;
    }

    warmupTask->setEstimatedItemCount(harvester.total());

    LOG(EXTENSION_LOG_DEBUG, "Completed log read in %s with %ld entries",
        hrtime2text(end1 - start).c_str(), harvester.total());

    harvester.apply(&cb, &warmupLogCallback);
    mutationLog.resetCounts(harvester.getItemsSeen());

    hrtime_t end2(gethrtime());
    LOG(EXTENSION_LOG_DEBUG, "Completed repopulation from log in %llums",
        ((end2 - end1) / 1000000));

    // Anything left in the "loading" map at this point is uncommitted.
    std::vector<mutation_log_uncommitted_t> uitems;
    harvester.getUncommitted(uitems);
    if (uitems.size() > 0) {
        LOG(EXTENSION_LOG_WARNING,
            "%ld items were uncommitted in the mutation log file. "
            "Deleting them from the underlying data store.\n", uitems.size());
        std::vector<mutation_log_uncommitted_t>::iterator uit = uitems.begin();
        for (; uit != uitems.end(); ++uit) {
            const mutation_log_uncommitted_t &record = *uit;
            RCPtr<VBucket> vb = getVBucket(record.vbucket);
            if (!vb) {
                continue;
            }

            bool should_delete = false;
            if (record.type == ML_NEW) {
                Item itm(record.key.c_str(), record.key.size(),
                         0, 0, // flags, expiration
                         NULL, 0, // data
                         0, // CAS,
                         record.rowid, record.vbucket);
                if (vb->ht.insert(itm, false, true) == NOT_FOUND) {
                    should_delete = true;
                }
            } else if (record.type == ML_DEL) {
                should_delete = true;
            }

            if (should_delete) {
                ItemMetaData itemMeta;

                // Deletion is pushed into the checkpoint for persistence.
                uint64_t cas = 0;
                deleteItem(record.key, &cas,
                           record.vbucket, NULL,
                           true, false, // force, use_meta
                           false, &itemMeta);
            }
        }
    }

    return rv;
}

void EventuallyPersistentStore::maybeEnableTraffic()
{
    // @todo rename.. skal vaere isTrafficDisabled elns
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());

    if (memoryUsed  >= stats.mem_low_wat) {
        LOG(EXTENSION_LOG_WARNING,
            "Total memory use reached to the low water mark, stop warmup");
        stats.warmupComplete.set(true);
    }
    if (memoryUsed > (maxSize * stats.warmupMemUsedCap)) {
        LOG(EXTENSION_LOG_WARNING,
                "Enough MB of data loaded to enable traffic");
        stats.warmupComplete.set(true);
    } else if (stats.warmedUpValues > (stats.warmedUpKeys * stats.warmupNumReadCap)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        LOG(EXTENSION_LOG_WARNING,
            "Enough number of items loaded to enable traffic");
        stats.warmupComplete.set(true);
    }
}

void EventuallyPersistentStore::stopWarmup(void)
{
    // forcefully stop current warmup task
    if (engine.stillWarmingUp()) {
        LOG(EXTENSION_LOG_WARNING, "Stopping warmup while engine is loading "
            "data from underlying storage, shutdown = %s\n",
            stats.shutdown.isShutdown ? "yes" : "no");
        warmupTask->stop();
    }
}

void EventuallyPersistentStore::setExpiryPagerSleeptime(size_t val) {
    LockHolder lh(expiryPager.mutex);

    if (expiryPager.sleeptime != 0) {
        getNonIODispatcher()->cancel(expiryPager.task);
    }

    expiryPager.sleeptime = val;
    if (val != 0) {
        shared_ptr<DispatcherCallback> exp_cb(new ExpiredItemPager(this, stats,
                                                                   expiryPager.sleeptime));

        getNonIODispatcher()->schedule(exp_cb, &expiryPager.task,
                                       Priority::ItemPagerPriority,
                                       expiryPager.sleeptime);
    }
}

void EventuallyPersistentStore::setAccessScannerSleeptime(size_t val) {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.sleeptime != 0) {
        auxIODispatcher->cancel(accessScanner.task);
    }

    // store sleeptime in seconds
    accessScanner.sleeptime = val * 60;
    if (accessScanner.sleeptime != 0) {
        AccessScanner *as = new AccessScanner(*this, stats, accessScanner.sleeptime);
        shared_ptr<DispatcherCallback> cb(as);
        auxIODispatcher->schedule(cb, &accessScanner.task,
                                  Priority::AccessScannerPriority,
                                  accessScanner.sleeptime);
        stats.alogTime.set(accessScanner.task->getWaketime().tv_sec);
    }
}

void EventuallyPersistentStore::resetAccessScannerStartTime() {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.sleeptime != 0) {
        auxIODispatcher->cancel(accessScanner.task);
        // re-schedule task according to the new task start hour
        AccessScanner *as = new AccessScanner(*this, stats, accessScanner.sleeptime);
        shared_ptr<DispatcherCallback> cb(as);
        auxIODispatcher->schedule(cb, &accessScanner.task,
                                  Priority::AccessScannerPriority,
                                  accessScanner.sleeptime);
        stats.alogTime.set(accessScanner.task->getWaketime().tv_sec);
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

/**
 * Visit all the items in memory and dump them into a new mutation log file.
 */
class LogCompactionVisitor : public VBucketVisitor {
public:
    LogCompactionVisitor(MutationLog &log, EPStats &st)
        : mutationLog(log), stats(st), numItemsLogged(0), totalItemsLogged(0)
    { }

    void visit(StoredValue *v) {
        if (!v->isDeleted() && v->hasId()) {
            ++numItemsLogged;
            mutationLog.newItem(currentBucket->getId(), v->getKey(), v->getId());
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
        update();
        return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        if (numItemsLogged > 0) {
            mutationLog.commit1();
            mutationLog.commit2();
            LOG(EXTENSION_LOG_INFO,
                "Mutation log compactor: Dumped %ld items from VBucket %d "
                "into a new mutation log file.",
                numItemsLogged, currentBucket->getId());
            totalItemsLogged += numItemsLogged;
            numItemsLogged = 0;
        }
    }

    void complete() {
        update();
        LOG(EXTENSION_LOG_INFO,
            "Mutation log compactor: Completed by dumping total %ld items "
            "into a new mutation log file.", totalItemsLogged);
    }

private:
    MutationLog &mutationLog;
    EPStats     &stats;
    size_t       numItemsLogged;
    size_t       totalItemsLogged;
};

bool EventuallyPersistentStore::compactMutationLog(size_t& sleeptime) {
    size_t num_new_items = mutationLog.itemsLogged[ML_NEW];
    size_t num_del_items = mutationLog.itemsLogged[ML_DEL];
    size_t num_logged_items = num_new_items + num_del_items;
    size_t num_unique_items = num_new_items - num_del_items;
    size_t queue_size = stats.diskQueueSize.get();

    bool rv = true;
    bool schedule_compactor =
        mutationLog.logSize > mlogCompactorConfig.getMaxLogSize() &&
        num_logged_items > (num_unique_items * mlogCompactorConfig.getMaxEntryRatio()) &&
        queue_size < mlogCompactorConfig.getQueueCap();

    if (schedule_compactor) {
        std::string compact_file = mutationLog.getLogFile() + ".compact";
        if (access(compact_file.c_str(), F_OK) == 0 &&
            remove(compact_file.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Can't remove the existing compacted log file \"%s\"",
                compact_file.c_str());
            return false;
        }

        BlockTimer timer(&stats.mlogCompactorHisto, "klogCompactorTime", stats.timingLog);
        pauseFlusher();
        try {
            MutationLog new_log(compact_file, mutationLog.getBlockSize());
            new_log.open();
            assert(new_log.isEnabled());
            new_log.setSyncConfig(mutationLog.getSyncConfig());

            LogCompactionVisitor compact_visitor(new_log, stats);
            visit(compact_visitor);
            mutationLog.replaceWith(new_log);
        } catch (MutationLog::ReadException e) {
            LOG(EXTENSION_LOG_WARNING,
                "Error in creating a new mutation log for compaction:  %s",
                e.what());
        } catch (...) {
            LOG(EXTENSION_LOG_WARNING, "Fatal error caught in Mutation Log "
                "Compactor task");
        }

        if (!mutationLog.isOpen()) {
            mutationLog.disable();
            rv = false;
        }
        resumeFlusher();
        ++stats.mlogCompactorRuns;
    }
    sleeptime = mlogCompactorConfig.getSleepTime();
    return rv;
}

VBCBAdaptor::VBCBAdaptor(EventuallyPersistentStore *s,
                         shared_ptr<VBucketVisitor> v,
                         const char *l, double sleep) :
    store(s), visitor(v), label(l), sleepTime(sleep), currentvb(0)
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

bool VBCBAdaptor::callback(Dispatcher & d, TaskId &t) {
    if (!vbList.empty()) {
        currentvb = vbList.front();
        RCPtr<VBucket> vb = store->vbMap.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                d.snooze(t, sleepTime);
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
