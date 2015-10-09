/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "conflict_resolution.h"
#include "defragmenter.h"
#include "ep.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "flusher.h"
#include "htresizer.h"
#include "kvshard.h"
#include "kvstore.h"
#include "locks.h"
#include "mutation_log.h"
#include "warmup.h"
#include "connmap.h"
#include "replicationthrottle.h"

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats &st, EventuallyPersistentStore &str)
        : stats(st), store(str) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("max_size") == 0) {
            stats.setMaxDataSize(value);
            store.getEPEngine().getDcpConnMap(). \
                                     updateMaxActiveSnoozingBackfills(value);
            size_t low_wat = static_cast<size_t>
                    (static_cast<double>(value) * stats.mem_low_wat_percent);
            size_t high_wat = static_cast<size_t>
                    (static_cast<double>(value) * stats.mem_high_wat_percent);
            stats.mem_low_wat.store(low_wat);
            stats.mem_high_wat.store(high_wat);
            store.setCursorDroppingLowerUpperThresholds(value);
        } else if (key.compare("mem_low_wat") == 0) {
            stats.mem_low_wat.store(value);
            stats.mem_low_wat_percent.store(
                                    (double)(value) / stats.getMaxDataSize());
        } else if (key.compare("mem_high_wat") == 0) {
            stats.mem_high_wat.store(value);
            stats.mem_high_wat_percent.store(
                                    (double)(value) / stats.getMaxDataSize());
        } else if (key.compare("replication_throttle_threshold") == 0) {
            stats.replicationThrottleThreshold.store(
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
    EventuallyPersistentStore &store;
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
        } else if (key.compare("compaction_write_queue_cap") == 0) {
            store.setCompactionWriteQueueCap(value);
        } else if (key.compare("exp_pager_stime") == 0) {
            store.setExpiryPagerSleeptime(value);
        } else if (key.compare("exp_pager_initial_run_time") == 0) {
            store.setExpiryPagerTasktime(value);
        } else if (key.compare("alog_sleep_time") == 0) {
            store.setAccessScannerSleeptime(value, false);
        } else if (key.compare("alog_task_time") == 0) {
            store.resetAccessScannerStartTime();
        } else if (key.compare("mutation_mem_threshold") == 0) {
            double mem_threshold = static_cast<double>(value) / 100;
            StoredValue::setMutationMemoryThreshold(mem_threshold);
        } else if (key.compare("backfill_mem_threshold") == 0) {
            double backfill_threshold = static_cast<double>(value) / 100;
            store.setBackfillMemoryThreshold(backfill_threshold);
        } else if (key.compare("compaction_exp_mem_threshold") == 0) {
            store.setCompactionExpMemThreshold(value);
        } else if (key.compare("replication_throttle_queue_cap") == 0) {
            store.getEPEngine().getReplicationThrottle().setQueueCap(value);
        } else if (key.compare("replication_throttle_cap_pcnt") == 0) {
            store.getEPEngine().getReplicationThrottle().setCapPercent(value);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to change value for unknown variable, %s\n",
                key.c_str());
        }
    }

    virtual void booleanValueChanged(const std::string &key, bool value) {
        if (key.compare("access_scanner_enabled") == 0) {
            if (value) {
                store.enableAccessScannerTask();
            } else {
                store.disableAccessScannerTask();
            }
        } else if (key.compare("bfilter_enabled") == 0) {
            store.setAllBloomFilters(value);
        } else if (key.compare("exp_pager_enabled") == 0) {
            if (value) {
                store.enableExpiryPager();
            } else {
                store.disableExpiryPager();
            }
        }
    }

    virtual void floatValueChanged(const std::string &key, float value) {
        if (key.compare("bfilter_residency_threshold") == 0) {
            store.setBfiltersResidencyThreshold(value);
        } else if (key.compare("dcp_min_compression_ratio") == 0) {
            store.getEPEngine().updateDcpMinCompressionRatio(value);
        }
    }

private:
    EventuallyPersistentStore &store;
};

/**
 * Callback class used by EpStore, for adding relevent keys
 * to bloomfilter during compaction.
 */
class BloomFilterCallback : public Callback<std::string&, bool&> {
public:
    BloomFilterCallback(EventuallyPersistentStore& eps, uint16_t vbid,
                        bool residentRatioAlert)
        : store(eps), vbucketId(vbid),
          residentRatioLessThanThreshold(residentRatioAlert) {
    }

    void callback(std::string& key, bool& isDeleted) {
        RCPtr<VBucket> vb = store.getVBucket(vbucketId);
        if (vb) {
            if (vb->isTempFilterAvailable()) {
                if (store.getItemEvictionPolicy() == VALUE_ONLY) {
                    /**
                     * VALUE-ONLY EVICTION POLICY
                     * Consider deleted items only.
                     */
                    if (isDeleted) {
                        vb->addToTempFilter(key);
                    }
                } else {
                    /**
                     * FULL EVICTION POLICY
                     * If vbucket's resident ratio is found to be less than
                     * the residency threshold, consider all items, otherwise
                     * consider deleted and non-resident items only.
                     */
                    if (residentRatioLessThanThreshold) {
                        vb->addToTempFilter(key);
                    } else {
                        if (isDeleted || !store.isMetaDataResident(vb, key)) {
                            vb->addToTempFilter(key);
                        }
                    }
                }
            }
        }
    }

private:
    EventuallyPersistentStore& store;
    uint16_t vbucketId;
    bool residentRatioLessThanThreshold;
};

class VBucketMemoryDeletionTask : public GlobalTask {
public:
    VBucketMemoryDeletionTask(EventuallyPersistentEngine &eng,
                              RCPtr<VBucket> &vb, double delay) :
                              GlobalTask(&eng,
                              Priority::VBMemoryDeletionPriority, delay, true),
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
    defragmenterTask(NULL),
    bgFetchQueue(0),
    diskFlushAll(false), bgFetchDelay(0),
    backfillMemoryThreshold(0.95),
    statsSnapshotTaskId(0), lastTransTimePerItem(0)
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

    stats.schedulingHisto = new Histogram<hrtime_t>[MAX_TYPE_ID];
    stats.taskRuntimeHisto = new Histogram<hrtime_t>[MAX_TYPE_ID];

    for (size_t i = 0; i < MAX_TYPE_ID; i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }

    ExecutorPool::get()->registerTaskable(ObjectRegistry::getCurrentEngine()->getTaskable());

    size_t num_vbs = config.getMaxVbuckets();
    vb_mutexes = new Mutex[num_vbs];
    schedule_vbstate_persist = new AtomicValue<bool>[num_vbs];
    for (size_t i = 0; i < num_vbs; ++i) {
        schedule_vbstate_persist[i] = false;
    }

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    if (config.getConflictResolutionType().compare("seqno") == 0) {
        conflictResolver = new ConflictResolution();
    }

    stats.setMaxDataSize(config.getMaxSize());
    config.addValueChangedListener("max_size",
                                   new StatsValueChangeListener(stats, *this));
    getEPEngine().getDcpConnMap().updateMaxActiveSnoozingBackfills(
                                                        config.getMaxSize());

    stats.mem_low_wat.store(config.getMemLowWat());
    config.addValueChangedListener("mem_low_wat",
                                   new StatsValueChangeListener(stats, *this));
    stats.mem_low_wat_percent.store(
                (double)(stats.mem_low_wat.load()) / stats.getMaxDataSize());

    stats.mem_high_wat.store(config.getMemHighWat());
    config.addValueChangedListener("mem_high_wat",
                                   new StatsValueChangeListener(stats, *this));
    stats.mem_high_wat_percent.store(
                (double)(stats.mem_high_wat.load()) / stats.getMaxDataSize());

    setCursorDroppingLowerUpperThresholds(config.getMaxSize());

    stats.replicationThrottleThreshold.store(static_cast<double>
                                    (config.getReplicationThrottleThreshold())
                                     / 100.0);
    config.addValueChangedListener("replication_throttle_threshold",
                                   new StatsValueChangeListener(stats, *this));

    stats.replicationThrottleWriteQueueCap.store(
                                    config.getReplicationThrottleQueueCap());
    config.addValueChangedListener("replication_throttle_queue_cap",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("replication_throttle_cap_pcnt",
                                   new EPStoreValueChangeListener(*this));

    setBGFetchDelay(config.getBgFetchDelay());
    config.addValueChangedListener("bg_fetch_delay",
                                   new EPStoreValueChangeListener(*this));

    stats.warmupMemUsedCap.store(static_cast<double>
                               (config.getWarmupMinMemoryThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_memory_threshold",
                                   new StatsValueChangeListener(stats, *this));
    stats.warmupNumReadCap.store(static_cast<double>
                                (config.getWarmupMinItemsThreshold()) / 100.0);
    config.addValueChangedListener("warmup_min_items_threshold",
                                   new StatsValueChangeListener(stats, *this));

    double mem_threshold = static_cast<double>
                                      (config.getMutationMemThreshold()) / 100;
    StoredValue::setMutationMemoryThreshold(mem_threshold);
    config.addValueChangedListener("mutation_mem_threshold",
                                   new EPStoreValueChangeListener(*this));

    double backfill_threshold = static_cast<double>
                                      (config.getBackfillMemThreshold()) / 100;
    setBackfillMemoryThreshold(backfill_threshold);
    config.addValueChangedListener("backfill_mem_threshold",
                                   new EPStoreValueChangeListener(*this));

    config.addValueChangedListener("bfilter_enabled",
                                   new EPStoreValueChangeListener(*this));

    bfilterResidencyThreshold = config.getBfilterResidencyThreshold();
    config.addValueChangedListener("bfilter_residency_threshold",
                                   new EPStoreValueChangeListener(*this));

    compactionExpMemThreshold = config.getCompactionExpMemThreshold();
    config.addValueChangedListener("compaction_exp_mem_threshold",
                                   new EPStoreValueChangeListener(*this));

    compactionWriteQueueCap = config.getCompactionWriteQueueCap();
    config.addValueChangedListener("compaction_write_queue_cap",
                                   new EPStoreValueChangeListener(*this));

    config.addValueChangedListener("dcp_min_compression_ratio",
                                   new EPStoreValueChangeListener(*this));

    const std::string &policy = config.getItemEvictionPolicy();
    if (policy.compare("value_only") == 0) {
        eviction_policy = VALUE_ONLY;
    } else {
        eviction_policy = FULL_EVICTION;
    }

    warmupTask = new Warmup(*this);
}

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

    warmupTask->start();

    if (config.isFailpartialwarmup() && stats.warmOOM > 0) {
        LOG(EXTENSION_LOG_WARNING,
            "Warmup failed to load %d records due to OOM, exiting.\n",
            static_cast<unsigned int>(stats.warmOOM));
        return false;
    }

    itmpTask = new ItemPager(&engine, stats);
    ExecutorPool::get()->schedule(itmpTask, NONIO_TASK_IDX);

    expiryPager.enabled = config.isExpPagerEnabled();
    size_t expiryPagerSleeptime = config.getExpPagerStime();
    setExpiryPagerSleeptime(expiryPagerSleeptime);
    config.addValueChangedListener("exp_pager_stime",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("exp_pager_enabled",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("exp_pager_initial_run_time",
                                   new EPStoreValueChangeListener(*this));

    ExTask htrTask = new HashtableResizerTask(this, 10);
    ExecutorPool::get()->schedule(htrTask, NONIO_TASK_IDX);

    size_t checkpointRemoverInterval = config.getChkRemoverStime();
    chkTask = new ClosedUnrefCheckpointRemoverTask(&engine, stats,
                                                   checkpointRemoverInterval);
    ExecutorPool::get()->schedule(chkTask, NONIO_TASK_IDX);

    ExTask vbSnapshotTask = new DaemonVBSnapshotTask(&engine);
    ExecutorPool::get()->schedule(vbSnapshotTask, WRITER_TASK_IDX);

    ExTask workloadMonitorTask = new WorkLoadMonitor(&engine, false);
    ExecutorPool::get()->schedule(workloadMonitorTask, NONIO_TASK_IDX);

#if HAVE_JEMALLOC
    /* Only create the defragmenter task if we have an underlying memory
     * allocator which can facilitate defragmenting memory.
     */
    defragmenterTask = new DefragmenterTask(&engine, stats);
    ExecutorPool::get()->schedule(defragmenterTask, NONIO_TASK_IDX);
#endif

    return true;
}

EventuallyPersistentStore::~EventuallyPersistentStore() {
    stopWarmup();
    stopBgFetcher();
    ExecutorPool::get()->stopTaskGroup(engine.getTaskable().getGID(), NONIO_TASK_IDX,
                                       stats.forceShutdown);

    ExecutorPool::get()->cancel(statsSnapshotTaskId);

    LockHolder lh(accessScanner.mutex);
    ExecutorPool::get()->cancel(accessScanner.task);
    lh.unlock();

    stopFlusher();

    ExecutorPool::get()->unregisterTaskable(engine.getTaskable(),
                                            stats.forceShutdown);

    delete [] vb_mutexes;
    delete [] schedule_vbstate_persist;
    delete [] stats.schedulingHisto;
    delete [] stats.taskRuntimeHisto;
    delete conflictResolver;
    delete warmupTask;
    delete storageProperties;
    defragmenterTask.reset();

    std::vector<MutationLog*>::iterator it;
    for (it = accessLog.begin(); it != accessLog.end(); it++) {
        delete *it;
    }
}

const Flusher* EventuallyPersistentStore::getFlusher(uint16_t shardId) {
    return vbMap.getShard(shardId)->getFlusher();
}

uint16_t EventuallyPersistentStore::getCommitInterval(uint16_t shardId) {
    Flusher *flusher = vbMap.shards[shardId]->getFlusher();
    return flusher->getCommitInterval();
}

uint16_t EventuallyPersistentStore::decrCommitInterval(uint16_t shardId) {
    Flusher *flusher = vbMap.shards[shardId]->getFlusher();
    return flusher->decrCommitInterval();
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
                    "Attempted to resume flusher in state [%s], "
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
                "Failed to start bg fetcher for shard %d", i);
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
                                             uint64_t revSeqno,
                                             exp_type_t source) {
    RCPtr<VBucket> vb = getVBucket(vbid);
    if (vb) {
        ReaderLockHolder(vb->getStateLock());
        if (vb->getState() == vbucket_state_active) {
            int bucket_num(0);
            incExpirationStat(vb, source);
            LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, false);
            if (v) {
                if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                    // This is a temporary item whose background fetch for metadata
                    // has completed.
                    bool deleted = vb->ht.unlocked_del(key, bucket_num);
                    if (!deleted) {
                        throw std::logic_error("EPStore::deleteExpiredItem: "
                                "Failed to delete key '" + key + "' from bucket "
                                + std::to_string(bucket_num));
                    }
                } else if (v->isExpired(startTime) && !v->isDeleted()) {
                    vb->ht.unlocked_softDelete(v, 0, getItemEvictionPolicy());
                    queueDirty(vb, v, &lh, NULL, false);
                }
            } else {
                if (eviction_policy == FULL_EVICTION) {
                    // Create a temp item and delete and push it
                    // into the checkpoint queue, only if the bloomfilter
                    // predicts that the item may exist on disk.
                    if (vb->maybeKeyExistsInFilter(key)) {
                        add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                                    eviction_policy);
                        if (rv == ADD_NOMEM) {
                            return;
                        }
                        v = vb->ht.unlocked_find(key, bucket_num, true, false);
                        v->setDeleted();
                        v->setRevSeqno(revSeqno);
                        vb->ht.unlocked_softDelete(v, 0, eviction_policy);
                        queueDirty(vb, v, &lh, NULL, false);
                    }
                }
            }
        }
    }
}

void
EventuallyPersistentStore::deleteExpiredItems(std::list<std::pair<uint16_t,
                                                        std::string> > &keys,
                                              exp_type_t source) {
    std::list<std::pair<uint16_t, std::string> >::iterator it;
    time_t startTime = ep_real_time();
    for (it = keys.begin(); it != keys.end(); it++) {
        deleteExpiredItem(it->first, it->second, startTime, 0, source);
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
    if (v && !v->isDeleted() && !v->isTempItem()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (vb->getState() != vbucket_state_active) {
                return wantDeleted ? v : NULL;
            }
            if (queueExpired) {
                incExpirationStat(vb, EXP_BY_ACCESS);
                vb->ht.unlocked_softDelete(v, 0, eviction_policy);
                queueDirty(vb, v, NULL, NULL, false, true);
            }
            return wantDeleted ? v : NULL;
        }
    }
    return v;
}

bool EventuallyPersistentStore::isMetaDataResident(RCPtr<VBucket> &vb,
                                                   const std::string &key) {

    if (!vb) {
        throw std::invalid_argument("EPStore::isMetaDataResident: vb is NULL");
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, false, false);

    if (v && !v->isTempItem()) {
        return true;
    } else {
        return false;
    }
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

                // Add key to bloom filter incase of full eviction mode
                if (getItemEvictionPolicy() == FULL_EVICTION) {
                    vb->addToFilter(key);
                }
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
                                                        bool metadataOnly,
                                                        bool isReplication) {

    add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                eviction_policy,
                                                isReplication);
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
            bgFetch(key, vb->getId(), cookie, metadataOnly);
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
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a set op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    bool cas_op = (itm.getCas() != 0);
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num,
                                          /*wantsDeleted*/true,
                                          /*trackReference*/false);
    if (v && v->isLocked(ep_current_time()) &&
        (vb->getState() == vbucket_state_replica ||
         vb->getState() == vbucket_state_pending)) {
        v->unlock();
    }

    bool maybeKeyExists = true;
    // If we didn't find a valid item, check Bloomfilter's prediction if in
    // full eviction policy and for a CAS operation.
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction_policy == FULL_EVICTION) &&
        (itm.getCas() != 0)) {
        // Check Bloomfilter's prediction
        if (!vb->maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, itm.getCas(), true, false,
                                                eviction_policy, nru,
                                                maybeKeyExists);

    Item& it = const_cast<Item&>(itm);
    uint64_t seqno = 0;
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
        it.setCas(vb->nextHLCCas());
        v->setCas(it.getCas());
        queueDirty(vb, v, &lh, &seqno);
        it.setBySeqno(seqno);
        break;
    case NEED_BG_FETCH:
    {   // CAS operation with non-resident item + full eviction.
        if (v) {
            // temp item is already created. Simply schedule a bg fetch job
            lh.unlock();
            bgFetch(itm.getKey(), vb->getId(), cookie, true);
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
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a add op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    if (itm.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return ENGINE_NOT_STORED;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);

    bool maybeKeyExists = true;
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction_policy == FULL_EVICTION)) {
        // Check bloomfilter's prediction
        if (!vb->maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    add_type_t atype = vb->ht.unlocked_add(bucket_num, v, itm,
                                           eviction_policy,
                                           true, true,
                                           maybeKeyExists);

    Item& it = const_cast<Item&>(itm);
    uint64_t seqno = 0;
    switch (atype) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_TMP_AND_BG_FETCH:
        return addTempItemForBgFetch(lh, bucket_num, it.getKey(), vb,
                                     cookie, true);
    case ADD_BG_FETCH:
        lh.unlock();
        bgFetch(it.getKey(), vb->getId(), cookie, true);
        return ENGINE_EWOULDBLOCK;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        it.setCas(vb->nextHLCCas());
        v->setCas(it.getCas());
        queueDirty(vb, v, &lh, &seqno);
        it.setBySeqno(seqno);
        break;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::replace(const Item &itm,
                                                     const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);
    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }

        mutation_type_t mtype;
        if (eviction_policy == FULL_EVICTION && v->isTempInitialItem()) {
            mtype = NEED_BG_FETCH;
        } else {
            mtype = vb->ht.unlocked_set(v, itm, 0, true, false, eviction_policy,
                                        0xff);
        }

        Item& it = const_cast<Item&>(itm);
        uint64_t seqno = 0;
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        switch (mtype) {
            case NOMEM:
                ret = ENGINE_ENOMEM;
                break;
            case IS_LOCKED:
                ret = ENGINE_KEY_EEXISTS;
                break;
            case INVALID_CAS:
            case NOT_FOUND:
                ret = ENGINE_NOT_STORED;
                break;
                // FALLTHROUGH
            case WAS_DIRTY:
                // Even if the item was dirty, push it into the vbucket's open
                // checkpoint.
            case WAS_CLEAN:
                it.setCas(vb->nextHLCCas());
                v->setCas(it.getCas());
                queueDirty(vb, v, &lh, &seqno);
                it.setBySeqno(seqno);
                break;
            case NEED_BG_FETCH:
            {
                // temp item is already created. Simply schedule a bg fetch job
                lh.unlock();
                bgFetch(it.getKey(), vb->getId(), cookie, true);
                ret = ENGINE_EWOULDBLOCK;
                break;
            }
            case INVALID_VBUCKET:
                ret = ENGINE_NOT_MY_VBUCKET;
                break;
        }

        return ret;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        }

        if (vb->maybeKeyExistsInFilter(itm.getKey())) {
            return addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                         cookie, false);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for replace().
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTAPBackfillItem(
                                                        const Item &itm,
                                                        uint8_t nru,
                                                        bool genBySeqno,
                                                        ExtendedMetaData *emd) {

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

    // Note that this function is only called on replica or pending vbuckets.
    if (v && v->isLocked(ep_current_time())) {
        v->unlock();
    }
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
        // FALLTHROUGH, to ensure the bySeqno for the hashTable item is
        // set correctly, and also the sequence numbers are ordered correctly.
        // (MB-14003)
    case NOT_FOUND:
        // FALLTHROUGH
    case WAS_CLEAN:
        /* set the conflict resolution mode from the extended meta data *
         * Given that the mode is already set, we don't need to set the *
         * conflict resolution mode in queueDirty */
        if (emd) {
            v->setConflictResMode(
                 static_cast<enum conflict_resolution_mode>(
                                      emd->getConflictResMode()));
        }
        vb->setMaxCas(v->getCas());
        queueDirty(vb, v, &lh, NULL,true, true, genBySeqno, false);
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case NEED_BG_FETCH:
        // SET on a non-active vbucket should not require a bg_metadata_fetch.
        abort();
    }

    // Update drift counter for vbucket upon a success only
    if (ret == ENGINE_SUCCESS && emd) {
        vb->setDriftCounter(emd->getAdjustedTime());
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
                snapshot_range_t range;
                vb->getPersistedSnapshot(range);
                std::string failovers = vb->failovers->toJSON();
                uint64_t chkId = vbuckets.getPersistenceCheckpointId(vb->getId());

                vbucket_state vb_state(vb->getState(), chkId, 0,
                                       vb->getHighSeqno(), vb->getPurgeSeqno(),
                                       range.start, range.end, vb->getMaxCas(),
                                       vb->getDriftCounter() ,failovers);
                states.insert(std::pair<uint16_t, vbucket_state>(vb->getId(), vb_state));
            }
            return false;
        }

        void visit(StoredValue*) {
            throw std::logic_error("VBucketStateVisitor:visit: Should never be called");
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

    bool success = true;
    vbucket_map_t::reverse_iterator iter = v.states.rbegin();
    for (; iter != v.states.rend(); ++iter) {
        LockHolder lh(vb_mutexes[iter->first], true /*tryLock*/);
        if (!lh.islocked()) {
            continue;
        }
        KVStore *rwUnderlying = getRWUnderlying(iter->first);
        if (!rwUnderlying->snapshotVBucket(iter->first, iter->second,
                    &kvcb)) {
            LOG(EXTENSION_LOG_WARNING,
                    "VBucket snapshot task failed!!! Rescheduling");
            success = false;
            break;
        }

        if (priority == Priority::VBucketPersistHighPriority) {
            if (vbMap.setBucketCreation(iter->first, false)) {
                LOG(EXTENSION_LOG_INFO, "VBucket %d created", iter->first);
            }
        }
    }

    if (!success) {
        scheduleVBSnapshot(priority, shard->getId());
    } else {
        stats.snapshotVbucketHisto.add((gethrtime() - start) / 1000);
    }
}

bool EventuallyPersistentStore::persistVBState(const Priority &priority,
                                               uint16_t vbid) {
    schedule_vbstate_persist[vbid] = false;

    RCPtr<VBucket> vb = getVBucket(vbid);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING,
            "VBucket %d not exist!!! vb_state persistence task failed!!!", vbid);
        return false;
    }

    KVStatsCallback kvcb(this);
    uint64_t chkId = vbMap.getPersistenceCheckpointId(vbid);
    std::string failovers = vb->failovers->toJSON();

    snapshot_range_t range;
    vb->getPersistedSnapshot(range);
    vbucket_state vb_state(vb->getState(), chkId, 0, vb->getHighSeqno(),
                           vb->getPurgeSeqno(), range.start, range.end,
                           vb->getMaxCas(), vb->getDriftCounter(),
                           failovers);

    bool inverse = false;
    LockHolder lh(vb_mutexes[vbid], true /*tryLock*/);
    if (!lh.islocked()) {
        if (schedule_vbstate_persist[vbid].compare_exchange_strong(inverse,
                                                                   true)) {
            return true;
        } else {
            return false;
        }
    }

    KVStore *rwUnderlying = getRWUnderlying(vbid);
    if (rwUnderlying->snapshotVBucket(vbid, vb_state, &kvcb)) {
        if (vbMap.setBucketCreation(vbid, false)) {
            LOG(EXTENSION_LOG_INFO, "VBucket %d created", vbid);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "VBucket %d: vb_state persistence task failed!!! Rescheduling", vbid);

        if (schedule_vbstate_persist[vbid].compare_exchange_strong(inverse,
                                                                   true)) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                           vbucket_state_t to,
                                                           bool transfer,
                                                           bool notify_dcp) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb && to == vb->getState()) {
        return ENGINE_SUCCESS;
    }

    if (vb) {
        vbucket_state_t oldstate = vb->getState();
        if (oldstate != to && notify_dcp) {
            engine.getDcpConnMap().vbucketStateChanged(vbid, to);
        }

        vb->setState(to);

        if (to == vbucket_state_active && oldstate == vbucket_state_replica) {
            /**
             * Update snapshot range when vbucket goes from being a replica
             * to active, to maintain the correct snapshot sequence numbers
             * even in a failover scenario.
             */
            vb->checkpointManager.resetSnapshotRange();
        }

        if (to == vbucket_state_active && !transfer) {
            snapshot_range_t range;
            vb->getPersistedSnapshot(range);
            if (range.end == vbMap.getPersistenceSeqno(vbid)) {
                vb->failovers->createEntry(range.end);
            } else {
                vb->failovers->createEntry(range.start);
            }
        }

        lh.unlock();
        if (oldstate == vbucket_state_pending &&
            to == vbucket_state_active) {
            ExTask notifyTask = new PendingOpsNotification(engine, vb);
            ExecutorPool::get()->schedule(notifyTask, NONIO_TASK_IDX);
        }
        scheduleVBStatePersist(Priority::VBucketPersistLowPriority, vbid);
    } else if (vbid < vbMap.getSize()) {
        FailoverTable* ft = new FailoverTable(engine.getMaxFailoverEntries());
        KVShard* shard = vbMap.getShard(vbid);
        shared_ptr<Callback<uint16_t> > cb(new NotifyFlusherCB(shard));
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats,
                                         engine.getCheckpointConfig(),
                                         shard, 0, 0, 0, ft, cb));
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
        scheduleVBStatePersist(Priority::VBucketPersistHighPriority, vbid);
    } else {
        return ENGINE_ERANGE;
    }
    return ENGINE_SUCCESS;
}

bool EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p) {
    KVShard *shard = NULL;
    if (p == Priority::VBucketPersistHighPriority) {
        for (size_t i = 0; i < vbMap.numShards; ++i) {
            shard = vbMap.shards[i];
            if (shard->setHighPriorityVbSnapshotFlag(true)) {
                ExTask task = new VBSnapshotTask(&engine, p, i, true);
                ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            }
        }
    } else {
        for (size_t i = 0; i < vbMap.numShards; ++i) {
            shard = vbMap.shards[i];
            if (shard->setLowPriorityVbSnapshotFlag(true)) {
                ExTask task = new VBSnapshotTask(&engine, p, i, true);
                ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            }
        }
    }
    if (stats.isShutdown) {
        return false;
    }
    return true;
}

void EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p,
                                                   uint16_t shardId,
                                                   bool force) {
    KVShard *shard = vbMap.shards[shardId];
    if (p == Priority::VBucketPersistHighPriority) {
        if (force || shard->setHighPriorityVbSnapshotFlag(true)) {
            ExTask task = new VBSnapshotTask(&engine, p, shardId, true);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
        }
    } else {
        if (force || shard->setLowPriorityVbSnapshotFlag(true)) {
            ExTask task = new VBSnapshotTask(&engine, p, shardId, true);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
        }
    }
}

void EventuallyPersistentStore::scheduleVBStatePersist(const Priority &priority,
                                                       uint16_t vbid,
                                                       bool force) {
    bool inverse = false;
    if (force ||
        schedule_vbstate_persist[vbid].compare_exchange_strong(inverse, true)) {
        ExTask task = new VBStatePersistTask(&engine, priority, vbid, true);
        ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
    }
}

bool EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid,
                                                        const void* cookie) {
    LockHolder lh(vbsetMutex);

    hrtime_t start_time(gethrtime());
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead ||
         vbMap.isBucketDeletion(vbid)) {
        lh.unlock();
        LockHolder vlh(vb_mutexes[vbid]);
        getRWUnderlying(vbid)->delVBucket(vbid);
        vbMap.setBucketDeletion(vbid, false);
        vbMap.setBucketCreation(vbid, false);
        vbMap.setPersistenceSeqno(vbid, 0);
        ++stats.vbucketDeletions;
    }

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

void EventuallyPersistentStore::scheduleVBDeletion(RCPtr<VBucket> &vb,
                                                   const void* cookie,
                                                   double delay) {
    ExTask delTask = new VBucketMemoryDeletionTask(engine, vb, delay);
    ExecutorPool::get()->schedule(delTask, NONIO_TASK_IDX);

    if (vbMap.setBucketDeletion(vb->getId(), true)) {
        ExTask task = new VBDeleteTask(&engine, vb->getId(), cookie,
                                       Priority::VBucketDeletionPriority);
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

    engine.getDcpConnMap().vbucketStateChanged(vbid, vbucket_state_dead);
    vbMap.removeBucket(vbid);
    lh.unlock();
    scheduleVBDeletion(vb, c);
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

    LockHolder lh(compactionLock);
    ExTask task = new CompactVBucketTask(&engine, Priority::CompactorPriority,
                                         vbid, c, cookie);
    compactionTasks.push_back(std::make_pair(vbid, task));
    if (compactionTasks.size() > 1) {
        if ((stats.diskQueueSize > compactionWriteQueueCap &&
            compactionTasks.size() > (vbMap.getNumShards() / 2)) ||
            engine.getWorkLoadPolicy().getWorkLoadPattern() == READ_HEAVY) {
            // Snooze a new compaction task.
            // We will wake it up when one of the existing compaction tasks is done.
            task->snooze(60);
        }
    }

    ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);

    LOG(EXTENSION_LOG_DEBUG,
        "Scheduled compaction task %" PRIu64 " on vbucket %d,"
        "purge_before_ts = %" PRIu64 ", purge_before_seq = %" PRIu64
        ", dropdeletes = %d",
        uint64_t(task->getId()), vbid, c.purge_before_ts,
        c.purge_before_seq, c.drop_deletes);

   return ENGINE_EWOULDBLOCK;
}

class ExpiredItemsCallback : public Callback<std::string&, uint64_t&> {
    public:
        ExpiredItemsCallback(EventuallyPersistentStore *store, uint16_t vbid,
                             time_t start)
            : epstore(store), vbucket(vbid), startTime(start) { }

        void callback(std::string& key, uint64_t& revSeqno) {
            if (epstore->compactionCanExpireItems()) {
                epstore->deleteExpiredItem(vbucket, key, startTime, revSeqno,
                                           EXP_BY_COMPACTOR);
            }
        }

    private:
        EventuallyPersistentStore *epstore;
        uint16_t vbucket;
        time_t startTime;
};

bool EventuallyPersistentStore::compactVBucket(const uint16_t vbid,
                                               compaction_ctx *ctx,
                                               const void *cookie) {
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        LockHolder lh(vb_mutexes[vbid], true /*tryLock*/);
        if (!lh.islocked()) {
            return true; // Schedule a compaction task again.
        }

        Configuration &config = getEPEngine().getConfiguration();
        if (config.isBfilterEnabled()) {
            size_t initial_estimation = config.getBfilterKeyCount();
            size_t estimated_count;
            size_t num_deletes =
                    getROUnderlying(vbid)->getNumPersistedDeletes(vbid);
            if (eviction_policy == VALUE_ONLY) {
                /**
                 * VALUE-ONLY EVICTION POLICY
                 * Obtain number of persisted deletes from underlying kvstore.
                 * Bloomfilter's estimated_key_count = 1.25 * deletes
                 */

                estimated_count = round(1.25 * num_deletes);
                shared_ptr<Callback<std::string&, bool&> >
                    filter(new BloomFilterCallback(*this, vbid, false));
                ctx->bloomFilterCallback = filter;
            } else {
                /**
                 * FULL EVICTION POLICY
                 * First determine if the resident ratio of vbucket is less than
                 * the threshold from configuration.
                 */

                bool residentRatioAlert = vb->isResidentRatioUnderThreshold(
                                                getBfiltersResidencyThreshold(),
                                                eviction_policy);
                shared_ptr<Callback<std::string&, bool&> >
                    filter(new BloomFilterCallback(*this, vbid, residentRatioAlert));
                ctx->bloomFilterCallback = filter;

                /**
                 * Based on resident ratio against threshold, estimate count.
                 *
                 * 1. If resident ratio is greater than the threshold:
                 * Obtain number of persisted deletes from underlying kvstore.
                 * Obtain number of non-resident-items for vbucket.
                 * Bloomfilter's estimated_key_count =
                 *                              1.25 * (deletes + non-resident)
                 *
                 * 2. Otherwise:
                 * Obtain number of items for vbucket.
                 * Bloomfilter's estimated_key_count =
                 *                              1.25 * (num_items)
                 */

                if (residentRatioAlert) {
                    estimated_count = round(1.25 *
                                            vb->getNumItems(eviction_policy));
                } else {
                    estimated_count = round(1.25 * (num_deletes +
                                vb->getNumNonResidentItems(eviction_policy)));
                }
            }
            if (estimated_count < initial_estimation) {
                estimated_count = initial_estimation;
            }
            vb->initTempFilter(estimated_count, config.getBfilterFpProb());
        }

        if (vb->getState() == vbucket_state_active) {
            // Set the current time ONLY for active vbuckets.
            ctx->curr_time = ep_real_time();
        } else {
            ctx->curr_time = 0;
        }
        shared_ptr<Callback<std::string&, uint64_t&> >
            expiry(new ExpiredItemsCallback(this, vbid, ctx->curr_time));
        ctx->expiryCallback = expiry;

        KVStatsCallback kvcb(this);
        if (getRWUnderlying(vbid)->compactVBucket(vbid, ctx, kvcb)) {
            if (config.isBfilterEnabled()) {
                vb->swapFilter();
            } else {
                vb->clearFilter();
            }
        } else {
            LOG(EXTENSION_LOG_WARNING, "Compaction: Not successful for vb %u, "
                    "clearing bloom filter, if any.", vb->getId());
            vb->clearFilter();
        }
        vb->setPurgeSeqno(ctx->max_purged_seq);
    } else {
        err = ENGINE_NOT_MY_VBUCKET;
        engine.storeEngineSpecific(cookie, NULL);
        //Decrement session counter here, as memcached thread wouldn't
        //visit the engine interface in case of a NOT_MY_VB notification
        engine.decrementSessionCtr();

    }

    LockHolder lh(compactionLock);
    bool erased = false, woke = false;
    std::list<CompTaskEntry>::iterator it = compactionTasks.begin();
    while (it != compactionTasks.end()) {
        if ((*it).first == vbid) {
            it = compactionTasks.erase(it);
            erased = true;
        } else {
            ExTask &task = (*it).second;
            if (task->getState() == TASK_SNOOZED) {
                ExecutorPool::get()->wake(task->getId());
                woke = true;
            }
            ++it;
        }
        if (erased && woke) {
            break;
        }
    }
    lh.unlock();

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

        vbMap.removeBucket(vbid);
        lh.unlock();

        std::list<std::string> tap_cursors = vb->checkpointManager.
                                             getCursorNames();
        // Delete and recreate the vbucket database file
        scheduleVBDeletion(vb, NULL, 0);
        setVBucketState(vbid, vbstate, false);

        // Copy the all cursors from the old vbucket into the new vbucket
        RCPtr<VBucket> newvb = vbMap.getBucket(vbid);
        newvb->checkpointManager.resetCursors(tap_cursors);

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
        if (cookie == nullptr) {
            throw std::invalid_argument("add_stat: cookie is NULL");
        }
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
    bool rv = engine.getStats(&snap, NULL, 0, add_stat) == ENGINE_SUCCESS &&
              engine.getStats(&snap, "tap", 3, add_stat) == ENGINE_SUCCESS &&
              engine.getStats(&snap, "dcp", 3, add_stat) == ENGINE_SUCCESS;

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
    if (stop >= start && start >= init) {
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
    getROUnderlying(vbucket)->get(key, vbucket, gcb);
    gcb.waitForValue();
    ENGINE_ERROR_CODE status = gcb.val.getStatus();

    // Lock to prevent a race condition between a fetch for restore and delete
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (vb) {
        int bucket_num(0);
        LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = fetchValidValue(vb, key, bucket_num, true);
        if (isMeta) {
            if ((v && v->unlocked_restoreMeta(gcb.val.getValue(),
                                             gcb.val.getStatus(), vb->ht))
                || ENGINE_KEY_ENOENT == status) {
                /* If ENGINE_KEY_ENOENT is the status from storage and the temp
                 key is removed from hash table by the time bgfetch returns
                 (in case multiple bgfetch is scheduled for a key), we still
                 need to return ENGINE_SUCCESS to the memcached worker thread,
                 so that the worker thread can visit the ep-engine and figure
                 out the correct flow */
                status = ENGINE_SUCCESS;
            }
        } else {
            bool restore = false;
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            } else if (v && v->isDeleted()) {
                status = ENGINE_KEY_ENOENT;
            } else {
                switch (eviction_policy) {
                    case VALUE_ONLY:
                        if (v && !v->isResident() && !v->isDeleted()) {
                            restore = true;
                        }
                        break;
                    case FULL_EVICTION:
                        if (v) {
                            if (v->isTempInitialItem() ||
                                (!v->isResident() && !v->isDeleted())) {
                                restore = true;
                            }
                        }
                        break;
                    default:
                        throw std::logic_error("Unknown eviction policy");
                }
            }

            if (restore) {
                if (gcb.val.getStatus() == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(gcb.val.getValue(), vb->ht);
                    if (!v->isResident()) {
                        throw std::logic_error("EPStore::completeBGFetch: "
                                "storedvalue (which has key " + v->getKey() +
                                ") should be resident after calling restoreValue()");
                    }
                    if (vb->getState() == vbucket_state_active &&
                        v->getExptime() != gcb.val.getValue()->getExptime() &&
                        v->getCas() == gcb.val.getValue()->getCas()) {
                        // MB-9306: It is possible that by the time bgfetcher
                        // returns, the item may have been updated and queued
                        // Hence test the CAS value to be the same first.
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, v, &hlh, NULL);
                    }
                } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
                    v->setNonExistent();
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
                        "Failed background fetch for vb=%d "
                        "seq=%" PRId64 " key=%s", vbucket, v->getBySeqno(),
                        key.c_str());
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
            if ((v && v->unlocked_restoreMeta(fetchedValue, status, vb->ht))
                || ENGINE_KEY_ENOENT == status) {
                /* If ENGINE_KEY_ENOENT is the status from storage and the temp
                 key is removed from hash table by the time bgfetch returns
                 (in case multiple bgfetch is scheduled for a key), we still
                 need to return ENGINE_SUCCESS to the memcached worker thread,
                 so that the worker thread can visit the ep-engine and figure
                 out the correct flow */
                status = ENGINE_SUCCESS;
            }
        } else {
            bool restore = false;
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            } else if (v && v->isDeleted()) {
                status = ENGINE_KEY_ENOENT;
            } else {
                switch (eviction_policy) {
                    case VALUE_ONLY:
                        if (v && !v->isResident() && !v->isDeleted()) {
                            restore = true;
                        }
                        break;
                    case FULL_EVICTION:
                        if (v) {
                            if (v->isTempInitialItem() ||
                                (!v->isResident() && !v->isDeleted())) {
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
                    v->unlocked_restoreValue(fetchedValue, vb->ht);
                    if (!v->isResident()) {
                        throw std::logic_error("EPStore::completeBGFetchMulti: "
                            "storedvalue (which has key " + v->getKey() +
                            ") should be resident after calling restoreValue()");
                    }
                    if (vb->getState() == vbucket_state_active &&
                        v->getExptime() != fetchedValue->getExptime() &&
                        v->getCas() == fetchedValue->getCas()) {
                        // MB-9306: It is possible that by the time
                        // bgfetcher returns, the item may have been
                        // updated and queued
                        // Hence test the CAS value to be the same first.
                        // exptime mutated, schedule it into new checkpoint
                        queueDirty(vb, v, &blh, NULL);
                    }
                } else if (status == ENGINE_KEY_ENOENT) {
                    v->setNonExistent();
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
                        "Failed background fetch for vb=%d "
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
        "EP Store completes %" PRIu64 " of batched background fetch "
        "for vBucket = %d endTime = %" PRIu64,
        uint64_t(fetchedItems.size()), vbId, gethrtime()/1000000);
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        bool isMeta) {
    if (multiBGFetchEnabled()) {
        RCPtr<VBucket> vb = getVBucket(vbucket);
        if (!vb) {
            throw std::invalid_argument("EPStore::bgFetch: vbucket (which is " +
                                        std::to_string(vbucket) +
                                        ") is not present in vbMap");
        }
        KVShard *myShard = vbMap.getShard(vbucket);

        // schedule to the current batch of background fetch of the given
        // vbucket
        VBucketBGFetchItem * fetchThis = new VBucketBGFetchItem(cookie,
                                                                isMeta);
        size_t bgfetch_size = vb->queueBGFetchItem(key, fetchThis,
                                                   myShard->getBgFetcher());
        myShard->getBgFetcher()->notifyBGEvent();
        LOG(EXTENSION_LOG_DEBUG, "Queued a background fetch, now at %" PRIu64,
            uint64_t(bgfetch_size));
    } else {
        bgFetchQueue++;
        stats.maxRemainingBgJobs = std::max(stats.maxRemainingBgJobs,
                                            bgFetchQueue.load());
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new BGFetchTask(&engine, key, vbucket, cookie,
                                      isMeta,
                                      Priority::BgFetcherGetMetaPriority,
                                      bgFetchDelay, false);
        iom->schedule(task, READER_TASK_IDX);
        LOG(EXTENSION_LOG_DEBUG, "Queued a background fetch, now at %" PRIu64,
            uint64_t(bgFetchQueue.load()));
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
        if (v->isDeleted()) {
            GetValue rv;
            return rv;
        }
        if (v->isTempDeletedItem() || v->isTempNonExistentItem()) {
            // Delete a temp non-existent item to ensure that
            // if the get were issued over an item that doesn't
            // exist, then we dont preserve a temp item.
            vb->ht.unlocked_del(key, bucket_num);
            GetValue rv;
            return rv;
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, cookie);
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

        if (vb->maybeKeyExistsInFilter(key)) {
            ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
            if (queueBG) { // Full eviction and need a bg fetch.
                ec = addTempItemForBgFetch(lh, bucket_num, key, vb,
                                           cookie, false);
            }
            return GetValue(NULL, ec, -1, true);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENONET, for getInternal().
            GetValue rv;
            return rv;
        }
    }
}

GetValue EventuallyPersistentStore::getRandomKey() {
    VBucketMap::id_type max = vbMap.getSize();

    const long start = random() % max;
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
                                                        uint8_t &confResMode,
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
            bgFetch(key, vbucket, cookie, true);
            return ENGINE_EWOULDBLOCK;
        } else if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return ENGINE_KEY_ENOENT;
        } else {
            if (v->isTempDeletedItem() || v->isDeleted() ||
                v->isExpired(ep_real_time())) {
                deleted |= GET_META_ITEM_DELETED_FLAG;
            }

            if (v->isLocked(ep_current_time())) {
                metadata.cas = static_cast<uint64_t>(-1);
            } else {
                metadata.cas = v->getCas();
            }
            metadata.flags = v->getFlags();
            metadata.exptime = v->getExptime();
            metadata.revSeqno = v->getRevSeqno();
            confResMode = v->getConflictResMode();
            return ENGINE_SUCCESS;
        }
    } else {
        // The key wasn't found. However, this may be because it was previously
        // deleted or evicted with the full eviction strategy.
        // So, add a temporary item corresponding to the key to the hash table
        // and schedule a background fetch for its metadata from the persistent
        // store. The item's state will be updated after the fetch completes.
        //
        // Schedule this bgFetch only if the key is predicted to be may-be
        // existent on disk by the bloomfilter.

        if (vb->maybeKeyExistsInFilter(key)) {
            return addTempItemForBgFetch(lh, bucket_num, key, vb, cookie, true);
        } else {
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setWithMeta(
                                                     const Item &itm,
                                                     uint64_t cas,
                                                     uint64_t *seqno,
                                                     const void *cookie,
                                                     bool force,
                                                     bool allowExisting,
                                                     uint8_t nru,
                                                     bool genBySeqno,
                                                     ExtendedMetaData *emd,
                                                     bool isReplication)
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
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a setWithMeta op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true,
                                          false);

    bool maybeKeyExists = true;
    if (!force) {
        if (v)  {
            if (v->isTempInitialItem()) {
                bgFetch(itm.getKey(), itm.getVBucketId(), cookie, true);
                return ENGINE_EWOULDBLOCK;
            }

            enum conflict_resolution_mode confResMode = revision_seqno;
            if (emd) {
                confResMode = static_cast<enum conflict_resolution_mode>(
                                                       emd->getConflictResMode());
            }

            if (!conflictResolver->resolve(vb, v, itm.getMetaData(), false,
                                           confResMode)) {
                ++stats.numOpsSetMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            if (vb->maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                             cookie, true, isReplication);
            } else {
                maybeKeyExists = false;
            }
        }
    } else {
        if (eviction_policy == FULL_EVICTION) {
            // Check Bloomfilter's prediction
            if (!vb->maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (vb->getState() == vbucket_state_replica ||
         vb->getState() == vbucket_state_pending)) {
        v->unlock();
    }

    mutation_type_t mtype = vb->ht.unlocked_set(v, itm, cas, allowExisting,
                                                true, eviction_policy, nru,
                                                maybeKeyExists, isReplication);

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
        /* set the conflict resolution mode from the extended meta data *
         * Given that the mode is already set, we don't need to set the *
         * conflict resolution mode in queueDirty */
        if (emd) {
            v->setConflictResMode(
                      static_cast<enum conflict_resolution_mode>(
                                            emd->getConflictResMode()));
        }
        vb->setMaxCas(v->getCas());
        queueDirty(vb, v, &lh, seqno, false, true, genBySeqno, false);
        break;
    case NOT_FOUND:
        ret = ENGINE_KEY_ENOENT;
        break;
    case NEED_BG_FETCH:
        {            // CAS operation with non-resident item + full eviction.
            if (v) { // temp item is already created. Simply schedule a
                lh.unlock(); // bg fetch job.
                bgFetch(itm.getKey(), vb->getId(), cookie, true);
                return ENGINE_EWOULDBLOCK;
            }

            ret = addTempItemForBgFetch(lh, bucket_num, itm.getKey(), vb,
                                        cookie, true, isReplication);
        }
    }

    // Update drift counter for vbucket upon a success only
    if (ret == ENGINE_SUCCESS && emd) {
        vb->setDriftCounter(emd->getAdjustedTime());
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
            bgFetch(key, vbucket, cookie);
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
            // persist the item in the underlying storage for
            // mutated exptime
            queueDirty(vb, v, &lh, NULL);
        }
        return rv;
    } else {
        if (eviction_policy == VALUE_ONLY) {
            GetValue rv;
            return rv;
        } else {
            if (vb->maybeKeyExistsInFilter(key)) {
                ENGINE_ERROR_CODE ec = addTempItemForBgFetch(lh, bucket_num,
                                                             key, vb, cookie,
                                                             false);
                return GetValue(NULL, ec, -1, true);
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getAndUpdateTtl().
                GetValue rv;
                return rv;
            }
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

    getROUnderlying(vbid)->get(key, vbid, gcb);
    gcb.waitForValue();

    if (eviction_policy == FULL_EVICTION) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (vb) {
            int bucket_num(0);
            LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
            StoredValue *v = fetchValidValue(vb, key, bucket_num, true);
            if (v && v->isTempInitialItem()) {
                if (gcb.val.getStatus() == ENGINE_SUCCESS) {
                    v->unlocked_restoreValue(gcb.val.getValue(), vb->ht);
                    if (!v->isResident()) {
                        throw std::logic_error("EPStore::completeStatsVKey: "
                            "storedvalue (which has key " + v->getKey() +
                            ") should be resident after calling restoreValue()");
                    }
                } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
                    v->setNonExistent();
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Failed background fetch for vb=%d "
                        "seq=%" PRId64 " key=%s", vbid, v->getBySeqno(),
                        key.c_str());
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
                bgFetch(key, vbucket, cookie);
            }
            GetValue rv(NULL, ENGINE_EWOULDBLOCK, -1, true);
            cb.callback(rv);
            return false;
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout);

        Item *it = v->toItem(false, vbucket);
        it->setCas(vb->nextHLCCas());
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
            if (vb->maybeKeyExistsInFilter(key)) {
                ENGINE_ERROR_CODE ec = addTempItemForBgFetch(lh, bucket_num,
                                                             key, vb, cookie,
                                                             false);
                GetValue rv(NULL, ec, -1, true);
                cb.callback(rv);
                return false;
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getLocked().
                GetValue rv;
                cb.callback(rv);
                return true;
            }
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
            bgFetch(key, vbucket, cookie, true);
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
            if (bgfetch && vb->maybeKeyExistsInFilter(key)) {
                return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                             cookie, true);
            } else {
                // If bgFetch were false, or bloomfilter predicted that
                // item surely doesn't exist on disk, return ENOENT for
                // getKeyStats().
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
                                                        uint64_t *cas,
                                                        uint16_t vbucket,
                                                        const void *cookie,
                                                        bool force,
                                                        ItemMetaData *itemMeta,
                                                        mutation_descr_t *mutInfo,
                                                        bool tapBackfill)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() == vbucket_state_dead && !force)) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a delete op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
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
                    if (vb->maybeKeyExistsInFilter(key)) {
                        return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                                     cookie, true);
                    } else {
                        // As bloomfilter predicted that item surely doesn't
                        // exist on disk, return ENOENT for deleteItem().
                        return ENGINE_KEY_ENOENT;
                    }
                } else if (v->isTempInitialItem()) {
                    lh.unlock();
                    bgFetch(key, vbucket, cookie, true);
                    return ENGINE_EWOULDBLOCK;
                } else { // Non-existent or deleted key.
                    if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                        // Delete a temp non-existent item to ensure that
                        // if a delete were issued over an item that doesn't
                        // exist, then we don't preserve a temp item.
                        vb->ht.unlocked_del(key, bucket_num);
                    }
                    return ENGINE_KEY_ENOENT;
                }
            } else {
                if (!v) { // Item might be evicted from cache.
                    // Create a temp item and delete it below as it is a
                    // force deletion, only if bloomfilter predicts that
                    // item may exist on disk.
                    if (vb->maybeKeyExistsInFilter(key)) {
                        add_type_t rv = vb->ht.unlocked_addTempItem(
                                                               bucket_num,
                                                               key,
                                                               eviction_policy);
                        if (rv == ADD_NOMEM) {
                            return ENGINE_ENOMEM;
                        }
                        v = vb->ht.unlocked_find(key, bucket_num, true, false);
                        v->setDeleted();
                    } else {
                        return ENGINE_KEY_ENOENT;
                    }
                } else if (v->isTempInitialItem()) {
                    v->setDeleted();
                } else { // Non-existent or deleted key.
                    if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                        // Delete a temp non-existent item to ensure that
                        // if a delete were issued over an item that doesn't
                        // exist, then we don't preserve a temp item.
                        vb->ht.unlocked_del(key, bucket_num);
                    }
                    return ENGINE_KEY_ENOENT;
                }
            }
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (vb->getState() == vbucket_state_replica ||
         vb->getState() == vbucket_state_pending)) {
        v->unlock();
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

    uint64_t seqno = 0;
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
        if (v) {
            queueDirty(vb, v, &lh, NULL, tapBackfill);
        }
        break;
    case WAS_DIRTY:
    case WAS_CLEAN:
        queueDirty(vb, v, &lh, &seqno, tapBackfill);
        mutInfo->seqno = seqno;
        mutInfo->vbucket_uuid = vb->failovers->getLatestUUID();
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
                                                     uint64_t *cas,
                                                     uint64_t *seqno,
                                                     uint16_t vbucket,
                                                     const void *cookie,
                                                     bool force,
                                                     ItemMetaData *itemMeta,
                                                     bool tapBackfill,
                                                     bool genBySeqno,
                                                     uint64_t bySeqno,
                                                     ExtendedMetaData *emd,
                                                     bool isReplication)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() == vbucket_state_dead && !force)) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a deleteWithMeta "
                "op, becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true, false);
    if (!force) { // Need conflict resolution.
        if (v)  {
            if (v->isTempInitialItem()) {
                bgFetch(key, vbucket, cookie, true);
                return ENGINE_EWOULDBLOCK;
            }

            enum conflict_resolution_mode confResMode = revision_seqno;
            if (emd) {
                confResMode = static_cast<enum conflict_resolution_mode>(
                                                       emd->getConflictResMode());
            }

            if (!conflictResolver->resolve(vb, v, *itemMeta, true, confResMode)) {
                ++stats.numOpsDelMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            if (vb->maybeKeyExistsInFilter(key)) {
                return addTempItemForBgFetch(lh, bucket_num, key, vb,
                                             cookie, true, isReplication);
            } else {
                // Even though bloomfilter predicted that item doesn't exist
                // on disk, we must put this delete on disk if the cas is valid.
                add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                            eviction_policy,
                                                            isReplication);
                if (rv == ADD_NOMEM) {
                    return ENGINE_ENOMEM;
                }
                v = vb->ht.unlocked_find(key, bucket_num, true, false);
                v->setDeleted();
            }
        }
    } else {
        if (!v) {
            // We should always try to persist a delete here.
            add_type_t rv = vb->ht.unlocked_addTempItem(bucket_num, key,
                                                        eviction_policy,
                                                        isReplication);
            if (rv == ADD_NOMEM) {
                return ENGINE_ENOMEM;
            }
            v = vb->ht.unlocked_find(key, bucket_num, true, false);
            v->setDeleted();
        } else if (v->isTempInitialItem()) {
            v->setDeleted();
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (vb->getState() == vbucket_state_replica ||
         vb->getState() == vbucket_state_pending)) {
        v->unlock();
    }
    mutation_type_t delrv;
    delrv = vb->ht.unlocked_softDelete(v, *cas, *itemMeta,
                                       eviction_policy, true);
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
        if (!genBySeqno) {
            v->setBySeqno(bySeqno);
        }

        /* set the conflict resolution mode from the extended meta data *
         * Given that the mode is already set, we don't need to set the *
         * conflict resolution mode in queueDirty */
        if (emd) {
            v->setConflictResMode(
               static_cast<enum conflict_resolution_mode>(
                                         emd->getConflictResMode()));
        }
        vb->setMaxCas(v->getCas());
        queueDirty(vb, v, &lh, seqno, tapBackfill, true, genBySeqno, false);
        break;
    case NEED_BG_FETCH:
        lh.unlock();
        bgFetch(key, vbucket, cookie, true);
        ret = ENGINE_EWOULDBLOCK;
    }

    // Update drift counter for vbucket upon a success only
    if (ret == ENGINE_SUCCESS && emd) {
        vb->setDriftCounter(emd->getAdjustedTime());
    }

    return ret;
}

void EventuallyPersistentStore::reset() {
    auto buckets = vbMap.getBuckets();
    for (auto vbid : buckets) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (vb) {
            LockHolder lh(vb_mutexes[vb->getId()]);
            vb->ht.clear();
            vb->checkpointManager.clear(vb->getState());
            vb->resetStats();
            vb->setPersistedSnapshot(0, 0);
        }
    }

    ++stats.diskQueueSize;
    bool inverse = true;
    flushAllTaskCtx.delayFlushAll.compare_exchange_strong(inverse, false);
    // Waking up (notifying) one flusher is good enough for diskFlushAll
    vbMap.shards[EP_PRIMARY_SHARD]->getFlusher()->notifyFlushEvent();
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
                        EventuallyPersistentStore& st, EPStats& s, uint64_t c)
        : queuedItem(qi), vbucket(vb), store(st), stats(s), cas(c) {
        if (!vb) {
            throw std::invalid_argument("PersistenceCallback(): vb is NULL");
        }
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(),
                                                        &bucket_num);
            StoredValue *v = store.fetchValidValue(vbucket,
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
                        vbucket->incrMetaDataDisk(*queuedItem);
                    } else { // Update in full eviction mode.
                        vbucket->ht.decrNumTotalItems();
                        ++vbucket->opsUpdate;
                    }
                    v->setNewCacheItem(false);
                } else { // Update in value-only or full eviction mode.
                    ++vbucket->opsUpdate;
                }
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats.decrDiskQueueSize(1);
            stats.totalPersisted++;
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                int bucket_num(0);
                LockHolder lh = vbucket->ht.getLockedBucket(
                                           queuedItem->getKey(), &bucket_num);
                StoredValue *v = store.fetchValidValue(vbucket,
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
                stats.decrDiskQueueSize(1);
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
        if (value > 1) {
            throw std::logic_error("PersistenceCallback::callback: value "
                    "(which is " + std::to_string(value) +
                    ") should be <= 1 for deletions");
        }
        // -1 means fail
        // 1 means we deleted one row
        // 0 means we did not delete a row, but did not fail (did not exist)
        if (value >= 0) {
            // We have successfully removed an item from the disk, we
            // may now remove it from the hash table.
            int bucket_num(0);
            LockHolder lh = vbucket->ht.getLockedBucket(queuedItem->getKey(),
                                                        &bucket_num);
            StoredValue *v = store.fetchValidValue(vbucket,
                                                   queuedItem->getKey(),
                                                   bucket_num, true, false);
            if (v && v->isDeleted()) {
                bool newCacheItem = v->isNewCacheItem();
                bool deleted = vbucket->ht.unlocked_del(queuedItem->getKey(),
                                                        bucket_num);
                if (!deleted) {
                    throw std::logic_error("PersistenceCallback:callback: "
                            "Failed to delete key '" + queuedItem->getKey() +
                            "' from bucket " + std::to_string(bucket_num));
                }
                if (newCacheItem && value > 0) {
                    // Need to decrement the item counter again for an item that
                    // exists on DB file, but not in memory (i.e., full eviction),
                    // because we created the temp item in memory and incremented
                    // the item counter when a deletion is pushed in the queue.
                    vbucket->ht.decrNumTotalItems();
                }

                /**
                 * Deleted items are to be added to the bloomfilter,
                 * in either eviction policy.
                 */
                vbucket->addToFilter(queuedItem->getKey());
            }

            if (value > 0) {
                ++stats.totalPersisted;
                ++vbucket->opsDelete;
            }
            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats.decrDiskQueueSize(1);
            vbucket->decrMetaDataDisk(*queuedItem);
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
        if (store.vbMap.isBucketDeletion(vbucket->getId())) {
            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            stats.decrDiskQueueSize(1);
            return;
        }
        ++stats.flushFailed;
        store.invokeOnLockedStoredValue(queuedItem->getKey(),
                                         queuedItem->getVBucketId(),
                                         &StoredValue::reDirty);
        vbucket->rejectQueue.push(queuedItem);
    }

    const queued_item queuedItem;
    RCPtr<VBucket> vbucket;
    EventuallyPersistentStore& store;
    EPStats& stats;
    uint64_t cas;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

bool EventuallyPersistentStore::scheduleFlushAllTask(const void* cookie,
                                                     time_t when) {
    bool inverse = false;
    if (diskFlushAll.compare_exchange_strong(inverse, true)) {
        flushAllTaskCtx.cookie = cookie;
        flushAllTaskCtx.delayFlushAll.compare_exchange_strong(inverse, true);
        ExTask task = new FlushAllTask(&engine, static_cast<double>(when));
        ExecutorPool::get()->schedule(task, NONIO_TASK_IDX);
        return true;
    } else {
        return false;
    }
}

void EventuallyPersistentStore::setFlushAllComplete() {
    // Notify memcached about flushAll task completion, and
    // set diskFlushall flag to false
    if (flushAllTaskCtx.cookie) {
        engine.notifyIOComplete(flushAllTaskCtx.cookie, ENGINE_SUCCESS);
    }
    bool inverse = false;
    flushAllTaskCtx.delayFlushAll.compare_exchange_strong(inverse, true);
    inverse = true;
    diskFlushAll.compare_exchange_strong(inverse, false);
}

void EventuallyPersistentStore::flushOneDeleteAll() {
    for (VBucketMap::id_type i = 0; i < vbMap.getSize(); ++i) {
        RCPtr<VBucket> vb = getVBucket(i);
        if (vb) {
            LockHolder lh(vb_mutexes[vb->getId()]);
            getRWUnderlying(vb->getId())->reset(i);
        }
    }

    stats.decrDiskQueueSize(1);
    setFlushAllComplete();
}

int EventuallyPersistentStore::flushVBucket(uint16_t vbid) {
    KVShard *shard = vbMap.getShard(vbid);
    if (diskFlushAll && !flushAllTaskCtx.delayFlushAll) {
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
    rel_time_t flush_start = ep_current_time();

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb) {
        LockHolder lh(vb_mutexes[vbid], true /*tryLock*/);
        if (!lh.islocked()) { // Try another bucket if this one is locked
            return RETRY_FLUSH_VBUCKET; // to avoid blocking flusher
        }

        std::vector<queued_item> items;
        KVStore *rwUnderlying = getRWUnderlying(vbid);

        while (!vb->rejectQueue.empty()) {
            items.push_back(vb->rejectQueue.front());
            vb->rejectQueue.pop();
        }

        const std::string cursor(CheckpointManager::pCursorName);
        vb->getBackfillItems(items);

        snapshot_range_t range;
        range = vb->checkpointManager.getAllItemsForCursor(cursor, items);

        if (!items.empty()) {
            while (!rwUnderlying->begin()) {
                ++stats.beginFailed;
                LOG(EXTENSION_LOG_WARNING, "Failed to start a transaction!!! "
                    "Retry in 1 sec ...");
                sleep(1);
            }
            rwUnderlying->optimizeWrites(items);

            Item *prev = NULL;
            uint64_t maxSeqno = 0;
            uint64_t maxCas = 0;
            uint64_t maxDeletedRevSeqno = 0;
            std::list<PersistenceCallback*>& pcbs = rwUnderlying->getPersistenceCbList();
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

                    maxSeqno = std::max(maxSeqno, (uint64_t)(*it)->getBySeqno());
                    maxCas = std::max(maxCas, (uint64_t)(*it)->getCas());
                    if ((*it)->isDeleted()) {
                        maxDeletedRevSeqno = std::max(maxDeletedRevSeqno,
                                                      (uint64_t)(*it)->getRevSeqno());
                    }
                    ++stats.flusher_todo;
                } else {
                    stats.decrDiskQueueSize(1);
                    vb->doStatsForFlushing(*(*it), (*it)->size());
                }
            }


            if (vb->getState() == vbucket_state_active) {
                range.start = maxSeqno;
                range.end = maxSeqno;
            }

            std::string failovers = vb->failovers->toJSON();
            vbucket_state vbState(vb->getState(), vbMap.getPersistenceCheckpointId(vbid),
                                  maxDeletedRevSeqno, vb->getHighSeqno(),
                                  vb->getPurgeSeqno(), range.start, range.end, maxCas,
                                  vb->getDriftCounter(), failovers);

            if (rwUnderlying->snapshotVBucket(vb->getId(), vbState,
                                              NULL, false) != true) {
                return RETRY_FLUSH_VBUCKET;
            }

            //commit all mutations to disk if the commit interval is zero
            if (decrCommitInterval(shard->getId()) == 0) {
                commit(shard->getId());
            }

            hrtime_t end = gethrtime();
            uint64_t trans_time = (end - flush_start) / 1000000;

            lastTransTimePerItem.store((items_flushed == 0) ? 0 :
                                       static_cast<double>(trans_time) /
                                       static_cast<double>(items_flushed));
            stats.cumulativeFlushTime.fetch_add(ep_current_time()
                                                - flush_start);
            stats.flusher_todo.store(0);
            if (vb->rejectQueue.empty()) {
                vb->setPersistedSnapshot(range.start, range.end);
                uint64_t highSeqno = rwUnderlying->getLastPersistedSeqno(vbid);
                if (highSeqno > 0 &&
                    highSeqno != vbMap.getPersistenceSeqno(vbid)) {
                    vbMap.setPersistenceSeqno(vbid, highSeqno);
                    vb->notifySeqnoPersisted(highSeqno);
                }
            }
        }

        rwUnderlying->pendingTasks();

        if (vb->checkpointManager.getNumCheckpoints() > 1) {
            wakeUpCheckpointRemover();
        }

        if (vb->rejectQueue.empty()) {
            vb->checkpointManager.itemsPersisted();
            uint64_t seqno = vbMap.getPersistenceSeqno(vbid);
            uint64_t chkid = vb->checkpointManager.getPersistenceCursorPreChkId();
            vb->notifyCheckpointPersisted(engine, seqno, true);
            vb->notifyCheckpointPersisted(engine, chkid, false);
            if (chkid > 0 && chkid != vbMap.getPersistenceCheckpointId(vbid)) {
                vbMap.setPersistenceCheckpointId(vbid, chkid);
            }
        } else {
            return RETRY_FLUSH_VBUCKET;
        }
    }

    return items_flushed;
}

void EventuallyPersistentStore::commit(uint16_t shardId) {
    KVStore *rwUnderlying = getRWUnderlyingByShard(shardId);
    std::list<PersistenceCallback *>& pcbs = rwUnderlying->getPersistenceCbList();
    BlockTimer timer(&stats.diskCommitHisto, "disk_commit", stats.timingLog);
    hrtime_t commit_start = gethrtime();

    KVStatsCallback cb(this);
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
    hrtime_t commit_end = gethrtime();
    uint64_t commit_time = (commit_end - commit_start) / 1000000;
    stats.commit_time.store(commit_time);
    stats.cumulativeCommitTime.fetch_add(commit_time);
}

PersistenceCallback*
EventuallyPersistentStore::flushOneDelOrSet(const queued_item &qi,
                                            RCPtr<VBucket> &vb) {

    if (!vb) {
        stats.decrDiskQueueSize(1);
        return NULL;
    }

    int64_t bySeqno = qi->getBySeqno();
    bool deleted = qi->isDeleted();
    rel_time_t queued(qi->getQueuedTime());

    int dirtyAge = ep_current_time() - queued;
    stats.dirtyAgeHisto.add(dirtyAge * 1000000);
    stats.dirtyAge.store(dirtyAge);
    stats.dirtyAgeHighWat.store(std::max(stats.dirtyAge.load(),
                                         stats.dirtyAgeHighWat.load()));

    // Wait until the vbucket database is created by the vbucket state
    // snapshot task.
    if (vbMap.isBucketCreation(qi->getVBucketId()) ||
        vbMap.isBucketDeletion(qi->getVBucketId())) {
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
            new PersistenceCallback(qi, vb, *this, stats, qi->getCas());
        rwUnderlying->set(*qi, *cb);
        return cb;
    } else {
        BlockTimer timer(&stats.diskDelHisto, "disk_delete",
                         stats.timingLog);
        PersistenceCallback *cb =
            new PersistenceCallback(qi, vb, *this, stats, 0);
        rwUnderlying->del(*qi, *cb);
        return cb;
    }
}

void EventuallyPersistentStore::queueDirty(RCPtr<VBucket> &vb,
                                           StoredValue* v,
                                           LockHolder *plh,
                                           uint64_t *seqno,
                                           bool tapBackfill,
                                           bool notifyReplicator,
                                           bool genBySeqno,
                                           bool setConflictMode) {
    if (vb) {
        if (setConflictMode && (v->getConflictResMode() != last_write_wins) &&
            vb->isTimeSyncEnabled()) {
            v->setConflictResMode(last_write_wins);
        }

        queued_item qi(v->toItem(false, vb->getId()));

        bool rv = tapBackfill ? vb->queueBackfillItem(qi, genBySeqno) :
                                vb->checkpointManager.queueDirty(vb, qi,
                                                                 genBySeqno);
        v->setBySeqno(qi->getBySeqno());

        if (seqno) {
            *seqno = v->getBySeqno();
        }

        if (plh) {
            plh->unlock();
        }

        if (rv) {
            KVShard* shard = vbMap.getShard(vb->getId());
            shard->getFlusher()->notifyFlushEvent();

        }
        if (!tapBackfill && notifyReplicator) {
            engine.getTapConnMap().notifyVBConnections(vb->getId());
            engine.getDcpConnMap().notifyVBConnections(vb->getId(),
                                                       qi->getBySeqno());
        }
    }
}

std::vector<vbucket_state *> EventuallyPersistentStore::loadVBucketState()
{
    return getOneROUnderlying()->listPersistedVbuckets();
}

void EventuallyPersistentStore::warmupCompleted() {
    // Run the vbucket state snapshot job once after the warmup
    scheduleVBSnapshot(Priority::VBucketPersistHighPriority);

    if (engine.getConfiguration().getAlogPath().length() > 0) {

        if (engine.getConfiguration().isAccessScannerEnabled()) {
            LockHolder lh(accessScanner.mutex);
            accessScanner.enabled = true;
            lh.unlock();
            LOG(EXTENSION_LOG_NOTICE, "Access Scanner task enabled");
            size_t smin = engine.getConfiguration().getAlogSleepTime();
            setAccessScannerSleeptime(smin, true);
        } else {
            LockHolder lh(accessScanner.mutex);
            accessScanner.enabled = false;
            LOG(EXTENSION_LOG_NOTICE, "Access Scanner task disabled");
        }

        Configuration &config = engine.getConfiguration();
        config.addValueChangedListener("access_scanner_enabled",
                                       new EPStoreValueChangeListener(*this));
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
        LOG(EXTENSION_LOG_NOTICE,
            "Total memory use reached to the low water mark, stop warmup");
        return true;
    } else if (memoryUsed > (maxSize * stats.warmupMemUsedCap)) {
        LOG(EXTENSION_LOG_NOTICE,
                "Enough MB of data loaded to enable traffic");
        return true;
    } else if (eviction_policy == VALUE_ONLY &&
               stats.warmedUpValues >=
                               (stats.warmedUpKeys * stats.warmupNumReadCap)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        LOG(EXTENSION_LOG_NOTICE,
            "Enough number of items loaded to enable traffic");
        return true;
    } else if (eviction_policy == FULL_EVICTION &&
               stats.warmedUpValues >=
                            (warmupTask->getEstimatedItemCount() *
                             stats.warmupNumReadCap)) {
        // In case of FULL EVICTION, warmed up keys always matches the number
        // of warmed up values, therefore for honoring the min_item threshold
        // in this scenario, we can consider warmup's estimated item count.
        LOG(EXTENSION_LOG_NOTICE,
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

bool EventuallyPersistentStore::isMemoryUsageTooHigh() {
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());
    return memoryUsed > (maxSize * backfillMemoryThreshold);
}

void EventuallyPersistentStore::setBackfillMemoryThreshold(
                                                double threshold) {
    backfillMemoryThreshold = threshold;
}

void EventuallyPersistentStore::setExpiryPagerSleeptime(size_t val) {
    LockHolder lh(expiryPager.mutex);

    ExecutorPool::get()->cancel(expiryPager.task);

    expiryPager.sleeptime = val;
    if (expiryPager.enabled) {
        ExTask expTask = new ExpiredItemPager(&engine, stats,
                                              expiryPager.sleeptime);
        expiryPager.task = ExecutorPool::get()->schedule(expTask, NONIO_TASK_IDX);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry pager disabled, "
                                 "enabling it will make exp_pager_stime (%lu)"
                                 "to go into effect!", val);
    }
}

void EventuallyPersistentStore::setExpiryPagerTasktime(ssize_t val) {
    LockHolder lh(expiryPager.mutex);
    if (expiryPager.enabled) {
        ExecutorPool::get()->cancel(expiryPager.task);
        ExTask expTask = new ExpiredItemPager(&engine, stats,
                                              expiryPager.sleeptime,
                                              val);
        expiryPager.task = ExecutorPool::get()->schedule(expTask,
                                                         NONIO_TASK_IDX);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry pager disabled, "
                                 "enabling it will make exp_pager_stime (%lu)"
                                 "to go into effect!", val);
    }
}

void EventuallyPersistentStore::enableExpiryPager() {
    LockHolder lh(expiryPager.mutex);
    if (!expiryPager.enabled) {
        expiryPager.enabled = true;

        ExecutorPool::get()->cancel(expiryPager.task);
        ExTask expTask = new ExpiredItemPager(&engine, stats,
                                              expiryPager.sleeptime);
        expiryPager.task = ExecutorPool::get()->schedule(expTask,
                                                         NONIO_TASK_IDX);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry Pager already enabled!");
    }
}

void EventuallyPersistentStore::disableExpiryPager() {
    LockHolder lh(expiryPager.mutex);
    if (expiryPager.enabled) {
        ExecutorPool::get()->cancel(expiryPager.task);
        expiryPager.enabled = false;
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry Pager already disabled!");
    }
}

void EventuallyPersistentStore::enableAccessScannerTask() {
    LockHolder lh(accessScanner.mutex);
    if (!accessScanner.enabled) {
        accessScanner.enabled = true;

        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
        }

        size_t alogSleepTime = engine.getConfiguration().getAlogSleepTime();
        accessScanner.sleeptime = alogSleepTime * 60;
        if (accessScanner.sleeptime != 0) {
            ExTask task = new AccessScanner(*this, stats,
                                            Priority::AccessScannerPriority,
                                            accessScanner.sleeptime, true);
            accessScanner.task = ExecutorPool::get()->schedule(task,
                                                               AUXIO_TASK_IDX);
        } else {
            LOG(EXTENSION_LOG_NOTICE, "Did not enable access scanner task, "
                                      "as alog_sleep_time is set to zero!");
        }
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Access scanner already enabled!");
    }
}

void EventuallyPersistentStore::disableAccessScannerTask() {
    LockHolder lh(accessScanner.mutex);
    if (accessScanner.enabled) {
        ExecutorPool::get()->cancel(accessScanner.task);
        accessScanner.sleeptime = 0;
        accessScanner.enabled = false;
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Access scanner already disabled!");
    }
}

void EventuallyPersistentStore::setAccessScannerSleeptime(size_t val,
                                                          bool useStartTime) {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.enabled) {
        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
        }

        // store sleeptime in seconds
        accessScanner.sleeptime = val * 60;
        if (accessScanner.sleeptime != 0) {
            ExTask task = new AccessScanner(*this, stats,
                                            Priority::AccessScannerPriority,
                                            accessScanner.sleeptime,
                                            useStartTime);
            accessScanner.task = ExecutorPool::get()->schedule(task,
                                                               AUXIO_TASK_IDX);
        }
    }
}

void EventuallyPersistentStore::resetAccessScannerStartTime() {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.enabled) {
        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
            // re-schedule task according to the new task start hour
            ExTask task = new AccessScanner(*this, stats,
                                            Priority::AccessScannerPriority,
                                            accessScanner.sleeptime, true);
            accessScanner.task = ExecutorPool::get()->schedule(task,
                                                               AUXIO_TASK_IDX);
        }
    }
}

void EventuallyPersistentStore::setAllBloomFilters(bool to) {
    for (VBucketMap::id_type vbid = 0; vbid < vbMap.getSize(); vbid++) {
        RCPtr<VBucket> vb = vbMap.getBucket(vbid);
        if (vb) {
            if (to) {
                vb->setFilterStatus(BFILTER_ENABLED);
            } else {
                vb->setFilterStatus(BFILTER_DISABLED);
            }
        }
    }
}

void EventuallyPersistentStore::visit(VBucketVisitor &visitor)
{
    for (VBucketMap::id_type vbid = 0; vbid < vbMap.getSize(); ++vbid) {
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

EventuallyPersistentStore::Position
EventuallyPersistentStore::pauseResumeVisit(PauseResumeEPStoreVisitor& visitor,
                                            Position& start_pos)
{
    uint16_t vbid = start_pos.vbucket_id;
    for (; vbid < vbMap.getSize(); ++vbid) {
        RCPtr<VBucket> vb = vbMap.getBucket(vbid);
        if (vb) {
            bool paused = !visitor.visit(vbid, vb->ht);
            if (paused) {
                break;
            }
        }
    }

    return EventuallyPersistentStore::Position(vbid);
}

EventuallyPersistentStore::Position
EventuallyPersistentStore::startPosition() const
{
    return EventuallyPersistentStore::Position(0);
}

EventuallyPersistentStore::Position
EventuallyPersistentStore::endPosition() const
{
    return EventuallyPersistentStore::Position(vbMap.getSize());
}

VBCBAdaptor::VBCBAdaptor(EventuallyPersistentStore *s,
                         shared_ptr<VBucketVisitor> v,
                         const char *l, const Priority &p, double sleep) :
    GlobalTask(&s->getEPEngine(), p, 0, false), store(s),
    visitor(v), label(l), sleepTime(sleep), currentvb(0)
{
    const VBucketFilter &vbFilter = visitor->getVBucketFilter();
    for (auto vbid : store->vbMap.getBuckets()) {
        RCPtr<VBucket> vb = store->vbMap.getBucket(vbid);
        if (vb && vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBCBAdaptor::run(void) {
    if (!vbList.empty()) {
        currentvb.store(vbList.front());
        RCPtr<VBucket> vb = store->vbMap.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                snooze(sleepTime);
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
    for (auto vbid : store->vbMap.getShard(shardID)->getVBuckets()) {
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
                snooze(sleepTime);
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

    for (size_t i = 0; i < MAX_TYPE_ID; i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }
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

class Rollback : public RollbackCB {
public:
    Rollback(EventuallyPersistentEngine& e)
        : RollbackCB(), engine(e) {

    }

    void callback(GetValue& val) {
        if (val.getValue() == nullptr) {
            throw std::invalid_argument("Rollback::callback: val is NULL");
        }
        if (dbHandle == nullptr) {
            throw std::logic_error("Rollback::callback: dbHandle is NULL");
        }
        Item *itm = val.getValue();
        RCPtr<VBucket> vb = engine.getVBucket(itm->getVBucketId());
        int bucket_num(0);
        RememberingCallback<GetValue> gcb;
        engine.getEpStore()->getROUnderlying(itm->getVBucketId())->
                                             getWithHeader(dbHandle,
                                                           itm->getKey(),
                                                           itm->getVBucketId(),
                                                           gcb);
        gcb.waitForValue();
        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            Item *it = gcb.val.getValue();
            if (it->isDeleted()) {
                LockHolder lh = vb->ht.getLockedBucket(it->getKey(),
                        &bucket_num);
                bool ret = vb->ht.unlocked_del(it->getKey(), bucket_num);
                if(!ret) {
                    setStatus(ENGINE_KEY_ENOENT);
                } else {
                    setStatus(ENGINE_SUCCESS);
                }
            } else {
                mutation_type_t mtype = vb->ht.set(*it, it->getCas(),
                                                   true, true,
                                                   engine.getEpStore()->
                                                        getItemEvictionPolicy(),
                                                   INITIAL_NRU_VALUE);
                if (mtype == NOMEM) {
                    setStatus(ENGINE_ENOMEM);
                }
            }
            delete it;
        } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
            LockHolder lh = vb->ht.getLockedBucket(itm->getKey(), &bucket_num);
            bool ret = vb->ht.unlocked_del(itm->getKey(), bucket_num);
            if (!ret) {
                setStatus(ENGINE_KEY_ENOENT);
            } else {
                setStatus(ENGINE_SUCCESS);
            }
        } else {
            LOG(EXTENSION_LOG_WARNING, "Unexpected Error Status: %d",
                gcb.val.getStatus());
        }
        delete itm;
    }

private:
    EventuallyPersistentEngine& engine;
};

ENGINE_ERROR_CODE
EventuallyPersistentStore::rollback(uint16_t vbid,
                                    uint64_t rollbackSeqno) {
    LockHolder lh(vb_mutexes[vbid], true /*tryLock*/);
    if (!lh.islocked()) {
        return ENGINE_TMPFAIL; // Reschedule a vbucket rollback task.
    }

    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    uint64_t prevHighSeqno = static_cast<uint64_t>
                                    (vb->checkpointManager.getHighSeqno());
    if (rollbackSeqno != 0) {
        shared_ptr<Rollback> cb(new Rollback(engine));
        KVStore* rwUnderlying = vbMap.getShard(vbid)->getRWUnderlying();
        RollbackResult result = rwUnderlying->rollback(vbid, rollbackSeqno, cb);

        if (result.success) {
            vb->failovers->pruneEntries(result.highSeqno);
            vb->checkpointManager.clear(vb, result.highSeqno);
            vb->setPersistedSnapshot(result.snapStartSeqno, result.snapEndSeqno);
            vb->incrRollbackItemCount(prevHighSeqno - result.highSeqno);
            return ENGINE_SUCCESS;
        }
    }

    if (resetVBucket(vbid)) {
        RCPtr<VBucket> newVb = vbMap.getBucket(vbid);
        newVb->incrRollbackItemCount(prevHighSeqno);
        return ENGINE_SUCCESS;
    }
    return ENGINE_NOT_MY_VBUCKET;
}

void EventuallyPersistentStore::runDefragmenterTask() {
    defragmenterTask->run();
}

void EventuallyPersistentStore::runAccessScannerTask() {
    ExecutorPool::get()->wake(accessScanner.task);
}

void EventuallyPersistentStore::setCursorDroppingLowerUpperThresholds(
                                                            size_t maxSize) {
    Configuration &config = engine.getConfiguration();
    stats.cursorDroppingLThreshold.store(static_cast<size_t>(maxSize *
                    ((double)(config.getCursorDroppingLowerMark()) / 100)));
    stats.cursorDroppingUThreshold.store(static_cast<size_t>(maxSize *
                    ((double)(config.getCursorDroppingUpperMark()) / 100)));
}

std::ostream& operator<<(std::ostream& os,
                         const EventuallyPersistentStore::Position& pos) {
    os << "vbucket:" << pos.vbucket_id;
    return os;
}
