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

#include <phosphor/phosphor.h>
#include <platform/make_unique.h>

#include "access_scanner.h"
#include "checkpoint_remover.h"
#include "collections/manager.h"
#include "conflict_resolution.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "defragmenter.h"
#include "ep_time.h"
#include "ext_meta_parser.h"
#include "failover-table.h"
#include "flusher.h"
#include "htresizer.h"
#include "kv_bucket.h"
#include "kvshard.h"
#include "kvstore.h"
#include "locks.h"
#include "mutation_log.h"
#include "replicationthrottle.h"
#include "statwriter.h"
#include "tapconnmap.h"
#include "tasks.h"
#include "vb_count_visitor.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats& st, KVBucket& str)
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
    EPStats& stats;
    KVBucket& store;
};

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
class EPStoreValueChangeListener : public ValueChangedListener {
public:
    EPStoreValueChangeListener(KVBucket& st) : store(st) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("bg_fetch_delay") == 0) {
            store.setBGFetchDelay(static_cast<uint32_t>(value));
        } else if (key.compare("compaction_write_queue_cap") == 0) {
            store.setCompactionWriteQueueCap(value);
        } else if (key.compare("exp_pager_stime") == 0) {
            store.setExpiryPagerSleeptime(value);
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
        } else if (key.compare("replication_throttle_cap_pcnt") == 0) {
            store.getEPEngine().getReplicationThrottle().setCapPercent(value);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to change value for unknown variable, %s\n",
                key.c_str());
        }
    }

    virtual void ssizeValueChanged(const std::string& key, ssize_t value) {
        if (key.compare("exp_pager_initial_run_time") == 0) {
            store.setExpiryPagerTasktime(value);
        } else if (key.compare("replication_throttle_queue_cap") == 0) {
            store.getEPEngine().getReplicationThrottle().setQueueCap(value);
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
    KVBucket& store;
};

/**
 * Callback class used by EpStore, for adding relevant keys
 * to bloomfilter during compaction.
 */
class BloomFilterCallback : public Callback<uint16_t&, const DocKey&, bool&> {
public:
    BloomFilterCallback(KVBucket& eps)
        : store(eps) {
    }

    void callback(uint16_t& vbucketId, const DocKey& key, bool& isDeleted) {
        VBucketPtr vb = store.getVBucket(vbucketId);
        if (vb) {
            /* Check if a temporary filter has been initialized. If not,
             * initialize it. If initialization fails, throw an exception
             * to the caller and let the caller deal with it.
             */
            bool tempFilterInitialized = vb->isTempFilterAvailable();
            if (!tempFilterInitialized) {
                tempFilterInitialized = initTempFilter(vbucketId);
            }

            if (!tempFilterInitialized) {
                throw std::runtime_error("BloomFilterCallback::callback: Failed "
                    "to initialize temporary filter for vbucket: " +
                    std::to_string(vbucketId));
            }

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
                bool residentRatioLessThanThreshold =
                        vb->isResidentRatioUnderThreshold(
                                store.getBfiltersResidencyThreshold());
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

private:
    bool initTempFilter(uint16_t vbucketId);
    KVBucket& store;
};

bool BloomFilterCallback::initTempFilter(uint16_t vbucketId) {
    Configuration& config = store.getEPEngine().getConfiguration();
    VBucketPtr vb = store.getVBucket(vbucketId);
    if (!vb) {
        return false;
    }

    size_t initial_estimation = config.getBfilterKeyCount();
    size_t estimated_count;
    size_t num_deletes = store.getROUnderlying(vbucketId)->
                                         getNumPersistedDeletes(vbucketId);
    item_eviction_policy_t eviction_policy = store.getItemEvictionPolicy();
    if (eviction_policy == VALUE_ONLY) {
        /**
         * VALUE-ONLY EVICTION POLICY
         * Obtain number of persisted deletes from underlying kvstore.
         * Bloomfilter's estimated_key_count = 1.25 * deletes
         */
        estimated_count = round(1.25 * num_deletes);
    } else {
        /**
         * FULL EVICTION POLICY
         * First determine if the resident ratio of vbucket is less than
         * the threshold from configuration.
         */
        bool residentRatioAlert = vb->isResidentRatioUnderThreshold(
                store.getBfiltersResidencyThreshold());

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
             estimated_count = round(1.25 * vb->getNumItems());
         } else {
             estimated_count = round(1.25 * (num_deletes +
                                      vb->getNumNonResidentItems()));
         }
    }

    if (estimated_count < initial_estimation) {
        estimated_count = initial_estimation;
    }

    vb->initTempFilter(estimated_count, config.getBfilterFpProb());

    return true;
}

class ExpiredItemsCallback : public Callback<Item&, time_t&> {
    public:
        ExpiredItemsCallback(KVBucket& store)
            : epstore(store) { }

        void callback(Item& it, time_t& startTime) {
            if (epstore.compactionCanExpireItems()) {
                epstore.deleteExpiredItem(it, startTime, ExpireBy::Compactor);
            }
        }

    private:
        KVBucket& epstore;
};

class PendingOpsNotification : public GlobalTask {
public:
    PendingOpsNotification(EventuallyPersistentEngine& e, VBucketPtr& vb)
        : GlobalTask(&e, TaskId::PendingOpsNotification, 0, false),
          engine(e),
          vbucket(vb),
          description("Notify pending operations for vbucket " +
                      std::to_string(vbucket->getId())) {
    }

    cb::const_char_buffer getDescription() {
        return description;
    }

    bool run(void) {
        TRACE_EVENT("ep-engine/task", "PendingOpsNotification",
                     vbucket->getId());
        vbucket->fireAllOps(engine);
        return false;
    }

private:
    EventuallyPersistentEngine &engine;
    VBucketPtr vbucket;
    const std::string description;
};

KVBucket::KVBucket(EventuallyPersistentEngine& theEngine)
    : engine(theEngine),
      stats(engine.getEpStats()),
      vbMap(theEngine.getConfiguration(), *this),
      defragmenterTask(NULL),
      diskDeleteAll(false),
      bgFetchDelay(0),
      backfillMemoryThreshold(0.95),
      statsSnapshotTaskId(0),
      lastTransTimePerItem(0),
      collectionsManager(std::make_unique<Collections::Manager>()) {
    cachedResidentRatio.activeRatio.store(0);
    cachedResidentRatio.replicaRatio.store(0);

    Configuration &config = engine.getConfiguration();
    for (uint16_t i = 0; i < config.getMaxNumShards(); i++) {
        accessLog.emplace_back(
                config.getAlogPath() + "." + std::to_string(i),
                config.getAlogBlockSize());
    }


    const size_t size = GlobalTask::allTaskIds.size();
    stats.schedulingHisto.resize(size);
    stats.taskRuntimeHisto.resize(size);

    for (size_t i = 0; i < GlobalTask::allTaskIds.size(); i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }

    ExecutorPool::get()->registerTaskable(ObjectRegistry::getCurrentEngine()->getTaskable());

    size_t num_vbs = config.getMaxVbuckets();
    vb_mutexes = new std::mutex[num_vbs];

    *stats.memOverhead = sizeof(KVBucket);

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

    initializeWarmupTask();
}

bool KVBucket::initialize() {
    // We should nuke everything unless we want warmup
    Configuration &config = engine.getConfiguration();
    if (!config.isWarmup()) {
        reset();
    }

    startWarmupTask();

    // Always create the item pager; but leave scheduling up to the specific
    // KVBucket subclasses.
    itemPagerTask = std::make_shared<ItemPager>(&engine, stats);

    initializeExpiryPager(config);

    ExTask htrTask = std::make_shared<HashtableResizerTask>(this, 10);
    ExecutorPool::get()->schedule(htrTask);

    size_t checkpointRemoverInterval = config.getChkRemoverStime();
    chkTask = std::make_shared<ClosedUnrefCheckpointRemoverTask>(
            &engine, stats, checkpointRemoverInterval);
    ExecutorPool::get()->schedule(chkTask);

    ExTask workloadMonitorTask =
            std::make_shared<WorkLoadMonitor>(&engine, false);
    ExecutorPool::get()->schedule(workloadMonitorTask);

#if HAVE_JEMALLOC
    /* Only create the defragmenter task if we have an underlying memory
     * allocator which can facilitate defragmenting memory.
     */
    defragmenterTask = std::make_shared<DefragmenterTask>(&engine, stats);
    ExecutorPool::get()->schedule(defragmenterTask);
#endif

    return true;
}

void KVBucket::initializeWarmupTask() {
    if (engine.getConfiguration().isWarmup()) {
        warmupTask = std::make_unique<Warmup>(*this, engine.getConfiguration());
    }
}

void KVBucket::startWarmupTask() {
    if (warmupTask) {
        warmupTask->start();
    } else {
        // No warmup, immediately online the bucket.
        warmupCompleted();
    }
}

void KVBucket::deinitialize() {
    stopWarmup();
    ExecutorPool::get()->stopTaskGroup(engine.getTaskable().getGID(),
                                       NONIO_TASK_IDX, stats.forceShutdown);

    ExecutorPool::get()->cancel(statsSnapshotTaskId);

    {
        LockHolder lh(accessScanner.mutex);
        ExecutorPool::get()->cancel(accessScanner.task);
    }

    ExecutorPool::get()->unregisterTaskable(engine.getTaskable(),
                                            stats.forceShutdown);
}

KVBucket::~KVBucket() {
    delete [] vb_mutexes;
    defragmenterTask.reset();
}

const Flusher* KVBucket::getFlusher(uint16_t shardId) {
    return vbMap.shards[shardId]->getFlusher();
}

Warmup* KVBucket::getWarmup(void) const {
    return warmupTask.get();
}

bool KVBucket::pauseFlusher() {
    // Nothing do to - no flusher in this class
    return false;
}

bool KVBucket::resumeFlusher() {
    // Nothing do to - no flusher in this class
    return false;
}

void KVBucket::wakeUpFlusher() {
    // Nothing do to - no flusher in this class
}

protocol_binary_response_status KVBucket::evictKey(const DocKey& key,
                                                   VBucket::id_type vbucket,
                                                   const char** msg) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb || (vb->getState() != vbucket_state_active)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    return vb->evictKey(key, msg);
}

void KVBucket::deleteExpiredItem(Item& it,
                                 time_t startTime,
                                 ExpireBy source) {
    VBucketPtr vb = getVBucket(it.getVBucketId());

    if (vb) {
        auto info = it.toItemInfo(vb->failovers->getLatestUUID(),
                                  vb->getHLCEpochSeqno());
        if (engine.getServerApi()->document->pre_expiry(info)) {
            // The payload is modified and contains data we should use
            value_t value(Blob::New(static_cast<char*>(info.value[0].iov_base),
                                    info.value[0].iov_len,
                                    &info.datatype, 1));
            it.setValue(value);
        } else {
            // We should drop the entire body
            it.setValue({});
        }

        // Obtain reader access to the VB state change lock so that
        // the VB can't switch state whilst we're processing
        ReaderLockHolder rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_active) {
            vb->deleteExpiredItem(it, startTime, source);
        }
    }
}

void KVBucket::deleteExpiredItems(
        std::list<Item>& itms, ExpireBy source) {
    std::list<std::pair<uint16_t, std::string> >::iterator it;
    time_t startTime = ep_real_time();
    for (auto& it : itms) {
        deleteExpiredItem(it, startTime, source);
    }
}

bool KVBucket::isMetaDataResident(VBucketPtr &vb, const DocKey& key) {

    if (!vb) {
        throw std::invalid_argument("EPStore::isMetaDataResident: vb is NULL");
    }

    auto hbl = vb->ht.getLockedBucket(key);
    StoredValue* v = vb->ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::No, TrackReference::No);

    if (v && !v->isTempItem()) {
        return true;
    } else {
        return false;
    }
}

ENGINE_ERROR_CODE KVBucket::set(Item& itm,
                                const void* cookie,
                                cb::StoreIfPredicate predicate) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this set
    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a set op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    { // collections read-lock scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(itm.getKey())) {
            return ENGINE_UNKNOWN_COLLECTION;
        } // now hold collections read access for the duration of the set

        return vb->set(itm, cookie, engine, bgFetchDelay, predicate);
    }
}

ENGINE_ERROR_CODE KVBucket::add(Item &itm, const void *cookie)
{
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this add
    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
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

    { // collections read-lock scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(itm.getKey())) {
            return ENGINE_UNKNOWN_COLLECTION;
        } // now hold collections read access for the duration of the set

        return vb->add(itm, cookie, engine, bgFetchDelay);
    }
}

ENGINE_ERROR_CODE KVBucket::replace(Item& itm,
                                    const void* cookie,
                                    cb::StoreIfPredicate predicate) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this replace
    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    { // collections read-lock scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(itm.getKey())) {
            return ENGINE_UNKNOWN_COLLECTION;
        } // now hold collections read access for the duration of the set

        return vb->replace(itm, cookie, engine, bgFetchDelay, predicate);
    }
}

ENGINE_ERROR_CODE KVBucket::addBackfillItem(Item& itm,
                                            GenerateBySeqno genBySeqno,
                                            ExtendedMetaData* emd) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this add-tapbackfill
    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return ENGINE_KEY_EEXISTS;
    }

    return vb->addBackfillItem(itm, genBySeqno);
}

ENGINE_ERROR_CODE KVBucket::setVBucketState(uint16_t vbid,
                                            vbucket_state_t to,
                                            bool transfer,
                                            bool notify_dcp) {
    // Lock to prevent a race condition between a failed update and add.
    std::unique_lock<std::mutex> lh(vbsetMutex);
    return setVBucketState_UNLOCKED(vbid, to, transfer, notify_dcp, lh);
}

ENGINE_ERROR_CODE KVBucket::setVBucketState_UNLOCKED(
        uint16_t vbid,
        vbucket_state_t to,
        bool transfer,
        bool notify_dcp,
        std::unique_lock<std::mutex>& vbset,
        WriterLockHolder* vbStateLock) {
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (vb && to == vb->getState()) {
        return ENGINE_SUCCESS;
    }

    if (vb) {
        vbucket_state_t oldstate = vb->getState();

        if (vbStateLock) {
            vb->setState_UNLOCKED(to, *vbStateLock);
        } else {
            vb->setState(to);
        }

        if (oldstate != to && notify_dcp) {
            bool closeInboundStreams = false;
            if (to == vbucket_state_active && !transfer) {
                /**
                 * Close inbound (passive) streams into the vbucket
                 * only in case of a failover.
                 */
                closeInboundStreams = true;
            }
            engine.getDcpConnMap().vbucketStateChanged(vbid, to,
                                                       closeInboundStreams);
        }

        if (to == vbucket_state_active && oldstate == vbucket_state_replica) {
            /**
             * Update snapshot range when vbucket goes from being a replica
             * to active, to maintain the correct snapshot sequence numbers
             * even in a failover scenario.
             */
            vb->checkpointManager.resetSnapshotRange();
        }

        if (to == vbucket_state_active && !transfer) {
            const snapshot_range_t range = vb->getPersistedSnapshot();
            auto highSeqno = range.end == vb->getPersistenceSeqno()
                                     ? range.end
                                     : range.start;
            vb->failovers->createEntry(highSeqno);

            auto entry = vb->failovers->getLatestEntry();
            LOG(EXTENSION_LOG_NOTICE,
                "KVBucket::setVBucketState: vb:%" PRIu16
                " created new failover entry with "
                "uuid:%" PRIu64 " and seqno:%" PRIu64,
                vbid,
                entry.vb_uuid,
                entry.by_seqno);
        }

        if (oldstate == vbucket_state_pending &&
            to == vbucket_state_active) {
            ExTask notifyTask =
                    std::make_shared<PendingOpsNotification>(engine, vb);
            ExecutorPool::get()->schedule(notifyTask);
        }
        scheduleVBStatePersist(vbid);
    } else if (vbid < vbMap.getSize()) {
        auto ft =
                std::make_unique<FailoverTable>(engine.getMaxFailoverEntries());
        KVShard* shard = vbMap.getShardByVbId(vbid);

        VBucketPtr newvb =
                makeVBucket(vbid,
                            to,
                            shard,
                            std::move(ft),
                            std::make_unique<NotifyNewSeqnoCB>(*this));

        Configuration& config = engine.getConfiguration();
        if (config.isBfilterEnabled()) {
            // Initialize bloom filters upon vbucket creation during
            // bucket creation and rebalance
            newvb->createFilter(config.getBfilterKeyCount(),
                                config.getBfilterFpProb());
        }

        // The first checkpoint for active vbucket should start with id 2.
        uint64_t start_chk_id = (to == vbucket_state_active) ? 2 : 0;
        newvb->checkpointManager.setOpenCheckpointId(start_chk_id);

        // Before adding the VB to the map increment the revision
        getRWUnderlying(vbid)->incrementRevision(vbid);

        // If active, update the VB from the bucket's collection state
        if (to == vbucket_state_active) {
            collectionsManager->update(*newvb);
        }

        if (vbMap.addBucket(newvb) == ENGINE_ERANGE) {
            return ENGINE_ERANGE;
        }
        // When the VBucket is constructed we initialize
        // persistenceSeqno(0) && persistenceCheckpointId(0)
        newvb->setBucketCreation(true);
        scheduleVBStatePersist(vbid);
    } else {
        return ENGINE_ERANGE;
    }
    return ENGINE_SUCCESS;
}

void KVBucket::scheduleVBStatePersist() {
    for (auto vbid : vbMap.getBuckets()) {
        scheduleVBStatePersist(vbid);
    }
}

void KVBucket::scheduleVBStatePersist(VBucket::id_type vbid) {
    VBucketPtr vb = getVBucket(vbid);

    if (!vb) {
        LOG(EXTENSION_LOG_WARNING,
            "EPStore::scheduleVBStatePersist: vb:%" PRIu16
            " does not not exist. Unable to schedule persistence.", vbid);
        return;
    }

    vb->checkpointManager.queueSetVBState(*vb);
}

ENGINE_ERROR_CODE KVBucket::deleteVBucket(uint16_t vbid, const void* c) {
    // Lock to prevent a race condition between a failed update and add
    // (and delete).
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    {
        std::unique_lock<std::mutex> vbSetLh(vbsetMutex);
        // Obtain the vb_mutex for the VB to ensure we interlock with other
        // threads that are manipulating the VB (particularly ones which may
        // try and change the disk revision e.g. deleteAll and compaction).
        std::unique_lock<std::mutex> vbMutLh(vb_mutexes[vbid]);

        vb->setState(vbucket_state_dead);
        engine.getDcpConnMap().vbucketStateChanged(vbid, vbucket_state_dead);

        // Drop the VB to begin the delete, the last holder of the VB will
        // unknowingly trigger the destructor which schedules a deletion task.
        vbMap.dropVBucketAndSetupDeferredDeletion(vbid, c);
    }

    if (c) {
        return ENGINE_EWOULDBLOCK;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE KVBucket::checkForDBExistence(DBFileId db_file_id) {
    std::string backend = engine.getConfiguration().getBackend();
    if (backend.compare("couchdb") == 0) {
        VBucketPtr vb = vbMap.getBucket(db_file_id);
        if (!vb) {
            return ENGINE_NOT_MY_VBUCKET;
        }
    } else if (backend.compare("forestdb") == 0) {
        if (db_file_id > (vbMap.getNumShards() - 1)) {
            //TODO: find a better error code
            return ENGINE_EINVAL;
        }
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "Unknown backend specified for db file id: %d", db_file_id);
        return ENGINE_FAILED;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE KVBucket::scheduleCompaction(uint16_t vbid, compaction_ctx c,
                                               const void *cookie) {
    ENGINE_ERROR_CODE errCode = checkForDBExistence(c.db_file_id);
    if (errCode != ENGINE_SUCCESS) {
        return errCode;
    }

    /* Obtain the vbucket so we can get the previous purge seqno */
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    /* Update the compaction ctx with the previous purge seqno */
    c.max_purged_seq[vbid] = vb->getPurgeSeqno();

    LockHolder lh(compactionLock);
    ExTask task = std::make_shared<CompactTask>(&engine, c, cookie);
    compactionTasks.push_back(std::make_pair(c.db_file_id, task));
    if (compactionTasks.size() > 1) {
        if ((stats.diskQueueSize > compactionWriteQueueCap &&
            compactionTasks.size() > (vbMap.getNumShards() / 2)) ||
            engine.getWorkLoadPolicy().getWorkLoadPattern() == READ_HEAVY) {
            // Snooze a new compaction task.
            // We will wake it up when one of the existing compaction tasks is done.
            task->snooze(60);
        }
    }

    ExecutorPool::get()->schedule(task);

    LOG(EXTENSION_LOG_DEBUG,
        "Scheduled compaction task %" PRIu64 " on db %d,"
        "purge_before_ts = %" PRIu64 ", purge_before_seq = %" PRIu64
        ", dropdeletes = %d",
        uint64_t(task->getId()),c.db_file_id, c.purge_before_ts,
        c.purge_before_seq, c.drop_deletes);

   return ENGINE_EWOULDBLOCK;
}

uint16_t KVBucket::getDBFileId(const protocol_binary_request_compact_db& req) {
    KVStore *store = vbMap.shards[0]->getROUnderlying();
    return store->getDBFileId(req);
}

void KVBucket::compactInternal(compaction_ctx *ctx) {
    BloomFilterCBPtr filter(new BloomFilterCallback(*this));
    ctx->bloomFilterCallback = filter;

    ExpiredItemsCBPtr expiry(new ExpiredItemsCallback(*this));
    ctx->expiryCallback = expiry;

    KVShard* shard = vbMap.getShardByVbId(ctx->db_file_id);
    KVStore* store = shard->getRWUnderlying();
    bool result = store->compactDB(ctx);

    Configuration& config = getEPEngine().getConfiguration();
    /* Iterate over all the vbucket ids set in max_purged_seq map. If there is an entry
     * in the map for a vbucket id, then it was involved in compaction and thus can
     * be used to update the associated bloom filters and purge sequence numbers
     */
    for (auto& it : ctx->max_purged_seq) {
        const uint16_t vbid = it.first;
        VBucketPtr vb = getVBucket(vbid);
        if (!vb) {
            continue;
        }

        if (config.isBfilterEnabled() && result) {
            vb->swapFilter();
        } else {
            vb->clearFilter();
        }
        vb->setPurgeSeqno(it.second);
    }
}

bool KVBucket::doCompact(compaction_ctx *ctx, const void *cookie) {
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
    StorageProperties storeProp = getStorageProperties();
    bool concWriteCompact = storeProp.hasConcWriteCompact();
    uint16_t vbid = ctx->db_file_id;

    /**
     * Check if the underlying storage engine allows writes concurrently
     * as the database file is being compacted. If not, a lock needs to
     * be held in order to serialize access to the database file between
     * the writer and compactor threads
     */
    if (concWriteCompact == false) {
        VBucketPtr vb = getVBucket(vbid);
        if (!vb) {
            err = ENGINE_NOT_MY_VBUCKET;
            engine.storeEngineSpecific(cookie, NULL);
            /**
             * Decrement session counter here, as memcached thread wouldn't
             * visit the engine interface in case of a NOT_MY_VB notification
             */
            engine.decrementSessionCtr();
        } else {
            std::unique_lock<std::mutex> lh(vb_mutexes[vbid], std::try_to_lock);
            if (!lh.owns_lock()) {
                return true;
            }

            compactInternal(ctx);
        }
    } else {
        compactInternal(ctx);
    }

    updateCompactionTasks(ctx->db_file_id);

    if (cookie) {
        engine.notifyIOComplete(cookie, err);
    }
    --stats.pendingCompactions;
    return false;
}

void KVBucket::updateCompactionTasks(DBFileId db_file_id) {
    LockHolder lh(compactionLock);
    bool erased = false, woke = false;
    std::list<CompTaskEntry>::iterator it = compactionTasks.begin();
    while (it != compactionTasks.end()) {
        if ((*it).first == db_file_id) {
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
}

bool KVBucket::resetVBucket(uint16_t vbid) {
    std::unique_lock<std::mutex> lh(vbsetMutex);
    // Obtain the vb_mutex for the VB to ensure we interlock with other
    // threads that are manipulating the VB (particularly ones which may
    // try and change the disk revision).
    std::unique_lock<std::mutex> vbMutLh(vb_mutexes[vbid]);
    return resetVBucket_UNLOCKED(vbid, lh, vbMutLh);
}

bool KVBucket::resetVBucket_UNLOCKED(uint16_t vbid,
                                     std::unique_lock<std::mutex>& vbset,
                                     std::unique_lock<std::mutex>& vbMutex) {
    bool rv(false);

    VBucketPtr vb = vbMap.getBucket(vbid);
    if (vb) {
        vbucket_state_t vbstate = vb->getState();

        vbMap.dropVBucketAndSetupDeferredDeletion(vbid, nullptr /*no cookie*/);

        checkpointCursorInfoList cursors =
                                        vb->checkpointManager.getAllCursors();
        // Delete and recreate the vbucket database file
        setVBucketState_UNLOCKED(vbid, vbstate,
                                 false/*transfer*/, true/*notifyDcp*/, vbset);

        // Copy the all cursors from the old vbucket into the new vbucket
        VBucketPtr newvb = vbMap.getBucket(vbid);
        newvb->checkpointManager.resetCursors(cursors);

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

void KVBucket::snapshotStats() {
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

void KVBucket::getAggregatedVBucketStats(const void* cookie,
                                         ADD_STAT add_stat) {
    // Create visitors for each of the four vBucket states, and collect
    // stats for each.
    auto active = makeVBCountVisitor(vbucket_state_active);
    auto replica = makeVBCountVisitor(vbucket_state_replica);
    auto pending = makeVBCountVisitor(vbucket_state_pending);
    auto dead = makeVBCountVisitor(vbucket_state_dead);

    VBucketCountAggregator aggregator;
    aggregator.addVisitor(active.get());
    aggregator.addVisitor(replica.get());
    aggregator.addVisitor(pending.get());
    aggregator.addVisitor(dead.get());
    visit(aggregator);

    updateCachedResidentRatio(active->getMemResidentPer(),
                              replica->getMemResidentPer());
    engine.getReplicationThrottle().adjustWriteQueueCap(active->getNumItems() +
                                                        replica->getNumItems() +
                                                        pending->getNumItems());

    // And finally actually return the stats using the ADD_STAT callback.
    appendAggregatedVBucketStats(
            *active, *replica, *pending, *dead, cookie, add_stat);
}

std::unique_ptr<VBucketCountVisitor> KVBucket::makeVBCountVisitor(
        vbucket_state_t state) {
    return std::make_unique<VBucketCountVisitor>(state);
}

void KVBucket::appendAggregatedVBucketStats(VBucketCountVisitor& active,
                                            VBucketCountVisitor& replica,
                                            VBucketCountVisitor& pending,
                                            VBucketCountVisitor& dead,
                                            const void* cookie,
                                            ADD_STAT add_stat) {
// Simplify the repetition of calling add_casted_stat with `add_stat` and
// cookie each time. (Note: if we had C++14 we could use a polymorphic
// lambda, but for now will have to stick to C++98 and macros :).
#define DO_STAT(k, v)                            \
    do {                                         \
        add_casted_stat(k, v, add_stat, cookie); \
    } while (0)

    // Top-level stats:
    DO_STAT("ep_flush_all", isDeleteAllScheduled());
    DO_STAT("curr_items", active.getNumItems());
    DO_STAT("curr_temp_items", active.getNumTempItems());
    DO_STAT("curr_items_tot",
            active.getNumItems() + replica.getNumItems() +
                    pending.getNumItems());

    // Active vBuckets:
    DO_STAT("vb_active_backfill_queue_size", active.getBackfillQueueSize());
    DO_STAT("vb_active_num", active.getVBucketNumber());
    DO_STAT("vb_active_curr_items", active.getNumItems());
    DO_STAT("vb_active_hp_vb_req_size", active.getNumHpVBReqs());
    DO_STAT("vb_active_num_non_resident", active.getNonResident());
    DO_STAT("vb_active_perc_mem_resident", active.getMemResidentPer());
    DO_STAT("vb_active_eject", active.getEjects());
    DO_STAT("vb_active_expired", active.getExpired());
    DO_STAT("vb_active_meta_data_memory", active.getMetaDataMemory());
    DO_STAT("vb_active_meta_data_disk", active.getMetaDataDisk());
    DO_STAT("vb_active_ht_memory", active.getHashtableMemory());
    DO_STAT("vb_active_itm_memory", active.getItemMemory());
    DO_STAT("vb_active_ops_create", active.getOpsCreate());
    DO_STAT("vb_active_ops_update", active.getOpsUpdate());
    DO_STAT("vb_active_ops_delete", active.getOpsDelete());
    DO_STAT("vb_active_ops_reject", active.getOpsReject());
    DO_STAT("vb_active_queue_size", active.getQueueSize());
    DO_STAT("vb_active_queue_memory", active.getQueueMemory());
    DO_STAT("vb_active_queue_age", active.getAge());
    DO_STAT("vb_active_queue_pending", active.getPendingWrites());
    DO_STAT("vb_active_queue_fill", active.getQueueFill());
    DO_STAT("vb_active_queue_drain", active.getQueueDrain());
    DO_STAT("vb_active_rollback_item_count", active.getRollbackItemCount());

    // Replica vBuckets:
    DO_STAT("vb_replica_backfill_queue_size", replica.getBackfillQueueSize());
    DO_STAT("vb_replica_num", replica.getVBucketNumber());
    DO_STAT("vb_replica_curr_items", replica.getNumItems());
    DO_STAT("vb_replica_hp_vb_req_size", replica.getNumHpVBReqs());
    DO_STAT("vb_replica_num_non_resident", replica.getNonResident());
    DO_STAT("vb_replica_perc_mem_resident", replica.getMemResidentPer());
    DO_STAT("vb_replica_eject", replica.getEjects());
    DO_STAT("vb_replica_expired", replica.getExpired());
    DO_STAT("vb_replica_meta_data_memory", replica.getMetaDataMemory());
    DO_STAT("vb_replica_meta_data_disk", replica.getMetaDataDisk());
    DO_STAT("vb_replica_ht_memory", replica.getHashtableMemory());
    DO_STAT("vb_replica_itm_memory", replica.getItemMemory());
    DO_STAT("vb_replica_ops_create", replica.getOpsCreate());
    DO_STAT("vb_replica_ops_update", replica.getOpsUpdate());
    DO_STAT("vb_replica_ops_delete", replica.getOpsDelete());
    DO_STAT("vb_replica_ops_reject", replica.getOpsReject());
    DO_STAT("vb_replica_queue_size", replica.getQueueSize());
    DO_STAT("vb_replica_queue_memory", replica.getQueueMemory());
    DO_STAT("vb_replica_queue_age", replica.getAge());
    DO_STAT("vb_replica_queue_pending", replica.getPendingWrites());
    DO_STAT("vb_replica_queue_fill", replica.getQueueFill());
    DO_STAT("vb_replica_queue_drain", replica.getQueueDrain());
    DO_STAT("vb_replica_rollback_item_count", replica.getRollbackItemCount());

    // Pending vBuckets:
    DO_STAT("vb_pending_backfill_queue_size", pending.getBackfillQueueSize());
    DO_STAT("vb_pending_num", pending.getVBucketNumber());
    DO_STAT("vb_pending_curr_items", pending.getNumItems());
    DO_STAT("vb_pending_hp_vb_req_size", pending.getNumHpVBReqs());
    DO_STAT("vb_pending_num_non_resident", pending.getNonResident());
    DO_STAT("vb_pending_perc_mem_resident", pending.getMemResidentPer());
    DO_STAT("vb_pending_eject", pending.getEjects());
    DO_STAT("vb_pending_expired", pending.getExpired());
    DO_STAT("vb_pending_meta_data_memory", pending.getMetaDataMemory());
    DO_STAT("vb_pending_meta_data_disk", pending.getMetaDataDisk());
    DO_STAT("vb_pending_ht_memory", pending.getHashtableMemory());
    DO_STAT("vb_pending_itm_memory", pending.getItemMemory());
    DO_STAT("vb_pending_ops_create", pending.getOpsCreate());
    DO_STAT("vb_pending_ops_update", pending.getOpsUpdate());
    DO_STAT("vb_pending_ops_delete", pending.getOpsDelete());
    DO_STAT("vb_pending_ops_reject", pending.getOpsReject());
    DO_STAT("vb_pending_queue_size", pending.getQueueSize());
    DO_STAT("vb_pending_queue_memory", pending.getQueueMemory());
    DO_STAT("vb_pending_queue_age", pending.getAge());
    DO_STAT("vb_pending_queue_pending", pending.getPendingWrites());
    DO_STAT("vb_pending_queue_fill", pending.getQueueFill());
    DO_STAT("vb_pending_queue_drain", pending.getQueueDrain());
    DO_STAT("vb_pending_rollback_item_count", pending.getRollbackItemCount());

    // Dead vBuckets:
    DO_STAT("vb_dead_num", dead.getVBucketNumber());

    // Totals:
    DO_STAT("ep_vb_total",
            active.getVBucketNumber() + replica.getVBucketNumber() +
                    pending.getVBucketNumber() + dead.getVBucketNumber());
    DO_STAT("ep_total_new_items",
            active.getOpsCreate() + replica.getOpsCreate() +
                    pending.getOpsCreate());
    DO_STAT("ep_total_del_items",
            active.getOpsDelete() + replica.getOpsDelete() +
                    pending.getOpsDelete());
    DO_STAT("ep_diskqueue_memory",
            active.getQueueMemory() + replica.getQueueMemory() +
                    pending.getQueueMemory());
    DO_STAT("ep_diskqueue_fill",
            active.getQueueFill() + replica.getQueueFill() +
                    pending.getQueueFill());
    DO_STAT("ep_diskqueue_drain",
            active.getQueueDrain() + replica.getQueueDrain() +
                    pending.getQueueDrain());
    DO_STAT("ep_diskqueue_pending",
            active.getPendingWrites() + replica.getPendingWrites() +
                    pending.getPendingWrites());
    DO_STAT("ep_meta_data_memory",
            active.getMetaDataMemory() + replica.getMetaDataMemory() +
                    pending.getMetaDataMemory());
    DO_STAT("ep_meta_data_disk",
            active.getMetaDataDisk() + replica.getMetaDataDisk() +
                    pending.getMetaDataDisk());
    DO_STAT("ep_total_cache_size",
            active.getCacheSize() + replica.getCacheSize() +
                    pending.getCacheSize());
    DO_STAT("rollback_item_count",
            active.getRollbackItemCount() + replica.getRollbackItemCount() +
                    pending.getRollbackItemCount());
    DO_STAT("ep_num_non_resident",
            active.getNonResident() + pending.getNonResident() +
                    replica.getNonResident());
    DO_STAT("ep_chk_persistence_remains",
            active.getChkPersistRemaining() + pending.getChkPersistRemaining() +
                    replica.getChkPersistRemaining());

    // Add stats for tracking HLC drift
    DO_STAT("ep_active_hlc_drift", active.getTotalAbsHLCDrift().total);
    DO_STAT("ep_active_hlc_drift_count", active.getTotalAbsHLCDrift().updates);
    DO_STAT("ep_replica_hlc_drift", replica.getTotalAbsHLCDrift().total);
    DO_STAT("ep_replica_hlc_drift_count",
            replica.getTotalAbsHLCDrift().updates);

    DO_STAT("ep_active_ahead_exceptions",
            active.getTotalHLCDriftExceptionCounters().ahead);
    DO_STAT("ep_active_behind_exceptions",
            active.getTotalHLCDriftExceptionCounters().behind);
    DO_STAT("ep_replica_ahead_exceptions",
            replica.getTotalHLCDriftExceptionCounters().ahead);
    DO_STAT("ep_replica_behind_exceptions",
            replica.getTotalHLCDriftExceptionCounters().behind);

    // A single total for ahead exceptions accross all active/replicas
    DO_STAT("ep_clock_cas_drift_threshold_exceeded",
            active.getTotalHLCDriftExceptionCounters().ahead +
                    replica.getTotalHLCDriftExceptionCounters().ahead);

    for (uint8_t ii = 0; ii < active.getNumDatatypes(); ++ii) {
        std::string name = "ep_active_datatype_";
        name += mcbp::datatype::to_string(ii);
        DO_STAT(name.c_str(), active.getDatatypeCount(ii));
    }

    for (uint8_t ii = 0; ii < replica.getNumDatatypes(); ++ii) {
        std::string name = "ep_replica_datatype_";
        name += mcbp::datatype::to_string(ii);
        DO_STAT(name.c_str(), replica.getDatatypeCount(ii));
    }

#undef DO_STAT
}

void KVBucket::completeBGFetch(const DocKey& key,
                               uint16_t vbucket,
                               const void* cookie,
                               ProcessClock::time_point init,
                               bool isMeta) {
    ProcessClock::time_point startTime(ProcessClock::now());
    // Go find the data
    GetValue gcb = getROUnderlying(vbucket)->get(key, vbucket, isMeta);

    {
      // Lock to prevent a race condition between a fetch for restore and delete
        LockHolder lh(vbsetMutex);

        VBucketPtr vb = getVBucket(vbucket);
        if (vb) {
            VBucketBGFetchItem item{&gcb, cookie, init, isMeta};
            ENGINE_ERROR_CODE status =
                    vb->completeBGFetchForSingleItem(key, item, startTime);
            engine.notifyIOComplete(item.cookie, status);
        } else {
            LOG(EXTENSION_LOG_INFO, "vb:%" PRIu16 " file was deleted in the "
                "middle of a bg fetch for key{%.*s}\n", vbucket, int(key.size()),
                key.data());
            engine.notifyIOComplete(cookie, ENGINE_NOT_MY_VBUCKET);
        }
    }

    --stats.numRemainingBgJobs;
}

void KVBucket::completeBGFetchMulti(uint16_t vbId,
                                    std::vector<bgfetched_item_t>& fetchedItems,
                                    ProcessClock::time_point startTime) {
    VBucketPtr vb = getVBucket(vbId);
    if (vb) {
        for (const auto& item : fetchedItems) {
            auto& key = item.first;
            auto* fetched_item = item.second;
            ENGINE_ERROR_CODE status = vb->completeBGFetchForSingleItem(
                    key, *fetched_item, startTime);
            engine.notifyIOComplete(fetched_item->cookie, status);
        }
        LOG(EXTENSION_LOG_DEBUG,
            "EP Store completes %" PRIu64 " of batched background fetch "
            "for vBucket = %d endTime = %" PRIu64,
            uint64_t(fetchedItems.size()), vbId, gethrtime()/1000000);
    } else {
        for (const auto& item : fetchedItems) {
            engine.notifyIOComplete(item.second->cookie,
                                    ENGINE_NOT_MY_VBUCKET);
        }
        LOG(EXTENSION_LOG_WARNING,
            "EP Store completes %d of batched background fetch for "
            "for vBucket = %d that is already deleted\n",
            (int)fetchedItems.size(), vbId);

    }
}

GetValue KVBucket::getInternal(const DocKey& key,
                               uint16_t vbucket,
                               const void *cookie,
                               vbucket_state_t allowedState,
                               get_options_t options) {

    vbucket_state_t disallowedState = (allowedState == vbucket_state_active) ?
        vbucket_state_replica : vbucket_state_active;
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    }

    const bool honorStates = (options & HONOR_STATES);

    ReaderLockHolder rlh(vb->getStateLock());
    if (honorStates) {
        vbucket_state_t vbState = vb->getState();
        if (vbState == vbucket_state_dead) {
            ++stats.numNotMyVBuckets;
            return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
        } else if (vbState == disallowedState) {
            ++stats.numNotMyVBuckets;
            return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
        } else if (vbState == vbucket_state_pending) {
            if (vb->addPendingOp(cookie)) {
                return GetValue(NULL, ENGINE_EWOULDBLOCK);
            }
        }
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return GetValue(NULL, ENGINE_UNKNOWN_COLLECTION);
        }

        return vb->getInternal(key,
                               cookie,
                               engine,
                               bgFetchDelay,
                               options,
                               diskDeleteAll,
                               VBucket::GetKeyOnly::No);
    }
}

GetValue KVBucket::getRandomKey() {
    VBucketMap::id_type max = vbMap.getSize();

    const long start = random() % max;
    long curr = start;
    std::unique_ptr<Item> itm;

    while (itm == NULL) {
        VBucketPtr vb = getVBucket(curr++);
        while (!vb || vb->getState() != vbucket_state_active) {
            if (curr == start) {
                return GetValue(NULL, ENGINE_KEY_ENOENT);
            }
            if (curr == max) {
                curr = 0;
            }

            vb = getVBucket(curr++);
        }

        if ((itm = vb->ht.getRandomKey(random()))) {
            GetValue rv(std::move(itm), ENGINE_SUCCESS);
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

ENGINE_ERROR_CODE KVBucket::getMetaData(const DocKey& key,
                                        uint16_t vbucket,
                                        const void* cookie,
                                        ItemMetaData& metadata,
                                        uint32_t& deleted,
                                        uint8_t& datatype)
{
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return ENGINE_UNKNOWN_COLLECTION;
        }

        return vb->getMetaData(
                key, cookie, engine, bgFetchDelay, metadata, deleted, datatype);
    }
}

ENGINE_ERROR_CODE KVBucket::setWithMeta(Item &itm,
                                        uint64_t cas,
                                        uint64_t *seqno,
                                        const void *cookie,
                                        bool force,
                                        bool allowExisting,
                                        GenerateBySeqno genBySeqno,
                                        GenerateCas genCas,
                                        ExtendedMetaData *emd,
                                        bool isReplication)
{
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
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

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return ENGINE_KEY_EEXISTS;
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(itm.getKey())) {
            return ENGINE_UNKNOWN_COLLECTION;
        }

        return vb->setWithMeta(itm,
                               cas,
                               seqno,
                               cookie,
                               engine,
                               bgFetchDelay,
                               force,
                               allowExisting,
                               genBySeqno,
                               genCas,
                               isReplication);
    }
}

GetValue KVBucket::getAndUpdateTtl(const DocKey& key, uint16_t vbucket,
                                   const void *cookie, time_t exptime)
{
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
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

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return GetValue(NULL, ENGINE_UNKNOWN_COLLECTION);
        }

        return vb->getAndUpdateTtl(key, cookie, engine, bgFetchDelay, exptime);
    }
}

GetValue KVBucket::getLocked(const DocKey& key, uint16_t vbucket,
                             rel_time_t currentTime, uint32_t lockTimeout,
                             const void *cookie) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb || vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return GetValue(NULL, ENGINE_UNKNOWN_COLLECTION);
        }

        return vb->getLocked(
                key, currentTime, lockTimeout, cookie, engine, bgFetchDelay);
    }
}

ENGINE_ERROR_CODE KVBucket::unlockKey(const DocKey& key,
                                      uint16_t vbucket,
                                      uint64_t cas,
                                      rel_time_t currentTime)
{

    VBucketPtr vb = getVBucket(vbucket);
    if (!vb || vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto collectionsRHandle = vb->lockCollections();
    if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
        return ENGINE_UNKNOWN_COLLECTION;
    }

    auto hbl = vb->ht.getLockedBucket(key);
    StoredValue* v = vb->fetchValidValue(hbl,
                                         key,
                                         WantsDeleted::Yes,
                                         TrackReference::Yes,
                                         QueueExpired::Yes);

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

ENGINE_ERROR_CODE KVBucket::getKeyStats(const DocKey& key,
                                        uint16_t vbucket,
                                        const void* cookie,
                                        struct key_stats& kstats,
                                        WantsDeleted wantsDeleted) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return ENGINE_UNKNOWN_COLLECTION;
        }

        return vb->getKeyStats(
                key, cookie, engine, bgFetchDelay, kstats, wantsDeleted);
}
}

std::string KVBucket::validateKey(const DocKey& key, uint16_t vbucket,
                                  Item &diskItem) {
    VBucketPtr vb = getVBucket(vbucket);
    auto hbl = vb->ht.getLockedBucket(key);
    StoredValue* v = vb->fetchValidValue(
            hbl, key, WantsDeleted::Yes, TrackReference::No, QueueExpired::Yes);

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

ENGINE_ERROR_CODE KVBucket::deleteItem(const DocKey& key,
                                       uint64_t& cas,
                                       uint16_t vbucket,
                                       const void* cookie,
                                       ItemMetaData* itemMeta,
                                       mutation_descr_t* mutInfo) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    } else if (vb->isTakeoverBackedUp()) {
        LOG(EXTENSION_LOG_DEBUG, "(vb %u) Returned TMPFAIL to a delete op"
                ", becuase takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }
    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return ENGINE_UNKNOWN_COLLECTION;
        }

        return vb->deleteItem(
                key, cas, cookie, engine, bgFetchDelay, itemMeta, mutInfo);
    }
}

ENGINE_ERROR_CODE KVBucket::deleteWithMeta(const DocKey& key,
                                           uint64_t& cas,
                                           uint64_t* seqno,
                                           uint16_t vbucket,
                                           const void* cookie,
                                           bool force,
                                           const ItemMetaData& itemMeta,
                                           bool backfill,
                                           GenerateBySeqno genBySeqno,
                                           GenerateCas generateCas,
                                           uint64_t bySeqno,
                                           ExtendedMetaData* emd,
                                           bool isReplication) {
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
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
                "op, because takeover is lagging", vb->getId());
        return ENGINE_TMPFAIL;
    }

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itemMeta.cas)) {
        return ENGINE_KEY_EEXISTS;
    }

    { // collections read scope
        auto collectionsRHandle = vb->lockCollections();
        if (!collectionsRHandle.doesKeyContainValidCollection(key)) {
            return ENGINE_UNKNOWN_COLLECTION;
        }

        return vb->deleteWithMeta(key,
                                  cas,
                                  seqno,
                                  cookie,
                                  engine,
                                  bgFetchDelay,
                                  force,
                                  itemMeta,
                                  backfill,
                                  genBySeqno,
                                  generateCas,
                                  bySeqno,
                                  isReplication);
    }
}

void KVBucket::reset() {
    auto buckets = vbMap.getBuckets();
    for (auto vbid : buckets) {
        VBucketPtr vb = getVBucket(vbid);
        if (vb) {
            LockHolder lh(vb_mutexes[vb->getId()]);
            vb->ht.clear();
            vb->checkpointManager.clear(vb->getState());
            vb->resetStats();
            vb->setPersistedSnapshot(0, 0);
        }
    }
}

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * KVBucket::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback : public Callback<mutation_result>,
                            public Callback<int> {
public:

    PersistenceCallback(const queued_item &qi, VBucketPtr &vb,
                        EPStats& s, uint64_t c)
        : queuedItem(qi), vbucket(vb), stats(s), cas(c) {
        if (!vb) {
            throw std::invalid_argument("PersistenceCallback(): vb is NULL");
        }
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            auto hbl = vbucket->ht.getLockedBucket(queuedItem->getKey());
            StoredValue* v = vbucket->fetchValidValue(hbl,
                                                      queuedItem->getKey(),
                                                      WantsDeleted::Yes,
                                                      TrackReference::No,
                                                      QueueExpired::Yes);
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
                        ++vbucket->opsUpdate;
                    }

                    v->setNewCacheItem(false);
                } else { // Update in value-only or full eviction mode.
                    ++vbucket->opsUpdate;
                }
            }

            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            --stats.diskQueueSize;
            stats.totalPersisted++;
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                auto hbl = vbucket->ht.getLockedBucket(queuedItem->getKey());
                StoredValue* v = vbucket->fetchValidValue(hbl,
                                                          queuedItem->getKey(),
                                                          WantsDeleted::Yes,
                                                          TrackReference::No,
                                                          QueueExpired::Yes);
                if (v) {
                    LOG(EXTENSION_LOG_WARNING,
                        "PersistenceCallback::callback: Persisting on "
                        "vb:%" PRIu16 ", seqno:%" PRIu64 " returned 0 updates",
                        queuedItem->getVBucketId(), v->getBySeqno());
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "PersistenceCallback::callback: Error persisting, a key"
                        "is missing from vb:%" PRIu16,
                        queuedItem->getVBucketId());
                }

                vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
                --stats.diskQueueSize;
            } else {
                LOG(EXTENSION_LOG_WARNING,
                    "PersistenceCallback::callback: Fatal error in persisting "
                    "SET on vb:%" PRIu16, queuedItem->getVBucketId());
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
            vbucket->deletedOnDiskCbk(*queuedItem, (value > 0));
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "PersistenceCallback::callback: Fatal error in persisting "
                "DELETE on vb:%" PRIu16, queuedItem->getVBucketId());
            redirty();
        }
    }

    VBucketPtr& getVBucket() {
        return vbucket;
    }

private:

    void redirty() {
        if (vbucket->isDeletionDeferred()) {
            // updating the member stats for the vbucket is not really necessary
            // as the vbucket is about to be deleted
            vbucket->doStatsForFlushing(*queuedItem, queuedItem->size());
            // the following is a global stat and so is worth updating
            --stats.diskQueueSize;
            return;
        }
        ++stats.flushFailed;
        vbucket->markDirty(queuedItem->getKey());
        vbucket->rejectQueue.push(queuedItem);
        ++vbucket->opsReject;
    }

    const queued_item queuedItem;
    VBucketPtr vbucket;
    EPStats& stats;
    uint64_t cas;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

bool KVBucket::scheduleDeleteAllTask(const void* cookie) {
    bool inverse = false;
    if (diskDeleteAll.compare_exchange_strong(inverse, true)) {
        deleteAllTaskCtx.cookie = cookie;
        deleteAllTaskCtx.delay.compare_exchange_strong(inverse, true);
        ExTask task = std::make_shared<DeleteAllTask>(&engine);
        ExecutorPool::get()->schedule(task);
        return true;
    } else {
        return false;
    }
}

void KVBucket::setDeleteAllComplete() {
    // Notify memcached about delete all task completion, and
    // set diskFlushall flag to false
    if (deleteAllTaskCtx.cookie) {
        engine.notifyIOComplete(deleteAllTaskCtx.cookie, ENGINE_SUCCESS);
    }
    bool inverse = false;
    deleteAllTaskCtx.delay.compare_exchange_strong(inverse, true);
    inverse = true;
    diskDeleteAll.compare_exchange_strong(inverse, false);
}

void KVBucket::flushOneDeleteAll() {
    for (VBucketMap::id_type i = 0; i < vbMap.getSize(); ++i) {
        VBucketPtr vb = getVBucket(i);
        // Reset the vBucket if it's non-null and not already in the middle of
        // being created / destroyed.
        if (vb && !(vb->isBucketCreation() || vb->isDeletionDeferred())) {
            LockHolder lh(vb_mutexes[vb->getId()]);
            getRWUnderlying(vb->getId())->reset(i);
        }
    }

    --stats.diskQueueSize;
    setDeleteAllComplete();
}

int KVBucket::flushVBucket(uint16_t vbid) {
    KVShard *shard = vbMap.getShardByVbId(vbid);
    if (diskDeleteAll && !deleteAllTaskCtx.delay) {
        if (shard->getId() == EP_PRIMARY_SHARD) {
            flushOneDeleteAll();
        } else {
            // disk flush is pending just return
            return 0;
        }
    }

    int items_flushed = 0;
    const hrtime_t flush_start = gethrtime();

    VBucketPtr vb = vbMap.getBucket(vbid);
    if (vb) {
        std::unique_lock<std::mutex> lh(vb_mutexes[vbid], std::try_to_lock);
        if (!lh.owns_lock()) { // Try another bucket if this one is locked
            return RETRY_FLUSH_VBUCKET; // to avoid blocking flusher
        }

        std::vector<queued_item> items;
        KVStore *rwUnderlying = getRWUnderlying(vbid);

        while (!vb->rejectQueue.empty()) {
            items.push_back(vb->rejectQueue.front());
            vb->rejectQueue.pop();
        }

        // Append any 'backfill' items (mutations added by a TAP stream).
        vb->getBackfillItems(items);

        // Append all items outstanding for the persistence cursor.
        snapshot_range_t range;
        hrtime_t _begin_ = gethrtime();
        range = vb->checkpointManager.getAllItemsForCursor(
                CheckpointManager::pCursorName, items);
        stats.persistenceCursorGetItemsHisto.add((gethrtime() - _begin_) / 1000);

        if (!items.empty()) {
            while (!rwUnderlying->begin()) {
                ++stats.beginFailed;
                LOG(EXTENSION_LOG_WARNING, "Failed to start a transaction!!! "
                    "Retry in 1 sec ...");
                sleep(1);
            }
            rwUnderlying->optimizeWrites(items);

            Item *prev = NULL;
            auto vbstate = vb->getVBucketState();
            uint64_t maxSeqno = 0;
            range.start = std::max(range.start, vbstate.lastSnapStart);

            bool mustCheckpointVBState = false;
            std::list<PersistenceCallback*>& pcbs = rwUnderlying->getPersistenceCbList();

            SystemEventFlush sef;

            for (const auto& item : items) {

                if (!item->shouldPersist()) {
                    continue;
                }

                // Pass the Item through the SystemEventFlush which may filter
                // the item away (return Skip).
                if (sef.process(item) == ProcessStatus::Skip) {
                    // The item has no further flushing actions i.e. we've
                    // absorbed it in the process function.
                    // Update stats and carry-on
                    --stats.diskQueueSize;
                    vb->doStatsForFlushing(*item, item->size());
                    continue;
                }

                if (item->getOperation() == queue_op::set_vbucket_state) {
                    // No actual item explicitly persisted to (this op exists
                    // to ensure a commit occurs with the current vbstate);
                    // flag that we must trigger a snapshot even if there are
                    // no 'real' items in the checkpoint.
                    mustCheckpointVBState = true;

                    // Update queuing stats how this item has logically been
                    // processed.
                    --stats.diskQueueSize;
                    vb->doStatsForFlushing(*item, item->size());

                } else if (!prev || prev->getKey() != item->getKey()) {
                    prev = item.get();
                    ++items_flushed;
                    PersistenceCallback *cb = flushOneDelOrSet(item, vb);
                    if (cb) {
                        pcbs.push_back(cb);
                    }

                    maxSeqno = std::max(maxSeqno, (uint64_t)item->getBySeqno());
                    vbstate.maxCas = std::max(vbstate.maxCas, item->getCas());
                    if (item->isDeleted()) {
                        vbstate.maxDeletedSeqno =
                                std::max(vbstate.maxDeletedSeqno,
                                         item->getRevSeqno());
                    }
                    ++stats.flusher_todo;

                } else {
                    // Item is the same key as the previous[1] one - don't need
                    // to flush to disk.
                    // [1] Previous here really means 'next' - optimizeWrites()
                    //     above has actually re-ordered items such that items
                    //     with the same key are ordered from high->low seqno.
                    //     This means we only write the highest (i.e. newest)
                    //     item for a given key, and discard any duplicate,
                    //     older items.
                    --stats.diskQueueSize;
                    vb->doStatsForFlushing(*item, item->size());
                }
            }


            {
                ReaderLockHolder rlh(vb->getStateLock());
                if (vb->getState() == vbucket_state_active) {
                    if (maxSeqno) {
                        range.start = maxSeqno;
                        range.end = maxSeqno;
                    }
                }

                // Update VBstate based on the changes we have just made,
                // then tell the rwUnderlying the 'new' state
                // (which will persisted as part of the commit() below).
                vbstate.lastSnapStart = range.start;
                vbstate.lastSnapEnd = range.end;

                // Track the lowest seqno written in spock and record it as
                // the HLC epoch, a seqno which we can be sure the value has a
                // HLC CAS.
                vbstate.hlcCasEpochSeqno = vb->getHLCEpochSeqno();
                if (vbstate.hlcCasEpochSeqno == HlcCasSeqnoUninitialised) {
                    vbstate.hlcCasEpochSeqno = range.start;
                    vb->setHLCEpochSeqno(range.start);
                }

                // Do we need to trigger a persist of the state?
                // If there are no "real" items to flush, and we encountered
                // a set_vbucket_state meta-item.
                auto options = VBStatePersist::VBSTATE_CACHE_UPDATE_ONLY;
                if ((items_flushed == 0) && mustCheckpointVBState) {
                    options = VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT;
                }

                if (rwUnderlying->snapshotVBucket(vb->getId(), vbstate,
                                                  options) != true) {
                    return RETRY_FLUSH_VBUCKET;
                }

                if (vb->setBucketCreation(false)) {
                    LOG(EXTENSION_LOG_INFO, "VBucket %" PRIu16 " created", vbid);
                }
            }

            /* Perform an explicit commit to disk if the commit
             * interval reaches zero and if there is a non-zero number
             * of items to flush.
             * Or if there is a manifest item
             */
            if (items_flushed > 0 || sef.getCollectionsManifestItem()) {
                commit(*rwUnderlying, sef.getCollectionsManifestItem());

                // Now the commit is complete, vBucket file must exist.
                if (vb->setBucketCreation(false)) {
                    LOG(EXTENSION_LOG_INFO, "VBucket %" PRIu16 " created", vbid);
                }
            }

            hrtime_t flush_end = gethrtime();
            uint64_t trans_time = (flush_end - flush_start) / 1000000;

            lastTransTimePerItem.store((items_flushed == 0) ? 0 :
                                       static_cast<double>(trans_time) /
                                       static_cast<double>(items_flushed));
            stats.cumulativeFlushTime.fetch_add(trans_time);
            stats.flusher_todo.store(0);
            stats.totalPersistVBState++;

            if (vb->rejectQueue.empty()) {
                vb->setPersistedSnapshot(range.start, range.end);
                uint64_t highSeqno = rwUnderlying->getLastPersistedSeqno(vbid);
                if (highSeqno > 0 &&
                    highSeqno != vb->getPersistenceSeqno()) {
                    vb->setPersistenceSeqno(highSeqno);
                }
            }
        }

        rwUnderlying->pendingTasks();

        if (vb->checkpointManager.getNumCheckpoints() > 1) {
            wakeUpCheckpointRemover();
        }

        if (vb->rejectQueue.empty()) {
            vb->checkpointManager.itemsPersisted();
            uint64_t seqno = vb->getPersistenceSeqno();
            uint64_t chkid = vb->checkpointManager.getPersistenceCursorPreChkId();
            vb->notifyHighPriorityRequests(
                    engine, seqno, HighPriorityVBNotify::Seqno);
            vb->notifyHighPriorityRequests(
                    engine, chkid, HighPriorityVBNotify::ChkPersistence);
            if (chkid > 0 && chkid != vb->getPersistenceCheckpointId()) {
                vb->setPersistenceCheckpointId(chkid);
            }
        } else {
            return RETRY_FLUSH_VBUCKET;
        }
    }

    return items_flushed;
}

void KVBucket::commit(KVStore& kvstore, const Item* collectionsManifest) {
    std::list<PersistenceCallback*>& pcbs = kvstore.getPersistenceCbList();
    BlockTimer timer(&stats.diskCommitHisto, "disk_commit", stats.timingLog);
    hrtime_t commit_start = gethrtime();

    while (!kvstore.commit(collectionsManifest)) {
        ++stats.commitFailed;
        LOG(EXTENSION_LOG_WARNING,
            "KVBucket::commit: kvstore.commit failed!!! Retry in 1 sec...");
        sleep(1);
    }

    //Update the total items in the case of full eviction
    if (getItemEvictionPolicy() == FULL_EVICTION) {
        std::unordered_set<uint16_t> vbSet;
        for (auto pcbIter : pcbs) {
            PersistenceCallback *pcb = pcbIter;
            VBucketPtr& vb = pcb->getVBucket();
            uint16_t vbid = vb->getId();
            auto found = vbSet.find(vbid);
            if (found == vbSet.end()) {
                vbSet.insert(vbid);
                vb->ht.setNumTotalItems(
                        getRWUnderlying(vbid)->getItemCount(vbid));
            }
        }
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

PersistenceCallback* KVBucket::flushOneDelOrSet(const queued_item &qi,
                                                VBucketPtr &vb) {

    if (!vb) {
        --stats.diskQueueSize;
        return NULL;
    }

    int64_t bySeqno = qi->getBySeqno();
    rel_time_t queued(qi->getQueuedTime());

    int dirtyAge = ep_current_time() - queued;
    stats.dirtyAgeHisto.add(dirtyAge * 1000000);
    stats.dirtyAge.store(dirtyAge);
    stats.dirtyAgeHighWat.store(std::max(stats.dirtyAge.load(),
                                         stats.dirtyAgeHighWat.load()));

    KVStore *rwUnderlying = getRWUnderlying(qi->getVBucketId());
    if (SystemEventFlush::isUpsert(*qi)) {
        // TODO: Need to separate disk_insert from disk_update because
        // bySeqno doesn't give us that information.
        BlockTimer timer(bySeqno == -1 ?
                         &stats.diskInsertHisto : &stats.diskUpdateHisto,
                         bySeqno == -1 ? "disk_insert" : "disk_update",
                         stats.timingLog);
        PersistenceCallback *cb =
            new PersistenceCallback(qi, vb, stats, qi->getCas());
        rwUnderlying->set(*qi, *cb);
        return cb;
    } else {
        BlockTimer timer(&stats.diskDelHisto, "disk_delete",
                         stats.timingLog);
        PersistenceCallback *cb =
            new PersistenceCallback(qi, vb, stats, 0);
        rwUnderlying->del(*qi, *cb);
        return cb;
    }
}

std::vector<vbucket_state *> KVBucket::loadVBucketState()
{
    return getOneROUnderlying()->listPersistedVbuckets();
}

void KVBucket::warmupCompleted() {
    // Snapshot VBucket state after warmup to ensure Failover table is
    // persisted.
    scheduleVBStatePersist();

    if (engine.getConfiguration().getAlogPath().length() > 0) {

        if (engine.getConfiguration().isAccessScannerEnabled()) {
            {
                LockHolder lh(accessScanner.mutex);
                accessScanner.enabled = true;
            }
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
    ExTask task = std::make_shared<StatSnap>(&engine, 0, false);
    statsSnapshotTaskId = iom->schedule(task);
}

bool KVBucket::maybeEnableTraffic()
{
    // @todo rename.. skal vaere isTrafficDisabled elns
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());

    if (memoryUsed  >= stats.mem_low_wat) {
        LOG(EXTENSION_LOG_NOTICE,
            "Total memory use reached to the low water mark, stop warmup"
            ": memoryUsed (%f) >= low water mark (%" PRIu64 ")",
            memoryUsed, uint64_t(stats.mem_low_wat.load()));
        return true;
    } else if (memoryUsed > (maxSize * stats.warmupMemUsedCap)) {
        LOG(EXTENSION_LOG_NOTICE,
                "Enough MB of data loaded to enable traffic"
                ": memoryUsed (%f) > (maxSize(%f) * warmupMemUsedCap(%f))",
                 memoryUsed, maxSize, stats.warmupMemUsedCap.load());
        return true;
    } else if (eviction_policy == VALUE_ONLY &&
               stats.warmedUpValues >=
                               (stats.warmedUpKeys * stats.warmupNumReadCap)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        LOG(EXTENSION_LOG_NOTICE,
            "Enough number of items loaded to enable traffic (value eviction)"
            ": warmedUpValues(%" PRIu64 ") >= (warmedUpKeys(%" PRIu64 ") * "
            "warmupNumReadCap(%f))",  uint64_t(stats.warmedUpValues.load()),
            uint64_t(stats.warmedUpKeys.load()), stats.warmupNumReadCap.load());
        return true;
    } else if (eviction_policy == FULL_EVICTION &&
               stats.warmedUpValues >=
                            (warmupTask->getEstimatedItemCount() *
                             stats.warmupNumReadCap)) {
        // In case of FULL EVICTION, warmed up keys always matches the number
        // of warmed up values, therefore for honoring the min_item threshold
        // in this scenario, we can consider warmup's estimated item count.
        LOG(EXTENSION_LOG_NOTICE,
            "Enough number of items loaded to enable traffic (full eviction)"
            ": warmedUpValues(%" PRIu64 ") >= (warmup est items(%" PRIu64 ") * "
            "warmupNumReadCap(%f))",  uint64_t(stats.warmedUpValues.load()),
            uint64_t(warmupTask->getEstimatedItemCount()),
            stats.warmupNumReadCap.load());
        return true;
    }
    return false;
}

bool KVBucket::isWarmingUp() {
    return warmupTask && !warmupTask->isComplete();
}

bool KVBucket::isWarmupOOMFailure() {
    return warmupTask && warmupTask->hasOOMFailure();
}

void KVBucket::stopWarmup(void)
{
    // forcefully stop current warmup task
    if (isWarmingUp()) {
        LOG(EXTENSION_LOG_NOTICE, "Stopping warmup while engine is loading "
            "data from underlying storage, shutdown = %s\n",
            stats.isShutdown ? "yes" : "no");
        warmupTask->stop();
    }
}

bool KVBucket::isMemoryUsageTooHigh() {
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());
    return memoryUsed > (maxSize * backfillMemoryThreshold);
}

void KVBucket::setBackfillMemoryThreshold(double threshold) {
    backfillMemoryThreshold = threshold;
}

void KVBucket::setExpiryPagerSleeptime(size_t val) {
    LockHolder lh(expiryPager.mutex);

    ExecutorPool::get()->cancel(expiryPager.task);

    expiryPager.sleeptime = val;
    if (expiryPager.enabled) {
        ExTask expTask = std::make_shared<ExpiredItemPager>(
                &engine, stats, expiryPager.sleeptime);
        expiryPager.task = ExecutorPool::get()->schedule(expTask);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry pager disabled, "
                                 "enabling it will make exp_pager_stime (%lu)"
                                 "to go into effect!", val);
    }
}

void KVBucket::setExpiryPagerTasktime(ssize_t val) {
    LockHolder lh(expiryPager.mutex);
    if (expiryPager.enabled) {
        ExecutorPool::get()->cancel(expiryPager.task);
        ExTask expTask = std::make_shared<ExpiredItemPager>(
                &engine, stats, expiryPager.sleeptime, val);
        expiryPager.task = ExecutorPool::get()->schedule(expTask);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry pager disabled, "
                                 "enabling it will make exp_pager_stime (%lu)"
                                 "to go into effect!", val);
    }
}

void KVBucket::enableExpiryPager() {
    LockHolder lh(expiryPager.mutex);
    if (!expiryPager.enabled) {
        expiryPager.enabled = true;

        ExecutorPool::get()->cancel(expiryPager.task);
        ExTask expTask = std::make_shared<ExpiredItemPager>(
                &engine, stats, expiryPager.sleeptime);
        expiryPager.task = ExecutorPool::get()->schedule(expTask);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry Pager already enabled!");
    }
}

void KVBucket::disableExpiryPager() {
    LockHolder lh(expiryPager.mutex);
    if (expiryPager.enabled) {
        ExecutorPool::get()->cancel(expiryPager.task);
        expiryPager.enabled = false;
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Expiry Pager already disabled!");
    }
}

void KVBucket::enableItemPager() {
    ExecutorPool::get()->cancel(itemPagerTask->getId());
    ExecutorPool::get()->schedule(itemPagerTask);
}

void KVBucket::disableItemPager() {
    ExecutorPool::get()->cancel(itemPagerTask->getId());
}

void KVBucket::enableAccessScannerTask() {
    LockHolder lh(accessScanner.mutex);
    if (!accessScanner.enabled) {
        accessScanner.enabled = true;

        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
        }

        size_t alogSleepTime = engine.getConfiguration().getAlogSleepTime();
        accessScanner.sleeptime = alogSleepTime * 60;
        if (accessScanner.sleeptime != 0) {
            ExTask task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    accessScanner.sleeptime,
                                                    true);
            accessScanner.task = ExecutorPool::get()->schedule(task);
        } else {
            LOG(EXTENSION_LOG_NOTICE, "Did not enable access scanner task, "
                                      "as alog_sleep_time is set to zero!");
        }
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Access scanner already enabled!");
    }
}

void KVBucket::disableAccessScannerTask() {
    LockHolder lh(accessScanner.mutex);
    if (accessScanner.enabled) {
        ExecutorPool::get()->cancel(accessScanner.task);
        accessScanner.sleeptime = 0;
        accessScanner.enabled = false;
    } else {
        LOG(EXTENSION_LOG_DEBUG, "Access scanner already disabled!");
    }
}

void KVBucket::setAccessScannerSleeptime(size_t val, bool useStartTime) {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.enabled) {
        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
        }

        // store sleeptime in seconds
        accessScanner.sleeptime = val * 60;
        if (accessScanner.sleeptime != 0) {
            ExTask task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    accessScanner.sleeptime,
                                                    useStartTime);
            accessScanner.task = ExecutorPool::get()->schedule(task);
        }
    }
}

void KVBucket::resetAccessScannerStartTime() {
    LockHolder lh(accessScanner.mutex);

    if (accessScanner.enabled) {
        if (accessScanner.sleeptime != 0) {
            ExecutorPool::get()->cancel(accessScanner.task);
            // re-schedule task according to the new task start hour
            ExTask task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    accessScanner.sleeptime,
                                                    true);
            accessScanner.task = ExecutorPool::get()->schedule(task);
        }
    }
}

void KVBucket::setAllBloomFilters(bool to) {
    for (VBucketMap::id_type vbid = 0; vbid < vbMap.getSize(); vbid++) {
        VBucketPtr vb = vbMap.getBucket(vbid);
        if (vb) {
            if (to) {
                vb->setFilterStatus(BFILTER_ENABLED);
            } else {
                vb->setFilterStatus(BFILTER_DISABLED);
            }
        }
    }
}

void KVBucket::visit(VBucketVisitor &visitor)
{
    for (VBucketMap::id_type vbid = 0; vbid < vbMap.getSize(); ++vbid) {
        VBucketPtr vb = vbMap.getBucket(vbid);
        if (vb) {
            visitor.visitBucket(vb);
        }
    }
    visitor.complete();
}

size_t KVBucket::visit(std::unique_ptr<VBucketVisitor> visitor,
                       const char* lbl,
                       TaskId id,
                       double sleepTime) {
    return ExecutorPool::get()->schedule(std::make_shared<VBCBAdaptor>(
            this, id, std::move(visitor), lbl, sleepTime));
}

KVBucket::Position KVBucket::pauseResumeVisit(PauseResumeVBVisitor& visitor,
                                              Position& start_pos) {
    uint16_t vbid = start_pos.vbucket_id;
    for (; vbid < vbMap.getSize(); ++vbid) {
        VBucketPtr vb = vbMap.getBucket(vbid);
        if (vb) {
            bool paused = !visitor.visit(*vb);
            if (paused) {
                break;
            }
        }
    }

    return KVBucket::Position(vbid);
}

KVBucket::Position KVBucket::startPosition() const
{
    return KVBucket::Position(0);
}

KVBucket::Position KVBucket::endPosition() const
{
    return KVBucket::Position(vbMap.getSize());
}

VBCBAdaptor::VBCBAdaptor(KVBucket* s,
                         TaskId id,
                         std::unique_ptr<VBucketVisitor> v,
                         const char* l,
                         double sleep,
                         bool shutdown)
    : GlobalTask(&s->getEPEngine(), id, 0, shutdown),
      store(s),
      visitor(std::move(v)),
      label(l),
      sleepTime(sleep),
      currentvb(0) {
    updateDescription();
    const VBucketFilter& vbFilter = visitor->getVBucketFilter();
    for (auto vbid : store->getVBuckets().getBuckets()) {
        if (vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBCBAdaptor::run(void) {
    if (!vbList.empty()) {
        TRACE_EVENT("ep-engine/task", "VBCBAdaptor", vbList.front());
        currentvb.store(vbList.front());
        updateDescription();
        VBucketPtr vb = store->getVBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                snooze(sleepTime);
                return true;
            }
            visitor->visitBucket(vb);
        }
        vbList.pop();
    }

    bool isdone = vbList.empty();
    if (isdone) {
        visitor->complete();
    }
    return !isdone;
}

void VBCBAdaptor::updateDescription() {
    std::unique_lock<std::mutex> lock(description.mutex);
    description.text =
            std::string(label) + " on vb " + std::to_string(currentvb.load());
}

void KVBucket::resetUnderlyingStats(void)
{
    for (size_t i = 0; i < vbMap.shards.size(); i++) {
        KVShard *shard = vbMap.shards[i].get();
        shard->getRWUnderlying()->resetStats();
        shard->getROUnderlying()->resetStats();
    }

    for (size_t i = 0; i < GlobalTask::allTaskIds.size(); i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }
}

void KVBucket::addKVStoreStats(ADD_STAT add_stat, const void* cookie) {
    for (size_t i = 0; i < vbMap.shards.size(); i++) {
        /* Add the different KVStore instances into a set and then
         * retrieve the stats from each instance separately. This
         * is because CouchKVStore has separate read only and read
         * write instance whereas ForestKVStore has only instance
         * for both read write and read-only.
         */
        std::set<KVStore *> underlyingSet;
        underlyingSet.insert(vbMap.shards[i]->getRWUnderlying());
        underlyingSet.insert(vbMap.shards[i]->getROUnderlying());

        for (auto* store : underlyingSet) {
            store->addStats(add_stat, cookie);
        }
    }
}

void KVBucket::addKVStoreTimingStats(ADD_STAT add_stat, const void* cookie) {
    for (size_t i = 0; i < vbMap.shards.size(); i++) {
        std::set<KVStore*> underlyingSet;
        underlyingSet.insert(vbMap.shards[i]->getRWUnderlying());
        underlyingSet.insert(vbMap.shards[i]->getROUnderlying());

        for (auto* store : underlyingSet) {
            store->addTimingStats(add_stat, cookie);
        }
    }
}

bool KVBucket::getKVStoreStat(const char* name, size_t& value, KVSOption option)
{
    value = 0;
    bool success = true;
    for (const auto& shard : vbMap.shards) {
        size_t per_shard_value;

        if (option == KVSOption::RO || option == KVSOption::BOTH) {
            success &= shard->getROUnderlying()->getStat(name, per_shard_value);
            value += per_shard_value;
        }

        if (option == KVSOption::RW || option == KVSOption::BOTH) {
            success &= shard->getRWUnderlying()->getStat(name, per_shard_value);
            value += per_shard_value;
        }
    }
    return success;
}

KVStore *KVBucket::getOneROUnderlying(void) {
    return vbMap.shards[EP_PRIMARY_SHARD]->getROUnderlying();
}

KVStore *KVBucket::getOneRWUnderlying(void) {
    return vbMap.shards[EP_PRIMARY_SHARD]->getRWUnderlying();
}

ENGINE_ERROR_CODE KVBucket::rollback(uint16_t vbid, uint64_t rollbackSeqno) {
    std::unique_lock<std::mutex> vbset(vbsetMutex);

    std::unique_lock<std::mutex> vbMutexLh(vb_mutexes[vbid], std::try_to_lock);

    if (!vbMutexLh.owns_lock()) {
        return ENGINE_TMPFAIL; // Reschedule a vbucket rollback task.
    }

    VBucketPtr vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_replica) {
        uint64_t prevHighSeqno = static_cast<uint64_t>
                                        (vb->checkpointManager.getHighSeqno());
        if (rollbackSeqno != 0) {
            RollbackResult result = doRollback(vbid, rollbackSeqno);

            if (result.success /* not suceess hence reset vbucket to
                                  avoid data loss */
                &&
                (result.highSeqno > 0) /* if 0, reset vbucket for a clean start
                                          instead of deleting everything in it
                                        */) {
                rollbackUnpersistedItems(*vb, result.highSeqno);
                vb->postProcessRollback(result, prevHighSeqno);
                return ENGINE_SUCCESS;
            }
        }

        if (resetVBucket_UNLOCKED(vbid, vbset, vbMutexLh)) {
            VBucketPtr newVb = vbMap.getBucket(vbid);
            newVb->incrRollbackItemCount(prevHighSeqno);
            return ENGINE_SUCCESS;
        }
        return ENGINE_NOT_MY_VBUCKET;
    } else {
        return ENGINE_EINVAL;
    }
}

void KVBucket::runDefragmenterTask() {
    defragmenterTask->run();
}

bool KVBucket::runAccessScannerTask() {
    return ExecutorPool::get()->wake(accessScanner.task);
}

void KVBucket::runVbStatePersistTask(int vbid) {
    scheduleVBStatePersist(vbid);
}

void KVBucket::setCursorDroppingLowerUpperThresholds(size_t maxSize) {
    Configuration &config = engine.getConfiguration();
    stats.cursorDroppingLThreshold.store(static_cast<size_t>(maxSize *
                    ((double)(config.getCursorDroppingLowerMark()) / 100)));
    stats.cursorDroppingUThreshold.store(static_cast<size_t>(maxSize *
                    ((double)(config.getCursorDroppingUpperMark()) / 100)));
}

size_t KVBucket::getActiveResidentRatio() const {
    return cachedResidentRatio.activeRatio.load();
}

size_t KVBucket::getReplicaResidentRatio() const {
    return cachedResidentRatio.replicaRatio.load();
}

ENGINE_ERROR_CODE KVBucket::forceMaxCas(uint16_t vbucket, uint64_t cas) {
    VBucketPtr vb = vbMap.getBucket(vbucket);
    if (vb) {
        vb->forceMaxCas(cas);
        return ENGINE_SUCCESS;
    }
    return ENGINE_NOT_MY_VBUCKET;
}

std::ostream& operator<<(std::ostream& os, const KVBucket::Position& pos) {
    os << "vbucket:" << pos.vbucket_id;
    return os;
}

void KVBucket::notifyFlusher(const uint16_t vbid) {
    KVShard* shard = vbMap.getShardByVbId(vbid);
    if (shard) {
        shard->getFlusher()->notifyFlushEvent();
    } else {
        throw std::logic_error(
                "KVBucket::notifyFlusher() : shard null for "
                "vbucket " +
                std::to_string(vbid));
    }
}

void KVBucket::notifyReplication(const uint16_t vbid, const int64_t bySeqno) {
    engine.getTapConnMap().notifyVBConnections(vbid);
    engine.getDcpConnMap().notifyVBConnections(vbid, bySeqno);
}

void KVBucket::initializeExpiryPager(Configuration& config) {
    {
        LockHolder elh(expiryPager.mutex);
        expiryPager.enabled = config.isExpPagerEnabled();
    }

    setExpiryPagerSleeptime(config.getExpPagerStime());

    config.addValueChangedListener("exp_pager_stime",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("exp_pager_enabled",
                                   new EPStoreValueChangeListener(*this));
    config.addValueChangedListener("exp_pager_initial_run_time",
                                   new EPStoreValueChangeListener(*this));
}

cb::engine_error KVBucket::setCollections(cb::const_char_buffer json) {
    // cJSON can't accept a size so we must create a string
    std::string manifest(json.data(), json.size());

    // Inhibit VB state changes whilst updating the vbuckets
    LockHolder lh(vbsetMutex);

    return collectionsManager->update(*this, manifest);
}

const Collections::Manager& KVBucket::getCollectionsManager() const {
    return *collectionsManager.get();
}
