/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "kv_bucket.h"
#include "access_scanner.h"
#include "bucket_logger.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "conflict_resolution.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "defragmenter.h"
#include "durability/durability_completion_task.h"
#include "durability_timeout_task.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "ext_meta_parser.h"
#include "failover-table.h"
#include "flusher.h"
#include "htresizer.h"
#include "item.h"
#include "item_compressor.h"
#include "item_freq_decayer.h"
#include "kvshard.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "replicationthrottle.h"
#include "rollback_result.h"
#include "seqno_persistence_notify_task.h"
#include "tasks.h"
#include "trace_helpers.h"
#include "vb_count_visitor.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucketdeletiontask.h"
#include <executor/executorpool.h>
#include <executor/notifiable_task.h>

#include <memcached/collections.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/server_document_iface.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/timeutils.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats& st, KVBucket& str)
        : stats(st), store(str) {
        // EMPTY
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "max_size") {
            store.getEPEngine().setMaxDataSize(value);
        } else if (key == "mem_low_wat") {
            stats.setLowWaterMark(value);
        } else if (key == "mem_high_wat") {
            stats.setHighWaterMark(value);
        } else if (key == "replication_throttle_threshold") {
            stats.replicationThrottleThreshold.store(
                    static_cast<double>(value) / 100.0);
        } else if (key == "warmup_min_memory_threshold") {
            stats.warmupMemUsedCap.store(static_cast<double>(value) / 100.0);
        } else if (key == "warmup_min_items_threshold") {
            stats.warmupNumReadCap.store(static_cast<double>(value) / 100.0);
        } else {
            EP_LOG_WARN(
                    "StatsValueChangeListener(size_t) failed to change value "
                    "for "
                    "unknown variable, {}",
                    key);
        }
    }

    void floatValueChanged(const std::string& key, float value) override {
        if (key == "mem_used_merge_threshold_percent") {
            store.getEPEngine()
                    .getArenaMallocClient()
                    .setEstimateUpdateThreshold(stats.getMaxDataSize(), value);
        } else {
            EP_LOG_WARN(
                    "StatsValueChangeListener(float) failed to change value "
                    "for "
                    "unknown variable, {}",
                    key);
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
    explicit EPStoreValueChangeListener(KVBucket& st) : store(st) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "compaction_write_queue_cap") {
            store.setCompactionWriteQueueCap(value);
        } else if (key == "exp_pager_stime") {
            store.setExpiryPagerSleeptime(value);
        } else if (key == "mutation_mem_threshold") {
            VBucket::setMutationMemoryThreshold(value);
        } else if (key == "backfill_mem_threshold") {
            double backfill_threshold = static_cast<double>(value) / 100;
            store.setBackfillMemoryThreshold(backfill_threshold);
        } else if (key == "max_ttl") {
            store.setMaxTtl(value);
        } else if (key == "checkpoint_max_size") {
            store.setCheckpointMaxSize(value);
        } else if (key == "seqno_persistence_timeout") {
            store.setSeqnoPersistenceTimeout(std::chrono::seconds(value));
        } else {
            EP_LOG_WARN("Failed to change value for unknown variable, {}", key);
        }
    }

    void ssizeValueChanged(const std::string& key, ssize_t value) override {
        if (key == "exp_pager_initial_run_time") {
            store.setExpiryPagerTasktime(value);
        }
    }

    void booleanValueChanged(const std::string& key, bool value) override {
        if (key == "bfilter_enabled") {
            store.setAllBloomFilters(value);
        } else if (key == "exp_pager_enabled") {
            if (value) {
                store.enableExpiryPager();
            } else {
                store.disableExpiryPager();
            }
        } else if (key == "xattr_enabled") {
            store.setXattrEnabled(value);
        }
    }

    void floatValueChanged(const std::string& key, float value) override {
        if (key == "bfilter_residency_threshold") {
            store.setBfiltersResidencyThreshold(value);
        } else if (key == "dcp_min_compression_ratio") {
            store.getEPEngine().updateDcpMinCompressionRatio(value);
        } else if (key == "checkpoint_memory_ratio") {
            store.setCheckpointMemoryRatio(value);
        } else if (key == "checkpoint_memory_recovery_upper_mark") {
            store.setCheckpointMemoryRecoveryUpperMark(value);
        } else if (key == "checkpoint_memory_recovery_lower_mark") {
            store.setCheckpointMemoryRecoveryLowerMark(value);
        } else if (key == "compaction_max_concurrent_ratio") {
            store.setCompactionMaxConcurrency(value);
        }
    }

    void stringValueChanged(const std::string& key,
                            const char* value) override {
        if (key == "durability_min_level") {
            const auto res = store.setMinDurabilityLevel(
                    cb::durability::to_level(value));
            if (res != cb::engine_errc::success) {
                throw std::invalid_argument(
                        "Failed to set durability_min_level: " +
                        to_string(res));
            }
        }
    }

private:
    KVBucket& store;
};

class PendingOpsNotification : public GlobalTask {
public:
    PendingOpsNotification(EventuallyPersistentEngine& e, VBucketPtr& vb)
        : GlobalTask(&e, TaskId::PendingOpsNotification, 0, false),
          engine(e),
          vbucket(vb),
          description("Notify pending operations for " +
                      vbucket->getId().to_string()) {
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This should be a very fast operation (p50 under 10us), however we
        // have observed long tails: p99.9 of 20ms; so use a threshold of 100ms.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT1("ep-engine/task",
                     "PendingOpsNotification",
                     "vb",
                     (vbucket->getId()).get());
        vbucket->fireAllOps(engine);
        return false;
    }

private:
    EventuallyPersistentEngine &engine;
    VBucketPtr vbucket;
    const std::string description;
};

class RespondAmbiguousNotification : public GlobalTask {
public:
    RespondAmbiguousNotification(EventuallyPersistentEngine& e,
                                 VBucketPtr& vb,
                                 std::vector<const CookieIface*>&& cookies_)
        : GlobalTask(&e, TaskId::RespondAmbiguousNotification, 0, false),
          weakVb(vb),
          cookies(std::move(cookies_)),
          description("Notify clients of Sync Write Ambiguous " +
                      vb->getId().to_string()) {
        for (const auto* cookie : cookies) {
            if (!cookie) {
                throw std::invalid_argument(
                        "RespondAmbiguousNotification: Null cookie specified "
                        "for notification");
            }
        }
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Copied from PendingOpsNotification as this task is very similar
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        auto vbucket = weakVb.lock();
        if (!vbucket) {
            return false;
        }

        TRACE_EVENT1("ep-engine/task",
                     "RespondAmbiguousNotification",
                     "vb",
                     (vbucket->getId()).get());

        for (const auto* cookie : cookies) {
            vbucket->notifyClientOfSyncWriteComplete(
                    cookie, cb::engine_errc::sync_write_ambiguous);
        }

        return false;
    }

private:
    std::weak_ptr<VBucket> weakVb;
    std::vector<const CookieIface*> cookies;
    const std::string description;
};

KVBucket::KVBucket(EventuallyPersistentEngine& theEngine)
    : engine(theEngine),
      stats(engine.getEpStats()),
      vbMap(*this),
      defragmenterTask(nullptr),
      itemCompressorTask(nullptr),
      itemFreqDecayerTask(nullptr),
      vb_mutexes(engine.getConfiguration().getMaxVbuckets()),
      backfillMemoryThreshold(0.95),
      lastTransTimePerItem(0),
      collectionsManager(std::make_shared<Collections::Manager>()),
      xattrEnabled(true),
      maxTtl(engine.getConfiguration().getMaxTtl()),
      checkpointMemoryRatio(
              engine.getConfiguration().getCheckpointMemoryRatio()),
      checkpointMemoryRecoveryUpperMark(
              engine.getConfiguration().getCheckpointMemoryRecoveryUpperMark()),
      checkpointMemoryRecoveryLowerMark(
              engine.getConfiguration()
                      .getCheckpointMemoryRecoveryLowerMark()) {
    cachedResidentRatio.activeRatio.store(0);
    cachedResidentRatio.replicaRatio.store(0);

    Configuration &config = engine.getConfiguration();

    const size_t size = GlobalTask::allTaskIds.size();
    stats.schedulingHisto.resize(size);
    stats.taskRuntimeHisto.resize(size);

    for (size_t i = 0; i < GlobalTask::allTaskIds.size(); i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }

    ExecutorPool::get()->registerTaskable(ObjectRegistry::getCurrentEngine()->getTaskable());

    // Reset memory overhead when bucket is created.
    for (auto& core : stats.coreLocal) {
        core->memOverhead = 0;
    }
    stats.coreLocal.get()->memOverhead = sizeof(KVBucket);

    config.addValueChangedListener(
            "mem_used_merge_threshold_percent",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    config.addValueChangedListener(
            "max_size",
            std::make_unique<StatsValueChangeListener>(stats, *this));
    getEPEngine().getDcpConnMap().updateMaxRunningBackfills(
            config.getMaxSize());

    config.addValueChangedListener(
            "mem_low_wat",
            std::make_unique<StatsValueChangeListener>(stats, *this));
    config.addValueChangedListener(
            "mem_high_wat",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    stats.replicationThrottleThreshold.store(static_cast<double>
                                    (config.getReplicationThrottleThreshold())
                                     / 100.0);
    config.addValueChangedListener(
            "replication_throttle_threshold",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    stats.warmupMemUsedCap.store(static_cast<double>
                               (config.getWarmupMinMemoryThreshold()) / 100.0);
    config.addValueChangedListener(
            "warmup_min_memory_threshold",
            std::make_unique<StatsValueChangeListener>(stats, *this));
    stats.warmupNumReadCap.store(static_cast<double>
                                (config.getWarmupMinItemsThreshold()) / 100.0);
    config.addValueChangedListener(
            "warmup_min_items_threshold",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    VBucket::setMutationMemoryThreshold(config.getMutationMemThreshold());
    config.addValueChangedListener(
            "mutation_mem_threshold",
            std::make_unique<EPStoreValueChangeListener>(*this));

    double backfill_threshold = static_cast<double>
                                      (config.getBackfillMemThreshold()) / 100;
    setBackfillMemoryThreshold(backfill_threshold);
    config.addValueChangedListener(
            "backfill_mem_threshold",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "bfilter_enabled",
            std::make_unique<EPStoreValueChangeListener>(*this));

    bfilterResidencyThreshold = config.getBfilterResidencyThreshold();
    config.addValueChangedListener(
            "bfilter_residency_threshold",
            std::make_unique<EPStoreValueChangeListener>(*this));

    compactionWriteQueueCap = config.getCompactionWriteQueueCap();
    config.addValueChangedListener(
            "compaction_write_queue_cap",
            std::make_unique<EPStoreValueChangeListener>(*this));

    compactionMaxConcurrency = config.getCompactionMaxConcurrentRatio();
    config.addValueChangedListener(
            "compaction_max_concurrent_ratio",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "dcp_min_compression_ratio",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "xattr_enabled",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "max_ttl", std::make_unique<EPStoreValueChangeListener>(*this));

    xattrEnabled = config.isXattrEnabled();

    auto numConcurrentPagers = config.getConcurrentPagers();

    // Always create the item pager; but initially disable, leaving scheduling
    // up to the specific KVBucket subclasses.
    itemPagerTask =
            std::make_shared<ItemPager>(engine, stats, numConcurrentPagers);
    disableItemPager();

    minDurabilityLevel =
            cb::durability::to_level(config.getDurabilityMinLevel());
    config.addValueChangedListener(
            "durability_min_level",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "checkpoint_memory_ratio",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "checkpoint_memory_recovery_upper_mark",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "checkpoint_memory_recovery_lower_mark",
            std::make_unique<EPStoreValueChangeListener>(*this));

    // Note: setting via setter for handling auto-setup. See setter for details.
    setCheckpointMaxSize(config.getCheckpointMaxSize());
    config.addValueChangedListener(
            "checkpoint_max_size",
            std::make_unique<EPStoreValueChangeListener>(*this));

    setSeqnoPersistenceTimeout(
            std::chrono::seconds(config.getSeqnoPersistenceTimeout()));
    config.addValueChangedListener(
            "seqno_persistence_timeout",
            std::make_unique<EPStoreValueChangeListener>(*this));
}

bool KVBucket::initialize() {
    // We should nuke everything unless we want warmup
    Configuration &config = engine.getConfiguration();
    if ((config.getBucketType() == "ephemeral") || (!config.isWarmup())) {
        reset();
    }

    initializeExpiryPager(config);

    ExTask htrTask = std::make_shared<HashtableResizerTask>(*this, 10);
    ExecutorPool::get()->schedule(htrTask);

    const auto checkpointRemoverInterval = config.getChkRemoverStime();
    const auto numChkRemovers = config.getCheckpointRemoverTaskCount();
    for (size_t id = 0; id < numChkRemovers; ++id) {
        auto task = std::make_shared<CheckpointMemRecoveryTask>(
                &engine, stats, checkpointRemoverInterval, id);
        chkRemovers.emplace_back(task);
        ExecutorPool::get()->schedule(task);
    }

    auto numCkptDestroyers = config.getCheckpointDestructionTasks();
    for (size_t i = 0; i < numCkptDestroyers; ++i) {
        ckptDestroyerTasks.push_back(
                std::make_shared<CheckpointDestroyerTask>(&engine));
        ExecutorPool::get()->schedule(ckptDestroyerTasks.back());
    }

    // Setup tasks related to SyncWrite timeout handling. At present there are
    // two possible modes:
    // * "polling" (added in Mad-Hatter) which uses a task per Bucket and polls
    //   all vBuckets for any timed out SyncWrites every
    //   durability_timeout_task_interval ms.
    // * "event-driven" (added in Neo) which uses a task per vBucket; each of
    //   which is scheduled to run when the next SyncWrite to be completed is
    //   due to exceed it's timeout. If that SyncWrite is completed before the
    //   timeout then the task is re-scheduled (and doesn't run).
    if (config.getDurabilityTimeoutMode() == "polling") {
        durabilityTimeoutTask = std::make_shared<DurabilityTimeoutTask>(
                engine,
                std::chrono::milliseconds(
                        config.getDurabilityTimeoutTaskInterval()));
        ExecutorPool::get()->schedule(durabilityTimeoutTask);
    } else if (config.getDurabilityTimeoutMode() == "event-driven") {
        syncWriteTimeoutFactory =
                [&taskable =
                         this->getEPEngine().getTaskable()](VBucket& vbucket) {
                    return std::make_unique<EventDrivenDurabilityTimeout>(
                            taskable, vbucket);
                };
    }

    durabilityCompletionTask =
            std::make_shared<DurabilityCompletionTask>(engine);
    ExecutorPool::get()->schedule(durabilityCompletionTask);

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

    enableItemCompressor();

    /*
     * Creates the ItemFreqDecayer task which is used to ensure that the
     * frequency counters of items stored in the hash table do not all
     * become saturated.  Once the task runs it will snooze for int max
     * seconds and will only be woken up when the frequency counter of an
     * item in the hash table becomes saturated.
     */
    itemFreqDecayerTask = std::make_shared<ItemFreqDecayerTask>(
            &engine, config.getItemFreqDecayerPercent());
    ExecutorPool::get()->schedule(itemFreqDecayerTask);

    createAndScheduleSeqnoPersistenceNotifier();

    return true;
}

void KVBucket::deinitialize() {
    EP_LOG_INFO("KVBucket::deinitialize forceShutdown:{}", stats.forceShutdown);
    ExecutorPool::get()->unregisterTaskable(engine.getTaskable(),
                                            stats.forceShutdown);
}

KVBucket::~KVBucket() {
    EP_LOG_INFO_RAW("Deleting vb_mutexes");
    EP_LOG_INFO_RAW("Deleting defragmenterTask");
    defragmenterTask.reset();
    EP_LOG_INFO_RAW("Deleting itemCompressorTask");
    itemCompressorTask.reset();
    EP_LOG_INFO_RAW("Deleting itemFreqDecayerTask");
    itemFreqDecayerTask.reset();
    EP_LOG_INFO_RAW("Deleted KvBucket.");
}

Warmup* KVBucket::getWarmup() const {
    return nullptr;
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

cb::mcbp::Status KVBucket::evictKey(const DocKey& key,
                                    Vbid vbucket,
                                    const char** msg) {
    auto vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::mcbp::Status::NotMyVbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return cb::mcbp::Status::NotMyVbucket;
    }

    // collections read-lock scope
    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        return cb::mcbp::Status::UnknownCollection;
    } // now hold collections read access for the duration of the evict

    return vb->evictKey(msg, cHandle);
}

void KVBucket::getValue(Item& it) {
    auto gv = getROUnderlying(it.getVBucketId())
                      ->get(DiskDocKey{it}, it.getVBucketId());

    if (gv.getStatus() != cb::engine_errc::success) {
        // Cannot continue to pre_expiry, log this failed get and return
        EP_LOG_WARN(
                "KVBucket::getValue failed get for item {}, it.seqno:{}, "
                "status:{}",
                it.getVBucketId(),
                it.getBySeqno(),
                gv.getStatus());
        return;
    } else if (!gv.item->isDeleted()) {
        it.replaceValue(gv.item->getValue().get());
    }

    // Ensure the datatype is set from what we loaded. MB-32669 was an example
    // of an issue where they could differ.
    it.setDataType(gv.item->getDataType());
}

const StorageProperties KVBucket::getStorageProperties() const {
    const auto* store = vbMap.shards[0]->getROUnderlying();
    return store->getStorageProperties();
}

void KVBucket::runPreExpiryHook(VBucket& vb, Item& it) {
    it.decompressValue(); // A no-op for already decompressed items
    auto info =
            it.toItemInfo(vb.failovers->getLatestUUID(), vb.getHLCEpochSeqno());
    auto result = engine.getServerApi()->document->pre_expiry(info);
    if (!result.empty()) {
        // A modified value was returned, use it
        it.replaceValue(TaggedPtr<Blob>(Blob::New(result.data(), result.size()),
                                        TaggedPtrBase::NoTagValue));
        // The API states only uncompressed xattr values are returned
        it.setDataType(PROTOCOL_BINARY_DATATYPE_XATTR);
    } else {
        // Make the document empty and raw
        it.replaceValue(
                TaggedPtr<Blob>(Blob::New(0), TaggedPtrBase::NoTagValue));
        it.setDataType(PROTOCOL_BINARY_RAW_BYTES);
    }
}

void KVBucket::processExpiredItem(Item& it, time_t startTime, ExpireBy source) {
    // Yield if checkpoint's full - The call also wakes up the mem recovery task
    if (verifyCheckpointMemoryState() == CheckpointMemoryState::Full) {
        return;
    }

    auto vb = getVBucket(it.getVBucketId());
    if (!vb) {
        return;
    }

    // MB-25931: Empty XATTR items need their value before we can call
    // pre_expiry. These occur because the value has been evicted.
    if (mcbp::datatype::is_xattr(it.getDataType()) && it.getNBytes() == 0) {
        getValue(it);
    }

    // Process positive seqnos (ignoring special *temp* items) and only those
    // items with a value
    if (it.getBySeqno() >= 0 && it.getNBytes()) {
        runPreExpiryHook(*vb, it);
    }

    // Obtain reader access to the VB state change lock so that the VB can't
    // switch state whilst we're processing
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_active) {
        vb->processExpiredItem(it, startTime, source);
    }
}

bool KVBucket::isMetaDataResident(VBucketPtr &vb, const DocKey& key) {

    if (!vb) {
        throw std::invalid_argument("EPStore::isMetaDataResident: vb is NULL");
    }

    auto result = vb->ht.findForRead(key, TrackReference::No, WantsDeleted::No);

    return result.storedValue && !result.storedValue->isTempItem();
}

void KVBucket::logQTime(const GlobalTask& task,
                        std::string_view threadName,
                        std::chrono::steady_clock::duration enqTime) {
    // MB-25822: It could be useful to have the exact datetime of long
    // schedule times, in the same way we have for long runtimes.
    // It is more difficult to estimate the expected schedule time than
    // the runtime for a task, because the schedule times depends on
    // things "external" to the task itself (e.g., how many tasks are
    // in queue in the same priority-group).
    // Also, the schedule time depends on the runtime of the previous
    // run. That means that for Read/Write/AuxIO tasks it is even more
    // difficult to predict because that do IO.
    // So, for now we log long schedule times for AUX_IO and NON_IO tasks.
    // We consider 1 second a sensible schedule overhead limit for NON_IO
    // tasks, and 10 seconds a generous (but hopefully not overly common)
    // schedule overhead for AUX_IO tasks.
    const auto taskType = GlobalTask::getTaskType(task.getTaskId());
    if ((taskType == task_type_t::NONIO_TASK_IDX &&
         enqTime > std::chrono::seconds(1)) ||
        (taskType == task_type_t::AUXIO_TASK_IDX &&
         enqTime > std::chrono::seconds(10))) {
        EP_LOG_WARN(
                "Slow scheduling for {} task '{}' on thread {}. "
                "Schedule overhead: {}",
                to_string(taskType),
                task.getDescription(),
                threadName,
                cb::time2text(enqTime));
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(enqTime);
    stats.schedulingHisto[static_cast<int>(task.getTaskId())].add(us);
}

void KVBucket::logRunTime(const GlobalTask& task,
                          std::string_view threadName,
                          std::chrono::steady_clock::duration runTime) {
    // Check if exceeded expected duration; and if so log.
    if (runTime > task.maxExpectedDuration()) {
        EP_LOG_WARN("Slow runtime for '{}' on thread {}: {}",
                    task.getDescription(),
                    threadName,
                    cb::time2text(runTime));
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(runTime);
    stats.taskRuntimeHisto[static_cast<int>(task.getTaskId())].add(us);
}

cb::engine_errc KVBucket::set(Item& itm,
                              const CookieIface* cookie,
                              cb::StoreIfPredicate predicate) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this set
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return cb::engine_errc::would_block;
        }
    } else if (vb->isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a set op, because "
                "takeover is lagging",
                vb->getId());
        return cb::engine_errc::temporary_failure;
    }

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        auto status =
                cHandle.handleWriteStatus(engine, cookie, itm.getNBytes());
        if (status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the set

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());

        result = vb->set(itm, cookie, engine, predicate, cHandle);
        if (result == cb::engine_errc::success) {
            itm.isDeleted() ? cHandle.incrementOpsDelete()
                            : cHandle.incrementOpsStore();
        }
    }

    if (itm.isPending()) {
        vb->notifyActiveDMOfLocalSyncWrite();
    }

    return result;
}

cb::engine_errc KVBucket::add(Item& itm, const CookieIface* cookie) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this add
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return cb::engine_errc::would_block;
        }
    } else if (vb->isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a add op"
                ", becuase takeover is lagging",
                vb->getId());
        return cb::engine_errc::temporary_failure;
    }

    if (itm.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return cb::engine_errc::not_stored;
    }

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        auto status =
                cHandle.handleWriteStatus(engine, cookie, itm.getNBytes());
        if (status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the add

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());
        result = vb->add(itm, cookie, engine, cHandle);
        if (result == cb::engine_errc::success) {
            itm.isDeleted() ? cHandle.incrementOpsDelete()
                            : cHandle.incrementOpsStore();
        }
    }

    if (itm.isPending()) {
        vb->notifyActiveDMOfLocalSyncWrite();
    }

    return result;
}

cb::engine_errc KVBucket::replace(Item& itm,
                                  const CookieIface* cookie,
                                  cb::StoreIfPredicate predicate) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    // Obtain read-lock on VB state to ensure VB state changes are interlocked
    // with this replace
    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return cb::engine_errc::would_block;
        }
    }

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        auto status =
                cHandle.handleWriteStatus(engine, cookie, itm.getNBytes());
        if (status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the replace

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());
        result = vb->replace(itm, cookie, engine, predicate, cHandle);
        if (result == cb::engine_errc::success) {
            itm.isDeleted() ? cHandle.incrementOpsDelete()
                            : cHandle.incrementOpsStore();
        }
    }

    if (itm.isPending()) {
        vb->notifyActiveDMOfLocalSyncWrite();
    }

    return result;
}

GetValue KVBucket::get(const DocKey& key,
                       Vbid vbucket,
                       const CookieIface* cookie,
                       get_options_t options) {
    return getInternal(key, vbucket, cookie, ForGetReplicaOp::No, options);
}

GetValue KVBucket::getReplica(const DocKey& key,
                              Vbid vbucket,
                              const CookieIface* cookie,
                              get_options_t options) {
    return getInternal(key, vbucket, cookie, ForGetReplicaOp::Yes, options);
}

uint64_t KVBucket::getLastPersistedSeqno(Vbid vb) {
    auto vbucket = vbMap.getBucket(vb);
    if (vbucket) {
        return vbucket->getPersistenceSeqno();
    } else {
        return 0;
    }
}

void KVBucket::releaseBlockedCookies() {
    for (size_t vbid = 0; vbid < vbMap.size; ++vbid) {
        VBucketPtr vb = vbMap.getBucket(Vbid{gsl::narrow<uint16_t>(vbid)});
        if (!vb) {
            continue;
        }
        folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
        if (vb->getState() != vbucket_state_active) {
            continue;
        }

        auto cookies = vb->getCookiesForInFlightSyncWrites();
        if (!cookies.empty()) {
            EP_LOG_INFO("{} Cancel {} blocked durability requests",
                        vb->getId(),
                        cookies.size());
            ExTask notifyTask = std::make_shared<RespondAmbiguousNotification>(
                    engine, vb, std::move(cookies));
            ExecutorPool::get()->schedule(notifyTask);
        }
    }
}

cb::engine_errc KVBucket::setVBucketState(Vbid vbid,
                                          vbucket_state_t to,
                                          const nlohmann::json* meta,
                                          TransferVB transfer,
                                          const CookieIface* cookie) {
    // MB-25197: we shouldn't process setVBState if warmup hasn't yet loaded
    // the vbucket state data.
    if (cookie && maybeWaitForVBucketWarmup(cookie)) {
        EP_LOG_INFO(
                "KVBucket::setVBucketState blocking {}, to:{}, transfer:{}, "
                "cookie:{}",
                vbid,
                VBucket::toString(to),
                transfer,
                static_cast<const void*>(cookie));
        return cb::engine_errc::would_block;
    }

    // Lock to prevent a race condition between a failed update and add.
    std::unique_lock<std::mutex> lh(vbsetMutex);
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (vb) {
        folly::SharedMutex::WriteHolder vbStateLock(vb->getStateLock());
        setVBucketState_UNLOCKED(
                vb, to, meta, transfer, true /*notifyDcp*/, lh, vbStateLock);
    } else if (vbid.get() < vbMap.getSize()) {
        return createVBucket_UNLOCKED(vbid, to, meta, lh);
    } else {
        return cb::engine_errc::out_of_range;
    }
    return cb::engine_errc::success;
}

void KVBucket::setVBucketState_UNLOCKED(
        VBucketPtr& vb,
        vbucket_state_t to,
        const nlohmann::json* meta,
        TransferVB transfer,
        bool notify_dcp,
        std::unique_lock<std::mutex>& vbset,
        folly::SharedMutex::WriteHolder& vbStateLock) {
    // Return success immediately if the new state is the same as the old,
    // and no extra metadata was included.
    if (to == vb->getState() && !meta) {
        return;
    }

    // We need to process any outstanding SyncWrites before we set the
    // vBucket state so that we can keep our invariant that we do not use
    // an ActiveDurabilityMonitor in a state other than active. This is done
    // under a write lock of the vbState and we will set the vBucket state
    // under the same lock so we will not attempt to queue any more
    // SyncWrites after sending these notifications.
    if (vb->getState() == vbucket_state_active && to != vb->getState()) {
        // At state change to !active we should return
        // cb::engine_errc::sync_write_ambiguous to any clients waiting for the
        // result of a SyncWrite as they will timeout anyway.

        // Get a list of cookies that we should respond to
        auto connectionsToRespondTo = vb->prepareTransitionAwayFromActive();
        if (!connectionsToRespondTo.empty()) {
            ExTask notifyTask = std::make_shared<RespondAmbiguousNotification>(
                    engine, vb, std::move(connectionsToRespondTo));
            ExecutorPool::get()->schedule(notifyTask);
        }
    }

    auto oldstate = vbMap.setState_UNLOCKED(*vb, to, meta, vbStateLock);
    vb->updateStatsForStateChange(oldstate, to);

    if (oldstate != to && notify_dcp) {
        bool closeInboundStreams = false;
        if (to == vbucket_state_active && transfer == TransferVB::No) {
            /**
             * Close inbound (passive) streams into the vbucket
             * only in case of a failover.
             */
            closeInboundStreams = true;
        }
        engine.getDcpConnMap().vbucketStateChanged(
                vb->getId(), to, closeInboundStreams, &vbStateLock);
    }

    /**
     * Expect this to happen for failover
     */
    if (to == vbucket_state_active && oldstate != vbucket_state_active) {
        /**
         * Create a new checkpoint to ensure that we do not now write to a
         * Disk checkpoint. This updates the snapshot range to maintain the
         * correct snapshot sequence numbers even in a failover scenario.
         */
        vb->checkpointManager->createNewCheckpoint();

        /**
         * Update the manifest of this vBucket from the
         * collectionsManager to ensure that it did not miss a manifest
         * that was not replicated via DCP.
         */
        collectionsManager->maybeUpdate(*vb);

        // MB-37917: The vBucket is becoming an active and can no longer be
        // receiving an initial disk snapshot. It is now the source of truth so
        // we should not prevent any Consumer from streaming from it.
        vb->setReceivingInitialDiskSnapshot(false);
    }

    if (to == vbucket_state_active && oldstate != vbucket_state_active &&
        transfer == TransferVB::No) {
        // Changed state to active and this isn't a transfer (i.e.
        // takeover), which means this is a new fork in the vBucket history
        // - create a new failover table entry.
        const snapshot_range_t range = vb->getPersistedSnapshot();
        auto highSeqno = range.getEnd() == vb->getPersistenceSeqno()
                                 ? range.getEnd()
                                 : range.getStart();
        vb->createFailoverEntry(highSeqno);

        auto entry = vb->failovers->getLatestEntry();
        EP_LOG_INFO(
                "KVBucket::setVBucketState: {} created new failover entry "
                "with uuid:{} and seqno:{}",
                vb->getId(),
                entry.vb_uuid,
                entry.by_seqno);
    }

    if (oldstate == vbucket_state_pending && to == vbucket_state_active) {
        ExTask notifyTask =
                std::make_shared<PendingOpsNotification>(engine, vb);
        ExecutorPool::get()->schedule(notifyTask);
    }

    if (to == vbucket_state_dead) {
        // Reset takeover state (which sets vBucket to dead)
        vb->setTakeoverBackedUpState(false);
    }

    scheduleVBStatePersist(vb->getId());
}

cb::engine_errc KVBucket::createVBucket_UNLOCKED(
        Vbid vbid,
        vbucket_state_t to,
        const nlohmann::json* meta,
        std::unique_lock<std::mutex>& vbset) {
    auto ft = std::make_unique<FailoverTable>(engine.getMaxFailoverEntries());
    KVShard* shard = vbMap.getShardByVbId(vbid);

    VBucketPtr newvb = makeVBucket(
            vbid,
            to,
            shard,
            std::move(ft),
            std::make_unique<NotifyNewSeqnoCB>(*this),
            std::make_unique<Collections::VB::Manifest>(collectionsManager));

    newvb->setFreqSaturatedCallback(
            [this] { this->wakeItemFreqDecayerTask(); });

    Configuration& config = engine.getConfiguration();
    if (config.isBfilterEnabled()) {
        // Initialize bloom filters upon vbucket creation during
        // bucket creation and rebalance
        newvb->createFilter(config.getBfilterKeyCount(),
                            config.getBfilterFpProb());
    }

    // Before adding the VB to the map, notify KVStore of the create
    vbMap.getShardByVbId(vbid)->forEachKVStore(
            [vbid](KVStoreIface* kvs) { kvs->prepareToCreate(vbid); });

    // If active, update the VB from the bucket's collection state.
    // Note: Must be done /before/ adding the new VBucket to vbMap so that
    // it has the correct collections state when it is exposed to operations
    if (to == vbucket_state_active) {
        collectionsManager->maybeUpdate(*newvb);
    }

    if (vbMap.addBucket(newvb) == cb::engine_errc::out_of_range) {
        return cb::engine_errc::out_of_range;
    }

    // @todo-durability: Can the following happen?
    //     For now necessary at least for tests.
    // Durability: Re-set vb-state for applying the ReplicationChain
    //     encoded in 'meta'. This is for supporting the case where
    //     ns_server issues a single set-vb-state call for creating a VB.
    // Note: Must be done /after/ the new VBucket has been added to vbMap.
    if (to == vbucket_state_active || to == vbucket_state_replica) {
        vbMap.setState(*newvb, to, meta);
    }

    // When the VBucket is constructed we initialize
    // persistenceSeqno(0) && persistenceCheckpointId(0)
    newvb->setBucketCreation(true);
    scheduleVBStatePersist(vbid);
    return cb::engine_errc::success;
}

void KVBucket::scheduleVBStatePersist() {
    for (auto vbid : vbMap.getBuckets()) {
        scheduleVBStatePersist(vbid);
    }
}

void KVBucket::scheduleVBStatePersist(Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);

    if (!vb) {
        EP_LOG_WARN(
                "EPStore::scheduleVBStatePersist: {} does not not exist. "
                "Unable to schedule persistence.",
                vbid);
        return;
    }

    vb->checkpointManager->queueSetVBState();
}

cb::engine_errc KVBucket::deleteVBucket(Vbid vbid, const CookieIface* c) {
    {
        std::unique_lock<std::mutex> vbSetLh(vbsetMutex);
        // Obtain a locked VBucket to ensure we interlock with other
        // threads that are manipulating the VB (particularly ones which may
        // try and change the disk revision e.g. deleteAll and compaction).
        auto lockedVB = getLockedVBucket(vbid);
        if (!lockedVB) {
            return cb::engine_errc::not_my_vbucket;
        }

        vbMap.setState(*lockedVB, vbucket_state_dead, nullptr);
        getRWUnderlying(vbid)->abortCompactionIfRunning(lockedVB.getLock(),
                                                        vbid);
        engine.getDcpConnMap().vbucketStateChanged(vbid, vbucket_state_dead);

        // Drop the VB to begin the delete, the last holder of the VB will
        // unknowingly trigger the destructor which schedules a deletion task.
        vbMap.dropVBucketAndSetupDeferredDeletion(vbid, c);
    }

    if (c) {
        return cb::engine_errc::would_block;
    }
    return cb::engine_errc::success;
}

cb::engine_errc KVBucket::checkForDBExistence(Vbid db_file_id) {
    std::string backend = engine.getConfiguration().getBackend();
    if (backend == "couchdb" || backend == "magma" || backend == "nexus") {
        VBucketPtr vb = vbMap.getBucket(db_file_id);
        if (!vb) {
            return cb::engine_errc::not_my_vbucket;
        }
    } else {
        EP_LOG_WARN("Unknown backend specified for db file id: {}",
                    db_file_id.get());
        return cb::engine_errc::failed;
    }

    return cb::engine_errc::success;
}

bool KVBucket::resetVBucket(Vbid vbid) {
    std::unique_lock<std::mutex> vbsetLock(vbsetMutex);
    // Obtain a locked VBucket to ensure we interlock with other
    // threads that are manipulating the VB (particularly ones which may
    // try and change the disk revision).
    auto lockedVB = getLockedVBucket(vbid);
    return resetVBucket_UNLOCKED(lockedVB, vbsetLock);
}

bool KVBucket::resetVBucket_UNLOCKED(LockedVBucketPtr& vb,
                                     std::unique_lock<std::mutex>& vbset) {
    bool rv(false);

    if (vb) {
        vbucket_state_t vbstate = vb->getState();

        // 1) Remove the vb from the map and begin the deferred deletion
        getRWUnderlying(vb->getId())
                ->abortCompactionIfRunning(vb.getLock(), vb->getId());
        vbMap.dropVBucketAndSetupDeferredDeletion(vb->getId(),
                                                  nullptr /*no cookie*/);

        // 2) Create a new vbucket
        createVBucket_UNLOCKED(vb->getId(), vbstate, {}, vbset);

        // Move the cursors from the old vbucket into the new vbucket
        VBucketPtr newvb = vbMap.getBucket(vb->getId());
        newvb->checkpointManager->takeAndResetCursors(*vb->checkpointManager);
        rv = true;
    }
    return rv;
}

void KVBucket::snapshotStats(bool shuttingDown) {
    std::map<std::string, std::string> statsMap;
    auto snapshot_add_stat = [&statsMap](std::string_view key,
                                         std::string_view value,
                                         const void* ctx) {
        statsMap.insert(std::pair<std::string, std::string>(
                std::string{key.data(), key.size()},
                std::string{value.data(), value.size()}));
    };
    cb::tracing::Traceable traceable;
    bool rv = engine.getStats(&traceable, {}, {}, snapshot_add_stat) ==
                      cb::engine_errc::success;

    nlohmann::json snapshotStats(statsMap);
    if (rv && shuttingDown) {
        snapshotStats["ep_force_shutdown"] =
                stats.forceShutdown ? "true" : "false";
        snapshotStats["ep_shutdown_time"] = fmt::format("{}", ep_real_time());
    }
    getOneRWUnderlying()->snapshotStats(snapshotStats);
}

void KVBucket::getAggregatedVBucketStats(
        const BucketStatCollector& collector,
        cb::prometheus::Cardinality cardinality) {
    // track whether high or low cardinality stats should be collected
    auto doHigh = cardinality == cb::prometheus::Cardinality::All ||
                  cardinality == cb::prometheus::Cardinality::High;
    auto doLow = cardinality == cb::prometheus::Cardinality::All ||
                 cardinality == cb::prometheus::Cardinality::Low;

    // Create visitors for each of the four vBucket states, and collect
    // stats for each.
    auto active = makeVBCountVisitor(vbucket_state_active);
    auto replica = makeVBCountVisitor(vbucket_state_replica);
    auto pending = makeVBCountVisitor(vbucket_state_pending);
    auto dead = makeVBCountVisitor(vbucket_state_dead);

    DatatypeStatVisitor activeDatatype(vbucket_state_active);
    DatatypeStatVisitor replicaDatatype(vbucket_state_replica);

    VBucketStatAggregator aggregator;
    if (doLow) {
        // aggregate the important stats which need frequent updating
        aggregator.addVisitor(active.get());
        aggregator.addVisitor(replica.get());
        aggregator.addVisitor(pending.get());
        aggregator.addVisitor(dead.get());
    }

    if (doHigh) {
        // aggregate the datatype stats, which produce a lot of results and can
        // be collected less frequently
        aggregator.addVisitor(&activeDatatype);
        aggregator.addVisitor(&replicaDatatype);
    }
    visit(aggregator);

    if (doLow) {
        updateCachedResidentRatio(active->getMemResidentPer(),
                                  replica->getMemResidentPer());
        updateCachedResidentRatio(active->getMemResidentPer(),
                                  replica->getMemResidentPer());

        // And finally actually return the stats using the AddStatFn callback.
        appendAggregatedVBucketStats(
                *active, *replica, *pending, *dead, collector);
    }

    if (doHigh) {
        appendDatatypeStats(activeDatatype, replicaDatatype, collector);
    }
}

std::unique_ptr<VBucketCountVisitor> KVBucket::makeVBCountVisitor(
        vbucket_state_t state) {
    return std::make_unique<VBucketCountVisitor>(state);
}

void KVBucket::appendAggregatedVBucketStats(
        const VBucketCountVisitor& active,
        const VBucketCountVisitor& replica,
        const VBucketCountVisitor& pending,
        const VBucketCountVisitor& dead,
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    // Top-level stats:
    collector.addStat(Key::curr_items, active.getNumItems());
    collector.addStat(Key::curr_temp_items, active.getNumTempItems());
    collector.addStat(Key::curr_items_tot,
                      active.getNumItems() + replica.getNumItems() +
                              pending.getNumItems());

    for (const auto& visitor : {active, replica, pending}) {
        auto state = VBucket::toString(visitor.getVBucketState());
        auto stateCol = collector.withLabels({{"state", state}});

        stateCol.addStat(Key::vb_num, visitor.getVBucketNumber());
        stateCol.addStat(Key::vb_curr_items, visitor.getNumItems());
        stateCol.addStat(Key::vb_hp_vb_req_size, visitor.getNumHpVBReqs());
        stateCol.addStat(Key::vb_num_non_resident, visitor.getNonResident());
        stateCol.addStat(Key::vb_perc_mem_resident,
                         visitor.getMemResidentPer());
        stateCol.addStat(Key::vb_eject, visitor.getEjects());
        stateCol.addStat(Key::vb_expired, visitor.getExpired());
        stateCol.addStat(Key::vb_meta_data_memory, visitor.getMetaDataMemory());
        stateCol.addStat(Key::vb_meta_data_disk, visitor.getMetaDataDisk());

        stateCol.addStat(Key::vb_checkpoint_memory,
                         visitor.getCheckpointMemory());
        stateCol.addStat(Key::vb_checkpoint_memory_queue,
                         visitor.getCheckpointMemoryQueue());

        stateCol.addStat(Key::vb_checkpoint_memory_overhead_allocator,
                         visitor.getCheckpointMemOverheadAllocatorBytes());
        stateCol.addStat(Key::vb_checkpoint_memory_overhead_allocator_queue,
                         visitor.getCheckpointMemOverheadAllocatorBytesQueue());
        stateCol.addStat(Key::vb_checkpoint_memory_overhead_allocator_index,
                         visitor.getCheckpointMemOverheadAllocatorBytesIndex());
        stateCol.addStat(
                Key::vb_checkpoint_memory_overhead_allocator_index_key,
                visitor.getCheckpointMemOverheadAllocatorBytesIndexKey());

        stateCol.addStat(Key::vb_checkpoint_memory_overhead,
                         visitor.getCheckpointMemOverhead());
        stateCol.addStat(Key::vb_checkpoint_memory_overhead_queue,
                         visitor.getCheckpointMemOverheadQueue());
        stateCol.addStat(Key::vb_checkpoint_memory_overhead_index,
                         visitor.getCheckpointMemOverheadIndex());

        stateCol.addStat(Key::vb_mem_freed_by_checkpoint_item_expel,
                         visitor.getCheckpointMemFreedByItemExpel());
        stateCol.addStat(Key::vb_mem_freed_by_checkpoint_removal,
                         visitor.getCheckpointMemFreedByRemoval());

        stateCol.addStat(Key::vb_ht_memory, visitor.getHashtableMemory());
        stateCol.addStat(Key::vb_ht_item_memory, visitor.getItemMemory());
        stateCol.addStat(Key::vb_ht_item_memory_uncompressed,
                         visitor.getUncompressedItemMemory());
        stateCol.addStat(Key::vb_bloom_filter_memory,
                         visitor.getBloomFilterMemory());
        stateCol.addStat(Key::vb_ops_create, visitor.getOpsCreate());
        stateCol.addStat(Key::vb_ops_update, visitor.getOpsUpdate());
        stateCol.addStat(Key::vb_ops_delete, visitor.getOpsDelete());
        stateCol.addStat(Key::vb_ops_get, visitor.getOpsGet());
        stateCol.addStat(Key::vb_ops_reject, visitor.getOpsReject());
        stateCol.addStat(Key::vb_queue_size, visitor.getQueueSize());
        stateCol.addStat(Key::vb_queue_memory, visitor.getQueueMemory());
        stateCol.addStat(Key::vb_queue_age, visitor.getAge());
        stateCol.addStat(Key::vb_queue_pending, visitor.getPendingWrites());
        stateCol.addStat(Key::vb_queue_fill, visitor.getQueueFill());
        stateCol.addStat(Key::vb_queue_drain, visitor.getQueueDrain());
        stateCol.addStat(Key::vb_rollback_item_count,
                         visitor.getRollbackItemCount());
    }

    for (const auto& visitor : {active, replica}) {
        auto state = VBucket::toString(visitor.getVBucketState());
        auto stateCol = collector.withLabels({{"state", state}});

        stateCol.addStat(Key::vb_sync_write_accepted_count,
                         visitor.getSyncWriteAcceptedCount());
        stateCol.addStat(Key::vb_sync_write_committed_count,
                         visitor.getSyncWriteCommittedCount());
        stateCol.addStat(Key::vb_sync_write_aborted_count,
                         visitor.getSyncWriteAbortedCount());
    }

    // Dead vBuckets:
    collector.withLabels({{"state", "dead"}})
            .addStat(Key::vb_num, dead.getVBucketNumber());

    // Totals:
    collector.addStat(Key::ep_vb_total,
                      active.getVBucketNumber() + replica.getVBucketNumber() +
                              pending.getVBucketNumber() +
                              dead.getVBucketNumber());
    collector.addStat(Key::ep_total_new_items,
                      active.getOpsCreate() + replica.getOpsCreate() +
                              pending.getOpsCreate());
    collector.addStat(Key::ep_total_del_items,
                      active.getOpsDelete() + replica.getOpsDelete() +
                              pending.getOpsDelete());
    collector.addStat(Key::ep_diskqueue_memory,
                      active.getQueueMemory() + replica.getQueueMemory() +
                              pending.getQueueMemory());
    collector.addStat(Key::ep_diskqueue_fill,
                      active.getQueueFill() + replica.getQueueFill() +
                              pending.getQueueFill());
    collector.addStat(Key::ep_diskqueue_drain,
                      active.getQueueDrain() + replica.getQueueDrain() +
                              pending.getQueueDrain());
    collector.addStat(Key::ep_diskqueue_pending,
                      active.getPendingWrites() + replica.getPendingWrites() +
                              pending.getPendingWrites());
    collector.addStat(Key::ep_ht_item_memory,
                      active.getItemMemory() + replica.getItemMemory() +
                              pending.getItemMemory());
    collector.addStat(Key::ep_meta_data_memory,
                      active.getMetaDataMemory() + replica.getMetaDataMemory() +
                              pending.getMetaDataMemory());
    collector.addStat(Key::ep_meta_data_disk,
                      active.getMetaDataDisk() + replica.getMetaDataDisk() +
                              pending.getMetaDataDisk());
    collector.addStat(Key::ep_checkpoint_memory,
                      active.getCheckpointMemory() +
                              replica.getCheckpointMemory() +
                              pending.getCheckpointMemory());
    collector.addStat(Key::ep_checkpoint_memory_overhead_allocator,
                      active.getCheckpointMemOverheadAllocatorBytes() +
                              replica.getCheckpointMemOverheadAllocatorBytes() +
                              pending.getCheckpointMemOverheadAllocatorBytes());
    collector.addStat(Key::ep_total_cache_size,
                      active.getCacheSize() + replica.getCacheSize() +
                              pending.getCacheSize());
    collector.addStat(Key::rollback_item_count,
                      active.getRollbackItemCount() +
                              replica.getRollbackItemCount() +
                              pending.getRollbackItemCount());
    collector.addStat(Key::ep_num_non_resident,
                      active.getNonResident() + pending.getNonResident() +
                              replica.getNonResident());
    collector.addStat(Key::ep_chk_persistence_remains,
                      active.getChkPersistRemaining() +
                              pending.getChkPersistRemaining() +
                              replica.getChkPersistRemaining());

    // Add stats for tracking HLC drift
    for (const auto& visitor : {active, replica}) {
        auto state = VBucket::toString(visitor.getVBucketState());
        auto stateCol = collector.withLabels({{"state", state}});
        stateCol.addStat(Key::ep_hlc_drift,
                         visitor.getTotalAbsHLCDrift().total);
        stateCol.addStat(Key::ep_hlc_drift_count,
                         visitor.getTotalAbsHLCDrift().updates);

        stateCol.addStat(Key::ep_ahead_exceptions,
                         visitor.getTotalHLCDriftExceptionCounters().ahead);
        stateCol.addStat(Key::ep_behind_exceptions,
                         visitor.getTotalHLCDriftExceptionCounters().behind);
    }

    // A single total for ahead exceptions accross all active/replicas
    collector.addStat(
            Key::ep_clock_cas_drift_threshold_exceeded,
            active.getTotalHLCDriftExceptionCounters().ahead +
                    replica.getTotalHLCDriftExceptionCounters().ahead);
}

void KVBucket::appendDatatypeStats(const DatatypeStatVisitor& active,
                                   const DatatypeStatVisitor& replica,
                                   const BucketStatCollector& collector) {
    using namespace cb::stats;

    for (uint8_t ii = 0; ii < active.getNumDatatypes(); ++ii) {
        auto datatypeStr = mcbp::datatype::to_string(ii);
        auto labelled = collector.withLabels(
                {{"datatype", datatypeStr}, {"vbucket_state", "active"}});

        labelled.addStat(Key::datatype_count, active.getDatatypeCount(ii));
    }

    for (uint8_t ii = 0; ii < replica.getNumDatatypes(); ++ii) {
        auto datatypeStr = mcbp::datatype::to_string(ii);
        auto labelled = collector.withLabels(
                {{"datatype", datatypeStr}, {"vbucket_state", "replica"}});

        labelled.addStat(Key::datatype_count, replica.getDatatypeCount(ii));
    }
}

void KVBucket::completeBGFetchMulti(
        Vbid vbId,
        std::vector<bgfetched_item_t>& fetchedItems,
        std::chrono::steady_clock::time_point startTime) {
    VBucketPtr vb = getVBucket(vbId);
    if (vb) {
        for (const auto& item : fetchedItems) {
            auto& key = item.first;
            item.second->complete(engine, vb, startTime, key);
        }
        EP_LOG_DEBUG(
                "EP Store completes {} of batched background fetch "
                "for {} endTime = {}",
                uint64_t(fetchedItems.size()),
                vbId,
                std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count());
    } else {
        std::map<const CookieIface*, cb::engine_errc> toNotify;
        for (const auto& item : fetchedItems) {
            item.second->abort(
                    engine, cb::engine_errc::not_my_vbucket, toNotify);
        }
        for (auto& notify : toNotify) {
            engine.notifyIOComplete(notify.first, notify.second);
        }
        EP_LOG_WARN(
                "EP Store completes {} of batched background fetch for "
                "for {} that is already deleted",
                (int)fetchedItems.size(),
                vbId);
    }
}

GetValue KVBucket::getInternal(const DocKey& key,
                               Vbid vbucket,
                               const CookieIface* cookie,
                               const ForGetReplicaOp getReplicaItem,
                               get_options_t options) {
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    }

    const bool honorStates = (options & HONOR_STATES);

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (honorStates) {
        vbucket_state_t disallowedState =
                (getReplicaItem == ForGetReplicaOp::Yes)
                        ? vbucket_state_active
                        : vbucket_state_replica;
        vbucket_state_t vbState = vb->getState();
        if (vbState == vbucket_state_dead) {
            ++stats.numNotMyVBuckets;
            return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
        } else if (vbState == disallowedState) {
            ++stats.numNotMyVBuckets;
            return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
        } else if (vbState == vbucket_state_pending) {
            /*
             * If the vbucket is in a pending state and
             * we are performing a getReplica then instead of adding the
             * operation to the pendingOps list return
             * cb::engine_errc::not_my_vbucket.
             */
            if (getReplicaItem == ForGetReplicaOp::Yes) {
                ++stats.numNotMyVBuckets;
                return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
            }
            if (vb->addPendingOp(cookie)) {
                if (options & TRACK_STATISTICS) {
                    vb->opsGet++;
                }
                return GetValue(nullptr, cb::engine_errc::would_block);
            }
        }
    }

    { // hold collections read handle for duration of get
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(cookie,
                                                    cHandle.getManifestUid());
            return GetValue(nullptr, cb::engine_errc::unknown_collection);
        }

        auto result = vb->getInternal(cookie,
                                      engine,
                                      options,
                                      VBucket::GetKeyOnly::No,
                                      cHandle,
                                      getReplicaItem);

        if (result.getStatus() != cb::engine_errc::would_block) {
            cHandle.incrementOpsGet();
        }
        return result;
    }
}

GetValue KVBucket::getRandomKey(CollectionID cid, const CookieIface* cookie) {
    size_t max = vbMap.getSize();
    const Vbid::id_type start = labs(getRandom()) % max;
    Vbid::id_type curr = start;
    std::unique_ptr<Item> itm;

    while (itm == nullptr) {
        VBucketPtr vb = getVBucket(Vbid(curr++));
        if (vb) {
            folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
            if (vb->getState() == vbucket_state_active) {
                auto cHandle = vb->lockCollections();
                if (!cHandle.exists(cid)) {
                    engine.setUnknownCollectionErrorContext(
                            cookie, cHandle.getManifestUid());
                    return GetValue(nullptr,
                                    cb::engine_errc::unknown_collection);
                }
                if (cHandle.getItemCount(cid) != 0) {
                    if (auto retItm = vb->ht.getRandomKey(cid, getRandom());
                        retItm) {
                        return GetValue(std::move(retItm),
                                        cb::engine_errc::success);
                    }
                }
            }
        }

        if (curr == max) {
            curr = 0;
        }
        if (curr == start) {
            break;
        }
        // Search next vbucket
    }

    return GetValue(nullptr, cb::engine_errc::no_such_key);
}

cb::engine_errc KVBucket::getMetaData(const DocKey& key,
                                      Vbid vbucket,
                                      const CookieIface* cookie,
                                      ItemMetaData& metadata,
                                      uint32_t& deleted,
                                      uint8_t& datatype) {
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    { // collections read scope
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(cookie,
                                                    cHandle.getManifestUid());
            return cb::engine_errc::unknown_collection;
        }

        return vb->getMetaData(
                cookie, engine, cHandle, metadata, deleted, datatype);
    }
}

cb::engine_errc KVBucket::setWithMeta(Item& itm,
                                      uint64_t cas,
                                      uint64_t* seqno,
                                      const CookieIface* cookie,
                                      PermittedVBStates permittedVBStates,
                                      CheckConflicts checkConflicts,
                                      bool allowExisting,
                                      GenerateBySeqno genBySeqno,
                                      GenerateCas genCas,
                                      ExtendedMetaData* emd) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        if (vb->getState() == vbucket_state_pending) {
            if (vb->addPendingOp(cookie)) {
                return cb::engine_errc::would_block;
            }
        } else {
            ++stats.numNotMyVBuckets;
            return cb::engine_errc::not_my_vbucket;
        }
    } else if (vb->isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a setWithMeta op"
                ", becuase takeover is lagging",
                vb->getId());
        return cb::engine_errc::temporary_failure;
    }

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return cb::engine_errc::key_already_exists;
    }

    cb::engine_errc rv = cb::engine_errc::success;
    { // hold collections read lock for duration of set

        auto cHandle = vb->lockCollections(itm.getKey());
        rv = cHandle.handleWriteStatus(
                engine, cookie, vb->getState(), itm.getNBytes());

        if (rv == cb::engine_errc::success) {
            cHandle.processExpiryTime(itm, getMaxTtl());
            rv = vb->setWithMeta(itm,
                                 cas,
                                 seqno,
                                 cookie,
                                 engine,
                                 checkConflicts,
                                 allowExisting,
                                 genBySeqno,
                                 genCas,
                                 cHandle);
        }
    }

    if (rv == cb::engine_errc::success) {
        checkAndMaybeFreeMemory();
    }
    return rv;
}

cb::engine_errc KVBucket::prepare(Item& itm, const CookieIface* cookie) {
    VBucketPtr vb = getVBucket(itm.getVBucketId());
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    PermittedVBStates permittedVBStates = {vbucket_state_replica,
                                           vbucket_state_pending};
    if (!permittedVBStates.test(vb->getState())) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    // check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return cb::engine_errc::key_already_exists;
    }

    cb::engine_errc rv = cb::engine_errc::success;
    { // hold collections read lock for duration of prepare

        auto cHandle = vb->lockCollections(itm.getKey());
        rv = cHandle.handleWriteStatus(engine, cookie, itm.getNBytes());
        if (rv == cb::engine_errc::success) {
            cHandle.processExpiryTime(itm, getMaxTtl());
            rv = vb->prepare(itm,
                             0,
                             nullptr,
                             cookie,
                             engine,
                             CheckConflicts::No,
                             true /*allowExisting*/,
                             GenerateBySeqno::No,
                             GenerateCas::No,
                             cHandle);
        }
    }

    if (rv == cb::engine_errc::success) {
        checkAndMaybeFreeMemory();
    }
    return rv;
}

GetValue KVBucket::getAndUpdateTtl(const DocKey& key,
                                   Vbid vbucket,
                                   const CookieIface* cookie,
                                   time_t exptime) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(nullptr, cb::engine_errc::would_block);
        }
    }

    { // collections read scope
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(cookie,
                                                    cHandle.getManifestUid());
            return GetValue(nullptr, cb::engine_errc::unknown_collection);
        }

        auto result = vb->getAndUpdateTtl(
                cookie,
                engine,
                cHandle.processExpiryTime(exptime, getMaxTtl()),
                cHandle);

        if (result.getStatus() == cb::engine_errc::success) {
            cHandle.incrementOpsStore();
            cHandle.incrementOpsGet();
        }
        return result;
    }
}

GetValue KVBucket::getLocked(const DocKey& key,
                             Vbid vbucket,
                             rel_time_t currentTime,
                             uint32_t lockTimeout,
                             const CookieIface* cookie) {
    auto vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    }

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(cookie,
                                                cHandle.getManifestUid());
        return GetValue(nullptr, cb::engine_errc::unknown_collection);
    }

    auto result =
            vb->getLocked(currentTime, lockTimeout, cookie, engine, cHandle);
    if (result.getStatus() == cb::engine_errc::success) {
        cHandle.incrementOpsGet();
    }
    return result;
}

cb::engine_errc KVBucket::unlockKey(const DocKey& key,
                                    Vbid vbucket,
                                    uint64_t cas,
                                    rel_time_t currentTime,
                                    const CookieIface* cookie) {
    auto vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(cookie,
                                                cHandle.getManifestUid());
        return cb::engine_errc::unknown_collection;
    }

    auto res = vb->fetchValueForWrite(cHandle);
    switch (res.status) {
    case VBucket::FetchForWriteResult::Status::OkFound: {
        auto* v = res.storedValue;
        if (VBucket::isLogicallyNonExistent(*v, cHandle)) {
            vb->ht.cleanupIfTemporaryItem(res.lock, *v);
            return cb::engine_errc::no_such_key;
        }
        if (v->isLocked(currentTime)) {
            if (v->getCas() == cas) {
                v->unlock();
                return cb::engine_errc::success;
            }
            return cb::engine_errc::locked_tmpfail;
        }
        return cb::engine_errc::temporary_failure;
    }
    case VBucket::FetchForWriteResult::Status::OkVacant:
        if (eviction_policy == EvictionPolicy::Value) {
            return cb::engine_errc::no_such_key;
        } else {
            // With the full eviction, an item's lock is automatically
            // released when the item is evicted from memory. Therefore,
            // we simply return cb::engine_errc::temporary_failure when we
            // receive unlockKey for an item that is not in memocy cache. Note
            // that we don't spawn any bg fetch job to figure out if an item
            // actually exists in disk or not.
            return cb::engine_errc::temporary_failure;
        }

    case VBucket::FetchForWriteResult::Status::ESyncWriteInProgress:
        return cb::engine_errc::sync_write_in_progress;
    }
    folly::assume_unreachable();
}

cb::engine_errc KVBucket::getKeyStats(const DocKey& key,
                                      Vbid vbucket,
                                      const CookieIface* cookie,
                                      struct key_stats& kstats,
                                      WantsDeleted wantsDeleted) {
    auto vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(cookie,
                                                cHandle.getManifestUid());
        return cb::engine_errc::unknown_collection;
    }

    return vb->getKeyStats(cookie, engine, kstats, wantsDeleted, cHandle);
}

std::string KVBucket::validateKey(const DocKey& key,
                                  Vbid vbucket,
                                  Item& diskItem) {
    VBucketPtr vb = getVBucket(vbucket);

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        return "collection_unknown";
    }

    auto res =
            vb->fetchValidValue(WantsDeleted::Yes, TrackReference::No, cHandle);
    auto* v = res.storedValue;
    if (v) {
        if (VBucket::isLogicallyNonExistent(*v, cHandle)) {
            vb->ht.cleanupIfTemporaryItem(res.lock, *v);
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

cb::engine_errc KVBucket::deleteItem(
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        const CookieIface* cookie,
        std::optional<cb::durability::Requirements> durability,
        ItemMetaData* itemMeta,
        mutation_descr_t& mutInfo) {
    auto vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return cb::engine_errc::would_block;
        }
    } else if (vb->isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a delete op"
                ", becuase takeover is lagging",
                vb->getId());
        return cb::engine_errc::temporary_failure;
    }

    // Yield if checkpoint's full - The call also wakes up the mem recovery task
    if (verifyCheckpointMemoryState() == CheckpointMemoryState::Full) {
        return cb::engine_errc::temporary_failure;
    }

    cb::engine_errc result;
    { // collections read scope
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(cookie,
                                                    cHandle.getManifestUid());
            return cb::engine_errc::unknown_collection;
        }

        result = vb->deleteItem(
                cas, cookie, engine, durability, itemMeta, mutInfo, cHandle);
    }

    if (durability) {
        vb->notifyActiveDMOfLocalSyncWrite();
    }

    return result;
}

cb::engine_errc KVBucket::deleteWithMeta(const DocKey& key,
                                         uint64_t& cas,
                                         uint64_t* seqno,
                                         Vbid vbucket,
                                         const CookieIface* cookie,
                                         PermittedVBStates permittedVBStates,
                                         CheckConflicts checkConflicts,
                                         const ItemMetaData& itemMeta,
                                         GenerateBySeqno genBySeqno,
                                         GenerateCas generateCas,
                                         uint64_t bySeqno,
                                         ExtendedMetaData* emd,
                                         DeleteSource deleteSource) {
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        if (vb->getState() == vbucket_state_pending) {
            if (vb->addPendingOp(cookie)) {
                return cb::engine_errc::would_block;
            }
        } else {
            ++stats.numNotMyVBuckets;
            return cb::engine_errc::not_my_vbucket;
        }
    } else if (vb->isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a deleteWithMeta op"
                ", becuase takeover is lagging",
                vb->getId());
        return cb::engine_errc::temporary_failure;
    }

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itemMeta.cas)) {
        return cb::engine_errc::key_already_exists;
    }

    { // hold collections read lock for duration of delete
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(cookie,
                                                    cHandle.getManifestUid());
            return cb::engine_errc::unknown_collection;
        }

        return vb->deleteWithMeta(cas,
                                  seqno,
                                  cookie,
                                  engine,
                                  checkConflicts,
                                  itemMeta,
                                  genBySeqno,
                                  generateCas,
                                  bySeqno,
                                  cHandle,
                                  deleteSource);
    }
}

void KVBucket::reset() {
    auto buckets = vbMap.getBuckets();
    for (auto vbid : buckets) {
        auto vb = getLockedVBucket(vbid);
        if (vb) {
            vb->ht.clear();
            vb->checkpointManager->clear();
            vb->resetStats();
            vb->setPersistedSnapshot({0, 0});
            EP_LOG_INFO("KVBucket::reset(): Successfully flushed {}", vbid);
        }
    }
    EP_LOG_INFO_RAW("KVBucket::reset(): Successfully flushed bucket");
}

bool KVBucket::isWarmupLoadingData() {
    return false;
}

bool KVBucket::isWarmupComplete() {
    return false;
}

bool KVBucket::isWarmupOOMFailure() {
    return false;
}

bool KVBucket::hasWarmupSetVbucketStateFailed() const {
    return false;
}

bool KVBucket::maybeWaitForVBucketWarmup(const CookieIface* cookie) {
    return false;
}

bool KVBucket::isMemUsageAboveBackfillThreshold() {
    auto memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    auto maxSize = static_cast<double>(stats.getMaxDataSize());
    return memoryUsed > (maxSize * backfillMemoryThreshold);
}

// Trigger memory reduction (ItemPager) if we've exceeded the pageable high
// watermark.
void KVBucket::checkAndMaybeFreeMemory() {
    if (getPageableMemCurrent() > getPageableMemHighWatermark()) {
        attemptToFreeMemory();
    }
}

void KVBucket::setBackfillMemoryThreshold(double threshold) {
    backfillMemoryThreshold = threshold;
}

void KVBucket::setExpiryPagerSleeptime(size_t val) {
    expiryPagerTask->updateSleepTime(std::chrono::seconds(val));
    if (!expiryPagerTask->isEnabled()) {
        EP_LOG_DEBUG(
                "Expiry pager disabled, "
                "enabling it will make exp_pager_stime ({})"
                "to go into effect!",
                val);
    }
}

void KVBucket::setExpiryPagerTasktime(ssize_t val) {
    expiryPagerTask->updateInitialRunTime(val);
    if (!expiryPagerTask->isEnabled()) {
        EP_LOG_DEBUG(
                "Expiry pager disabled, "
                "enabling it will make exp_pager_initial_run_time ({})"
                "to go into effect!",
                val);
    }
}

void KVBucket::enableExpiryPager() {
    if (!expiryPagerTask->enable()) {
        EP_LOG_DEBUG_RAW("Expiry Pager already enabled!");
    }
}

void KVBucket::disableExpiryPager() {
    if (!expiryPagerTask->disable()) {
        EP_LOG_DEBUG_RAW("Expiry Pager already disabled!");
    }
}

void KVBucket::wakeUpExpiryPager() {
    if (expiryPagerTask->isEnabled()) {
        ExecutorPool::get()->wake(expiryPagerTask->getId());
    }
}

void KVBucket::wakeItemPager() {
    if (itemPagerTask->getState() == TASK_SNOOZED) {
        ExecutorPool::get()->wake(itemPagerTask->getId());
    }
}

void KVBucket::enableItemPager() {
    ExecutorPool::get()->cancel(itemPagerTask->getId());
    ExecutorPool::get()->schedule(itemPagerTask);
}

void KVBucket::disableItemPager() {
    ExecutorPool::get()->cancel(itemPagerTask->getId());
}

void KVBucket::wakeItemFreqDecayerTask() {
    auto& t = dynamic_cast<ItemFreqDecayerTask&>(*itemFreqDecayerTask);
    t.wakeup();
}

void KVBucket::enableAccessScannerTask() {
    std::lock_guard<std::mutex> lh(accessScanner.mutex);
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
            EP_LOG_INFO_RAW(
                    "Did not enable access scanner task, "
                    "as alog_sleep_time is set to zero!");
        }
    } else {
        EP_LOG_DEBUG_RAW("Access scanner already enabled!");
    }
}

void KVBucket::disableAccessScannerTask() {
    std::lock_guard<std::mutex> lh(accessScanner.mutex);
    if (accessScanner.enabled) {
        ExecutorPool::get()->cancel(accessScanner.task);
        accessScanner.sleeptime = 0;
        accessScanner.enabled = false;
    } else {
        EP_LOG_DEBUG_RAW("Access scanner already disabled!");
    }
}

void KVBucket::setAccessScannerSleeptime(size_t val, bool useStartTime) {
    std::lock_guard<std::mutex> lh(accessScanner.mutex);

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
    std::lock_guard<std::mutex> lh(accessScanner.mutex);

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

void KVBucket::enableItemCompressor() {
    itemCompressorTask = std::make_shared<ItemCompressorTask>(&engine, stats);
    ExecutorPool::get()->schedule(itemCompressorTask);
}

void KVBucket::setAllBloomFilters(bool to) {
    for (auto vbid : vbMap.getBuckets()) {
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

void KVBucket::visit(VBucketVisitor& visitor) {
    for (auto vbid : vbMap.getBuckets()) {
        if (visitor.getVBucketFilter()(Vbid(vbid))) {
            VBucketPtr vb = vbMap.getBucket(vbid);
            if (vb) {
                visitor.visitBucket(*vb);
            }
        }
    }
}

size_t KVBucket::visitAsync(
        std::unique_ptr<InterruptableVBucketVisitor> visitor,
        const char* lbl,
        TaskId id,
        std::chrono::microseconds maxExpectedDuration) {
    auto task = std::make_shared<VBCBAdaptor>(this,
                                              id,
                                              std::move(visitor),
                                              lbl,
                                              /*shutdown*/ false);
    task->setMaxExpectedDuration(maxExpectedDuration);
    return ExecutorPool::get()->schedule(task);
}

KVBucket::Position KVBucket::pauseResumeVisit(PauseResumeVBVisitor& visitor,
                                              Position& start_pos,
                                              VBucketFilter* filter) {
    Vbid vbid = start_pos.vbucket_id;
    for (; vbid.get() < vbMap.getSize(); ++vbid) {
        if (!filter || (*filter)(vbid)) {
            VBucketPtr vb = vbMap.getBucket(vbid);
            if (vb) {
                bool paused = !visitor.visit(*vb);
                if (paused) {
                    break;
                }
            }
        }
    }

    return KVBucket::Position(vbid);
}

KVBucket::Position KVBucket::startPosition() const
{
    return KVBucket::Position(Vbid(0));
}

KVBucket::Position KVBucket::endPosition() const
{
    return KVBucket::Position(Vbid(vbMap.getSize()));
}

VBCBAdaptor::VBCBAdaptor(KVBucket* s,
                         TaskId id,
                         std::unique_ptr<InterruptableVBucketVisitor> v,
                         const char* l,
                         bool shutdown)
    : GlobalTask(&s->getEPEngine(), id, 0 /*initialSleepTime*/, shutdown),
      store(s),
      visitor(std::move(v)),
      label(l),
      maxDuration(std::chrono::microseconds::max()) {
    // populate the list of vbuckets to visit, and order them as needed by
    // the visitor.
    const auto numVbs = store->getVBuckets().getSize();

    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        if (visitor->getVBucketFilter()(Vbid(vbid))) {
            vbucketsToVisit.emplace_back(vbid);
        }
    }
    std::sort(vbucketsToVisit.begin(),
              vbucketsToVisit.end(),
              visitor->getVBucketComparator());
}

std::string VBCBAdaptor::getDescription() const {
    auto value = currentvb.load();
    if (value == None) {
        return std::string(label) + " no vbucket assigned";
    } else {
        return std::string(label) + " on " + Vbid(value).to_string();
    }
}

bool VBCBAdaptor::run() {
    visitor->begin();

    while (!vbucketsToVisit.empty()) {
        const auto vbid = vbucketsToVisit.front();
        VBucketPtr vb = store->getVBucket(vbid);
        if (vb) {
            currentvb = vbid.get();

            using State = InterruptableVBucketVisitor::ExecutionState;
            switch (visitor->shouldInterrupt()) {
            case State::Continue:
                break;
            case State::Pause:
                snooze(0);
                return true;
            case State::Stop:
                visitor->complete();
                return false;
            }

            visitor->visitBucket(*vb);
        }
        vbucketsToVisit.pop_front();
    }

    // Processed all vBuckets now, do not need to run again.
    visitor->complete();
    return false;
}

void KVBucket::resetUnderlyingStats()
{
    for (auto& i : vbMap.shards) {
        KVShard* shard = i.get();
        shard->getRWUnderlying()->resetStats();
    }

    for (size_t i = 0; i < GlobalTask::allTaskIds.size(); i++) {
        stats.schedulingHisto[i].reset();
        stats.taskRuntimeHisto[i].reset();
    }
}

void KVBucket::addKVStoreStats(const AddStatFn& add_stat,
                               const CookieIface* cookie) {
    for (const auto& shard : vbMap.shards) {
        /* Add the different KVStore instances into a set and then
         * retrieve the stats from each instance separately. This
         * is because CouchKVStore has separate read only and read
         * write instance whereas RocksDBKVStore has only instance
         * for both read write and read-only.
         */
        std::set<const KVStoreIface*> underlyingSet;
        underlyingSet.insert(shard->getRWUnderlying());

        for (auto* store : underlyingSet) {
            store->addStats(add_stat, cookie);
        }
    }
}

void KVBucket::addKVStoreTimingStats(const AddStatFn& add_stat,
                                     const CookieIface* cookie) {
    for (const auto& shard : vbMap.shards) {
        std::set<const KVStoreIface*> underlyingSet;
        underlyingSet.insert(shard->getRWUnderlying());

        for (auto* store : underlyingSet) {
            store->addTimingStats(add_stat, cookie);
        }
    }
}

bool KVBucket::getKVStoreStat(std::string_view name, size_t& value) {
    std::array<std::string_view, 1> keys = {{name}};
    auto kvStats = getKVStoreStats(keys);
    auto stat = kvStats.find(name);
    if (stat != kvStats.end()) {
        value = stat->second;
        return true;
    }
    return false;
}

GetStatsMap KVBucket::getKVStoreStats(gsl::span<const std::string_view> keys) {
    GetStatsMap statsMap;
    auto aggShardStats = [&](const KVStoreIface* store) {
        auto shardStats = store->getStats(keys);
        for (const auto& [name, value] : shardStats) {
            auto [itr, emplaced] = statsMap.try_emplace(name, value);
            if (!emplaced) {
                itr->second += value;
            }
        }
    };
    for (const auto& shard : vbMap.shards) {
        aggShardStats(shard->getRWUnderlying());
    }
    return statsMap;
}

const KVStoreIface* KVBucket::getOneROUnderlying() const {
    return vbMap.shards[EP_PRIMARY_SHARD]->getROUnderlying();
}

KVStoreIface* KVBucket::getOneRWUnderlying() {
    return vbMap.shards[EP_PRIMARY_SHARD]->getRWUnderlying();
}

TaskStatus KVBucket::rollback(Vbid vbid, uint64_t rollbackSeqno) {
    std::unique_lock<std::mutex> vbset(vbsetMutex);

    auto vb = getLockedVBucket(vbid, std::try_to_lock);

    if (!vb.owns_lock()) {
        return TaskStatus::Reschedule; // Reschedule a vbucket rollback task.
    }

    if (!vb.getVB()) {
        EP_LOG_WARN("{} Aborting rollback as the vbucket was not found", vbid);
        return TaskStatus::Abort;
    }

    auto ctx = prepareToRollback(vbid);

    // Acquire the vb stateLock in exclusive mode as we will recreate the
    // DurabilityMonitor in the vBucket as part of rollback and this could race
    // with stats calls.
    folly::SharedMutex::WriteHolder wlh(vb->getStateLock());
    if ((vb->getState() == vbucket_state_replica) ||
        (vb->getState() == vbucket_state_pending)) {
        auto prevHighSeqno =
                static_cast<uint64_t>(vb->checkpointManager->getHighSeqno());
        if (rollbackSeqno != 0) {
            RollbackResult result = doRollback(vbid, rollbackSeqno);
            if (result.success) {
                if (result.highSeqno > 0) {
                    rollbackUnpersistedItems(*vb, result.highSeqno);
                    const auto loadResult = loadPreparedSyncWrites(*vb);
                    if (loadResult.success) {
                        auto& epVb = static_cast<EPVBucket&>(*vb.getVB());
                        epVb.postProcessRollback(result, prevHighSeqno, *this);
                        engine.getDcpConnMap().closeStreamsDueToRollback(vbid);
                        return TaskStatus::Complete;
                    }
                    EP_LOG_WARN(
                            "{} KVBucket::rollback(): loadPreparedSyncWrites() "
                            "failed to scan for prepares, resetting vbucket",
                            vbid);
                }
                // if 0, reset vbucket for a clean start instead of deleting
                // everything in it
            } else {
                // not success hence reset vbucket to avoid data loss
                EP_LOG_WARN(
                        "{} KVBucket::rollback(): on disk rollback failed, "
                        "resetting vbucket",
                        vbid);
            }
        }

        if (resetVBucket_UNLOCKED(vb, vbset)) {
            VBucketPtr newVb = vbMap.getBucket(vbid);
            newVb->incrRollbackItemCount(prevHighSeqno);
            engine.getDcpConnMap().closeStreamsDueToRollback(vbid);
            return TaskStatus::Complete;
        }
        EP_LOG_WARN("{} Aborting rollback as reset of the vbucket failed",
                    vbid);
        return TaskStatus::Abort;
    } else {
        EP_LOG_WARN("{} Rollback not supported on the vbucket state {}",
                    vbid,
                    VBucket::toString(vb->getState()));
        return TaskStatus::Abort;
    }
}

void KVBucket::attemptToFreeMemory() {
    static_cast<ItemPager*>(itemPagerTask.get())->scheduleNow();
}

void KVBucket::wakeUpCheckpointMemRecoveryTask() {
    for (const auto& task : chkRemovers) {
        if (task) {
            task->wakeup();
        }
    }
}

void KVBucket::runDefragmenterTask() {
    defragmenterTask->execute("");
}

void KVBucket::runItemFreqDecayerTask() {
    itemFreqDecayerTask->execute("");
}

bool KVBucket::runAccessScannerTask() {
    return ExecutorPool::get()->wakeAndWait(accessScanner.task);
}

void KVBucket::runVbStatePersistTask(Vbid vbid) {
    scheduleVBStatePersist(vbid);
}

size_t KVBucket::getActiveResidentRatio() const {
    return cachedResidentRatio.activeRatio.load();
}

size_t KVBucket::getReplicaResidentRatio() const {
    return cachedResidentRatio.replicaRatio.load();
}

cb::engine_errc KVBucket::forceMaxCas(Vbid vbucket, uint64_t cas) {
    VBucketPtr vb = vbMap.getBucket(vbucket);
    if (vb) {
        vb->forceMaxCas(cas);
        return cb::engine_errc::success;
    }
    return cb::engine_errc::not_my_vbucket;
}

std::ostream& operator<<(std::ostream& os, const KVBucket::Position& pos) {
    os << pos.vbucket_id;
    return os;
}

void KVBucket::notifyFlusher(const Vbid vbid) {
    auto vb = getVBucket(vbid);
    // Can't assert vb here as this may be called from Warmup before we add the
    // VBucket to the VBucketMap
    if (vb) {
        auto* flusher = vb->getFlusher();
        // Can't assert flusher here as this may be called for ephemeral
        // VBuckets which do not have a flusher
        if (flusher) {
            flusher->notifyFlushEvent(vb);
        }
    }
}

void KVBucket::notifyReplication(const Vbid vbid,
                                 const int64_t bySeqno,
                                 SyncWriteOperation syncWrite) {
    engine.getDcpConnMap().notifyVBConnections(vbid, bySeqno, syncWrite);
}

void KVBucket::initializeExpiryPager(Configuration& config) {
    expiryPagerTask = std::make_shared<ExpiredItemPager>(
            &engine,
            stats,
            config.getExpPagerStime(),
            config.getExpPagerInitialRunTime(),
            config.getExpiryPagerConcurrency());

    if (config.isExpPagerEnabled()) {
        enableExpiryPager();
    }

    config.addValueChangedListener(
            "exp_pager_stime",
            std::make_unique<EPStoreValueChangeListener>(*this));
    config.addValueChangedListener(
            "exp_pager_enabled",
            std::make_unique<EPStoreValueChangeListener>(*this));
    config.addValueChangedListener(
            "exp_pager_initial_run_time",
            std::make_unique<EPStoreValueChangeListener>(*this));
}

cb::engine_error KVBucket::setCollections(std::string_view manifest,
                                          const CookieIface* cookie) {
    // Only allow a new manifest once warmup has progressed past vbucket warmup
    // 1) This means any prior manifest has been loaded
    // 2) All vbuckets can have the new manifest applied
    if (cookie && maybeWaitForVBucketWarmup(cookie)) {
        EP_LOG_INFO("KVBucket::setCollections blocking for warmup cookie:{}",
                    static_cast<const void*>(cookie));
        return cb::engine_error(cb::engine_errc::would_block,
                                "KVBucket::setCollections waiting for warmup");
    }

    // Inhibit VB state changes whilst updating the vbuckets
    std::lock_guard<std::mutex> lh(vbsetMutex);

    auto status = collectionsManager->update(*this, manifest, cookie);
    if (status.code() != cb::engine_errc::success &&
        status.code() != cb::engine_errc::would_block) {
        EP_LOG_WARN("KVBucket::setCollections error:{} {}",
                    status.code(),
                    status.what());
    }
    return status;
}

std::pair<cb::mcbp::Status, nlohmann::json> KVBucket::getCollections(
        const Collections::IsVisibleFunction& isVisible) const {
    return collectionsManager->getManifest(isVisible);
}

cb::EngineErrorGetCollectionIDResult KVBucket::getCollectionID(
        std::string_view path) const {
    try {
        return collectionsManager->getCollectionID(path);
    } catch (const cb::engine_error& e) {
        return cb::EngineErrorGetCollectionIDResult{
                cb::engine_errc(e.code().value())};
    }
}

cb::EngineErrorGetScopeIDResult KVBucket::getScopeID(
        std::string_view path) const {
    try {
        return collectionsManager->getScopeID(path);
    } catch (const cb::engine_error& e) {
        return cb::EngineErrorGetScopeIDResult{
                cb::engine_errc(e.code().value())};
    }
}

std::pair<uint64_t, std::optional<ScopeID>> KVBucket::getScopeID(
        CollectionID cid) const {
    return collectionsManager->getScopeID(cid);
}

const Collections::Manager& KVBucket::getCollectionsManager() const {
    return *collectionsManager;
}

Collections::Manager& KVBucket::getCollectionsManager() {
    return *collectionsManager;
}

const std::shared_ptr<Collections::Manager>&
KVBucket::getSharedCollectionsManager() const {
    return collectionsManager;
}

bool KVBucket::isXattrEnabled() const {
    return xattrEnabled;
}

void KVBucket::setXattrEnabled(bool value) {
    xattrEnabled = value;
}

std::chrono::seconds KVBucket::getMaxTtl() const {
    return std::chrono::seconds{maxTtl.load()};
}

void KVBucket::setMaxTtl(size_t max) {
    maxTtl = max;
}

uint16_t KVBucket::getNumOfVBucketsInState(vbucket_state_t state) const {
    return vbMap.getVBStateCount(state);
}

size_t KVBucket::getMemFootPrint() {
    size_t mem = 0;
    for (auto& i : vbMap.shards) {
        KVShard* shard = i.get();
        mem += shard->getRWUnderlying()->getMemFootPrint();
    }
    return mem;
}

SyncWriteResolvedCallback KVBucket::makeSyncWriteResolvedCB() {
    return [this](Vbid vbid) {
        if (this->durabilityCompletionTask) {
            this->durabilityCompletionTask->notifySyncWritesToComplete(vbid);
        }
    };
}

SyncWriteCompleteCallback KVBucket::makeSyncWriteCompleteCB() {
    return [&engine = this->engine](const CookieIface* cookie,
                                    cb::engine_errc status) {
        if (status != cb::engine_errc::success) {
            // For non-success status codes clear the cookie's engine_specific;
            // as the operation is now complete. This ensures that any
            // subsequent call by the same cookie to store() is treated as a new
            // operation (and not the completion of the previous one).
            engine.storeEngineSpecific(cookie, nullptr);
        }
        engine.notifyIOComplete(cookie, status);
    };
}

SeqnoAckCallback KVBucket::makeSeqnoAckCB() const {
    return [&engine = this->engine](Vbid vbid, int64_t seqno) {
        engine.getDcpConnMap().seqnoAckVBPassiveStream(vbid, seqno);
    };
}

void KVBucket::scheduleDestruction(CheckpointList&& checkpoints, Vbid vbid) {
    getCkptDestroyerTask(vbid).queueForDestruction(std::move(checkpoints));
}

std::unique_ptr<KVStoreIface> KVBucket::takeRW(size_t shardId) {
    return vbMap.shards[shardId]->takeRW();
}

void KVBucket::setRW(size_t shardId, std::unique_ptr<KVStoreIface> rw) {
    vbMap.shards[shardId]->setRWUnderlying(std::move(rw));
}

cb::engine_errc KVBucket::setMinDurabilityLevel(cb::durability::Level level) {
    if (!isValidBucketDurabilityLevel(level)) {
        return cb::engine_errc::durability_invalid_level;
    }

    minDurabilityLevel = level;

    return cb::engine_errc::success;
}

cb::durability::Level KVBucket::getMinDurabilityLevel() const {
    return minDurabilityLevel;
}

KVShard::id_type KVBucket::getShardId(Vbid vbid) const {
    return vbMap.getShardByVbId(vbid)->getId();
}

cb::engine_errc KVBucket::setCheckpointMemoryRatio(float ratio) {
    if (ratio < 0.0 || ratio > 1.0) {
        EP_LOG_ERR("KVBucket::setCheckpointMemoryRatio: invalid argument {}",
                   ratio);
        return cb::engine_errc::invalid_arguments;
    }
    checkpointMemoryRatio = ratio;
    return cb::engine_errc::success;
}

float KVBucket::getCheckpointMemoryRatio() const {
    return checkpointMemoryRatio;
}

cb::engine_errc KVBucket::setCheckpointMemoryRecoveryUpperMark(float ratio) {
    if (ratio < 0.0 || ratio > 1.0) {
        EP_LOG_ERR(
                "KVBucket::setCheckpointMemoryRecoveryUpperMark: invalid "
                "argument {}",
                ratio);
        return cb::engine_errc::invalid_arguments;
    }
    if (ratio < checkpointMemoryRecoveryLowerMark) {
        EP_LOG_ERR(
                "KVBucket::setCheckpointMemoryRecoveryUpperMark: invalid "
                "argument {}, lower than lower-mark {}",
                ratio,
                checkpointMemoryRecoveryLowerMark);
        return cb::engine_errc::invalid_arguments;
    }

    checkpointMemoryRecoveryUpperMark = ratio;
    return cb::engine_errc::success;
}

float KVBucket::getCheckpointMemoryRecoveryUpperMark() const {
    return checkpointMemoryRecoveryUpperMark;
}

cb::engine_errc KVBucket::setCheckpointMemoryRecoveryLowerMark(float ratio) {
    if (ratio < 0.0 || ratio > 1.0) {
        EP_LOG_ERR(
                "KVBucket::setCheckpointMemoryRecoveryLowerMark: invalid "
                "argument {}",
                ratio);
        return cb::engine_errc::invalid_arguments;
    }
    if (ratio > checkpointMemoryRecoveryUpperMark) {
        EP_LOG_ERR(
                "KVBucket::setCheckpointMemoryRecoveryLowerMark: invalid "
                "argument {}, greater than upper-mark {}",
                ratio,
                checkpointMemoryRecoveryUpperMark);
        return cb::engine_errc::invalid_arguments;
    }

    checkpointMemoryRecoveryLowerMark = ratio;
    return cb::engine_errc::success;
}

float KVBucket::getCheckpointMemoryRecoveryLowerMark() const {
    return checkpointMemoryRecoveryLowerMark;
}

cb::engine_errc KVBucket::setCheckpointMaxSize(size_t size) {
    if (size == 0) {
        return autoConfigCheckpointMaxSize();
    }

    engine.getCheckpointConfig().setCheckpointMaxSize(size);
    return cb::engine_errc::success;
}

cb::engine_errc KVBucket::autoConfigCheckpointMaxSize() {
    // Note: We don't account dead vbuckets as that identifies VBucket objects
    // set-up for deferred deletion on disk but that have already been destroyed
    // in memory.
    const auto numVBuckets = vbMap.getNumAliveVBuckets();

    if (numVBuckets == 0) {
        // Temporary/benign state - before the first VBucket is created
        return cb::engine_errc::success;
    }

    const auto& config = engine.getConfiguration();
    const auto checkpointQuota = config.getMaxSize() * checkpointMemoryRatio;
    const auto numCheckpointsPerVB = config.getMaxCheckpoints();
    Expects(numCheckpointsPerVB > 0);
    engine.getCheckpointConfig().setCheckpointMaxSize(
            checkpointQuota / numVBuckets / numCheckpointsPerVB);

    return cb::engine_errc::success;
}

bool KVBucket::isCheckpointMaxSizeAutoConfig() const {
    return engine.getConfiguration().getCheckpointMaxSize() == 0;
}

size_t KVBucket::getVBMapSize() const {
    return vbMap.getSize();
}

KVBucket::CheckpointMemoryState KVBucket::getCheckpointMemoryState() const {
    const auto checkpointQuota = stats.getMaxDataSize() * checkpointMemoryRatio;
    const auto recoveryThreshold =
            checkpointQuota * checkpointMemoryRecoveryUpperMark;
    const auto usage = stats.getCheckpointManagerEstimatedMemUsage();

    if (usage < recoveryThreshold) {
        return CheckpointMemoryState::Available;
    } else if (usage < checkpointQuota) {
        return CheckpointMemoryState::NeedsRecovery;
    } else {
        return CheckpointMemoryState::Full;
    }

    folly::assume_unreachable();
}

KVBucket::CheckpointMemoryState KVBucket::verifyCheckpointMemoryState() {
    const auto state = getCheckpointMemoryState();
    switch (state) {
    case CheckpointMemoryState::Available:
        break;
    case CheckpointMemoryState::NeedsRecovery:
    case CheckpointMemoryState::Full:
        wakeUpCheckpointMemRecoveryTask();
        break;
    }

    return state;
}

size_t KVBucket::getCMQuota() const {
    return stats.getMaxDataSize() * checkpointMemoryRatio;
}

size_t KVBucket::getCMRecoveryUpperMarkBytes() const {
    return getCMQuota() * checkpointMemoryRecoveryUpperMark;
}

size_t KVBucket::getCMRecoveryLowerMarkBytes() const {
    return getCMQuota() * checkpointMemoryRecoveryLowerMark;
}

size_t KVBucket::getRequiredCheckpointMemoryReduction() const {
    const auto recoveryThreshold = getCMRecoveryUpperMarkBytes();
    const auto usage = stats.getCheckpointManagerEstimatedMemUsage();
    const auto recoveryTarget = getCMRecoveryLowerMarkBytes();

    if (usage <= recoveryTarget) {
        return 0;
    }

    const size_t amountOfMemoryToClear = usage - recoveryTarget;

    const auto toMB = [](size_t bytes) { return bytes / (1024 * 1024); };
    EP_LOG_DEBUG(
            "Triggering memory recovery as checkpoint memory usage ({} MB) "
            "exceeds the upper_mark ({}, "
            "{} MB) - total checkpoint quota {}, {} MB . Attempting to free {} "
            "MB of memory.",
            toMB(usage),
            checkpointMemoryRecoveryUpperMark,
            toMB(recoveryThreshold),
            checkpointMemoryRatio,
            toMB(getCMQuota()),
            toMB(amountOfMemoryToClear));

    return amountOfMemoryToClear;
}

bool KVBucket::isCheckpointMemoryReductionRequired() const {
    const auto recoveryThreshold = getCMRecoveryUpperMarkBytes();
    const auto usage = stats.getCheckpointManagerEstimatedMemUsage();

    return usage > recoveryThreshold;
}

CheckpointDestroyerTask& KVBucket::getCkptDestroyerTask(Vbid vbid) const {
    Expects(!ckptDestroyerTasks.empty());
    return *ckptDestroyerTasks[vbid.get() % ckptDestroyerTasks.size()];
}

size_t KVBucket::getCheckpointPendingDestructionMemoryUsage() const {
    size_t memoryUsage = 0;
    for (const auto& task : ckptDestroyerTasks) {
        memoryUsage += task->getMemoryUsage();
    }
    return memoryUsage;
}

size_t KVBucket::getNumCheckpointsPendingDestruction() const {
    size_t count = 0;
    for (const auto& task : ckptDestroyerTasks) {
        count += task->getNumCheckpoints();
    }
    return count;
}

void NotifyNewSeqnoCB::callback(const Vbid& vbid,
                                const VBNotifyCtx& notifyCtx) {
    kvBucket.notifyNewSeqno(vbid, notifyCtx);
}

void KVBucket::setSeqnoPersistenceTimeout(std::chrono::seconds timeout) {
    seqnoPersistenceTimeout = timeout;
}

std::chrono::seconds KVBucket::getSeqnoPersistenceTimeout() const {
    return seqnoPersistenceTimeout;
}

std::shared_ptr<SeqnoPersistenceNotifyTask>
KVBucket::createAndScheduleSeqnoPersistenceNotifier() {
    Expects(!seqnoPersistenceNotifyTask);
    seqnoPersistenceNotifyTask =
            std::make_shared<SeqnoPersistenceNotifyTask>(*this);
    ExecutorPool::get()->schedule(seqnoPersistenceNotifyTask);
    return seqnoPersistenceNotifyTask;
}

void KVBucket::addVbucketWithSeqnoPersistenceRequest(
        Vbid vbid, std::chrono::steady_clock::time_point deadline) {
    if (seqnoPersistenceNotifyTask) {
        seqnoPersistenceNotifyTask->addVbucket(vbid, deadline);
    }
}

std::chrono::steady_clock::time_point
KVBucket::getSeqnoPersistenceNotifyTaskWakeTime() const {
    // Added for unit-testing, so expect the task to exist
    Expects(seqnoPersistenceNotifyTask);
    return seqnoPersistenceNotifyTask->getWaketime();
}

std::pair<cb::engine_errc, cb::rangescan::Id> KVBucket::createRangeScan(
        Vbid,
        CollectionID,
        cb::rangescan::KeyView,
        cb::rangescan::KeyView,
        std::unique_ptr<RangeScanDataHandlerIFace>,
        const CookieIface&,
        cb::rangescan::KeyOnly,
        std::optional<cb::rangescan::SnapshotRequirements>,
        std::optional<cb::rangescan::SamplingConfiguration>) {
    return {cb::engine_errc::not_supported, {}};
}
cb::engine_errc KVBucket::continueRangeScan(Vbid,
                                            cb::rangescan::Id,
                                            const CookieIface&,
                                            size_t,
                                            std::chrono::milliseconds) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc KVBucket::cancelRangeScan(Vbid,
                                          cb::rangescan::Id,
                                          const CookieIface&) {
    return cb::engine_errc::not_supported;
}
