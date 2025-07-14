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
#include "bucket_quota_change_task.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "conflict_resolution.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "defragmenter.h"
#include "doc_pre_expiry.h"
#include "durability/durability_completion_task.h"
#include "durability_timeout_task.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "event_driven_timeout_task.h"
#include "ext_meta_parser.h"
#include "failover-table.h"
#include "flusher.h"
#include "htresizer.h"
#include "initial_mfu_task.h"
#include "item.h"
#include "item_compressor.h"
#include "item_freq_decayer.h"
#include "item_pager.h"
#include "kvshard.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "rollback_result.h"
#include "seqno_persistence_notify_task.h"
#include "tasks.h"
#include "trace_helpers.h"
#include "vb_adapters.h"
#include "vb_count_visitor.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucketdeletiontask.h"
#include <executor/executorpool.h>
#include <executor/notifiable_task.h>
#include <platform/json_log_conversions.h>

#include <folly/CancellationToken.h>
#include <folly/container/F14Map.h>
#include <memcached/collections.h>
#include <memcached/cookie_iface.h>
#include <memcached/document_expired.h>
#include <memcached/range_scan_optional_configuration.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/timeutils.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <utilities/math_utilities.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

class StatsValueChangeListener : public ValueChangedListener {
private:
    static constexpr const char* UnknownKeyMessage =
            "StatsValueChangeListener failed to change value for unknown "
            "variable";

public:
    StatsValueChangeListener(EPStats& st, KVBucket& str)
        : stats(st), store(str) {
        // EMPTY
    }

    void sizeValueChanged(std::string_view key, size_t value) override {
        EP_LOG_WARN_CTX(UnknownKeyMessage, {"type", "size_t"}, {"key", key});
    }

    void floatValueChanged(std::string_view key, float value) override {
        if (key == "mem_low_wat_percent") {
            stats.setLowWaterMarkPercent(value);
        } else if (key == "mem_high_wat_percent") {
            stats.setHighWaterMarkPercent(value);
        } else if (key == "mem_used_merge_threshold_percent") {
            store.getEPEngine()
                    .getArenaMallocClient()
                    .setEstimateUpdateThreshold(stats.getMaxDataSize(), value);
        } else {
            EP_LOG_WARN_CTX(UnknownKeyMessage, {"type", "float"}, {"key", key});
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

    void sizeValueChanged(std::string_view key, size_t value) override {
        if (key == "exp_pager_stime") {
            store.setExpiryPagerSleeptime(value);
        } else if (key == "backfill_mem_threshold") {
            double backfill_threshold = static_cast<double>(value) / 100;
            store.setBackfillMemoryThreshold(backfill_threshold);
        } else if (key == "max_ttl") {
            store.setMaxTtl(value);
        } else if (key == "ht_size") {
            store.setMinimumHashTableSize(value);
        } else if (key == "ht_temp_items_allowed_percent") {
            store.setHtTempItemsAllowedPercent(value);
        } else if (key == "checkpoint_max_size") {
            store.setCheckpointMaxSize(value);
        } else if (key == "checkpoint_destruction_tasks") {
            store.createAndScheduleCheckpointDestroyerTasks();
        } else if (key == "seqno_persistence_timeout") {
            store.setSeqnoPersistenceTimeout(std::chrono::seconds(value));
        } else if (key == "history_retention_seconds") {
            store.setHistoryRetentionSeconds(std::chrono::seconds(value));
        } else if (key == "history_retention_bytes") {
            store.setHistoryRetentionBytes(value);
        } else if (key == "dcp_backfill_in_progress_per_connection_limit") {
            store.getKVStoreScanTracker().updateMaxRunningDcpBackfills(value);
        } else {
            EP_LOG_WARN("Failed to change value for unknown variable, {}", key);
        }
    }

    void ssizeValueChanged(std::string_view key, ssize_t value) override {
        if (key == "exp_pager_initial_run_time") {
            store.setExpiryPagerTasktime(value);
        }
    }

    void booleanValueChanged(std::string_view key, bool value) override {
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
        } else if (key.compare("compaction_expiry_fetch_inline") == 0) {
            store.setCompactionExpiryFetchInline(value);
        } else if (key == "continuous_backup_enabled") {
            store.setContinuousBackupEnabled(value);
        }
    }

    void floatValueChanged(std::string_view key, float value) override {
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
        } else if (key == "mutation_mem_ratio") {
            store.setMutationMemRatio(value);
        }
    }

    void stringValueChanged(std::string_view key, const char* value) override {
        if (key == "durability_min_level") {
            const auto res = store.setMinDurabilityLevel(
                    cb::durability::to_level(value));
            if (res != cb::engine_errc::success) {
                throw std::invalid_argument(
                        "Failed to set durability_min_level: " +
                        to_string(res));
            }
        } else if (key == "hlc_invalid_strategy") {
            store.setHlcInvalidStrategy(store.parseHlcInvalidStrategy(value));
        } else if (key == "dcp_hlc_invalid_strategy") {
            store.setDcpHlcInvalidStrategy(
                    store.parseHlcInvalidStrategy(value));
        } else {
            EP_LOG_WARN("Failed to change value for unknown variable, {}", key);
        }
    }

private:
    KVBucket& store;
};

class PendingOpsNotification : public EpTask {
public:
    PendingOpsNotification(EventuallyPersistentEngine& e, VBucketPtr& vb)
        : EpTask(e, TaskId::PendingOpsNotification, 0, false),
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

class RespondAmbiguousNotification : public EpTask {
public:
    RespondAmbiguousNotification(EventuallyPersistentEngine& e,
                                 VBucketPtr& vb,
                                 std::vector<CookieIface*>&& cookies_)
        : EpTask(e, TaskId::RespondAmbiguousNotification, 0, false),
          id(vb->getId()),
          cookies(std::move(cookies_)),
          description("Notify clients of Sync Write Ambiguous " +
                      id.to_string()) {
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
        TRACE_EVENT1("ep-engine/task",
                     "RespondAmbiguousNotification",
                     "vb",
                     id.get());
        for (auto* cookie : cookies) {
            engine->clearEngineSpecific(*cookie);
            engine->notifyIOComplete(*cookie,
                                     cb::engine_errc::sync_write_ambiguous);
        }
        return false;
    }

private:
    Vbid id;
    std::vector<CookieIface*> cookies;
    const std::string description;
};

KVBucket::KVBucket(EventuallyPersistentEngine& theEngine)
    : engine(theEngine),
      stats(engine.getEpStats()),
      vbMap(*this),
      initialMfuValue(Item::initialFreqCount),
      defragmenterTask(nullptr),
      itemCompressorTask(nullptr),
      itemFreqDecayerTask(nullptr),
      vb_mutexes(engine.getConfiguration().getMaxVbuckets()),
      paused(false),
      backfillMemoryThreshold(0.95),
      lastTransTimePerItem(0),
      collectionsManager(std::make_shared<Collections::Manager>()),
      xattrEnabled(true),
      crossBucketHtQuotaSharing(
              engine.getConfiguration().isCrossBucketHtQuotaSharing()),
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

    ExecutorPool::get()->registerTaskable(ObjectRegistry::getCurrentEngine()->getTaskable());

    // Reset memory overhead when bucket is created.
    for (auto& core : stats.coreLocal) {
        core->memOverhead.reset();
    }
    stats.coreLocal.get()->memOverhead += sizeof(KVBucket);

    config.addValueChangedListener(
            "mem_used_merge_threshold_percent",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    getKVStoreScanTracker().updateMaxRunningScans(
            config.getMaxSize(),
            config.getRangeScanKvStoreScanRatio(),
            config.getDcpBackfillInProgressPerConnectionLimit());

    config.addValueChangedListener(
            "mem_low_wat_percent",
            std::make_unique<StatsValueChangeListener>(stats, *this));
    config.addValueChangedListener(
            "mem_high_wat_percent",
            std::make_unique<StatsValueChangeListener>(stats, *this));

    setMutationMemRatio(config.getMutationMemRatio());
    config.addValueChangedListener(
            "mutation_mem_ratio",
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

    itemPagerTask = engine.createItemPager();

    minDurabilityLevel =
            cb::durability::to_level(config.getDurabilityMinLevelString());
    config.addValueChangedListener(
            "durability_min_level",
            std::make_unique<EPStoreValueChangeListener>(*this));

    setHlcInvalidStrategy(
            parseHlcInvalidStrategy(config.getHlcInvalidStrategyString()));
    config.addValueChangedListener(
            "hlc_invalid_strategy",
            std::make_unique<EPStoreValueChangeListener>(*this));
    setDcpHlcInvalidStrategy(
            parseHlcInvalidStrategy(config.getDcpHlcInvalidStrategyString()));
    config.addValueChangedListener(
            "dcp_hlc_invalid_strategy",
            std::make_unique<EPStoreValueChangeListener>(*this));

    setMinimumHashTableSize(config.getHtSize());
    config.addValueChangedListener(
            "ht_size", std::make_unique<EPStoreValueChangeListener>(*this));

    setHtTempItemsAllowedPercent(config.getHtTempItemsAllowedPercent());
    config.addValueChangedListener(
            "ht_temp_items_allowed_percent",
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

    setHistoryRetentionSeconds(
            std::chrono::seconds(config.getHistoryRetentionSeconds()));
    config.addValueChangedListener(
            "history_retention_seconds",
            std::make_unique<EPStoreValueChangeListener>(*this));

    setHistoryRetentionBytes(config.getHistoryRetentionBytes());
    config.addValueChangedListener(
            "history_retention_bytes",
            std::make_unique<EPStoreValueChangeListener>(*this));

    setCompactionExpiryFetchInline(config.isCompactionExpiryFetchInline());
    config.addValueChangedListener(
            "compaction_expiry_fetch_inline",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "dcp_backfill_in_progress_per_connection_limit",
            std::make_unique<EPStoreValueChangeListener>(*this));

    config.addValueChangedListener(
            "checkpoint_destruction_tasks",
            std::make_unique<EPStoreValueChangeListener>(*this));

    // Just initialise the value from the config, do not call the method, since
    // that will trigger an unnecessary vbstate flush.
    continuousBackupEnabled = config.isContinuousBackupEnabled();
    config.addValueChangedListener(
            "continuous_backup_enabled",
            std::make_unique<EPStoreValueChangeListener>(*this));
}

bool KVBucket::initialize() {
    // We should nuke everything unless we want warmup
    Configuration &config = engine.getConfiguration();
    if ((config.getBucketTypeString() == "ephemeral") || (!config.isWarmup())) {
        reset();
    }
    initializeExpiryPager(config);
    initializeInitialMfuUpdater(config);

    ExTask htrTask = std::make_shared<HashtableResizerTask>(*this, 10);
    ExecutorPool::get()->schedule(htrTask);

    createAndScheduleCheckpointRemoverTasks();

    createAndScheduleCheckpointDestroyerTasks();

    // Setup tasks related to SyncWrite timeout handling.
    // We use a task per vBucket; each of which is scheduled to run when the
    // next SyncWrite to be completed is due to exceed its timeout. If that
    // SyncWrite is completed before the timeout then the task is re-scheduled
    // (and doesn't run).
    syncWriteTimeoutFactory = [&engine = getEPEngine()](VBucket& vbucket) {
        return std::make_unique<EventDrivenTimeoutTask>(
                std::make_shared<VBucketSyncWriteTimeoutTask>(engine, vbucket));
    };

    durabilityCompletionTask =
            std::make_shared<DurabilityCompletionTask>(engine);
    ExecutorPool::get()->schedule(durabilityCompletionTask);

    workloadMonitorTask = std::make_shared<WorkLoadMonitor>(engine, false);
    ExecutorPool::get()->schedule(workloadMonitorTask);

#if HAVE_JEMALLOC
    /* Only create the defragmenter task if we have an underlying memory
     * allocator which can facilitate defragmenting memory.
     */
    defragmenterTask = std::make_shared<DefragmenterTask>(engine, stats);
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
    itemFreqDecayerTask = ItemFreqDecayerTaskManager::get().create(
            engine,
            gsl::narrow_cast<uint16_t>(config.getItemFreqDecayerPercent()));
    ExecutorPool::get()->schedule(itemFreqDecayerTask);

    createAndScheduleSeqnoPersistenceNotifier();

    bucketQuotaChangeTask = std::make_shared<BucketQuotaChangeTask>(engine);
    ExecutorPool::get()->schedule(bucketQuotaChangeTask);

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

Warmup* KVBucket::getPrimaryWarmup() const {
    return nullptr;
}

Warmup* KVBucket::getSecondaryWarmup() const {
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

cb::engine_errc KVBucket::evictKey(const DocKeyView& key,
                                   Vbid vbucket,
                                   const char** msg) {
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return lr.error();
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return cb::engine_errc::not_my_vbucket;
    }

    // collections read-lock scope
    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        return cb::engine_errc::unknown_collection;
    } // now hold collections read access for the duration of the eviction

    return vb->evictKey(msg, rlh, cHandle);
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
    }
    if (!gv.item->isDeleted()) {
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
    auto result = document_pre_expiry(it.getValueView(), it.getDataType());
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
    if (isCheckpointMemoryStateFull(verifyCheckpointMemoryState())) {
        return;
    }

    auto vb = getVBucket(it.getVBucketId());
    if (!vb) {
        return;
    }

    // MB-25931: Empty XATTR items need their value before we can call
    // pre_expiry. These occur because the value has been evicted.
    if (cb::mcbp::datatype::is_xattr(it.getDataType()) && it.getNBytes() == 0) {
        getValue(it);
    }

    // Process positive seqnos (ignoring special *temp* items) and only those
    // items with a value
    if (it.getBySeqno() >= 0 && it.getNBytes()) {
        runPreExpiryHook(*vb, it);
    }

    std::unique_ptr<CompactionBGFetchItem> bgfetch;

    // Obtain reader access to the VB state change lock so that the VB can't
    // switch state whilst we're processing
    {
        std::shared_lock rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_active) {
            bgfetch = vb->processExpiredItem(it, startTime, source);
        }
    }
    processExpiredItemHook();

    if (!bgfetch) {
        return;
    }

    Expects(source == ExpireBy::Compactor);

    auto fetchStartTime = cb::time::steady_clock::now();

    auto key = DiskDocKey(it);
    auto gv = getROUnderlying(vb->getId())->get(key, vb->getId());

    bgfetch->value = &gv;

    bgfetch->complete(engine, vb, fetchStartTime, key);
}

bool KVBucket::isMetaDataResident(VBucketPtr& vb, const DocKeyView& key) {
    if (!vb) {
        throw std::invalid_argument("EPStore::isMetaDataResident: vb is NULL");
    }

    auto result = vb->ht.findForRead(key, TrackReference::No, WantsDeleted::No);

    return result.storedValue && !result.storedValue->isTempItem();
}

void KVBucket::logQTime(const GlobalTask& task,
                        std::string_view threadName,
                        cb::time::steady_clock::duration enqTime) {
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
    if ((taskType == TaskType::NonIO && enqTime > std::chrono::seconds(1)) ||
        (taskType == TaskType::AuxIO && enqTime > std::chrono::seconds(10))) {
        EP_LOG_WARN_CTX("Slow scheduling",
                        {"task", task.getDescription()},
                        {"thread", threadName},
                        {"overhead", enqTime});
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(enqTime);
    stats.schedulingHisto[static_cast<int>(task.getTaskId())].add(us);
}

void KVBucket::logRunTime(const GlobalTask& task,
                          std::string_view threadName,
                          cb::time::steady_clock::duration runTime) {
    // Check if exceeded expected duration; and if so log.
    if (runTime > task.maxExpectedDuration()) {
        cb::logger::Json ctx{{"task", task.getDescription()},
                             {"thread", threadName},
                             {"runtime", runTime}};

        // Print the profiling information (if any).
        auto* profile = task.getRuntimeProfile();
        if (profile && !profile->empty()) {
            auto j = cb::logger::Json::object();
            for (auto& p : *profile) {
                // Durations less than 5% of the total runtime are hidden to
                // avoid logging uninteresting entries.
                if (p.second.second < (runTime / 20)) {
                    continue;
                }

                j[static_cast<std::string_view>(p.first)] = {p.second.first,
                                                             p.second.second};
            }
            ctx["profile"] = std::move(j);
        }

        EP_LOG_WARN_CTX("Slow runtime", std::move(ctx));
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(runTime);
    stats.taskRuntimeHisto[static_cast<int>(task.getTaskId())].add(us);
}

cb::engine_errc KVBucket::set(Item& itm,
                              CookieIface* cookie,
                              cb::StoreIfPredicate predicate) {
    Expects(cookie);
    auto lr = operationPrologue(itm.getVBucketId(),
                                *cookie,
                                {vbucket_state_active},
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        if (auto status = cHandle.handleWriteStatus(engine, cookie);
            status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the set

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());

        result = vb->set(rlh, itm, cookie, engine, predicate, cHandle);
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

cb::engine_errc KVBucket::add(Item& itm, CookieIface* cookie) {
    Expects(cookie);
    auto lr = operationPrologue(itm.getVBucketId(),
                                *cookie,
                                {vbucket_state_active},
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    if (itm.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return cb::engine_errc::not_stored;
    }

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        if (auto status = cHandle.handleWriteStatus(engine, cookie);
            status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the add

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());
        result = vb->add(rlh, itm, cookie, engine, cHandle);
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
                                  CookieIface* cookie,
                                  cb::StoreIfPredicate predicate) {
    Expects(cookie);
    auto lr = operationPrologue(itm.getVBucketId(),
                                *cookie,
                                {vbucket_state_active},
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    cb::engine_errc result;
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(itm.getKey());
        if (auto status = cHandle.handleWriteStatus(engine, cookie);
            status != cb::engine_errc::success) {
            return status;
        } // now hold collections read access for the duration of the replace

        // maybe need to adjust expiry of item
        cHandle.processExpiryTime(itm, getMaxTtl());
        result = vb->replace(rlh, itm, cookie, engine, predicate, cHandle);
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

GetValue KVBucket::get(const DocKeyView& key,
                       Vbid vbucket,
                       CookieIface* cookie,
                       get_options_t options) {
    return getInternal(key, vbucket, cookie, ForGetReplicaOp::No, options);
}

GetValue KVBucket::getReplica(const DocKeyView& key,
                              Vbid vbucket,
                              CookieIface* cookie,
                              get_options_t options) {
    return getInternal(key, vbucket, cookie, ForGetReplicaOp::Yes, options);
}

uint64_t KVBucket::getLastPersistedSeqno(Vbid vb) {
    auto vbucket = vbMap.getBucket(vb);
    if (vbucket) {
        return vbucket->getPersistenceSeqno();
    }
    return 0;
}

void KVBucket::releaseBlockedCookies() {
    for (size_t vbid = 0; vbid < vbMap.size; ++vbid) {
        VBucketPtr vb = vbMap.getBucket(Vbid{gsl::narrow<uint16_t>(vbid)});
        if (!vb) {
            continue;
        }
        // tmp_fail and remove all current seqno_persistence requests, they
        // are unlikely to be completed successfully at this stage of bucket
        // deletion, and the associated connection could block deletion if
        // not notified now.
        vb->failAllSeqnoPersistenceReqs(engine);

        std::shared_lock rlh(vb->getStateLock());
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

void KVBucket::initiateShutdown() {
    EP_LOG_INFO_RAW(
            "Shutting down all DCP connections in "
            "preparation for bucket deletion.");
    engine.getDcpConnMap().shutdownAllConnections();
}

cb::engine_errc KVBucket::setVBucketState(Vbid vbid,
                                          vbucket_state_t to,
                                          const nlohmann::json* meta,
                                          TransferVB transfer,
                                          CookieIface* cookie) {
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

    const bool useSnapshot = meta && meta->contains("use_snapshot");

    // Lock to prevent a race condition between a failed update and add.
    std::unique_lock<std::mutex> lh(vbsetMutex);
    if (!useSnapshot && isVBucketLoading_UNLOCKED(vbid, lh)) {
        EP_LOG_WARN_CTX(
                "Received plain setVBucketState while vBucket is being "
                "loaded or mounted",
                {"vb", vbid},
                {"to", VBucket::toString(to)});
        return cb::engine_errc::key_already_exists;
    }
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (vb) {
        if (useSnapshot) {
            Expects(cookie);
            return loadVBucket_UNLOCKED(*cookie, vbid, to, *meta, lh);
        }
        std::unique_lock vbStateLock(vb->getStateLock());
        setVBucketState_UNLOCKED(vb,
                                 to,
                                 false /*deleteVB*/,
                                 meta,
                                 transfer,
                                 true /*notifyDcp*/,
                                 lh,
                                 vbStateLock);
    } else if (vbid.get() < vbMap.getSize()) {
        if (useSnapshot) {
            Expects(cookie);
            return loadVBucket_UNLOCKED(*cookie, vbid, to, *meta, lh);
        }
        return createVBucket_UNLOCKED(vbid, to, meta, lh);
    } else {
        return cb::engine_errc::out_of_range;
    }
    return cb::engine_errc::success;
}

void KVBucket::setVBucketState_UNLOCKED(
        VBucketPtr& vb,
        vbucket_state_t to,
        bool deleteVB,
        const nlohmann::json* meta,
        TransferVB transfer,
        bool notifyDcp,
        std::unique_lock<std::mutex>& vbset,
        std::unique_lock<folly::SharedMutex>& vbStateLock) {
    // Return success immediately if the new state is the same as the old,
    // no extra metadata was included, and the vbucket is not being deleted.
    if (to == vb->getState() && !meta && !deleteVB) {
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

    if (notifyDcp && (oldstate != to || deleteVB)) {
        // Close inbound (passive) streams into the vbucket
        // in case of a failover or if we are deleting the vbucket.
        bool closeInboundStreams = deleteVB || (to == vbucket_state_active &&
                                                transfer == TransferVB::No);
        engine.getDcpConnMap().vbucketStateChanged(
                vb->getId(), to, closeInboundStreams, &vbStateLock);
    }

    bool needToPersistVbState = true;
    if (to == vbucket_state_active) {
        needToPersistVbState =
                continueToActive(oldstate, transfer, vb, vbStateLock);
    }

    if (needToPersistVbState) {
        persistVBState(vb->getId());
    }
}

bool KVBucket::continueToActive(
        vbucket_state_t oldstate,
        TransferVB transfer,
        VBucketPtr& vb,
        std::unique_lock<folly::SharedMutex>& vbStateLock) {
    /**
     * Expect this to happen for failover
     */
    if (oldstate != vbucket_state_active) {
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
        collectionsManager->maybeUpdate(vbStateLock, *vb);

        // MB-37917: The vBucket is becoming an active and can no longer be
        // receiving an initial disk snapshot. It is now the source of truth so
        // we should not prevent any Consumer from streaming from it.
        vb->setReceivingInitialDiskSnapshot(false);
    }

    bool needToPersistVbState = true;
    if (oldstate != vbucket_state_active && transfer == TransferVB::No) {
        // Changed state to active and this isn't a transfer (i.e.
        // takeover), which means this is a new fork in the vBucket history
        const auto entry = vb->processFailover();
        needToPersistVbState = false; // processFailover always queues a vbstate
                                      // so caller has no need to persist
        EP_LOG_INFO(
                "KVBucket::setVBucketState: {} created new failover entry "
                "with uuid:{} and seqno:{}",
                vb->getId(),
                entry.vb_uuid,
                entry.by_seqno);
    }

    if (oldstate == vbucket_state_pending) {
        ExTask notifyTask =
                std::make_shared<PendingOpsNotification>(engine, vb);
        ExecutorPool::get()->schedule(notifyTask);
    }

    return needToPersistVbState;
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
            std::make_unique<Collections::VB::Manifest>(collectionsManager));

    newvb->setFreqSaturatedCallback(
            [this] { itemFrequencyCounterSaturated(); });

    const auto& config = engine.getConfiguration();
    const auto storageProperties = getStorageProperties();
    if (config.isBfilterEnabled() && !storageProperties.hasBloomFilter()) {
        // Initialize bloom filters upon vbucket creation during bucket
        // creation and rebalance. We avoid creating this during warmup for
        // couchstore, since the bloomfilter isn't saved to disk & we don't
        // have a view of all the keys on disk until the next compaction
        // runs.  When the next compaction runs, we'll create this bloom
        // filter.
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
        std::shared_lock rlh(newvb->getStateLock());
        collectionsManager->maybeUpdate(rlh, *newvb);
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
    persistVBState(vbid);
    return cb::engine_errc::success;
}

size_t KVBucket::getExpiryPagerSleeptime() {
    return expiryPagerTask->getSleepTime().count();
}

VBucketStateLockMap<std::shared_lock<folly::SharedMutex>>
KVBucket::lockAllVBucketStates() {
    VBucketStateLockMap<std::shared_lock<folly::SharedMutex>> vbStateLocks;
    vbStateLocks.reserve(getVBuckets().getSize());
    for (Vbid::id_type i = 0; i < getVBuckets().getSize(); i++) {
        auto vb = getVBuckets().getBucket(Vbid(i));
        if (vb) {
            vbStateLocks.emplace(Vbid(i), vb->getStateLock());
        }
    }
    return vbStateLocks;
}

cb::engine_errc KVBucket::deleteVBucket(Vbid vbid, CookieIface* c) {
    {
        std::unique_lock<std::mutex> vbSetLh(vbsetMutex);
        // Obtain a locked VBucket to ensure we interlock with other
        // threads that are manipulating the VB (particularly ones which may
        // try and change the disk revision e.g. deleteAll and compaction).
        auto lockedVB = getLockedVBucket(vbid);
        if (!lockedVB) {
            return cb::engine_errc::not_my_vbucket;
        }

        {
            std::unique_lock vbStateLock(lockedVB->getStateLock());
            setVBucketState_UNLOCKED(lockedVB.getVB(),
                                     vbucket_state_dead,
                                     true /*deleteVB*/,
                                     nullptr /*meta*/,
                                     TransferVB::No,
                                     true /*notifyDcp*/,
                                     vbSetLh,
                                     vbStateLock);
        }

        // Drop the VB to begin deletion, the last holder of the VB will
        // unknowingly trigger the destructor which schedules a deletion task.
        vbMap.dropVBucketAndSetupDeferredDeletion(vbid, c);

        deleteVbucketImpl(lockedVB);
    }

    if (c) {
        return cb::engine_errc::would_block;
    }
    return cb::engine_errc::success;
}

cb::engine_errc KVBucket::checkForDBExistence(Vbid db_file_id) {
    std::string backend = engine.getConfiguration().getBackendString();
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

void KVBucket::persistShutdownContext() {
    nlohmann::json snapshotStats = {
            {"ep_force_shutdown", stats.forceShutdown ? "true" : "false"},
            {"ep_shutdown_time", fmt::format("{}", ep_real_time())}};
    getOneRWUnderlying()->snapshotStats(snapshotStats);
}

void KVBucket::getAggregatedVBucketStats(
        const BucketStatCollector& collector,
        cb::prometheus::MetricGroup metricGroup) {
    // track whether high or low cardinality stats should be collected
    auto doHigh = metricGroup == cb::prometheus::MetricGroup::All ||
                  metricGroup == cb::prometheus::MetricGroup::High;
    auto doLow = metricGroup == cb::prometheus::MetricGroup::All ||
                 metricGroup == cb::prometheus::MetricGroup::Low;

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

        stateCol.addStat(Key::logical_data_size, visitor.getLogicalDiskSize());

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

        // Will be removed in Morpheus.Next. Tracked via MB-60598.
        stateCol.addStat(Key::vb_ht_memory, visitor.getHashtableMemory());
        stateCol.addStat(Key::vb_ht_memory_overhead,
                         visitor.getHashtableMemory());
        stateCol.addStat(Key::vb_ht_item_memory, visitor.getItemMemory());
        stateCol.addStat(Key::vb_ht_item_memory_uncompressed,
                         visitor.getUncompressedItemMemory());
        stateCol.addStat(Key::vb_ht_max_size, visitor.getHtMaxSize());
        stateCol.addStat(Key::vb_ht_avg_size,
                         (visitor.getVBucketNumber() == 0)
                                 ? 0
                                 : static_cast<double>(visitor.getHtSizeSum()) /
                                           static_cast<double>(
                                                   visitor.getVBucketNumber()));
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
        // Add stat for the max history disk size of a vbucket
        stateCol.addStat(Key::vb_max_history_disk_size,
                         visitor.getMaxHistoryDiskSize());
        stateCol.addStat(Key::vb_dm_mem_used,
                         visitor.getDurabilityMonitorMemory());
        stateCol.addStat(Key::vb_dm_num_tracked,
                         visitor.getDurabilityMonitorItems());
    }

    for (const auto& visitor : {active, replica}) {
        auto state = VBucket::toString(visitor.getVBucketState());
        auto stateCol = collector.withLabels({{"state", state}});

        stateCol.addStat(Key::vb_sync_write_accepted_count,
                         visitor.getSyncWriteAcceptedCount());
        stateCol.addStat(Key::vb_sync_write_committed_count,
                         visitor.getSyncWriteCommittedCount());
        stateCol.addStat(Key::vb_sync_write_committed_not_durable_count,
                         visitor.getSyncWriteCommittedNotDurableCount());
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
    collector.addStat(Key::ep_total_cache_size,
                      active.getItemMemory() + replica.getItemMemory() +
                              pending.getItemMemory());
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
        auto datatypeStr = cb::mcbp::datatype::to_string(ii);
        auto labelled = collector.withLabels(
                {{"datatype", datatypeStr}, {"vbucket_state", "active"}});

        labelled.addStat(Key::datatype_count, active.getDatatypeCount(ii));
    }

    for (uint8_t ii = 0; ii < replica.getNumDatatypes(); ++ii) {
        auto datatypeStr = cb::mcbp::datatype::to_string(ii);
        auto labelled = collector.withLabels(
                {{"datatype", datatypeStr}, {"vbucket_state", "replica"}});

        labelled.addStat(Key::datatype_count, replica.getDatatypeCount(ii));
    }
}

void KVBucket::completeBGFetchMulti(
        Vbid vbId,
        std::vector<bgfetched_item_t>& fetchedItems,
        cb::time::steady_clock::time_point startTime) {
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
                        cb::time::steady_clock::now().time_since_epoch())
                        .count());
    } else {
        std::map<CookieIface*, cb::engine_errc> toNotify;
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

KVBucketResult<VBucketPtr> KVBucket::lookupVBucket(Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return folly::Unexpected(cb::engine_errc::not_my_vbucket);
    }
    return vb;
}

cb::engine_errc KVBucket::requireVBucketState(
        VBucketStateLockRef vbStateLock,
        VBucket& vb,
        PermittedVBStates permittedVBStates,
        CookieIface& cookie) {
    auto vbState = vb.getState();
    if (permittedVBStates.test(vbState)) {
        return cb::engine_errc::success;
    }
    if (permittedVBStates.test(vbucket_state_active) &&
        vbState == vbucket_state_pending) {
        // We only enqueue ops for pending vBuckets if the operation is for
        // actives. For replicas, we do not reach this code and always NVMB.
        Expects(vb.addPendingOp(&cookie));
        return cb::engine_errc::would_block;
    }

    ++stats.numNotMyVBuckets;
    return cb::engine_errc::not_my_vbucket;
}

cb::engine_errc KVBucket::maybeAllowMutation(VBucket& vb,
                                             std::string_view debugOpcode) {
    if (vb.isTakeoverBackedUp()) {
        EP_LOG_DEBUG(
                "({}) Returned TMPFAIL to a {} op"
                ", becuase takeover is lagging",
                vb.getId(),
                debugOpcode);
        return cb::engine_errc::temporary_failure;
    }
    return cb::engine_errc::success;
}

KVBucketResult<std::tuple<VBucketPtr, std::shared_lock<folly::SharedMutex>>>
KVBucket::operationPrologue(Vbid vbid,
                            CookieIface& cookie,
                            PermittedVBStates permittedVBStates,
                            IsMutationOp isMutationOp,
                            std::string_view debugOpcode) {
    auto lr = lookupVBucket(vbid);
    if (!lr) {
        return folly::Unexpected(lr.error());
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    cb::engine_errc rv =
            requireVBucketState(rlh, *vb, permittedVBStates, cookie);
    if (rv != cb::engine_errc::success) {
        return folly::Unexpected(rv);
    }
    if (rv = maybeAllowMutation(*vb, debugOpcode);
        rv != cb::engine_errc::success) {
        return folly::Unexpected(rv);
    }

    return std::make_tuple(std::move(vb), std::move(rlh));
}

GetValue KVBucket::getInternal(const DocKeyView& key,
                               Vbid vbucket,
                               CookieIface* cookie,
                               const ForGetReplicaOp getReplicaItem,
                               get_options_t options) {
    Expects(cookie);
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return GetValue(nullptr, lr.error());
    }
    auto vb = std::move(*lr);

    const bool honorStates = (options & HONOR_STATES);

    std::shared_lock rlh(vb->getStateLock());
    if (honorStates) {
        vbucket_state_t permittedState =
                (getReplicaItem == ForGetReplicaOp::Yes) ? vbucket_state_replica
                                                         : vbucket_state_active;
        cb::engine_errc rv =
                requireVBucketState(rlh, *vb, {permittedState}, *cookie);
        if (rv != cb::engine_errc::success) {
            return GetValue(nullptr, rv);
        }
    }

    { // hold collections read handle for duration of get
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(*cookie,
                                                    cHandle.getManifestUid());
            return GetValue(nullptr, cb::engine_errc::unknown_collection);
        }

        auto result = vb->getInternal(rlh,
                                      cookie,
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

GetValue KVBucket::getRandomKey(CollectionID cid, CookieIface& cookie) {
    const auto max = vbMap.getSize();
    const auto start = static_cast<Vbid::id_type>(labs(getRandom()) % max);
    Vbid::id_type curr = start;
    std::unique_ptr<Item> itm;

    // Must setup cookie metering state, do this by checking the Manifest
    auto [uid, entry] = getCollectionEntry(cid);
    if (!entry) {
        engine.setUnknownCollectionErrorContext(cookie, uid);
        return GetValue(nullptr, cb::engine_errc::unknown_collection);
    }
    cookie.setCurrentCollectionInfo(
            entry->sid,
            cid,
            uid,
            entry->metered == Collections::Metered::Yes,
            Collections::isSystemCollection(entry->name, cid));

    while (itm == nullptr) {
        VBucketPtr vb = getVBucket(Vbid(curr++));
        if (vb) {
            std::shared_lock rlh(vb->getStateLock());
            if (vb->getState() == vbucket_state_active) {
                auto cHandle = vb->lockCollections();
                if (!cHandle.exists(cid)) {
                    // even after successfully checking the manifest, the vb
                    // may not know the collection (could be dropped after the
                    // getCollectionEntry check)
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

cb::engine_errc KVBucket::getMetaData(const DocKeyView& key,
                                      Vbid vbucket,
                                      CookieIface* cookie,
                                      ItemMetaData& metadata,
                                      uint32_t& deleted,
                                      uint8_t& datatype) {
    Expects(cookie);
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return lr.error();
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    // For getMeta, we allow active and pending vBuckets only, and if pending,
    // we perform the operation without queueing.
    if (vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    { // collections read scope
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(*cookie,
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
                                      CookieIface* cookie,
                                      PermittedVBStates permittedVBStates,
                                      CheckConflicts checkConflicts,
                                      bool allowExisting,
                                      GenerateBySeqno genBySeqno,
                                      GenerateCas genCas,
                                      ExtendedMetaData* emd,
                                      EnforceMemCheck enforceMemCheck) {
    Expects(cookie);
    auto lr = operationPrologue(itm.getVBucketId(),
                                *cookie,
                                permittedVBStates,
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return cb::engine_errc::cas_value_invalid;
    }

    // To differentiate between a Replication and a non-replication setWithMeta,
    // we know that genBySeqno will always be set to no for a replication
    // stream and yes otherwise.
    const bool isReplication = genBySeqno == GenerateBySeqno::No;
    InvalidCasStrategy strategy = getHlcInvalidStrategy(isReplication);
    if (!vb->isValidCas(itm.getCas())) {
        ++stats.numInvalidCas;
        if (strategy == InvalidCasStrategy::Error) {
            return cb::engine_errc::cas_value_invalid;
        }
        if (strategy == InvalidCasStrategy::Replace) {
            ++stats.numCasRegenerated;
            genCas = GenerateCas::Yes;
        }
    }

    auto rv = cb::engine_errc::success;
    {
        // hold collections read lock for duration of set
        auto cHandle = vb->lockCollections(itm.getKey());
        rv = cHandle.handleWriteStatus(engine, cookie);

        if (rv == cb::engine_errc::success) {
            cHandle.processExpiryTime(itm, getMaxTtl());
            rv = vb->setWithMeta(rlh,
                                 itm,
                                 cas,
                                 seqno,
                                 cookie,
                                 engine,
                                 checkConflicts,
                                 allowExisting,
                                 genBySeqno,
                                 genCas,
                                 cHandle,
                                 enforceMemCheck);
        }
    }

    if (rv == cb::engine_errc::success) {
        checkAndMaybeFreeMemory();
    }
    return rv;
}

cb::engine_errc KVBucket::prepare(Item& itm,
                                  CookieIface* cookie,
                                  EnforceMemCheck enforceMemCheck) {
    Expects(cookie);
    auto lr = lookupVBucket(itm.getVBucketId());
    if (!lr) {
        return lr.error();
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    PermittedVBStates permittedVBStates = {vbucket_state_replica,
                                           vbucket_state_pending};
    if (!permittedVBStates.test(vb->getState())) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    // check for the incoming item's CAS validity
    if (!Item::isValidCas(itm.getCas())) {
        return cb::engine_errc::cas_value_invalid;
    }

    // In contrast to setWithMeta/deleteWithMeta, prepare is only called via
    // DCP and will always be a replication stream.
    const bool isReplication = true;
    auto generateCas = GenerateCas::No;
    InvalidCasStrategy strategy = getHlcInvalidStrategy(isReplication);
    if (!vb->isValidCas(itm.getCas())) {
        ++stats.numInvalidCas;
        if (strategy == InvalidCasStrategy::Error) {
            return cb::engine_errc::cas_value_invalid;
        }
        if (strategy == InvalidCasStrategy::Replace) {
            ++stats.numCasRegenerated;
            generateCas = GenerateCas::Yes;
        }
    }

    cb::engine_errc rv = cb::engine_errc::success;
    { // hold collections read lock for duration of prepare

        auto cHandle = vb->lockCollections(itm.getKey());
        rv = cHandle.handleWriteStatus(engine, cookie);
        if (rv == cb::engine_errc::success) {
            cHandle.processExpiryTime(itm, getMaxTtl());
            rv = vb->prepare(rlh,
                             itm,
                             0,
                             nullptr,
                             cookie,
                             engine,
                             CheckConflicts::No,
                             true /*allowExisting*/,
                             GenerateBySeqno::No,
                             generateCas,
                             cHandle,
                             enforceMemCheck);
        }
    }

    if (rv == cb::engine_errc::success) {
        checkAndMaybeFreeMemory();
    }
    return rv;
}

GetValue KVBucket::getAndUpdateTtl(const DocKeyView& key,
                                   Vbid vbucket,
                                   CookieIface* cookie,
                                   time_t exptime) {
    Expects(cookie);
    // If the exptime changed, we will end up queueing a mutation, which will
    // also need to be replicated.
    auto lr = operationPrologue(vbucket,
                                *cookie,
                                {vbucket_state_active},
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return GetValue(nullptr, lr.error());
    }
    auto [vb, rlh] = std::move(*lr);

    {
        // collections read scope
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(*cookie,
                                                    cHandle.getManifestUid());
            return GetValue(nullptr, cb::engine_errc::unknown_collection);
        }

        auto result = vb->getAndUpdateTtl(
                rlh,
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

GetValue KVBucket::getLocked(const DocKeyView& key,
                             Vbid vbucket,
                             rel_time_t currentTime,
                             std::chrono::seconds lockTimeout,
                             CookieIface* cookie) {
    Expects(cookie);
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return GetValue(nullptr, lr.error());
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return GetValue(nullptr, cb::engine_errc::not_my_vbucket);
    }

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(*cookie,
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

cb::engine_errc KVBucket::unlockKey(const DocKeyView& key,
                                    Vbid vbucket,
                                    uint64_t cas,
                                    rel_time_t currentTime,
                                    CookieIface* cookie) {
    Expects(cookie);
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return lr.error();
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(*cookie,
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
            if (v->getCasForWrite(currentTime) == cas) {
                v->unlock();
                return cb::engine_errc::success;
            }
            return cb::engine_errc::locked_tmpfail;
        }
        return engine.getNotLockedError();
    }
    case VBucket::FetchForWriteResult::Status::OkVacant:
        if (eviction_policy == EvictionPolicy::Value) {
            return cb::engine_errc::no_such_key;
        } else {
            // An item's lock is not persisted to disk. Therefore, if the item
            // is found on disk and has not been deleted we return not_locked.

            // Release HashBucketLock so that vb->getMetaData() can acquire it.
            res.lock.getHTLock().unlock();

            ItemMetaData itemMeta;
            uint32_t deleted;
            uint8_t datatype;
            auto ret = vb->getMetaData(
                    cookie, engine, cHandle, itemMeta, deleted, datatype);
            if (ret != cb::engine_errc::success) {
                return ret;
            }
            if (deleted) {
                return cb::engine_errc::no_such_key;
            }

            return engine.getNotLockedError();
        }
    case VBucket::FetchForWriteResult::Status::ESyncWriteInProgress:
        return cb::engine_errc::sync_write_in_progress;
    }
    folly::assume_unreachable();
}

cb::engine_errc KVBucket::getKeyStats(const DocKeyView& key,
                                      Vbid vbucket,
                                      CookieIface& cookie,
                                      struct key_stats& kstats,
                                      WantsDeleted wantsDeleted) {
    auto lr = lookupVBucket(vbucket);
    if (!lr) {
        return lr.error();
    }
    auto vb = std::move(*lr);

    std::shared_lock rlh(vb->getStateLock());
    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        engine.setUnknownCollectionErrorContext(cookie,
                                                cHandle.getManifestUid());
        return cb::engine_errc::unknown_collection;
    }

    return vb->getKeyStats(rlh, cookie, engine, kstats, wantsDeleted, cHandle);
}

std::string KVBucket::validateKey(const DocKeyView& key,
                                  Vbid vbucket,
                                  Item& diskItem) {
    VBucketPtr vb = getVBucket(vbucket);
    std::shared_lock rlh(vb->getStateLock());

    auto cHandle = vb->lockCollections(key);
    if (!cHandle.valid()) {
        return "collection_unknown";
    }

    auto res = vb->fetchValidValue(
            rlh, WantsDeleted::Yes, TrackReference::No, cHandle);
    auto* v = res.storedValue;
    if (v) {
        if (VBucket::isLogicallyNonExistent(*v, cHandle)) {
            vb->ht.cleanupIfTemporaryItem(res.lock, *v);
            return "item_deleted";
        }

        if (diskItem.getFlags() != v->getFlags()) {
            return "flags_mismatch";
        }

        if (v->isResident()) {
            if (diskItem.getDataType() != v->getDatatype()) {
                auto d1 = diskItem.getDataType();
                auto d2 = v->getDatatype();

                using cb::mcbp::datatype::is_snappy;
                if (is_snappy(d1) && !is_snappy(d2)) {
                    diskItem.decompressValue();
                } else if (!is_snappy(d1) && is_snappy(d2)) {
                    diskItem.compressValue();
                }

                if (diskItem.getDataType() != v->getDatatype()) {
                    return fmt::format(
                            "datatype_mismatch: {} vs {}",
                            cb::mcbp::datatype::to_string(
                                    diskItem.getDataType()),
                            cb::mcbp::datatype::to_string(v->getDatatype()));
                }
            }

            if (diskItem.getNBytes() != v->getValue()->valueSize()) {
                return fmt::format("data_size_mismatch: {} vs {}",
                                   diskItem.getNBytes(),
                                   v->getValue()->valueSize());
            }

            if (diskItem.getRevSeqno() != v->getRevSeqno()) {
                return fmt::format("revseqno_mismatch: {} vs {}",
                                   diskItem.getRevSeqno(),
                                   v->getRevSeqno());
            }

            if (diskItem.getCas() != v->getCas()) {
                return fmt::format("cas_mismatch: {} vs {}",
                                   diskItem.getCas(),
                                   v->getCas());
            }

            if (memcmp(diskItem.getData(),
                       v->getValue()->getData(),
                       diskItem.getNBytes())) {
                return "data_mismatch";
            }
        }

        return "valid";
    }
    return "item_deleted";
}

cb::engine_errc KVBucket::deleteItem(
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        CookieIface* cookie,
        std::optional<cb::durability::Requirements> durability,
        ItemMetaData* itemMeta,
        mutation_descr_t& mutInfo) {
    Expects(cookie);
    auto lr = operationPrologue(vbucket,
                                *cookie,
                                {vbucket_state_active},
                                IsMutationOp::Yes,
                                __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    // Yield if checkpoint's full - The call also wakes up the mem recovery task
    if (isCheckpointMemoryStateFull(verifyCheckpointMemoryState())) {
        return cb::engine_errc::temporary_failure;
    }

    cb::engine_errc result;
    {
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(*cookie,
                                                    cHandle.getManifestUid());
            return cb::engine_errc::unknown_collection;
        }

        result = vb->deleteItem(rlh,
                                cas,
                                cookie,
                                engine,
                                durability,
                                itemMeta,
                                mutInfo,
                                cHandle);
    }

    if (durability) {
        vb->notifyActiveDMOfLocalSyncWrite();
    }

    return result;
}

cb::engine_errc KVBucket::deleteWithMeta(const DocKeyView& key,
                                         uint64_t& cas,
                                         uint64_t* seqno,
                                         Vbid vbucket,
                                         CookieIface* cookie,
                                         PermittedVBStates permittedVBStates,
                                         CheckConflicts checkConflicts,
                                         const ItemMetaData& itemMeta,
                                         GenerateBySeqno genBySeqno,
                                         GenerateCas generateCas,
                                         uint64_t bySeqno,
                                         ExtendedMetaData* emd,
                                         DeleteSource deleteSource,
                                         EnforceMemCheck enforceMemCheck) {
    Expects(cookie);
    auto lr = operationPrologue(
            vbucket, *cookie, permittedVBStates, IsMutationOp::Yes, __func__);
    if (!lr) {
        return lr.error();
    }
    auto [vb, rlh] = std::move(*lr);

    //check for the incoming item's CAS validity
    if (!Item::isValidCas(itemMeta.cas)) {
        return cb::engine_errc::cas_value_invalid;
    }

    // To differentiate between a Replication and a non-replication setWithMeta,
    // we know that genBySeqno will always be set to no for a replication
    // stream and yes otherwise.
    const bool isReplication = genBySeqno == GenerateBySeqno::No;
    InvalidCasStrategy strategy = getHlcInvalidStrategy(isReplication);
    if (!vb->isValidCas(itemMeta.cas)) {
        ++stats.numInvalidCas;
        if (strategy == InvalidCasStrategy::Error) {
            return cb::engine_errc::cas_value_invalid;
        }
        if (strategy == InvalidCasStrategy::Replace) {
            ++stats.numCasRegenerated;
            generateCas = GenerateCas::Yes;
        }
    }

    {
        // hold collections read lock for duration of delete
        auto cHandle = vb->lockCollections(key);
        if (!cHandle.valid()) {
            engine.setUnknownCollectionErrorContext(*cookie,
                                                    cHandle.getManifestUid());
            return cb::engine_errc::unknown_collection;
        }

        return vb->deleteWithMeta(rlh,
                                  cas,
                                  seqno,
                                  cookie,
                                  engine,
                                  checkConflicts,
                                  itemMeta,
                                  genBySeqno,
                                  generateCas,
                                  bySeqno,
                                  cHandle,
                                  deleteSource,
                                  enforceMemCheck);
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

bool KVBucket::isExpPagerEnabled() {
    return expiryPagerTask->isEnabled();
}

bool KVBucket::isWarmupLoadingData() const {
    return false;
}

bool KVBucket::isPrimaryWarmupLoadingData() const {
    return false;
}

bool KVBucket::isWarmupOOMFailure() const {
    return false;
}

cb::engine_errc KVBucket::doWarmupStats(const AddStatFn& add_stat,
                                        CookieIface& cookie) const {
    return cb::engine_errc::no_such_key;
}

bool KVBucket::hasWarmupSetVbucketStateFailed() const {
    return false;
}

bool KVBucket::maybeWaitForVBucketWarmup(CookieIface* cookie) {
    return false;
}

bool KVBucket::isMemUsageAboveBackfillThreshold() {
    return !engine.getMemoryTracker().isBelowBackfillThreshold();
}

// Trigger memory reduction (ItemPager) if we've exceeded the high watermark.
void KVBucket::checkAndMaybeFreeMemory() {
    if (engine.getMemoryTracker().needsToFreeMemory()) {
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

void KVBucket::wakeUpStrictItemPager() {
    auto* pager = dynamic_cast<ItemPager*>(itemPagerTask.get());
    Expects(pager);
    pager->wakeUp();
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
    Expects(!isCrossBucketHtQuotaSharing());
    ExecutorPool::get()->cancel(itemPagerTask->getId());
}

void KVBucket::wakeItemFreqDecayerTask() {
    if (crossBucketHtQuotaSharing) {
        // Run the cross bucket decayer, which is going to wakeup the
        // ItemFreqDecayerTasks of all buckets sharing memory.
        ItemFreqDecayerTaskManager::get().getCrossBucketDecayer()->schedule();
    } else {
        auto& t = dynamic_cast<ItemFreqDecayerTask&>(*itemFreqDecayerTask);
        t.wakeup();
    }
}

void KVBucket::itemFrequencyCounterSaturated() {
    wakeItemFreqDecayerTask();
}

void KVBucket::enableAccessScannerTask() {
    auto acLock = accessScanner.wlock();
    if (!acLock->enabled) {
        acLock->enabled = true;

        if (acLock->sleeptime != 0) {
            ExecutorPool::get()->cancel(acLock->task);
        }

        size_t alogSleepTime = engine.getConfiguration().getAlogSleepTime();
        acLock->sleeptime = alogSleepTime * 60;
        if (acLock->sleeptime != 0) {
            ExTask task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    acLock->sleeptime,
                                                    true);
            acLock->task = ExecutorPool::get()->schedule(task);
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
    auto acLock = accessScanner.wlock();
    if (acLock->enabled) {
        ExecutorPool::get()->cancel(acLock->task);
        acLock->sleeptime = 0;
        acLock->enabled = false;
    } else {
        EP_LOG_DEBUG_RAW("Access scanner already disabled!");
    }
}

void KVBucket::setAccessScannerSleeptime(size_t val, bool useStartTime) {
    auto acLock = accessScanner.wlock();

    if (acLock->enabled) {
        if (acLock->sleeptime != 0) {
            ExecutorPool::get()->cancel(acLock->task);
        }

        // store sleeptime in seconds
        acLock->sleeptime = val * 60;
        if (acLock->sleeptime != 0) {
            auto task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    acLock->sleeptime,
                                                    useStartTime);
            acLock->task = ExecutorPool::get()->schedule(task);
        }
    }
}

void KVBucket::resetAccessScannerStartTime() {
    auto acLock = accessScanner.wlock();

    if (acLock->enabled) {
        if (acLock->sleeptime != 0) {
            ExecutorPool::get()->cancel(acLock->task);
            // re-schedule task according to the new task start hour
            ExTask task =
                    std::make_shared<AccessScanner>(*this,
                                                    engine.getConfiguration(),
                                                    stats,
                                                    acLock->sleeptime,
                                                    true);
            acLock->task = ExecutorPool::get()->schedule(task);
        }
    }
}

void KVBucket::enableItemCompressor() {
    itemCompressorTask = std::make_shared<ItemCompressorTask>(engine, stats);
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

size_t KVBucket::pauseResumeVisit(PauseResumeVBVisitor& visitor,
                                  size_t currentPosition,
                                  gsl::span<Vbid> vbsToVisit) {
    for (; currentPosition < vbsToVisit.size(); ++currentPosition) {
        VBucketPtr vb = getVBucket(vbsToVisit[currentPosition]);
        if (vb) {
            bool visitorPaused = !visitor.visit(*vb);
            if (visitorPaused) {
                break;
            }
        }
    }
    return currentPosition;
}

KVBucket::Position KVBucket::startPosition() const
{
    return KVBucket::Position(Vbid(0));
}

KVBucket::Position KVBucket::endPosition() const
{
    return KVBucket::Position(Vbid(vbMap.getSize()));
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

void KVBucket::addKVStoreStats(const AddStatFn& add_stat, CookieIface& cookie) {
    for (const auto& shard : vbMap.shards) {
        /* Add the different KVStore instances into a set and then
         * retrieve the stats from each instance separately. This
         * is because CouchKVStore has separate read only and read
         * write instance.
         */
        std::set<const KVStoreIface*> underlyingSet;
        underlyingSet.insert(shard->getRWUnderlying());

        for (auto* store : underlyingSet) {
            store->addStats(add_stat, cookie);
        }
    }
}

void KVBucket::addKVStoreTimingStats(const AddStatFn& add_stat,
                                     CookieIface& cookie) {
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
    std::unique_lock<std::mutex> vbset(vbsetMutex, std::try_to_lock);
    if (!vbset.owns_lock()) {
        return TaskStatus::Reschedule; // Reschedule a vbucket rollback task.
    }

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
    std::unique_lock wlh(vb->getStateLock());
    if ((vb->getState() == vbucket_state_replica) ||
        (vb->getState() == vbucket_state_pending)) {
        // Before rollback gets busy with tidying hash-tables and clearing
        // checkpoints, ensure DCP streams are closed
        engine.getDcpConnMap().closeStreamsDueToRollback(vbid);

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
                        epVb.postProcessRollback(
                                wlh, result, prevHighSeqno, *this);
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
            return TaskStatus::Complete;
        }
        EP_LOG_WARN("{} Aborting rollback as reset of the vbucket failed",
                    vbid);
        return TaskStatus::Abort;
    }
    EP_LOG_WARN("{} Rollback not supported on the vbucket state {}",
                vbid,
                VBucket::toString(vb->getState()));
    return TaskStatus::Abort;
}

void KVBucket::attemptToFreeMemory() {
    wakeUpStrictItemPager();
}

void KVBucket::wakeUpCheckpointMemRecoveryTask() {
    for (const auto& task : chkRemovers) {
        if (task) {
            task->wakeup();
        }
    }
}

void KVBucket::wakeUpChkRemoversAndGetNotified(
        const std::shared_ptr<cb::Waiter>& waiter, size_t count) {
    for (size_t i = chkRemovers.size(); i < count; i++) {
        // Signal immediatley if we are not going to be able to wake up the
        // requested number of checkpoint removers.
        waiter->signal();
    }
    size_t toWake = std::min(chkRemovers.size(), count);
    for (size_t i = 0; i < toWake; i++) {
        chkRemovers[i]->wakeupAndGetNotified(waiter);
    }
}

void KVBucket::runWorkloadMonitor() {
    workloadMonitorTask->execute("");
}

void KVBucket::runDefragmenterTask() {
    defragmenterTask->execute("");
}

std::chrono::milliseconds KVBucket::getDefragmenterTaskSleepTime() const {
    if (!defragmenterTask) {
        return {};
    }

    return std::static_pointer_cast<DefragmenterTask>(defragmenterTask)->getCurrentSleepTime();
}

void KVBucket::runItemFreqDecayerTask() {
    itemFreqDecayerTask->execute("");
}

bool KVBucket::runAccessScannerTask() {
    return ExecutorPool::get()->wakeAndWait(accessScanner.rlock()->task);
}

void KVBucket::runVbStatePersistTask(Vbid vbid) {
    persistVBState(vbid);
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
        EP_LOG_WARN("KVBucket::forceMaxCas {} current:{}, new:{}",
                    vbucket,
                    vb->getMaxCas(),
                    cas);
        vb->forceMaxCas(cas);
        return cb::engine_errc::success;
    }
    return cb::engine_errc::not_my_vbucket;
}

std::ostream& operator<<(std::ostream& os, const KVBucket::Position& pos) {
    os << pos.vbucket_id;
    return os;
}
cb::engine_errc KVBucketIface::prepareForPause(
        folly::CancellationToken cancellationToken) {
    // By default nothing to do.
    return cb::engine_errc::success;
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
            flusher->notifyFlushEvent(*vb);
        }
    }
}

void KVBucket::notifyReplication(const Vbid vbid, queue_op op) {
    engine.getDcpConnMap().notifyVBConnections(vbid, op);
}

void KVBucket::setInitialMFU(uint8_t mfu) {
    initialMfuValue = mfu;
}

uint8_t KVBucket::getInitialMFU() const {
    return initialMfuValue;
}

void KVBucket::initializeExpiryPager(Configuration& config) {
    expiryPagerTask = std::make_shared<ExpiredItemPager>(
            engine,
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

void KVBucket::initializeInitialMfuUpdater(Configuration& config) {
    initialMfuUpdaterTask = std::make_shared<InitialMFUTask>(engine);
    ExecutorPool::get()->schedule(initialMfuUpdaterTask);

    config.addAndNotifyValueChangedCallback(
            "item_eviction_strategy", [this](std::string_view) {
                // Task requires to be woken up manually when the eviction
                // strategy changes.
                ExecutorPool::get()->wake(initialMfuUpdaterTask->getId());
            });
}

cb::engine_error KVBucket::setCollections(std::string_view manifest,
                                          CookieIface* cookie) {
    // Only allow a new manifest once warmup has progressed past vbucket warmup
    // 1) This means any prior manifest has been loaded
    // 2) All vbuckets can have the new manifest applied
    if (cookie && maybeWaitForVBucketWarmup(cookie)) {
        EP_LOG_INFO("KVBucket::setCollections blocking for warmup cookie:{}",
                    static_cast<const void*>(cookie));
        return {cb::engine_errc::would_block,
                "KVBucket::setCollections waiting for warmup"};
    }

    // Inhibit VB state changes whilst updating the vbuckets
    std::lock_guard<std::mutex> lh(vbsetMutex);
    // Lock all VB states individually. While the VB state cannot be changed
    // under the vbsetMutex lock, we do need to take all locks here so that we
    // pass the locks down to functions that expect a VBucketStateLockRef.
    // In addition, we take these locks here because:
    // 1) vbStateLock must be locked after vbsetMutex
    // 2) vbStateLock must be locked before the manifest is locked in update()
    auto vbStateLocks = lockAllVBucketStates();

    auto status = collectionsManager->update(vbStateLocks, *this, manifest);
    Expects(status.code() != cb::engine_errc::would_block &&
            "update can't return would block");
    if (status.code() != cb::engine_errc::success) {
        EP_LOG_WARN("KVBucket::setCollections error:{} {}",
                    status.code().value(),
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

std::pair<uint64_t, std::optional<Collections::CollectionMetaData>>
KVBucket::getCollectionEntry(CollectionID cid) const {
    return collectionsManager->getCollectionEntry(cid);
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

InvalidCasStrategy KVBucket::parseHlcInvalidStrategy(std::string_view strat) {
    using namespace std::string_view_literals;
    if (strat == "error"sv) {
        return InvalidCasStrategy::Error;
    }
    if (strat == "ignore"sv) {
        return InvalidCasStrategy::Ignore;
    }
    if (strat == "replace"sv) {
        return InvalidCasStrategy::Replace;
    }
    throw std::invalid_argument(
            "parseHlcInvalidStrategy: invalid mode specified");
}

bool KVBucket::isCrossBucketHtQuotaSharing() const {
    return crossBucketHtQuotaSharing;
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
    return [&engine = this->engine](CookieIface* cookie,
                                    cb::engine_errc status) {
        if (status != cb::engine_errc::success) {
            // For non-success status codes clear the cookie's engine_specific;
            // as the operation is now complete. This ensures that any
            // subsequent call by the same cookie to store() is treated as a new
            // operation (and not the completion of the previous one).
            engine.clearEngineSpecific(*cookie);
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
    getCkptDestroyerTask(vbid)->queueForDestruction(std::move(checkpoints));
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

    if (getConfiguration().getCheckpointMaxSize() == 0) {
        autoConfigCheckpointMaxSize();
    }

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
    const auto maxSize = config.getMaxSize();
    const auto ckptMemRatio = checkpointMemoryRatio.load();
    const auto numCheckpointsPerVB = config.getMaxCheckpoints();

    const auto checkpointQuota = maxSize * ckptMemRatio;
    Expects(checkpointQuota >= 0);

    const size_t newComputedCheckpointMaxSize =
            checkpointQuota / numVBuckets / numCheckpointsPerVB;
    if (newComputedCheckpointMaxSize == 0) {
        EP_LOG_WARN_CTX(
                "KVBucket::autoConfigCheckpointMaxSize: "
                "newComputedCheckpointMaxSize is 0",
                {"max_size", maxSize},
                {"checkpoint_memory_ratio", ckptMemRatio},
                {"checkpoint_quota", checkpointQuota},
                {"num_vbuckets", numVBuckets},
                {"num_checkpoints_per_vb", numCheckpointsPerVB});
    }

    const auto flushMaxBytes = config.getFlushBatchMaxBytes();
    if (newComputedCheckpointMaxSize > flushMaxBytes) {
        EP_LOG_WARN(
                "KVBucket::autoConfigCheckpointMaxSize: "
                "checkpoint_computed_max_size {} can't cross "
                "flush_batch_max_bytes {}, capping to {}",
                newComputedCheckpointMaxSize,
                flushMaxBytes,
                flushMaxBytes);
    }
    const auto newSize = std::min(newComputedCheckpointMaxSize, flushMaxBytes);
    engine.getCheckpointConfig().setCheckpointMaxSize(newSize);

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
    const auto recoveryThreshold = getCMRecoveryUpperMarkBytes();
    const auto usage = stats.getCheckpointManagerEstimatedMemUsage() +
                       getCheckpointPendingDestructionMemoryUsage();

    if (usage < recoveryThreshold) {
        return CheckpointMemoryState::Available;
    }
    if (usage < checkpointQuota) {
        return isCMMemoryReductionRequired()
                       ? CheckpointMemoryState::HighAndNeedsRecovery
                       : CheckpointMemoryState::High;
    }
    return isCMMemoryReductionRequired()
                   ? CheckpointMemoryState::FullAndNeedsRecovery
                   : CheckpointMemoryState::Full;
}

KVBucket::CheckpointMemoryState KVBucket::verifyCheckpointMemoryState() {
    const auto state = getCheckpointMemoryState();
    switch (state) {
    case CheckpointMemoryState::Available:
    case CheckpointMemoryState::High:
    case CheckpointMemoryState::Full:
        break;
    case CheckpointMemoryState::HighAndNeedsRecovery:
    case CheckpointMemoryState::FullAndNeedsRecovery:
        wakeUpCheckpointMemRecoveryTask();
        break;
    }

    return state;
}

size_t KVBucket::getCMQuota() const {
    return stats.getMaxDataSize() * checkpointMemoryRatio;
}

size_t KVBucket::getCheckpointConsumerLimit() const {
    return getCMQuota() +
           engine.getDcpConsumerBufferRatio() * stats.getMaxDataSize();
}

size_t KVBucket::getCMRecoveryUpperMarkBytes() const {
    return getCMQuota() * checkpointMemoryRecoveryUpperMark;
}

size_t KVBucket::getCMRecoveryLowerMarkBytes() const {
    return getCMQuota() * checkpointMemoryRecoveryLowerMark;
}

size_t KVBucket::getRequiredCMMemoryReduction() const {
    const auto recoveryThreshold = getCMRecoveryUpperMarkBytes();
    const auto recoveryTarget = getCMRecoveryLowerMarkBytes();
    const auto cmUsage = stats.getCheckpointManagerEstimatedMemUsage();
    const auto pendingDealloc = getCheckpointPendingDestructionMemoryUsage();
    const auto usage = cmUsage + pendingDealloc;

    if (usage <= recoveryTarget) {
        return 0;
    }

    const size_t toClear = usage - recoveryTarget;

    if (toClear <= pendingDealloc) {
        return 0;
    }

    const size_t cmToClear = toClear - pendingDealloc;

    const auto toMB = [](size_t bytes) { return bytes / (1_MiB); };
    EP_LOG_DEBUG(
            "Triggering memory recovery as CM memory usage ({} MB) plus "
            "detached checkpoint usage ({} MB) ({} MB in total) exceeds the "
            "upper_mark ({}, {} MB) - total checkpoint quota {}, {} MB . "
            "Attempting to free {} MB of memory.",
            toMB(cmUsage),
            toMB(pendingDealloc),
            toMB(usage),
            checkpointMemoryRecoveryUpperMark,
            toMB(recoveryThreshold),
            checkpointMemoryRatio,
            toMB(getCMQuota()),
            toMB(cmToClear));

    return cmToClear;
}

bool KVBucket::isCMMemoryReductionRequired() const {
    return getRequiredCMMemoryReduction() > 0;
}

KVBucket::CheckpointDestroyer KVBucket::getCkptDestroyerTask(Vbid vbid) const {
    const auto locked = ckptDestroyerTasks.rlock();
    Expects(!locked->empty());
    return locked->at(vbid.get() % locked->size());
}

size_t KVBucket::getCheckpointPendingDestructionMemoryUsage() const {
    size_t memoryUsage = 0;
    const auto locked = ckptDestroyerTasks.rlock();
    for (const auto& task : *locked) {
        memoryUsage += task->getMemoryUsage();
    }
    return memoryUsage;
}

size_t KVBucket::getNumCheckpointsPendingDestruction() const {
    size_t count = 0;
    const auto locked = ckptDestroyerTasks.rlock();
    for (const auto& task : *locked) {
        count += task->getNumCheckpoints();
    }
    return count;
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

void KVBucket::createAndScheduleCheckpointDestroyerTasks() {
    // Cancel all the existing tasks.
    // Note: Running tasks will complete their execution before being destroyed.
    auto locked = ckptDestroyerTasks.wlock();
    for (const auto& task : *locked) {
        ExecutorPool::get()->cancel(task->getId(), true /*remove*/);
    }
    locked->clear();

    // Then recreate the pool.
    const auto num = engine.getConfiguration().getCheckpointDestructionTasks();
    for (size_t i = 0; i < num; ++i) {
        locked->push_back(std::make_shared<CheckpointDestroyerTask>(engine));
        ExecutorPool::get()->schedule(locked->back());
    }
}

void KVBucket::createAndScheduleCheckpointRemoverTasks() {
    const auto numChkRemovers =
            engine.getConfiguration().getCheckpointRemoverTaskCount();
    for (size_t id = 0; id < numChkRemovers; ++id) {
        auto task =
                std::make_shared<CheckpointMemRecoveryTask>(engine, stats, id);
        chkRemovers.emplace_back(task);
        ExecutorPool::get()->schedule(task);
    }
}

void KVBucket::createAndScheduleBucketQuotaChangeTask() {
    Expects(!bucketQuotaChangeTask);
    bucketQuotaChangeTask =
            std::make_shared<BucketQuotaChangeTask>(getEPEngine());
    ExecutorPool::get()->schedule(bucketQuotaChangeTask);
}

void KVBucket::addVbucketWithSeqnoPersistenceRequest(
        Vbid vbid, cb::time::steady_clock::time_point deadline) {
    if (seqnoPersistenceNotifyTask) {
        seqnoPersistenceNotifyTask->addVbucket(vbid, deadline);
    }
}

cb::time::steady_clock::time_point
KVBucket::getSeqnoPersistenceNotifyTaskWakeTime() const {
    // Added for unit-testing, so expect the task to exist
    Expects(seqnoPersistenceNotifyTask);
    return seqnoPersistenceNotifyTask->getWaketime();
}

std::pair<cb::engine_errc, cb::rangescan::Id> KVBucket::createRangeScan(
        CookieIface&,
        std::unique_ptr<RangeScanDataHandlerIFace>,
        const cb::rangescan::CreateParameters&) {
    return {cb::engine_errc::not_supported, {}};
}

cb::engine_errc KVBucket::continueRangeScan(
        CookieIface&, const cb::rangescan::ContinueParameters&) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc KVBucket::cancelRangeScan(Vbid,
                                          cb::rangescan::Id,
                                          CookieIface&) {
    return cb::engine_errc::not_supported;
}

void KVBucket::processBucketQuotaChange(size_t desiredQuota) {
    Expects(bucketQuotaChangeTask);
    bucketQuotaChangeTask->notifyNewQuotaChange(desiredQuota);
}

void KVBucket::processCompressionModeChange() {
    if (itemCompressorTask) {
        itemCompressorTask->notifyCompressionModeChange();
    }
}

float KVBucket::getMutationMemRatio() const {
    return mutationMemRatio;
}

void KVBucket::setMutationMemRatio(float ratio) {
    mutationMemRatio = ratio;
}

void KVBucket::setHistoryRetentionSeconds(std::chrono::seconds seconds) {
    if (historyRetentionSeconds.load() == seconds) {
        return;
    }

    const auto wasEnabled = isHistoryRetentionEnabled();

    for (auto& i : vbMap.shards) {
        KVShard* shard = i.get();
        shard->getRWUnderlying()->setHistoryRetentionSeconds(seconds);
    }
    historyRetentionSeconds = seconds;

    if (isHistoryRetentionEnabled() != wasEnabled) {
        postHistoryRetentionChange(!wasEnabled);
    }
}

std::chrono::seconds KVBucket::getHistoryRetentionSeconds() const {
    return historyRetentionSeconds;
}

void KVBucket::setHistoryRetentionBytes(size_t bytes) {
    if (historyRetentionBytes == bytes) {
        return;
    }

    const auto wasEnabled = isHistoryRetentionEnabled();

    for (auto& i : vbMap.shards) {
        KVShard* shard = i.get();
        shard->getRWUnderlying()->setHistoryRetentionBytes(bytes,
                                                           vbMap.getSize());
    }

    historyRetentionBytes = bytes;

    if (isHistoryRetentionEnabled() != wasEnabled) {
        postHistoryRetentionChange(!wasEnabled);
    }
}

void KVBucket::postHistoryRetentionChange(bool enabled) {
    // New mutations needs to be queued in a new checkpoint created with the
    // correct history flag.
    createNewActiveCheckpoints();
    if (continuousBackupEnabled) {
        if (enabled) {
            EP_LOG_INFO_RAW(
                    "History retention enabled, starting continuous backup");
        } else {
            EP_LOG_WARN_RAW(
                    "History retention disabled, stopping continuous backup");
        }
        // Continuous backup is not enabled at the KVStore level without
        // history retention. When history retention is enabled, we need to
        // persist the vbstate to trigger backup to start.
        persistVBState();
    }
}

size_t KVBucket::getHistoryRetentionBytes() const {
    return historyRetentionBytes;
}

bool KVBucket::isHistoryRetentionEnabled() const {
    return historyRetentionBytes > 0 ||
           historyRetentionSeconds.load().count() > 0;
}

void KVBucket::createNewActiveCheckpoints() {
    for (auto vbid : vbMap.getBuckets()) {
        auto vb = getVBucket(vbid);
        Expects(vb);
        {
            const auto l =
                    std::shared_lock<folly::SharedMutex>(vb->getStateLock());
            if (vb->getState() == vbucket_state_active) {
                vb->checkpointManager->createNewCheckpoint();
            }
        }
    }
}

void KVBucket::completeLoadingVBuckets() {
    for (auto& i : vbMap.shards) {
        KVShard* shard = i.get();
        shard->getRWUnderlying()->completeLoadingVBuckets();
    }
}

void KVBucket::setCompactionExpiryFetchInline(bool value) {
    compactionExpiryFetchInline = value;
}

bool KVBucket::isCompactionExpiryFetchInline() const {
    return compactionExpiryFetchInline;
}

void KVBucket::setContinuousBackupEnabled(bool enabled) {
    continuousBackupEnabled = enabled;
    // The new setting will take effect after the vbstate flush.
    persistVBState();
    if (enabled && !isHistoryRetentionEnabled()) {
        EP_LOG_WARN_RAW(
                "Continuous backup requires history retention to be enabled");
    }
}

bool KVBucket::isContinuousBackupEnabled() const {
    return continuousBackupEnabled;
}

size_t KVBucket::getNumCheckpointDestroyers() const {
    return ckptDestroyerTasks.rlock()->size();
}

KVBucket::ReplicationThrottleStatus KVBucket::getReplicationThrottleStatus() {
    if (getMemAvailableForReplication() > 0) {
        return ReplicationThrottleStatus::Process;
    }
    return disconnectReplicationAtOOM() ? ReplicationThrottleStatus::Disconnect
                                        : ReplicationThrottleStatus::Pause;
}

size_t KVBucket::getMemAvailableForReplication() {
    if (isCheckpointMemoryStateFull(verifyCheckpointMemoryState())) {
        return 0;
    }

    const auto& stats = engine.getEpStats();
    const auto memoryUsed = stats.getEstimatedTotalMemoryUsed();
    const auto bucketQuota = stats.getMaxDataSize();
    const auto replicationThreshold =
            cb::fractionOf(bucketQuota, mutationMemRatio);
    return memoryUsed <= replicationThreshold
                   ? (replicationThreshold - memoryUsed)
                   : 0;
}

const Configuration& KVBucket::getConfiguration() const {
    return engine.getConfiguration();
}

Configuration& KVBucket::getConfiguration() {
    return engine.getConfiguration();
}