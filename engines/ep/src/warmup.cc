/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "warmup.h"

#include "bucket_logger.h"
#include "callbacks.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_task.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "flusher.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "mutation_log.h"
#include "vb_visitors.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"
#include <executor/executorpool.h>
#include <phosphor/phosphor.h>
#include <platform/dirutils.h>
#include <platform/string_utilities.h>
#include <platform/timeutils.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <folly/lang/Assume.h>

struct WarmupCookie {
    WarmupCookie(EPBucket& s,
                 const Warmup& warmup,
                 StatusCallback<GetValue>& c,
                 MutationLog& log)
        : epstore(s), warmup(warmup), cb(c), log(log) {
    }
    EPBucket& epstore;
    const Warmup& warmup;
    StatusCallback<GetValue>& cb;
    MutationLog& log;
};

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Helper class used to insert data into the epstore                     //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public StatusCallback<GetValue> {
public:
    LoadStorageKVPairCallback(
            EPBucket& ep,
            Warmup& warmup,
            bool shouldCheckIfWarmupThresholdReached,
            WarmupState::State warmupState,
            std::optional<const std::chrono::steady_clock::duration>
                    deltaDeadlineFromNow = std::nullopt);

    void callback(GetValue& val) override;

    void updateDeadLine(std::chrono::steady_clock::time_point chunkStart) {
        if (deltaDeadlineFromNow) {
            deadline = (chunkStart + *deltaDeadlineFromNow);
        }
    }

private:
    bool shouldEject() const;
    EPStats& stats;
    EPBucket& epstore;
    Warmup& warmup;
    std::optional<const std::chrono::steady_clock::duration>
            deltaDeadlineFromNow;
    std::chrono::steady_clock::time_point deadline;

    /**
     * If true, after each K/V pair loaded check if the bucket has reached any
     * of the thresholds which require warmup to stop. LoadStorageKVPairCallback
     * will then stop and return control to scan (with status cancelled).
     */
    const bool shouldCheckIfWarmupThresholdReached{false};
    WarmupState::State warmupState;
};

using CacheLookupCallBackPtr = std::unique_ptr<StatusCallback<CacheLookup>>;

class LoadValueCallback : public StatusCallback<CacheLookup> {
public:
    LoadValueCallback(Warmup& warmup) : warmup(warmup) {
    }

    void callback(CacheLookup& lookup) override;

private:
    Warmup& warmup;
};

// Warmup Tasks ///////////////////////////////////////////////////////////////

class WarmupInitialize : public EpTask {
public:
    WarmupInitialize(EPBucket& st, Warmup* w)
        : EpTask(st.getEPEngine(), TaskId::WarmupInitialize, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return "Warmup - initialize";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Typically takes single-digits ms.
        return std::chrono::milliseconds(50);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupInitialize");
        _warmup->initialize();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupCreateVBuckets : public EpTask {
public:
    WarmupCreateVBuckets(EPBucket& st, uint16_t sh, Warmup* w)
        : EpTask(st.getEPEngine(), TaskId::WarmupCreateVBuckets, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - creating vbuckets: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // VB creation typically takes some 10s of milliseconds.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCreateVBuckets");
        _warmup->createVBuckets(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingCollectionCounts : public EpTask {
public:
    WarmupLoadingCollectionCounts(EPBucket& st, uint16_t sh, Warmup& w)
        : EpTask(st.getEPEngine(),
                 TaskId::WarmupLoadingCollectionCounts,
                 0,
                 false),
          shardId(sh),
          warmup(w) {
        warmup.addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return "Warmup - loading collection counts: shard " +
               std::to_string(shardId);
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This task has to open each VB's data-file and (certainly for
        // couchstore) read a small document per defined collection
        return std::chrono::seconds(10);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingCollectionCounts");
        warmup.loadCollectionStatsForShard(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
};

class WarmupEstimateDatabaseItemCount : public EpTask {
public:
    WarmupEstimateDatabaseItemCount(EPBucket& st, uint16_t sh, Warmup* w)
        : EpTask(st.getEPEngine(),
                 TaskId::WarmupEstimateDatabaseItemCount,
                 0,
                 false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - estimate item count: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Typically takes a few 10s of milliseconds (need to open kstore files
        // and read statistics.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarpupEstimateDatabaseItemCount");
        _warmup->estimateDatabaseItemCount(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

/**
 * Warmup task which loads any prepared SyncWrites which are not yet marked
 * as Committed (or Aborted) from disk.
 */
class WarmupLoadPreparedSyncWrites : public EpTask {
public:
    WarmupLoadPreparedSyncWrites(EventuallyPersistentEngine& engine,
                                 uint16_t shard,
                                 Warmup& warmup)
        : EpTask(engine, TaskId::WarmupLoadPreparedSyncWrites, 0, false),
          shardId(shard),
          warmup(warmup),
          description("Warmup - loading prepared SyncWrites: shard " +
                      std::to_string(shardId)) {
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Runtime is a function of how many prepared sync writes exist in the
        // buckets for this shard - can be minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::minutes(10);
    }

    bool run() override {
        TRACE_EVENT1("ep-engine/task",
                     "WarmupLoadPreparedSyncWrites",
                     "shard",
                     shardId);
        warmup.loadPreparedSyncWrites(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
    const std::string description;
};

/**
 * Warmup task which moves all warmed-up VBuckets into the bucket's vbMap
 */
class WarmupPopulateVBucketMap : public EpTask {
public:
    WarmupPopulateVBucketMap(EPBucket& st, uint16_t shard, Warmup& warmup)
        : EpTask(st.getEPEngine(), TaskId::WarmupPopulateVBucketMap, 0, false),
          shardId(shard),
          warmup(warmup),
          description("Warmup - populate VB Map: shard " +
                      std::to_string(shardId)) {
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // We have to flush for each vBucket in the shard so runtime could be
        // a few ms easily depending on IO and number of shards
        return std::chrono::seconds(1);
    }

    bool run() override {
        TRACE_EVENT1(
                "ep-engine/task", "WarmupPopulateVBucketMap", "shard", shardId);
        warmup.populateVBucketMap(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
    const std::string description;
};

class WarmupBackfillTask;
/**
 * Implementation of a PauseResumeVBVisitor to be used for the
 * WarmupBackfillTask WarmupVbucketVisitor keeps record of the current vbucket
 * being backfilled and the current state of scan context.
 */
class WarmupVbucketVisitor : public PauseResumeVBVisitor {
public:
    WarmupVbucketVisitor(EPBucket& ep, const WarmupBackfillTask& task)
        : ep(ep), backfillTask(task) {
    }

    /**
     * Informs the visitor that it is about to start visiting one or more
     * vbuckets - so we can set the deadline for when the visitor should
     * pause (inside the kvCallback).
     */
    void begin() {
        chunkStart = std::chrono::steady_clock::now();
    }

    /**
     * Visiting was cancelled, e.g VB deleted
     */
    void cancel() {
        currentScanCtx.reset();
    }

    bool visit(VBucket& vb) override;

private:
    EPBucket& ep;
    const WarmupBackfillTask& backfillTask;
    std::unique_ptr<BySeqnoScanContext> currentScanCtx;
    /// Time when this chunk of work (task run()) begin, used to determine when
    /// the visitor should yield.
    std::chrono::steady_clock::time_point chunkStart;
};

/**
 * Abstract Task to perform a backfill during warmup on a shards vbuckets, in a
 * pause-resume fashion.
 *
 * The task will also transition the warmup's state to the next warmup state
 * once threadTaskCount has meet the total number of shards.
 */
class WarmupBackfillTask : public EpTask {
public:
    /**
     * Constructor of WarmupBackfillTask
     * @param bucket EPBucket the task is back filling for
     * @param shardId of the shard we're performing the backfill on
     * @param warmup ref to the warmup class the backfill is for
     * @param taskId of the the backfill that is to be performed
     * @param taskDesc description of the task
     * @param threadTaskCount ref to atomic size_t that keeps count of how many
     * of the per tasks shards have been completed. If this value is equal to
     * the number of shards the run() method will transition warmup to the next
     * state.
     */
    WarmupBackfillTask(EPBucket& bucket,
                       size_t shardId,
                       Warmup& warmup,
                       TaskId taskId,
                       std::string_view taskDesc,
                       std::atomic<size_t>& threadTaskCount)
        : EpTask(bucket.getEPEngine(), taskId, 0, true),
          warmup(warmup),
          shardId(shardId),
          description(fmt::format("Warmup - {} shard {}", taskDesc, shardId)),
          // Max expected duration is the chunk duration this task yields after,
          // plus additional margin to account for the time taken to process
          // the last item, and also to only catch truly "slow" outlying
          // runs - 20% margin.
          maxExpectedRuntime((engine->getConfiguration()
                                      .getWarmupBackfillScanChunkDuration() *
                              120) /
                             100),
          currentNumBackfillTasks(threadTaskCount),
          visitor(bucket, *this),
          vbsToVisit(warmup.shardVbIds[shardId]),
          currentVb(vbsToVisit.begin()) {
        warmup.addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return maxExpectedRuntime;
    }

    bool run() override {
        TRACE_EVENT1(
                "ep-engine/task", "WarmupBackfillTask", "shard", getShardId());
        if (vbsToVisit.empty() || engine->getEpStats().isShutdown) {
            // Technically "isShutdown" being true doesn't equate to a
            // successful task finish, however if we are shutting down we want
            // warmup to advance and be considered "done".
            finishTask(true);
            return false;
        }

        visitor.begin();
        try {
            for (; currentVb != vbsToVisit.end(); ++currentVb) {
                auto vb = warmup.tryAndGetVbucket(*currentVb);
                if (vb) {
                    if (!visitor.visit(*vb)) {
                        break;
                    }
                } else {
                    visitor.cancel();
                }
            }
        } catch (const std::exception& e) {
            EP_LOG_CRITICAL(
                    "Warmup({}) WarmupBackfillTask::run(): caught exception "
                    "while running backfill in State::{} - aborting warmup: {}",
                    warmup.getName(),
                    to_string(warmup.getWarmupState()),
                    e.what());
            finishTask(false);
            return false;
        }

        if (currentVb == vbsToVisit.end()) {
            finishTask(true);
            return false;
        }

        return true;
    }

    size_t getShardId() const {
        return shardId;
    }

    Warmup& getWarmup() const {
        return warmup;
    }

    virtual WarmupState::State getNextState() const = 0;
    virtual ValueFilter getValueFilter() const = 0;

    /**
     * Function for sub-class to determine if LoadStorageKVPairCallback should
     * check hasWarmupReachedThresholds after every loaded key.
     */
    virtual bool shouldCheckIfWarmupThresholdReached() const = 0;

    virtual CacheLookupCallBackPtr makeCacheLookupCallback() const = 0;

protected:
    Warmup& warmup;

private:
    /**
     * Finish the current task, transitioning to the next phase of warmup if
     * backfill has successfully finished for all shards.
     * @param success True if task finished successfully, else false.
     */
    void finishTask(bool success) {
        warmup.removeFromTaskSet(uid);
        if (!success) {
            // Unsuccessful task runs don't count against required task
            // completions.
            return;
        }
        // If this is the last backfill task (all shards have finished) then
        // move us to the next state.
        if (++currentNumBackfillTasks ==
            engine->getKVBucket()->getVBuckets().getNumShards()) {
            warmup.transition(getNextState());
        }
    }

    const size_t shardId;
    const std::string description;
    /// After how long should this task yield, allowing other tasks to run?
    const std::chrono::milliseconds maxExpectedRuntime;
    std::atomic<size_t>& currentNumBackfillTasks;
    WarmupVbucketVisitor visitor;
    const std::vector<Vbid> vbsToVisit;
    std::vector<Vbid>::const_iterator currentVb;
};

bool WarmupVbucketVisitor::visit(VBucket& vb) {
    auto* kvstore = ep.getROUnderlyingByShard(backfillTask.getShardId());

    if (!currentScanCtx) {
        const auto chunkDuration = std::chrono::milliseconds{
                ep.getEPEngine()
                        .getConfiguration()
                        .getWarmupBackfillScanChunkDuration()};
        auto kvLookup = std::make_unique<LoadStorageKVPairCallback>(
                ep,
                backfillTask.getWarmup(),
                backfillTask.shouldCheckIfWarmupThresholdReached(),
                backfillTask.getWarmup().getWarmupState(),
                chunkDuration);
        currentScanCtx = kvstore->initBySeqnoScanContext(
                std::move(kvLookup),
                backfillTask.makeCacheLookupCallback(),
                vb.getId(),
                0,
                DocumentFilter::NO_DELETES,
                backfillTask.getValueFilter(),
                SnapshotSource::Head);
        if (!currentScanCtx) {
            if (vb.getState() == vbucket_state_replica &&
                vb.getHighSeqno() == 0 && vb.isBucketCreation()) {
                // Note: ideally we could determine a more detailed reason as to
                // why initBySeqnoScanContext failed. Here the assumption is
                // that given that earlier the VBucket existed (was added via
                // warmup populate VBMap) and now we have a creating VBucket,
                // it has rolled back.
                EP_LOG_INFO(
                        "WarmupVbucketVisitor::visit(): {} shardId:{} "
                        "tolerating failure of initBySeqnoScanContext and "
                        "assuming vbucket rolled back during warmup.",
                        vb.getId(),
                        backfillTask.getShardId());
                return true;
            }
            throw std::runtime_error(fmt::format(
                    "WarmupVbucketVisitor::visit(): {} shardId:{} failed to "
                    "create BySeqnoScanContext, for backfill task:'{}'",
                    vb.getId(),
                    backfillTask.getShardId(),
                    backfillTask.getDescription()));
        }
    }
    // Update backfill deadline for when we need to next pause
    auto& kvCallback = dynamic_cast<LoadStorageKVPairCallback&>(
            currentScanCtx->getValueCallback());
    kvCallback.updateDeadLine(chunkStart);

    auto scanStatus = kvstore->scan(*currentScanCtx);
    switch (scanStatus) {
    case ScanStatus::Cancelled: {
        const auto cacheCbStatus =
                currentScanCtx->getCacheCallback().getStatus();
        const auto valueCbStatus =
                currentScanCtx->getValueCallback().getStatus();

        // One callback must provide a !success status
        if (cacheCbStatus == cb::engine_errc::success &&
            valueCbStatus == cb::engine_errc::success) {
            throw std::logic_error(
                    "WarmupVbucketVisitor::visit scan cancelled but both "
                    "callbacks report success");
        }

        auto logCancelled = [this, &vb](cb::engine_errc status,
                                        std::string_view who) {
            if (status == cb::engine_errc::success) {
                return;
            }
            if (status != cb::engine_errc::cancelled &&
                status != cb::engine_errc::not_my_vbucket) {
                throw std::logic_error(
                        "WarmupVbucketVisitor::visit unexpected callback "
                        "status:" +
                        cb::to_string(status));
            }
            EP_LOG_INFO_CTX("WarmupVbucketVisitor::visit(): scan cancelled",
                            {"who", who},
                            {"phase", backfillTask.getWarmup().getName()},
                            {"vb", vb.getId().to_string()},
                            {"shard", backfillTask.getShardId()},
                            {"lastReadSeqno", currentScanCtx->lastReadSeqno},
                            {"status", status});
        };

        logCancelled(valueCbStatus, "ValueCallback");
        logCancelled(cacheCbStatus, "CacheCallback");

        [[fallthrough]]; // fallthrough to reset currentScanCtx and return true
    }
    case ScanStatus::Success:
        // Finished or Cancelled backfill for this vbucket so we need to reset
        // currentScanCtx ready for any continuation with the next vbucket.
        currentScanCtx.reset();
        return true;
    case ScanStatus::Yield:
        // Yield is always due to the scan time deadline being reached.
        return false;
    case ScanStatus::Failed:
        // Disk error scanning keys - cannot continue warmup.
        currentScanCtx.reset();
        throw std::runtime_error(fmt::format(
                "WarmupVbucketVisitor::visit(): {} shardId:{} failed to "
                "scan BySeqnoScanContext, for backfill task:'{}' {}",
                vb.getId(),
                backfillTask.getShardId(),
                backfillTask.getDescription(),
                scanStatus));
    }
    folly::assume_unreachable();
}

/**
 * [Value-eviction only]
 * Task that loads all keys into memory for each vBucket in the given shard in a
 * pause resume fashion.
 */
class WarmupKeyDump : public WarmupBackfillTask {
public:
    WarmupKeyDump(EPBucket& bucket,
                  size_t shardId,
                  Warmup& warmup,
                  std::atomic<size_t>& threadTaskCount)
        : WarmupBackfillTask(bucket,
                             shardId,
                             warmup,
                             TaskId::WarmupKeyDump,
                             "key dump",
                             threadTaskCount) {
    }

    WarmupState::State getNextState() const override {
        if (warmup.hasReachedThreshold()) {
            return WarmupState::State::Done;
        }
        return WarmupState::State::CheckForAccessLog;
    }

    ValueFilter getValueFilter() const override {
        return ValueFilter::KEYS_ONLY;
    }

    bool shouldCheckIfWarmupThresholdReached() const override {
        // KeyDump has bespoke code in LoadStorageKVPairCallback which doesn't
        // use hasWarmupReachedThresholds.
        return false;
    }

    CacheLookupCallBackPtr makeCacheLookupCallback() const override {
        return std::make_unique<NoLookupCallback>();
    }
};

class WarmupCheckforAccessLog : public EpTask {
public:
    WarmupCheckforAccessLog(EPBucket& st, Warmup* w)
        : EpTask(st.getEPEngine(), TaskId::WarmupCheckforAccessLog, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return "Warmup - check for access log";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Checking for the access log is a disk task (so can take a variable
        // amount of time), however it should be relatively quick as we are
        // just checking files exist.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCheckForAccessLog");
        _warmup->checkForAccessLog();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupLoadAccessLog : public EpTask {
public:
    WarmupLoadAccessLog(EPBucket& st, uint16_t sh, Warmup* w)
        : EpTask(st.getEPEngine(), TaskId::WarmupLoadAccessLog, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading access log: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Runtime is a function of the number of keys in the access log files;
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadAccessLog");
        if (_warmup->loadingAccessLog(_shardId)) {
            return true;
        }
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

/**
 * [Full-eviction only]
 * Task that loads both keys and values into memory for each vBucket in the
 * given shard in a pause resume fashion.
 */
class WarmupLoadingKVPairs : public WarmupBackfillTask {
public:
    WarmupLoadingKVPairs(EPBucket& bucket,
                         size_t shardId,
                         Warmup& warmup,
                         std::atomic<size_t>& threadTaskCount)
        : WarmupBackfillTask(bucket,
                             shardId,
                             warmup,
                             TaskId::WarmupLoadingKVPairs,
                             "loading KV Pairs",
                             threadTaskCount) {
    }

    WarmupState::State getNextState() const override {
        return WarmupState::State::Done;
    }

    ValueFilter getValueFilter() const override {
        return warmup.store.getValueFilterForCompressionMode();
    }

    bool shouldCheckIfWarmupThresholdReached() const override {
        return warmup.store.getItemEvictionPolicy() == EvictionPolicy::Full;
    }

    CacheLookupCallBackPtr makeCacheLookupCallback() const override {
        return std::make_unique<LoadValueCallback>(warmup);
    }
};

/**
 * Task that loads values into memory for each vBucket in the given shard in a
 * pause resume fashion.
 */
class WarmupLoadingData : public WarmupBackfillTask {
public:
    WarmupLoadingData(EPBucket& bucket,
                      size_t shardId,
                      Warmup& warmup,
                      std::atomic<size_t>& threadTaskCount)
        : WarmupBackfillTask(bucket,
                             shardId,
                             warmup,
                             TaskId::WarmupLoadingData,
                             "loading data",
                             threadTaskCount) {
    }

    WarmupState::State getNextState() const override {
        return WarmupState::State::Done;
    }

    ValueFilter getValueFilter() const override {
        return warmup.store.getValueFilterForCompressionMode();
    }

    bool shouldCheckIfWarmupThresholdReached() const override {
        return true;
    }

    CacheLookupCallBackPtr makeCacheLookupCallback() const override {
        return std::make_unique<LoadValueCallback>(warmup);
    }
};

class WarmupCompletion : public EpTask {
public:
    WarmupCompletion(EPBucket& st, Warmup* w)
        : EpTask(st.getEPEngine(), TaskId::WarmupCompletion, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() const override {
        return "Warmup - completion";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This task should be very quick - just the final warmup steps.
        return std::chrono::milliseconds(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCompletion");
        _warmup->done();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

static bool batchWarmupCallback(Vbid vbId,
                                const std::set<StoredDocKey>& fetches,
                                void* arg) {
    Expects(arg);
    auto& c = *static_cast<WarmupCookie*>(arg);

    if (!c.warmup.hasReachedThreshold()) {
        vb_bgfetch_queue_t items2fetch;
        for (auto& key : fetches) {
            // Access log only records Committed keys, therefore construct
            // DiskDocKey with pending == false.
            DiskDocKey diskKey{key, /*prepared*/ false};
            // Deleted below via a unique_ptr in the next loop
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items2fetch[diskKey];
            bg_itm_ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
                    nullptr, c.epstore.getValueFilterForCompressionMode(), 0));
        }

        c.epstore.getROUnderlying(vbId)->getMulti(
                vbId,
                items2fetch,
                c.epstore.getEPEngine().getCreateItemCallback());

        // applyItem controls the  mode this loop operates in.
        // true we will attempt the callback (attempt a HashTable insert)
        // false we don't attempt the callback
        // in both cases the loop must delete the VBucketBGFetchItem we
        // allocated above.
        bool applyItem = true;
        for (auto& items : items2fetch) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items.second;
            if (applyItem) {
                if (bg_itm_ctx.value.getStatus() == cb::engine_errc::success) {
                    // NB: callback will delete the GetValue's Item
                    c.cb.callback(bg_itm_ctx.value);
                } else {
                    EP_LOG_WARN(
                            "Warmup({}) failed to load data for {}"
                            " key{{{}}} error = {}",
                            c.warmup.getName(),
                            vbId,
                            cb::UserData{items.first.to_string()},
                            bg_itm_ctx.value.getStatus());
                    c.log.incrementKeyError();
                }

                if (c.cb.getStatus() == cb::engine_errc::success) {
                    c.log.incrementKeyLoaded();
                } else {
                    // Failed to apply an Item, so fail the rest
                    applyItem = false;
                }
            } else {
                c.log.incrementKeySkipped();
            }
        }

        return true;
    }
    c.log.incrementKeySkipped();
    return false;
}

const char* WarmupState::toString() const {
    return getStateDescription(state.load());
}

const char* WarmupState::getStateDescription(State st) const {
    switch (st) {
    case State::Initialize:
        return "initialize";
    case State::CreateVBuckets:
        return "creating vbuckets";
    case State::LoadingCollectionCounts:
        return "loading collection counts";
    case State::EstimateDatabaseItemCount:
        return "estimating database item count";
    case State::LoadPreparedSyncWrites:
        return "loading prepared SyncWrites";
    case State::PopulateVBucketMap:
        return "populating vbucket map";
    case State::KeyDump:
        return "loading keys";
    case State::CheckForAccessLog:
        return "determine access log availability";
    case State::LoadingAccessLog:
        return "loading access log";
    case State::LoadingKVPairs:
        return "loading k/v pairs";
    case State::LoadingData:
        return "loading data";
    case State::Done:
        return "done";
    }
    return "Illegal state";
}

std::string to_string(WarmupState::State st) {
    using namespace std::string_literals;
#define X(name)                    \
    case WarmupState::State::name: \
        return #name##s;
    switch (st) {
        X(Initialize)
        X(CreateVBuckets)
        X(LoadingCollectionCounts)
        X(EstimateDatabaseItemCount)
        X(LoadPreparedSyncWrites)
        X(PopulateVBucketMap)
        X(KeyDump)
        X(CheckForAccessLog)
        X(LoadingAccessLog)
        X(LoadingKVPairs)
        X(LoadingData)
        X(Done)
    }
#undef X
    folly::assume_unreachable();
}

void WarmupState::transition(State to, bool allowAnyState) {
    auto currentState = state.load();
    // If we're in the done state already this is a special case as it's always
    // our final state, which we may not transition from.
    if (currentState == State::Done) {
        return;
    }
    auto checkLegal = [this, &currentState, &to, &allowAnyState]() -> bool {
        if (allowAnyState || legalTransition(currentState, to)) {
            return true;
        }
        // Throw an exception to make it possible to test the logic ;)
        throw std::runtime_error(
                fmt::format("Illegal state transition from \"{}\" to {} ({})",
                            getStateDescription(currentState),
                            getStateDescription(to),
                            int(to)));
    };
    transitionHook();
    while (checkLegal() && !state.compare_exchange_weak(currentState, to)) {
        currentState = state.load();
        // If we're in the done state already this is a special case as it's
        // always our final state, which we may not transition from. It's
        // possible that the state has be set to Done by another threads, if
        // we're shutting down the bucket (See Warmup::stop() and is usage).
        if (currentState == State::Done) {
            break;
        }
    }
    EP_LOG_DEBUG("Warmup transition from state \"{}\" to \"{}\"",
                 getStateDescription(currentState),
                 getStateDescription(to));
}

bool WarmupState::legalTransition(State from, State to) const {
    switch (from) {
    case State::Initialize:
        return (to == State::CreateVBuckets || to == State::CheckForAccessLog);
    case State::CreateVBuckets:
        return (to == State::LoadingCollectionCounts);
    case State::LoadingCollectionCounts:
        return (to == State::EstimateDatabaseItemCount);
    case State::EstimateDatabaseItemCount:
        return (to == State::LoadPreparedSyncWrites);
    case State::LoadPreparedSyncWrites:
        return (to == State::PopulateVBucketMap);
    case State::PopulateVBucketMap:
        return (to == State::KeyDump || to == State::CheckForAccessLog ||
                to == State::Done);
    case State::KeyDump:
        return (to == State::LoadingKVPairs || to == State::CheckForAccessLog ||
                to == State::Done);
    case State::CheckForAccessLog:
        return (to == State::LoadingAccessLog || to == State::LoadingData ||
                to == State::LoadingKVPairs);
    case State::LoadingAccessLog:
        return (to == State::Done || to == State::LoadingData);
    case State::LoadingKVPairs:
        return (to == State::Done);
    case State::LoadingData:
        return (to == State::Done);
    case State::Done:
        return false;
    }

    return false;
}

std::ostream& operator<<(std::ostream& out, const WarmupState& state) {
    out << state.toString();
    return out;
}

LoadStorageKVPairCallback::LoadStorageKVPairCallback(
        EPBucket& ep,
        Warmup& warmup,
        bool shouldCheckIfWarmupThresholdReached,
        WarmupState::State warmupState,
        std::optional<const std::chrono::steady_clock::duration>
                deltaDeadlineFromNow)
    : stats(ep.getEPEngine().getEpStats()),
      epstore(ep),
      warmup(warmup),
      deltaDeadlineFromNow(std::move(deltaDeadlineFromNow)),
      deadline(std::chrono::steady_clock::time_point::max()),
      shouldCheckIfWarmupThresholdReached(shouldCheckIfWarmupThresholdReached),
      warmupState(warmupState) {
}

void LoadStorageKVPairCallback::callback(GetValue& val) {
    // "Reset" the status to success here to indicate to the caller
    // (KVStore::scan) that it should continue scanning. This means that any
    // returns without setting the status explicitly to something else will
    // continue.
    setStatus(cb::engine_errc::success);

    // This callback method is responsible for deleting the Item
    std::unique_ptr<Item> i(std::move(val.item));

    epstore.getEPEngine().visitWarmupHook();

    // Don't attempt to load the system event documents.
    if (i->getKey().isInSystemEventCollection()) {
        return;
    }

    // Prepared SyncWrites are ignored here  -
    // they are handled in the earlier warmup State::LoadPreparedSyncWrites
    if (i->isPending()) {
        return;
    }

    bool stopLoading = false;

    if (warmup.isFinishedLoading()) {
        setStatus(cb::engine_errc::cancelled);
        return;
    }

    auto vb = warmup.tryAndGetVbucket(i->getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    if (i->getCas() == static_cast<uint64_t>(-1)) {
        if (val.isPartial()) {
            i->setCas(0);
        } else {
            i->setCas(vb->nextHLCCas());
        }
    }

    auto* epVb = dynamic_cast<EPVBucket*>(vb.get());
    if (!epVb) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    const auto res = epVb->insertFromWarmup(
            *i, shouldEject(), val.isPartial(), true /*check mem_used*/);
    switch (res) {
    case MutationStatus::NoMem:
        EP_LOG_DEBUG(
                "LoadStorageKVPairCallback::callback(): {} "
                "NoMem",
                vb->getId());
        ++stats.warmOOM;
        break;
    case MutationStatus::InvalidCas:
        EP_LOG_DEBUG(
                "LoadStorageKVPairCallback::callback(): {} "
                "Value changed in memory before restore from disk. "
                "Ignored disk value for: key{{{}}}.",
                vb->getId(),
                i->getKey());
        ++stats.warmDups;
        break;
    case MutationStatus::NotFound:
        EP_LOG_DEBUG(
                "LoadStorageKVPairCallback::callback: Inserted into HT key:{}",
                i->getKey());
        break;
    default:
        throw std::logic_error(
                "LoadStorageKVPairCallback::callback: "
                "Unexpected result from HashTable::insert: " +
                std::to_string(static_cast<uint16_t>(res)));
    }

    if (shouldCheckIfWarmupThresholdReached) {
        stopLoading = warmup.hasReachedThreshold();
    }

    switch (warmupState) {
    case WarmupState::State::KeyDump:
        // Another shard may have triggered OOM so alway check and stop
        if (stats.warmOOM) {
            warmup.setOOMFailure();
            stopLoading = true;
        }

        // Even if stopping, a key may of loaded on this shard, check the
        // insertFromWarmup result
        if (res == MutationStatus::NotFound) {
            warmup.incrementKeys();
        }
        break;
    case WarmupState::State::LoadingData:
    case WarmupState::State::LoadingAccessLog:
        if (epstore.getItemEvictionPolicy() == EvictionPolicy::Full) {
            warmup.incrementKeys();
        }
        warmup.incrementValues();
        break;
    default:
        warmup.incrementKeys();
        warmup.incrementValues();
    }

    if (stopLoading) {
        // Returns to scan with status Cancelled, note any engine_errc other
        // than not_my_bucket or temporary_failure can be used to cancel...
        setStatus(cb::engine_errc::cancelled);
        return;
    }

    if (deltaDeadlineFromNow && std::chrono::steady_clock::now() >= deadline) {
        yield(); // Returns to scan with status Yield
    }
}

bool LoadStorageKVPairCallback::shouldEject() const {
    return stats.getEstimatedTotalMemoryUsed() >= stats.mem_low_wat;
}

void LoadValueCallback::callback(CacheLookup& lookup) {
    // If not value-eviction (LoadingData), then skip attempting to check for
    // value already resident, given we assume nothing has been loaded for this
    // document yet.
    if (warmup.getWarmupState() != WarmupState::State::LoadingData) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // Prepared SyncWrites are ignored in the normal LoadValueCallback -
    // they are handled in an earlier warmup phase so return
    // cb::engine_errc::key_already_exists to indicate this key should be
    // skipped.
    if (lookup.getKey().isPrepared()) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto vb = warmup.tryAndGetVbucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::not_my_vbucket);
        return;
    }

    // We explicitly want the committedSV (if exists).
    auto res = vb->ht.findOnlyCommitted(lookup.getKey().getDocKey());
    if (res.storedValue && res.storedValue->isResident()) {
        // Already resident in memory - skip loading from disk.
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    // Otherwise - item value not in hashTable - continue with disk load.
    setStatus(cb::engine_errc::success);
}

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Implementation of the warmup class                                    //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

Warmup::Warmup(EPBucket& st,
               const Configuration& config,
               std::function<void()> warmupDoneFunction,
               size_t memoryThreshold,
               size_t itemsThreshold,
               std::string name)
    : store(st),
      config(config),
      stats(store.getEPEngine().getEpStats()),
      warmupDoneFunction(std::move(warmupDoneFunction)),
      shardVbStates(store.vbMap.getNumShards()),
      shardVbIds(store.vbMap.getNumShards()),
      warmedUpVbuckets(std::in_place, config.getMaxVbuckets()),
      name(std::move(name)) {
    setup(memoryThreshold, itemsThreshold);
}

Warmup::Warmup(Warmup& warmup,
               size_t memoryThreshold,
               size_t itemsThreshold,
               std::string name)
    : store(warmup.store),
      config(warmup.config),
      stats(store.getEPEngine().getEpStats()),
      warmupDoneFunction([]() { /* no done callback needed*/ }),
      shardVbStates(std::move(warmup.shardVbStates)),
      shardVbIds(std::move(warmup.shardVbIds)),
      estimatedKeyCount(warmup.getEstimatedKeyCount()),
      weakVbMap(std::move(warmup.weakVbMap)),
      name(std::move(name)) {
    setup(memoryThreshold, itemsThreshold);
    // Jump into the state machine at CheckForAccessLog to begin loading data.
    transition(WarmupState::State::CheckForAccessLog);
}

void Warmup::setup(size_t memoryThreshold, size_t itemsThreshold) {
    {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        warmupStart.time = std::chrono::steady_clock::now();
    }
    setMemoryThreshold(memoryThreshold);
    setItemThreshold(itemsThreshold);
}

Warmup::~Warmup() = default;

void Warmup::incrementKeys() {
    ++keys;
    ++store.getEPEngine().getEpStats().warmedUpKeys;
}

void Warmup::incrementValues() {
    ++values;
    ++store.getEPEngine().getEpStats().warmedUpValues;
}

size_t Warmup::getKeys() const {
    return keys;
}

size_t Warmup::getValues() const {
    return values;
}

void Warmup::addToTaskSet(size_t taskId) {
    std::lock_guard<std::mutex> lh(taskSetMutex);
    taskSet.insert(taskId);
}

void Warmup::removeFromTaskSet(size_t taskId) {
    std::lock_guard<std::mutex> lh(taskSetMutex);
    taskSet.erase(taskId);
}

size_t Warmup::getEstimatedValueCount() const {
    return estimatedValueCount.load();
}

void Warmup::setEstimatedValueCount(size_t to) {
    estimatedValueCount.store(to);
}

size_t Warmup::getEstimatedKeyCount() const {
    return estimatedKeyCount.load();
}

void Warmup::start() {
    step();
}

void Warmup::stop() {
    {
        std::lock_guard<std::mutex> lh(taskSetMutex);
        if (taskSet.empty()) {
            return;
        }
        for (auto id : taskSet) {
            ExecutorPool::get()->cancel(id);
        }
        taskSet.clear();
    }
    transition(WarmupState::State::Done, true);
    done();
}

void Warmup::initialize() {
    auto session_stats = store.getOneROUnderlying()->getPersistedStats();
    auto it = session_stats.find("ep_force_shutdown");
    if (it != session_stats.end() && it.value() == "false") {
        cleanShutdown = true;
        // We want to ensure that if we crash from now and then warmup again.
        // That we will generate a new failover entry and not treat the last
        // shutdown as being clean. To do this we just need to set
        // 'ep_force_shutdown=true' in the stats.json file.
        session_stats["ep_force_shutdown"] = "true";
        while (!store.getOneRWUnderlying()->snapshotStats(session_stats)) {
            EP_LOG_ERR(
                    "Warmup({})::initialize(): failed to persist setting "
                    "ep_force_shutdown=true to stats.json, sleeping for 1 sec "
                    "before retrying",
                    getName());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    if (!store.getCollectionsManager().warmupLoadManifest(
                store.getConfiguration().getDbname())) {
        EP_LOG_CRITICAL(
                "Warmup({})::initialize aborting as "
                "CollectionsManager::warmupLoadManifest failed",
                getName());
        return;
    }

    populateShardVbStates();

    transition(WarmupState::State::CreateVBuckets);
}

void Warmup::createVBuckets(uint16_t shardId) {
    size_t maxEntries = store.getEPEngine().getMaxFailoverEntries();

    // Iterate over all VBucket states defined for this shard, creating VBucket
    // objects if they do not already exist.
    for (const auto& itr : shardVbStates[shardId]) {
        Vbid vbid = itr.first;
        const vbucket_state& vbs = itr.second;

        // Collections and sync-repl requires that the VBucket datafiles have
        // 'namespacing' applied to the key space
        if (!vbs.supportsNamespaces) {
            EP_LOG_CRITICAL(
                    "Warmup({})::createVBuckets aborting warmup as {} datafile "
                    "is unusable, name-spacing is not enabled.",
                    getName(),
                    vbid);
            return;
        }

        VBucketPtr vb = store.getVBucket(vbid);
        if (!vb) {
            std::unique_ptr<FailoverTable> table;
            if (vbs.transition.failovers.empty()) {
                table = std::make_unique<FailoverTable>(maxEntries);
            } else {
                table = std::make_unique<FailoverTable>(
                        vbs.transition.failovers, maxEntries, vbs.highSeqno);
            }
            KVShard* shard = store.getVBuckets().getShardByVbId(vbid);

            std::unique_ptr<Collections::VB::Manifest> manifest;
            if (config.isCollectionsEnabled()) {
                auto [getManifestStatus, persistedManifest] =
                        store.getROUnderlyingByShard(shardId)
                                ->getCollectionsManifest(vbid);
                if (!getManifestStatus) {
                    EP_LOG_CRITICAL(
                            "Warmup({})::createVBuckets: {} failed to read "
                            " collections manifest from disk",
                            getName(),
                            vbid);
                    return;
                }

                manifest = std::make_unique<Collections::VB::Manifest>(
                        store.getSharedCollectionsManager(), persistedManifest);
            } else {
                manifest = std::make_unique<Collections::VB::Manifest>(
                        store.getSharedCollectionsManager());
            }

            const auto* topology =
                    vbs.transition.replicationTopology.empty()
                            ? nullptr
                            : &vbs.transition.replicationTopology;
            vb = store.makeVBucket(vbid,
                                   vbs.transition.state,
                                   shard,
                                   std::move(table),
                                   std::move(manifest),
                                   vbs.transition.state,
                                   vbs.highSeqno,
                                   vbs.lastSnapStart,
                                   vbs.lastSnapEnd,
                                   vbs.purgeSeqno,
                                   vbs.maxCas,
                                   vbs.hlcCasEpochSeqno,
                                   vbs.mightContainXattrs,
                                   topology,
                                   vbs.maxVisibleSeqno,
                                   vbs.persistedPreparedSeqno);

            if (vbs.transition.state == vbucket_state_active &&
                (!cleanShutdown ||
                 store.getCollectionsManager().needsUpdating(*vb))) {
                if (static_cast<uint64_t>(vbs.highSeqno) == vbs.lastSnapEnd) {
                    vb->createFailoverEntry(vbs.lastSnapEnd);
                } else {
                    vb->createFailoverEntry(vbs.lastSnapStart);
                }

                auto entry = vb->failovers->getLatestEntry();
                EP_LOG_INFO(
                        "Warmup({})::createVBuckets: {} created new failover "
                        "entry "
                        "with uuid:{} and seqno:{} due to {}",
                        getName(),
                        vbid,
                        entry.vb_uuid,
                        entry.by_seqno,
                        !cleanShutdown ? "unclean shutdown" : "manifest uid");
            }
            vb->setFreqSaturatedCallback(
                    [this]() { store.itemFrequencyCounterSaturated(); });

            // Add the new vbucket to our local map, it will later be added
            // to the bucket's vbMap once the vbuckets are fully initialised
            // from KVStore data
            warmedUpVbuckets.lock()->insert(std::make_pair(vbid.get(), vb));
            // Capture a weak_ptr reference to the vb, all data loading steps
            // will access the VBucket via a weak_ptr.
            weakVbMap.lock()->insert(std::make_pair(vbid.get(), vb));
        }

        // Pass the max deleted seqno for each vbucket.
        vb->ht.setMaxDeletedRevSeqno(vbs.maxDeletedSeqno);

        // For each vbucket, set the last persisted seqno checkpoint
        vb->setPersistenceSeqno(vbs.highSeqno);
    }

    if (++threadtask_count == getNumShards()) {
        transition(WarmupState::State::LoadingCollectionCounts);
    }
}

void Warmup::notifyWaitingCookies(cb::engine_errc status) {
    PendingCookiesQueue toNotify;
    {
        std::unique_lock<std::mutex> lock(pendingCookiesMutex);
        mustWaitForWarmup = false;
        pendingCookies.swap(toNotify);
    }
    if (toNotify.empty()) {
        return;
    }

    EP_LOG_INFO(
            "Warmup({})::notifyWaitingCookies unblocking {} cookie(s) "
            "status:{}",
            getName(),
            toNotify.size(),
            status);
    for (auto* c : toNotify) {
        store.getEPEngine().notifyIOComplete(c, status);
    }
}

bool Warmup::maybeWaitForVBucketWarmup(CookieIface* cookie) {
    std::lock_guard<std::mutex> lg(pendingCookiesMutex);
    if (mustWaitForWarmup) {
        pendingCookies.push_back(cookie);
        return true;
    }
    return false;
}

void Warmup::loadCollectionStatsForShard(uint16_t shardId) {
    // get each VB in the shard and iterate its collections manifest
    // load the _local doc count value

    const auto* kvstore = store.getROUnderlyingByShard(shardId);
    // Iterate the VBs in the shard
    for (const auto vbid : shardVbIds[shardId]) {
        auto vb = lookupVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Take the KVFileHandle before we lock the manifest to prevent lock
        // order inversions.
        auto kvstoreContext = kvstore->makeFileHandle(vbid);
        if (!kvstoreContext) {
            EP_LOG_CRITICAL(
                    "Warmup({})::loadCollectionStatsForShard() Unable to make "
                    "KVFileHandle for {}, aborting warmup as we will not be "
                    "able to check collection stats.",
                    getName(),
                    vbid);
            return;
        }

        std::unique_lock wlh(vb->getStateLock());
        auto wh = vb->getManifest().wlock(wlh);
        // For each collection in the VB, get its stats
        for (auto& collection : wh) {
            // start tracking in-memory stats before items are warmed up.
            // This may be called repeatedly; it is idempotent.
            store.stats.trackCollectionStats(collection.first);

            // getCollectionStats() can still can fail if the data store on disk
            // has been corrupted between the call to makeFileHandle() and
            // getCollectionStats()
            auto [status, stats] = kvstore->getCollectionStats(
                    *kvstoreContext, collection.first);
            if (status == KVStore::GetCollectionStatsStatus::Failed) {
                EP_LOG_CRITICAL(
                        "Warmup({})::loadCollectionStatsForShard(): "
                        "getCollectionStats() failed for {}, aborting warmup "
                        "as we will not be "
                        "able to check collection stats.",
                        getName(),
                        vbid);
                return;
            }
            // For NotFound we're ok to use the default initialised stats

            collection.second.setItemCount(stats.itemCount);
            collection.second.setPersistedHighSeqno(stats.highSeqno);
            collection.second.setDiskSize(stats.diskSize);
            // Set the in memory high seqno - might be 0 in the case of the
            // default collection so we have to reset the monotonic value
            collection.second.resetHighSeqno(stats.highSeqno);
        }
    }

    if (++threadtask_count == getNumShards()) {
        transition(WarmupState::State::EstimateDatabaseItemCount);
    }
}

void Warmup::estimateDatabaseItemCount(uint16_t shardId) {
    auto st = std::chrono::steady_clock::now();
    size_t item_count = 0;

    for (const auto vbid : shardVbIds[shardId]) {
        auto vb = lookupVBucket(vbid);
        if (!vb) {
            continue;
        }
        auto& epVb = static_cast<EPVBucket&>(*vb);
        epVb.setNumTotalItems(*store.getRWUnderlyingByShard(shardId));
        const auto vbItemCount = epVb.getNumTotalItems();
        item_count += vbItemCount;
        EP_LOG_INFO_CTX("Warmup::estimateDatabaseItemCount: ",
                        {"shard_id", shardId},
                        {"vb", vbid},
                        {"item_count", vbItemCount});
    }

    // Start off by adding each shard's total item count. A healthy warmup and
    // this would represent 100%
    estimatedKeyCount.fetch_add(item_count);
    estimateTime.fetch_add(std::chrono::steady_clock::now() - st);

    if (++threadtask_count == getNumShards()) {
        transition(WarmupState::State::LoadPreparedSyncWrites);
    }
}

void Warmup::loadPreparedSyncWrites(uint16_t shardId) {
    for (const auto vbid : shardVbIds[shardId]) {
        auto vb = lookupVBucket(vbid);
        if (!vb) {
            continue;
        }

        // Our EPBucket function will do the load for us as we re-use the code
        // for rollback.
        auto [itemsVisited, preparesLoaded, defaultCollectionMVS, success] =
                store.loadPreparedSyncWrites(*vb);
        if (!success) {
            EP_LOG_CRITICAL(
                    "Warmup({})::loadPreparedSyncWrites(): "
                    "EPBucket::loadPreparedSyncWrites() failed for {} aborting "
                    "Warmup",
                    getName(),
                    vbid);
            return;
        }
        auto& epStats = store.getEPEngine().getEpStats();
        epStats.warmupItemsVisitedWhilstLoadingPrepares += itemsVisited;
        epStats.warmedUpPrepares += preparesLoaded;
        vb->getManifest().setDefaultCollectionLegacySeqnos(
                defaultCollectionMVS,
                vbid,
                *store.getRWUnderlyingByShard(shardId));
    }

    if (++threadtask_count == getNumShards()) {
        transition(WarmupState::State::PopulateVBucketMap);
    }
}

void Warmup::populateVBucketMap(uint16_t shardId) {
    for (const auto vbid : shardVbIds[shardId]) {
        auto vbPtr = lookupVBucket(vbid);
        if (vbPtr) {
            // Take the vBucket lock to stop the flusher from racing with our
            // set vBucket state. It MUST go to disk in the first flush batch
            // or we run the risk of not rolling back replicas that we should
            auto lockedVb = store.getLockedVBucket(vbid);
            Expects(lockedVb.owns_lock());
            Expects(!lockedVb);

            vbPtr->checkpointManager->queueSetVBState();

            {
                // Note this lock is here for correctness - the VBucket is not
                // accessible yet, so its state cannot be changed by other code.
                std::shared_lock rlh(vbPtr->getStateLock());
                if (vbPtr->getState() == vbucket_state_active) {
                    // For all active vbuckets, call through to the manager so
                    // that they are made 'current' with the manifest.
                    store.getCollectionsManager().maybeUpdate(rlh, *vbPtr);
                }
            }

            auto result = store.flushVBucket_UNLOCKED(
                    {vbPtr, std::move(lockedVb.getLock())});
            // if flusher returned MoreAvailable::Yes, this indicates the single
            // flush of the vbucket state failed.
            if (result.moreAvailable == EPBucket::MoreAvailable::Yes) {
                // Disabling writes to this node as we're unable to persist
                // vbucket state to disk.
                EP_LOG_CRITICAL(
                        "Warmup({})::populateVBucketMap() flush state failed "
                        "for "
                        "{} highSeqno:{}, write traffic will be disabled for "
                        "this node.",
                        getName(),
                        vbid,
                        vbPtr->getHighSeqno());
                failedToSetAVbucketState = true;
            }

            store.vbMap.addBucket(vbPtr);
        }
    }

    if (++threadtask_count == getNumShards()) {
        // All threads have finished populating the vBucket map (and potentially
        // flushing a new vBucket state).

        // We can let the KVStore know. This will enable history eviction if it
        // was previously disable due to continuous backup.
        store.completeLoadingVBuckets();
        // It's now safe for us to start the flushers.
        store.startFlusher();

        // Can now drop the shared_ptrs from Warmup
        warmedUpVbuckets.lock()->clear();

        // Once we have populated the VBMap we can release operations that are
        // waiting for the VBuckets to of been loaded E.g. setVBState
        // and GetFailoverLog
        notifyWaitingCookies(cb::engine_errc::success);
        if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
            transition(WarmupState::State::KeyDump);
        } else if (hasReachedThreshold()) {
            transition(WarmupState::State::Done);
        } else {
            transition(WarmupState::State::CheckForAccessLog);
        }

        {
            std::lock_guard<std::mutex> lock(warmupStart.mutex);
            metadata.store(std::chrono::steady_clock::now() - warmupStart.time);
        }
        EP_LOG_INFO("Warmup({}) metadata loaded in {}",
                    getName(),
                    cb::time2text(std::chrono::nanoseconds(metadata.load())));
    }
}

void Warmup::scheduleShardedTasks(const WarmupState::State phase) {
    threadtask_count = 0;

    for (size_t shardId = 0; shardId < store.vbMap.shards.size(); ++shardId) {
        switch (phase) {
        case WarmupState::State::CreateVBuckets:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupCreateVBuckets>(
                            store, shardId, this));
            break;
        case WarmupState::State::LoadingCollectionCounts:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupLoadingCollectionCounts>(
                            store, shardId, *this));
            break;
        case WarmupState::State::EstimateDatabaseItemCount:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupEstimateDatabaseItemCount>(
                            store, shardId, this));
            break;
        case WarmupState::State::LoadPreparedSyncWrites:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupLoadPreparedSyncWrites>(
                            store.getEPEngine(), shardId, *this));
            break;
        case WarmupState::State::PopulateVBucketMap:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupPopulateVBucketMap>(
                            store, shardId, *this));
            break;
        case WarmupState::State::KeyDump:
            ExecutorPool::get()->schedule(std::make_shared<WarmupKeyDump>(
                    store, shardId, *this, threadtask_count));
            break;
        case WarmupState::State::LoadingAccessLog:
            ExecutorPool::get()->schedule(std::make_shared<WarmupLoadAccessLog>(
                    store, shardId, this));
            break;
        case WarmupState::State::LoadingKVPairs:
            ExecutorPool::get()->schedule(
                    std::make_shared<WarmupLoadingKVPairs>(
                            store, shardId, *this, threadtask_count));
            break;
        case WarmupState::State::LoadingData:
            ExecutorPool::get()->schedule(std::make_shared<WarmupLoadingData>(
                    store, shardId, *this, threadtask_count));
            break;
        case WarmupState::State::Initialize:
        case WarmupState::State::CheckForAccessLog:
        case WarmupState::State::Done:
            throw std::logic_error(
                    "Warmup::scheduleShardedTasks: Unexpected phase:" +
                    to_string(phase));
        }
    }
}

void Warmup::checkForAccessLog() {
    // As all warm-ups will progress though this step, set the estimate now.
    // From here on LoadingAccessLog, LoadingKVPairs or LoadingData can all be
    // reached and all can load values (each can reach 100% or transition to
    // Done at some lesser value).
    if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
        // Value Eviction. Warmup will now only proceed to load a value for each
        // resident key. Thus warmedUpKeys now represents 100% (and a healthy
        // warmup this will be all keys).
        setEstimatedValueCount(store.getEPEngine().getEpStats().warmedUpKeys);
    } else {
        // For this full eviction path set the estimate to  be the estimated
        // keys.
        setEstimatedValueCount(getEstimatedKeyCount());
    }

    size_t accesslogs = 0;
    if (config.isAccessScannerEnabled()) {
        accessLog.resize(getNumShards());
        for (uint16_t i = 0; i < accessLog.size(); i++) {
            std::string file = config.getAlogPath() + "." + std::to_string(i);
            auto& shardLogs = accessLog[i];

            // The order here is important as the load phase will work from back
            // so will use the current file before trying .old
            if (cb::io::isFile(file + ".old")) {
                shardLogs.emplace_back(
                        std::make_unique<MutationLog>(file + ".old"));
            }

            if (cb::io::isFile(file)) {
                shardLogs.emplace_back(std::make_unique<MutationLog>(
                        file, config.getAlogBlockSize()));
            }

            if (!shardLogs.empty()) {
                // If we found any access log, increase this counter by 1
                ++accesslogs;
            }
        }
    }
    if (accesslogs == getNumShards()) {
        transition(WarmupState::State::LoadingAccessLog);
    } else {
        // We aren't loading anything from the accessLog, nuke it
        accessLog.clear();

        if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
            transition(WarmupState::State::LoadingData);
        } else {
            transition(WarmupState::State::LoadingKVPairs);
        }
    }
}

bool Warmup::loadingAccessLog(uint16_t shardId) {
    Expects(!accessLog[shardId].empty());

    // Always work back to front (note that the checkForAccessLog puts what is
    // considered the most recent log at the back)
    auto& log = accessLog[shardId].back();
    Expects(log);
    auto status = loadFromAccessLog(*log, shardId);
    switch (status) {
    case WarmupAccessLogState::Yield:
        return true;
    case WarmupAccessLogState::Failed:
        corruptAccessLog = true;
        // Get rid of the current MutationLog object
        accessLog[shardId].pop_back();
        break;
    case WarmupAccessLogState::Done:
        // Get rid of all MutationLog objects for the shard
        accessLog[shardId].clear();
        break;
    }

    if (!accessLog[shardId].empty()) {
        // More logs to try, yield
        return true;
    }

    // No more logs for this shard, check if can we change state?
    if (++threadtask_count == getNumShards()) {
        // We don't need the accessLog anymore, and it uses a bunch of memory.
        // Nuke it now to get the memory back.
        accessLog.clear();

        if (hasReachedThreshold()) {
            transition(WarmupState::State::Done);
        } else {
            transition(WarmupState::State::LoadingData);
        }
    }
    return false;
}

Warmup::WarmupAccessLogState Warmup::loadFromAccessLog(MutationLog& log,
                                                       uint16_t shardId) {
    if (!log.isOpen()) {
        log.open();
    }

    try {
        LoadStorageKVPairCallback load_cb(store, *this, true, state.getState());
        return doWarmup(log, shardVbStates[shardId], load_cb);
    } catch (const std::exception& e) {
        corruptAccessLog = true;
        EP_LOG_WARN(
                "Warmup({}) Error reading access log: {}", getName(), e.what());
    }
    return WarmupAccessLogState::Failed;
}

Warmup::WarmupAccessLogState Warmup::doWarmup(
        MutationLog& lf,
        const std::map<Vbid, vbucket_state>& vbmap,
        StatusCallback<GetValue>& cb) {
    MutationLogHarvester harvester(lf, &store.getEPEngine());
    std::map<Vbid, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVBucket(it->first);
    }

    // To constrain the number of elements from the access log we have to keep
    // alive (there may be millions of items per-vBucket), process it
    // a batch at a time.
    WarmupCookie cookie(store, *this, cb, lf);

    using namespace std::chrono;
    auto start = steady_clock::now();
    auto maxDuration = milliseconds{config.getWarmupAccesslogLoadDuration()};
    auto batchSize = config.getWarmupBatchSize();

    // Keep loading batches until time is up.
    while (harvester.loadBatchAndApply(
            batchSize,
            &cookie,
            &batchWarmupCallback,
            store.getItemEvictionPolicy() == EvictionPolicy::Value)) {
        if (steady_clock::now() - start >= maxDuration) {
            return WarmupAccessLogState::Yield;
        }
    }

    EP_LOG_INFO(
            "Warmup({}) access log loaded items:{}, skipped:{}, error:{} in {}",
            getName(),
            lf.getLoaded(),
            lf.getSkipped(),
            lf.getError(),
            cb::time2text(lf.getDurationSinceOpen()));
    return WarmupAccessLogState::Done;
}

void Warmup::done() {
    if (setFinishedLoading()) {
        setWarmupTime();
        warmupDoneFunction();
        logStats();
    }
}

void Warmup::step() {
    const auto state = this->state.getState();
    switch (state) {
    case WarmupState::State::Initialize:
        ExecutorPool::get()->schedule(
                std::make_shared<WarmupInitialize>(store, this));
        return;
    case WarmupState::State::Done:
        ExecutorPool::get()->schedule(
                std::make_shared<WarmupCompletion>(store, this));
        return;
    case WarmupState::State::CheckForAccessLog:
        ExecutorPool::get()->schedule(
                std::make_shared<WarmupCheckforAccessLog>(store, this));
        return;
    case WarmupState::State::EstimateDatabaseItemCount: {
        estimateTime.store(std::chrono::steady_clock::duration::zero());
        estimatedKeyCount = 0;
    }
    case WarmupState::State::CreateVBuckets:
    case WarmupState::State::LoadingCollectionCounts:
    case WarmupState::State::LoadPreparedSyncWrites:
    case WarmupState::State::PopulateVBucketMap:
    case WarmupState::State::KeyDump:
    case WarmupState::State::LoadingAccessLog:
    case WarmupState::State::LoadingKVPairs:
    case WarmupState::State::LoadingData:
        return scheduleShardedTasks(state);
    }
    folly::assume_unreachable();
}

void Warmup::transition(WarmupState::State to, bool force) {
    EP_LOG_DEBUG("Warmup({}) transition to {}", getName(), to_string(to));
    state.transition(to, force);
    stateTransitionHook(to);
    step();
}

VBucketPtr Warmup::lookupVBucket(Vbid vbid) const {
    switch (state.getState()) {
    case WarmupState::State::LoadingCollectionCounts:
    case WarmupState::State::EstimateDatabaseItemCount:
    case WarmupState::State::LoadPreparedSyncWrites:
    case WarmupState::State::PopulateVBucketMap:
        break;
    case WarmupState::State::Initialize:
    case WarmupState::State::CreateVBuckets:
    case WarmupState::State::KeyDump:
    case WarmupState::State::CheckForAccessLog:
    case WarmupState::State::LoadingAccessLog:
    case WarmupState::State::LoadingKVPairs:
    case WarmupState::State::LoadingData:
    case WarmupState::State::Done:
        throw std::runtime_error(
                fmt::format("Warmup({})::lookupVBucket({}): called for illegal "
                            "warmup state:{}",
                            getName(),
                            vbid,
                            to_string(state.getState())));
    }

    auto locked = warmedUpVbuckets.lock();
    auto it = locked->find(vbid.get());
    if (it == locked->end()) {
        return {};
    }
    return it->second;
}

void Warmup::addCommonStats(const StatCollector& collector) const {
    using namespace cb::stats;
    using namespace std::chrono;

    collector.addStat(Key::ep_warmup_thread, getThreadStatState());

    EPStats& stats = store.getEPEngine().getEpStats();
    collector.addStat(Key::ep_warmup_oom, stats.warmOOM);
    collector.addStat(Key::ep_warmup_dups, stats.warmDups);

    auto w_time = warmup.load();
    if (w_time > w_time.zero()) {
        collector.addStat(Key::ep_warmup_time,
                          duration_cast<microseconds>(w_time).count());
    }
}

void Warmup::addStats(const StatCollector& collector) const {
    using namespace cb::stats;
    using namespace std::chrono;

    EPStats& stats = store.getEPEngine().getEpStats();
    collector.addStat(Key::ep_warmup, "enabled");
    const char* stateName = state.toString();
    collector.addStat(Key::ep_warmup_state, stateName);

    addCommonStats(collector);

    collector.addStat(Key::ep_warmup_key_count, stats.warmedUpKeys);
    collector.addStat(Key::ep_warmup_value_count, stats.warmedUpValues);
    collector.addStat(Key::ep_primary_warmup_min_memory_threshold,
                      maxSizeScaleFactor * 100.0);
    collector.addStat(Key::ep_primary_warmup_min_items_threshold,
                      maxItemsScaleFactor * 100.0);

    auto md_time = metadata.load();
    if (md_time > md_time.zero()) {
        collector.addStat(Key::ep_warmup_keys_time,
                          duration_cast<microseconds>(md_time).count());
    }

    size_t itemCount = estimatedKeyCount.load();
    if (itemCount == std::numeric_limits<size_t>::max()) {
        collector.addStat(Key::ep_warmup_estimated_key_count, "unknown");
    } else {
        auto e_time = estimateTime.load();
        if (e_time != e_time.zero()) {
            collector.addStat(Key::ep_warmup_estimate_time,
                              duration_cast<microseconds>(e_time).count());
        }
        collector.addStat(Key::ep_warmup_estimated_key_count, itemCount);
    }

    if (corruptAccessLog) {
        collector.addStat(Key::ep_warmup_access_log, "corrupt");
    }

    size_t warmupCount = estimatedValueCount.load();
    if (warmupCount == std::numeric_limits<size_t>::max()) {
        collector.addStat(Key::ep_warmup_estimated_value_count, "unknown");
    } else {
        collector.addStat(Key::ep_warmup_estimated_value_count, warmupCount);
    }
}

void Warmup::addSecondaryWarmupStats(const StatCollector& collector) const {
    using namespace cb::stats;
    using namespace std::chrono;
    collector.addStat(Key::ep_secondary_warmup_status, getThreadStatState());
    collector.addStat(Key::ep_secondary_warmup_time,
                      duration_cast<microseconds>(getTime()).count());

    const char* stateName = state.toString();
    collector.addStat(Key::ep_secondary_warmup_state, stateName);
    collector.addStat(Key::ep_secondary_warmup_min_memory_threshold,
                      maxSizeScaleFactor * 100.0);
    collector.addStat(Key::ep_secondary_warmup_min_items_threshold,
                      maxItemsScaleFactor * 100.0);

    auto md_time = metadata.load();
    if (md_time > md_time.zero()) {
        collector.addStat(Key::ep_secondary_warmup_keys_time,
                          duration_cast<microseconds>(md_time).count());
    }

    if (corruptAccessLog) {
        collector.addStat(Key::ep_secondary_warmup_access_log, "corrupt");
    }

    // Secondary uses the key count from Primary - so don't report the key count
    // but Secondary should proceed to estimate the values
    size_t warmupCount = estimatedValueCount.load();
    if (warmupCount == std::numeric_limits<size_t>::max()) {
        collector.addStat(Key::ep_secondary_warmup_estimated_value_count,
                          "unknown");
    } else {
        collector.addStat(Key::ep_secondary_warmup_estimated_value_count,
                          warmupCount);
    }
}

void Warmup::addStatusMetrics(const StatCollector& collector) const {
    auto currentState = state.getState();

    using State = WarmupState::State;
    for (auto i = int(State::Initialize); i <= int(State::Done); i++) {
        auto s = State(i);
        using namespace cb::stats;
        collector.addStat(Key::ep_warmup_status,
                          s == currentState,
                          {{"state", to_string(s)}});
    }
}

size_t Warmup::getNumShards() const {
    return store.vbMap.getNumShards();
}

void Warmup::populateShardVbStates() {
    const uint16_t numShards = getNumShards();
    for (uint16_t shardIdx = 0; shardIdx < numShards; ++shardIdx) {
        const std::vector<vbucket_state*> curShardStates =
                store.getRWUnderlyingByShard(shardIdx)->listPersistedVbuckets();
        auto& statesMap = shardVbStates[shardIdx];
        auto& statesVec = shardVbIds[shardIdx];
        for (uint16_t vbIdx = 0; vbIdx < curShardStates.size(); ++vbIdx) {
            if (!curShardStates[vbIdx]) {
                continue;
            }
            const Vbid vbid(vbIdx * numShards + shardIdx);
            statesMap.insert({vbid, *curShardStates[vbIdx]});
        }

        // First push all active vbuckets and then the rest
        for (const auto& item : statesMap) {
            if (item.second.transition.state == vbucket_state_active) {
                statesVec.push_back(item.first);
            }
        }
        for (const auto& item : statesMap) {
            if (item.second.transition.state != vbucket_state_active) {
                statesVec.push_back(item.first);
            }
        }
    }
}

void Warmup::setMemoryThreshold(size_t perc) {
    maxSizeScaleFactor = static_cast<double>(perc) / 100.0;
}

void Warmup::setItemThreshold(size_t perc) {
    maxItemsScaleFactor = static_cast<double>(perc) / 100.0;
}

bool Warmup::hasReachedThreshold() const {
    const auto memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    const auto maxSize = static_cast<double>(stats.getMaxDataSize());

    if (memoryUsed >= stats.mem_low_wat) {
        EP_LOG_INFO(
                "Warmup({}) Total memory use reached to the low water mark, "
                "stop warmup: memoryUsed ({}) >= low water mark ({})",
                getName(),
                memoryUsed,
                uint64_t(stats.mem_low_wat.load()));
        return true;
    }
    if (memoryUsed > (maxSize * maxSizeScaleFactor)) {
        EP_LOG_INFO(
                "Warmup({}) Enough MB of data loaded to enable traffic"
                ": memoryUsed ({}) > (maxSize({}) * warmupMemUsedCap({}))",
                getName(),
                memoryUsed,
                maxSize,
                maxSizeScaleFactor);
        return true;
    }
    if (store.getItemEvictionPolicy() == EvictionPolicy::Value &&
        stats.warmedUpValues >= (stats.warmedUpKeys * maxItemsScaleFactor)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        EP_LOG_INFO(
                "Warmup({}) Enough number of items loaded to enable traffic "
                "(value eviction): warmedUpValues({}) >= (warmedUpKeys({}) * "
                "warmupNumReadCap({}))",
                getName(),
                uint64_t(stats.warmedUpValues.load()),
                uint64_t(stats.warmedUpKeys.load()),
                maxItemsScaleFactor);
        return true;
    }
    if (store.getItemEvictionPolicy() == EvictionPolicy::Full &&
        stats.warmedUpValues >=
                (getEstimatedKeyCount() * maxItemsScaleFactor)) {
        // In case of FULL EVICTION, warmed up keys always matches the number
        // of warmed up values, therefore for honoring the min_item threshold
        // in this scenario, we can consider warmup's estimated key count.
        EP_LOG_INFO(
                "Warmup({}) Enough number of items loaded to enable traffic "
                "(full eviction): warmedUpValues({}) >= (warmup est items({})"
                " * warmupNumReadCap({}))",
                getName(),
                uint64_t(stats.warmedUpValues.load()),
                uint64_t(getEstimatedKeyCount()),
                maxItemsScaleFactor);
        return true;
    }
    return false;
}

bool Warmup::hasLoadedMetaData() const {
    switch (state.getState()) {
    case WarmupState::State::Initialize:
    case WarmupState::State::CreateVBuckets:
    case WarmupState::State::LoadingCollectionCounts:
    case WarmupState::State::EstimateDatabaseItemCount:
    case WarmupState::State::LoadPreparedSyncWrites:
    case WarmupState::State::PopulateVBucketMap:
        return false;
    case WarmupState::State::KeyDump:
    case WarmupState::State::CheckForAccessLog:
    case WarmupState::State::LoadingAccessLog:
    case WarmupState::State::LoadingKVPairs:
    case WarmupState::State::LoadingData:
    case WarmupState::State::Done:
        return true;
    }
    folly::assume_unreachable();
}

void Warmup::logStats() const {
    const auto time = getTime();
    std::chrono::duration<double, std::chrono::seconds::period> seconds = time;
    double keysPerSecond =
            seconds.count() > 0.0 ? getKeys() / seconds.count() : 0.0;
    double valuesPerSecond =
            seconds.count() > 0.0 ? getValues() / seconds.count() : 0.0;
    const auto bytes = stats.getEstimatedTotalMemoryUsed();
    const auto& stats = store.getEPEngine().getEpStats();
    EP_LOG_INFO(
            "Warmup({}) completed: {} keys (total:{}) and {} values (total:{}) "
            "loaded in {} ({:.2f} keys/s {:.2f} values/s), mem_used now at {} "
            "({})",
            getName(),
            getKeys(),
            stats.warmedUpKeys,
            getValues(),
            stats.warmedUpValues,
            cb::time2text(std::chrono::nanoseconds(time)),
            keysPerSecond,
            valuesPerSecond,
            cb::size2human(bytes),
            cb::calculateThroughput(bytes, time));
}

VBucketPtr Warmup::tryAndGetVbucket(Vbid vbid) const {
    switch (state.getState()) {
    case WarmupState::State::Initialize:
    case WarmupState::State::CreateVBuckets:
    case WarmupState::State::LoadingCollectionCounts:
    case WarmupState::State::EstimateDatabaseItemCount:
    case WarmupState::State::LoadPreparedSyncWrites:
    case WarmupState::State::PopulateVBucketMap:
        throw std::runtime_error(
                fmt::format("Warmup({})::tryAndGetVbucket({}): called for "
                            "illegal warmup state:{}",
                            getName(),
                            vbid,
                            to_string(state.getState())));
    case WarmupState::State::KeyDump:
    case WarmupState::State::CheckForAccessLog:
    case WarmupState::State::LoadingAccessLog:
    case WarmupState::State::LoadingKVPairs:
    case WarmupState::State::LoadingData:
    case WarmupState::State::Done:
        break;
    }

    auto weak = weakVbMap.withLock([vbid](const auto& map) {
        auto itr = map.find(vbid);
        if (itr == map.end()) {
            return std::weak_ptr<VBucket>();
        }
        return itr->second;
    });

    return weak.lock();
}
