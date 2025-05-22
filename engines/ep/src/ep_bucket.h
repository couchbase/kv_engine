/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "auto_refreshed_value.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_owner.h"
#include "snapshots/cache.h"
#include "snapshots/download_snapshot_controller.h"
#include "utilities/synchronized_init_once_ptr.h"
#include "utilities/testing_hook.h"
#include <variant>

class BgFetcher;
namespace Collections::VB {
class Flush;
}
namespace VB {
class Commit;
}
enum class ValueFilter;
namespace cb {
class AwaitableSemaphore;
}
class BucketStatCollector;
class CompactTask;
struct CompactionContext;
struct CompactionStats;
class SeqnoPersistenceNotifyTask;
class VBucketLoadingTask;

/**
 * Eventually Persistent Bucket
 *
 * A bucket type which stores modifications to disk asynchronously
 * ("eventually").
 * Uses hash partitioning of the keyspace into VBuckets, to support
 * replication, rebalance, failover.
 */
class EPBucket : public KVBucket {
public:
    explicit EPBucket(EventuallyPersistentEngine& theEngine);

    ~EPBucket() override;

    bool initialize() override;

    void deinitialize() override;

    enum class MoreAvailable : uint8_t { No = 0, Yes };

    struct FlushResult {
        FlushResult(MoreAvailable m, size_t n)
            : moreAvailable(m), numFlushed(n) {
        }

        bool operator==(const FlushResult& other) const {
            return (moreAvailable == other.moreAvailable &&
                    numFlushed == other.numFlushed);
        }

        MoreAvailable moreAvailable = MoreAvailable::No;
        size_t numFlushed = 0;
    };

    /**
     * @param lastFlushed The last mutation persisted for some key
     * @param candidate The mutation to be persisted for the same key
     * @param historical The history state from the flush batch
     * @returns true if the item `candidate` can be de-duplicated (skipped)
     *  because `lastFlushed` already supercedes it.
     */
    bool canDeduplicate(Item* lastFlushed,
                        Item& candidate,
                        CheckpointHistorical historical) const;

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return an instance of FlushResult
     */
    FlushResult flushVBucket(Vbid vbid);
    FlushResult flushVBucket_UNLOCKED(LockedVBucketPtr vbPtr);

    /**
     * Set the number of flusher items which can be included in a
     * single flusher commit. For more details see flusherBatchSplitTrigger
     * description.
     */
    void setFlusherBatchSplitTrigger(size_t limit);

    size_t getFlusherBatchSplitTrigger();

    /**
     * Set the max bytes of the single flush-batch that is passed to the
     * storage for persistence.
     */
    void setFlushBatchMaxBytes(size_t bytes);

    size_t getFlushBatchMaxBytes() const;

    /**
     * Persist whatever flush-batch previously queued into KVStore.
     *
     * @param kvstore
     * @param txnCtx context for the current transaction (consumed on use)
     * @param [out] commitData
     * @return true if flush succeeds, false otherwise
     */
    bool commit(KVStoreIface& kvstore,
                std::unique_ptr<TransactionContext> txnCtx,
                VB::Commit& commitData);

    /// Start the Flusher for all shards in this bucket.
    void startFlusher();

    /// Stop the Flusher for all shards in this bucket.
    void stopFlusher();

    bool pauseFlusher() override;
    bool resumeFlusher() override;

    void wakeUpFlusher() override;

    /**
     * Starts the background fetcher for each shard.
     * @return true if successful.
     */
    bool startBgFetcher();

    /// Stops the background fetcher for each shard.
    void stopBgFetcher();

    /**
     * Schedule compaction with a config -override of KVBucket method
     */
    cb::engine_errc scheduleCompaction(
            Vbid vbid,
            const CompactionConfig& c,
            CookieIface* ck,
            std::chrono::milliseconds delay) override;

    /**
     * This function is used by internally requested compaction, where there is
     * no cookie. The compaction will be created with the default
     * CompactionConfig but internally_requested set to true.
     * If a CompactTask is already scheduled then the task will still run, but
     * with whatever config it already has + internally_requested=true.
     * If a task is already scheduled, the given delay parameter also takes
     * effect, delaying the existing task.
     */
    cb::engine_errc scheduleCompaction(Vbid vbid,
                                       std::chrono::milliseconds delay);

    cb::engine_errc cancelCompaction(Vbid vbid) override;

    /**
     * Compaction of a database file
     *
     * @param Vbid vbucket to compact
     * @param config Compaction configuration to use
     * @param cookies used to notify connections of operation completion. This
     *        is non-const as doCompact will update cookies, removing all the
     *        cookies it notified.
     *
     * return true if the compaction needs to be rescheduled and false
     *             otherwise
     */
    bool doCompact(Vbid vbid,
                   CompactionConfig& config,
                   std::vector<CookieIface*>& cookies);

    /**
     * After compaction completes the task can be removed if no further
     * compaction is required. If other compaction tasks exist, one of them
     * will be 'poked' to run. This method is called from CompactTask
     *
     * @param vbid id of vbucket that has completed compaction
     * @returns True if the current task must be re-scheduled, else false.
     */
    bool updateCompactionTasks(Vbid vbid);

    uint64_t getTotalDiskSize() override;

    cb::engine_errc getFileStats(const BucketStatCollector& collector) override;

    cb::engine_errc getImplementationStats(
            const BucketStatCollector& collector) const override;

    cb::engine_errc getPerVBucketDiskStats(CookieIface& cookie,
                                           const AddStatFn& add_stat) override;

    size_t getPageableMemCurrent() const override;
    size_t getPageableMemHighWatermark() const override;
    size_t getPageableMemLowWatermark() const override;

    /**
     * Creates a VBucket object from warmup (can set collection state)
     */
    VBucketPtr makeVBucket(Vbid id,
                           vbucket_state_t state,
                           KVShard* shard,
                           std::unique_ptr<FailoverTable> table,
                           std::unique_ptr<Collections::VB::Manifest> manifest,
                           vbucket_state_t initState,
                           int64_t lastSeqno,
                           uint64_t lastSnapStart,
                           uint64_t lastSnapEnd,
                           uint64_t purgeSeqno,
                           uint64_t maxCas,
                           int64_t hlcEpochSeqno,
                           bool mightContainXattrs,
                           const nlohmann::json* replicationTopology,
                           uint64_t maxVisibleSeqno,
                           uint64_t maxPrepareSeqno) override;

    KVBucketResult<std::vector<std::string>> mountVBucket(
            CookieIface& cookie,
            Vbid vbid,
            const std::vector<std::string>& paths) override;

    cb::engine_errc unmountVBucket(Vbid vbid) override;

    bool isVBucketLoading_UNLOCKED(
            Vbid vbid,
            const std::unique_lock<std::mutex>& vbset) const override;

    cb::engine_errc loadVBucket_UNLOCKED(
            CookieIface& cookie,
            Vbid vbid,
            vbucket_state_t toState,
            const nlohmann::json& meta,
            std::unique_lock<std::mutex>& vbset) override;

    cb::engine_errc statsVKey(const DocKeyView& key,
                              Vbid vbucket,
                              CookieIface& cookie) override;

    void completeStatsVKey(CookieIface& cookie,
                           const DocKeyView& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) override;

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override;

    LoadPreparedSyncWritesResult loadPreparedSyncWrites(VBucket& vb) override;

    /**
     * Returns the ValueFilter to use for KVStore scans, given the bucket
     * compression mode and (optional) cookie.
     * @param Cookie we are performing the operation for. If non-null, then
     *        acts as an additional constraint on ValueFilter - if cookie
     *        doesn't support Snappy compression then ValueFilter will not
     *        return compressed data.
     */
    ValueFilter getValueFilterForCompressionMode(
            CookieIface* cookie = nullptr) const;

    void notifyNewSeqno(const Vbid vbid, const VBNotifyCtx& notifyCtx) override;

    bool isGetAllKeysSupported() const override {
        return true;
    }

    void setRetainErroneousTombstones(bool value) {
        retainErroneousTombstones = value;
    }

    bool isRetainErroneousTombstones() const {
        return retainErroneousTombstones.load();
    }

    Warmup* getPrimaryWarmup() const override;

    /**
     * Obtain a pointer to the optional secondary Warmup.
     *
     * @return pointer (can be null) to the secondary Warmup
     */
    Warmup* getSecondaryWarmup() const override;

    /**
     * @return true if Primary and Secondary warm-up has yet to signal finished
     * loading.
     */
    bool isWarmupLoadingData() const override;

    /**
     * @return true if Primary warm-up has yet to signal finished loading. Note
     *         that Secondary warm-up has no input on this function.
     */
    bool isPrimaryWarmupLoadingData() const override;

    /**
     * @return true if Primary warmup has finished loading and reached the Done
     * state. Based on the configuration this does not mean that anything has
     * been loaded.
     */
    bool isPrimaryWarmupComplete() const;

    bool hasPrimaryWarmupLoadedMetaData() override;

    /**
     * Add warmup stats if the warmupTask object exists
     */
    cb::engine_errc doWarmupStats(const AddStatFn& add_stat,
                                  CookieIface& cookie) const override;

    bool isWarmupOOMFailure() const override;

    bool hasWarmupSetVbucketStateFailed() const override;

    /**
     * This method store the given cookie for later notification iff Warmup has
     * yet to reach and complete the PopulateVBucketMap phase.
     *
     * @param cookie the callers cookie which might be stored for later
     *        notification (see return value)
     * @return true if the cookie was stored for later notification, false if
     *         not.
     */
    bool maybeWaitForVBucketWarmup(CookieIface* cookie) override;

    /**
     * Creates a warmup task if the engine configuration has "warmup=true"
     */
    void initializeWarmupTask();

    /**
     * Starts the warmup task if one is present
     */
    void startWarmupTask();

    /**
     * Function will execute all final parts of Primary warm-up completion. This
     * includes if configured the creation and scheduling of Secondary warm-up.
     * This function should only be called once by Primary warm-up as it has
     * side effects that should only be executed once.
     */
    void primaryWarmupCompleted();

    /**
     * Construct a compaction context.
     *
     * @param vbid to compact
     * @param config of the compaction
     * @param purgeSeqno starting rollback purge seqno
     * @return
     */
    virtual std::shared_ptr<CompactionContext> makeCompactionContext(
            Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno);

    // implemented by querying StorageProperties for the buckets KVStore
    bool isByIdScanSupported() const override;

    void releaseBlockedCookies() override;

    void initiateShutdown() override;

    bool canEvictFromReplicas() override {
        return true;
    }

    bool maybeScheduleManifestPersistence(
            CookieIface* cookie,
            const Collections::Manifest& newManifest) override;

    /**
     * Gets the BgFetcher for the corresponding vBucket. If there are multiple
     * BgFetchers assigned to this vBucket, the distributionKey is used to
     * select a BgFetcher from that set.
     */
    BgFetcher& getBgFetcher(Vbid vbid, uint32_t distributionKey);
    Flusher* getFlusher(Vbid vbid);

    Flusher* getOneFlusher() override;

    /// Hook that gets called after a the EPBuckets purge seqno has been update
    /// during an implicit compaction
    TestingHook<> postPurgeSeqnoImplicitCompactionHook;

    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            CookieIface& cookie,
            std::unique_ptr<RangeScanDataHandlerIFace> handler,
            const cb::rangescan::CreateParameters& params) override;

    cb::engine_errc continueRangeScan(
            CookieIface& cookie,
            const cb::rangescan::ContinueParameters& params) override;

    cb::engine_errc cancelRangeScan(Vbid vbid,
                                    cb::rangescan::Id uuid,
                                    CookieIface& cookie) override;

    cb::engine_errc prepareForPause(folly::CancellationToken) override;

    cb::engine_errc prepareForResume() override;

    ReadyRangeScans* getReadyRangeScans() {
        return &rangeScans;
    }

    /**
     * Take the next available scan out of the 'ready' scans container.
     * The calling task must provide their unique ID and if there are no scans
     * available the task is removed from the internal set of known tasks. The
     * calling task must now exit and not reschedule.
     *
     * @param taskId unique-id of the tasking taking the next scan
     * @return The next scan or a nullptr if no scans available
     */
    std::shared_ptr<RangeScan> takeNextRangeScan(size_t taskId);

    bool disconnectReplicationAtOOM() const override;

    cb::engine_errc prepareSnapshot(
            CookieIface& cookie,
            Vbid vbid,
            const std::function<void(const nlohmann::json&)>& callback)
            override;

    cb::engine_errc downloadSnapshot(CookieIface& cookie,
                                     Vbid vbid,
                                     std::string_view metadata) override;

    cb::engine_errc getSnapshotFileInfo(
            CookieIface& cookie,
            std::string_view uuid,
            std::size_t file_id,
            const std::function<void(const nlohmann::json&)>& callback)
            override;

    cb::engine_errc releaseSnapshot(
            CookieIface& cookie,
            std::variant<Vbid, std::string_view> snapshotToRelease) override;

    cb::engine_errc initialiseSnapshots();

    cb::engine_errc doSnapshotDebugStats(const StatCollector&) override;

    cb::engine_errc syncFusionLogstore(Vbid vbid) override;

    cb::engine_errc startFusionUploader(Vbid vbid, uint64_t term) override;

    cb::engine_errc stopFusionUploader(Vbid vbid) override;

    cb::engine_errc doFusionStats(CookieIface& cookie,
                                  const AddStatFn& add_stat,
                                  std::string_view statKey) override;

    /**
     * Handle the brief snapshot-status stat which is used by ns_server to
     * monitor downloads.
     * Function will collect snapshot status for each vbucket or one vbucket if
     * the input specifies a vbid.
     */
    cb::engine_errc doSnapshotStatus(const StatCollector&,
                                     std::string_view) override;

    /**
     * @return the cached disk space info (non-const as this call may change the
     *         cached value as time progresses)
     */
    std::filesystem::space_info getCachedDiskSpaceInfo();

    /// Hook that gets called from prepareForPause. Phase of prepareForPause()
    /// specified by the single string_view arg
    TestingHook<std::string_view> prepareForPauseTestingHook;

    std::variant<cb::engine_errc, std::unordered_set<std::string>>
    getEncryptionKeyIds() override;

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    class ValueChangedListener;

    /**
     * Add the given queued_item to the flush batch
     *
     * @param txnCtx context for the transaction (flush batch)
     * @param qi item to add
     * @param vb EP vBucket being flushed
     */
    void flushOneDelOrSet(TransactionContext& txnCtx,
                          const queued_item& qi,
                          EPVBucket& vb);

    /**
     * Compaction of a database file
     *
     * @param config the configuration to use for running compaction
     * @return bool if the compaction was successful or not
     */
    bool compactInternal(LockedVBucketPtr& vb, CompactionConfig& config);

    /**
     * Callback to be called on completion of the compaction (just before the
     * atomic switch of the files)
     */
    void compactionCompletionCallback(CompactionContext& ctx);

    /**
     * Update collection state (VB::Manifest) after compaction has completed.
     *
     * @param vb VBucket ref
     * @param stats Map of cid to new size value (new value not delta)
     * @param onDiskDroppedCollectionDataExists true if the compacted file
     *        has dropped collections (documents and/or metadata).
     */
    void updateCollectionStatePostCompaction(
            VBucket& vb,
            CompactionStats::CollectionSizeUpdates& stats,
            bool onDiskDroppedCollectionDataExists);

    void stopWarmup();

    /// function which is passed down to compactor for dropping keys
    virtual void dropKey(VBucket& vb,
                         const DiskDocKey& key,
                         int64_t bySeqno,
                         bool isAbort,
                         int64_t highCompletedSeqno);

    /**
     * Performs operations that must be performed after flush succeeds,
     * regardless of whether we flush non-meta items or a new vbstate only.
     *
     * @param vb
     * @param flushStart Used for updating stats
     * @param itemsFlushed Used for updating stats
     * @param aggStats Used for updating stats
     * @param collectionFlush Used for performing collection-related operations
     */
    virtual void flushSuccessEpilogue(
            VBucket& vb,
            const cb::time::steady_clock::time_point flushStart,
            size_t itemsFlushed,
            const AggregatedFlushStats& aggStats,
            Collections::VB::Flush& collectionFlush);

    /**
     * Performs operations that must be performed after flush fails,
     * regardless of whether we flush non-meta items or a new vbstate only.
     *
     * @param itemsToFlush Used for performing post-flush operations
     */
    void flushFailureEpilogue(EPVBucket& vb,
                              ItemsToFlush& itemsToFlush,
                              std::size_t numItems,
                              std::string_view snapStart,
                              std::string_view snapEnd);

    bool isValidBucketDurabilityLevel(
            cb::durability::Level level) const override;

    /**
     * Setup shards.
     */
    void initializeShards();

    /**
     * Schedule a new CompactTask or request any existing task is rescheduled
     */
    cb::engine_errc scheduleOrRescheduleCompaction(
            Vbid vbid,
            const CompactionConfig& config,
            CookieIface* cookie,
            std::chrono::milliseconds delay);

    /**
     * Recalculate the number of compaction tasks that are allowed to run
     * concurrently.
     *
     * If the number goes down, currently running tasks will not be stopped, but
     * new/waiting tasks will respect the new limit.
     */
    void updateCompactionConcurrency();

    /**
     * Return disk usage information, summed across all shards.
     * @return total file info
     */
    DBFileInfo getAggregatedFileInfo();

    /**
     * Called from deinitialize path - iterates over all vbuckets of the bucket
     * 1) Checks (and warns) if pending fetches exist
     * 2) Cancels all range-scans
     */
    void allVbucketsDeinitialize();

    /**
     * Given the value of "config:warmup_behavior" setup the 4 configuration
     * parameters (primary/secondary)
     * @param behavior see configuration.json warmup_behavior
     */
    void setupWarmupConfig(std::string_view behavior);

    /**
     * @return space_info for the config.getDbname directory
     */
    std::filesystem::space_info getDiskSpaceUsed() const;

    cb::engine_errc doFusionVBucketStats(CookieIface& cookie,
                                         const AddStatFn& add_stat,
                                         FusionStat stat,
                                         Vbid vbid);

    cb::engine_errc doFusionAggregatedStats(CookieIface& cookie,
                                            const AddStatFn& add_stat,
                                            FusionStat stat);

    cb::engine_errc doFusionAggregatedGuestVolumesStats(
            CookieIface& cookie, const AddStatFn& add_stat);

    cb::engine_errc doFusionAggregatedUploaderStats(CookieIface& cookie,
                                                    const AddStatFn& add_stat);

    /**
     * Max number of backill items in a single flusher batch before we split
     * into multiple batches. Actual batch size may be larger as we will not
     * split Memory Checkpoints, a hard limit is only imposed for Disk
     * Checkpoints (i.e. replica backfills).
     * Atomic as can be changed by ValueChangedListener on one thread and read
     * by flusher on other thread.
     */
    std::atomic<size_t> flusherBatchSplitTrigger;

    /**
     * Max size (in bytes) of a single flush-batch passed to the KVStore for
     * persistence.
     */
    std::atomic<size_t> flushBatchMaxBytes;

    /**
     * Indicates whether erroneous tombstones need to retained or not during
     * compaction
     */
    cb::RelaxedAtomic<bool> retainErroneousTombstones;

    std::unique_ptr<Warmup> warmupTask;

    // The secondary warmup task is created only once. Some operations require
    // synchronisation. After the task is created, we want to be able to cheaply
    // read it for stats.
    SynchronizedInitOncePtr<Warmup> secondaryWarmup;

    std::unordered_map<Vbid, std::shared_ptr<VBucketLoadingTask>>
            vbucketsLoading;

    std::vector<std::unique_ptr<BgFetcher>> bgFetchers;

    /**
     * The Flusher objects belonging to this bucket. Each Flusher is responsible
     * for a subset of the vBuckets. Currently indexed by shard but in a future
     * patch will be indexed by a new configuration variable.
     *
     * @TODO MB-39745: Update above comment.
     */
    std::vector<std::unique_ptr<Flusher>> flushers;

    folly::Synchronized<std::unordered_map<Vbid, std::shared_ptr<CompactTask>>>
            compactionTasks;
    // Semaphore limiting how many compaction tasks may run concurrently
    std::unique_ptr<cb::AwaitableSemaphore> compactionSemaphore;

    /**
     * Bool referenced during compaction that is checked to determine if we
     * should abort the compaction due to an incoming shutdown.
     */
    std::atomic<bool> cancelEWBCompactionTasks = false;

    /**
     * Testing hook called from EPBucket::compactionCompletionCallback function
     * before we update the stats.
     */
    TestingHook<> postCompactionCompletionStatsUpdateHook;

    /**
     * A 'container' of RangeScan objects that have been "continued". These are
     * all eligible for execution on an I/O task to return data to a waiting
     * client
     */
    ReadyRangeScans rangeScans;

    /// The snapshot manager responsible for keeping track of all snapshots
    /// for this bucket
    cb::snapshot::Cache snapshotCache;

    /// The snapshot controller responsible for keeping track of snapshot
    /// download tasks for this bucket
    cb::snapshot::DownloadSnapshotController snapshotController;

    /// A cached (refresh by elapsed time) space_info for the db_name directory
    /// lock ensures only one update when expired and safe read/write of data
    folly::Synchronized<AutoRefreshedValue<std::filesystem::space_info>,
                        std::mutex>
            diskSpaceInfo;
};

std::ostream& operator<<(std::ostream& os, const EPBucket::FlushResult& res);

/**
 * Callback for notifying flusher about pending mutations.
 */
class NotifyFlusherCB : public Callback<Vbid> {
public:
    NotifyFlusherCB(KVShard* sh) : shard(sh) {
    }

    void callback(Vbid& vb) override;

private:
    KVShard* shard;
};
