/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "callbacks.h"
#include "checkpoint_types.h"
#include "ep_types.h"
#include "kv_bucket_iface.h"
#include "kvstore/kvstore_iface.h"
#include "locked_vbucket_ptr.h"
#include "stored-value.h"
#include "storeddockey.h"
#include "utility.h"
#include "vbucket_fwd.h"
#include "vbucket_types.h"
#include "vbucketmap.h"
#include <executor/task_type.h>

#include <cstdlib>
#include <deque>

class CheckpointDestroyerTask;
class DurabilityCompletionTask;
class NotifiableTask;
class ReplicationThrottle;
class SeqnoPersistenceNotifyTask;
class VBucketCountVisitor;
class DatatypeStatVisitor;
namespace Collections {
class Manager;
}

/**
 * VBucket Callback Adaptor is a helper task used to implement visitAsync().
 *
 * It is used to assist in visiting multiple vBuckets, without creating a
 * separate task (and associated task overhead) for each vBucket individually.
 *
 * The set of vBuckets to visit is obtained by applying
 * VBucketVisitor::getVBucketFilter() to the set of vBuckets the Bucket has.
 */
class VBCBAdaptor : public GlobalTask {
public:
    VBCBAdaptor(KVBucket* s,
                TaskId id,
                std::unique_ptr<InterruptableVBucketVisitor> v,
                const char* l,
                bool shutdown);
    VBCBAdaptor(const VBCBAdaptor&) = delete;
    const VBCBAdaptor& operator=(const VBCBAdaptor&) = delete;

    std::string getDescription() const override;

    /// Set the maximum expected duration for this task.
    void setMaxExpectedDuration(std::chrono::microseconds duration) {
        maxDuration = duration;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return maxDuration;
    }

    /**
     * Execute the VBCBAdapter task using our visitor.
     *
     * Calls the visitVBucket() method of the visitor object for each vBucket.
     * Before each visitVBucket() call, calls shouldInterrupt() to check if
     * visiting should be paused or stopped.
     * If paused, will sleep for 0s, yielding execution back to the executor -
     * to allow any higher priority tasks to run. When run() is called again,
     * will resume from the vBucket it paused at.
     * While if stopped, the task will just complete and return to the executor.
     */
    bool run() override;

private:
    KVBucket* store;
    std::unique_ptr<InterruptableVBucketVisitor> visitor;
    const char                 *label;
    std::chrono::microseconds maxDuration;

    /**
     * VBuckets the visitor has not yet visited.
     * Vbs will be sorted according to visitor->getVBucketComparator().
     * Once visited, vbuckets will be removed, so the visitor can resume after
     * pausing at the first element.
     */
    std::deque<Vbid> vbucketsToVisit;

    /**
     * Current VBucket.
     * This value starts as "None" and is only changed to another value when
     * we attempt to work on a valid vbucket.
     *
     * RelaxedAtomic as this is used by getDescription to generate the task
     * description, which can be called by threads other than the one executing.
     */
    const Vbid::id_type None = std::numeric_limits<Vbid::id_type>::max();
    cb::RelaxedAtomic<Vbid::id_type> currentvb{None};
};

const uint16_t EP_PRIMARY_SHARD = 0;
class KVShard;

/**
 * KVBucket is the base class for concrete Key/Value bucket implementations
 * which use the concept of VBuckets to support replication, persistence and
 * failover.
 *
 */
class KVBucket : public KVBucketIface {
public:
    explicit KVBucket(EventuallyPersistentEngine& theEngine);
    virtual ~KVBucket();

    KVBucket(const KVBucket&) = delete;
    const KVBucket& operator=(const KVBucket&) = delete;

    bool initialize() override;

    void deinitialize() override;

    cb::engine_errc set(Item& item,
                        const CookieIface* cookie,
                        cb::StoreIfPredicate predicate = {}) override;

    cb::engine_errc add(Item& item, const CookieIface* cookie) override;

    cb::engine_errc replace(Item& item,
                            const CookieIface* cookie,
                            cb::StoreIfPredicate predicate = {}) override;

    GetValue get(const DocKey& key,
                 Vbid vbucket,
                 const CookieIface* cookie,
                 get_options_t options) override;

    GetValue getRandomKey(CollectionID cid, const CookieIface* cookie) override;

    GetValue getReplica(const DocKey& key,
                        Vbid vbucket,
                        const CookieIface* cookie,
                        get_options_t options) override;

    cb::engine_errc getMetaData(const DocKey& key,
                                Vbid vbucket,
                                const CookieIface* cookie,
                                ItemMetaData& metadata,
                                uint32_t& deleted,
                                uint8_t& datatype) override;

    cb::engine_errc setWithMeta(
            Item& item,
            uint64_t cas,
            uint64_t* seqno,
            const CookieIface* cookie,
            PermittedVBStates permittedVBStates,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno = GenerateBySeqno::Yes,
            GenerateCas genCas = GenerateCas::No,
            ExtendedMetaData* emd = nullptr) override;

    cb::engine_errc prepare(Item& item, const CookieIface* cookie) override;

    GetValue getAndUpdateTtl(const DocKey& key,
                             Vbid vbucket,
                             const CookieIface* cookie,
                             time_t exptime) override;

    cb::engine_errc deleteItem(
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const CookieIface* cookie,
            std::optional<cb::durability::Requirements> durability,
            ItemMetaData* itemMeta,
            mutation_descr_t& mutInfo) override;

    cb::engine_errc deleteWithMeta(const DocKey& key,
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
                                   DeleteSource deleteSource) override;

    void reset() override;

    bool pauseFlusher() override;
    bool resumeFlusher() override;
    void wakeUpFlusher() override;

    void snapshotStats(bool shuttingDown) override;

    void getAggregatedVBucketStats(
            const BucketStatCollector& collector,
            cb::prometheus::Cardinality cardinality) override;

    /**
     * Helper to default to collecting all stats, while avoiding default args
     * on a virtual method.
     */
    void getAggregatedVBucketStats(const BucketStatCollector& collector) {
        getAggregatedVBucketStats(collector, cb::prometheus::Cardinality::All);
    }

    void completeBGFetchMulti(Vbid vbId,
                              std::vector<bgfetched_item_t>& fetchedItems,
                              std::chrono::steady_clock::time_point start) override;

    /**
     * Returns the number of vbuckets in a given state.
     * @param state  the vbucket state to compare against
     * @return  the number of vbuckets in the requested state
     */
    uint16_t getNumOfVBucketsInState(vbucket_state_t state) const;

    /**
     * Returns a vector containing the vbuckets from the vbMap that are in
     * the given state.
     * @param state  the state used to filter which vbuckets to return
     * @return  vector of vbuckets that are in the given state.
     */
    std::vector<Vbid> getVBucketsInState(vbucket_state_t state) const {
        return vbMap.getBucketsInState(state);
    }

    VBucketPtr getVBucket(Vbid vbid) override {
        return vbMap.getBucket(vbid);
    }

    /**
     * Return a pointer to the given VBucket, acquiring the appropriate VB
     * mutex lock at the same time.
     * @param vbid VBucket ID to get.
     * @return A RAII-style handle which owns the correct VBucket mutex,
     *         alongside a shared pointer to the requested VBucket.
     */
    LockedVBucketPtr getLockedVBucket(Vbid vbid) {
        std::unique_lock<std::mutex> lock(vb_mutexes[vbid.get()]);
        return {vbMap.getBucket(vbid), std::move(lock)};
    }

    /**
     * Attempt to return a pointer to the given VBucket and the VBucket's mutex,
     * if the mutex isn't already acquired.
     * @param vbid VBucket ID to get.
     * @return If the mutex was available then a pointer to the VBucket and
     *         a lock on the mutex. If the mutex was already locked then returns
     *         nullptr and a lock object which owns_lock() returns false.
     *
     * For correct usage, clients should call owns_lock() to check if they
     * successfully acquired a locked VBucket.
     */
    LockedVBucketPtr getLockedVBucket(Vbid vbid, std::try_to_lock_t) {
        std::unique_lock<std::mutex> lock(vb_mutexes[vbid.get()],
                                          std::try_to_lock);
        if (!lock) {
            return {{}, std::move(lock)};
        }
        return {vbMap.getBucket(vbid), std::move(lock)};
    }

    size_t getMemFootPrint();

    std::pair<uint64_t, bool> getLastPersistedCheckpointId(Vbid vb) override {
        // No persistence at the KVBucket class level.
        return {0, false};
    }

    uint64_t getLastPersistedSeqno(Vbid vb) override;

    /**
     * Release all cookies blocked on pending requests (e.g. SyncWrites,
     * requests waiting for warmup to complete).
     *
     * This method is called during bucket shutdown to make sure that
     * all of the cookies blocked waiting on a request to complete are released
     * so that we can continue bucket deletion.
     * For example, as part of bucket deletion one of the first things we do is
     * to tear down the DCP streams so that SyncWrites will never be notified
     * and would be stuck waiting for a timeout if we don't explicitly release
     * them.
     */
    virtual void releaseBlockedCookies();

    /**
     * Sets the vbucket or creates a vbucket with the desired state
     *
     * @param vbid vbucket id
     * @param state desired state of the vbucket
     * @param meta optional meta information to apply alongside the state
     * @param transfer indicates that the vbucket is transferred to the active
     *                 post a failover and/or rebalance
     * @param cookie under certain conditions we may use ewouldblock
     *
     * @return status of the operation
     */
    cb::engine_errc setVBucketState(Vbid vbid,
                                    vbucket_state_t state,
                                    const nlohmann::json* meta = {},
                                    TransferVB transfer = TransferVB::No,
                                    const CookieIface* cookie = nullptr);

    /**
     * Sets the vbucket to the desired state
     *
     * @param vb shared_ptr to the vbucket to set the state on
     * @param state desired state for the vbucket
     * @param meta optional meta information to apply alongside the state
     * @param transfer indicates that the vbucket is transferred to the active
     *                 post a failover and/or rebalance
     * @param notify_dcp indicates whether we must consider closing DCP streams
     *                    associated with the vbucket
     * @param vbset std::lock_guard<std::mutex> acquiring the 'vbsetMutex' lock
     * in the EventuallyPersistentStore class
     * @param vbStateLock WriterLockHolder of 'stateLock' in the vbucket
     *                    class.
     */
    void setVBucketState_UNLOCKED(VBucketPtr& vb,
                                  vbucket_state_t to,
                                  const nlohmann::json* meta,
                                  TransferVB transfer,
                                  bool notify_dcp,
                                  std::unique_lock<std::mutex>& vbset,
                                  folly::SharedMutex::WriteHolder& vbStateLock);

    /**
     * Creates the vbucket in the desired state
     *
     * @param vbid vbucket id
     * @param state desired state of the vbucket
     * @param meta optional meta information to apply alongside the state
     * @param vbset std::lock_guard<std::mutex> acquiring the 'vbsetMutex' lock
     * in the EventuallyPersistentStore class
     *
     * @return status of the operation
     */
    cb::engine_errc createVBucket_UNLOCKED(Vbid vbid,
                                           vbucket_state_t state,
                                           const nlohmann::json* meta,
                                           std::unique_lock<std::mutex>& vbset);
    /**
     * Returns the 'vbsetMutex'
     */
    std::mutex& getVbSetMutexLock() {
        return vbsetMutex;
    }

    cb::engine_errc deleteVBucket(Vbid vbid,
                                  const CookieIface* c = nullptr) override;

    cb::engine_errc checkForDBExistence(Vbid db_file_id) override;

    bool resetVBucket(Vbid vbid) override;

    void visit(VBucketVisitor &visitor) override;

    size_t visitAsync(std::unique_ptr<InterruptableVBucketVisitor> visitor,
                      const char* lbl,
                      TaskId id,
                      std::chrono::microseconds maxExpectedDuration) override;

    Position pauseResumeVisit(PauseResumeVBVisitor& visitor,
                              Position& start_pos,
                              VBucketFilter* filter = nullptr) override;

    Position startPosition() const override;

    Position endPosition() const override;

    Warmup* getWarmup() const override;

    cb::engine_errc getKeyStats(const DocKey& key,
                                Vbid vbucket,
                                const CookieIface* cookie,
                                key_stats& kstats,
                                WantsDeleted wantsDeleted) override;

    std::string validateKey(const DocKey& key, Vbid vbucket, Item& diskItem) override;

    GetValue getLocked(const DocKey& key,
                       Vbid vbucket,
                       rel_time_t currentTime,
                       uint32_t lockTimeout,
                       const CookieIface* cookie) override;

    cb::engine_errc unlockKey(const DocKey& key,
                              Vbid vbucket,
                              uint64_t cas,
                              rel_time_t currentTime,
                              const CookieIface* cookie) override;

    KVStoreIface* getRWUnderlying(Vbid vbId) override {
        return vbMap.getShardByVbId(vbId)->getRWUnderlying();
    }

    KVStoreIface* getRWUnderlyingByShard(size_t shardId) override {
        return vbMap.shards[shardId]->getRWUnderlying();
    }

    const KVStoreIface* getROUnderlyingByShard(size_t shardId) const override {
        return vbMap.shards[shardId]->getROUnderlying();
    }

    const KVStoreIface* getROUnderlying(Vbid vbId) const override {
        return vbMap.getShardByVbId(vbId)->getROUnderlying();
    }

    std::unique_ptr<KVStoreIface> takeRW(size_t shardId) override;

    void setRW(size_t shardId, std::unique_ptr<KVStoreIface> rw) override;

    cb::mcbp::Status evictKey(const DocKey& key,
                              Vbid vbucket,
                              const char** msg) override;

    /**
     * Run the server-api pre-expiry hook against the Item - the function
     * may (if the pre-expiry hook dictates) mutate the Item value so that
     * xattrs and the value are removed. The method doesn't care for the Item
     * state (i.e. isDeleted) and the callers should be passing expired/deleted
     * items only.
     *
     * @param vb The vbucket it belongs to
     * @param it A reference to the Item to run the hook against and possibly
     *        mutate.
     */
    void runPreExpiryHook(VBucket& vb, Item& it);

    void processExpiredItem(Item& it,
                            time_t startTime,
                            ExpireBy source) override;

    /**
     * Get the value for the Item
     * If the value is already deleted no update occurs
     * If a value can be retrieved then it is updated via setValue
     * @param it reference to an Item which maybe updated
     */
    void getValue(Item& it);

    const StorageProperties getStorageProperties() const override;

    void scheduleVBStatePersist() override;

    void scheduleVBStatePersist(Vbid vbid) override;

    const VBucketMap& getVBuckets() const override {
        return vbMap;
    }

    EventuallyPersistentEngine& getEPEngine() override {
        return engine;
    }

    const EventuallyPersistentEngine& getEPEngine() const override {
        return engine;
    }

    size_t getExpiryPagerSleeptime() override {
        return expiryPagerTask->getSleepTime().count();
    }

    size_t getTransactionTimePerItem() override {
        return lastTransTimePerItem.load();
    }

    void setBackfillMemoryThreshold(double threshold) override;

    void setExpiryPagerSleeptime(size_t val) override;
    void setExpiryPagerTasktime(ssize_t val) override;
    void enableExpiryPager() override;
    void disableExpiryPager() override;

    /// Wake up the expiry pager (if enabled), scheduling it for immediate run.
    void wakeUpExpiryPager();

    /// Wake up the item pager (if enabled), scheduling it for immediate run.
    /// Currently this is used only during testing.
    void wakeItemPager();
    void enableItemPager();
    void disableItemPager();

    /// Wake up the ItemFreqDecayer Task, scheduling it for immediate run.
    void wakeItemFreqDecayerTask();

    void enableAccessScannerTask() override;
    void disableAccessScannerTask() override;
    void setAccessScannerSleeptime(size_t val, bool useStartTime) override;
    void resetAccessScannerStartTime() override;

    void resetAccessScannerTasktime() override {
        accessScanner.lastTaskRuntime = std::chrono::steady_clock::now();
    }

    void enableItemCompressor();

    void setAllBloomFilters(bool to) override;

    float getBfiltersResidencyThreshold() override {
        return bfilterResidencyThreshold;
    }

    void setBfiltersResidencyThreshold(float to) override {
        bfilterResidencyThreshold = to;
    }

    bool isMetaDataResident(VBucketPtr &vb, const DocKey& key) override;

    void logQTime(const GlobalTask& taskType,
                  std::string_view threadName,
                  std::chrono::steady_clock::duration enqTime) override;

    void logRunTime(const GlobalTask& taskType,
                    std::string_view threadName,
                    std::chrono::steady_clock::duration runTime) override;

    void updateCachedResidentRatio(size_t activePerc, size_t replicaPerc) override {
        cachedResidentRatio.activeRatio.store(activePerc);
        cachedResidentRatio.replicaRatio.store(replicaPerc);
    }

    bool isMemUsageAboveBackfillThreshold() override;

    /**
     * @return The current pageable memory usage of the Bucket. This is the
     * amount of bytes which could _potentially_ be paged out (made available)
     * by the ItemPager.
     * Note: This *isn't* necessarily the same as mem_used - for example an
     * Ephemeral bucket can only page out (auto-delete) non-replica items so
     * this is a measure of active memory there.
     */
    virtual size_t getPageableMemCurrent() const = 0;

    /**
     * @return The pageable memory high watermark of the Bucket. this is the
     * threshold of pageable memory that the ItemPager will be run to attempt
     * to reduce memory usage.
     */
    virtual size_t getPageableMemHighWatermark() const = 0;

    /**
     * @return The pageable memory low watermark of the Bucket. this is the
     * amount of bytes the ItemPager will attempt to reduce pagable memory
     * usage to when it has exceeded the pageable high watermark.
     */
    virtual size_t getPageableMemLowWatermark() const = 0;

    /**
     * Check the status of memory used and maybe begin to free memory if
     * required.
     */
    void checkAndMaybeFreeMemory();

    void addKVStoreStats(const AddStatFn& add_stat,
                         const CookieIface* cookie) override;

    void addKVStoreTimingStats(const AddStatFn& add_stat,
                               const CookieIface* cookie) override;

    GetStatsMap getKVStoreStats(
            gsl::span<const std::string_view> keys) override;

    bool getKVStoreStat(std::string_view name, size_t& value) override;

    void resetUnderlyingStats() override;
    const KVStoreIface* getOneROUnderlying() const override;
    KVStoreIface* getOneRWUnderlying() override;

    EvictionPolicy getItemEvictionPolicy() const override {
        return eviction_policy;
    }

    /**
     * Check if this bucket supports eviction from replica vbuckets.
     */
    virtual bool canEvictFromReplicas() = 0;

    TaskStatus rollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void attemptToFreeMemory() override;

    void wakeUpCheckpointMemRecoveryTask() override;

    void runDefragmenterTask() override;

    void runItemFreqDecayerTask() override;

    bool runAccessScannerTask() override;

    void runVbStatePersistTask(Vbid vbid) override;

    void setCompactionWriteQueueCap(size_t to) override {
        compactionWriteQueueCap = to;
    }

    void setCompactionMaxConcurrency(float to) override {
        compactionMaxConcurrency = to;
    }

    bool isAccessScannerEnabled() override {
        std::lock_guard<std::mutex> lh(accessScanner.mutex);
        return accessScanner.enabled;
    }

    bool isExpPagerEnabled() override {
        return expiryPagerTask->isEnabled();
    }

    bool isWarmupLoadingData() override;
    bool isWarmupComplete() override;
    bool isWarmupOOMFailure() override;

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
    bool maybeWaitForVBucketWarmup(const CookieIface* cookie) override;

    size_t getActiveResidentRatio() const override;

    size_t getReplicaResidentRatio() const override;

    cb::engine_errc forceMaxCas(Vbid vbucket, uint64_t cas) override;

    VBucketPtr makeVBucket(Vbid id,
                           vbucket_state_t state,
                           KVShard* shard,
                           std::unique_ptr<FailoverTable> table,
                           NewSeqnoCallback newSeqnoCb,
                           std::unique_ptr<Collections::VB::Manifest> manifest,
                           vbucket_state_t initState = vbucket_state_dead,
                           int64_t lastSeqno = 0,
                           uint64_t lastSnapStart = 0,
                           uint64_t lastSnapEnd = 0,
                           uint64_t purgeSeqno = 0,
                           uint64_t maxCas = 0,
                           int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
                           bool mightContainXattrs = false,
                           const nlohmann::json* replicationTopology = {},
                           uint64_t maxVisibleSeqno = 0) override = 0;

    /**
     * Method to handle set_collections commands
     * @param json a buffer containing a JSON manifest to apply to the bucket
     * @param cookie for the caller (i/o complete requirement)
     */
    cb::engine_error setCollections(std::string_view json,
                                    const CookieIface* cookie);

    /**
     * Method to handle get_collections commands
     * @return a pair with the status and JSON as a std::string
     */
    std::pair<cb::mcbp::Status, nlohmann::json> getCollections(
            const Collections::IsVisibleFunction& isVisible) const;

    /**
     * Method to handle get_collection_id command
     * @param path A path for scope.collection
     * @return pair with error status and result if success
     */
    cb::EngineErrorGetCollectionIDResult getCollectionID(
            std::string_view path) const;

    /**
     * Method to handle get_scope_id command
     * @param path A path for scope
     * @return pair with error status and result if success
     */
    cb::EngineErrorGetScopeIDResult getScopeID(std::string_view path) const;

    /**
     * Method to lookup a collection's scope
     * @param cid collection to lookup
     * @return pair with the manifest uid and the optional scope,
     *              if the returned optional is empty the collection
     *              does not exist in the manifest with the returned
     *              uid
     */
    std::pair<uint64_t, std::optional<ScopeID>> getScopeID(
            CollectionID cid) const;

    /**
     * @return the Collections::Manager as a const reference
     */
    const Collections::Manager& getCollectionsManager() const;

    /**
     * @return the Collections::Manager as a reference
     */
    Collections::Manager& getCollectionsManager();

    /**
     * @return the Collections::Manager (as a shared_ptr)
     */
    const std::shared_ptr<Collections::Manager>& getSharedCollectionsManager()
            const;

    bool isXattrEnabled() const;

    void setXattrEnabled(bool value);

    /**
     * Returns the replication throttle instance
     *
     * @return Ref to replication throttle
     */
    ReplicationThrottle& getReplicationThrottle() {
        return *replicationThrottle;
    }

    /// return the buckets maxTtl value
    std::chrono::seconds getMaxTtl() const;

    /// set the buckets maxTtl
    void setMaxTtl(size_t max);

    /**
     * Set the Bucket Minimum Durability Level to the given level.
     *
     * @param level
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc setMinDurabilityLevel(cb::durability::Level level);

    cb::durability::Level getMinDurabilityLevel() const;

    /**
     * @param vbid
     * @return The ShardId
     */
    KVShard::id_type getShardId(Vbid vbid) const;

    /**
     * Set the max ratio of bucket quota that can be allocated in checkpoints.
     *
     * @param ratio
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc setCheckpointMemoryRatio(float ratio);

    float getCheckpointMemoryRatio() const;

    /**
     * Set the ratio of the checkpoint quota (as computed by
     * checkpoint_memory_ratio) that triggers attempt of memory releasing from
     * checkpoint.
     *
     * @param ratio
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc setCheckpointMemoryRecoveryUpperMark(float ratio);

    float getCheckpointMemoryRecoveryUpperMark() const;

    /**
     * Sets the ratio of the checkpoint quota (as computed by
     * checkpoint_memory_ratio) that represents the target of checkpoint memory
     * recovery. Memory recovery yields when reached.
     *
     * @param ratio
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc setCheckpointMemoryRecoveryLowerMark(float ratio);

    float getCheckpointMemoryRecoveryLowerMark() const;

    /**
     * Sets the maximum size (in bytes) for a single checkpoint.
     * '0' triggers EP auto-setup, see 'autoConfigCheckpointMaxSize()' for
     * details.
     *
     * @param size
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc setCheckpointMaxSize(size_t size);

    /**
     * Sets the checkpoint-max-size based on other system parameters (eg, num of
     * vbuckets, checkpoint quota, max num of checkpoints per vbucket, etc..)
     * for optimal balance and checkpoint memory releasing capabilities.
     *
     * @return success if the operation succeeds, an error code otherwise
     */
    cb::engine_errc autoConfigCheckpointMaxSize();

    bool isCheckpointMaxSizeAutoConfig() const;

    /**
     * @return The size of the vb-map, ie how many checkpoint this bucket can
     *  manage.
     */
    size_t getVBMapSize() const;

    enum class CheckpointMemoryState : uint8_t {
        Available,
        NeedsRecovery,
        Full
    };

    /**
     * @return the current state of total checkpoints memory usage as defined by
     * CheckpointMemoryState
     */
    CheckpointMemoryState getCheckpointMemoryState() const;

    /**
     * Verifies the current state of total checkpoints memory usage. This may
     * trigger attempt of checkpoint memory recovery.
     *
     * @return Information on the state of checkpoints mem-usage as defined by
     * CheckpointMemoryState
     */
    CheckpointMemoryState verifyCheckpointMemoryState();

    /**
     * @return The Checkpoint memory quota (in bytes)
     */
    size_t getCMQuota() const;

    /**
     * @return The Checkpoint memory recovery threshold (in bytes). That is the
     * level of CM mem-usage that triggers memory recovery.
     */
    size_t getCMRecoveryUpperMarkBytes() const;

    /**
     * @return The Checkpoint memory recovery target (in bytes). When CM memory
     * recovery triggers, that is final CM mem-usage target that we aim to hit.
     */
    size_t getCMRecoveryLowerMarkBytes() const;

    /**
     * Determines if checkpoint memory recovery is necessary and returns a
     * computed memory reduction target. Everything is based on the Checkpoint
     * Quota and the memory-recovery triggers in configuration.
     *
     * The following diagram depicts the bucket memory where Q is the bucket
     * quota and the labelled vertical lines each show thresholds/stats used in
     * deciding if memory reduction is needed.
     *
     * 0                                                                      Q
     * ├──────────────┬────────────┬───┬───────┬──────────────────────────────┤
     * │              │            │   │       │                              │
     * │              │            │   │       │                              │
     * └──────────────▼────────────▼───▼───────▼──────────────────────────────┘
     *                A            B   X       C
     *
     * A = checkpoint_memory_recovery_lower_mark
     * B = checkpoint_memory_recovery_upper_mark
     * C = checkpoints quota (as defined by checkpoint_memory_ratio)
     * X = current checkpoint memory used
     * Q = bucket quota
     *
     * This function will return (X - A) as the target amount to free.
     *
     * @return a calculated memory reduction target (in bytes) - 0 if checkpoint
     *  memory is not above the low mark
     */
    size_t getRequiredCheckpointMemoryReduction() const;

    /**
     * Check if checkpoint memory usage exceeds the high mark.
     */
    bool isCheckpointMemoryReductionRequired() const;

    /**
     * Schedule destructoin of the given checkpoints for vbid.
     *
     * @param checkpoints
     * @param vbid
     */
    void scheduleDestruction(CheckpointList&& checkpoints, Vbid vbid);

    size_t getCheckpointRemoverTaskCount() const {
        return chkRemovers.size();
    }

    size_t getCheckpointPendingDestructionMemoryUsage() const;

    /**
     * Sets the timeout in seconds after which a pending SeqnoPersistence
     * operation is temp-failed
     *
     * @param timeout
     */
    void setSeqnoPersistenceTimeout(std::chrono::seconds timeout);

    std::chrono::seconds getSeqnoPersistenceTimeout() const;

    std::shared_ptr<SeqnoPersistenceNotifyTask>
    createAndScheduleSeqnoPersistenceNotifier();

    /**
     * Add a vbucket that has a SeqnoPersistentRequest (so it can be notified
     * of expiry).
     *
     * @param vbid ID of vbucket with new SeqnoPersistenceRequest
     * @param deadline the absolute time at which a request should result in the
     *        client being notified and the request removed.
     */
    void addVbucketWithSeqnoPersistenceRequest(
            Vbid vbid, std::chrono::steady_clock::time_point deadline);

    /// @return the waketime of the SeqnoPersistenceNotifyTask
    std::chrono::steady_clock::time_point
    getSeqnoPersistenceNotifyTaskWakeTime() const;

    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            Vbid vbid,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            std::unique_ptr<RangeScanDataHandlerIFace> handler,
            const CookieIface& cookie,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig)
            override;
    cb::engine_errc continueRangeScan(
            Vbid vbid,
            cb::rangescan::Id uuid,
            const CookieIface& cookie,
            size_t itemLimit,
            std::chrono::milliseconds timeLimit) override;
    cb::engine_errc cancelRangeScan(Vbid vbid, cb::rangescan::Id uuid) override;

protected:
    /**
     * Get the checkpoint destroyer task responsible for checkpoints from the
     * given vbid.
     */
    CheckpointDestroyerTask& getCkptDestroyerTask(Vbid vbid) const;

    GetValue getInternal(const DocKey& key,
                         Vbid vbucket,
                         const CookieIface* cookie,
                         ForGetReplicaOp getReplicaItem,
                         get_options_t options) override;

    bool resetVBucket_UNLOCKED(LockedVBucketPtr& vb,
                               std::unique_lock<std::mutex>& vbset);

    /* Notify flusher of a new seqno being added in the vbucket */
    virtual void notifyFlusher(const Vbid vbid);

    /**
     * Notify replication of a new seqno being added in the vbucket
     *
     * @param vbid vBucket ID
     * @param bySeqno new high seqno
     * @param syncWriteOnly is this a SyncWrite operation (we don't notify
     *        producers that do not care about SyncWrites of prepares).
     */
    void notifyReplication(const Vbid vbid,
                           const int64_t bySeqno,
                           SyncWriteOperation syncWrite);

    /// Helper method from initialize() to setup the expiry pager
    void initializeExpiryPager(Configuration& config);

    /**
     * Check whether the ItemFreqDecayer Task is snoozed.  Currently only
     * used for testing purposes.
     */
    bool isItemFreqDecayerTaskSnoozed() const {
        return (itemFreqDecayerTask->getState() == TASK_SNOOZED);
    }

    /// Factory method to create a VBucket count visitor of the correct type.
    virtual std::unique_ptr<VBucketCountVisitor> makeVBCountVisitor(
            vbucket_state_t state);

    /**
     * Helper method used by getAggregatedVBucketStats to output aggregated
     * bucket stats.
     */
    virtual void appendAggregatedVBucketStats(
            const VBucketCountVisitor& active,
            const VBucketCountVisitor& replica,
            const VBucketCountVisitor& pending,
            const VBucketCountVisitor& dead,
            const BucketStatCollector& collector);

    /**
     * Helper method used by getAggregatedVBucketStats to output aggregated
     * datatype stats.
     */
    void appendDatatypeStats(const DatatypeStatVisitor& active,
                             const DatatypeStatVisitor& replica,
                             const BucketStatCollector& collector);

    /**
     * Returns the callback function to be invoked when SyncWrite(s) have been
     * resolved for the given vBucket and are awaiting Completion (Commit /
     * Abort). Used by makeVBucket().
     */
    SyncWriteResolvedCallback makeSyncWriteResolvedCB();

    /**
     * Returns the callback function to be invoked when a SyncWrite has been
     * completed. Used by makeVBucket().
     */
    SyncWriteCompleteCallback makeSyncWriteCompleteCB();

    /**
     * Returns the callback function to be invoked at Replica for sending a
     * SeqnoAck to the Active.
     */
    SeqnoAckCallback makeSeqnoAckCB() const;

    /**
     * Chech if the given level is a valid Bucket Durability Level for this
     * Bucket.
     *
     * @param level
     * @return
     */
    virtual bool isValidBucketDurabilityLevel(
            cb::durability::Level level) const = 0;

    friend class Warmup;
    friend class WarmupLoadingKVPairs;
    friend class WarmupLoadingData;
    friend class PersistenceCallback;

    // Ref to the Engine that owns this KVBucket
    EventuallyPersistentEngine& engine;

    EPStats                        &stats;
    VBucketMap                      vbMap;
    ExTask itemPagerTask;

    // Checkpoint recovery tasks. The number is determined by config param
    std::vector<std::shared_ptr<NotifiableTask>> chkRemovers;

    /**
     * One or more tasks responsible for destroying checkpoints which are
     * no longer needed.
     *
     * Checkpoints are queued for a task to destroy, rather than being
     * destroyed in the thead of the last user as checkpoints may be
     * large and expensive to destroy.
     *
     * There will always be at least one task; the number is controlled by the
     * config param "checkpoint_destruction_tasks".
     * Checkpoints are distributed across the tasks based on vbid
     * (i.e., taskIndex = vbid % numTasks).
     * Multiple tasks allows checkpoint destruction to be scaled; under heavy
     * workloads checkpoints may be created and consumed faster than they can
     * be destroyed by a single task.
     */
    std::vector<std::shared_ptr<CheckpointDestroyerTask>> ckptDestroyerTasks;

    float                           bfilterResidencyThreshold;
    ExTask                          defragmenterTask;
    ExTask itemCompressorTask;
    // The itemFreqDecayerTask is used to decay the frequency count of items
    // stored in the hash table.  This is required to ensure that all the
    // frequency counts do not become saturated.
    ExTask itemFreqDecayerTask;
    size_t                          compactionWriteQueueCap;

    /**
     * Cached copy of Configuration::compaction_max_concurrent_ratio, to
     * avoid taking that mutex on every compaction schedule.
     */
    float compactionMaxConcurrency;

    // Responsible for enforcing the Durability Timeout for the SyncWrites
    // tracked in this KVBucket.
    ExTask durabilityTimeoutTask;

    /**
     * Factory function which returns on object to handle SyncWrite timeouts
     * for a given vBucket.
     * Only used if durability_timeout_mode == event-driven.
     * Note this defaults to NoopSyncWriteTimeoutFactory to aid in testing;
     * initialise() will set to a non-noop factory (if mode == event-driven).
     */
    SyncWriteTimeoutHandlerFactory syncWriteTimeoutFactory =
            NoopSyncWriteTimeoutFactory;

    /// Responsible for completing (commiting or aborting SyncWrites which have
    /// completed in this KVBucket.
    std::shared_ptr<DurabilityCompletionTask> durabilityCompletionTask;

    /* Vector of mutexes for each vbucket
     * Used by flush operations: flushVB, deleteVB, compactVB, snapshotVB */
    std::vector<std::mutex>       vb_mutexes;

    std::mutex vbsetMutex;
    double backfillMemoryThreshold;

    std::shared_ptr<ExpiredItemPager> expiryPagerTask;

    struct ALogTask {
        ALogTask()
            : sleeptime(0),
              task(0),
              lastTaskRuntime(std::chrono::steady_clock::now()),
              enabled(true) {
        }
        std::mutex mutex;
        size_t sleeptime;
        size_t task;
        std::chrono::steady_clock::time_point lastTaskRuntime;
        bool enabled;
    } accessScanner;
    struct ResidentRatio {
        std::atomic<size_t> activeRatio;
        std::atomic<size_t> replicaRatio;
    } cachedResidentRatio;

    std::atomic<size_t> lastTransTimePerItem;
    EvictionPolicy eviction_policy;

    const std::shared_ptr<Collections::Manager> collectionsManager;

    /**
     * Status of XATTR support for this bucket - this is set from the
     * bucket config and also via set_flush_param. Written/read by differing
     * threads.
     */
    cb::RelaxedAtomic<bool> xattrEnabled;

    /* Contains info about throttling the replication */
    std::unique_ptr<ReplicationThrottle> replicationThrottle;

    std::atomic<size_t> maxTtl;

    /**
     * Allows us to override the random function.  This is used for testing
     * purposes where we want a constant number as opposed to a random one.
     */
    std::function<int()> getRandom = std::rand;

    /**
     * The Minimum Durability Level enforced by this Bucket:
     *
     *  - if Level::None, then just use the Level provided in the SW's dur-reqs,
     *   if any (Level::None actually disable the bucket durability level)
     *  - if lower than the SW's level, again just enforce the SW's dur-reqs
     *  - if higher than the SW's level, then up-level the SW
     *
     * Requires synchronization as it stores a dynamic configuration param.
     */
    std::atomic<cb::durability::Level> minDurabilityLevel;

    /**
     * Max ratio of the bucket quota that can be allocated in checkpoints.
     * Requires synchronization as it stores a dynamic configuration param.
     */
    std::atomic<float> checkpointMemoryRatio;

    /**
     * Fraction of the checkpoint quota (as computed by checkpoint_memory_ratio)
     * that triggers attempt of memory releasing from checkpoint.
     * Requires synchronization as it stores a dynamic configuration param.
     */
    std::atomic<float> checkpointMemoryRecoveryUpperMark;

    /**
     * Fraction of the checkpoint quota (as computed by checkpoint_memory_ratio)
     * that represents the target of checkpoint memory recovery. Memory recovery
     * yields when reached.
     * Requires synchronization as it stores a dynamic configuration param.
     */
    std::atomic<float> checkpointMemoryRecoveryLowerMark;

    /// Timeout in seconds after which a pending SeqnoPersistence operation is
    /// temp-failed
    std::atomic<std::chrono::seconds> seqnoPersistenceTimeout;

    /**
     * A task that will notify (success or timeout) of SeqnoPersistenceRequests.
     */
    std::shared_ptr<SeqnoPersistenceNotifyTask> seqnoPersistenceNotifyTask;

    friend class KVBucketTest;

};

/**
 * Callback for notifying clients of the Bucket (KVBucket) about a new item in
 * the partition (Vbucket).
 */
class NotifyNewSeqnoCB : public Callback<const Vbid, const VBNotifyCtx&> {
public:
    explicit NotifyNewSeqnoCB(KVBucket& kvb) : kvBucket(kvb) {
    }

    void callback(const Vbid& vbid, const VBNotifyCtx& notifyCtx) override;

private:
    KVBucket& kvBucket;
};
