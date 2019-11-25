/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
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

#pragma once

#include "ep_types.h"
#include "kv_bucket_iface.h"
#include "mutation_log.h"
#include "stored-value.h"
#include "storeddockey.h"
#include "task_type.h"
#include "utility.h"
#include "vbucket.h"
#include "vbucketmap.h"

#include <cstdlib>
#include <deque>

class DurabilityCompletionTask;
class ReplicationThrottle;
class VBucketCountVisitor;
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
                std::unique_ptr<PausableVBucketVisitor> v,
                const char* l,
                bool shutdown);

    std::string getDescription() override;

    /// Set the maximum expected duration for this task.
    void setMaxExpectedDuration(std::chrono::microseconds duration) {
        maxDuration = duration;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        return maxDuration;
    }

    /**
     * Execute the VBCBAdapter task using our visitor.
     *
     * Calls the visitVBucket() method of the visitor object for each vBucket.
     * Before each visitVBucket() call, calls pauseVisitor() to check if
     * visiting should be paused. If true, will sleep for 0s, yielding execution
     * back to the executor - to allow any higher priority tasks to run.
     * When run() is called again, will resume from the vBucket it paused at.
     */
    bool run() override;

private:
    KVBucket* store;
    std::unique_ptr<PausableVBucketVisitor> visitor;
    const char                 *label;
    std::chrono::microseconds maxDuration;

    /**
     * Current VBucket.
     * RelaxedAtomic as this is used by getDescription to generate the task
     * description, which can be called by threads other than the one executing.
     */
    cb::RelaxedAtomic<Vbid::id_type> currentvb;

    DISALLOW_COPY_AND_ASSIGN(VBCBAdaptor);
};

const uint16_t EP_PRIMARY_SHARD = 0;
class KVShard;

typedef std::pair<Vbid, ExTask> CompTaskEntry;

/**
 * KVBucket is the base class for concrete Key/Value bucket implementations
 * which use the concept of VBuckets to support replication, persistence and
 * failover.
 *
 */
class KVBucket : public KVBucketIface {
public:

    KVBucket(EventuallyPersistentEngine &theEngine);
    virtual ~KVBucket();

    bool initialize() override;

    void deinitialize() override;

    ENGINE_ERROR_CODE set(Item& item,
                          const void* cookie,
                          cb::StoreIfPredicate predicate = {}) override;

    ENGINE_ERROR_CODE add(Item &item, const void *cookie) override;

    ENGINE_ERROR_CODE replace(Item& item,
                              const void* cookie,
                              cb::StoreIfPredicate predicate = {}) override;

    GetValue get(const DocKey& key,
                 Vbid vbucket,
                 const void* cookie,
                 get_options_t options) override {
        return getInternal(key, vbucket, cookie, ForGetReplicaOp::No, options);
    }

    GetValue getRandomKey() override;

    GetValue getReplica(const DocKey& key,
                        Vbid vbucket,
                        const void* cookie,
                        get_options_t options) override {
        return getInternal(key, vbucket, cookie, ForGetReplicaOp::Yes, options);
    }

    ENGINE_ERROR_CODE getMetaData(const DocKey& key,
                                  Vbid vbucket,
                                  const void* cookie,
                                  ItemMetaData& metadata,
                                  uint32_t& deleted,
                                  uint8_t& datatype) override;

    ENGINE_ERROR_CODE setWithMeta(
            Item& item,
            uint64_t cas,
            uint64_t* seqno,
            const void* cookie,
            PermittedVBStates permittedVBStates,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno = GenerateBySeqno::Yes,
            GenerateCas genCas = GenerateCas::No,
            ExtendedMetaData* emd = NULL) override;

    ENGINE_ERROR_CODE prepare(Item& item, const void* cookie);

    GetValue getAndUpdateTtl(const DocKey& key,
                             Vbid vbucket,
                             const void* cookie,
                             time_t exptime) override;

    ENGINE_ERROR_CODE deleteItem(
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const void* cookie,
            boost::optional<cb::durability::Requirements> durability,
            ItemMetaData* itemMeta,
            mutation_descr_t& mutInfo) override;

    ENGINE_ERROR_CODE deleteWithMeta(const DocKey& key,
                                     uint64_t& cas,
                                     uint64_t* seqno,
                                     Vbid vbucket,
                                     const void* cookie,
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

    void snapshotStats() override;

    void getAggregatedVBucketStats(const void* cookie,
                                   const AddStatFn& add_stat) override;

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

    size_t getMemFootPrint(void) {
        size_t mem = 0;
        for (size_t i = 0; i < vbMap.shards.size(); i++) {
            KVShard* shard = vbMap.shards[i].get();
            mem += shard->getRWUnderlying()->getMemFootPrint();
            mem += shard->getROUnderlying()->getMemFootPrint();
        }
        return mem;
    }

    std::pair<uint64_t, bool> getLastPersistedCheckpointId(Vbid vb) override {
        // No persistence at the KVBucket class level.
        return {0, false};
    }

    uint64_t getLastPersistedSeqno(Vbid vb) override {
        auto vbucket = vbMap.getBucket(vb);
        if (vbucket) {
            return vbucket->getPersistenceSeqno();
        } else {
            return 0;
        }
    }

    /**
     * Release all cookies blocked for sync write
     *
     * This method is called during bucket shutdown to make sure that
     * all of the cookies waiting for a durable write is released so that
     * we can continue bucket deletion. As part of bucket deletion one of
     * the first things we do is to tear down the DCP streams so that
     * the durable writes will never be notified and would be stuck
     * waiting for a timeout if we don't explicitly release them.
     */
    void releaseRegisteredSyncWrites();

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
    ENGINE_ERROR_CODE setVBucketState(Vbid vbid,
                                      vbucket_state_t state,
                                      const nlohmann::json& meta = {},
                                      TransferVB transfer = TransferVB::No,
                                      const void* cookie = nullptr);

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
     * @param vbset LockHolder acquiring the 'vbsetMutex' lock in the
     *              EventuallyPersistentStore class
     * @param vbStateLock WriterLockHolder of 'stateLock' in the vbucket
     *                    class.
     */
    void setVBucketState_UNLOCKED(VBucketPtr& vb,
                                  vbucket_state_t to,
                                  const nlohmann::json& meta,
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
     * @param vbset LockHolder acquiring the 'vbsetMutex' lock in the
     *              EventuallyPersistentStore class
     *
     * @return status of the operation
     */
    ENGINE_ERROR_CODE createVBucket_UNLOCKED(
            Vbid vbid,
            vbucket_state_t state,
            const nlohmann::json& meta,
            std::unique_lock<std::mutex>& vbset);
    /**
     * Returns the 'vbsetMutex'
     */
    std::mutex& getVbSetMutexLock() {
        return vbsetMutex;
    }

    ENGINE_ERROR_CODE deleteVBucket(Vbid vbid, const void* c = NULL) override;

    ENGINE_ERROR_CODE checkForDBExistence(Vbid db_file_id) override;

    Vbid getDBFileId(const cb::mcbp::Request& req) override;

    bool resetVBucket(Vbid vbid) override;

    void visit(VBucketVisitor &visitor) override;

    size_t visitAsync(std::unique_ptr<PausableVBucketVisitor> visitor,
                      const char* lbl,
                      TaskId id,
                      std::chrono::microseconds maxExpectedDuration) override;

    Position pauseResumeVisit(PauseResumeVBVisitor& visitor,
                              Position& start_pos) override;

    Position startPosition() const override;

    Position endPosition() const override;

    const Flusher* getFlusher(uint16_t shardId) override;

    Warmup* getWarmup() const override;

    ENGINE_ERROR_CODE getKeyStats(const DocKey& key,
                                  Vbid vbucket,
                                  const void* cookie,
                                  key_stats& kstats,
                                  WantsDeleted wantsDeleted) override;

    std::string validateKey(const DocKey& key, Vbid vbucket, Item& diskItem) override;

    GetValue getLocked(const DocKey& key,
                       Vbid vbucket,
                       rel_time_t currentTime,
                       uint32_t lockTimeout,
                       const void* cookie) override ;

    ENGINE_ERROR_CODE unlockKey(const DocKey& key,
                                Vbid vbucket,
                                uint64_t cas,
                                rel_time_t currentTime,
                                const void* cookie) override;

    KVStore* getRWUnderlying(Vbid vbId) override {
        return vbMap.getShardByVbId(vbId)->getRWUnderlying();
    }

    KVStore* getRWUnderlyingByShard(size_t shardId) override {
        return vbMap.shards[shardId]->getRWUnderlying();
    }

    KVStore* getROUnderlyingByShard(size_t shardId) override {
        return vbMap.shards[shardId]->getROUnderlying();
    }

    KVStore* getROUnderlying(Vbid vbId) override {
        return vbMap.getShardByVbId(vbId)->getROUnderlying();
    }

    KVStoreRWRO takeRWRO(size_t shardId) override;

    void setRWRO(size_t shardId,
                 std::unique_ptr<KVStore> rw,
                 std::unique_ptr<KVStore> ro) override;

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

    void deleteExpiredItem(Item& it, time_t startTime, ExpireBy source) override;
    void deleteExpiredItems(std::list<Item>&, ExpireBy) override;

    /**
     * Get the value for the Item
     * If the value is already deleted no update occurs
     * If a value can be retrieved then it is updated via setValue
     * @param it reference to an Item which maybe updated
     */
    void getValue(Item& it);

    const StorageProperties getStorageProperties() const override {
        KVStore* store  = vbMap.shards[0]->getROUnderlying();
        return store->getStorageProperties();
    }

    void scheduleVBStatePersist() override;

    void scheduleVBStatePersist(Vbid vbid) override;

    const VBucketMap &getVBuckets() override {
        return vbMap;
    }

    EventuallyPersistentEngine& getEPEngine() override {
        return engine;
    }

    size_t getExpiryPagerSleeptime() override {
        LockHolder lh(expiryPager.mutex);
        return expiryPager.sleeptime;
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

    void logQTime(TaskId taskType,
                  const std::chrono::steady_clock::duration enqTime) override;

    void logRunTime(TaskId taskType,
                    const std::chrono::steady_clock::duration runTime) override;

    void updateCachedResidentRatio(size_t activePerc, size_t replicaPerc) override {
        cachedResidentRatio.activeRatio.store(activePerc);
        cachedResidentRatio.replicaRatio.store(replicaPerc);
    }

    bool isMemoryUsageTooHigh() override;

    /**
     * Check the status of memory used and maybe begin to free memory if
     * required.
     *
     * This checks if the bucket's mem_used has exceeded the high water mark.
     */
    void checkAndMaybeFreeMemory();

    void addKVStoreStats(const AddStatFn& add_stat,
                         const void* cookie,
                         const std::string& args) override;

    void addKVStoreTimingStats(const AddStatFn& add_stat,
                               const void* cookie) override;

    bool getKVStoreStat(const char* name, size_t& value,
                        KVSOption option) override;

    void resetUnderlyingStats() override;
    KVStore *getOneROUnderlying() override;
    KVStore *getOneRWUnderlying() override;

    EvictionPolicy getItemEvictionPolicy() const override {
        return eviction_policy;
    }

    TaskStatus rollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void attemptToFreeMemory() override;

    void wakeUpCheckpointRemover() override;

    void runDefragmenterTask() override;

    void runItemFreqDecayerTask() override;

    bool runAccessScannerTask() override;

    void runVbStatePersistTask(Vbid vbid) override;

    void setCompactionWriteQueueCap(size_t to) override {
        compactionWriteQueueCap = to;
    }

    void setCompactionExpMemThreshold(size_t to) override {
        compactionExpMemThreshold = static_cast<double>(to) / 100.0;
    }

    bool compactionCanExpireItems() override;

    void setCursorDroppingLowerUpperThresholds(size_t maxSize) override;

    bool isAccessScannerEnabled() override {
        LockHolder lh(accessScanner.mutex);
        return accessScanner.enabled;
    }

    bool isExpPagerEnabled() override {
        LockHolder lh(expiryPager.mutex);
        return expiryPager.enabled;
    }

    bool isWarmingUp() override;

    bool isWarmupOOMFailure() override;

    /**
     * This method store the given cookie for later notification iff Warmup has
     * yet to reach and complete the PopulateVBucketMap phase.
     *
     * @param cookie the callers cookie which might be stored for later
     *        notification (see return value)
     * @return true if the cookie was stored for later notification, false if
     *         not.
     */
    bool maybeWaitForVBucketWarmup(const void* cookie) override;

    size_t getActiveResidentRatio() const override;

    size_t getReplicaResidentRatio() const override;

    ENGINE_ERROR_CODE forceMaxCas(Vbid vbucket, uint64_t cas) override;

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
                           const nlohmann::json& replicationTopology = {},
                           uint64_t maxVisibleSeqno = 0) override = 0;

    /**
     * Method to handle set_collections commands
     * @param json a buffer containing a JSON manifest to apply to the bucket
     */
    cb::engine_error setCollections(cb::const_char_buffer json);

    /**
     * Method to handle get_collections commands
     * @return a pair with the status and JSON as a std::string
     */
    std::pair<cb::mcbp::Status, std::string> getCollections() const;

    /**
     * Method to handle get_collection_id command
     * @param path A path for scope.collection
     * @return pair with error status and result if success
     */
    cb::EngineErrorGetCollectionIDResult getCollectionID(
            cb::const_char_buffer path) const;

    /**
     * Method to handle get_scope_id command
     * @param path A path for scope
     * @return pair with error status and result if success
     */
    cb::EngineErrorGetScopeIDResult getScopeID(
            cb::const_char_buffer path) const;

    const Collections::Manager& getCollectionsManager() const;

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

protected:
    GetValue getInternal(const DocKey& key,
                         Vbid vbucket,
                         const void* cookie,
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
    virtual void appendAggregatedVBucketStats(VBucketCountVisitor& active,
                                              VBucketCountVisitor& replica,
                                              VBucketCountVisitor& pending,
                                              VBucketCountVisitor& dead,
                                              const void* cookie,
                                              const AddStatFn& add_stat);

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

    friend class Warmup;
    friend class PersistenceCallback;

    EventuallyPersistentEngine     &engine;
    EPStats                        &stats;
    VBucketMap                      vbMap;
    ExTask itemPagerTask;
    ExTask                          chkTask;
    float                           bfilterResidencyThreshold;
    ExTask                          defragmenterTask;
    ExTask itemCompressorTask;
    // The itemFreqDecayerTask is used to decay the frequency count of items
    // stored in the hash table.  This is required to ensure that all the
    // frequency counts do not become saturated.
    ExTask itemFreqDecayerTask;
    size_t                          compactionWriteQueueCap;
    float                           compactionExpMemThreshold;

    // Responsible for enforcing the Durability Timeout for the SyncWrites
    // tracked in this KVBucket.
    ExTask durabilityTimeoutTask;

    /// Responsible for completing (commiting or aborting SyncWrites which have
    /// completed in this KVBucket.
    std::shared_ptr<DurabilityCompletionTask> durabilityCompletionTask;

    /* Vector of mutexes for each vbucket
     * Used by flush operations: flushVB, deleteVB, compactVB, snapshotVB */
    std::vector<std::mutex>       vb_mutexes;
    std::deque<MutationLog>       accessLog;

    std::mutex vbsetMutex;
    double backfillMemoryThreshold;
    struct ExpiryPagerDelta {
        ExpiryPagerDelta() : sleeptime(0), task(0), enabled(true) {}
        std::mutex mutex;
        size_t sleeptime;
        size_t task;
        bool enabled;
    } expiryPager;
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
    size_t statsSnapshotTaskId;
    std::atomic<size_t> lastTransTimePerItem;
    EvictionPolicy eviction_policy;

    std::mutex compactionLock;
    std::list<CompTaskEntry> compactionTasks;

    std::unique_ptr<Collections::Manager> collectionsManager;

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
    std::function<long()> getRandom = std::rand;

    friend class KVBucketTest;

    DISALLOW_COPY_AND_ASSIGN(KVBucket);
};

/**
 * Callback for notifying clients of the Bucket (KVBucket) about a new item in
 * the partition (Vbucket).
 */
class NotifyNewSeqnoCB : public Callback<const Vbid, const VBNotifyCtx&> {
public:
    NotifyNewSeqnoCB(KVBucket& kvb) : kvBucket(kvb) {
    }

    void callback(const Vbid& vbid, const VBNotifyCtx& notifyCtx) {
        kvBucket.notifyNewSeqno(vbid, notifyCtx);
    }

private:
    KVBucket& kvBucket;
};
