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

#include "config.h"

#include "ep_types.h"
#include "executorpool.h"
#include "item_freq_decayer.h"
#include "kv_bucket_iface.h"
#include "mutation_log.h"
#include "stored-value.h"
#include "storeddockey.h"
#include "task_type.h"
#include "utility.h"
#include "vbucket.h"
#include "vbucketmap.h"

#include <deque>

class ReplicationThrottle;
class VBucketCountVisitor;
namespace Collections {
class Manager;
}

/**
 * VBucket visitor callback adaptor.
 */
class VBCBAdaptor : public GlobalTask {
public:
    VBCBAdaptor(KVBucket* s,
                TaskId id,
                std::unique_ptr<VBucketVisitor> v,
                const char* l,
                double sleep,
                bool shutdown);

    std::string getDescription();

    /// Set the maximum expected duration for this task.
    void setMaxExpectedDuration(std::chrono::microseconds duration) {
        maxDuration = duration;
    }

    std::chrono::microseconds maxExpectedDuration() {
        return maxDuration;
    }

    bool run(void);

private:
    std::queue<Vbid> vbList;
    KVBucket* store;
    std::unique_ptr<VBucketVisitor> visitor;
    const char                 *label;
    double                      sleepTime;
    std::chrono::microseconds maxDuration;
    std::atomic<Vbid> currentvb;

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

    /**
     * Start necessary tasks.
     * Client calling initialize must also call deinitialize before deleting
     * the EPBucket instance
     */
    bool initialize();

    /**
     * Creates a warmup task if the engine configuration has "warmup=true"
     */
    void initializeWarmupTask();

    /**
     * Starts the warmup task if one is present
     */
    void startWarmupTask();

    /**
     * Stop tasks started in initialize()
     */
    void deinitialize();

    /**
     * Set an item in the store.
     * @param item the item to set. On success, this will have its seqno and
     *             CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @param predicate an optional function to call which if returns true,
     *        the replace will succeed. The function is called against any
     *        existing item.
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE set(Item& item,
                          const void* cookie,
                          cb::StoreIfPredicate predicate = {});

    /**
     * Add an item in the store.
     * @param item the item to add. On success, this will have its seqno and
     *             CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE add(Item &item, const void *cookie);

    /**
     * Replace an item in the store.
     * @param item the item to replace. On success, this will have its seqno
     *             and CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @param predicate an optional function to call which if returns true,
     *        the replace will succeed. The func§tion is called against any
     *        existing item.
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE replace(Item& item,
                              const void* cookie,
                              cb::StoreIfPredicate predicate = {});

    /**
     * Add a DCP backfill item into its corresponding vbucket
     * @param item the item to be added
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addBackfillItem(Item& item,
                                      ExtendedMetaData* emd = NULL);

    /**
     * Retrieve a value.
     *
     * @param key     the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie  the connection cookie
     * @param options options specified for retrieval
     *
     * @return a GetValue representing the result of the request
     */
    GetValue get(const DocKey& key,
                 Vbid vbucket,
                 const void* cookie,
                 get_options_t options) {
        return getInternal(key, vbucket, cookie, vbucket_state_active,
                           options);
    }

    /**
     * Retrieve a value randomly from the store.
     *
     * @return a GetValue representing the value retrieved
     */
    GetValue getRandomKey(void);

    /**
     * Retrieve a value from a vbucket in replica state.
     *
     * @param key     the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie  the connection cookie
     * @param options options specified for retrieval
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getReplica(const DocKey& key,
                        Vbid vbucket,
                        const void* cookie,
                        get_options_t options) {
        return getInternal(key, vbucket, cookie, vbucket_state_replica,
                           options);
    }

    /**
     * Retrieve the meta data for an item
     *
     * @parapm key the key to get the meta data for
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param[out] metadata where to store the meta informaion
     * @param[out] deleted specifies whether or not the key is deleted
     * @param[out] datatype specifies the datatype for the given item
     */
    ENGINE_ERROR_CODE getMetaData(const DocKey& key,
                                  Vbid vbucket,
                                  const void* cookie,
                                  ItemMetaData& metadata,
                                  uint32_t& deleted,
                                  uint8_t& datatype);

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
            ExtendedMetaData* emd = NULL,
            bool isReplication = false);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param exptime the new expiry time for the object
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(const DocKey& key,
                             Vbid vbucket,
                             const void* cookie,
                             time_t exptime);

    ENGINE_ERROR_CODE deleteItem(const DocKey& key,
                                 uint64_t& cas,
                                 Vbid vbucket,
                                 const void* cookie,
                                 ItemMetaData* itemMeta,
                                 mutation_descr_t& mutInfo);

    ENGINE_ERROR_CODE deleteWithMeta(const DocKey& key,
                                     uint64_t& cas,
                                     uint64_t* seqno,
                                     Vbid vbucket,
                                     const void* cookie,
                                     PermittedVBStates permittedVBStates,
                                     CheckConflicts checkConflicts,
                                     const ItemMetaData& itemMeta,
                                     bool backfill,
                                     GenerateBySeqno genBySeqno,
                                     GenerateCas generateCas,
                                     uint64_t bySeqno,
                                     ExtendedMetaData* emd,
                                     bool isReplication);

    virtual void reset();

    /**
     * Set the background fetch delay.
     *
     * This exists for debugging and testing purposes.  It
     * artificially injects delays into background fetches that are
     * performed when the user requests an item whose value is not
     * currently resident.
     *
     * @param to how long to delay before performing a bg fetch
     */
    void setBGFetchDelay(uint32_t to) {
        bgFetchDelay = to;
    }

    double getBGFetchDelay(void) { return (double)bgFetchDelay; }

    virtual bool pauseFlusher();
    virtual bool resumeFlusher();
    virtual void wakeUpFlusher();

    /**
     * Takes a snapshot of the current stats and persists them to disk.
     */
    void snapshotStats(void);

    virtual void getAggregatedVBucketStats(const void* cookie,
                                           ADD_STAT add_stat);

   /**
     * Complete a background fetch of a non resident value or metadata.
     *
     * @param key the key that was fetched
     * @param vbucket the vbucket in which the key lived
     * @param cookie the cookie of the requestor
     * @param init the timestamp of when the request came in
     * @param isMeta whether the fetch is for a non-resident value or metadata of
     *               a (possibly) deleted item
     */
    void completeBGFetch(const DocKey& key,
                         Vbid vbucket,
                         const void* cookie,
                         std::chrono::steady_clock::time_point init,
                         bool isMeta);
    /**
     * Complete a batch of background fetch of a non resident value or metadata.
     *
     * @param vbId the vbucket in which the requested key lived
     * @param fetchedItems vector of completed background feches containing key,
     *                     value, client cookies
     * @param start the time when the background fetch was started
     *
     */
    void completeBGFetchMulti(Vbid vbId,
                              std::vector<bgfetched_item_t>& fetchedItems,
                              std::chrono::steady_clock::time_point start);

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

    VBucketPtr getVBucket(Vbid vbid) {
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

    std::pair<uint64_t, bool> getLastPersistedCheckpointId(Vbid vb) {
        // No persistence at the KVBucket class level.
        return {0, false};
    }

    uint64_t getLastPersistedSeqno(Vbid vb) {
        auto vbucket = vbMap.getBucket(vb);
        if (vbucket) {
            return vbucket->getPersistenceSeqno();
        } else {
            return 0;
        }
    }

    /**
     * Sets the vbucket or creates a vbucket with the desired state
     *
     * @param vbid vbucket id
     * @param state desired state of the vbucket
     * @param transfer indicates that the vbucket is transferred to the active
     *                 post a failover and/or rebalance
     * @param cookie under certain conditions we may use ewouldblock
     *
     * @return status of the operation
     */
    ENGINE_ERROR_CODE setVBucketState(Vbid vbid,
                                      vbucket_state_t state,
                                      bool transfer,
                                      const void* cookie = nullptr);

    /**
     * Sets the vbucket or creates a vbucket with the desired state
     *
     * @param vbid vbucket id
     * @param state desired state of the vbucket
     * @param transfer indicates that the vbucket is transferred to the active
     *                 post a failover and/or rebalance
     * @param notify_dcp indicates whether we must consider closing DCP streams
     *                    associated with the vbucket
     * @param vbset LockHolder acquiring the 'vbsetMutex' lock in the
     *              EventuallyPersistentStore class
     * @param vbStateLock ptr to WriterLockHolder of 'stateLock' in the vbucket
     *                    class. if passed as null, the function acquires the
     *                    vbucket 'stateLock'
     *
     * return status of the operation
     */
    ENGINE_ERROR_CODE setVBucketState_UNLOCKED(
            Vbid vbid,
            vbucket_state_t state,
            bool transfer,
            bool notify_dcp,
            std::unique_lock<std::mutex>& vbset,
            WriterLockHolder* vbStateLock = nullptr);

    /**
     * Returns the 'vbsetMutex'
     */
    std::mutex& getVbSetMutexLock() {
        return vbsetMutex;
    }

    /**
     * Deletes a vbucket
     *
     * @param vbid The vbucket to delete.
     * @param c The cookie for this connection.
     *          Used in synchronous bucket deletes
     *          to notify the connection of operation completion.
     */
    ENGINE_ERROR_CODE deleteVBucket(Vbid vbid, const void* c = NULL);

    /**
     * Check for the existence of a vbucket in the case of couchstore.
     *
     * @param db_file_id vbucketid for couchstore.
     */
    ENGINE_ERROR_CODE checkForDBExistence(Vbid db_file_id);

    /**
     * Get the database file id for the compaction request
     *
     * @param req compaction request structure
     *
     * returns the database file id from the underlying KV store
     */
    Vbid getDBFileId(const protocol_binary_request_compact_db& req);

    /**
     * Remove completed compaction tasks or wake snoozed tasks
     *
     * @param db_file_id vbucket id for couchstore.
     */
    void updateCompactionTasks(Vbid db_file_id);

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket deletion in that
     * it does not delete the vbucket instance from memory hash table.
     */
    bool resetVBucket(Vbid vbid);

    /**
     * Run a vBucket visitor, visiting all items. Synchronous.
     */
    void visit(VBucketVisitor &visitor);

    /**
     * Run a vbucket visitor with separate jobs per vbucket.
     *
     * Note that this is asynchronous.
     */
    size_t visit(std::unique_ptr<VBucketVisitor> visitor,
                 const char* lbl,
                 TaskId id,
                 double sleepTime,
                 std::chrono::microseconds maxExpectedDuration);

    Position pauseResumeVisit(PauseResumeVBVisitor& visitor,
                              Position& start_pos);

    /**
     * Return a position at the start of the epStore.
     */
    Position startPosition() const;

    /**
     * Return a position at the end of the epStore. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    Position endPosition() const;

    const Flusher* getFlusher(uint16_t shardId);

    Warmup* getWarmup(void) const;

    /**
     * Looks up the key stats for the given {vbucket, key}.
     * @param key The key to lookup
     * @param vbucket The vbucket the key belongs to.
     * @param cookie The client's cookie
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If yes then return keystats even if the item is
     *                     marked as deleted. If no then will return
     *                     ENGINE_KEY_ENOENT for deleted items.
     */
    ENGINE_ERROR_CODE getKeyStats(const DocKey& key,
                                  Vbid vbucket,
                                  const void* cookie,
                                  key_stats& kstats,
                                  WantsDeleted wantsDeleted);

    std::string validateKey(const DocKey& key, Vbid vbucket, Item& diskItem);

    GetValue getLocked(const DocKey& key,
                       Vbid vbucket,
                       rel_time_t currentTime,
                       uint32_t lockTimeout,
                       const void* cookie);

    ENGINE_ERROR_CODE unlockKey(const DocKey& key,
                                Vbid vbucket,
                                uint64_t cas,
                                rel_time_t currentTime);

    KVStore* getRWUnderlying(Vbid vbId) {
        return vbMap.getShardByVbId(vbId)->getRWUnderlying();
    }

    KVStore* getRWUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getRWUnderlying();
    }

    KVStore* getROUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getROUnderlying();
    }

    KVStore* getROUnderlying(Vbid vbId) {
        return vbMap.getShardByVbId(vbId)->getROUnderlying();
    }

    cb::mcbp::Status evictKey(const DocKey& key,
                              Vbid vbucket,
                              const char** msg);

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
    void deleteExpiredItem(Item& it, time_t startTime, ExpireBy source);
    void deleteExpiredItems(std::list<Item>&, ExpireBy);

    /**
     * Get the value for the Item
     * If the value is already deleted no update occurs
     * If a value can be retrieved then it is updated via setValue
     * @param it reference to an Item which maybe updated
     */
    void getValue(Item& it);

    /**
     * Get the memoized storage properties from the DB.kv
     */
    const StorageProperties getStorageProperties() const {
        KVStore* store  = vbMap.shards[0]->getROUnderlying();
        return store->getStorageProperties();
    }

    virtual void scheduleVBStatePersist();

    virtual void scheduleVBStatePersist(Vbid vbid);

    const VBucketMap &getVBuckets() {
        return vbMap;
    }

    EventuallyPersistentEngine& getEPEngine() {
        return engine;
    }

    size_t getExpiryPagerSleeptime(void) {
        LockHolder lh(expiryPager.mutex);
        return expiryPager.sleeptime;
    }

    size_t getTransactionTimePerItem() {
        return lastTransTimePerItem.load();
    }

    bool isDeleteAllScheduled() {
        return diskDeleteAll.load();
    }

    void setDeleteAllComplete();

    void setBackfillMemoryThreshold(double threshold);

    void setExpiryPagerSleeptime(size_t val);
    void setExpiryPagerTasktime(ssize_t val);
    void enableExpiryPager();
    void disableExpiryPager();

    /// Wake up the expiry pager (if enabled), scheduling it for immediate run.
    void wakeUpExpiryPager();

    /// Wake up the item pager (if enabled), scheduling it for immediate run.
    /// Currently this is used only during testing.
    void wakeItemPager();
    void enableItemPager();
    void disableItemPager();

    /// Wake up the ItemFreqDecayer Task, scheduling it for immediate run.
    void wakeItemFreqDecayerTask();

    void enableAccessScannerTask();
    void disableAccessScannerTask();
    void setAccessScannerSleeptime(size_t val, bool useStartTime);
    void resetAccessScannerStartTime();

    void resetAccessScannerTasktime() {
        accessScanner.lastTaskRuntime = std::chrono::steady_clock::now();
    }

    void setAllBloomFilters(bool to);

    float getBfiltersResidencyThreshold() {
        return bfilterResidencyThreshold;
    }

    void setBfiltersResidencyThreshold(float to) {
        bfilterResidencyThreshold = to;
    }

    bool isMetaDataResident(VBucketPtr &vb, const DocKey& key);

    void logQTime(TaskId taskType,
                  const std::chrono::steady_clock::duration enqTime);

    void logRunTime(TaskId taskType,
                    const std::chrono::steady_clock::duration runTime);

    bool multiBGFetchEnabled() {
        StorageProperties storeProp = getStorageProperties();
        return storeProp.hasEfficientGet();
    }

    void updateCachedResidentRatio(size_t activePerc, size_t replicaPerc) {
        cachedResidentRatio.activeRatio.store(activePerc);
        cachedResidentRatio.replicaRatio.store(replicaPerc);
    }

    bool isWarmingUp();

    /**
     * Method checks with Warmup if a setVBState should block.
     * On returning true, Warmup will have saved the cookie ready for
     * IO notify complete.
     * If there's no Warmup returns false
     * @param cookie the callers cookie for later notification.
     * @return true if setVBState should return EWOULDBLOCK
     */
    bool shouldSetVBStateBlock(const void* cookie);

    bool maybeEnableTraffic(void);

    /**
     * Checks the memory consumption.
     * To be used by backfill tasks (DCP).
     */
    bool isMemoryUsageTooHigh();

    /**
     * Check the status of memory used and maybe begin to free memory if
     * required.
     *
     * This checks if the bucket's mem_used has exceeded the high water mark.
     */
    void checkAndMaybeFreeMemory();

    void addKVStoreStats(ADD_STAT add_stat, const void* cookie);

    void addKVStoreTimingStats(ADD_STAT add_stat, const void* cookie);

    /* Given a named KVStore statistic, return the value of that statistic,
     * accumulated across any shards.
     *
     * @param name The name of the statistic
     * @param[out] value The value of the statistic.
     * @param option the KVStore to read stats from.
     * @return True if the statistic was successfully returned via {value},
     *              else false.
     */
    bool getKVStoreStat(const char* name, size_t& value,
                        KVSOption option);

    void resetUnderlyingStats(void);
    KVStore *getOneROUnderlying(void);
    KVStore *getOneRWUnderlying(void);

    item_eviction_policy_t getItemEvictionPolicy(void) const {
        return eviction_policy;
    }

    /*
     * Request a rollback of the vbucket to the specified seqno.
     * If the rollbackSeqno is not a checkpoint boundary, then the rollback
     * will be to the nearest checkpoint.
     * There are also cases where the rollback will be forced to 0.
     * various failures or if the rollback is > 50% of the data.
     *
     * A check of the vbucket's high-seqno indicates if a rollback request
     * was not honoured exactly.
     *
     * @param vbid The vbucket to rollback
     * @rollbackSeqno The seqno to rollback to.
     *
     * @return TaskStatus::Complete upon successful rollback
     *         TaskStatus::Abort if vbucket is not replica or
     *                           if vbucket is not valid
     *                           if vbucket reset and rollback fails
     *         TaskStatus::Reschedule if you cannot get a lock on the vbucket
     */
    TaskStatus rollback(Vbid vbid, uint64_t rollbackSeqno);

    virtual void attemptToFreeMemory();

    void wakeUpCheckpointRemover() {
        if (chkTask && chkTask->getState() == TASK_SNOOZED) {
            ExecutorPool::get()->wake(chkTask->getId());
        }
    }

    void runDefragmenterTask();

    /**
     * Invoke the run method of the ItemFreqDecayerTask.  Currently only used
     * for testing purposes.
     */
    void runItemFreqDecayerTask();

    bool runAccessScannerTask();

    void runVbStatePersistTask(Vbid vbid);

    void setCompactionWriteQueueCap(size_t to) {
        compactionWriteQueueCap = to;
    }

    void setCompactionExpMemThreshold(size_t to) {
        compactionExpMemThreshold = static_cast<double>(to) / 100.0;
    }

    bool compactionCanExpireItems();

    void setCursorDroppingLowerUpperThresholds(size_t maxSize);

    bool isAccessScannerEnabled() {
        LockHolder lh(accessScanner.mutex);
        return accessScanner.enabled;
    }

    bool isExpPagerEnabled() {
        LockHolder lh(expiryPager.mutex);
        return expiryPager.enabled;
    }

    //Check if there were any out-of-memory errors during warmup
    bool isWarmupOOMFailure(void);

    size_t getActiveResidentRatio() const;

    size_t getReplicaResidentRatio() const;
    /*
     * Change the max_cas of the specified vbucket to cas without any
     * care for the data or ongoing operations...
     */
    ENGINE_ERROR_CODE forceMaxCas(Vbid vbucket, uint64_t cas);

    /**
     * Create a VBucket object appropriate for this Bucket class.
     */
    virtual VBucketPtr makeVBucket(
            Vbid id,
            vbucket_state_t state,
            KVShard* shard,
            std::unique_ptr<FailoverTable> table,
            NewSeqnoCallback newSeqnoCb,
            vbucket_state_t initState = vbucket_state_dead,
            int64_t lastSeqno = 0,
            uint64_t lastSnapStart = 0,
            uint64_t lastSnapEnd = 0,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
            bool mightContainXattrs = false,
            const Collections::VB::PersistedManifest& collectionsManifest =
                    {}) = 0;

    /**
     * Method to handle set_collections commands
     * @param json a buffer containing a JSON manifest to apply to the bucket
     */
    cb::engine_error setCollections(cb::const_char_buffer json);

    /**
     * Method to handle get_collections commands
     * @return a pair of status and JSON as a std::string
     */
    cb::EngineErrorStringPair getCollections() const;

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

    /**
     * Perform actions for erasing keys based on a vbucket's collection's
     * manifest. This method examines key@bySeqno against the vbucket's (vbid)
     * collections manifest to determine if the key should be erased. Once the
     * code has processed all keys of the collection, the manifest is updated
     * to finalise the deleted collection.
     *
     * @param vbid the vbucket the key belongs too.
     * @param key the key to examine.
     * @param bySeqno the seqno for the key.
     * @param deleted is the key marked as deleted.
     * @param eraserContext context for processing keys against
     * @return true if the collection manifest for the vbucket determines the
     *         key at bySeqno is part of a deleted collection.
     */
    bool collectionsEraseKey(Vbid vbid,
                             const DocKey key,
                             int64_t bySeqno,
                             bool deleted,
                             Collections::VB::EraserContext& eraserContext);

    /// return the buckets maxTtl value
    std::chrono::seconds getMaxTtl() const;

    /// set the buckets maxTtl
    void setMaxTtl(size_t max);

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    // Methods called during warmup
    std::vector<vbucket_state *> loadVBucketState();

    void warmupCompleted();
    void stopWarmup(void);

    /**
     * Get metadata and value for a given key
     *
     * @param key Key for which metadata and value should be retrieved
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie The connection cookie
     * @param allowedState Whether getting for active or replica vbucket
     * @param options Flags indicating some retrieval related info
     *
     * @return the result of the operation
     */
    GetValue getInternal(const DocKey& key,
                         Vbid vbucket,
                         const void* cookie,
                         vbucket_state_t allowedState,
                         get_options_t options);

    bool resetVBucket_UNLOCKED(LockedVBucketPtr& vb,
                               std::unique_lock<std::mutex>& vbset);

    /* Notify flusher of a new seqno being added in the vbucket */
    virtual void notifyFlusher(const Vbid vbid);

    /* Notify replication of a new seqno being added in the vbucket */
    void notifyReplication(const Vbid vbid, const int64_t bySeqno);

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
                                              ADD_STAT add_stat);

    friend class Warmup;
    friend class PersistenceCallback;

    EventuallyPersistentEngine     &engine;
    EPStats                        &stats;
    std::unique_ptr<Warmup> warmupTask;
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

    /* Vector of mutexes for each vbucket
     * Used by flush operations: flushVB, deleteVB, compactVB, snapshotVB */
    std::vector<std::mutex>       vb_mutexes;
    std::deque<MutationLog>       accessLog;

    std::atomic<bool> diskDeleteAll;
    struct DeleteAllTaskCtx {
        DeleteAllTaskCtx() : delay(true), cookie(NULL) {
        }
        std::atomic<bool> delay;
        const void* cookie;
    } deleteAllTaskCtx;

    std::mutex vbsetMutex;
    uint32_t bgFetchDelay;
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
    item_eviction_policy_t eviction_policy;

    std::mutex compactionLock;
    std::list<CompTaskEntry> compactionTasks;

    std::unique_ptr<Collections::Manager> collectionsManager;

    /**
     * Status of XATTR support for this bucket - this is set from the
     * bucket config and also via set_flush_param. Written/read by differing
     * threads.
     */
    Couchbase::RelaxedAtomic<bool> xattrEnabled;

    /* Contains info about throttling the replication */
    std::unique_ptr<ReplicationThrottle> replicationThrottle;

    std::atomic<size_t> maxTtl;

    /**
     * Allows us to override the random function.  This is used for testing
     * purposes where we want a constant number as opposed to a random one.
     */
    std::function<long()> getRandom = random;

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
