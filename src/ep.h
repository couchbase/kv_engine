/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_EP_H_
#define SRC_EP_H_ 1

#include "config.h"

#include "executorpool.h"
#include "stored-value.h"
#include "task_type.h"
#include "vbucket.h"
#include "vbucketmap.h"
#include "utility.h"

class ExtendedMetaData;

/**
 * vbucket-aware hashtable visitor.
 */
class VBucketVisitor : public HashTableVisitor {
public:

    VBucketVisitor() : HashTableVisitor() { }

    VBucketVisitor(const VBucketFilter &filter) :
        HashTableVisitor(), vBucketFilter(filter) { }

    /**
     * Begin visiting a bucket.
     *
     * @param vb the vbucket we are beginning to visit
     *
     * @return true iff we want to walk the hashtable in this vbucket
     */
    virtual bool visitBucket(RCPtr<VBucket> &vb) {
        if (vBucketFilter(vb->getId())) {
            currentBucket = vb;
            return true;
        }
        return false;
    }

    // This is unused in all implementations so far.
    void visit(StoredValue* v) {
        (void)v;
        abort();
    }

    const VBucketFilter &getVBucketFilter() {
        return vBucketFilter;
    }

    /**
     * Called after all vbuckets have been visited.
     */
    virtual void complete() { }

    /**
     * Return true if visiting vbuckets should be paused temporarily.
     */
    virtual bool pauseVisitor() {
        return false;
    }

protected:
    VBucketFilter vBucketFilter;
    RCPtr<VBucket> currentBucket;
};

// Forward declaration
class BGFetchCallback;
class ConflictResolution;
class DefragmenterTask;
class EventuallyPersistentStore;
class Flusher;
class MutationLog;
class PauseResumeEPStoreVisitor;
class PersistenceCallback;
class Warmup;

/**
 * VBucket visitor callback adaptor.
 */
class VBCBAdaptor : public GlobalTask {
public:

    VBCBAdaptor(EventuallyPersistentStore *s,
                std::shared_ptr<VBucketVisitor> v, const char *l, const Priority &p,
                double sleep=0);

    std::string getDescription() {
        std::stringstream rv;
        rv << label << " on vb " << currentvb.load();
        return rv.str();
    }

    bool run(void);

private:
    std::queue<uint16_t>        vbList;
    EventuallyPersistentStore  *store;
    std::shared_ptr<VBucketVisitor>  visitor;
    const char                 *label;
    double                      sleepTime;
    AtomicValue<uint16_t>       currentvb;

    DISALLOW_COPY_AND_ASSIGN(VBCBAdaptor);
};


/**
 * Vbucket visitor task for a generic scheduler.
 */
class VBucketVisitorTask : public GlobalTask {
public:

    VBucketVisitorTask(EventuallyPersistentStore *s,
                       std::shared_ptr<VBucketVisitor> v, uint16_t sh,
                       const char *l, double sleep=0, bool shutdown=true);

    std::string getDescription() {
        std::stringstream rv;
        rv << label << " on vb " << currentvb;
        return rv.str();
    }

    bool run();

private:
    std::queue<uint16_t>         vbList;
    EventuallyPersistentStore   *store;
    std::shared_ptr<VBucketVisitor>   visitor;
    const char                  *label;
    double                       sleepTime;
    uint16_t                     currentvb;
    uint16_t                     shardID;
};

const uint16_t EP_PRIMARY_SHARD = 0;
class KVShard;

typedef std::pair<uint16_t, ExTask> CompTaskEntry;

/**
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum exp_type_t {
    EXP_BY_PAGER,
    EXP_BY_COMPACTOR,
    EXP_BY_ACCESS
};

/**
 * The following options can be specified
 * for retrieving an item for get calls
 */
enum get_options_t {
    NONE             = 0x0000,  //no option
    TRACK_STATISTICS = 0x0001,  //whether statistics need to be tracked or not
    QUEUE_BG_FETCH   = 0x0002,  //whether a background fetch needs to be queued
    HONOR_STATES     = 0x0004,  //whether a retrieval should depend on the state
                                //of the vbucket
    TRACK_REFERENCE  = 0x0008,  //whether NRU bit needs to be set for the item
    DELETE_TEMP      = 0x0010,  //whether temporary items need to be deleted
    HIDE_LOCKED_CAS  = 0x0020   //whether locked items should have their CAS
                                //hidden (return -1).
};

/**
 * Manager of all interaction with the persistence.
 */
class EventuallyPersistentStore {
public:

    /**
     * Represents a position within the epStore, used when visiting items.
     *
     * Currently opaque (and constant), clients can pass them around but
     * cannot reposition the iterator.
     */
    class Position {
    public:
        bool operator==(const Position& other) const {
            return (vbucket_id == other.vbucket_id);
        }

    private:
        Position(uint16_t vbucket_id_) : vbucket_id(vbucket_id_) {}

        uint16_t vbucket_id;

        friend class EventuallyPersistentStore;
        friend std::ostream& operator<<(std::ostream& os, const Position& pos);
    };

    EventuallyPersistentStore(EventuallyPersistentEngine &theEngine);
    ~EventuallyPersistentStore();

    bool initialize();

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cookie the cookie representing the client to store the item
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @param nru the nru bit value for the item
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE set(const Item &item,
                          const void *cookie,
                          bool force = false,
                          uint8_t nru = 0xff);

    /**
     * Add an item in the store.
     * @param item the item to add
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE add(const Item &item, const void *cookie);

    /**
     * Replace an item in the store.
     * @param item the item to replace
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE replace(const Item &item, const void *cookie);

    /**
     * Add an TAP backfill item into its corresponding vbucket
     * @param item the item to be added
     * @param nru the nru bit for the item
     * @param genBySeqno whether or not to generate sequence number
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addTAPBackfillItem(const Item &item, uint8_t nru = 0xff,
                                         bool genBySeqno = true,
                                         ExtendedMetaData *emd = NULL);

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
    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie,
                 get_options_t options = static_cast<get_options_t>(
                                                        QUEUE_BG_FETCH |
                                                        HONOR_STATES |
                                                        TRACK_REFERENCE |
                                                        DELETE_TEMP |
                                                        HIDE_LOCKED_CAS)) {
        return getInternal(key, vbucket, cookie, vbucket_state_active,
                           options);
    }

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
    GetValue getReplica(const std::string &key, uint16_t vbucket,
                        const void *cookie,
                        get_options_t options = static_cast<get_options_t>(
                                                        QUEUE_BG_FETCH |
                                                        HONOR_STATES |
                                                        TRACK_REFERENCE |
                                                        DELETE_TEMP |
                                                        HIDE_LOCKED_CAS)) {
        return getInternal(key, vbucket, cookie, vbucket_state_replica,
                           options);
    }


    /**
     * Retrieve the meta data for an item
     *
     * @parapm key the key to get the meta data for
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param metadata where to store the meta informaion
     * @param deleted specifies whether or not the key is deleted
     * @param confResMode specifies the Conflict Resolution mode for the item
     * @param trackReference true if we want to set the nru bit for the item
     */
    ENGINE_ERROR_CODE getMetaData(const std::string &key,
                                  uint16_t vbucket,
                                  const void *cookie,
                                  ItemMetaData &metadata,
                                  uint32_t &deleted,
                                  uint8_t &confResMode,
                                  bool trackReference = false);

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param force override vbucket states
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param nru the nru bit for the item
     * @param genBySeqno whether or not to generate sequence number
     * @param emd ExtendedMetaData class object that contains any ext meta
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(const Item &item,
                                  uint64_t cas,
                                  uint64_t *seqno,
                                  const void *cookie,
                                  bool force,
                                  bool allowExisting,
                                  uint8_t nru = 0xff,
                                  bool genBySeqno = true,
                                  ExtendedMetaData *emd = NULL,
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
    GetValue getAndUpdateTtl(const std::string &key, uint16_t vbucket,
                             const void *cookie, time_t exptime);

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param cb callback to return an item fetched from the disk
     *
     * @return a status resulting form executing the method
     */
    ENGINE_ERROR_CODE statsVKey(const std::string &key,
                                uint16_t vbucket,
                                const void *cookie);

    void completeStatsVKey(const void* cookie, std::string &key, uint16_t vbid,
                           uint64_t bySeqNum);

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size,
                                             bool force=false);

    /**
     * delete an item in the store.
     * @param key the key of the item
     * @param cas the CAS ID for a CASed delete (0 to override)
     * @param vbucket the vbucket for the key
     * @param cookie the cookie representing the client
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @param itemMeta the pointer to the metadata memory.
     * @param tapBackfill true if an item deletion is from TAP backfill stream
     *
     * (deleteWithMeta)
     * @param genBySeqno whether or not to generate sequence number
     * @param emd ExtendedMetaData class object that contains any ext meta
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the delete operation
     */
    ENGINE_ERROR_CODE deleteItem(const std::string &key,
                                 uint64_t* cas,
                                 uint16_t vbucket,
                                 const void *cookie,
                                 bool force,
                                 ItemMetaData *itemMeta,
                                 mutation_descr_t *mutInfo,
                                 bool tapBackfill=false);

    ENGINE_ERROR_CODE deleteWithMeta(const std::string &key,
                                     uint64_t* cas,
                                     uint64_t* seqno,
                                     uint16_t vbucket,
                                     const void *cookie,
                                     bool force,
                                     ItemMetaData *itemMeta,
                                     bool tapBackfill=false,
                                     bool genBySeqno=true,
                                     uint64_t bySeqno=0,
                                     ExtendedMetaData *emd = NULL,
                                     bool isReplication=false);

    void reset();

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

    void stopFlusher(void);

    bool startFlusher(void);

    bool pauseFlusher(void);
    bool resumeFlusher(void);
    void wakeUpFlusher(void);

    bool startBgFetcher(void);
    void stopBgFetcher(void);

    /**
     * Takes a snapshot of the current stats and persists them to disk.
     */
    void snapshotStats(void);

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param vbucket the vbucket in which the key lives
     * @param cookie the cookie of the requestor
     * @param type whether the fetch is for a non-resident value or metadata of
     *             a (possibly) deleted item
     */
    void bgFetch(const std::string &key,
                 uint16_t vbucket,
                 const void *cookie,
                 bool isMeta = false);

    /**
     * Complete a background fetch of a non resident value or metadata.
     *
     * @param key the key that was fetched
     * @param vbucket the vbucket in which the key lived
     * @param cookie the cookie of the requestor
     * @param init the timestamp of when the request came in
     * @param type whether the fetch is for a non-resident value or metadata of
     *             a (possibly) deleted item
     */
    void completeBGFetch(const std::string &key,
                         uint16_t vbucket,
                         const void *cookie,
                         hrtime_t init,
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
    void completeBGFetchMulti(uint16_t vbId,
                              std::vector<bgfetched_item_t> &fetchedItems,
                              hrtime_t start);

    /**
     * Helper function to update stats after completion of a background fetch
     * for either the value of metadata of a key.
     *
     * @param init the time of epstore's initialization
     * @param start the time when the background fetch was started
     * @param stop the time when the background fetch completed
     */
    void updateBGStats(const hrtime_t init,
                       const hrtime_t start,
                       const hrtime_t stop);

    RCPtr<VBucket> getVBucket(uint16_t vbid) {
        return vbMap.getBucket(vbid);
    }

    uint64_t getLastPersistedCheckpointId(uint16_t vb) {
        return vbMap.getPersistenceCheckpointId(vb);
    }

    uint64_t getLastPersistedSeqno(uint16_t vb) {
        return vbMap.getPersistenceSeqno(vb);
    }

    void snapshotVBuckets(const Priority &priority, uint16_t shardId);

    /* transfer should be set to true *only* if this vbucket is becoming master
     * as the result of the previous master cleanly handing off control.
     * @param force_persist: Should the VBstate be forcefully persisted to disk,
     *                       even if another VBStatePersistTask is already
     *                       scheduled.
     */
    ENGINE_ERROR_CODE setVBucketState(uint16_t vbid, vbucket_state_t state,
                                      bool transfer,
                                      bool notify_dcp = true,
                                      bool force_persist = false);

    /**
     * Physically deletes a VBucket from disk. This function should only
     * be called on a VBucket that has already been logically deleted.
     *
     * @param vbid              vbucket id
     * @param cookie            The connection that requested the deletion
     * @param clearVbCreateFlag clear the vbucket creation flag
     */
    bool completeVBucketDeletion(uint16_t vbid, const void* cookie,
                                 bool clearVbCreateFlag);

    /**
     * Deletes a vbucket
     *
     * @param vbid The vbucket to delete.
     * @param c The cookie for this connection.
     *          Used in synchronous bucket deletes
     *          to notify the connection of operation completion.
     */
    ENGINE_ERROR_CODE deleteVBucket(uint16_t vbid, const void* c = NULL);

    /**
     * Check for the existence of a vbucket in the case of couchstore
     * or shard in the case of forestdb. Note that this function will be
     * deprecated once forestdb is the only backend supported
     *
     * @param db_file_id vbucketid for couchstore or shard id in the
     *                   case of forestdb
     */
    ENGINE_ERROR_CODE checkForDBExistence(uint16_t db_file_id);

    /**
     * Triggers compaction of a vbucket
     *
     * @param c The context for compaction of a DB file
     * @param ck cookie used to notify connection of operation completion
     */
    ENGINE_ERROR_CODE compactDB(compaction_ctx c, const void *ck);

    /**
     * Callback to do the compaction of a vbucket
     *
     * @param ctx Context for couchstore compaction hooks
     * @param ck cookie used to notify connection of operation completion
     */
    bool doCompact(compaction_ctx *ctx, const void *ck);

    /**
     * Remove completed compaction tasks or wake snoozed tasks
     *
     * @param db_file_id vbucket id for couchstore or shard id in the
     *                   case of forestdb
     */
    void updateCompactionTasks(uint16_t db_file_id);

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket deletion in that
     * it does not delete the vbucket instance from memory hash table.
     */
    bool resetVBucket(uint16_t vbid);

    /**
     * Run a vBucket visitor, visiting all items. Synchronous.
     */
    void visit(VBucketVisitor &visitor);

    /**
     * Run a vbucket visitor with separate jobs per vbucket.
     *
     * Note that this is asynchronous.
     */
    size_t visit(std::shared_ptr<VBucketVisitor> visitor, const char *lbl,
               task_type_t taskGroup, const Priority &prio,
               double sleepTime=0) {
        return ExecutorPool::get()->schedule(new VBCBAdaptor(this, visitor,
                                             lbl, prio, sleepTime), taskGroup);
    }

    /**
     * Visit the items in this epStore, starting the iteration from the
     * given startPosition and allowing the visit to be paused at any point.
     *
     * During visitation, the visitor object can request that the visit
     * is stopped after the current item. The position passed to the
     * visitor can then be used to restart visiting at the *APPROXIMATE*
     * same position as it paused.
     * This is approximate as various locks are released when the
     * function returns, so any changes to the underlying epStore may cause
     * the visiting to restart at the slightly different place.
     *
     * As a consequence, *DO NOT USE THIS METHOD* if you need to guarantee
     * that all items are visited!
     *
     * @param visitor The visitor object.
     * @return The final epStore position visited; equal to
     *         EventuallyPersistentStore::end() if all items were visited
     *         otherwise the position to resume from.
     */
    Position pauseResumeVisit(PauseResumeEPStoreVisitor& visitor,
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

    ENGINE_ERROR_CODE getKeyStats(const std::string &key, uint16_t vbucket,
                                  const void* cookie, key_stats &kstats,
                                  bool bgfetch, bool wantsDeleted=false);

    std::string validateKey(const std::string &key,  uint16_t vbucket,
                            Item &diskItem);

    GetValue getLocked(const std::string &key, uint16_t vbucket,
                       rel_time_t currentTime, uint32_t lockTimeout,
                       const void *cookie);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime);


    KVStore* getRWUnderlying(uint16_t vbId) {
        return vbMap.getShardByVbId(vbId)->getRWUnderlying();
    }

    KVStore* getRWUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getRWUnderlying();
    }

    KVStore* getROUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getROUnderlying();
    }

    KVStore* getROUnderlying(uint16_t vbId) {
        return vbMap.getShardByVbId(vbId)->getROUnderlying();
    }

    void deleteExpiredItem(uint16_t, std::string &, time_t, uint64_t,
                           exp_type_t);
    void deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &,
                            exp_type_t);


    /**
     * Get the memoized storage properties from the DB.kv
     */
    const StorageProperties getStorageProperties() const {
        return *storageProperties;
    }

    /**
     * schedule a vb_state snapshot task for all the shards.
     */
    bool scheduleVBSnapshot(const Priority &priority);

    /**
     * schedule a vb_state snapshot task for a given shard.
     */
    void scheduleVBSnapshot(const Priority &priority, uint16_t shardId,
                            bool force = false);

    /**
     * Schedule a vbstate persistence task for a given vbucket.
     */
    void scheduleVBStatePersist(const Priority &priority, uint16_t vbid,
                                bool force = false);

    /**
     * Persist a vbucket's state.
     */
    bool persistVBState(const Priority &priority, uint16_t vbid);

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

    bool isFlushAllScheduled() {
        return diskFlushAll.load();
    }

    bool scheduleFlushAllTask(const void* cookie, time_t when);

    void setFlushAllComplete();

    void setBackfillMemoryThreshold(double threshold);

    void setExpiryPagerSleeptime(size_t val);
    void setExpiryPagerTasktime(ssize_t val);
    void enableExpiryPager();
    void disableExpiryPager();

    void enableAccessScannerTask();
    void disableAccessScannerTask();
    void setAccessScannerSleeptime(size_t val, bool useStartTime);
    void resetAccessScannerStartTime();

    void resetAccessScannerTasktime() {
        accessScanner.lastTaskRuntime = gethrtime();
    }

    void setAllBloomFilters(bool to);

    float getBfiltersResidencyThreshold() {
        return bfilterResidencyThreshold;
    }

    void setBfiltersResidencyThreshold(float to) {
        bfilterResidencyThreshold = to;
    }

    bool isMetaDataResident(RCPtr<VBucket> &vb, const std::string &key);

    void incExpirationStat(RCPtr<VBucket> &vb, exp_type_t source) {
        switch (source) {
        case EXP_BY_PAGER:
            ++stats.expired_pager;
            break;
        case EXP_BY_COMPACTOR:
            ++stats.expired_compactor;
            break;
        case EXP_BY_ACCESS:
            ++stats.expired_access;
            break;
        }
        ++vb->numExpiredItems;
    }

    void logQTime(type_id_t taskType, hrtime_t enqTime) {
        stats.schedulingHisto[taskType].add(enqTime);
    }

    void logRunTime(type_id_t taskType, hrtime_t runTime) {
        stats.taskRuntimeHisto[taskType].add(runTime);
    }

    bool multiBGFetchEnabled() {
        return storageProperties->hasEfficientGet();
    }

    void updateCachedResidentRatio(size_t activePerc, size_t replicaPerc) {
        cachedResidentRatio.activeRatio.store(activePerc);
        cachedResidentRatio.replicaRatio.store(replicaPerc);
    }

    bool isWarmingUp();

    bool maybeEnableTraffic(void);

    /**
     * Checks the memory consumption.
     * To be used by backfill tasks (tap & dcp).
     */
    bool isMemoryUsageTooHigh();

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return The number of items flushed
     */
    int flushVBucket(uint16_t vbid);

    void commit(uint16_t shardId);

    void addKVStoreStats(ADD_STAT add_stat, const void* cookie);

    void addKVStoreTimingStats(ADD_STAT add_stat, const void* cookie);

    /* Given a named KVStore statistic, return the value of that statistic,
     * accumulated across any shards.
     *
     * @param name The name of the statistic
     * @param[out] value The value of the statistic.
     * @return True if the statistic was successfully returned via {value},
     *              else false.
     */
    bool getKVStoreStat(const char* name, size_t& value);

    void resetUnderlyingStats(void);
    KVStore *getOneROUnderlying(void);
    KVStore *getOneRWUnderlying(void);

    item_eviction_policy_t getItemEvictionPolicy(void) const {
        return eviction_policy;
    }

    ENGINE_ERROR_CODE rollback(uint16_t vbid, uint64_t rollbackSeqno);

    void wakeUpItemPager() {
        if (itmpTask->getState() == TASK_SNOOZED) {
            ExecutorPool::get()->wake(itmpTask->getId());
        }
    }

    void wakeUpCheckpointRemover() {
        if (chkTask->getState() == TASK_SNOOZED) {
            ExecutorPool::get()->wake(chkTask->getId());
        }
    }

    void runDefragmenterTask();

    void runAccessScannerTask();

    void runVbStatePersistTask(int vbid);

    void setCompactionWriteQueueCap(size_t to) {
        compactionWriteQueueCap = to;
    }

    void setCompactionExpMemThreshold(size_t to) {
        compactionExpMemThreshold = static_cast<double>(to) / 100.0;
    }

    bool compactionCanExpireItems() {
        // Process expired items only if memory usage is lesser than
        // compaction_exp_mem_threshold and disk queue is small
        // enough (marked by replication_throttle_queue_cap)

        bool isMemoryUsageOk = (stats.getTotalMemoryUsed() <
                          (stats.getMaxDataSize() * compactionExpMemThreshold));

        size_t queueSize = stats.diskQueueSize.load();
        bool isQueueSizeOk = ((stats.replicationThrottleWriteQueueCap == -1) ||
             (queueSize < static_cast<size_t>(stats.replicationThrottleWriteQueueCap)));

        return (isMemoryUsageOk && isQueueSizeOk);
    }

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

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    // Methods called during warmup
    std::vector<vbucket_state *> loadVBucketState();

    void warmupCompleted();
    void stopWarmup(void);

private:

    void scheduleVBDeletion(RCPtr<VBucket> &vb,
                            const void* cookie,
                            double delay = 0,
                            bool clearVbCreateFlag = true);

    /* Queue an item for persistence and replication
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param vb the vbucket that contains the dirty item
     * @param v the dirty item
     * @param plh the pointer to the hash table partition lock for the dirty item
     *        Note that the lock is released inside this function
     * @param seqno sequence number of the mutation
     * @param tapBackfill if the item is from backfill replication
     * @param notifyReplicator whether or not to notify the replicator
     * @param genBySeqno whether or not to generate sequence number
     * @param setConflictMode set the conflict resolution mode
     */
    void queueDirty(RCPtr<VBucket> &vb,
                    StoredValue* v,
                    LockHolder *plh,
                    uint64_t *seqno,
                    bool tapBackfill = false,
                    bool notifyReplicator = true,
                    bool genBySeqno = true,
                    bool setConflictMode = true);

    /**
     * Retrieve a StoredValue and invoke a method on it.
     *
     * Note that because of complications with void/non-void methods
     * and potentially missing StoredValues along with the way I
     * actually intend to use this, I don't return any values from
     * this.
     *
     * @param key the item's key to retrieve
     * @param vbid the vbucket containing the item
     * @param f the method to invoke on the item
     *
     * @return true if the object was found and method was invoked
     */
    bool invokeOnLockedStoredValue(const std::string &key, uint16_t vbid,
                                   void (StoredValue::* f)()) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (!vb) {
            return false;
        }

        int bucket_num(0);
        LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true);

        if (v) {
            std::mem_fun(f)(v);
        }
        return v != NULL;
    }

    void flushOneDeleteAll(void);
    PersistenceCallback* flushOneDelOrSet(const queued_item &qi,
                                          RCPtr<VBucket> &vb);

    StoredValue *fetchValidValue(RCPtr<VBucket> &vb, const std::string &key,
                                 int bucket_num, bool wantsDeleted=false,
                                 bool trackReference=true, bool queueExpired=true);

    GetValue getInternal(const std::string &key, uint16_t vbucket,
                         const void *cookie,
                         vbucket_state_t allowedState,
                         get_options_t options = TRACK_REFERENCE);

    ENGINE_ERROR_CODE addTempItemForBgFetch(LockHolder &lock, int bucket_num,
                                            const std::string &key, RCPtr<VBucket> &vb,
                                            const void *cookie, bool metadataOnly,
                                            bool isReplication = false);

    uint16_t getCommitInterval(uint16_t shardId);

    uint16_t decrCommitInterval(uint16_t shardId);

    friend class Warmup;
    friend class Flusher;
    friend class BGFetchCallback;
    friend class VKeyStatBGFetchCallback;
    friend class PersistenceCallback;
    friend class Deleter;
    friend class VBCBAdaptor;
    friend class VBucketVisitorTask;
    friend class ItemPager;
    friend class PagingVisitor;

    EventuallyPersistentEngine     &engine;
    EPStats                        &stats;
    StorageProperties              *storageProperties;
    Warmup                         *warmupTask;
    ConflictResolution             *conflictResolver;
    VBucketMap                      vbMap;
    ExTask                          itmpTask;
    ExTask                          chkTask;
    float                           bfilterResidencyThreshold;
    ExTask                          defragmenterTask;

    size_t                          compactionWriteQueueCap;
    float                           compactionExpMemThreshold;

    /* Array of mutexes for each vbucket
     * Used by flush operations: flushVB, deleteVB, compactVB, snapshotVB */
    Mutex                          *vb_mutexes;
    AtomicValue<bool>              *schedule_vbstate_persist;
    std::vector<MutationLog*>       accessLog;

    AtomicValue<size_t> bgFetchQueue;

    AtomicValue<bool> diskFlushAll;
    struct FlushAllTaskCtx {
        FlushAllTaskCtx(): delayFlushAll(true), cookie(NULL) {}
        AtomicValue<bool> delayFlushAll;
        const void* cookie;
    } flushAllTaskCtx;

    Mutex vbsetMutex;
    uint32_t bgFetchDelay;
    double backfillMemoryThreshold;
    struct ExpiryPagerDelta {
        ExpiryPagerDelta() : sleeptime(0), task(0), enabled(true) {}
        Mutex mutex;
        size_t sleeptime;
        size_t task;
        bool enabled;
    } expiryPager;
    struct ALogTask {
        ALogTask() : sleeptime(0), task(0), lastTaskRuntime(gethrtime()),
                     enabled(true) {}
        Mutex mutex;
        size_t sleeptime;
        size_t task;
        hrtime_t lastTaskRuntime;
        bool enabled;
    } accessScanner;
    struct ResidentRatio {
        AtomicValue<size_t> activeRatio;
        AtomicValue<size_t> replicaRatio;
    } cachedResidentRatio;
    size_t statsSnapshotTaskId;
    AtomicValue<size_t> lastTransTimePerItem;
    item_eviction_policy_t eviction_policy;

    Mutex compactionLock;
    std::list<CompTaskEntry> compactionTasks;

    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};

/**
 * Base class for visiting an epStore with pause/resume support.
 */
class PauseResumeEPStoreVisitor {
public:
    virtual ~PauseResumeEPStoreVisitor() {}

    /**
     * Visit a hashtable within an epStore.
     *
     * @param vbucket_id ID of the vbucket being visited.
     * @param ht a reference to the hashtable.
     * @return True if visiting should continue, otherwise false.
     */
    virtual bool visit(uint16_t vbucket_id, HashTable& ht) = 0;
};

#endif  // SRC_EP_H_
