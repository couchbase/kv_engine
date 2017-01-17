/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#pragma once

#include "config.h"

#include "executorpool.h"
#include "stored-value.h"
#include "task_type.h"
#include "vbucket.h"
#include "vbucketmap.h"
#include "utility.h"

/* Forward declarations */
class ExtendedMetaData;
class BGFetchCallback;
class ConflictResolution;
class DefragmenterTask;
class KVBucket;
class Flusher;
class MutationLog;
class PauseResumeEPStoreVisitor;
class PersistenceCallback;
class Warmup;


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
    HIDE_LOCKED_CAS  = 0x0020,  //whether locked items should have their CAS
                                //hidden (return -1).
    GET_DELETED_VALUE = 0x0040  //whether to retrieve value of a deleted item
};

/**
 * vbucket-aware hashtable visitor.
 */
class VBucketVisitor {
public:
    VBucketVisitor() = default;

    VBucketVisitor(const VBucketFilter &filter)
        : vBucketFilter(filter) { }

    virtual ~VBucketVisitor() = default;

    /**
     * Begin visiting a bucket.
     *
     * @param vb the vbucket we are beginning to visit
     */
    virtual void visitBucket(RCPtr<VBucket> &vb) = 0;

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

/**
 * This is the abstract base class that manages the bucket behavior in
 * ep-engine.
 * Different bucket types can be derived from this class.
 */

class KVBucketIface {
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

        friend class KVBucket;
        friend std::ostream& operator<<(std::ostream& os, const Position& pos);
    };

    KVBucketIface() {}
    virtual ~KVBucketIface() {}

    /**
     * Start necessary tasks.
     * Client calling initialize must also call deinitialize before deleting
     * the EPBucket instance
     */
    virtual bool initialize() = 0;

    /**
     * Stop tasks started in initialize()
     */
    virtual void deinitialize() = 0;

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cookie the cookie representing the client to store the item
     * @return the result of the store operation
     */
    virtual ENGINE_ERROR_CODE set(Item &item, const void *cookie) = 0;

    /**
     * Add an item in the store.
     * @param item the item to add
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    virtual ENGINE_ERROR_CODE add(Item &item, const void *cookie) = 0;

    /**
     * Replace an item in the store.
     * @param item the item to replace
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    virtual ENGINE_ERROR_CODE replace(Item &item, const void *cookie) = 0;

    /**
     * Add an TAP backfill item into its corresponding vbucket
     * @param item the item to be added
     * @param genBySeqno whether or not to generate sequence number
     * @return the result of the operation
     */
    virtual ENGINE_ERROR_CODE addTAPBackfillItem(Item &item,
                                         bool genBySeqno = true,
                                         ExtendedMetaData *emd = NULL) = 0;

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
    virtual GetValue get(const DocKey& key, uint16_t vbucket,
                         const void *cookie, get_options_t options) = 0;

    virtual GetValue getRandomKey(void) = 0;

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
    virtual GetValue getReplica(const DocKey& key, uint16_t vbucket,
                                const void *cookie,
                                get_options_t options = static_cast<get_options_t>(
                                                                                   QUEUE_BG_FETCH |
                                                                                   HONOR_STATES |
                                                                                   TRACK_REFERENCE |
                                                                                   DELETE_TEMP |
                                                                                   HIDE_LOCKED_CAS)) = 0;


    /**
     * Retrieve the meta data for an item
     *
     * @parapm key the key to get the meta data for
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param metadata where to store the meta informaion
     * @param deleted specifies whether or not the key is deleted
     */
    virtual ENGINE_ERROR_CODE getMetaData(const DocKey& key,
                                          uint16_t vbucket,
                                          const void *cookie,
                                          ItemMetaData &metadata,
                                          uint32_t &deleted) = 0;

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param force override vbucket states
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param genBySeqno whether or not to generate sequence number
     * @param emd ExtendedMetaData class object that contains any ext meta
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the store operation
     */
    virtual ENGINE_ERROR_CODE setWithMeta(Item &item,
                                          uint64_t cas,
                                          uint64_t *seqno,
                                          const void *cookie,
                                          bool force,
                                          bool allowExisting,
                                          GenerateBySeqno genBySeqno = GenerateBySeqno::Yes,
                                          GenerateCas genCas = GenerateCas::No,
                                          ExtendedMetaData *emd = NULL,
                                          bool isReplication = false) = 0;

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
    virtual GetValue getAndUpdateTtl(const DocKey& key, uint16_t vbucket,
                                     const void *cookie, time_t exptime) = 0;

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
    virtual ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                        uint16_t vbucket,
                                        const void *cookie) = 0;

    virtual void completeStatsVKey(const void* cookie, const DocKey& key,
                                   uint16_t vbid, uint64_t bySeqNum) = 0;

    virtual protocol_binary_response_status evictKey(const DocKey& key,
                                                     uint16_t vbucket,
                                                     const char **msg,
                                                     size_t *msg_size) = 0;

    /**
     * delete an item in the store.
     * @param key the key of the item
     * @param cas the CAS ID for a CASed delete (0 to override)
     * @param vbucket the vbucket for the key
     * @param cookie the cookie representing the client
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @param itm item holding a deleted value. A NULL value is passed
                  if an empty body is to be used for deletion.
     * @param itemMeta the pointer to the metadata memory.
     * @param mutInfo mutation information
     *
     * (deleteWithMeta)
     * @param genBySeqno whether or not to generate sequence number
     * @param emd ExtendedMetaData class object that contains any ext meta
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     *
     * @return the result of the delete operation
     */
    virtual ENGINE_ERROR_CODE deleteItem(const DocKey& key,
                                         uint64_t* cas,
                                         uint16_t vbucket,
                                         const void *cookie,
                                         bool force,
                                         Item* itm,
                                         ItemMetaData *itemMeta,
                                         mutation_descr_t *mutInfo) = 0;

    virtual ENGINE_ERROR_CODE deleteWithMeta(const DocKey& key,
                                             uint64_t* cas,
                                             uint64_t* seqno,
                                             uint16_t vbucket,
                                             const void *cookie,
                                             bool force,
                                             ItemMetaData *itemMeta,
                                             bool tapBackfill,
                                             GenerateBySeqno genBySeqno,
                                             GenerateCas generateCas,
                                             uint64_t bySeqno,
                                             ExtendedMetaData *emd,
                                             bool isReplication) = 0;

    virtual void reset() = 0;

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
    virtual void setBGFetchDelay(uint32_t to) = 0;

    virtual double getBGFetchDelay(void) = 0;

    virtual void stopFlusher(void) = 0;

    virtual bool startFlusher(void) = 0;

    virtual bool pauseFlusher(void) = 0;
    virtual bool resumeFlusher(void) = 0;
    virtual void wakeUpFlusher(void) = 0;

    virtual bool startBgFetcher(void) = 0;
    virtual void stopBgFetcher(void) = 0;

    /**
     * Takes a snapshot of the current stats and persists them to disk.
     */
    virtual void snapshotStats(void) = 0;

    /**
     * Get file statistics
     */
    virtual DBFileInfo getFileStats(const void *cookie) = 0;

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
    virtual void completeBGFetch(const DocKey& key,
                                 uint16_t vbucket,
                                 const void *cookie,
                                 hrtime_t init,
                                 bool isMeta) = 0;
    /**
     * Complete a batch of background fetch of a non resident value or metadata.
     *
     * @param vbId the vbucket in which the requested key lived
     * @param fetchedItems vector of completed background feches containing key,
     *                     value, client cookies
     * @param start the time when the background fetch was started
     *
     */
    virtual void completeBGFetchMulti(
                                    uint16_t vbId,
                                    std::vector<bgfetched_item_t> &fetchedItems,
                                    hrtime_t start) = 0;

    virtual RCPtr<VBucket> getVBucket(uint16_t vbid) = 0;

    virtual uint64_t getLastPersistedCheckpointId(uint16_t vb) = 0;

    virtual uint64_t getLastPersistedSeqno(uint16_t vb) = 0;

    /* transfer should be set to true *only* if this vbucket is becoming master
     * as the result of the previous master cleanly handing off control. */
    virtual ENGINE_ERROR_CODE setVBucketState(uint16_t vbid,
                                              vbucket_state_t state,
                                              bool transfer,
                                              bool notify_dcp = true) = 0;

    /**
     * Physically deletes a VBucket from disk. This function should only
     * be called on a VBucket that has already been logically deleted.
     *
     * @param vbid vbucket id
     * @param cookie The connection that requested the deletion
     */
    virtual bool completeVBucketDeletion(uint16_t vbid, const void* cookie) = 0;

    /**
     * Deletes a vbucket
     *
     * @param vbid The vbucket to delete.
     * @param c The cookie for this connection.
     *          Used in synchronous bucket deletes
     *          to notify the connection of operation completion.
     */
    virtual ENGINE_ERROR_CODE deleteVBucket(uint16_t vbid,
                                            const void* c = NULL) = 0;

    /**
     * Check for the existence of a vbucket in the case of couchstore
     * or shard in the case of forestdb. Note that this function will be
     * deprecated once forestdb is the only backend supported
     *
     * @param db_file_id vbucketid for couchstore or shard id in the
     *                   case of forestdb
     */
    virtual ENGINE_ERROR_CODE checkForDBExistence(uint16_t db_file_id) = 0;

    /**
     * Triggers compaction of a database file
     *
     * @param vbid The vbucket being compacted
     * @param c The context for compaction of a DB file
     * @param ck cookie used to notify connection of operation completion
     */
    virtual ENGINE_ERROR_CODE scheduleCompaction(uint16_t vbid,
                                                 compaction_ctx c,
                                                 const void *ck) = 0;

    /**
     * Compaction of a database file
     *
     * @param ctx Context for compaction hooks
     * @param ck cookie used to notify connection of operation completion
     *
     * return true if the compaction needs to be rescheduled and false
     *             otherwise
     */
    virtual bool doCompact(compaction_ctx *ctx, const void *ck) = 0;

    /**
     * Get the database file id for the compaction request
     *
     * @param req compaction request structure
     *
     * returns the database file id from the underlying KV store
     */
    virtual uint16_t getDBFileId(
                            const protocol_binary_request_compact_db& req) = 0;

    /**
     * Remove completed compaction tasks or wake snoozed tasks
     *
     * @param db_file_id vbucket id for couchstore or shard id in the
     *                   case of forestdb
     */
    virtual void updateCompactionTasks(uint16_t db_file_id) = 0;

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket
     * deletion in that it does not delete the vbucket instance from memory hash
     * table.
     */
    virtual bool resetVBucket(uint16_t vbid) = 0;

    /**
     * Run a vBucket visitor, visiting all items. Synchronous.
     */
    virtual void visit(VBucketVisitor &visitor) = 0;

    /**
     * Run a vbucket visitor with separate jobs per vbucket.
     *
     * Note that this is asynchronous.
     */
    virtual size_t visit(std::unique_ptr<VBucketVisitor> visitor,
                         const char* lbl,
                         task_type_t taskGroup,
                         TaskId id,
                         double sleepTime = 0) = 0;

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
     *         EPBucket::end() if all items were visited
     *         otherwise the position to resume from.
     */
    virtual Position pauseResumeVisit(PauseResumeEPStoreVisitor& visitor,
                                      Position& start_pos) = 0;


    /**
     * Return a position at the start of the epStore.
     */
    virtual Position startPosition() const = 0;

    /**
     * Return a position at the end of the epStore. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    virtual Position endPosition() const = 0;

    virtual const Flusher* getFlusher(uint16_t shardId) = 0;

    virtual Warmup* getWarmup(void) const = 0;

    /**
     * Looks up the key stats for the given {vbucket, key}.
     * @param key The key to lookup
     * @param vbucket The vbucket the key belongs to.
     * @param cookie The client's cookie
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If true then return keystats even if the item is
     *                     marked as deleted. If false then will return
     *                     ENGINE_KEY_ENOENT for deleted items.
     */
    virtual ENGINE_ERROR_CODE getKeyStats(const DocKey& key,
                                          uint16_t vbucket, const void* cookie,
                                          key_stats &kstats,
                                          bool wantsDeleted) = 0;

    virtual std::string validateKey(const DocKey& key, uint16_t vbucket,
                                    Item &diskItem) = 0;

    virtual GetValue getLocked(const DocKey& key, uint16_t vbucket,
                               rel_time_t currentTime, uint32_t lockTimeout,
                               const void *cookie) = 0;

    virtual ENGINE_ERROR_CODE unlockKey(const DocKey& key,
                                        uint16_t vbucket,
                                        uint64_t cas,
                                        rel_time_t currentTime) = 0;


    virtual KVStore* getRWUnderlying(uint16_t vbId) = 0;

    virtual KVStore* getRWUnderlyingByShard(size_t shardId) = 0;

    virtual KVStore* getROUnderlyingByShard(size_t shardId) = 0;

    virtual KVStore* getROUnderlying(uint16_t vbId) = 0;

    virtual void deleteExpiredItem(
            uint16_t, const DocKey&, time_t, uint64_t, ExpireBy) = 0;
    virtual void deleteExpiredItems(
            std::list<std::pair<uint16_t, StoredDocKey>>&, ExpireBy) = 0;

    /**
     * Get the memoized storage properties from the DB.kv
     */
    virtual const StorageProperties getStorageProperties() const = 0;

    /**
     * schedule a vb_state snapshot task for a given shard.
     */
    virtual void scheduleVBStatePersist() = 0;

    /**
     * Schedule a vbstate persistence task for a given vbucket.
     */
    virtual void scheduleVBStatePersist(uint16_t vbid) = 0;

    virtual const VBucketMap &getVBuckets() = 0;

    virtual EventuallyPersistentEngine& getEPEngine() = 0;

    virtual size_t getExpiryPagerSleeptime(void) = 0;

    virtual size_t getTransactionTimePerItem() = 0;

    virtual bool isFlushAllScheduled() = 0;

    virtual bool scheduleFlushAllTask(const void* cookie) = 0;

    virtual void setFlushAllComplete() = 0;

    virtual void setBackfillMemoryThreshold(double threshold) = 0;

    virtual void setExpiryPagerSleeptime(size_t val) = 0;

    virtual void setExpiryPagerTasktime(ssize_t val) = 0;

    virtual void enableExpiryPager() = 0;
    virtual void disableExpiryPager() = 0;

    virtual void enableAccessScannerTask() = 0;
    virtual void disableAccessScannerTask() = 0;
    virtual void setAccessScannerSleeptime(size_t val, bool useStartTime) = 0;
    virtual void resetAccessScannerStartTime() = 0;

    virtual void resetAccessScannerTasktime() = 0;

    virtual void setAllBloomFilters(bool to) = 0;

    virtual float getBfiltersResidencyThreshold() = 0;

    virtual void setBfiltersResidencyThreshold(float to) = 0;

    virtual bool isMetaDataResident(RCPtr<VBucket> &vb,
                                    const DocKey& key) = 0;

    virtual void logQTime(TaskId taskType,
                          const ProcessClock::duration enqTime) = 0;

    virtual void logRunTime(TaskId taskType,
                            const ProcessClock::duration runTime) = 0;

    virtual bool multiBGFetchEnabled() = 0;

    virtual void updateCachedResidentRatio(size_t activePerc,
                                           size_t replicaPerc) = 0;

    virtual bool isWarmingUp() = 0;

    virtual bool maybeEnableTraffic(void) = 0;

    /**
     * Checks the memory consumption.
     * To be used by backfill tasks (tap & dcp).
     */
    virtual bool isMemoryUsageTooHigh() = 0;

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return The number of items flushed
     */
    virtual int flushVBucket(uint16_t vbid) = 0;

    virtual void commit(uint16_t shardId) = 0;

    virtual void addKVStoreStats(ADD_STAT add_stat, const void* cookie) = 0;

    virtual void addKVStoreTimingStats(ADD_STAT add_stat,
                                       const void* cookie) = 0;

    /**
     * The following options will be used to identify
     * the kind of KVStores to be considered for stat collection.
     */
    enum class KVSOption {
        RO,          // RO KVStore
        RW,          // RW KVStore
        BOTH         // Both KVStores
    };

    /* Given a named KVStore statistic, return the value of that statistic,
     * accumulated across any shards.
     *
     * @param name The name of the statistic
     * @param[out] value The value of the statistic.
     * @param option the KVStore to read stats from.
     * @return True if the statistic was successfully returned via {value},
     *              else false.
     */
    virtual bool getKVStoreStat(const char* name, size_t& value,
                                KVSOption option) = 0;

    virtual void resetUnderlyingStats(void) = 0;
    virtual KVStore *getOneROUnderlying(void) = 0;
    virtual KVStore *getOneRWUnderlying(void) = 0;

    virtual item_eviction_policy_t getItemEvictionPolicy(void) const  = 0;
    virtual ENGINE_ERROR_CODE rollback(uint16_t vbid,
                                       uint64_t rollbackSeqno) = 0;

    virtual void wakeUpItemPager() = 0;

    virtual void wakeUpCheckpointRemover() = 0;

    virtual void runDefragmenterTask() = 0;

    virtual bool runAccessScannerTask() = 0;

    virtual void runVbStatePersistTask(int vbid) = 0;

    virtual void setCompactionWriteQueueCap(size_t to) = 0;

    virtual void setCompactionExpMemThreshold(size_t to) = 0;

    virtual bool compactionCanExpireItems() = 0;

    virtual void setCursorDroppingLowerUpperThresholds(size_t maxSize) = 0;

    virtual bool isAccessScannerEnabled() = 0;

    virtual bool isExpPagerEnabled() = 0;

    //Check if there were any out-of-memory errors during warmup
    virtual bool isWarmupOOMFailure(void) = 0;

    virtual size_t getActiveResidentRatio() const = 0;

    virtual size_t getReplicaResidentRatio() const = 0;

    /*
     * Change the max_cas of the specified vbucket to cas without any
     * care for the data or ongoing operations...
     */
    virtual ENGINE_ERROR_CODE forceMaxCas(uint16_t vbucket, uint64_t cas) = 0;

    /**
     * Create a VBucket object appropriate for this Bucket class.
     */
    virtual RCPtr<VBucket> makeVBucket(
            VBucket::id_type id,
            vbucket_state_t state,
            KVShard* shard,
            std::unique_ptr<FailoverTable> table,
            std::shared_ptr<Callback<VBucket::id_type>> cb,
            NewSeqnoCallback newSeqnoCb,
            vbucket_state_t initState = vbucket_state_dead,
            int64_t lastSeqno = 0,
            uint64_t lastSnapStart = 0,
            uint64_t lastSnapEnd = 0,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0) = 0;

    /**
     * Notify all the clients of a new seqno being added in the vbucket
     */
    virtual void notifyNewSeqno(const uint16_t vbid,
                                const VBNotifyCtx& notifyCtx) = 0;

protected:

    // Methods called during warmup
    virtual std::vector<vbucket_state *> loadVBucketState() = 0;

    virtual void warmupCompleted() = 0;
    virtual void stopWarmup(void) = 0;

    /**
     * Compaction of a database file
     *
     * @param ctx Context for compaction hooks
     */
    virtual void compactInternal(compaction_ctx *ctx) = 0;

    virtual void scheduleVBDeletion(RCPtr<VBucket> &vb,
                            const void* cookie,
                            double delay = 0) = 0;

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
    virtual bool invokeOnLockedStoredValue(const DocKey& key,
                                           uint16_t vbid,
                                           void (StoredValue::* f)()) = 0;

    virtual void flushOneDeleteAll(void) = 0;
    virtual PersistenceCallback* flushOneDelOrSet(const queued_item &qi,
                                                  RCPtr<VBucket> &vb) = 0;

    virtual GetValue getInternal(const DocKey& key, uint16_t vbucket,
                                 const void *cookie,
                                 vbucket_state_t allowedState,
                                 get_options_t options = TRACK_REFERENCE) = 0;

    virtual uint16_t getCommitInterval(uint16_t shardId) = 0;

    virtual uint16_t decrCommitInterval(uint16_t shardId) = 0;

    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    friend class Warmup;
    friend class PersistenceCallback;
    friend class VBCBAdaptor;
};
