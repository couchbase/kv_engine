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

#include <memcached/engine.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "atomic.h"
#include "bgfetcher.h"
#include "item_pager.h"
#include "kvstore.h"
#include "locks.h"
#include "executorpool.h"
#include "stats.h"
#include "stored-value.h"
#include "vbucket.h"
#include "vbucketmap.h"

#define BACKFILL_MEM_THRESHOLD 0.95

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
class EventuallyPersistentStore;
class Flusher;
class MutationLog;
class PersistenceCallback;
class Warmup;

/**
 * VBucket visitor callback adaptor.
 */
class VBCBAdaptor : public GlobalTask {
public:

    VBCBAdaptor(EventuallyPersistentStore *s,
                shared_ptr<VBucketVisitor> v, const char *l, const Priority &p,
                double sleep=0);

    std::string getDescription() {
        std::stringstream rv;
        rv << label << " on vb " << currentvb;
        return rv.str();
    }

    bool run(void);

private:
    std::queue<uint16_t>        vbList;
    EventuallyPersistentStore  *store;
    shared_ptr<VBucketVisitor>  visitor;
    const char                 *label;
    double                      sleepTime;
    uint16_t                    currentvb;

    DISALLOW_COPY_AND_ASSIGN(VBCBAdaptor);
};


/**
 * Vbucket visitor task for a generic scheduler.
 */
class VBucketVisitorTask : public GlobalTask {
public:

    VBucketVisitorTask(EventuallyPersistentStore *s,
                       shared_ptr<VBucketVisitor> v, uint16_t sh,
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
    shared_ptr<VBucketVisitor>   visitor;
    const char                  *label;
    double                       sleepTime;
    uint16_t                     currentvb;
    uint16_t                     shardID;
};

const uint16_t EP_PRIMARY_SHARD = 0;
class KVShard;

/**
 * Manager of all interaction with the persistence.
 */
class EventuallyPersistentStore {
public:

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
     * @param meta contains meta info or not
     * @param nru the nru bit for the item
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addTAPBackfillItem(const Item &item, uint8_t nru = 0xff,
                                         bool genBySeqno = true);

    /**
     * Retrieve a value.
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param honorStates if false, fetch a result regardless of state
     * @param trackReference true if we want to set the nru bit for the item
     *
     * @return a GetValue representing the result of the request
     */
    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie, bool queueBG=true,
                 bool honorStates=true, bool trackReference=true) {
        return getInternal(key, vbucket, cookie, queueBG, honorStates,
                           vbucket_state_active, trackReference);
    }

    GetValue getRandomKey(void);

    /**
     * Retrieve a value from a vbucket in replica state.
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getReplica(const std::string &key, uint16_t vbucket,
                        const void *cookie, bool queueBG=true) {
        return getInternal(key, vbucket, cookie, queueBG, true,
                           vbucket_state_replica);
    }


    /**
     * Retrieve the meta data for an item
     *
     * @parapm key the key to get the meta data for
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param metadata where to store the meta informaion
     * @param true if we want to set the nru bit for the item
     * @param deleted specifies whether or not the key is deleted
     */
    ENGINE_ERROR_CODE getMetaData(const std::string &key,
                                  uint16_t vbucket,
                                  const void *cookie,
                                  ItemMetaData &metadata,
                                  uint32_t &deleted,
                                  bool trackReference = false);

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cas value to match
     * @param cookie the cookie representing the client to store the item
     * @param force override vbucket states
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param nru the nru bit for the item
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(const Item &item,
                                  uint64_t cas,
                                  const void *cookie,
                                  bool force,
                                  bool allowReplace,
                                  uint8_t nru = 0xff,
                                  bool genBySeqno = true);

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
     * @return the result of the delete operation
     */
    ENGINE_ERROR_CODE deleteItem(const std::string &key,
                                 uint64_t* cas,
                                 uint16_t vbucket,
                                 const void *cookie,
                                 bool force,
                                 ItemMetaData *itemMeta,
                                 bool tapBackfill=false);

    ENGINE_ERROR_CODE deleteWithMeta(const std::string &key,
                                     uint64_t* cas,
                                     uint16_t vbucket,
                                     const void *cookie,
                                     bool force,
                                     ItemMetaData *itemMeta,
                                     bool tapBackfill=false,
                                     bool genBySeqno=true,
                                     uint64_t bySeqno=0);

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
     * @param rowid the rowid of the record within its shard
     * @param cookie the cookie of the requestor
     * @param type whether the fetch is for a non-resident value or metadata of
     *             a (possibly) deleted item
     */
    void bgFetch(const std::string &key,
                 uint16_t vbucket,
                 uint64_t rowid,
                 const void *cookie,
                 bool isMeta = false);

    /**
     * Complete a background fetch of a non resident value or metadata.
     *
     * @param key the key that was fetched
     * @param vbucket the vbucket in which the key lived
     * @param rowid the rowid of the record within its shard
     * @param cookie the cookie of the requestor
     * @param init the timestamp of when the request came in
     * @param type whether the fetch is for a non-resident value or metadata of
     *             a (possibly) deleted item
     */
    void completeBGFetch(const std::string &key,
                         uint16_t vbucket,
                         uint64_t rowid,
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
     * as the result of the previous master cleanly handing off contorol. */
    ENGINE_ERROR_CODE setVBucketState(uint16_t vbid, vbucket_state_t state,
                                      bool transfer, bool notify_upr = true);

    /**
     * Physically deletes a VBucket from disk. This function should only
     * be called on a VBucket that has already been logically deleted.
     *
     * @param vb The VBucket object
     * @param cookie The connection that requested the deletion
     * @param recreate Whether or not to recreate the VBucket after deletion
     */
    bool completeVBucketDeletion(RCPtr<VBucket> vb, const void* cookie,
                                 bool recreate);

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
     * Triggers compaction of a vbucket
     *
     * @param vbid The vbucket to compact.
     * @param c The context for compaction of a DB file
     * @param ck cookie used to notify connection of operation completion
     */
    ENGINE_ERROR_CODE compactDB(uint16_t vbid, compaction_ctx c,
                                const void *ck);

    /**
     * Callback to do the compaction of a vbucket
     *
     * @param vbid The Id of the VBucket which needs to be compacted
     * @param ctx Context for couchstore compaction hooks
     * @param ck cookie used to notify connection of operation completion
     */
    bool compactVBucket(const uint16_t vbid, compaction_ctx *ctx,
                        const void *ck);

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket deletion in that
     * it does not delete the vbucket instance from memory hash table.
     */
    bool resetVBucket(uint16_t vbid);

    void visit(VBucketVisitor &visitor);

    /**
     * Run a vbucket visitor with separate jobs per vbucket.
     *
     * Note that this is asynchronous.
     */
    size_t visit(shared_ptr<VBucketVisitor> visitor, const char *lbl,
               task_type_t taskGroup, const Priority &prio,
               double sleepTime=0) {
        return ExecutorPool::get()->schedule(new VBCBAdaptor(this, visitor,
                                             lbl, prio, sleepTime), taskGroup);
    }

    const Flusher* getFlusher(uint16_t shardId);
    Warmup* getWarmup(void) const;

    ENGINE_ERROR_CODE getKeyStats(const std::string &key, uint16_t vbucket,
                                  const void* cookie, key_stats &kstats,
                                  bool bgfetch, bool wantsDeleted=false);

    std::string validateKey(const std::string &key,  uint16_t vbucket,
                            Item &diskItem);

    bool getLocked(const std::string &key, uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime, uint32_t lockTimeout,
                   const void *cookie);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime);


    KVStore* getRWUnderlying(uint16_t vbId) {
        return vbMap.getShard(vbId)->getRWUnderlying();
    }

    KVStore* getRWUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getRWUnderlying();
    }

    KVStore* getROUnderlyingByShard(size_t shardId) {
        return vbMap.shards[shardId]->getROUnderlying();
    }

    KVStore* getROUnderlying(uint16_t vbId) {
        return vbMap.getShard(vbId)->getROUnderlying();
    }

    void deleteExpiredItem(uint16_t, std::string &, time_t, uint64_t );
    void deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &);


    /**
     * Get the memoized storage properties from the DB.kv
     */
    const StorageProperties getStorageProperties() const {
        return *storageProperties;
    }

    /**
     * schedule snapshot for entire shards
     */
    void scheduleVBSnapshot(const Priority &priority);

    /**
     * schedule snapshot for specified shard
     */
    void scheduleVBSnapshot(const Priority &priority, uint16_t shardId,
                            bool force = false);

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
        return lastTransTimePerItem;
    }

    bool isFlushAllScheduled() {
        return diskFlushAll.load();
    }

    void setExpiryPagerSleeptime(size_t val);
    void setAccessScannerSleeptime(size_t val);
    void resetAccessScannerStartTime();

    void resetAccessScannerTasktime() {
        accessScanner.lastTaskRuntime = gethrtime();
    }

    void incExpirationStat(RCPtr<VBucket> &vb, bool byPager = true) {
        if (byPager) {
            ++stats.expired_pager;
        } else {
            ++stats.expired_access;
        }
        ++vb->numExpiredItems;
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
     * To be used by backfill tasks (tap & upr).
     */
    bool isMemoryUsageTooHigh();

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return The amount of items flushed
     */
    int flushVBucket(uint16_t vbid);

    void addKVStoreStats(ADD_STAT add_stat, const void* cookie);

    void addKVStoreTimingStats(ADD_STAT add_stat, const void* cookie);

    void resetUnderlyingStats(void);
    KVStore *getOneROUnderlying(void);
    KVStore *getOneRWUnderlying(void);

    item_eviction_policy_t getItemEvictionPolicy(void) const {
        return eviction_policy;
    }

    ENGINE_ERROR_CODE rollback(uint16_t vbid, uint64_t rollbackSeqno,
                               shared_ptr<RollbackCB> cb);

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    // Methods called during warmup
    std::map<uint16_t, vbucket_state> loadVBucketState();

    void warmupCompleted();
    void stopWarmup(void);

private:

    void scheduleVBDeletion(RCPtr<VBucket> &vb,
                            const void* cookie,
                            double delay = 0,
                            bool recreate = false);

    RCPtr<VBucket> getVBucket(uint16_t vbid, vbucket_state_t wanted_state);

    /* Queue an item for persistence and replication
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param vb the vbucket that contains the dirty item
     * @param v the dirty item
     * @param plh the pointer to the hash table partition lock for the dirty item.
     *        Note that the lock is released inside this function.
     * @param tapBackfill if the item is from backfill replication
     * @param notifyReplicator whether or not to notify the replicator
     */
    void queueDirty(RCPtr<VBucket> &vb,
                    StoredValue* v,
                    LockHolder *plh,
                    bool tapBackfill = false,
                    bool notifyReplicator = true,
                    bool genBySeqno = true);

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
                         const void *cookie, bool queueBG,
                         bool honorStates,
                         vbucket_state_t allowedState,
                         bool trackReference=true);

    ENGINE_ERROR_CODE addTempItemForBgFetch(LockHolder &lock, int bucket_num,
                                            const std::string &key, RCPtr<VBucket> &vb,
                                            const void *cookie, bool metadataOnly);

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
    /* Array of mutexes for each vbucket
     * Used by flush operations: flushVB, deleteVB, compactVB, snapshotVB */
    Mutex                          *vb_mutexes;
    SyncObject                      mutex;
    std::vector<MutationLog*>       accessLog;

    AtomicValue<size_t> bgFetchQueue;
    AtomicValue<bool> diskFlushAll;
    Mutex vbsetMutex;
    uint32_t bgFetchDelay;
    struct ExpiryPagerDelta {
        ExpiryPagerDelta() : sleeptime(0), task(0) {}
        Mutex mutex;
        size_t sleeptime;
        size_t task;
    } expiryPager;
    struct ALogTask {
        ALogTask() : sleeptime(0), task(0), lastTaskRuntime(gethrtime()) {}
        Mutex mutex;
        size_t sleeptime;
        size_t task;
        hrtime_t lastTaskRuntime;
    } accessScanner;
    struct ResidentRatio {
        AtomicValue<size_t> activeRatio;
        AtomicValue<size_t> replicaRatio;
    } cachedResidentRatio;
    size_t statsSnapshotTaskId;
    size_t lastTransTimePerItem;
    AtomicValue<bool> snapshotVBState;
    item_eviction_policy_t eviction_policy;

    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};

#endif  // SRC_EP_H_
