/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#ifndef EP_HH
#define EP_HH 1

#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdexcept>
#include <iostream>
#include <queue>
#include <limits>
#include <unistd.h>

#include <set>
#include <list>
#include <queue>
#include <algorithm>

#include <memcached/engine.h>

#include "queueditem.hh"
#include "stats.hh"
#include "locks.hh"
#include "kvstore.hh"
#include "stored-value.hh"
#include "atomic.hh"
#include "dispatcher.hh"
#include "vbucket.hh"
#include "vbucketmap.hh"
#include "item_pager.hh"
#include "mutation_log.hh"
#include "mutation_log_compactor.hh"

#define MAX_BG_FETCH_DELAY 900

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

typedef std::pair<int64_t, int64_t> chunk_range_t;
typedef std::list<chunk_range_t>::iterator chunk_range_iterator_t;

// Forward declaration
class Flusher;
class Warmup;
class TapBGFetchCallback;
class EventuallyPersistentStore;

class PersistenceCallback;

/**
 * Maintains scope of a underlying storage transaction, being useful
 * and what not.
 */
class TransactionContext {
public:

    TransactionContext(EPStats &st, KVStore *ks, MutationLog &log)
        : stats(st), underlying(ks), mutationLog(log), _remaining(0), intxn(false) {}

    /**
     * Call this whenever entering a transaction.
     *
     * This will (when necessary) begin the tranasaction and reset the
     * counter of remaining items for a transaction.
     *
     * @return true if we're in a transaction
     */
    bool enter();

    /**
     * Called whenever leaving, having completed the given number of
     * updates.
     *
     * When the number of updates completed exceeds the number
     * permitted per transaction, a transaction will be closed and
     * reopened.
     */
    void leave(int completed);

    /**
     * Explicitly commit a transaction.
     *
     * This will reset the remaining counter and begin a new
     * transaction for the next batch.
     */
    void commit();

    /**
     * Get the number of updates permitted by this transaction.
     */
    size_t remaining() {
        return _remaining;
    }

    /**
     * Request a commit occur at the next opportunity.
     */
    void commitSoon() {
        _remaining = 0;
    }

    /**
     * Get the current number of updates permitted per transaction.
     */
    int getTxnSize() {
        return txnSize.get();
    }

    /**
     * Set the current number of updates permitted per transaction.
     */
    void setTxnSize(int to) {
        txnSize.set(to);
        underlying->processTxnSizeChange(to);
    }

    void addUncommittedItem(const queued_item &item);

    size_t getNumUncommittedItems() {
        return numUncommittedItems;
    }

    void addCallback(PersistenceCallback *cb) {
        transactionCallbacks.push_back(cb);
    }

private:
    EPStats     &stats;
    KVStore     *underlying;
    MutationLog &mutationLog;
    int          _remaining;
    Atomic<int>  txnSize;
    Atomic<size_t> numUncommittedItems;
    bool         intxn;
    std::list<queued_item>     uncommittedItems;
    std::list<PersistenceCallback*> transactionCallbacks;
};

/**
 * VBucket visitor callback adaptor.
 */
class VBCBAdaptor : public DispatcherCallback {
public:

    VBCBAdaptor(EventuallyPersistentStore *s,
                shared_ptr<VBucketVisitor> v, const char *l, double sleep=0);

    std::string description() {
        std::stringstream rv;
        rv << label << " on vb " << currentvb;
        return rv.str();
    }

    bool callback(Dispatcher &d, TaskId t);

private:
    std::queue<uint16_t>        vbList;
    EventuallyPersistentStore  *store;
    shared_ptr<VBucketVisitor>  visitor;
    const char                 *label;
    double                      sleepTime;
    uint16_t                    currentvb;

    DISALLOW_COPY_AND_ASSIGN(VBCBAdaptor);
};

class EventuallyPersistentEngine;

typedef enum {
    BG_FETCH_VALUE,
    BG_FETCH_METADATA
} bg_fetch_type_t;

/**
 * Manager of all interaction with the persistence.
 */
class EventuallyPersistentStore {
public:

    EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                              KVStore *t, bool startVb0,
                              bool concurrentDB);

    ~EventuallyPersistentStore();

    void initialize();

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cookie the cookie representing the client to store the item
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE set(const Item &item,
                          const void *cookie,
                          bool force = false);

    ENGINE_ERROR_CODE add(const Item &item, const void *cookie);

    /**
     * Add an TAP backfill item into its corresponding vbucket
     * @param item the item to be added
     * @param meta contains meta info or not
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addTAPBackfillItem(const Item &item, bool meta);

    /**
     * Retrieve a value.
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param honorStates if false, fetch a result regardless of state
     * @param trackReference true if we want to set the reference bit for the item
     *
     * @return a GetValue representing the result of the request
     */
    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie, bool queueBG=true,
                 bool honorStates=true, bool trackReference=true) {
        return getInternal(key, vbucket, cookie, queueBG, honorStates,
                           vbucket_state_active, trackReference);
    }

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
     * @param meta where to store the meta informaion
     * @param cas where to store the cas information
     * @param flags where to store the flags information
     */
    ENGINE_ERROR_CODE getMetaData(const std::string &key,
                                  uint16_t vbucket,
                                  const void *cookie,
                                  std::string &meta,
                                  uint64_t &cas,
                                  uint32_t &flags);

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cas value to match
     * @param cookie the cookie representing the client to store the item
     * @param force override vbucket states
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(const Item &item,
                                  uint64_t cas,
                                  const void *cookie,
                                  bool force,
                                  bool allowReplace);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param exptime the new expiry time for the object
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(const std::string &key, uint16_t vbucket,
                             const void *cookie, bool queueBG, time_t exptime);

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
    ENGINE_ERROR_CODE getFromUnderlying(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        shared_ptr<Callback<GetValue> > cb);

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size,
                                             bool force=false);

    /**
     * delete an item in the store.
     * @param key the key of the item
     * @param newSeqno the new seq no of the item
     * @param cas the CAS ID for a CASed delete (0 to override)
     * @param vbucket the vbucket for the key
     * @param cookie the cookie representing the client
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @param use_meta delete an item using its meta data
     * @param newItem pointer to metadata of new item
     * @return the result of the delete operation
     */
    ENGINE_ERROR_CODE deleteItem(const std::string &key,
                                 uint64_t cas,
                                 uint16_t vbucket,
                                 const void *cookie,
                                 bool force,
                                 bool use_meta,
                                 ItemMetaData *newItemMeta);



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

    void startDispatcher(void);

    void startNonIODispatcher(void);

    /**
     * Get the current dispatcher.
     *
     * You can use this to queue io related jobs.  Don't do stupid things with
     * it.
     */
    Dispatcher* getDispatcher(void) {
        assert(dispatcher);
        return dispatcher;
    }

    /**
     * Get the current read-only IO dispatcher.
     */
    Dispatcher* getRODispatcher(void) {
        assert(roDispatcher);
        return roDispatcher;
    }

    /**
     * True if the RW dispatcher and RO dispatcher are distinct.
     */
    bool hasSeparateRODispatcher() {
        return dispatcher != roDispatcher;
    }

    /**
     * Get the current tap-replication IO dispatcher.
     */
    Dispatcher* getTapDispatcher(void) {
        assert(tapDispatcher);
        return tapDispatcher;
    }

    /**
     * True if the RO dispatcher and tap-replication dispatcher are distinct.
     */
    bool hasSeparateTapDispatcher() {
        return roDispatcher != tapDispatcher;
    }

    /**
     * Get the current non-io dispatcher.
     *
     * Use this dispatcher to queue non-io jobs.
     */
    Dispatcher* getNonIODispatcher(void) {
        assert(nonIODispatcher);
        return nonIODispatcher;
    }

    void stopFlusher(void);

    void startFlusher(void);

    bool pauseFlusher(void);
    bool resumeFlusher(void);

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
                 bg_fetch_type_t type = BG_FETCH_VALUE);

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
                         bg_fetch_type_t type);

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

    RCPtr<VBucket> getVBucket(uint16_t vbid);

    uint64_t getLastPersistedCheckpointId(uint16_t vb) {
        return vbuckets.getPersistenceCheckpointId(vb);
    }

    void snapshotVBuckets(const Priority &priority);
    void setVBucketState(uint16_t vbid,
                         vbucket_state_t state);

    /**
     * Perform a fast vbucket deletion.
     */
    vbucket_del_result completeVBucketDeletion(uint16_t vbid);

    /**
     * Deletes a vbucket
     *
     * @param vbid The vbucket to delete.
     * @param c The cookie for this connection. Used in synchronous bucket deletes
     *          to notify the connection of operation completion.
     */
    ENGINE_ERROR_CODE deleteVBucket(uint16_t vbid, const void* c = NULL);

    void firePendingVBucketOps();

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
    void visit(shared_ptr<VBucketVisitor> visitor, const char *lbl,
               Dispatcher *d, const Priority &prio, bool isDaemon=true, double sleepTime=0) {
        d->schedule(shared_ptr<DispatcherCallback>(new VBCBAdaptor(this, visitor, lbl, sleepTime)),
                    NULL, prio, 0, isDaemon);
    }

    int getTxnSize() {
        return tctx.getTxnSize();
    }

    void setTxnSize(int to) {
        tctx.setTxnSize(to);
    }

    size_t getNumUncommittedItems() {
        return tctx.getNumUncommittedItems();
    }

    const Flusher* getFlusher();
    Warmup* getWarmup(void) const;

    ENGINE_ERROR_CODE getKeyStats(const std::string &key, uint16_t vbucket,
                                  key_stats &kstats);

    bool getLocked(const std::string &key, uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime, uint32_t lockTimeout,
                   const void *cookie);

    /**
     * Retrieve the StoredValue associated with a key/vbucket pair.
     *
     * @param key the key
     * @param vbucket the vbucket's ID
     * @param honorStates if false, fetch a result regardless of state
     *
     * @return a pointer to the StoredValue associated with the key/vbucket,
     *         if any, NULL otherwise
     */
    StoredValue* getStoredValue(const std::string &key,
                                uint16_t vbucket,
                                bool honorStates = true);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime);


    KVStore* getRWUnderlying() {
        // This method might also be called leakAbstraction()
        return rwUnderlying;
    }

    KVStore* getROUnderlying() {
        // This method might also be called leakAbstraction()
        return roUnderlying;
    }

    KVStore* getTapUnderlying() {
        // This method might also be called leakAbstraction()
        return tapUnderlying;
    }

    void deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &);

    /**
     * Get the memoized storage properties from the DB.kv
     */
    const StorageProperties getStorageProperties() const {
        return storageProperties;
    }

    void scheduleVBSnapshot(const Priority &priority);

    const VBucketMap &getVBuckets() {
        return vbuckets;
    }

    EventuallyPersistentEngine& getEPEngine() {
        return engine;
    }

    size_t getExpiryPagerSleeptime(void) {
        LockHolder lh(expiryPager.mutex);
        return expiryPager.sleeptime;
    }

    /**
     * During restore from backup we read the most recent values first
     * and works our way back until epoch.. We should therefore only
     * add values to the backup if they're not there;
     *
     * @return 0 success, 1 skipped, -1 invalid vbucket
     */
    int restoreItem(const Item &itm, enum queue_operation op);

    bool isFlushAllScheduled() {
        return diskFlushAll.get();
    }

    void setItemExpiryWindow(size_t value) {
        itemExpiryWindow = value;
    }

    void setVbDelChunkSize(size_t value) {
        vbDelChunkSize = value;
    }

    void setVbChunkDelThresholdTime(size_t value) {
        vbChunkDelThresholdTime = value;
    }

    void setExpiryPagerSleeptime(size_t val);
    void setAccessScannerSleeptime(size_t val);

    /**
     * Complete the degraded mode phase by clearing the list of deleted items that
     * are received from the upstream master via TAP or from the normal clients
     */
    void completeDegradedMode();


    /**
     * Get access to the mutation log.
     */
    const MutationLog *getMutationLog() const { return &mutationLog; }

    /**
     * Get the config of the mutation log compactor.
     */
    MutationLogCompactorConfig &getMutationLogCompactorConfig() {
        return mlogCompactorConfig;
    }

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;
    void maybeEnableTraffic(void);

    // Methods called during warmup
    std::map<uint16_t, vbucket_state> loadVBucketState();
    void loadSessionStats();

    bool warmupFromLog(const std::map<uint16_t, vbucket_state> &state,
                       shared_ptr<Callback<GetValue> >cb);
    void warmupCompleted();

private:

    void scheduleVBDeletion(RCPtr<VBucket> vb,
                            const void* cookie, double delay);

    RCPtr<VBucket> getVBucket(uint16_t vbid, vbucket_state_t wanted_state);

    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key,
                    uint16_t vbid,
                    enum queue_operation op,
                    uint32_t seqno,
                    int64_t rowid,
                    bool tapBackfill = false);

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
     * @param arg the argument to supply to the method f
     *
     * @return true if the object was found and method was invoked
     */
    template<typename A>
    bool invokeOnLockedStoredValue(const std::string &key, uint16_t vbid,
                                   void (StoredValue::* f)(A),
                                   A &arg) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (!vb) {
            return false;
        }

        int bucket_num(0);
        LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true);

        if (v) {
            std::mem_fun(f)(v, arg);
        }
        return v != NULL;
    }

    std::queue<queued_item> *beginFlush();
    void pushToOutgoingQueue();
    void requeueRejectedItems(std::queue<queued_item> *rejects);
    void completeFlush(rel_time_t flush_start);

    void enqueueCommit();
    int flushSome(std::queue<queued_item> *q,
                  std::queue<queued_item> *rejectQueue);
    int flushOne(std::queue<queued_item> *q,
                 std::queue<queued_item> *rejectQueue);
    int flushOneDeleteAll(void);
    int flushOneDelOrSet(const queued_item &qi, std::queue<queued_item> *rejectQueue);

    StoredValue *fetchValidValue(RCPtr<VBucket> vb, const std::string &key,
                                 int bucket_num, bool wantsDeleted=false, bool trackReference=true);

    bool shouldPreemptFlush(size_t completed) {
        return (completed > 100
                && bgFetchQueue > 0
                && !hasSeparateRODispatcher());
    }

    size_t getWriteQueueSize(void);

    bool hasItemsForPersistence(void);

    GetValue getInternal(const std::string &key, uint16_t vbucket,
                         const void *cookie, bool queueBG,
                         bool honorStates,
                         vbucket_state_t allowedState,
                         bool trackReference=true);

    /**
     * Process NEED_METADATA response from hash table set call in order to figure
     * out if an item's meta data should be fetched from disk or not.
     */
    ENGINE_ERROR_CODE processNeedMetaData(const RCPtr<VBucket> &vb,
                                          const Item &itm,
                                          const void *cookie);

    friend class Warmup;
    friend class Flusher;
    friend class BGFetchCallback;
    friend class VKeyStatBGFetchCallback;
    friend class TapBGFetchCallback;
    friend class TapConnection;
    friend class PersistenceCallback;
    friend class Deleter;
    friend class VBCBAdaptor;

    EventuallyPersistentEngine     &engine;
    EPStats                        &stats;
    bool                            doPersistence;
    KVStore                        *rwUnderlying;
    KVStore                        *roUnderlying;
    KVStore                        *tapUnderlying;
    StorageProperties               storageProperties;
    Dispatcher                     *dispatcher;
    Dispatcher                     *roDispatcher;
    Dispatcher                     *tapDispatcher;
    Dispatcher                     *nonIODispatcher;
    Flusher                        *flusher;
    Warmup                         *warmupTask;
    VBucketMap                      vbuckets;
    SyncObject                      mutex;

    MutationLog                     mutationLog;
    MutationLogCompactorConfig      mlogCompactorConfig;
    MutationLog                     accessLog;

    // The writing queue is used by the flusher thread to keep
    // track of the objects it works on. It should _not_ be used
    // by any other threads (because the flusher use it without
    // locking...
    std::queue<queued_item>              writing;
    std::vector<queued_item>            *dbShardQueues;
    std::map<uint16_t, vbucket_state_t>  flusherCachedVbStates;
    pthread_t                            thread;
    Atomic<size_t>                       bgFetchQueue;
    Atomic<bool>                         diskFlushAll;
    TransactionContext                   tctx;
    Mutex                                vbsetMutex;
    uint32_t                             bgFetchDelay;
    // During restore we're bypassing the checkpoint lists with the
    // objects we're restoring, but we need them to be persisted.
    // This is solved by using a separate list for those objects.
    struct {
        Mutex mutex;
        std::map<uint16_t, std::vector<queued_item> > items;
        // Maintain the list of deleted keys that are received from the upstream
        // master via TAP or from the normal clients during online restore.
        // As an alternative to std::set, we can consider boost::unordered_set later.
        std::set<std::string> itemsDeleted;
    } restore;
    struct ExpiryPagerDelta {
        ExpiryPagerDelta() : sleeptime(0) {}
        Mutex mutex;
        size_t sleeptime;
        TaskId task;
    } expiryPager;
    struct ALogTask {
        ALogTask() : sleeptime(0) {}
        Mutex mutex;
        size_t sleeptime;
        TaskId task;
    } accessScanner;
    size_t itemExpiryWindow;
    size_t vbDelChunkSize;
    size_t vbChunkDelThresholdTime;

    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};

/**
 * Object whose existence maintains a counter incremented.
 *
 * When the object is constructed, it increments the given counter,
 * when destructed, it decrements the counter.
 */
class BGFetchCounter {
public:

    BGFetchCounter(Atomic<size_t> &c) : counter(c) {
        ++counter;
    }

    ~BGFetchCounter() {
        --counter;
        assert(counter.get() < GIGANTOR);
    }

private:
    Atomic<size_t> &counter;
};


#endif /* EP_HH */
