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
#include <stdbool.h>
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

extern EXTENSION_LOGGER_DESCRIPTOR *getLogger(void);

#include "stats.hh"
#include "locks.hh"
#include "sqlite-kvstore.hh"
#include "stored-value.hh"
#include "atomic.hh"
#include "dispatcher.hh"
#include "vbucket.hh"

#define DEFAULT_TXN_SIZE 50000
#define MAX_TXN_SIZE 10000000

#define MAX_DATA_AGE_PARAM 86400
#define MAX_BG_FETCH_DELAY 900

extern "C" {
    extern rel_time_t (*ep_current_time)();
    extern time_t (*ep_abs_time)(rel_time_t);
    extern time_t ep_real_time();
}

enum queue_operation {
    queue_op_set,
    queue_op_del,
    queue_op_flush
};

class QueuedItem {
public:
    QueuedItem(const std::string &k, const uint16_t vb, enum queue_operation o)
        : key(k), op(o), vbucket(vb), dirtied(ep_current_time()) {}

    std::string getKey(void) const { return key; }
    uint16_t getVBucketId(void) const { return vbucket; }
    rel_time_t getDirtied(void) const { return dirtied; }
    enum queue_operation getOperation(void) const { return op; }

    bool operator <(const QueuedItem &other) const {
        return vbucket == other.vbucket ? key < other.key : vbucket < other.vbucket;
    }

    size_t size() const {
        return sizeof(QueuedItem) + key.size();
    }

private:
    std::string key;
    enum queue_operation op;
    uint16_t vbucket;
    rel_time_t dirtied;
};

/**
 * vbucket-aware hashtable visitor.
 */
class VBucketVisitor : public HashTableVisitor {
public:

    VBucketVisitor() : HashTableVisitor() { }

    /**
     * Begin visiting a bucket.
     *
     * @param vbid the vbucket we are beginning to visit
     *
     * @return true iff we want to walk the hashtable in this vbucket
     */
    virtual bool visitBucket(RCPtr<VBucket> vb) {
        currentBucket = vb;
        return true;
    }

protected:
    RCPtr<VBucket> currentBucket;
};

// Forward declaration
class Flusher;

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public Callback<GetValue> {
public:
    LoadStorageKVPairCallback(VBucketMap &vb, EPStats &st)
        : vbuckets(vb), stats(st), hasPurged(false) { }

    void initVBucket(uint16_t vbid, vbucket_state_t state = pending);
    void callback(GetValue &val);

private:

    bool shouldBeResident() {
        return StoredValue::getCurrentSize(stats) < stats.mem_low_wat;
    }

    void purge();

    VBucketMap &vbuckets;
    EPStats    &stats;
    bool        hasPurged;
};

class EventuallyPersistentEngine;

class EventuallyPersistentStore {
public:

    EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                              StrategicSqlite3 *t, bool startVb0);

    ~EventuallyPersistentStore();

    ENGINE_ERROR_CODE set(const Item &item,
                          const void *cookie,
                          bool force=false);

    ENGINE_ERROR_CODE add(const Item &item, const void *cookie);

    /**
     * Retrieve a value.
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param core the server API
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param honorStates if false, fetch a result regardless of state
     *
     * @return a GetValue representing the result of the request
     */
    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie, bool queueBG=true,
                 bool honorStates=true);

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
                                             const char **msg);

    ENGINE_ERROR_CODE del(const std::string &key, uint16_t vbucket,
                          const void *cookie);

    void reset();

    void setMinDataAge(int to);

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

    void setQueueAgeCap(int to);

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
     * @param the key to be bg fetched
     * @param vbucket the vbucket in which the key lives
     * @param cookie the cookie of the requestor
     */
    void bgFetch(const std::string &key,
                 uint16_t vbucket,
                 uint64_t rowid,
                 const void *cookie);

    /**
     * Complete a background fetch.
     *
     * @param key the key that was fetched
     * @param vbucket the vbucket in which the key lived
     * @param gv the result
     */
    void completeBGFetch(const std::string &key,
                         uint16_t vbucket,
                         uint64_t rowid,
                         const void *cookie,
                         hrtime_t init, hrtime_t start);

    RCPtr<VBucket> getVBucket(uint16_t vbid);

    void completeSetVBState(uint16_t vbid,
                            const std::string &key);
    void setVBucketState(uint16_t vbid,
                         vbucket_state_t state);

    void completeVBucketDeletion(uint16_t vbid);
    bool deleteVBucket(uint16_t vbid);

    void visit(VBucketVisitor &visitor) {
        size_t maxSize = vbuckets.getSize();
        for (size_t i = 0; i <= maxSize; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (vb) {
                bool wantData = visitor.visitBucket(vb);
                // We could've lost this along the way.
                if (wantData) {
                    vb->ht.visit(visitor);
                }
            }
        }
    }

    void visitDepth(HashTableDepthVisitor &visitor) {
        // TODO: Something smarter for multiple vbuckets.
        RCPtr<VBucket> vb = vbuckets.getBucket(0);
        assert(vb);
        vb->ht.visitDepth(visitor);
    }

    size_t getHashSize() {
        // TODO: Something smarter for multiple vbuckets.
        RCPtr<VBucket> vb = vbuckets.getBucket(0);
        assert(vb);
        return vb->ht.getSize();
    }

    size_t getHashLocks() {
        // TODO: Something smarter for multiple vbuckets.
        RCPtr<VBucket> vb = vbuckets.getBucket(0);
        assert(vb);
        return vb->ht.getNumLocks();
    }

    void warmup() {
        LoadStorageKVPairCallback cb(vbuckets, stats);
        std::map<uint16_t, std::string> state = underlying->listPersistedVbuckets();
        std::map<uint16_t, std::string>::iterator it;
        for (it = state.begin(); it != state.end(); ++it) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Reloading vbucket %d - was in %s state\n",
                             it->first, it->second.c_str());
            cb.initVBucket(it->first);
        }
        underlying->dump(cb);
    }

    int getTxnSize() {
        return txnSize.get();
    }

    void setTxnSize(int to) {
        txnSize.set(to);
    }

    const Flusher* getFlusher();

    bool getKeyStats(const std::string &key, uint16_t vbucket,
                     key_stats &kstats);

    bool getLocked(const std::string &key, uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime, uint32_t lockTimeout);

    StrategicSqlite3* getUnderlying() {
        // This method might also be called leakAbstraction()
        return underlying;
    }

    void deleteMany(std::list<std::pair<uint16_t, std::string> > &);

private:

    RCPtr<VBucket> getVBucket(uint16_t vbid, vbucket_state_t wanted_state);

    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key, uint16_t vbid, enum queue_operation op);

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
     * @param f the method to invoke on the item (see std::mem_fun)
     * @param arg the argument to supply to the method f
     */
    template<typename A>
    void invokeOnLockedStoredValue(const std::string &key, uint16_t vbid,
                                   std::mem_fun1_t<void, StoredValue, A> f,
                                   A arg) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (!vb) {
            return;
        }

        int bucket_num = vb->ht.bucket(key);
        LockHolder lh(vb->ht.getMutex(bucket_num));
        StoredValue *v = fetchValidValue(vb, key, bucket_num);

        if (v) {
            f(v, arg);
        }
    }

    std::queue<QueuedItem> *beginFlush();
    void completeFlush(std::queue<QueuedItem> *rejects,
                       rel_time_t flush_start);

    int flushSome(std::queue<QueuedItem> *q,
                  std::queue<QueuedItem> *rejectQueue);
    int flushOne(std::queue<QueuedItem> *q,
                 std::queue<QueuedItem> *rejectQueue);
    int flushOneDeleteAll(void);
    int flushOneDelOrSet(QueuedItem &qi, std::queue<QueuedItem> *rejectQueue);

    StoredValue *fetchValidValue(RCPtr<VBucket> vb, const std::string &key,
                                 int bucket_num, bool wantsDeleted=false);

    friend class Flusher;
    friend class BGFetchCallback;
    friend class VKeyStatBGFetchCallback;
    friend class TapConnection;
    friend class PersistenceCallback;
    friend class Deleter;

    EventuallyPersistentEngine &engine;
    EPStats                    &stats;
    bool                       doPersistence;
    StrategicSqlite3          *underlying;
    Dispatcher                *dispatcher;
    Dispatcher                *nonIODispatcher;
    Flusher                   *flusher;
    VBucketMap                 vbuckets;
    SyncObject                 mutex;
    AtomicQueue<QueuedItem>    towrite;
    std::queue<QueuedItem>     writing;
    pthread_t                  thread;
    Atomic<int>                txnSize;
    Atomic<size_t>             bgFetchQueue;
    Mutex                      vbsetMutex;
    uint32_t                   bgFetchDelay;

    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};


#endif /* EP_HH */
