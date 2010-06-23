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
#include <unistd.h>

#include <set>
#include <queue>

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

#define DEFAULT_MIN_DATA_AGE 120
#define DEFAULT_MIN_DATA_AGE_CAP 900

#define MAX_DATA_AGE_PARAM 86400

extern "C" {
    extern rel_time_t (*ep_current_time)();
}

enum queue_operation {
    queue_op_set,
    queue_op_del,
    queue_op_flush,
    queue_op_vb_flush,
    queue_op_vb_set
};

class QueuedItem {
public:
    QueuedItem(const std::string &k, const uint16_t vb, enum queue_operation o)
        : key(k), op(o), vbucket(vb) {}

    std::string getKey(void) const { return key; }
    uint16_t getVBucketId(void) const { return vbucket; }
    enum queue_operation getOperation(void) const { return op; }

    bool operator <(const QueuedItem &other) const {
        return vbucket == other.vbucket ? key < other.key : vbucket < other.vbucket;
    }

private:
    std::string key;
    enum queue_operation op;
    uint16_t vbucket;
};

/**
 * vbucket-aware hashtable visitor.
 */
class VBucketVisitor : public HashTableVisitor {
public:

    VBucketVisitor() : HashTableVisitor(), currentBucket(0) { }

    /**
     * Begin visiting a bucket.
     *
     * @param vbid the vbucket ID we are beginning to visit
     * @param state the state of this vbucket
     *
     * @return true iff we want to walk the hashtable in this vbucket
     */
    virtual bool visitBucket(uint16_t vbid, vbucket_state_t state) {
        (void)state;
        currentBucket = vbid;
        return true;
    }

protected:
    uint16_t currentBucket;
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
        : vbuckets(vb), stats(st) { }

    void initVBucket(uint16_t vbid, vbucket_state_t state = pending) {
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (!vb) {
            vb.reset(new VBucket(vbid, state));
            vbuckets.addBucket(vb);
        }
    }

    void callback(GetValue &val) {
        Item *i = val.getValue();
        if (i != NULL) {
            RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
            if (!vb) {
                vb.reset(new VBucket(i->getVBucketId(), pending));
                vbuckets.addBucket(vb);
            }
            vb->ht.add(*i, true);
            delete i;
        }
        stats.warmedUp++;
    }

private:
    VBucketMap &vbuckets;
    EPStats    &stats;
};

class EventuallyPersistentStore {
public:

    EventuallyPersistentStore(StrategicSqlite3 *t, bool startVb0);

    ~EventuallyPersistentStore();

    ENGINE_ERROR_CODE set(const Item &item,
                          const void *cookie,
                          bool force=false);

    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie);

    void getFromUnderlying(const std::string &key, uint16_t vbucket,
                           shared_ptr<Callback<GetValue> > cb);

    ENGINE_ERROR_CODE del(const std::string &key, uint16_t vbucket,
                          const void *cookie);

    void reset();

    EPStats& getStats() { return stats; }

    void setMinDataAge(int to);

    void setQueueAgeCap(int to);

    void resetStats(void);

    void startDispatcher(void);

    void stopFlusher(void);

    void startFlusher(void);

    bool pauseFlusher(void);
    bool resumeFlusher(void);

    RCPtr<VBucket> getVBucket(uint16_t vbid);
    void setVBucketState(uint16_t vbid, vbucket_state_t state, SERVER_CORE_API *core);
    bool deleteVBucket(uint16_t vbid);

    void visit(VBucketVisitor &visitor) {
        std::vector<int> vbucketIds(vbuckets.getBuckets());
        std::vector<int>::iterator it;
        for (it = vbucketIds.begin(); it != vbucketIds.end(); ++it) {
            int vbid = *it;
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            bool wantData = visitor.visitBucket(vbid, vb ? vb->getState() : dead);
            // We could've lost this along the way.
            if (wantData) {
                vb->ht.visit(visitor);
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
        std::map<uint16_t, std::string> state = underlying->listPersistedVbuckets();
        std::map<uint16_t, std::string>::iterator it;
        for (it = state.begin(); it != state.end(); ++it) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Reloading vbucket %d - was in %s state\n",
                             it->first, it->second.c_str());
            loadStorageKVPairCallback.initVBucket(it->first);
        }
        underlying->dump(loadStorageKVPairCallback);
    }

    int getTxnSize() {
        return txnSize.get();
    }

    void setTxnSize(int to) {
        txnSize.set(to);
    }

    void setTapStats(size_t depth, size_t fetched) {
        stats.tap_queue.set(depth);
        stats.tap_fetched.set(fetched);
    }

    const Flusher* getFlusher();

    bool getKeyStats(const std::string &key, uint16_t vbucket,
                     key_stats &kstats);

    bool getLocked(const std::string &key, uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime, uint32_t lockTimeout);

private:

    RCPtr<VBucket> getVBucket(uint16_t vbid, vbucket_state_t wanted_state);

    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key, uint16_t vbid, enum queue_operation op);

    std::queue<QueuedItem> *beginFlush();
    void completeFlush(std::queue<QueuedItem> *rejects,
                       rel_time_t flush_start);

    int flushSome(std::queue<QueuedItem> *q,
                  std::queue<QueuedItem> *rejectQueue);
    int flushOne(std::queue<QueuedItem> *q,
                 std::queue<QueuedItem> *rejectQueue);
    int flushOneDeleteAll(void);
    int flushOneDeleteVBucket(QueuedItem &qi, std::queue<QueuedItem> *rejectQueue);
    int flushOneDelOrSet(QueuedItem &qi, std::queue<QueuedItem> *rejectQueue);
    int flushVBSet(QueuedItem &qi, std::queue<QueuedItem> *rejectQueue);

    friend class Flusher;
    bool                       doPersistence;
    StrategicSqlite3          *underlying;
    Dispatcher                *dispatcher;
    Flusher                   *flusher;
    VBucketMap                 vbuckets;
    SyncObject                 mutex;
    AtomicQueue<QueuedItem>    towrite;
    std::queue<QueuedItem>     writing;
    pthread_t                  thread;
    EPStats                    stats;
    LoadStorageKVPairCallback  loadStorageKVPairCallback;
    Atomic<int>                txnSize;
    Mutex                      vbsetMutex;
    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};


#endif /* EP_HH */
