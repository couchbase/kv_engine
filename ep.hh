/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "kvstore.hh"
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

class QueuedItem {
public:
    QueuedItem(const std::string &k, const uint16_t vb) : key(k), vbucket(vb) {}

    std::string getKey(void) const { return key; }
    uint16_t getVBucketId(void) const { return vbucket; }

    bool operator <(const QueuedItem &other) const {
        return vbucket == other.vbucket ? key < other.key : vbucket < other.vbucket;
    }

private:
    std::string key;
    uint16_t vbucket;
};

class VBucketVisitor : public HashTableVisitor {
public:

    VBucketVisitor() : HashTableVisitor(), currentBucket(0) { }

    void setCurrentBucket(uint16_t to) {
        currentBucket = to;
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

    void callback(GetValue &val) {
        Item *i = val.getValue();
        if (i != NULL) {
            // TODO: Something smarter for multiple vbuckets.
            RCPtr<VBucket> vb = vbuckets.getBucket(0);
            assert(vb);
            vb->ht.add(*i, true);
            delete i;
        }
        stats.warmedUp++;
    }

private:
    VBucketMap &vbuckets;
    EPStats    &stats;
};

class EventuallyPersistentStore : public KVStore {
public:

    EventuallyPersistentStore(KVStore *t, size_t est=32768);

    ~EventuallyPersistentStore();

    void set(const Item &item, Callback<std::pair<bool, int64_t> > &cb);

    void get(const std::string &key, uint16_t vbucket,
             Callback<GetValue> &cb);

    void getFromUnderlying(const std::string &key, uint16_t vbucket,
                           shared_ptr<Callback<GetValue> > cb);

    void del(const std::string &key, uint16_t vbucket,
             Callback<bool> &cb);

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
    void setVBucketState(uint16_t vbid, vbucket_state_t state);

    virtual void dump(Callback<GetValue>&) {
        throw std::runtime_error("not implemented");
    }

    void visit(VBucketVisitor &visitor) {
        std::vector<int> vbucketIds(vbuckets.getBuckets());
        std::vector<int>::iterator it;
        for (it = vbucketIds.begin(); it != vbucketIds.end(); ++it) {
            int vbid = *it;
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            visitor.setCurrentBucket(vbid);
            // We could've lost this along the way.
            if (vb && vb->getState() != dead) {
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
        static_cast<StrategicSqlite3*>(underlying)->dump(loadStorageKVPairCallback);
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
    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key, uint16_t vbid);

    std::queue<QueuedItem> *beginFlush();
    void completeFlush(std::queue<QueuedItem> *rejects,
                       rel_time_t flush_start);

    int flushSome(std::queue<QueuedItem> *q,
                  std::queue<QueuedItem> *rejectQueue);
    int flushOne(std::queue<QueuedItem> *q,
                  std::queue<QueuedItem> *rejectQueue);

    friend class Flusher;
    bool                       doPersistence;
    KVStore                   *underlying;
    size_t                     est_size;
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
