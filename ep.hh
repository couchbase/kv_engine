/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef EP_HH
#define EP_HH 1

#include "config.h"

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

#include "stats.hh"
#include "kvstore.hh"
#include "locks.hh"
#include "sqlite-kvstore.hh"
#include "stored-value.hh"
#include "atomic.hh"

extern EXTENSION_LOGGER_DESCRIPTOR *getLogger(void);

#define DEFAULT_TXN_SIZE 50000
#define MAX_TXN_SIZE 10000000

#define DEFAULT_MIN_DATA_AGE 120
#define DEFAULT_MIN_DATA_AGE_CAP 900

#define MAX_DATA_AGE_PARAM 86400

extern "C" {
    extern rel_time_t (*ep_current_time)();
}

// Forward declaration
class Flusher;

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public Callback<GetValue> {
public:
    LoadStorageKVPairCallback(HashTable &ht, struct ep_stats &st)
        : hashtable(ht), stats(st) { }

    void callback(GetValue &val) {
        Item *i = val.getValue();
        if (i != NULL) {
            hashtable.add(*i, true);
            delete i;
        }
        stats.warmedUp++;
    }

private:
    HashTable       &hashtable;
    struct ep_stats &stats;
};

class EventuallyPersistentStore : public KVStore {
public:

    EventuallyPersistentStore(KVStore *t, size_t est=32768);

    ~EventuallyPersistentStore();

    void set(const Item &item, Callback<bool> &cb);

    void get(const std::string &key, Callback<GetValue> &cb);

    void del(const std::string &key, Callback<bool> &cb);

    void getStats(struct ep_stats *out);

    void setMinDataAge(int to);

    void setQueueAgeCap(int to);

    void resetStats(void);

    void stopFlusher(void);

    void startFlusher(void);

    bool pauseFlusher(void);
    bool resumeFlusher(void);

    virtual void dump(Callback<GetValue>&) {
        throw std::runtime_error("not implemented");
    }

    void visit(HashTableVisitor &visitor) {
        storage.visit(visitor);
    }

    void visitDepth(HashTableDepthVisitor &visitor) {
        storage.visitDepth(visitor);
    }

    void warmup() {
        static_cast<StrategicSqlite3*>(underlying)->dump(loadStorageKVPairCallback);
    }

    int getTxnSize() {
        LockHolder lh(mutex);
        return txnSize;
    }

    void setTxnSize(int to) {
        LockHolder lh(mutex);
        txnSize = to;
    }

    void setTapStats(size_t depth, size_t fetched) {
        LockHolder lh(mutex);
        stats.tap_queue = depth;
        stats.tap_fetched = fetched;
    }

    const Flusher* getFlusher();

    bool getKeyStats(const std::string &key, key_stats &kstats);

private:
    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key) {
        if (doPersistence) {
            // Assume locked.
            towrite.push(key);
            totalEnqueued.incr();
            mutex.notify();
        }
    }

    std::queue<std::string> *beginFlush(bool shouldWait);
    void completeFlush(std::queue<std::string> *rejects,
                       rel_time_t flush_start);

    int flushSome(std::queue<std::string> *q,
                  std::queue<std::string> *rejectQueue);
    int flushOne(std::queue<std::string> *q,
                  std::queue<std::string> *rejectQueue);

    friend class Flusher;
    bool doPersistence;
    KVStore                   *underlying;
    size_t                     est_size;
    Flusher                   *flusher;
    HashTable                  storage;
    SyncObject                 mutex;
    AtomicQueue<std::string>   towrite;
    std::queue<std::string>    writing;
    pthread_t                  thread;
    struct ep_stats            stats;
    Atomic<size_t>     totalEnqueued;
    Atomic<size_t>     curr_items;
    LoadStorageKVPairCallback  loadStorageKVPairCallback;
    int                        txnSize;
    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};


#endif /* EP_HH */
