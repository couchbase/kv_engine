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

#include "kvstore.hh"
#include "locks.hh"
#include "sqlite-kvstore.hh"
#include "stored-value.hh"

extern EXTENSION_LOGGER_DESCRIPTOR *getLogger(void);

#define DEFAULT_TXN_SIZE 500000
#define DEFAULT_MIN_DATA_AGE 120
#define DEFAULT_MIN_DATA_AGE_CAP 900

#define MAX_DATA_AGE_PARAM 86400

extern "C" {
    extern rel_time_t (*ep_current_time)();
}

struct ep_stats {
    // How long it took us to load the data from disk.
    time_t warmupTime;
    // Whether we're warming up.
    bool warmupComplete;
    // Number of records warmed up.
    size_t warmedUp;
    // size of the input queue
    size_t queue_size;
    // Size of the in-process (output) queue.
    size_t flusher_todo;
    // Objects that were rejected from persistence for being too fresh.
    size_t tooYoung;
    // Objects that were forced into persistence for being too old.
    size_t tooOld;
    // How long an object is dirty before written.
    rel_time_t dirtyAge;
    rel_time_t dirtyAgeHighWat;
    // How old persisted data was when it hit the persistence layer
    rel_time_t dataAge;
    rel_time_t dataAgeHighWat;
    // How long does it take to do an entire flush cycle.
    rel_time_t flushDuration;
    rel_time_t flushDurationHighWat;
    // Amount of time spent in the commit phase.
    rel_time_t commit_time;
    // Total number of items; this would be total_items if we recycled
    // items, but we don't right now.
    uint32_t curr_items;
    // Beyond this point are config items
    // Minimum data age before a record can be persisted
    uint32_t min_data_age;
    // Maximum data age before a record is forced to be persisted
    uint32_t queue_age_cap;
};

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

typedef enum {
    STOPPED=0, RUNNING=1, SHUTTING_DOWN=2
} flusher_state;

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

    flusher_state getFlusherState();

    const char *getFlusherStateAsString() {
        static const char * const flusherStates[] = {
            "stopped", "running", "shutting down"
        };
        assert(flusherState >= STOPPED && flusherState <= SHUTTING_DOWN);
        return flusherStates[flusherState];
    }

    virtual void dump(Callback<GetValue>&) {
        throw std::runtime_error("not implemented");
    }

    void reset();

    void visit(HashTableVisitor &visitor) {
        storage.visit(visitor);
    }

    void warmup() {
        static_cast<MultiDBSqlite3*>(underlying)->dump(loadStorageKVPairCallback);
    }

private:
    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key) {
        if (doPersistence) {
            // Assume locked.
            towrite->push(key);
            stats.queue_size++;
            mutex.notify();
        }
    }

    std::queue<std::string> *beginFlush(bool shouldWait);
    void completeFlush(std::queue<std::string> *rejects,
                       rel_time_t flush_start);

    int flushSome(std::queue<std::string> *q, Callback<bool> &cb,
                  std::queue<std::string> *rejectQueue);
    int flushOne(std::queue<std::string> *q, Callback<bool> &cb,
                  std::queue<std::string> *rejectQueue);
    void flusherStopped();
    void initQueue();

    friend class Flusher;
    bool doPersistence;
    KVStore                   *underlying;
    size_t                     est_size;
    Flusher                   *flusher;
    HashTable                  storage;
    SyncObject                 mutex;
    std::queue<std::string>   *towrite;
    pthread_t                  thread;
    struct ep_stats            stats;
    LoadStorageKVPairCallback  loadStorageKVPairCallback;
    flusher_state              flusherState;
    int                        txnSize;
    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};


#endif /* EP_HH */
