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

#include <set>
#include <queue>

#include <memcached/engine.h>

#include "kvstore.hh"
#include "locks.hh"
#include "sqlite-kvstore.hh"

#define DEFAULT_TXN_SIZE 100000

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
};

// Forward declaration for StoredValue
class HashTable;

class StoredValue {
public:
    StoredValue() : key(), value(), flags(0), exptime(0), dirtied(0), next(NULL) {
    }
    StoredValue(std::string &k, const char *v, size_t nv, StoredValue *n) :
        key(k), value(), flags(0), exptime(0), dirtied(0), next(n)
    {
        setValue(v, nv);
    }

    StoredValue(const Item &itm, StoredValue *n) :
        key(itm.getKey()), value(const_cast<Item&>(itm).getData(), itm.nbytes), flags(itm.flags),
        exptime(itm.exptime), dirtied(0), next(n)
    {
        markDirty();
    }

    StoredValue(const Item &itm, StoredValue *n, bool setDirty) :
        key(itm.getKey()), value(const_cast<Item&>(itm).getData(), itm.nbytes), flags(itm.flags),
        exptime(itm.exptime), dirtied(0), next(n)
    {
        if (setDirty) {
            markDirty();
        } else {
            markClean(NULL, NULL);
        }
    }

    ~StoredValue() {
    }
    void markDirty() {
        data_age = ep_current_time();
        if (!isDirty()) {
            dirtied = data_age;
        }
    }
    // returns time this object was dirtied.
    void markClean(rel_time_t *dirtyAge, rel_time_t *dataAge) {
        if (dirtyAge) {
            *dirtyAge = dirtied;
        }
        if (dataAge) {
            *dataAge = data_age;
        }
        dirtied = 0;
        data_age = 0;
    }

    bool isDirty() const {
        return dirtied != 0;
    }

    bool isClean() const {
        return dirtied == 0;
    }

    const std::string &getKey() const {
        return key;
    }

    const std::string &getValue() const {
        return value;
    }

    rel_time_t getExptime() const {
        return exptime;
    }

    uint32_t getFlags() const {
        return flags;
    }

    void setValue(const char *v, const size_t nv) {
        value.assign(v, nv);
        markDirty();
    }

private:

    friend class HashTable;

    std::string key;
    std::string value;
    uint32_t flags;
    rel_time_t exptime;
    rel_time_t dirtied;
    rel_time_t data_age;
    StoredValue *next;
    DISALLOW_COPY_AND_ASSIGN(StoredValue);
};

typedef enum {
    NOT_FOUND, WAS_CLEAN, WAS_DIRTY
} mutation_type_t;

class HashTableVisitor {
public:
    virtual ~HashTableVisitor() {}
    virtual void visit(StoredValue *v) = 0;
};

class HashTable {
public:

    // Construct with number of buckets and locks.
    HashTable(size_t s = 196613, size_t l = 193) {
        size = s;
        n_locks = l;
        active = true;
        values = (StoredValue**)calloc(s, sizeof(StoredValue**));
        mutexes = new Mutex[l];
    }

    ~HashTable() {
        clear();
        delete []mutexes;
        free(values);
    }

    void clear() {
        assert(active);
        for (int i = 0; i < (int)size; i++) {
            LockHolder lh(getMutex(i));
            while (values[i]) {
                StoredValue *v = values[i];
                values[i] = v->next;
                delete v;
            }
        }
    }

    StoredValue *find(std::string &key) {
        assert(active);
        int bucket_num = bucket(key);
        LockHolder lh(getMutex(bucket_num));
        return unlocked_find(key, bucket_num);
    }

    mutation_type_t set(const Item &val) {
        assert(active);
        mutation_type_t rv = NOT_FOUND;
        int bucket_num = bucket(val.getKey());
        LockHolder lh(getMutex(bucket_num));
        StoredValue *v = unlocked_find(val.getKey(), bucket_num);
        Item &itm = const_cast<Item&>(val);
        if (v) {
            rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
            v->setValue(itm.getData(), itm.nbytes);
        } else {
            v = new StoredValue(itm, values[bucket_num]);
            values[bucket_num] = v;
        }
        return rv;
    }

    bool add(const Item &val, bool isDirty) {
        assert(active);
        int bucket_num = bucket(val.getKey());
        LockHolder lh(getMutex(bucket_num));
        StoredValue *v = unlocked_find(val.getKey(), bucket_num);
        if (v) {
            return false;
        } else {
            v = new StoredValue(const_cast<Item&>(val), values[bucket_num], isDirty);
            values[bucket_num] = v;
        }

        return true;
    }

    bool add(const Item &val) {
        return add(val, true);
    }

    StoredValue *unlocked_find(const std::string &key, int bucket_num) {
        StoredValue *v = values[bucket_num];
        while (v) {
            if (key.compare(v->key) == 0) {
                return v;
            }
            v = v->next;
        }
        return NULL;
    }

    inline int bucket(const std::string &key) {
        assert(active);
        int h=5381;
        int i=0;
        const char *str = key.c_str();

        for(i=0; str[i] != 0x00; i++) {
            h = ((h << 5) + h) ^ str[i];
        }

        return abs(h) % (int)size;
    }

    // Get the mutex for a bucket (for doing your own lock management)
    inline Mutex &getMutex(int bucket_num) {
        assert(active);
        assert(bucket_num < (int)size);
        assert(bucket_num >= 0);
        int lock_num = bucket_num % (int)n_locks;
        assert(lock_num < (int)n_locks);
        assert(lock_num >= 0);
        return mutexes[lock_num];
    }

    // True if it existed
    bool del(const std::string &key) {
        assert(active);
        int bucket_num = bucket(key);
        LockHolder lh(getMutex(bucket_num));

        StoredValue *v = values[bucket_num];

        // Special case empty bucket.
        if (!v) {
            return false;
        }

        // Special case the first one
        if (key.compare(v->key) == 0) {
            values[bucket_num] = v->next;
            delete v;
            return true;
        }

        while (v->next) {
            if (key.compare(v->next->key) == 0) {
                StoredValue *tmp = v->next;
                v->next = v->next->next;
                delete tmp;
                return true;
            }
        }

        return false;
    }

    void visit(HashTableVisitor &visitor) {
        for (int i = 0; i < (int)size; i++) {
            LockHolder lh(getMutex(i));
            StoredValue *v = values[i];
            while (v) {
                visitor.visit(v);
                v = v->next;
            }
        }
    }

private:
    size_t            size;
    size_t            n_locks;
    bool              active;
    StoredValue     **values;
    Mutex            *mutexes;

    DISALLOW_COPY_AND_ASSIGN(HashTable);
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
        if (val.value != NULL) {
            hashtable.add(*val.value, false);
            delete val.value;
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

    void resetStats(void);

    void stopFlusher(void);

    void startFlusher(void);

    flusher_state getFlusherState();

    virtual void dump(Callback<GetValue>&) {
        throw std::runtime_error("not implemented");
    }

    void reset();

    void visit(HashTableVisitor &visitor) {
        storage.visit(visitor);
    }

    void warmup() {
        static_cast<MultiTableSqlite3*>(underlying)->dump(loadStorageKVPairCallback);
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

    void flush(bool shouldWait);
    void flushSome(std::queue<std::string> *q, Callback<bool> &cb);
    void flushOne(std::queue<std::string> *q, Callback<bool> &cb);
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

class Flusher {
public:
    Flusher(EventuallyPersistentStore *st) {
        store = st;
        running = true;
    }
    ~Flusher() {
        stop();
    }
    void stop() {
        running = false;
    }
    void run() {
        running = true;
        time_t start = time(NULL);
        store->warmup();
        store->stats.warmupTime = time(NULL) - start;
        store->stats.warmupComplete = true;
        try {
            while(running) {
                store->flush(true);
            }
            std::cout << "Shutting down flusher." << std::endl;
            store->flush(false);
            std::cout << "Flusher stopped" << std::endl;

        } catch(std::runtime_error &e) {
            std::cerr << "Exception in executor loop: "
                      << e.what() << std::endl;
            assert(false);
        }
        // Signal our completion.
        store->flusherStopped();
    }
private:
    EventuallyPersistentStore *store;
    volatile bool running;
};

#endif /* EP_HH */
