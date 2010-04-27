/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "ep.hh"
#include "flusher.hh"
#include "locks.hh"

#include <vector>
#include <time.h>
#include <string.h>

extern "C" {
    static void* launch_flusher_thread(void* arg) {
        Flusher *flusher = (Flusher*) arg;
        try {
            flusher->run();
        } catch (std::exception& e) {
            std::cerr << "flusher exception caught: " << e.what() << std::endl;
        } catch(...) {
            std::cerr << "Caught a fatal exception in the flusher thread" << std::endl;
        }
        return NULL;
    }

    static rel_time_t uninitialized_current_time(void) {
        abort();
        return 0;
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
}

EventuallyPersistentStore::EventuallyPersistentStore(KVStore *t,
                                                     size_t est) :
    loadStorageKVPairCallback(storage, stats)
{
    est_size = est;
    towrite = NULL;
    memset(&stats, 0, sizeof(stats));
    stats.min_data_age = DEFAULT_MIN_DATA_AGE;
    stats.queue_age_cap = DEFAULT_MIN_DATA_AGE_CAP;
    initQueue();

    doPersistence = getenv("EP_NO_PERSITENCE") == NULL;
    flusher = new Flusher(this);

    txnSize = DEFAULT_TXN_SIZE;

    underlying = t;

    startFlusher();
    assert(underlying);
}

class VerifyStoredVisitor : public HashTableVisitor {
public:
    std::vector<std::string> dirty;
    virtual void visit(StoredValue *v) {
        if (v->isDirty()) {
            dirty.push_back(v->getKey());
        }
    }
};

EventuallyPersistentStore::~EventuallyPersistentStore() {
    stopFlusher();
    if (flusher != NULL) {
        pthread_join(thread, NULL);
    }

    // Verify that we don't have any dirty objects!
    if (getenv("EP_VERIFY_SHUTDOWN_FLUSH") != NULL) {
        VerifyStoredVisitor walker;
        storage.visit(walker);
        if (!walker.dirty.empty()) {
            std::vector<std::string>::const_iterator iter;
            for (iter = walker.dirty.begin();
                 iter != walker.dirty.end();
                 ++iter) {
                std::cerr << "ERROR: Object dirty after flushing: "
                          << iter->c_str() << std::endl;
            }

            throw std::runtime_error("Internal error, objects dirty objects exists");
        }
    }

    delete flusher;
    delete towrite;
}

const Flusher* EventuallyPersistentStore::getFlusher() {
    return flusher;
}

void EventuallyPersistentStore::startFlusher() {
    LockHolder lh(mutex);

    // Run in a thread...
    if(pthread_create(&thread, NULL, launch_flusher_thread, flusher)
       != 0) {
        throw std::runtime_error("Error initializing queue thread");
    }
    mutex.notify();
}

void EventuallyPersistentStore::stopFlusher() {
    LockHolder lh(mutex);
    flusher->stop();
    mutex.notify();
}

bool EventuallyPersistentStore::pauseFlusher() {
    LockHolder lh(mutex);
    flusher->pause();
    mutex.notify();
    return true;
}

bool EventuallyPersistentStore::resumeFlusher() {
    LockHolder lh(mutex);
    flusher->resume();
    mutex.notify();
    return true;
}

void EventuallyPersistentStore::initQueue() {
    assert(!towrite);
    stats.queue_size = 0;
    towrite = new std::queue<std::string>;
}

void EventuallyPersistentStore::set(const Item &item, Callback<bool> &cb) {
    mutation_type_t mtype = storage.set(item);
    bool rv = true;

    if (mtype == INVALID_CAS) {
        rv = false;
    } else if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        LockHolder lh(mutex);
        queueDirty(item.getKey());
        if (mtype == NOT_FOUND) {
            ++stats.curr_items;
        }
    }
    cb.callback(rv);
}

void EventuallyPersistentStore::reset() {
    // flush(false); // XXX:  I think reset may not be used.
    LockHolder lh(mutex);
    underlying->reset();
    delete towrite;
    towrite = NULL;
    memset(&stats, 0, sizeof(stats));
    initQueue();
    storage.clear();
}

void EventuallyPersistentStore::get(const std::string &key,
                                    Callback<GetValue> &cb) {
    int bucket_num = storage.bucket(key);
    LockHolder lh(storage.getMutex(bucket_num));
    StoredValue *v = storage.unlocked_find(key, bucket_num);

    if (v) {
        GetValue rv(new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(), v->getCas()));
        cb.callback(rv);
    } else {
        GetValue rv(false);
        cb.callback(rv);
    }
    lh.unlock();
}

void EventuallyPersistentStore::getStats(struct ep_stats *out) {
    LockHolder lh(mutex);
    *out = stats;
}

void EventuallyPersistentStore::setMinDataAge(int to) {
    LockHolder lh(mutex);
    stats.min_data_age = to;
}

void EventuallyPersistentStore::setQueueAgeCap(int to) {
    LockHolder lh(mutex);
    stats.queue_age_cap = to;
}

void EventuallyPersistentStore::resetStats(void) {
    LockHolder lh(mutex);
    stats.tooYoung = 0;
    stats.tooOld = 0;
    stats.dirtyAge = 0;
    stats.dirtyAgeHighWat = 0;
    stats.flushDuration = 0;
    stats.flushDurationHighWat = 0;
    stats.commit_time = 0;
}

void EventuallyPersistentStore::del(const std::string &key, Callback<bool> &cb) {
    bool existed = storage.del(key);
    if (existed) {
        LockHolder lh(mutex);
        queueDirty(key);
        --stats.curr_items;
    }
    cb.callback(existed);
}

std::queue<std::string>* EventuallyPersistentStore::beginFlush(bool shouldWait) {
    LockHolder lh(mutex);
    std::queue<std::string>* rv = NULL;

    if (towrite->empty()) {
        stats.dirtyAge = 0;
        if (shouldWait) {
            mutex.wait();
        }
    } else {
        rv = towrite;
        towrite = NULL;
        initQueue();
        lh.unlock();

        assert(underlying);

        stats.flusher_todo = rv->size();
    }
    return rv;
}

void EventuallyPersistentStore::completeFlush(std::queue<std::string> *rej,
                                              rel_time_t flush_start) {
    // Requeue the rejects.
    LockHolder lh(mutex);
    while (!rej->empty()) {
        std::string key = rej->front();
        rej->pop();
        towrite->push(key);
        stats.queue_size++;
    }

    lh.unlock();

    rel_time_t complete_time = ep_current_time();
    stats.flushDuration = complete_time - flush_start;
    stats.flushDurationHighWat = stats.flushDuration > stats.flushDurationHighWat
                               ? stats.flushDuration : stats.flushDurationHighWat;
}

int EventuallyPersistentStore::flushSome(std::queue<std::string> *q,
                                         Callback<bool> &cb,
                                         std::queue<std::string> *rejectQueue) {
    underlying->begin();
    int oldest = stats.min_data_age;
    for (int i = 0; i < txnSize && !q->empty(); i++) {
        int n = flushOne(q, cb, rejectQueue);
        if (n != 0 && n < oldest) {
            oldest = n;
        }
    }
    rel_time_t cstart = ep_current_time();
    underlying->commit();
    rel_time_t complete_time = ep_current_time();
    // One more lock to update a stat.
    LockHolder lh_stat(mutex);
    stats.commit_time = complete_time - cstart;

    return oldest;
}

int EventuallyPersistentStore::flushOne(std::queue<std::string> *q,
                                        Callback<bool> &cb,
                                        std::queue<std::string> *rejectQueue) {

    std::string key = q->front();
    q->pop();

    int bucket_num = storage.bucket(key);
    LockHolder lh(storage.getMutex(bucket_num));
    StoredValue *v = storage.unlocked_find(key, bucket_num);

    bool found = v != NULL;
    bool isDirty = (found && v->isDirty());
    Item *val = NULL;

    int ret = 0;

    if (isDirty) {
        rel_time_t queued, dirtied;
        v->markClean(&queued, &dirtied);
        assert(dirtied > 0);
        // Calculate stats if this had a positive time.
        rel_time_t now = ep_current_time();
        int dataAge = now - dirtied;
        int dirtyAge = now - queued;
        bool eligible = true;

        if (dirtyAge > (int)stats.queue_age_cap) {
            stats.tooOld++;
        } else if (dataAge < (int)stats.min_data_age) {
            eligible = false;
            // Skip this one.  It's too young.
            ret = stats.min_data_age - dataAge;
            isDirty = false;
            stats.tooYoung++;
            v->reDirty(queued, dirtied);
            rejectQueue->push(key);
        }

        if (eligible) {
            stats.dirtyAge = dirtyAge;
            stats.dataAge = dataAge;
            assert(stats.dirtyAge < (86400 * 30));
            assert(stats.dataAge <= stats.dirtyAge);
            stats.dirtyAgeHighWat = stats.dirtyAge > stats.dirtyAgeHighWat
                ? stats.dirtyAge : stats.dirtyAgeHighWat;
            stats.dataAgeHighWat = stats.dataAge > stats.dataAgeHighWat
                ? stats.dataAge : stats.dataAgeHighWat;
            // Copy it for the duration.
            val = new Item(key, v->getFlags(), v->getExptime(), v->getValue(),
                           v->getCas());
        }
    }
    stats.flusher_todo--;
    lh.unlock();

    if (found && isDirty) {
        underlying->set(*val, cb);
    } else if (!found) {
        underlying->del(key, cb);
    }

    if (val != NULL) {
        delete val;
    }

    return ret;
}
