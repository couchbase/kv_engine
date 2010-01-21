/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "ep.hh"
#include "locks.hh"

#include <time.h>
#include <string.h>

extern "C" {
    static void* launch_flusher_thread(void* arg) {
        Flusher *flusher = (Flusher*) arg;
        try {
            flusher->run();
        } catch(...) {
            std::cerr << "Caught a fatal exception in the thread" << std::endl;
        }
        return NULL;
    }

    static rel_time_t uninitialized_current_time(void) {
        abort();
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
}

EventuallyPersistentStore::EventuallyPersistentStore(KVStore *t,
                                                     size_t est) :
    loadStorageKVPairCallback(storage)
{
    est_size = est;
    towrite = NULL;
    memset(&stats, 0, sizeof(stats));
    initQueue();

    flusher = new Flusher(this);

    flusherState = STOPPED;
    startFlusher();

    underlying = t;
    assert(underlying);
}

EventuallyPersistentStore::~EventuallyPersistentStore() {
    LockHolder lh(mutex);
    stopFlusher();
    mutex.notify();
    lh.unlock();
    if (flusherState != STOPPED) {
        pthread_join(thread, NULL);
    }
    delete flusher;
    delete towrite;
}

void EventuallyPersistentStore::startFlusher() {
    LockHolder lh(mutex);
    if (flusherState != STOPPED) {
        return;
    }

    // Run in a thread...
    if(pthread_create(&thread, NULL, launch_flusher_thread, flusher)
       != 0) {
        throw std::runtime_error("Error initializing queue thread");
    }
    flusherState = RUNNING;
}

void EventuallyPersistentStore::stopFlusher() {
    LockHolder lh(mutex);
    if (flusherState != RUNNING) {
        return;
    }

    flusherState = SHUTTING_DOWN;
    flusher->stop();
}

flusher_state EventuallyPersistentStore::getFlusherState() {
    LockHolder lh(mutex);
    return flusherState;
}

void EventuallyPersistentStore::initQueue() {
    assert(!towrite);
    stats.queue_size = 0;
    towrite = new std::queue<std::string>;
}

void EventuallyPersistentStore::set(std::string &key, std::string &val,
                                    Callback<bool> &cb) {
    mutation_type_t mtype = storage.set(key, val);

    if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        LockHolder lh(mutex);
        queueDirty(key);
    }
    bool rv = true;
    cb.callback(rv);
}

void EventuallyPersistentStore::set(std::string &key, const char *val,
                                    size_t nbytes,
                                    Callback<bool> &cb) {
    mutation_type_t mtype = storage.set(key, val, nbytes);

    if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        LockHolder lh(mutex);
        queueDirty(key);
    }
    bool rv = true;
    cb.callback(rv);
}

void EventuallyPersistentStore::reset() {
    flush(false);
    LockHolder lh(mutex);
    underlying->reset();
    delete towrite;
    towrite = NULL;
    memset(&stats, 0, sizeof(stats));
    initQueue();
    storage.clear();
}

void EventuallyPersistentStore::get(std::string &key,
                                    Callback<GetValue> &cb) {
    int bucket_num = storage.bucket(key);
    LockHolder lh(storage.getMutex(bucket_num));
    StoredValue *v = storage.unlocked_find(key, bucket_num);
    bool success = v != NULL;
    GetValue rv(success ? v->getValue() : std::string(":("),
                success);
    cb.callback(rv);
    lh.unlock();
}

void EventuallyPersistentStore::getStats(struct ep_stats *out) {
    LockHolder lh(mutex);
    *out = stats;
}

void EventuallyPersistentStore::resetStats(void) {
    LockHolder lh(mutex);
    memset(&stats, 0, sizeof(stats));
}

void EventuallyPersistentStore::del(std::string &key, Callback<bool> &cb) {
    bool existed = storage.del(key);
    if (existed) {
        queueDirty(key);
    }
    cb.callback(existed);
}

void EventuallyPersistentStore::queueDirty(std::string &key) {
    // Assume locked.
    towrite->push(key);
    stats.queue_size++;
    mutex.notify();
}

void EventuallyPersistentStore::flush(bool shouldWait) {
    LockHolder lh(mutex);

    if (towrite->empty()) {
        stats.dirtyAge = 0;
        if (shouldWait) {
            mutex.wait();
        }
    } else {
        std::queue<std::string> *q = towrite;
        towrite = NULL;
        initQueue();
        lh.unlock();

        RememberingCallback<bool> cb;
        assert(underlying);

        stats.flusher_todo = q->size();

        underlying->begin();
        while (!q->empty()) {
            flushSome(q, cb);
        }
        rel_time_t cstart = ep_current_time();
        underlying->commit();
        // One more lock to update a stat.
        LockHolder lh_stat(mutex);
        stats.commit_time = ep_current_time() - cstart;

        delete q;
    }
}

void EventuallyPersistentStore::flushSome(std::queue<std::string> *q,
                                          Callback<bool> &cb) {

    std::string key = q->front();
    q->pop();

    int bucket_num = storage.bucket(key);
    LockHolder lh(storage.getMutex(bucket_num));
    StoredValue *v = storage.unlocked_find(key, bucket_num);

    bool found = v != NULL;
    bool isDirty = (found && v->isDirty());
    std::string val;
    if (isDirty) {
        rel_time_t dirtied = v->markClean();
        assert(dirtied > 0);
        // Calculate stats if this had a positive time.
        stats.dirtyAge = ep_current_time() - dirtied;
        assert(stats.dirtyAge < (86400 * 30));
        stats.dirtyAgeHighWat = stats.dirtyAge > stats.dirtyAgeHighWat
            ? stats.dirtyAge : stats.dirtyAgeHighWat;
        // Copy it for the duration.
        val.assign(v->getValue());
    }
    stats.flusher_todo--;
    lh.unlock();

    if (found && isDirty) {
        underlying->set(key, val, cb);
    } else if (!found) {
        underlying->del(key, cb);
    }
}

void EventuallyPersistentStore::flusherStopped() {
    LockHolder lh(mutex);
    flusherState = STOPPED;
}
