/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "ep.hh"
#include "locks.hh"

#include <string.h>

namespace kvtest {

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
    }

    EventuallyPersistentStore::EventuallyPersistentStore(KVStore *t,
                                                         size_t est) {

        pthread_mutex_init(&mutex, NULL);
        pthread_cond_init(&cond, NULL);
        est_size = est;
        towrite = NULL;
        initQueue();
        flusher = new Flusher(this);

        // Run in a thread...
        if(pthread_create(&thread, NULL, launch_flusher_thread, flusher)
           != 0) {
            throw std::runtime_error("Error initializing queue thread");
        }

        underlying = t;
        assert(underlying);
    }

    EventuallyPersistentStore::~EventuallyPersistentStore() {
        LockHolder lh(&mutex);
        flusher->stop();
        if(pthread_cond_signal(&cond) != 0) {
            throw std::runtime_error("Error signaling change.");
        }
        lh.unlock();
        pthread_join(thread, NULL);
        delete flusher;
        delete towrite;
        pthread_cond_destroy(&cond);
        pthread_mutex_destroy(&mutex);
    }

    void EventuallyPersistentStore::initQueue() {
        assert(!towrite);
        towrite = new std::queue<std::string>;
    }

    void EventuallyPersistentStore::set(std::string &key, std::string &val,
                                        Callback<bool> &cb) {
        mutation_type_t mtype = storage.set(key, val);

        if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
            LockHolder lh(&mutex);
            queueDirty(key);
        }
        bool rv = true;
        cb.callback(rv);
    }

    void EventuallyPersistentStore::set(std::string &key, const char *val,
                                        Callback<bool> &cb) {
        mutation_type_t mtype = storage.set(key, val);

        if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
            LockHolder lh(&mutex);
            queueDirty(key);
        }
        bool rv = true;
        cb.callback(rv);
    }

    void EventuallyPersistentStore::reset() {
        flush(false);
        LockHolder lh(&mutex);
        underlying->reset();
        delete towrite;
        towrite = NULL;
        initQueue();
        storage.clear();
    }

    void EventuallyPersistentStore::get(std::string &key,
                                        Callback<kvtest::GetValue> &cb) {
        int bucket_num = storage.bucket(key);
        LockHolder lh(storage.getMutex(bucket_num));
        StoredValue *v = storage.unlocked_find(key, bucket_num);
        bool success = v != NULL;
        const char *sval = v ? v->getValue() : NULL;
        kvtest::GetValue rv(success ? sval : std::string(":("),
                            success);
        cb.callback(rv);
        lh.unlock();
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
        if(pthread_cond_signal(&cond) != 0) {
            throw std::runtime_error("Error signaling change.");
        }
    }

    void EventuallyPersistentStore::flush(bool shouldWait) {
        LockHolder lh(&mutex);

        if (towrite->empty()) {
            if (shouldWait) {
                if(pthread_cond_wait(&cond, &mutex) != 0) {
                    throw std::runtime_error("Error waiting for signal.");
                }
            }
        } else {
            std::queue<std::string> *q = towrite;
            towrite = NULL;
            initQueue();
            lh.unlock();

            RememberingCallback<bool> cb;
            assert(underlying);

            underlying->begin();
            while (!q->empty()) {
                flushSome(q, cb);
            }
            underlying->commit();

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
        const char *val = NULL;
        if (isDirty) {
            v->markClean();
            // Copy it for the duration.
            val = strdup(v->getValue());
        }
        lh.unlock();

        if (found && isDirty) {
            underlying->set(key, val, cb);
        } else if (!found) {
            underlying->del(key, cb);
        }

        free((void*)val);
    }

}
