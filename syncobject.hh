/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SYNCOBJECT_HH
#define SYNCOBJECT_HH 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <pthread.h>

/**
 * Abstraction built on top of pthread mutexes
 */
class SyncObject : public Mutex {
public:
    SyncObject() : Mutex() {
        if (pthread_cond_init(&cond, NULL) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to initialize cond.");
        }
    }

    ~SyncObject() {
        if (pthread_cond_destroy(&cond) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to destroy cond.");
        }
    }

    void wait() {
        if (pthread_cond_wait(&cond, &mutex) != 0) {
            throw std::runtime_error("Failed to wait for condition.");
        }
    }

    void notify() {
        if(pthread_cond_broadcast(&cond) != 0) {
            throw std::runtime_error("Failed to broadcast change.");
        }
    }

private:
    pthread_cond_t cond;

    SyncObject(const SyncObject&);
    void operator=(const SyncObject&);
};

#endif

