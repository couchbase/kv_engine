/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MUTEX_HH
#define MUTEX_HH 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <pthread.h>

/**
 * Abstraction built on top of pthread mutexes
 */
class Mutex {
public:
    Mutex() : locked(false) {
        if (pthread_mutex_init(&mutex, NULL) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to initialize mutex.");
        }
    }

    virtual ~Mutex() {
        if (locked) {
            throw std::runtime_error("MUTEX ERROR: destroying a locked mutex.");
        }

        if (pthread_mutex_destroy(&mutex) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to destroy mutex.");
        }
    }

    void aquire() {
        if (locked) {
            throw std::runtime_error("MUTEX ERROR: trying to aquire a locked mutex.");
        }

        if (pthread_mutex_lock(&mutex) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to acquire lock.");
        }
        locked = true;
    }

    void release() {
        if (pthread_mutex_unlock(&mutex) != 0) {
            throw std::runtime_error("MUTEX_ERROR: Failed to release lock.");
        }
        locked = false;
    }

protected:
    pthread_mutex_t mutex;
    bool locked;

    Mutex(const Mutex&);
    void operator=(const Mutex&);
};

#endif
