/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MUTEX_HH
#define MUTEX_HH 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <pthread.h>
#include <cerrno>
#include <cstring>

#include "common.hh"

/**
 * Abstraction built on top of pthread mutexes
 */
class Mutex {
public:
    Mutex() {
        int e(0);
        if ((e = pthread_mutex_init(&mutex, NULL)) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    virtual ~Mutex() {
        int e(0);
        if ((e = pthread_mutex_destroy(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    void acquire() {
        int e(0);
        if ((e = pthread_mutex_lock(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to acquire lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    void release() {
        int e(0);
        if ((e = pthread_mutex_unlock(&mutex)) != 0) {
            std::string message = "MUTEX_ERROR: Failed to release lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

protected:
    pthread_mutex_t mutex;

    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif
