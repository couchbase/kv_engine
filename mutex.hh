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
        if (pthread_mutex_init(&mutex, NULL) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(strerror(errno));
            throw std::runtime_error(message);
        }
    }

    virtual ~Mutex() {
        if (pthread_mutex_destroy(&mutex) != 0) {
            std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
            message.append(strerror(errno));
            throw std::runtime_error(message);
        }
    }

    void aquire() {
        if (pthread_mutex_lock(&mutex) != 0) {
            std::string message = "MUTEX ERROR: Failed to acquire lock: ";
            message.append(strerror(errno));
            throw std::runtime_error(message);
        }
    }

    void release() {
        if (pthread_mutex_unlock(&mutex) != 0) {
            std::string message = "MUTEX_ERROR: Failed to release lock: ";
            message.append(strerror(errno));
            throw std::runtime_error(message);
        }
    }

protected:
    pthread_mutex_t mutex;

    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif
