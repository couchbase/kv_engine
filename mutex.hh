/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MUTEX_HH
#define MUTEX_HH 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <pthread.h>
#include <cerrno>
#include <cstring>
#include <cassert>

#include "common.hh"

/**
 * Abstraction built on top of pthread mutexes
 */
class Mutex {
public:
    Mutex() : held(false)
    {
        pthread_mutexattr_t *attr = NULL;
        int e=0;

#ifdef HAVE_PTHREAD_MUTEX_ERRORCHECK
        pthread_mutexattr_t the_attr;
        attr = &the_attr;

        if (pthread_mutexattr_init(attr) != 0 ||
            (e = pthread_mutexattr_settype(attr, PTHREAD_MUTEX_ERRORCHECK)) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
#endif

        if ((e = pthread_mutex_init(&mutex, attr)) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    virtual ~Mutex() {
        int e;
        if ((e = pthread_mutex_destroy(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    /**
     * True if I own this lock.
     *
     * Use this only for assertions.
     */
    bool ownsLock() {
        return held && pthread_equal(holder, pthread_self());
    }

protected:

    // The holders of locks twiddle these flags.
    friend class LockHolder;
    friend class MultiLockHolder;

    void acquire() {
        int e;
        if ((e = pthread_mutex_lock(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to acquire lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
        setHolder(true);
    }

    void release() {
        assert(held && pthread_equal(holder, pthread_self()));
        setHolder(false);
        int e;
        if ((e = pthread_mutex_unlock(&mutex)) != 0) {
            std::string message = "MUTEX_ERROR: Failed to release lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    void setHolder(bool isHeld) {
        held = isHeld;
        holder = pthread_self();
    }

    pthread_mutex_t mutex;
    pthread_t holder;
    bool held;

    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif
