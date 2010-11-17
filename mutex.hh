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
    Mutex()
#ifndef WIN32
        : holder(0)
#endif
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
        setHolder();
    }

    void release() {
#ifndef WIN32
        assert(holder == pthread_self());
        holder = 0;
#endif
        int e;
        if ((e = pthread_mutex_unlock(&mutex)) != 0) {
            std::string message = "MUTEX_ERROR: Failed to release lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    void setHolder() {
#ifndef WIN32
        holder = pthread_self();
#endif
    }

    pthread_mutex_t mutex;
#ifndef WIN32
    pthread_t holder;
#endif

    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif
