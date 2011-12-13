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
    Mutex();

    virtual ~Mutex();

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

    void acquire();
    void release();

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
