/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_MUTEX_H_
#define SRC_MUTEX_H_ 1

#include "config.h"

#include <pthread.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "common.h"

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

private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif  // SRC_MUTEX_H_
