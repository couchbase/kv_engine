/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LOCKS_H
#define LOCKS_H 1

#include <stdexcept>
#include <iostream>
#include <sstream>

#include "common.hh"
#include "mutex.hh"
#include "syncobject.hh"

class LockHolder {
public:
    /**
     * Acquire the lock in the given mutex.
     */
    LockHolder(Mutex &m) : mutex(m), locked(false) {
        lock();
    }

    /**
     * Release the lock.
     */
    ~LockHolder() {
        unlock();
    }

    void lock() {
        mutex.acquire();
        locked = true;
    }

    void unlock() {
        if (locked) {
            locked = false;
            mutex.release();
        }
    }

private:
    Mutex &mutex;
    bool locked;

    DISALLOW_COPY_AND_ASSIGN(LockHolder);
};

class MultiLockHolder {
public:

    MultiLockHolder(Mutex *m, size_t n) : mutexes(m),
                                          locked(NULL),
                                          n_locks(n) {
        locked = new bool[n];
        lock();
    }

    ~MultiLockHolder() {
        unlock();
        delete[] locked;
    }

    void lock() {
        for (size_t i = 0; i < n_locks; i++) {
            mutexes[i].acquire();
            locked[i] = true;
        }
    }

    void unlock() {
        for (size_t i = 0; i < n_locks; i++) {
            if (locked[i]) {
                locked[i] = false;
                mutexes[i].release();
            }
        }
    }

private:
    Mutex  *mutexes;
    bool   *locked;
    size_t  n_locks;

    DISALLOW_COPY_AND_ASSIGN(MultiLockHolder);
};

#endif /* LOCKS_H */
