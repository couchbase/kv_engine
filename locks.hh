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
        mutex.aquire();
        locked = true;
    }

    void unlock() {
        if (locked) {
            mutex.release();
            locked = false;
        }
    }

private:
    Mutex &mutex;
    bool locked;

    DISALLOW_COPY_AND_ASSIGN(LockHolder);
};

#endif /* LOCKS_H */
