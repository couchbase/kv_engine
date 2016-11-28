/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_LOCKS_H_
#define SRC_LOCKS_H_ 1

#include "config.h"

#include <functional>
#include <mutex>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "rwlock.h"
#include "utility.h"

using LockHolder = std::lock_guard<std::mutex>;

/**
 * RAII lock holder over multiple locks.
 */
class MultiLockHolder {
public:

    /**
     * Acquire a series of locks.
     *
     * @param m beginning of an array of locks
     * @param n the number of locks to lock
     */
    MultiLockHolder(std::mutex* m, size_t n)
        : mutexes(m),
          n_locks(n) {
        lock();
    }

    ~MultiLockHolder() {
        unlock();
    }

private:
    /**
     * Relock the series after having manually unlocked it.
     */
    void lock() {
        for (size_t i = 0; i < n_locks; i++) {
            mutexes[i].lock();
        }
    }

    /**
     * Manually unlock the series.
     */
    void unlock() {
        for (size_t i = 0; i < n_locks; i++) {
            mutexes[i].unlock();
        }
    }

    std::mutex* mutexes;
    size_t n_locks;

    DISALLOW_COPY_AND_ASSIGN(MultiLockHolder);
};
#define MultiLockHolder(x) \
    static_assert(false, "MultiLockHolder: missing variable name for scoped lock.")

// RAII Reader lock
// deprecated, prefer std::lock_guard<ReaderLock> rlh(rwLock.reader())
class ReaderLockHolder {
public:
    typedef RWLock mutex_type;

    ReaderLockHolder(RWLock& lock)
        : lh(lock) {
    }

private:
    std::lock_guard<ReaderLock> lh;

    DISALLOW_COPY_AND_ASSIGN(ReaderLockHolder);
};
#define ReaderLockHolder(x) \
    static_assert(false, "ReaderLockHolder: missing variable name for scoped lock.")

// RAII Writer lock
// deprecated, prefer std::lock_guard<WriterLock> wlh(rwLock.writer())
class WriterLockHolder {
public:
    typedef RWLock mutex_type;

    WriterLockHolder(RWLock& lock)
        : lh(lock) {
    }

private:
    std::lock_guard<WriterLock> lh;

    DISALLOW_COPY_AND_ASSIGN(WriterLockHolder);
};
#define WriterLockHolder(x) \
    static_assert(false, "WriterLockHolder: missing variable name for scoped lock.")

/**
 * Lock holder wrapper to assist to debugging locking issues - Logs when the
 * time taken to acquire a lock, or the duration a lock is held exceeds the
 * specified thresholds.
 *
 * Implemented as a template class around a RAII-style lock holder:
 *
 *   T - underlying lock holder type (LockHolder, ReaderLockHolder etc).
 *   ACQUIRE_MS - Report instances when it takes longer than this to acquire a
 *                lock.
 *   HELD_MS - Report instance when a lock is held (locked) for longer than
 *             this.
 *
 * Usage:
 * To debug a single lock holder - wrap the class with a LockTimer<>, adding
 * a lock name as an additional argument - e.g.
 *
 *   LockHolder lh(mutex)
 *
 * becomes:
 *
 *   LockTimer<LockHolder> lh(mutex, "my_func_lockholder")
 *
 */
template <typename T,
          size_t ACQUIRE_MS = 100,
          size_t HELD_MS = 100>
class LockTimer {
public:

    /** Create a new LockTimer, acquiring the underlying lock.
     *  If it takes longer than ACQUIRE_MS to acquire the lock then report to
     *  the log file.
     *  @param m underlying mutex to acquire
     *  @param name_ A name for this mutex, used in log messages.
     */
    LockTimer(typename T::mutex_type& m, const char* name_)
        : name(name_),
          start(gethrtime()),
          lock_holder(m) {

        acquired = gethrtime();
        const uint64_t msec = (acquired - start) / 1000000;
        if (msec > ACQUIRE_MS) {
            LOG(EXTENSION_LOG_WARNING,
                "LockHolder<%s> Took too long to acquire lock: %" PRIu64 " ms",
                name, msec);
        }
    }

    /** Destroy the DebugLockHolder releasing the underlying lock.
     *  If the lock was held for longer than HELS_MS to then report to the
     *  log file.
     */
    ~LockTimer() {
        check_held_duration();
        // upon destruction the lock_holder will also be destroyed and hence
        // unlocked...
    }

    /* explicitly unlock the lock */
    void unlock() {
        check_held_duration();
        lock_holder.unlock();
    }

private:

    void check_held_duration() {
        const hrtime_t released = gethrtime();
        const uint64_t msec = (released - acquired) / 1000000;
        if (msec > HELD_MS) {
            LOG(EXTENSION_LOG_WARNING, "LockHolder<%s> Held lock for too long: "
                    "%" PRIu64 " ms", name, msec);
        }
    }

    const char* name;

    // Time when lock acquisition started.
    hrtime_t start;

    // Time when we completed acquiring the lock.
    hrtime_t acquired;

    // The underlying 'real' lock holder we are wrapping.
    T lock_holder;
};

#endif  // SRC_LOCKS_H_
