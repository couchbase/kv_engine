/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "bucket_logger.h"
#include <chrono>

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
template <typename T, size_t ACQUIRE_MS = 100, size_t HELD_MS = 100>
class LockTimer {
public:
    /** Create a new LockTimer, acquiring the underlying lock.
     *  If it takes longer than ACQUIRE_MS to acquire the lock then report to
     *  the log file.
     *  @param m underlying mutex to acquire
     *  @param name_ A name for this mutex, used in log messages.
     */
    LockTimer(typename T::mutex_type& m, const char* name_)
        : name(name_), start(std::chrono::steady_clock::now()), lock_holder(m) {
        acquired = std::chrono::steady_clock::now();
        const uint64_t msec =
                std::chrono::duration_cast<std::chrono::milliseconds>(acquired -
                                                                      start)
                        .count();
        if (msec > ACQUIRE_MS) {
            EP_LOG_WARN("LockHolder<{}> Took too long to acquire lock: {} ms",
                        name,
                        msec);
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
        const auto released = std::chrono::steady_clock::now();
        const uint64_t msec =
                std::chrono::duration_cast<std::chrono::milliseconds>(released -
                                                                      acquired)
                        .count();
        if (msec > HELD_MS) {
            EP_LOG_WARN(
                    "LockHolder<{}> Held lock for too long: {} ms", name, msec);
        }
    }

    const char* name;

    // Time when lock acquisition started.
    std::chrono::steady_clock::time_point start;

    // Time when we completed acquiring the lock.
    std::chrono::steady_clock::time_point acquired;

    // The underlying 'real' lock holder we are wrapping.
    T lock_holder;
};
