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
#include <platform/cb_time.h>
#include <chrono>
#include <source_location>

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
 *   std::lock_guard<std::mutex> lh(mutex)
 *
 * becomes:
 *
 *   LockTimer<std::lock_guard<std::mutex>> lh(mutex, "my_func_lockholder")
 *
 */
template <typename T, size_t ACQUIRE_MS = 100, size_t HELD_MS = 100>
class LockTimer {
public:
    /**
     * Create a new LockTimer, acquiring the underlying lock
     *
     * @param m underlying mutex to acquire
     * @param name_ A name for this mutex, used in log messages.
     * @param loc_ caller used in log messages
     */
    template <typename U = T>
    LockTimer(typename U::mutex_type& m,
              const char* name_,
              std::source_location loc_ = std::source_location::current())
        : name(name_),
          loc(loc_),
          start(cb::time::steady_clock::now()),
          lock_holder(m) {
        check_acquire_duration();
    }

    /**
     * Create a new LockTimer around a folly::Synchronized LockedPtr, acquiring
     * the lock of sync
     *
     * @param sync the folly::Synchronized to lock
     * @param name_ name for this lock, used in log messages
     * @param loc_ caller used in log messages.
     */
    template <typename SynchronizedT>
    LockTimer(SynchronizedT* sync,
              const char* name_,
              std::source_location loc_ = std::source_location::current())
        : name(name_),
          loc(loc_),
          start(cb::time::steady_clock::now()),
          lock_holder(sync) {
        check_acquire_duration();
    }

    /**
     * Destroy the LockTimer releasing the underlying lock.
     * If the lock was held for longer than HELD_MS then report to the log
     * file. If T can be unlocked, the report is written after the release.
     */
    ~LockTimer() {
        if constexpr (requires(T& t) { t.unlock(); }) {
            unlock();
        } else {
            // lock_holder is destroyed (and unlocked) after this body
            check_held_duration(cb::time::steady_clock::now());
        }
    }

    auto operator->() const {
        return lock_holder.operator->();
    }

    auto& operator*() const {
        return *lock_holder;
    }

    /* explicitly unlock the lock */
    void unlock() {
        if (unlocked) {
            return;
        }
        const auto released = cb::time::steady_clock::now();
        lock_holder.unlock();
        unlocked = true;
        check_held_duration(released);
    }

private:
    void check_acquire_duration() {
        acquired = cb::time::steady_clock::now();
        const auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(
                acquired - start);
        if (msec > std::chrono::milliseconds(ACQUIRE_MS)) {
            EP_LOG_WARN(
                    "LockHolder<{}> Took too long to acquire lock: {} ms in {}",
                    name,
                    msec.count(),
                    loc.function_name());
        }
    }

    void check_held_duration(cb::time::steady_clock::time_point released) {
        const auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(
                released - acquired);
        if (msec > std::chrono::milliseconds(HELD_MS)) {
            EP_LOG_WARN("LockHolder<{}> Held lock for too long: {} ms in {}",
                        name,
                        msec.count(),
                        loc.function_name());
        }
    }

    const char* name;

    bool unlocked = false;

    std::source_location loc;

    // Time when lock acquisition started.
    cb::time::steady_clock::time_point start;

    // Time when we completed acquiring the lock.
    cb::time::steady_clock::time_point acquired;

    // The underlying 'real' lock holder we are wrapping.
    T lock_holder;
};
