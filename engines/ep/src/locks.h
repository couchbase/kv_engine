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
#pragma once

#include "utility.h"
#include <platform/rwlock.h>
#include <mutex>
#include <vector>

using LockHolder = std::lock_guard<std::mutex>;

/**
 * RAII lock holder over multiple locks.
 */
class MultiLockHolder {
public:

    /**
     * Acquire a series of locks.
     *
     * @param m reference to a vector of locks
     */
    MultiLockHolder(std::vector<std::mutex>& m)
        : mutexes(m) {
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
        for (auto& m : mutexes) {
            m.lock();
        }
    }

    /**
     * Manually unlock the series.
     */
    void unlock() {
        for (auto& m : mutexes) {
            m.unlock();
        }
    }

    std::vector<std::mutex>& mutexes;

    DISALLOW_COPY_AND_ASSIGN(MultiLockHolder);
};
#define MultiLockHolder(x) \
    static_assert(false, "MultiLockHolder: missing variable name for scoped lock.")

// RAII Reader lock
// deprecated, prefer std::lock_guard<ReaderLock> rlh(rwLock.reader())
class ReaderLockHolder {
public:
    typedef cb::RWLock mutex_type;

    ReaderLockHolder(cb::RWLock& lock)
        : lh(lock) {
    }

private:
    std::lock_guard<cb::ReaderLock> lh;

    DISALLOW_COPY_AND_ASSIGN(ReaderLockHolder);
};
#define ReaderLockHolder(x) \
    static_assert(false, "ReaderLockHolder: missing variable name for scoped lock.")

// RAII Writer lock
// deprecated, prefer std::lock_guard<WriterLock> wlh(rwLock.writer())
class WriterLockHolder {
public:
    typedef cb::RWLock mutex_type;

    WriterLockHolder(cb::RWLock& lock)
        : lh(lock) {
    }

private:
    std::lock_guard<cb::WriterLock> lh;

    DISALLOW_COPY_AND_ASSIGN(WriterLockHolder);
};
#define WriterLockHolder(x) \
    static_assert(false, "WriterLockHolder: missing variable name for scoped lock.")
