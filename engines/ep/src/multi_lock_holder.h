/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "utility.h"
#include <mutex>
#include <vector>

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
    explicit MultiLockHolder(std::vector<std::mutex>& m) : mutexes(m) {
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
    static_assert(false,   \
                  "MultiLockHolder: missing variable name for scoped lock.")
