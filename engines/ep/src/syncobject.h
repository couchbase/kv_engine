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

#include <chrono>
#include <condition_variable>

/**
 * Abstraction built on top of std::condition_variable & std::mutex
 */
class SyncObject : public std::mutex {
public:
    SyncObject() {
    }

    ~SyncObject() {
    }

    // Note: there is no `wait(lock)` override (without Predicate) provided.
    // This is because the underlying std::condition_variable::wait(lock)
    // method can get spurious wakeups, and hence can miss notifications if
    // using a non-predicated wait - see
    // http://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#Rconc-wait

    template <class Predicate>
    void wait(std::unique_lock<std::mutex>& lock, Predicate pred) {
        cond.wait(lock, pred);
    }

    void wait_for(std::unique_lock<std::mutex>& lock,
                  const double secs) {
        cond.wait_for(lock, std::chrono::milliseconds(int64_t(secs * 1000.0)));
    }

    void wait_for(std::unique_lock<std::mutex>& lock,
                  const std::chrono::nanoseconds nanoSecs) {
        cond.wait_for(lock, nanoSecs);
    }

    template <class Predicate>
    void wait_for(std::unique_lock<std::mutex>& lock,
                  const std::chrono::nanoseconds nanoSecs,
                  Predicate pred) {
        cond.wait_for(lock, nanoSecs, pred);
    }

    void notify_all() {
        cond.notify_all();
    }

    void notify_one() {
        cond.notify_one();
    }

private:
    std::condition_variable cond;

    DISALLOW_COPY_AND_ASSIGN(SyncObject);
};
