/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <atomic>
#include <mutex>

/*
 * A singleton which tracks memory usage for use in benchmarks.
 *
 * This class provides hooks for new and delete which are registered when the
 * singleton is created.
 *
 * Tracks the current allocation along with the maximum total allocation size
 * it has seen.
 */
class BenchmarkMemoryTracker {
public:
    ~BenchmarkMemoryTracker();

    static BenchmarkMemoryTracker* getInstance();

    static void destroyInstance();

    static void reset();

    static size_t getMaxAlloc() {
        return maxTotalAllocation;
    }

    static size_t getCurrentAlloc() {
        return currentAlloc;
    }

protected:
    BenchmarkMemoryTracker() = default;
    static void connectHooks();
    static void NewHook(const void* ptr, size_t);
    static void DeleteHook(const void* ptr);

    static std::atomic<BenchmarkMemoryTracker*> instance;
    static std::mutex instanceMutex;
    static std::atomic<size_t> maxTotalAllocation;
    static std::atomic<size_t> currentAlloc;
};
