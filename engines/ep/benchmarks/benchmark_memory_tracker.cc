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

#include "benchmark_memory_tracker.h"
#include <objectregistry.h>
#include <utility.h>

#include <engines/ep/src/bucket_logger.h>
#include <platform/cb_arena_malloc.h>

#include <platform/cb_malloc.h>
#include <algorithm>

std::atomic<BenchmarkMemoryTracker*> BenchmarkMemoryTracker::instance;
std::mutex BenchmarkMemoryTracker::instanceMutex;
std::atomic<size_t> BenchmarkMemoryTracker::maxTotalAllocation;
std::atomic<size_t> BenchmarkMemoryTracker::currentAlloc;

BenchmarkMemoryTracker::~BenchmarkMemoryTracker() {
    cb_remove_new_hook(&NewHook);
    cb_remove_delete_hook(&DeleteHook);
}

BenchmarkMemoryTracker* BenchmarkMemoryTracker::getInstance() {
    BenchmarkMemoryTracker* tmp = instance.load();
    if (tmp == nullptr) {
        std::lock_guard<std::mutex> lock(instanceMutex);
        tmp = instance.load();
        if (tmp == nullptr) {
            tmp = new BenchmarkMemoryTracker();
            instance.store(tmp);
            instance.load()->connectHooks();
        }
    }
    return tmp;
}

void BenchmarkMemoryTracker::destroyInstance() {
    std::lock_guard<std::mutex> lock(instanceMutex);
    BenchmarkMemoryTracker* tmp = instance.load();
    if (tmp != nullptr) {
        delete tmp;
        instance = nullptr;
    }
}

void BenchmarkMemoryTracker::connectHooks() {
    if (cb_add_new_hook(&NewHook)) {
        EP_LOG_DEBUG("Registered add hook");
        if (cb_add_delete_hook(&DeleteHook)) {
            EP_LOG_DEBUG("Registered delete hook");
            return;
        }
        cb_remove_new_hook(&NewHook);
    }
    EP_LOG_WARN("Failed to register allocator hooks");
}
void BenchmarkMemoryTracker::NewHook(const void* ptr, size_t) {
    if (ptr != nullptr) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = cb::ArenaMalloc::malloc_usable_size(p);
        currentAlloc += alloc;
        maxTotalAllocation.store(
                std::max(currentAlloc.load(), maxTotalAllocation.load()));
    }
}
void BenchmarkMemoryTracker::DeleteHook(const void* ptr) {
    if (ptr != nullptr) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = cb::ArenaMalloc::malloc_usable_size(p);
        currentAlloc -= alloc;
    }
}

void BenchmarkMemoryTracker::reset() {
    currentAlloc.store(0);
    maxTotalAllocation.store(0);
}
