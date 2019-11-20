/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "benchmark_memory_tracker.h"
#include <objectregistry.h>
#include <utility.h>

#include <engines/ep/src/bucket_logger.h>
#include <algorithm>

std::atomic<BenchmarkMemoryTracker*> BenchmarkMemoryTracker::instance;
std::mutex BenchmarkMemoryTracker::instanceMutex;
std::atomic<size_t> BenchmarkMemoryTracker::maxTotalAllocation;
std::atomic<size_t> BenchmarkMemoryTracker::currentAlloc;

BenchmarkMemoryTracker::~BenchmarkMemoryTracker() {
    hooks_api.remove_new_hook(&NewHook);
    hooks_api.remove_delete_hook(&DeleteHook);
}

BenchmarkMemoryTracker* BenchmarkMemoryTracker::getInstance(
        const ServerAllocatorIface& hooks_api_) {
    BenchmarkMemoryTracker* tmp = instance.load();
    if (tmp == nullptr) {
        std::lock_guard<std::mutex> lock(instanceMutex);
        tmp = instance.load();
        if (tmp == nullptr) {
            tmp = new BenchmarkMemoryTracker(hooks_api_);
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

size_t BenchmarkMemoryTracker::getMaxAlloc() {
    return maxTotalAllocation;
}

size_t BenchmarkMemoryTracker::getCurrentAlloc() {
    return currentAlloc;
}

BenchmarkMemoryTracker::BenchmarkMemoryTracker(
        const ServerAllocatorIface& hooks_api)
    : hooks_api(hooks_api) {
    ObjectRegistry::initialize(hooks_api.get_allocation_size);
}
void BenchmarkMemoryTracker::connectHooks() {
    if (hooks_api.add_new_hook(&NewHook)) {
        EP_LOG_DEBUG("Registered add hook");
        if (hooks_api.add_delete_hook(&DeleteHook)) {
            EP_LOG_DEBUG("Registered delete hook");
            return;
        }
        hooks_api.remove_new_hook(&NewHook);
    }
    EP_LOG_WARN("Failed to register allocator hooks");
}
void BenchmarkMemoryTracker::NewHook(const void* ptr, size_t) {
    if (ptr != NULL) {
        const auto* tracker = BenchmarkMemoryTracker::instance.load();
        void* p = const_cast<void*>(ptr);
        size_t alloc = tracker->hooks_api.get_allocation_size(p);
        currentAlloc += alloc;
        maxTotalAllocation.store(
                std::max(currentAlloc.load(), maxTotalAllocation.load()));
    }
}
void BenchmarkMemoryTracker::DeleteHook(const void* ptr) {
    if (ptr != NULL) {
        const auto* tracker = BenchmarkMemoryTracker::instance.load();
        void* p = const_cast<void*>(ptr);
        size_t alloc = tracker->hooks_api.get_allocation_size(p);
        currentAlloc -= alloc;
    }
}

void BenchmarkMemoryTracker::reset() {
    currentAlloc.store(0);
    maxTotalAllocation.store(0);
}
