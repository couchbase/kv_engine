/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include "config.h"

#include <memcached/engine.h>

#include <string>
#include <utility>

#include "memory_tracker.h"
#include "objectregistry.h"
#include "utility.h"

bool MemoryTracker::tracking = false;
std::atomic<MemoryTracker*> MemoryTracker::instance;
std::mutex MemoryTracker::instance_mutex;

void MemoryTracker::statsThreadMainLoop(void* arg) {
    MemoryTracker* tracker = static_cast<MemoryTracker*>(arg);
    while (true) {
        // Wait for either the shutdown condvar to be notified, or for
        // 250ms. If we hit the timeout then time to update the stats.
        std::unique_lock<std::mutex> lock(tracker->mutex);
        if (tracker->shutdown_cv.wait_for(
                lock,
                std::chrono::milliseconds(250),
                [tracker]{return !tracker->trackingMemoryAllocations();})) {
            // No longer tracking - exit.
            return;
        } else {
            tracker->updateStats();
        }
    }
}

MemoryTracker* MemoryTracker::getInstance() {
    MemoryTracker* tmp = instance.load();
    if (tmp == nullptr) {
        // Double-checked locking if instance is null - ensure two threads
        // don't both create an instance.
        std::lock_guard<std::mutex> lock(instance_mutex);
        tmp = instance.load();
        if (tmp == nullptr) {
            tmp = new MemoryTracker();
            instance.store(tmp);
        }
    }
    return tmp;
}

void MemoryTracker::destroyInstance() {
    std::lock_guard<std::mutex> lock(instance_mutex);
    MemoryTracker* tmp = instance.load();
    if (tmp != nullptr) {
        delete tmp;
        instance = nullptr;
    }
}

extern "C" {
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            size_t alloc = getHooksApi()->get_allocation_size(p);
            ObjectRegistry::memoryAllocated(alloc);
        }
    }

    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            size_t alloc = getHooksApi()->get_allocation_size(p);
            ObjectRegistry::memoryDeallocated(alloc);
        }
    }
}

MemoryTracker::MemoryTracker() {
    if (getenv("EP_NO_MEMACCOUNT") != NULL) {
        LOG(EXTENSION_LOG_NOTICE, "Memory allocation tracking disabled");
        return;
    }
    stats.ext_stats_size = getHooksApi()->get_extra_stats_size();
    stats.ext_stats = (allocator_ext_stat*) calloc(stats.ext_stats_size,
                                                   sizeof(allocator_ext_stat));
    if (getHooksApi()->add_new_hook(&NewHook)) {
        LOG(EXTENSION_LOG_DEBUG, "Registered add hook");
        if (getHooksApi()->add_delete_hook(&DeleteHook)) {
            LOG(EXTENSION_LOG_DEBUG, "Registered delete hook");
            tracking = true;
            updateStats();
            if (cb_create_named_thread(&statsThreadId,
                                       statsThreadMainLoop,
                                           this, 0, "mc:mem stats") != 0) {
                throw std::runtime_error(
                                      "Error creating thread to update stats");
            }
            return;
        }
        getHooksApi()->remove_new_hook(&NewHook);
    }
    LOG(EXTENSION_LOG_WARNING, "Failed to register allocator hooks");
}

MemoryTracker::~MemoryTracker() {
    getHooksApi()->remove_new_hook(&NewHook);
    getHooksApi()->remove_delete_hook(&DeleteHook);
    if (tracking) {
        tracking = false;
        shutdown_cv.notify_all();
        cb_join_thread(statsThreadId);
    }
    free(stats.ext_stats);
    instance = NULL;
}

void MemoryTracker::getAllocatorStats(std::map<std::string, size_t>
                                                               &alloc_stats) {
    if (!trackingMemoryAllocations()) {
        return;
    }

    for (size_t i = 0; i < stats.ext_stats_size; ++i) {
        alloc_stats.insert(std::pair<std::string, size_t>(
                                                    stats.ext_stats[i].key,
                                                    stats.ext_stats[i].value));
    }
    alloc_stats.insert(std::make_pair("total_allocated_bytes",
                                      stats.allocated_size));
    alloc_stats.insert(std::make_pair("total_heap_bytes",
                                      stats.heap_size));
    alloc_stats.insert(std::make_pair("total_free_mapped_bytes",
                                      stats.free_mapped_size));
    alloc_stats.insert(std::make_pair("total_free_unmapped_bytes",
                                      stats.free_unmapped_size));
    alloc_stats.insert(std::make_pair("total_fragmentation_bytes",
                                      stats.fragmentation_size));
}

void MemoryTracker::getDetailedStats(char* buffer, int size) {
    getHooksApi()->get_detailed_stats(buffer, size);
}

void MemoryTracker::updateStats() {
    getHooksApi()->get_allocator_stats(&stats);
}

size_t MemoryTracker::getFragmentation() {
    return stats.fragmentation_size;
}

size_t MemoryTracker::getTotalBytesAllocated() {
    return stats.allocated_size;
}

size_t MemoryTracker::getTotalHeapBytes() {
    return stats.heap_size;
}

bool MemoryTracker::trackingMemoryAllocations() {
    return tracking;
}
