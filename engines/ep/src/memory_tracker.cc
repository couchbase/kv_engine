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

#include <memcached/engine.h>
#include <platform/cb_arena_malloc.h>

#include <string>
#include <utility>

#include "bucket_logger.h"
#include "memory_tracker.h"
#include "utility.h"

std::atomic<bool> MemoryTracker::tracking{false};
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

MemoryTracker* MemoryTracker::getInstance(
        const ServerAllocatorIface& hooks_api_) {
    MemoryTracker* tmp = instance.load();
    if (tmp == nullptr) {
        // Double-checked locking if instance is null - ensure two threads
        // don't both create an instance.
        std::lock_guard<std::mutex> lock(instance_mutex);
        tmp = instance.load();
        if (tmp == nullptr) {
            // Note that object construction and hook attaching is
            // split. The issue is that until the constructor
            // completes (and {instance} is assigned) it is not
            // possible to 'service' any memory allocator callbacks -
            // as the hooks need to read {instance}.hooks_api
            tmp = new MemoryTracker(hooks_api_);
            instance.store(tmp);

            instance.load()->startThread();
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

MemoryTracker::MemoryTracker(const ServerAllocatorIface& hooks_api_)
    : hooks_api(hooks_api_) {
    // Just create the object, actual hook registration happens
    // once we have a concrete object constructed.
}

void MemoryTracker::startThread() {
    if (getenv("EP_NO_MEMACCOUNT") != NULL) {
        EP_LOG_INFO(
                "Memory allocation tracking disabled, not starting "
                "statsThreadMainLoop");
        return;
    }

    if (cb::ArenaMalloc::canTrackAllocations()) {
        tracking = true;
        updateStats();
        if (cb_create_named_thread(&statsThreadId,
                                   statsThreadMainLoop,
                                   this,
                                   0,
                                   "mc:mem stats") != 0) {
            throw std::runtime_error("Error creating thread to update stats");
        }
        return;
    }
}

MemoryTracker::~MemoryTracker() {
    if (tracking) {
        tracking = false;
        shutdown_cv.notify_all();
        cb_join_thread(statsThreadId);
    }
    instance = NULL;
}

void MemoryTracker::getAllocatorStats(std::map<std::string, size_t>
                                                               &alloc_stats) {
    if (!trackingMemoryAllocations()) {
        return;
    }

    for (auto& ext_stat : stats.ext_stats) {
        alloc_stats.insert(std::pair<std::string, size_t>(
                                                    ext_stat.key,
                                                    ext_stat.value));
    }
    alloc_stats.insert(std::make_pair("total_allocated_bytes",
                                      stats.allocated_size));
    alloc_stats.insert(std::make_pair("total_heap_bytes",
                                      stats.heap_size));
    alloc_stats.insert(
            std::make_pair("total_metadata_bytes", stats.metadata_size));
    alloc_stats.insert(
            std::make_pair("total_resident_bytes", stats.resident_size));
    alloc_stats.insert(
            std::make_pair("total_retained_bytes", stats.retained_size));
    alloc_stats.insert(std::make_pair("total_fragmentation_bytes",
                                      stats.fragmentation_size));
}

void MemoryTracker::getDetailedStats(char* buffer, int size) {
    hooks_api.get_detailed_stats(buffer, size);
}

void MemoryTracker::updateStats() {
    hooks_api.get_allocator_stats(&stats);
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
