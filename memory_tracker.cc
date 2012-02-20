/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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

#include "memory_tracker.hh"

#include "objectregistry.hh"
#include <memcached/engine.h>

bool MemoryTracker::trackingAllocations = false;
MemoryTracker *MemoryTracker::instance = NULL;

MemoryTracker *MemoryTracker::getInstance() {
    if (!instance) {
        instance = new MemoryTracker();
    }
    return instance;
}

MemoryTracker::MemoryTracker() {
    if (getHooksApi()->add_new_hook(&NewHook)) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Registered add hook");
        if (getHooksApi()->add_delete_hook(&DeleteHook)) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Registered delete hook");
            std::cout.flush();
            trackingAllocations = true;
            return;
        }
        std::cout.flush();
        getHooksApi()->remove_new_hook(&NewHook);
    }
    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Failed to register allocator hooks");
}

MemoryTracker::~MemoryTracker() {
    getHooksApi()->remove_new_hook(&NewHook);
    getHooksApi()->remove_delete_hook(&DeleteHook);
}

void MemoryTracker::getAllocatorStats(std::map<std::string, size_t> &allocator_stats) {
    int stats_size = getHooksApi()->get_stats_size();
    allocator_stat* stats = (allocator_stat*)calloc(stats_size, sizeof(allocator_stat));
    getHooksApi()->get_allocator_stats(stats);

    int i;
    for (i = 0; i < stats_size; i++) {
        std::string key((*(stats + i)).key);
        size_t value = (*(stats + i)).value;
        allocator_stats.insert(std::pair<std::string, size_t>(key, value));
        free((*(stats + i)).key);
    }
    if (stats) {
        free(stats);
    }
}

bool MemoryTracker::trackingMemoryAllocations() {
    return trackingAllocations;
}

void MemoryTracker::NewHook(const void* ptr, size_t size) {
    (void)size;
    if (ptr != NULL) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = getHooksApi()->get_allocation_size(p);
        ObjectRegistry::memoryAllocated(alloc);
    }
}

void MemoryTracker::DeleteHook(const void* ptr) {
    if (ptr != NULL) {
        void* p = const_cast<void*>(ptr);
        size_t alloc = getHooksApi()->get_allocation_size(p);
        ObjectRegistry::memoryDeallocated(alloc);
    }
}
