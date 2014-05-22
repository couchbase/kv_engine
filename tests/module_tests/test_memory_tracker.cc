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

#include "memory_tracker.h"

/**
 * THIS FILE SHOULD NEVER ACTUALLY BE RUN. IT IS JUST USED TO GET SOME OF OUR
 * TESTS TO COMPILE.
 */

extern "C" {
    static bool mock_add_new_hook(void (*)(const void* ptr, size_t size)) {
        return false;
    }

    static bool mock_remove_new_hook(void (*)(const void* ptr, size_t size)) {
        return false;
    }

    static bool mock_add_delete_hook(void (*)(const void* ptr)) {
        return false;
    }

    static bool mock_remove_delete_hook(void (*)(const void* ptr)) {
        return false;
    }

    static int mock_get_extra_stats_size() {
        return 0;
    }

    static void mock_get_allocator_stats(allocator_stats*) {
        // Empty
    }

    static size_t mock_get_allocation_size(const void*) {
        return 0;
    }
}

ALLOCATOR_HOOKS_API* getHooksApi(void) {
    static ALLOCATOR_HOOKS_API hooksApi;
    hooksApi.add_new_hook = mock_add_new_hook;
    hooksApi.remove_new_hook = mock_remove_new_hook;
    hooksApi.add_delete_hook = mock_add_delete_hook;
    hooksApi.remove_delete_hook = mock_remove_delete_hook;
    hooksApi.get_extra_stats_size = mock_get_extra_stats_size;
    hooksApi.get_allocator_stats = mock_get_allocator_stats;
    hooksApi.get_allocation_size = mock_get_allocation_size;
    return &hooksApi;
}

bool MemoryTracker::tracking = false;
MemoryTracker *MemoryTracker::instance = 0;

MemoryTracker *MemoryTracker::getInstance() {
    if (!instance) {
        instance = new MemoryTracker();
    }
    return instance;
}

MemoryTracker::MemoryTracker() {
    // Do nothing
}

MemoryTracker::~MemoryTracker() {
    // Do nothing
}

void MemoryTracker::getAllocatorStats(std::map<std::string, size_t> &allocator_stats) {
    (void) allocator_stats;
    // Do nothing
}

bool MemoryTracker::trackingMemoryAllocations() {
    // This should ALWAYS return false
    return tracking;
}

size_t MemoryTracker::getFragmentation() {
    return 0;
}

size_t MemoryTracker::getTotalBytesAllocated() {
    return 0;
}

size_t MemoryTracker::getTotalHeapBytes() {
    return 0;
}

