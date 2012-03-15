#include "config.h"

#include "memory_tracker.hh"

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

    static int mock_get_stats_size() {
        return 0;
    }

    static void mock_get_allocator_stats(allocator_stat*) {
        // Empty
    }

    static size_t mock_get_allocation_size(void*) {
        return 0;
    }

    static size_t mock_get_fragmented_size(void) {
        return 0;
    }

    static size_t mock_get_allocated_size(void) {
        return 0;
    }
}

ALLOCATOR_HOOKS_API* getHooksApi(void) {
    static ALLOCATOR_HOOKS_API hooksApi;
    hooksApi.add_new_hook = mock_add_new_hook;
    hooksApi.remove_new_hook = mock_remove_new_hook;
    hooksApi.add_delete_hook = mock_add_delete_hook;
    hooksApi.remove_delete_hook = mock_remove_delete_hook;
    hooksApi.get_stats_size = mock_get_stats_size;
    hooksApi.get_allocator_stats = mock_get_allocator_stats;
    hooksApi.get_allocation_size = mock_get_allocation_size;
    hooksApi.get_fragmented_size = mock_get_fragmented_size;
    hooksApi.get_allocated_size = mock_get_allocated_size;
    return &hooksApi;
}

bool MemoryTracker::trackingAllocations = false;
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
    return trackingAllocations;
}

