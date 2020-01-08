/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

/**
 * Use this file as an abstraction to the underlying hooks api
 */

#include <stdint.h>
#include <stdlib.h>
#include <vector>

#ifndef __cplusplus
#include <stdbool.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct allocator_ext_stat {
    char key[48];
    size_t value;
} allocator_ext_stat;

typedef struct allocator_stats {
    /* Bytes of memory allocated by the application. Doesn't include allocator
       overhead or fragmentation. */
    size_t allocated_size;

    /* Bytes of memory reserved by the allocator */
    size_t heap_size;

    /* mem occupied by allocator metadata */
    size_t metadata_size;

    /* Memory overhead of the allocator*/
    size_t fragmentation_size;

    /* memory that has not been given back to the OS */
    size_t retained_size;

    /* max bytes in resident pages mapped by the allocator*/
    size_t resident_size;

    /* Vector of additional allocator-specific statistics */
    std::vector<allocator_ext_stat> ext_stats;

} allocator_stats;

/**
 * Engine allocator hooks for memory tracking.
 */
struct ServerAllocatorIface {
    /**
     * Add a hook into the memory allocator that will be called each
     * time memory is allocated from the heap. Returns true if the
     * hook was successfully registered with the allocator. Returns
     * false if the hook was not registered properly or if a hooks
     * API doesn't exist for the allocator in use.
     */
    bool (*add_new_hook)(void (*)(const void* ptr, size_t size));

    /**
     * Remove a hook from the memory allocator that will be called each
     * time memory is allocated from the heap. Returns true if the hook
     * was registered and removed and false if the specified hook is not
     * registered or if a hooks API doesn't exist for the allocator.
     */
    bool (*remove_new_hook)(void (*)(const void* ptr, size_t size));

    /**
     * Add a hook into the memory allocator that will be called each
     * time memory is freed from the heap. Returns true if the hook
     * was successfully registered with the allocator. Returns false
     * if the hook was not registered properly or if a hooks API
     * doesn't exist for the allocator in use.
     */
    bool (*add_delete_hook)(void (*)(const void* ptr));

    /**
     * Remove a hook from the memory allocator that will be called each
     * time memory is freed from the heap. Returns true if the hook was
     * registered and removed and false if the specified hook is not
     * registered or if a hooks API doesn't exist for the allocator.
     */
    bool (*remove_delete_hook)(void (*)(const void* ptr));
};

#ifdef __cplusplus
}
#endif
