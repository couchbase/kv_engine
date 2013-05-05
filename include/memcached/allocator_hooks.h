/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * Use this file as an abstraction to the underlying hooks api
 */

#ifndef ALLOCATOR_HOOKS_H
#define ALLOCATOR_HOOKS_H

#include <stdint.h>

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
    size_t allocated_size;
    size_t heap_size;
    size_t free_size;
    size_t fragmentation_size;
    allocator_ext_stat *ext_stats;
    size_t ext_stats_size;
} allocator_stats;

/**
 * Engine allocator hooks for memory tracking.
 */
typedef struct engine_allocator_hooks_v1 {

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

    /**
     * Returns the number of extra stats for the current allocator.
     */
    int (*get_extra_stats_size)(void);

    /**
     * Obtains relevant statistics from the allocator. Every allocator
     * is required to return total allocated bytes, total heap bytes,
     * total free bytes, and toal fragmented bytes. An allocator will
     * also provide a varying number of allocator specific stats
     */
    void (*get_allocator_stats)(allocator_stats*);

    /**
     * Returns the total bytes allocated by the allocator. This value
     * may be computed differently based on the allocator in use.
     */
    size_t (*get_allocation_size)(void*);

    /**
     * Fills a buffer with special detailed allocator stats.
     */
    void (*get_detailed_stats)(char*, int);

} ALLOCATOR_HOOKS_API;

#ifdef __cplusplus
}
#endif

#endif /* ALLOCATOR_HOOKS_H */
