
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

typedef struct allocator_stat {
    char* key;
    size_t value;
} allocator_stat;

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
     * Returns the number of stats for the current allocator.
     */
    int (*get_stats_size)(void);
    /**
     * Obtains relevant statistics from the allocator. These statistics
     * may vary depending on which allocator is in use. This function
     * fills the empty array (passed as a parameter) with allocator
     * stats. In order to correctly ize your array use the
     * get_stats_size() function.
     */
    void (*get_allocator_stats)(allocator_stat*);

    /**
     * Returns the total bytes allocated by the allocator. This value
     * may be computed differently based on the allocator in use.
     */
    size_t (*get_allocation_size)(void*);

    /**
     * Gets the heap fragmentation in bytes from the allocator. This
     * value may be computed differently based on the allocator in
     * use.
     */
    size_t (*get_fragmented_size)(void);

    /**
     * Gets the amount of bytes allocated for a specific allocation
     * from the allocator in use.
     */
    size_t (*get_allocated_size)(void);

} ALLOCATOR_HOOKS_API;

#ifdef __cplusplus
}
#endif

#endif /* ALLOCATOR_HOOKS_H */
