/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

/**
 * Use this file as an abstraction to the underlying hooks api
 */
#include <cstddef>

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
