/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

// Expose the compile-time selected allocator hooks implementation:

#include "alloc_hooks_dummy.h"
#include "alloc_hooks_jemalloc.h"

#if defined(HAVE_JEMALLOC)
using AllocHooks = JemallocHooks;
#else
using AllocHooks = DummyAllocHooks;
#endif

/*
 * Compatibility functions for existing code to aid in transition.
 */

static inline void init_alloc_hooks(void) {
    AllocHooks::initialize();
}
static inline bool mc_add_new_hook(malloc_new_hook_t f) {
    return AllocHooks::add_new_hook(f);
}
static inline bool mc_remove_new_hook(malloc_new_hook_t f) {
    return AllocHooks::remove_new_hook(f);
}
static inline bool mc_add_delete_hook(malloc_delete_hook_t f) {
    return AllocHooks::add_delete_hook(f);
}
static inline bool mc_remove_delete_hook(malloc_delete_hook_t f) {
    return AllocHooks::remove_delete_hook(f);
}
static inline void mc_get_allocator_stats(allocator_stats* stats) {
    AllocHooks::get_allocator_stats(stats);
}
static inline int mc_get_extra_stats_size() {
    return AllocHooks::get_extra_stats_size();
}
static inline size_t mc_get_allocation_size(const void* ptr) {
    return AllocHooks::get_allocation_size(ptr);
}
static inline void mc_get_detailed_stats(char* buffer, int size) {
    return AllocHooks::get_detailed_stats(buffer, size);
}
static inline void mc_release_free_memory(void) {
    AllocHooks::release_free_memory();
}
static inline bool mc_enable_thread_cache(bool enable) {
    return AllocHooks::enable_thread_cache(enable);
}
