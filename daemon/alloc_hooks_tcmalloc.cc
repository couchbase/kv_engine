/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/**
 * This file defines the memory allocation hooks used when TCMalloc
 * is used. Note that the Couchbase alloc hooks API was designed around
 * the tcmalloc api, so its a pretty good 1:1 mapping ;-)
 */
#include "config.h"

#include "alloc_hooks_tcmalloc.h"
#include <cstring>
#include <stdbool.h>
#include "memcached/extension_loggers.h"

#include <gperftools/malloc_extension_c.h>
#include <gperftools/malloc_hook_c.h>
#include <platform/cb_malloc.h>

void TCMallocHooks::initialize() {
    // TCMalloc's aggressive decommit setting has a significant performance
    // impact on us; and from gperftools v2.4 it is enabled by default.
    // Turn it off.
    if (!MallocExtension_SetNumericProperty
        ("tcmalloc.aggressive_memory_decommit", 0)) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to disable tcmalloc.aggressive_memory_decommit");
    }
}

bool TCMallocHooks::add_new_hook(void(*hook)(const void* ptr, size_t size)) {
    return cb_add_new_hook(hook);
}

bool TCMallocHooks::remove_new_hook(void(*hook)(const void* ptr, size_t size)) {
    return cb_remove_new_hook(hook);
}

bool TCMallocHooks::add_delete_hook(void(*hook)(const void* ptr)) {
    return cb_add_delete_hook(hook);
}

bool TCMallocHooks::remove_delete_hook(void(*hook)(const void* ptr)) {
    return cb_remove_delete_hook(hook);
}

int TCMallocHooks::get_extra_stats_size() {
    return 3;
}

void TCMallocHooks::get_allocator_stats(allocator_stats* stats) {
    MallocExtension_GetNumericProperty("generic.current_allocated_bytes",
                                       &(stats->allocated_size));
    MallocExtension_GetNumericProperty("generic.heap_size",
                                       &(stats->heap_size));

    // Free memory is sum of:
    //   free, mapped bytes   (tcmalloc.pageheap_free_bytes)
    // & free, unmapped bytes (tcmalloc.pageheap_unmapped_bytes)
    MallocExtension_GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                       &(stats->free_mapped_size));
    MallocExtension_GetNumericProperty("tcmalloc.pageheap_unmapped_bytes",
                                       &(stats->free_unmapped_size));

    stats->fragmentation_size = stats->heap_size
                                - stats->allocated_size
                                - stats->free_mapped_size
                                - stats->free_unmapped_size;

    strcpy(stats->ext_stats[0].key, "tcmalloc_max_thread_cache_bytes");
    strcpy(stats->ext_stats[1].key, "tcmalloc_current_thread_cache_bytes");
    strcpy(stats->ext_stats[2].key, "tcmalloc.aggressive_memory_decommit");

    MallocExtension_GetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                       &(stats->ext_stats[0].value));
    MallocExtension_GetNumericProperty(
        "tcmalloc.current_total_thread_cache_bytes",
        &(stats->ext_stats[1].value));
    MallocExtension_GetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                       &(stats->ext_stats[2].value));
}

size_t TCMallocHooks::get_allocation_size(const void* ptr) {
    // Only try to get allocation size if we allocated the object
    if (MallocExtension_GetOwnership(ptr) == MallocExtension_kOwned) {
        return MallocExtension_GetAllocatedSize(ptr);
    }

    return 0;
}

void TCMallocHooks::get_detailed_stats(char* buffer, int size) {
    MallocExtension_GetStats(buffer, size);
}

void TCMallocHooks::release_free_memory() {
    MallocExtension_ReleaseFreeMemory();
}

bool TCMallocHooks::enable_thread_cache(bool enable) {
    // Not supported with TCMalloc - thread cache is always enabled.
    return true;
}

bool TCMallocHooks::get_allocator_property(const char* name, size_t* value) {
    return MallocExtension_GetNumericProperty(name, value);
}

bool TCMallocHooks::set_allocator_property(const char* name, size_t value) {
    return MallocExtension_SetNumericProperty(name, value);
}
