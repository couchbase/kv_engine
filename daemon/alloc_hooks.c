/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "alloc_hooks.h"

#ifndef DONT_HAVE_TCMALLOC
#include <gperftools/malloc_extension_c.h>
#include <gperftools/malloc_hook_c.h>
#endif

static int (*addNewHook)(void (*hook)(const void *ptr, size_t size));
static int (*removeNewHook)(void (*hook)(const void *ptr, size_t size));
static int (*addDelHook)(void (*hook)(const void *ptr));
static int (*removeDelHook)(void (*hook)(const void *ptr));
static int (*getStatsProp)(const char* property, size_t* value);
static size_t (*getAllocSize)(const void *ptr);
static void (*getDetailedStats)(char *buffer, int nbuffer);

static alloc_hooks_type type = none;

#ifndef DONT_HAVE_TCMALLOC
static void init_tcmalloc_hooks(void) {
    addNewHook = MallocHook_AddNewHook;
    removeNewHook = MallocHook_RemoveNewHook;
    addDelHook = MallocHook_AddDeleteHook;
    removeDelHook = MallocHook_RemoveDeleteHook;
    getStatsProp = MallocExtension_GetNumericProperty;
    getAllocSize = MallocExtension_GetAllocatedSize;
    getDetailedStats = MallocExtension_GetStats;
    type = tcmalloc;
}
#else
static int invalid_addrem_new_hook(void (*hook)(const void *ptr, size_t size)) {
    (void)hook;
    return -1;
}

static int invalid_addrem_del_hook(void (*hook)(const void *ptr)) {
    (void)hook;
    return -1;
}

static int invalid_get_stats_prop(const char* property, size_t* value) {
    (void)property;
    (void)value;
    return -1;
}

static size_t invalid_get_alloc_size(const void *ptr) {
    (void)ptr;
    return 0;
}

static void invalid_get_detailed_stats(char *buffer, int nbuffer) {
    (void)buffer;
    (void)nbuffer;
}

static void init_no_hooks(void) {
    addNewHook = invalid_addrem_new_hook;
    removeNewHook = invalid_addrem_new_hook;
    addDelHook = invalid_addrem_del_hook;
    removeDelHook = invalid_addrem_del_hook;
    getStatsProp = invalid_get_stats_prop;
    getAllocSize = invalid_get_alloc_size;
    getDetailedStats = invalid_get_detailed_stats;
    type = none;
}
#endif

void init_alloc_hooks() {
#ifndef DONT_HAVE_TCMALLOC
    init_tcmalloc_hooks();
#else
    init_no_hooks();
    get_stderr_logger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Couldn't find allocator hooks for accurate memory tracking");
#endif
}

bool mc_add_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return addNewHook(hook) ? true : false;
}

bool mc_remove_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return removeNewHook(hook) ? true : false;
}

bool mc_add_delete_hook(void (*hook)(const void* ptr)) {
    return addDelHook(hook) ? true : false;
}

bool mc_remove_delete_hook(void (*hook)(const void* ptr)) {
    return removeDelHook(hook) ? true : false;
}

int mc_get_extra_stats_size() {
    if (type == tcmalloc) {
        return 3;
    }
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
    if (type == tcmalloc) {
        getStatsProp("generic.current_allocated_bytes", &(stats->allocated_size));
        getStatsProp("generic.heap_size", &(stats->heap_size));
        getStatsProp("tcmalloc.pageheap_free_bytes", &(stats->free_size));
        stats->fragmentation_size = stats->heap_size - stats->allocated_size - stats->free_size;

        strcpy(stats->ext_stats[0].key, "tcmalloc_unmapped_bytes");
        strcpy(stats->ext_stats[1].key, "tcmalloc_max_thread_cache_bytes");
        strcpy(stats->ext_stats[2].key, "tcmalloc_current_thread_cache_bytes");

        getStatsProp("tcmalloc.pageheap_unmapped_bytes",
                            &(stats->ext_stats[0].value));
        getStatsProp("tcmalloc.max_total_thread_cache_bytes",
                            &(stats->ext_stats[1].value));
        getStatsProp("tcmalloc.current_total_thread_cache_bytes",
                            &(stats->ext_stats[2].value));
    }
}

size_t mc_get_allocation_size(void* ptr) {
    return getAllocSize(ptr);
}

void mc_get_detailed_stats(char* buffer, int size) {
    getDetailedStats(buffer, size);
}

alloc_hooks_type get_alloc_hooks_type(void) {
    return type;
}
