
#include "alloc_hooks.h"

static func_ptr addNewHook;
static func_ptr addDelHook;
static func_ptr removeNewHook;
static func_ptr removeDelHook;
static func_ptr getStatsProp;
static func_ptr getAllocSize;

static alloc_hooks_type type;

void init_no_hooks(void);
bool init_tcmalloc_hooks(void);

bool invalid_hook_function(void (*)(const void*, size_t));
size_t invalid_size_function(const char*, size_t*);

#define TCMALLOC_DLL "libtcmalloc_minimal-4.dll"

void init_alloc_hooks() {
    if (!init_tcmalloc_hooks()) {
        init_no_hooks();
        get_stderr_logger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Couldn't find allocator hooks for accurate memory tracking");
    }
}

bool init_tcmalloc_hooks(void) {
#ifndef __WIN32__
    void* handle = dlopen(NULL, RTLD_LAZY);
#else
    void* handle = dlopen(TCMALLOC_DLL, RTLD_LAZY);
#endif

    if (!handle) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Was unable to dlopen itself in order to activate allocator hooks: %s", dlerror());
        return false;
    }

    addNewHook.ptr = dlsym(handle, "MallocHook_AddNewHook");
    addDelHook.ptr = dlsym(handle, "MallocHook_AddDeleteHook");
    removeNewHook.ptr = dlsym(handle, "MallocHook_RemoveNewHook");
    removeDelHook.ptr = dlsym(handle, "MallocHook_RemoveDeleteHook");
    getStatsProp.ptr = dlsym(handle, "MallocExtension_GetNumericProperty");
    getAllocSize.ptr = dlsym(handle, "MallocExtension_GetAllocatedSize");

    if (addNewHook.ptr && addDelHook.ptr && removeNewHook.ptr && removeDelHook.ptr
        && getStatsProp.ptr && getAllocSize.ptr) {
        type = tcmalloc;
        return true;
    }
    return false;
}

void init_no_hooks(void) {
    addNewHook.func = (void *(*)())invalid_hook_function;
    addDelHook.func = (void *(*)())invalid_hook_function;
    removeNewHook.func = (void *(*)())invalid_hook_function;
    removeDelHook.func = (void *(*)())invalid_hook_function;
    getStatsProp.func = (void *(*)())invalid_size_function;
    getAllocSize.func = (void *(*)())invalid_size_function;
    type = unknown;
}

bool mc_add_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return (addNewHook.func)(hook) ? true : false;
}

bool mc_remove_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return (removeNewHook.func)(hook) ? true : false;
}

bool mc_add_delete_hook(void (*hook)(const void* ptr)) {
    return (addDelHook.func)(hook) ? true : false;
}

bool mc_remove_delete_hook(void (*hook)(const void* ptr)) {
    return (removeDelHook.func)(hook) ? true : false;
}

int mc_get_extra_stats_size() {
    if (type == tcmalloc) {
        return 3;
    }
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
    if (type == tcmalloc) {
        (getStatsProp.func)("generic.current_allocated_bytes", &(stats->allocated_size));
        (getStatsProp.func)("generic.heap_size", &(stats->heap_size));
        (getStatsProp.func)("tcmalloc.pageheap_free_bytes", &(stats->free_size));
        stats->fragmentation_size = stats->heap_size - stats->allocated_size - stats->free_size;

        strcpy(stats->ext_stats[0].key, "tcmalloc_unmapped_bytes");
        strcpy(stats->ext_stats[1].key, "tcmalloc_max_thread_cache_bytes");
        strcpy(stats->ext_stats[2].key, "tcmalloc_current_thread_cache_bytes");

        (getStatsProp.func)("tcmalloc.pageheap_unmapped_bytes",
                            &(stats->ext_stats[0].value));
        (getStatsProp.func)("tcmalloc.max_total_thread_cache_bytes",
                            &(stats->ext_stats[1].value));
        (getStatsProp.func)("tcmalloc.current_total_thread_cache_bytes",
                            &(stats->ext_stats[2].value));
    }
}

size_t mc_get_allocation_size(void* ptr) {
    return (size_t)(getAllocSize.func)(ptr);
}

bool invalid_hook_function(void (*hook)(const void* ptr, size_t size)) {
    return false;
}

size_t invalid_size_function(const char* property, size_t* value) {
    return 0;
}
