
#include "alloc_hooks.h"
#include <gperftools/malloc_extension_c.h>
#include <gperftools/malloc_hook_c.h>

bool mc_add_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return MallocHook_AddNewHook(hook) ? true : false;
}

bool mc_remove_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return MallocHook_RemoveNewHook(hook) ? true : false;
}

bool mc_add_delete_hook(void (*hook)(const void* ptr)) {
    return MallocHook_AddDeleteHook(hook) ? true : false;
}

bool mc_remove_delete_hook(void (*hook)(const void* ptr)) {
    return MallocHook_RemoveDeleteHook(hook) ? true : false;
}

int mc_get_extra_stats_size() {
    return 3;
}

void mc_get_allocator_stats(allocator_stats* stats) {
    MallocExtension_GetNumericProperty("generic.current_allocated_bytes",
                                       &(stats->allocated_size));
    MallocExtension_GetNumericProperty("generic.heap_size",
                                       &(stats->heap_size));
    MallocExtension_GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                       &(stats->free_size));
    stats->fragmentation_size = stats->heap_size - stats->allocated_size
                                - stats->free_size;

    strcpy(stats->ext_stats[0].key, "tcmalloc_unmapped_bytes");
    strcpy(stats->ext_stats[1].key, "tcmalloc_max_thread_cache_bytes");
    strcpy(stats->ext_stats[2].key, "tcmalloc_current_thread_cache_bytes");

    MallocExtension_GetNumericProperty("tcmalloc.pageheap_unmapped_bytes",
                                       &(stats->ext_stats[0].value));
    MallocExtension_GetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                       &(stats->ext_stats[1].value));
    MallocExtension_GetNumericProperty("tcmalloc.current_total_thread_cache_bytes",
                                       &(stats->ext_stats[2].value));
}

size_t mc_get_allocation_size(void* ptr) {
    return MallocExtension_GetAllocatedSize(ptr);
}

void mc_get_detailed_stats(char* buffer, int size) {
        MallocExtension_GetStats(buffer, size);
}
