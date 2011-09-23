/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "tcmalloc_stats.hh"

void TCMallocStats::getStats(std::map<std::string, size_t> &tc_stats) {
    size_t allocated_memory = 0;
    size_t heap_size = 0;
    size_t pageheap_free_bytes = 0;
    size_t pageheap_unmapped_bytes = 0;
    size_t max_thread_cache_bytes = 0;
    size_t current_thread_cache_bytes = 0;

    MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes",
                                                    &allocated_memory);
    MallocExtension::instance()->GetNumericProperty("generic.heap_size",
                                                    &heap_size);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                                    &pageheap_free_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_unmapped_bytes",
                                                    &pageheap_unmapped_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                                    &max_thread_cache_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.current_total_thread_cache_bytes",
                                                    &current_thread_cache_bytes);

    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_allocated_bytes",
                                                   allocated_memory));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_heap_size",
                                                   heap_size));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_free_bytes",
                                                   pageheap_free_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_unmapped_bytes",
                                                   pageheap_unmapped_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_max_thread_cache_bytes",
                                                   max_thread_cache_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_current_thread_cache_bytes",
                                                   current_thread_cache_bytes));
}
