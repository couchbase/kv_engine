// adapted from example program at
// https://github.com/jemalloc/jemalloc/wiki/Getting-Started
#include <platform/cb_malloc.h>
#include <jemalloc/jemalloc.h>

#include <array>

int main() {
    std::array<void*, 1000> allocations;
    // allocate a variety of different sizes via cb_malloc (so that they do go
    // via jemalloc when configured)
    for (size_t i = 0; i < 1000; i++) {
        allocations[i] = cb_malloc(i * 100);
    }

    // Dump allocator statistics to stderr.
    je_malloc_stats_print(nullptr, nullptr, nullptr);

    // And clean up to avoid ASAN warnings
    for (size_t i = 0; i < 1000; i++) {
        cb_free(allocations[i]);
    }

    return 0;
}
