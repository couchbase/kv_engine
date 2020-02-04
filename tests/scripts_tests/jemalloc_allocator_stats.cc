// adapted from example program at
// https://github.com/jemalloc/jemalloc/wiki/Getting-Started
#include <platform/cb_malloc.h>
#include <jemalloc/jemalloc.h>

int main() {
    for (size_t i = 1; i < 1000; i++) {
        void* x = cb_malloc(i * 100);
        (void)x;
    }

    // Dump allocator statistics to stderr.
    je_malloc_stats_print(nullptr, nullptr, nullptr);

    return 0;
}
