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

/*
 * Simple shared library which can be used to interpose malloc and
 * inject failures.
 *
 * Usage:
 *
 * 1. Configure Couchbase with badmalloc as the allocator -
 *     make EXTRA_CMAKE_OPTIONS=-DCOUCHBASE_MEMORY_ALLOCATOR=badmalloc
 * 2. Run tests as usual - malloc will randomly fail.
 *
 * To optionally control the behvaiour of badmalloc, the following environement
 * variables can be set:
 *
 *   BADMALLOC_FAILURE_RATIO=<float> - Configure how often malloc() / realloc()
 *                                     should return NULL.
 *   BADMALLOC_SEED=<unsigned int>   - Use a fixed seed for the random number
 *                                     generator which controls when to fail.
 *   BADMALLOC_GRACE_PERIOD=<unsigned int> - Allow the first N calls to
 *                                     malloc() / realloc() to succeed.
 */

#include "config.h"

#include "alloc_hooks.h"

#include <platform/backtrace.h>
#include <platform/visibility.h>

#include <dlfcn.h>
#include <memory>
#include <random>

#if defined(WIN32)
#error badmalloc not supported on Windows (not possible to interposse malloc).
#endif

// Determines if a re-entrant memory allocation call has been made; for example
// BadMalloc::malloc may result in a recursive call to malloc() if we perform
// a memory allocation in our handling of malloc.
// If this value is non-zero (i.e. we are in a recursive call) then the malloc
// always succeeds (i.e. we don't inject errors recursively).
//
// TODO: Make this thread-local - this will increase the instances that badmalloc
// can return failure. At the moment if two threads call malloc simultaneously
// then one of them will never have errors injected. Main reason it isn't already
// thread-local is that OS X clang doesn't support C++11 thread_local specifier :(
static std::atomic_int recursion_depth(0);

struct BadMalloc {
    BadMalloc()
        : rand_device(),
          gen(),
          failure_ratio(0.01),
          seed(rand_device()),
          grace_period(0)
    {
        // Parse configuration arguments (from env vars).
        char* failure_ratio_env = getenv("BADMALLOC_FAILURE_RATIO");
        if (failure_ratio_env != nullptr) {
            try {
                failure_ratio = std::stof(failure_ratio_env);
            } catch (std::exception& e) {
                fprintf(stderr, "badmalloc: Error parsing BADMALLOC_FAILURE_RATIO: %s\n",
                        e.what());
            }
        }

        char* seed_env = getenv("BADMALLOC_SEED");
        if (seed_env != nullptr) {
            try {
                seed = std::stoul(seed_env);
            } catch (std::exception& e) {
                fprintf(stderr, "badmalloc: Error parsing BADMALLOC_SEED: %s\n",
                        e.what());
            }
        }

        char* grace_period_env = getenv("BADMALLOC_GRACE_PERIOD");
        if (grace_period_env != nullptr) {
            try {
                grace_period = std::stoul(grace_period_env);
            } catch (std::exception& e) {
                fprintf(stderr, "badmalloc: Error parsing BADMALLOC_GRACE_PERIOD: %s\n",
                        e.what());
            }
        }

        // Setup our RNG.
        gen.seed(seed);
        distribution.reset(new std::bernoulli_distribution(failure_ratio));

        fprintf(stderr, "badmalloc: Loaded. Using failure likelihood:%f, seed:%u, grace_period:%u\n",
                distribution->p(), seed, grace_period);
    }

    bool shouldFail() {
        // Allow the first N operations to always succeed (initialization, etc).
        if (grace_period > 0) {
            grace_period--;
            return false;
        }
        // Also don't fail if we have been recursively been called.
        if (recursion_depth > 1) {
            return false;
        }
        return distribution->operator()(gen);
    }

    std::random_device rand_device;
    std::mt19937 gen;
    std::unique_ptr<std::bernoulli_distribution> distribution;

    // Configuration parameters
    float failure_ratio;
    unsigned int seed;
    unsigned int grace_period;
};

std::unique_ptr<BadMalloc> badMalloc;

typedef void* (*malloc_t)(size_t);
typedef void* (*realloc_t)(void*, size_t);

/* Create BadMalloc (and hence start returning malloc failures only when
 * init_alloc_hooks() is called - this ensures that anything pre-main()
 * - C++ static initialization is all completed successfully.
 */
void init_alloc_hooks() {
    badMalloc.reset(new BadMalloc());
}

extern "C" {

/* Exported malloc functions */
EXPORT_SYMBOL void* malloc(size_t size) {
    static malloc_t real_malloc = (malloc_t)dlsym(RTLD_NEXT, "malloc");

    recursion_depth++;
    if (badMalloc != nullptr && badMalloc->shouldFail()) {
        fprintf(stderr, "badmalloc: Failing malloc of size %" PRIu64 "\n",
                uint64_t(size));
        print_backtrace_to_file(stderr);
        recursion_depth--;
        return nullptr;
    }
    recursion_depth--;
    return real_malloc(size);
}

EXPORT_SYMBOL void* realloc(void* ptr, size_t size) {
    static realloc_t real_realloc = (realloc_t)dlsym(RTLD_NEXT, "realloc");

    recursion_depth++;
    if (badMalloc != nullptr && badMalloc->shouldFail()) {
        fprintf(stderr, "badmalloc: Failing realloc of size %" PRIu64 "\n",
                uint64_t(size));
        print_backtrace_to_file(stderr);
        recursion_depth--;
        return nullptr;
    }
    recursion_depth--;
    return real_realloc(ptr, size);
}

} // extern "C"

/*
 * Various alloc hooks. None of these are actually used in badmalloc.
*/
bool mc_add_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_remove_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_add_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

bool mc_remove_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

int mc_get_extra_stats_size() {
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
}

size_t mc_get_allocation_size(const void* ptr) {
    return 0;
}

void mc_get_detailed_stats(char* buffer, int size) {
    // empty
}

void mc_release_free_memory() {
    // empty
}

bool mc_enable_thread_cache(bool enable) {
    return true;
}

bool mc_get_allocator_property(const char* name, size_t* value) {
    return false;
}

bool mc_set_allocator_property(const char* name, size_t value) {
    return false;
}
