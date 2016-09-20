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
#include "config.h"
#include "alloc_hooks.h"
#include <stdbool.h>

#include "memcached/visibility.h"
#include <memcached/extension_loggers.h>

/* Irrespective of how jemalloc was configured on this platform,
* don't rename je_FOO to FOO.
*/
#define JEMALLOC_NO_RENAME
#include <jemalloc/jemalloc.h>
#if defined(WIN32)
#    error Memory tracking not supported with jemalloc on Windows.
#elif defined(__APPLE__)
/* memory tracking implemented using custom malloc zone: */
#    include "darwin_zone.h"
#else
/* assume some other *ix-style OS which permits malloc/free symbol interposing. */
#    define INTERPOSE_MALLOC 1
#endif

#if defined(HAVE_MEMALIGN)
#include <malloc.h>
#endif

/******************************************************************************
 * jemalloc memory tracking support.
 * jemalloc (unlike TCmalloc) has no builtin support for this, so instead we
 * use our own malloc wrapper functions which will check for the presence of
 * registered hooks and call if necessary.
 *
 *****************************************************************************/

/* jemalloc checks for this symbol, and it's contents for the config to use. */
const char* je_malloc_conf =
    /* Use just one arena, instead of the default based on number of CPUs.
       Helps to minimize heap fragmentation. */
    "narenas:1";

static malloc_new_hook_t new_hook = NULL;
static malloc_delete_hook_t delete_hook = NULL;

#if defined(INTERPOSE_MALLOC)

static inline void invoke_new_hook(void* ptr, size_t size) {
    if (new_hook != NULL) {
        new_hook(ptr, size);
    }
}

static inline void invoke_delete_hook(void* ptr) {
    if (delete_hook != NULL) {
        delete_hook(ptr);
    }
}

extern "C" {

#if defined(__sun) || defined(__FreeBSD__)
#define throwspec
#else
#define throwspec throw()
#endif

MEMCACHED_PUBLIC_API void* malloc(size_t size) throwspec {
    void* ptr = je_malloc(size);
    invoke_new_hook(ptr, size);
    return ptr;
}

MEMCACHED_PUBLIC_API void* calloc(size_t nmemb, size_t size) throwspec {
    void* ptr = je_calloc(nmemb, size);
    invoke_new_hook(ptr, nmemb * size);
    return ptr;
}

MEMCACHED_PUBLIC_API void* realloc(void* ptr, size_t size) throwspec {
    invoke_delete_hook(ptr);
    void* result = je_realloc(ptr, size);
    invoke_new_hook(result, size);
    return result;
}

MEMCACHED_PUBLIC_API void free(void* ptr) throwspec {
    invoke_delete_hook(ptr);
    je_free(ptr);
}

#if defined(HAVE_MEMALIGN)
MEMCACHED_PUBLIC_API void *memalign(size_t alignment, size_t size) throwspec {
    void* result = je_memalign(alignment, size);
    invoke_new_hook(result, size);
    return result;
}
#endif

MEMCACHED_PUBLIC_API int posix_memalign(void **memptr, size_t alignment,
                                        size_t size) throwspec {
    int result = je_posix_memalign(memptr, alignment, size);
    invoke_new_hook(*memptr, size);
    return result;
}

#undef throwspec

} // extern "C"

#endif /* INTERPOSE_MALLOC */

static int jemalloc_get_stats_prop(const char* property, size_t* value) {
    size_t size = sizeof(*value);
    return je_mallctl(property, value, &size, NULL, 0);
}

struct write_state {
    char* buffer;
    int remaining;
    bool cropped;
};
static const char cropped_error[] = "=== Exceeded buffer size - output cropped ===\n";

/* Write callback used by jemalloc's malloc_stats_print() below */
static void write_cb(void* opaque, const char* msg) {
    int len;
    struct write_state* st = (struct write_state*) opaque;
    if (st->cropped) {
        /* already cropped output - nothing to do. */
        return;
    }
    len = snprintf(st->buffer, st->remaining, "%s", msg);
    if (len < 0) {
        /*
         * snprintf _FAILED_. Terminate the buffer where it used to be
         * and ignore the rest
         */
        st->buffer[0] = '\0';
        return;
    }
    if (len > st->remaining) {
        /* insufficient space - have to crop output. Note we reserved enough
           space (see below) to be able to write an error if this occurs. */
        sprintf(st->buffer, cropped_error);
        st->cropped = true;
        return;
    }
    st->buffer += len;
    st->remaining -= len;
}

void JemallocHooks::initialize() {
#if defined(__APPLE__)
    // Register our wrapper malloc zone to allow us to track mem_used
    register_wrapper_zone(&new_hook, &delete_hook);
#endif
}

bool JemallocHooks::add_new_hook(void (* hook)(const void* ptr, size_t size)) {
    if (new_hook == NULL) {
        new_hook = hook;
        return true;
    } else {
        return false;
    }
}

bool JemallocHooks::remove_new_hook(void (* hook)(const void* ptr, size_t size)) {
    if (new_hook == hook) {
        new_hook = NULL;
        return true;
    } else {
        return false;
    }
}

bool JemallocHooks::add_delete_hook(void (* hook)(const void* ptr)) {
    if (delete_hook == NULL) {
        delete_hook = hook;
        return true;
    } else {
        return false;
    }
}

bool JemallocHooks::remove_delete_hook(void (* hook)(const void* ptr)) {
    if (delete_hook == hook) {
        delete_hook = NULL;
        return true;
    } else {
        return false;
    }
}

int JemallocHooks::get_extra_stats_size() {
    return 0;
}

void JemallocHooks::get_allocator_stats(allocator_stats* stats) {
    size_t epoch = 1;
    size_t sz = sizeof(epoch);
    /* jemalloc can cache its statistics - force a refresh */
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    jemalloc_get_stats_prop("stats.allocated", &(stats->allocated_size));
    jemalloc_get_stats_prop("stats.mapped", &(stats->heap_size));

    /* No explicit 'free' memory measurements for jemalloc; however: */

    /* 1. free_mapped_size is approximately the same as jemalloc's dirty
     * pages * page_size.
     */

    /* lookup current number of arenas, then use that to lookup
     * 'stats.arenas.NARENAS.pdirty' - i.e. number of dirty pages
     * across all arenas.
     */
    unsigned int narenas;
    size_t len = sizeof(narenas);
    if (je_mallctl("arenas.narenas", &narenas, &len, NULL, 0) != 0) {
        return;
    }
    size_t mib[4];
    size_t miblen = sizeof(mib) / sizeof(mib[0]);
    if (je_mallctlnametomib("stats.arenas.0.pdirty", mib, &miblen) != 0) {
        return;
    }
    mib[2] = narenas;
    size_t pdirty;
    len = sizeof(pdirty);
    if (je_mallctlbymib(mib, miblen, &pdirty, &len, NULL, 0) != 0) {
        return;
    }

    /* convert to pages to bytes */
    size_t psize;
    len = sizeof(psize);
    if (je_mallctl("arenas.page", &psize, &len, NULL, 0) != 0) {
        return;
    }
    stats->free_mapped_size = pdirty * psize;

    /* 2. free_unmapped_size is approximately:
     * "mapped - active - stats.arenas.<i>.pdirty"
     */
    size_t active_bytes;
    jemalloc_get_stats_prop("stats.active", &active_bytes);
    stats->free_unmapped_size = stats->heap_size
                                - active_bytes
                                - stats->free_mapped_size;

    stats->fragmentation_size = stats->heap_size
                                - stats->allocated_size
                                - stats->free_mapped_size;
}

size_t JemallocHooks::get_allocation_size(const void* ptr) {
    /* je_malloc_usable_size on my linux masks this down to
     * malloc_usable_size causing it to omit a compiler warning.
     * Let's just nuke away the const here, as you may always
     * pass a non-const pointer to a function who accepts a
     * const pointer
     */
    return je_malloc_usable_size((void*) ptr);
}

void JemallocHooks::get_detailed_stats(char* buffer, int size) {
    struct write_state st;
    st.buffer = buffer;
    st.cropped = false;
    /* reserve enough space to write out an error if the output is cropped. */
    st.remaining = size - sizeof(cropped_error);
    je_malloc_stats_print(write_cb, &st, "a"/* omit per-arena stats*/);
}

void JemallocHooks::release_free_memory() {
    /* Note: jemalloc doesn't necessarily free this memory
     * immediately, but it will schedule to be freed as soon as is
     * possible.
     *
     * See: http://www.canonware.com/download/jemalloc/jemalloc-latest/doc/jemalloc.html,
     * specifically mallctl() for informaiton how the MIB api works.
     */

    /* lookup current number of arenas, then use that to invoke
     * 'arenas.NARENAS.purge' (replacing the '0' with NARENAS) to
     * release any dirty pages back to the OS.
     */
    unsigned int narenas;
    size_t len = sizeof(narenas);
    int err = je_mallctl("arenas.narenas", &narenas, &len, NULL, 0);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                     "could not determine narenas.", err);
        return;
    }
    size_t mib[3]; /* Components in "arena.0.purge" MIB. */
    size_t miblen = sizeof(mib) / sizeof(mib[0]);
    err = je_mallctlnametomib("arena.0.purge", mib, &miblen);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                     "could not lookup MIB.", err);
        return;
    }
    mib[1] = narenas;
    err = je_mallctlbymib(mib, miblen, NULL, 0, NULL, 0);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                     "could not invoke arenas.N.purge.", err);
    }
}

bool JemallocHooks::enable_thread_cache(bool enable) {
    bool old;
    size_t size = sizeof(old);
    int err = je_mallctl("thread.tcache.enabled", &old, &size, &enable,
                         sizeof(enable));
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_enable_thread_cache(%s) error %d",
                                 (enable ? "true" : "false"), err);
    }
    return old;
}

bool JemallocHooks::get_allocator_property(const char* name, size_t* value) {
    return jemalloc_get_stats_prop(name, value);
}

bool JemallocHooks::set_allocator_property(const char* name, size_t value) {
    /* Not yet implemented */
    return 0;
}
