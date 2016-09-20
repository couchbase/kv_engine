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
#include <platform/cb_malloc.h>


/* Irrespective of how jemalloc was configured on this platform,
* don't rename je_FOO to FOO.
*/
#define JEMALLOC_NO_RENAME
#include <jemalloc/jemalloc.h>

#if defined(HAVE_MEMALIGN)
#include <malloc.h>
#endif

/* jemalloc checks for this symbol, and it's contents for the config to use. */
const char* je_malloc_conf =
    /* Use just one arena, instead of the default based on number of CPUs.
       Helps to minimize heap fragmentation. */
    "narenas:1";

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
    // No initialization required.
}

bool JemallocHooks::add_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return cb_add_new_hook(hook);
}

bool JemallocHooks::remove_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return cb_remove_new_hook(hook);
}

bool JemallocHooks::add_delete_hook(void (* hook)(const void* ptr)) {
    return cb_add_delete_hook(hook);
}

bool JemallocHooks::remove_delete_hook(void (* hook)(const void* ptr)) {
    return cb_remove_delete_hook(hook);
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
