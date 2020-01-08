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
#include "alloc_hooks.h"
#include <stdbool.h>

#include "memcached/visibility.h"
#include <platform/cb_malloc.h>


/* Irrespective of how jemalloc was configured on this platform,
* don't rename je_FOO to FOO.
*/
#define JEMALLOC_NO_RENAME

/*
 * If we are on Windows, we need to declare this to make sure
 * that dllexport is configured correctly
 */
#ifdef WIN32
#define DLLEXPORT
#endif

#include <jemalloc/jemalloc.h>
#include <logger/logger.h>

#if defined(HAVE_MEMALIGN)
#include <malloc.h>
#endif

/* jemalloc checks for this symbol, and it's contents for the config to use. */
JEMALLOC_EXPORT
const char* je_malloc_conf =
/* Enable background worker thread for asynchronous purging.
 * Background threads are non-functional in jemalloc 5.1.0 on macOS due to
 * implementation discrepancies between the background threads and mutexes.
 */
#ifndef __APPLE__
        "background_thread:true,"
#endif
        /* Use just one arena, instead of the default based on number of CPUs.
           Helps to minimize heap fragmentation. */
        "narenas:1,"
        /* Start with profiling enabled but inactive; this allows us to
           turn it on/off at runtime. */
        "prof:true,prof_active:false";

void JemallocHooks::initialize() {
    // No initialization required.
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
        LOG_WARNING(
                "jemalloc_release_free_memory() error {} - "
                "could not determine narenas.",
                err);

        return;
    }
    size_t mib[3]; /* Components in "arena.0.purge" MIB. */
    size_t miblen = sizeof(mib) / sizeof(mib[0]);
    err = je_mallctlnametomib("arena.0.purge", mib, &miblen);
    if (err != 0) {
        LOG_WARNING(
                "jemalloc_release_free_memory() error {} - "
                "could not lookup MIB.",
                err);
        return;
    }
    mib[1] = narenas;
    err = je_mallctlbymib(mib, miblen, NULL, 0, NULL, 0);
    if (err != 0) {
        LOG_WARNING(
                "jemalloc_release_free_memory() error {} - "
                "could not invoke arenas.N.purge.",
                err);
    }
}

