/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Support for replacing the default malloc zone on OS X, when using jemalloc
 * as the memory allocator. This is needed to allow us to insert wrappers
 * around (je)malloc & friends to perform allocation tracking for mem_used.
 *
 * Based on zone.c from jemalloc to perform similar functionality.
 */

/*
jemalloc is released under the terms of the following BSD-derived license:

Copyright (C) 2002-2014 Jason Evans <jasone@canonware.com>.
All rights reserved.
Copyright (C) 2007-2012 Mozilla Foundation.  All rights reserved.
Copyright (C) 2009-2014 Facebook, Inc.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice(s),
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice(s),
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#if !defined(__APPLE__)
#error "This source file is for Darwin (OS X)"
#endif

#include "darwin_zone.h"

#include <malloc/malloc.h>

/******************************************************************************/
/* Data. */

static malloc_zone_t zone;
static malloc_zone_t* default_zone;

static malloc_new_hook_t* new_hook = NULL;
static malloc_delete_hook_t* delete_hook = NULL;

/******************************************************************************/
/* Function prototypes for non-inline static functions. */

static size_t zone_size(malloc_zone_t *zone, const void *ptr);
static void *zone_malloc(malloc_zone_t *zone, size_t size);
static void *zone_calloc(malloc_zone_t *zone, size_t num, size_t size);
static void *zone_valloc(malloc_zone_t *zone, size_t size);
static void zone_free(malloc_zone_t *zone, void *ptr);
static void *zone_realloc(malloc_zone_t *zone, void *ptr, size_t size);
static void *zone_memalign(malloc_zone_t *zone, size_t alignment,
    size_t size);
static void zone_free_definite_size(malloc_zone_t *zone, void *ptr,
    size_t size);
static void zone_destroy(malloc_zone_t *zone);

static inline void invoke_new_hook(void* ptr, size_t size) {
    if (*new_hook != NULL) {
        (*new_hook)(ptr, size);
    }
}

static inline void invoke_delete_hook(void* ptr) {
    if (*delete_hook != NULL) {
        (*delete_hook)(ptr);
    }
}

/******************************************************************************/
/*
 * malloc zone functions. These all just call down to the current default zone,
 * calling the new/delete hooks as appropriate.
 */

static size_t
zone_size(malloc_zone_t *zone, const void *ptr)
{
    return default_zone->size(default_zone, ptr);
}

static void *
zone_malloc(malloc_zone_t *zone, size_t size)
{
    void *ptr = default_zone->malloc(default_zone, size);
    invoke_new_hook(ptr, size);
    return ptr;
}

static void *
zone_calloc(malloc_zone_t *zone, size_t num_items, size_t size)
{
    void *ptr = default_zone->calloc(zone, num_items, size);
    invoke_new_hook(ptr, num_items * size);
    return ptr;
}

static void *
zone_valloc(malloc_zone_t *zone, size_t size)
{
    void *ptr = default_zone->valloc(default_zone, size);
    invoke_new_hook(ptr, size);
    return ptr;
}

static void
zone_free(malloc_zone_t *zone, void *ptr)
{
    invoke_delete_hook(ptr);
    default_zone->free(default_zone, ptr);
}

static void *
zone_realloc(malloc_zone_t *zone, void *ptr, size_t size)
{
    invoke_delete_hook(ptr);
    void *ptr2 = default_zone->realloc(default_zone, ptr, size);
    invoke_new_hook(ptr2, size);
    return ptr2;
}

static void *
zone_memalign(malloc_zone_t *zone, size_t alignment, size_t size)
{
    void *ptr = default_zone->memalign(default_zone, alignment, size);
    invoke_new_hook(ptr, size);
    return ptr;
}

static void
zone_free_definite_size(malloc_zone_t *zone, void *ptr, size_t size)
{
    invoke_delete_hook(ptr);
    default_zone->free_definite_size(default_zone, ptr, size);
}

static void
zone_destroy(malloc_zone_t *zone)
{
    default_zone->destroy(default_zone);
}

/* Actually register the wrapper zone */
void register_wrapper_zone(malloc_new_hook_t* new_hook_,
                           malloc_delete_hook_t* delete_hook_) {

    // Get current default zone. This is what all requests will be forwarded to
    default_zone = malloc_default_zone();
    new_hook = new_hook_;
    delete_hook = delete_hook_;

    // Populate our wrapper zone.
    zone.size = zone_size;
    zone.malloc = zone_malloc;
    zone.calloc = zone_calloc;
    zone.valloc = zone_valloc;
    zone.free = zone_free;
    zone.realloc = zone_realloc;
    zone.destroy = zone_destroy;
    zone.zone_name = "CouchbaseWrapperZone";
    zone.batch_malloc = NULL;
    zone.batch_free = NULL;
    zone.introspect = default_zone->introspect;
    zone.version = 8;
    zone.memalign = zone_memalign;
    zone.free_definite_size = zone_free_definite_size;
    zone.pressure_relief = NULL;

    /* Register the custom zone.  At this point it won't be the default. */
    malloc_zone_register(&zone);

    do {
        malloc_zone_t *cur_default_zone = malloc_default_zone();
        /*
         * Unregister and reregister the default zone.  On OSX >= 10.6,
         * unregistering takes the last registered zone and places it
         * at the location of the specified zone.  Unregistering the
         * default zone thus makes the last registered one the default.
         * On OSX < 10.6, unregistering shifts all registered zones.
         * The first registered zone then becomes the default.
         */
        malloc_zone_unregister(cur_default_zone);
        malloc_zone_register(cur_default_zone);
    } while (malloc_default_zone() != &zone);
}
