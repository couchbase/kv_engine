/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include "config.h"

#include <fcntl.h>
#include <errno.h>
#include <platform/cb_malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <stdarg.h>

#ifdef VALGRIND
// switch to malloc if VALGRIND so we can get some useful insight.
#define USE_SYSTEM_MALLOC (1)
#endif

#include "default_engine_internal.h"

/*
 * Forward Declarations
 */
static int do_slabs_newslab(struct default_engine *engine, const unsigned int id);
static void *memory_allocate(struct default_engine *engine, size_t size);

#ifndef DONT_PREALLOC_SLABS
/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);
#endif

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(struct default_engine *engine, const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > engine->slabs.slabclass[res].size)
        if (res++ == engine->slabs.power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

static void *my_allocate(struct default_engine *e, size_t size) {
    void *ptr;
    /* Is threre room? */
    if (e->slabs.allocs.next == e->slabs.allocs.size) {
        size_t n = e->slabs.allocs.size + 1024;
        void *p = cb_realloc(e->slabs.allocs.ptrs, n * sizeof(void*));
        if (p == NULL) {
            return NULL;
        }
        e->slabs.allocs.ptrs = p;
        e->slabs.allocs.size = n;
    }

    ptr = cb_malloc(size);
    if (ptr != NULL) {
        e->slabs.allocs.ptrs[e->slabs.allocs.next++] = ptr;

    }
    return ptr;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
ENGINE_ERROR_CODE slabs_init(struct default_engine *engine,
                             const size_t limit,
                             const double factor,
                             const bool prealloc) {
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(hash_item) + (unsigned int)engine->config.chunk_size;

    engine->slabs.mem_limit = limit;

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        engine->slabs.mem_base = my_allocate(engine, engine->slabs.mem_limit);
        if (engine->slabs.mem_base != NULL) {
            engine->slabs.mem_current = engine->slabs.mem_base;
            engine->slabs.mem_avail = engine->slabs.mem_limit;
        } else {
            return ENGINE_ENOMEM;
        }
    }

    memset(engine->slabs.slabclass, 0, sizeof(engine->slabs.slabclass));

    while (++i < POWER_LARGEST && size <= engine->config.item_size_max / factor) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        engine->slabs.slabclass[i].size = size;
        engine->slabs.slabclass[i].perslab = (unsigned int)engine->config.item_size_max / engine->slabs.slabclass[i].size;
        size = (unsigned int)(size * factor);
        if (engine->config.verbose > 1) {
            EXTENSION_LOGGER_DESCRIPTOR *logger;
            logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "slab class %3d: chunk size %9u perslab %7u\n",
                        i, engine->slabs.slabclass[i].size,
                        engine->slabs.slabclass[i].perslab);
        }
    }

    engine->slabs.power_largest = i;
    engine->slabs.slabclass[engine->slabs.power_largest].size = (unsigned int)engine->config.item_size_max;
    engine->slabs.slabclass[engine->slabs.power_largest].perslab = 1;
    if (engine->config.verbose > 1) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "slab class %3d: chunk size %9u perslab %7u\n",
                    i, engine->slabs.slabclass[i].size,
                    engine->slabs.slabclass[i].perslab);
    }

    /* for the test suite:  faking of how much we've already malloc'd */
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            engine->slabs.mem_malloced = (size_t)atol(t_initial_malloc);
        }

    }

#ifndef DONT_PREALLOC_SLABS
    {
        char *pre_alloc = getenv("T_MEMD_SLABS_ALLOC");

        if (pre_alloc == NULL || atoi(pre_alloc) != 0) {
            slabs_preallocate(power_largest);
        }
    }
#endif

    return ENGINE_SUCCESS;
}

#ifndef DONT_PREALLOC_SLABS
static void slabs_preallocate (const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
        if (++prealloc > maxslabs)
            return;
        do_slabs_newslab(i);
    }

}
#endif

static int grow_slab_list (struct default_engine *engine, const unsigned int id) {
    slabclass_t *p = &engine->slabs.slabclass[id];
    if (p->slabs == p->list_size) {
        unsigned int new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = cb_realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static int do_slabs_newslab(struct default_engine *engine, const unsigned int id) {
    slabclass_t *p = &engine->slabs.slabclass[id];
    int len = p->size * p->perslab;
    char *ptr;

    if ((engine->slabs.mem_limit && engine->slabs.mem_malloced + len > engine->slabs.mem_limit && p->slabs > 0) ||
        (grow_slab_list(engine, id) == 0) ||
        ((ptr = memory_allocate(engine, (size_t)len)) == 0)) {

        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }

    memset(ptr, 0, (size_t)len);
    p->end_page_ptr = ptr;
    p->end_page_free = p->perslab;

    p->slab_list[p->slabs++] = ptr;
    engine->slabs.mem_malloced += len;
    MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

    return 1;
}

/*@null@*/
static void *do_slabs_alloc(struct default_engine *engine, const size_t size, unsigned int id) {
    slabclass_t *p;
    void *ret = NULL;

    if (id < POWER_SMALLEST || id > engine->slabs.power_largest) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
        return NULL;
    }

    p = &engine->slabs.slabclass[id];

#ifdef USE_SYSTEM_MALLOC
    if (engine->slabs.mem_limit && engine->slabs.mem_malloced + size > engine->slabs.mem_limit) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
        return 0;
    }
    engine->slabs.mem_malloced += size;
    ret = cb_calloc(1, size);
    MEMCACHED_SLABS_ALLOCATE(size, id, 0, ret);
    return ret;
#endif

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->end_page_ptr != 0 || p->sl_curr != 0 ||
           do_slabs_newslab(engine, id) != 0)) {
        /* We don't have more memory available */
        ret = NULL;
    } else if (p->sl_curr != 0) {
        /* return off our freelist */
        ret = p->slots[--p->sl_curr];
    } else {
        /* if we recently allocated a whole page, return from that */
        cb_assert(p->end_page_ptr != NULL);
        ret = p->end_page_ptr;
        if (--p->end_page_free != 0) {
            p->end_page_ptr = ((unsigned char *)p->end_page_ptr) + p->size;
        } else {
            p->end_page_ptr = 0;
        }
    }

    if (ret) {
        p->requested += size;
        MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    } else {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
    }

    return ret;
}

static void do_slabs_free(struct default_engine *engine, void *ptr, const size_t size, unsigned int id) {
    slabclass_t *p;

    if (id < POWER_SMALLEST || id > engine->slabs.power_largest)
        return;

    MEMCACHED_SLABS_FREE(size, id, ptr);
    p = &engine->slabs.slabclass[id];

#ifdef USE_SYSTEM_MALLOC
    engine->slabs.mem_malloced -= size;
    cb_free(ptr);
    return;
#endif

    if (p->sl_curr == p->sl_total) { /* need more space on the free list */
        int new_size = (p->sl_total != 0) ? p->sl_total * 2 : 16;  /* 16 is arbitrary */
        void **new_slots = cb_realloc(p->slots, new_size * sizeof(void *));
        if (new_slots == 0)
            return;
        p->slots = new_slots;
        p->sl_total = new_size;
    }
    p->slots[p->sl_curr++] = ptr;
    p->requested -= size;
    return;
}

void add_statistics(const void *cookie, ADD_STAT add_stats,
                    const char* prefix, int num, const char *key,
                    const char *fmt, ...) {
    char name[80];
    char val[80];
    int klen = 0;
    int vlen;
    int nw;
    va_list ap;

    cb_assert(cookie);
    cb_assert(add_stats);
    cb_assert(key);

    va_start(ap, fmt);
    vlen = vsnprintf(val, sizeof(val) - 1, fmt, ap);
    va_end(ap);

    if (vlen < 0 || vlen >= sizeof(val)) {
        return;
    }

    if (prefix != NULL) {
        klen = snprintf(name, sizeof(name), "%s:", prefix);
        if (klen < 0 || klen >= sizeof(name)) {
            return;
        }
    }

    if (num != -1) {
        nw = snprintf(name + klen, sizeof(name) - klen, "%d:", num);
        if (nw < 0 || nw >= (sizeof(name) - klen)) {
            return;
        }
        klen += nw;
    }

    nw = snprintf(name + klen, sizeof(name) - klen, "%s", key);
    if (nw < 0 || nw >= (sizeof(name) - klen)) {
        return;
    }

    klen += nw;

    add_stats(name, klen, val, vlen, cookie);
}

/*@null@*/
static void do_slabs_stats(struct default_engine *engine, ADD_STAT add_stats, const void *cookie) {
    unsigned int i;
    unsigned int total = 0;

    for(i = POWER_SMALLEST; i <= engine->slabs.power_largest; i++) {
        slabclass_t *p = &engine->slabs.slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            add_statistics(cookie, add_stats, NULL, i, "chunk_size", "%u",
                           p->size);
            add_statistics(cookie, add_stats, NULL, i, "chunks_per_page", "%u",
                           perslab);
            add_statistics(cookie, add_stats, NULL, i, "total_pages", "%u",
                           slabs);
            add_statistics(cookie, add_stats, NULL, i, "total_chunks", "%u",
                           slabs * perslab);
            add_statistics(cookie, add_stats, NULL, i, "used_chunks", "%u",
                           slabs*perslab - p->sl_curr - p->end_page_free);
            add_statistics(cookie, add_stats, NULL, i, "free_chunks", "%u",
                           p->sl_curr);
            add_statistics(cookie, add_stats, NULL, i, "free_chunks_end", "%u",
                           p->end_page_free);
            add_statistics(cookie, add_stats, NULL, i, "mem_requested", "%"PRIu64,
                           (uint64_t)p->requested);
            total++;
        }
    }

    /* add overall slab stats and append terminator */

    add_statistics(cookie, add_stats, NULL, -1, "active_slabs", "%d", total);
    add_statistics(cookie, add_stats, NULL, -1, "total_malloced", "%"PRIu64,
                   (uint64_t)engine->slabs.mem_malloced);
}

static void *memory_allocate(struct default_engine *engine, size_t size) {
    void *ret;

    if (engine->slabs.mem_base == NULL) {
        /* We are not using a preallocated large memory chunk */
        ret = my_allocate(engine, size);
    } else {
        ret = engine->slabs.mem_current;

        if (size > engine->slabs.mem_avail) {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        engine->slabs.mem_current = ((char*)engine->slabs.mem_current) + size;
        if (size < engine->slabs.mem_avail) {
            engine->slabs.mem_avail -= size;
        } else {
            engine->slabs.mem_avail = 0;
        }
    }

    return ret;
}

void *slabs_alloc(struct default_engine *engine, size_t size, unsigned int id) {
    void *ret;

    cb_mutex_enter(&engine->slabs.lock);
    ret = do_slabs_alloc(engine, size, id);
    cb_mutex_exit(&engine->slabs.lock);
    return ret;
}

void slabs_free(struct default_engine *engine, void *ptr, size_t size, unsigned int id) {
    cb_mutex_enter(&engine->slabs.lock);
    do_slabs_free(engine, ptr, size, id);
    cb_mutex_exit(&engine->slabs.lock);
}

void slabs_stats(struct default_engine *engine, ADD_STAT add_stats, const void *c) {
    cb_mutex_enter(&engine->slabs.lock);
    do_slabs_stats(engine, add_stats, c);
    cb_mutex_exit(&engine->slabs.lock);
}

void slabs_adjust_mem_requested(struct default_engine *engine, unsigned int id, size_t old, size_t ntotal)
{
    slabclass_t *p;
    cb_mutex_enter(&engine->slabs.lock);
    if (id < POWER_SMALLEST || id > engine->slabs.power_largest) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Internal error! Invalid slab class\n");
        cb_assert(false);
    }

    p = &engine->slabs.slabclass[id];
    p->requested = p->requested - old + ntotal;
    cb_mutex_exit(&engine->slabs.lock);
}

void slabs_destroy(struct default_engine *e)
{
    /* Release the allocated backing store */
    size_t ii;
    unsigned int jj;

    for (ii = 0; ii < e->slabs.allocs.next; ++ii) {
        cb_free(e->slabs.allocs.ptrs[ii]);
    }
    cb_free(e->slabs.allocs.ptrs);

    /* Release the freelists */
    for (jj = POWER_SMALLEST; jj <= e->slabs.power_largest; jj++) {
        slabclass_t *p = &e->slabs.slabclass[jj];
        cb_free(p->slots);
        cb_free(p->slab_list);
    }
}
