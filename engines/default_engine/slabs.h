/* slabs memory allocation */
#pragma once

#include <memcached/engine_common.h>
#include <memcached/engine_error.h>

#include <mutex>

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST 200
#define CHUNK_ALIGN_BYTES 8
#define DONT_PREALLOC_SLABS
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)

/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void **slots;           /* list of item ptrs */
    unsigned int sl_total;  /* size of previous array */
    unsigned int sl_curr;   /* first free slot */

    void *end_page_ptr;         /* pointer to next free item at end of page, or 0 */
    unsigned int end_page_free; /* number of items remaining at end of last alloced page */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;  /* index+1 of dying slab, or zero if none */
    size_t requested; /* The number of requested bytes */
} slabclass_t;

struct slabs {
   slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
   size_t mem_limit;
   size_t mem_malloced;
   unsigned int power_largest;

   void *mem_base;
   void *mem_current;
   size_t mem_avail;

   struct {
      void **ptrs;
      size_t next;
      size_t size;
   } allocs;

   /**
    * Access to the slab allocator is protected by this lock
    */
   std::mutex lock;
};

class CookieIface;

/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor.
    3rd argument specifies if the slab allocator should allocate all memory
    up front (if true), or allocate memory in chunks as it is needed (if false)
*/
cb::engine_errc slabs_init(struct default_engine* engine,
                           const size_t limit,
                           const double factor,
                           const bool prealloc);

void slabs_destroy(struct default_engine *engine);

/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(struct default_engine *engine, const size_t size);

/** Allocate object of given length. 0 on error */ /*@null@*/
void *slabs_alloc(struct default_engine *engine, size_t size, unsigned int id);

/** Free previously allocated object */
void slabs_free(struct default_engine *engine, void *ptr, size_t size, unsigned int id);

/** Adjust the stats for memory requested */
void slabs_adjust_mem_requested(struct default_engine *engine, unsigned int id, size_t old, size_t ntotal);

/** Fill buffer with stats */ /*@null@*/
void slabs_stats(struct default_engine* engine,
                 const AddStatFn& add_stats,
                 CookieIface& c);

void add_statistics(CookieIface& cookie,
                    const AddStatFn& add_stats,
                    const char* prefix,
                    int num,
                    const char* key,
                    const char* fmt,
                    ...);
