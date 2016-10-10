/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

#include <platform/cb_malloc.h>
#include <platform/crc32c.h>
#include "default_engine_internal.h"
#include "engine_manager.h"

/* Forward Declarations */
static void item_link_q(struct default_engine *engine, hash_item *it);
static void item_unlink_q(struct default_engine *engine, hash_item *it);
static hash_item *do_item_alloc(struct default_engine *engine,
                                const hash_key *key,
                                const int flags, const rel_time_t exptime,
                                const int nbytes,
                                const void *cookie,
                                uint8_t datatype);
static hash_item *do_item_get(struct default_engine *engine,
                              const hash_key* key);
static int do_item_link(struct default_engine *engine, hash_item *it);
static void do_item_unlink(struct default_engine *engine, hash_item *it);
static void do_item_release(struct default_engine *engine, hash_item *it);
static void do_item_update(struct default_engine *engine, hash_item *it);
static int do_item_replace(struct default_engine *engine,
                            hash_item *it, hash_item *new_it);
static void item_free(struct default_engine *engine, hash_item *it);

static bool hash_key_create(hash_key* hkey,
                            const void* key,
                            const size_t nkey,
                            struct default_engine* engine,
                            const void* cookie);

static void hash_key_destroy(hash_key* hkey);
static void hash_key_copy_to_item(hash_item* dst, const hash_key* src);

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60
/*
 * To avoid scanning through the complete cache in some circumstances we'll
 * just give up and return an error after inspecting a fixed number of objects.
 */
static const int search_items = 50;

void item_stats_reset(struct default_engine *engine) {
    cb_mutex_enter(&engine->items.lock);
    memset(engine->items.itemstats, 0, sizeof(engine->items.itemstats));
    cb_mutex_exit(&engine->items.lock);
}


/* warning: don't use these macros with a function, as it evals its arg twice */
static size_t ITEM_ntotal(struct default_engine *engine,
                          const hash_item *item) {
    size_t ret = sizeof(*item) + hash_key_get_alloc_size(item_get_key(item)) + item->nbytes;
    if (engine->config.use_cas) {
        ret += sizeof(uint64_t);
    }

    return ret;
}

/* Get the next CAS id for a new item. */
static uint64_t get_cas_id(void) {
    static uint64_t cas_id = 0;
    return ++cas_id;
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
                fprintf(stderr, "item %p refcnt(%c) %d %c%c\n", \
                        it, op, it->refcount, \
                        (it->iflag & ITEM_LINKED) ? 'L' : ' ', \
                        (it->iflag & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif


/*@null@*/
hash_item *do_item_alloc(struct default_engine *engine,
                         const hash_key *key,
                         const int flags,
                         const rel_time_t exptime,
                         const int nbytes,
                         const void *cookie,
                         uint8_t datatype) {
    hash_item *it = NULL;
    int tries = search_items;
    hash_item *search;
    rel_time_t oldest_live;
    rel_time_t current_time;
    unsigned int id;

    size_t ntotal = sizeof(hash_item) + hash_key_get_alloc_size(key) + nbytes;
    if (engine->config.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    if ((id = slabs_clsid(engine, ntotal)) == 0) {
        return 0;
    }

    /* do a quick check if we have any expired items in the tail.. */
    tries = search_items;
    oldest_live = engine->config.oldest_live;
    current_time = engine->server.core->get_current_time();

    for (search = engine->items.tails[id];
         tries > 0 && search != NULL;
         tries--, search=search->prev) {
        if (search->refcount == 0 &&
            ((search->time < oldest_live) || /* dead by flush */
             (search->exptime != 0 && search->exptime < current_time))) {
            it = search;
            /* I don't want to actually free the object, just steal
             * the item to avoid to grab the slab mutex twice ;-)
             */
            cb_mutex_enter(&engine->stats.lock);
            engine->stats.reclaimed++;
            cb_mutex_exit(&engine->stats.lock);
            engine->items.itemstats[id].reclaimed++;
            it->refcount = 1;
            slabs_adjust_mem_requested(engine, it->slabs_clsid, ITEM_ntotal(engine, it), ntotal);
            do_item_unlink(engine, it);
            /* Initialize the item block: */
            it->slabs_clsid = 0;
            it->refcount = 0;
            break;
        }
    }

    if (it == NULL && (it = slabs_alloc(engine, ntotal, id)) == NULL) {
        /*
        ** Could not find an expired item at the tail, and memory allocation
        ** failed. Try to evict some items!
        */
        tries = search_items;

        /* If requested to not push old items out of cache when memory runs out,
         * we're out of luck at this point...
         */

        if (engine->config.evict_to_free == 0) {
            engine->items.itemstats[id].outofmemory++;
            return NULL;
        }

        /*
         * try to get one off the right LRU
         * don't necessariuly unlink the tail because it may be locked: refcount>0
         * search up from tail an item with refcount==0 and unlink it; give up after search_items
         * tries
         */

        if (engine->items.tails[id] == 0) {
            engine->items.itemstats[id].outofmemory++;
            return NULL;
        }

        for (search = engine->items.tails[id]; tries > 0 && search != NULL; tries--, search=search->prev) {
            if (search->refcount == 0) {
                if (search->exptime == 0 || search->exptime > current_time) {
                    engine->items.itemstats[id].evicted++;
                    engine->items.itemstats[id].evicted_time = current_time - search->time;
                    if (search->exptime != 0) {
                        engine->items.itemstats[id].evicted_nonzero++;
                    }
                    cb_mutex_enter(&engine->stats.lock);
                    engine->stats.evictions++;
                    cb_mutex_exit(&engine->stats.lock);
                    const hash_key* search_key = item_get_key(search);
                    engine->server.stat->evicting(cookie,
                                                  hash_key_get_client_key(search_key),
                                                  hash_key_get_client_key_len(search_key));
                } else {
                    engine->items.itemstats[id].reclaimed++;
                    cb_mutex_enter(&engine->stats.lock);
                    engine->stats.reclaimed++;
                    cb_mutex_exit(&engine->stats.lock);
                }
                do_item_unlink(engine, search);
                break;
            }
        }
        it = slabs_alloc(engine, ntotal, id);
        if (it == 0) {
            engine->items.itemstats[id].outofmemory++;
            /* Last ditch effort. There is a very rare bug which causes
             * refcount leaks. We've fixed most of them, but it still happens,
             * and it may happen in the future.
             * We can reasonably assume no item can stay locked for more than
             * three hours, so if we find one in the tail which is that old,
             * free it anyway.
             */
            tries = search_items;
            for (search = engine->items.tails[id]; tries > 0 && search != NULL; tries--, search=search->prev) {
                if (search->refcount != 0 && search->time + TAIL_REPAIR_TIME < current_time) {
                    engine->items.itemstats[id].tailrepairs++;
                    search->refcount = 0;
                    do_item_unlink(engine, search);
                    break;
                }
            }
            it = slabs_alloc(engine, ntotal, id);
            if (it == 0) {
                return NULL;
            }
        }
    }

    cb_assert(it->slabs_clsid == 0);

    it->slabs_clsid = id;

    cb_assert(it != engine->items.heads[it->slabs_clsid]);

    it->next = it->prev = it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
    DEBUG_REFCNT(it, '*');
    it->iflag = engine->config.use_cas ? ITEM_WITH_CAS : 0;
    it->nbytes = nbytes;
    it->flags = flags;
    it->datatype = datatype;
    it->exptime = exptime;
    hash_key_copy_to_item(it, key);
    return it;
}

static void item_free(struct default_engine *engine, hash_item *it) {
    size_t ntotal = ITEM_ntotal(engine, it);
    unsigned int clsid;
    cb_assert((it->iflag & ITEM_LINKED) == 0);
    cb_assert(it != engine->items.heads[it->slabs_clsid]);
    cb_assert(it != engine->items.tails[it->slabs_clsid]);
    cb_assert(it->refcount == 0 || engine->scrubber.force_delete);

    /* so slab size changer can tell later if item is already free or not */
    clsid = it->slabs_clsid;
    it->slabs_clsid = 0;
    it->iflag |= ITEM_SLABBED;
    DEBUG_REFCNT(it, 'F');
    slabs_free(engine, it, ntotal, clsid);
}

static void item_link_q(struct default_engine *engine, hash_item *it) { /* item is the new head */
    hash_item **head, **tail;
    cb_assert(it->slabs_clsid < POWER_LARGEST);
    cb_assert((it->iflag & ITEM_SLABBED) == 0);

    head = &engine->items.heads[it->slabs_clsid];
    tail = &engine->items.tails[it->slabs_clsid];
    cb_assert(it != *head);
    cb_assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    engine->items.sizes[it->slabs_clsid]++;
    return;
}

static void item_unlink_q(struct default_engine *engine, hash_item *it) {
    hash_item **head, **tail;
    cb_assert(it->slabs_clsid < POWER_LARGEST);
    head = &engine->items.heads[it->slabs_clsid];
    tail = &engine->items.tails[it->slabs_clsid];

    if (*head == it) {
        cb_assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        cb_assert(it->next == 0);
        *tail = it->prev;
    }
    cb_assert(it->next != it);
    cb_assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    engine->items.sizes[it->slabs_clsid]--;
    return;
}

int do_item_link(struct default_engine *engine, hash_item *it) {
    const hash_key* key = item_get_key(it);
    MEMCACHED_ITEM_LINK(hash_key_get_client_key(key), hash_key_get_client_key_len(key), it->nbytes);
    cb_assert((it->iflag & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    it->iflag |= ITEM_LINKED;
    it->time = engine->server.core->get_current_time();

    assoc_insert(engine, crc32c(hash_key_get_key(key),
                                hash_key_get_key_len(key), 0),
                 it);

    cb_mutex_enter(&engine->stats.lock);
    engine->stats.curr_bytes += ITEM_ntotal(engine, it);
    engine->stats.curr_items += 1;
    engine->stats.total_items += 1;
    cb_mutex_exit(&engine->stats.lock);

    /* Allocate a new CAS ID on link. */
    item_set_cas(NULL, NULL, it, get_cas_id());

    item_link_q(engine, it);

    return 1;
}

void do_item_unlink(struct default_engine *engine, hash_item *it) {
    const hash_key* key = item_get_key(it);
    MEMCACHED_ITEM_UNLINK(hash_key_get_client_key(key),
                          hash_key_get_client_key_len(key),
                          it->nbytes);
    if ((it->iflag & ITEM_LINKED) != 0) {
        it->iflag &= ~ITEM_LINKED;
        cb_mutex_enter(&engine->stats.lock);
        engine->stats.curr_bytes -= ITEM_ntotal(engine, it);
        engine->stats.curr_items -= 1;
        cb_mutex_exit(&engine->stats.lock);
        assoc_delete(engine, crc32c(hash_key_get_key(key),
                                    hash_key_get_key_len(key), 0),
                     key);
        item_unlink_q(engine, it);
        if (it->refcount == 0 || engine->scrubber.force_delete) {
            item_free(engine, it);
        }
    }
}

void do_item_release(struct default_engine *engine, hash_item *it) {
    MEMCACHED_ITEM_REMOVE(hash_key_get_client_key(item_get_key(it)),
                          hash_key_get_client_key_len(item_get_key(it)),
                          it->nbytes);
    if (it->refcount != 0) {
        it->refcount--;
        DEBUG_REFCNT(it, '-');
    }
    if (it->refcount == 0 && (it->iflag & ITEM_LINKED) == 0) {
        item_free(engine, it);
    }
}

void do_item_update(struct default_engine *engine, hash_item *it) {
    rel_time_t current_time = engine->server.core->get_current_time();
    MEMCACHED_ITEM_UPDATE(hash_key_get_client_key(item_get_key(it)),
                          hash_key_get_client_key_len(item_get_key(it)),
                          it->nbytes);
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        cb_assert((it->iflag & ITEM_SLABBED) == 0);

        if ((it->iflag & ITEM_LINKED) != 0) {
            item_unlink_q(engine, it);
            it->time = current_time;
            item_link_q(engine, it);
        }
    }
}

int do_item_replace(struct default_engine *engine,
                    hash_item *it, hash_item *new_it) {
    MEMCACHED_ITEM_REPLACE(hash_key_get_client_key(item_get_key(it)),
                           hash_key_get_client_key_len(item_get_key(it)),
                           it->nbytes,
                           hash_key_get_client_key(item_get_key(new_it)),
                           hash_key_get_client_key_len(item_get_key(new_it)),
                           new_it->nbytes);
    cb_assert((it->iflag & ITEM_SLABBED) == 0);

    do_item_unlink(engine, it);
    return do_item_link(engine, new_it);
}

static void do_item_stats(struct default_engine *engine,
                          ADD_STAT add_stats, const void *c) {
    int i;
    rel_time_t current_time = engine->server.core->get_current_time();
    for (i = 0; i < POWER_LARGEST; i++) {
        if (engine->items.tails[i] != NULL) {
            const char *prefix = "items";
            int search = search_items;
            while (search > 0 &&
                   engine->items.tails[i] != NULL &&
                   ((engine->config.oldest_live != 0 && /* Item flushd */
                     engine->config.oldest_live <= current_time &&
                     engine->items.tails[i]->time <= engine->config.oldest_live) ||
                    (engine->items.tails[i]->exptime != 0 && /* and not expired */
                     engine->items.tails[i]->exptime < current_time))) {
                --search;
                if (engine->items.tails[i]->refcount == 0) {
                    do_item_unlink(engine, engine->items.tails[i]);
                } else {
                    break;
                }
            }
            if (engine->items.tails[i] == NULL) {
                /* We removed all of the items in this slab class */
                continue;
            }

            add_statistics(c, add_stats, prefix, i, "number", "%u",
                           engine->items.sizes[i]);
            add_statistics(c, add_stats, prefix, i, "age", "%u",
                           engine->items.tails[i]->time);
            add_statistics(c, add_stats, prefix, i, "evicted",
                           "%u", engine->items.itemstats[i].evicted);
            add_statistics(c, add_stats, prefix, i, "evicted_nonzero",
                           "%u", engine->items.itemstats[i].evicted_nonzero);
            add_statistics(c, add_stats, prefix, i, "evicted_time",
                           "%u", engine->items.itemstats[i].evicted_time);
            add_statistics(c, add_stats, prefix, i, "outofmemory",
                           "%u", engine->items.itemstats[i].outofmemory);
            add_statistics(c, add_stats, prefix, i, "tailrepairs",
                           "%u", engine->items.itemstats[i].tailrepairs);;
            add_statistics(c, add_stats, prefix, i, "reclaimed",
                           "%u", engine->items.itemstats[i].reclaimed);;
        }
    }
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
static void do_item_stats_sizes(struct default_engine *engine,
                                ADD_STAT add_stats, const void *c) {

    /* max 1MB object, divided into 32 bytes size buckets */
    const int num_buckets = 32768;
    unsigned int *histogram = cb_calloc(num_buckets, sizeof(unsigned int));

    if (histogram != NULL) {
        int i;

        /* build the histogram */
        for (i = 0; i < POWER_LARGEST; i++) {
            hash_item *iter = engine->items.heads[i];
            while (iter) {
                size_t ntotal = ITEM_ntotal(engine, iter);
                size_t bucket = ntotal / 32;
                if ((ntotal % 32) != 0) {
                    bucket++;
                }
                if (bucket < num_buckets) {
                    histogram[bucket]++;
                }
                iter = iter->next;
            }
        }

        /* write the buffer */
        for (i = 0; i < num_buckets; i++) {
            if (histogram[i] != 0) {
                char key[8], val[32];
                int klen, vlen;
                klen = snprintf(key, sizeof(key), "%d", i * 32);
                vlen = snprintf(val, sizeof(val), "%u", histogram[i]);
                if (klen > 0 && klen < sizeof(key) && vlen > 0 &&
                    vlen < sizeof(val)) {
                    add_stats(key, klen, val, vlen, c);
                }
            }
        }
        cb_free(histogram);
    }
}

/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get(struct default_engine *engine,
                       const hash_key *key) {
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *it = assoc_find(engine,
                               crc32c(hash_key_get_key(key),
                                      hash_key_get_key_len(key), 0),
                               key);
    int was_found = 0;

    if (engine->config.verbose > 2) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        if (it == NULL) {
            logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "> NOT FOUND in bucket %d, %s",
                        hash_key_get_bucket_index(key),
                        hash_key_get_client_key(key));
        } else {
            logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "> FOUND KEY in bucket %d, %s",
                        hash_key_get_bucket_index(item_get_key(it)),
                        hash_key_get_client_key(item_get_key(it)));
            was_found++;
        }
    }

    if (it != NULL && engine->config.oldest_live != 0 &&
        engine->config.oldest_live <= current_time &&
        it->time <= engine->config.oldest_live) {
        do_item_unlink(engine, it);           /* MTSAFE - items.lock held */
        it = NULL;
    }

    if (it == NULL && was_found) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by flush");
        was_found--;
    }

    if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
        do_item_unlink(engine, it);           /* MTSAFE - items.lock held */
        it = NULL;
    }

    if (it == NULL && was_found) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by expire");
        was_found--;
    }

    if (it != NULL) {
        it->refcount++;
        DEBUG_REFCNT(it, '+');
        do_item_update(engine, it);
    }

    return it;
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
static ENGINE_ERROR_CODE do_store_item(struct default_engine *engine,
                                       hash_item *it,
                                       ENGINE_STORE_OPERATION operation,
                                       const void *cookie,
                                       hash_item** stored_item) {
    const hash_key* key = item_get_key(it);
    hash_item *old_it = do_item_get(engine, key);
    ENGINE_ERROR_CODE stored = ENGINE_NOT_STORED;

    hash_item *new_it = NULL;

    if (old_it != NULL && operation == OPERATION_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(engine, old_it);
    } else if (!old_it && operation == OPERATION_REPLACE) {
        /* replace only replaces an existing value; don't store */
    } else if (operation == OPERATION_CAS) {
        /* validate cas operation */
        if(old_it == NULL) {
            /* LRU expired */
            stored = ENGINE_KEY_ENOENT;
        }
        else if (item_get_cas(it) == item_get_cas(old_it)) {
            /* cas validates */
            /* it and old_it may belong to different classes. */
            /* I'm updating the stats for the one that's getting pushed out */
            do_item_replace(engine, old_it, it);
            stored = ENGINE_SUCCESS;
        } else {
            if (engine->config.verbose > 1) {
                EXTENSION_LOGGER_DESCRIPTOR *logger;
                logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
                logger->log(EXTENSION_LOG_INFO, NULL,
                        "CAS:  failure: expected %"PRIu64", got %"PRIu64"\n",
                        item_get_cas(old_it),
                        item_get_cas(it));
            }
            stored = ENGINE_KEY_EEXISTS;
        }
    } else {
        if (stored == ENGINE_NOT_STORED) {
            if (old_it != NULL) {
                do_item_replace(engine, old_it, it);
            } else {
                do_item_link(engine, it);
            }

            *stored_item = it;
            stored = ENGINE_SUCCESS;
        }
    }

    if (old_it != NULL) {
        do_item_release(engine, old_it);         /* release our reference */
    }

    if (new_it != NULL) {
        do_item_release(engine, new_it);
    }

    if (stored == ENGINE_SUCCESS) {
        *stored_item = it;
    }

    return stored;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *item_alloc(struct default_engine *engine,
                      const void *key, size_t nkey, int flags,
                      rel_time_t exptime, int nbytes, const void *cookie,
                      uint8_t datatype) {
    hash_item *it;
    hash_key hkey;
    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return NULL;
    }
    cb_mutex_enter(&engine->items.lock);
    it = do_item_alloc(engine, &hkey, flags, exptime, nbytes, cookie, datatype);
    cb_mutex_exit(&engine->items.lock);
    hash_key_destroy(&hkey);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
hash_item *item_get(struct default_engine *engine,
                    const void *cookie,
                    const void *key,
                    const size_t nkey) {
    hash_item *it;
    hash_key hkey;
    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return NULL;
    }
    cb_mutex_enter(&engine->items.lock);
    it = do_item_get(engine, &hkey);
    cb_mutex_exit(&engine->items.lock);
    hash_key_destroy(&hkey);
    return it;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_release(struct default_engine *engine, hash_item *item) {
    cb_mutex_enter(&engine->items.lock);
    do_item_release(engine, item);
    cb_mutex_exit(&engine->items.lock);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(struct default_engine *engine, hash_item *item) {
    cb_mutex_enter(&engine->items.lock);
    do_item_unlink(engine, item);
    cb_mutex_exit(&engine->items.lock);
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE store_item(struct default_engine *engine,
                             hash_item *item, uint64_t *cas,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie) {
    ENGINE_ERROR_CODE ret;
    hash_item* stored_item = NULL;

    cb_mutex_enter(&engine->items.lock);
    ret = do_store_item(engine, item, operation, cookie, &stored_item);
    if (ret == ENGINE_SUCCESS) {
        *cas = item_get_cas(stored_item);
    }
    cb_mutex_exit(&engine->items.lock);
    return ret;
}

static hash_item *do_touch_item(struct default_engine *engine,
                                const hash_key *hkey,
                                uint32_t exptime)
{
   hash_item *item = do_item_get(engine, hkey);
   if (item != NULL) {
       item->exptime = exptime;
   }
   return item;
}

hash_item *touch_item(struct default_engine *engine,
                      const void* cookie,
                      const void* key,
                      uint16_t nkey,
                      uint32_t exptime)
{
    hash_item *ret;
    hash_key hkey;
    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return NULL;
    }
    cb_mutex_enter(&engine->items.lock);
    ret = do_touch_item(engine, &hkey, exptime);
    cb_mutex_exit(&engine->items.lock);
    hash_key_destroy(&hkey);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void item_flush_expired(struct default_engine *engine) {
    cb_mutex_enter(&engine->items.lock);

    rel_time_t now = engine->server.core->get_current_time();
    if (now > engine->config.oldest_live) {
        engine->config.oldest_live = now - 1;
    }

    for (int ii = 0; ii < POWER_LARGEST; ii++) {
        hash_item *iter, *next;
        /*
         * The LRU is sorted in decreasing time order, and an item's
         * timestamp is never newer than its last access time, so we
         * only need to walk back until we hit an item older than the
         * oldest_live time.
         * The oldest_live checking will auto-expire the remaining items.
         */
        for (iter = engine->items.heads[ii]; iter != NULL; iter = next) {
            if (iter->time >= engine->config.oldest_live) {
                next = iter->next;
                if ((iter->iflag & ITEM_SLABBED) == 0) {
                    do_item_unlink(engine, iter);
                }
            } else {
                /* We've hit the first old item. Continue to the next queue. */
                break;
            }
        }
    }
    cb_mutex_exit(&engine->items.lock);
}

void item_stats(struct default_engine *engine,
                   ADD_STAT add_stat, const void *cookie)
{
    cb_mutex_enter(&engine->items.lock);
    do_item_stats(engine, add_stat, cookie);
    cb_mutex_exit(&engine->items.lock);
}


void item_stats_sizes(struct default_engine *engine,
                      ADD_STAT add_stat, const void *cookie)
{
    cb_mutex_enter(&engine->items.lock);
    do_item_stats_sizes(engine, add_stat, cookie);
    cb_mutex_exit(&engine->items.lock);
}

static void do_item_link_cursor(struct default_engine *engine,
                                hash_item *cursor, int ii)
{
    cursor->slabs_clsid = (uint8_t)ii;
    cursor->next = NULL;
    cursor->prev = engine->items.tails[ii];
    engine->items.tails[ii]->next = cursor;
    engine->items.tails[ii] = cursor;
    engine->items.sizes[ii]++;
}

typedef ENGINE_ERROR_CODE (*ITERFUNC)(struct default_engine *engine,
                                      hash_item *item, void *cookie);

static bool do_item_walk_cursor(struct default_engine *engine,
                                hash_item *cursor,
                                int steplength,
                                ITERFUNC itemfunc,
                                void* itemdata,
                                ENGINE_ERROR_CODE *error)
{
    int ii = 0;
    *error = ENGINE_SUCCESS;

    while (cursor->prev != NULL && ii < steplength) {
        /* Move cursor */
        hash_item *ptr = cursor->prev;
        bool done = false;

        ++ii;
        item_unlink_q(engine, cursor);

        if (ptr == engine->items.heads[cursor->slabs_clsid]) {
            done = true;
            cursor->prev = NULL;
        } else {
            cursor->next = ptr;
            cursor->prev = ptr->prev;
            cursor->prev->next = cursor;
            ptr->prev = cursor;
        }

        /* Ignore cursors */
        if (item_get_key(ptr)->header.len == 0 && ptr->nbytes == 0) {
            --ii;
        } else {
            *error = itemfunc(engine, ptr, itemdata);
            if (*error != ENGINE_SUCCESS) {
                return false;
            }
        }

        if (done) {
            return false;
        }
    }

    return (cursor->prev != NULL);
}

static ENGINE_ERROR_CODE item_scrub(struct default_engine *engine,
                                    hash_item *item,
                                    void *cookie) {
    rel_time_t current_time = engine->server.core->get_current_time();
    (void)cookie;
    engine->scrubber.visited++;
    /*
        scrubber is used for generic bucket deletion and scrub_cmd
        all expired or orphaned items are unlinked
    */
    if (engine->scrubber.force_delete && item->refcount > 0) {
        // warn that someone isn't releasing items before deleting their bucket.
        EXTENSION_LOGGER_DESCRIPTOR* logger;
        logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Bucket (%d) deletion is removing an item with refcount %d",
                     engine->bucket_id,
                     item->refcount);
    }

    if (engine->scrubber.force_delete || (item->refcount == 0 &&
       (item->exptime != 0 && item->exptime < current_time))) {
        do_item_unlink(engine, item);
        engine->scrubber.cleaned++;
    }
    return ENGINE_SUCCESS;
}

static void item_scrub_class(struct default_engine *engine,
                             hash_item *cursor) {

    ENGINE_ERROR_CODE ret;
    bool more;
    do {
        cb_mutex_enter(&engine->items.lock);
        more = do_item_walk_cursor(engine, cursor, 200, item_scrub, NULL, &ret);
        cb_mutex_exit(&engine->items.lock);
        if (ret != ENGINE_SUCCESS) {
            break;
        }
    } while (more);
}

void item_scrubber_main(struct default_engine *engine)
{
    hash_item cursor;
    int ii;

    memset(&cursor, 0, sizeof(cursor));
    cursor.refcount = 1;
    for (ii = 0; ii < POWER_LARGEST; ++ii) {
        bool skip = false;
        cb_mutex_enter(&engine->items.lock);
        if (engine->items.heads[ii] == NULL) {
            skip = true;
        } else {
            /* add the item at the tail */
            do_item_link_cursor(engine, &cursor, ii);
        }
        cb_mutex_exit(&engine->items.lock);

        if (!skip) {
            item_scrub_class(engine, &cursor);
        }
    }

    cb_mutex_enter(&engine->scrubber.lock);
    engine->scrubber.stopped = time(NULL);
    engine->scrubber.running = false;
    cb_mutex_exit(&engine->scrubber.lock);
}

bool item_start_scrub(struct default_engine *engine)
{
    bool ret = false;

    cb_mutex_enter(&engine->scrubber.lock);

    /*
        If the scrubber is already scrubbing items, ignore
    */
    if (!engine->scrubber.running) {

        engine->scrubber.started = time(NULL);
        engine->scrubber.stopped = 0;
        engine->scrubber.visited = 0;
        engine->scrubber.cleaned = 0;
        engine->scrubber.running = true;
        engine_manager_scrub_engine(engine);
        ret = true;
    }
    cb_mutex_exit(&engine->scrubber.lock);

    return ret;
}

struct tap_client {
    hash_item cursor;
    hash_item *it;
};

static ENGINE_ERROR_CODE item_tap_iterfunc(struct default_engine *engine,
                                    hash_item *item,
                                    void *cookie) {
    struct tap_client *client = cookie;
    client->it = item;
    ++client->it->refcount;
    return ENGINE_SUCCESS;
}

static tap_event_t do_item_tap_walker(struct default_engine *engine,
                                         const void *cookie, item **itm,
                                         void **es, uint16_t *nes, uint8_t *ttl,
                                         uint16_t *flags, uint32_t *seqno,
                                         uint16_t *vbucket)
{
    ENGINE_ERROR_CODE r;
    struct tap_client *client = engine->server.cookie->get_engine_specific(cookie);
    if (client == NULL) {
        return TAP_DISCONNECT;
    }

    *es = NULL;
    *nes = 0;
    *ttl = (uint8_t)-1;
    *seqno = 0;
    *flags = 0;
    *vbucket = 0;
    client->it = NULL;

    do {
        if (!do_item_walk_cursor(engine, &client->cursor, 1, item_tap_iterfunc, client, &r)) {
            /* find next slab class to look at.. */
            bool linked = false;
            int ii;
            for (ii = client->cursor.slabs_clsid + 1; ii < POWER_LARGEST && !linked;  ++ii) {
                if (engine->items.heads[ii] != NULL) {
                    /* add the item at the tail */
                    do_item_link_cursor(engine, &client->cursor, ii);
                    linked = true;
                }
            }
            if (!linked) {
                break;
            }
        }
    } while (client->it == NULL);
    *itm = client->it;

    return (*itm == NULL) ? TAP_DISCONNECT : TAP_MUTATION;
}

tap_event_t item_tap_walker(ENGINE_HANDLE* handle,
                            const void *cookie, item **itm,
                            void **es, uint16_t *nes, uint8_t *ttl,
                            uint16_t *flags, uint32_t *seqno,
                            uint16_t *vbucket)
{
    tap_event_t ret;
    struct default_engine *engine = (struct default_engine*)handle;
    cb_mutex_enter(&engine->items.lock);
    ret = do_item_tap_walker(engine, cookie, itm, es, nes, ttl, flags, seqno, vbucket);
    cb_mutex_exit(&engine->items.lock);

    return ret;
}

bool initialize_item_tap_walker(struct default_engine *engine,
                                const void* cookie)
{
    bool linked = false;
    int ii;
    struct tap_client *client = cb_calloc(1, sizeof(*client));
    if (client == NULL) {
        return false;
    }
    client->cursor.refcount = 1;

    /* Link the cursor! */
    for (ii = 0; ii < POWER_LARGEST && !linked; ++ii) {
        cb_mutex_enter(&engine->items.lock);
        if (engine->items.heads[ii] != NULL) {
            /* add the item at the tail */
            do_item_link_cursor(engine, &client->cursor, ii);
            linked = true;
        }
        cb_mutex_exit(&engine->items.lock);
    }

    engine->server.cookie->store_engine_specific(cookie, client);
    return true;
}

void link_dcp_walker(struct default_engine *engine,
                     struct dcp_connection *connection)
{
    bool linked = false;
    int ii;
    connection->cursor.refcount = 1;

    /* Link the cursor! */
    for (ii = 0; ii < POWER_LARGEST && !linked; ++ii) {
        cb_mutex_enter(&engine->items.lock);
        if (engine->items.heads[ii] != NULL) {
            /* add the item at the tail */
            do_item_link_cursor(engine, &connection->cursor, ii);
            linked = true;
        }
        cb_mutex_exit(&engine->items.lock);
    }
}

static ENGINE_ERROR_CODE item_dcp_iterfunc(struct default_engine *engine,
                                           hash_item *item,
                                           void *cookie) {
    struct dcp_connection *connection = cookie;
    connection->it = item;
    ++connection->it->refcount;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_item_dcp_step(struct default_engine *engine,
                                          struct dcp_connection *connection,
                                          const void *cookie,
                                          struct dcp_message_producers *producers)
{
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    while (connection->it == NULL) {
        if (!do_item_walk_cursor(engine, &connection->cursor, 1,
                                 item_dcp_iterfunc, connection, &ret)) {
            /* find next slab class to look at.. */
            bool linked = false;
            int ii;
            for (ii = connection->cursor.slabs_clsid + 1; ii < POWER_LARGEST && !linked;  ++ii) {
                if (engine->items.heads[ii] != NULL) {
                    /* add the item at the tail */
                    do_item_link_cursor(engine, &connection->cursor, ii);
                    linked = true;
                }
            }
            if (!linked) {
                break;
            }
        }
    }

    if (connection->it != NULL) {
        rel_time_t current_time = engine->server.core->get_current_time();
        rel_time_t exptime = connection->it->exptime;

        if (exptime != 0 && exptime < current_time) {
            const hash_key* key = item_get_key(connection->it);
            ret = producers->expiration(cookie, connection->opaque,
                                        hash_key_get_client_key(key),
                                        hash_key_get_client_key_len(key),
                                        item_get_cas(connection->it),
                                        0, 0, 0, NULL, 0);
            if (ret == ENGINE_SUCCESS) {
                do_item_unlink(engine, connection->it);
                do_item_release(engine, connection->it);
            }
        } else {
            ret = producers->mutation(cookie, connection->opaque,
                                      connection->it, 0, 0, 0, 0, NULL, 0, 0);
        }

        if (ret == ENGINE_SUCCESS) {
            connection->it = NULL;
        }
    } else {
        return ENGINE_DISCONNECT;
    }

    return ret;
}

ENGINE_ERROR_CODE item_dcp_step(struct default_engine *engine,
                                struct dcp_connection *connection,
                                const void *cookie,
                                struct dcp_message_producers *producers)
{
    ENGINE_ERROR_CODE ret;
    cb_mutex_enter(&engine->items.lock);
    ret = do_item_dcp_step(engine, connection, cookie, producers);
    cb_mutex_exit(&engine->items.lock);
    return ret;
}

static bool hash_key_create(hash_key* hkey,
                            const void* key,
                            const size_t nkey,
                            struct default_engine* engine,
                            const void* cookie) {

    int hash_key_len = sizeof(bucket_id_t) + nkey;
    if (nkey > sizeof(hkey->key_storage.client_key)) {
        hkey->header.full_key = cb_malloc(hash_key_len);
        if (hkey->header.full_key == NULL) {
            return false;
        }
    } else {
        hkey->header.full_key = (hash_key_data*)&hkey->key_storage;
    }
    hash_key_set_len(hkey, hash_key_len);
    hash_key_set_bucket_index(hkey, engine->bucket_id);
    hash_key_set_client_key(hkey, key, nkey);
    return true;
}

static void hash_key_destroy(hash_key* hkey) {
    if ((void*)hkey->header.full_key != (void*)&hkey->key_storage) {
       cb_free(hkey->header.full_key);
    }
}

/*
 * The item object stores a hash_key in a contiguous allocation
 * This method ensures correct copying into a contiguous hash_key
 */
static void hash_key_copy_to_item(hash_item* dst, const hash_key* src) {
    hash_key* key = item_get_key(dst);
    memcpy(key, src, sizeof(hash_key_header));
    key->header.full_key = (hash_key_data*)&key->key_storage;
    memcpy(hash_key_get_key(key), hash_key_get_key(src), hash_key_get_key_len(src));
}
