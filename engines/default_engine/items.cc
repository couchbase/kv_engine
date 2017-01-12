/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

#include <memcached/server_api.h>
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
                              const hash_key* key,
                              const DocumentState document_state);
static int do_item_link(struct default_engine *engine,
                        const void* cookie,
                        hash_item *it);
static void do_item_unlink(struct default_engine *engine, hash_item *it);
static ENGINE_ERROR_CODE do_safe_item_unlink(struct default_engine *engine,
                                             hash_item *it);
static void do_item_release(struct default_engine *engine, hash_item *it);
static void do_item_update(struct default_engine *engine, hash_item *it);
static int do_item_replace(struct default_engine* engine,
                           const void* cookie,
                           hash_item* it,
                           hash_item* new_it);
static void item_free(struct default_engine *engine, hash_item *it);

static bool hash_key_create(hash_key* hkey,
                            const void* key,
                            const size_t nkey,
                            struct default_engine* engine,
                            const void* cookie);

static void hash_key_destroy(hash_key* hkey);
static void hash_key_copy_to_item(hash_item* dst, const hash_key* src);

static const DocumentState document_any_state = DocumentState(
    uint8_t(DocumentState::Alive) |
    uint8_t(DocumentState::Deleted));

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

    if ((id = slabs_clsid(engine, ntotal)) == 0) {
        return 0;
    }

    /* do a quick check if we have any expired items in the tail.. */
    oldest_live = engine->config.oldest_live;
    current_time = engine->server.core->get_current_time();

    for (search = engine->items.tails[id];
         tries > 0 && search != NULL;
         tries--, search=search->prev) {
        if (search->refcount == 0 &&
            ((search->time < oldest_live) || /* dead by flush */
             (search->exptime != 0 && search->exptime < current_time)) &&
            (search->locktime <= current_time)) {
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

    if (it == NULL &&
        (it = static_cast<hash_item*>(slabs_alloc(engine, ntotal, id))) == NULL) {
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
            if (search->refcount == 0 && search->locktime <= current_time) {
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
        it = static_cast<hash_item*>(slabs_alloc(engine, ntotal, id));
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
            it = static_cast<hash_item*>(slabs_alloc(engine, ntotal, id));
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
    it->iflag = 0;
    it->nbytes = nbytes;
    it->flags = flags;
    it->datatype = datatype;
    it->exptime = exptime;
    it->locktime = 0;
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

int do_item_link(struct default_engine *engine,
                 const void* cookie,
                 hash_item *it) {
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

    auto cas = get_cas_id();

    /* Allocate a new CAS ID on link. */
    item_set_cas(NULL, NULL, it, cas);

    item_info info = {};
    info.cas = cas;
    info.exptime = it->exptime;
    info.nbytes = it->nbytes;
    info.flags = it->flags;
    info.nkey = hash_key_get_client_key_len(key);
    info.key = hash_key_get_client_key(key);
    info.value[0].iov_base = item_get_data(it);
    info.value[0].iov_len = it->nbytes;
    info.datatype = it->datatype;

    if (engine->server.document->pre_link(cookie, info) != ENGINE_SUCCESS) {
        return 0;
    }

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

ENGINE_ERROR_CODE do_safe_item_unlink(struct default_engine* engine,
                                      hash_item* it) {

    const hash_key* key = item_get_key(it);
    auto* stored = do_item_get(engine, key, document_any_state);
    if (stored == nullptr) {
        return ENGINE_KEY_ENOENT;
    }

    auto ret = ENGINE_SUCCESS;

    if (it->cas == stored->cas) {
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
    } else {
        ret = ENGINE_KEY_EEXISTS;
    }

    do_item_release(engine, stored);
    return ret;
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
                    const void* cookie,
                    hash_item *it,
                    hash_item *new_it) {
    MEMCACHED_ITEM_REPLACE(hash_key_get_client_key(item_get_key(it)),
                           hash_key_get_client_key_len(item_get_key(it)),
                           it->nbytes,
                           hash_key_get_client_key(item_get_key(new_it)),
                           hash_key_get_client_key_len(item_get_key(new_it)),
                           new_it->nbytes);
    cb_assert((it->iflag & ITEM_SLABBED) == 0);

    do_item_unlink(engine, it);
    return do_item_link(engine, cookie, new_it);
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
    unsigned int* histogram = static_cast<unsigned int*>
        (cb_calloc(num_buckets, sizeof(unsigned int)));

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
                if (klen > 0 && klen < int(sizeof(key)) && vlen > 0 &&
                    vlen < int(sizeof(val))) {
                    add_stats(key, klen, val, vlen, c);
                }
            }
        }
        cb_free(histogram);
    }
}

/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get(struct default_engine *engine,
                       const hash_key *key,
                       const DocumentState document_state) {
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *it = assoc_find(engine,
                               crc32c(hash_key_get_key(key),
                                      hash_key_get_key_len(key), 0),
                               key);
    int was_found = 0;

    if (engine->config.verbose > 2) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = static_cast<EXTENSION_LOGGER_DESCRIPTOR*>
            (engine->server.extension->get_extension(EXTENSION_LOGGER));
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
        logger = static_cast<EXTENSION_LOGGER_DESCRIPTOR*>
            (engine->server.extension->get_extension(EXTENSION_LOGGER));
        logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by flush");
        was_found--;
    }

    if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
        do_item_unlink(engine, it);           /* MTSAFE - items.lock held */
        it = NULL;
    }

    if (it == NULL && was_found) {
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        logger = static_cast<EXTENSION_LOGGER_DESCRIPTOR*>
            (engine->server.extension->get_extension(EXTENSION_LOGGER));
        logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by expire");
        was_found--;
    }

    if (it != NULL) {
        if (it->iflag & ITEM_ZOMBIE) {
            if ((uint8_t(document_state) & uint8_t(DocumentState::Deleted)) == 0) {
                // The requested document is deleted, and you asked for alive
                return nullptr;
            }
        } else {
            if ((uint8_t(document_state) & uint8_t(DocumentState::Alive)) == 0) {
                // The requested document is Alive, and you asked for Dead
                return nullptr;
            }
        }

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
    hash_item *old_it = do_item_get(engine, key, document_any_state);
    ENGINE_ERROR_CODE stored = ENGINE_NOT_STORED;

    bool locked = false;
    if (old_it != nullptr && old_it->locktime != 0) {
        locked = old_it->locktime > engine->server.core->get_current_time();
    }

    if (old_it != NULL && operation == OPERATION_ADD &&
        (old_it->iflag & ITEM_ZOMBIE) == 0) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(engine, old_it);
    } else if (!old_it && operation == OPERATION_REPLACE) {
        /* replace only replaces an existing value; don't store */
    } else if (operation == OPERATION_CAS) {
        /* validate cas operation */
        if (old_it == NULL) {
            /* LRU expired */
            stored = ENGINE_KEY_ENOENT;
        } else if (it->cas == old_it->cas) {
            /* cas validates */
            /* it and old_it may belong to different classes. */
            /* I'm updating the stats for the one that's getting pushed out */
            do_item_replace(engine, cookie, old_it, it);
            stored = ENGINE_SUCCESS;
        } else {
            if (engine->config.verbose > 1) {
                EXTENSION_LOGGER_DESCRIPTOR *logger;
                logger = static_cast<EXTENSION_LOGGER_DESCRIPTOR*>
                    (engine->server.extension->get_extension(EXTENSION_LOGGER));
                logger->log(EXTENSION_LOG_INFO, NULL,
                        "CAS:  failure: expected %" PRIu64", got %" PRIu64"\n",
                        old_it->cas, it->cas);
            }

            if (locked) {
                stored = ENGINE_LOCKED;
            } else {
                stored = ENGINE_KEY_EEXISTS;
            }
        }
    } else {
        if (locked) {
            stored = ENGINE_LOCKED;
        } else {
            stored = ENGINE_SUCCESS;
            if (old_it != NULL) {
                do_item_replace(engine, cookie, old_it, it);
            } else {
                if (do_item_link(engine, cookie, it) == 0) {
                    stored = ENGINE_FAILED;
                }
            }
        }
    }

    if (old_it != NULL) {
        do_item_release(engine, old_it);         /* release our reference */
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
                    const size_t nkey,
                    DocumentState document_state) {
    hash_item *it;
    hash_key hkey;
    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return NULL;
    }
    cb_mutex_enter(&engine->items.lock);
    it = do_item_get(engine, &hkey, document_state);
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

ENGINE_ERROR_CODE safe_item_unlink(struct default_engine *engine,
                                   hash_item *it) {
    cb_mutex_enter(&engine->items.lock);
    auto ret = do_safe_item_unlink(engine, it);
    cb_mutex_exit(&engine->items.lock);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE store_item(struct default_engine *engine,
                             hash_item *item, uint64_t *cas,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie,
                             const DocumentState document_state) {
    ENGINE_ERROR_CODE ret;
    hash_item* stored_item = NULL;

    if (document_state == DocumentState::Deleted) {
        item->iflag |= ITEM_ZOMBIE;
    }

    cb_mutex_enter(&engine->items.lock);
    ret = do_store_item(engine, item, operation, cookie, &stored_item);
    if (ret == ENGINE_SUCCESS) {
        *cas = stored_item->cas;
    }
    cb_mutex_exit(&engine->items.lock);
    return ret;
}

static hash_item *do_touch_item(struct default_engine *engine,
                                const hash_key *hkey,
                                uint32_t exptime)
{
   hash_item *item = do_item_get(engine, hkey, DocumentState::Alive);
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

ENGINE_ERROR_CODE do_item_get_locked(struct default_engine* engine,
                                     const void* cookie,
                                     hash_item** it,
                                     const hash_key* hkey,
                                     rel_time_t locktime) {

    hash_item* item = do_item_get(engine, hkey, DocumentState::Alive);
    if (item == nullptr) {
        return ENGINE_KEY_ENOENT;
    }

    if (item->locktime != 0 &&
        item->locktime > engine->server.core->get_current_time()) {
        do_item_release(engine, item);
        return ENGINE_LOCKED;
    }

    /*
     * Unfortunately I have to create an extra copy of the item to return
     * back to the caller, as I need a way to know if I should mask out
     * the CAS or not. We don't have enough memory in the hash_item obj to
     * identify the connection when we need to check if the CAS should be
     * masked out (or not). I don't want us to lock memory on a per item
     * base as I don't suspect most items to ever be locked.
     *
     * Instead I decided to create an extra clone of the item, and if the
     * item isn't linked we should revel the real CAS.
     *
     * If I failed to create the temporary item I'm returning ENGINE_TMPFAIL.
     *
     * There is one minor optimization here.. If I'm the only one accessing
     * the object (refcount == 1) I can update the in-memory object and only
     * create a single copy. If multiple users holds a reference to the object
     * I have to create two clones (one to put in the hashmap, and the
     * temporary object to return back).
     */
    if (item->refcount == 1) {
        // we're the only one with access, let's just do an in-place
        // update of the metadata.

        // Unfortunately I can't return the actual object as that'll cause
        // the item's cas to be masked out ;-)
        auto* clone = do_item_alloc(engine, hkey, item->flags, item->exptime,
                                    item->nbytes, cookie, item->datatype);
        if (clone == nullptr) {
            do_item_release(engine, item);
            return ENGINE_TMPFAIL;
        }

        // let's just do an in-place update of the metadata. and return the
        // copy
        clone->locktime = item->locktime = locktime;
        clone->cas = item->cas = get_cas_id();

        // Copy the payload
        std::memcpy(item_get_data(clone), item_get_data(item), item->nbytes);

        // Release the one in the linked table
        do_item_release(engine, item);
        *it = clone;
    } else {
        // Multiple entities holds a reference to the object. We
        // need to do a copy/replace.
        auto* clone1 = do_item_alloc(engine, hkey, item->flags, item->exptime,
                                     item->nbytes, cookie, item->datatype);
        if (clone1 == nullptr) {
            do_item_release(engine, item);
            return ENGINE_TMPFAIL;
        }

        auto* clone2 = do_item_alloc(engine, hkey, item->flags, item->exptime,
                                     item->nbytes, cookie, item->datatype);
        if (clone2 == nullptr) {
            do_item_release(engine, item);
            do_item_release(engine, clone1);
            return ENGINE_TMPFAIL;
        }

        std::memcpy(item_get_data(clone1), item_get_data(item), item->nbytes);
        std::memcpy(item_get_data(clone2), item_get_data(item), item->nbytes);
        clone1->locktime = clone2->locktime = locktime;

        do_item_replace(engine, cookie, item, clone1);

        // do_item_replace generated a new cas id for this object
        clone2->cas = clone1->cas;

        // Release references
        do_item_release(engine, item);
        do_item_release(engine, clone1);
        *it = clone2;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_get_locked(struct default_engine* engine,
                                  const void* cookie,
                                  hash_item** it,
                                  const void* key,
                                  const size_t nkey,
                                  rel_time_t locktime) {
    hash_key hkey;

    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return ENGINE_TMPFAIL;
    }

    cb_mutex_enter(&engine->items.lock);
    ENGINE_ERROR_CODE ret = do_item_get_locked(engine, cookie, it, &hkey,
                                               locktime);
    cb_mutex_exit(&engine->items.lock);
    hash_key_destroy(&hkey);

    return ret;
}

static ENGINE_ERROR_CODE do_item_unlock(struct default_engine* engine,
                                        const void* cookie,
                                        const hash_key* hkey,
                                        uint64_t cas) {
    hash_item* item = do_item_get(engine, hkey, DocumentState::Alive);
    if (item == nullptr) {
        return ENGINE_KEY_ENOENT;
    }

    if (item->cas != cas) {
        // Invalid CAS value
        do_item_release(engine, item);
        return ENGINE_KEY_EEXISTS;
    }

    if (item->refcount == 1) {
        // I'm the only one with a reference to the object..
        // Just do an in-place release of the object
        item->locktime = 0;
        do_item_release(engine, item);
    } else {
        // Someone else holds a reference to the object.
        auto* clone = do_item_alloc(engine, hkey, item->flags, item->exptime,
                                    item->nbytes, cookie, item->datatype);
        if (clone == nullptr) {
            do_item_release(engine, item);
            return ENGINE_TMPFAIL;
        }

        std::memcpy(item_get_data(clone), item_get_data(item), item->nbytes);
        clone->locktime = 0;

        do_item_replace(engine, cookie, item, clone);
        do_item_release(engine, clone);
        do_item_release(engine, item);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_unlock(struct default_engine* engine,
                              const void* cookie,
                              const void* key,
                              const size_t nkey,
                              uint64_t cas) {
    hash_key hkey;

    if (!hash_key_create(&hkey, key, nkey, engine, cookie)) {
        return ENGINE_TMPFAIL;
    }

    cb_mutex_enter(&engine->items.lock);
    ENGINE_ERROR_CODE ret = do_item_unlock(engine, cookie, &hkey, cas);
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
        logger = static_cast<EXTENSION_LOGGER_DESCRIPTOR*>
            (engine->server.extension->get_extension(EXTENSION_LOGGER));
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

static bool hash_key_create(hash_key* hkey,
                            const void* key,
                            const size_t nkey,
                            struct default_engine* engine,
                            const void* cookie) {

    int hash_key_len = sizeof(bucket_id_t) + nkey;
    if (nkey > sizeof(hkey->key_storage.client_key)) {
        hkey->header.full_key =
            static_cast<hash_key_data*>(cb_malloc(hash_key_len));
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
