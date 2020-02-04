/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 */
#include "default_engine_internal.h"

#include <logger/logger.h>
#include <platform/cbassert.h>
#include <platform/crc32c.h>
#include <platform/platform_thread.h>
#include <platform/strerror.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>


#define hashsize(n) ((size_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

struct Assoc {
    Assoc(unsigned int hp) : hashpower(hp) {
        primary_hashtable.resize(hashsize(hashpower));
    }

    /* how many powers of 2's worth of buckets we use */
    unsigned int hashpower;


    /* Main hash table. This is where we look except during expansion. */
    std::vector<hash_item*> primary_hashtable;

    /*
     * Previous hash table. During expansion, we look here for keys that haven't
     * been moved over to the primary yet.
     */
    std::vector<hash_item*> old_hashtable;

    /* Number of items in the hash table. */
    unsigned int hash_items{0};

    /* Flag: Are we in the middle of expanding now? */
    bool expanding{false};

    /*
     * During expansion we migrate values with bucket granularity; this is how
     * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
     */
    unsigned int expand_bucket{0};

    /*
     * serialise access to the hashtable
     */
    std::mutex mutex;
};

/* One hashtable for all */
static struct Assoc* global_assoc = nullptr;

/* assoc factory. returns one new assoc or NULL if out-of-memory */
static struct Assoc* assoc_consruct(int hashpower) {
    try {
        return new Assoc(hashpower);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

ENGINE_ERROR_CODE assoc_init(struct default_engine *engine) {
    /*
        construct and save away one assoc for use by all buckets.
    */
    if (global_assoc == nullptr) {
        global_assoc = assoc_consruct(16);
    }
    return (global_assoc != nullptr) ? ENGINE_SUCCESS : ENGINE_ENOMEM;
}

void assoc_destroy() {
    if (global_assoc != nullptr) {
        while (global_assoc->expanding) {
            std::this_thread::sleep_for(std::chrono::microseconds(250));
        }
        delete global_assoc;
        global_assoc = nullptr;
    }
}

hash_item *assoc_find(uint32_t hash, const hash_key *key) {
    hash_item *it;
    unsigned int oldbucket;
    hash_item *ret = nullptr;
    int depth = 0;
    std::lock_guard<std::mutex> guard(global_assoc->mutex);
    if (global_assoc->expanding &&
        (oldbucket = (hash & hashmask(global_assoc->hashpower - 1))) >= global_assoc->expand_bucket)
    {
        it = global_assoc->old_hashtable[oldbucket];
    } else {
        it = global_assoc->primary_hashtable[hash & hashmask(global_assoc->hashpower)];
    }

    while (it) {
        const hash_key* it_key = item_get_key(it);
        if ((hash_key_get_key_len(key) == hash_key_get_key_len(it_key)) &&
            (memcmp(hash_key_get_key(key),
                    hash_key_get_key(it_key),
                    hash_key_get_key_len(key)) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    return ret;
}

/*
    returns the address of the item pointer before the key.  if *item == 0,
    the item wasn't found
    assoc->lock is assumed to be held by the caller.
*/
static hash_item** _hashitem_before(uint32_t hash, const hash_key* key) {
    hash_item **pos;
    unsigned int oldbucket;

    if (global_assoc->expanding &&
        (oldbucket = (hash & hashmask(global_assoc->hashpower - 1))) >= global_assoc->expand_bucket)
    {
        pos = &global_assoc->old_hashtable[oldbucket];
    } else {
        pos = &global_assoc->primary_hashtable[hash & hashmask(global_assoc->hashpower)];
    }

    while (*pos) {
        const hash_key* pos_key = item_get_key(*pos);
        if ((hash_key_get_key_len(key) != hash_key_get_key_len(pos_key)) ||
            (memcmp(hash_key_get_key(key),
                    hash_key_get_key(pos_key),
                    hash_key_get_key_len(key)))) {
             pos = &(*pos)->h_next;
        } else {
            break;
        }
    }

    return pos;
}

static void assoc_maintenance_thread(void *arg);

/*
    grows the hashtable to the next power of 2.
    assoc->lock is assumed to be held by the caller.
*/
static void assoc_expand() {
    global_assoc->old_hashtable.swap(global_assoc->primary_hashtable);

    try {
        global_assoc->primary_hashtable.resize(hashsize(global_assoc->hashpower + 1));
    } catch (const std::bad_alloc&) {
        global_assoc->primary_hashtable.swap(global_assoc->old_hashtable);
        /* Bad news, but we can keep running. */
        return;
    }

    int ret = 0;
    cb_thread_t tid;

    global_assoc->hashpower++;
    global_assoc->expanding = true;
    global_assoc->expand_bucket = 0;

    /* start a thread to do the expansion */
    if ((ret = cb_create_named_thread(&tid, assoc_maintenance_thread,
                                      nullptr, 1, "mc:assoc_maint")) != 0)
    {
        LOG_ERROR("Can't create thread for rebalance assoc table: {}",
                  cb_strerror());
        global_assoc->hashpower--;
        global_assoc->expanding = false;
        global_assoc->primary_hashtable.swap(global_assoc->old_hashtable);
        global_assoc->old_hashtable.resize(0);
        global_assoc->old_hashtable.shrink_to_fit();
    }
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(uint32_t hash, hash_item *it) {
    unsigned int oldbucket;

    cb_assert(assoc_find(hash, item_get_key(it)) == nullptr);  /* shouldn't have duplicately named things defined */

    std::lock_guard<std::mutex> guard(global_assoc->mutex);
    if (global_assoc->expanding &&
        (oldbucket = (hash & hashmask(global_assoc->hashpower - 1))) >= global_assoc->expand_bucket)
    {
        it->h_next = global_assoc->old_hashtable[oldbucket];
        global_assoc->old_hashtable[oldbucket] = it;
    } else {
        it->h_next = global_assoc->primary_hashtable[hash & hashmask(global_assoc->hashpower)];
        global_assoc->primary_hashtable[hash & hashmask(global_assoc->hashpower)] = it;
    }

    global_assoc->hash_items++;
    if (! global_assoc->expanding && global_assoc->hash_items > (hashsize(global_assoc->hashpower) * 3) / 2) {
        assoc_expand();
    }
    return 1;
}

void assoc_delete(uint32_t hash, const hash_key *key) {
    std::lock_guard<std::mutex> guard(global_assoc->mutex);
    hash_item **before = _hashitem_before(hash, key);

    if (*before) {
        hash_item *nxt;
        global_assoc->hash_items--;
        nxt = (*before)->h_next;
        (*before)->h_next = nullptr;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    cb_assert(*before != nullptr);
}



#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void assoc_maintenance_thread(void *arg) {
    bool done = false;
    do {
        int ii;
        std::lock_guard<std::mutex> guard(global_assoc->mutex);

        for (ii = 0; ii < hash_bulk_move && global_assoc->expanding; ++ii) {
            hash_item *it, *next;
            int bucket;

            for (it = global_assoc->old_hashtable[global_assoc->expand_bucket];
                 nullptr != it; it = next) {
                next = it->h_next;
                const hash_key* key = item_get_key(it);
                bucket = crc32c(hash_key_get_key(key),
                                hash_key_get_key_len(key),
                                0) & hashmask(global_assoc->hashpower);
                it->h_next = global_assoc->primary_hashtable[bucket];
                global_assoc->primary_hashtable[bucket] = it;
            }

            global_assoc->old_hashtable[global_assoc->expand_bucket] = nullptr;
            global_assoc->expand_bucket++;
            if (global_assoc->expand_bucket == hashsize(global_assoc->hashpower - 1)) {
                global_assoc->expanding = false;
                global_assoc->old_hashtable.resize(0);
                global_assoc->old_hashtable.shrink_to_fit();
                LOG_INFO("Hash table expansion done");
            }
        }
        if (!global_assoc->expanding) {
            done = true;
        }
    } while (!done);
}

bool assoc_expanding() {
    std::lock_guard<std::mutex> guard(global_assoc->mutex);
    return global_assoc->expanding;
}
