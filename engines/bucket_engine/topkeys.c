#include <sys/types.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <pthread.h>
#include "topkeys.h"

static topkey_item_t *topkey_item_init(const void *key, int nkey, rel_time_t ct) {
    topkey_item_t *it = calloc(sizeof(topkey_item_t) + nkey, 1);
    assert(it);
    assert(key);
    assert(nkey > 0);
    it->ti_nkey = nkey;
    it->ti_ctime = ct;
    it->ti_atime = ct;
    /* Copy the key into the part trailing the struct */
    memcpy(it->ti_key, key, nkey);
    return it;
}

static inline size_t topkey_item_size(const topkey_item_t *it) {
    return sizeof(topkey_item_t) + it->ti_nkey;
}

static inline topkey_item_t* topkeys_tail(topkeys_t *tk) {
    return (topkey_item_t*)(tk->list.prev);
}

static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

topkeys_t *topkeys_init(int max_keys) {
    topkeys_t *tk = calloc(sizeof(topkeys_t), 1);
    if (tk == NULL) {
        return NULL;
    }

    if (pthread_mutex_init(&tk->mutex, NULL) != 0) {
        free(tk);
        return NULL;
    }
    tk->max_keys = max_keys;
    tk->list.next = &tk->list;
    tk->list.prev = &tk->list;

    static struct hash_ops my_hash_ops = {
        .hashfunc = genhash_string_hash,
        .hasheq = my_hash_eq,
        .dupKey = NULL,
        .dupValue = NULL,
        .freeKey = NULL,
        .freeValue = NULL,
    };

    tk->hash = genhash_init(max_keys, my_hash_ops);
    if (tk->hash == NULL) {
        return NULL;
    }
    return tk;
}

void topkeys_free(topkeys_t *tk) {
    assert(pthread_mutex_destroy(&tk->mutex)== 0);
    genhash_free(tk->hash);
    dlist_t *p = tk->list.next;
    while (p != &tk->list) {
        dlist_t *tmp = p->next;
        free(p);
        p = tmp;
    }
}

static inline void dlist_remove(dlist_t *list) {
    assert(list->prev->next == list);
    assert(list->next->prev == list);
    list->prev->next = list->next;
    list->next->prev = list->prev;
}

static inline void dlist_insert_after(dlist_t *list, dlist_t *new) {
    new->next = list->next;
    new->prev = list;
    list->next->prev = new;
    list->next = new;
}

static inline void dlist_iter(dlist_t *list,
                              void (*iterfunc)(dlist_t *item, void *arg),
                              void *arg)
{
    dlist_t *p = list;
    while ((p = p->next) != list) {
        iterfunc(p, arg);
    }
}

static inline void topkeys_item_delete(topkeys_t *tk, topkey_item_t *it) {
    genhash_delete(tk->hash, it->ti_key, it->ti_nkey);
    dlist_remove(&it->ti_list);
    --tk->nkeys;
    free(it);
}

topkey_item_t *topkeys_item_get_or_create(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ct) {
    topkey_item_t *it = genhash_find(tk->hash, key, nkey);
    if (it == NULL) {
        it = topkey_item_init(key, nkey, ct);
        if (it != NULL) {
            if (++tk->nkeys > tk->max_keys) {
                topkeys_item_delete(tk, topkeys_tail(tk));
            }
            genhash_update(tk->hash, it->ti_key, it->ti_nkey,
                           it, topkey_item_size(it));
        } else {
            return NULL;
        }
    } else {
        dlist_remove(&it->ti_list);
    }
    dlist_insert_after(&tk->list, &it->ti_list);
    return it;
}

struct tk_context {
    const void *cookie;
    ADD_STAT add_stat;
    rel_time_t current_time;
};

#define TK_FMT(name) #name "=%d,"
#define TK_ARGS(name) it->name,

static void tk_iterfunc(dlist_t *list, void *arg) {
    struct tk_context *c = arg;
    topkey_item_t *it = (topkey_item_t*)list;
    char val_str[TK_MAX_VAL_LEN];
    /* This line is magical. The missing comma before item->ctime is because the TK_ARGS macro ends with a comma. */
    int vlen = snprintf(val_str, sizeof(val_str) - 1, TK_OPS(TK_FMT)"ctime=%"PRIu32",atime=%"PRIu32, TK_OPS(TK_ARGS)
                        c->current_time - it->ti_ctime, c->current_time - it->ti_atime);
    c->add_stat(it->ti_key, it->ti_nkey, val_str, vlen, c->cookie);
}

ENGINE_ERROR_CODE topkeys_stats(topkeys_t **tks, size_t shards,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat) {
    struct tk_context context;
    context.cookie = cookie;
    context.add_stat = add_stat;
    context.current_time = current_time;
    for (size_t i = 0; i < shards; i++) {
        topkeys_t *tk = tks[i];
        assert(tk);
        must_lock(&tk->mutex);
        dlist_iter(&tk->list, tk_iterfunc, &context);
        must_unlock(&tk->mutex);
    }
    return ENGINE_SUCCESS;
}

topkeys_t *tk_get_shard(topkeys_t **tks, const void *key, size_t nkey) {
    // This is special-cased for 8
    assert(TK_SHARDS == 8);
    int khash = genhash_string_hash(key, nkey);
    return tks[khash & 0x07];
}
