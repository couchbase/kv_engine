#include "config.h"
#include <sys/types.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <platform/platform.h>
#include "topkeys.h"

static topkey_item_t *topkey_item_init(const void *key, int nkey, rel_time_t ct) {
    topkey_item_t *it = calloc(sizeof(topkey_item_t) + nkey, 1);
    cb_assert(it);
    cb_assert(key);
    cb_assert(nkey > 0);
    it->ti_nkey = nkey;
    it->ti_ctime = ct;
    it->ti_atime = ct;
    /* Copy the key into the part trailing the struct */
    memcpy(it + 1, key, nkey);
    return it;
}

static size_t topkey_item_size(const topkey_item_t *it) {
    return sizeof(topkey_item_t) + it->ti_nkey;
}

static topkey_item_t* topkeys_tail(topkeys_t *tk) {
    return (topkey_item_t*)(tk->list.prev);
}

static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

topkeys_t *topkeys_init(int max_keys) {
    static struct hash_ops my_hash_ops;
    topkeys_t *tk = calloc(sizeof(topkeys_t), 1);
    if (tk == NULL) {
        return NULL;
    }

    my_hash_ops.hashfunc = genhash_string_hash;
    my_hash_ops.hasheq = my_hash_eq;
    my_hash_ops.dupKey = NULL;
    my_hash_ops.dupValue = NULL;
    my_hash_ops.freeKey = NULL;
    my_hash_ops.freeValue = NULL;

    cb_mutex_initialize(&tk->mutex);
    tk->max_keys = max_keys;
    tk->list.next = &tk->list;
    tk->list.prev = &tk->list;

    tk->hash = genhash_init(max_keys, my_hash_ops);
    if (tk->hash == NULL) {
        return NULL;
    }

    return tk;
}

void topkeys_free(topkeys_t *tk) {
    dlist_t *p;
    cb_mutex_destroy(&tk->mutex);
    genhash_free(tk->hash);
    p = tk->list.next;
    while (p != &tk->list) {
        dlist_t *tmp = p->next;
        free(p);
        p = tmp;
    }
    free(tk);
}

static void dlist_remove(dlist_t *list) {
    cb_assert(list->prev->next == list);
    cb_assert(list->next->prev == list);
    list->prev->next = list->next;
    list->next->prev = list->prev;
}

static void dlist_insert_after(dlist_t *list, dlist_t *new) {
    new->next = list->next;
    new->prev = list;
    list->next->prev = new;
    list->next = new;
}

static void dlist_iter(dlist_t *list,
                              void (*iterfunc)(dlist_t *item, void *arg),
                              void *arg)
{
    dlist_t *p = list;
    while ((p = p->next) != list) {
        iterfunc(p, arg);
    }
}

static void topkeys_item_delete(topkeys_t *tk, topkey_item_t *it) {
    genhash_delete(tk->hash, it + 1, it->ti_nkey);
    dlist_remove(&it->ti_list);
    --tk->nkeys;
    free(it);
}

topkey_item_t *topkeys_item_get_or_create(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ct) {
    topkey_item_t *it = genhash_find(tk->hash, key, nkey);
    if (it == NULL) {
        it = topkey_item_init(key, (int)nkey, ct);
        if (it != NULL) {
            if (++tk->nkeys > tk->max_keys) {
                topkeys_item_delete(tk, topkeys_tail(tk));
            }
            genhash_update(tk->hash, it + 1, it->ti_nkey,
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

static void tk_iterfunc(dlist_t *list, void *arg) {
    struct tk_context *c = arg;
    topkey_item_t *it = (topkey_item_t*)list;
    char val_str[TK_MAX_VAL_LEN];
    int vlen = snprintf(val_str, sizeof(val_str) - 1, "get_hits=%d,"
                        "get_misses=0,cmd_set=0,incr_hits=0,incr_misses=0,"
                        "decr_hits=0,decr_misses=0,delete_hits=0,"
                        "delete_misses=0,evictions=0,cas_hits=0,cas_badval=0,"
                        "cas_misses=0,get_replica=0,evict=0,getl=0,unlock=0,"
                        "get_meta=0,set_meta=0,del_meta=0,ctime=%"PRIu32
                        ",atime=%"PRIu32, it->access_count,
                        c->current_time - it->ti_ctime,
                        c->current_time - it->ti_atime);
    c->add_stat((char*)(it + 1), it->ti_nkey, val_str, vlen, c->cookie);
}

ENGINE_ERROR_CODE topkeys_stats(topkeys_t **tks, size_t shards,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat) {
    struct tk_context context;
    size_t i;
    context.cookie = cookie;
    context.add_stat = add_stat;
    context.current_time = current_time;
    for (i = 0; i < shards; i++) {
        topkeys_t *tk = tks[i];
        cb_assert(tk);
        cb_mutex_enter(&tk->mutex);
        dlist_iter(&tk->list, tk_iterfunc, &context);
        cb_mutex_exit(&tk->mutex);
    }
    return ENGINE_SUCCESS;
}

topkeys_t *tk_get_shard(topkeys_t **tks, const void *key, size_t nkey) {
    /* This is special-cased for 8 */
    int khash;
    cb_assert(TK_SHARDS == 8);
    khash = genhash_string_hash(key, nkey);
    return tks[khash & 0x07];
}

/* Update the access_count for any valid operation */
void topkeys_update(topkeys_t **tks, const void *key, size_t nkey,
                    rel_time_t operation_time) {
    if (tks) {
        cb_assert(key);
        cb_assert(nkey > 0);
        topkeys_t *tk = tk_get_shard(tks, key, nkey);
        cb_mutex_enter(&tk->mutex);
        topkey_item_t *tmp = topkeys_item_get_or_create(tk, key, nkey,
                                                        operation_time);
        if (tmp != NULL) {
            tmp->access_count++;
        }
        cb_mutex_exit(&tk->mutex);
    }
}
