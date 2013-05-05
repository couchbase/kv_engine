/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TOPKEYS_H
#define TOPKEYS_H 1

#include <memcached/engine.h>
#include "genhash.h"

/* A list of operations for which we have int stats */
#define TK_OPS(C) C(get_hits) C(get_misses) C(cmd_set) C(incr_hits) \
    C(incr_misses) C(decr_hits) C(decr_misses)                      \
    C(delete_hits) C(delete_misses) C(evictions)                    \
    C(cas_hits) C(cas_badval) C(cas_misses) C(get_replica)          \
    C(evict) C(getl) C(unlock) C(get_meta) C(set_meta)              \
    C(del_meta)

#define TK_MAX_VAL_LEN 500

#define TK_SHARDS 8

/* Update the correct stat for a given operation */
#define TK(tks, op, key, nkey, ctime) \
{ \
    if (tks) { \
        topkeys_t *tk; \
        topkey_item_t *tmp; \
        assert(key); \
        assert(nkey > 0); \
        tk = tk_get_shard((tks), (key), (nkey)); \
        cb_mutex_enter(&tk->mutex); \
        tmp = topkeys_item_get_or_create((tk), (key), (nkey), (ctime)); \
        if (tmp != NULL) { \
            tmp->op++; \
        } \
        cb_mutex_exit(&tk->mutex); \
    } \
}

typedef struct dlist {
    struct dlist *next;
    struct dlist *prev;
} dlist_t;

typedef struct topkey_item {
    dlist_t ti_list; /* Must be at the beginning because we downcast! */
    int ti_nkey;
    rel_time_t ti_ctime, ti_atime; /* Time this item was created/last accessed */
#define TK_CUR(ti_name) int ti_name;
    TK_OPS(TK_CUR)
#undef TK_CUR
    /* char ti_key[]; /\* A variable length array in the struct itself *\/ */
} topkey_item_t;

typedef struct topkeys {
    dlist_t list;
    cb_mutex_t mutex;
    genhash_t *hash;
    int nkeys;
    int max_keys;
} topkeys_t;

topkeys_t *topkeys_init(int max_keys);
void topkeys_free(topkeys_t *topkeys);
topkeys_t *tk_get_shard(topkeys_t **tk, const void *key, size_t nkey);
topkey_item_t *topkeys_item_get_or_create(topkeys_t *tk,
                                          const void *key,
                                          size_t nkey,
                                          const rel_time_t ctime);

ENGINE_ERROR_CODE topkeys_stats(topkeys_t **tk, size_t n,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat);

#endif
