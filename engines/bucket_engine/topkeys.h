/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TOPKEYS_H
#define TOPKEYS_H 1

#include <platform/cbassert.h>
#include <memcached/engine.h>
#include "genhash.h"

#define TK_MAX_VAL_LEN 500

#define TK_SHARDS 8

typedef struct dlist {
    struct dlist *next;
    struct dlist *prev;
} dlist_t;

typedef struct topkey_item {
    dlist_t ti_list; /* Must be at the beginning because we downcast! */
    int ti_nkey;
    rel_time_t ti_ctime, ti_atime; /* Time this item was created/last accessed */
    int access_count; /* Int count for number of times key has been accessed */
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
/* Update the access_count for any valid operation */
void topkeys_update(topkeys_t **tks, const void *key, size_t nkey,
                    rel_time_t operation_time);

ENGINE_ERROR_CODE topkeys_stats(topkeys_t **tk, size_t n,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat);

#endif
