/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"
#include <sys/types.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <platform/platform.h>
#include "topkeys.h"

static topkey_item_t *topkey_item_init(const void *key, int nkey, rel_time_t ct) {
    topkey_item_t *it = (topkey_item_t *)calloc(sizeof(topkey_item_t) + nkey, 1);
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

TopKeys::TopKeys(int mkeys)
    : nkeys(0), max_keys(mkeys)
{
    list.next = &list;
    list.prev = &list;
}

TopKeys::~TopKeys() {
    dlist_t *p = list.next;
    while (p != &list) {
        dlist_t *tmp = p->next;
        free(p);
        p = tmp;
    }
}

static void dlist_remove(dlist_t *list) {
    cb_assert(list->prev->next == list);
    cb_assert(list->next->prev == list);
    list->prev->next = list->next;
    list->next->prev = list->prev;
}

static void dlist_insert_after(dlist_t *list, dlist_t *newitem) {
    newitem->next = list->next;
    newitem->prev = list;
    list->next->prev = newitem;
    list->next = newitem;
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

void TopKeys::deleteTail() {
    topkey_item_t *it = (topkey_item_t*)(list.prev);
    std::string key(reinterpret_cast<char*>(it+1), it->ti_nkey);
    auto iterator = hash.find(key);
    cb_assert(iterator != hash.end());
    hash.erase(iterator);
    dlist_remove(&it->ti_list);
    --nkeys;
    free(it);
}


topkey_item_t *TopKeys::getOrCreate(const void *key, size_t nkey, const rel_time_t ct) {
    try {
        std::string k(reinterpret_cast<const char*>(key), nkey);
        auto iterator = hash.find(k);
        topkey_item_t *it = nullptr;

        if (iterator == hash.end()) {
            it = topkey_item_init(key, (int)nkey, ct);
            if (it != NULL) {
                if (++nkeys > max_keys) {
                    deleteTail();
                }
                hash[k] = it;
            } else {
                return NULL;
            }
        } else {
            it = iterator->second;
            dlist_remove(&it->ti_list);
        }
        dlist_insert_after(&list, &it->ti_list);
        return it;
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

void TopKeys::updateKey(const void *key, size_t nkey,
                      rel_time_t operation_time) {
    cb_assert(key);
    cb_assert(nkey > 0);
    std::lock_guard<std::mutex> lock(mutex);
    topkey_item_t *tmp = getOrCreate(key, nkey, operation_time);
    if (tmp != NULL) {
        tmp->ti_access_count++;
    }
}

struct tk_context {
    tk_context(const void *c, ADD_STAT a, rel_time_t t, cJSON *arr)
        : cookie(c), add_stat(a), current_time(t), array(arr)
    {
        // empty
    }

    const void *cookie;
    ADD_STAT add_stat;
    rel_time_t current_time;
    cJSON *array;
};

static void tk_iterfunc(dlist_t *list, void *arg) {
    struct tk_context *c = (struct tk_context*)arg;
    topkey_item_t *it = (topkey_item_t*)list;
    char val_str[500];
    int vlen = snprintf(val_str, sizeof(val_str) - 1, "get_hits=%d,"
                        "get_misses=0,cmd_set=0,incr_hits=0,incr_misses=0,"
                        "decr_hits=0,decr_misses=0,delete_hits=0,"
                        "delete_misses=0,evictions=0,cas_hits=0,cas_badval=0,"
                        "cas_misses=0,get_replica=0,evict=0,getl=0,unlock=0,"
                        "get_meta=0,set_meta=0,del_meta=0,ctime=%" PRIu32
                        ",atime=%" PRIu32, it->ti_access_count,
                        c->current_time - it->ti_ctime,
                        c->current_time - it->ti_atime);
    c->add_stat((char*)(it + 1), it->ti_nkey, val_str, vlen, c->cookie);
}

/**
 * Passing in a list of keys, context, and cJSON array will populate that
 * array with an object for each key in the following format:
 * {
 *    "key": "somekey",
 *    "access_count": nnn,
 *    "ctime": ccc,
 *    "atime": aaa
 * }
 */
static void tk_jsonfunc(dlist_t *list, void *arg) {
    struct tk_context *c = (struct tk_context*)arg;
    topkey_item_t *it = (topkey_item_t*)list;
    cb_assert(it != NULL);
    cJSON *key = cJSON_CreateObject();
    cJSON_AddItemToObject(key, "key", cJSON_CreateString((char *)(it + 1)));
    cJSON_AddItemToObject(key, "access_count",
                          cJSON_CreateNumber(it->ti_access_count));
    cJSON_AddItemToObject(key, "ctime", cJSON_CreateNumber(c->current_time
                                                           - it->ti_ctime));
    cJSON_AddItemToObject(key, "atime", cJSON_CreateNumber(c->current_time
                                                           - it->ti_atime));
    cb_assert(c->array != NULL);
    cJSON_AddItemToArray(c->array, key);
}

ENGINE_ERROR_CODE TopKeys::stats(const void *cookie,
                                 const rel_time_t current_time,
                                 ADD_STAT add_stat) {
    struct tk_context context(cookie, add_stat, current_time, nullptr);

    std::lock_guard<std::mutex> lock(mutex);
    dlist_iter(&list, tk_iterfunc, &context);

    return ENGINE_SUCCESS;
}

/**
 * Passing a set of topkeys, and relevant context data will
 * return a cJSON object containing an array of topkeys (with each key
 * appearing as in the example above for tk_jsonfunc):
 * {
 *   "topkeys": [
 *      { ... }, ..., { ... }
 *    ]
 * }
 */
ENGINE_ERROR_CODE TopKeys::json_stats(cJSON *object,
                                     const rel_time_t current_time) {

    cJSON *topkeys = cJSON_CreateArray();
    struct tk_context context(nullptr, nullptr, current_time, topkeys);

    /* Collate the topkeys JSON object */
    {
        std::lock_guard<std::mutex> lock(mutex);
        dlist_iter(&list, tk_jsonfunc, &context);
    }

    cJSON_AddItemToObject(object, "topkeys", topkeys);
    return ENGINE_SUCCESS;
}
