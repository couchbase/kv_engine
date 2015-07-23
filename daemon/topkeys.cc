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

#include <algorithm>
#include <sys/types.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <platform/platform.h>
#include "topkeys.h"

TopKeys::TopKeys(int mkeys)
    : max_keys(mkeys)
{
}

TopKeys::~TopKeys() {
}

topkey_item_t *TopKeys::getOrCreate(const void *key, size_t nkey, const rel_time_t ct) {
    try {
        std::string k(reinterpret_cast<const char*>(key), nkey);
        auto result = hash.emplace(std::make_pair(k, rel_time_t(ct)));
        if (result.second) {
            // New item was inserted.

            // check maximum keys hasn't been exceeded. If so remove
            // least-recently used item.
            if (hash.size() > max_keys) {
                const std::string* victim = list.back();
                cb_assert(hash.erase(*victim) == 1);
                list.pop_back();
            }
            // Add new item to head of the list (marking it as most-recently used).
            list.push_front(&result.first->first);

        } else {
            // Item already exists. Move this item to the head of the list
            // (marking it as MRU).
            auto it = std::find(list.begin(), list.end(),
                                &result.first->first);
            list.splice(list.begin(), list, it);
        }

        return &result.first->second;
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

static void tk_iterfunc(const std::string& key, const topkey_item_t& it,
                        void *arg) {
    struct tk_context *c = (struct tk_context*)arg;
    char val_str[500];
    int vlen = snprintf(val_str, sizeof(val_str) - 1, "get_hits=%d,"
                        "get_misses=0,cmd_set=0,incr_hits=0,incr_misses=0,"
                        "decr_hits=0,decr_misses=0,delete_hits=0,"
                        "delete_misses=0,evictions=0,cas_hits=0,cas_badval=0,"
                        "cas_misses=0,get_replica=0,evict=0,getl=0,unlock=0,"
                        "get_meta=0,set_meta=0,del_meta=0,ctime=%" PRIu32
                        ",atime=%" PRIu32, it.ti_access_count,
                        c->current_time - it.ti_ctime,
                        c->current_time - it.ti_atime);
    c->add_stat(key.c_str(), key.size(), val_str, vlen, c->cookie);
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
static void tk_jsonfunc(const std::string& key, const topkey_item_t& it,
                        void* arg) {
    struct tk_context *c = (struct tk_context*)arg;
    cJSON *obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "key", cJSON_CreateString(key.c_str()));
    cJSON_AddItemToObject(obj, "access_count",
                          cJSON_CreateNumber(it.ti_access_count));
    cJSON_AddItemToObject(obj, "ctime", cJSON_CreateNumber(c->current_time
                                                           - it.ti_ctime));
    cJSON_AddItemToObject(obj, "atime", cJSON_CreateNumber(c->current_time
                                                           - it.ti_atime));
    cb_assert(c->array != NULL);
    cJSON_AddItemToArray(c->array, obj);
}

ENGINE_ERROR_CODE TopKeys::stats(const void *cookie,
                                 const rel_time_t current_time,
                                 ADD_STAT add_stat) {
    struct tk_context context(cookie, add_stat, current_time, nullptr);

    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& key : list) {
        tk_iterfunc(*key, hash.at(*key), (void*)&context);
    }

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
        for (const auto& key : list) {
            tk_jsonfunc(*key, hash.at(*key), (void*)&context);
        }
    }

    cJSON_AddItemToObject(object, "topkeys", topkeys);
    return ENGINE_SUCCESS;
}
