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
#include <cstring>
#include <sys/types.h>
#include <stdlib.h>
#include <inttypes.h>
#include <platform/platform.h>

#include "topkeys.h"

/*
 * Implementation Details
 *
 * === TopKeys ===
 *
 * The TopKeys class is split into NUM_SHARDS shards, each which owns
 * 1/NUM_SHARDS of the keyspace. This is to allow some level of
 * concurrent access - each shard has a mutex guarding all access.
 * Other than that the TopKeys class is pretty uninteresting - it
 * simply passes on requests to the correct Shard, and when statistics
 * are requested it aggregates information from each shard.
 *
 * === TopKeys::Shard ===
 *
 * This is where the action happens. Each Shard maintains an ordered
 * list of key names and related statistics, orded by when they were
 * last accessed. There is a fixed number of keys each Shard tracks.

 * Given we generally track only a small number of keys per shard (10
 * by default), it's highly likely that any one key access will 'miss'
 * - i.e. not find an existing topkey. Therefore this class is
 * optimized for that case, attempting to minimise memory allocation
 * in steady-state.
 *
 * Internally Shard consists of a vector of topkey_t, storing the
 * current max_keys top_keys, and a list of topkey_t pointers, used to
 * main the current most -> least recently used ordering:

 * max_keys elements. Each element is a tuple of
 * {hash(key), key, topkey_stats}:
 *
 *
 *       vector<topkey_t>
 *   +----------+-------------+---------------+
 *   | size_t   | std::string | topkey_item_t |
 *   +----------+-------------+---------------+
 *   | <hash 1> | <key 1>     | stats 1       | <--- Node1
 *   | <hash 2> | <key 2>     | stats 2       | <--- Node2
 *   | <hash 3> | <key 3>     | stats 3       | <--- Node3
 *   . ....                                   .
 *   | <hash N> | <key N>     | stats N       | <--- NodeN
 *   +----------------------------------------+
 *
 *   Node3 <---> Node1 <---> ... <---> Node2
 *   ^^^                               ^^^
 *   MRU                               LRU
 *
 *
 * Upon a key 'hit', TopKeys::updateKey() is called. That hashes the
 * key, and finds the Shard responsible for that
 * key. TopKeys::Shard::updateKey() is then called.
 *
 * Shard::updateKey() iterates over the vector, searching for a hash
 * match with an existing element, using the actual key string to
 * validate (in case of a hash collision). If it is found, then the
 * stats are simply updated.  If it is not found, then the
 * least-recently-used element is selected as a 'victim' and it's
 * contents is replaced by the incoming key.  Finally the linked-list
 * is updated to move the updated element to the head of the list.
 */


TopKeys::TopKeys(int mkeys) {
    for (auto& shard : shards) {
        shard.setMaxKeys(mkeys);
    }
}

TopKeys::~TopKeys() {
}

TopKeys::Shard& TopKeys::getShard(size_t key_hash) {
    /* This is special-cased for 8 */
    static_assert(NUM_SHARDS == 8,
                  "Topkeys::getShard() special-cased for SHARDS==8");
    return shards[key_hash & 0x7];
}

TopKeys::Shard::topkey_t*
TopKeys::Shard::searchForKey(size_t key_hash,
                             const const_char_buffer& key) {

    for (auto& topkey : storage) {
        if (topkey.first.hash == key_hash) {
            // Double-check with full compare
            if (topkey.first.key.compare(0, topkey.first.key.size(),
                                         key.buf, key.len) == 0) {
                // Match found.
                return &topkey;
            }
        }
    }
    return nullptr;
}

bool TopKeys::Shard::updateKey(const const_char_buffer& key,
                               size_t key_hash,
                               const rel_time_t ct) {
    try {
        std::lock_guard<std::mutex> lock(mutex);

        topkey_t* found_key = searchForKey(key_hash, key);

        if (found_key == NULL) {
            // Key not found.
            if (storage.size() == max_keys) {
                // Re-use the lowest keys' storage.
                found_key = list.back();
                found_key->first.hash = key_hash;
                found_key->first.key.assign(key.buf, key.len);
                found_key->second = topkey_item_t(ct);

                // Move back element to the front, shuffling down the
                // rest.
                list.splice(list.begin(), list, --list.end());

            } else {
                // add a new element to the storage array.

                storage.emplace_back(
                    std::make_pair(KeyId{key_hash,
                                         std::string(key.buf, key.len)},
                                   topkey_item_t(ct)));
                found_key = &storage.back();

                // Insert the new item to the front of the list
                list.push_front(found_key);
            }
        } else {
            // Found - shuffle to the front.
            auto it = std::find(list.begin(), list.end(),
                                found_key);
            list.splice(list.begin(), list, it);
        }

        // Increment access count.
        found_key->second.ti_access_count++;
        return true;

    } catch (const std::bad_alloc&) {
        // Failed to update.
        return false;
    }
}

void TopKeys::updateKey(const void *key, size_t nkey,
                        rel_time_t operation_time) {
    cb_assert(key);
    cb_assert(nkey > 0);

    try {
        const_char_buffer key_buf(static_cast<const char*>(key), nkey);
        std::hash<const_char_buffer > hash_fn;
        const size_t key_hash = hash_fn(key_buf);

        getShard(key_hash).updateKey(key_buf, key_hash, operation_time);
    } catch (std::bad_alloc) {
        // Failed to increment topkeys, continue...
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
    /* Note we use accessed time for both 'atime' and 'ctime' below. They have
     * had the same value since the topkeys code was added; but given that
     * clients may expect separate values we print both.
     */
    rel_time_t created_time = c->current_time - it.ti_ctime;
    int vlen = snprintf(val_str, sizeof(val_str) - 1, "get_hits=%d,"
                        "get_misses=0,cmd_set=0,incr_hits=0,incr_misses=0,"
                        "decr_hits=0,decr_misses=0,delete_hits=0,"
                        "delete_misses=0,evictions=0,cas_hits=0,cas_badval=0,"
                        "cas_misses=0,get_replica=0,evict=0,getl=0,unlock=0,"
                        "get_meta=0,set_meta=0,del_meta=0,ctime=%" PRIu32
                        ",atime=%" PRIu32, it.ti_access_count,
                        created_time, created_time);
    if (vlen > 0 && vlen < int(sizeof(val_str) - 1)) {
        c->add_stat(key.c_str(), key.size(), val_str, vlen, c->cookie);
    }
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
    cb_assert(c->array != NULL);
    cJSON_AddItemToArray(c->array, obj);
}

ENGINE_ERROR_CODE TopKeys::stats(const void *cookie,
                                 const rel_time_t current_time,
                                 ADD_STAT add_stat) {
    struct tk_context context(cookie, add_stat, current_time, nullptr);

    for (auto& shard : shards) {
        shard.accept_visitor(tk_iterfunc, &context);
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
    for (auto& shard : shards) {
        shard.accept_visitor(tk_jsonfunc, &context);
    }

    cJSON_AddItemToObject(object, "topkeys", topkeys);
    return ENGINE_SUCCESS;
}

void TopKeys::Shard::accept_visitor(iterfunc_t visitor_func,
                                    void* visitor_ctx) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto key : list) {
        visitor_func(key->first.key, key->second, visitor_ctx);
    }
}
