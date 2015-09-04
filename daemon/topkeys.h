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
#pragma once

#include <array>
#include <platform/cbassert.h>
#include <memcached/engine.h>
#include <cJSON.h>
#include <mutex>
#include <list>
#include <string>
#include <unordered_map>

struct topkey_item_t {
    topkey_item_t(rel_time_t create_time)
        : ti_ctime(create_time),
          ti_access_count(0) { }

    rel_time_t ti_ctime; /* Time this item was created */
    int ti_access_count; /* Int count for number of times key has been accessed */
};

/* Class to track the "top" keys in a bucket.
 */
class TopKeys {
public:
    /* Constructor.
     * @param mkeys Number of keys stored in each shard (i.e. up to
     * mkeys * SHARDS will be tracked).
     */
    TopKeys(int mkeys);
    ~TopKeys();

    void updateKey(const void *key,
                   size_t nkey,
                   rel_time_t operation_time);

    ENGINE_ERROR_CODE stats(const void *cookie,
                            const rel_time_t current_time,
                            ADD_STAT add_stat);

    /**
     * Passing a set of topkeys, and relevant context data will
     * return a cJSON object containing an array of topkeys:
     * {
     *   "topkeys": [
     *      {
     *          "key": "somekey",
     *          "access_count": nnn,
     *          "ctime": ccc,
     *          "atime": aaa
     *      }, ..., { ... }
     *    ]
     * }
     */
    ENGINE_ERROR_CODE json_stats(cJSON *object,
                                 const rel_time_t current_time);


private:
    // Number of shards the keyspace is broken into. Permits some level of
    // concurrent update (there is one mutex per shard).
    static const int NUM_SHARDS = 8;

    class Shard;

    Shard& getShard(const std::string& key);

    class Shard {
    public:

        void setMaxKeys(int mkeys) {
            max_keys = mkeys;
        }

        // Updates the topkey 'ranking' for the specified key.
        // If the item does not exist it will be created (with it's creation
        // time set to operation_time), otherwise the existing item will be
        // updated.
        // On success returns true, If insufficient memory to create a
        // new item, returns false.
        bool updateKey(const std::string& key,
                       rel_time_t operation_time);

        typedef void (*iterfunc_t)(const std::string& key,
                                   const topkey_item_t& it,
                                   void *arg);

        /* For each key in this shard, invoke the given callback function.
         */
        void accept_visitor(iterfunc_t visitor_func, void* visitor_ctx);

    private:

        // Maxumum numbers of keys to be tracked per shard.
        unsigned int max_keys;

        // mutex to serial access to this shard.
        std::mutex mutex;

        // list of keys, ordered from most-recently used (front) to least recently
        // used (back).
        typedef std::list<const std::string*> key_history_t;
        key_history_t list;

        // map of key to topkey stats.
        typedef std::unordered_map<std::string, topkey_item_t> key_hash_t;
        key_hash_t hash;

    };

    // array of topkey shards.
    std::array<Shard, NUM_SHARDS> shards;
};
