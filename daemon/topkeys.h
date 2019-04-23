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

#include <memcached/engine.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>
#include <array>

#include <folly/CachelinePadded.h>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
struct tk_context;
/*
 * TopKeys
 *
 * Tracks the top N most recently accessed keys. The details are
 * accessible by a stats call, which is used by ns_server to print the
 * top keys list in the GUI.
 */

struct topkey_item_t {
    topkey_item_t(rel_time_t create_time)
        : ti_ctime(create_time), ti_access_count(0) {
    }

    rel_time_t ti_ctime; /* Time this item was created */
    int ti_access_count; /* Int count for number of times key has been accessed
                          */
};

/* Class to track the "top" keys in a bucket.
 */
class TopKeys {
public:
    /** Constructor.
     * @param mkeys Number of keys stored in each shard (i.e. up to
     *              mkeys * SHARDS will be tracked).
     */
    explicit TopKeys(int mkeys);
    ~TopKeys();

    // Key type used in Shard. We store hashes of the key for faster lookup.
    // Public for use in shard visitor functions.
    struct KeyId {
        size_t hash;
        std::string key;
    };

    // Pair of the key's string and the statistics related to it.
    typedef std::pair<KeyId, topkey_item_t> topkey_t;
    typedef std::pair<std::string, topkey_item_t> topkey_stat_t;

    void updateKey(const void* key, size_t nkey, rel_time_t operation_time);

    ENGINE_ERROR_CODE stats(const void* cookie,
                            rel_time_t current_time,
                            const AddStatFn& add_stat);

    /**
     * Passing a set of topkeys, and relevant context data will
     * return a json object containing an array of topkeys:
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
    ENGINE_ERROR_CODE json_stats(nlohmann::json& object,
                                 rel_time_t current_time);

protected:
    void doUpdateKey(const void* key, size_t nkey, rel_time_t operation_time);

    void doStatsInner(const tk_context& stat_context);
    ENGINE_ERROR_CODE doStats(const void* cookie,
                              rel_time_t current_time,
                              const AddStatFn& add_stat);

    ENGINE_ERROR_CODE do_json_stats(nlohmann::json& object,
                                    rel_time_t current_time);
private:
    /**
     * Topkeys previously worked by storing 8 shards with variable size. As we
     * now shard per core and not by key, topkeys may exist in multiple shards.
     * We need to multiply our requested shard size by 8 to ensure that we
     * have the same topkeys capacity.
     */
    const size_t legacy_multiplier = 8;

    /**
     * The number of topkeys that we return via stats
     */
    const size_t keys_to_return;

    class Shard;

    Shard& getShard();

    // One of N Shards which the keyspace has been broken
    // into.
    // Responsible for tracking the top {mkeys} within it's keyspace.
    class Shard {
    public:
        void setMaxKeys(size_t mkeys) {
            max_keys = mkeys;
            storage.reserve(max_keys);
            // reallocating storage invalidates the LRU list.
            list.clear();
        }

        // Updates the topkey 'ranking' for the specified key.
        // If the item does not exist it will be created (with it's creation
        // time set to operation_time), otherwise the existing item will be
        // updated.
        // On success returns true, If insufficient memory to create a
        // new item, returns false.
        bool updateKey(const cb::const_char_buffer& key,
                       size_t key_hash,
                       rel_time_t operation_time);

        typedef void (*iterfunc_t)(const topkey_t& it, void* arg);

        /* For each key in this shard, invoke the given callback function.
         */
        void accept_visitor(iterfunc_t visitor_func, void* visitor_ctx);

    private:
        // An ordered list of topkey_t*, used for LRU.
        typedef std::list<topkey_t*> key_history_t;

        // Vector topkey_t, used for actual topkey storage.
        typedef std::vector<topkey_t> key_storage_t;

        // Searches for the given key. If found returns a pointer to the
        // topkey_t, else returns NULL.
        topkey_t* searchForKey(size_t hash, const cb::const_char_buffer& key);

        // Maximum numbers of keys to be tracked per shard.
        size_t max_keys;

        // list of keys, ordered from most-recently used (front) to least
        // recently used (back).
        key_history_t list;

        // Underlying topkey storage.
        key_storage_t storage;

        // Mutex to serialize access to this shard.
        std::mutex mutex;
    };

    // Array of topkey shards. We have one shard per core so we need to
    // cache line pad the shards to prevent any contention.
    std::vector<folly::CachelinePadded<Shard>> shards;
};
