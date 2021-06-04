/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine.h>
#include <nlohmann/json_fwd.hpp>
#include <array>

#include <folly/lang/Aligned.h>
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
    explicit topkey_item_t(rel_time_t create_time)
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
    using topkey_t = std::pair<KeyId, topkey_item_t>;
    using topkey_stat_t = std::pair<std::string, topkey_item_t>;

    void updateKey(const void* key, size_t nkey, rel_time_t operation_time);

    cb::engine_errc stats(const CookieIface* cookie,
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
    cb::engine_errc json_stats(nlohmann::json& object, rel_time_t current_time);

protected:
    void doUpdateKey(const void* key, size_t nkey, rel_time_t operation_time);

    void doStatsInner(const tk_context& stat_context);
    cb::engine_errc doStats(const CookieIface* cookie,
                            rel_time_t current_time,
                            const AddStatFn& add_stat);

    cb::engine_errc do_json_stats(nlohmann::json& object,
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
        bool updateKey(std::string_view key,
                       size_t key_hash,
                       rel_time_t operation_time);

        using iterfunc_t = void (*)(const topkey_t&, void*);

        /* For each key in this shard, invoke the given callback function.
         */
        void accept_visitor(iterfunc_t visitor_func, void* visitor_ctx);

    private:
        // An ordered list of topkey_t*, used for LRU.
        using key_history_t = std::list<topkey_t*>;

        // Vector topkey_t, used for actual topkey storage.
        using key_storage_t = std::vector<topkey_t>;

        // Searches for the given key. If found returns a pointer to the
        // topkey_t, else returns NULL.
        topkey_t* searchForKey(size_t hash, std::string_view key);

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
    std::vector<folly::cacheline_aligned<Shard>> shards;
};
