/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "topkeys.h"
#include "settings.h"

#include <platform/sysinfo.h>

#include <folly/concurrency/CacheLocality.h>
#include <inttypes.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <gsl/gsl>
#include <set>
#include <stdexcept>
#include <utility>

/*
 * Implementation Details
 *
 * === TopKeys ===
 *
 * The TopKeys class is split into shards for performance reasons. We create one
 * shard per (logical) core to:
 *
 * a) prevent any cache contention
 * b) allow as much concurrent access as possible (each shard is guarded by a
 *    mutex)
 *
 * Topkeys passes on requests to the correct Shard (determined by the core id of
 * the calling thread), and when statistics are requested it aggregates
 * information from each shard. When aggregating information, the TopKeys class
 * has to remove duplicates from the possible pool of top keys because we shard
 * per core for performance. Previously, TopKeys would create 8 shards
 * (regardless of machine size), each with storage for a configurably amount of
 * keys and shard by key hash (which meant that we would not have duplicate keys
 * across shards). Now that we shard per core instead of by key hash, to keep
 * the size of the stat output the same we need a minimum of 8 X N keys per
 * shard (because each shard could be an exact duplicate of the others).
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
TopKeys::TopKeys(int mkeys)
    : keys_to_return(mkeys * legacy_multiplier), shards(cb::get_cpu_count()) {
    for (auto& shard : shards) {
        shard->setMaxKeys(keys_to_return);
    }
}

TopKeys::~TopKeys() {
}

void TopKeys::updateKey(const void* key,
                        size_t nkey,
                        rel_time_t operation_time) {
    if (Settings::instance().isTopkeysEnabled()) {
        doUpdateKey(key, nkey, operation_time);
    }
}

cb::engine_errc TopKeys::stats(const void* cookie,
                               rel_time_t current_time,
                               const AddStatFn& add_stat) {
    if (Settings::instance().isTopkeysEnabled()) {
        return doStats(cookie, current_time, add_stat);
    }

    return cb::engine_errc::success;
}

cb::engine_errc TopKeys::json_stats(nlohmann::json& object,
                                    rel_time_t current_time) {
    if (Settings::instance().isTopkeysEnabled()) {
        return do_json_stats(object, current_time);
    }

    return cb::engine_errc::success;
}

TopKeys::Shard& TopKeys::getShard() {
    auto stripe =
            folly::AccessSpreader<std::atomic>::cachedCurrent(shards.size());
    return *shards[stripe];
}

TopKeys::topkey_t* TopKeys::Shard::searchForKey(size_t key_hash,
                                                std::string_view key) {
    for (auto& topkey : storage) {
        if (topkey.first.hash == key_hash) {
            // Double-check with full compare
            if (topkey.first.key == key) {
                // Match found.
                return &topkey;
            }
        }
    }
    return nullptr;
}

bool TopKeys::Shard::updateKey(std::string_view key,
                               size_t key_hash,
                               const rel_time_t ct) {
    try {
        std::lock_guard<std::mutex> lock(mutex);

        topkey_t* found_key = searchForKey(key_hash, key);

        if (!found_key) {
            // Key not found.
            if (storage.size() == max_keys) {
                // Re-use the lowest keys' storage.
                found_key = list.back();
                found_key->first.hash = key_hash;
                found_key->first.key = key;
                found_key->second = topkey_item_t(ct);

                // Move back element to the front, shuffling down the
                // rest.
                list.splice(list.begin(), list, --list.end());

            } else {
                // add a new element to the storage array.

                storage.emplace_back(std::make_pair(
                        KeyId{key_hash, std::string(key)}, topkey_item_t(ct)));
                found_key = &storage.back();

                // Insert the new item to the front of the list
                list.push_front(found_key);
            }
        } else {
            // Found - shuffle to the front.
            auto it = std::find(list.begin(), list.end(), found_key);
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

void TopKeys::doUpdateKey(const void* key,
                          size_t nkey,
                          rel_time_t operation_time) {
    if (key == nullptr || nkey == 0) {
        throw std::invalid_argument(
                "TopKeys::doUpdateKey: key must be specified");
    }

    try {
        // We store a key hash to make lookup of topkeys faster and because the
        // memory footprint is relatively small.
        std::string_view key_buf(static_cast<const char*>(key), nkey);
        std::hash<std::string_view> hash_fn;
        const size_t key_hash = hash_fn(key_buf);

        getShard().updateKey(key_buf, key_hash, operation_time);
    } catch (const std::bad_alloc&) {
        // Failed to increment topkeys, continue...
    }
}

struct tk_context {
    using CallbackFn = void (*)(const std::string&,
                                const topkey_item_t&,
                                void*);
    tk_context(const void* c,
               AddStatFn a,
               rel_time_t t,
               nlohmann::json* arr,
               CallbackFn callbackFn)
        : cookie(c),
          add_stat(std::move(a)),
          current_time(t),
          array(arr),
          callbackFunction(callbackFn) {
        // empty
    }

    const void* cookie;
    AddStatFn add_stat;
    rel_time_t current_time;
    nlohmann::json* array;
    CallbackFn callbackFunction;
};

static void tk_iterfunc(const std::string& key,
                        const topkey_item_t& it,
                        void* arg) {
    auto* c = static_cast<struct tk_context*>(arg);
    char val_str[500];
    /* Note we use accessed time for both 'atime' and 'ctime' below. They have
     * had the same value since the topkeys code was added; but given that
     * clients may expect separate values we print both.
     */
    rel_time_t created_time = c->current_time - it.ti_ctime;
    int vlen = snprintf(val_str,
                        sizeof(val_str) - 1,
                        "get_hits=%d,"
                        "get_misses=0,cmd_set=0,incr_hits=0,incr_misses=0,"
                        "decr_hits=0,decr_misses=0,delete_hits=0,"
                        "delete_misses=0,evictions=0,cas_hits=0,cas_badval=0,"
                        "cas_misses=0,get_replica=0,evict=0,getl=0,unlock=0,"
                        "get_meta=0,set_meta=0,del_meta=0,ctime=%" PRIu32
                        ",atime=%" PRIu32,
                        it.ti_access_count,
                        created_time,
                        created_time);
    if (vlen > 0 && vlen < int(sizeof(val_str) - 1)) {
        c->add_stat(key, val_str, c->cookie);
    }
}

/**
 * Passing in a list of keys, context, and json array will populate that
 * array with an object for each key in the following format:
 * {
 *    "key": "somekey",
 *    "access_count": nnn,
 *    "ctime": ccc,
 *    "atime": aaa
 * }
 */
static void tk_jsonfunc(const std::string& key,
                        const topkey_item_t& it,
                        void* arg) {
    auto* c = static_cast<struct tk_context*>(arg);
    if (c->array == nullptr) {
        throw std::invalid_argument("tk_jsonfunc: c->array can't be nullptr");
    }

    nlohmann::json obj;
    obj["key"] = key;
    obj["access_count"] = it.ti_access_count;
    obj["ctime"] = c->current_time - it.ti_ctime;

    c->array->push_back(obj);
}

static void tk_aggregate_func(const TopKeys::topkey_t& it, void* arg) {
    auto* map =
            static_cast<std::unordered_map<std::string, topkey_item_t>*>(arg);

    auto res = map->insert(std::make_pair(it.first.key, it.second));

    // If insert failed, then we have a duplicate top key. Add the stats
    if (!res.second) {
        res.first->second.ti_access_count += it.second.ti_access_count;
    }
}

cb::engine_errc TopKeys::doStats(const void* cookie,
                                 rel_time_t current_time,
                                 const AddStatFn& add_stat) {
    struct tk_context context(
            cookie, add_stat, current_time, nullptr, &tk_iterfunc);
    doStatsInner(context);

    return cb::engine_errc::success;
}

/**
 * Passing a set of topkeys, and relevant context data will
 * return a json object containing an array of topkeys (with each key
 * appearing as in the example above for tk_jsonfunc):
 * {
 *   "topkeys": [
 *      { ... }, ..., { ... }
 *    ]
 * }
 */
cb::engine_errc TopKeys::do_json_stats(nlohmann::json& object,
                                       rel_time_t current_time) {
    nlohmann::json topkeys = nlohmann::json::array();
    struct tk_context context(
            nullptr, nullptr, current_time, &topkeys, &tk_jsonfunc);
    doStatsInner(context);

    auto str = topkeys.dump();
    object["topkeys"] = topkeys;

    return cb::engine_errc::success;
}

void TopKeys::Shard::accept_visitor(iterfunc_t visitor_func,
                                    void* visitor_ctx) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto* key : list) {
        visitor_func(*key, visitor_ctx);
    }
}

void TopKeys::doStatsInner(const tk_context& stat_context) {
    // 1) Find the unique set of top keys by putting every top key in a map
    std::unordered_map<std::string, topkey_item_t> map =
            std::unordered_map<std::string, topkey_item_t>();

    for (auto& shard : shards) {
        shard->accept_visitor(tk_aggregate_func, &map);
    }

    // Easiest way to sort this by access_count is to drop the contents of the
    // map into a vector and sort that.
    auto items = std::vector<topkey_stat_t>(map.begin(), map.end());
    std::sort(items.begin(),
              items.end(),
              [](const TopKeys::topkey_stat_t& a,
                 const TopKeys::topkey_stat_t& b) {
                  // Sort by number of accesses
                  return a.second.ti_access_count > b.second.ti_access_count;
              });

    // 2) Iterate on this set making the required callback for each key.
    // Call for no more than keys_to_return times.
    size_t count = 0;
    for (const auto& t : items) {
        if (++count > keys_to_return) {
            break;
        }
        stat_context.callbackFunction(t.first, t.second, (void*)&stat_context);
    }
}
