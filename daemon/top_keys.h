/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "memcached/dockey_view.h"

#include <nlohmann/json_fwd.hpp>
#include <platform/cb_time.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace cb::trace::topkeys {

struct KeyInfo {
    std::string key;
    uint32_t count = 0;
};
void to_json(nlohmann::json& json, const KeyInfo& info);

struct Result {
    /// The number of keys monitored
    std::size_t num_keys_collected = 0;
    /// The number of shards
    std::size_t shards = 0;
    /// The number of access to key(s) omitted due to hitting the key limit
    std::size_t num_keys_omitted = 0;

    /// The single most accessed key and its count
    std::pair<std::string, KeyInfo> topkey;

    /// The list of top accessed keys per bucket and their counts
    std::unordered_map<std::string, std::vector<KeyInfo>> keys;
};

void to_json(nlohmann::json& json, const Result& result);

class Collector {
public:
    virtual ~Collector() = default;

    /**
     * Create a Collector instance used to track key access.
     *
     * @param num_keys The maximum number of keys to track in a shard
     * @param shards The number of internal maps to shard the keyspace across
     * @param expiry_time The time after which the collected data should be
     *                    considered expired and can be discarded (and the
     *                    collector can be reset)
     * @param buckets If non-empty only track keys accessed in the specified
     *                bucket. For space efficiencty we use the bucket *id*
     *                and not the bucket name (as buckets typically don't
     *                come and go all the time)
     */
    static std::shared_ptr<Collector> create(
            std::size_t num_keys,
            std::size_t shards,
            cb::time::steady_clock::time_point expiry_time =
                    cb::time::steady_clock::now() + std::chrono::minutes(1),
            std::vector<std::size_t> buckets = {});

    /// Is this collector expired or not (e.g. should we discard the collected
    /// data). The time is passed in as an argument to avoid having to fetch the
    /// clock multiple times when we want to check multiple collectors (e.g. for
    /// each front end thread).
    bool is_expired(cb::time::steady_clock::time_point now) {
        return now >= expiry_time;
    }

    /**
     * Register access for a key in a given bucket
     *
     * @param bucket The bucket index for the key
     * @param key_contains_collection True if the key contains a collection
     * @param key The key which was accessed
     */
    virtual void access(size_t bucket,
                        bool key_contains_collection,
                        std::string_view key) = 0;

    /**
     * Get the top accessed keys
     * @param limit The maximum number of keys to return
     * @throws std::bad_alloc if we run out of memory
     */
    virtual Result getResults(size_t limit) const = 0;

protected:
    Collector(cb::time::steady_clock::time_point exp) : expiry_time(exp) {};
    const cb::time::steady_clock::time_point expiry_time;
};

} // namespace cb::trace::topkeys
