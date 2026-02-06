/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "top_keys.h"

#include "bucket_manager.h"
#include "nobucket_taskable.h"
#include "one_shot_task.h"
#include "top_keys_controller.h"

#include <executor/executorpool.h>
#include <memcached/dockey_view.h>
#include <memcached/limits.h>
#include <memcached/storeddockey.h>
#include <nlohmann/json.hpp>

namespace cb::trace::topkeys {
void to_json(nlohmann::json& json, const KeyInfo& info) {
    json = {{info.key, info.count}};
}

void to_json(nlohmann::json& json,
             const std::unordered_map<std::string, std::vector<KeyInfo>>& map) {
    json = nlohmann::json::object();
    for (const auto& [bucketname, value] : map) {
        nlohmann::json bucket = nlohmann::json::object();
        for (const auto& [key, count] : value) {
            if (!key.starts_with("cid:")) {
                continue;
            }
            auto idx = key.find(':', 4);
            if (idx == std::string::npos) {
                continue;
            }

            auto collection = key.substr(0, idx);
            auto keyname = key.substr(idx + 1);
            if (!bucket.contains(collection)) {
                bucket[collection] = nlohmann::json::object();
            }
            bucket[collection][keyname] = count;
        }

        json[bucketname] = std::move(bucket);
    }
}

void to_json(nlohmann::json& json, const Result& result) {
    json = {{"num_keys_omitted", result.num_keys_omitted},
            {"shards", result.shards},
            {"num_keys_collected", result.num_keys_collected},
            {"keys", result.keys}};
    if (result.topkey.second.count > 0) {
        auto& key = result.topkey.second.key;
        auto& count = result.topkey.second.count;
        if (!key.starts_with("cid:")) {
            return;
        }
        const auto idx = key.find(':', 4);
        if (idx == std::string::npos) {
            return;
        }
        auto collection = key.substr(0, idx);
        auto keyname = key.substr(idx + 1);

        json["topkey"] = {{"bucket", result.topkey.first},
                          {"collection", collection},
                          {"key", keyname},
                          {"count", count}};
    }
}

Collector::~Collector() {
    if (expiry_remover) {
        expiry_remover->cancel();
    }
}

class CountingCollector : public Collector {
public:
    CountingCollector(std::size_t max,
                      std::size_t shards,
                      std::chrono::seconds expiry_time,
                      bool install_cleanup_task)
        : Collector(expiry_time, install_cleanup_task),
          limit(max),
          shardmaps(shards) {
    }

    void access(const size_t bucket,
                const bool key_contains_collection,
                const std::string_view key) override {
        if (key_contains_collection) {
            do_access(bucket, DocKeyView{key, DocKeyEncodesCollectionId::Yes});
        } else {
            StoredDocKey dockey(DocKeyView{key, DocKeyEncodesCollectionId::No});
            do_access(bucket, dockey);
        }
    }

    Result getResults(size_t limit) const override;

protected:
    void do_access(size_t bucket, const DocKeyView& key);

    const std::size_t limit;
    std::atomic<size_t> num_keys_collected;
    std::atomic_size_t num_keys_omitted = 0;

    /// Each shard contains a map of key to count. The key is constructed
    /// as "<bucket><key>" to avoid collisions between buckets. To save
    /// space we use a single byte to store the bucket index (The current limit
    /// the number of buckets is currently less than 255 so this code needs
    /// to be updated if that changes).
    using LockedMap =
            folly::Synchronized<std::unordered_map<std::string, uint32_t>,
                                std::mutex>;
    std::vector<LockedMap> shardmaps;
};

Result CountingCollector::getResults(size_t collect_limit) const {
    Result result;
    result.num_keys_collected = num_keys_collected.load();
    result.num_keys_omitted = num_keys_omitted.load();
    result.shards = shardmaps.size();
    std::vector<KeyInfo> keys;
    keys.reserve(collect_limit);
    for (const auto& shard : shardmaps) {
        auto locked = shard.lock();
        for (const auto& [key, count] : *locked) {
            if (keys.empty()) {
                keys.push_back({key, count});
            } else if (keys.size() < collect_limit ||
                       count > keys.back().count) {
                // Insert in sorted order
                auto iter = std::upper_bound(
                        keys.begin(),
                        keys.end(),
                        count,
                        [](std::size_t lhs, const KeyInfo& rhs) {
                            return lhs > rhs.count;
                        });
                keys.insert(iter, {key, count});
            }
        }
    }

    if (keys.empty()) {
        return result;
    }
    // Convert the keys back to the bucket names
    std::unordered_map<std::size_t, std::string> bucketnames;
    for (std::size_t idx = 0; idx < cb::limits::TotalBuckets; ++idx) {
        auto name = BucketManager::instance().getName(idx);
        if (name.empty()) {
            bucketnames.insert({idx, fmt::format("bid:{}", idx)});
        } else {
            bucketnames.insert({idx, std::move(name)});
        }
    }
    for (auto& [key, count] : keys) {
        std::string_view view = key;
        std::size_t bucket = static_cast<uint8_t>(view.front());
        view.remove_prefix(1);
        DocKeyView dockey{view, DocKeyEncodesCollectionId::Yes};
        result.keys[bucketnames[bucket]].push_back({dockey.to_string(), count});
        if (count > result.topkey.second.count) {
            result.topkey = {bucketnames[bucket], {dockey.to_string(), count}};
        }
    }

    return result;
}

void CountingCollector::do_access(size_t bucket, const DocKeyView& key) {
    auto [id, k] = key.getIdAndKey();
    if (k.empty()) {
        return;
    }
    auto buffer = key.getBuffer();

    static_assert(cb::limits::TotalBuckets < 255,
                  "We use a single byte to store the bucket id");

    // Create a full key where we use the bucket index as the first character
    // in the key to avoid collisions between buckets. This also allows us to
    // easily extract the bucket when we need to generate the results.
    // (and it makes it easier to report back the top keys across buckets
    // without having to search multiple maps (if we had one per bucket))
    std::string full_key;
    full_key.reserve(key.size() + 1);
    full_key.push_back(static_cast<char>(bucket));
    std::ranges::copy(buffer, std::back_inserter(full_key));

    const auto hash = std::hash<std::string>{}(full_key);
    auto locked = shardmaps[hash % shardmaps.size()].lock();
    auto& keys = *locked;
    auto iter = keys.find(full_key);
    if (iter != keys.end()) {
        ++iter->second;
        return;
    }

    // Only insert if we haven't yet hit the max limit. We'll not try to
    // syncrhonize this as it's not important to be exact and we may live
    // happily ever after if we add a few more keys than the limit (1 per
    // front end thread in worst case scenario)
    if (num_keys_collected < limit) {
        ++num_keys_collected;
        keys[full_key] = 1;
    } else {
        ++num_keys_omitted;
    }
}

class FilteredCountingCollector : public CountingCollector {
public:
    FilteredCountingCollector(std::size_t max,
                              std::size_t shards,
                              std::chrono::seconds expiry_time,
                              std::vector<std::size_t> buckets,
                              bool install_cleanup_task)
        : CountingCollector(max, shards, expiry_time, install_cleanup_task),
          bucketfilter(std::move(buckets)) {
    }

    void access(const size_t bucket,
                const bool key_contains_collection,
                const std::string_view key) override {
        if (std::ranges::find(bucketfilter, bucket) != bucketfilter.end()) {
            CountingCollector::access(bucket, key_contains_collection, key);
        }
    }

protected:
    std::vector<std::size_t> bucketfilter;
};

std::shared_ptr<Collector> Collector::create(std::size_t num_keys,
                                             std::size_t shards,
                                             std::chrono::seconds expiry_time,
                                             std::vector<std::size_t> buckets,
                                             bool install_cleanup_task) {
    if (num_keys) {
        if (buckets.empty()) {
            return std::make_unique<CountingCollector>(
                    num_keys, shards, expiry_time, install_cleanup_task);
        }
        return std::make_unique<FilteredCountingCollector>(
                num_keys,
                shards,
                expiry_time,
                std::move(buckets),
                install_cleanup_task);
    }
    return {};
}

Collector::Collector(std::chrono::seconds exp, bool install_cleanup_task)
    : expiry_time(cb::time::steady_clock::now() + exp) {
    if (install_cleanup_task) {
        // Install the cleanup handler to run 1 second *after* it should
        // expire as that would allow the front end threads to potentially
        // disconnect from the trace *before* we remove it (which means
        // we don't need to inject a task in each front end thread)
        // and can release the memory immediately in this thread context
        expiry_remover = std::make_shared<OneShotTask>(
                TaskId::Core_ExpiredTopKeysRemover,
                fmt::format("Expired TopKeys remover: {}", ::to_string(uuid)),
                [collector_uuid = uuid]() {
                    Controller::instance().onExpiry(collector_uuid);
                },
                std::chrono::milliseconds(20),
                exp + std::chrono::seconds(1));

        ExecutorPool::get()->schedule(expiry_remover);
    }
}

} // namespace cb::trace::topkeys
