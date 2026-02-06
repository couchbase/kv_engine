/*
 *    Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "memcached/storeddockey.h"
#include "top_keys.h"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

using namespace cb::trace::topkeys;

TEST(TopKeyCollector, to_json) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100000;
    auto collector = Collector::create(num_keys, num_shards);
    collector->access(0, false, "mykey");
    nlohmann::json json = collector->getResults(10);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(num_shards, json.value("shards", -1)) << "json: " << json.dump(2);
    EXPECT_EQ(1, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_TRUE(buckets.contains("bid:0")) << "json: " << json.dump(2);
    auto& bucket = buckets["bid:0"];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
    ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
    auto& keys = bucket["cid:0x0"];
    ASSERT_EQ(1, keys.size()) << "json: " << keys.dump(2);
    ASSERT_TRUE(keys.contains("mykey")) << "json: " << keys.dump(2);
    ASSERT_EQ(1, keys["mykey"].get<int>()) << "json: " << keys.dump(2);
}

/// Test we count access to X keys accross multiple shards correctly
/// and drop key counts once we hit the limit
TEST(TopKeyCollector, TestAccessNoFilter) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 10;
    auto collector = Collector::create(num_keys, num_shards);
    for (int ii = 0; ii < 10; ++ii) {
        collector->access(ii, false, fmt::format("key-{}", ii));
    }
    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(num_shards, json.value("shards", -1)) << "json: " << json.dump(2);
    EXPECT_EQ(num_keys, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    for (int ii = 0; ii < 10; ++ii) {
        auto bucketname = fmt::format("bid:{}", ii);
        ASSERT_TRUE(buckets.contains(bucketname));
        auto& bucket = buckets[bucketname];
        ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
        ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
        ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
        auto& keys = bucket["cid:0x0"];
        ASSERT_EQ(1, keys.size()) << "json: " << keys.dump(2);
        EXPECT_EQ(1, keys.value(fmt::format("key-{}", ii), -1))
                << "json: " << keys.dump(2);
    }
}

/// Test we ignore keys for buckets not in the filter
TEST(TopKeyCollector, testFiltered) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 10;
    auto collector = Collector::create(
            num_keys, num_shards, std::chrono::minutes(1), {{0, 1}});
    for (int ii = 0; ii < 10; ++ii) {
        collector->access(ii, false, fmt::format("key-{}", ii));
    }
    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(num_shards, json.value("shards", -1)) << "json: " << json.dump(2);
    EXPECT_EQ(2, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);
    auto& buckets = json["keys"];
    ASSERT_EQ(2, buckets.size());

    for (int ii = 0; ii < 1; ++ii) {
        auto bucketname = fmt::format("bid:{}", ii);
        ASSERT_TRUE(buckets.contains(bucketname));
        auto& bucket = buckets[bucketname];
        ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
        ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
        ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
        auto& keys = bucket["cid:0x0"];
        ASSERT_EQ(1, keys.size()) << "json: " << keys.dump(2);
        EXPECT_EQ(1, keys.value(fmt::format("key-{}", ii), -1))
                << "json: " << keys.dump(2);
    }
}

/// Test that once we hit the key limit we start omitting keys
TEST(TopKeyCollector, TestAccessnum_keys_omitted) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 1;
    auto collector = Collector::create(num_keys, num_shards);
    for (int ii = 0; ii < 100; ++ii) {
        collector->access(0, false, fmt::format("key-{}", ii));
    }
    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(99, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(num_shards, json.value("shards", -1)) << "json: " << json.dump(2);
    EXPECT_EQ(num_keys, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    const std::string bucketname = "bid:0";
    ASSERT_TRUE(buckets.contains(bucketname));
    auto& bucket = buckets[bucketname];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
    ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
    auto& keys = bucket["cid:0x0"];
    ASSERT_EQ(1, keys.size()) << "json: " << keys.dump(2);
}

TEST(TopKeyCollector, TestSingleShard) {
    constexpr std::size_t num_shards = 1;
    constexpr std::size_t num_keys = 1000;
    auto collector = Collector::create(num_keys, num_shards);
    for (std::size_t ii = 0; ii < num_keys; ++ii) {
        collector->access(0, false, fmt::format("key-{}", ii));
    }
    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(num_shards, json.value("shards", -1)) << "json: " << json.dump(2);
    EXPECT_EQ(num_keys, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_TRUE(buckets.contains("bid:0")) << "json: " << json.dump(2);
    auto& bucket = buckets["bid:0"];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
    ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
    auto& keys = bucket["cid:0x0"];
    ASSERT_EQ(100, keys.size()) << "json: " << keys.dump(2);
}

TEST(TopKeyCollector, testExpiration) {
    cb::time::steady_clock::use_chrono = false;
    auto collector = Collector::create(1, 1);
    EXPECT_FALSE(collector->is_expired(cb::time::steady_clock::now()));
    cb::time::steady_clock::advance(std::chrono::minutes(2));
    EXPECT_TRUE(collector->is_expired(cb::time::steady_clock::now()));
    cb::time::steady_clock::use_chrono = true;
}
