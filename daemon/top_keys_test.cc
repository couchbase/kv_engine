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

// FilteredCountingCollector tests

/// Test collection filtering with keys containing collection IDs
TEST(FilteredCountingCollector, CollectionFilteringWithCollectionIds) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100;
    // Filter for collection ID 8 and 9 (user collections > 7)
    std::vector<std::size_t> bucket_ids = {
            0}; // Need to specify the bucket filter to be able to filter on
                // collections
    std::vector<CollectionIDType> collection_filter = {8, 9};

    auto collector = Collector::create(num_keys,
                                       num_shards,
                                       std::chrono::minutes(1),
                                       bucket_ids,
                                       collection_filter);

    // Access keys in matching collections (should be counted)
    StoredDocKey key1("key1", CollectionID(8));
    StoredDocKey key2("key2", CollectionID(9));
    collector->access(0, true, std::string(key1));
    collector->access(0, true, std::string(key2));

    // Access keys in non-matching collections (should be ignored)
    StoredDocKey key3("key3", CollectionID(10));
    StoredDocKey key4("key4", CollectionID(11));
    collector->access(0, true, std::string(key3));
    collector->access(0, true, std::string(key4));

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(2, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    auto& bucket = buckets["bid:0"];
    // Keys should be in collections 8 and 9
    ASSERT_TRUE(bucket.contains("cid:0x8"));
    ASSERT_TRUE(bucket.contains("cid:0x9"));
}

/// Test collection filtering with default collection (keys not containing
/// collection IDs)
TEST(FilteredCountingCollector, CollectionFilteringDefaultCollection) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100;
    std::vector<std::size_t> bucket_ids = {
            0}; // Need to specify the bucket filter to be able to filter on
                // collections
    std::vector<CollectionIDType> collection_filter = {
            CollectionIDType(CollectionID::Default)};

    auto collector = Collector::create(num_keys,
                                       num_shards,
                                       std::chrono::minutes(1),
                                       bucket_ids,
                                       collection_filter);

    // Access keys without collection encoding (default collection,
    // key_contains_collection=false) Should be counted since Default collection
    // is in the filter
    collector->access(0, false, "key1");
    collector->access(0, false, "key2");

    // Access keys with non-default collection encoding (should be ignored)
    StoredDocKey key3("key3", CollectionID(8));
    collector->access(0, true, std::string(key3));

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(2, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    auto& bucket = buckets["bid:0"];
    // Keys should be in default collection
    ASSERT_TRUE(bucket.contains("cid:0x0"));
}

/// Test collection filtering where default collection is NOT in the filter
TEST(FilteredCountingCollector, CollectionFilteringExcludesDefaultCollection) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100;
    std::vector<std::size_t> bucket_ids = {
            0}; // Need to specify the bucket filter to be able to filter on
                // collections
    std::vector<CollectionIDType> collection_filter = {8, 9};

    auto collector = Collector::create(num_keys,
                                       num_shards,
                                       std::chrono::minutes(1),
                                       bucket_ids,
                                       collection_filter);

    // Access keys without collection encoding (default collection)
    // Should be ignored since Default collection is NOT in the filter
    collector->access(0, false, "key1");
    collector->access(0, false, "key2");

    // Access keys in matching user collections (should be counted)
    StoredDocKey key3("key3", CollectionID(8));
    StoredDocKey key4("key4", CollectionID(9));
    collector->access(0, true, std::string(key3));
    collector->access(0, true, std::string(key4));

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(2, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    auto& bucket = buckets["bid:0"];
    // Should only have collections 8 and 9, not default
    ASSERT_FALSE(bucket.contains("cid:0x0"));
    ASSERT_TRUE(bucket.contains("cid:0x8"));
    ASSERT_TRUE(bucket.contains("cid:0x9"));
}

/// Test combined bucket and collection filtering
TEST(FilteredCountingCollector, CombinedBucketAndCollectionFiltering) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100;
    std::vector<std::size_t> bucket_filter = {0, 1};
    std::vector<CollectionIDType> collection_filter = {8};

    auto collector = Collector::create(num_keys,
                                       num_shards,
                                       std::chrono::minutes(1),
                                       bucket_filter,
                                       collection_filter);

    // Access keys in matching bucket (0) and collection (8) - should count
    StoredDocKey key1("key1", CollectionID(8));
    collector->access(0, true, std::string(key1));

    // Access keys in matching bucket (1) and collection (8) - should count
    StoredDocKey key2("key2", CollectionID(8));
    collector->access(1, true, std::string(key2));

    // Access keys in matching bucket (0) but wrong collection (9) - ignore
    StoredDocKey key3("key3", CollectionID(9));
    collector->access(0, true, std::string(key3));

    // Access keys in correct collection (8) but wrong bucket (2) - ignore
    StoredDocKey key4("key4", CollectionID(8));
    collector->access(2, true, std::string(key4));

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(2, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    ASSERT_EQ(2, buckets.size());
    // Should have keys from bucket 0 and 1, but not bucket 2
    ASSERT_TRUE(buckets.contains("bid:0"));
    ASSERT_TRUE(buckets.contains("bid:1"));
    ASSERT_FALSE(buckets.contains("bid:2"));
}

/// Test multiple accesses to same key in filtered collector
TEST(FilteredCountingCollector, CountingMultipleAccesses) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 100;
    std::vector<std::size_t> bucket_ids = {
            0}; // Need to specify the bucket filter to be able to filter on
                // collections
    std::vector<CollectionIDType> collection_filter = {8};

    auto collector = Collector::create(num_keys,
                                       num_shards,
                                       std::chrono::minutes(1),
                                       bucket_ids,
                                       collection_filter);

    StoredDocKey key1("key1", CollectionID(8));

    // Access same key multiple times
    for (int ii = 0; ii < 5; ++ii) {
        collector->access(0, true, std::string(key1));
    }

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(0, json.value("num_keys_omitted", -1))
            << "json: " << json.dump(2);
    EXPECT_EQ(1, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);

    auto& buckets = json["keys"];
    auto& bucket = buckets["bid:0"];
    auto& keys = bucket["cid:0x8"];
    EXPECT_EQ(5, keys.value("key1", -1)) << "json: " << json.dump(2);
}

/// Test FilteredCountingCollector with empty filters acts like normal
/// CountingCollector
TEST(FilteredCountingCollector, EmptyFilters) {
    constexpr std::size_t num_shards = 4;
    constexpr std::size_t num_keys = 10;

    // Create collector with empty bucket list (which actually means no
    // filtering) Note: Looking at Collector::create logic, if buckets is empty
    // it creates CountingCollector, not FilteredCountingCollector. So this test
    // validates that behavior.
    auto collector = Collector::create(num_keys, num_shards);

    for (int ii = 0; ii < 10; ++ii) {
        collector->access(ii, false, fmt::format("key-{}", ii));
    }

    nlohmann::json json = collector->getResults(100);
    EXPECT_EQ(num_keys, json.value("num_keys_collected", -1))
            << "json: " << json.dump(2);
}
