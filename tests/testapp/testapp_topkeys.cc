/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "memcached/storeddockey.h"
#include "memcached/storeddockey_fwd.h"
#include "testapp.h"
#include "testapp_client_test.h"
#include "utilities/test_manifest.h"

#include <fmt/format.h>
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>
#include <platform/uuid.h>

/// Test fixture for the TopKeys feature. Given that we don't want to
/// create a large database we'll just test the tracing functionality
/// by trying to fetch non-existing keys (ideally we should have tried
/// all commands, but that's just too much work for now).
class Topkeys : public TestappClientTest {
public:
    static void SetUpTestCase() {
        TestappTest::SetUpTestCase();
        createUserConnection = true;
    }

    cb::uuid::uuid_t enableTracing(const std::string_view bucket_filter = {}) {
        auto rsp = adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::IoctlSet,
                fmt::format("topkeys.start?limit=10&shards=1{}",
                            bucket_filter.empty()
                                    ? ""
                                    : fmt::format("&bucket_filter={}",
                                                  bucket_filter))});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    fmt::format("Failed to start topkeys tracing: {} {}",
                                rsp.getStatus(),
                                rsp.getDataView()));
        }
        auto json = rsp.getDataJson();
        if (!json.contains("uuid")) {
            throw std::runtime_error(
                    fmt::format("Failed to start topkeys tracing: no uuid in "
                                "response json: {}",
                                json.dump(2)));
        }
        return cb::uuid::from_string(json["uuid"].get<std::string>());
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         Topkeys,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(Topkeys, TraceAllBucket) {
    enableTracing();
    for (int ii = 0; ii < 20; ++ii) {
        BinprotGetCommand cmd("key-" + std::to_string(ii));
        auto rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlGet,
                                  fmt::format("topkeys.stop?limit={}", 10)});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();
    auto json = rsp.getDataJson();
    // We collected 10 keys, ran 20 requests.. 10 should be omitted
    EXPECT_EQ(10, json.value("num_keys_omitted", -1));

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    ASSERT_TRUE(buckets.contains(bucketName));
    auto& bucket = buckets[bucketName];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
    ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
    auto& keys = bucket["cid:0x0"];

    // We should have 10 keys
    EXPECT_EQ(10, keys.size());
    for (auto it = keys.begin(); it != keys.end(); ++it) {
        EXPECT_TRUE(it.key().starts_with("key-")) << it.key();
    }
}

TEST_P(Topkeys, TraceBucketFilter) {
    mcd_env->getTestBucket().createBucket("bucket", {}, *adminConnection);
    enableTracing("bucket");

    // Try to access keys in the default bucket
    for (int ii = 0; ii < 20; ++ii) {
        BinprotGetCommand cmd("key-" + std::to_string(ii));
        auto rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }
    // and in the bucket named bucket (which we filter on)
    userConnection->selectBucket("bucket");
    for (int ii = 0; ii < 20; ++ii) {
        BinprotGetCommand cmd("key-" + std::to_string(ii));
        auto rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }
    userConnection->selectBucket("default");
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlGet,
                                  fmt::format("topkeys.stop?limit={}", 10)});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();
    auto json = rsp.getDataJson();
    // We collected 10 keys, ran 20 requests.. 10 should be omitted
    EXPECT_EQ(10, json.value("num_keys_omitted", -1));

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    ASSERT_TRUE(buckets.contains("bucket"));
    auto& bucket = buckets["bucket"];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);
    ASSERT_EQ(1, bucket.size()) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0x0")) << "json: " << bucket.dump(2);
    auto& keys = bucket["cid:0x0"];
    // We should have 10 keys
    EXPECT_EQ(10, keys.size());
    for (auto it = keys.begin(); it != keys.end(); ++it) {
        EXPECT_TRUE(it.key().starts_with("key-")) << it.key();
    }
}

TEST_P(Topkeys, TraceAlreadyRunning) {
    const auto uuid = enableTracing("bucket");

    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlSet,
                                  "topkeys.start?limit=10&shards=1"});

    ASSERT_FALSE(rsp.isSuccess())
            << rsp.getStatus() << " " << rsp.getDataView();
    auto json = rsp.getDataJson();
    ASSERT_TRUE(json.contains("uuid")) << "json: " << json.dump(2);
    EXPECT_EQ(uuid, cb::uuid::from_string(json["uuid"].get<std::string>()));

    // Try to stop the collector with a wrong uuid
    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlGet,
                                  fmt::format("topkeys.stop?limit={}&uuid={}",
                                              10,
                                              to_string(cb::uuid::random()))});
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus())
            << rsp.getDataView();
    json = rsp.getDataJson();
    ASSERT_TRUE(json.contains("uuid")) << "json: " << json.dump(2);
    EXPECT_EQ(uuid, cb::uuid::from_string(json["uuid"].get<std::string>()));

    // But we should be able to stop with the correct uuid
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet,
            fmt::format("topkeys.stop?limit={}&uuid={}", 10, to_string(uuid))});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();

    // And if we try to stop when no is running we should get no such key
    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlGet,
                                  fmt::format("topkeys.stop?limit={}", 10)});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << rsp.getDataView();
}

TEST_P(Topkeys, TraceCollectionFilter) {
    // Create collections manifest with fruit and vegetable collections
    CollectionsManifest manifest;
    manifest.add(CollectionEntry::fruit).add(CollectionEntry::vegetable);

    // Set the manifest
    adminConnection->executeInBucket(bucketName, [&](auto& connection) {
        auto response = connection.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::CollectionsSetManifest,
                {},
                manifest.getJson().dump()});
        if (!response.isSuccess()) {
            throw std::runtime_error(
                    fmt::format("Failed to set collections manifest: {} {}",
                                response.getStatus(),
                                response.getDataView()));
        }
    });

    // Enable collections on the connection
    userConnection->setFeature(cb::mcbp::Feature::Collections, true);

    // Start tracing with collection_filter for fruit collection only
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet,
            "topkeys.start?limit=10&shards=1&bucket_filter=default"
            "&collection_filter=9"});
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                fmt::format("Failed to start topkeys tracing: {} {}",
                            rsp.getStatus(),
                            rsp.getDataView()));
    }
    auto json = rsp.getDataJson();
    if (!json.contains("uuid")) {
        throw std::runtime_error(
                fmt::format("Failed to start topkeys tracing: no uuid in "
                            "response json: {}",
                            json.dump(2)));
    }
    cb::uuid::uuid_t uuid =
            cb::uuid::from_string(json["uuid"].get<std::string>());

    // Access keys in fruit collection
    for (int ii = 0; ii < 20; ++ii) {
        std::string key = "fruit-" + std::to_string(ii);
        StoredDocKey fruitKey(key, CollectionID(CollectionUid::fruit));
        BinprotGetCommand cmd(std::string{fruitKey});
        cmd.setVBucket(Vbid(0));
        rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    // Access keys in vegetable collection (these should not be collected)
    for (int ii = 0; ii < 20; ++ii) {
        std::string key = "vegetable-" + std::to_string(ii);
        StoredDocKey vegetableKey(key, CollectionID(CollectionUid::vegetable));
        BinprotGetCommand cmd(std::string{vegetableKey});
        cmd.setVBucket(Vbid(0));
        rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    // Stop tracing and get results
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet,
            fmt::format("topkeys.stop?limit={}&uuid={}", 10, to_string(uuid))});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();
    json = rsp.getDataJson();

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    ASSERT_TRUE(buckets.contains(bucketName));
    auto& bucket = buckets[bucketName];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);

    // Should only contain the fruit collection (cid:0x9)
    ASSERT_TRUE(bucket.contains("cid:0x9")) << "json: " << bucket.dump(2);
    ASSERT_FALSE(bucket.contains("cid:0xa")) << "json: " << bucket.dump(2);

    auto& keys = bucket["cid:0x9"];
    // We should have 10 keys from the fruit collection
    EXPECT_EQ(10, keys.size());
    for (auto it = keys.begin(); it != keys.end(); ++it) {
        EXPECT_TRUE(it.key().starts_with("fruit-")) << it.key();
    }
}

TEST_P(Topkeys, TraceMultipleCollectionFilter) {
    // Create collections manifest with fruit, vegetable, and dairy collections
    CollectionsManifest manifest;
    manifest.add(CollectionEntry::fruit)
            .add(CollectionEntry::vegetable)
            .add(CollectionEntry::customer1)
            .add(CollectionEntry::dairy);

    // Set the manifest
    adminConnection->executeInBucket(bucketName, [&](auto& connection) {
        auto response = connection.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::CollectionsSetManifest,
                {},
                manifest.getJson().dump()});
        if (!response.isSuccess()) {
            throw std::runtime_error(
                    fmt::format("Failed to set collections manifest: {} {}",
                                response.getStatus(),
                                response.getDataView()));
        }
    });

    // Enable collections on the connection
    userConnection->setFeature(cb::mcbp::Feature::Collections, true);

    // Start tracing with collection_filter for fruit and vegetable collections,
    // plus dairy (which is in the filter but we won't access)
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet,
            "topkeys.start?limit=100&shards=1&bucket_filter=default"
            "&collection_filter=9,10,12"});
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                fmt::format("Failed to start topkeys tracing: {} {}",
                            rsp.getStatus(),
                            rsp.getDataView()));
    }
    auto json = rsp.getDataJson();
    if (!json.contains("uuid")) {
        throw std::runtime_error(
                fmt::format("Failed to start topkeys tracing: no uuid in "
                            "response json: {}",
                            json.dump(2)));
    }
    cb::uuid::uuid_t uuid =
            cb::uuid::from_string(json["uuid"].get<std::string>());

    // Access keys in fruit collection (should be collected)
    for (int ii = 0; ii < 20; ++ii) {
        std::string key = "fruit-" + std::to_string(ii);
        StoredDocKey fruitKey(key, CollectionID(CollectionUid::fruit));
        BinprotGetCommand cmd(std::string{fruitKey});
        cmd.setVBucket(Vbid(0));
        rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    // Access keys in vegetable collection (should be collected)
    for (int ii = 0; ii < 20; ++ii) {
        std::string key = "vegetable-" + std::to_string(ii);
        StoredDocKey vegetableKey(key, CollectionID(CollectionUid::vegetable));
        BinprotGetCommand cmd(std::string{vegetableKey});
        cmd.setVBucket(Vbid(0));
        rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    // Don't access any keys in dairy collection (it's in filter but not used),
    // access keys in customer1 collection (should ignored as the filter
    // doesn't include them)
    for (int ii = 0; ii < 20; ++ii) {
        std::string key = "customer1-" + std::to_string(ii);
        StoredDocKey customer1Key(key, CollectionID(CollectionUid::customer1));
        BinprotGetCommand cmd(std::string{customer1Key});
        cmd.setVBucket(Vbid(0));
        rsp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    // Stop tracing and get results
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet,
            fmt::format(
                    "topkeys.stop?limit={}&uuid={}", 100, to_string(uuid))});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();
    json = rsp.getDataJson();

    auto& buckets = json["keys"];
    ASSERT_EQ(1, buckets.size());
    ASSERT_TRUE(buckets.contains(bucketName));
    auto& bucket = buckets[bucketName];
    ASSERT_TRUE(bucket.is_object()) << "json: " << bucket.dump(2);

    // Should contain fruit (cid:0x9) and vegetable (cid:0xa) collections but
    // not dairy (cid:0xc) because no keys were accessed in dairy and not
    // customer1 (cid:0xb) because it's not in the filter
    ASSERT_TRUE(bucket.contains("cid:0x9")) << "json: " << bucket.dump(2);
    ASSERT_TRUE(bucket.contains("cid:0xa")) << "json: " << bucket.dump(2);
    ASSERT_FALSE(bucket.contains("cid:0xc")) << "json: " << bucket.dump(2);
    ASSERT_FALSE(bucket.contains("cid:0xb")) << "json: " << bucket.dump(2);

    // Check fruit collection keys
    auto& fruitKeys = bucket["cid:0x9"];
    EXPECT_EQ(20, fruitKeys.size());
    for (auto it = fruitKeys.begin(); it != fruitKeys.end(); ++it) {
        EXPECT_TRUE(it.key().starts_with("fruit-")) << it.key();
    }

    // Check vegetable collection keys
    auto& vegetableKeys = bucket["cid:0xa"];
    EXPECT_EQ(20, vegetableKeys.size());
    for (auto it = vegetableKeys.begin(); it != vegetableKeys.end(); ++it) {
        EXPECT_TRUE(it.key().starts_with("vegetable-")) << it.key();
    }
}
