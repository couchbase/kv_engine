/*
 *    Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"
#include "memcached/rbac/privileges.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

class ThrottlingTests : public cb::test::ClusterTest {
public:
    void SetUp() override {
        cb::test::ClusterTest::SetUp();

        // Create 5 additional buckets named bucket1, bucket2, etc.
        for (int i = 0; i < 5; ++i) {
            std::string bucketName = "bucket" + std::to_string(i);

            std::string rbac = R"({
"buckets": {
  "bucket@": {
    "privileges": [
      "Read",
      "SimpleStats",
      "Insert",
      "Delete",
      "Upsert",
      "DcpProducer",
      "DcpStream",
      "RangeScan"
    ]
  }
},
"privileges": [],
"domain": "external"
})";
            size_t pos = rbac.find('@');
            if (pos != std::string::npos) {
                rbac.replace(pos, 1, std::to_string(i));
            }
            cluster->getAuthProviderService().upsertUser(
                    {bucketName, bucketName, nlohmann::json::parse(rbac)});

            auto bucket =
                    cluster->createBucket(bucketName, {{"max_vbuckets", 8}});
            if (!bucket) {
                throw std::runtime_error("Failed to create bucket: " +
                                         bucketName);
            }

            cluster->collections = {};
            cluster->collections.add(CollectionEntry::vegetable);
            bucket->setCollectionManifest(cluster->collections.getJson());

            // Set throttle limits to 1000 units
            bucket->setThrottleLimits(1000, 1000);
        }
    }

    void TearDown() override {
        for (int i = 0; i < 5; ++i) {
            std::string bucketName = "bucket" + std::to_string(i);
            if (cluster->getBucket(bucketName)) {
                cluster->deleteBucket(bucketName);
            }
        }

        cb::test::ClusterTest::TearDown();
    }

    std::unique_ptr<MemcachedConnection> getConnection(
            const std::string& bucketName) {
        auto bucket = cluster->getBucket(bucketName);
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate(bucketName, bucketName);
        conn->selectBucket(bucket->getName());
        conn->setFeatures({cb::mcbp::Feature::SELECT_BUCKET,
                           cb::mcbp::Feature::JSON,
                           cb::mcbp::Feature::SNAPPY,
                           cb::mcbp::Feature::Collections});
        return conn;
    }

    nlohmann::json getThrottlingStats(
            std::unique_ptr<MemcachedConnection>& conn,
            const std::string& bucketName) {
        nlohmann::json stats;
        conn->authenticate("@admin", "password");
        conn->stats(
                [&stats](const auto& k, const auto& v) {
                    stats = nlohmann::json::parse(v);
                },
                std::string{"bucket_details "} + bucketName);
        return stats;
    }

    static void mutate(MemcachedConnection& conn,
                       std::string id,
                       MutationType type) {
        Document doc{};
        doc.value = R"({"json":true})";
        doc.info.id = std::move(id);
        doc.info.datatype = cb::mcbp::Datatype::JSON;
        const auto info = conn.mutate(doc, Vbid{0}, type);
        EXPECT_NE(0, info.cas);
    }
};

TEST_F(ThrottlingTests, OpsAreThrottled) {
    auto opsAreThrottled = [this](const std::string& bucketName) {
        auto conn = getConnection(bucketName);

        auto key = DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                                     "Throttled");
        Document doc{};
        doc.info.id = key;
        doc.value = "Throttled Document";

        conn->mutate(doc, Vbid{0}, MutationType::Set);

        // Run 4k mutations - roughly 4 seconds with throttling
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 4096; ++i) {
            conn->get(key, Vbid{0});
        }
        auto end = std::chrono::steady_clock::now();
        EXPECT_LT(
                std::chrono::seconds{2},
                std::chrono::duration_cast<std::chrono::seconds>(end - start));

        // Check that at least some ops are throttled
        auto stats = getThrottlingStats(conn, bucketName);
        ASSERT_FALSE(stats.empty());
        ASSERT_EQ(4096, stats["throttle_ru_total"]); // 4096 reads done
        ASSERT_EQ(1, stats["throttle_wu_total"]); // 1 write done
        ASSERT_LE(3, stats["num_throttled"]);
        ASSERT_NE(0, stats["throttle_wait_time"]);
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back(
                [opsAreThrottled, name = "bucket" + std::to_string(i)]() {
                    // Set very low throttle limits to ensure that ops are
                    // throttled
                    auto bucket = cluster->getBucket(name);
                    bucket->setThrottleLimits(1000, 1000);

                    opsAreThrottled(name);
                });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST_F(ThrottlingTests, OpsAreNotThrottled) {
    auto opsAreNotThrottled = [this](const std::string& bucketName) {
        auto conn = getConnection(bucketName);

        auto key = DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                                     "NotThrottled");
        Document doc{};
        doc.info.id = key;
        doc.value = "Document";

        conn->mutate(doc, Vbid{0}, MutationType::Set);

        // Run 4k mutations
        for (int i = 0; i < 4096; ++i) {
            conn->get(key, Vbid{0});
        }
        auto stats = getThrottlingStats(conn, bucketName);
        ASSERT_FALSE(stats.empty());
        ASSERT_EQ(4096, stats["throttle_ru_total"]); // 4096 reads done
        ASSERT_EQ(1, stats["throttle_wu_total"]); // 1 write done
        ASSERT_EQ(0, stats["num_throttled"]);
        ASSERT_EQ(0, stats["throttle_wait_time"]);
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back(
                [opsAreNotThrottled, name = "bucket" + std::to_string(i)]() {
                    // Set high throttle limits to ensure that ops are not
                    // throttled
                    auto bucket = cluster->getBucket(name);
                    bucket->setThrottleLimits(5000, 5000);

                    opsAreNotThrottled(name);
                });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}