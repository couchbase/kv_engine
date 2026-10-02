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
#include <mcbp/codec/range_scan_continue_codec.h>
#include <memcached/range_scan_id.h>
#include <platform/base64.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

#include <cstring>
#include <random>
#include <unordered_set>

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

        cluster->changeConfig([](nlohmann::json& config) {
            config["throttle_enabled"] = true;
        });
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

    void opsAreThrottled(const std::string& bucketName) {
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
        ASSERT_EQ(4096, stats["ru_total"]); // 4096 reads done
        ASSERT_EQ(1, stats["wu_total"]); // 1 write done
        ASSERT_LE(3, stats["num_throttled"]);
    };

    void opsAreNotThrottled(const std::string& bucketName,
                            int ru_consumed,
                            int wu_consumed) {
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
        ASSERT_EQ(ru_consumed, stats["ru_total"]); // 4096 reads done
        ASSERT_EQ(wu_consumed, stats["wu_total"]); // 1 write done
        ASSERT_EQ(0, stats["num_throttled"]);
    };
};

TEST_F(ThrottlingTests, OpsAreThrottled) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([this, name = "bucket" + std::to_string(i)]() {
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
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([this, name = "bucket" + std::to_string(i)]() {
            // Set high throttle limits to ensure that ops are not
            // throttled
            auto bucket = cluster->getBucket(name);
            bucket->setThrottleLimits(5000, 5000);

            opsAreNotThrottled(name, 4096, 1);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST_F(ThrottlingTests, ThrottleDisabled) {
    // Disable throttling at the server level. Even with low per-bucket
    // throttle limits, operations should not be throttled when
    // throttle_enabled is false.
    cluster->changeConfig(
            [](nlohmann::json& config) { config["throttle_enabled"] = false; });

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([this, name = "bucket" + std::to_string(i)]() {
            // Set very low throttle limits
            // throttle_enabled is false, ops should not be throttled
            auto bucket = cluster->getBucket(name);
            bucket->setThrottleLimits(100, 100);

            opsAreNotThrottled(name, 4096, 1);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Restore throttle_enabled so subsequent tests are not affected
    cluster->changeConfig(
            [](nlohmann::json& config) { config["throttle_enabled"] = true; });
}

// A range-scan-continue with no limits must still be subject to throttling.
// The scan reads far more than the bucket's hard limit, so the continue must
// yield (RangeScanMore) once the limit is reached rather than returning the
// whole range in one go, and the throttle must be visible in num_throttled.
TEST_F(ThrottlingTests, RangeScanContinueIsThrottled) {
    const std::string bucketName = "bucket0";
    auto bucket = cluster->getBucket(bucketName);
    auto conn = getConnection(bucketName);
    auto statsConn = getConnection(bucketName);
    // Mutations must return seqno/vb_uuid for the snapshot requirements
    conn->setFeature(cb::mcbp::Feature::MUTATION_SEQNO, true);

    // 64 incompressible ~4KiB documents, each costs 1 RU to read, the full
    // scan costs ~64 RU. Loading costs 256 WU which is below the fixture's
    // limit of 1000.
    constexpr size_t numDocs = 64;
    std::mt19937 gen(0);
    std::uniform_int_distribution<int> dist('a', 'z');
    std::unordered_set<std::string> expectedKeys;
    MutationInfo lastMutation;
    for (size_t i = 0; i < numDocs; ++i) {
        Document doc{};
        doc.info.id = DocKeyView::makeWireEncodedString(
                CollectionEntry::vegetable, fmt::format("rs{:03}", i));
        doc.value.resize(4000);
        for (auto& c : doc.value) {
            c = char(dist(gen));
        }
        lastMutation = conn->mutate(doc, Vbid{0}, MutationType::Set);
        expectedKeys.emplace(fmt::format("rs{:03}", i));
    }

    // Let a tick pass so the load's write units are not carried over, then
    // drop the limits far below the cost of the scan.
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});
    bucket->setThrottleLimits(10, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});
    const auto throttledBefore =
            getThrottlingStats(statsConn, bucketName)["num_throttled"]
                    .get<size_t>();

    nlohmann::json config = {
            {"range",
             {{"start", cb::base64::encode("rs")},
              {"end", cb::base64::encode("rs\xFF")}}},
            {"collection",
             fmt::format("{0:x}", uint32_t(CollectionEntry::vegetable.uid))},
            {"snapshot_requirements",
             {{"seqno", lastMutation.seqno},
              {"vb_uuid", std::to_string(lastMutation.vbucketuuid)},
              {"timeout_ms", 120000}}}};
    auto createResp = conn->execute(BinprotRangeScanCreate(Vbid(0), config));
    ASSERT_EQ(cb::mcbp::Status::Success, createResp.getStatus());
    cb::rangescan::Id id;
    ASSERT_EQ(sizeof(id.data), createResp.getDataView().size());
    std::memcpy(id.data,
                createResp.getDataView().data(),
                createResp.getDataView().size());

    std::unordered_set<std::string> seenKeys;
    // Issue a single continue with no limits and drain all of its frames.
    // @return the status of the final frame
    auto continueOnce = [&conn, &id, &seenKeys]() {
        conn->sendCommand(BinprotRangeScanContinue(Vbid(0), id, 0, 0, 0));
        BinprotResponse resp;
        do {
            conn->recvResponse(resp);
            if (resp.getDataView().empty() ||
                !(resp.getStatus() == cb::mcbp::Status::Success ||
                  resp.getStatus() == cb::mcbp::Status::RangeScanMore ||
                  resp.getStatus() == cb::mcbp::Status::RangeScanComplete)) {
                continue;
            }
            cb::mcbp::response::RangeScanContinueValuePayload payload(
                    resp.getDataView());
            for (auto record = payload.next(); record.key.data();
                 record = payload.next()) {
                EXPECT_TRUE(seenKeys.emplace(record.key).second)
                        << "Duplicate key " << record.key;
            }
        } while (resp.getStatus() == cb::mcbp::Status::Success);
        return resp.getStatus();
    };

    // The first continue must be cut short by throttling
    auto status = continueOnce();
    EXPECT_EQ(cb::mcbp::Status::RangeScanMore, status)
            << "range-scan-continue read " << seenKeys.size() << " documents (~"
            << seenKeys.size()
            << " RU) in a single request with a hard limit of 10 RU/s";
    EXPECT_LT(seenKeys.size(), numDocs);

    const auto stats = getThrottlingStats(statsConn, bucketName);
    EXPECT_LT(throttledBefore, stats["num_throttled"].get<size_t>())
            << "range-scan-continue was never throttled";

    // Drain the rest of the scan, it must still complete with every key
    for (int ii = 0; ii < 100 && status == cb::mcbp::Status::RangeScanMore;
         ++ii) {
        status = continueOnce();
    }
    EXPECT_EQ(cb::mcbp::Status::RangeScanComplete, status);
    EXPECT_EQ(expectedKeys, seenKeys);
}

TEST_F(ThrottlingTests, SetParamRejectsInvalidThrottleValue) {
    using cb::mcbp::request::SetParamPayload;
    auto bucket = cluster->getBucket("bucket0");
    bucket->setThrottleLimits(1000, 2000);

    auto conn = cluster->getConnection(0);
    conn->authenticate("@admin", "password");
    conn->selectBucket("bucket0");

    auto getLimits = [&conn]() {
        auto stats = conn->stats("config");
        return std::make_pair(stats["ep_throttle_reserved"].get<size_t>(),
                              stats["ep_throttle_hard_limit"].get<size_t>());
    };

    for (const auto& key : {"throttle_reserved", "throttle_hard_limit"}) {
        for (const auto& value : {"abc", "unlimited", "12abc"}) {
            auto rsp = conn->execute(BinprotSetParamCommand(
                    SetParamPayload::Type::Config, key, value));
            EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus())
                    << key << "=" << value << ": " << rsp.getDataView();
            EXPECT_EQ(std::make_pair(size_t{1000}, size_t{2000}), getLimits())
                    << key << "=" << value << " must not change the limits";
        }
    }
}
