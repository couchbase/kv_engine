/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <boost/filesystem.hpp>
#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <serverless/config.h>
#include <deque>
#include <vector>

namespace cb::test {
constexpr size_t MaxConnectionsPerBucket = 16;

std::unique_ptr<Cluster> ServerlessTest::cluster;

void ServerlessTest::StartCluster() {
    cluster = Cluster::create(
            3, {}, [](std::string_view, nlohmann::json& config) {
                config["deployment_model"] = "serverless";
                auto file =
                        boost::filesystem::path{
                                config["root"].get<std::string>()} /
                        "etc" / "couchbase" / "kv" / "serverless" /
                        "configuration.json";
                create_directories(file.parent_path());
                nlohmann::json json;
                json["max_connections_per_bucket"] = MaxConnectionsPerBucket;
                FILE* fp = fopen(file.generic_string().c_str(), "w");
                fprintf(fp, "%s\n", json.dump(2).c_str());
                fclose(fp);
            });
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void ServerlessTest::SetUpTestCase() {
    if (!cluster) {
        std::cerr << "Cluster not running" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    try {
        for (int ii = 0; ii < 5; ++ii) {
            const auto name = "bucket-" + std::to_string(ii);
            std::string rbac = R"({
"buckets": {
  "bucket-@": {
    "privileges": [
      "Read",
      "SimpleStats",
      "Insert",
      "Delete",
      "Upsert"
    ]
  }
},
"privileges": [],
"domain": "external"
})";
            rbac[rbac.find('@')] = '0' + ii;
            cluster->getAuthProviderService().upsertUser(
                    {name, name, nlohmann::json::parse(rbac)});

            auto bucket = cluster->createBucket(
                    name, {{"replicas", 2}, {"max_vbuckets", 8}});
            if (!bucket) {
                throw std::runtime_error("Failed to create bucket: " + name);
            }

            // Running under sanitizers slow down the system a lot so
            // lets use a lower limit to ensure that operations actually
            // gets throttled.
            bucket->setThrottleLimit(folly::kIsSanitize ? 256 : 1024);

            // @todo add collections and scopes
        }
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void ServerlessTest::TearDownTestCase() {
    // @todo iterate over the buckets and delete all of them
}

void ServerlessTest::ShutdownCluster() {
    cluster.reset();
}

void ServerlessTest::SetUp() {
    Test::SetUp();
}

void ServerlessTest::TearDown() {
    Test::TearDown();
}

TEST_F(ServerlessTest, MaxConnectionPerBucket) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto getNumClients = [&admin]() -> std::size_t {
        size_t num_clients = 0;
        admin->stats(
                [&num_clients](const auto& k, const auto& v) {
                    nlohmann::json json = nlohmann::json::parse(v);
                    num_clients = json["clients"].get<size_t>();
                },
                "bucket_details bucket-0");
        return num_clients;
    };

    std::deque<std::unique_ptr<MemcachedConnection>> connections;
    bool done = false;
    BinprotResponse rsp;
    do {
        auto conn = cluster->getConnection(0);
        conn->authenticate("bucket-0", "bucket-0");
        rsp = conn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
        if (rsp.isSuccess()) {
            connections.emplace_back(std::move(conn));
            ASSERT_LE(getNumClients(), MaxConnectionsPerBucket);
        } else {
            ASSERT_EQ(cb::mcbp::Status::RateLimitedMaxConnections,
                      rsp.getStatus());
            // Without XERROR E2BIG should be returned
            conn->setXerrorSupport(false);
            rsp = conn->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
            ASSERT_FALSE(rsp.isSuccess());
            ASSERT_EQ(cb::mcbp::Status::E2big, rsp.getStatus());
            done = true;
        }
    } while (!done);

    // But we should be allowed to connect internal users
    for (int ii = 0; ii < 5; ++ii) {
        auto conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("bucket-0");
        connections.emplace_back(std::move(conn));
    }
    EXPECT_EQ(MaxConnectionsPerBucket + 5, getNumClients());
}

TEST_F(ServerlessTest, OpsAreThrottled) {
    auto func = [this](const std::string& name) {
        auto conn = cluster->getConnection(0);
        conn->authenticate(name, name);
        conn->selectBucket(name);
        conn->setReadTimeout(std::chrono::seconds{3});

        Document document;
        document.info.id = "OpsAreThrottled";
        document.value = "This is the awesome document";

        // store a document
        conn->mutate(document, Vbid{0}, MutationType::Set);

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 4096; ++i) { // Run 4k mutations
            conn->get(document.info.id, Vbid{0});
        }
        auto end = std::chrono::steady_clock::now();
        EXPECT_LT(
                std::chrono::seconds{2},
                std::chrono::duration_cast<std::chrono::seconds>(end - start));

        nlohmann::json stats;
        conn->authenticate("@admin", "password");
        conn->stats(
                [&stats](const auto& k, const auto& v) {
                    stats = nlohmann::json::parse(v);
                },
                std::string{"bucket_details "} + name);
        ASSERT_FALSE(stats.empty());
        ASSERT_LE(3, stats["num_throttled"]);
        // it's hard to compare this with a "real value"; but it should at
        // least be non-zero
        ASSERT_NE(0, stats["throttle_wait_time"]);
    };

    std::vector<std::thread> threads;
    for (int ii = 0; ii < 5; ++ii) {
        threads.emplace_back(
                std::thread{[func, name = "bucket-" + std::to_string(ii)]() {
                    func(name);
                }});
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(ServerlessTest, ComputeUnitsReported) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("bucket-0", "bucket-0");
    conn->selectBucket("bucket-0");
    conn->setFeature(cb::mcbp::Feature::ReportComputeUnitUsage, true);
    conn->setReadTimeout(std::chrono::seconds{3});

    DocumentInfo info;
    info.id = "ComputeUnitsReported";

    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("This is a document");
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    ASSERT_TRUE(rsp.isSuccess());

    auto rcu = rsp.getReadComputeUnits();
    auto wcu = rsp.getWriteComputeUnits();

    ASSERT_FALSE(rcu.has_value()) << "mutate should not use RCU";
    ASSERT_TRUE(wcu.has_value()) << "mutate should use WCU";
    ASSERT_EQ(1, *wcu) << "The value should be 1 WCU";
    wcu.reset();

    rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, info.id});

    rcu = rsp.getReadComputeUnits();
    wcu = rsp.getWriteComputeUnits();

    ASSERT_TRUE(rcu.has_value()) << "get should use RCU";
    ASSERT_FALSE(wcu.has_value()) << "get should not use WCU";
    ASSERT_EQ(1, *rcu) << "The value should be 1 RCU";
}

} // namespace cb::test
