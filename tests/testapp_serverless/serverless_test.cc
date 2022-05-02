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

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

namespace cb::test {

std::unique_ptr<Cluster> ServerlessTest::cluster;

void ServerlessTest::StartCluster() {
    cluster = Cluster::create(
            3, {}, [](std::string_view, nlohmann::json& config) {
                config["deployment_model"] = "serverless";
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
      "Write"
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
} // namespace cb::test
