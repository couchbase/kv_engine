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
#include <event2/thread.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <platform/cbassert.h>
#include <platform/platform_socket.h>
#include <platform/socket.h>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <string>

namespace cb::test {
std::unique_ptr<cb::test::Cluster> cluster;

/// Start the cluster with 3 nodes all set to serverless deployment;
/// create 5 buckets named [bucket-0, bucket-4] and set up the
/// authentication module to provide users with access to those buckets
void startCluster() {
    cluster = cb::test::Cluster::create(
            3, {}, [](std::string_view, nlohmann::json& config) {
                config["deployment_model"] = "serverless";
                auto file =
                        std::filesystem::path{
                                config["root"].get<std::string>()} /
                        "etc" / "couchbase" / "kv" / "serverless" /
                        "configuration.json";
                create_directories(file.parent_path());
                nlohmann::json json;
                json["max_connections_per_bucket"] =
                        cb::test::MaxConnectionsPerBucket;
                FILE* fp = fopen(file.generic_string().c_str(), "w");
                fprintf(fp, "%s\n", json.dump(2).c_str());
                fclose(fp);
            });
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
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
      "Upsert",
      "DcpProducer",
      "DcpStream"
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

void shutdownCluster() {
    cluster.reset();
}

} // namespace cb::test

int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    cb::net::initialize();

#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    const auto failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    const auto failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    if (failed) {
        std::cerr << "Failed to enable libevent locking. Terminating program"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    setenv("MEMCACHED_UNIT_TESTS", "true", 1);

    auto pwdb = std::filesystem::path{SOURCE_ROOT} / "tests" /
                "testapp_serverless" / "pwdb.json";
    setenv("CBSASL_PWFILE", pwdb.generic_string().c_str(), 1);

    auto rbac = std::filesystem::path{SOURCE_ROOT} / "tests" /
                "testapp_serverless" / "rbac.json";
    setenv("MEMCACHED_RBAC", rbac.generic_string().c_str(), 1);

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE" << std::endl;
        return 1;
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);

    cb::test::startCluster();
    const auto ret = RUN_ALL_TESTS();
    cb::test::shutdownCluster();

    return ret;
}
