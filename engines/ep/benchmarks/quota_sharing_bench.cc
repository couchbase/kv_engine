/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Benchmarks testing quota-sharing buckets using different workloads.
 * These should allows us to detect regressions introduced in the quota-sharing
 * code paths.
 */

#include <benchmark/benchmark.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <event2/thread.h>
#include <fmt/format.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>

static bool setUpEnvironment() {
    cb::net::initialize();

#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    const auto failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    const auto failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    const auto isasl_file_name = cb::io::sanitizePath(
            SOURCE_ROOT "/tests/testapp_cluster/cbsaslpw.json");
    setenv("CBSASL_PWFILE", isasl_file_name.data(), true);
    return !failed;
}

class QuotaSharingBench : public benchmark::Fixture {
public:
    void SetUp(benchmark::State& state) override {
        static bool isEnvSetup = setUpEnvironment();
        EXPECT_TRUE(isEnvSetup);

        quotaSharing = state.range(0);
        numBuckets = state.range(1);
        cluster = cb::test::Cluster::create(
                1, {}, [](auto&&, nlohmann::json& config) {
                    config["deployment_model"] = "serverless";
                });

        const nlohmann::json bucketConfig{
                {"max_size", 128 * 1024 * 1024},
                {"replicas", 0},
                {"backend", "magma"},
                {"item_eviction_policy", "full_eviction"},
                {"max_vbuckets", getNumVBuckets()},
                {"cross_bucket_ht_quota_sharing", quotaSharing}};

        for (size_t i = 0; i < numBuckets; i++) {
            if (!cluster->createBucket(fmt::format("bucket-{}", i),
                                       bucketConfig)) {
                throw std::runtime_error(
                        fmt::format("Failed to create bucket-{}", i));
            }
        }
    }

    void TearDown(benchmark::State& state) override {
        cluster.reset();
    }

    size_t getNumBuckets() const {
        return numBuckets;
    }

    size_t getNumVBuckets() const {
        return 64;
    }

    std::unique_ptr<MemcachedConnection> getBucketConnection(size_t bucketNum) {
        auto name = fmt::format("bucket-{}", bucketNum);
        auto bucket = cluster->getBucket(name);
        auto conn = bucket->getAuthedConnection(Vbid(0));
        return conn;
    }

private:
    std::shared_ptr<cb::test::Cluster> cluster;
    bool quotaSharing;
    size_t numBuckets;
};

BENCHMARK_DEFINE_F(QuotaSharingBench, UniformWriteWorkload)
(benchmark::State& state) {
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
    for (size_t i = 0; i < getNumBuckets(); i++) {
        connections.push_back(getBucketConnection(i));
    }

    size_t itemCounter = 0;
    for (auto _ : state) {
        for (auto& conn : connections) {
            ++itemCounter;
            conn->store(std::to_string(itemCounter),
                        Vbid(itemCounter % getNumVBuckets()),
                        std::string(2048, 'x'));
        }
    }
}

BENCHMARK_DEFINE_F(QuotaSharingBench, UniformReadWorkload)
(benchmark::State& state) {
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
    for (size_t i = 0; i < getNumBuckets(); i++) {
        connections.push_back(getBucketConnection(i));
    }

    const size_t numItems = 1000;
    for (size_t i = 0; i < numItems; i++) {
        for (auto& conn : connections) {
            conn->store(std::to_string(i),
                        Vbid(i % getNumVBuckets()),
                        std::string(2048, 'x'));
        }
    }

    size_t itemCounter = 0;
    for (auto _ : state) {
        for (auto& conn : connections) {
            itemCounter = (itemCounter + 1) % numItems;
            conn->get(std::to_string(itemCounter),
                      Vbid(itemCounter % getNumVBuckets()));
        }
    }
}

/**
 * Run a uniform write workload with 2KiB items across either 1 or 10 quota
 * sharing buckets. Compare with the same workload but with quota-sharing off.
 * The number of iterations corresponds to the number of items stored per
 * bucket. This should be run with --benchmark_min_time set to a reasonable
 * value.
 */
BENCHMARK_REGISTER_F(QuotaSharingBench, UniformWriteWorkload)
        ->ArgNames({"quota_sharing", "buckets"})
        ->ArgsProduct({{0, 1}, {1, 10}})
        ->UseRealTime();

/**
 * Run a uniform read workload with 1K items of size 2KiB across either 1 or 10
 * quota sharing buckets. Compare with the same workload but with quota-sharing
 * off. The number of iterations corresponds to the number of items stored per
 * bucket. This should be run with --benchmark_min_time set to a reasonable
 * value.
 */
BENCHMARK_REGISTER_F(QuotaSharingBench, UniformReadWorkload)
        ->ArgNames({"quota_sharing", "buckets"})
        ->ArgsProduct({{0, 1}, {1, 10}})
        ->UseRealTime();
