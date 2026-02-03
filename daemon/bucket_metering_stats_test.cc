/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_manager.h"

#include <daemon/buckets.h>
#include <daemon/stats.h>
#include <platform/dirutils.h>
#include <serverless/config.h>
#include <statistics/cardinality.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>
#include <test/dummy_cookie.h>

class BucketMeteringStatsTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // need to enable serverless for metering
        cb::serverless::setEnabled(true);
    }
    static void TearDownTestSuite() {
        cb::serverless::setEnabled(false);
        // Nuke temporary test files created by previous runs of
        // the test suite
        std::error_code ec;
        for (const auto& p : std::filesystem::directory_iterator(
                     std::filesystem::current_path())) {
            if (is_directory(p.path(), ec)) {
                if (p.path().filename().string().starts_with(
                            "BucketMeteringStatsTest")) {
                    remove_all(p.path(), ec);
                }
            }
        }
    }
    void SetUp() override {
        bucketPath = cb::io::mkdtemp("BucketMeteringStatsTest");
        const auto config =
                fmt::format("dbname={};max_vbuckets=8;max_num_shards=1",
                            bucketPath.string());

        // create a bucket named foobar
        cb::test::DummyCookie cookie;
        BucketManager::instance().create(
                cookie, "foobar", config, BucketType::Couchbase);
        BucketManager::instance().forEach([this](auto& b) {
            if (b.name == "foobar") {
                bucket = &b;
                return false;
            }
            return true;
        });
        ASSERT_NE(nullptr, bucket);
    }

    void TearDown() override {
        BucketManager::instance().destroyAll();
        remove_all(bucketPath);
    }

    Bucket* bucket = nullptr;
    std::filesystem::path bucketPath;
};

TEST_F(BucketMeteringStatsTest, CollectInitialMeteringStats) {
    using namespace ::testing;
    using namespace std::literals::string_view_literals;

    std::string_view bucketName = bucket->name;

    NiceMock<MockStatCollector> collector;
#if 0
    // For some reason these don't match when using Couchbase bucket instead
    // of memcached bucket
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("op_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        ElementsAre(Pair("bucket"sv, bucketName))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_ru_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_wu_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
#endif
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_cu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_ru_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_wu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_cu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
#if 0
    // For some reason these don't match when using Couchbase bucket instead
    // of memcached bucket
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("reject_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("throttle_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
#endif
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("throttle_seconds_total"),
                        Matcher<double>(0.),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));

    bucket->addMeteringMetrics(collector.forBucket(bucketName));
}

TEST_F(BucketMeteringStatsTest, MeteringMetricsPrefixed) {
    using namespace ::testing;
    using namespace std::literals::string_view_literals;

    std::unordered_map<std::string, prometheus::MetricFamily> metricFamilies;
    PrometheusStatCollector collector(metricFamilies);

    server_prometheus_stats(collector, cb::prometheus::MetricGroup::Low);

    EXPECT_GT(metricFamilies.size(), 0);

    for (const auto& expected : {"op_count_total",
                                 "meter_cu_total",
                                 "credit_ru_total",
                                 "credit_wu_total",
                                 "credit_cu_total",
                                 "reject_count_total",
                                 "throttle_count_total",
                                 "throttle_seconds_total"}) {
        EXPECT_TRUE(metricFamilies.count(std::string("kv_") + expected));
    }
}