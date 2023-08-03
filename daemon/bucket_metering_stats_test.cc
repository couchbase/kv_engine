/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/buckets.h>
#include <daemon/stats.h>
#include <fmt/format.h>
#include <serverless/config.h>
#include <statistics/cardinality.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>
#include <test/dummy_cookie.h>

/**
 * Helper class for access to protected/private state of
 * BucketManager.
 *
 * Avoids requiring a real Cookie and Connection to use
 * certain methods (or needing to friend each test fixture class)
 */
class BucketManagerIntrospector {
public:
    static cb::engine_errc create(const std::string name,
                                  const std::string config,
                                  BucketType type) {
        cb::test::DummyCookie cookie;
        return BucketManager::instance().create(cookie, name, config, type);
    }

    static cb::engine_errc destroy(const std::string name,
                                   bool force,
                                   std::optional<BucketType> type = {}) {
        cb::test::DummyCookie cookie;
        return BucketManager::instance().doBlockingDestroy(
                cookie, name, force, type);
    }
};

void engine_manager_shutdown();
class BucketMeteringStatsTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // need to enable serverless for metering
        cb::serverless::setEnabled(true);
    }
    static void TearDownTestSuite() {
        cb::serverless::setEnabled(false);
    }
    void SetUp() override {
        // create a bucket named foobar
        BucketManagerIntrospector::create("foobar", "", BucketType::Memcached);
        bucket = &BucketManager::instance().at(1);
    }

    void TearDown() override {
        // destroy the bucket which was created for this test
        BucketManagerIntrospector::destroy("foobar", true);
    }

    Bucket* bucket;
};

MATCHER_P(StatDefNameMatcher,
          expectedName,
          fmt::format("Check the name of the StatDef matches '{}'",
                      expectedName)) {
    return arg.cbstatsKey.key == expectedName;
}

TEST_F(BucketMeteringStatsTest, CollectInitialMeteringStats) {
    using namespace ::testing;
    using namespace std::literals::string_view_literals;

    std::string_view bucketName = bucket->name;

    NiceMock<MockStatCollector> collector;
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
                                 "meter_ru_total",
                                 "meter_wu_total",
                                 "meter_cu_total",
                                 "credit_ru_total",
                                 "credit_wu_total",
                                 "credit_cu_total",
                                 "reject_count_total",
                                 "throttle_count_total",
                                 "throttle_seconds_total"}) {
        // MB-56934: still expect the unprefixed versions too for now, to
        // give users time to switch over
        EXPECT_TRUE(metricFamilies.count(expected));
        EXPECT_TRUE(metricFamilies.count(std::string("kv_") + expected));
    }
}