/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/buckets.h>
#include <daemon/stats.h>
#include <statistics/cardinality.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>
#include <test/dummy_cookie.h>

class BucketStatsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // create a bucket named foobar
        BucketManager::instance().create(
                cookie, "foobar", {}, BucketType::Memcached);
        bucket = &BucketManager::instance().at(1);
    }

    void TearDown() override {
        // destroy the bucket which was created for this test
        BucketManager::instance().doBlockingDestroy(cookie, "foobar", true, {});
    }

    Bucket* bucket = nullptr;
    cb::test::DummyCookie cookie;
};

class MetricAndOpcode {
public:
    MetricAndOpcode(std::string_view metric, std::string_view opcode = {})
        : metric{metric}, opcode{opcode} {
    }

    bool operator==(const MetricAndOpcode& other) const noexcept {
        return metric == other.metric && opcode == other.opcode;
    }

    std::string_view metric;
    std::string_view opcode;
};

template <>
struct std::hash<MetricAndOpcode> {
    std::size_t operator()(const MetricAndOpcode& mop) const noexcept {
        std::hash<std::string_view> hash;
        return hash(mop.metric) * 3 + hash(mop.opcode);
    }
};

using MetricsAndOpcodesByBucket =
        std::unordered_map<std::string_view,
                           std::unordered_set<MetricAndOpcode>>;

using MetricFamilies =
        std::unordered_map<std::string, prometheus::MetricFamily>;

static MetricsAndOpcodesByBucket getMetricsAndOpcodesByBucket(
        const MetricFamilies& metricFamilies) {
    MetricsAndOpcodesByBucket ret;

    for (auto& family : metricFamilies) {
        for (auto& metric : family.second.metric) {
            std::string_view bucketName;
            std::string_view opcode;
            for (auto& label : metric.label) {
                if (label.name == "bucket") {
                    EXPECT_TRUE(bucketName.empty());
                    bucketName = label.value;
                } else if (label.name == "opcode") {
                    EXPECT_TRUE(opcode.empty());
                    opcode = label.value;
                }
            }

            auto& bucket = ret[bucketName];
            bucket.insert({family.first});
            if (!opcode.empty()) {
                bucket.insert({family.first, opcode});
            }
        }
    }

    return ret;
}

TEST_F(BucketStatsTest, BucketStats) {
    using namespace std::chrono_literals;
    using namespace std::string_view_literals;

    auto& noBucket = BucketManager::instance().at(0);
    ASSERT_EQ(BucketType::NoBucket, noBucket.type);

    noBucket.timings.collect(cb::mcbp::ClientOpcode::Hello, 1ms);
    bucket->timings.collect(cb::mcbp::ClientOpcode::Get, 2ms);

    MetricFamilies metricFamiliesLow;
    MetricFamilies metricFamiliesHigh;
    server_prometheus_stats({metricFamiliesLow},
                            cb::prometheus::MetricGroup::Low);
    server_prometheus_stats({metricFamiliesHigh},
                            cb::prometheus::MetricGroup::High);

    auto low = getMetricsAndOpcodesByBucket(metricFamiliesLow);
    auto high = getMetricsAndOpcodesByBucket(metricFamiliesHigh);
    auto& noBucketLow = low.at(""sv);
    auto& noBucketHigh = high.at(""sv);
    auto& bucketLow = low.at("foobar"sv);
    auto& bucketHigh = high.at("foobar"sv);

    EXPECT_TRUE(bucketLow.count({"kv_cmd_mutation"sv}));
    EXPECT_FALSE(bucketHigh.count({"kv_cmd_mutation"sv}));
    EXPECT_FALSE(noBucketLow.count({"kv_cmd_mutation"sv}));
    EXPECT_FALSE(noBucketHigh.count({"kv_cmd_mutation"sv}));

    EXPECT_TRUE(bucketLow.count({"kv_memcache_total_items"sv}));
    EXPECT_FALSE(bucketHigh.count({"kv_memcache_total_items"sv}));
    EXPECT_FALSE(noBucketLow.count({"kv_memcache_total_items"sv}));
    EXPECT_FALSE(noBucketHigh.count({"kv_memcache_total_items"sv}));

    EXPECT_FALSE(bucketLow.count({"kv_cmd_duration_seconds"sv}));
    EXPECT_TRUE(bucketHigh.count({"kv_cmd_duration_seconds"sv}));
    EXPECT_FALSE(noBucketLow.count({"kv_cmd_duration_seconds"sv}));
    EXPECT_TRUE(noBucketHigh.count({"kv_cmd_duration_seconds"sv}));

    EXPECT_FALSE(noBucketLow.count({"kv_cmd_duration_seconds"sv, "HELLO"sv}));
    EXPECT_FALSE(bucketLow.count({"kv_cmd_duration_seconds"sv, "GET"sv}));
    EXPECT_TRUE(noBucketHigh.count({"kv_cmd_duration_seconds"sv, "HELLO"sv}));
    EXPECT_TRUE(bucketHigh.count({"kv_cmd_duration_seconds"sv, "GET"sv}));
}
