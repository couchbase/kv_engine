/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "prometheus_test.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <daemon/mcaudit.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <daemon/timings.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest-matchers.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <statistics/prometheus_collector.h>

PrometheusStatTest::EndpointMetrics PrometheusStatTest::getMetrics() const {
    EndpointMetrics metrics;

    server_prometheus_stats(PrometheusStatCollector(metrics.low),
                            cb::prometheus::Cardinality::Low);
    server_prometheus_stats(PrometheusStatCollector(metrics.high),
                            cb::prometheus::Cardinality::High);
    server_prometheus_metering(PrometheusStatCollector(metrics.metering));
    return metrics;
}

TEST_F(PrometheusStatTest, auditStatsNotPerBucket) {
    // confirm audit stats are not labelled with a bucket - they are actually
    // global stats.
    initialize_audit();
    auto metrics = getMetrics();
    using namespace cb::stats;
    using namespace ::testing;

    for (const auto& metricName :
         {"kv_audit_dropped_events", "kv_audit_enabled"}) {
        // confirm audit stats are not in cardinality endpoint
        EXPECT_EQ(0, metrics.high.count(metricName));

        // confirm audit stats _are_ in low cardinality endpoint
        EXPECT_EQ(1, metrics.low.count(metricName));

        const auto& metric = metrics.low[metricName];

        // there should only be one instance of the metric
        EXPECT_THAT(metric.metric, SizeIs(1));

        // That metric should have no labels. Per-bucket metrics have
        // a bucket name label.
        EXPECT_THAT(metric.metric.front().label, IsEmpty());
    }
}

TEST_F(PrometheusStatTest, EmptyOpTimingHistograms) {
    // Check that empty cmd_duration per-op histograms are not exposed if
    // memcached has not processed any occurrences of that operation
    // (and thus has no timing data for it)
    using namespace cb::stats;
    using namespace ::testing;
    using namespace std::chrono_literals;

    auto metricName = "cmd_duration_seconds";

    Timings dummyTimings;

    // Add a fake tining record for a set op. This histogram should then be
    // present, but all others should be absent.
    dummyTimings.collect(cb::mcbp::ClientOpcode::Set, 10ms);

    StatMap stats;

    server_bucket_timing_stats(
            PrometheusStatCollector(stats).forBucket("foobar"), dummyTimings);

    // confirm metricFamily family _is_ in  output
    EXPECT_EQ(1, stats.count(metricName));

    const auto& metricFamily = stats[metricName];

    // Only the one instance should be present, for set.
    // ASSERT as later expectations are not necessarily valid if this is not
    // true.
    ASSERT_THAT(metricFamily.metric, SizeIs(1));

    // Check that the set histogram has the correct range of buckets
    auto expectedLabel = prometheus::ClientMetric::Label();
    expectedLabel.name = "opcode";
    expectedLabel.value = "SET";

    auto clientMetric = metricFamily.metric.front();
    const auto& histogram = clientMetric.histogram;

    EXPECT_EQ(1, histogram.sample_count);

    auto maxTrackable =
            dummyTimings
                    .get_timing_histogram(uint8_t(cb::mcbp::ClientOpcode::Set))
                    ->getMaxTrackableValue();

    auto expectedBuckets = int(std::ceil(std::log2(maxTrackable))) + 1;
    // plus one for the lowest bucket [0,1], log2(2) == 1 but
    // a histogram with maxTrackable==2 would be expected to have two
    // buckets.

    EXPECT_THAT(histogram.bucket, SizeIs(expectedBuckets));
}

TEST_F(PrometheusStatTest, MeteringNotPrefixed) {
    // Check that _metering endpoint metrics are not prefixed with kv_
    using namespace ::testing;
    using namespace std::chrono_literals;

    auto metrics = getMetrics();

    EXPECT_THAT(metrics.metering, Contains(Key("boot_timestamp_seconds")));
    EXPECT_THAT(metrics.metering,
                Not(Contains(Key("kv_boot_timestamp_seconds"))));
}

TEST_F(PrometheusStatTest, MeteringIncludedInHighCardinality) {
    // Check that metering metrics are included in the high cardinality endpoint
    // iff serverless deployment, and that they are not prefixed with kv_
    using namespace ::testing;
    using namespace std::chrono_literals;

    {
        auto metrics = getMetrics();

        EXPECT_THAT(metrics.high, Not(Contains(Key("boot_timestamp_seconds"))));
        EXPECT_THAT(metrics.high,
                    Not(Contains(Key("kv_boot_timestamp_seconds"))));
    }

    Settings::instance().setDeploymentModel(DeploymentModel::Serverless);

    {
        auto metrics = getMetrics();

        EXPECT_THAT(metrics.high, Contains(Key("boot_timestamp_seconds")));
        EXPECT_THAT(metrics.high,
                    Not(Contains(Key("kv_boot_timestamp_seconds"))));
    }
}