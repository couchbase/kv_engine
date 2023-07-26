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

#include <daemon/buckets.h>
#include <daemon/mcaudit.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <daemon/timings.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest-matchers.h>
#include <prometheus/text_serializer.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <statistics/prometheus_collector.h>
#include <statistics/statdef.h>

#include <serverless/config.h>
#include <chrono>
#include <vector>

PrometheusStatTest::EndpointMetrics PrometheusStatTest::getMetrics() const {
    EndpointMetrics metrics;

    server_prometheus_stats(PrometheusStatCollector(metrics.low),
                            cb::prometheus::MetricGroup::Low);
    server_prometheus_stats(PrometheusStatCollector(metrics.high),
                            cb::prometheus::MetricGroup::High);
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

        EXPECT_THAT(metrics.low, Not(Contains(Key("boot_timestamp_seconds"))));
        EXPECT_THAT(metrics.low,
                    Not(Contains(Key("kv_boot_timestamp_seconds"))));

        EXPECT_THAT(metrics.high, Not(Contains(Key("boot_timestamp_seconds"))));
        EXPECT_THAT(metrics.high,
                    Not(Contains(Key("kv_boot_timestamp_seconds"))));
    }

    cb::serverless::setEnabled(true);

    {
        auto metrics = getMetrics();

        // MB-56934: still expect the unprefixed versions too for now, to
        // give users time to switch over to the prefixed version
        EXPECT_THAT(metrics.low, Contains(Key("boot_timestamp_seconds")));
        EXPECT_THAT(metrics.low, Contains(Key("kv_boot_timestamp_seconds")));

        // but still not in high cardinality endpoint
        EXPECT_THAT(metrics.high, Not(Contains(Key("boot_timestamp_seconds"))));
        EXPECT_THAT(metrics.high,
                    Not(Contains(Key("kv_boot_timestamp_seconds"))));
    }
}

TEST_F(PrometheusStatTest, metricType) {
    // Check that stats expected to be exposed as a counter/gauge/histogram
    // actually are.
    // use some audit stats to test that stats declared in
    // stat_definitions.json plumb through the metric type to the generated
    // StatDefs, and then to Prometheus.
    auto metrics = getMetrics();
    using namespace cb::stats;
    using namespace ::testing;

    // check that a selection of the collected metrics match the definitions
    EXPECT_EQ(::prometheus::MetricType::Untyped,
              metrics.low.at("kv_audit_enabled").type);

    EXPECT_EQ(::prometheus::MetricType::Counter,
              metrics.low.at("kv_audit_dropped_events").type);

    // and are serialised as expected

    ::prometheus::TextSerializer serialiser;

    std::string metricStr =
            serialiser.Serialize({metrics.low.at("kv_audit_enabled"),
                                  metrics.low.at("kv_audit_dropped_events")});

    // it's a little brittle to expect an exact string value
    // but it's a simple test and will show up any unexpected changes
    std::string expected = R"(# TYPE kv_audit_enabled untyped
kv_audit_enabled 0
# TYPE kv_audit_dropped_events counter
kv_audit_dropped_events 0
)";

    EXPECT_EQ(expected, metricStr);
}

TEST_F(PrometheusStatTest, counterGaugeValuesExposed) {
    // Check that the value provided for a counter/gauge is correctly exposed

    using namespace cb::stats;

    StatMap stats;
    PrometheusStatCollector collector(stats);

    collector.addStat(StatDef("some_counter",
                              units::none,
                              prometheus::MetricType::Counter,
                              {/* no labels*/},
                              StatDef::PrometheusOnlyTag{}),
                      12345);

    collector.addStat(StatDef("some_gauge",
                              units::none,
                              prometheus::MetricType::Gauge,
                              {/* no labels */},
                              StatDef::PrometheusOnlyTag{}),
                      54321);

    // check that each is serialised as expected

    ::prometheus::TextSerializer serialiser;

    {
        std::string metricStr =
                serialiser.Serialize({stats.at("some_counter")});

        std::string expected = R"(# TYPE some_counter counter
some_counter 12345
)";

        // Prior to MB-53979, this would fail as gauges/counters erroneously set
        // the "untyped" value in prometheus::ClientMetric, but exposed the
        // gauge/counter value - defaulting to 0.
        EXPECT_EQ(expected, metricStr);
    }

    {
        std::string metricStr = serialiser.Serialize({stats.at("some_gauge")});

        std::string expected = R"(# TYPE some_gauge gauge
some_gauge 54321
)";

        EXPECT_EQ(expected, metricStr);
    }
}

TEST_F(PrometheusStatTest, ThrottleSecondsTotalScaledToSeconds) {
    // verify that throttle_seconds_total is correctly scaled us -> s
    using namespace cb::stats;

    StatMap stats;
    PrometheusStatCollector collector(stats);

    class MockBucket : public Bucket {
    public:
        using Bucket::throttle_wait_time;
    } b;

    using namespace std::chrono;
    using namespace std::chrono_literals;
    b.throttle_wait_time = duration_cast<microseconds>(10.5s).count();
    b.addMeteringMetrics(collector.forBucket("foobar"));

    auto& metricVector = stats.at("throttle_seconds_total").metric;

    using namespace ::testing;
    ASSERT_THAT(metricVector, SizeIs(1));

    auto exposedValue = metricVector.front().counter.value;

    EXPECT_NEAR(10.5, exposedValue, 0.0001);
}

TEST_F(PrometheusStatTest, HistogramsHaveCorrectMetricType) {
    // MB-54789: Verify that adding a histogram stat will correctly provide
    // prometheus-cpp with prometheus::MetricType::Histogram

    using namespace cb::stats;

    StatMap stats;
    PrometheusStatCollector collector(stats);

    HistogramData hist;
    hist.buckets.push_back(HistogramBucket{0, 10, 1});
    hist.sampleCount = 1;
    hist.mean = 5;
    hist.sampleSum = 5;

    // hdrhistogram as if declared in stats_definitions.json as "histogram"
    collector.addStat(StatDef({},
                              cb::stats::units::none,
                              "DeclaredAsHistogram",
                              prometheus::MetricType::Histogram),
                      hist);

    // hdrhistogram as if declared in stats_definitions.json without a
    // specified type
    collector.addStat(StatDef({},
                              cb::stats::units::none,
                              "DeclaredAsDefault",
                              prometheus::MetricType::Untyped),
                      hist);

    for (auto type : {prometheus::MetricType::Gauge,
                      prometheus::MetricType::Counter,
                      prometheus::MetricType::Summary}) {
        EXPECT_THROW(collector.addStat(StatDef({},
                                               cb::stats::units::none,
                                               "DeclaredWrong",
                                               type),
                                       hist),
                     std::logic_error)
                << "Stat collector allowed a histogram value to be exposed "
                   "for a metric declared as type "
                << int(type);
    }

    // check that each added stat will provide prometheus-ccp with
    // prometheus::MetricType::Histogram as the type.
    using namespace ::testing;
    EXPECT_THAT(stats, SizeIs(2));

    EXPECT_THAT(stats,
                Each(Pair(_,
                          Field(&prometheus::MetricFamily::type,
                                prometheus::MetricType::Histogram))));
}

// Check that samples which exceed the highest bucket the timing histogram
// supports are still included in the Prometheus histogram (via the
// 'sample_count' field).
TEST_F(PrometheusStatTest, OverflowSamplesRecorded) {
    // Check that empty cmd_duration per-op histograms are not exposed if
    // memcached has not processed any occurrences of that operation
    // (and thus has no timing data for it)
    using namespace cb::stats;
    using namespace ::testing;
    using namespace std::chrono_literals;

    Timings dummyTimings;

    // Add two samples for operations - one within the range Timings supports,
    // one which exceeds it.
    dummyTimings.collect(cb::mcbp::ClientOpcode::Get, 10ms);
    dummyTimings.collect(cb::mcbp::ClientOpcode::Get, 10h);

    // Collect the timing stats for consumption by Prometheus.
    StatMap stats;
    server_bucket_timing_stats(
            PrometheusStatCollector(stats).forBucket("foobar"), dummyTimings);

    // Only the one instance should be present, for GET.
    const auto& metricFamily = stats["cmd_duration_seconds"];
    ASSERT_THAT(metricFamily.metric, SizeIs(1));

    auto getMetric = metricFamily.metric.front();
    const auto& histogram = getMetric.histogram;

    // Sanity check - last bucket should _not_ include the overflow values,
    // as that would imply that the "out of range" value used above was too
    // small - it actually _does_ reside in one of the defined bucket.
    // (Note Prometheus-cpp adds an extra "le=Inf" bucket, using the total
    // sample_count when it exposes the MetricFamily over HTTP).
    ASSERT_EQ(1, histogram.bucket.back().cumulative_count)
            << "last defined bucket should not include overflow samples";

    EXPECT_EQ(2, histogram.sample_count)
            << "sample_count should include samples in range and overflows";
}
