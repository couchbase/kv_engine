/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "prometheus_test.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <daemon/mcaudit.h>
#include <daemon/stats.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest-matchers.h>
#include <statistics/prometheus.h>
#include <statistics/prometheus_collector.h>

PrometheusStatTest::EndpointMetrics PrometheusStatTest::getMetrics() const {
    EndpointMetrics metrics;

    server_prometheus_stats(PrometheusStatCollector(metrics.low),
                            cb::prometheus::Cardinality::Low);
    server_prometheus_stats(PrometheusStatCollector(metrics.high),
                            cb::prometheus::Cardinality::High);
    return metrics;
}

TEST_F(PrometheusStatTest, auditStatsNotPerBucket) {
    // confirm audit stats are not labelled with a bucket - they are actually
    // global stats.
    initialize_audit();
    auto metrics = getMetrics();
    using namespace cb::stats;
    using namespace ::testing;

    for (const auto& metricName : {"audit_dropped_events", "audit_enabled"}) {
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