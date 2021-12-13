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

#include <statistics/prometheus_collector.h>

#include <hdrhistogram/hdrhistogram.h>
#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <statistics/labelled_collector.h>

#include <cmath>

void PrometheusStatCollector::addStat(const cb::stats::StatDef& spec,
                                      const HistogramData& hist,
                                      const Labels& additionalLabels) const {
    if (!spec.isPrometheusStat()) {
        return;
    }
    prometheus::ClientMetric metric;

    metric.histogram.sample_count = hist.sampleCount;
    // sample sum should be converted to the same unit as the
    // bucket values.
    metric.histogram.sample_sum = spec.unit.toBaseUnit(hist.sampleSum);

    uint64_t cumulativeCount = 0;

    for (const auto& bucket : hist.buckets) {
        cumulativeCount += bucket.count;
        auto normalised = spec.unit.toBaseUnit(bucket.upperBound);
        metric.histogram.bucket.push_back(
                {cumulativeCount, gsl::narrow_cast<double>(normalised)});
    }

    addClientMetric(spec,
                    additionalLabels,
                    std::move(metric),
                    prometheus::MetricType::Histogram);
}

void PrometheusStatCollector::addStat(const cb::stats::StatDef& spec,
                                      const HdrHistogram& v,
                                      const Labels& labels) const {
    HistogramData histData;

    if (v.getValueCount() > 0) {
        histData.mean = std::round(v.getMean());
        histData.sampleCount = v.getValueCount();
    }

    for (const auto& bucket :
         v.logViewRepresentable(1 /*firstBucketWidth*/, 2 /* logBase */)) {
        histData.buckets.push_back(
                {bucket.lower_bound, bucket.upper_bound, bucket.count});

        // TODO: HdrHistogram doesn't track the sum of all added values but
        //  prometheus requires that value. For now just approximate it
        //  from bucket counts.
        auto avgBucketValue = (bucket.lower_bound + bucket.upper_bound) / 2;
        histData.sampleSum += avgBucketValue * bucket.count;
    }

    // ns_server may rely on some stats being present in prometheus,
    // even if they are empty.
    addStat(spec, histData, labels);
}

void PrometheusStatCollector::addStat(const cb::stats::StatDef& spec,
                                      double v,
                                      const Labels& additionalLabels) const {
    if (!spec.isPrometheusStat()) {
        return;
    }
    prometheus::ClientMetric metric;
    metric.untyped.value = spec.unit.toBaseUnit(v);
    addClientMetric(spec,
                    additionalLabels,
                    std::move(metric),
                    prometheus::MetricType::Untyped);
}

cb::engine_errc PrometheusStatCollector::testPrivilegeForStat(
        std::optional<ScopeID> sid, std::optional<CollectionID> cid) const {
    // Prometheus MetricServer requires the authed user to have the Stats
    // privilege, so a PrometheusStatCollector will only be created for
    // users with access to _all_ stats
    return cb::engine_errc::success;
}

void PrometheusStatCollector::addClientMetric(
        const cb::stats::StatDef& key,
        const Labels& additionalLabels,
        prometheus::ClientMetric metric,
        prometheus::MetricType metricType) const {
    auto name = key.metricFamily;

    auto [itr, inserted] = metricFamilies.try_emplace(
            std::string(name), prometheus::MetricFamily());
    auto& metricFamily = itr->second;
    if (inserted) {
        metricFamily.name = prefix + std::string(name);
        metricFamily.type = metricType;
    }

    metric.label.reserve(key.labels.size() + additionalLabels.size());

    // start with the labels passed down from the collector
    // these are labels common to many stats, and have runtime values like
    // {{"bucket", "someBucketName"},{"scope", "scopeName"}}
    for (const auto& [label, value] : additionalLabels) {
        // Strip scope and collection ID labels. The IDs are used by
        // CBStatCollector so are set in collector labels, but they are not
        // desired to in Prometheus.
        if (label == ScopeStatCollector::scopeIDKey ||
            label == ColStatCollector::collectionIDKey) {
            continue;
        }
        metric.label.push_back({std::string(label), std::string(value)});
    }

    // Set the labels specific to this stat. These are specified in
    // stats.def.h.
    // These are labels intrinsic to the specific stat - e.g.,
    // "cmd_set" and "cmd_get"
    // could be exposed as
    //  cmd_count{operation="set"}
    //  cmd_count{operation="get"}
    // These labels are not expected to be overriden with different values.
    for (const auto& [label, value] : key.labels) {
        // It is not currently expected that any of these labels will also have
        // a value set in additionalLabels (from the collector or addStat call).
        // If this changes, the stat should _not_ be added here, to avoid
        // duplicate labels being exposed to Prometheus.
        Expects(!additionalLabels.count(label));
        metric.label.push_back({std::string(label), std::string(value)});
    }
    metricFamily.metric.emplace_back(std::move(metric));
}