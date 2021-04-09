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

#pragma once

#include "collector.h"
#include "definitions.h"
#include <prometheus/client_metric.h>
#include <prometheus/metric_family.h>

class STATISTICS_PUBLIC_API PrometheusStatCollector : public StatCollector {
public:
    /**
     * Construct a collector for Prometheus stats.
     * @param metricFamilies[in,out] map of family name to MetricFamily struct
     *        in which to store all collected stats
     * @param prefix string to prepend to all collected stat names
     */
    PrometheusStatCollector(
            std::unordered_map<std::string, prometheus::MetricFamily>&
                    metricFamilies,
            std::string prefix = "kv_")
        : metricFamilies(metricFamilies), prefix(std::move(prefix)) {
    }

    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) const override {
        // silently discard text stats (for now). Prometheus can't expose them
    }

    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) const override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) const override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) const override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 const HdrHistogram& hist,
                 const Labels& labels) const override;

    bool includeAggregateMetrics() const override {
        return false;
    }

    cb::engine_errc testPrivilegeForStat(
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override;

protected:
    void addClientMetric(const cb::stats::StatDef& key,
                         const Labels& additionalLabels,
                         prometheus::ClientMetric metric,
                         prometheus::MetricType metricType =
                                 prometheus::MetricType::Untyped) const;

    std::unordered_map<std::string, prometheus::MetricFamily>& metricFamilies;
    const std::string prefix;
};
