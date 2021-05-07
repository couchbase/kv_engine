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

#include <statistics/labelled_collector.h>

#include <memcached/dockey.h>

LabelledStatCollector::LabelledStatCollector(const StatCollector& parent,
                                             const Labels& labels)
    : parent(parent), defaultLabels(labels.begin(), labels.end()) {
}

LabelledStatCollector LabelledStatCollector::withLabels(Labels&& labels) const {
    // take the specific labels passed as parameters
    Labels mergedLabels{labels.begin(), labels.end()};
    // add in the labels stored in this collector
    // (will not overwrite labels passed as parameters)
    for (const auto& label : defaultLabels) {
        mergedLabels.emplace(label.first.c_str(), label.second);
    }

    // create a LabelledStatCollector directly wrapping the parent collector,
    // with the merged set of labels. This avoids chaining through multiple
    // LabelledStatCollectors.
    return {parent, mergedLabels};
}

bool LabelledStatCollector::hasLabel(std::string_view labelKey) const {
    return defaultLabels.count(std::string(labelKey)) != 0;
}

bool LabelledStatCollector::includeAggregateMetrics() const {
    return parent.includeAggregateMetrics();
}

void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    std::string_view v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}

void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    bool v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}

void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    int64_t v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}
void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    uint64_t v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}
void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    double v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}

void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    const HistogramData& v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}

void LabelledStatCollector::addStat(const cb::stats::StatDef& k,
                                    const HdrHistogram& v,
                                    const Labels& labels) const {
    forwardToParent(k, v, labels);
}

cb::engine_errc LabelledStatCollector::testPrivilegeForStat(
        std::optional<ScopeID> sid, std::optional<CollectionID> cid) const {
    return parent.testPrivilegeForStat(std::move(sid), std::move(cid));
}

BucketStatCollector::BucketStatCollector(const StatCollector& parent,
                                         std::string_view bucket)
    : LabelledStatCollector(parent, {{"bucket", bucket}}) {
}
ScopeStatCollector BucketStatCollector::forScope(std::string_view scopeName,
                                                 ScopeID scope) const {
    return {*this, scopeName, scope};
}

ScopeStatCollector::ScopeStatCollector(const BucketStatCollector& parent,
                                       std::string_view scopeName,
                                       ScopeID scope)
    : LabelledStatCollector(parent.withLabels(
              {{scopeNameKey, scopeName}, {scopeIDKey, scope.to_string()}})) {
}
ColStatCollector ScopeStatCollector::forCollection(
        std::string_view collectionName, CollectionID collection) const {
    return {*this, collectionName, collection};
}

ColStatCollector::ColStatCollector(const ScopeStatCollector& parent,
                                   std::string_view collectionName,
                                   CollectionID collection)
    : LabelledStatCollector(
              parent.withLabels({{collectionNameKey, collectionName},
                                 {collectionIDKey, collection.to_string()}})) {
}
