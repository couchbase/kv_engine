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

#include <unordered_map>

/**
 * StatCollector which wraps another StatCollector instance.
 * LabelledCollector forwards all calls to addStat(...) to the wrapped
 * instance with added labels.
 *
 * Should only be used through the derived Bucket/Scope/Collection collectors
 *
 * For example,
 *
 *  StatCollector c;
 *  ...
 *  c.addStat("stat_key1", value1, {{"bucket", "someBucketName"}});
 *  c.addStat("stat_key2", value2, {{"bucket", "someBucketName"}});
 *  c.addStat("stat_key3", value3, {{"bucket", "someBucketName"},
 *                                  {"scope", "scopeFoo"}});
 *
 * May be replaced with
 *
 *  StatCollector c;
 *  ...
 *  auto bucketC = c.forBucket("someBucketName");
 *  bucketC.addStat("stat_key1", value1);
 *  bucketC.addStat("stat_key2", value2);
 *  auto scopeC = bucketC.forScope(ScopeID(0x8));
 *  scopeC.addStat("stat_key3", value3);
 *
 * LabelledStatCollector implements the StatCollector interface, simply
 * adding additional labels to every addStat call.
 *
 */
class LabelledStatCollector : public StatCollector {
public:
    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;
    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 float v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) const override;

    void addStat(const cb::stats::StatDef& k,
                 const HdrHistogram& hist,
                 const Labels& labels) const override;

    cb::engine_errc testPrivilegeForStat(
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override;

    /**
     * Create a new LabelledStatCollector with all the labels of the current
     * instance, plus the _additional_ labels provided as arguments.
     */
    [[nodiscard]] LabelledStatCollector withLabels(Labels&& labels) const;

    /**
     * Test if a label has been set with the provided key.
     */
    bool hasLabel(std::string_view labelKey) const;

    bool includeAggregateMetrics() const override;

protected:
    /**
     * Construct a labelled stat collector, which forwards addStat calls
     * to the provided parent collector, and adds the provided labels to every
     * call.
     *
     * Not to be constructed directly, instead use the derived
     * Bucket/Scope/Collection types.
     *
     * @param parent collector to pass stats to
     * @param labels labels to add to each stat forwarded to parent
     */
    LabelledStatCollector(const StatCollector& parent, const Labels& labels);

    LabelledStatCollector(LabelledStatCollector&& other) = default;

    /**
     * Pass the provided stat to the stat collector this instance wraps,
     * adding the labels set in this instance.
     * @tparam T value type
     * @param k stat key
     */
    template <class T>
    void forwardToParent(const cb::stats::StatDef& k,
                         T&& v,
                         const Labels& labels) const {
        // take the specific labels passed as parameters
        Labels allLabels{labels.begin(), labels.end()};
        // add in the "default" labels stored in this collector
        // (will not overwrite labels passed as parameters)
        for (const auto& label : defaultLabels) {
            allLabels.emplace(label.first.c_str(), label.second);
        }

        parent.addStat(k, v, allLabels);
    }

    const StatCollector& parent;
    const std::unordered_map<std::string, std::string> defaultLabels;
};

class ScopeStatCollector;
class ColStatCollector;

/**
 * A collector for stats for a single bucket.
 *
 * Methods expecting to add stats for a specific bucket should take this
 * as a parameter, e.g.,
 *
 * addBucketStats(const BucketStatCollector& collector);
 *
 * This guarantees the collector has all required information (i.e., a bucket
 * label) to add stats for a single bucket, regardless of stat backend
 * implementation.
 */
class BucketStatCollector : public LabelledStatCollector {
public:
    BucketStatCollector(const StatCollector& parent, std::string_view bucket);
    [[nodiscard]] ScopeStatCollector forScope(std::string_view scopeName,
                                              ScopeID scope) const;
};

/**
 * A collector for stats for a single scope.
 *
 * See BucketStatCollector.
 */
class ScopeStatCollector : public LabelledStatCollector {
public:
    ScopeStatCollector(const BucketStatCollector& parent,
                       std::string_view scopeName,
                       ScopeID scope);
    [[nodiscard]] ColStatCollector forCollection(
            std::string_view collectionName, CollectionID collection) const;

    static constexpr const char* scopeNameKey{"scope"};
    static constexpr const char* scopeIDKey{"scope_id"};
};

/**
 * A collector for stats for a single collection.
 *
 * See BucketStatCollector.
 */
class ColStatCollector : public LabelledStatCollector {
public:
    ColStatCollector(const ScopeStatCollector& parent,
                     std::string_view collectionName,
                     CollectionID collection);

    static constexpr const char* collectionNameKey{"collection"};
    static constexpr const char* collectionIDKey{"collection_id"};
};
