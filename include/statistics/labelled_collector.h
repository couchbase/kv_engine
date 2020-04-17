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

#pragma once

#include "collector.h"

#include <unordered_map>

/**
 * StatCollector which wraps another StatCollector instance.
 * LabelledCollector forwards all calls to addStat(...) to the wrapped
 * instance with added labels.
 *
 * Should only be constructed through StatCollector::withLabel(...);
 *
 * For example, rather than
 *
 *  StatCollector c;
 *  ...
 *  c.addStat("stat_key1", value1, {{"bucket", "someBucketName"}});
 *  c.addStat("stat_key2", value2, {{"bucket", "someBucketName"}});
 *  c.addStat("stat_key3", value3, {{"bucket", "someBucketName"},
 *                                  {"scope", "scopeFoo"}});
 *
 * This class simplifies this to
 *
 *  StatCollector c;
 *  ...
 *  auto labelled = c.withLabels({{"bucket", "someBucketName"}});
 *  labelled.addStat("stat_key1", value1);
 *  labelled.addStat("stat_key2", value2);
 *  labelled.addStat("stat_key3", value3, {{"scope", "scopeFoo"}});
 *
 * LabelledStatCollector implements the StatCollector interface,
 * and so may be used transparently by methods requiring a StatCollector;
 *
 *  void addCollectionStats(StatCollector&);
 *  ....
 *  StatCollector c;
 *  auto labelled = c.withLabels({{"bucket", "someBucketName"}});
 *  addCollectionStats(labelled);
 *
 */
class LabelledStatCollector : public StatCollector {
public:
    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    /**
     * Override the default LabelStatCollector creation. Creating a wrapper
     * around _this_ LabelledStatCollector would work fine, but would be
     * inefficient if the nesting gets deeper.
     *
     * Instead, construct a LabelledStatCollector wrapping the same `parent`
     * as this instance, with all of the labels of this instance, plus
     * those provided as arguments. With this, the cost of addStat remains the
     * same, and does not require an additional call for each "intermediate"
     * LabelledStatCollector
     *
     * e.g., given
     *
     *  StatCollector collector;
     *  auto a = collector.withLabels({{"bucket", "bucketName"}});
     *  auto b = a.withLabels({{"scope", "scopeName"}});
     *
     * calling
     *  b.addStat("mem_used", 9999);
     * expands directly to
     *  collector.addStat("mem_used", 9999, {{"bucket, "bucketName"},
     *                                       {"scope", "scopeName"}});
     * _without_ an intermediate call to a.addStat.
     *
     * This may also add clarity when debugging, as it avoids stepping down
     * through several wrappers before the real StatCollector is reached.
     */
    [[nodiscard]] LabelledStatCollector withLabels(
            const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) override;

    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) override;

    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) override;

    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) override;

private:
    /**
     * Construct a labelled stat collector, which forwards addStat calls
     * to the provided parent collector, and adds the provided labels to every
     * call.
     *
     * Not to be constructed directly, instead use the `withLabels()` method
     * of an existing StatCollector.
     *
     *  CBStatCollector cbstat;
     *  auto labelled = cbstat.withLabels({{"bucket", "bucketName"}});
     *  labelled.addStat("statName", statValue);
     *
     * @param parent collector to pass stats to
     * @param labels labels to add to each stat forwarded to parent
     */
    LabelledStatCollector(StatCollector& parent, const Labels& labels);

    /**
     * Pass the provided stat to the stat collector this instance wraps,
     * adding the labels set in this instance.
     * @tparam T value type
     * @param k stat key
     */
    template <class T>
    void forwardToParent(const cb::stats::StatDef& k,
                         T&& v,
                         const Labels& labels) {
        // take the specific labels passed as parameters
        Labels allLabels{labels.begin(), labels.end()};
        // add in the "default" labels stored in this collector
        // (will not overwrite labels passed as parameters)
        allLabels.insert(defaultLabels.begin(), defaultLabels.end());

        parent.addStat(k, v, allLabels);
    }

    StatCollector& parent;
    std::unordered_map<std::string, std::string> defaultLabels;

    // allow StatCollector to construct this type, in withLabels(...)
    friend class StatCollector;
};