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

#include "units.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace cb::stats {
/**
 * Type representing the key under which a stat should be reported.
 *
 * StatCollector implementations which support labelled stats with
 * shared names may use the metricFamilyKey and labels, if present -
 * for example PrometheusStatCollector.
 *
 * Implementations which do not support labels should use the uniqueKey.
 * These keys should be unique per-bucket.
 *
 * For example, "cmd_get" and "cmd_set". CBStats must report them under
 * unique names, but Prometheus could report them under the same metric
 * name (e.g., "cmd") but with a label to distinguish them.
 * This is not a necessity, but is a nice-to-have to make Prometheus
 * querying more expressive.
 *
 * This type should mainly be constructed from the definitions in
 * stats.def.h, but can also be created directly as a fallback until
 * all stats have been transitioned to stats.def.h.
 */
struct StatDef {
    using Labels = std::unordered_map<std::string_view, std::string_view>;
    /**
     * Construct from char* to minimize code change while migrating to
     * the StatCollector interface.
     *
     * Ideally stats would specify a StatKey to lookup a StatDef,
     * but until all stats have been added to stats.def.h, this
     * allows convenient construction of a basic StatDef.
     */
    StatDef(const char* uniqueKey) : StatDef(std::string_view(uniqueKey)) {
    }

    /**
     * Constructs a stat specification including the information needed to
     * expose a stat through either CBStats or Prometheus.
     *
     * @param uniqueKey name of stat which is unique within a bucket. Used by
     * CBStats.
     * @param unit the quantity this stat represents (bytes, seconds etc.) and
     * the scale prefix (kilo, micro etc.). Used by PrometheusStatCollector to
     * normalise stats into their base units, and suffix them correctly.
     * @param metricFamilyKey name which may be shared by multiple stats with
     * distinguishing labels. Used to group stats.
     * @param labels key/value pairs used by Prometheus to filter/aggregate
     * stats
     */
    StatDef(std::string_view uniqueKey,
            cb::stats::Unit unit = cb::stats::units::none,
            std::string_view metricFamilyKey = "",
            Labels&& labels = {});

    // Key which is unique per bucket. Used by CBStats
    std::string_view uniqueKey;
    // The unit this stat represents, e.g., microseconds.
    // Used to scale to base units and name the stat correctly for Prometheus,
    cb::stats::Unit unit;
    // Metric name used by Prometheus. Used to "group"
    // stats in combination with distinguishing labels.
    std::string metricFamily;
    // Labels for this metric. Labels set here will
    // override defaults labels set in the StatCollector
    Labels labels;
};

// don't need labels for the enum
#define LABEL(...)
#define STAT(statName, ...) statName,
enum class Key {
#include "stats.def.h"

    enum_max
};
#undef STAT
#undef LABEL

using namespace units;

extern const std::array<StatDef, size_t(Key::enum_max)> statDefinitions;

} // namespace cb::stats
