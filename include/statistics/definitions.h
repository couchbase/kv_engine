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

#include "units.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace cb::stats {

/**
 * Small wrapper to track whether a key from stats.def.h needs to be formatted.
 *
 * This is used to distinguish:
 *
 *  STAT(foo_bar, "foo_bar",...)
 *    -> StatDef("foo_bar", ...)
 * from
 *  STAT(foo_bar, FMT("{scope}:foo_bar"),...)
 *    -> StatDef( CBStatsKey("{scope}:foo_bar", NeedsFormattingTag{}), ...)
 */
class CBStatsKey {
public:
    // default constructor required for stats.def.h if cbstats name is not given
    CBStatsKey() = default;

    CBStatsKey(std::string_view key) : key(key) {
    }

    struct NeedsFormattingTag {};

    CBStatsKey(std::string_view key, NeedsFormattingTag)
        : key(key), needsFormatting(true) {
    }

    operator std::string_view() const {
        return key;
    }

    bool empty() const {
        return key.empty();
    }

    const std::string_view key;
    const bool needsFormatting = false;
};

/**
 * Type representing the key under which a stat should be reported.
 *
 * StatCollector implementations which support labelled stats with
 * shared names may use the metricFamilyKey and labels, if present -
 * for example PrometheusStatCollector.
 *
 * Implementations which do not support labels should use the cbstatsKey.
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
struct STATISTICS_PUBLIC_API StatDef {
    using Labels = std::unordered_map<std::string_view, std::string_view>;
    /**
     * Construct from char* to minimize code change while migrating to
     * the StatCollector interface.
     *
     * Ideally stats would specify a StatKey to lookup a StatDef,
     * but until all stats have been added to stats.def.h, this
     * allows convenient construction of a basic StatDef.
     *
     * StatDefs created in this manner will _not_ be formatted. Code which
     * is unaware of StatCollectors may generate stat keys which are not
     * safe to provide to fmt::format (e.g., may happen to contain invalid
     * replacement specifiers).
     */
    StatDef(const char* uniqueKey) : StatDef(std::string_view(uniqueKey)) {
    }

    StatDef(std::string_view uniqueKey) : cbstatsKey(CBStatsKey(uniqueKey)) {
    }

    /**
     * Construct a stat definition for use by CBStats and Prometheus.
     *
     * Constructs a stat specification including the information needed to
     * expose a stat through either CBStats or Prometheus.
     *
     * @param cbstatsKey name of stat which is unique within a bucket. Used by
     * CBStats.
     * @param unit the quantity this stat represents (bytes, seconds etc.) and
     * the scale prefix (kilo, micro etc.). Used by PrometheusStatCollector to
     * normalise stats into their base units, and suffix them correctly.
     * @param metricFamilyKey name which may be shared by multiple stats with
     * distinguishing labels. Used to group stats.
     * @param labels key/value pairs used by Prometheus to filter/aggregate
     * stats
     */
    StatDef(CBStatsKey cbstatsKey,
            cb::stats::Unit unit = cb::stats::units::none,
            std::string_view metricFamilyKey = "",
            Labels&& labels = {});

    /**
     * Tag struct used to explicitly identify stats which should not be exposed
     * to Prometheus.
     */
    struct CBStatsOnlyTag {};

    /**
     * Tag struct used to explicitly identify stats which should not be exposed
     * for cbstats.
     */
    struct PrometheusOnlyTag {};

    /**
     * Construct a CBStats-only stat definition.
     *
     * Constructs a stat specification including the information needed to
     * expose a stat only through CBStats. The stat will _not_ be exposed over
     * Prometheus.
     *
     * @param cbstatsKey name of stat which is unique within a bucket.
     */
    StatDef(CBStatsKey cbstatsKey, CBStatsOnlyTag);

    /**
     * Construct a Prometheus-only stat definition.
     *
     * Constructs a stat specification including the information needed to
     * expose a stat only for Prometheus. The stat will _not_ be exposed over
     * cbstats.
     *
     * @param metricFamilyKey name of stat which is unique within a bucket.
     */
    StatDef(std::string_view metricFamilyKey,
            cb::stats::Unit unit,
            Labels&& labels,
            PrometheusOnlyTag);

    bool isCBStat() const {
        return !cbstatsKey.empty();
    }

    bool isPrometheusStat() const {
        return !metricFamily.empty();
    }

    bool needsFormatting() const {
        return cbstatsKey.needsFormatting;
    }

    // Key which should be unique per bucket, possibly after being formatted
    // with values from `labels`. Used by CBStats.
    const CBStatsKey cbstatsKey;
    // The unit this stat represents, e.g., microseconds.
    // Used to scale to base units and name the stat correctly for Prometheus,
    const cb::stats::Unit unit = cb::stats::units::none;
    // Metric name used by Prometheus. Used to "group"
    // stats in combination with distinguishing labels.
    std::string metricFamily;
    // Labels for this metric. Labels set here will
    // override defaults labels set in the StatCollector
    const Labels labels;
};

// don't need labels for the enum
#define LABEL(...)
// don't need formatted cbstats keys for the enum, only the enum key is needed.
#define FMT(CBStatName)
#define STAT(statName, ...) statName,
#define CBSTAT(statName, ...) statName,
#define PSTAT(statName, ...) statName,
enum class STATISTICS_PUBLIC_API Key {
#include "stats.def.h"

    enum_max
};
#undef PSTAT
#undef CBSTAT
#undef STAT
#undef FMT
#undef LABEL

using namespace units;

extern const std::array<StatDef, size_t(Key::enum_max)> statDefinitions;

} // namespace cb::stats
