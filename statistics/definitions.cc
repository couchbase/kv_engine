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

#include <statistics/definitions.h>

#include <statistics/units.h>

#include <string_view>
#include <utility>

namespace cb::stats {

using namespace std::string_view_literals;

#define LABEL(key, value) \
    { #key, #value }

// cbstatsName may have been specified ether as a string literal "foobar" or
// if formatting is required, as FMT("{foo}bar") which expands to
// CBStatsKey("{foo}bar", NeedsFormattingTag{}).
#define STAT(statEnum, cbstatsName, unit, prometheusName, ...)               \
    StatDef(std::string_view(cbstatsName).empty() ? CBStatsKey(#statEnum)    \
                                                  : CBStatsKey(cbstatsName), \
            unit,                                                            \
            std::string_view(#prometheusName).empty()                        \
                    ? #statEnum                                              \
                    : std::string_view(#prometheusName),                     \
            {__VA_ARGS__}),

#define CBSTAT(statEnum, cbstatsName, ...)                                   \
    StatDef(std::string_view(cbstatsName).empty() ? CBStatsKey(#statEnum)    \
                                                  : CBStatsKey(cbstatsName), \
            cb::stats::StatDef::CBStatsOnlyTag{}),

#define PSTAT(metricFamily, unit, ...) \
    StatDef(#metricFamily,             \
            unit,                      \
            {__VA_ARGS__},             \
            cb::stats::StatDef::PrometheusOnlyTag{}),

// Creates a CBStatsKey which _does_ need formatting. CBStatCollector will
// only use fmt::format to substitute label values into the key if this
// is used.
#define FMT(cbstatsName) \
    CBStatsKey(cbstatsName, CBStatsKey::NeedsFormattingTag{})

const std::array<StatDef, size_t(Key::enum_max)> statDefinitions{{
#include <statistics/stats.def.h>
}};
#undef FMT
#undef PSTAT
#undef CBSTAT
#undef STAT
#undef LABEL

StatDef::StatDef(CBStatsKey cbstatsKey,
                 cb::stats::Unit unit,
                 std::string_view metricFamilyKey,
                 Labels&& labels)
    : cbstatsKey(std::move(cbstatsKey)),
      unit(unit),
      metricFamily(metricFamilyKey),
      labels(std::move(labels)) {
    if (metricFamily.empty()) {
        metricFamily = std::string(cbstatsKey);
    }
    metricFamily += unit.getSuffix();
}

StatDef::StatDef(CBStatsKey cbstatsKey, CBStatsOnlyTag)
    : cbstatsKey(std::move(cbstatsKey)) {
}

StatDef::StatDef(std::string_view metricFamilyKey,
                 cb::stats::Unit unit,
                 Labels&& labels,
                 PrometheusOnlyTag)
    : unit(unit), metricFamily(metricFamilyKey), labels(std::move(labels)) {
    metricFamily += unit.getSuffix();
}

} // end namespace cb::stats