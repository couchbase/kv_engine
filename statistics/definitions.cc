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

// constexpr check if character would match [a-zA-Z]
constexpr bool isAlpha(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}
// constexpr check if character would match [a-zA-Z0-9]
constexpr bool isAlphaNum(char c) {
    return isAlpha(c) || (c >= '0' && c <= '9');
}

template <int N>
// must accept a string literal, std::array is not suitable.
// NOLINTNEXTLINE(modernize-avoid-c-arrays)
constexpr bool isEmptyStr(const char (&name)[N]) {
    return N == 1 && name[0] == '\0';
}

/**
 * Validate at compile time that a metric family string literal name meets the
 * regex:
 *
 *   [a-zA-Z_][a-zA-Z0-9_]*
 *
 * Note: metric names _may_ also include colons, but these are reserved for
 * user defined recording rules and should _not_ be included in any names
 * exposed by KV.
 */
template <int N>
// NOLINTNEXTLINE(modernize-avoid-c-arrays)
constexpr bool isValidMetricFamily(const char (&name)[N]) {
    if (isEmptyStr(name)) {
        return false;
    }
    if (!(isAlpha(name[0]) || name[0] == '_')) {
        return false;
    }
    // checking chars [1, N-1] as the first char has been tested, and the
    // last char should be \0
    for (int i = 1; i < N - 1; ++i) {
        const auto& c = name[i];
        if (!(isAlphaNum(c) || c == '_')) {
            return false;
        }
    }
    return true;
}

//// Validation
//// verify prometheus metric families have valid names.

// label does not need validating here
#define LABEL(key, value)

// if no promtheusName was specified, fall back to the statEnum
#define STAT(statEnum, cbstatsName, unit, prometheusName, ...) \
    static_assert(                                             \
            isValidMetricFamily(#prometheusName) ||            \
            (isEmptyStr(#prometheusName) && isValidMetricFamily(#statEnum)));

// no validation, does not have a prometheus metric family name
#define CBSTAT(statEnum, cbstatsName, ...)

#define PSTAT(metricFamily, unit, ...) \
    static_assert(isValidMetricFamily(#metricFamily));

// no validation here, is used by cbstats
#define FMT(cbstatsName)
#include <statistics/stats.def.h>
#undef FMT
#undef PSTAT
#undef CBSTAT
#undef STAT
#undef LABEL

//// Stat definitions
//// Names have been validated above, now use stats.def.h to generate
//// the actual stat definitions.

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