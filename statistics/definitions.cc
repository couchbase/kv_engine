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

#include <statistics/definitions.h>

#include <statistics/units.h>

#include <string_view>

namespace cb::stats {

using namespace std::string_view_literals;

#define LABEL(key, value) \
    { #key, #value }
#define STAT(statEnum, cbstatsName, unit, prometheusName, ...) \
    StatDef(std::string_view(cbstatsName).empty()              \
                    ? #statEnum                                \
                    : std::string_view(cbstatsName),           \
            unit,                                              \
            #prometheusName,                                   \
            {__VA_ARGS__}),
#define CBSTAT(statEnum, cbstatsName, ...)           \
    StatDef(std::string_view(cbstatsName).empty()    \
                    ? #statEnum                      \
                    : std::string_view(cbstatsName), \
            cb::stats::StatDef::CBStatsOnlyTag{}),
const std::array<StatDef, size_t(Key::enum_max)> statDefinitions{{
#include <statistics/stats.def.h>
}};
#undef CBSTAT
#undef STAT
#undef LABEL

StatDef::StatDef(std::string_view cbstatsKey,
                 cb::stats::Unit unit,
                 std::string_view metricFamilyKey,
                 Labels&& labels)
    : cbstatsKey(cbstatsKey),
      unit(unit),
      metricFamily(metricFamilyKey),
      labels(std::move(labels)) {
    if (metricFamily.empty()) {
        metricFamily = cbstatsKey;
    }
    metricFamily += unit.getSuffix();
}

StatDef::StatDef(std::string_view cbstatsKey, CBStatsOnlyTag)
    : cbstatsKey(cbstatsKey), cbStatsOnly(true) {
}

} // end namespace cb::stats