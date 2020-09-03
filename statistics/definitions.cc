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

#define LABEL(key, value) \
    { #key, #value }
#define STAT(statName, unit, prometheusName, ...) \
    StatDef(#statName, unit, #prometheusName, {__VA_ARGS__}),
const std::array<StatDef, size_t(Key::enum_max)> statDefinitions{{
#include <statistics/stats.def.h>
}};
#undef STAT
#undef LABEL

StatDef::StatDef(std::string_view uniqueKey,
                 cb::stats::Unit unit,
                 std::string_view metricFamilyKey,
                 Labels&& labels)
    : uniqueKey(uniqueKey),
      unit(unit),
      metricFamily(metricFamilyKey),
      labels(std::move(labels)) {
    if (metricFamily.empty()) {
        metricFamily = uniqueKey;
    }
    metricFamily += unit.getSuffix();
}

} // end namespace cb::stats