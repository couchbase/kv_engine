/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <statistics/statdef.h>

#include <statistics/units.h>

#include <string_view>
#include <utility>

namespace cb::stats {
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