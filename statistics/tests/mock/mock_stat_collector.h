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

#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <statistics/collector.h>

#include <gmock/gmock.h>

/**
 * Mock StatCollector which by default does nothing when addStat is called.
 */
class MockStatCollector : public StatCollector {
public:
    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k, bool v, const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k, int64_t v, const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k, uint64_t v, const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k, float v, const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k, double v, const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels),
                (const, override));
    MOCK_METHOD(void,
                addStat,
                (const cb::stats::StatDef& k,
                 const HdrHistogram& hist,
                 const Labels& labels),
                (const, override));

    MOCK_METHOD(bool, includeAggregateMetrics, (), (const, override));

    MOCK_METHOD(cb::engine_errc,
                testPrivilegeForStat,
                (std::optional<ScopeID> sid, std::optional<CollectionID> cid),
                (const, override));
};