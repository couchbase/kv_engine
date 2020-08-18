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
                (std::string_view k, std::string_view v, const Labels& labels),
                (override));
    MOCK_METHOD(void,
                addStat,
                (std::string_view k, bool v, const Labels& labels),
                (override));
    MOCK_METHOD(void,
                addStat,
                (std::string_view k, int64_t v, const Labels& labels),
                (override));
    MOCK_METHOD(void,
                addStat,
                (std::string_view k, uint64_t v, const Labels& labels),
                (override));
    MOCK_METHOD(void,
                addStat,
                (std::string_view k, double v, const Labels& labels),
                (override));
    MOCK_METHOD(void,
                addStat,
                (std::string_view k,
                 const HistogramData& hist,
                 const Labels& labels),
                (override));
};