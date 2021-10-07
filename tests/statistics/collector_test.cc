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

#include "collector_test.h"

#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>

#include <string_view>

TEST_F(CollectorTest, FloatValueStringRepresentation) {
    // MB-48694: Moving to fmt led to float values being formatted to string
    // for CBStats differently to the prior behaviour.
    // This resulted from floats being converted to doubles before formatting.
    // Verify simple float values are formatted as expected

    using namespace testing;
    StrictMock<MockFunction<void(
            std::string_view key, std::string_view value, const void* ctx)>>
            mockAddStat;

    using namespace std::string_view_literals;
    auto key = "foobar"sv;

    InSequence s;
    EXPECT_CALL(mockAddStat, Call(key, "0.001"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.01"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.1"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.9"sv, _)).Times(1);

    CBStatCollector collector(mockAddStat.AsStdFunction(), nullptr);

    collector.addStat(key, 0.001f);
    collector.addStat(key, 0.01f);
    collector.addStat(key, 0.1f);
    collector.addStat(key, 0.9f);

    // MB-48694: This would fail, e.g., 0.01 would be represented as
    // 0.009999999776482582
    // It appears this derives from fmt::format's behaviour when called with
    // a double converted from a float.
}