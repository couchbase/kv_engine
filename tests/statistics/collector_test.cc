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

#include <programs/engine_testapp/mock_cookie.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>

#include <string_view>

TEST_F(CollectorTest, FloatValueStringRepresentation) {
    // MB-48694: Moving to fmt led to float values being formatted to string
    // for CBStats differently to the prior behaviour.
    // This resulted from floats being converted to doubles before formatting.
    // Verify simple float values are formatted as expected

    using namespace testing;
    StrictMock<MockFunction<void(
            std::string_view key, std::string_view value, CookieIface & ctx)>>
            mockAddStat;

    using namespace std::string_view_literals;
    auto key = "foobar"sv;

    InSequence s;
    EXPECT_CALL(mockAddStat, Call(key, "0.001"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.01"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.1"sv, _)).Times(1);
    EXPECT_CALL(mockAddStat, Call(key, "0.9"sv, _)).Times(1);

    MockCookie cookie;
    CBStatCollector collector(mockAddStat.AsStdFunction(), cookie);

    collector.addStat(key, 0.001f);
    collector.addStat(key, 0.01f);
    collector.addStat(key, 0.1f);
    collector.addStat(key, 0.9f);

    // MB-48694: This would fail, e.g., 0.01 would be represented as
    // 0.009999999776482582
    // It appears this derives from fmt::format's behaviour when called with
    // a double converted from a float.
}

TEST_F(CollectorTest, LabelledCollector) {
    // Verify collector.withLabels() can be used to create a collector
    // which will add the provided labels for all uses of addStat

    using namespace testing;
    StrictMock<MockStatCollector> collector;

    using namespace std::string_view_literals;
    auto labelled = collector.withLabels({{"foo", "bar"}});

    auto key1 = "apples"sv;
    auto key2 = "oranges"sv;

    InSequence s;
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(0.0),
                        UnorderedElementsAre(Pair("foo"sv, "bar"sv))))
            .Times(1);
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(1.0),
                        UnorderedElementsAre(Pair("foo"sv, "bar"sv))))
            .Times(1);

    labelled.addStat(key1, 0.0);
    labelled.addStat(key2, 1.0);

    // and test that further labels can be added by chaining another call to
    // withLabels

    auto moreLabelled = labelled.withLabels({{"baz", "qux"}});

    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(0.0),
                        UnorderedElementsAre(Pair("foo"sv, "bar"sv),
                                             Pair("baz"sv, "qux"sv))))
            .Times(1);
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(1.0),
                        UnorderedElementsAre(Pair("foo"sv, "bar"sv),
                                             Pair("baz"sv, "qux"sv))))
            .Times(1);

    moreLabelled.addStat(key1, 0.0);
    moreLabelled.addStat(key2, 1.0);
}
