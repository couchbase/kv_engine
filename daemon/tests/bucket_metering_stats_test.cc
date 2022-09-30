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

#include <daemon/buckets.h>
#include <daemon/stats.h>
#include <fmt/format.h>
#include <statistics/labelled_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>

class BucketMeteringStatsTest : public ::testing::Test {
protected:
    void SetUp() override {
        bucket = &BucketManager::instance().at(0);
    }

    Bucket* bucket;
};

MATCHER_P(StatDefNameMatcher,
          expectedName,
          fmt::format("Check the name of the StatDef matches '{}'",
                      expectedName)) {
    return arg.cbstatsKey.key == expectedName;
}

TEST_F(BucketMeteringStatsTest, CollectInitialMeteringStats) {
    using namespace ::testing;
    using namespace std::literals::string_view_literals;

    std::string_view bucketName = bucket->name;

    NiceMock<MockStatCollector> collector;
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("op_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        ElementsAre(Pair("bucket"sv, bucketName))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_ru_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_wu_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("meter_cu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_ru_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_wu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("credit_cu_total"),
                        Matcher<int64_t>(int64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("reject_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("throttle_count_total"),
                        Matcher<uint64_t>(uint64_t(0)),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("throttle_seconds_total"),
                        Matcher<double>(0.),
                        UnorderedElementsAre(Pair("bucket"sv, bucketName),
                                             Pair("for"sv, "kv"))));

    bucket->addMeteringMetrics(collector.forBucket(bucketName));
}