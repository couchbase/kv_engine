/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include "test_helpers.h"
#include "vb_filter.h"

#include <sstream>

class VBucketFilterTest : public ::testing::Test {};

TEST_F(VBucketFilterTest, DefaultIsMatchAll) {
    VBucketFilter filter = VBucketFilter::createMatchAll();
    EXPECT_TRUE(filter.isMatchAll());
    for (Vbid::id_type i = 0; i < 1024; i++) {
        EXPECT_TRUE(filter(Vbid(i)));
    }
}

TEST_F(VBucketFilterTest, ExplicitEmptySetIsMatchNone) {
    VBucketFilter filter = VBucketFilter::create(std::set<Vbid>{});
    EXPECT_FALSE(filter.isMatchAll());
    for (Vbid::id_type i = 0; i < 1024; i++) {
        EXPECT_FALSE(filter(Vbid(i)));
    }
}

TEST_F(VBucketFilterTest, StreamOutput) {
    auto str = [](const VBucketFilter& f) {
        std::ostringstream oss;
        oss << f;
        return oss.str();
    };

    EXPECT_EQ("{ match-all }", str(VBucketFilter::createMatchAll()));
    EXPECT_EQ("{ empty }", str(VBucketFilter::create(std::set<Vbid>{})));
    EXPECT_EQ("{ vb:5 }",
              str(VBucketFilter::create(std::vector<Vbid>{Vbid(5)})));
    // Two consecutive items are printed individually (range requires 3+).
    EXPECT_EQ("{ vb:0, vb:1 }",
              str(VBucketFilter::create(std::vector<Vbid>{Vbid(0), Vbid(1)})));
    // Three or more consecutive items are compressed into a range.
    EXPECT_EQ("{ [vb:0,vb:2] }",
              str(VBucketFilter::create(
                      std::vector<Vbid>{Vbid(0), Vbid(1), Vbid(2)})));
    // Range followed by a non-consecutive item.
    EXPECT_EQ("{ [vb:0,vb:2], vb:5 }",
              str(VBucketFilter::create(
                      std::vector<Vbid>{Vbid(0), Vbid(1), Vbid(2), Vbid(5)})));
}

TEST_F(VBucketFilterTest, EqualityConsidersMatchAll) {
    VBucketFilter matchAll = VBucketFilter::createMatchAll();
    VBucketFilter matchNone = VBucketFilter::create(std::set<Vbid>{});
    EXPECT_NE(matchAll, matchNone);
    EXPECT_EQ(matchAll, VBucketFilter::createMatchAll());
}

TEST_F(VBucketFilterTest, Slice) {
    std::vector<Vbid> vbids;
    for (Vbid::id_type i = 0; i < 10; i++) {
        vbids.emplace_back(i);
    }
    VBucketFilter filter = VBucketFilter::create(std::move(vbids));
    EXPECT_EQ(10u, filter.slice(0, 1).getVBSet().size());
    std::set<Vbid> slice0stride3 = {Vbid(0), Vbid(3), Vbid(6), Vbid(9)};
    EXPECT_EQ(slice0stride3, filter.slice(0, 3).getVBSet());
    std::set<Vbid> slice1stride3 = {Vbid(1), Vbid(4), Vbid(7)};
    EXPECT_EQ(slice1stride3, filter.slice(1, 3).getVBSet());
    std::set<Vbid> slice2stride3 = {Vbid(2), Vbid(5), Vbid(8)};
    EXPECT_EQ(slice2stride3, filter.slice(2, 3).getVBSet());
}

// Confirm that splitting a filter into several disjoint filters works as
// expected. Used when creating multiple PagingVisitors
TEST_F(VBucketFilterTest, Split) {
    const VBucketFilter filter = VBucketFilter::create(
            std::vector<Vbid>{Vbid(0), Vbid(1), Vbid(2), Vbid(3)});

    using namespace testing;
    {
        CB_SCOPED_TRACE("Identity");
        auto filters = filter.split(1);
        EXPECT_THAT(filters, SizeIs(1));
        EXPECT_TRUE(filter == filters.at(0));
    }

    {
        CB_SCOPED_TRACE("Split N");
        // Expected: {0}, {1}, {2}, {3}
        auto filters = filter.split(4);
        EXPECT_THAT(filters, SizeIs(4));
        for (Vbid::id_type i = 0; i < 4; ++i) {
            EXPECT_THAT(filters.at(i).getVBSet(), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
    }

    {
        CB_SCOPED_TRACE("Split >N");
        // Expected: {0}, {1}, {2}, {3}
        auto filters = filter.split(5);
        // Never return more filters than there are vBuckets.
        EXPECT_THAT(filters, SizeIs(4));
        for (Vbid::id_type i = 0; i < 4; ++i) {
            EXPECT_THAT(filters.at(i).getVBSet(), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
    }

    {
        CB_SCOPED_TRACE("Split <N");
        // Expected: {0, 3}, {1}, {2}
        auto filters = filter.split(3);
        EXPECT_THAT(filters, SizeIs(3));
        // round robin means first filter has more items

        EXPECT_THAT(filters.at(0).getVBSet(), SizeIs(2));
        EXPECT_TRUE(filters.at(0)(Vbid(0)));
        EXPECT_TRUE(filters.at(0)(Vbid(3)));

        for (Vbid::id_type i = 1; i < 3; ++i) {
            EXPECT_THAT(filters.at(i).getVBSet(), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
    }
}
