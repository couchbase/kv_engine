/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "vb_filter.h"

class VBucketFilterTest : public ::testing::Test {};

TEST_F(VBucketFilterTest, Slice) {
    VBucketFilter filter;
    for (int i = 0; i < 10; i++) {
        filter.addVBucket(Vbid(i));
    }

    EXPECT_EQ(10, filter.slice(0, 1).size());
    std::set<Vbid> slice0stride3 = {Vbid(0), Vbid(3), Vbid(6), Vbid(9)};
    EXPECT_EQ(slice0stride3, filter.slice(0, 3).getVBSet());
    std::set<Vbid> slice1stride3 = {Vbid(1), Vbid(4), Vbid(7)};
    EXPECT_EQ(slice1stride3, filter.slice(1, 3).getVBSet());
    std::set<Vbid> slice2stride3 = {Vbid(2), Vbid(5), Vbid(8)};
    EXPECT_EQ(slice2stride3, filter.slice(2, 3).getVBSet());
}
