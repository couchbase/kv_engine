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
#include <memory>

#include "packed_ptr.h"

class PackedPtrTest : public ::testing::Test {};

TEST_F(PackedPtrTest, CanSetValue) {
    PackedPtr<int> p;
    p = nullptr;
    (void)p;
    int x;
    p = &x;
    EXPECT_EQ(&x, p);
}

TEST_F(PackedPtrTest, CanConstructFromPtr) {
    PackedPtr<int> p(nullptr);
    int x;
    PackedPtr<int> p2(&x);
    EXPECT_EQ(&x, p2);
}

TEST_F(PackedPtrTest, CanGetValue) {
    PackedPtr<int> p(nullptr);
    int* vp = p;
    EXPECT_EQ(vp, p);
}

TEST_F(PackedPtrTest, CanAccessAsPointer) {
    int x;
    PackedPtr<int> p(&x);
    *p;
    int* p2 = p.operator->();
    EXPECT_EQ(&x, p2);
}

TEST_F(PackedPtrTest, DeleterWorksWithUniquePtr) {
    std::unique_ptr<int, PackedPtrDeleter<int, std::default_delete<int>>> p;
    static_assert(sizeof(p) == sizeof(PackedPtr<int>));
}
