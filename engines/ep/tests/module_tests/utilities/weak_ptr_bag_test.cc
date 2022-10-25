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

#include "utilities/weak_ptr_bag.h"

#include <folly/portability/GTest.h>

/**
 * Check that storing in the bag does not strongly reference objects.
 */
TEST(WeakPtrBagTest, BagReferencesWeakly) {
    WeakPtrBag<int> bag;
    auto n1 = std::make_shared<int>();

    bag.push(n1);

    ASSERT_TRUE(n1.unique());
    ASSERT_EQ(1, bag.getNonExpired().size());
}

TEST(WeakPtrBagTest, CompactRemovesExpiredItems) {
    WeakPtrBag<int> bag;
    auto n1 = std::make_shared<int>();
    bag.push(n1);
    auto n2 = std::make_shared<int>();
    bag.push(n2);

    n1.reset();

    auto removedCount = bag.compact();
    ASSERT_EQ(1, removedCount);
}

/**
 * Add two items and check that we can get them back
 */
TEST(WeakPtrBagTest, Push) {
    WeakPtrBag<int> bag;
    auto n1 = std::make_shared<int>();
    bag.push(n1);
    auto n2 = std::make_shared<int>();
    bag.push(n2);

    auto nonExpired = bag.getNonExpired();
    ASSERT_EQ(2, nonExpired.size());
    ASSERT_NE(std::find(nonExpired.begin(), nonExpired.end(), n1),
              nonExpired.end());
    ASSERT_NE(std::find(nonExpired.begin(), nonExpired.end(), n2),
              nonExpired.end());
}

/**
 * Add two items, expire one and check that we only get the other one back.
 */
TEST(WeakPtrBagTest, PushAndExpire) {
    WeakPtrBag<int> bag;
    auto n1 = std::make_shared<int>();
    bag.push(n1);
    auto n2 = std::make_shared<int>();
    bag.push(n2);

    // Expire one of the items
    n1.reset();

    auto nonExpired = bag.getNonExpired();
    ASSERT_EQ(1, nonExpired.size());
    ASSERT_EQ(nonExpired[0], n2);
}