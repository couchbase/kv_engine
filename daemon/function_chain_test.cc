/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "function_chain.h"
#include <folly/portability/GTest.h>

static int doubleInput(int input) {
    return input * 2;
}

static int iCountMyCalls() {
    static int called = 0;
    return ++called;
}

static bool function1Called = false;
static bool function2Called = false;
static bool function3Called = false;

const int success = 5;

static int function1() {
    function1Called = true;
    return success;
}

static int function2() {
    function2Called = true;
    return success;
}

static int function3() {
    function3Called = true;
    return success;
}

int getSuccessValue() {
    return success;
}
/*
 * An empty chain should return the defined Success value.
 */
TEST(FunctionChainTest, EmptyChain) {
    FunctionChain<int, getSuccessValue, int> chain;
    ASSERT_EQ(success, chain.invoke(0));
}

/*
 * Check the chain invokes the pushed function.
 */
TEST(FunctionChainTest, SingleChain) {
    FunctionChain<int, getSuccessValue, int> chain;
    chain.push_unique(makeFunction<int, getSuccessValue, int>(doubleInput));
    // check return val is double input
    int input = 102;
    ASSERT_EQ(input * 2, chain.invoke(input));
}

/*
 * Check the chain push_unique ignores subsequent pushes of a function.
 */
TEST(FunctionChainTest, Uniqueness) {
    FunctionChain<int, getSuccessValue> chain;

    for (int ii = 0; ii < 6; ii++) {
        chain.push_unique(makeFunction<int, getSuccessValue>(iCountMyCalls));
    }

    ASSERT_EQ(1, iCountMyCalls()); // manually invoke returns 1
    ASSERT_EQ(2, chain.invoke()); // should now be 2
    ASSERT_EQ(3, iCountMyCalls()); // manually invoke now returns 3
}

/*
 * Check the chain calls many functions.
 */
TEST(FunctionChainTest, Chain) {
    FunctionChain<int, getSuccessValue> chain;

    chain.push_unique(makeFunction<int, getSuccessValue>(function1));
    chain.push_unique(makeFunction<int, getSuccessValue>(function2));
    chain.push_unique(makeFunction<int, getSuccessValue>(function3));
    ASSERT_FALSE(function1Called);
    ASSERT_FALSE(function2Called);
    ASSERT_FALSE(function3Called);
    ASSERT_EQ(success, chain.invoke());
    ASSERT_TRUE(function1Called);
    ASSERT_TRUE(function2Called);
    ASSERT_TRUE(function3Called);
}
