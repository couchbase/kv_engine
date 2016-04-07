/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <daemon/function_chain.h>
#include <gtest/gtest.h>

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

static int function1() {
    function1Called = true;
    return 0;
}

static int function2() {
    function2Called = true;
    return 0;
}

static int function3() {
    function3Called = true;
    return 0;
}

/*
 * An empty chain should return the defined Success value.
 */
TEST(FunctionChainTest, EmptyChain) {
    FunctionChain<int, 5, int> chain;
    ASSERT_EQ(5, chain.invoke(0));
}

/*
 * Check the chain invokes the pushed function.
 */
TEST(FunctionChainTest, SingleChain) {
    FunctionChain<int, 5, int> chain;
    chain.push_unique(makeFunction<int, 5, int>(doubleInput));
    // check return val is double input
    int input = 102;
    ASSERT_EQ(input*2, chain.invoke(input));
}

/*
 * Check the chain push_unique ignores subsequent pushes of a function.
 */
TEST(FunctionChainTest, Uniqueness) {
    FunctionChain<int, 1> chain;

    for(int ii = 0; ii < 6; ii++) {
        chain.push_unique(makeFunction<int, 1>(iCountMyCalls));
    }

    ASSERT_EQ(1, iCountMyCalls()); // manually invoke returns 1
    ASSERT_EQ(2, chain.invoke());  // should now be 2
    ASSERT_EQ(3, iCountMyCalls()); // manually invoke now returns 3
}

/*
 * Check the chain calls many functions.
 */
TEST(FunctionChainTest, Chain) {
    FunctionChain<int, 0> chain;

    chain.push_unique(makeFunction<int, 0>(function1));
    chain.push_unique(makeFunction<int, 0>(function2));
    chain.push_unique(makeFunction<int, 0>(function3));
    ASSERT_FALSE(function1Called);
    ASSERT_FALSE(function2Called);
    ASSERT_FALSE(function3Called);
    ASSERT_EQ(0, chain.invoke());
    ASSERT_TRUE(function1Called);
    ASSERT_TRUE(function2Called);
    ASSERT_TRUE(function3Called);
}