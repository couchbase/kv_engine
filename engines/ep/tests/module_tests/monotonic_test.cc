/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "monotonic.h"

#include <gtest/gtest.h>

template <typename T>
class MonotonicTest : public ::testing::Test {
public:
    const T initialValue{1};
    T mono{1};
};

// Test both the Monotonic and AtomicMonotonic templates (with IgnorePolicy).
using MonotonicTypes = ::testing::Types<Monotonic<int, IgnorePolicy>,
                                        AtomicMonotonic<int, IgnorePolicy>>;
TYPED_TEST_CASE(MonotonicTest, MonotonicTypes);

TYPED_TEST(MonotonicTest, Increase) {
    this->mono = 2;
    EXPECT_EQ(2, this->mono);

    this->mono = 20;
    EXPECT_EQ(20, this->mono);
}

TYPED_TEST(MonotonicTest, Identical) {
    this->mono = 1;
    EXPECT_EQ(1, this->mono);
}

TYPED_TEST(MonotonicTest, Decrease) {
    this->mono = 0;
    EXPECT_EQ(this->initialValue, this->mono);
}

TYPED_TEST(MonotonicTest, Reset) {
    this->mono = 10;
    ASSERT_EQ(10, this->mono);
    this->mono.reset(5);
    EXPECT_EQ(5, this->mono);
}


// Similar, except with ThrowExceptionPolicy.
template <typename T>
class ThrowingMonotonicTest : public MonotonicTest<T> {};

using MonotonicThrowingTypes =
        ::testing::Types<Monotonic<int, ThrowExceptionPolicy>,
                         AtomicMonotonic<int, ThrowExceptionPolicy>>;

TYPED_TEST_CASE(ThrowingMonotonicTest, MonotonicThrowingTypes);

TYPED_TEST(ThrowingMonotonicTest, Increase) {
    this->mono = 2;
    EXPECT_EQ(2, this->mono);

    this->mono = 20;
    EXPECT_EQ(20, this->mono);
}

TYPED_TEST(ThrowingMonotonicTest, Identical) {
    EXPECT_THROW(this->mono = 1, std::logic_error);
}

TYPED_TEST(ThrowingMonotonicTest, Decrease) {
    EXPECT_THROW(this->mono = 0, std::logic_error);
}

TYPED_TEST(ThrowingMonotonicTest, Reset) {
    this->mono = 10;
    ASSERT_EQ(10, this->mono);
    this->mono.reset(5);
    EXPECT_EQ(5, this->mono);
}
