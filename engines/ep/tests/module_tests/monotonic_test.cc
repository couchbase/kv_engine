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

#include <folly/portability/GTest.h>

template <typename T>
class MonotonicTest : public ::testing::Test {
public:
    MonotonicTest() : initialValue(1), mono(1), large(100) {
    }

    void assign(T a) {
        mono = a;
    }

    const T initialValue;
    T mono;
    T large;
};

// Test both the Monotonic and AtomicMonotonic templates (with IgnorePolicy).
using MonotonicTypes = ::testing::Types<Monotonic<int, IgnorePolicy>,
                                        AtomicMonotonic<int, IgnorePolicy>>;
TYPED_TEST_SUITE(MonotonicTest, MonotonicTypes);

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

TYPED_TEST(MonotonicTest, PreIncrement) {
    EXPECT_EQ(2, ++this->mono);
}

TYPED_TEST(MonotonicTest, PostIncrement) {
    EXPECT_EQ(1, this->mono++);
    EXPECT_EQ(2, this->mono);
}

// Similar, except with ThrowExceptionPolicy.
template <typename T>
class ThrowingAllMonotonicTest : public MonotonicTest<T> {};

using AllMonotonicThrowingTypes =
        ::testing::Types<Monotonic<int, ThrowExceptionPolicy>,
                         AtomicMonotonic<int, ThrowExceptionPolicy>>;

TYPED_TEST_SUITE(ThrowingAllMonotonicTest, AllMonotonicThrowingTypes);

TYPED_TEST(ThrowingAllMonotonicTest, Increase) {
    this->mono = 2;
    EXPECT_EQ(2, this->mono);

    this->mono = 20;
    EXPECT_EQ(20, this->mono);
}

TYPED_TEST(ThrowingAllMonotonicTest, Identical) {
    EXPECT_THROW(this->mono = 1, std::logic_error);
}

TYPED_TEST(ThrowingAllMonotonicTest, Decrease) {
    EXPECT_THROW(this->mono = 0, std::logic_error);
}

TYPED_TEST(ThrowingAllMonotonicTest, Reset) {
    this->mono = 10;
    ASSERT_EQ(10, this->mono);
    this->mono.reset(5);
    EXPECT_EQ(5, this->mono);
}

TYPED_TEST(ThrowingAllMonotonicTest, PreIncrement) {
    EXPECT_EQ(2, ++this->mono);
}

TYPED_TEST(ThrowingAllMonotonicTest, PostIncrement) {
    EXPECT_EQ(1, this->mono++);
    EXPECT_EQ(2, this->mono);
}

// AtomicMonotonic deletes assignment, so this test is for Monotonic only
template <typename T>
class ThrowingMonotonicTest : public MonotonicTest<T> {};

using MonotonicThrowingTypes =
        ::testing::Types<Monotonic<int, ThrowExceptionPolicy>>;

TYPED_TEST_SUITE(ThrowingMonotonicTest, MonotonicThrowingTypes);

TYPED_TEST(ThrowingMonotonicTest, Identical) {
    EXPECT_THROW(this->mono = 1, std::logic_error);
    EXPECT_THROW(this->mono = this->mono, std::logic_error);
}

TYPED_TEST(ThrowingMonotonicTest, Decrease) {
    EXPECT_THROW(this->mono = 0, std::logic_error);
    EXPECT_THROW(this->large = this->mono, std::logic_error);
}

// Similar but testing WeaklyMonotonic (i.e. Identical is allowed).
template <typename T>
class WeaklyMonotonicTest : public MonotonicTest<T> {};

using WeaklyMonotonicTypes =
        ::testing::Types<WeaklyMonotonic<int, IgnorePolicy>,
                         AtomicWeaklyMonotonic<int, IgnorePolicy>,
                         WeaklyMonotonic<int, ThrowExceptionPolicy>,
                         AtomicWeaklyMonotonic<int, ThrowExceptionPolicy>>;

TYPED_TEST_SUITE(WeaklyMonotonicTest, WeaklyMonotonicTypes);

TYPED_TEST(WeaklyMonotonicTest, Identical) {
    EXPECT_NO_THROW(this->mono = 1);
    EXPECT_EQ(1, this->mono);
}

template <typename T>
class WeaklyMonotonicThrowTest : public MonotonicTest<T> {};

using WeaklyMonotonicThrowTypes =
        ::testing::Types<WeaklyMonotonic<int, ThrowExceptionPolicy>>;

TYPED_TEST_SUITE(WeaklyMonotonicThrowTest, WeaklyMonotonicThrowTypes);

TYPED_TEST(WeaklyMonotonicThrowTest, Decrease) {
    EXPECT_THROW(this->large = this->mono, std::logic_error);
    EXPECT_THROW(this->large = 0, std::logic_error);
}