/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "config.h"
#include "atomic.h"

#include "checkpoint_iterator.h"

#include <gtest/gtest.h>
#include <deque>
#include <iterator>

/*
 * Unit tests for the CheckpointIterator
 */

class MyInt : public RCValue {
public:
    MyInt(int v) : value(v) {
    }

    int getValue() {
        return value;
    }

private:
    int value;
};

typedef SingleThreadedRCPtr<MyInt> testItem;
typedef std::deque<testItem> DequeContainer;
typedef CheckpointIterator<DequeContainer> DequeContainerIterator;

template <typename S>
CheckpointIterator<S> checkpointBegin(S& v) {
    return CheckpointIterator<S>(v, 0);
}
template <typename S>
CheckpointIterator<S> checkpointEnd(S& v) {
    return CheckpointIterator<S>(v, v.size());
}

DequeContainerIterator dequeContainerBegin(DequeContainer& c) {
    return checkpointBegin<DequeContainer>(c);
}

DequeContainerIterator dequeContainerEnd(DequeContainer& c) {
    return checkpointEnd<DequeContainer>(c);
}

// Test with an empty deque.
TEST(CheckpointIteratorTest, emptyDeque) {
    DequeContainer c;
    ASSERT_EQ(0, c.size());

    int iteratorCount = 0;
    for (auto cursor = dequeContainerBegin(c); cursor != dequeContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(c.size(), iteratorCount);
}

// Iterate forwards with only a null element in the deque.
TEST(CheckpointIteratorTest, nullFirstElementIterateForwards) {
    DequeContainer c;
    c.push_back(nullptr);
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = dequeContainerBegin(c); cursor != dequeContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(0, iteratorCount);
}

// Iterate backwards with only a null element in the deque.
TEST(CheckpointIteratorTest, nullFirstElementIterateBackwards) {
    DequeContainer c;
    c.push_back(nullptr);
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = dequeContainerEnd(c); cursor != dequeContainerBegin(c);
         --cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(0, iteratorCount);
}

// Iterate forwards with only a non-null element in the deque.
TEST(CheckpointIteratorTest, nonNullFirstElementIterateForwards) {
    DequeContainer c;
    c.push_back(std::make_unique<MyInt>(1));
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = dequeContainerBegin(c); cursor != dequeContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(1, iteratorCount);
}

// Iterate backwards with only a non-null element in the deque.
TEST(CheckpointIteratorTest, nonNullFirstElementIterateBackwards) {
    DequeContainer c;
    c.push_back(std::make_unique<MyInt>(1));
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = dequeContainerEnd(c); cursor != dequeContainerBegin(c);
         --cursor) {
        ++iteratorCount;
    };

    EXPECT_EQ(1, iteratorCount);
}

// Iterate forwards with mixture of non-null and null elements in the deque.
TEST(CheckpointIteratorTest, forwardsNullAndNonNullElements) {
    DequeContainer c;
    // Populate with 3 elements, one of which is pointing to null
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = dequeContainerBegin(c); cursor != dequeContainerEnd(c);
         ++cursor) {
        switch (iteratorCount) {
        case 0: {
            EXPECT_EQ(1, (*cursor)->getValue());
            break;
        }
        case 1: {
            EXPECT_EQ(2, (*cursor)->getValue());
            break;
        }
        default:
            FAIL() << "Should not reach default";
        }

        ++iteratorCount;
    }
    EXPECT_EQ(2, iteratorCount);
}

// Iterate forwards with mixture of non-null and null elements in the deque,
// but this time with first element pointing to null.
TEST(CheckpointIteratorTest, forwardsNullAndNonNullElementsWithFirstNull) {
    DequeContainer c;
    // Populate with 3 elements, with first element pointing to null.
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = dequeContainerBegin(c); cursor != dequeContainerEnd(c);
         ++cursor) {
        switch (iteratorCount) {
        case 0: {
            EXPECT_EQ(1, (*cursor)->getValue());
            break;
        }
        case 1: {
            EXPECT_EQ(2, (*cursor)->getValue());
            break;
        }
        default:
            FAIL() << "Should not reach default";
        }

        ++iteratorCount;
    }
    EXPECT_EQ(2, iteratorCount);
}

// Iterate backwards with mixture of non-null and null elements in the deque.
TEST(CheckpointIteratorTest, backwardsNullAndNonNullElements) {
    DequeContainer c;
    // Populate with 3 elements, one of which is pointing to null
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = dequeContainerEnd(c); cursor != dequeContainerBegin(c);
         --cursor) {
        // Because end is not valid we have to read the previous cursor
        auto previous = cursor;
        --previous;
        switch (iteratorCount) {
        case 0: {
            EXPECT_EQ(2, (*previous)->getValue());
            break;
        }
        case 1: {
            EXPECT_EQ(1, (*previous)->getValue());
            break;
        }
        default:
            FAIL() << "Should not reach default";
        }

        ++iteratorCount;
    };
    EXPECT_EQ(2, iteratorCount);
}

// Iterate backwards with mixture of non-null and null elements in the deque,
// but this time with first element pointing to null.
TEST(CheckpointIteratorTest, backwardsNullAndNonNullElementsWithFirstNull) {
    DequeContainer c;
    // Populate with 3 elements, with first element pointing to null.
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = dequeContainerEnd(c); cursor != dequeContainerBegin(c);
         --cursor) {
        // Because end is not valid we have to read the previous cursor
        auto previous = cursor;
        --previous;
        switch (iteratorCount) {
        case 0: {
            EXPECT_EQ(2, (*previous)->getValue());
            break;
        }
        case 1: {
            EXPECT_EQ(1, (*previous)->getValue());
            break;
        }
        default:
            FAIL() << "Should not reach default";
        }

        ++iteratorCount;
    };
    EXPECT_EQ(2, iteratorCount);
}
