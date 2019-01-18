/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
#include <iterator>
#include <list>

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
typedef std::list<testItem> ListContainer;
typedef CheckpointIterator<ListContainer> ListContainerIterator;

template <typename S>
CheckpointIterator<S> checkpointBegin(S& v) {
    return CheckpointIterator<S>(v, CheckpointIterator<S>::Position::begin);
}
template <typename S>
CheckpointIterator<S> checkpointEnd(S& v) {
    return CheckpointIterator<S>(v, CheckpointIterator<S>::Position::end);
}

ListContainerIterator listContainerBegin(ListContainer& c) {
    return checkpointBegin<ListContainer>(c);
}

ListContainerIterator listContainerEnd(ListContainer& c) {
    return checkpointEnd<ListContainer>(c);
}

// Test with an empty list.
TEST(CheckpointIteratorTest, emptyList) {
    ListContainer c;
    ASSERT_EQ(0, c.size());

    int iteratorCount = 0;
    for (auto cursor = listContainerBegin(c); cursor != listContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(c.size(), iteratorCount);
}

// Try to increment beyond end.
TEST(CheckpointIteratorTest, incrementBeyondEnd) {
    ListContainer c;
    ASSERT_EQ(0, c.size());
    auto cursor = listContainerEnd(c);

    ++cursor;
    auto compare = (listContainerEnd(c) == cursor);
    EXPECT_TRUE(compare);
}

// Try to decrement beyond begin.
TEST(CheckpointIteratorTest, decrementBeyondBegin) {
    ListContainer c;
    ASSERT_EQ(0, c.size());
    auto cursor = listContainerBegin(c);
    --cursor;
    auto compare = (listContainerBegin(c) == cursor);
    EXPECT_TRUE(compare);
}

// Iterate forwards with only a null element in the list.
TEST(CheckpointIteratorTest, nullFirstElementIterateForwards) {
    ListContainer c;
    c.push_back(nullptr);
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = listContainerBegin(c); cursor != listContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(0, iteratorCount);
}

// Iterate backwards with only a null element in the list.
TEST(CheckpointIteratorTest, nullFirstElementIterateBackwards) {
    ListContainer c;
    c.push_back(nullptr);
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = listContainerEnd(c); cursor != listContainerBegin(c);
         --cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(0, iteratorCount);
}

// Iterate forwards with only a non-null element in the list.
TEST(CheckpointIteratorTest, nonNullFirstElementIterateForwards) {
    ListContainer c;
    c.push_back(std::make_unique<MyInt>(1));
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = listContainerBegin(c); cursor != listContainerEnd(c);
         ++cursor) {
        ++iteratorCount;
    }
    EXPECT_EQ(1, iteratorCount);
}

// Iterate backwards with only a non-null element in the list.
TEST(CheckpointIteratorTest, nonNullFirstElementIterateBackwards) {
    ListContainer c;
    c.push_back(std::make_unique<MyInt>(1));
    ASSERT_EQ(1, c.size());

    int iteratorCount = 0;
    for (auto cursor = listContainerEnd(c); cursor != listContainerBegin(c);
         --cursor) {
        ++iteratorCount;
    };

    EXPECT_EQ(1, iteratorCount);
}

// Iterate forwards with mixture of non-null and null elements in the list.
TEST(CheckpointIteratorTest, forwardsNullAndNonNullElements) {
    ListContainer c;
    // Populate with 3 elements, one of which is pointing to null
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = listContainerBegin(c); cursor != listContainerEnd(c);
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

// Iterate forwards with mixture of non-null and null elements in the list,
// but time with first element pointing to null.
TEST(CheckpointIteratorTest, forwardsNullAndNonNullElementsWithFirstNull) {
    ListContainer c;
    // Populate with 3 elements, with first element pointing to null.
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = listContainerBegin(c); cursor != listContainerEnd(c);
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

// Iterate backwards with mixture of non-null and null elements in the list.
TEST(CheckpointIteratorTest, backwardsNullAndNonNullElements) {
    ListContainer c;
    // Populate with 3 elements, one of which is pointing to null
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = listContainerEnd(c); cursor != listContainerBegin(c);
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

// Iterate backwards with mixture of non-null and null elements in the list,
// but time with first element pointing to null.
TEST(CheckpointIteratorTest, backwardsNullAndNonNullElementsWithFirstNull) {
    ListContainer c;
    // Populate with 3 elements, with first element pointing to null.
    c.push_back(nullptr);
    c.push_back(std::make_unique<MyInt>(1));
    c.push_back(std::make_unique<MyInt>(2));
    ASSERT_EQ(3, c.size());

    // Should only find the 2 non-null elements
    int iteratorCount = 0;
    for (auto cursor = listContainerEnd(c); cursor != listContainerBegin(c);
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
