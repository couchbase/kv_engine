/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "atomic.h"

#include "checkpoint_iterator.h"

#include <folly/portability/GTest.h>
#include <iterator>
#include <list>

/*
 * Unit tests for the CheckpointIterator
 */

class MyInt : public RCValue {
public:
    explicit MyInt(int v) : value(v) {
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

ListContainerIterator listContainerBegin(ListContainer& c) {
    return ListContainerIterator(c, ListContainerIterator::Position::begin);
}

ListContainerIterator listContainerEnd(ListContainer& c) {
    return ListContainerIterator(c, ListContainerIterator::Position::end);
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
#if _ITERATOR_DEBUG_LEVEL == 0
// Windowsiterator debug throws an error below when we increment an iterator
// past end of container so skip this test if debug iterators enabled.
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
#endif // if _ITERATOR_DEBUG_LEVEL == 0

// Iterate forwards with only a null element in the list.
TEST(CheckpointIteratorTest, nullFirstElementIterateForwards) {
    ListContainer c;
    c.emplace_back();
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
    c.emplace_back();
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
    c.emplace_back(new MyInt(1));
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
    c.emplace_back(new MyInt(1));
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
    c.emplace_back(new MyInt(1));
    c.emplace_back();
    c.emplace_back(new MyInt(2));
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
    c.emplace_back();
    c.emplace_back(new MyInt(1));
    c.emplace_back(new MyInt(2));
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
    c.emplace_back(new MyInt(1));
    c.emplace_back();
    c.emplace_back(new MyInt(2));
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
    c.emplace_back();
    c.emplace_back(new MyInt(1));
    c.emplace_back(new MyInt(2));
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
