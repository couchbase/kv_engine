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

#include <utilities/memory_tracking_allocator.h>

#include <folly/portability/GTest.h>

#include <deque>
#include <list>

/*
 * Unit tests for the MemoryTrackingAllocator class.
 */

typedef std::list<int, MemoryTrackingAllocator<int>> List;
typedef std::deque<int, MemoryTrackingAllocator<int>> Deque;

class MemoryTrackingAllocatorListTest : public ::testing::Test {
protected:
    MemoryTrackingAllocatorListTest() : theList(allocator) {
    }

    void SetUp() override {
#if WIN32
        // On windows for an empty list we still allocate space for
        // containing one element.
        extra = perElementOverhead;
#if _DEBUG
        // additional 16 bytes overhead in an empty list with Debug CRT.
        extra += 16;
#endif
#endif
    }

    MemoryTrackingAllocator<int> allocator;
    List theList;
    size_t extra = 0;
    const size_t perElementOverhead = 3 * sizeof(uintptr_t);
};

// Test empty List
TEST_F(MemoryTrackingAllocatorListTest, initialValueForList) {
    EXPECT_EQ(extra + 0, theList.get_allocator().getBytesAllocated());
}

// Test adding single int to List
TEST_F(MemoryTrackingAllocatorListTest, addElementToList) {
    theList.push_back(1);
    EXPECT_EQ(extra + perElementOverhead,
              theList.get_allocator().getBytesAllocated());
    theList.clear();
    EXPECT_EQ(extra + 0, theList.get_allocator().getBytesAllocated());
}

// Test adding 4096 ints to List.
TEST_F(MemoryTrackingAllocatorListTest, addManyElementsToList) {
    for (int ii = 0; ii < 4096; ++ii) {
        theList.push_back(ii);
    }
    EXPECT_EQ(extra + (perElementOverhead * 4096),
              theList.get_allocator().getBytesAllocated());
    theList.clear();
    EXPECT_EQ(extra + 0, theList.get_allocator().getBytesAllocated());
}

// Test bytesAllocates is correct when a re-bind occurs.
TEST(MemoryTrackingAllocatorTest, rebindTest) {
    MemoryTrackingAllocator<int> allocator;
    // Create deque passing in the allocator
    Deque correctlyAllocatedDeque(allocator);
    // Create deque using the default constructor
    Deque notCorrectlyAllocatedDeque;

    // Add items to both deques
    correctlyAllocatedDeque.push_back(1);
    notCorrectlyAllocatedDeque.push_back(1);

    auto correctlyAllocatedSize =
            correctlyAllocatedDeque.get_allocator().getBytesAllocated();
    auto notCorrectlyAllocatedSize =
            notCorrectlyAllocatedDeque.get_allocator().getBytesAllocated();
#ifdef _LIBCPP_VERSION
    // When using libc++ the correctly allocated deque will be larger
    // because it combines the size allocated for metadata and the size
    // allocated for the data items only when the allocator is passed into
    // the constructor.
    EXPECT_LT(notCorrectlyAllocatedSize, correctlyAllocatedSize);
#else
    // For libstdc++ it combines the size allocated for metadata and the
    // size allocated for the data items when using the default constructor
    // as it copies the original allocator.
    EXPECT_EQ(notCorrectlyAllocatedSize, correctlyAllocatedSize);
#endif
}

// Test bytesAllocated is correct when a copy occurs.
TEST(MemoryTrackingAllocatorTest, copyTest) {
    MemoryTrackingAllocator<int> allocator;
    Deque theDeque(allocator);
    theDeque.push_back(0);
    auto theDequeSize = theDeque.get_allocator().getBytesAllocated();

    // Copy the deque.
    Deque copy = theDeque;
    auto copySize = copy.get_allocator().getBytesAllocated();
    EXPECT_EQ(theDequeSize, copySize);

    // Add a further 4095 items to the deque - which will cause a resize
    for (int ii = 1; ii < 4096; ++ii) {
        theDeque.push_back(ii);
    }

    auto newTheDequeSize = theDeque.get_allocator().getBytesAllocated();
    auto newCopySize = copy.get_allocator().getBytesAllocated();
    // The original deque should have increased in size.
    EXPECT_LT(theDequeSize, newTheDequeSize);
    // The copied deque should not have changed in size.
    EXPECT_EQ(copySize, newCopySize);
}
