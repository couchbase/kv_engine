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
#include "atomic_unordered_map.h"

#include <folly/portability/GTest.h>
#include <platform/atomic.h>
#include <thread>

/* Test class which inherits from RCValue */
struct DummyValue : public RCValue {
    explicit DummyValue(size_t value_) : value(value_) {
    }

    bool operator==(const DummyValue& other) const {
        return value == other.value;
    }

    size_t value;
};

class AtomicUnorderedMapTest : public ::testing::Test {
public:
    using TestMap = AtomicUnorderedMap<int, SingleThreadedRCPtr<DummyValue>>;

    // Add N items to a map starting from the given offset.
    static void insert_into_map(TestMap& map, size_t n, size_t offset) {
        for (unsigned int ii = 0; ii < n; ii++) {
            TestMap::mapped_type val{new DummyValue(ii * 10)};
            map.insert({offset + ii, std::move(val)});
        }
    }

protected:
    TestMap map;
};

/* Basic functionality sanity checks. */

TEST_F(AtomicUnorderedMapTest, Empty) {
    ASSERT_EQ(0u, map.size());
    EXPECT_FALSE(map.find(0).second) << "Should start with empty map";
}

TEST_F(AtomicUnorderedMapTest, InsertOne) {
    TestMap::mapped_type ptr{new DummyValue(10)};
    map.insert({0, ptr});

    EXPECT_EQ(1, map.size());
    EXPECT_EQ(ptr, map.find(0).first);
}

TEST_F(AtomicUnorderedMapTest, ReplaceOne) {

    TestMap::mapped_type ptr{new DummyValue(10)};
    TestMap::mapped_type ptr2{new DummyValue(20)};

    EXPECT_TRUE(map.insert({0, ptr}));
    EXPECT_TRUE(map.insert({1, ptr2}));
    EXPECT_EQ(2, map.size()) << "Adding another item should succeed";
    EXPECT_EQ(ptr2, map.find(1).first);

    TestMap::mapped_type ptr3{new DummyValue(30)};
    EXPECT_FALSE(map.insert({1, ptr3}))
        << "Inserting a key which already exists should fail";
    auto erased1 = map.erase(1);
    EXPECT_TRUE(erased1.second) << "Erasing key 1 should succeed";
    EXPECT_EQ(ptr2, erased1.first) << "Erasing key 1 should return value 1";

    EXPECT_TRUE(map.insert({1, ptr3}))
        << "Inserting a key which has been erased should succeed";
    EXPECT_EQ(2, map.size()) << "Replacing an item should keep size the same";
    EXPECT_EQ(ptr3, map.find(1).first);

    auto erased = map.erase(0);
    EXPECT_TRUE(erased.second) << "Failed to erase key 0";
    EXPECT_EQ(ptr, erased.first) << "Erasing key 0 should return value 0";

    map.clear();
    EXPECT_EQ(0, map.size()) << "Clearing map should remove all items";
    EXPECT_FALSE(map.find(0).second) << "Should end with empty map";
}

// Test that performing concurrent, disjoint insert (different keys) is thread-safe.
TEST_F(AtomicUnorderedMapTest, ConcurrentDisjointInsert) {
    // Add 10 elements from two threads, with the second starting from offset 10.
    const size_t n_elements{10};
    std::thread t1{insert_into_map, std::ref(map), n_elements, 0};
    std::thread t2{insert_into_map, std::ref(map), n_elements, n_elements};
    t1.join();
    t2.join();

    EXPECT_EQ(n_elements * 2, map.size());
}
// Test that performing concurrent, overlapping insert (same) is thread-safe.
TEST_F(AtomicUnorderedMapTest, ConcurrentOverlappingInsert) {
    // Add 10 elements from two threads, but starting from the same offset.
    // Should result in only 10 elements existing at the end.
    const size_t n_elements{10};
    std::thread t1{insert_into_map, std::ref(map), n_elements, 0};
    std::thread t2{insert_into_map, std::ref(map), n_elements, 0};
    t1.join();
    t2.join();

    EXPECT_EQ(n_elements, map.size());
}

// Test find_if2 using similar types to how it is to be used in DcpProducer
// Return a shared_ptr to an object or an empty shared_ptr
TEST_F(AtomicUnorderedMapTest, find_if2) {
    size_t value = 20;
    auto func = [value](TestMap::value_type& vt) {
        if (vt.second->value == value) {
            return std::make_shared<std::string>("Found");
        }
        return std::shared_ptr<std::string>{};
    };
    auto rv = map.find_if2(func);

    EXPECT_FALSE(rv);

    insert_into_map(map, 10, 0);
    rv = map.find_if2(func);
    ASSERT_TRUE(rv);
    EXPECT_EQ("Found", *rv);
    map.erase(2);
    rv = map.find_if2(func);
    EXPECT_FALSE(rv);
}
