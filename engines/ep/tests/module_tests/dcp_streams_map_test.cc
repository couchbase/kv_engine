/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp/streams_map.h"

#include <folly/portability/GTest.h>

#include <algorithm>
#include <array>
#include <thread>
#include <vector>

class DcpStreamsMapTest : public ::testing::Test {
protected:
    DcpStreamsMap<int> map{4};
};

TEST_F(DcpStreamsMapTest, FindEmpty) {
    EXPECT_EQ(4, map.capacity());
    for (uint16_t vb = 0; vb < 4; ++vb) {
        EXPECT_FALSE(map.find(Vbid(vb)));
    }
    // Outside the capacity of the map isn't mapped
    EXPECT_FALSE(map.find(Vbid(4)));
    EXPECT_FALSE(map.find(Vbid(0xffff)));
}

TEST_F(DcpStreamsMapTest, Insert) {
    auto value = std::make_shared<int>(1);
    auto [mapped, inserted] = map.insert(Vbid(2), value);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(value, mapped);
    EXPECT_EQ(value, map.find(Vbid(2)));
    EXPECT_FALSE(map.find(Vbid(1)));
    EXPECT_FALSE(map.find(Vbid(3)));
}

TEST_F(DcpStreamsMapTest, InsertExisting) {
    auto first = std::make_shared<int>(1);
    ASSERT_TRUE(map.insert(Vbid(0), first).second);

    // A second insert doesn't replace the value but returns the existing one
    auto [mapped, inserted] = map.insert(Vbid(0), std::make_shared<int>(2));
    EXPECT_FALSE(inserted);
    EXPECT_EQ(first, mapped);
    EXPECT_EQ(first, map.find(Vbid(0)));
}

TEST_F(DcpStreamsMapTest, InsertInvalid) {
    EXPECT_THROW(map.insert(Vbid(4), std::make_shared<int>(1)),
                 std::out_of_range);
    EXPECT_THROW(map.insert(Vbid(0), {}), std::invalid_argument);
    EXPECT_FALSE(map.find(Vbid(0)));
}

TEST_F(DcpStreamsMapTest, ForEach) {
    map.insert(Vbid(3), std::make_shared<int>(3));
    map.insert(Vbid(1), std::make_shared<int>(1));

    // Only the mapped vBuckets are visited, in vBucket order
    std::vector<std::pair<Vbid, int>> visited;
    map.for_each([&visited](Vbid vbid, const auto& value) {
        visited.emplace_back(vbid, *value);
    });
    const std::vector<std::pair<Vbid, int>> expected{{Vbid(1), 1},
                                                     {Vbid(3), 3}};
    EXPECT_EQ(expected, visited);
}

TEST_F(DcpStreamsMapTest, FindIf) {
    map.insert(Vbid(0), std::make_shared<int>(10));
    map.insert(Vbid(1), std::make_shared<int>(11));
    map.insert(Vbid(2), std::make_shared<int>(12));

    // Stops at the first result which evaluates to true
    int calls = 0;
    auto found = map.find_if([&calls](Vbid, const auto& value) {
        ++calls;
        return *value >= 11 ? value : nullptr;
    });
    ASSERT_TRUE(found);
    EXPECT_EQ(11, *found);
    EXPECT_EQ(2, calls);

    // Returns a value initialised result if nothing matches
    EXPECT_FALSE(map.find_if([](Vbid, const auto&) { return false; }));
}

/**
 * Multiple threads racing to insert into the same vBucket must result in
 * exactly one of the values being inserted, and every thread observing it.
 */
TEST_F(DcpStreamsMapTest, ConcurrentInsert) {
    constexpr int numThreads = 8;
    std::atomic<bool> start{false};
    std::array<std::shared_ptr<int>, numThreads> results;
    std::array<bool, numThreads> inserted{};
    std::vector<std::thread> threads;
    for (int idx = 0; idx < numThreads; ++idx) {
        threads.emplace_back([this, idx, &start, &results, &inserted]() {
            while (!start) {
                std::this_thread::yield();
            }
            auto [mapped, didInsert] =
                    map.insert(Vbid(1), std::make_shared<int>(idx));
            results[idx] = mapped;
            inserted[idx] = didInsert;
        });
    }
    start = true;
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(1, std::ranges::count(inserted, true));
    const auto& winner = map.find(Vbid(1));
    ASSERT_TRUE(winner);
    for (const auto& result : results) {
        EXPECT_EQ(winner, result);
    }
}
