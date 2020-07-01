/*
 *     Copyright 2016 Couchbase, Inc
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

#include <unordered_set>

#include <folly/portability/GTest.h>
#include <random>

#include "bloomfilter.h"
#include "murmurhash3.h"
#include "tests/module_tests/test_helpers.h"

class BloomFilterTest : public ::testing::Test {};

// Test the size calculation when creating a bloom filter.
// See: https://en.wikipedia.org/wiki/Bloom_filter
TEST_F(BloomFilterTest, SizeCalculation) {
    struct Params {
        size_t keys;
        size_t bits;
        size_t hashes;
    };
    std::vector<Params> params{{1, 10, 7},
                               {10, 96, 7},
                               {100, 959, 7},
                               {1000, 9585, 7},
                               {10000, 95851, 7},
                               {100000, 958506, 7}};

    for (const auto& p : params) {
        BloomFilter bf(p.keys, 0.01, BFILTER_ENABLED);
        EXPECT_EQ(p.bits, bf.getFilterSize()) << "For keys=" << p.keys;
        EXPECT_EQ(p.hashes, bf.getNoOfHashes()) << "For keys=" << p.keys;
    }
}

// Test the filtering of the bloom filter.
// Add N keys, then test that all N keys "may" exist in filter.
TEST_F(BloomFilterTest, PositiveCheck) {
    // Generate N keys. First insert them all, then check they all exist.
    const int numKeys = 10000;
    const double targetFalsePositive = 0.01;

    // Create bloom filter sized for the given number of keys, and insert them
    // all.
    BloomFilter bf(numKeys, targetFalsePositive, BFILTER_ENABLED);
    for (int i = 0; i < numKeys; i++) {
        bf.addKey(makeStoredDocKey("key_" + std::to_string(i)));
    }

    // Test - check the N keys for existence
    for (int i = 0; i < numKeys; i++) {
        auto key = makeStoredDocKey("key_" + std::to_string(i));
        EXPECT_TRUE(bf.maybeKeyExists(key)) << "For key:" << key.to_string();
    }
}

// Test the false positive rate of the bloom filter.
// Only expect a FP _close_ to the estimated (given the probabilstic nature
// of the Bloom filter).
TEST_F(BloomFilterTest, FalsePositiveRate) {
    // Generate 2 x N keys. First half will be inserted, second half
    // will be tested for membership - any which are found are false positives.
    const int numKeys = 10000;
    const double targetFalsePositive = 0.01;

    // Create bloom filter sized for the given number of keys, and insert them
    // all.
    BloomFilter bf(numKeys, targetFalsePositive, BFILTER_ENABLED);
    for (int i = 0; i < numKeys; i++) {
        bf.addKey(makeStoredDocKey("key_" + std::to_string(i)));
    }

    // Test - check the next N keys for existence
    int falsePositives = 0;
    for (int i = 0; i < numKeys; i++) {
        if (bf.maybeKeyExists(
                    makeStoredDocKey("key_" + std::to_string(numKeys + i)))) {
            falsePositives++;
        }
    }

    // Allow a FP rate 10% either way.
    const int expectedFalsePositives = numKeys * targetFalsePositive;
    EXPECT_NEAR(expectedFalsePositives,
                falsePositives,
                expectedFalsePositives * 0.1);
}

class BloomFilterDocKeyTest
    : public BloomFilter,
      public ::testing::TestWithParam<std::tuple<CollectionID, CollectionID>> {
public:
    BloomFilterDocKeyTest() : BloomFilter(10000, 0.01, BFILTER_ENABLED) {
    }
};

/*
 * The following test is just checking for some basic hashing properties
 * for all namespaces, not checking for distribution quality etc...
 */
TEST_P(BloomFilterDocKeyTest, check_hashing) {
    if (std::get<0>(GetParam()) != std::get<1>(GetParam())) {
        auto key1 = StoredDocKey("key", std::get<0>(GetParam()));
        auto key2 = StoredDocKey("key", std::get<1>(GetParam()));
        std::unordered_set<uint64_t> key1_hashes;
        std::unordered_set<uint64_t> key2_hashes;

        // Always insert the first two hashes then check every subsequent hash
        // has no match in the set.
        key1_hashes.insert(hashDocKey(key1, 0));
        key2_hashes.insert(hashDocKey(key2, 0));
        for (size_t i = 1; i < noOfHashes; i++)  {
            EXPECT_EQ(0, key1_hashes.count(hashDocKey(key1, i)));
            EXPECT_EQ(0, key2_hashes.count(hashDocKey(key1, i)));
            EXPECT_EQ(0, key1_hashes.count(hashDocKey(key2, i)));
            EXPECT_EQ(0, key2_hashes.count(hashDocKey(key2, i)));
            key1_hashes.insert(hashDocKey(key1, i));
            key2_hashes.insert(hashDocKey(key2, i));
        }
    }
}

TEST_P(BloomFilterDocKeyTest, check_addKey) {
    auto key1 = StoredDocKey("key", std::get<0>(GetParam()));
    auto key2 = StoredDocKey("key", std::get<1>(GetParam()));
    addKey(key1);
    addKey(key2);
    if (std::get<0>(GetParam()) != std::get<1>(GetParam())) {
        EXPECT_EQ(2, getNumOfKeysInFilter());
    } else {
        EXPECT_EQ(1, getNumOfKeysInFilter());
    }
}

TEST_P(BloomFilterDocKeyTest, check_maybeKeyExist) {
    auto key1 = StoredDocKey("key", std::get<0>(GetParam()));
    auto key2 = StoredDocKey("key", std::get<1>(GetParam()));
    addKey(key1);
    EXPECT_EQ(1, getNumOfKeysInFilter());
    if (std::get<0>(GetParam()) != std::get<1>(GetParam())) {
        EXPECT_FALSE(maybeKeyExists(key2));
    } else {
        EXPECT_TRUE(maybeKeyExists(key2));
    }
}

// Test params includes our labelled collections that have 'special meaning' and
// one normal collection ID (100)
static std::vector<CollectionID> allDocNamespaces = {
        {CollectionID::Default, CollectionID::System, 100}};

INSTANTIATE_TEST_SUITE_P(
        DocNamespace,
        BloomFilterDocKeyTest,
        ::testing::Combine(::testing::ValuesIn(allDocNamespaces),
                           ::testing::ValuesIn(allDocNamespaces)));
