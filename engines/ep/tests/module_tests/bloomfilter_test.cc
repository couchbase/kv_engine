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

#include <gtest/gtest.h>

#include "bloomfilter.h"
#include "murmurhash3.h"
#include "tests/module_tests/test_helpers.h"

class BloomFilterDocKeyTest : public BloomFilter,
                              public ::testing::TestWithParam<
                                      std::tuple<DocNamespace, DocNamespace>> {
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
static std::vector<DocNamespace> allDocNamespaces = {
        {DocNamespace::DefaultCollection, DocNamespace::System, 100}};

INSTANTIATE_TEST_CASE_P(
        DocNamespace,
        BloomFilterDocKeyTest,
        ::testing::Combine(::testing::ValuesIn(allDocNamespaces),
                           ::testing::ValuesIn(allDocNamespaces)), );
