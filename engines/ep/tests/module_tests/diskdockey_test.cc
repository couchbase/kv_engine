/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Unit tests for DiskDocKey
 */

#include "diskdockey.h"

#include <folly/portability/GTest.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/storeddockey.h>

class DiskDocKeyTest : public ::testing::TestWithParam<CollectionID> {};

class DiskDocKeyTestCombi
    : public ::testing::TestWithParam<std::tuple<CollectionID, CollectionID>> {
};

TEST_P(DiskDocKeyTest, constructors) {
    // From StoredDocKey (simplest way to get a DocKey object).
    DiskDocKey key1{StoredDocKey{"key", GetParam()}};

    // The key size/data now include a unsigned_leb128 encoded CollectionID
    // So size won't match (DiskDocKey contains more data)
    EXPECT_LT(strlen("key"), key1.size());
    // Data is no longer the c-string input
    EXPECT_NE(0, std::memcmp("key", key1.data(), sizeof("key")));
    // We expect to get back the CollectionID used in initialisation
    EXPECT_EQ(GetParam(), key1.getDocKey().getCollectionID());

    // Test construction from a raw char* which is a view onto unsigned_leb128
    // prefixed key - i.e. a collection-aware key from disk.
    std::array<char, 4> keyRaw{{char(uint32_t{GetParam()}), 'k', 'e', 'y'}};
    DiskDocKey key3{keyRaw.data(), keyRaw.size()};

    // Very important that the both objects return the same hash and ==
    EXPECT_EQ(key3.hash(), key1.hash());
    EXPECT_EQ(key3, key1);

    // Expect different .data (DiskDocKey has allocated/copied)
    EXPECT_NE(key1.data(), key3.data());

    // Expect the same bytes
    EXPECT_EQ(key1.size(), key3.size());
    for (size_t ii = 0; ii < key3.size(); ii++) {
        EXPECT_EQ(key1.data()[ii], key3.data()[ii]);
    }

    // Expect we can get back the CollectionID
    EXPECT_EQ(GetParam(), key3.getDocKey().getCollectionID());
}

TEST_P(DiskDocKeyTest, construct_prepared) {
    // Two keys, same key & collection but different prepared status.
    DiskDocKey key1{StoredDocKey{"key", GetParam()}, false};
    DiskDocKey key1_pre{StoredDocKey{"key", GetParam()}, true};

    // Should be different
    EXPECT_NE(key1, key1_pre);

    // But should be totally ordered
    EXPECT_TRUE(key1 < key1_pre || key1_pre < key1);

    // Should correctly report Committed / Prepared
    EXPECT_TRUE(key1.isCommitted());
    EXPECT_FALSE(key1.isPrepared());
    EXPECT_TRUE(key1_pre.isPrepared());
    EXPECT_FALSE(key1_pre.isCommitted());

    // Underlying docKeys (stripping any prepared prefix) should have the same
    // collectionId and key.
    EXPECT_EQ(key1.getDocKey().getIdAndKey(),
              key1_pre.getDocKey().getIdAndKey());
}

TEST_P(DiskDocKeyTestCombi, equalityOperators) {
    DiskDocKey key1{StoredDocKey{"key1", std::get<0>(GetParam())}};
    DiskDocKey key2{StoredDocKey{"key1", std::get<1>(GetParam())}};
    DiskDocKey key3{StoredDocKey{"key3", std::get<0>(GetParam())}};
    DiskDocKey key4{StoredDocKey{"key3", std::get<1>(GetParam())}};

    EXPECT_TRUE(key1 != key3);
    EXPECT_TRUE(key2 != key4);
    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_TRUE(key1 == key2);
        EXPECT_FALSE(key1 != key2);
    } else {
        EXPECT_FALSE(key1 == key2);
        EXPECT_TRUE(key1 != key2);
    }
}

// Test params includes our labelled collections that have 'special meaning' and
// one normal collection ID (100)
static std::vector<CollectionID> allDocNamespaces = {
        {CollectionID::Default, CollectionID::System, 100}};

INSTANTIATE_TEST_SUITE_P(
        CollectionID,
        DiskDocKeyTestCombi,
        ::testing::Combine(::testing::ValuesIn(allDocNamespaces),
                           ::testing::ValuesIn(allDocNamespaces)));

// Test params includes our labelled types that have 'special meaning' and
// one normal collection ID (100)
INSTANTIATE_TEST_SUITE_P(DocNamespace,
                         DiskDocKeyTest,
                         ::testing::Values(CollectionID::Default,
                                           CollectionID::System,
                                           100));
