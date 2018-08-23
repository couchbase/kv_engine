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

#include <gtest/gtest.h>

#include "storeddockey.h"

#include <map>

class StoredDocKeyTest : public ::testing::TestWithParam<DocNamespace> {};

class StoredDocKeyTestCombi : public ::testing::TestWithParam<
                                      std::tuple<DocNamespace, DocNamespace>> {
};

class SerialisedDocKeyTest : public ::testing::TestWithParam<DocNamespace> {};

TEST_P(StoredDocKeyTest, constructors) {
    // C-string/std::string
    StoredDocKey key1("key", GetParam());

    // The key size/data now include a unsigned_leb128 encoded CollectionID
    // So size won't match (StoredDocKey contains more data)
    EXPECT_LT(strlen("key"), key1.size());
    // Data is no longer the c-string input
    EXPECT_NE(0, std::memcmp("key", key1.data(), sizeof("key")));
    // We expect to get back the CollectionID used in initialisation
    EXPECT_EQ(GetParam(), key1.getCollectionID());

    // Test construction from a DocKey which is a view onto unsigned_leb128
    // prefixed key - i.e. a collection-aware key
    uint8_t keyRaw[5] = {uint8_t(GetParam()), 'k', 'e', 'y', '!'};
    DocKey docKey(keyRaw, 5, DocKeyEncodesCollectionId::Yes);
    StoredDocKey key3(docKey);

    // Very important that the both objects return the same hash and ==
    EXPECT_EQ(key3.hash(), docKey.hash());
    EXPECT_EQ(key3, docKey);

    // Expect different .data (StoredDocKey has allocated/copied)
    EXPECT_NE(docKey.data(), key3.data());

    // Expect the same bytes
    EXPECT_EQ(docKey.size(), key3.size());
    for (size_t ii = 0; ii < key3.size(); ii++) {
        EXPECT_EQ(docKey.data()[ii], key3.data()[ii]);
    }

    // Expect we can get back the CollectionID
    EXPECT_EQ(GetParam(), key3.getCollectionID());
}

TEST(StoredDocKey, no_encoded_collectionId) {
    // Test construction from a DocKey which is a view onto a key with no
    // encoded prefix
    uint8_t keyRaw[4] = {'k', 'e', 'y', '!'};
    DocKey docKey(keyRaw, 4, DocKeyEncodesCollectionId::No);
    StoredDocKey key3(docKey);

    // Very important that the both objects return the same hash and ==
    EXPECT_EQ(key3.hash(), docKey.hash());
    EXPECT_EQ(key3, docKey);
    EXPECT_NE(docKey.data(), key3.data());
    EXPECT_NE(docKey.data()[0], key3.data()[0]);
    EXPECT_EQ(0, key3.getCollectionID());
}

TEST_P(StoredDocKeyTest, copy_constructor) {
    StoredDocKey key1("key1", GetParam());
    StoredDocKey key2(key1);

    // exterally check rather than just use ==
    EXPECT_EQ(key1.size(), key2.size());
    EXPECT_EQ(key1.getDocNamespace(), key2.getDocNamespace());
    EXPECT_NE(key1.data(), key2.data()); // must be different pointers
    EXPECT_TRUE(std::memcmp(key1.data(), key2.data(), key1.size()) == 0);
    EXPECT_EQ(key1, key2);
}

TEST_P(StoredDocKeyTest, assignment) {
    StoredDocKey key1("key1", GetParam());
    StoredDocKey key2("anotherkey", GetParam());

    key1 = key2;

    // exterally check
    EXPECT_EQ(key1.size(), key2.size());
    EXPECT_EQ(key1.getDocNamespace(), key2.getDocNamespace());
    EXPECT_NE(key1.data(), key2.data()); // must be different pointers
    EXPECT_TRUE(std::memcmp(key1.data(), key2.data(), key1.size()) == 0);
    EXPECT_EQ(key1, key2);
}

TEST_P(StoredDocKeyTestCombi, hash) {
    StoredDocKey key1("key1", std::get<0>(GetParam()));
    StoredDocKey key2("key1", std::get<1>(GetParam()));
    DocKey docKey1(key1);
    DocKey docKey2(key2);
    auto serialKey1 = SerialisedDocKey::make(key1);
    auto serialKey2 = SerialisedDocKey::make(key2);
    if (std::get<0>(GetParam()) != std::get<1>(GetParam())) {
        EXPECT_NE(key1.hash(), key2.hash());
        EXPECT_NE(docKey1.hash(), docKey2.hash());
        EXPECT_NE(serialKey1->hash(), serialKey2->hash());
    } else {
        EXPECT_EQ(key1.hash(), key2.hash());
        EXPECT_EQ(docKey1.hash(), docKey2.hash());
        EXPECT_EQ(serialKey1->hash(), serialKey2->hash());
    }
}

TEST_P(StoredDocKeyTestCombi, equalityOperators) {
    StoredDocKey key1("key1", std::get<0>(GetParam()));
    StoredDocKey key2("key1", std::get<1>(GetParam()));
    StoredDocKey key3("key3", std::get<0>(GetParam()));
    StoredDocKey key4("key3", std::get<1>(GetParam()));

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

TEST_P(StoredDocKeyTestCombi, lessThan) {
    StoredDocKey key1("zzb", std::get<0>(GetParam()));
    StoredDocKey key2(key1);
    EXPECT_FALSE(key1 < key2); // same key

    StoredDocKey key1_ns1("zzb", std::get<1>(GetParam()));
    StoredDocKey key2_ns1(key1_ns1);
    EXPECT_FALSE(key1_ns1 < key2_ns1); // same key

    StoredDocKey key3("zza::thing", std::get<0>(GetParam()));
    StoredDocKey key3_ns1("zza::thing", std::get<1>(GetParam()));

    if (std::get<0>(GetParam()) < std::get<1>(GetParam())) {
        // DocNamespace is compared first, so if it is less all compares will
        // be less
        EXPECT_TRUE(key3 < key1_ns1);
        EXPECT_TRUE(key3 < key2_ns1);
        EXPECT_TRUE(key3 < key3_ns1);
        EXPECT_TRUE(key1 < key1_ns1);
        EXPECT_TRUE(key1 < key2_ns1);
        EXPECT_TRUE(key2 < key2_ns1);
        EXPECT_TRUE(key2 < key1_ns1);
    } else if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        // Same namespace, so it's a check of key
        EXPECT_FALSE(key1 < key1_ns1);
        EXPECT_TRUE(key3 < key1);
        EXPECT_TRUE(key3 < key1_ns1);
    } else {
        EXPECT_FALSE(key3 < key1_ns1);
        EXPECT_FALSE(key3 < key2_ns1);
        EXPECT_FALSE(key3 < key3_ns1);
        EXPECT_FALSE(key1 < key1_ns1);
        EXPECT_FALSE(key1 < key2_ns1);
        EXPECT_FALSE(key2 < key2_ns1);
        EXPECT_FALSE(key2 < key1_ns1);
    }
}

// Test that the StoredDocKey can be used in std::map
TEST_P(StoredDocKeyTestCombi, map) {
    std::map<StoredDocKey, int> map;
    StoredDocKey key1("key1", std::get<0>(GetParam()));
    StoredDocKey key2("key2", std::get<0>(GetParam()));

    StoredDocKey key1_ns1("key1", std::get<1>(GetParam()));
    StoredDocKey key2_ns1("key2", std::get<1>(GetParam()));

    EXPECT_EQ(0, map.count(key1));
    EXPECT_EQ(0, map.count(key1_ns1));
    EXPECT_EQ(0, map.count(key2));
    EXPECT_EQ(0, map.count(key2_ns1));

    map[key1] = 1;
    map[key1_ns1] = 101;

    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_EQ(1, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_TRUE(map[key1] == 101);
    } else {
        EXPECT_EQ(2, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key1_ns1));
        EXPECT_TRUE(map[key1] == 1);
        EXPECT_TRUE(map[key1_ns1] == 101);
    }

    map[key2] = 2;
    map[key2_ns1] = 102;

    if (std::get<0>(GetParam()) == std::get<1>(GetParam())) {
        EXPECT_EQ(2, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key2));
        EXPECT_TRUE(map[key1] == 101);
        EXPECT_TRUE(map[key2] == 102);
    } else {
        EXPECT_EQ(4, map.size());
        EXPECT_EQ(1, map.count(key1));
        EXPECT_EQ(1, map.count(key1_ns1));
        EXPECT_EQ(1, map.count(key2));
        EXPECT_EQ(1, map.count(key2_ns1));
        EXPECT_TRUE(map[key1] == 1);
        EXPECT_TRUE(map[key1_ns1] == 101);
        EXPECT_TRUE(map[key2] == 2);
        EXPECT_TRUE(map[key2_ns1] == 102);
    }
}

TEST_P(SerialisedDocKeyTest, constructor) {
    StoredDocKey key("key", GetParam());
    auto serialKey = SerialisedDocKey::make(key);

    // The key size/data now include a unsigned_leb128 encoded CollectionID
    // So size won't match (SerialisedDocKey contains more data)
    EXPECT_LT(strlen("key"), serialKey->size());
    // Data is no longer the c-string input
    EXPECT_NE(0, std::memcmp("key", serialKey->data(), sizeof("key")));

    // But we can retrieve the CID
    EXPECT_EQ(GetParam(), serialKey->getDocNamespace());
}

TEST_P(SerialisedDocKeyTest, equals) {
    StoredDocKey key("key", GetParam());
    auto serialKey = SerialisedDocKey::make(key);
    EXPECT_EQ(*serialKey, key);
}

TEST_P(StoredDocKeyTest, constructFromSerialisedDocKey) {
    StoredDocKey key1("key", GetParam());
    auto serialKey = SerialisedDocKey::make(key1);
    StoredDocKey key2(*serialKey);

    // Check key2 equals key1
    EXPECT_EQ(key1, key2);

    // These hash the same
    EXPECT_EQ(serialKey->hash(), key1.hash());

    // Key1 equals serialKey
    EXPECT_EQ(*serialKey, key1);

    // Key 2 must equal serialKey (compare size, data, namespace)
    EXPECT_EQ(serialKey->size(), key2.size());
    for (size_t ii = 0; ii < key2.size(); ii++) {
        EXPECT_EQ(serialKey->data()[ii], key2.data()[ii]);
    }
    EXPECT_EQ(serialKey->getDocNamespace(), key2.getDocNamespace());
}

TEST_P(StoredDocKeyTest, getObjectSize) {
    auto key1 = SerialisedDocKey::make({"key_of_15_chars", GetParam()});
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(GetParam());

    // Should be 15 bytes plus 4 byte CID and 1 byte for length.
    EXPECT_EQ(15 + leb128.size() + 1, key1->getObjectSize());
}

// Test params includes our labelled collections that have 'special meaning' and
// one normal collection ID (100)
static std::vector<DocNamespace> allDocNamespaces = {
        {DocNamespace::DefaultCollection, DocNamespace::System, 100}};

INSTANTIATE_TEST_CASE_P(
        DocNamespace,
        StoredDocKeyTestCombi,
        ::testing::Combine(::testing::ValuesIn(allDocNamespaces),
                           ::testing::ValuesIn(allDocNamespaces)), );

// Test params includes our labelled types that have 'special meaning' and
// one normal collection ID (100)
INSTANTIATE_TEST_CASE_P(DocNamespace,
                        StoredDocKeyTest,
                        ::testing::Values(DocNamespace::DefaultCollection,
                                          DocNamespace::System,
                                          100), );

INSTANTIATE_TEST_CASE_P(DocNamespace,
                        SerialisedDocKeyTest,
                        ::testing::Values(DocNamespace::DefaultCollection,
                                          DocNamespace::System,
                                          100), );
