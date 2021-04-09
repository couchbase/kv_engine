/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <memcached/dockey.h>

#include <array>

class DocKeyTest : public ::testing::Test {
protected:
    void golden(cb::const_byte_buffer buffer,
                size_t logicalKeyLen,
                CollectionID encoded);
    void golden(cb::const_byte_buffer buffer);
};

TEST_F(DocKeyTest, invalid) {
    std::array<uint8_t, 4> data1 = {{0, 'k', 'e', 'y'}};
    std::array<char, 4> data2 = {{0, 'k', 'e', 'y'}};
    std::string_view buf{data2.data(), 0};

    std::unique_ptr<DocKey> ptr;
    EXPECT_THROW(ptr = std::make_unique<DocKey>(
                         data1.data(), 0, DocKeyEncodesCollectionId::Yes),
                 std::invalid_argument);

    EXPECT_THROW(ptr = std::make_unique<DocKey>(
                         nullptr, 4, DocKeyEncodesCollectionId::Yes),
                 std::invalid_argument);

    EXPECT_THROW(
            ptr = std::make_unique<DocKey>(buf, DocKeyEncodesCollectionId::Yes),
            std::invalid_argument);

    EXPECT_NO_THROW(
            ptr = std::make_unique<DocKey>(
                    data1.data(), data1.size(), DocKeyEncodesCollectionId::No));
    EXPECT_NO_THROW(
            ptr = std::make_unique<DocKey>(data1.data(),
                                           data1.size(),
                                           DocKeyEncodesCollectionId::Yes));
    EXPECT_NO_THROW(ptr = std::make_unique<DocKey>(
                            nullptr, 0, DocKeyEncodesCollectionId::No));
}

// A DocKey can view nothing (len:0) if it does no encode a collection
TEST_F(DocKeyTest, zeroLength) {
    std::array<uint8_t, 4> data1 = {{0, 'k', 'e', 'y'}};
    // Safe to construct and we expect that it behaves ok
    DocKey key(data1.data(), 0, DocKeyEncodesCollectionId::No);
    EXPECT_EQ(0, key.size());
    EXPECT_EQ(CollectionID::Default, key.getCollectionID());
    EXPECT_FALSE(key.isInSystemCollection());
    EXPECT_TRUE(key.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key.getEncoding());

    auto pair = key.getIdAndKey();
    EXPECT_EQ(CollectionID::Default, pair.first);
    EXPECT_EQ(0, pair.second.size());

    auto key2 = key.makeDocKeyWithoutCollectionID();
    EXPECT_EQ(0, key2.size());
    EXPECT_EQ(CollectionID::Default, key2.getCollectionID());
    EXPECT_FALSE(key2.isInSystemCollection());
    EXPECT_TRUE(key2.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key2.getEncoding());
}

// A DocKey can view nothing (null,len:0) if it does not encode a collection
// There are some places in the code which construct with no data pointer
TEST_F(DocKeyTest, nullZeroLength) {
    DocKey key(nullptr, 0, DocKeyEncodesCollectionId::No);
    EXPECT_EQ(0, key.size());
    EXPECT_EQ(CollectionID::Default, key.getCollectionID());
    EXPECT_FALSE(key.isInSystemCollection());
    EXPECT_TRUE(key.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key.getEncoding());

    auto pair = key.getIdAndKey();
    EXPECT_EQ(CollectionID::Default, pair.first);
    EXPECT_EQ(0, pair.second.size());

    auto key2 = key.makeDocKeyWithoutCollectionID();
    EXPECT_EQ(0, key2.size());
    EXPECT_EQ(CollectionID::Default, key2.getCollectionID());
    EXPECT_FALSE(key2.isInSystemCollection());
    EXPECT_TRUE(key2.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key2.getEncoding());
}

void DocKeyTest::golden(cb::const_byte_buffer buffer,
                        size_t logicalKeyLen,
                        CollectionID encoded) {
    DocKey key(buffer.data(), buffer.size(), DocKeyEncodesCollectionId::Yes);
    EXPECT_EQ(buffer.size(), key.size());
    EXPECT_EQ(encoded, key.getCollectionID());
    if (encoded == CollectionID::Default) {
        EXPECT_TRUE(key.isInDefaultCollection());
        EXPECT_FALSE(key.isInSystemCollection());
    }
    EXPECT_EQ(DocKeyEncodesCollectionId::Yes, key.getEncoding());

    auto pair = key.getIdAndKey();
    EXPECT_EQ(encoded, pair.first);
    EXPECT_EQ(logicalKeyLen, pair.second.size());

    auto key2 = key.makeDocKeyWithoutCollectionID();
    EXPECT_EQ(logicalKeyLen, key2.size());
    EXPECT_EQ(CollectionID::Default, key2.getCollectionID());
    if (encoded == CollectionID::Default) {
        EXPECT_TRUE(key2.isInDefaultCollection());
        EXPECT_FALSE(key2.isInSystemCollection());
    }
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key2.getEncoding());
}

TEST_F(DocKeyTest, golden) {
    std::array<uint8_t, 4> data1 = {{8, 'k', 'e', 'y'}};
    golden({data1.data(), data1.size()}, 3, CollectionID(8));
    std::array<uint8_t, 5> data2 = {{0xf8, 0, 'k', 'e', 'y'}};
    golden({data2.data(), data2.size()}, 3, CollectionID(120));
    std::array<uint8_t, 4> data3 = {{0, 'k', 'e', 'y'}};
    golden({data3.data(), data3.size()}, 3, CollectionID::Default);
}

void DocKeyTest::golden(cb::const_byte_buffer buffer) {
    DocKey key(buffer.data(), buffer.size(), DocKeyEncodesCollectionId::No);
    EXPECT_EQ(buffer.size(), key.size());
    EXPECT_EQ(CollectionID::Default, key.getCollectionID());
    EXPECT_FALSE(key.isInSystemCollection());
    EXPECT_TRUE(key.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key.getEncoding());

    auto pair = key.getIdAndKey();
    EXPECT_EQ(CollectionID::Default, pair.first);
    EXPECT_EQ(buffer.size(), pair.second.size());

    auto key2 = key.makeDocKeyWithoutCollectionID();
    EXPECT_EQ(buffer.size(), key2.size());
    EXPECT_EQ(CollectionID::Default, key2.getCollectionID());
    EXPECT_FALSE(key2.isInSystemCollection());
    EXPECT_TRUE(key2.isInDefaultCollection());
    EXPECT_EQ(DocKeyEncodesCollectionId::No, key2.getEncoding());
}

TEST_F(DocKeyTest, golden_nocollection_encoded) {
    std::array<uint8_t, 4> data1 = {{8, 'k', 'e', 'y'}};
    golden({data1.data(), data1.size()});
    std::array<uint8_t, 5> data2 = {{0xf8, 0, 'k', 'e', 'y'}};
    golden({data2.data(), data2.size()});
}

TEST_F(DocKeyTest, no_prepare) {
    std::array<uint8_t, 4> data1 = {
            {CollectionID::DurabilityPrepare, 'k', 'e', 'y'}};
    DocKey key(data1.data(), data1.size(), DocKeyEncodesCollectionId::No);
    try {
        DocKey key(data1.data(), 0, DocKeyEncodesCollectionId::Yes);
        FAIL() << "Expected constructor to throw an exception";
    } catch (const std::invalid_argument&) {
        // do nothing - expected to throw
    }
}
