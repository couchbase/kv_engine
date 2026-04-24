/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <mcbp/protocol/dcp_cache_transfer_buffer.h>
#include <string>
#include <vector>

using namespace cb::mcbp;
using namespace cb::mcbp::request;

class DcpCacheTransferBufferTest : public ::testing::Test {
protected:
    // Helper to build a buffer with one or more items.
    // Each pair is {key, value}. CAS and seqno are auto-generated.
    std::string buildBuffer(
            const std::vector<std::pair<std::string, std::string>>& items) {
        std::string buffer;
        uint64_t seqno = 1;
        for (const auto& [key, value] : items) {
            DcpCacheTransferPayload payload(
                    static_cast<uint16_t>(key.size()),
                    static_cast<uint32_t>(value.size()),
                    seqno, // cas = seqno
                    seqno,
                    1, // rev_seqno
                    0, // flags
                    0, // expiration
                    PROTOCOL_BINARY_DATATYPE_JSON, // datatype
                    128); // cacheHint
            auto buf = payload.getBuffer();
            buffer.append(reinterpret_cast<const char*>(buf.data()),
                          buf.size());
            buffer.append(key);
            buffer.append(value);
            ++seqno;
        }
        return buffer;
    }
};

TEST_F(DcpCacheTransferBufferTest, ConstructWithValidData) {
    auto buffer = buildBuffer({{"key1", "value1"}});
    EXPECT_NO_THROW(DcpCacheTransferBuffer buf(buffer));
}

TEST_F(DcpCacheTransferBufferTest, ConstructWithEmptyDataThrows) {
    EXPECT_THROW(DcpCacheTransferBuffer buf(""), std::invalid_argument);
}

TEST_F(DcpCacheTransferBufferTest, ConstructWithTooSmallDataThrows) {
    for (size_t size = 0; size < DcpCacheTransferBuffer::minSize(); ++size) {
        std::string buffer(size, 'x');
        EXPECT_THROW(DcpCacheTransferBuffer buf(buffer), std::invalid_argument);
    }
}

TEST_F(DcpCacheTransferBufferTest, ConstructWithMinimumValidSize) {
    // Minimum valid buffer: payload header + 2-byte key (minimum key length)
    DcpCacheTransferPayload payload(
            2, 0, 1, 1, 1, 0, 0, PROTOCOL_BINARY_DATATYPE_JSON, 128);
    auto buf = payload.getBuffer();
    std::string buffer(reinterpret_cast<const char*>(buf.data()), buf.size());
    buffer.append("kk"); // 2-byte key
    EXPECT_NO_THROW(DcpCacheTransferBuffer dcpBuf(buffer));
}

TEST_F(DcpCacheTransferBufferTest, IterateSingleItem) {
    auto buffer = buildBuffer({{"testkey", "testvalue"}});
    DcpCacheTransferBuffer buf(buffer);

    int count = 0;
    for (const auto& item : buf) {
        EXPECT_EQ("testkey", item.getKey());
        EXPECT_EQ("testvalue", item.getValue());
        EXPECT_EQ(1, item.getCas());
        EXPECT_EQ(1, item.getBySeqno());
        EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, item.getDatatype());
        EXPECT_EQ(128, item.getCacheHint());
        ++count;
    }
    EXPECT_EQ(1, count);
}

TEST_F(DcpCacheTransferBufferTest, IterateMultipleItems) {
    auto buffer = buildBuffer(
            {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}});
    DcpCacheTransferBuffer buf(buffer);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<uint64_t> casValues;

    for (const auto& item : buf) {
        keys.emplace_back(item.getKey());
        values.emplace_back(item.getValue());
        casValues.push_back(item.getCas());
    }

    ASSERT_EQ(3, keys.size());
    EXPECT_EQ("key1", keys[0]);
    EXPECT_EQ("key2", keys[1]);
    EXPECT_EQ("key3", keys[2]);

    EXPECT_EQ("value1", values[0]);
    EXPECT_EQ("value2", values[1]);
    EXPECT_EQ("value3", values[2]);

    // CAS values are auto-generated as seqno (1, 2, 3)
    EXPECT_EQ(1, casValues[0]);
    EXPECT_EQ(2, casValues[1]);
    EXPECT_EQ(3, casValues[2]);
}

TEST_F(DcpCacheTransferBufferTest, IterateItemWithEmptyValue) {
    auto buffer = buildBuffer({{"keyonly", ""}});
    DcpCacheTransferBuffer buf(buffer);

    int count = 0;
    for (const auto& item : buf) {
        EXPECT_EQ("keyonly", item.getKey());
        EXPECT_EQ("", item.getValue());
        EXPECT_EQ(1, item.getCas());
        ++count;
    }
    EXPECT_EQ(1, count);
}

TEST_F(DcpCacheTransferBufferTest, IterateItemWithShortKeyFails) {
    // Keys must be at least 2 bytes (collection prefix + 1 byte)
    // A 1-byte key should result in an iteration error
    auto buffer = buildBuffer({{"x", "somevalue"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    EXPECT_TRUE(it.hasError());
    EXPECT_FALSE(it.getError().is_null());
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, IterateItemWithEmptyKeyFails) {
    // Empty key should result in an iteration error
    auto buffer = buildBuffer({{"", "somevalue"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    EXPECT_TRUE(it.hasError());
    EXPECT_FALSE(it.getError().is_null());
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, IterateItemWithOversizedKeyFails) {
    // Keys larger than MaxCollectionsKeyLen must fail validation.
    const std::string oversizedKey(MaxCollectionsKeyLen + 1, 'a');
    auto buffer = buildBuffer({{oversizedKey, "somevalue"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    EXPECT_TRUE(it.hasError());
    ASSERT_FALSE(it.getError().is_null());
    EXPECT_EQ("invalid_key_length", it.getError().at("reason"));
    EXPECT_EQ(MaxCollectionsKeyLen, it.getError().at("max_key_len"));
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, IteratorErrorOnTruncatedData) {
    // Build a valid item then truncate it
    auto buffer = buildBuffer({{"key1", "value1"}});
    buffer = buffer.substr(0, buffer.size() - 5); // Remove last 5 bytes

    DcpCacheTransferBuffer buf(buffer);
    auto it = buf.begin();

    // First item should fail to parse
    EXPECT_TRUE(it.hasError());
    EXPECT_FALSE(it.getError().is_null());
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, IteratorErrorOnSecondItem) {
    // Build two items, then truncate the second
    auto buffer = buildBuffer({{"key1", "value1"}, {"key2", "value2"}});
    // Find where second item starts and truncate there
    size_t firstItemSize =
            sizeof(DcpCacheTransferPayload) + 4 + 6; // key1 + value1
    buffer = buffer.substr(0,
                           firstItemSize + sizeof(DcpCacheTransferPayload) - 1);

    DcpCacheTransferBuffer buf(buffer);
    auto it = buf.begin();

    // First item should succeed
    EXPECT_FALSE(it.hasError());
    EXPECT_EQ("key1", it->getKey());

    // Advance to second item
    ++it;

    // Second item should fail
    EXPECT_TRUE(it.hasError());
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, ManualIteration) {
    auto buffer = buildBuffer({{"aa", "1"}, {"bb", "2"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    ASSERT_NE(it, buf.end());
    EXPECT_EQ("aa", (*it).getKey());

    ++it;
    ASSERT_NE(it, buf.end());
    EXPECT_EQ("bb", it->getKey());

    ++it;
    EXPECT_EQ(it, buf.end());
}

TEST_F(DcpCacheTransferBufferTest, PostIncrementIterator) {
    auto buffer = buildBuffer({{"xx", "y"}, {"zz", "w"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    auto prev = it++;

    EXPECT_EQ("xx", prev->getKey());
    EXPECT_EQ("zz", it->getKey());
}

TEST_F(DcpCacheTransferBufferTest, SizeAndEmpty) {
    auto buffer = buildBuffer({{"key", "val"}});
    DcpCacheTransferBuffer buf(buffer);

    EXPECT_FALSE(buf.empty());
    EXPECT_EQ(buffer.size(), buf.size());
    EXPECT_EQ(buffer, buf.getData());
}

TEST_F(DcpCacheTransferBufferTest, LargeKeyAndValue) {
    // Use the largest valid key (MaxCollectionsKeyLen) paired with a large
    // value.
    std::string largeKey(MaxCollectionsKeyLen, 'k');
    std::string largeValue(100000, 'v');
    auto buffer = buildBuffer({{largeKey, largeValue}});
    DcpCacheTransferBuffer buf(buffer);

    int count = 0;
    for (const auto& item : buf) {
        EXPECT_EQ(largeKey, item.getKey());
        EXPECT_EQ(largeValue, item.getValue());
        ++count;
    }
    EXPECT_EQ(1, count);
}

TEST_F(DcpCacheTransferBufferTest, PayloadFieldsPreserved) {
    std::string buffer;
    DcpCacheTransferPayload payload(3, // key_len
                                    5, // value_len
                                    12345, // cas
                                    67890, // by_seqno
                                    111, // rev_seqno
                                    222, // flags
                                    333, // expiration
                                    PROTOCOL_BINARY_DATATYPE_JSON, // datatype
                                    200); // cacheHint
    auto payloadBuf = payload.getBuffer();
    buffer.append(reinterpret_cast<const char*>(payloadBuf.data()),
                  payloadBuf.size());
    buffer.append("key");
    buffer.append("value");

    DcpCacheTransferBuffer dcpBuf(buffer);
    auto it = dcpBuf.begin();

    EXPECT_EQ(12345, it->getCas());
    EXPECT_EQ(67890, it->getBySeqno());
    EXPECT_EQ(111, it->getRevSeqno());
    EXPECT_EQ(222, it->getFlags());
    EXPECT_EQ(333, it->getExpiration());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, it->getDatatype());
    EXPECT_EQ(200, it->getCacheHint());
    EXPECT_EQ("key", it->getKey());
    EXPECT_EQ("value", it->getValue());
}

TEST_F(DcpCacheTransferBufferTest, AccessPayloadDirectly) {
    auto buffer = buildBuffer({{"mykey", "myval"}});
    DcpCacheTransferBuffer buf(buffer);

    auto it = buf.begin();
    const auto& payload = it->getPayload();

    EXPECT_EQ(5, payload.getKeyLen());
    EXPECT_EQ(5, payload.getValueLen());
    EXPECT_EQ(1, payload.getCas()); // CAS = seqno = 1
}
