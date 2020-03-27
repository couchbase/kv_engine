/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "item_compressor_test.h"
#include "item.h"
#include "item_compressor_visitor.h"
#include "test_helpers.h"
#include "vbucket.h"

TEST_P(ItemCompressorTest, testCompressionInActiveMode) {
    std::string compressibleValue(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    std::string nonCompressibleValue(
            R"({"user": "scott", "password": "tiger"})");

    auto key1 = makeStoredDocKey("key1");
    auto key2 = makeStoredDocKey("key2");
    auto evictedKey = makeStoredDocKey("evictme");

    auto item1 = make_item(vbucket->getId(),
                           key1,
                           compressibleValue,
                           0,
                           PROTOCOL_BINARY_DATATYPE_JSON);

    auto item2 = make_item(vbucket->getId(),
                           key2,
                           nonCompressibleValue,
                           0,
                           PROTOCOL_BINARY_DATATYPE_JSON);

    auto compressible_item = makeCompressibleItem(vbucket->getId(),
                                                  key1,
                                                  compressibleValue,
                                                  PROTOCOL_BINARY_DATATYPE_JSON,
                                                  true);

    auto rv = public_processSet(item1, 0);
    ASSERT_EQ(MutationStatus::WasClean, rv);

    rv = public_processSet(item2, 0);
    ASSERT_EQ(MutationStatus::WasClean, rv);

    // Save the datatype counts before and after compression. The test needs
    // to verify if the datatype counts are updated correctly after
    // compression
    auto curr_datatype = item1.getDataType();
    auto curr_datatype_count = vbucket->ht.getDatatypeCounts()[curr_datatype];
    auto new_datatype =
            (PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY);
    auto new_datatype_count = vbucket->ht.getDatatypeCounts()[new_datatype];

    auto itemCount = vbucket->ht.getNumItems();

    PauseResumeVBAdapter prAdapter(std::make_unique<ItemCompressorVisitor>());

    auto& visitor =
            dynamic_cast<ItemCompressorVisitor&>(prAdapter.getHTVisitor());
    visitor.setCompressionMode(BucketCompressionMode::Active);
    visitor.setMinCompressionRatio(config.getMinCompressionRatio());
    prAdapter.visit(*vbucket);

    std::string compressed_str(compressible_item->getData(),
                               compressible_item->getNBytes());

    std::string uncompressed_str(item2.getData(), item2.getNBytes());

    StoredValue* v1 = findValue(key1);
    StoredValue* v2 = findValue(key2);

    EXPECT_EQ(compressed_str, v1->getValue()->to_s());
    EXPECT_EQ(uncompressed_str, v2->getValue()->to_s());
    EXPECT_EQ(new_datatype, v1->getDatatype());
    EXPECT_EQ(curr_datatype_count - 1,
              vbucket->ht.getDatatypeCounts()[curr_datatype]);
    EXPECT_EQ(new_datatype_count + 1,
              vbucket->ht.getDatatypeCounts()[new_datatype]);
    EXPECT_EQ(itemCount, vbucket->ht.getNumItems());
}

// Test that an item will be left as uncompressed if the
// ratio between its uncompressed and compressed size doesn't
// exceed the configured compression ratio
TEST_P(ItemCompressorTest, testStoreUncompressedInActiveMode) {
    std::string nonCompressibleValue(
            R"({"user": "scott", "user": "tiger"})");

    auto key = makeStoredDocKey("key");
    auto item = make_item(vbucket->getId(),
                          key,
                          nonCompressibleValue,
                          0,
                          PROTOCOL_BINARY_DATATYPE_JSON);
    auto rv = public_processSet(item, 0);
    ASSERT_EQ(MutationStatus::WasClean, rv);

    auto* stored_item = findValue(key);
    EXPECT_NE(nullptr, stored_item);

    PauseResumeVBAdapter prAdapter(std::make_unique<ItemCompressorVisitor>());

    auto& visitor =
            dynamic_cast<ItemCompressorVisitor&>(prAdapter.getHTVisitor());
    visitor.setCompressionMode(BucketCompressionMode::Active);
    visitor.setMinCompressionRatio(config.getMinCompressionRatio());
    prAdapter.visit(*vbucket);

    std::string uncompressed_str(item.getData(), item.getNBytes());
    StoredValue* v = findValue(key);

    EXPECT_EQ(uncompressed_str, v->getValue()->to_s());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, v->getDatatype());
}

INSTANTIATE_TEST_SUITE_P(
        AllVBTypesAllEvictionModes,
        ItemCompressorTest,
        ::testing::Combine(
                ::testing::Values(VBucketTestBase::VBType::Persistent,
                                  VBucketTestBase::VBType::Ephemeral),
                ::testing::Values(EvictionPolicy::Value, EvictionPolicy::Full)),
        VBucketTest::PrintToStringParamName);
