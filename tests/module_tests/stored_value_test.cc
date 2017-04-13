/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/*
 * Unit tests for the StoredValue class.
 */

#include "config.h"

#include "hash_table.h"
#include "stats.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"

#include <gtest/gtest.h>

/**
 * Test fixture for StoredValue tests. Type-parameterized to test both
 * StoredValue and OrderedStoredValue.
 */
template <typename Factory>
class ValueTest : public ::testing::Test {
public:
    ValueTest()
        : stats(),
          factory(stats),
          ht(stats, /*size:default*/ 0, /*locks*/ 1),
          item(make_item(0, makeStoredDocKey("key"), "value")) {
    }

    void SetUp() override {
        // Create an initial stored value for testing - key length (3) and
        // value length (5).
        sv = factory(item, {}, ht);
    }

    /// Returns the number of bytes in the Fixed part of StoredValue
    static size_t getFixedSize() {
        return sizeof(typename Factory::value_type);
    }

    /// Allow testing access to StoredValue::getRequiredStorage
    static size_t public_getRequiredStorage(const Item& item) {
        return Factory::value_type::getRequiredStorage(item);
    }

protected:
    EPStats stats;
    Factory factory;
    HashTable ht;
    Item item;
    StoredValue::UniquePtr sv;
};

using ValueFactories =
        ::testing::Types<StoredValueFactory, OrderedStoredValueFactory>;
TYPED_TEST_CASE(ValueTest, ValueFactories);

// Check that the size calculation methods return the expected sizes.

TYPED_TEST(ValueTest, getObjectSize) {
    // Check the size are as expected: Fixed size of (Ordered)StoredValue, plus
    // 3 bytes
    // for 'key', 1 byte for length of key and 1 byte for StoredDocKey
    // namespace.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*namespace*/ 1,
              this->sv->getObjectSize());
}

TYPED_TEST(ValueTest, metaDataSize) {
    // Check metadata size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*namespace*/ 1,
              this->sv->metaDataSize());
}

TYPED_TEST(ValueTest, valuelen) {
    // Check valuelen reports correctly.
    EXPECT_EQ(/*value length*/ 5 + /*extmeta*/ 2, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta";
}

TYPED_TEST(ValueTest, valuelenDeletedWithValue) {
    // Check valuelen reports correctly for a StoredValue just marked delete
    // (with xattrs deleted items can have value)
    this->sv->markDeleted();
    EXPECT_EQ(/*value length*/ 5 + /*extmeta*/ 2, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta as we want to keep deleted body";
}

TYPED_TEST(ValueTest, valuelenDeletedWithoutValue) {
    // Check valuelen reports correctly for a StoredValue logically delete
    this->sv->del(this->ht);
    EXPECT_EQ(0, this->sv->valuelen())
            << "valuelen() expected to be 0 as we do not want to keep deleted "
               "body";
}

TYPED_TEST(ValueTest, size) {
    // Check size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*namespace*/ 1 + /*valuelen*/ 5 + /*extmeta*/ 2,
              this->sv->size());
}

TYPED_TEST(ValueTest, getRequiredStorage) {
    EXPECT_EQ(this->sv->getObjectSize(),
              this->public_getRequiredStorage(this->item))
            << "Actual object size doesn't match what getRequiredStorage "
               "predicted";
}

/// Check that StoredValue / OrderedStoredValue don't unexpectedly change in
/// size (we've carefully crafted them to be as efficient as possible).
TEST(StoredValueTest, expectedSize) {
    EXPECT_EQ(56, sizeof(StoredValue))
            << "Unexpected change in StoredValue fixed size";
    auto item = make_item(0, makeStoredDocKey("k"), "v");
    EXPECT_EQ(59, StoredValue::getRequiredStorage(item))
            << "Unexpected change in StoredValue storage size for item: "
            << item;
}

TEST(OrderedStoredValueTest, expectedSize) {
    EXPECT_EQ(72, sizeof(OrderedStoredValue))
            << "Unexpected change in OrderedStoredValue fixed size";
    auto item = make_item(0, makeStoredDocKey("k"), "v");
    EXPECT_EQ(75, OrderedStoredValue::getRequiredStorage(item))
            << "Unexpected change in OrderedStoredValue storage size for item: "
            << item;
}
