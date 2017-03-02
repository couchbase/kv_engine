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
#include "stored-value.h"
#include "stats.h"
#include "tests/module_tests/test_helpers.h"

#include <gtest/gtest.h>

// Test fixture for StoredValue tests.
class StoredValueTest : public ::testing::Test {
public:
    StoredValueTest()
        : stats(),
          factory(stats),
          ht(stats, /*size:default*/0, /*locks*/1),
          item(make_item(0, makeStoredDocKey("key"), "value")) {}

    void SetUp() override {
        // Create an initial stored value for testing - key length (3) and
        // value length (5).
        sv = factory(item, {}, ht);
    }

    /// Returns the number of bytes in the Fixed part of StoredValue
    static size_t getStoredValueFixedSize() {
        return offsetof(StoredValue, key);
    }

    /// Allow testing access to StoredValue::getRequiredStorage
    static size_t public_getRequiredStorage(const Item& item) {
        return StoredValue::getRequiredStorage(item);
    }

protected:
    EPStats stats;
    StoredValueFactory factory;
    HashTable ht;
    Item item;
    std::unique_ptr<StoredValue> sv;
};

// Check that the size calculation methods return the expected sizes.

TEST_F(StoredValueTest, getObjectSize) {
    // Check the size are as expected: Fixed size of StoredValue, plus 3 bytes
    // for 'key', 1 byte for length of key and 1 byte for StoredDocKey namespace.
    EXPECT_EQ(getStoredValueFixedSize() + /*key*/3 + /*len*/1 + /*namespace*/1,
              sv->getObjectSize());
}

TEST_F(StoredValueTest, metaDataSize) {
    // Check metadata size reports correctly.
    EXPECT_EQ(getStoredValueFixedSize() + /*key*/3 + /*len*/1 + /*namespace*/1,
              sv->metaDataSize());
}

TEST_F(StoredValueTest, valuelen) {
    // Check valuelen reports correctly.
    EXPECT_EQ(/*value length*/ 5 + /*extmeta*/ 2, sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta";
}

TEST_F(StoredValueTest, size) {
    // Check size reports correctly.
    EXPECT_EQ(getStoredValueFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*namespace*/ 1 + /*valuelen*/ 5 + /*extmeta*/ 2,
              sv->size());
}

TEST_F(StoredValueTest, getRequiredStorage) {
    EXPECT_EQ(sv->getObjectSize(), public_getRequiredStorage(item))
            << "Actual object size doesn't match what getRequiredStorage "
               "predicted";
}
