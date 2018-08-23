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

#include "../../daemon/alloc_hooks.h"
#include "hash_table.h"
#include "item_eviction.h"
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
          ht(stats,
             std::make_unique<Factory>(stats),
             /*size*/ 47,
             /*locks*/ 1),
          item(make_item(0, makeStoredDocKey("key"), "value")) {
    }

    void SetUp() override {
        // Create an initial stored value for testing - key length (3) and
        // value length (5).
        sv = factory(item, {});
    }

    /// Returns the number of bytes in the Fixed part of StoredValue
    static size_t getFixedSize() {
        return sizeof(typename Factory::value_type);
    }

    /// Allow testing access to StoredValue::getRequiredStorage
    static size_t public_getRequiredStorage(const DocKey& key) {
        return Factory::value_type::getRequiredStorage(key);
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
                      /*default collection-ID*/ 1,
              this->sv->getObjectSize());
}

/* Disabled if jemalloc is not in use as this test relies upon
 * the specific bin sizes used by jemalloc. Additionally, other
 * AllocHooks don't necessarily implement get_allocation_size.
 */
#if defined(HAVE_JEMALLOC)
TYPED_TEST(ValueTest, StoredValueReallocateGivesSameSize) {
#else
TYPED_TEST(ValueTest, DISABLED_StoredValueReallocateGivesSameSize) {
#endif

    /* MB-25143: Ensure reallocation doesn't allocate excess bytes
     * Make an item with a value of size 182.
     * sizeof(Blob) = 12, but three of those bytes are padding
     * used for the data. Therefore, the allocation size for the blob is
     * blob.size + sizeof(Blob) - 3
     * 182 + 12 - 3 = 191
     * Jemalloc has a bin of size 192, which should be chosen for
     * the allocation of the blob.
     * As noted in MB-25143, the reallocation done by the defragmenter
     * overallocated by two bytes. This would push it over the bin size
     */

    auto sv = this->factory(
            make_item(0,
                      makeStoredDocKey(std::string(10, 'k').c_str()),
                      std::string(182, 'v').c_str()),
            {});

    auto blob = sv->getValue();
    ASSERT_EQ(191, blob->getSize());
    int before = AllocHooks::get_allocation_size(blob.get().get());

    /* While the initial bug in MB-25143 would only increase the size of
     * the blob once, by two bytes, we iterate here to ensure that there
     * is no slow increase with each reallocation. We would only see
     * an increase in je_malloc_usable_size once a bin size is exceeded.
     * ( in this case, the bin size is 192)
     */
    for (int i = 0; i < 100; ++i) {
        sv->reallocate();

        blob = sv->getValue();
        int after = AllocHooks::get_allocation_size(blob.get().get());

        EXPECT_EQ(before, after);
    }
}

/* MB-30097: Set the stored value as uncompressible and ensure that
 * the size of the Blob doesn't change on a reallocation
 */
TYPED_TEST(ValueTest, StoredValueUncompressibleReallocateGivesSameSize) {
    auto sv = this->factory(
            make_item(0,
                      makeStoredDocKey(std::string(10, 'k').c_str()),
                      std::string(182, 'v').c_str()),
            {});

    auto blob = sv->getValue();
    int beforeValueSize = blob->valueSize();
    sv->setUncompressible();
    sv->reallocate();
    blob = sv->getValue();
    EXPECT_EQ(beforeValueSize, blob->valueSize());
}

TYPED_TEST(ValueTest, metaDataSize) {
    // Check metadata size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*default collection-ID*/ 1,
              this->sv->metaDataSize());
}

TYPED_TEST(ValueTest, valuelen) {
    // Check valuelen reports correctly.
    EXPECT_EQ(/*value length*/ 5, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta";
}

TYPED_TEST(ValueTest, valuelenDeletedWithValue) {
    // Check valuelen reports correctly for a StoredValue just marked delete
    // (with xattrs deleted items can have value)
    this->sv->markDeleted();
    EXPECT_EQ(/*value length*/ 5, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta as we want to keep deleted body";
}

TYPED_TEST(ValueTest, valuelenDeletedWithoutValue) {
    // Check valuelen reports correctly for a StoredValue logically delete
    this->sv->del();
    EXPECT_TRUE(this->sv->isResident());
    EXPECT_EQ(0, this->sv->valuelen())
            << "valuelen() expected to be 0 as we do not want to keep deleted "
               "body";
}

TYPED_TEST(ValueTest, size) {
    // Check size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*default collection-ID*/ 1 + /*valuelen*/ 5,
              this->sv->size());
}

TYPED_TEST(ValueTest, getRequiredStorage) {
    EXPECT_EQ(this->sv->getObjectSize(),
              this->public_getRequiredStorage(this->item.getKey()))
            << "Actual object size doesn't match what getRequiredStorage "
               "predicted";
}

/// Check if the value is resident. Also, check for
/// residency once the value has been ejected
TYPED_TEST(ValueTest, checkIfResident) {
    EXPECT_TRUE(this->sv->getValue());
    EXPECT_TRUE(this->sv->isResident());

    this->sv->ejectValue();
    EXPECT_FALSE(this->sv->getValue());
    EXPECT_FALSE(this->sv->isResident());
}

/// Check if the value is resident for a temporary
/// item
TYPED_TEST(ValueTest, checkIfTempItemIsResident) {
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t) TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             StoredValue::state_temp_init);
    this->sv->setValue(itm);
    EXPECT_TRUE(this->sv->isTempItem());
    EXPECT_FALSE(this->sv->isResident());
}

/// Check if the deleted item is resident or not
TYPED_TEST(ValueTest, checkIfDeletedWithValueIsResident) {
    ///Just mark the value as deleted. There should
    ///be a value and the item should be resident
    this->sv->markDeleted();
    EXPECT_TRUE(this->sv->getValue());
    EXPECT_TRUE(this->sv->isResident());

    this->sv->ejectValue();
    EXPECT_FALSE(this->sv->isResident());
}

/** Check that when an item is deleted (with no value) its datatype is set
 * to RAW_BYTES.
 */
TYPED_TEST(ValueTest, deletedValueDatatypeIsBinary) {
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, this->sv->getDatatype());
    this->sv->del();
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, this->sv->getDatatype())
            << "datatype should be RAW BYTES after deletion.";
}

/**
 * Check that NRU still works (given we now do some bitfield munging)
 */
TYPED_TEST(ValueTest, nru) {
    EXPECT_EQ(INITIAL_NRU_VALUE, this->sv->getNRUValue());
    this->sv->incrNRUValue();
    EXPECT_EQ(INITIAL_NRU_VALUE + 1, this->sv->getNRUValue());
    this->sv->referenced();
    EXPECT_EQ(INITIAL_NRU_VALUE, this->sv->getNRUValue());
}

/**
 * Test the get / set of the frequency counter
 */
TYPED_TEST(ValueTest, freqCounter) {
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(1);
    EXPECT_EQ(1, this->sv->getFreqCounterValue());
}

TYPED_TEST(ValueTest, initialFreqCounterForTemp) {
    Item itm = make_item(0,
                         makeStoredDocKey(std::string("key").c_str()),
                         std::string("value").c_str());
    itm.setBySeqno(StoredValue::state_temp_init);

    auto storedVal = this->factory(itm, {});

    ASSERT_TRUE(storedVal->isTempItem());
    EXPECT_EQ(0, storedVal->getFreqCounterValue());
}

TYPED_TEST(ValueTest, replaceValue) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto sv = this->factory(
            make_item(0,
                      makeStoredDocKey(std::string("key").c_str()),
                      std::string("value").c_str()),
            {});

    this->sv->replaceValue(sv->getValue().get());
    EXPECT_EQ(100, this->sv->getFreqCounterValue());
}

TYPED_TEST(ValueTest, restoreValue) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto itm = make_item(0,
                      makeStoredDocKey(std::string("key").c_str()),
                      std::string("value").c_str());

    this->sv->restoreValue(itm);
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
}

TYPED_TEST(ValueTest, restoreMeta) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto itm = make_item(0,
                      makeStoredDocKey(std::string("key").c_str()),
                      std::string("value").c_str());

    this->sv->restoreMeta(itm);
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
}

/// Check that StoredValue / OrderedStoredValue don't unexpectedly change in
/// size (we've carefully crafted them to be as efficient as possible).
TEST(StoredValueTest, expectedSize) {
    EXPECT_EQ(56, sizeof(StoredValue))
            << "Unexpected change in StoredValue fixed size";
    auto key = makeStoredDocKey("k");
    EXPECT_EQ(59, StoredValue::getRequiredStorage(key))
            << "Unexpected change in StoredValue storage size for key: " << key;
}

/**
 * Test fixture for OrderedStoredValue-only tests.
 */
class OrderedStoredValueTest : public ValueTest<OrderedStoredValueFactory> {};

TEST_F(OrderedStoredValueTest, expectedSize) {
    EXPECT_EQ(72, sizeof(OrderedStoredValue))
            << "Unexpected change in OrderedStoredValue fixed size";

    auto key = makeStoredDocKey("k");
    EXPECT_EQ(75, OrderedStoredValue::getRequiredStorage(key))
            << "Unexpected change in OrderedStoredValue storage size for key: "
            << key;
}

// Check that when we copy a OSV, the freqCounter is also copied. (Cannot copy
// StoredValues, hence no version for them).
TEST_F(OrderedStoredValueTest, copyStoreValue) {
    ASSERT_EQ(4, sv->getFreqCounterValue());
    sv->setFreqCounterValue(100);
    ASSERT_EQ(100, sv->getFreqCounterValue());

    auto copy = factory.copyStoredValue(*sv, {});

    EXPECT_EQ(100, copy->getFreqCounterValue());
}
